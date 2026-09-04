//! Cron-job mutations shared by every caller that creates or changes a job.
//!
//! The `cron` tool (in conversation), the gateway's `/api/cron/*` handlers and
//! the direct CLI adapter all funnel through these functions, so validation —
//! schedule parsing, name uniqueness, the initial `next_run_at` — cannot fork
//! between the paths. `OperatorActions` wraps them for the operator-control
//! surface; it does not reimplement them.

use komo_core::domain::cron::{
    CronAction, CronJob, CronJobRepository, CronJobSpec, CronJobStatus,
    DEFAULT_CRON_JOB_TIMEOUT_SECS, FeishuMatch, MAX_ANY_TRIGGERS, MAX_CRON_JOB_NAME_LEN, Trigger,
    compile_glob, next_occurrence_local, schedule_is_once, valid_cron_job_name,
};
use komo_core::domain::policy::{Matcher, RuleSpec};

/// The one place a trigger *string* becomes a [`Trigger`]. Both string-holding
/// callers — the `komo cron` CLI and the agent's `cron` tool — go through it,
/// so every written form is accepted identically on both.
///
/// - `0 8 * * *` — a 5-field cron expression, local time.
/// - `@at YYYY-MM-DD HH:MM` — one local moment; the job completes after it.
/// - `@webhook <name>` — `POST /api/hooks/<name>`.
/// - `@feishu <chat> mention` / `keyword a,b` / `reaction <emoji>`.
/// - `@file <root> [glob]` — a file under `root` matching `glob` changed.
/// - `a | b` — any of them fires it (an [`Trigger::Any`], at most
///   [`MAX_ANY_TRIGGERS`] members). `|` appears in none of the forms above, so
///   splitting on it is unambiguous.
///
/// Parsing here also proves the trigger actionable — a past `@at`, an unknown
/// feishu match, a glob that does not compile are all refused while the person
/// who typed them is still there.
pub fn parse_schedule(schedule: &str, now: i64) -> anyhow::Result<Trigger> {
    let schedule = schedule.trim();
    if schedule.is_empty() {
        anyhow::bail!("a cron job needs a schedule");
    }
    if schedule.contains('|') {
        let triggers = schedule
            .split('|')
            .map(|member| parse_one_trigger(member.trim(), now))
            .collect::<anyhow::Result<Vec<_>>>()?;
        return Ok(Trigger::Any { triggers });
    }
    parse_one_trigger(schedule, now)
}

fn parse_one_trigger(schedule: &str, now: i64) -> anyhow::Result<Trigger> {
    if schedule.is_empty() {
        anyhow::bail!("a cron job needs a schedule");
    }
    let event = match schedule.split_once(char::is_whitespace) {
        Some((word, rest)) if word.starts_with('@') && word != "@at" => Some((word, rest.trim())),
        _ => None,
    };
    let Some((word, rest)) = event else {
        let at = next_occurrence_local(schedule, now)?;
        return Ok(match schedule_is_once(schedule) {
            true => Trigger::At { at },
            false => Trigger::cron(schedule),
        });
    };
    match word {
        "@webhook" => {
            if rest.is_empty() {
                anyhow::bail!("`@webhook` needs a name, e.g. `@webhook ci-done`");
            }
            Ok(Trigger::Webhook {
                name: rest.to_string(),
            })
        }
        "@feishu" => {
            let (chat, rule) = rest
                .split_once(char::is_whitespace)
                .ok_or_else(|| anyhow::anyhow!("`@feishu` needs a chat id and a match rule"))?;
            let (kind, argument) = match rule.trim().split_once(char::is_whitespace) {
                Some((kind, argument)) => (kind, argument.trim()),
                None => (rule.trim(), ""),
            };
            let matcher = match kind {
                "mention" => FeishuMatch::Mention,
                "keyword" => {
                    let keywords: Vec<String> = argument
                        .split(',')
                        .map(str::trim)
                        .filter(|k| !k.is_empty())
                        .map(str::to_string)
                        .collect();
                    if keywords.is_empty() {
                        anyhow::bail!("`@feishu <chat> keyword` needs at least one keyword");
                    }
                    FeishuMatch::Keyword { keywords }
                }
                "reaction" => {
                    if argument.is_empty() {
                        anyhow::bail!("`@feishu <chat> reaction` needs an emoji name");
                    }
                    FeishuMatch::Reaction {
                        emoji: argument.to_string(),
                    }
                }
                other => anyhow::bail!(
                    "unknown feishu match `{other}` (mention | keyword <a,b> | reaction <emoji>)"
                ),
            };
            Ok(Trigger::Feishu {
                chat: chat.to_string(),
                matcher,
            })
        }
        "@file" => {
            let (root, glob) = match rest.split_once(char::is_whitespace) {
                Some((root, glob)) => (root, glob.trim()),
                None => (rest, ""),
            };
            if root.is_empty() {
                anyhow::bail!("`@file` needs a directory, e.g. `@file /srv/notes **/*.md`");
            }
            Ok(Trigger::FileChanged {
                root: std::path::PathBuf::from(root),
                glob: glob.to_string(),
            })
        }
        other => anyhow::bail!(
            "unknown trigger `{other}` (@at | @webhook | @feishu | @file, or a cron expression)"
        ),
    }
}

/// Reject a trigger nothing could act on, and resolve what has to be resolved
/// at creation time, before it reaches the store.
fn validate_trigger(trigger: Trigger, now: i64) -> anyhow::Result<Trigger> {
    if let Trigger::Any { triggers } = &trigger {
        if triggers.is_empty() {
            anyhow::bail!("an `any` trigger needs at least one listener");
        }
        if triggers.len() > MAX_ANY_TRIGGERS {
            anyhow::bail!("an `any` trigger holds at most {MAX_ANY_TRIGGERS} listeners");
        }
        if triggers.iter().any(|t| matches!(t, Trigger::Any { .. })) {
            anyhow::bail!("`any` triggers do not nest");
        }
    }
    let trigger = normalize_event_trigger(trigger)?;
    // Proves every cron member parses, and that a schedule-shaped trigger still
    // has a future — a `@at` moment that has passed schedules nothing.
    if trigger.next_slot(now)?.is_none() && trigger.is_scheduled() {
        anyhow::bail!("`{}` is already past", trigger.describe());
    }
    Ok(trigger)
}

/// The event-shaped triggers' own creation-time proofs, and the one thing that
/// has to be resolved rather than checked.
///
/// A watched directory is canonicalized **now**, while the person who typed it
/// is still there — the same reason an agent job's workspace is. A path
/// resolved late would fail at the gateway's next start, and a routine that
/// never fires looks exactly like one nothing ever happened for; a symlinked
/// root would also fail to prefix-match the paths the watcher reports.
fn normalize_event_trigger(trigger: Trigger) -> anyhow::Result<Trigger> {
    match trigger {
        Trigger::Webhook { name } => {
            let name = name.trim().to_string();
            if name.is_empty() {
                anyhow::bail!("a webhook trigger needs a name");
            }
            if name.contains('/') {
                anyhow::bail!("`{name}` is not a webhook name: it is one path segment");
            }
            Ok(Trigger::Webhook { name })
        }
        Trigger::Feishu { chat, matcher } => {
            let chat = chat.trim().to_string();
            if chat.is_empty() {
                anyhow::bail!("a feishu trigger needs a chat id");
            }
            let matcher = match matcher {
                FeishuMatch::Keyword { keywords } => {
                    let keywords: Vec<String> = keywords
                        .into_iter()
                        .map(|k| k.trim().to_string())
                        .filter(|k| !k.is_empty())
                        .collect();
                    if keywords.is_empty() {
                        anyhow::bail!("a feishu keyword trigger needs at least one keyword");
                    }
                    FeishuMatch::Keyword { keywords }
                }
                other => other,
            };
            Ok(Trigger::Feishu { chat, matcher })
        }
        Trigger::FileChanged { root, glob } => {
            let glob = glob.trim().to_string();
            compile_glob(&glob).map_err(|e| anyhow::anyhow!("invalid glob `{glob}`: {e}"))?;
            let raw = root.to_string_lossy().into_owned();
            let resolved = komo_config::expand_home(&raw)
                .canonicalize()
                .map_err(|e| anyhow::anyhow!("watched directory `{raw}` cannot be used: {e}"))?;
            if !resolved.is_dir() {
                anyhow::bail!("watched path `{raw}` is not a directory");
            }
            Ok(Trigger::FileChanged {
                root: resolved,
                glob,
            })
        }
        Trigger::Any { triggers } => Ok(Trigger::Any {
            triggers: triggers
                .into_iter()
                .map(normalize_event_trigger)
                .collect::<anyhow::Result<Vec<_>>>()?,
        }),
        clock => Ok(clock),
    }
}

/// Validate a job spec and create it — schedule parsed with the same cron
/// parser the sweep uses (so nothing invalid ever reaches the store), name
/// uniqueness enforced, and the initial `next_run_at` computed from now.
/// Shared by the gateway's `/api/cron/add` handler and the direct adapter, so
/// validation can't fork between the two paths.
pub async fn add_cron_job(
    jobs: &dyn CronJobRepository,
    spec: CronJobSpec,
    now: i64,
) -> anyhow::Result<CronJob> {
    let name = spec.name.trim();
    if name.is_empty() {
        anyhow::bail!("a cron job needs a name");
    }
    // A name is a key (every `komo cron` subcommand, and an agent job's session
    // id) — keep it key-shaped. Matters most on the agent's `cron` tool path,
    // where the name is model-authored.
    if !valid_cron_job_name(name) {
        anyhow::bail!(
            "invalid job name `{name}`: no whitespace or `:` `/` `\\`, at most \
             {MAX_CRON_JOB_NAME_LEN} characters"
        );
    }
    // Normalize + validate the action per kind.
    let action = match spec.action {
        CronAction::Command {
            command,
            args,
            workdir,
            timeout_secs,
        } => {
            if command.trim().is_empty() {
                anyhow::bail!("a command cron job needs a command");
            }
            CronAction::Command {
                command: command.trim().to_string(),
                args,
                workdir: workdir.filter(|w| !w.trim().is_empty()),
                timeout_secs: if timeout_secs > 0 {
                    timeout_secs
                } else {
                    DEFAULT_CRON_JOB_TIMEOUT_SECS
                },
            }
        }
        CronAction::Agent {
            prompt,
            skills,
            workspace,
        } => {
            if prompt.trim().is_empty() {
                anyhow::bail!("an agent cron job needs a prompt");
            }
            CronAction::Agent {
                prompt: prompt.trim().to_string(),
                skills: skills
                    .into_iter()
                    .filter(|s| !s.trim().is_empty())
                    .collect(),
                workspace: resolve_workspace(workspace)?,
            }
        }
    };
    if jobs.find_by_name(name).await?.is_some() {
        anyhow::bail!("a cron job named `{name}` already exists");
    }
    let trigger = validate_trigger(spec.trigger, now)?;
    // An event-only trigger has no moment to wait for: `0` is what keeps the
    // sweep from reading "due since the epoch".
    let next_run_at = trigger
        .next_slot(now)?
        .map(|(at, _)| at)
        .unwrap_or_default();
    let mut job = CronJob::new(name, trigger, action, next_run_at)
        .with_grants(normalize_grants(spec.grants)?);
    job.catch_up = spec.catch_up;
    job.notify = spec.notify;
    jobs.save(&job).await?;
    Ok(job)
}

/// Resolve an agent job's workspace to a canonical absolute directory.
///
/// Checked **now**, while whoever asked for the job is still here. The
/// alternative is a job that looks fine in `cron list` and fails at 03:00
/// against a path with a typo in it — and unlike a bad prompt, this one fails
/// as a permission refusal on every file the turn touches, which reads like a
/// policy problem rather than a spelling one.
///
/// Canonical because the workspace check is lexical: a root given as a symlink
/// would never prefix-match the real paths the tools resolve, and the turn would
/// be denied its own directory.
fn resolve_workspace(workspace: Option<String>) -> anyhow::Result<Option<String>> {
    let Some(raw) = workspace
        .map(|w| w.trim().to_string())
        .filter(|w| !w.is_empty())
    else {
        return Ok(None);
    };
    let path = komo_config::expand_home(&raw);
    let resolved = path
        .canonicalize()
        .map_err(|e| anyhow::anyhow!("workspace `{raw}` cannot be used: {e}"))?;
    if !resolved.is_dir() {
        anyhow::bail!("workspace `{raw}` is not a directory");
    }
    Ok(Some(resolved.to_string_lossy().into_owned()))
}

/// Validate and normalize the grants a job is created with.
///
/// The caller supplies only *what* to allow — category, matcher, value. The
/// rest of the rule's shape is fixed here rather than accepted:
///
/// - `effect = allow` — a job grant is a whitelist; a denial belongs in config,
///   where it applies to everything rather than to one job;
/// - `unattended = true` — granting a job an action it can only take with a
///   human present would grant nothing at all;
/// - `include_dangerous = false` — that stays a config-only opt-in, the same
///   floor a saved grant has;
/// - `channels = None` — a job's turn has no channel to scope to, so a channel
///   scope here could only ever make the grant silently dead.
///
/// A malformed entry is a **hard error**, never dropped: a silently-discarded
/// grant produces a job that is refused at 3am for a reason nobody can see, and
/// the operator already approved a list they believed was complete.
pub fn normalize_grants(grants: Vec<RuleSpec>) -> anyhow::Result<Vec<RuleSpec>> {
    grants
        .into_iter()
        .map(|mut spec| {
            spec.effect = "allow".to_string();
            spec.unattended = true;
            spec.include_dangerous = false;
            spec.channels = None;
            // An entry with neither `match` nor `value` is the whole-category
            // wildcard, spelled explicitly so `describe()` reads as one. A
            // matcher with an empty value stays invalid — the same rule config
            // parsing uses, so a typo can never widen into "everything".
            if spec.matcher.trim().is_empty() && spec.value.trim().is_empty() {
                spec.matcher = "any".to_string();
            }
            let matcher_is_any = Matcher::parse(&spec.matcher) == Some(Matcher::Any);
            if !matcher_is_any && spec.value.trim().is_empty() {
                anyhow::bail!(
                    "grant `{}` has an empty value: give a target, or use match = \"any\" \
                     to mean the whole category",
                    spec.category
                );
            }
            spec.value = spec.value.trim().to_string();
            if spec.to_rule().is_none() {
                anyhow::bail!(
                    "invalid grant `{} {} {}`: category must be one of \
                     shell/file/network/homeassistant/mcp/wiki, match one of \
                     prefix/suffix/exact/contains/any, access one of read/write",
                    spec.category,
                    spec.matcher,
                    spec.value
                );
            }
            Ok(spec)
        })
        .collect()
}

/// Pause or resume a job; `None` = no such job. Resuming recomputes
/// `next_run_at` from now — a stale past slot must not fire the moment the job
/// comes back (a broken-schedule job that the sweep paused keeps its stored
/// expression, so this also surfaces the parse error to the operator; a
/// one-shot whose moment has passed is refused the same way). A completed
/// one-shot is terminal: its row is the record of what ran, never a schedule.
pub async fn set_cron_enabled(
    jobs: &dyn CronJobRepository,
    name: &str,
    enabled: bool,
    now: i64,
) -> anyhow::Result<Option<CronJob>> {
    let Some(mut job) = jobs.find_by_name(name).await? else {
        return Ok(None);
    };
    if job.status == CronJobStatus::Done {
        anyhow::bail!(
            "cron job `{name}` already completed (one-shot) — create a new job to run it again"
        );
    }
    if enabled && job.status == CronJobStatus::Paused {
        job.next_run_at = match job.trigger.next_slot(now)? {
            Some((at, _)) => at,
            None if job.trigger.is_scheduled() => anyhow::bail!(
                "cron job `{name}` has no future occurrence left (`{}` is already past) — \
                 create a new job to run it again",
                job.trigger.describe()
            ),
            // An event-only routine has nothing to schedule; resuming it just
            // puts it back in earshot.
            None => 0,
        };
    }
    job.status = if enabled {
        CronJobStatus::Active
    } else {
        CronJobStatus::Paused
    };
    jobs.update(&job).await?;
    Ok(Some(job))
}

/// Make a job due immediately (the sweep picks it up on its next tick);
/// `None` = no such job. The job must be active — triggering a paused job
/// would silently do nothing until someone resumed it, and a completed
/// one-shot is terminal.
pub async fn trigger_cron_job(
    jobs: &dyn CronJobRepository,
    name: &str,
    now: i64,
) -> anyhow::Result<Option<CronJob>> {
    let Some(mut job) = jobs.find_by_name(name).await? else {
        return Ok(None);
    };
    match job.status {
        CronJobStatus::Done => anyhow::bail!(
            "cron job `{name}` already completed (one-shot) — create a new job to run it again"
        ),
        CronJobStatus::Paused => {
            anyhow::bail!("cron job `{name}` is paused — enable it first (`komo cron enable`)")
        }
        CronJobStatus::Active => {}
    }
    job.next_run_at = now;
    jobs.update(&job).await?;
    Ok(Some(job))
}

/// One uniform unknown-job message (the gateway's 404 body and the direct
/// path's error must read identically).
pub fn no_cron_job_message(name: &str) -> String {
    format!("no cron job named `{name}`")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeJobs {
        jobs: Mutex<Vec<CronJob>>,
    }

    #[async_trait::async_trait]
    impl CronJobRepository for FakeJobs {
        async fn save(&self, job: &CronJob) -> anyhow::Result<()> {
            self.jobs.lock().unwrap().push(job.clone());
            Ok(())
        }
        async fn list(&self) -> anyhow::Result<Vec<CronJob>> {
            Ok(self.jobs.lock().unwrap().clone())
        }
        async fn find_by_name(&self, name: &str) -> anyhow::Result<Option<CronJob>> {
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .iter()
                .find(|j| j.name == name)
                .cloned())
        }
        async fn update(&self, job: &CronJob) -> anyhow::Result<()> {
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(slot) = jobs.iter_mut().find(|j| j.id == job.id) {
                *slot = job.clone();
            }
            Ok(())
        }
        async fn delete(&self, name: &str) -> anyhow::Result<bool> {
            let mut jobs = self.jobs.lock().unwrap();
            let before = jobs.len();
            jobs.retain(|j| j.name != name);
            Ok(jobs.len() != before)
        }
    }

    fn done_job(name: &str) -> CronJob {
        let mut job = CronJob::new_command(name, Trigger::At { at: 100 }, "/bin/true", 0);
        job.status = CronJobStatus::Done;
        let run = job.begin_run(100, "@at".into());
        job.finish_run(
            &run,
            komo_core::domain::cron::RoutineRunStatus::Ok,
            "",
            None,
        );
        job
    }

    /// A completed one-shot is terminal — its row is the record of what ran.
    /// Both resume and trigger refuse rather than resurrecting a past moment.
    #[tokio::test]
    async fn a_completed_one_shot_refuses_enable_and_trigger() {
        let jobs = FakeJobs::default();
        jobs.save(&done_job("once")).await.unwrap();

        let err = set_cron_enabled(&jobs, "once", true, 1000)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already completed"), "{err}");
        let err = trigger_cron_job(&jobs, "once", 1000).await.unwrap_err();
        assert!(err.to_string().contains("already completed"), "{err}");
        assert_eq!(
            jobs.find_by_name("once").await.unwrap().unwrap().status,
            CronJobStatus::Done,
            "refusal must not mutate the record"
        );
    }

    fn command_spec(name: &str, trigger: Trigger) -> CronJobSpec {
        CronJobSpec {
            catch_up: Default::default(),
            notify: Default::default(),
            name: name.into(),
            trigger,
            action: CronAction::Command {
                command: "/bin/true".into(),
                args: vec![],
                workdir: None,
                timeout_secs: 0,
            },
            grants: vec![],
        }
    }

    #[tokio::test]
    async fn add_accepts_a_future_one_shot_and_rejects_a_past_one() {
        let jobs = FakeJobs::default();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let job = add_cron_job(
            &jobs,
            command_spec(
                "reboot-nas",
                parse_schedule("@at 2030-01-02 08:30", now).unwrap(),
            ),
            now,
        )
        .await
        .unwrap();
        assert!(job.is_once());
        assert!(job.next_run_at > now);
        assert_eq!(job.status, CronJobStatus::Active);

        // Refused at the parse, where the person who typed it still is.
        let err = parse_schedule("@at 2020-01-01 08:00", now).unwrap_err();
        assert!(err.to_string().contains("already past"), "{err}");
        // …and again at the store, for a structured trigger that never passed
        // through a string.
        let err = add_cron_job(
            &jobs,
            command_spec("too-late", Trigger::At { at: 100 }),
            now,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("already past"), "{err}");
        assert!(jobs.find_by_name("too-late").await.unwrap().is_none());
    }

    /// The single string→trigger parse site: a 5-field expression stays one, an
    /// `@at` becomes the moment it names.
    #[tokio::test]
    async fn parse_schedule_maps_both_written_forms() {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        assert_eq!(
            parse_schedule("0 8 * * *", now).unwrap(),
            Trigger::cron("0 8 * * *")
        );
        let Trigger::At { at } = parse_schedule("@at 2030-01-02 08:30", now).unwrap() else {
            panic!("@at is a one-shot moment");
        };
        assert!(at > now);
        assert!(parse_schedule("not a cron", now).is_err());
        assert!(parse_schedule("  ", now).is_err());
    }

    /// Both string-holding callers — the CLI and the agent's `cron` tool — go
    /// through this one parse, so every trigger shape has to be writable in it
    /// or half of §5.12–5.14 is unreachable from the surfaces people use.
    #[test]
    fn every_event_trigger_is_writable_as_a_string() {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        assert_eq!(
            parse_schedule("@webhook ci-done", now).unwrap(),
            Trigger::Webhook {
                name: "ci-done".into()
            }
        );
        assert_eq!(
            parse_schedule("@feishu oc_team mention", now).unwrap(),
            Trigger::Feishu {
                chat: "oc_team".into(),
                matcher: FeishuMatch::Mention
            }
        );
        assert_eq!(
            parse_schedule("@feishu oc_team keyword 值班, oncall", now).unwrap(),
            Trigger::Feishu {
                chat: "oc_team".into(),
                matcher: FeishuMatch::Keyword {
                    keywords: vec!["值班".into(), "oncall".into()]
                }
            }
        );
        assert_eq!(
            parse_schedule("@feishu oc_team reaction THUMBSUP", now).unwrap(),
            Trigger::Feishu {
                chat: "oc_team".into(),
                matcher: FeishuMatch::Reaction {
                    emoji: "THUMBSUP".into()
                }
            }
        );
        assert_eq!(
            parse_schedule("@file /srv/notes **/*.md", now).unwrap(),
            Trigger::FileChanged {
                root: "/srv/notes".into(),
                glob: "**/*.md".into()
            }
        );
        // No glob is "anything under the root".
        assert_eq!(
            parse_schedule("@file /srv/notes", now).unwrap(),
            Trigger::FileChanged {
                root: "/srv/notes".into(),
                glob: String::new()
            }
        );
        // `|` appears in none of the written forms, so it can only mean "any".
        assert_eq!(
            parse_schedule("0 8 * * * | @webhook ci", now).unwrap(),
            Trigger::Any {
                triggers: vec![
                    Trigger::cron("0 8 * * *"),
                    Trigger::Webhook { name: "ci".into() }
                ]
            }
        );

        for bad in [
            "@webhook",
            "@feishu oc_team",
            "@feishu oc_team shouted",
            "@feishu oc_team keyword",
            "@feishu oc_team reaction",
            "@file",
            "@nonsense x",
        ] {
            assert!(parse_schedule(bad, now).is_err(), "`{bad}` must be refused");
        }
    }

    /// A watched directory is resolved when the routine is created, like an
    /// agent job's workspace and for the same reason: a typo has to fail while
    /// the person who typed it is still there, not silently at the next start.
    #[tokio::test]
    async fn a_watched_directory_is_proven_and_canonicalized_at_creation() {
        let jobs = FakeJobs::default();
        let missing = add_cron_job(
            &jobs,
            command_spec(
                "watch-nowhere",
                Trigger::FileChanged {
                    root: "/definitely/not/here".into(),
                    glob: "**/*".into(),
                },
            ),
            1000,
        )
        .await;
        assert!(missing.is_err(), "a directory that is not there is refused");

        let bad_glob = add_cron_job(
            &jobs,
            command_spec(
                "watch-badglob",
                Trigger::FileChanged {
                    root: std::env::temp_dir(),
                    glob: "[".into(),
                },
            ),
            1000,
        )
        .await;
        assert!(
            bad_glob.is_err(),
            "a glob that will never compile is refused"
        );

        let job = add_cron_job(
            &jobs,
            command_spec(
                "watch-temp",
                Trigger::FileChanged {
                    root: std::env::temp_dir(),
                    glob: "**/*.md".into(),
                },
            ),
            1000,
        )
        .await
        .unwrap();
        let Trigger::FileChanged { root, .. } = &job.trigger else {
            panic!("stored as written");
        };
        assert_eq!(root, &std::env::temp_dir().canonicalize().unwrap());
    }

    /// An event-only routine is stored with no moment at all, so the sweep
    /// reads it as waiting rather than as due since the epoch.
    #[tokio::test]
    async fn an_event_only_trigger_schedules_nothing() {
        let jobs = FakeJobs::default();
        let job = add_cron_job(
            &jobs,
            command_spec("on-ci", Trigger::Webhook { name: "ci".into() }),
            1000,
        )
        .await
        .unwrap();
        assert_eq!(job.next_run_at, 0);
        assert!(!job.is_due(i64::MAX));
    }

    /// `Any` schedules to its soonest member, and its shape is checked before
    /// it is stored.
    #[tokio::test]
    async fn an_any_trigger_is_bounded_and_schedules_to_its_soonest_member() {
        let jobs = FakeJobs::default();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let soon = now + 60;
        let job = add_cron_job(
            &jobs,
            command_spec(
                "either",
                Trigger::Any {
                    triggers: vec![Trigger::cron("0 8 * * *"), Trigger::At { at: soon }],
                },
            ),
            now,
        )
        .await
        .unwrap();
        assert_eq!(job.next_run_at, soon);

        for bad in [
            Trigger::Any { triggers: vec![] },
            Trigger::Any {
                triggers: vec![Trigger::cron("0 8 * * *"); MAX_ANY_TRIGGERS + 1],
            },
            Trigger::Any {
                triggers: vec![Trigger::Any {
                    triggers: vec![Trigger::cron("0 8 * * *")],
                }],
            },
            Trigger::Any {
                triggers: vec![Trigger::cron("not a cron")],
            },
        ] {
            assert!(
                add_cron_job(&jobs, command_spec("nope", bad.clone()), now)
                    .await
                    .is_err(),
                "{bad:?}"
            );
        }
    }

    /// The two per-job settings the operator gives at creation are stored, not
    /// dropped for the defaults.
    #[tokio::test]
    async fn catch_up_and_notify_survive_creation() {
        use komo_core::domain::cron::{CatchUp, NotifyPolicy};
        let jobs = FakeJobs::default();
        let mut spec = command_spec("lights", Trigger::cron("0 23 * * *"));
        spec.catch_up = CatchUp::Skip;
        spec.notify = NotifyPolicy::OnError;
        let job = add_cron_job(&jobs, spec, 1000).await.unwrap();
        assert_eq!(job.catch_up, CatchUp::Skip);
        assert_eq!(job.notify, NotifyPolicy::OnError);
    }

    fn spec(category: &str, matcher: &str, value: &str) -> RuleSpec {
        RuleSpec {
            category: category.to_string(),
            matcher: matcher.to_string(),
            value: value.to_string(),
            access: None,
            channels: None,
            effect: String::new(),
            include_dangerous: false,
            unattended: false,
        }
    }

    /// The caller says *what* to allow; the rule's shape is not up to them.
    #[test]
    fn normalize_fixes_the_rule_shape() {
        let mut asked = spec("homeassistant", "exact", "climate.set_temperature");
        asked.effect = "deny".into();
        asked.include_dangerous = true;
        asked.channels = Some(vec!["feishu".into()]);

        let out = normalize_grants(vec![asked]).unwrap();
        assert_eq!(out[0].effect, "allow");
        assert!(
            out[0].unattended,
            "a grant that needs a human grants nothing"
        );
        assert!(!out[0].include_dangerous, "dangerous stays config-only");
        assert_eq!(
            out[0].channels, None,
            "a job turn has no channel to scope to"
        );
    }

    /// Omitting both `match` and `value` is the explicit whole-category
    /// wildcard, so `describe()` reads as one rather than as an empty pattern.
    #[test]
    fn an_empty_entry_becomes_the_explicit_wildcard() {
        let out = normalize_grants(vec![spec("shell", "", "")]).unwrap();
        assert_eq!(out[0].matcher, "any");
    }

    /// …but a real matcher with an empty value stays an error: a typo must
    /// never widen a grant into "everything".
    #[test]
    fn a_matcher_with_an_empty_value_is_rejected() {
        assert!(normalize_grants(vec![spec("shell", "prefix", "  ")]).is_err());
    }

    /// A bad entry fails the whole call rather than being dropped — a silently
    /// discarded grant becomes a job refused at 3am for no visible reason.
    #[test]
    fn an_unparseable_grant_is_an_error_not_a_drop() {
        let err = normalize_grants(vec![
            spec("homeassistant", "exact", "climate.set_temperature"),
            spec("teleport", "exact", "somewhere"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("teleport"), "{err}");
    }

    fn agent_spec(name: &str, workspace: Option<&str>) -> CronJobSpec {
        CronJobSpec {
            catch_up: Default::default(),
            notify: Default::default(),
            name: name.into(),
            trigger: Trigger::cron("0 8 * * *"),
            action: CronAction::Agent {
                prompt: "tidy up".into(),
                skills: vec![],
                workspace: workspace.map(str::to_string),
            },
            grants: vec![],
        }
    }

    fn workspace_of(job: &CronJob) -> Option<String> {
        match &job.action {
            CronAction::Agent { workspace, .. } => workspace.clone(),
            _ => panic!("agent job"),
        }
    }

    /// The stored root is the *resolved* one: the workspace check is lexical,
    /// so a root that is a symlink would never prefix-match the real paths the
    /// tools resolve, and the job would be denied its own directory.
    #[tokio::test]
    async fn an_agent_workspace_is_stored_canonicalized() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("project");
        std::fs::create_dir(&nested).unwrap();
        let scenic = nested.join("..").join("project");

        let jobs = FakeJobs::default();
        let job = add_cron_job(
            &jobs,
            agent_spec("tidy", Some(&scenic.to_string_lossy())),
            1000,
        )
        .await
        .unwrap();

        assert_eq!(
            workspace_of(&job).map(std::path::PathBuf::from),
            Some(nested.canonicalize().unwrap())
        );
    }

    /// A path with a typo in it is refused while the person who typed it is
    /// still here — not at 03:00, where it surfaces as every file call being
    /// denied and reads like a policy problem instead of a spelling one.
    #[tokio::test]
    async fn an_agent_workspace_that_does_not_exist_is_refused_at_creation() {
        let jobs = FakeJobs::default();
        let err = add_cron_job(&jobs, agent_spec("tidy", Some("/no/such/place")), 1000)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("/no/such/place"), "{err}");
        assert!(
            jobs.list().await.unwrap().is_empty(),
            "a refused job must not be stored"
        );
    }

    #[tokio::test]
    async fn an_agent_workspace_pointing_at_a_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("notes.md");
        std::fs::write(&file, b"x").unwrap();

        let jobs = FakeJobs::default();
        let err = add_cron_job(
            &jobs,
            agent_spec("tidy", Some(&file.to_string_lossy())),
            1000,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not a directory"), "{err}");
    }

    #[tokio::test]
    async fn an_agent_job_without_a_workspace_stores_none() {
        let jobs = FakeJobs::default();
        let job = add_cron_job(&jobs, agent_spec("tidy", None), 1000)
            .await
            .unwrap();
        assert!(workspace_of(&job).is_none());

        // Blank is the same as absent, not a root of "".
        let job = add_cron_job(&jobs, agent_spec("tidy2", Some("   ")), 1000)
            .await
            .unwrap();
        assert!(workspace_of(&job).is_none());
    }
}
