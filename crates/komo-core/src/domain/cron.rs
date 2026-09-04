//! Scheduled cron jobs: deterministic commands the gateway executes unattended
//! on a cron schedule (hermes' `no_agent` cron jobs analog).
//!
//! Jobs live in their own durable store (`~/.komo/cron.db`) — not in
//! `config.toml`, because an operator can accumulate many of them, and not in
//! the disposable `state.db`, because a job silently vanishing on a state reset
//! means its work silently stops happening. A command job is **operator-authored**
//! (added via `komo cron add` or the loopback-gated api) — the same trust
//! boundary as running `komo gateway` itself — so execution is direct: no shell
//! tool, no approver, no `[policy]` involvement at fire time.
//!
//! A job created in conversation through the agent's `cron` tool is
//! *model*-authored, so that path moves the human decision to creation time: the
//! tool gates every mutation through the `Approver` (a command job prominently,
//! since approving it approves every future execution). By the time a job is in
//! the store the sweep treats both origins identically.

use async_trait::async_trait;
use croner::Cron;

use crate::domain::policy::{Rule, RuleSpec};

/// Default wall-clock budget for a job command — hermes' cron-job budget
/// (15 min), generous enough for a script that clones a repo and pushes an MR.
pub const DEFAULT_CRON_JOB_TIMEOUT_SECS: u64 = 900;

/// A job's lifecycle state, stored — the single authority on whether it fires.
///
/// - `Active` — fires when `next_run_at` arrives.
/// - `Paused` — the operator's stop switch (also where the sweep parks a job
///   whose schedule no longer parses, with the reason in `last_error`).
/// - `Done` — a one-shot job that has fired. Terminal: the row stays as the
///   queryable record of what ran and what it produced; re-running means
///   creating a new job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CronJobStatus {
    Active,
    Paused,
    Done,
}

/// What a job wants done with a slot that was missed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatchUp {
    /// Run it late, once, if it is not *too* late — the default, and what komo
    /// has always done (minus the bound).
    #[default]
    Late,
    /// Never run late. For work that is only correct at its hour: turning the
    /// lights off at 23:00 is not a thing to do at 09:00 the next morning.
    Skip,
}

impl CatchUp {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Late => "late",
            Self::Skip => "skip",
        }
    }
}

pub fn parse_catch_up(s: &str) -> CatchUp {
    match s.trim() {
        "skip" => CatchUp::Skip,
        // Anything else — including rows written before the column — is the
        // long-standing behaviour.
        _ => CatchUp::Late,
    }
}

/// The answer to "this job is due; should it run?"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpVerdict {
    /// Due now, on schedule.
    OnTime,
    /// A missed slot worth running anyway.
    Late { late_by: i64 },
    /// A missed slot to abandon: skip to the next one.
    TooLate { late_by: i64 },
}

impl CronJobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Done => "done",
        }
    }
}

/// Anything unrecognized reads as `Active`: the failure mode of a mangled row
/// is a job that fires on schedule, not one that silently stops.
pub fn parse_cron_job_status(s: &str) -> CronJobStatus {
    match s {
        "paused" => CronJobStatus::Paused,
        "done" => CronJobStatus::Done,
        _ => CronJobStatus::Active,
    }
}

/// Outcome of a job's most recent execution.
///
/// `Waiting` is neither: an agent job that hit an action its grants don't cover
/// stopped to ask the operator (docs/bot-runtime.md §5.4). It did not fail —
/// nothing went wrong and the turn is coming back — and it did not succeed, so
/// recording either would make "did last night's routine work?" unanswerable.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CronRunStatus {
    Ok,
    Failed,
    Waiting,
}

impl CronRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::Waiting => "waiting",
        }
    }
}

/// `""` (never ran) → `None`; anything not `ok` / `waiting` parses as failed.
pub fn parse_cron_run_status(s: &str) -> Option<CronRunStatus> {
    match s {
        "" => None,
        "ok" => Some(CronRunStatus::Ok),
        "waiting" => Some(CronRunStatus::Waiting),
        _ => Some(CronRunStatus::Failed),
    }
}

/// What a job does when it fires. Internally tagged (`kind`) so the HTTP path
/// and the db both round-trip it without a separate discriminator column having
/// to be threaded by hand.
///
/// - `Command` — run a fixed program and deliver its stdout verbatim (hermes'
///   `no_agent` mode). Deterministic, no LLM. The reliable default for scripts.
/// - `Agent` — run a prompt through an **unattended, tool-capable agent turn**
///   and deliver the reply. Optional `skills` are loaded first. The agent runs
///   with the full tool set but side effects are gated by the permission
///   policy: with no human to prompt, a `Risk::Normal` action passes only
///   through this job's own [`CronJob::grants`] (approved when it was created)
///   or an `unattended = true` `[policy]` rule.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CronAction {
    Command {
        /// Program to execute (an absolute path; run directly, not via a shell).
        command: String,
        #[serde(default)]
        args: Vec<String>,
        /// Working directory. `None` = the gateway's cwd.
        #[serde(default)]
        workdir: Option<String>,
        /// Wall-clock budget in seconds; the process is killed past it.
        timeout_secs: u64,
    },
    Agent {
        /// The instruction the agent turn runs.
        prompt: String,
        /// Skills to load before running the prompt (progressive disclosure —
        /// the turn is told to `skill` view each one first).
        #[serde(default)]
        skills: Vec<String>,
        /// Directory the turn's file and shell tools are confined to
        /// (canonical absolute path, validated when the job is created).
        /// `None` = the gateway's own workspace.
        ///
        /// Deliberately not the same thing as [`CronAction::Command`]'s
        /// `workdir`, which only sets a child process's cwd: this is a
        /// *confinement boundary*, so it is resolved to its real path — the
        /// workspace check is lexical, and a symlinked root would fail to match
        /// the paths the tools actually resolve.
        #[serde(default)]
        workspace: Option<String>,
    },
}

impl CronAction {
    /// Short label for listings/logs.
    pub fn kind(&self) -> &'static str {
        match self {
            CronAction::Command { .. } => "command",
            CronAction::Agent { .. } => "agent",
        }
    }
}

/// One scheduled job. `name` is the operator-facing key (unique); `id` is the
/// storage key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CronJob {
    pub id: String,
    pub name: String,
    /// 5-field cron expression (local timezone), or `@at YYYY-MM-DD HH:MM`
    /// (local) for a one-shot job that fires once and completes.
    pub schedule: String,
    /// What the job does when it fires (command vs agent turn).
    pub action: CronAction,
    /// Lifecycle state — only `Active` jobs fire. `Paused`/`Done` rows stay
    /// listed and inspectable.
    pub status: CronJobStatus,
    /// What to do with a slot the gateway slept through. See
    /// [`CronJob::catch_up_verdict`].
    #[serde(default)]
    pub catch_up: CatchUp,
    /// Next scheduled fire (unix seconds). The sweep runs a job once its
    /// `next_run_at` is due, then advances it — set to "now" to trigger an
    /// off-schedule run on the next sweep tick. For a `Done` one-shot this
    /// keeps the slot that fired.
    pub next_run_at: i64,
    pub last_run_at: Option<i64>,
    pub last_status: Option<CronRunStatus>,
    /// Schedule/config problem detail (e.g. an expression that stopped
    /// parsing). Run output — success and failure alike — lives in
    /// `last_output`.
    pub last_error: String,
    /// What the most recent run produced (delivered body, capped), success and
    /// failure alike. Empty = never ran.
    pub last_output: String,
    /// Session id of the most recent agent-mode run (`cron:<name>:<unix>`),
    /// for `komo run inspect`. `None` for command jobs / never ran.
    pub last_run_session: Option<String>,
    pub created_at: i64,
    /// Actions this job may take unattended, approved by a human when the job
    /// was created. Empty = no side-effecting action is granted, which is what
    /// every job created before grants existed carries.
    ///
    /// Scoped to *this job's* turns, so deleting the job revokes them — unlike a
    /// global `unattended = true` `[[policy.rule]]`, which outlives whatever it
    /// was written for. Only ever an allow list: a denial belongs in config,
    /// where it applies to everything.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<RuleSpec>,
}

impl CronJob {
    /// A new enabled job with the given action. The caller (the shared operator
    /// action) validates the schedule and computes the initial `next_run_at` —
    /// this stays parse-free so komo-core needs no cron dependency.
    pub fn new(name: &str, schedule: &str, action: CronAction, next_run_at: i64) -> Self {
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            name: name.to_string(),
            schedule: schedule.to_string(),
            action,
            status: CronJobStatus::Active,
            catch_up: CatchUp::default(),
            next_run_at,
            last_run_at: None,
            last_status: None,
            last_error: String::new(),
            last_output: String::new(),
            last_run_session: None,
            created_at: time::OffsetDateTime::now_utc().unix_timestamp(),
            grants: Vec::new(),
        }
    }

    /// Attach the grants a human approved when this job was created.
    pub fn with_grants(mut self, grants: Vec<RuleSpec>) -> Self {
        self.grants = grants;
        self
    }

    /// Convenience constructor for a command-mode job with default timeout.
    pub fn new_command(name: &str, schedule: &str, command: &str, next_run_at: i64) -> Self {
        Self::new(
            name,
            schedule,
            CronAction::Command {
                command: command.to_string(),
                args: Vec::new(),
                workdir: None,
                timeout_secs: DEFAULT_CRON_JOB_TIMEOUT_SECS,
            },
            next_run_at,
        )
    }

    /// Due = active and the scheduled fire time has arrived.
    pub fn is_due(&self, now: i64) -> bool {
        self.status == CronJobStatus::Active && self.next_run_at <= now
    }

    /// What to do with a slot the gateway slept through.
    ///
    /// A due job is not necessarily a job worth running *now*. The host is a
    /// laptop: closing the lid over a weekend leaves Friday's 07:00 job due,
    /// and firing it on Monday afternoon is not "catching up", it is doing the
    /// wrong thing at the wrong time. `is_due` alone cannot tell those apart —
    /// it has no upper bound on lateness at all.
    pub fn catch_up_verdict(&self, now: i64) -> CatchUpVerdict {
        let late_by = now.saturating_sub(self.next_run_at);
        if late_by <= 0 {
            return CatchUpVerdict::OnTime;
        }
        if self.catch_up == CatchUp::Skip {
            return CatchUpVerdict::TooLate { late_by };
        }
        // A one-shot has no interval to measure against, and no later slot to
        // wait for: running it late is the only way it runs at all.
        if self.is_once() {
            return CatchUpVerdict::Late { late_by };
        }
        // Bound lateness by the job's *own* period rather than a fixed grace:
        // 30 minutes late is nothing to a weekly job and absurd for one that
        // runs every five. An unreadable schedule keeps the old behaviour
        // (run it) — refusing to run because the expression puzzled us would be
        // a worse failure than running late.
        match next_occurrence_local(&self.schedule, self.next_run_at) {
            Ok(following) if late_by >= (following - self.next_run_at).max(1) => {
                CatchUpVerdict::TooLate { late_by }
            }
            _ => CatchUpVerdict::Late { late_by },
        }
    }

    /// One-shot job: fires once, then completes (`Done`) instead of
    /// rescheduling. Derived from the schedule's shape, which is the one
    /// authority on when it fires.
    pub fn is_once(&self) -> bool {
        schedule_is_once(&self.schedule)
    }

    /// This job's grants as policy rules.
    ///
    /// An entry that no longer parses is **dropped with a warning** rather than
    /// failing the run: grants are validated where a job is created, so the only
    /// way to get one here is a hand-edited db or a downgrade, and in both cases
    /// the safe reading of "I don't understand this permission" is to withhold
    /// it — never to fail the job in a way that looks like the schedule broke.
    pub fn granted_rules(&self) -> Vec<Rule> {
        self.grants
            .iter()
            .filter_map(|spec| match spec.to_rule() {
                Some(rule) => Some(rule),
                None => {
                    tracing::warn!(
                        job = %self.name,
                        category = %spec.category,
                        value = %spec.value,
                        "unparseable job grant ignored"
                    );
                    None
                }
            })
            .collect()
    }
}

/// Longest job name accepted. Names appear in notification titles, `komo cron`
/// listings and the per-run session id, so an essay is never one.
pub const MAX_CRON_JOB_NAME_LEN: usize = 64;

/// Shape floor for a job name. A name is a key: it identifies the job in every
/// `komo cron` subcommand and becomes part of an agent job's session id
/// (`cron:<name>:<unix>`), so whitespace and the separators that structure those
/// strings are refused. Everything else — including CJK — is allowed, because a
/// name is for the operator to read. Enforced in the shared create action, so
/// the CLI, the api channel and the agent's `cron` tool agree.
pub fn valid_cron_job_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().count() <= MAX_CRON_JOB_NAME_LEN
        && !name
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, ':' | '/' | '\\'))
}

/// The operator's request to create a job (`komo cron add` / `POST
/// /api/cron/add`). Validation and `next_run_at` computation happen in the
/// shared operator action, not here.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CronJobSpec {
    pub name: String,
    pub schedule: String,
    pub action: CronAction,
    /// Actions this job should be allowed to take unattended. Normalized and
    /// validated by the shared create action — see `cron_actions`.
    #[serde(default)]
    pub grants: Vec<RuleSpec>,
    #[serde(default)]
    pub catch_up: CatchUp,
}

#[async_trait]
pub trait CronJobRepository: Send + Sync {
    async fn save(&self, job: &CronJob) -> anyhow::Result<()>;
    /// Every job, enabled or not, ordered by name.
    async fn list(&self) -> anyhow::Result<Vec<CronJob>>;
    async fn find_by_name(&self, name: &str) -> anyhow::Result<Option<CronJob>>;
    /// Update every mutable field of an existing job (matched by `id`).
    async fn update(&self, job: &CronJob) -> anyhow::Result<()>;
    /// Remove a job by name; `false` = no such job.
    async fn delete(&self, name: &str) -> anyhow::Result<bool>;
}

#[cfg(test)]
mod catch_up_tests {
    use super::*;

    fn job(schedule: &str, next_run_at: i64, catch_up: CatchUp) -> CronJob {
        let mut j = CronJob::new(
            "j",
            schedule,
            CronAction::Command {
                command: "/bin/true".into(),
                args: Vec::new(),
                workdir: None,
                timeout_secs: 1,
            },
            next_run_at,
        );
        j.catch_up = catch_up;
        j
    }

    /// The bound is the job's own period, not a fixed grace: half an hour late
    /// is nothing to a daily job and absurd for one that runs every five
    /// minutes. A fixed window would have to be wrong for one of them.
    #[test]
    fn lateness_is_bounded_by_the_jobs_own_interval() {
        // 2026-01-01 08:00 local-ish; the exact epoch does not matter, only the
        // distances from it.
        let due = 1_767_225_600;
        let hour = 3_600;

        let daily = job("0 8 * * *", due, CatchUp::Late);
        assert_eq!(daily.catch_up_verdict(due), CatchUpVerdict::OnTime);
        assert!(matches!(
            daily.catch_up_verdict(due + 3 * hour),
            CatchUpVerdict::Late { .. }
        ));
        // Slept through more than a whole day: the next slot is closer than the
        // one that was missed, so run that instead.
        assert!(matches!(
            daily.catch_up_verdict(due + 30 * hour),
            CatchUpVerdict::TooLate { .. }
        ));

        // Same 30 minutes, opposite answer, because the period differs.
        let every_five = job("*/5 * * * *", due, CatchUp::Late);
        assert!(matches!(
            every_five.catch_up_verdict(due + 1800),
            CatchUpVerdict::TooLate { .. }
        ));
    }

    /// Some work is only correct at its hour — turning the lights off at 23:00
    /// is not something to do at 09:00 the next morning, however "recent" the
    /// miss looks against a daily period.
    #[test]
    fn skip_never_runs_late_however_small_the_miss() {
        let due = 1_767_225_600;
        let lights = job("0 23 * * *", due, CatchUp::Skip);
        assert_eq!(lights.catch_up_verdict(due), CatchUpVerdict::OnTime);
        assert!(matches!(
            lights.catch_up_verdict(due + 60),
            CatchUpVerdict::TooLate { .. }
        ));
    }

    /// A one-shot has no later slot to wait for: running it late is the only
    /// way it runs at all.
    #[test]
    fn a_one_shot_runs_however_late_it_is() {
        let due = 1_767_225_600;
        let once = job("@at 2026-01-01 08:00", due, CatchUp::Late);
        assert!(matches!(
            once.catch_up_verdict(due + 30 * 86_400),
            CatchUpVerdict::Late { .. }
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_command_job_is_active_with_default_timeout() {
        let job = CronJob::new_command("weekly", "0 14 * * 5", "/opt/rotate.py", 1000);
        assert_eq!(job.status, CronJobStatus::Active);
        assert_eq!(job.action.kind(), "command");
        let CronAction::Command { timeout_secs, .. } = &job.action else {
            panic!("command job");
        };
        assert_eq!(*timeout_secs, DEFAULT_CRON_JOB_TIMEOUT_SECS);
        assert_eq!(job.next_run_at, 1000);
        assert!(job.last_status.is_none());
        assert!(!job.id.is_empty());
    }

    #[test]
    fn agent_action_roundtrips_through_json() {
        let action = CronAction::Agent {
            prompt: "summarize my day".into(),
            skills: vec!["calendar".into()],
            workspace: Some("/srv/notes".into()),
        };
        let job = CronJob::new("brief", "0 8 * * *", action, 0);
        let json = serde_json::to_string(&job).unwrap();
        assert!(json.contains("\"kind\":\"agent\""));
        let back: CronJob = serde_json::from_str(&json).unwrap();
        assert_eq!(back.action.kind(), "agent");
        let CronAction::Agent {
            prompt,
            skills,
            workspace,
        } = &back.action
        else {
            panic!("agent job");
        };
        assert_eq!(prompt, "summarize my day");
        assert_eq!(skills, &vec!["calendar".to_string()]);
        assert_eq!(workspace.as_deref(), Some("/srv/notes"));
    }

    /// A job stored before agent jobs could name a workspace must still
    /// deserialize — as one that names none.
    #[test]
    fn an_agent_job_written_without_a_workspace_reads_as_having_none() {
        let stored = r#"{"kind":"agent","prompt":"p","skills":[]}"#;
        let action: CronAction = serde_json::from_str(stored).unwrap();
        let CronAction::Agent { workspace, .. } = &action else {
            panic!("agent job");
        };
        assert!(workspace.is_none());
    }

    #[test]
    fn due_requires_active_and_elapsed() {
        let mut job = CronJob::new_command("j", "* * * * *", "/bin/true", 100);
        assert!(job.is_due(100));
        assert!(job.is_due(101));
        assert!(!job.is_due(99));
        job.status = CronJobStatus::Paused;
        assert!(!job.is_due(200), "a paused job is never due");
        job.status = CronJobStatus::Done;
        assert!(!job.is_due(200), "a completed one-shot is never due");
    }

    #[test]
    fn once_is_derived_from_the_schedule_shape() {
        let once = CronJob::new_command("o", "@at 2030-01-02 08:30", "/bin/true", 100);
        assert!(once.is_once());
        let recurring = CronJob::new_command("r", "0 8 * * *", "/bin/true", 100);
        assert!(!recurring.is_once());
    }

    #[test]
    fn job_status_roundtrip() {
        for status in [
            CronJobStatus::Active,
            CronJobStatus::Paused,
            CronJobStatus::Done,
        ] {
            assert_eq!(parse_cron_job_status(status.as_str()), status);
        }
        // A mangled row fires on schedule rather than silently stopping.
        assert_eq!(parse_cron_job_status("garbage"), CronJobStatus::Active);
        assert_eq!(parse_cron_job_status(""), CronJobStatus::Active);
    }

    #[test]
    fn job_names_must_stay_key_shaped() {
        assert!(valid_cron_job_name("morning-brief"));
        assert!(valid_cron_job_name("weekly_alarm.rotation"));
        // A name is for the operator to read, so CJK is fine.
        assert!(valid_cron_job_name("每日简报"));
        assert!(!valid_cron_job_name(""));
        assert!(
            !valid_cron_job_name("morning brief"),
            "whitespace splits it"
        );
        assert!(
            !valid_cron_job_name("cron:brief"),
            "`:` structures the session id"
        );
        assert!(!valid_cron_job_name("a/b"));
        assert!(!valid_cron_job_name("a\\b"));
        assert!(!valid_cron_job_name("a\nb"));
        assert!(!valid_cron_job_name(&"x".repeat(MAX_CRON_JOB_NAME_LEN + 1)));
        assert!(valid_cron_job_name(&"x".repeat(MAX_CRON_JOB_NAME_LEN)));
    }

    #[test]
    fn run_status_roundtrip() {
        assert_eq!(parse_cron_run_status(""), None);
        assert_eq!(parse_cron_run_status("ok"), Some(CronRunStatus::Ok));
        assert_eq!(parse_cron_run_status("failed"), Some(CronRunStatus::Failed));
        assert_eq!(
            parse_cron_run_status("waiting"),
            Some(CronRunStatus::Waiting),
            "a routine parked on an approval must not read back as a failure"
        );
        assert_eq!(
            parse_cron_run_status("garbage"),
            Some(CronRunStatus::Failed)
        );
    }

    #[test]
    fn ids_are_unique_across_rapid_creation() {
        let a = CronJob::new_command("a", "* * * * *", "/bin/true", 0);
        let b = CronJob::new_command("b", "* * * * *", "/bin/true", 0);
        assert_ne!(a.id, b.id);
    }
}

/// Prefix marking a one-shot schedule: `@at YYYY-MM-DD HH:MM` (local time).
pub const ONCE_PREFIX: &str = "@at ";

/// A one-shot schedule (`@at …`), as opposed to a recurring cron expression.
pub fn schedule_is_once(expr: &str) -> bool {
    expr.trim_start().starts_with(ONCE_PREFIX)
}

/// Compute the next occurrence of a schedule strictly after `after`: the next
/// cron slot, or — for a one-shot `@at YYYY-MM-DD HH:MM` — the named moment
/// itself, which is an **error once it has passed** (that is what makes `add`
/// reject a past time and `enable` refuse to resurrect an elapsed one-shot).
/// Timezone-generic so tests can use `FixedOffset` for determinism while
/// production uses `Local`.
pub fn next_occurrence_in<Tz>(
    expr: &str,
    after: chrono::DateTime<Tz>,
) -> anyhow::Result<chrono::DateTime<Tz>>
where
    Tz: chrono::TimeZone + Clone,
{
    if let Some(at) = expr.trim().strip_prefix(ONCE_PREFIX) {
        let at = at.trim();
        let naive = chrono::NaiveDateTime::parse_from_str(at, "%Y-%m-%d %H:%M").map_err(|e| {
            anyhow::anyhow!("invalid one-shot time `{at}` (expected `@at YYYY-MM-DD HH:MM`): {e}")
        })?;
        let moment = match after.timezone().from_local_datetime(&naive) {
            chrono::LocalResult::Single(dt) => dt,
            // DST fold: two readings exist; the earlier one fires, same as cron.
            chrono::LocalResult::Ambiguous(dt, _) => dt,
            chrono::LocalResult::None => {
                anyhow::bail!("one-shot time `{at}` does not exist in this timezone (DST gap)")
            }
        };
        if moment <= after {
            anyhow::bail!("one-shot time `{at}` is already past");
        }
        return Ok(moment);
    }
    let cron = expr
        .parse::<Cron>()
        .map_err(|e| anyhow::anyhow!("invalid cron expression `{expr}`: {e}"))?;
    Ok(cron.find_next_occurrence(&after, false)?)
}

/// Production wrapper: compute the next local-time occurrence after `after_unix`
/// and return it as a Unix timestamp. Computes from the given time (usually
/// `now`) so a resting daemon always jumps to the next future slot.
pub fn next_occurrence_local(expr: &str, after_unix: i64) -> anyhow::Result<i64> {
    let after_utc = chrono::DateTime::from_timestamp(after_unix, 0)
        .ok_or_else(|| anyhow::anyhow!("invalid unix timestamp: {after_unix}"))?;
    let after_local = after_utc.with_timezone(&chrono::Local);
    let next = next_occurrence_in(expr, after_local)?;
    Ok(next.timestamp())
}

#[cfg(test)]
mod schedule_tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Timelike};

    #[test]
    fn next_occurrence_in_rejects_invalid_expr() {
        let result = next_occurrence_in("not a cron", chrono::Utc::now());
        assert!(result.is_err());
    }

    #[test]
    fn next_occurrence_in_computes_strictly_future_fire() {
        let tz = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
        let expr = "0 9 * * *"; // 9 AM daily

        // 8 AM local → next occurrence is 9 AM the same day
        let at_8am = tz.with_ymd_and_hms(2024, 1, 1, 8, 0, 0).unwrap();
        let next = next_occurrence_in(expr, at_8am).unwrap();
        assert_eq!(next.hour(), 9);
        assert_eq!(next.day(), 1);

        // exactly 9 AM local → next is 9 AM the following day (strictly future)
        let at_9am = tz.with_ymd_and_hms(2024, 1, 1, 9, 0, 0).unwrap();
        let next = next_occurrence_in(expr, at_9am).unwrap();
        assert_eq!(next.hour(), 9);
        assert_eq!(next.day(), 2);
    }

    #[test]
    fn at_schedule_fires_at_the_named_local_moment() {
        let tz = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
        let now = tz.with_ymd_and_hms(2024, 1, 1, 8, 0, 0).unwrap();
        let next = next_occurrence_in("@at 2024-01-02 09:30", now).unwrap();
        assert_eq!((next.day(), next.hour(), next.minute()), (2, 9, 30));
        assert_eq!(next.offset().local_minus_utc(), 8 * 3600, "local, not UTC");
    }

    #[test]
    fn at_schedule_rejects_past_and_present_moments() {
        let tz = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
        let now = tz.with_ymd_and_hms(2024, 1, 2, 9, 30, 0).unwrap();
        // Exactly now counts as past — "strictly after", same as cron.
        let err = next_occurrence_in("@at 2024-01-02 09:30", now).unwrap_err();
        assert!(err.to_string().contains("already past"), "{err}");
        assert!(next_occurrence_in("@at 2023-12-31 09:30", now).is_err());
    }

    #[test]
    fn at_schedule_rejects_malformed_times() {
        let now = chrono::Utc::now();
        for bad in ["@at tomorrow", "@at 2024-1-2", "@at 2024-01-02", "@at "] {
            assert!(
                next_occurrence_in(bad, now).is_err(),
                "{bad} must not parse"
            );
        }
    }
}
