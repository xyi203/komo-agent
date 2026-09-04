//! Routines: work the gateway runs unattended when something happens — a cron
//! slot, a named moment, and (5.12–5.14) an external event.
//!
//! A routine is a [`CronJob`]: a [`Trigger`], an action, and the [`RoutineRun`]
//! history of its firings. Jobs live in the **durable** `cron_job_records` —
//! not in `config.toml`, because an operator can accumulate many of them, and
//! not in a disposable table, because a job silently vanishing means its work
//! silently stops happening. A command job is **operator-authored**
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

/// What makes a routine fire (docs/bot-runtime.md §3.3). Replaces the bare
/// schedule string: a routine is "run this when X happens", and a cron slot is
/// only one shape of X.
///
/// Two variants are **schedule-shaped** — `Cron` and `At` name a moment the
/// sweep computes in advance, which is what `next_run_at` holds. The
/// event-shaped ones (`Feishu`, `Webhook`, `FileChanged`) are defined here and
/// fired by 5.12–5.14; until then they have no occurrence, so a job triggered
/// only by them never becomes due and the sweep passes over it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Trigger {
    /// 5-field cron expression, local timezone.
    Cron {
        expr: String,
    },
    /// One local moment (unix seconds) — the `@at` one-shot, resolved to its
    /// instant when the job is created.
    At {
        at: i64,
    },
    Feishu {
        chat: String,
        #[serde(rename = "match")]
        matcher: FeishuMatch,
    },
    Webhook {
        name: String,
    },
    FileChanged {
        root: std::path::PathBuf,
        glob: String,
    },
    /// Any of these fires the routine. Capped at [`MAX_ANY_TRIGGERS`]; one
    /// firing is one [`RoutineRun`], whose `event` names the member that hit.
    Any {
        triggers: Vec<Trigger>,
    },
}

/// How a feishu message is matched to a routine.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FeishuMatch {
    Mention,
    Keyword { keywords: Vec<String> },
    Reaction { emoji: String },
}

impl FeishuMatch {
    pub fn describe(&self) -> String {
        match self {
            Self::Mention => "mention".to_string(),
            Self::Keyword { keywords } => format!("keyword {}", keywords.join("/")),
            Self::Reaction { emoji } => format!("reaction {emoji}"),
        }
    }
}

/// How many listeners one `Any` may hold — the set is re-read on every sweep
/// tick, and a routine nobody can read is not a routine.
pub const MAX_ANY_TRIGGERS: usize = 8;

impl Trigger {
    pub fn cron(expr: &str) -> Self {
        Self::Cron {
            expr: expr.to_string(),
        }
    }

    /// Does this trigger name moments a scheduler can compute? Event-shaped
    /// triggers do not, and a job made only of them is never *due* — it waits.
    pub fn is_scheduled(&self) -> bool {
        match self {
            Self::Cron { .. } | Self::At { .. } => true,
            Self::Any { triggers } => triggers.iter().any(Self::is_scheduled),
            _ => false,
        }
    }

    /// Does it fire again after the slot it is on? A `Cron` does; an `At` is
    /// spent once it has passed.
    pub fn recurs(&self) -> bool {
        match self {
            Self::Cron { .. } => true,
            Self::Any { triggers } => triggers.iter().any(Self::recurs),
            _ => false,
        }
    }

    /// The next moment this trigger fires strictly after `after`, with the
    /// member that owns it.
    ///
    /// `Ok(None)` = nothing left to fire (event-only, or every one-shot spent);
    /// `Err` = an expression that no longer parses, which pauses the job rather
    /// than erroring every tick.
    pub fn next_slot(&self, after: i64) -> anyhow::Result<Option<(i64, &Trigger)>> {
        match self {
            Self::Cron { expr } => Ok(Some((next_occurrence_local(expr, after)?, self))),
            Self::At { at } => Ok((*at > after).then_some((*at, self))),
            Self::Any { triggers } => {
                let mut soonest: Option<(i64, &Trigger)> = None;
                for member in triggers {
                    if let Some(slot) = member.next_slot(after)?
                        && soonest.is_none_or(|(at, _)| slot.0 < at)
                    {
                        soonest = Some(slot);
                    }
                }
                Ok(soonest)
            }
            _ => Ok(None),
        }
    }

    /// Which member is responsible for a fire at `slot` — the one whose own
    /// occurrence *is* that slot. Two members landing on one slot answer the
    /// first: one firing is one run, so the record names one of them.
    pub fn owner_of(&self, slot: i64) -> Option<&Trigger> {
        match self {
            Self::Cron { expr } => next_occurrence_local(expr, slot - 1)
                .is_ok_and(|next| next == slot)
                .then_some(self),
            Self::At { at } => (*at == slot).then_some(self),
            Self::Any { triggers } => triggers.iter().find_map(|m| m.owner_of(slot)),
            _ => None,
        }
    }

    /// One line naming this trigger, for listings and approval prompts.
    pub fn describe(&self) -> String {
        match self {
            Self::Cron { expr } => format!("cron `{expr}`"),
            Self::At { at } => format!("@at {}", local_minute(*at)),
            Self::Feishu { chat, matcher } => format!("feishu {chat} {}", matcher.describe()),
            Self::Webhook { name } => format!("webhook `{name}`"),
            Self::FileChanged { root, glob } => format!("file {}/{glob}", root.display()),
            Self::Any { triggers } => format!(
                "any({})",
                triggers
                    .iter()
                    .map(Self::describe)
                    .collect::<Vec<_>>()
                    .join(" | ")
            ),
        }
    }

    /// The one-line account of *why this run happened*, recorded on the
    /// [`RoutineRun`]. For an `Any` this is what says which member matched —
    /// without it "why did that fire?" has no answer in the record.
    pub fn slot_event(&self, slot: i64) -> String {
        match self.owner_of(slot) {
            Some(at @ Self::At { .. }) => at.describe(),
            Some(owner) => format!("{} @ {}", owner.describe(), local_minute(slot)),
            None => format!("{} @ {}", self.describe(), local_minute(slot)),
        }
    }
}

/// Where a routine's result goes (docs/bot-runtime.md §5.15). `Always` is what
/// komo has always done; `OnError` is "only tell me when something breaks".
///
/// It governs *results* only: a routine that stopped for an approval is
/// delivered under every policy, because that message is not a report — it is
/// the routine asking for something, and nobody is coming otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotifyPolicy {
    #[default]
    Always,
    OnError,
    Never,
}

impl NotifyPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::OnError => "on_error",
            Self::Never => "never",
        }
    }

    pub fn delivers(&self, status: RoutineRunStatus) -> bool {
        matches!(
            (self, status),
            (_, RoutineRunStatus::Waiting)
                | (Self::Always, _)
                | (Self::OnError, RoutineRunStatus::Error)
        )
    }
}

/// Anything unrecognized reads as `Always`: a mangled row must never silence a
/// routine, which is the one failure nobody would notice.
pub fn parse_notify_policy(s: &str) -> NotifyPolicy {
    match s.trim() {
        "on_error" => NotifyPolicy::OnError,
        "never" => NotifyPolicy::Never,
        _ => NotifyPolicy::Always,
    }
}

/// How one firing ended.
///
/// `Waiting` is neither ok nor error: an agent job that hit an action its
/// grants don't cover stopped to ask the operator (docs/bot-runtime.md §5.4).
/// It did not fail — nothing went wrong and the turn is coming back — and it
/// did not succeed, so recording either would make "did last night's routine
/// work?" unanswerable. `Running` is the claim, written before the action
/// starts, so a crash mid-run leaves a record of what was in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutineRunStatus {
    Running,
    Ok,
    Error,
    Waiting,
}

impl RoutineRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Waiting => "waiting",
        }
    }
}

/// `ok` / `waiting` / `running` as written; anything else is an error — the
/// same reading the pre-`Trigger` `last_status` column had.
pub fn parse_routine_run_status(s: &str) -> RoutineRunStatus {
    match s {
        "ok" => RoutineRunStatus::Ok,
        "waiting" => RoutineRunStatus::Waiting,
        "running" => RoutineRunStatus::Running,
        _ => RoutineRunStatus::Error,
    }
}

/// How many firings a routine keeps. Enough to answer "has this been failing
/// all week?", bounded because the whole list rides in one durable column that
/// every sweep tick reads.
pub const ROUTINE_RUN_HISTORY: usize = 20;

/// How much of a run's delivered body that history keeps. The notification
/// carries the full text; the record is for looking back.
pub const ROUTINE_RUN_OUTPUT_CAP: usize = 1000;

/// One firing of a routine.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RoutineRun {
    pub id: String,
    pub status: RoutineRunStatus,
    pub started_at: i64,
    /// What set it off, in one line — the cron slot, the matched trigger, the
    /// message. A routine that does not record this cannot say why it ran.
    pub event: String,
    /// Ledger session of an agent-mode run; `None` for a command job.
    #[serde(default)]
    pub session_id: Option<String>,
    /// The delivered body, capped at [`ROUTINE_RUN_OUTPUT_CAP`].
    #[serde(default)]
    pub output: String,
}

fn cap_output(text: &str) -> String {
    if text.chars().count() <= ROUTINE_RUN_OUTPUT_CAP {
        return text.to_string();
    }
    let head: String = text.chars().take(ROUTINE_RUN_OUTPUT_CAP).collect();
    format!("{head}\n… (truncated)")
}

/// Local `YYYY-MM-DD HH:MM` — the form every cron surface prints a moment in.
pub fn local_minute(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|t| {
            t.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| unix.to_string())
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
    /// What makes it fire. Schedule-shaped triggers drive `next_run_at`;
    /// event-shaped ones wait for their event.
    pub trigger: Trigger,
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
    /// `0` = nothing scheduled (an event-only trigger).
    pub next_run_at: i64,
    /// Where a run's outcome is delivered. `Always` = today's behaviour.
    #[serde(default)]
    pub notify: NotifyPolicy,
    /// Trigger/config problem detail (e.g. an expression that stopped parsing).
    /// Run output — success and failure alike — lives in `runs`.
    pub last_error: String,
    /// The most recent firings, newest last, capped at [`ROUTINE_RUN_HISTORY`].
    /// Empty = never ran.
    #[serde(default)]
    pub runs: Vec<RoutineRun>,
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
    /// A new enabled job with the given trigger and action. The caller (the
    /// shared operator action) computes the initial `next_run_at`.
    pub fn new(name: &str, trigger: Trigger, action: CronAction, next_run_at: i64) -> Self {
        Self {
            id: uuid::Uuid::now_v7().to_string(),
            name: name.to_string(),
            trigger,
            action,
            status: CronJobStatus::Active,
            catch_up: CatchUp::default(),
            next_run_at,
            notify: NotifyPolicy::default(),
            last_error: String::new(),
            runs: Vec::new(),
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
    pub fn new_command(name: &str, trigger: Trigger, command: &str, next_run_at: i64) -> Self {
        Self::new(
            name,
            trigger,
            CronAction::Command {
                command: command.to_string(),
                args: Vec::new(),
                workdir: None,
                timeout_secs: DEFAULT_CRON_JOB_TIMEOUT_SECS,
            },
            next_run_at,
        )
    }

    /// Due = active and a scheduled fire time has arrived. `next_run_at == 0`
    /// is "no moment": an event-only routine waits rather than firing at once.
    pub fn is_due(&self, now: i64) -> bool {
        self.status == CronJobStatus::Active && self.next_run_at > 0 && self.next_run_at <= now
    }

    /// Claim this firing: record it as `running` before the action starts, so a
    /// crash mid-run leaves the record of what was in flight. Answers the run's
    /// id, which [`CronJob::finish_run`] settles.
    pub fn begin_run(&mut self, started_at: i64, event: String) -> String {
        let id = uuid::Uuid::now_v7().to_string();
        self.runs.push(RoutineRun {
            id: id.clone(),
            status: RoutineRunStatus::Running,
            started_at,
            event,
            session_id: None,
            output: String::new(),
        });
        let overflow = self.runs.len().saturating_sub(ROUTINE_RUN_HISTORY);
        self.runs.drain(..overflow);
        id
    }

    /// Settle the run `begin_run` opened.
    pub fn finish_run(
        &mut self,
        id: &str,
        status: RoutineRunStatus,
        output: &str,
        session_id: Option<String>,
    ) {
        if let Some(run) = self.runs.iter_mut().find(|r| r.id == id) {
            run.status = status;
            run.output = cap_output(output);
            run.session_id = session_id;
        }
    }

    /// The most recent firing — what every "how did that job go?" surface reads.
    pub fn last_run(&self) -> Option<&RoutineRun> {
        self.runs.last()
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
        // Bound lateness by the job's *own* period rather than a fixed grace:
        // 30 minutes late is nothing to a weekly job and absurd for one that
        // runs every five. No later slot at all — a one-shot — means running it
        // late is the only way it runs, and an unreadable expression keeps the
        // old behaviour (run it): refusing because the trigger puzzled us would
        // be a worse failure than running late.
        match self.trigger.next_slot(self.next_run_at) {
            Ok(Some((following, _))) if late_by >= (following - self.next_run_at).max(1) => {
                CatchUpVerdict::TooLate { late_by }
            }
            _ => CatchUpVerdict::Late { late_by },
        }
    }

    /// One-shot job: fires once, then completes (`Done`) instead of
    /// rescheduling.
    pub fn is_once(&self) -> bool {
        self.trigger.is_scheduled() && !self.trigger.recurs()
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
    /// What makes it fire. A caller holding a schedule *string* turns it into
    /// one with `cron_actions::parse_schedule` — the single parse site.
    pub trigger: Trigger,
    pub action: CronAction,
    /// Where its results go. Absent = `Always`, which is what every job written
    /// before the policy existed does.
    #[serde(default)]
    pub notify: NotifyPolicy,
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

    fn job(trigger: Trigger, next_run_at: i64, catch_up: CatchUp) -> CronJob {
        let mut j = CronJob::new(
            "j",
            trigger,
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

        let daily = job(Trigger::cron("0 8 * * *"), due, CatchUp::Late);
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
        let every_five = job(Trigger::cron("*/5 * * * *"), due, CatchUp::Late);
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
        let lights = job(Trigger::cron("0 23 * * *"), due, CatchUp::Skip);
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
        let once = job(Trigger::At { at: due }, due, CatchUp::Late);
        assert!(matches!(
            once.catch_up_verdict(due + 30 * 86_400),
            CatchUpVerdict::Late { .. }
        ));
    }
}

#[cfg(test)]
mod trigger_tests {
    use super::*;

    fn at(unix: i64) -> Trigger {
        Trigger::At { at: unix }
    }

    #[test]
    fn a_spent_one_shot_has_no_next_slot_but_a_cron_always_does() {
        let moment = 1_767_225_600;
        assert_eq!(
            at(moment).next_slot(moment - 1).unwrap().map(|(t, _)| t),
            Some(moment)
        );
        assert!(at(moment).next_slot(moment).unwrap().is_none());
        assert!(
            Trigger::cron("0 8 * * *")
                .next_slot(moment)
                .unwrap()
                .is_some()
        );
    }

    /// The soonest member wins, and once the one-shot is spent the recurring
    /// member carries the job on — which is why an `Any` holding both is not a
    /// one-shot.
    #[test]
    fn any_schedules_to_its_soonest_member() {
        let moment = 1_767_225_600;
        let any = Trigger::Any {
            triggers: vec![Trigger::cron("0 8 * * *"), at(moment)],
        };
        let cron_slot = Trigger::cron("0 8 * * *")
            .next_slot(moment - 86_400)
            .unwrap()
            .unwrap()
            .0;
        let (soonest, _) = any.next_slot(moment - 86_400).unwrap().unwrap();
        assert_eq!(soonest, cron_slot.min(moment));
        assert!(any.recurs());
        assert!(any.is_scheduled());
    }

    /// Judgement 5: two members due at the same moment produce one run, and the
    /// event says which of them owns it.
    #[test]
    fn a_slot_two_members_share_is_owned_by_one_of_them() {
        // A slot `0 8 * * *` really lands on, so both members claim it.
        let slot = next_occurrence_local("0 8 * * *", 1_767_225_600).unwrap();
        let any = Trigger::Any {
            triggers: vec![Trigger::cron("0 8 * * *"), at(slot)],
        };
        let owner = any.owner_of(slot).expect("one of them owns the slot");
        assert!(matches!(owner, Trigger::Cron { .. }), "{owner:?}");
        let event = any.slot_event(slot);
        assert!(event.contains("0 8 * * *"), "{event}");
        assert!(
            !event.contains("any("),
            "the event names the member, not the set"
        );

        // The other way round: only the one-shot owns a moment cron never hits.
        let odd = slot + 61;
        let any = Trigger::Any {
            triggers: vec![Trigger::cron("0 8 * * *"), at(odd)],
        };
        assert_eq!(any.owner_of(odd), Some(&at(odd)));
        assert!(
            any.slot_event(odd).starts_with("@at"),
            "{}",
            any.slot_event(odd)
        );
    }

    /// Defined but not fired this round: an event-only trigger has no moment,
    /// so the sweep never finds the job due.
    #[test]
    fn event_triggers_have_no_occurrence() {
        for trigger in [
            Trigger::Webhook { name: "ci".into() },
            Trigger::Feishu {
                chat: "oc_x".into(),
                matcher: FeishuMatch::Mention,
            },
            Trigger::FileChanged {
                root: "/srv/notes".into(),
                glob: "**/*.md".into(),
            },
        ] {
            assert!(!trigger.is_scheduled(), "{trigger:?}");
            assert!(trigger.next_slot(0).unwrap().is_none(), "{trigger:?}");
            let job = CronJob::new_command("j", trigger, "/bin/true", 0);
            assert!(!job.is_due(i64::MAX), "an event-only routine is never due");
        }
    }

    #[test]
    fn a_broken_expression_is_an_error_not_an_absent_slot() {
        assert!(Trigger::cron("not a cron").next_slot(0).is_err());
        assert!(
            Trigger::Any {
                triggers: vec![Trigger::cron("not a cron")]
            }
            .next_slot(0)
            .is_err()
        );
    }

    #[test]
    fn triggers_roundtrip_through_json() {
        let trigger = Trigger::Any {
            triggers: vec![
                Trigger::cron("0 8 * * *"),
                Trigger::At { at: 42 },
                Trigger::Feishu {
                    chat: "oc_x".into(),
                    matcher: FeishuMatch::Keyword {
                        keywords: vec!["值班".into()],
                    },
                },
            ],
        };
        let json = serde_json::to_string(&trigger).unwrap();
        assert!(json.contains("\"kind\":\"any\""), "{json}");
        assert!(json.contains("\"match\""), "{json}");
        assert_eq!(serde_json::from_str::<Trigger>(&json).unwrap(), trigger);
    }
}

#[cfg(test)]
mod run_history_tests {
    use super::*;

    fn job() -> CronJob {
        CronJob::new_command("j", Trigger::cron("* * * * *"), "/bin/true", 0)
    }

    #[test]
    fn a_run_is_claimed_running_and_settled_in_place() {
        let mut job = job();
        let id = job.begin_run(100, "cron `* * * * *` @ x".into());
        assert_eq!(job.last_run().unwrap().status, RoutineRunStatus::Running);
        job.finish_run(&id, RoutineRunStatus::Ok, "done", Some("s1".into()));
        let run = job.last_run().unwrap();
        assert_eq!(run.status, RoutineRunStatus::Ok);
        assert_eq!(run.output, "done");
        assert_eq!(run.session_id.as_deref(), Some("s1"));
        assert_eq!(job.runs.len(), 1, "settling must not append a second run");
    }

    #[test]
    fn history_keeps_the_newest_runs_only() {
        let mut job = job();
        for n in 0..ROUTINE_RUN_HISTORY + 5 {
            let id = job.begin_run(n as i64, format!("slot {n}"));
            job.finish_run(&id, RoutineRunStatus::Ok, "", None);
        }
        assert_eq!(job.runs.len(), ROUTINE_RUN_HISTORY);
        assert_eq!(job.runs[0].event, "slot 5");
        assert_eq!(
            job.last_run().unwrap().event,
            format!("slot {}", ROUTINE_RUN_HISTORY + 4)
        );
    }

    #[test]
    fn a_long_body_is_capped_in_the_record() {
        let mut job = job();
        let id = job.begin_run(0, "slot".into());
        job.finish_run(&id, RoutineRunStatus::Ok, &"x".repeat(5_000), None);
        let output = &job.last_run().unwrap().output;
        assert!(output.chars().count() < 5_000);
        assert!(output.ends_with("(truncated)"), "{output}");
    }

    /// The approval prompt a waiting routine sends is not a result report — it
    /// is the routine asking for something, so silencing results never silences
    /// it.
    #[test]
    fn notify_policies_filter_results_but_never_a_waiting_routine() {
        use RoutineRunStatus::*;
        for status in [Ok, Error] {
            assert!(NotifyPolicy::Always.delivers(status));
            assert!(!NotifyPolicy::Never.delivers(status));
        }
        assert!(NotifyPolicy::OnError.delivers(Error));
        assert!(!NotifyPolicy::OnError.delivers(Ok));
        for policy in [
            NotifyPolicy::Always,
            NotifyPolicy::OnError,
            NotifyPolicy::Never,
        ] {
            assert!(policy.delivers(Waiting), "{policy:?}");
        }
    }

    /// A mangled row must never silence a routine — the one failure nobody
    /// would notice.
    #[test]
    fn an_unreadable_notify_policy_reads_as_always() {
        assert_eq!(parse_notify_policy("garbage"), NotifyPolicy::Always);
        assert_eq!(parse_notify_policy(""), NotifyPolicy::Always);
        for policy in [
            NotifyPolicy::Always,
            NotifyPolicy::OnError,
            NotifyPolicy::Never,
        ] {
            assert_eq!(parse_notify_policy(policy.as_str()), policy);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_command_job_is_active_with_default_timeout() {
        let job = CronJob::new_command(
            "weekly",
            Trigger::cron("0 14 * * 5"),
            "/opt/rotate.py",
            1000,
        );
        assert_eq!(job.status, CronJobStatus::Active);
        assert_eq!(job.action.kind(), "command");
        let CronAction::Command { timeout_secs, .. } = &job.action else {
            panic!("command job");
        };
        assert_eq!(*timeout_secs, DEFAULT_CRON_JOB_TIMEOUT_SECS);
        assert_eq!(job.next_run_at, 1000);
        assert!(job.runs.is_empty());
        assert_eq!(job.notify, NotifyPolicy::Always);
        assert!(!job.id.is_empty());
    }

    #[test]
    fn agent_action_roundtrips_through_json() {
        let action = CronAction::Agent {
            prompt: "summarize my day".into(),
            skills: vec!["calendar".into()],
            workspace: Some("/srv/notes".into()),
        };
        let job = CronJob::new("brief", Trigger::cron("0 8 * * *"), action, 0);
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
        let mut job = CronJob::new_command("j", Trigger::cron("* * * * *"), "/bin/true", 100);
        assert!(job.is_due(100));
        assert!(job.is_due(101));
        assert!(!job.is_due(99));
        job.status = CronJobStatus::Paused;
        assert!(!job.is_due(200), "a paused job is never due");
        job.status = CronJobStatus::Done;
        assert!(!job.is_due(200), "a completed one-shot is never due");
    }

    #[test]
    fn once_is_derived_from_the_trigger_shape() {
        let once = CronJob::new_command("o", Trigger::At { at: 1_900_000_000 }, "/bin/true", 100);
        assert!(once.is_once());
        let recurring = CronJob::new_command("r", Trigger::cron("0 8 * * *"), "/bin/true", 100);
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
        for status in [
            RoutineRunStatus::Running,
            RoutineRunStatus::Ok,
            RoutineRunStatus::Error,
            RoutineRunStatus::Waiting,
        ] {
            assert_eq!(parse_routine_run_status(status.as_str()), status);
        }
        assert_eq!(
            parse_routine_run_status("waiting"),
            RoutineRunStatus::Waiting,
            "a routine parked on an approval must not read back as a failure"
        );
        assert_eq!(parse_routine_run_status("garbage"), RoutineRunStatus::Error);
    }

    #[test]
    fn ids_are_unique_across_rapid_creation() {
        let a = CronJob::new_command("a", Trigger::cron("* * * * *"), "/bin/true", 0);
        let b = CronJob::new_command("b", Trigger::cron("* * * * *"), "/bin/true", 0);
        assert_ne!(a.id, b.id);
    }
}

/// Prefix marking a one-shot schedule: `@at YYYY-MM-DD HH:MM` (local time).
pub const ONCE_PREFIX: &str = "@at ";

/// A one-shot schedule (`@at …`), as opposed to a recurring cron expression.
pub fn schedule_is_once(expr: &str) -> bool {
    expr.trim_start().starts_with(ONCE_PREFIX)
}

/// Resolve `@at YYYY-MM-DD HH:MM` to the local moment it names, **past or
/// future**. `next_occurrence_in` adds the "strictly after" rule on top; the
/// one-time backfill of pre-`Trigger` rows needs the moment itself, since a
/// one-shot that already fired still has to become a `Trigger::At`.
pub fn once_moment_in<Tz: chrono::TimeZone>(
    expr: &str,
    tz: &Tz,
) -> anyhow::Result<chrono::DateTime<Tz>> {
    let at = expr
        .trim()
        .strip_prefix(ONCE_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("not a one-shot schedule: `{expr}`"))?
        .trim();
    let naive = chrono::NaiveDateTime::parse_from_str(at, "%Y-%m-%d %H:%M").map_err(|e| {
        anyhow::anyhow!("invalid one-shot time `{at}` (expected `@at YYYY-MM-DD HH:MM`): {e}")
    })?;
    match tz.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Ok(dt),
        // DST fold: two readings exist; the earlier one fires, same as cron.
        chrono::LocalResult::Ambiguous(dt, _) => Ok(dt),
        chrono::LocalResult::None => {
            anyhow::bail!("one-shot time `{at}` does not exist in this timezone (DST gap)")
        }
    }
}

/// [`once_moment_in`] in the host's timezone, as a Unix timestamp.
pub fn once_moment_local(expr: &str) -> anyhow::Result<i64> {
    Ok(once_moment_in(expr, &chrono::Local)?.timestamp())
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
    if schedule_is_once(expr) {
        let moment = once_moment_in(expr, &after.timezone())?;
        if moment <= after {
            anyhow::bail!(
                "one-shot time `{}` is already past",
                expr.trim().trim_start_matches(ONCE_PREFIX).trim()
            );
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
