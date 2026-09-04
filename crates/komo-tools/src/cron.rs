//! The `cron` tool: scheduled jobs, managed from inside a conversation.
//!
//! `komo cron …` is the operator's surface for the same store; this is the
//! agent's. Both go through the shared operator actions
//! (`services::operator_control::actions`), so validation, name uniqueness and
//! the initial `next_run_at` can't fork between "the user typed a command" and
//! "the user asked in chat".
//!
//! Trust boundary. A CLI-authored job is operator-authored by construction —
//! whoever ran `komo cron add` already had shell on the host. A chat-authored
//! job is *model*-authored, so every mutation is gated through the `Approver`:
//!
//! - **agent mode** (a prompt) is `Risk::Normal`. The turn it schedules runs
//!   unattended, so its side effects pass only through this job's own `grants`
//!   or an `unattended = true` `[policy]` rule. The grants are approved in the
//!   **same** prompt as the job — one interaction covering both "what it does"
//!   and "what it may do" — which is why an `add` carrying grants drops the
//!   `cron:add` scope key: a scope key means "approve this kind of thing once
//!   per session", and every job's permission list is different.
//! - **command mode** (a program) is `Risk::Dangerous` and carries an
//!   `ActionRef::Shell`, so a `[policy]` deny rule fences it and no ordinary
//!   shell *allow* rule can silently grant it (`include_dangerous` is required).
//!   It runs directly, unattended, with no approver at fire time — the operator
//!   is approving every future execution at once, so the prompt says so.
//! - remove/enable/disable/run are `Risk::Normal`, scope `cron:manage`.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    approval::{ActionRef, ApprovalRequest, Decision},
    context::ToolContext,
    cron::{
        CronAction, CronJob, CronJobRepository, CronJobSpec, CronJobStatus,
        DEFAULT_CRON_JOB_TIMEOUT_SECS, NotifyPolicy, RoutineRun, parse_notify_policy,
    },
    policy::RuleSpec,
    tool::{Tool, ToolError, ToolOutput, parse_args},
};
use komo_services::cron_actions as actions;

/// How much of an agent job's prompt a listing shows.
const PROMPT_PREVIEW: usize = 100;

#[derive(Deserialize)]
struct CronArgs {
    action: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    schedule: Option<String>,
    /// Where each run's outcome goes: `always` (default), `on_error`, `never`.
    #[serde(default)]
    notify: Option<String>,
    // Agent-mode fields.
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    skills: Vec<String>,
    /// Agent-mode: the directory the job's file and shell tools are confined
    /// to. Validated (must exist) when the job is created.
    #[serde(default)]
    workspace: Option<String>,
    /// Agent-mode: the actions this job needs to be allowed to take when it
    /// runs with nobody watching. Approved as one list, together with the job.
    #[serde(default)]
    grants: Vec<GrantArg>,
    // Command-mode fields.
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    workdir: Option<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

/// One requested grant, as the model may state it: *what* to allow, and nothing
/// about the rule's shape. `effect`, `unattended`, `include_dangerous` and the
/// channel scope are fixed by `cron_actions::normalize_grants` — the model has
/// no say in how wide a grant's shape is, only in which action it names.
#[derive(Deserialize)]
struct GrantArg {
    category: String,
    #[serde(default, rename = "match")]
    matcher: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    access: Option<String>,
}

impl From<GrantArg> for RuleSpec {
    fn from(arg: GrantArg) -> Self {
        RuleSpec {
            category: arg.category,
            matcher: arg.matcher,
            value: arg.value,
            access: arg.access,
            channels: None,
            effect: String::new(),
            include_dangerous: false,
            unattended: false,
        }
    }
}

/// Lets the model create and manage the gateway's scheduled jobs (`cron.db`) —
/// the recurring-work counterpart to `reminder`, which only re-delivers a
/// message.
pub struct CronTool {
    jobs: Arc<dyn CronJobRepository>,
}

impl CronTool {
    pub fn new(jobs: Arc<dyn CronJobRepository>) -> Self {
        Self { jobs }
    }
}

/// Gate one management mutation (everything but `add`, which describes its own
/// action). One scope key for the family: approving "manage my jobs" once per
/// session shouldn't re-prompt per job. Returns the refusal text (carrying the
/// user's reason, if they gave one) when denied.
async fn approve_manage(ctx: &ToolContext, summary: String, kept: &str) -> Option<String> {
    let request = ApprovalRequest::normal(summary).with_scope_key("cron:manage".to_string());
    // Read as yes/no plus a reason rather than matched variant by variant: a
    // tool has no business knowing the shapes an answer can take (`AllowAlways`
    // is the same yes, and only `PolicyApprover` treats it differently).
    let decision = ctx.decide(&request).await;
    if decision.is_allowed() {
        return None;
    }
    Some(match decision.feedback() {
        Some(reason) => format!("Rejected by the user ({reason}); {kept}"),
        None => format!("Rejected by user; {kept}"),
    })
}

#[async_trait]
impl Tool for CronTool {
    fn name(&self) -> &'static str {
        "cron"
    }

    fn description(&self) -> &'static str {
        "Manage the gateway's scheduled jobs — scheduled *work*, unlike \
         `reminder`, which only re-delivers a message. \
         action=\"list\" returns every job with its trigger, status, next run \
         and last outcome; \
         action=\"add\" creates one (requires `name` + `schedule` — a 5-field \
         cron expression for recurring work, `@at YYYY-MM-DD HH:MM` for a \
         one-shot, both in the user's local timezone, or an event: \
         `@webhook <name>` / `@feishu <chat> keyword …` / `@file <dir> <glob>` \
         — plus either `prompt` for \
         an agent job — an unattended agent turn with your full tool set, \
         optionally preloading `skills` — or `command` \
         (+ `args`/`workdir`/`timeout_secs`) for a fixed program); \
         an agent job that must *do* something — control a device, write a file, \
         run a command — also needs `grants` naming those actions, or every one \
         of them is refused when it runs; \
         action=\"disable\" / \"enable\" pauses and resumes a job by `name`; \
         action=\"remove\" deletes it; action=\"run\" fires it once now. \
         `notify` decides where each run's outcome goes — \"on_error\" for \
         \"only tell me when it breaks\". \
         A one-shot job completes after firing (status `done`) and stays listed \
         with its output — do not remove it to \"clean up\", the row is the \
         record of what ran. \
         Jobs fire only while `komo gateway` runs, and each run's output is \
         delivered to the user's home channel, not into this conversation. \
         Creating or changing a job asks the user for approval. Use this for \
         \"every morning summarize X\" / \"明早 8 点跑一次这个\"; use `reminder` \
         for a plain nudge and `task` for one-off work with no clock."
    }

    /// These calls can park on an approval prompt, so they must outlast one.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(komo_core::domain::tool::APPROVAL_BOUND)
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "add", "remove", "enable", "disable", "run"],
                    "description": "The job operation."
                },
                "name": {
                    "type": "string",
                    "description": "Job name — the unique key every action but `list` takes. Short and descriptive (e.g. \"morning-brief\"); no whitespace or `:` `/` `\\`."
                },
                "schedule": {
                    "type": "string",
                    "description": "What makes the job fire (action=add). Clock triggers are in the user's local timezone. Recurring: a 5-field cron expression, e.g. \"0 8 * * *\" for 8 AM daily or \"0 14 * * 5\" for Friday 2 PM. One-shot: \"@at YYYY-MM-DD HH:MM\", e.g. \"@at 2026-08-12 08:30\" — fires once, then the job completes (a past time is rejected). Event triggers fire when something happens instead: \"@webhook <name>\" (an external system POSTs to /api/hooks/<name>), \"@feishu <chat_id> mention\" / \"@feishu <chat_id> keyword 值班,oncall\" / \"@feishu <chat_id> reaction THUMBSUP\" (something said or reacted to in that feishu chat — the routine runs on its own grants whoever set it off), \"@file <directory> <glob>\" e.g. \"@file /srv/notes **/*.md\" (the directory must already exist; a burst of writes fires it once). Combine with \" | \" for \"any of these\", e.g. \"0 8 * * * | @webhook ci-done\"."
                },
                "notify": {
                    "type": "string",
                    "enum": ["always", "on_error", "never"],
                    "description": "Where each run's outcome goes (action=add; default \"always\"). Use \"on_error\" when the user says something like \"只有出问题才告诉我\" / \"only ping me if it fails\" — a successful run then goes unreported and stays in the job's run history. \"never\" delivers nothing at all. A job that stops to ask for approval is delivered under every setting."
                },
                "prompt": {
                    "type": "string",
                    "description": "The instruction an agent-mode job runs each time it fires. Write it as a self-contained task — the turn has no conversation history (action=add; pick prompt OR command)."
                },
                "skills": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Skills the agent job loads before acting (action=add, agent mode)."
                },
                "workspace": {
                    "type": "string",
                    "description": "Directory the agent job's file and shell tools are confined to (action=add, agent mode). Give it when the task is about a specific project or folder — without it the job works in the gateway's own workspace, which is usually not where the user's repo is. The path must already exist; it is checked when the job is created, not when it runs."
                },
                "grants": {
                    "type": "array",
                    "description": "Actions this agent job must be allowed to take when it runs with nobody watching (action=add, agent mode). Without a grant, every side-effecting call the job makes is refused at run time. Declare only what the task plainly needs — the user approves this list together with the job, and an unneeded entry is a permission they did not want to give. If you are unsure an action is needed, leave it out: a missing grant fails loudly at run time and the user is told, whereas an extra one is silent.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "category": {
                                "type": "string",
                                "enum": ["shell", "file", "network", "homeassistant", "mcp", "wiki"],
                                "description": "What kind of action to allow."
                            },
                            "match": {
                                "type": "string",
                                "enum": ["exact", "prefix", "suffix", "contains", "any"],
                                "description": "How `value` is compared against the action's target. Prefer the narrowest that works — `exact` where you know the target. `any` means the whole category and needs no `value`; use it only when the job genuinely cannot be pinned down."
                            },
                            "value": {
                                "type": "string",
                                "description": "The target: a command prefix for shell (\"git \"), a path prefix for file, a host for network, `domain.service` for homeassistant (\"climate.set_temperature\"), `server.tool` for mcp, the action name for wiki (\"refresh\" / \"rebuild\")."
                            },
                            "access": {
                                "type": "string",
                                "enum": ["read", "write"],
                                "description": "file only: restrict the grant to reads or to writes."
                            }
                        },
                        "required": ["category"]
                    }
                },
                "command": {
                    "type": "string",
                    "description": "Absolute path of the program a command-mode job runs (no shell, so no pipes/globs). Needs prominent approval — prefer an agent job unless the user named a script (action=add; pick prompt OR command)."
                },
                "args": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Arguments for `command` (action=add, command mode)."
                },
                "workdir": {
                    "type": "string",
                    "description": "Working directory for `command` (action=add, command mode)."
                },
                "timeout_secs": {
                    "type": "integer",
                    "description": "Wall-clock budget for `command`; the process is killed past it (action=add, command mode; default 900)."
                }
            },
            "required": ["action"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: CronArgs = parse_args(&input)?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        match args.action.as_str() {
            "list" => {
                let jobs = self.jobs.list().await?;
                if jobs.is_empty() {
                    return Ok(ToolOutput::text("No scheduled jobs."));
                }
                Ok(
                    ToolOutput::text(jobs.iter().map(describe_job).collect::<Vec<_>>().join("\n"))
                        .with_title(format!("{} scheduled jobs", jobs.len())),
                )
            }

            "add" => {
                let name = require_name(&args.name)?;
                let schedule = args
                    .schedule
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        ToolError::InvalidInput(
                            "`schedule` is required for action=add — a 5-field cron \
                             expression like \"0 8 * * *\" (local time)"
                                .to_string(),
                        )
                    })?
                    .to_string();
                // The single string→trigger parse site, shared with the CLI:
                // a bad expression is refused here, not at 03:00.
                let trigger = actions::parse_schedule(&schedule, now)
                    .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
                let notify = args
                    .notify
                    .as_deref()
                    .map(str::trim)
                    .filter(|n| !n.is_empty())
                    .map(parse_notify_policy)
                    .unwrap_or_default();

                let (action, request, grants) = match (args.prompt, args.command) {
                    (Some(_), Some(_)) => {
                        return Err(ToolError::InvalidInput(
                            "pass either `prompt` (agent job) or `command` (program job), not both"
                                .to_string(),
                        ));
                    }
                    (Some(prompt), None) => {
                        // Normalize *before* prompting: what the operator reads
                        // has to be exactly what gets stored, and a malformed
                        // entry should fail here rather than after they said yes.
                        let grants = actions::normalize_grants(
                            args.grants.into_iter().map(RuleSpec::from).collect(),
                        )
                        .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
                        let workspace = args.workspace.clone();
                        let summary = match &workspace {
                            Some(dir) => format!(
                                "Schedule agent job `{name}` [{schedule}] in {dir}: {}",
                                oneline(&prompt, PROMPT_PREVIEW)
                            ),
                            None => format!(
                                "Schedule agent job `{name}` [{schedule}]: {}",
                                oneline(&prompt, PROMPT_PREVIEW)
                            ),
                        };
                        let request = if grants.is_empty() {
                            ApprovalRequest::normal(summary).with_scope_key("cron:add".to_string())
                        } else {
                            // Deliberately no scope key: a scope key means
                            // "approve this kind of thing once per session", and
                            // every job's grant list is different — reusing one
                            // would let the second job's permissions through on
                            // the first job's approval.
                            ApprovalRequest::normal(summary).with_detail(format!(
                                "This job will be allowed to take these actions unattended, \
                                 every time it runs (only this job — removing it revokes them):\n{}",
                                describe_grants(&grants)
                            ))
                        };
                        (
                            CronAction::Agent {
                                prompt,
                                skills: args.skills,
                                workspace,
                            },
                            request,
                            grants,
                        )
                    }
                    (None, Some(command)) => {
                        let line = command_line(&command, &args.args);
                        // Approving this approves every future execution: the
                        // sweep runs a command job directly, with no approver.
                        let request = ApprovalRequest::dangerous(
                            format!("Schedule command job `{name}` [{schedule}]: {line}"),
                            format!(
                                "The gateway will run `{line}` unattended on this schedule, \
                                 with no further approval each time. Remove it with \
                                 `komo cron remove {name}`."
                            ),
                        )
                        .with_action(ActionRef::Shell { command: line });
                        (
                            CronAction::Command {
                                command,
                                args: args.args,
                                workdir: args.workdir,
                                timeout_secs: args
                                    .timeout_secs
                                    .unwrap_or(DEFAULT_CRON_JOB_TIMEOUT_SECS),
                            },
                            request,
                            // A command job fires the program directly with no
                            // approver in the loop, so there is no gate for a
                            // grant to open — approving the job *is* the grant.
                            Vec::new(),
                        )
                    }
                    (None, None) => {
                        return Err(ToolError::InvalidInput(
                            "action=add needs either `prompt` (an agent job) or `command` \
                             (a fixed program)"
                                .to_string(),
                        ));
                    }
                };

                if let Decision::Deny { feedback } = ctx.decide(&request).await {
                    return Ok(ToolOutput::text(match feedback {
                        Some(reason) => format!(
                            "Job `{name}` rejected by the user; nothing was scheduled. \
                             They said: {reason}"
                        ),
                        None => format!("Job `{name}` rejected by user; nothing was scheduled."),
                    }));
                }

                // Shared with `komo cron add` and the api channel: schedule
                // parsing, name rules and uniqueness live in one place.
                let job = actions::add_cron_job(
                    self.jobs.as_ref(),
                    CronJobSpec {
                        name,
                        trigger,
                        action,
                        grants,
                        catch_up: Default::default(),
                        notify,
                    },
                    now,
                )
                .await?;
                let delivery = match job.notify {
                    NotifyPolicy::Always => "output goes to the home channel",
                    NotifyPolicy::OnError => "only failures are delivered to the home channel",
                    NotifyPolicy::Never => "nothing is delivered; check `cron list` for its runs",
                };
                Ok(ToolOutput::text(format!(
                    "Scheduled {} job `{}` [{}] — first run {}. Runs while `komo gateway` \
                     is up; {delivery}.",
                    job.action.kind(),
                    job.name,
                    job.trigger.describe(),
                    local_time(job.next_run_at)
                ))
                .with_structured(json!({
                    "name": job.name,
                    "kind": job.action.kind(),
                    "trigger": job.trigger,
                    "notify": job.notify.as_str(),
                })))
            }

            "remove" => {
                let name = require_name(&args.name)?;
                // Confirm it exists before prompting: "approve deleting a job
                // that isn't there" is a pointless question.
                if self.jobs.find_by_name(&name).await?.is_none() {
                    return Err(missing_job(&name));
                }
                if let Some(refusal) = approve_manage(
                    ctx,
                    format!("Delete scheduled job `{name}`"),
                    &format!("job `{name}` was kept."),
                )
                .await
                {
                    return Ok(ToolOutput::text(refusal));
                }
                if !self.jobs.delete(&name).await? {
                    return Err(missing_job(&name));
                }
                Ok(ToolOutput::text(format!("Removed job `{name}`.")))
            }

            action @ ("enable" | "disable") => {
                let name = require_name(&args.name)?;
                let enabled = action == "enable";
                let verb = if enabled { "Resume" } else { "Pause" };
                if self.jobs.find_by_name(&name).await?.is_none() {
                    return Err(missing_job(&name));
                }
                if let Some(refusal) = approve_manage(
                    ctx,
                    format!("{verb} scheduled job `{name}`"),
                    &format!("job `{name}` was left as it was."),
                )
                .await
                {
                    return Ok(ToolOutput::text(refusal));
                }
                let job = actions::set_cron_enabled(self.jobs.as_ref(), &name, enabled, now)
                    .await?
                    .ok_or_else(|| missing_job(&name))?;
                Ok(ToolOutput::text(if enabled {
                    format!(
                        "Enabled job `{}` — next run {}.",
                        job.name,
                        local_time(job.next_run_at)
                    )
                } else {
                    format!(
                        "Disabled job `{}`. It stays listed and can be re-enabled.",
                        job.name
                    )
                }))
            }

            "run" => {
                let name = require_name(&args.name)?;
                if self.jobs.find_by_name(&name).await?.is_none() {
                    return Err(missing_job(&name));
                }
                if let Some(refusal) = approve_manage(
                    ctx,
                    format!("Run scheduled job `{name}` now"),
                    &format!("job `{name}` was not run."),
                )
                .await
                {
                    return Ok(ToolOutput::text(refusal));
                }
                let job = actions::trigger_cron_job(self.jobs.as_ref(), &name, now)
                    .await?
                    .ok_or_else(|| missing_job(&name))?;
                Ok(ToolOutput::text(format!(
                    "Job `{}` is due now — the gateway runs it on its next sweep tick \
                     (within a minute) and delivers the output to the home channel.",
                    job.name
                )))
            }

            other => Err(ToolError::InvalidInput(format!(
                "unknown action `{other}` (expected list/add/remove/enable/disable/run)"
            ))),
        }
    }
}

fn require_name(name: &Option<String>) -> Result<String, ToolError> {
    let name = name
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .ok_or_else(|| ToolError::InvalidInput("`name` is required for this action".to_string()))?;
    Ok(name.to_string())
}

/// "No such job" is the model naming one that doesn't exist — its mistake to
/// fix from `action=list`, not a transient failure worth retrying.
fn missing_job(name: &str) -> ToolError {
    ToolError::InvalidInput(actions::no_cron_job_message(name))
}

/// One job as a line the model can relay: name, kind, schedule, state, target,
/// and the last outcome when there is one.
fn describe_job(job: &CronJob) -> String {
    let state = match job.status {
        CronJobStatus::Active => format!("next {}", local_time(job.next_run_at)),
        CronJobStatus::Paused => "paused".to_string(),
        CronJobStatus::Done => "done".to_string(),
    };
    let target = match &job.action {
        CronAction::Command { command, args, .. } => command_line(command, args),
        CronAction::Agent {
            prompt,
            skills,
            workspace,
        } => {
            let skills = if skills.is_empty() {
                String::new()
            } else {
                format!(" [skills: {}]", skills.join(", "))
            };
            let workspace = match workspace {
                Some(dir) => format!(" [in {dir}]"),
                None => String::new(),
            };
            format!("{}{skills}{workspace}", oneline(prompt, PROMPT_PREVIEW))
        }
    };
    let mut line = format!(
        "{} ({}) [{}] {} → {}",
        job.name,
        job.action.kind(),
        job.trigger.describe(),
        state,
        target
    );
    if job.notify != NotifyPolicy::Always {
        line.push_str(&format!(" | notify {}", job.notify.as_str()));
    }
    if let Some(run) = job.last_run() {
        line.push_str(&format!(" | {}", describe_run(run)));
    }
    if !job.last_error.is_empty() {
        line.push_str(&format!(
            " | trigger error: {}",
            oneline(&job.last_error, PROMPT_PREVIEW)
        ));
    }
    line
}

/// One firing as a line: when, how it went, what set it off, what it produced.
fn describe_run(run: &RoutineRun) -> String {
    let mut line = format!(
        "last run {} {} ({})",
        local_time(run.started_at),
        run.status.as_str(),
        run.event
    );
    if !run.output.is_empty() {
        line.push_str(&format!(" — {}", oneline(&run.output, PROMPT_PREVIEW)));
    }
    line
}

/// The grant list as the operator reads it at the approval prompt, one rule per
/// line. Uses `Rule::describe()` — the same rendering `komo policy list` prints,
/// so what is approved here and what is inspected later read identically.
///
/// Pure, and takes already-normalized specs, so the prompt cannot drift from
/// what gets stored.
fn describe_grants(grants: &[RuleSpec]) -> String {
    grants
        .iter()
        .filter_map(|spec| spec.to_rule())
        .map(|rule| format!("  {}", rule.describe()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn command_line(command: &str, args: &[String]) -> String {
    std::iter::once(command)
        .chain(args.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Collapse to one line and cap at `max` characters (never mid-char).
fn oneline(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        return flat;
    }
    format!("{}…", flat.chars().take(max).collect::<String>())
}

fn local_time(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| dt.with_timezone(&chrono::Local).to_rfc3339())
        .unwrap_or_else(|| unix.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::Decision;
    use komo_core::domain::cron::{RoutineRunStatus, Trigger};
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeJobs {
        jobs: Mutex<Vec<CronJob>>,
    }

    #[async_trait]
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

    /// Records what it was asked and answers with a fixed verdict. Keeps the
    /// detail and scope key too — for an `add` carrying grants they are the
    /// substance of the prompt, not decoration.
    struct Recorder {
        allow: bool,
        seen: Mutex<Vec<(String, komo_core::domain::approval::Risk)>>,
        details: Mutex<Vec<Option<String>>>,
        scope_keys: Mutex<Vec<Option<String>>>,
    }

    impl Recorder {
        fn new(allow: bool) -> Arc<Self> {
            Arc::new(Self {
                allow,
                seen: Mutex::new(Vec::new()),
                details: Mutex::new(Vec::new()),
                scope_keys: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl komo_core::domain::approval::Approver for Recorder {
        async fn decide(&self, request: &ApprovalRequest) -> Decision {
            self.seen
                .lock()
                .unwrap()
                .push((request.summary.clone(), request.risk));
            self.details.lock().unwrap().push(request.detail.clone());
            self.scope_keys
                .lock()
                .unwrap()
                .push(request.scope_key.clone());
            self.allow.into()
        }
    }

    fn tool(allow: bool) -> (CronTool, Arc<FakeJobs>, Arc<Recorder>) {
        let jobs = Arc::new(FakeJobs::default());
        let approver = Recorder::new(allow);
        let t = CronTool::new(jobs.clone() as Arc<dyn CronJobRepository>);
        (t, jobs, approver)
    }

    /// Run one call with `rec` as the turn's approver (it now rides on the
    /// context, not the tool).
    async fn run(t: &CronTool, args: Value, rec: &Arc<Recorder>) -> Result<ToolOutput, ToolError> {
        let ctx = ToolContext::new(
            komo_core::domain::context::SessionContext::detached("cli:test"),
            None,
            rec.clone(),
        );
        t.call(args, &ctx).await
    }

    #[tokio::test]
    async fn add_agent_job_persists_after_approval() {
        let (t, jobs, rec) = tool(true);
        let out = run(&t, json!({"action": "add", "name": "morning-brief", "schedule": "0 8 * * *", "prompt": "总结我今天的日程", "skills": ["calendar"]}), &rec)
            .await
            .unwrap()
            .text;
        assert!(out.contains("morning-brief"), "{out}");
        assert!(out.contains("first run"), "{out}");

        let stored = jobs.jobs.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].trigger, Trigger::cron("0 8 * * *"));
        let CronAction::Agent { prompt, skills, .. } = &stored[0].action else {
            panic!("agent job");
        };
        assert_eq!(prompt, "总结我今天的日程");
        assert_eq!(skills, &vec!["calendar".to_string()]);
        assert!(stored[0].next_run_at > 0, "schedule was resolved");

        let seen = rec.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].1, komo_core::domain::approval::Risk::Normal);
    }

    /// The whole point of the feature: creating the job and approving what it
    /// may do are **one** interaction, and the prompt spells the permissions out.
    #[tokio::test]
    async fn creating_a_job_approves_its_grants_in_the_same_prompt() {
        let (t, jobs, rec) = tool(true);
        let out = run(
            &t,
            json!({
                "action": "add", "name": "ac-temp", "schedule": "0 22 * * *",
                "prompt": "把卧室空调设到 26 度",
                "grants": [{"category": "homeassistant", "match": "exact",
                            "value": "climate.set_temperature"}]
            }),
            &rec,
        )
        .await
        .unwrap()
        .text;
        assert!(out.contains("ac-temp"), "{out}");

        // Exactly one prompt, and it names the action being granted.
        let seen = rec.seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "creating the job must ask exactly once");
        let detail = rec.details.lock().unwrap()[0]
            .clone()
            .expect("a grant list must be shown before it is approved");
        assert!(detail.contains("climate.set_temperature"), "{detail}");
        assert!(detail.contains("homeassistant"), "{detail}");

        // A grant list must not ride on a session scope key: the next job's
        // permissions would then ride in on this job's approval.
        assert_eq!(rec.scope_keys.lock().unwrap()[0], None);

        // Stored, with the rule shape fixed rather than taken from the caller.
        let stored = jobs.jobs.lock().unwrap();
        assert_eq!(stored[0].grants.len(), 1);
        let rule = &stored[0].granted_rules()[0];
        assert!(
            rule.unattended,
            "a job grant must work with nobody watching"
        );
        assert!(!rule.include_dangerous);
        assert_eq!(rule.channels, None);
    }

    /// Denying the prompt leaves nothing behind — no job, and so no grants.
    #[tokio::test]
    async fn denying_the_prompt_creates_no_job_and_no_grants() {
        let (t, jobs, rec) = tool(false);
        let out = run(
            &t,
            json!({
                "action": "add", "name": "ac-temp", "schedule": "0 22 * * *",
                "prompt": "设到 26 度",
                "grants": [{"category": "homeassistant", "match": "exact",
                            "value": "climate.set_temperature"}]
            }),
            &rec,
        )
        .await
        .unwrap()
        .text;
        assert!(out.contains("rejected"), "{out}");
        assert!(jobs.jobs.lock().unwrap().is_empty());
    }

    /// A malformed grant fails **before** the prompt: approving a list and then
    /// silently storing a different one is the failure this guards against.
    #[tokio::test]
    async fn an_invalid_grant_fails_before_asking() {
        let (t, jobs, rec) = tool(true);
        let err = run(
            &t,
            json!({
                "action": "add", "name": "x", "schedule": "0 22 * * *", "prompt": "do it",
                "grants": [{"category": "teleport", "match": "exact", "value": "somewhere"}]
            }),
            &rec,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)), "{err:?}");
        assert!(
            rec.seen.lock().unwrap().is_empty(),
            "must not prompt for a list it cannot store"
        );
        assert!(jobs.jobs.lock().unwrap().is_empty());
    }

    /// An `add` without grants keeps its session scope key — the old behavior,
    /// where approving "schedule jobs" once per session is the right trade.
    #[tokio::test]
    async fn an_add_without_grants_keeps_its_scope_key() {
        let (t, _jobs, rec) = tool(true);
        run(
            &t,
            json!({"action": "add", "name": "brief", "schedule": "0 8 * * *", "prompt": "summarize"}),
            &rec,
        )
        .await
        .unwrap();
        assert_eq!(
            rec.scope_keys.lock().unwrap()[0].as_deref(),
            Some("cron:add")
        );
    }

    #[tokio::test]
    async fn command_job_is_gated_as_dangerous() {
        let (t, jobs, rec) = tool(true);
        run(
            &t,
            json!({"action": "add", "name": "rotate", "schedule": "0 14 * * 5",
                   "command": "/opt/rotate.py", "args": ["--push"]}),
            &rec,
        )
        .await
        .unwrap();
        let seen = rec.seen.lock().unwrap();
        assert_eq!(seen[0].1, komo_core::domain::approval::Risk::Dangerous);
        assert!(seen[0].0.contains("/opt/rotate.py --push"), "{}", seen[0].0);
        assert_eq!(jobs.jobs.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn denied_add_stores_nothing() {
        let (t, jobs, rec) = tool(false);
        let out = run(
            &t,
            json!({"action": "add", "name": "x", "schedule": "0 8 * * *", "prompt": "hi"}),
            &rec,
        )
        .await
        .unwrap()
        .text;
        assert!(out.contains("rejected"), "{out}");
        assert!(jobs.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn add_rejects_bad_schedule_and_missing_action_fields() {
        let (t, jobs, rec) = tool(true);
        // A schedule croner can't parse never reaches the store.
        assert!(
            run(
                &t,
                json!({"action": "add", "name": "x", "schedule": "nope", "prompt": "hi"}),
                &rec
            )
            .await
            .is_err()
        );
        // Neither prompt nor command.
        assert!(
            run(
                &t,
                json!({"action": "add", "name": "x", "schedule": "0 8 * * *"}),
                &rec
            )
            .await
            .is_err()
        );
        // Both.
        assert!(
            run(
                &t,
                json!({"action": "add", "name": "x", "schedule": "0 8 * * *",
                       "prompt": "hi", "command": "/bin/true"}),
                &rec
            )
            .await
            .is_err()
        );
        // No schedule at all.
        assert!(
            run(
                &t,
                json!({"action": "add", "name": "x", "prompt": "hi"}),
                &rec
            )
            .await
            .is_err()
        );
        assert!(jobs.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn add_rejects_a_name_that_is_not_key_shaped() {
        let (t, jobs, rec) = tool(true);
        let err =             run(&t, json!({"action": "add", "name": "morning brief", "schedule": "0 8 * * *", "prompt": "hi"}), &rec)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid job name"), "{err}");
        assert!(jobs.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn add_rejects_duplicate_name() {
        let (t, _jobs, rec) = tool(true);
        let add = json!({"action": "add", "name": "dup", "schedule": "0 8 * * *", "prompt": "hi"});
        run(&t, add.clone(), &rec).await.unwrap();
        let err = run(&t, add, &rec).await.unwrap_err().to_string();
        assert!(err.contains("already exists"), "{err}");
    }

    #[tokio::test]
    async fn disable_then_enable_recomputes_next_run() {
        let (t, jobs, rec) = tool(true);
        run(
            &t,
            json!({"action": "add", "name": "j", "schedule": "0 8 * * *", "prompt": "hi"}),
            &rec,
        )
        .await
        .unwrap();

        let out = run(&t, json!({"action": "disable", "name": "j"}), &rec)
            .await
            .unwrap()
            .text;
        assert!(out.contains("Disabled"), "{out}");
        assert_eq!(jobs.jobs.lock().unwrap()[0].status, CronJobStatus::Paused);

        let out = run(&t, json!({"action": "enable", "name": "j"}), &rec)
            .await
            .unwrap()
            .text;
        assert!(out.contains("next"), "{out}");
        let stored = jobs.jobs.lock().unwrap();
        assert_eq!(stored[0].status, CronJobStatus::Active);
        assert!(stored[0].next_run_at > time::OffsetDateTime::now_utc().unix_timestamp());
    }

    #[tokio::test]
    async fn run_makes_the_job_due_now() {
        let (t, jobs, rec) = tool(true);
        run(
            &t,
            json!({"action": "add", "name": "j", "schedule": "0 8 * * *", "prompt": "hi"}),
            &rec,
        )
        .await
        .unwrap();
        let out = run(&t, json!({"action": "run", "name": "j"}), &rec)
            .await
            .unwrap()
            .text;
        assert!(out.contains("due now"), "{out}");
        assert!(
            jobs.jobs.lock().unwrap()[0].next_run_at
                <= time::OffsetDateTime::now_utc().unix_timestamp()
        );
    }

    #[tokio::test]
    async fn denied_management_leaves_the_job_alone() {
        let (t, jobs, rec) = tool(true);
        run(
            &t,
            json!({"action": "add", "name": "j", "schedule": "0 8 * * *", "prompt": "hi"}),
            &rec,
        )
        .await
        .unwrap();
        // Same tool and store, but this turn's approver denies.
        let denier = Recorder::new(false);
        let out = run(&t, json!({"action": "remove", "name": "j"}), &denier)
            .await
            .unwrap()
            .text;
        assert!(out.contains("kept"), "{out}");
        assert_eq!(jobs.jobs.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unknown_job_errors_without_prompting() {
        let (t, _jobs, rec) = tool(true);
        for action in ["remove", "enable", "disable", "run"] {
            let err = run(&t, json!({"action": action, "name": "ghost"}), &rec)
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("no cron job named"), "{action}: {err}");
        }
        assert!(
            rec.seen.lock().unwrap().is_empty(),
            "a missing job must not raise an approval prompt"
        );
    }

    #[tokio::test]
    async fn list_reports_schedule_state_and_last_outcome() {
        let (t, jobs, rec) = tool(true);
        assert_eq!(
            run(&t, json!({"action": "list"}), &rec).await.unwrap().text,
            "No scheduled jobs."
        );
        run(
            &t,
            json!({"action": "add", "name": "j", "schedule": "0 8 * * *",
                   "prompt": "a very long prompt that goes on and on"}),
            &rec,
        )
        .await
        .unwrap();
        {
            let mut stored = jobs.jobs.lock().unwrap();
            let id = stored[0].begin_run(1_700_000_000, "cron `0 8 * * *` @ slot".into());
            stored[0].finish_run(&id, RoutineRunStatus::Error, "boom\nsecond line", None);
        }
        let out = run(&t, json!({"action": "list"}), &rec).await.unwrap().text;
        assert!(out.contains("j (agent) [cron `0 8 * * *`]"), "{out}");
        assert!(out.contains("last run"), "{out}");
        assert!(out.contains("error (cron `0 8 * * *` @ slot)"), "{out}");
        assert!(out.contains("— boom second line"), "{out}");
    }

    /// "只有出问题才告诉我" reaches the store as the job's own policy, and the
    /// listing says so — a silenced job that looked ordinary would be the kind
    /// of surprise nobody debugs.
    #[tokio::test]
    async fn a_notify_policy_is_stored_and_listed() {
        let (t, jobs, rec) = tool(true);
        run(
            &t,
            json!({"action": "add", "name": "backup", "schedule": "0 3 * * *",
                   "prompt": "back things up", "notify": "on_error"}),
            &rec,
        )
        .await
        .unwrap();
        assert_eq!(
            jobs.jobs.lock().unwrap()[0].notify,
            komo_core::domain::cron::NotifyPolicy::OnError
        );
        let out = run(&t, json!({"action": "list"}), &rec).await.unwrap().text;
        assert!(out.contains("notify on_error"), "{out}");

        // Unstated stays today's behaviour, and is not called out in the list.
        run(
            &t,
            json!({"action": "add", "name": "brief", "schedule": "0 8 * * *", "prompt": "brief"}),
            &rec,
        )
        .await
        .unwrap();
        assert_eq!(
            jobs.jobs.lock().unwrap()[1].notify,
            komo_core::domain::cron::NotifyPolicy::Always
        );
    }

    /// The schedule is parsed where the model can be told it was wrong, not at
    /// 03:00 against a store that already accepted it.
    #[tokio::test]
    async fn a_broken_schedule_is_refused_at_add() {
        let (t, jobs, rec) = tool(true);
        let err = run(
            &t,
            json!({"action": "add", "name": "j", "schedule": "not a cron", "prompt": "p"}),
            &rec,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not a cron"), "{err}");
        assert!(jobs.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_action_errors() {
        let (t, _jobs, rec) = tool(true);
        assert!(
            run(&t, json!({"action": "frobnicate"}), &rec)
                .await
                .is_err()
        );
    }

    #[test]
    fn oneline_flattens_and_caps_on_char_boundaries() {
        assert_eq!(oneline("a\n b  c", 40), "a b c");
        assert_eq!(oneline("日程日程日程", 3), "日程日…");
    }
}
