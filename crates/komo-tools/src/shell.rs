use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;

use komo_core::domain::{
    approval::{ActionRef, ApprovalRequest, Decision},
    background::{TaskReport, TaskSpec},
    cancel::Cancelled,
    context::ToolContext,
    session_event::{TaskKind, ToolOutcome},
    tool::{Tool, ToolError, ToolOutput, parse_args},
    workspace::Workspace,
};

/// Why the command stopped being waited on. Both outcomes kill the process
/// group; they differ in what the caller is told.
enum Interrupt {
    /// The command's own `timeout` elapsed — reported to the model, which can
    /// retry with a bigger one.
    Timeout,
    /// The user stopped the turn. Nothing will read a reply, so this ends the
    /// call as an error the ledger records.
    Cancelled,
}

/// Command substrings treated as high-risk. Matching commands are flagged as
/// dangerous in the approval prompt.
const DANGEROUS_PATTERNS: &[&str] = &[
    "rm ",
    "rm -",
    "rmdir",
    "unlink",
    "git push",
    "git reset --hard",
    "git clean",
    "git branch -d",
    "git checkout --",
    "dd ",
    "mkfs",
    "sudo ",
    "shutdown",
    "reboot",
    "kill ",
    "killall",
    "chmod ",
    "chown ",
    "truncate",
    "> /dev/",
    "mv ",
    ":(){",
];

/// Commands that are never run, even with user approval (hermes calls this the
/// "hardline floor"): the blast radius is the whole machine, not the workspace.
const HARDLINE_PATTERNS: &[&str] = &[
    "rm -rf /",
    "rm -fr /",
    "mkfs",
    "dd if=/dev/zero of=/dev/",
    "of=/dev/sd",
    "of=/dev/disk",
    ":(){",
    "shutdown",
    "reboot",
    "halt",
];

/// True if `pattern` occurs in `haystack` (already lowercased) at a command
/// boundary, not buried inside a larger alphanumeric word. A naive `contains`
/// flags `terraform apply` and `kill -TERM 1` as the `rm ` pattern, because
/// "rm " is a substring of "terrafo*rm* " and "-te*rm* 1". We require the char
/// before the match to be a non-alphanumeric (or start), and — when the pattern
/// ends in a letter/digit — the char after it likewise, so the pattern lines up
/// with a real token rather than the middle of one.
fn matches_at_boundary(haystack: &str, pattern: &str) -> bool {
    let bytes = haystack.as_bytes();
    let pat = pattern.as_bytes();
    let pattern_ends_alnum = pat.last().is_some_and(u8::is_ascii_alphanumeric);
    let mut from = 0;
    while let Some(rel) = haystack[from..].find(pattern) {
        let at = from + rel;
        let before_ok = at == 0 || !bytes[at - 1].is_ascii_alphanumeric();
        let after = at + pat.len();
        let after_ok =
            !pattern_ends_alnum || after >= bytes.len() || !bytes[after].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
        // The matched pattern starts on an ASCII byte, so `at + 1` is a valid
        // char boundary to resume the scan from.
        from = at + 1;
    }
    false
}

fn dangerous_pattern(command: &str) -> Option<&'static str> {
    let lc = command.to_lowercase();
    DANGEROUS_PATTERNS
        .iter()
        .copied()
        .find(|p| matches_at_boundary(&lc, p))
}

fn hardline_pattern(command: &str) -> Option<&'static str> {
    let lc = command.to_lowercase();
    HARDLINE_PATTERNS
        .iter()
        .copied()
        .find(|p| matches_at_boundary(&lc, p))
}

#[derive(Deserialize)]
struct ShellArgs {
    command: String,
    /// Wall-clock budget in milliseconds. The model asks for more when it knows
    /// the command is slow (a build, a test run) rather than losing the work to a
    /// fixed ceiling.
    #[serde(default)]
    timeout: Option<u64>,
    /// Working directory, relative to the workspace root (default: the root).
    #[serde(default)]
    workdir: Option<String>,
    /// Hand the command off and return a task id instead of waiting for it.
    #[serde(default)]
    background: bool,
}

/// Default command budget, matching opencode v2's `bash`.
const DEFAULT_TIMEOUT_MS: u64 = 2 * 60 * 1_000;
/// Ceiling on what the model may ask for.
const MAX_TIMEOUT_MS: u64 = 10 * 60 * 1_000;

/// Markers that introduce a secret value as `marker=<secret>` (case-insensitive).
const SECRET_KEY_MARKERS: &[&str] = &[
    "api_key=",
    "apikey=",
    "api-key=",
    "token=",
    "secret=",
    "password=",
    "passwd=",
    "pwd=",
    "access_key=",
    "auth=",
];

/// Flags whose *following* token is a secret (`--password hunter2`).
const SECRET_FLAGS: &[&str] = &["--password", "--token", "--api-key", "--secret", "-p"];

/// Upper bound on how many bytes of stdout/stderr each stream is read into
/// memory. Well above the LLM result cap (which truncates the model-facing text
/// anyway), so it never clips useful output — it only stops a command that
/// spews unbounded output (`cat` a huge file, `yes`) from OOMing the gateway.
/// Reading stops at the cap and the child is killed (`kill_on_drop`).
const MAX_STREAM_BYTES: u64 = 256 * 1024;

/// A token that "looks like" an opaque credential: long and a single run of
/// url-safe-ish characters with no shell punctuation.
fn looks_like_secret(token: &str) -> bool {
    token.len() >= 24
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '+' | '/' | '='))
        && token.chars().any(|c| c.is_ascii_digit())
        && token.chars().any(|c| c.is_ascii_alphabetic())
}

/// Best-effort scrub of secret-looking substrings from a shell command before it
/// is written to the run ledger. Heuristic, dependency-free, whitespace-tokenized:
/// covers `key=value`, `Bearer <tok>`, `--password <tok>`, and high-entropy
/// tokens. The command structure stays readable; only the secret is replaced.
fn redact_secrets(command: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut scrub_next = false;
    for raw in command.split_whitespace() {
        if scrub_next {
            out.push("***".to_string());
            scrub_next = false;
            continue;
        }
        let lower = raw.to_lowercase();
        if lower == "bearer" || SECRET_FLAGS.contains(&lower.as_str()) {
            out.push(raw.to_string());
            scrub_next = true;
            continue;
        }
        if let Some(marker) = SECRET_KEY_MARKERS.iter().find(|m| lower.starts_with(**m)) {
            // Preserve the original-case key prefix, drop the value.
            out.push(format!("{}***", &raw[..marker.len()]));
            continue;
        }
        if looks_like_secret(raw) {
            out.push("***".to_string());
            continue;
        }
        out.push(raw.to_string());
    }
    out.join(" ")
}

/// Runs a shell command via `sh -c`, gated behind an [`Approver`]. Dangerous
/// commands (deletes, `git push`, `sudo`, ...) are flagged prominently. Runs
/// with the working directory set to the workspace root.
pub struct ShellTool {
    workspace: Arc<Workspace>,
}

impl ShellTool {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[async_trait]
impl Tool for ShellTool {
    fn name(&self) -> &'static str {
        "shell"
    }

    fn description(&self) -> &'static str {
        "Run a shell command on the local machine via `sh -c` and return its \
         combined stdout/stderr. Safe (read-only) commands run without a \
         prompt; destructive commands require an explicit dangerous-action \
         confirmation, and a few catastrophic ones are always refused. Pass \
         `background: true` for long work: the call returns a task id at once, \
         the turn is free to end, and the result comes back to this \
         conversation when it lands."
    }

    /// The caller may ask for up to [`MAX_TIMEOUT_MS`]; the executor's clock has
    /// to sit *above* that, or a legitimate long command would be aborted with an
    /// opaque error instead of this tool's "retry with a bigger timeout". The
    /// slack also covers a human sitting on the approval prompt.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(
            std::time::Duration::from_millis(MAX_TIMEOUT_MS)
                + komo_core::domain::tool::APPROVAL_BOUND,
        )
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to run, e.g. `ls -la`."
                },
                "timeout": {
                    "type": "integer",
                    "description": format!(
                        "Milliseconds to allow before the command (and anything it \
                         started) is killed. Default {DEFAULT_TIMEOUT_MS}, maximum \
                         {MAX_TIMEOUT_MS}. Raise it for builds and test runs."
                    )
                },
                "workdir": {
                    "type": "string",
                    "description": "Directory to run in, relative to the workspace root. Defaults to the root."
                },
                "background": {
                    "type": "boolean",
                    "description": "Run it detached: this call returns a task id immediately and the turn is free to end. You are told the result in this conversation when it lands, or you can stop and wait for it with `wait`. For work measured in minutes — never for something you need in order to answer now."
                }
            },
            "required": ["command"]
        })
    }

    /// Scrub secret-looking substrings from the command before it lands in the
    /// run ledger (the command itself is kept for audit; only secrets go).
    fn redact_args(&self, args: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(args) {
            Ok(mut v) => {
                if let Some(cmd) = v.get("command").and_then(|c| c.as_str()) {
                    v["command"] = serde_json::json!(redact_secrets(cmd));
                }
                v.to_string()
            }
            Err(_) => "<shell args redacted>".to_string(),
        }
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: ShellArgs = parse_args(&input)?;

        // Hardline floor: catastrophic commands are refused outright — no
        // approval can unlock them.
        if let Some(pattern) = hardline_pattern(&args.command) {
            return Ok(ToolOutput::text(format!(
                "Command refused: matched hardline pattern `{pattern}`. \
                 This command is never run, even with approval. Do not retry it."
            )));
        }

        // Approval gate (hermes-style): commands matching a dangerous pattern
        // prompt the user; everything else is `Risk::Safe` and an interactive
        // approver lets it through without asking.
        let summary = format!("run shell command: {}", args.command);
        let action = ActionRef::Shell {
            command: args.command.clone(),
        };
        let request = match dangerous_pattern(&args.command) {
            Some(pattern) => ApprovalRequest::dangerous(
                summary,
                format!("matched dangerous pattern `{pattern}`"),
            )
            .with_scope_key(format!("shell:{pattern}"))
            .with_action(action),
            None => ApprovalRequest::safe(summary).with_action(action),
        };
        // A denial may carry the user's reason ("use trash instead of rm") —
        // hand it back so the next round is a corrected command, not a retry.
        if let Decision::Deny { feedback } = ctx.decide(&request).await {
            return Ok(ToolOutput::text(match feedback {
                Some(reason) => format!(
                    "Command rejected by the user; nothing was run. \
                     They said: {reason}\nAct on that instead of retrying the same command."
                ),
                None => "Command rejected by user; nothing was run.".to_string(),
            }));
        }

        let selected_workspace = ctx
            .session
            .workspace_root
            .as_ref()
            .map(|root| Workspace::new(vec![root.clone()]));
        let workspace = selected_workspace
            .as_ref()
            .unwrap_or(self.workspace.as_ref());
        let cwd = match &args.workdir {
            Some(dir) => {
                let path = workspace
                    .resolve_contained(std::path::Path::new(dir))
                    .ok_or_else(|| {
                        ToolError::Denied(format!(
                            "workdir `{dir}` is outside the workspace and was blocked."
                        ))
                    })?;
                if !tokio::fs::metadata(&path)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false)
                {
                    return Err(ToolError::InvalidInput(format!(
                        "workdir `{dir}` is not a directory."
                    )));
                }
                Some(path)
            }
            None => workspace.roots().first().cloned(),
        };

        let timeout = std::time::Duration::from_millis(
            args.timeout
                .unwrap_or(DEFAULT_TIMEOUT_MS)
                .min(MAX_TIMEOUT_MS),
        );

        let plan = CommandPlan {
            command: args.command.clone(),
            cwd,
            timeout,
        };

        if args.background {
            return spawn_background(plan, ctx).await;
        }

        match plan.run(Some(ctx)).await {
            // The turn is already ending, so nothing will read a reply — the
            // point of returning an error is the ledger, which records this step
            // with the same wording as the run's own cancellation.
            Ran::Cancelled => Err(ToolError::Failed(Cancelled.into())),
            Ran::Broken(error) => Err(ToolError::Failed(anyhow::anyhow!(error))),
            outcome => Ok(plan.render(&outcome)),
        }
    }
}

/// One command, resolved: everything needed to run it and nothing borrowed from
/// the turn. That is what lets the *same* value run in the foreground under the
/// turn's cancellation and, detached, in a task that outlives it.
struct CommandPlan {
    command: String,
    cwd: Option<std::path::PathBuf>,
    timeout: std::time::Duration,
}

/// How the command stopped.
enum Ran {
    Exited {
        out: Vec<u8>,
        err: Vec<u8>,
        code: Option<i32>,
    },
    /// Its own `timeout` elapsed and the process group was killed. Whether the
    /// command had already done its work is not knowable — which is why a
    /// background task settles this as `Uncertain` rather than as a failure.
    TimedOut,
    /// The user stopped the turn. Foreground only: a detached task is not the
    /// turn's to cancel.
    Cancelled,
    /// Never ran, or could not be awaited.
    Broken(String),
}

impl CommandPlan {
    /// Run it, optionally racing the turn's cancellation. `cancel: None` is the
    /// background case: work explicitly detached from the turn must not die
    /// when the turn does.
    async fn run(&self, cancel: Option<&ToolContext>) -> Ran {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(&self.command)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // If the executor's wall-clock timeout aborts the task awaiting this
            // command, dropping the `Child` must kill the process — otherwise
            // `sh` (and its children) would be orphaned and keep running.
            .kill_on_drop(true);
        // Own a process *group*, so a timeout can kill the whole tree. Without
        // this, killing `sh` leaves its children (`sleep`, a dev server, a
        // compiler) running with the pipe still open.
        #[cfg(unix)]
        cmd.process_group(0);
        if let Some(dir) = &self.cwd {
            cmd.current_dir(dir);
        }
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => return Ran::Broken(format!("failed to spawn command: {e}")),
        };
        let pgid = child.id().map(|id| id as i32);

        // Read both streams concurrently, each bounded to MAX_STREAM_BYTES, so a
        // command emitting unbounded output can't buffer the whole thing into
        // memory and OOM the gateway. `stdin(null)` above means a command that
        // reads stdin sees EOF instead of blocking forever waiting for input.
        let mut out_pipe = child.stdout.take();
        let mut err_pipe = child.stderr.take();
        let read_out = async {
            let mut buf = Vec::new();
            if let Some(p) = out_pipe.as_mut() {
                let _ = p.take(MAX_STREAM_BYTES).read_to_end(&mut buf).await;
            }
            buf
        };
        let read_err = async {
            let mut buf = Vec::new();
            if let Some(p) = err_pipe.as_mut() {
                let _ = p.take(MAX_STREAM_BYTES).read_to_end(&mut buf).await;
            }
            buf
        };
        // Race the command against its budget. Reading the pipes is part of the
        // race: a command that writes nothing and hangs must time out too.
        let run = async {
            let (out, err) = tokio::join!(read_out, read_err);
            let status = child.wait().await;
            (out, err, status)
        };
        // Two ways to lose the race, and both kill the group: the command's own
        // budget elapsed, or the user asked to stop the turn. `shell` is the tool
        // that most needs the second one — interrupting a ten-minute build should
        // actually end the build, not just stop waiting for it.
        let stop = async {
            match cancel {
                Some(ctx) => ctx.cancelled().await,
                // Nothing interrupts a detached task; its own timeout is the
                // only way it stops early.
                None => std::future::pending().await,
            }
        };
        let outcome = tokio::select! {
            r = tokio::time::timeout(self.timeout, run) => r.map_err(|_| Interrupt::Timeout),
            _ = stop => Err(Interrupt::Cancelled),
        };

        match outcome {
            Ok((out, err, status)) => match status {
                Ok(status) => Ran::Exited {
                    out,
                    err,
                    code: status.code(),
                },
                Err(e) => Ran::Broken(format!("failed to await command: {e}")),
            },
            // Kill the whole group: `sh` alone would leave its children running
            // (and holding the pipes) forever.
            Err(Interrupt::Timeout) => {
                kill_group(pgid);
                Ran::TimedOut
            }
            Err(Interrupt::Cancelled) => {
                kill_group(pgid);
                Ran::Cancelled
            }
        }
    }

    /// The model-facing result of a finished command.
    fn render(&self, outcome: &Ran) -> ToolOutput {
        let clipped = |raw: &[u8]| (raw.len() as u64) >= MAX_STREAM_BYTES;
        match outcome {
            Ran::TimedOut => ToolOutput::text(format!(
                "Command timed out after {} ms and was killed (along with any \
                 processes it started). Retry with a larger `timeout` if it \
                 legitimately takes longer — the maximum is {MAX_TIMEOUT_MS} ms.",
                self.timeout.as_millis()
            ))
            .with_title(format!("shell (timed out): {}", self.command))
            // `structured` carries the machine-readable outcome (`exit`,
            // `truncated`, `timeout`) so a UI/ledger reader doesn't have to
            // parse the prose.
            .with_structured(json!({ "exit": null, "truncated": false, "timeout": true })),
            Ran::Exited { out, err, code } => {
                let stdout = String::from_utf8_lossy(out);
                let stderr = String::from_utf8_lossy(err);
                let status_text = code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".to_string());
                let mut result = format!("exit status: {status_text}");
                if !stdout.trim().is_empty() {
                    result.push_str(&format!("\n--- stdout ---\n{}", stdout.trim_end()));
                    if clipped(out) {
                        result.push_str("\n…[stdout truncated at the output limit]");
                    }
                }
                if !stderr.trim().is_empty() {
                    result.push_str(&format!("\n--- stderr ---\n{}", stderr.trim_end()));
                    if clipped(err) {
                        result.push_str("\n…[stderr truncated at the output limit]");
                    }
                }
                ToolOutput::text(result)
                    .with_title(format!("shell: {}", self.command))
                    .with_structured(json!({
                        "exit": code,
                        "truncated": clipped(out) || clipped(err),
                        "timeout": false,
                    }))
            }
            Ran::Cancelled => ToolOutput::text("Command cancelled."),
            Ran::Broken(error) => ToolOutput::text(format!("error: {error}")),
        }
    }
}

/// Hand the command to the background runtime and answer with its id.
///
/// The approval already happened above: starting a command in the background
/// and running it in the foreground are two ways of executing the same action,
/// so they are gated identically. What differs afterwards is only who waits.
async fn spawn_background(plan: CommandPlan, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
    let (Some(tasks), Some(turn_id)) = (ctx.background(), ctx.turn_id()) else {
        return Ok(ToolOutput::text(
            "This runtime cannot run a command in the background — nothing here \
             outlives the turn. Run it in the foreground instead (drop \
             `background`), raising `timeout` if it is slow.",
        ));
    };
    let label = plan.command.clone();
    let spec = TaskSpec {
        kind: TaskKind::Shell,
        label: label.clone(),
    };
    let work = Box::pin(async move {
        let outcome = plan.run(None).await;
        let full = plan.render(&outcome).text;
        TaskReport {
            outcome: match &outcome {
                Ran::Exited { code: Some(0), .. } => ToolOutcome::Succeeded,
                // Killed at its own deadline: the command may well have done its
                // work before the clock ran out, and nothing here can tell. The
                // model has to hear that rather than "it failed".
                Ran::TimedOut => ToolOutcome::Uncertain,
                _ => ToolOutcome::Failed,
            },
            summary: head_of(&full, BACKGROUND_SUMMARY_BYTES),
            full,
        }
    });
    match tasks
        .spawn(&ctx.session.session_id, turn_id, spec, work)
        .await
    {
        Ok(task_id) => Ok(ToolOutput::text(format!(
            "Started `{label}` in the background as task {task_id}. This turn does not wait \
             for it: finish what you are doing and answer. You will be told the result in \
             this conversation when it settles — or stop and wait for it now with \
             `wait` and `for_task: {task_id}`."
        ))
        .with_title(format!("shell (background): {label}"))
        .with_structured(json!({ "task_id": task_id, "background": true }))),
        Err(error) => Ok(ToolOutput::text(error.to_string())),
    }
}

/// How much of a background command's output rides in the wake that reports it.
/// The rest stays in the store, named by `result_ref`.
const BACKGROUND_SUMMARY_BYTES: usize = 2_000;

/// The first `budget` bytes, cut on a char boundary, saying so when it cut.
fn head_of(text: &str, budget: usize) -> String {
    if text.len() <= budget {
        return text.to_string();
    }
    let mut at = budget;
    while at > 0 && !text.is_char_boundary(at) {
        at -= 1;
    }
    format!("{}\n…[the rest is in the stored output]", &text[..at])
}

fn kill_group(pgid: Option<i32>) {
    #[cfg(unix)]
    if let Some(pgid) = pgid {
        // Safety: a plain syscall with a pid we spawned; an already-reaped group
        // just returns ESRCH, which is why the result is ignored.
        unsafe {
            libc::killpg(pgid, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = pgid;
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::{Approver, Decision, Risk};
    use komo_core::domain::context::{SessionContext, ToolContext};
    use std::sync::Mutex;

    fn ctx_with(approver: Arc<dyn Approver>) -> ToolContext {
        ToolContext::new(SessionContext::detached("cli:test"), None, approver)
    }

    struct AlwaysApprove;
    #[async_trait::async_trait]
    impl Approver for AlwaysApprove {
        async fn decide(&self, _request: &ApprovalRequest) -> Decision {
            Decision::Allow
        }
    }

    struct AlwaysReject;
    #[async_trait::async_trait]
    impl Approver for AlwaysReject {
        async fn decide(&self, _request: &ApprovalRequest) -> Decision {
            Decision::deny()
        }
    }

    /// Refuses, but explains why (`/deny <理由>` / the TUI's reason prompt).
    struct RejectWithReason(&'static str);
    #[async_trait::async_trait]
    impl Approver for RejectWithReason {
        async fn decide(&self, _request: &ApprovalRequest) -> Decision {
            Decision::deny_because(self.0)
        }
    }

    /// Records the risk level of the last request it saw.
    struct Recording {
        risk: Mutex<Option<Risk>>,
        approve: bool,
    }

    #[async_trait::async_trait]
    impl Approver for Recording {
        async fn decide(&self, request: &ApprovalRequest) -> Decision {
            *self.risk.lock().unwrap() = Some(request.risk);
            self.approve.into()
        }
    }

    fn workspace() -> Arc<Workspace> {
        Arc::new(Workspace::new(vec![std::env::temp_dir()]))
    }

    /// A fresh directory under the workspace root, for the workdir/orphan tests.
    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("komo_shell_{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn exit_status_is_also_reported_structurally() {
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "exit 3" }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert_eq!(out.structured["exit"], 3);
        assert_eq!(out.structured["timeout"], false);
        assert_eq!(out.structured["truncated"], false);
    }

    #[tokio::test]
    async fn a_slow_command_times_out_and_says_how_to_retry() {
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "sleep 30", "timeout": 200 }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert_eq!(out.structured["timeout"], true);
        assert!(out.text.contains("timed out"), "{}", out.text);
        // The model needs to know the knob exists, or it will just retry as-is.
        assert!(out.text.contains("timeout"), "{}", out.text);
    }

    /// The bug `process_group` + `killpg` fixes: killing `sh` alone leaves the
    /// processes it started running. Here a backgrounded child would create a
    /// marker file one second in — it must never get the chance.
    /// A timeout must end the whole tree, not just `sh` — a killed shell that
    /// leaves a build (or a `sleep`) running is the failure this tool's process
    /// group exists to prevent.
    ///
    /// Deliberately not "wait a while and check a file did not appear": that
    /// races the orphan's own clock against a loaded machine's scheduler, and
    /// loses on a busy CI box. The orphan announces its pid instead, and the
    /// test polls until that pid is gone — an answer about the process itself,
    /// not about who won a stopwatch.
    #[tokio::test]
    async fn a_timeout_kills_processes_the_command_started() {
        let dir = scratch("orphan");
        let pid_file = dir.join("orphan.pid");
        let tool = ShellTool::new(workspace());
        // The orphan outlives the tool's budget by two orders of magnitude, so
        // "still running" can only mean it was never killed.
        // `$!`, not `$$`: in POSIX sh a subshell's `$$` is still the *parent*
        // shell's pid, which `kill_on_drop` reaps anyway — the test would then
        // pass without the process group doing anything.
        let command = format!("sleep 30 & echo $! > {}; sleep 30", pid_file.display());

        let out = tool
            .call(
                json!({ "command": command, "timeout": 200 }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert_eq!(out.structured["timeout"], true);

        let Some(pid) = read_pid(&pid_file).await else {
            // Killed before it could even name itself — the outcome under test,
            // reached sooner. Nothing left to check.
            return;
        };
        assert!(
            wait_until_gone(pid).await,
            "a process started by the command survived the timeout (pid {pid})"
        );
    }

    /// The orphan's pid once it has written one, or `None` if it never does.
    async fn read_pid(path: &std::path::Path) -> Option<i32> {
        for _ in 0..40 {
            if let Ok(text) = std::fs::read_to_string(path)
                && let Ok(pid) = text.trim().parse::<i32>()
            {
                return Some(pid);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        None
    }

    /// Poll `kill(pid, 0)` until the process is gone. `true` if it went.
    async fn wait_until_gone(pid: i32) -> bool {
        for _ in 0..40 {
            // Safety: signal 0 performs the permission/existence check only and
            // sends nothing.
            if unsafe { libc::kill(pid, 0) } != 0 {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        false
    }

    /// A [`CancelSignal`] that fires after a delay — a stand-in for the user
    /// hitting stop mid-command.
    struct CancelAfter(std::time::Duration);
    #[async_trait::async_trait]
    impl komo_core::domain::cancel::CancelSignal for CancelAfter {
        fn is_cancelled(&self) -> bool {
            false
        }
        async fn cancelled(&self) {
            tokio::time::sleep(self.0).await;
        }
    }

    /// What 14 is actually for: cancelling a turn ends the *command*, not just
    /// komo's wait for it. Same orphan-marker shape as the timeout test — the
    /// backgrounded child must never get to write.
    #[tokio::test]
    async fn cancelling_the_turn_kills_the_command_and_its_children() {
        let dir = scratch("cancel");
        let marker = dir.join("alive.txt");
        let tool = ShellTool::new(workspace());
        // Tighter than the timeout test's timings on purpose: this test holds a
        // runtime while it sleeps, and the whole suite runs concurrently.
        let command = format!(
            "(sleep 0.5; echo alive > {}) & sleep 30",
            marker.to_string_lossy()
        );
        let session = SessionContext::detached("cli:test")
            .with_cancel(Arc::new(CancelAfter(std::time::Duration::from_millis(100))));
        let ctx = ToolContext::new(session, None, Arc::new(AlwaysApprove));

        let started = std::time::Instant::now();
        // A generous `timeout` so the command's own budget can't be what ends it.
        let err = tool
            .call(json!({ "command": command, "timeout": 60_000 }), &ctx)
            .await
            .unwrap_err();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "cancel should land promptly, took {:?}",
            started.elapsed()
        );
        // The ledger wording matches the run's own cancellation, so a step and
        // its run don't describe the same stop two different ways.
        assert_eq!(
            err.to_string(),
            komo_core::domain::cancel::CANCELLED_ERROR,
            "{err}"
        );

        tokio::time::sleep(std::time::Duration::from_millis(900)).await;
        assert!(
            !marker.exists(),
            "a process started by the command survived cancellation"
        );
    }

    /// Without a cancel signal (sweeps, cron, aux) the new `select!` arm must be
    /// inert — a turn nobody can interrupt behaves exactly as before.
    #[tokio::test]
    async fn a_turn_with_no_cancel_signal_runs_normally() {
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "echo hi" }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert!(out.text.contains("hi"), "{}", out.text);
    }

    #[tokio::test]
    async fn workdir_runs_the_command_there() {
        let dir = scratch("workdir");
        std::fs::write(dir.join("marker.txt"), "x").unwrap();
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "ls", "workdir": dir.to_string_lossy() }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert!(out.text.contains("marker.txt"), "{}", out.text);
    }

    #[tokio::test]
    async fn a_workdir_outside_the_workspace_is_denied() {
        let tool = ShellTool::new(Arc::new(Workspace::new(vec![std::path::PathBuf::from(
            "/home/user/project",
        )])));
        let err = tool
            .call(
                json!({ "command": "ls", "workdir": "/etc" }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)));
    }

    #[tokio::test]
    async fn a_workdir_that_is_not_a_directory_is_invalid_input() {
        let dir = scratch("notadir");
        let file = dir.join("f.txt");
        std::fs::write(&file, "x").unwrap();
        let tool = ShellTool::new(workspace());
        let err = tool
            .call(
                json!({ "command": "ls", "workdir": file.to_string_lossy() }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
    }

    /// A model asking for an hour gets the ceiling, not an error.
    #[tokio::test]
    async fn an_over_large_timeout_is_clamped_not_refused() {
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "true", "timeout": 60 * 60 * 1000 }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert_eq!(out.structured["exit"], 0);
    }

    #[tokio::test]
    async fn approved_command_runs() {
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "echo hello" }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert!(out.text.contains("hello"));
        assert!(out.text.contains("exit status: 0"));
    }

    #[tokio::test]
    async fn rejected_command_does_not_run() {
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "rm -r should_not_appear" }),
                &ctx_with(Arc::new(AlwaysReject)),
            )
            .await
            .unwrap();
        assert!(out.text.contains("rejected"));
        assert!(!out.text.contains("--- stdout ---"));
    }

    #[tokio::test]
    async fn a_denial_reason_is_relayed_to_the_model() {
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "rm -f /tmp/komo_shell_reason" }),
                &ctx_with(Arc::new(RejectWithReason("用 trash 代替 rm"))),
            )
            .await
            .unwrap();
        assert!(out.text.contains("用 trash 代替 rm"), "got: {}", out.text);
        // And it must be told not to just try again.
        assert!(out.text.contains("instead of retrying"));
        assert!(!out.text.contains("--- stdout ---"), "nothing ran");
    }

    #[tokio::test]
    async fn hardline_command_is_refused_without_consulting_approver() {
        let rec = Arc::new(Recording {
            risk: Mutex::new(None),
            approve: true,
        });
        let tool = ShellTool::new(workspace());
        let out = tool
            .call(
                json!({ "command": "sudo rm -rf / --no-preserve-root" }),
                &ctx_with(rec.clone()),
            )
            .await
            .unwrap();
        assert!(out.text.contains("refused"));
        // The approver was never asked: hardline sits above the approval gate.
        assert_eq!(*rec.risk.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn dangerous_commands_are_flagged() {
        for cmd in ["rm -rf foo", "git push origin main"] {
            let rec = Arc::new(Recording {
                risk: Mutex::new(None),
                approve: false,
            });
            let tool = ShellTool::new(workspace());
            let _ = tool
                .call(json!({ "command": cmd }), &ctx_with(rec.clone()))
                .await;
            assert_eq!(
                *rec.risk.lock().unwrap(),
                Some(Risk::Dangerous),
                "cmd: {cmd}"
            );
        }
    }

    #[test]
    fn dangerous_pattern_matches_at_command_boundary() {
        // Real dangerous commands still match.
        assert_eq!(dangerous_pattern("rm -rf foo"), Some("rm "));
        assert_eq!(dangerous_pattern("git push origin main"), Some("git push"));
        // ...including when chained after a shell separator.
        assert_eq!(dangerous_pattern("cd /tmp && rm -rf x"), Some("rm "));

        // `kill -TERM 1` is dangerous because of `kill `, NOT a stray `rm ` buried
        // in "-te*rm* 1" (the bug that mislabeled the prompt as `rm`).
        assert_eq!(dangerous_pattern("kill -TERM 1"), Some("kill "));

        // Innocuous commands that merely contain a pattern as a substring inside
        // a word must not be flagged.
        assert_eq!(dangerous_pattern("terraform apply"), None);
        assert_eq!(dangerous_pattern("echo perform task"), None);
    }

    #[test]
    fn redact_secrets_scrubs_common_shapes() {
        let cmd = "curl -H 'Authorization: Bearer sk-abc123def456ghi789' https://api.example.com";
        let r = redact_secrets(cmd);
        assert!(!r.contains("sk-abc123def456ghi789"));
        assert!(r.contains("Bearer"));

        let kv = redact_secrets("deploy --env api_key=AKIA1234567890SECRET token=zzz");
        assert!(!kv.contains("AKIA1234567890SECRET"));
        assert!(kv.contains("api_key=***"));

        let flag = redact_secrets("login --password hunter2longenoughxx");
        assert!(!flag.contains("hunter2longenoughxx"));
        assert!(flag.contains("--password ***"));

        let entropy = redact_secrets("echo ABCD1234efgh5678ijkl9012mnop");
        assert!(entropy.contains("***"));

        // Ordinary commands pass through untouched.
        assert_eq!(redact_secrets("ls -la /tmp"), "ls -la /tmp");
    }

    #[test]
    fn redact_args_scrubs_command_value() {
        let tool = ShellTool::new(workspace());
        let args = json!({ "command": "x token=supersecretvalue123456" }).to_string();
        let redacted = tool.redact_args(&args);
        assert!(!redacted.contains("supersecretvalue123456"));
    }

    #[tokio::test]
    async fn output_is_bounded_at_the_stream_limit() {
        // A command that emits more than the stream cap must be truncated, not
        // buffered whole into memory.
        let tool = ShellTool::new(workspace());
        let bytes = MAX_STREAM_BYTES + 50_000;
        let out = tool
            .call(
                json!({ "command": format!("yes a | head -c {bytes}") }),
                &ctx_with(Arc::new(AlwaysApprove)),
            )
            .await
            .unwrap();
        assert!(
            out.text.contains("stdout truncated"),
            "expected a truncation marker, got {} bytes",
            out.text.len()
        );
    }

    #[tokio::test]
    async fn command_reading_stdin_sees_eof_instead_of_hanging() {
        // stdin is wired to /dev/null, so a command that reads stdin gets EOF
        // and exits promptly rather than blocking forever waiting for input.
        let tool = ShellTool::new(workspace());
        let out = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tool.call(
                json!({ "command": "cat" }),
                &ctx_with(Arc::new(AlwaysApprove)),
            ),
        )
        .await
        .expect("cat must not hang on stdin")
        .unwrap();
        assert!(out.text.contains("exit status: 0"));
    }

    #[tokio::test]
    async fn safe_commands_are_safe_risk() {
        let rec = Arc::new(Recording {
            risk: Mutex::new(None),
            approve: true,
        });
        let tool = ShellTool::new(workspace());
        let _ = tool
            .call(json!({ "command": "echo hi" }), &ctx_with(rec.clone()))
            .await;
        assert_eq!(*rec.risk.lock().unwrap(), Some(Risk::Safe));
    }
}
