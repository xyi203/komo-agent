//! The run ledger — an execution/audit record of every agent turn
//! (docs/personal-agent-roadmap.md §7). One [`Run`] per user turn, with one
//! [`RunStep`] per tool invocation (captured at the single choke point every
//! tool call funnels through, the tool executor (`services::tool_execution`)).
//!
//! Runs are execution state bound to a session, so they live in `state.db`
//! (disposable dev state) alongside sessions/messages — not in the durable
//! kanban/memory files. Every ledger write is best-effort: it must never fail a
//! turn or a tool call (same contract as memory `mark_used`).
//!
//! **These are rows, not facts.** Every field here is folded out of the
//! session event log by `domain::run_projection` and committed through
//! `RunProjectionStore`; nothing writes a run or a step directly any more,
//! because two authoritative records of one turn disagreed after exactly the
//! crash they were meant to survive. What is left on the row is what the log
//! cannot state: the `outcome` a later turn revised, the `learned` watermark,
//! and the reconciler's ruling on whether an open turn is running or dead.
//!
//! `recoverable` marks the resumable set (§6): a turn with no terminal event
//! that no continuation has claimed. The claim is the continuation's own
//! `turn/started{resumed_from}`, so `komo run resume` is at-most-once by seq
//! assignment rather than by a row update racing another reader.

use async_trait::async_trait;

/// Verbatim caps so a row can't grow unbounded. `input`/`final_output` may be a
/// whole message; tool args/results are usually smaller but a `file`/`shell`
/// payload can be large.
pub const RUN_FIELD_CAP: usize = 4000;
pub const STEP_FIELD_CAP: usize = 2000;

/// Error stamped on a run reconciled at startup. A run left in `Running` is the
/// residue of a process that died mid-turn (a run is `Running` only while in
/// flight), so on the next start it is flipped to `Failed` with this reason.
pub const INTERRUPTED_ERROR: &str = "interrupted (process restarted)";

/// Truncate `s` to at most `cap` chars (char-boundary safe), appending an
/// ellipsis marker when cut so the reader knows the row is not the whole story.
pub fn truncate(s: &str, cap: usize) -> String {
    if s.chars().count() <= cap {
        return s.to_string();
    }
    let mut out: String = s.chars().take(cap).collect();
    out.push_str(" …[truncated]");
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// The turn is in flight (set at start; an in-flight crash leaves it here).
    Running,
    /// The turn stopped to wait for something outside itself — an approval, an
    /// answer, a timer, a job it started — and gave up its session slot.
    ///
    /// Not `Running`, because nothing is executing and a restart must not
    /// reconcile it as crash residue; not terminal, because it is coming back.
    /// What it is waiting for is the `turn/suspended` event's `wakeup`.
    Suspended,
    Done,
    Failed,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    /// Whether the turn has stopped for good. A running or suspended turn has
    /// no decided outcome — it is not an episode, and it is not residue.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }
}

pub fn parse_run_status(s: &str) -> anyhow::Result<RunStatus> {
    match s {
        "running" => Ok(RunStatus::Running),
        "suspended" => Ok(RunStatus::Suspended),
        "done" => Ok(RunStatus::Done),
        "failed" => Ok(RunStatus::Failed),
        other => Err(anyhow::anyhow!(
            "unknown run status `{other}` (expected running/done/failed)"
        )),
    }
}

/// One agent turn: the user input, a short outcome summary, the final reply,
/// and the status. Steps (tool calls) hang off it by `run_id`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Run {
    pub id: String,
    pub session_id: String,
    /// The user message that started the turn (truncated to [`RUN_FIELD_CAP`]).
    pub input: String,
    /// Post-turn summary: "respond" (no tools) or "<n> tool call(s)". The LLM
    /// owns tool dispatch, so this is derived from the recorded step count, not
    /// a planner decision.
    pub plan: String,
    pub status: RunStatus,
    /// The assistant reply (truncated). Empty until the turn finishes / on failure.
    pub final_output: String,
    /// Failure reason. Empty unless `status == Failed`.
    pub error: String,
    /// The run was interrupted mid-flight (process died) and can be resumed:
    /// no terminal event, and no continuation has claimed it. Only interruption
    /// produces a resumable run — an ordinary `Failed` has no half-done steps
    /// worth handing over.
    #[serde(default)]
    pub recoverable: bool,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    /// Tokens the turn's model round-trips spent, summed across rounds. `0` reads
    /// as *unknown* — a provider that reports no usage, a row written before the
    /// columns existed, or a turn that failed before its first completion — never
    /// as "this turn was free". `default` for the same
    /// forward/backward-compatibility reason as [`RunStep::elapsed_ms`].
    #[serde(default)]
    pub tokens_in: i64,
    #[serde(default)]
    pub tokens_out: i64,
    /// The part of `tokens_in` the provider served from its prefix cache — a
    /// subset, so `tokens_cached / tokens_in` is the turn's cache hit rate.
    /// Recorded because it is the only way to tell a prompt-assembly change
    /// that broke prefix stability from one that didn't: the token count barely
    /// moves either way, the hit rate collapses.
    #[serde(default)]
    pub tokens_cached: i64,
    /// Set on a run that continues an interrupted one from its turn journal —
    /// the audit link from the continuation back to the run whose context it
    /// picked up. `None` for ordinary turns and digest-primed resumes (those
    /// are fresh turns, not continuations).
    #[serde(default)]
    pub resumed_from: Option<String>,
    /// The memories that reached this turn's prompt. Empty for a turn with no
    /// enricher, and for rows written before the column.
    #[serde(default)]
    pub memories: RecalledMemories,
    /// The learning pass has consumed this run (extracted from it, or decided
    /// there was nothing to extract). The watermark is per-run rather than a
    /// per-session turn count because learning reads *episodes*: a count cannot
    /// say which turns were the new ones, only how many, and a rotated or
    /// repaired transcript makes the two disagree.
    ///
    /// Set only after a learning pass succeeds — a failed pass leaves it false
    /// so the next sweep retries the run. Runs the pass deliberately skips
    /// (cancelled turns, sweep sessions) are marked too: "considered and
    /// declined" and "not yet considered" have to be different states, or every
    /// sweep re-examines them forever.
    #[serde(default)]
    pub learned: bool,
    /// This turn's [`OutcomeAssessment`](super::episode::OutcomeAssessment) as
    /// JSON, or empty for a run never assessed (one written before the column,
    /// or one still in flight).
    ///
    /// Persisted rather than recomputed because it is **revisable**: most work
    /// is confirmed or refuted by what the user says *next*, which is a fact
    /// that does not exist when the turn ends. The deterministic reading is
    /// written when the run finishes, and later evidence is appended to it.
    #[serde(default)]
    pub outcome: String,
}

impl Run {
    /// A fresh turn id: the `turn_id` its `turn/started` event carries, and the
    /// id of the row projected from it. One id in two representations, so a
    /// ledger row and the events it folds from can never drift apart.
    pub fn new_id() -> String {
        format!("run-{}", uuid::Uuid::now_v7())
    }

    /// Open a new run for `session_id`, started now.
    pub fn start(session_id: &str, input: &str) -> Self {
        Self {
            id: Self::new_id(),
            session_id: session_id.to_string(),
            input: truncate(input, RUN_FIELD_CAP),
            plan: String::new(),
            status: RunStatus::Running,
            final_output: String::new(),
            error: String::new(),
            recoverable: false,
            started_at: time::OffsetDateTime::now_utc().unix_timestamp(),
            ended_at: None,
            tokens_in: 0,
            tokens_out: 0,
            tokens_cached: 0,
            resumed_from: None,
            memories: RecalledMemories::default(),
            learned: false,
            outcome: String::new(),
        }
    }
}

/// Which stored memories shaped a turn, by id and tier.
///
/// `recall_count` on the memory says a memory keeps being useful; this says
/// *where* it was used. The two answer different questions, and only this one
/// lets an operator work back from an answer they disagree with to the memory
/// that produced it — or forward from a memory they just corrected to the turns
/// it had already influenced.
///
/// Ids, not text: the memory store is the authority on content, and copying it
/// here would let the ledger drift from what the memory now says.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecalledMemories {
    /// Injected unconditionally (L1). Recorded per run because the pinned set
    /// changes over time, so "which were pinned *then*" is not derivable later.
    #[serde(default)]
    pub pinned: Vec<String>,
    /// Retrieved for this turn's question (L3) — the query-driven half, and the
    /// one that differs turn to turn.
    #[serde(default)]
    pub recall: Vec<String>,
}

impl RecalledMemories {
    pub fn is_empty(&self) -> bool {
        self.pinned.is_empty() && self.recall.is_empty()
    }
}

/// One occasion a stored memory reached a turn's prompt.
///
/// The reverse of [`Run::memories`], and the direction that actually gets
/// asked: "I just corrected this memory — which answers did it already shape?"
/// Kept as its own thin row rather than answered by scanning runs, because a
/// `Run` carries up to two 4000-char fields and reading thousands of them to
/// look at one JSON column is the wrong shape of query.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryUse {
    pub memory_id: String,
    pub run_id: String,
    pub session_id: String,
    /// Injected unconditionally (L1) rather than retrieved for this question.
    pub pinned: bool,
    pub started_at: i64,
}

/// One tool invocation within a run. `args`/`result` are stored verbatim
/// (truncated), except that each tool may redact its own args before they reach
/// the ledger (see [`crate::domain::tool::Tool::redact_args`]) — `shell` scrubs
/// secret-looking substrings, `file` drops write bodies.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunStep {
    pub run_id: String,
    /// Monotonic order within the run (assigned by the run's shared counter).
    pub seq: i64,
    pub tool_name: String,
    /// Redacted + truncated JSON args the model passed.
    pub args: String,
    /// Truncated result. Empty on failure.
    pub result: String,
    /// Tool error. Empty unless `!ok`.
    pub error: String,
    pub ok: bool,
    /// The call did not confirm its result — a wall-clock abort, or an
    /// ambiguous transport failure on a tool that is not idempotent. `!ok` and
    /// `uncertain` together mean **it may still have taken effect**, which is a
    /// different answer to "did that go through?" than a plain failure.
    ///
    /// `default`: rows written before the column, and an older gateway's
    /// `/api/runs/{id}` answering a newer CLI, both read as `false` — which is
    /// what they meant.
    #[serde(default)]
    pub uncertain: bool,
    pub started_at: i64,
    pub ended_at: i64,
    /// Measured duration off a monotonic clock. `started_at`/`ended_at` are
    /// whole seconds, so differencing them reports 0 for any sub-second call —
    /// this is the field a duration is rendered from.
    ///
    /// `default`: an operator CLI deserializes steps straight off a running
    /// gateway's `/api/runs/{id}`, and `komo upgrade` reinstalls the binary
    /// before restarting the gateway — so a new CLI against a not-yet-restarted
    /// old gateway must not fail on a field that release did not send. 0 already
    /// means "unknown" for pre-column rows, so the two cases coincide.
    #[serde(default)]
    pub elapsed_ms: i64,
    /// The tool's machine-readable third view (`ToolOutput::structured`) — the
    /// `shell` exit code, an `edit`'s diff stats. The model never paid tokens for
    /// it; it exists for the operator (`run inspect`) and the UI.
    ///
    /// `Null` means "this tool has no structured view" **or** "recorded before
    /// the column existed" — an absence either way, never an empty object. Over
    /// [`STEP_FIELD_CAP`] it is replaced by a marker rather than cut, because
    /// half a JSON document is worse than none.
    #[serde(default)]
    pub structured: serde_json::Value,
    /// Files holding this call's full output, when the result was too large to
    /// hand the model whole (`services::tool_output_store`). Empty is the common
    /// case; this is the operator's way back to what the preview elided.
    #[serde(default)]
    pub output_paths: Vec<String>,
    /// Which rung of the permission ladder let this call happen, projected from
    /// its `approval/resolved` event (`domain::approval`'s `DECIDED_BY_*`).
    /// Empty for a call that was never gated — most of them.
    ///
    /// "Why did this go through?" is asked long after the fact, and `ok` cannot
    /// answer it: a config rule, a grant saved months ago, the auto-reviewer and
    /// a person at a prompt all leave the same successful step behind.
    #[serde(default)]
    pub approved_by: String,
    /// How long the approval waited before it was answered, in milliseconds.
    /// `0` = never gated, or answered instantly. "It was allowed" and "someone
    /// thought about it for four minutes and then allowed it" are different
    /// facts about the same call.
    #[serde(default)]
    pub approval_waited_ms: i64,
}

/// Per-field cap on a step's args/result inside the resume digest, and the
/// budget for the digest as a whole (a turn can have up to 100 steps — past
/// the budget the rest is elided, `run inspect` still has everything).
const RESUME_SNIPPET_CAP: usize = 200;
const RESUME_DIGEST_CAP: usize = 8000;

/// Compose the priming input for resuming an interrupted run: the original
/// request plus a digest of the tool calls that had completed, so the model can
/// judge which side effects already took hold and continue rather than restart.
///
/// The ledger is an audit record, not a checkpoint — intermediate assistant
/// turns are never persisted and step args are redacted/truncated — so resume
/// re-dispatches a *fresh* turn primed with this digest instead of pretending
/// to replay the loop from mid-flight. The result is a normal user message in
/// the session transcript, visible as such.
pub fn resume_prompt(run: &Run, steps: &[RunStep]) -> String {
    // Collapse newlines so each step stays one digest line.
    let snip = |s: &str| truncate(&s.replace('\n', " "), RESUME_SNIPPET_CAP);

    let mut out = format!(
        "[resume {id}] The previous attempt at this task was interrupted (the \
         process restarted mid-turn). Original request:\n\n{input}\n\n",
        id = run.id,
        input = run.input,
    );
    if steps.is_empty() {
        out.push_str("No tool calls had completed before the interruption.\n");
    } else {
        out.push_str(&format!(
            "Before the interruption, {} tool call(s) had already completed:\n",
            steps.len()
        ));
        for (idx, s) in steps.iter().enumerate() {
            if out.len() > RESUME_DIGEST_CAP {
                out.push_str(&format!(
                    "…and {} more step(s), elided for length (full record: \
                     `komo run inspect {}`).\n",
                    steps.len() - idx,
                    run.id
                ));
                break;
            }
            let outcome = if s.ok {
                snip(&s.result)
            } else {
                format!("error: {}", snip(&s.error))
            };
            out.push_str(&format!(
                "{}. {} {} → {}\n",
                idx + 1,
                s.tool_name,
                snip(&s.args),
                outcome
            ));
        }
    }
    out.push_str(
        "\nReview what already took effect, then continue the task from where \
         it stopped. Do not re-apply side effects that already succeeded — \
         verify first when unsure. Reply with the completed outcome.",
    );
    out
}

/// Caps for [`tool_digest`]. Tighter than the resume digest's: a resume prompt is
/// written once for one turn, whereas a tool note rides along in *every*
/// subsequent turn's history until it ages out of the window — so its per-line
/// and total budgets are what keep cross-turn continuity from becoming a context
/// leak.
const TOOL_NOTE_SNIPPET_CAP: usize = 160;
const TOOL_NOTE_CAP: usize = 1500;

/// Opens the note. The tags and the disclaimer are not decoration: the note is
/// replayed into later turns' history, so whatever shape it has is a worked
/// example the model can copy. It used to open with a bare
/// `[tools used in this turn]` and render *inside the assistant turn's own
/// text* — which taught at least one model (DeepSeek) that writing that block,
/// complete with invented commands and invented results, was a thing an
/// assistant does instead of calling a tool. The turn then reported a confident
/// answer with zero tool steps in the ledger. `llm::to_turns` moved the note out
/// of the assistant's text; naming who wrote it is the other half.
const TOOL_NOTE_HEADER: &str = "<previous_turn_tools>\n\
     System record of the tool calls komo ran for you last turn. komo wrote this, not you: \
     never emit this block, and never claim a tool ran or report its output without making a \
     real tool call.\n";
const TOOL_NOTE_FOOTER: &str = "</previous_turn_tools>\n";

/// Fold a finished turn's tool calls into a compact note carried into later
/// turns' history (attached to the turn's assistant message as
/// [`Message::tool_note`](crate::domain::message::Message::tool_note)).
///
/// Without this, a turn's tool activity dies with the turn: only user and
/// assistant *text* is persisted, so the next turn's model cannot tell whether a
/// file was read, a command ran, or where a stored over-limit output went — it
/// re-runs the tool (paying twice) or answers from nothing. The note is a
/// summary, not a replay: provider-native `tool_use`/`tool_result` messages are
/// not portable across the model menu, and the ledger's args are already
/// redacted and truncated, so reconstructing a faithful transcript is not on
/// offer. Naming what happened is.
///
/// Returns an empty string for a turn with no tool calls (the common case), so
/// callers can store it unconditionally.
pub fn tool_digest(steps: &[RunStep]) -> String {
    if steps.is_empty() {
        return String::new();
    }
    let snip = |s: &str| truncate(&s.replace('\n', " "), TOOL_NOTE_SNIPPET_CAP);

    let mut out = String::from(TOOL_NOTE_HEADER);
    for (idx, s) in steps.iter().enumerate() {
        if out.len() > TOOL_NOTE_CAP {
            out.push_str(&format!("…and {} more call(s).\n", steps.len() - idx));
            break;
        }
        let outcome = if s.ok {
            snip(&s.result)
        } else {
            format!("error: {}", snip(&s.error))
        };
        out.push_str(&format!(
            "{}. {} {} → {}\n",
            idx + 1,
            s.tool_name,
            snip(&s.args),
            outcome
        ));
        // The whole point of storing an over-limit output (`tool_output_store`)
        // is that the model can go back for the part the preview elided — which
        // it can only do if the path outlives the turn that produced it.
        for path in &s.output_paths {
            out.push_str(&format!("   full output kept at: {path}\n"));
        }
    }
    out.push_str(TOOL_NOTE_FOOTER);
    out
}

#[async_trait]
pub trait RunRepository: Send + Sync {
    /// Most-recent runs first, capped at `limit`.
    async fn list(&self, limit: usize) -> anyhow::Result<Vec<Run>>;
    /// Fetch a single run by id.
    async fn get(&self, id: &str) -> anyhow::Result<Option<Run>>;
    /// Steps for a run, ordered by `seq`.
    async fn steps(&self, run_id: &str) -> anyhow::Result<Vec<RunStep>>;
    /// Delete every run started before `cutoff` (unix seconds) and its steps.
    /// Returns the number of runs removed. The ledger accumulates like messages,
    /// so this is the operator's manual prune (roadmap §9) — no automatic policy.
    async fn prune(&self, cutoff: i64) -> anyhow::Result<usize>;

    /// Flip every run still `Running` to `Failed`/[`INTERRUPTED_ERROR`], stamping
    /// `ended_at = now` and `recoverable = true`; return how many were
    /// reconciled. Called once at process startup: a run is `Running` only while
    /// in flight, so any left over is the residue of a crashed earlier process —
    /// leaving it would make `run list` lie. The runs it marks are the set
    /// `resume` picks from (§6).
    ///
    /// The one ruling the log cannot make, which is why it stays a row update:
    /// "open" and "dead" look identical in an append-only record, and only a
    /// process that has just started knows nothing of its own is running.
    async fn reconcile_interrupted(&self, now: i64) -> anyhow::Result<usize>;

    /// The most recent steps of one tool across all runs (newest first, capped
    /// at `limit`). Backs derived audit views — e.g. which turns loaded a given
    /// skill (`steps_by_tool("skill", …)` + [`step_views_skill`]) — without
    /// adding usage fields to any model.
    async fn steps_by_tool(&self, tool_name: &str, limit: usize) -> anyhow::Result<Vec<RunStep>>;

    /// The turns a given memory reached the prompt of, newest first.
    async fn runs_using_memory(
        &self,
        memory_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<MemoryUse>>;

    /// Finished runs the learning pass has not consumed yet ([`Run::learned`]),
    /// **oldest first** — learning replays a session in the order it happened,
    /// so a later correction lands after the claim it corrects.
    ///
    /// `session_id` scopes the query to one conversation (the after-turn
    /// trigger); `None` scans every session (the sweep). Runs still `Running`
    /// are never returned: an unfinished turn is not an episode.
    async fn unlearned(&self, session_id: Option<&str>, limit: usize) -> anyhow::Result<Vec<Run>>;

    /// Mark runs as consumed by the learning pass. Best-effort like every other
    /// ledger write: a failure just means those runs are offered again.
    async fn mark_learned(&self, run_ids: &[String]) -> anyhow::Result<()>;

    /// Store a run's outcome assessment (serialized). Overwrites: an
    /// assessment is a *reading* of the evidence, and later evidence produces a
    /// new reading rather than a second one.
    async fn set_outcome(&self, run_id: &str, outcome: &str) -> anyhow::Result<()>;

    /// The run immediately before `run_id` in the same session, if any — whose
    /// work the user's next message is most likely commenting on.
    async fn previous_in_session(&self, run_id: &str) -> anyhow::Result<Option<Run>>;
}

/// Whether a ledger step is the `skill` tool loading `skill_name`'s
/// instructions (`action=view`). The skill-invocation audit is *derived* from
/// the ledger — a skill "used" is exactly a skill viewed; no usage counters are
/// stored anywhere (roadmap §9 / "no dead fields").
pub fn step_views_skill(step: &RunStep, skill_name: &str) -> bool {
    skill_viewed(step).is_some_and(|name| name == skill_name)
}

/// The skill a ledger step loaded, or `None` when the step is not a `skill`
/// `view`. The aggregate usage report bucket-sorts on this, so it must stay the
/// single definition of "used" that [`step_views_skill`] also answers with.
pub fn skill_viewed(step: &RunStep) -> Option<String> {
    if step.tool_name != "skill" {
        return None;
    }
    let args = serde_json::from_str::<serde_json::Value>(&step.args).ok()?;
    if args.get("action").and_then(|v| v.as_str()) != Some("view") {
        return None;
    }
    args.get("name")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_keeps_short_strings_and_cuts_long_ones() {
        assert_eq!(truncate("hi", 10), "hi");
        let long = "x".repeat(50);
        let cut = truncate(&long, 10);
        assert!(cut.starts_with(&"x".repeat(10)));
        assert!(cut.contains("truncated"));
    }

    fn interrupted_run() -> Run {
        let mut run = Run::start("feishu:chat-1", "deploy the new build");
        run.status = RunStatus::Failed;
        run.error = INTERRUPTED_ERROR.to_string();
        run.recoverable = true;
        run
    }

    fn step(run: &Run, seq: i64, tool: &str, ok: bool) -> RunStep {
        RunStep {
            run_id: run.id.clone(),
            seq,
            tool_name: tool.to_string(),
            args: format!("{{\"n\":{seq}}}"),
            result: if ok { "done".into() } else { String::new() },
            error: if ok { String::new() } else { "boom".into() },
            ok,
            uncertain: false,
            started_at: 100 + seq,
            ended_at: 101 + seq,
            elapsed_ms: 10 + seq,
            structured: serde_json::Value::Null,
            output_paths: Vec::new(),
            approved_by: String::new(),
            approval_waited_ms: 0,
        }
    }

    #[test]
    fn resume_prompt_carries_input_and_step_digest() {
        let run = interrupted_run();
        let steps = vec![step(&run, 0, "shell", true), step(&run, 1, "file", false)];
        let prompt = resume_prompt(&run, &steps);

        assert!(prompt.contains(&run.id));
        assert!(prompt.contains("deploy the new build"));
        assert!(prompt.contains("2 tool call(s)"));
        assert!(prompt.contains("1. shell"));
        assert!(prompt.contains("2. file"));
        assert!(prompt.contains("error: boom"));
        assert!(prompt.contains("Do not re-apply side effects"));
    }

    #[test]
    fn resume_prompt_without_steps_says_so() {
        let run = interrupted_run();
        let prompt = resume_prompt(&run, &[]);
        assert!(prompt.contains("No tool calls had completed"));
    }

    #[test]
    fn resume_prompt_elides_past_the_digest_budget() {
        let run = interrupted_run();
        let steps: Vec<RunStep> = (0..100)
            .map(|seq| {
                let mut s = step(&run, seq, "web_fetch", true);
                s.result = "r".repeat(400); // each line lands near the snippet cap
                s
            })
            .collect();
        let prompt = resume_prompt(&run, &steps);
        assert!(prompt.contains("elided for length"));
        assert!(prompt.len() < RESUME_DIGEST_CAP + 2000);
    }

    #[test]
    fn tool_digest_names_each_call_and_its_outcome() {
        let run = interrupted_run();
        let steps = vec![step(&run, 0, "read", true), step(&run, 1, "shell", false)];
        let digest = tool_digest(&steps);

        assert!(digest.contains("1. read"));
        assert!(digest.contains("2. shell"));
        assert!(digest.contains("error: boom"));
        // Fenced and attributed, so a later turn cannot read it as something an
        // assistant writes (see TOOL_NOTE_HEADER).
        assert!(digest.starts_with("<previous_turn_tools>"), "{digest}");
        assert!(digest.ends_with("</previous_turn_tools>\n"), "{digest}");
        // One line per call: this rides in every later turn's context.
        let calls = digest
            .lines()
            .filter(|l| l.starts_with("1. ") || l.starts_with("2. "))
            .count();
        assert_eq!(calls, 2, "one line per call: {digest}");
    }

    /// The digest is what carries a stored over-limit output past the turn that
    /// produced it — without the path, `tool_output_store` keeps a file the model
    /// can no longer ask for.
    #[test]
    fn tool_digest_carries_stored_output_paths() {
        let run = interrupted_run();
        let mut s = step(&run, 0, "shell", true);
        s.output_paths = vec!["/tmp/komo/tool-output/s/run-1-0000.txt".to_string()];
        let digest = tool_digest(&[s]);
        assert!(
            digest.contains("/tmp/komo/tool-output/s/run-1-0000.txt"),
            "{digest}"
        );
    }

    #[test]
    fn a_turn_without_tools_has_no_digest() {
        assert!(tool_digest(&[]).is_empty());
    }

    #[test]
    fn tool_digest_elides_past_its_budget() {
        let run = interrupted_run();
        let steps: Vec<RunStep> = (0..100)
            .map(|seq| {
                let mut s = step(&run, seq, "web_fetch", true);
                s.result = "r".repeat(400);
                s
            })
            .collect();
        let digest = tool_digest(&steps);
        assert!(digest.contains("more call(s)"), "{digest}");
        assert!(digest.len() < TOOL_NOTE_CAP + 500);
    }

    #[test]
    fn step_views_skill_matches_only_view_steps_of_that_skill() {
        let run = interrupted_run();
        let mut s = step(&run, 0, "skill", true);
        s.args = r#"{"action":"view","name":"feishu-calendar"}"#.to_string();
        assert!(step_views_skill(&s, "feishu-calendar"));
        assert!(!step_views_skill(&s, "other-skill"));

        s.args = r#"{"action":"list"}"#.to_string();
        assert!(!step_views_skill(&s, "feishu-calendar"));

        let mut shell = step(&run, 1, "shell", true);
        shell.args = r#"{"action":"view","name":"feishu-calendar"}"#.to_string();
        assert!(!step_views_skill(&shell, "feishu-calendar"));

        s.args = "not json".to_string();
        assert!(!step_views_skill(&s, "feishu-calendar"));
    }

    #[test]
    fn status_roundtrips() {
        for s in [RunStatus::Running, RunStatus::Done, RunStatus::Failed] {
            assert_eq!(parse_run_status(s.as_str()).unwrap(), s);
        }
        assert!(parse_run_status("bogus").is_err());
    }
}
