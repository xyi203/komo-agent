use crate::compaction::Compactor;
use crate::learning_coordinator::{LearningCoordinator, LearningTrigger};
use komo_core::domain::{
    cancel::{CANCELLED_REPLY, CancelSignal, Cancelled, is_cancelled},
    checkpoint::CheckpointStore,
    events::{ToolEventSink, TurnEvent},
    hooks::{StepDecision, StepHook, TurnHook},
    llm::{DeltaSink, LlmClient, Step, TokenUsage, ToolOutcome},
    message::{Message, Role},
    repository::{MessageRepository, SessionEventRepository, SessionRepository},
    run::RecalledMemories,
    run::{Run, RunRepository, tool_digest, truncate},
    run_projection::{RunProjectionStore, project_runs, replay_floor},
    session::Session,
    session_event::{
        AssistantMessageEvent, MessageSource, SessionEvent, SessionEventKind, SurfacePlacement,
        TurnRecorder, TurnSuspendedEvent, UserMessageEvent, fold_turn_waits,
    },
    wakeup::{Suspended, WakeupRegistration, WakeupRepository, is_suspended},
};
/// A turn whose opening the log never confirmed: settle folds the whole log
/// rather than a tail it cannot locate. Always correct, only slower.
const UNKNOWN_START: u64 = u64::MAX;

/// How many of a session's runs retention asks the ledger about. The watermark
/// it needs is the *oldest* unlearned run, and the sweep retires runs in
/// batches, so a session with more pending than this has its floor pinned by
/// one of them regardless.
const RETENTION_LEDGER_SCAN: usize = 500;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;

use tracing::{Instrument, info, info_span, warn};

use komo_services::tool_execution::{
    RunContext, SessionContext, SpinDetector, ToolExecutor, ToolTurnContext, TurnResultBudget,
    current_session, with_session,
};

/// Fed back to the model in place of tool results once the per-turn round
/// budget (`max_turns`) is exceeded, so it answers instead of calling more
/// tools. The turn then terminates regardless of the model's next move.
const BUDGET_REACHED_NOTE: &str = "Tool-call budget for this turn reached; do not call any \
     more tools. Reply to the user now using what you already have.";

/// Sent to the user when the model ends a turn with no text at all (e.g. a final
/// round that is only tool calls the loop won't run, or an empty completion).
/// A chat channel rejects an empty message, so never hand one downstream.
const EMPTY_REPLY_FALLBACK: &str = "(我这次没能生成回复，请再说一次或换个说法。)";

/// Guard against handing an empty/whitespace-only reply to a channel (some
/// reject it outright); substitute a user-facing fallback.
fn non_empty(reply: String) -> String {
    if reply.trim().is_empty() {
        EMPTY_REPLY_FALLBACK.to_string()
    } else {
        reply
    }
}

pub struct AgentRuntime {
    pub llm: Arc<dyn LlmClient>,
    pub sessions: Arc<dyn SessionRepository>,
    pub messages: Arc<dyn MessageRepository>,
    /// The session's authoritative event log. Everything a turn does is
    /// appended here; `messages` is one projection of it.
    pub events: Arc<dyn SessionEventRepository>,
    /// Run ledger, for reading: the rows are a projection of the event log,
    /// so nothing here writes a turn or a step. See `domain/run.rs`, roadmap §7.
    pub runs: Arc<dyn RunRepository>,
    /// Where a turn's fold is committed as ledger rows — the one writer those
    /// tables have.
    pub projection: Arc<dyn RunProjectionStore>,
    /// Tool catalog the in-house loop dispatches against. komo (not rig) now
    /// owns the multi-step loop and hands each round of requested calls to the
    /// executor, which owns lookup/retry/ledger/cap. See `run_agent_loop`.
    pub tool_executor: ToolExecutor,
    /// Max tool-calling rounds per turn before the loop forces a final answer
    /// (config `max_turns`). The hard, loop-level budget — distinct from the
    /// executor's per-call fan-out cap.
    pub max_turns: usize,
    /// How many recent messages to load for the turn's agent loop (mirrors the
    /// LLM's `max_history_messages`; `0` = load the whole transcript). Keeps the
    /// per-turn hot path off a full-transcript read for long-lived chat
    /// sessions — the LLM windows again to the same bound, so this is loss-free.
    pub history_window: usize,
    /// Post-run learning goes through the shared coordinator (also driven by the
    /// gateway's scheduled sweep); `None` = this runtime never learns.
    pub learning: Option<Arc<LearningCoordinator>>,
    /// Summarises a conversation's oldest messages once the window has started
    /// dropping them. `None` = this runtime's long sessions keep the plain
    /// window, and what falls out of it is simply gone.
    pub compaction: Option<Arc<Compactor>>,
    /// Where a suspended turn's wait is registered, so a sweep comes back for
    /// it after this process is gone. `None` = nothing schedules a return, and
    /// only the startup re-check would find the turn.
    pub wakeups: Option<Arc<dyn WakeupRepository>>,
    /// Where a mutating tool leaves the bytes a file held before this turn
    /// touched it, so `komo run rollback` can put them back. `None` = this
    /// runtime's file changes are final.
    pub checkpoint: Option<Arc<dyn CheckpointStore>>,
    /// Turn lifecycle observers (see `domain::hooks`). Registered at wiring,
    /// awaited serially — a hook is a fast observer, never a worker. Empty for
    /// every runtime without plugins contributing one.
    pub turn_hooks: Vec<Arc<dyn TurnHook>>,
    /// Between-round hooks (see `domain::hooks`). They inject context the model
    /// is about to need, or stop a turn that has gone somewhere it should not.
    /// Empty for every runtime with no plugin contributing one.
    pub step_hooks: Vec<Arc<dyn StepHook>>,
}

/// Records one turn's events into its session's log, disarming itself after the
/// first failed write — recording buys resumability, and a broken store must
/// cost exactly that, not per-round latency and not the turn.
struct RunRecorder {
    events: Arc<dyn SessionEventRepository>,
    session_id: String,
    turn_id: String,
    broken: AtomicBool,
}

impl RunRecorder {
    fn new(events: Arc<dyn SessionEventRepository>, session_id: &str, turn_id: &str) -> Arc<Self> {
        Arc::new(Self {
            events,
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            broken: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl TurnRecorder for RunRecorder {
    fn turn_id(&self) -> &str {
        &self.turn_id
    }

    async fn record(&self, kinds: Vec<SessionEventKind>) {
        if self.broken.load(Ordering::Relaxed) {
            return;
        }
        if let Err(error) = self.events.append(&self.session_id, kinds).await {
            warn!(%error, turn_id = %self.turn_id,
                "turn event write failed; recording disabled for this turn");
            self.broken.store(true, Ordering::Relaxed);
        }
    }

    async fn durable(&self) {
        if self.broken.load(Ordering::Relaxed) {
            return;
        }
        if let Err(error) = self.events.durable_flush(&self.session_id).await {
            warn!(%error, turn_id = %self.turn_id, "turn events are not durable");
        }
    }
}

fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

impl AgentRuntime {
    pub async fn handle_input(
        &self,
        session_id: &str,
        user_input: String,
    ) -> anyhow::Result<String> {
        // Session-scoped tools (e.g. `todo`) read the turn's session from the
        // ambient context. The gateway dispatcher sets it (with a real reply
        // sink); the REPL calls us directly, so establish a detached context
        // here when none exists. Don't override an existing one — that would
        // drop the gateway's sink and break mid-turn approval.
        if current_session().is_none() {
            let ctx = SessionContext::detached(session_id);
            return with_session(ctx, self.run_turn(session_id, user_input)).await;
        }
        self.run_turn(session_id, user_input).await
    }

    /// Continue an interrupted run from its turn journal: rebuild the exact
    /// provider-level state the turn died with and drive the same agent loop
    /// forward — the tool rounds already paid for are replayed from the
    /// journal, not re-run. The continuation is its own ledger [`Run`], linked
    /// back through `resumed_from`.
    ///
    /// `Ok(None)` = this run *cannot* be continued (no journal store, no rows,
    /// or the transcript already ends in a reply); nothing was touched, and the
    /// caller falls back to the digest-primed fresh turn. `Err` = the
    /// continuation was attempted and genuinely failed.
    pub async fn resume_interrupted(&self, original: &Run) -> anyhow::Result<Option<String>> {
        let events = self.events.events(&original.session_id).await?;
        if !events
            .iter()
            .any(|e| e.turn_id_of_work() == Some(original.id.as_str()))
        {
            info!(run_id = %original.id, "the session log has nothing for this turn; falling back to digest resume");
            return Ok(None);
        }
        // A continuation appends an assistant reply with no new user message,
        // so the transcript must still end on the interrupted turn's user
        // message. Ending on anything else means the reply actually landed
        // (crash in the gap before the ledger closed), or the crash predated
        // the user message — either way a fresh turn is the right shape.
        // Checked here, before a ledger run is opened, so the refusal leaves
        // no failed-run residue; `turn_body` re-checks as a backstop.
        let ends_on_user = self
            .sessions
            .find_windowed(&original.session_id, self.history_window)
            .await?
            .and_then(|s| s.messages.last().map(|m| m.role == Role::User))
            .unwrap_or(false);
        if !ends_on_user {
            info!(run_id = %original.id,
                "transcript does not end on a user message; falling back to digest resume");
            return Ok(None);
        }

        let turn = self.run_ledgered(
            &original.session_id,
            Run::new_id(),
            TurnKind::Resume {
                events,
                turn_id: original.id.clone(),
            },
        );
        // Same ambient-context bridge as `handle_input`: session-scoped tools
        // and the approvers read the turn's session from the task-local.
        let reply = if current_session().is_none() {
            let ctx = SessionContext::detached(&original.session_id);
            with_session(ctx, turn).await?
        } else {
            turn.await?
        };
        Ok(Some(reply))
    }

    /// One turn = one id, carried by every event the turn appends and by the
    /// ledger row folded out of them. Runs the turn body under a `RunContext`
    /// (which orders the turn's tool steps) and a `run` tracing span, then
    /// settles the turn: project the ledger, then let the log do its upkeep.
    async fn run_turn(&self, session_id: &str, user_input: String) -> anyhow::Result<String> {
        self.run_ledgered(session_id, Run::new_id(), TurnKind::Fresh { user_input })
            .await
    }

    async fn run_ledgered(
        &self,
        session_id: &str,
        turn_id: String,
        kind: TurnKind,
    ) -> anyhow::Result<String> {
        let span = info_span!("run", run_id = %turn_id, session = %session_id);
        let ctx = RunContext::new(turn_id.clone()).with_checkpoint(self.checkpoint.clone());
        // Where this turn starts in the log, filled in as it opens. It is what
        // lets the settle below fold the turn's own tail instead of the whole
        // conversation; `UNKNOWN_START` means "read it all", which is always
        // correct and only slower.
        let opened_at = AtomicU64::new(UNKNOWN_START);

        let outcome = self
            .turn_body(session_id, kind, ctx, &opened_at)
            .instrument(span)
            .await;

        // The turn's own account of itself is the log's, not this function's:
        // every field the ledger row used to be assigned here — the reply, what
        // it cost, which memories shaped it, how it ended — is already an event
        // that `run_projection` folds. What is left is saying so out loud.
        let outcome = outcome.map(|(reply, _, _)| reply);
        match &outcome {
            Ok(_) => info!(run_id = %turn_id, "run done"),
            // Cancelled, not broken, and deliberately not resumable: there is
            // nothing to resume, the user asked it to stop.
            Err(error) if is_cancelled(error) => info!(run_id = %turn_id, "run cancelled"),
            Err(error) => warn!(run_id = %turn_id, %error, "run failed"),
        }
        self.settle_turn(session_id, opened_at.load(Ordering::Relaxed))
            .await;

        // Learning, detached from the reply path and dispatched **after** the
        // ledger closed. It reads this run back as an episode — status, steps
        // and all — so starting it from inside the turn would have it assemble a
        // run whose outcome had not been written yet. Whether the interval is
        // due, which episodes the extractor sees, and the watermark are all the
        // coordinator's knowledge; the runtime only reports that a run ended.
        if let Some(learning) = &self.learning {
            let learning = learning.clone();
            let run_id = turn_id.clone();
            tokio::spawn(async move {
                match learning.run(LearningTrigger::AfterRun { run_id }).await {
                    Ok(report) if !report.is_empty() => {
                        info!(?report, "self-improvement learning")
                    }
                    Ok(_) => {}
                    Err(error) => warn!(%error, "learning failed (non-fatal)"),
                }
            });
        }

        outcome
    }

    /// The turn's actual work: persist the user message (fresh turns), drive
    /// the agent loop (komo owns it — model round-trip, execute requested
    /// tools, feed results back, repeat), persist the reply, and kick off the
    /// periodic reviewer. A resumed turn differs only at the edges: its user
    /// message is already in the transcript, and its driver reopens mid-loop
    /// from the journal instead of starting fresh.
    async fn turn_body(
        &self,
        session_id: &str,
        kind: TurnKind,
        run: RunContext,
        opened_at: &AtomicU64,
    ) -> anyhow::Result<(String, TokenUsage, RecalledMemories)> {
        // Load only the recent window for the agent loop — the LLM windows the
        // history to the same bound anyway, so a long-lived chat session no
        // longer deserializes its whole transcript every turn. The reviewer
        // (below) still gets the full transcript, on the turns it actually runs.
        let mut session = match self
            .sessions
            .find_windowed(session_id, self.history_window)
            .await?
        {
            Some(s) => s,
            None => {
                // First turn on this id: the record inherits what is driving
                // the turn, so a sweep's session is marked one at creation.
                // That mark is what later decides how it is titled, whether the
                // session list shows it, and whether the learning pass may
                // extract from it — all of which used to be read off a prefix
                // in the id.
                let origin = current_session().map(|c| c.origin).unwrap_or_default();
                let s = Session::new(session_id).with_origin(origin);
                self.sessions.save(&s).await?;
                s
            }
        };

        let resume_entries = match kind {
            TurnKind::Fresh { user_input } => {
                let user_msg = Message::user(&user_input);
                let opening = self
                    .record(
                        session_id,
                        vec![
                            SessionEventKind::TurnStarted {
                                turn_id: run.run_id.clone(),
                                resumed_from: None,
                            },
                            SessionEventKind::UserMessage(UserMessageEvent {
                                turn_id: run.run_id.clone(),
                                content: user_input.clone(),
                                source: MessageSource::User,
                                surface: SurfacePlacement::append(),
                            }),
                        ],
                    )
                    .await;
                if let Some(first) = opening.first().map(|event| event.seq) {
                    opened_at.store(first, Ordering::Relaxed);
                }
                self.open_in_ledger(session_id, &opening).await;
                session.messages.push(user_msg);
                None
            }
            TurnKind::Resume { events, turn_id } => {
                // A continuation appends an assistant reply without a new user
                // message, so the transcript must still end on the interrupted
                // turn's user message. Ending on an assistant means the reply
                // actually landed (crash in the gap before the ledger closed) —
                // nothing to resume.
                anyhow::ensure!(
                    session.messages.last().map(|m| m.role == Role::User) == Some(true),
                    "transcript already ends in a reply — nothing to resume"
                );
                // A continuation is its own turn in the log too, linked back to
                // the one it picks up. Without this the log has no record that
                // the attempt happened at all — the interrupted turn's events
                // are all it would show.
                let opening = self
                    .record(
                        session_id,
                        vec![SessionEventKind::TurnStarted {
                            turn_id: run.run_id.clone(),
                            resumed_from: Some(turn_id.clone()),
                        }],
                    )
                    .await;
                // Deliberately not recorded: a continuation's settle has to
                // re-commit the turns it claimed, and those are earlier turns of
                // their own. A resume is rare; a full fold there is the cheap
                // way to keep the claim honest.
                self.open_in_ledger(session_id, &opening).await;
                // What the chain has waited for, and what ended the wait that
                // brought this attempt back. Read here, where the events are
                // already in hand, and carried on the run: the call that
                // stopped is about to be re-dispatched and has to recognise its
                // own wake instead of registering a second one.
                run.resumed_with(fold_turn_waits(&events, &turn_id));
                Some((events, turn_id))
            }
        };
        let is_fresh = resume_entries.is_none();

        // Lifecycle hooks: the loop is about to drive its first model round.
        for hook in &self.turn_hooks {
            hook.turn_started(session_id).await;
        }

        // Keep a handle on the run to read the tool-step count after the loop (the
        // counter is shared via `Arc`) and to fetch the steps themselves.
        let probe = run.clone();
        let TurnOutcome {
            reply,
            usage,
            memories,
            interjections,
        } = match self.run_agent_loop(&session, run, resume_entries).await {
            Ok(outcome) => outcome,
            // Stopped to wait for something outside itself. **Not** a failure
            // and not an answer: the turn gives up its session slot and comes
            // back when the wake arrives.
            //
            // Deliberately no assistant message — a suspended turn has not
            // answered, and the surface has to still end on the user message
            // for the continuation to be a continuation rather than a second
            // question. The prompt the user sees was delivered by whoever asked
            // for the approval, not by this transcript.
            Err(error) if is_suspended(&error) => {
                let pending = probe
                    .suspension()
                    .expect("a suspended turn carries what it is waiting for");
                let expires_at = pending.expires_at.or_else(|| {
                    komo_core::domain::wakeup::default_expiry_secs(&pending.wakeup)
                        .map(|secs| now() + secs)
                });
                // Durable before the wait is registered, and before the caller
                // is told: a registration for a suspension the log does not
                // hold would wake a turn that never stopped.
                self.record_durable(
                    session_id,
                    vec![SessionEventKind::TurnSuspended(TurnSuspendedEvent {
                        turn_id: probe.run_id.clone(),
                        wakeup: pending.wakeup.clone(),
                        call_id: pending.call_id.clone(),
                        summary: pending.summary.clone(),
                        expires_at,
                    })],
                )
                .await;
                self.register_wait(session_id, &probe.run_id, &pending, expires_at)
                    .await;
                info!(
                    run_id = %probe.run_id,
                    summary = %pending.summary,
                    "run suspended, waiting"
                );
                return Err(error);
            }
            Err(error) => {
                // The turn failed *after* the user message was persisted. Persist
                // an assistant turn too, so the transcript stays user/assistant-
                // alternating: the next turn's history would otherwise hold two
                // consecutive user messages, which several providers reject (and
                // the history-window repair only fixes a *leading* assistant
                // message, not an interior double-user). The stored note is
                // concise — the full error lives in the run ledger.
                //
                // A user cancel is not a failure, so it gets its own note: the
                // transcript should read as "I stopped this", not as an error.
                // A cancel that landed before the turn did anything is recorded
                // as such instead of leaving a tombstone: the transcript then
                // reads as if the turn never happened, while the log still
                // knows it did (the surface fold in `domain::session_event`).
                // "Did anything" means a tool ran — the only way a cancelled
                // turn can have effects worth remembering. Without this, a user
                // who sends a message and immediately stops it is left with a
                // "(已取消)" pair that every later turn replays. The run ledger
                // still records the cancelled run: the transcript is the
                // conversation, the ledger is the audit trail.
                // (Never on a resume: the trailing user message there belongs
                // to the interrupted turn, not to this continuation.)
                if is_fresh && is_cancelled(&error) && probe.steps_count() == 0 {
                    self.record_durable(
                        session_id,
                        vec![SessionEventKind::TurnCancelled {
                            turn_id: probe.run_id.clone(),
                            pristine: true,
                        }],
                    )
                    .await;
                    return Err(error);
                }
                let note = if is_cancelled(&error) {
                    CANCELLED_REPLY.to_string()
                } else {
                    format!(
                        "(上一条消息处理失败，未能完成回复：{})",
                        truncate(&format!("{error:#}"), 400)
                    )
                };
                let ended = if is_cancelled(&error) {
                    SessionEventKind::TurnCancelled {
                        turn_id: probe.run_id.clone(),
                        pristine: false,
                    }
                } else {
                    SessionEventKind::TurnFailed {
                        turn_id: probe.run_id.clone(),
                        error: truncate(&format!("{error:#}"), 400),
                    }
                };
                self.record_durable(
                    session_id,
                    vec![
                        SessionEventKind::AssistantMessage(AssistantMessageEvent {
                            turn_id: probe.run_id.clone(),
                            content: note,
                            tool_note: String::new(),
                            surface: SurfacePlacement::append(),
                        }),
                        ended,
                    ],
                )
                .await;
                return Err(error);
            }
        };

        // Anything the user said mid-turn is folded into *this turn's* stored
        // user message rather than appended as its own. Two consecutive user
        // messages is exactly what the transcript may not contain (several
        // providers reject it), and both halves really are one user's input for
        // one turn — the same merge a follow-up gets when it waits for the next
        // turn instead. Best-effort: the model already acted on them, so a
        // failure here costs the *next* turn context, not this one's answer.
        if !interjections.is_empty() {
            self.record(
                session_id,
                vec![SessionEventKind::UserMessage(UserMessageEvent {
                    turn_id: probe.run_id.clone(),
                    content: interjections.join("\n"),
                    source: MessageSource::Injected,
                    surface: SurfacePlacement::append(),
                })],
            )
            .await;
        }

        // Fold this turn's tool activity into a note on the assistant message, so
        // the *next* turn knows tools ran, what they found, and where an
        // over-limit output was kept. Without it the transcript carries only
        // user/assistant text: a follow-up question about something a tool just
        // read has to re-run the tool or be answered from nothing. Taken from
        // the turn's own steps — already redacted and truncated to exactly what
        // the log records — because the ledger's rows are a projection now and
        // this turn's are not committed until it closes.
        let tool_note = match probe.steps_count() {
            0 => String::new(),
            _ => tool_digest(&probe.steps()),
        };

        let assistant_msg = Message::assistant(&reply).with_tool_note(&tool_note);
        let mut closing = vec![SessionEventKind::AssistantMessage(AssistantMessageEvent {
            turn_id: probe.run_id.clone(),
            content: reply.clone(),
            tool_note,
            surface: SurfacePlacement::append(),
        })];
        // Which memories shaped this answer. Inside the turn's own closing batch
        // rather than after it: `turn/completed` is what ends a turn, and a
        // segment is sealed on that boundary, so an event recorded past it would
        // land in the next segment and outlive the turn it describes.
        if !memories.is_empty() {
            closing.push(SessionEventKind::TurnMemories {
                turn_id: probe.run_id.clone(),
                memories: memories.clone(),
            });
        }
        closing.push(SessionEventKind::TurnCompleted {
            turn_id: probe.run_id.clone(),
        });
        self.record_durable(session_id, closing).await;
        session.messages.push(assistant_msg);

        // Lifecycle hooks: the turn delivered. Failed/cancelled turns never
        // reach here — they surface through the run ledger instead.
        for hook in &self.turn_hooks {
            hook.turn_finished(session_id, &reply).await;
        }

        Ok((reply, usage, memories))
    }

    /// Register the wait a suspended turn is holding, so something comes back
    /// for it after this process is gone.
    ///
    /// Best-effort with a loud failure: a suspension the scheduler never learns
    /// about is a turn nobody wakes — but the log already says the turn is
    /// waiting, and the startup re-check (`reregister_suspended_turns`) reads it
    /// back from there, which is why this can fail without stranding anything
    /// permanently.
    async fn register_wait(
        &self,
        session_id: &str,
        turn_id: &str,
        pending: &komo_core::domain::context::PendingSuspension,
        expires_at: Option<i64>,
    ) {
        let Some(wakeups) = &self.wakeups else {
            warn!(
                turn_id,
                "no wakeup store wired: this turn waits until the next startup check"
            );
            return;
        };
        let registration = WakeupRegistration::new(session_id, pending.wakeup.clone(), now())
            .continuing(turn_id)
            .expiring_at(expires_at)
            // The job's grants ride across the wait: a routine that stopped to
            // ask still has to be able to act when it comes back.
            .with_grants(
                komo_services::tool_execution::current_job_grants()
                    .iter()
                    .map(komo_core::domain::policy::RuleSpec::from_rule)
                    .collect(),
            );
        if let Err(error) = wakeups.save(&registration).await {
            warn!(%error, turn_id, "failed to register a suspended turn's wait");
        }
    }

    /// Append this turn's events, best-effort, answering with them as the log
    /// stamped them. A record that fails to land must never fail the turn it
    /// describes — an empty answer is that failure.
    async fn record(&self, session_id: &str, kinds: Vec<SessionEventKind>) -> Vec<SessionEvent> {
        match self.events.append(session_id, kinds).await {
            Ok(appended) => appended,
            Err(error) => {
                warn!(%error, "failed to append session events (non-fatal)");
                Vec::new()
            }
        }
    }

    /// The turn's row, the moment the log says the turn exists.
    ///
    /// A crash leaves it `running`, which is exactly what the startup
    /// reconciler looks for: an interrupted turn has to be in the ledger to be
    /// listed, inspected or resumed, and after the flip to a projection nothing
    /// else would put it there until the turn was over. Folded from the opening
    /// events themselves — the only commit that needs no read of the log,
    /// because its caller has just written everything the fold needs.
    async fn open_in_ledger(&self, session_id: &str, opening: &[SessionEvent]) {
        let Some(through) = opening.last().map(|event| event.seq) else {
            return;
        };
        let runs = project_runs(session_id, opening);
        if let Err(error) = self.projection.commit(session_id, &runs, through).await {
            warn!(%error, "failed to open the turn in the ledger (non-fatal)");
        }
        // A continuation's opening is what takes the badge off: the wait ended
        // when the work restarted, not when it next finishes.
        self.commit_awaiting(session_id, opening).await;
    }

    /// Fold this turn's events onto the session's cached wait, best-effort.
    ///
    /// Rides on the reads the ledger commit already did — a suspension and its
    /// wake are events in the same tail — so a session list can say which
    /// conversations are stopped on someone without folding every transcript.
    async fn commit_awaiting(&self, session_id: &str, events: &[SessionEvent]) {
        if let Err(error) = self.sessions.commit_awaiting(session_id, events).await {
            warn!(%error, "failed to project the session's wait (non-fatal)");
        }
    }

    /// Append and make durable. Every way a turn can end goes through here:
    /// past this point the turn is over, so whatever it recorded has to have
    /// survived — including the ways that end badly. A failed turn whose events
    /// were only buffered reads afterwards as a turn that never happened.
    async fn record_durable(&self, session_id: &str, kinds: Vec<SessionEventKind>) {
        let _ = self.record(session_id, kinds).await;
        if let Err(error) = self.events.durable_flush(session_id).await {
            warn!(%error, "failed to make a finished turn durable (non-fatal)");
            // The log still holds unwritten events, and its upkeep is defined
            // over what has landed. Skipped rather than attempted and refused.
            return;
        }
    }

    /// Close the turn out: commit the ledger, then let the log do its upkeep.
    ///
    /// **One read of the log serves both.** The fold *is* the ledger — the rows
    /// `run list` and `skills audit` read are committed from it here — and the
    /// same fold says which turns retention may not cut, so reading the log
    /// twice per turn would be paying twice for one answer.
    ///
    /// Ordered after the turn's terminal event and before learning is
    /// dispatched: an episode is assembled from these rows, and the retention
    /// rule that protects the turn that just ended reads it as *unlearned*,
    /// which it cannot be until it is a row at all.
    async fn settle_turn(&self, session_id: &str, from_seq: u64) {
        let tail = match from_seq {
            // A turn whose start the log never confirmed, and every resume.
            UNKNOWN_START => self.events.events(session_id).await,
            from => self.events.events_from(session_id, from).await,
        };
        let events = match tail {
            Ok(events) => events,
            Err(error) => {
                warn!(%error, "failed to read the session log to settle the turn (non-fatal)");
                return;
            }
        };
        let runs = project_runs(session_id, &events);
        let through = events.last().map(|event| event.seq).unwrap_or(0);
        if let Err(error) = self.projection.commit(session_id, &runs, through).await {
            warn!(%error, "failed to project the run ledger (non-fatal)");
        }
        self.commit_awaiting(session_id, &events).await;
        // Before the boundary, so the checkpoint written there already holds the
        // summary — and inside this turn's session slot, which is what keeps two
        // compactions from planning against the same surface.
        if let Some(compaction) = &self.compaction
            && let Some(turn) = runs.last().map(|projected| projected.run.id.clone())
        {
            compaction.compact_if_long(session_id, &turn).await;
        }
        match self.events.turn_boundary(session_id).await {
            // Only a roll can put the log over its budget, so the cut is
            // considered only then.
            Ok(true) => self.retain(session_id).await,
            Ok(false) => {}
            Err(error) => warn!(%error, "session log upkeep failed at a turn boundary (non-fatal)"),
        }
    }

    /// Cut the session's log back toward its budget, keeping every turn that is
    /// still resumable or still unlearned.
    ///
    /// The floor is over every turn the session still holds, so this is the one
    /// place that reads the whole log — and a roll is the only moment the
    /// retained size can cross budget, so it is paid per *segment*, not per
    /// turn. `recoverable` comes off the fold; `learned` is the sweep's
    /// watermark, read from the rows it advances. A turn nobody has finished
    /// with outranks the space it costs: finding no safe cut is a normal
    /// answer, and leaves the session over budget.
    async fn retain(&self, session_id: &str) {
        let events = match self.events.events(session_id).await {
            Ok(events) => events,
            Err(error) => {
                warn!(%error, "failed to read the session log for retention (non-fatal)");
                return;
            }
        };
        let runs = &project_runs(session_id, &events);
        let unlearned: std::collections::HashSet<String> = self
            .runs
            .unlearned(Some(session_id), RETENTION_LEDGER_SCAN)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|run| run.id)
            .collect();
        let keep_from = runs
            .iter()
            // A turn that has not finished keeps its own log whatever else is
            // true of it: `recoverable` covers the one that crashed, and a
            // *suspended* turn is not recoverable — its return is scheduled —
            // but cutting its rounds away would leave the continuation
            // replaying nothing.
            .filter(|projected| {
                !projected.run.status.is_terminal() || unlearned.contains(&projected.run.id)
            })
            // Its own start is not enough: a resumable turn replays every
            // earlier attempt at it, and those are turns of their own in the log.
            .map(|projected| replay_floor(runs, projected))
            .min()
            .unwrap_or(u64::MAX);

        match self.events.retain(session_id, keep_from).await {
            Ok(Some(through)) => {
                info!(session_id, through, "cut the session log back into budget")
            }
            Ok(None) => {}
            Err(error) => warn!(%error, "failed to cut the session log (non-fatal)"),
        }
    }

    /// Await `work`, unless the turn is cancelled first.
    ///
    /// `Err(Cancelled)` rather than an `Option` so the loop's control points read
    /// as one `?` each: a cancel propagates out of the loop like any other turn
    /// failure, and every layer above tells it apart by downcasting.
    async fn until_cancelled<T>(
        cancel: Option<&Arc<dyn CancelSignal>>,
        work: impl Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        let Some(cancel) = cancel else {
            return work.await;
        };
        if cancel.is_cancelled() {
            return Err(Cancelled.into());
        }
        tokio::select! {
            // Bias the work: when both are ready, finishing beats discarding.
            biased;
            done = work => done,
            () = cancel.cancelled() => Err(Cancelled.into()),
        }
    }

    /// komo's own tool-calling loop (roadmap §7 — the loop lives here, not in
    /// rig, so control points can sit between rounds). Drive the model a round
    /// at a time: a [`Step::Final`] ends the turn; [`Step::ToolCalls`] go to the
    /// tool executor as one round (it owns lookup, retry, the per-call budget,
    /// the ledger, and the result cap) and the outcomes are threaded back. Once
    /// the per-turn *round* budget is exceeded, feed [`BUDGET_REACHED_NOTE`]
    /// back in place of results and force a final answer.
    async fn run_agent_loop(
        &self,
        session: &Session,
        run: RunContext,
        resume: Option<(Vec<SessionEvent>, String)>,
    ) -> anyhow::Result<TurnOutcome> {
        // Pin the tool catalog for this turn. The model is handed one set of
        // schemas below; a plugin mounting or unmounting mid-turn must not
        // change what the loop then dispatches against, or a call the model was
        // invited to make would answer "unknown tool" a round later. The
        // mutation is not lost — the next turn pins the new set.
        let tools = self.tool_executor.pin();

        // This turn's event recorder, bound to its ledger run — the loop and
        // the driver stay run-id-free. A run *is* a turn in komo, so the run id
        // is the turn id the events carry.
        let recorder: Option<Arc<dyn TurnRecorder>> = Some({
            RunRecorder::new(self.events.clone(), &session.id, &run.run_id) as Arc<dyn TurnRecorder>
        });
        // The executor gets the turn's context explicitly: the run handle this
        // turn opened, and the session established by the dispatcher / api /
        // handle_input (read once here — the one ambient-to-explicit bridge).
        let context = ToolTurnContext {
            // The stored session is the authority on which correspondent this
            // conversation answers; an ingress only knows how *it* was
            // addressed. Filling the address in here — where the record is
            // already loaded — means every ingress gets the same answer with no
            // plumbing of its own, and a client free to name a session id can
            // never name the channel its turn is evaluated against.
            session: current_session()
                .unwrap_or_else(|| SessionContext::detached(&session.id))
                .with_channel(session.channel.clone()),
            run: Some(run),
            // Bound the turn's cumulative tool output (0 = unlimited), so a long
            // tool chain can't quietly overflow the context window.
            budget: TurnResultBudget::new(tools.turn_result_cap()),
            // Fresh per turn: a repeat only means anything within the one
            // sequence of calls that is trying to accomplish one thing.
            spin: SpinDetector::default(),
        };
        // Cancellation, if this caller offers a stop. Raced against each await
        // rather than only checked between rounds: the model round-trip is the
        // longest wait in a turn and the likeliest thing a user interrupts.
        let cancel = context.session.cancel.clone();
        let cancel = cancel.as_ref();

        // Stream the model's output to whoever is watching. Only built when a
        // watcher is actually attached: an unwatched turn (every chat channel,
        // every sweep) hands the backend `None` and pays nothing per chunk.
        let deltas: Option<Arc<dyn DeltaSink>> = context
            .session
            .event_sink
            .clone()
            .map(|sink| Arc::new(StreamingDeltas(sink)) as Arc<dyn DeltaSink>);

        let mut driver = match &resume {
            None => self.llm.begin_turn(session, deltas, recorder).await?,
            Some((events, turn_id)) => {
                self.llm
                    .resume_turn(session, events, turn_id, deltas, recorder)
                    .await?
            }
        };
        let mut step = Self::until_cancelled(cancel, driver.first()).await?;
        let mut rounds = 0usize;
        // The model's most recent narration alongside its tool calls. Kept so the
        // budget cutoff below can answer in the model's own words instead of a
        // canned line — by then it has usually said what it was doing.
        let mut narration = String::new();
        // What the user said mid-turn, in order, for the caller to fold into
        // the transcript once the turn ends.
        let mut interjections: Vec<String> = Vec::new();

        let reply = loop {
            match step {
                Step::Final(text) => break non_empty(text),
                Step::ToolCalls { calls, text } => {
                    rounds += 1;
                    let over_budget = rounds > self.max_turns;

                    // Text the model wrote in the same breath as its tool calls.
                    // It never reaches a chat channel (the turn hasn't answered
                    // yet), but a watching client can render it, which is the
                    // only view komo offers into the model's reasoning mid-turn.
                    if !text.trim().is_empty() {
                        if let Some(sink) = &context.session.event_sink {
                            sink.emit(TurnEvent::AssistantText { text: text.clone() });
                        }
                        narration = text;
                    }

                    let results: Vec<ToolOutcome> = if over_budget {
                        calls
                            .iter()
                            .map(|call| ToolOutcome {
                                id: call.id.clone(),
                                call_id: call.call_id.clone(),
                                content: BUDGET_REACHED_NOTE.to_string(),
                                structured: serde_json::Value::Null,
                            })
                            .collect()
                    } else {
                        // One round, delegated whole: the executor runs the
                        // calls concurrently (order-preserving) and maps tool
                        // errors / unknown names into outcome content the model
                        // can recover from — only a driver/LLM error aborts the
                        // turn. A cancel here abandons the round's results; the
                        // calls themselves are spawned and still finish (see
                        // `domain::cancel`).
                        Self::until_cancelled(cancel, async {
                            Ok(tools.execute_round(&calls, &context).await)
                        })
                        .await?
                    };

                    // A call that stopped to wait — for an approval, or because
                    // the tool asked to be woken — ends the turn here.
                    // Checked between rounds rather than inside the round: the
                    // round's other calls have already run and settled, and
                    // their results are on record for the continuation to
                    // replay — what must not happen is another provider request
                    // carrying a result for a call that never ran.
                    if let Some(run) = &context.run
                        && run.suspension().is_some()
                    {
                        return Err(Suspended.into());
                    }

                    // Anything the user said while that round ran joins this
                    // step instead of waiting for a whole new turn — a
                    // correction is only worth anything before the agent
                    // finishes going the wrong way. Drained here, between
                    // rounds, so the model sees it at the one point it can
                    // change course. Kept for the transcript too: the next
                    // turn has to know what was said.
                    let said = context
                        .session
                        .interject
                        .as_ref()
                        .map(|source| source.take())
                        .unwrap_or_default();
                    let mut said = said;
                    if !said.is_empty() {
                        info!(count = said.len(), "user interjected mid-turn");
                        interjections.extend(said.iter().cloned());
                    }

                    // Between-round hooks. Their text rides the same channel as
                    // a user's interjection — appended to the message carrying
                    // this round's results — so it grows the request at the end
                    // and leaves the cached prefix alone. A `Stop` ends the turn
                    // the way the round budget's does: with an answer.
                    //
                    // Deliberately *not* folded into `interjections`: that list
                    // becomes part of the stored user message, and what a hook
                    // said is not something the user said. The ledger's step
                    // record is where it belongs.
                    let mut stopped_by_hook = None;
                    for hook in &self.step_hooks {
                        match hook.pre_step(&session.id, rounds).await {
                            StepDecision::Continue => {}
                            StepDecision::Inject(text) if text.trim().is_empty() => {}
                            StepDecision::Inject(text) => {
                                info!(hook = hook.name(), round = rounds, "hook injected context");
                                said.push(text);
                            }
                            StepDecision::Stop(reason) => {
                                info!(hook = hook.name(), round = rounds, "hook stopped the turn");
                                stopped_by_hook = Some(reason);
                                break;
                            }
                        }
                    }
                    if let Some(reason) = stopped_by_hook {
                        break non_empty(reason);
                    }

                    let interjected = if said.is_empty() {
                        None
                    } else {
                        Some(said.join("\n"))
                    };

                    // The model kept re-issuing one call even after the executor
                    // refused it (see `SpinDetector`). The refusals went back as
                    // well-formed results, so it gets this round to answer with
                    // what it has — but the turn ends either way rather than
                    // spending the rest of its rounds on the same call.
                    let spun = context.spin.should_stop();
                    let next =
                        Self::until_cancelled(cancel, driver.step(results, interjected)).await?;
                    // Over budget, the note went back as well-formed tool results;
                    // terminate now no matter what the model did with it.
                    step = if over_budget || spun {
                        let stopped = if spun { SPUN_STOP } else { BUDGET_STOP };
                        break non_empty(match next {
                            Step::Final(text) => text,
                            // It asked for more tools instead of answering. Its
                            // own last narration is a better account of where the
                            // turn got to than a canned apology, so prefer it.
                            Step::ToolCalls { text, .. } => stop_reply(stopped, &text, &narration),
                        });
                    } else {
                        next
                    };
                }
            }
        };
        Ok(TurnOutcome {
            reply,
            usage: driver.usage(),
            memories: driver.memories(),
            interjections,
        })
    }
}

/// Forwards the provider's streamed output onto the turn's event sink.
///
/// The two sinks exist for different reasons and are deliberately not merged:
/// [`ToolEventSink`] is the fire-and-forget channel every watcher already reads
/// (tool starts and finishes travel it), while [`DeltaSink`] is the seam the LLM
/// backend writes into and knows nothing about sessions. This is the one adapter
/// between them.
struct StreamingDeltas(Arc<dyn ToolEventSink>);

impl DeltaSink for StreamingDeltas {
    fn text(&self, delta: &str) {
        self.0.emit(TurnEvent::AssistantDelta {
            text: delta.to_string(),
        });
    }

    fn reasoning(&self, delta: &str) {
        self.0.emit(TurnEvent::ReasoningDelta {
            text: delta.to_string(),
        });
    }
}

/// What kind of turn [`AgentRuntime::turn_body`] is driving.
enum TurnKind {
    /// An ordinary user turn: persist the input, open a fresh driver.
    Fresh { user_input: String },
    /// A continuation of an interrupted turn: the user message is already in
    /// the conversation, and the driver reopens from the session's own events.
    Resume {
        events: Vec<SessionEvent>,
        turn_id: String,
    },
}

/// What one pass of the agent loop produced.
struct TurnOutcome {
    reply: String,
    usage: TokenUsage,
    /// The memories prompt assembly injected, on its way to the ledger — the
    /// same trip `usage` makes, for the same reason: both are facts about the
    /// turn that only the layer below knows.
    memories: RecalledMemories,
    /// User messages that arrived mid-turn and were folded into it. The loop
    /// already showed them to the model; the caller still has to get them into
    /// the transcript, or the next turn has no idea they were ever said.
    interjections: Vec<String>,
}

/// Told to the user when the round budget ran out.
const BUDGET_STOP: &str = "(Reached the tool-call limit for this turn; \
     answering with what I have.)";
/// Told to the user when the turn was ended for repeating one call — see
/// `SpinDetector`. Named rather than folded into [`BUDGET_STOP`] because the
/// two situations call for different next moves from the user: a budget stop
/// invites "keep going", a spin stop invites rephrasing.
const SPUN_STOP: &str = "(I was repeating the same step without progress, so I \
     stopped there. Answering with what I have — try rephrasing if this misses \
     what you needed.)";

/// The reply for a turn something cut short. The model's own words (this round's
/// text, else the last narration it managed) beat a canned line — but the user
/// still has to be told the turn stopped early rather than finished, and why.
fn stop_reply(stopped: &str, current: &str, narration: &str) -> String {
    let said = [current, narration]
        .into_iter()
        .map(str::trim)
        .find(|t| !t.is_empty());
    match said {
        Some(text) => format!("{text}\n\n{stopped}"),
        None => stopped.to_string(),
    }
}

#[cfg(test)]
pub(crate) mod tests {

    /// Append one message as an event and make it durable — what a turn does,
    /// condensed for a fixture that only cares that the message is there.
    async fn say(db: &Db, session_id: &str, message: Message) {
        use komo_core::domain::session_event::{
            AssistantMessageEvent, MessageSource, SurfacePlacement, UserMessageEvent,
        };
        let kind = match message.role {
            Role::Assistant => SessionEventKind::AssistantMessage(AssistantMessageEvent {
                turn_id: "t".into(),
                content: message.content,
                tool_note: message.tool_note,
                surface: SurfacePlacement::append(),
            }),
            _ => SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: "t".into(),
                content: message.content,
                source: MessageSource::User,
                surface: SurfacePlacement::append(),
            }),
        };
        SessionEventRepository::append(db, session_id, vec![kind])
            .await
            .unwrap();
        SessionEventRepository::durable_flush(db, session_id)
            .await
            .unwrap();
    }
    use super::*;
    use komo_infra::persistence::db::Db;
    use komo_tools::ask_user::AskUserTool;
    use komo_tools::time::TimeTool;
    use komo_tools::wait::WaitTool;

    use crate::interaction::{CancelState, CancelTicket};
    use async_trait::async_trait;
    use komo_core::domain::{
        cancel::CANCELLED_ERROR,
        llm::{LlmClient, Step, ToolCallReq, TurnDriver},
        message::Role,
        repository::SessionRepository,
        run::RunStatus,
        session::Session,
        session_event::Wakeup,
        tool::{Tool, ToolError, ToolOutput},
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// An [`LlmClient`] that replays a scripted sequence of [`Step`]s and records
    /// the tool results fed back to each `step()` — no rig, no network. Lets us
    /// drive `run_agent_loop` deterministically and assert dispatch, threading,
    /// the ledger, and the round budget.
    struct ScriptedLlm {
        script: Mutex<VecDeque<Step>>,
        received: Arc<Mutex<Vec<Vec<ToolOutcome>>>>,
        /// Mid-turn user messages the loop handed to `step()`, in order.
        interjected: Arc<Mutex<Vec<String>>>,
        /// How many journal rows `resume_turn` was handed; `None` until called.
        resumed_entries: Arc<Mutex<Option<usize>>>,
    }

    #[async_trait]
    impl LlmClient for ScriptedLlm {
        async fn complete(&self, _session: &Session) -> anyhow::Result<String> {
            Ok("unused".to_string())
        }
        async fn begin_turn(
            &self,
            _session: &Session,
            _deltas: Option<Arc<dyn DeltaSink>>,
            recorder: Option<Arc<dyn TurnRecorder>>,
        ) -> anyhow::Result<Box<dyn TurnDriver>> {
            // One turn per test, so hand the whole script to the driver.
            let steps = std::mem::take(&mut *self.script.lock().unwrap());
            Ok(Box::new(ScriptedDriver {
                steps,
                received: self.received.clone(),
                interjected: self.interjected.clone(),
                recorder,
                round: 0,
            }))
        }
        async fn resume_turn(
            &self,
            session: &Session,
            events: &[SessionEvent],
            turn_id: &str,
            deltas: Option<Arc<dyn DeltaSink>>,
            recorder: Option<Arc<dyn TurnRecorder>>,
        ) -> anyhow::Result<Box<dyn TurnDriver>> {
            let of_turn = events
                .iter()
                .filter(|e| e.turn_id_of_work() == Some(turn_id))
                .count();
            self.resumed_entries.lock().unwrap().replace(of_turn);
            self.begin_turn(session, deltas, recorder).await
        }
    }

    struct ScriptedDriver {
        steps: VecDeque<Step>,
        received: Arc<Mutex<Vec<Vec<ToolOutcome>>>>,
        interjected: Arc<Mutex<Vec<String>>>,
        /// The real driver records one `assistant/round` per provider
        /// completion, and a fixture that skips it leaves a log claiming the
        /// turn never called a model — which is most of what these tests read
        /// the log back for.
        recorder: Option<Arc<dyn TurnRecorder>>,
        round: u32,
    }

    impl ScriptedDriver {
        /// Record the round this step is the completion of. The scripted usage
        /// is reported whole on the last round, the way a driver that counts
        /// once at the end would.
        async fn record_round(&mut self, step: &Step) {
            let Some(recorder) = self.recorder.clone() else {
                return;
            };
            let last = self.steps.is_empty();
            let usage = if last {
                self.usage()
            } else {
                TokenUsage::default()
            };
            let blocks = match step {
                Step::Final(text) => serde_json::json!([{ "Text": text }]),
                Step::ToolCalls { calls, .. } => serde_json::json!(
                    calls
                        .iter()
                        .map(|c| serde_json::json!({
                            "ToolCall": {
                                "id": c.id,
                                "call_id": c.call_id,
                                "name": c.name,
                                "args": c.args,
                            }
                        }))
                        .collect::<Vec<_>>()
                ),
            };
            let round = self.round;
            self.round += 1;
            recorder
                .record(vec![SessionEventKind::AssistantRound(
                    komo_core::domain::session_event::AssistantRoundEvent {
                        turn_id: recorder.turn_id().to_string(),
                        round,
                        response_id: format!("resp-{round}"),
                        blocks,
                        tokens_in: usage.input,
                        tokens_out: usage.output,
                        tokens_cached: usage.cached_input,
                    },
                )])
                .await;
        }
    }

    #[async_trait]
    impl TurnDriver for ScriptedDriver {
        async fn first(&mut self) -> anyhow::Result<Step> {
            let step = self.steps.pop_front().expect("script exhausted at first()");
            self.record_round(&step).await;
            Ok(step)
        }
        async fn step(
            &mut self,
            results: Vec<ToolOutcome>,
            interjected: Option<String>,
        ) -> anyhow::Result<Step> {
            if let Some(text) = interjected {
                self.interjected.lock().unwrap().push(text);
            }
            self.received.lock().unwrap().push(results);
            let step = self.steps.pop_front().expect("script exhausted at step()");
            self.record_round(&step).await;
            Ok(step)
        }
        fn usage(&self) -> TokenUsage {
            // Fixed, non-zero counts, so a test can tell "recorded" from
            // "unknown". `cached_input` is a subset of `input`, as the provider
            // layer guarantees.
            TokenUsage {
                input: 1_200,
                output: 340,
                cached_input: 900,
            }
        }
    }

    /// A tool that echoes its raw input, for asserting result threading.
    struct EchoArgsTool;
    #[async_trait]
    impl Tool for EchoArgsTool {
        fn name(&self) -> &'static str {
            "echo"
        }
        fn description(&self) -> &'static str {
            "echoes its input args"
        }
        async fn call(
            &self,
            input: serde_json::Value,
            _ctx: &komo_core::domain::context::ToolContext,
        ) -> Result<ToolOutput, ToolError> {
            // Echo the payload, not its JSON encoding: the assertion is about
            // results threading back through the loop.
            let text = input
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| input.to_string());
            Ok(ToolOutput::text(format!("echo:{text}")))
        }
    }

    /// A tool that always errors, for asserting failures feed back (not abort).
    struct FailTool;
    #[async_trait]
    impl Tool for FailTool {
        fn name(&self) -> &'static str {
            "fail"
        }
        fn description(&self) -> &'static str {
            "always errors"
        }
        async fn call(
            &self,
            _input: serde_json::Value,
            _ctx: &komo_core::domain::context::ToolContext,
        ) -> Result<ToolOutput, ToolError> {
            Err(ToolError::Failed(anyhow::anyhow!("boom")))
        }
    }

    /// A komo home of this test's own, wiped first.
    ///
    /// The whole directory, not just the db file: a home now holds transcripts
    /// beside `state.db`, and two tests sharing a directory would read each
    /// other's conversations.
    pub(crate) fn sqlite_url(name: &str) -> String {
        let home = std::env::temp_dir().join(format!("komo-test-{name}"));
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        format!("turso:{}", home.join("state.db").display())
    }

    /// A tool-call step with no narration — the shape most tests care about.
    fn tool_calls(calls: Vec<ToolCallReq>) -> Step {
        Step::ToolCalls {
            calls,
            text: String::new(),
        }
    }

    fn call(name: &str, args: &str) -> ToolCallReq {
        ToolCallReq {
            id: format!("id-{name}"),
            call_id: None,
            name: name.to_string(),
            args: args.to_string(),
        }
    }

    /// Build a runtime whose LLM replays `script`, with `tools` registered and a
    /// round budget of `max_turns`. Returns the runtime and a handle to the tool
    /// results fed back to the driver, round by round.
    fn scripted_runtime(
        db: Arc<Db>,
        script: Vec<Step>,
        tools: Vec<Arc<dyn Tool>>,
        max_turns: usize,
    ) -> (AgentRuntime, Arc<Mutex<Vec<Vec<ToolOutcome>>>>) {
        let (rt, received, _) = scripted_runtime_seeing_interjections(db, script, tools, max_turns);
        (rt, received)
    }

    /// [`scripted_runtime`] plus a handle on the mid-turn user messages the loop
    /// fed to the driver — what an interjection test asserts on.
    #[allow(clippy::type_complexity)]
    fn scripted_runtime_seeing_interjections(
        db: Arc<Db>,
        script: Vec<Step>,
        tools: Vec<Arc<dyn Tool>>,
        max_turns: usize,
    ) -> (
        AgentRuntime,
        Arc<Mutex<Vec<Vec<ToolOutcome>>>>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let received = Arc::new(Mutex::new(Vec::new()));
        let interjected = Arc::new(Mutex::new(Vec::new()));
        let mut executor =
            ToolExecutor::new(komo_services::tool_execution::ToolExecutionConfig::default());
        for t in tools {
            executor.register(t);
        }
        // Same wiring as `cli::wiring`: the executor records each call's two
        // halves in the session log. Without it a fixture turn leaves a log that
        // says the turn ran no tools, which is the one thing these tests are
        // about.
        let executor = executor.with_events(db.clone());
        let rt = AgentRuntime {
            llm: Arc::new(ScriptedLlm {
                script: Mutex::new(script.into()),
                received: received.clone(),
                interjected: interjected.clone(),
                resumed_entries: Arc::new(Mutex::new(None)),
            }),
            sessions: db.clone(),
            messages: db.clone(),
            events: db.clone(),
            projection: db.clone(),
            runs: db.clone(),
            tool_executor: executor,
            max_turns,
            history_window: 0,
            learning: None,
            compaction: None,
            wakeups: None,
            checkpoint: None,
            turn_hooks: Vec::new(),
            step_hooks: Vec::new(),
        };
        (rt, received, interjected)
    }

    /// A tool that parks until released, so a turn can be cancelled *while* a
    /// round is in flight rather than only between rounds.
    struct BlockingTool {
        released: Arc<tokio::sync::Notify>,
        started: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl Tool for BlockingTool {
        fn name(&self) -> &'static str {
            "block"
        }
        fn description(&self) -> &'static str {
            "parks until released"
        }
        async fn call(
            &self,
            _input: serde_json::Value,
            _ctx: &komo_core::domain::context::ToolContext,
        ) -> Result<ToolOutput, ToolError> {
            self.started.notify_waiters();
            self.released.notified().await;
            Ok(ToolOutput::text("released"))
        }
    }

    /// A `SessionContext` carrying a cancellation signal, plus its trigger.
    ///
    /// The ticket is leaked into the returned pair's lifetime by holding it in
    /// the closure-free way a test can: dropping it would retire the slot, and
    /// then `cancel` would have nothing to flip.
    fn cancellable_ctx(session: &str) -> (SessionContext, Arc<CancelState>, CancelTicket) {
        let cancels = Arc::new(CancelState::new());
        let ticket = cancels.register(session);
        let ctx = SessionContext::detached(session).with_cancel(ticket.signal());
        (ctx, cancels, ticket)
    }

    #[tokio::test]
    async fn cancelling_mid_round_stops_the_turn_and_notes_it_in_the_transcript() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_cancel_mid.db"))
                .await
                .unwrap(),
        );
        let started = Arc::new(tokio::sync::Notify::new());
        let released = Arc::new(tokio::sync::Notify::new());
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("block", "{}")]),
                Step::Final("never reached".into()),
            ],
            vec![Arc::new(BlockingTool {
                started: started.clone(),
                released: released.clone(),
            })],
            30,
        );

        let (ctx, cancels, _ticket) = cancellable_ctx("cancel-mid");
        let wait_started = started.notified();
        let turn = tokio::spawn(with_session(ctx, async move {
            rt.handle_input("cancel-mid", "长任务".to_string()).await
        }));

        // Cancel while the tool round is still running.
        wait_started.await;
        assert!(cancels.cancel("cancel-mid"), "signal should be registered");

        let outcome = turn.await.unwrap();
        let error = outcome.expect_err("a cancelled turn fails");
        assert!(is_cancelled(&error), "expected Cancelled, got {error:#}");
        released.notify_waiters();

        // The transcript keeps alternating, with a note that says what happened.
        let messages = MessageRepository::list_by_session(&*db, "cancel-mid")
            .await
            .unwrap();
        let last = messages.last().unwrap();
        assert_eq!(last.role, Role::Assistant);
        assert_eq!(last.content, CANCELLED_REPLY);

        // The ledger says cancelled — not a failure, and not resumable.
        let run = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error, CANCELLED_ERROR);
        assert!(!run.recoverable);
        assert!(run.ended_at.is_some());

        assert_ledger_matches_log(&db, "cancel-mid").await;
    }

    #[tokio::test]
    async fn cancelling_before_the_first_round_never_calls_the_model() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_cancel_early.db"))
                .await
                .unwrap(),
        );
        // An empty script: reaching the model at all would panic ("script
        // exhausted"), so this also proves the check happens before the round.
        let (rt, _) = scripted_runtime(db.clone(), vec![], vec![], 30);

        let (ctx, cancels, _ticket) = cancellable_ctx("cancel-early");
        cancels.cancel("cancel-early");
        let error = with_session(ctx, rt.handle_input("cancel-early", "算了".to_string()))
            .await
            .expect_err("a cancelled turn fails");
        assert!(is_cancelled(&error));
    }

    /// An [`InterjectSource`] that hands over a fixed message once — the shape
    /// of a user typing while a round runs.
    struct SaysOnce(Mutex<Option<String>>);
    impl komo_core::domain::gateway::InterjectSource for SaysOnce {
        fn take(&self) -> Vec<String> {
            self.0.lock().unwrap().take().into_iter().collect()
        }
    }

    /// What the user says mid-turn reaches the model on the very next round —
    /// the whole point, since a correction is worthless once the agent has
    /// finished going the wrong way — and lands in the transcript folded into
    /// this turn's user message (never as a second one, which would leave two
    /// consecutive user messages for the next turn to replay).
    #[tokio::test]
    async fn a_mid_turn_interjection_reaches_the_model_and_the_transcript() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_interject.db"))
                .await
                .unwrap(),
        );
        let (rt, _, interjected) = scripted_runtime_seeing_interjections(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                Step::Final("好的，改看 B".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );

        let ctx = SessionContext::detached("cli:interject").with_interject(Arc::new(SaysOnce(
            Mutex::new(Some("不对，是 B 不是 A".to_string())),
        )));
        let reply = with_session(ctx, rt.handle_input("cli:interject", "看下 A".to_string()))
            .await
            .unwrap();
        assert_eq!(reply, "好的，改看 B");

        // Delivered to the model on the round right after it was said.
        assert_eq!(
            interjected.lock().unwrap().clone(),
            vec!["不对，是 B 不是 A"],
            "the interjection must reach the driver mid-turn"
        );

        // One user message for the turn, carrying both halves of what was said.
        let messages = MessageRepository::list_by_session(&*db, "cli:interject")
            .await
            .unwrap();
        let roles: Vec<Role> = messages.iter().map(|m| m.role.clone()).collect();
        assert_eq!(
            roles,
            vec![Role::User, Role::Assistant],
            "an interjection must not become a second user message"
        );
        assert!(
            messages[0].content.contains("看下 A") && messages[0].content.contains("是 B 不是 A"),
            "both halves belong to the turn's user message, got {:?}",
            messages[0].content
        );

        assert_ledger_matches_log(&db, "cli:interject").await;
    }

    /// A cancel that lands before any tool ran leaves nothing behind: the
    /// turn's own user message is rewound out, so the transcript reads as if it
    /// never happened and later turns don't replay a "(已取消)" pair forever.
    /// The ledger still records the cancelled run — that is the audit trail.
    #[tokio::test]
    async fn a_pristine_cancel_rewinds_its_user_message() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_cancel_pristine.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(db.clone(), vec![], vec![], 30);

        let (ctx, cancels, _ticket) = cancellable_ctx("cancel-pristine");
        cancels.cancel("cancel-pristine");
        let error = with_session(ctx, rt.handle_input("cancel-pristine", "算了".to_string()))
            .await
            .expect_err("a cancelled turn fails");
        assert!(is_cancelled(&error));

        let messages = MessageRepository::list_by_session(&*db, "cancel-pristine")
            .await
            .unwrap();
        assert!(
            messages.is_empty(),
            "a pristine cancel leaves no transcript, got {} message(s)",
            messages.len()
        );

        let run = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error, CANCELLED_ERROR);

        // The run is still in the ledger even though the conversation reads as
        // if the turn never happened — and so it must be in the projection.
        assert_ledger_matches_log(&db, &run.session_id).await;
    }

    #[tokio::test]
    async fn a_turn_without_a_cancel_signal_runs_normally() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_cancel_absent.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(db.clone(), vec![Step::Final("done".into())], vec![], 30);
        // Sweeps, cron and aux turns carry no signal; that must stay a no-op path.
        let reply = rt
            .handle_input("no-cancel", "hi".to_string())
            .await
            .unwrap();
        assert_eq!(reply, "done");
    }

    #[tokio::test]
    async fn cancel_state_reports_whether_a_turn_was_listening() {
        let cancels = Arc::new(CancelState::new());
        assert!(
            !cancels.cancel("nobody"),
            "no turn in flight → nothing to do"
        );

        let ticket = cancels.register("s1");
        assert!(!ticket.is_cancelled());
        assert!(cancels.cancel("s1"));
        assert!(ticket.is_cancelled());
        // Awaiting an already-cancelled signal resolves immediately.
        ticket.cancelled().await;

        drop(ticket);
        assert!(!cancels.cancel("s1"), "finished turns are unreachable");
    }

    /// Stop is pressed on a *conversation*, so it has to reach the turn running
    /// and the one queued behind it. With a single slot per session the queued
    /// caller could not register at all, and it then ran the very work the user
    /// had just stopped.
    #[tokio::test]
    async fn a_stop_reaches_the_queued_turn_as_well_as_the_running_one() {
        let cancels = Arc::new(CancelState::new());
        let running = cancels.register("s1");
        let queued = cancels.register("s1");

        assert!(cancels.cancel("s1"));
        assert!(running.is_cancelled());
        assert!(queued.is_cancelled(), "the caller waiting for the slot too");

        // Each registration retires only its own: the running turn finishing
        // must not make the queued one unstoppable.
        let still_queued = cancels.register("s1");
        drop(running);
        assert!(cancels.cancel("s1"));
        assert!(still_queued.is_cancelled());
        drop(queued);
        drop(still_queued);
        assert!(!cancels.cancel("s1"), "and the last one out clears the map");
    }

    #[tokio::test]
    async fn turn_with_a_tool_call_records_a_run_with_a_step() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_tool_run.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                Step::Final("the time is now".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );

        rt.handle_input("cli:s1", "hi".into()).await.unwrap();

        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, RunStatus::Done);
        assert_eq!(runs[0].plan, "1 tool call(s)");

        let steps = RunRepository::steps(&*db, &runs[0].id).await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].tool_name, "time");
        assert!(steps[0].ok);

        assert_ledger_matches_log(&db, "cli:s1").await;
    }

    /// A tool that asks before it acts, and reports which answer it got.
    struct Gated;

    #[async_trait]
    impl Tool for Gated {
        fn name(&self) -> &'static str {
            "gated"
        }
        fn description(&self) -> &'static str {
            "asks for approval, then claims to have acted"
        }
        async fn call(
            &self,
            _input: serde_json::Value,
            ctx: &komo_core::domain::context::ToolContext,
        ) -> Result<ToolOutput, ToolError> {
            let request = komo_core::domain::approval::ApprovalRequest::normal("delete the tree");
            let decision = ctx.decide(&request).await;
            match decision.is_allowed() {
                true => Ok(ToolOutput::text("acted")),
                false => Err(ToolError::Denied(
                    decision.feedback().unwrap_or("refused").to_string(),
                )),
            }
        }
    }

    /// An approver that answers "later" — the prompt is out, nobody has
    /// replied.
    struct Suspending;

    #[async_trait]
    impl komo_core::domain::approval::Approver for Suspending {
        async fn decide(
            &self,
            _request: &komo_core::domain::approval::ApprovalRequest,
        ) -> komo_core::domain::approval::Decision {
            komo_core::domain::approval::Decision::Suspend
        }
    }

    /// An approver that must never be consulted.
    struct NeverAsked(Arc<Mutex<usize>>);

    #[async_trait]
    impl komo_core::domain::approval::Approver for NeverAsked {
        async fn decide(
            &self,
            _request: &komo_core::domain::approval::ApprovalRequest,
        ) -> komo_core::domain::approval::Decision {
            *self.0.lock().unwrap() += 1;
            komo_core::domain::approval::Decision::deny()
        }
    }

    pub(crate) fn gated_runtime(
        db: Arc<Db>,
        approver: Arc<dyn komo_core::domain::approval::Approver>,
    ) -> AgentRuntime {
        let (mut rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("gated", "{}")]),
                Step::Final("done".into()),
            ],
            vec![],
            30,
        );
        let mut executor =
            ToolExecutor::new(komo_services::tool_execution::ToolExecutionConfig::default());
        executor.register(Arc::new(Gated));
        rt.tool_executor = executor.with_events(db.clone()).with_approver(approver);
        rt.wakeups = Some(db.clone());
        rt
    }

    /// A gated call whose answer has not arrived stops the turn instead of
    /// holding the session slot — and leaves behind exactly what a
    /// continuation needs: the request on record, the call unsettled, and a
    /// standing wait.
    #[tokio::test]
    async fn a_turn_waiting_on_an_approval_suspends_rather_than_failing() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_suspend.db"))
                .await
                .unwrap(),
        );
        let rt = gated_runtime(db.clone(), Arc::new(Suspending));

        let outcome = rt.handle_input("cli:wait", "delete it".into()).await;
        assert!(
            komo_core::domain::wakeup::is_suspended(&outcome.unwrap_err()),
            "the turn stops as suspended, not as a failure"
        );

        // The ledger says waiting: not running (a restart must not reconcile
        // it), not finished (there is no conclusion).
        let run = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(run.status, RunStatus::Suspended);
        assert!(!run.recoverable, "its return is scheduled, not manual");

        let events = SessionEventRepository::events(&*db, "cli:wait")
            .await
            .unwrap();
        // The wire tag, which is what the assertions below are about.
        let kinds: Vec<String> = events
            .iter()
            .map(|event| {
                serde_json::to_value(&event.kind).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert!(kinds.iter().any(|k| k == "approval/requested"), "{kinds:?}");
        assert!(kinds.iter().any(|k| k == "turn/suspended"), "{kinds:?}");
        assert!(
            !kinds.iter().any(|k| k == "tool/call-settled"),
            "a call that stopped to wait did not happen: {kinds:?}"
        );
        assert!(
            !kinds.iter().any(|k| k == "approval/resolved"),
            "and nobody has answered it: {kinds:?}"
        );
        assert!(
            !kinds.iter().any(|k| k == "assistant/message"),
            "a suspended turn has not answered, and the surface must still end \
             on the user message for the continuation to be one: {kinds:?}"
        );

        // And something is scheduled to come back for it.
        let waits = komo_core::domain::wakeup::WakeupRepository::list(&*db)
            .await
            .unwrap();
        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].turn_id.as_deref(), Some(run.id.as_str()));
        assert_eq!(
            waits[0].wakeup,
            komo_core::domain::session_event::Wakeup::Approval {
                call_id: "id-gated".into()
            }
        );
        assert!(
            waits[0].expires_at.is_some(),
            "a wait nobody answers has to come back and say so"
        );
    }

    /// The answer that arrived while the turn was suspended **is** the answer.
    /// Re-dispatching the call must not ask the user to approve the same action
    /// twice — and for a wait that expired, asking again would park the turn
    /// forever.
    #[tokio::test]
    async fn a_gated_call_honours_the_answer_already_in_the_log() {
        use komo_core::domain::session_event::ApprovalResolvedEvent;

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_answered.db"))
                .await
                .unwrap(),
        );
        let asked = Arc::new(Mutex::new(0usize));
        let rt = gated_runtime(db.clone(), Arc::new(NeverAsked(asked.clone())));

        // The turn opens, and the answer is already on record for the call it
        // is about to make.
        let turn_id = Run::new_id();
        SessionEventRepository::append(
            &*db,
            "cli:answered",
            vec![
                SessionEventKind::TurnStarted {
                    turn_id: turn_id.clone(),
                    resumed_from: None,
                },
                SessionEventKind::ApprovalResolved(ApprovalResolvedEvent {
                    turn_id: turn_id.clone(),
                    call_id: "id-gated".into(),
                    call_index: 0,
                    allowed: true,
                    decided_by: "human".into(),
                    reason: String::new(),
                    waited_ms: 1_000,
                }),
            ],
        )
        .await
        .unwrap();
        SessionEventRepository::durable_flush(&*db, "cli:answered")
            .await
            .unwrap();

        let reply = rt
            .run_ledgered(
                "cli:answered",
                turn_id.clone(),
                TurnKind::Fresh {
                    user_input: "delete it".into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(reply, "done");
        assert_eq!(
            *asked.lock().unwrap(),
            0,
            "the approver must not be asked about an answer that already landed"
        );
        let steps = RunRepository::steps(&*db, &turn_id).await.unwrap();
        assert_eq!(steps.len(), 1);
        assert!(steps[0].ok, "and the call ran: {:?}", steps[0].error);
    }

    /// The headline of the approval rework: the answer arrives after the
    /// process that asked is gone, and the turn still comes back and acts.
    ///
    /// Everything here is a fresh runtime over the same store — which is what a
    /// gateway restart is.
    #[tokio::test]
    async fn an_approval_answered_after_a_restart_resumes_the_turn_and_runs_the_call() {
        use komo_core::domain::session_event::ApprovalResolvedEvent;

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_answered_later.db"))
                .await
                .unwrap(),
        );

        // 1. A turn stops for an approval nobody has answered.
        let rt = gated_runtime(db.clone(), Arc::new(Suspending));
        assert!(
            rt.handle_input("cli:later", "delete it".into())
                .await
                .is_err()
        );
        drop(rt);
        let suspended = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(suspended.status, RunStatus::Suspended);

        // 2. The answer lands — what `/approve` writes.
        SessionEventRepository::append(
            &*db,
            "cli:later",
            vec![SessionEventKind::ApprovalResolved(ApprovalResolvedEvent {
                turn_id: suspended.id.clone(),
                call_id: "id-gated".into(),
                call_index: 0,
                allowed: true,
                decided_by: "human".into(),
                reason: String::new(),
                waited_ms: 90_000,
            })],
        )
        .await
        .unwrap();
        SessionEventRepository::durable_flush(&*db, "cli:later")
            .await
            .unwrap();

        // 3. A new process picks the turn up. The approver here would deny
        //    anything it was asked — it must not be asked.
        let asked = Arc::new(Mutex::new(0usize));
        let rt = gated_runtime(db.clone(), Arc::new(NeverAsked(asked.clone())));
        let reply = rt
            .resume_interrupted(&suspended)
            .await
            .unwrap()
            .expect("a suspended turn ends on the user message, so it is continuable");

        assert_eq!(reply, "done");
        assert_eq!(
            *asked.lock().unwrap(),
            0,
            "the answer was already on record"
        );

        // The continuation is its own run, linked back, and it is the one that
        // ran the call.
        let runs = RunRepository::list(&*db, 10).await.unwrap();
        let continuation = runs
            .iter()
            .find(|r| r.resumed_from.as_deref() == Some(suspended.id.as_str()))
            .expect("the continuation links back to the turn it picked up");
        assert_eq!(continuation.status, RunStatus::Done);
        let steps = RunRepository::steps(&*db, &continuation.id).await.unwrap();
        assert_eq!(steps.len(), 1, "the gated call ran exactly once");
        assert!(steps[0].ok, "{}", steps[0].error);
        assert!(
            RunRepository::steps(&*db, &suspended.id)
                .await
                .unwrap()
                .is_empty(),
            "and the suspended attempt still has no step: it never ran the call"
        );
    }

    /// The waiting is visible. A suspended turn holds no slot and writes no
    /// reply, so without this the conversation reads as idle in every list the
    /// operator has — and the run ledger is not where anyone looks for "which
    /// chat is stuck on me".
    #[tokio::test]
    async fn a_suspended_turn_shows_up_as_the_session_waiting() {
        use komo_core::domain::session_event::{ApprovalResolvedEvent, WakeupKind};

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_awaiting.db"))
                .await
                .unwrap(),
        );

        let rt = gated_runtime(db.clone(), Arc::new(Suspending));
        assert!(
            rt.handle_input("cli:awaiting", "delete it".into())
                .await
                .is_err()
        );
        drop(rt);

        let waiting = SessionRepository::find(&*db, "cli:awaiting")
            .await
            .unwrap()
            .unwrap()
            .awaiting
            .expect("the session is stopped on an approval");
        assert_eq!(waiting.kind, WakeupKind::Approval);
        assert!(
            waiting.expires_at.is_some(),
            "and it says when the question runs out"
        );

        // The answer lands and the turn is picked up again: the badge comes off
        // when the work restarts, not when it next finishes.
        let suspended = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        SessionEventRepository::append(
            &*db,
            "cli:awaiting",
            vec![SessionEventKind::ApprovalResolved(ApprovalResolvedEvent {
                turn_id: suspended.id.clone(),
                call_id: "id-gated".into(),
                call_index: 0,
                allowed: true,
                decided_by: "human".into(),
                reason: String::new(),
                waited_ms: 1_000,
            })],
        )
        .await
        .unwrap();
        let rt = gated_runtime(db.clone(), Arc::new(NeverAsked(Arc::new(Mutex::new(0)))));
        rt.resume_interrupted(&suspended)
            .await
            .unwrap()
            .expect("a suspended turn is continuable");

        assert!(
            SessionRepository::find(&*db, "cli:awaiting")
                .await
                .unwrap()
                .unwrap()
                .awaiting
                .is_none(),
            "nothing is waiting once the continuation has the turn"
        );
    }

    /// Nobody answered. The turn still comes back — and is told so, rather
    /// than being asked the same question again and parking itself forever.
    #[tokio::test]
    async fn an_approval_that_expired_comes_back_as_a_refusal() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_expired.db"))
                .await
                .unwrap(),
        );

        let rt = gated_runtime(db.clone(), Arc::new(Suspending));
        assert!(
            rt.handle_input("cli:expired", "delete it".into())
                .await
                .is_err()
        );
        drop(rt);
        let suspended = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();

        // What the wake writes when the deadline passes.
        SessionEventRepository::append(
            &*db,
            "cli:expired",
            vec![SessionEventKind::ApprovalExpired {
                turn_id: suspended.id.clone(),
                call_id: "id-gated".into(),
                call_index: 0,
            }],
        )
        .await
        .unwrap();
        SessionEventRepository::durable_flush(&*db, "cli:expired")
            .await
            .unwrap();

        let asked = Arc::new(Mutex::new(0usize));
        let rt = gated_runtime(db.clone(), Arc::new(NeverAsked(asked.clone())));
        let reply = rt
            .resume_interrupted(&suspended)
            .await
            .unwrap()
            .expect("the turn is continuable");

        assert_eq!(reply, "done");
        assert_eq!(
            *asked.lock().unwrap(),
            0,
            "an expiry is an answer; asking again would park the turn forever"
        );
        let continuation = RunRepository::list(&*db, 10)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.resumed_from.as_deref() == Some(suspended.id.as_str()))
            .unwrap();
        let steps = RunRepository::steps(&*db, &continuation.id).await.unwrap();
        assert_eq!(steps.len(), 1);
        // A refusal is a *recoverable, terminal* outcome, so it rides back as
        // the model-facing text rather than as a tool failure — but it must say
        // the action did not happen, and why.
        assert!(
            steps[0].result.contains("expired"),
            "the model is told nobody answered: {}",
            steps[0].result
        );
        assert_ne!(steps[0].result, "acted", "and the call did not run");
    }

    // ── the model's own waits (docs/bot-runtime.md §5.7 / §5.8) ──────────────

    /// A runtime whose turn calls one sentinel tool with `args`, then answers.
    /// Rebuilt per "process" in the tests below, which is what a restart is.
    fn waiting_runtime(db: Arc<Db>, tool: Arc<dyn Tool>, args: &str) -> AgentRuntime {
        let name = tool.name();
        let (mut rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call(name, args)]),
                Step::Final("done".into()),
            ],
            vec![],
            30,
        );
        let mut executor =
            ToolExecutor::new(komo_services::tool_execution::ToolExecutionConfig::default());
        executor.register(tool);
        rt.tool_executor = executor.with_events(db.clone());
        rt.wakeups = Some(db.clone());
        rt
    }

    /// A session context somebody is watching, so `ask_user` has an addressee.
    fn watched(session_id: &str, sent: Arc<Mutex<Vec<String>>>) -> SessionContext {
        struct Recording(Arc<Mutex<Vec<String>>>);

        #[async_trait]
        impl komo_core::domain::gateway::ReplySink for Recording {
            async fn send(&self, text: &str) -> anyhow::Result<()> {
                self.0.lock().unwrap().push(text.to_string());
                Ok(())
            }
        }

        SessionContext {
            sink: Arc::new(Recording(sent)),
            interactive: true,
            ..SessionContext::detached(session_id)
        }
    }

    /// What the sweep does when a wait comes due, minus the session slot the
    /// gateway holds: record the wake through the one writer of
    /// `wakeup/fired`, then continue the turn.
    struct TestWaker {
        runtime: Arc<AgentRuntime>,
        waits: crate::interaction::WaitParts,
    }

    #[async_trait]
    impl komo_core::domain::wakeup::WakeupDispatch for TestWaker {
        async fn fire(
            &self,
            registration: &WakeupRegistration,
            cause: komo_core::domain::session_event::WakeupCause,
        ) -> anyhow::Result<()> {
            let turn_id = registration.turn_id.clone().expect("a continuation");
            crate::interaction::record_wake(&self.waits, registration, &turn_id, cause, "").await?;
            let run = self
                .waits
                .runs
                .get(&turn_id)
                .await?
                .expect("the suspended run");
            self.runtime.resume_interrupted(&run).await?;
            Ok(())
        }
    }

    fn wait_parts(db: &Arc<Db>) -> crate::interaction::WaitParts {
        crate::interaction::WaitParts {
            runs: db.clone(),
            events: db.clone(),
            wakeups: db.clone(),
        }
    }

    /// `wait` stops the turn and leaves behind the two records a continuation
    /// needs: the log saying which call is waiting and for what, and a
    /// registration saying when to come back.
    #[tokio::test]
    async fn a_wait_stops_the_turn_and_says_when_to_come_back() {
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_wait.db")).await.unwrap());
        let rt = waiting_runtime(db.clone(), Arc::new(WaitTool), r#"{"until":"2h"}"#);

        let outcome = rt
            .handle_input("cli:timer", "check back in two hours".into())
            .await;
        assert!(
            komo_core::domain::wakeup::is_suspended(&outcome.unwrap_err()),
            "waiting is not failing"
        );

        let run = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(run.status, RunStatus::Suspended);
        assert!(
            RunRepository::steps(&*db, &run.id)
                .await
                .unwrap()
                .is_empty(),
            "a call that stopped to wait did not happen"
        );

        let events = SessionEventRepository::events(&*db, "cli:timer")
            .await
            .unwrap();
        let suspended = events
            .iter()
            .find_map(|event| match &event.kind {
                SessionEventKind::TurnSuspended(s) => Some(s.clone()),
                _ => None,
            })
            .expect("the log says the turn is waiting");
        assert_eq!(suspended.call_id, "id-wait", "and which call is");
        assert!(matches!(suspended.wakeup, Wakeup::At { .. }));
        assert_eq!(suspended.expires_at, None);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e.kind, SessionEventKind::AssistantMessage(_))),
            "a suspended turn has not answered"
        );

        let waits = komo_core::domain::wakeup::WakeupRepository::list(&*db)
            .await
            .unwrap();
        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].turn_id.as_deref(), Some(run.id.as_str()));
        assert_eq!(
            waits[0].expires_at, None,
            "a timer needs no second deadline"
        );
        let Wakeup::At { at } = waits[0].wakeup else {
            panic!("a delay is a timer: {:?}", waits[0].wakeup)
        };
        let now = now();
        assert!(
            (at - now - 7_200).abs() <= 5,
            "two hours from now, give or take the test's own clock"
        );
    }

    /// The headline of `wait`: the process that stopped is gone, the clock
    /// reaches the moment anyway, and the same call comes back — once — with
    /// the wake as its result.
    #[tokio::test]
    async fn a_timer_that_came_due_after_a_restart_continues_the_turn() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_wait_fired.db"))
                .await
                .unwrap(),
        );

        // 1. A turn stops on a timer, and the process ends.
        let rt = waiting_runtime(db.clone(), Arc::new(WaitTool), r#"{"until":"2h"}"#);
        assert!(
            rt.handle_input("cli:fired", "wait for it".into())
                .await
                .is_err()
        );
        drop(rt);
        let suspended = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(suspended.status, RunStatus::Suspended);

        // 2. A new process, and the clock reaches the moment. The sweep claims
        //    the registration, checks the log still says "waiting", and fires.
        let rt = Arc::new(waiting_runtime(
            db.clone(),
            Arc::new(WaitTool),
            r#"{"until":"2h"}"#,
        ));
        let sweep = crate::daemon::CronJobSweep {
            jobs: db.clone(),
            notifier: Arc::new(SilentNotifier),
            wakeups: Some(crate::daemon::WakeupWiring {
                registrations: db.clone(),
                events: db.clone(),
                dispatch: Arc::new(TestWaker {
                    runtime: rt.clone(),
                    waits: wait_parts(&db),
                }),
            }),
            runtime: None,
        };
        let wiring = sweep.wakeups.as_ref().unwrap();
        assert_eq!(
            sweep.fire_due_wakeups(wiring, now() + 7_200).await,
            1,
            "the moment arrived"
        );

        // 3. The continuation ran the call exactly once, and what it returned
        //    is the wake — not another wait.
        let continuation = RunRepository::list(&*db, 10)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.resumed_from.as_deref() == Some(suspended.id.as_str()))
            .expect("the continuation links back");
        assert_eq!(continuation.status, RunStatus::Done);
        let steps = RunRepository::steps(&*db, &continuation.id).await.unwrap();
        assert_eq!(steps.len(), 1, "the wait ran once, on the way back");
        assert!(
            steps[0].result.contains("The wait is over"),
            "the model is told the moment arrived: {}",
            steps[0].result
        );
        assert!(
            RunRepository::steps(&*db, &suspended.id)
                .await
                .unwrap()
                .is_empty(),
            "and the attempt that stopped still has no step"
        );
        assert!(
            komo_core::domain::wakeup::WakeupRepository::list(&*db)
                .await
                .unwrap()
                .is_empty(),
            "a fired wait is retired, not left to fire again"
        );
    }

    /// `ask_user` is the same primitive with a person on the other end: the
    /// question goes out, the turn stops, and the answer — arriving in another
    /// process — comes back as that call's result.
    #[tokio::test]
    async fn a_question_answered_after_a_restart_comes_back_as_the_answer() {
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_ask.db")).await.unwrap());
        let asked = Arc::new(Mutex::new(Vec::new()));

        let rt = waiting_runtime(
            db.clone(),
            Arc::new(AskUserTool::new()),
            r#"{"question":"红的还是蓝的?"}"#,
        );
        let outcome = with_session(
            watched("cli:ask", asked.clone()),
            rt.handle_input("cli:ask", "买一个".into()),
        )
        .await;
        assert!(komo_core::domain::wakeup::is_suspended(
            &outcome.unwrap_err()
        ));
        assert!(asked.lock().unwrap()[0].contains("红的还是蓝的"));
        drop(rt);

        let suspended = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(suspended.status, RunStatus::Suspended);
        let waits = komo_core::domain::wakeup::WakeupRepository::list(&*db)
            .await
            .unwrap();
        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].wakeup, Wakeup::UserReply);
        assert!(
            waits[0].expires_at.is_some(),
            "a question nobody answers has to come back and say so"
        );

        // The user answers — in a process that never asked.
        let parts = wait_parts(&db);
        crate::interaction::record_wake(
            &parts,
            &waits[0],
            &suspended.id,
            komo_core::domain::session_event::WakeupCause::Reply,
            "蓝的",
        )
        .await
        .unwrap();
        let rt = waiting_runtime(
            db.clone(),
            Arc::new(AskUserTool::new()),
            r#"{"question":"红的还是蓝的?"}"#,
        );
        let reply = with_session(
            watched("cli:ask", asked.clone()),
            rt.resume_interrupted(&suspended),
        )
        .await
        .unwrap()
        .expect("a suspended turn ends on the user message, so it is continuable");
        assert_eq!(reply, "done");

        let continuation = RunRepository::list(&*db, 10)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.resumed_from.as_deref() == Some(suspended.id.as_str()))
            .unwrap();
        let steps = RunRepository::steps(&*db, &continuation.id).await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].result, "User answered: 蓝的");
        assert_eq!(
            asked.lock().unwrap().len(),
            1,
            "and the question was not asked a second time"
        );
    }

    /// Seven days of silence. The turn still comes back, and is told nobody
    /// answered — a question that simply vanished would leave the model
    /// waiting on an answer that is never coming.
    #[tokio::test]
    async fn a_question_nobody_answered_comes_back_saying_so() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_ask_expired.db"))
                .await
                .unwrap(),
        );
        let asked = Arc::new(Mutex::new(Vec::new()));
        let rt = waiting_runtime(
            db.clone(),
            Arc::new(AskUserTool::new()),
            r#"{"question":"哪一个?"}"#,
        );
        assert!(
            with_session(
                watched("cli:silent", asked.clone()),
                rt.handle_input("cli:silent", "帮我改一下".into()),
            )
            .await
            .is_err()
        );
        drop(rt);
        let suspended = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();

        // The deadline passes: the sweep fires it as expired rather than
        // dropping the row.
        let rt = Arc::new(waiting_runtime(
            db.clone(),
            Arc::new(AskUserTool::new()),
            r#"{"question":"哪一个?"}"#,
        ));
        let sweep = crate::daemon::CronJobSweep {
            jobs: db.clone(),
            notifier: Arc::new(SilentNotifier),
            wakeups: Some(crate::daemon::WakeupWiring {
                registrations: db.clone(),
                events: db.clone(),
                dispatch: Arc::new(TestWaker {
                    runtime: rt.clone(),
                    waits: wait_parts(&db),
                }),
            }),
            runtime: None,
        };
        let wiring = sweep.wakeups.as_ref().unwrap();
        assert_eq!(sweep.fire_due_wakeups(wiring, now() + 8 * 86_400).await, 1);

        let events = SessionEventRepository::events(&*db, "cli:silent")
            .await
            .unwrap();
        assert!(
            events.iter().any(|event| matches!(
                &event.kind,
                SessionEventKind::WakeupFired(fired)
                    if fired.cause == komo_core::domain::session_event::WakeupCause::Expired
            )),
            "the expiry is on record, not silent"
        );
        let continuation = RunRepository::list(&*db, 10)
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.resumed_from.as_deref() == Some(suspended.id.as_str()))
            .unwrap();
        let steps = RunRepository::steps(&*db, &continuation.id).await.unwrap();
        assert_eq!(steps.len(), 1);
        assert!(
            steps[0].result.starts_with("No answer from the user"),
            "the model is told to proceed on an assumption: {}",
            steps[0].result
        );
    }

    struct SilentNotifier;

    #[async_trait]
    impl komo_core::domain::notify::Notifier for SilentNotifier {
        async fn notify(&self, _title: &str, _body: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// The turn has to be in the ledger *before* it ends, or a crash leaves
    /// nothing for `run list` to show and nothing for `run resume` to pick up.
    /// The rows are a projection now, and this is the one commit that happens
    /// while the turn is still running.
    #[tokio::test]
    async fn a_turn_is_in_the_ledger_while_it_is_still_running() {
        /// Reads the ledger from inside the turn that is writing it.
        struct Peek(Arc<Db>, Arc<Mutex<Vec<Run>>>);
        #[async_trait]
        impl Tool for Peek {
            fn name(&self) -> &'static str {
                "peek"
            }
            fn description(&self) -> &'static str {
                "reads the run ledger mid-turn"
            }
            async fn call(
                &self,
                _input: serde_json::Value,
                _ctx: &komo_core::domain::context::ToolContext,
            ) -> Result<ToolOutput, ToolError> {
                let runs = RunRepository::list(&*self.0, 10).await.unwrap();
                *self.1.lock().unwrap() = runs;
                Ok(ToolOutput::text("peeked"))
            }
        }

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_open_row.db"))
                .await
                .unwrap(),
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("peek", "{}")]),
                Step::Final("done".into()),
            ],
            vec![Arc::new(Peek(db.clone(), seen.clone()))],
            30,
        );

        rt.handle_input("cli:open", "go".into()).await.unwrap();

        let mid_turn = seen.lock().unwrap().clone();
        assert_eq!(mid_turn.len(), 1, "the open turn is already a row");
        assert_eq!(mid_turn[0].input, "go");
        assert_eq!(mid_turn[0].status, RunStatus::Running);
        assert!(
            mid_turn[0].recoverable,
            "an unterminated turn is what a crash leaves behind, and it is resumable"
        );
        // And the finished turn overwrites it rather than adding a second row.
        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, RunStatus::Done);
        assert!(!runs[0].recoverable);
        assert_ledger_matches_log(&db, "cli:open").await;
    }

    /// Assert the run ledger rows for `session_id` are exactly what folding its
    /// event log produces.
    ///
    /// The claim the projection rests on: the rows are a query index, so if the
    /// fold disagrees with the writer on a real turn, dropping the authoritative
    /// write loses whatever the two disagree about. Called from the tests that
    /// produce each turn shape rather than from one fixture of its own — a
    /// cancel, a failure and a tool round exercise different writer paths.
    async fn assert_ledger_matches_log(db: &Db, session_id: &str) {
        use komo_core::domain::run_projection::project_runs;

        let events = SessionEventRepository::events(db, session_id)
            .await
            .unwrap();
        let projected = project_runs(session_id, &events);
        let written: Vec<_> = RunRepository::list(db, 50)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.session_id == session_id)
            .rev()
            .collect();

        assert_eq!(projected.len(), written.len(), "run count");
        for folded in projected.iter() {
            // Paired by id, not by position: turns inside one second tie on
            // `started_at`, and the row order within a tie is the query's.
            let row = written
                .iter()
                .find(|row| row.id == folded.run.id)
                .unwrap_or_else(|| panic!("no row for folded run {}", folded.run.id));
            let run = &folded.run;
            assert_eq!(run.session_id, row.session_id);
            assert_eq!(run.input, row.input, "input of {}", row.id);
            assert_eq!(run.plan, row.plan, "plan of {}", row.id);
            assert_eq!(run.status, row.status, "status of {}", row.id);
            assert_eq!(run.final_output, row.final_output, "reply of {}", row.id);
            assert_eq!(run.error, row.error, "error of {}", row.id);
            assert_eq!(
                run.recoverable, row.recoverable,
                "recoverable of {}",
                row.id
            );
            assert_eq!(run.tokens_in, row.tokens_in, "tokens of {}", row.id);
            assert_eq!(run.tokens_out, row.tokens_out);
            assert_eq!(run.tokens_cached, row.tokens_cached);
            assert_eq!(run.resumed_from, row.resumed_from);
            assert_eq!(run.memories, row.memories);
            assert_eq!(run.learned, row.learned, "watermark of {}", row.id);
            // Exactly the same stamps, not merely close ones. They used to
            // differ by the append between them, because the row took `now()`
            // while the fold took the bracketing events' own timestamps — that
            // divergence *was* the double write, and it went away with it.
            assert_eq!(run.started_at, row.started_at, "started_at of {}", row.id);
            assert_eq!(run.ended_at, row.ended_at, "ended_at of {}", row.id);

            // The rows are exactly the calls that *settled*. A call the turn
            // died inside has no row at all — the step is written at settle —
            // which is the fact the log keeps and the ledger cannot.
            let rows = RunRepository::steps(db, &row.id).await.unwrap();
            let settled: Vec<_> = folded.steps.iter().filter(|s| s.settled).collect();
            assert_eq!(
                settled.len(),
                rows.len(),
                "settled step count of {}",
                row.id
            );
            for (folded, row) in settled.into_iter().map(|s| &s.step).zip(&rows) {
                assert_eq!(folded.run_id, row.run_id);
                assert_eq!(folded.seq, row.seq);
                assert_eq!(folded.tool_name, row.tool_name);
                assert_eq!(folded.args, row.args);
                assert_eq!(folded.result, row.result);
                assert_eq!(folded.error, row.error);
                assert_eq!(folded.ok, row.ok);
                assert_eq!(folded.uncertain, row.uncertain);
                assert_eq!(folded.elapsed_ms, row.elapsed_ms);
                assert_eq!(folded.structured, row.structured);
                assert_eq!(folded.output_paths, row.output_paths);
            }
        }
    }

    /// Wraps the real store, reports every turn as a roll, and records the
    /// retention floor the runtime computed instead of cutting.
    /// Records which shape of read the runtime asked the log for.
    struct ReadSpy {
        inner: Arc<Db>,
        reads: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SessionEventRepository for ReadSpy {
        async fn session_ids(&self) -> anyhow::Result<Vec<String>> {
            self.inner.session_ids().await
        }

        async fn surface(
            &self,
            session_id: &str,
        ) -> anyhow::Result<Option<komo_core::domain::session_event::SurfaceProjection>> {
            self.inner.surface(session_id).await
        }

        async fn append(
            &self,
            session_id: &str,
            kinds: Vec<SessionEventKind>,
        ) -> anyhow::Result<Vec<SessionEvent>> {
            self.inner.append(session_id, kinds).await
        }
        async fn durable_flush(&self, session_id: &str) -> anyhow::Result<()> {
            self.inner.durable_flush(session_id).await
        }
        async fn events(&self, session_id: &str) -> anyhow::Result<Vec<SessionEvent>> {
            self.reads.lock().unwrap().push("whole log".to_string());
            self.inner.events(session_id).await
        }
        async fn events_from(
            &self,
            session_id: &str,
            seq: u64,
        ) -> anyhow::Result<Vec<SessionEvent>> {
            self.reads.lock().unwrap().push(format!("from {seq}"));
            self.inner.events_from(session_id, seq).await
        }
        async fn turn_boundary(&self, session_id: &str) -> anyhow::Result<bool> {
            self.inner.turn_boundary(session_id).await
        }
        async fn retain(&self, session_id: &str, keep_from: u64) -> anyhow::Result<Option<u64>> {
            self.inner.retain(session_id, keep_from).await
        }
    }

    /// A session that has been talking for a while must not re-fold itself
    /// every turn. A turn settles from where it opened; the whole log is read
    /// only when a segment rolls, which is once per segment's worth of writing.
    #[tokio::test]
    async fn a_turn_settles_from_its_own_start_not_the_whole_log() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_tail_settle.db"))
                .await
                .unwrap(),
        );
        let reads = Arc::new(Mutex::new(Vec::new()));
        // The scripted driver is handed its whole script on the first turn, so
        // each turn gets its own runtime over the same store.
        for text in ["one", "two", "three"] {
            let (mut rt, _) =
                scripted_runtime(db.clone(), vec![Step::Final(text.into())], vec![], 30);
            rt.events = Arc::new(ReadSpy {
                inner: db.clone(),
                reads: reads.clone(),
            });
            rt.handle_input("cli:tail", text.into()).await.unwrap();
        }

        let reads = reads.lock().unwrap().clone();
        assert!(
            !reads.iter().any(|read| read == "whole log"),
            "no roll happened, so nothing had to read the whole log: {reads:?}"
        );
        // Five events a turn here: `turn/started`, the user message, the one
        // assistant round, the assistant message, `turn/completed`.
        assert_eq!(
            reads,
            vec!["from 0", "from 5", "from 10"],
            "each settle starts where its own turn did"
        );
        assert_eq!(RunRepository::list(&*db, 10).await.unwrap().len(), 3);
        assert_ledger_matches_log(&db, "cli:tail").await;
    }

    struct RetentionSpy {
        inner: Arc<Db>,
        floors: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl SessionEventRepository for RetentionSpy {
        async fn session_ids(&self) -> anyhow::Result<Vec<String>> {
            self.inner.session_ids().await
        }

        async fn surface(
            &self,
            session_id: &str,
        ) -> anyhow::Result<Option<komo_core::domain::session_event::SurfaceProjection>> {
            self.inner.surface(session_id).await
        }

        async fn append(
            &self,
            session_id: &str,
            kinds: Vec<SessionEventKind>,
        ) -> anyhow::Result<Vec<SessionEvent>> {
            self.inner.append(session_id, kinds).await
        }
        async fn durable_flush(&self, session_id: &str) -> anyhow::Result<()> {
            self.inner.durable_flush(session_id).await
        }
        async fn events(&self, session_id: &str) -> anyhow::Result<Vec<SessionEvent>> {
            self.inner.events(session_id).await
        }
        async fn events_from(
            &self,
            session_id: &str,
            seq: u64,
        ) -> anyhow::Result<Vec<SessionEvent>> {
            self.inner.events_from(session_id, seq).await
        }
        async fn turn_boundary(&self, _session_id: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn retain(&self, _session_id: &str, keep_from: u64) -> anyhow::Result<Option<u64>> {
            self.floors.lock().unwrap().push(keep_from);
            Ok(None)
        }
    }

    /// A conversation longer than its window keeps what fell out of it, as a
    /// summary standing where those messages did — and the log still holds
    /// them, which is what a human transcript reads.
    #[tokio::test]
    async fn a_conversation_past_its_window_is_compacted_into_a_summary() {
        struct FixedAux(&'static str);
        #[async_trait]
        impl LlmClient for FixedAux {
            async fn complete(&self, _session: &Session) -> anyhow::Result<String> {
                Ok(self.0.to_string())
            }
        }

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_compaction.db"))
                .await
                .unwrap(),
        );
        const SUMMARY: &str = "earlier: five questions about the log";
        // Six: small enough that five exchanges outgrow it, big enough to hold
        // the summary plus what stays verbatim.
        const WINDOW: usize = 6;
        let compactor = Arc::new(crate::compaction::Compactor::new(
            Arc::new(FixedAux(SUMMARY)),
            db.clone(),
            WINDOW,
        ));
        // The scripted driver takes its whole script on the first turn, so each
        // turn gets its own runtime over the same store.
        for i in 0..5 {
            let (mut rt, _) = scripted_runtime(
                db.clone(),
                vec![Step::Final(format!("answer {i}"))],
                vec![],
                30,
            );
            rt.compaction = Some(compactor.clone());
            rt.handle_input("cli:long", format!("question {i}"))
                .await
                .unwrap();
        }

        // What the *model* replays: the window, not the whole surface. The
        // summary has to be inside it, or compaction bought nothing.
        let session = SessionRepository::find_windowed(&*db, "cli:long", WINDOW)
            .await
            .unwrap()
            .unwrap();
        let history: Vec<(Role, String)> = session
            .messages
            .iter()
            .map(|m| (m.role.clone(), m.content.clone()))
            .collect();
        assert_eq!(
            history[0],
            (Role::User, SUMMARY.to_string()),
            "the summary stands where the messages it covers did"
        );
        assert!(
            !history.iter().any(|(_, text)| text == "question 0"),
            "and the model no longer replays them"
        );
        assert!(
            history.iter().any(|(_, text)| text == "answer 4"),
            "while the newest exchanges stay verbatim"
        );
        // The invariant a replacement is easiest to break: a summary is a user
        // message, so what follows it has to be the assistant's side.
        for pair in history.windows(2) {
            assert_ne!(
                (&pair[0].0, &pair[1].0),
                (&Role::User, &Role::User),
                "two user messages in a row: {history:?}"
            );
        }

        // Nothing was rewritten: what the summary covers is still in the log.
        let events = SessionEventRepository::events(&*db, "cli:long")
            .await
            .unwrap();
        assert!(
            events.iter().any(|event| matches!(
                &event.kind,
                SessionEventKind::UserMessage(m) if m.content == "question 0"
            )),
            "a human transcript still shows what the summary replaced"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.kind, SessionEventKind::CompactionCompleted { .. }))
                .count(),
            1,
            "one compaction, on the turn that pushed the surface past the window"
        );
    }

    #[tokio::test]
    async fn retention_will_not_cut_into_a_turn_nobody_has_finished_with() {
        // Space never outranks a turn that is still resumable or still
        // unlearned. The floor is the oldest such turn's own start, so a session
        // that has never been learned from cannot be cut at all — and once the
        // sweep retires that turn, the floor moves up to the next one.
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_retain")).await.unwrap());
        let floors = Arc::new(Mutex::new(Vec::new()));
        // The scripted driver is handed the whole script on its first turn, so
        // each turn here gets its own runtime over the same store.
        let spy = |floors: &Arc<Mutex<Vec<u64>>>| RetentionSpy {
            inner: db.clone(),
            floors: floors.clone(),
        };
        let (mut rt, _) = scripted_runtime(db.clone(), vec![Step::Final("one".into())], vec![], 30);
        rt.events = Arc::new(spy(&floors));
        rt.handle_input("cli:s-retain", "hi".into()).await.unwrap();
        assert_eq!(
            floors.lock().unwrap().as_slice(),
            &[0],
            "the turn that just ran is unlearned, so nothing below its start may go"
        );

        // The sweep retires it; now only the next turn holds the floor.
        let first = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        RunRepository::mark_learned(&*db, &[first.id.clone()])
            .await
            .unwrap();
        floors.lock().unwrap().clear();
        let (mut rt, _) = scripted_runtime(db.clone(), vec![Step::Final("two".into())], vec![], 30);
        rt.events = Arc::new(spy(&floors));
        rt.handle_input("cli:s-retain", "again".into())
            .await
            .unwrap();
        assert!(
            floors.lock().unwrap()[0] > 0,
            "a learned turn no longer pins the floor at the start of the log, got {:?}",
            floors.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn a_turn_that_grew_the_log_past_a_segment_seals_it_on_its_way_out() {
        // Segments are retention's unit of deletion, so one may only be cut
        // where a turn ended. Nothing sealed them at all until this seam
        // existed, which left every session as one file that grows forever and
        // gave retention no candidate to ever consider.
        let home = std::env::temp_dir().join("komo-test-komo_rt_seal");
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_seal")).await.unwrap());
        let (rt, _) = scripted_runtime(db.clone(), vec![Step::Final("ok".into())], vec![], 30);

        // One turn whose own user message is bigger than a segment.
        let big = "x".repeat(1024 * 1024 + 1024);
        rt.handle_input("cli:s-seal", big).await.unwrap();

        // The directory name is an encoding of the session id, so the segment
        // is found by walking rather than by rebuilding that encoding here.
        let sessions = std::fs::read_dir(home.join("sessions"))
            .expect("the session log directory")
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        assert_eq!(sessions.len(), 1);
        let segments = std::fs::read_dir(sessions[0].path())
            .expect("segments")
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".jsonl"))
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            segments.contains("000001.jsonl"),
            "the turn boundary should have opened a second segment, found {segments:?}"
        );
        // And the log still reads as one conversation across the two files.
        let session = SessionRepository::find(&*db, "cli:s-seal")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.messages.len(), 2);
        assert_eq!(session.messages[1].content, "ok");
    }

    #[tokio::test]
    async fn turn_without_tools_records_a_run_without_steps() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_direct_run.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![Step::Final("hello there".into())],
            vec![],
            30,
        );

        let reply = rt.handle_input("cli:s2", "hi".into()).await.unwrap();
        assert_eq!(reply, "hello there");

        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, RunStatus::Done);
        assert_eq!(runs[0].plan, "respond");
        assert_eq!(runs[0].final_output, "hello there");

        let steps = RunRepository::steps(&*db, &runs[0].id).await.unwrap();
        assert!(steps.is_empty());

        assert_ledger_matches_log(&db, "cli:s2").await;
    }

    #[tokio::test]
    async fn multi_round_threads_tool_results_back() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_threading.db"))
                .await
                .unwrap(),
        );
        let (rt, received) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("echo", "A")]),
                tool_calls(vec![call("echo", "B")]),
                Step::Final("done".into()),
            ],
            vec![Arc::new(EchoArgsTool)],
            30,
        );

        let reply = rt.handle_input("cli:s3", "hi".into()).await.unwrap();
        assert_eq!(reply, "done");

        let rec = received.lock().unwrap();
        assert_eq!(rec.len(), 2, "two tool rounds before the final answer");
        assert_eq!(rec[0][0].content, "echo:A");
        assert_eq!(rec[0][0].id, "id-echo");
        assert_eq!(rec[1][0].content, "echo:B");
    }

    #[tokio::test]
    async fn tool_error_feeds_back_without_aborting() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_toolerr.db"))
                .await
                .unwrap(),
        );
        let (rt, received) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("fail", "{}")]),
                Step::Final("recovered".into()),
            ],
            vec![Arc::new(FailTool)],
            30,
        );

        let reply = rt.handle_input("cli:s4", "hi".into()).await.unwrap();
        assert_eq!(reply, "recovered");
        assert!(received.lock().unwrap()[0][0].content.contains("failed"));

        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs[0].status, RunStatus::Done);
    }

    #[tokio::test]
    async fn unknown_tool_feeds_back_without_aborting() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_unknown.db"))
                .await
                .unwrap(),
        );
        let (rt, received) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("nope", "{}")]),
                Step::Final("ok".into()),
            ],
            vec![],
            30,
        );

        let reply = rt.handle_input("cli:s5", "hi".into()).await.unwrap();
        assert_eq!(reply, "ok");
        assert!(
            received.lock().unwrap()[0][0]
                .content
                .contains("unknown tool")
        );
    }

    /// An LLM whose turn always fails — stands in for a dead provider / a
    /// completion timeout.
    struct FailingLlm;
    #[async_trait]
    impl LlmClient for FailingLlm {
        async fn complete(&self, _session: &Session) -> anyhow::Result<String> {
            anyhow::bail!("provider down")
        }
        async fn begin_turn(
            &self,
            _session: &Session,
            _deltas: Option<Arc<dyn DeltaSink>>,
            _recorder: Option<Arc<dyn TurnRecorder>>,
        ) -> anyhow::Result<Box<dyn TurnDriver>> {
            anyhow::bail!("provider down")
        }
    }

    #[tokio::test]
    async fn failed_turn_persists_an_assistant_placeholder_for_alternation() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_failed_turn.db"))
                .await
                .unwrap(),
        );
        let rt = AgentRuntime {
            llm: Arc::new(FailingLlm),
            sessions: db.clone(),
            messages: db.clone(),
            events: db.clone(),
            projection: db.clone(),
            runs: db.clone(),
            tool_executor: ToolExecutor::new(
                komo_services::tool_execution::ToolExecutionConfig::default(),
            ),
            max_turns: 30,
            history_window: 0,
            learning: None,
            compaction: None,
            wakeups: None,
            checkpoint: None,
            turn_hooks: Vec::new(),
            step_hooks: Vec::new(),
        };

        let result = rt.handle_input("cli:sf", "hi".into()).await;
        assert!(result.is_err(), "the turn must surface the failure");

        // The transcript must still alternate user → assistant, so the next
        // turn's history doesn't hold two consecutive user messages.
        let session = SessionRepository::find(&*db, "cli:sf")
            .await
            .unwrap()
            .unwrap();
        let roles: Vec<Role> = session.messages.iter().map(|m| m.role.clone()).collect();
        assert_eq!(roles, vec![Role::User, Role::Assistant]);
        assert!(session.messages[1].content.contains("处理失败"));

        // The run is recorded as failed.
        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs[0].status, RunStatus::Failed);

        assert_ledger_matches_log(&db, "cli:sf").await;
    }

    #[tokio::test]
    async fn empty_final_answer_gets_a_fallback() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_empty_final.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(db.clone(), vec![Step::Final("   ".into())], vec![], 30);
        let reply = rt.handle_input("cli:se", "hi".into()).await.unwrap();
        assert_eq!(reply, EMPTY_REPLY_FALLBACK);
    }

    /// #5: what the turn cost is recorded on the run, so `komo run list` can price
    /// a conversation — and how much of the prompt the provider's cache served,
    /// which is the only way to tell a prompt change that broke prefix
    /// stability from one that didn't. 0 stays reserved for "the provider told
    /// us nothing".
    #[tokio::test]
    async fn a_finished_turn_records_its_token_usage_and_cache_hits() {
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_tokens.db")).await.unwrap());
        let (rt, _) = scripted_runtime(db.clone(), vec![Step::Final("hi".into())], vec![], 30);
        rt.handle_input("cli:tok", "hello".into()).await.unwrap();

        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs[0].tokens_in, 1_200);
        assert_eq!(runs[0].tokens_out, 340);
        assert_eq!(runs[0].tokens_cached, 900);
    }

    // ── Between-round hooks (domain::hooks::StepHook) ────────────────────────

    struct ScriptedStepHook {
        label: &'static str,
        decision: StepDecision,
        rounds: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl StepHook for ScriptedStepHook {
        fn name(&self) -> &'static str {
            self.label
        }
        async fn pre_step(&self, _session_id: &str, round: usize) -> StepDecision {
            self.rounds.lock().unwrap().push(round);
            self.decision.clone()
        }
    }

    fn step_hook(
        label: &'static str,
        decision: StepDecision,
    ) -> (Arc<ScriptedStepHook>, Arc<Mutex<Vec<usize>>>) {
        let rounds = Arc::new(Mutex::new(Vec::new()));
        let hook = Arc::new(ScriptedStepHook {
            label,
            decision,
            rounds: rounds.clone(),
        });
        (hook, rounds)
    }

    /// Injected text reaches the model on the round it was produced for, by the
    /// same channel a user's mid-turn message uses — which is what makes it
    /// append-only, and so free of any cost to the provider's cached prefix.
    #[tokio::test]
    async fn a_step_hook_injects_context_into_the_next_round() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_step_inject.db"))
                .await
                .unwrap(),
        );
        let (rt, _received, interjected) = scripted_runtime_seeing_interjections(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                Step::Final("noted".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );
        let (hook, rounds) = step_hook("reminder", StepDecision::Inject("budget is tight".into()));
        let rt = AgentRuntime {
            step_hooks: vec![hook],
            ..rt
        };

        let reply = rt.handle_input("cli:step", "go".into()).await.unwrap();
        assert_eq!(reply, "noted");
        assert_eq!(
            interjected.lock().unwrap().clone(),
            vec!["budget is tight"],
            "the hook's text must reach the model mid-turn"
        );
        // Called once, before the round that fed the first results back — never
        // before the opening round, whose context is assembled elsewhere.
        assert_eq!(rounds.lock().unwrap().clone(), vec![1]);
    }

    /// What a hook said is not something the user said: it reaches the model,
    /// and it stays out of the stored user message.
    #[tokio::test]
    async fn injected_context_does_not_become_part_of_the_user_message() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_step_transcript.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                Step::Final("done".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );
        let (hook, _) = step_hook("reminder", StepDecision::Inject("a hook said this".into()));
        let rt = AgentRuntime {
            step_hooks: vec![hook],
            ..rt
        };
        rt.handle_input("cli:steptx", "go".into()).await.unwrap();

        let messages = MessageRepository::list_by_session(&*db, "cli:steptx")
            .await
            .unwrap();
        assert_eq!(
            messages[0].content, "go",
            "the user said only what they said"
        );
        assert!(!messages[0].content.contains("a hook said this"));
    }

    /// A `Stop` ends the turn with an answer, the way the round budget does —
    /// not with an error, and without driving another round.
    #[tokio::test]
    async fn a_step_hook_can_stop_the_turn_with_an_answer() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_step_stop.db"))
                .await
                .unwrap(),
        );
        // The script has a second round the loop must never reach: reaching it
        // would panic ("script exhausted" is the other way round — here the
        // extra step simply proves it was not consumed).
        let (rt, received) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                Step::Final("should not be reached".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );
        let (hook, _) = step_hook("guard", StepDecision::Stop("stopping here".into()));
        let rt = AgentRuntime {
            step_hooks: vec![hook],
            ..rt
        };

        let reply = rt.handle_input("cli:stepstop", "go".into()).await.unwrap();
        assert_eq!(reply, "stopping here");
        assert!(
            received.lock().unwrap().is_empty(),
            "the stopped round must never reach the model"
        );

        // The turn is a normal, completed run — a hook stopping a turn is a
        // decision, not a failure.
        let run = RunRepository::list(&*db, 10).await.unwrap().pop().unwrap();
        assert_eq!(run.status, RunStatus::Done);
    }

    /// Order matters: every hook's text is delivered, and the first `Stop`
    /// short-circuits the ones after it.
    #[tokio::test]
    async fn hooks_run_in_order_and_the_first_stop_wins() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_step_order.db"))
                .await
                .unwrap(),
        );
        let (rt, _received, interjected) = scripted_runtime_seeing_interjections(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                Step::Final("unreached".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );
        let (first, _) = step_hook("first", StepDecision::Inject("one".into()));
        let (second, _) = step_hook("second", StepDecision::Stop("halt".into()));
        let (third, third_rounds) = step_hook("third", StepDecision::Inject("three".into()));
        let rt = AgentRuntime {
            step_hooks: vec![first, second, third],
            ..rt
        };

        let reply = rt.handle_input("cli:steporder", "go".into()).await.unwrap();
        assert_eq!(reply, "halt");
        assert!(
            third_rounds.lock().unwrap().is_empty(),
            "a hook after the stop must not run"
        );
        assert!(
            interjected.lock().unwrap().is_empty(),
            "a stopped round delivers nothing to the model"
        );
    }

    /// An empty injection is a no-op, not an empty line in front of the model.
    #[tokio::test]
    async fn an_empty_injection_changes_nothing() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_step_empty.db"))
                .await
                .unwrap(),
        );
        let (rt, _received, interjected) = scripted_runtime_seeing_interjections(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                Step::Final("fine".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );
        let (hook, _) = step_hook("quiet", StepDecision::Inject("   ".into()));
        let rt = AgentRuntime {
            step_hooks: vec![hook],
            ..rt
        };

        assert_eq!(
            rt.handle_input("cli:stepempty", "go".into()).await.unwrap(),
            "fine"
        );
        assert!(interjected.lock().unwrap().is_empty());
    }

    /// A turn dispatches against the catalog as it stood when the turn began.
    ///
    /// The model was handed one set of schemas; if a plugin unmounts a tool
    /// mid-turn, the call the model was invited to make must still run rather
    /// than come back "unknown tool" a round later. The mutation is not lost —
    /// it lands in the catalog and the next turn sees it.
    #[tokio::test]
    async fn a_turn_keeps_dispatching_against_the_catalog_it_started_with() {
        use komo_core::domain::catalog::Registration;

        /// Unmounts itself the first time it is called — the sharpest version
        /// of "the catalog changed mid-turn", since the change happens inside
        /// the very round that is running.
        struct SelfUnmounting(Mutex<Option<Registration>>);
        #[async_trait]
        impl Tool for SelfUnmounting {
            fn name(&self) -> &'static str {
                "vanishing"
            }
            fn description(&self) -> &'static str {
                "unmounts itself when called"
            }
            async fn call(
                &self,
                _input: serde_json::Value,
                _ctx: &komo_core::domain::context::ToolContext,
            ) -> Result<ToolOutput, ToolError> {
                // Dropping the registration takes it out of the catalog.
                drop(self.0.lock().unwrap().take());
                Ok(ToolOutput::text("still here"))
            }
        }

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_catalog_pin.db"))
                .await
                .unwrap(),
        );
        let (rt, received) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("vanishing", "{}")]),
                tool_calls(vec![call("vanishing", "{}")]),
                Step::Final("done".into()),
            ],
            vec![],
            30,
        );

        let catalog = rt.tool_executor.catalog().clone();
        let tool = Arc::new(SelfUnmounting(Mutex::new(None)));
        let registration = catalog.mount(tool.clone());
        *tool.0.lock().unwrap() = Some(registration);
        assert_eq!(catalog.snapshot().len(), 1);

        let reply = rt.handle_input("cli:pin", "go".into()).await.unwrap();
        assert_eq!(reply, "done");

        // Both rounds reached the tool, including the one that ran after it had
        // already removed itself.
        let rounds = received.lock().unwrap();
        assert_eq!(rounds[0][0].content, "still here");
        assert_eq!(
            rounds[1][0].content, "still here",
            "the turn's view is pinned; the unmount takes effect next turn"
        );

        // And the unmount really happened — the next turn would not see it.
        assert!(catalog.snapshot().is_empty(), "the catalog itself moved on");
    }

    /// #3: the turn's tool activity is folded onto the assistant message, so the
    /// next turn knows tools ran — while the user-visible reply stays the reply.
    #[tokio::test]
    async fn a_tool_turn_leaves_a_note_for_the_next_turn() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_tool_note.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("echo", "hello")]),
                Step::Final("it said hello".into()),
            ],
            vec![Arc::new(EchoArgsTool)],
            30,
        );
        rt.handle_input("cli:note", "echo something".into())
            .await
            .unwrap();

        let messages = MessageRepository::list_by_session(&*db, "cli:note")
            .await
            .unwrap();
        let assistant = messages.last().unwrap();
        assert_eq!(assistant.content, "it said hello", "reply stays clean");
        assert!(
            assistant.tool_note.contains("echo"),
            "the note should name the tool: {:?}",
            assistant.tool_note
        );
    }

    #[tokio::test]
    async fn a_tool_less_turn_leaves_no_note() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_no_note.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![Step::Final("just talk".into())],
            vec![],
            30,
        );
        rt.handle_input("cli:nonote", "hi".into()).await.unwrap();

        let messages = MessageRepository::list_by_session(&*db, "cli:nonote")
            .await
            .unwrap();
        assert!(messages.last().unwrap().tool_note.is_empty());
    }

    /// #6: text the model wrote alongside its tool calls reaches a watching
    /// client. Nothing else in komo surfaces the model's mid-turn reasoning.
    #[tokio::test]
    async fn narration_alongside_tool_calls_reaches_the_event_sink() {
        use komo_core::domain::events::ToolEventSink;

        #[derive(Default)]
        struct Captured(Mutex<Vec<TurnEvent>>);
        impl ToolEventSink for Captured {
            fn emit(&self, event: TurnEvent) {
                self.0.lock().unwrap().push(event);
            }
        }

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_narration.db"))
                .await
                .unwrap(),
        );
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![
                Step::ToolCalls {
                    calls: vec![call("time", "{}")],
                    text: "Checking the clock first.".into(),
                },
                Step::Final("it is late".into()),
            ],
            vec![Arc::new(TimeTool)],
            30,
        );

        let sink = Arc::new(Captured::default());
        let ctx = SessionContext::detached("cli:narr").with_event_sink(sink.clone());
        let reply = with_session(ctx, rt.handle_input("cli:narr", "what time".into()))
            .await
            .unwrap();

        assert_eq!(reply, "it is late", "narration is not the answer");
        let narrated: Vec<String> = sink
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                TurnEvent::AssistantText { text } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(narrated, vec!["Checking the clock first."]);
    }

    #[test]
    fn the_budget_cutoff_prefers_the_models_own_words() {
        // This round's text wins; the last narration is the fallback; a silent
        // model gets the canned line. Either way the user is told it stopped early.
        let reply = stop_reply(
            BUDGET_STOP,
            "Still digging through the logs.",
            "earlier note",
        );
        assert!(reply.starts_with("Still digging through the logs."));
        assert!(reply.contains("tool-call limit"));

        let fallback = stop_reply(BUDGET_STOP, "  ", "earlier note");
        assert!(fallback.starts_with("earlier note"));

        let silent = stop_reply(BUDGET_STOP, "", "");
        assert!(silent.contains("tool-call limit"));
        assert!(!silent.contains("\n\n"));
    }

    /// A model that will not stop re-issuing one call ends the turn well short
    /// of the round budget: the executor refuses the repeats, and when it keeps
    /// asking anyway the loop stops rather than spending 120 rounds on it.
    #[tokio::test]
    async fn a_turn_repeating_one_call_stops_long_before_the_round_budget() {
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_spin.db")).await.unwrap());
        // The driver would happily keep asking for the same call forever.
        let (rt, received) = scripted_runtime(
            db.clone(),
            (0..8)
                .map(|_| tool_calls(vec![call("time", "{}")]))
                .collect(),
            vec![Arc::new(TimeTool)],
            120,
        );

        let reply = rt
            .handle_input("cli:spin", "什么时候".into())
            .await
            .unwrap();
        assert!(
            reply.contains("repeating the same step"),
            "the user is told why it stopped: {reply}"
        );

        // Two real executions, then refusals — not 120 rounds of them.
        let runs = RunRepository::list(&*db, 10).await.unwrap();
        let steps = RunRepository::steps(&*db, &runs[0].id).await.unwrap();
        assert_eq!(steps.len(), 2, "only the first two calls reached the tool");
        let rounds = received.lock().unwrap().len();
        assert!(rounds <= 4, "the turn ended after {rounds} rounds");
    }

    #[tokio::test]
    async fn round_budget_forces_a_final_answer() {
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_budget.db")).await.unwrap());
        // Driver keeps requesting tools; with max_turns=2 the loop must stop.
        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![
                tool_calls(vec![call("time", "{}")]),
                tool_calls(vec![call("time", "{}")]),
                tool_calls(vec![call("time", "{}")]),
                tool_calls(vec![call("time", "{}")]),
            ],
            vec![Arc::new(TimeTool)],
            2,
        );

        let reply = rt.handle_input("cli:s6", "hi".into()).await.unwrap();
        assert!(reply.contains("tool-call limit"), "got: {reply}");

        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs[0].status, RunStatus::Done);
        // Only the first two rounds actually dispatched; round 3 got the budget
        // note instead of executing, so exactly two ledger steps.
        let steps = RunRepository::steps(&*db, &runs[0].id).await.unwrap();
        assert_eq!(steps.len(), 2);
    }

    /// Stage an interrupted turn: a session whose conversation ends on the user
    /// message (the crash landed before any reply), its failed ledger run, and
    /// the turn's recorded events. Returns the original run.
    async fn seed_interrupted(db: &Arc<Db>, session_id: &str, rounds: u32) -> Run {
        use komo_core::domain::session_event::{
            AssistantRoundEvent, HeaderReason, MessageSource, RequestHeaderEvent, SurfacePlacement,
            UserMessageEvent,
        };
        SessionRepository::save(&**db, &Session::new(session_id))
            .await
            .unwrap();
        let run = Run::start(session_id, "do the thing");
        // The turn as the log holds it: it opened, it was asked, it got as far
        // as `rounds` completions — and then the process died, so there is no
        // terminal event. Its `turn/started` is what makes it a turn at all.
        let mut kinds = vec![
            SessionEventKind::TurnStarted {
                turn_id: run.id.clone(),
                resumed_from: None,
            },
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: run.id.clone(),
                content: "do the thing".into(),
                source: MessageSource::User,
                surface: SurfacePlacement::append(),
            }),
            SessionEventKind::RequestHeader(RequestHeaderEvent {
                reason: HeaderReason::Initial,
                provider: "anthropic".into(),
                model: "claude-sonnet-4-6".into(),
                effort: String::new(),
                system: "You are komo.".into(),
                tools: vec![],
                extra: None,
            }),
        ];
        for round in 0..rounds {
            kinds.push(SessionEventKind::AssistantRound(AssistantRoundEvent {
                turn_id: run.id.clone(),
                round,
                response_id: format!("resp-{round}"),
                blocks: serde_json::json!([]),
                tokens_in: 0,
                tokens_out: 0,
                tokens_cached: 0,
            }));
        }
        SessionEventRepository::append(&**db, session_id, kinds)
            .await
            .unwrap();
        SessionEventRepository::durable_flush(&**db, session_id)
            .await
            .unwrap();
        // The row the interrupted turn left behind: opened and never closed,
        // exactly as the runtime commits it when a turn starts.
        let events = SessionEventRepository::events(&**db, session_id)
            .await
            .unwrap();
        let folded = project_runs(session_id, &events);
        RunProjectionStore::commit(
            &**db,
            session_id,
            &folded,
            events.last().map(|e| e.seq).unwrap(),
        )
        .await
        .unwrap();
        run
    }

    #[tokio::test]
    async fn resume_interrupted_continues_without_a_new_user_message() {
        let db = Arc::new(Db::connect(&sqlite_url("komo_rt_resume.db")).await.unwrap());
        let original = seed_interrupted(&db, "cli:rs1", 2).await;

        let resumed_entries = Arc::new(Mutex::new(None));
        let rt = AgentRuntime {
            llm: Arc::new(ScriptedLlm {
                script: Mutex::new(vec![Step::Final("resumed reply".into())].into()),
                received: Arc::new(Mutex::new(Vec::new())),
                interjected: Arc::new(Mutex::new(Vec::new())),
                resumed_entries: resumed_entries.clone(),
            }),
            sessions: db.clone(),
            messages: db.clone(),
            events: db.clone(),
            projection: db.clone(),
            runs: db.clone(),
            tool_executor: ToolExecutor::new(
                komo_services::tool_execution::ToolExecutionConfig::default(),
            ),
            max_turns: 30,
            history_window: 0,
            learning: None,
            compaction: None,
            wakeups: None,
            checkpoint: None,
            turn_hooks: Vec::new(),
            step_hooks: Vec::new(),
        };

        let reply = rt
            .resume_interrupted(&original)
            .await
            .unwrap()
            .expect("this run is continuable");
        assert_eq!(reply, "resumed reply");
        // The driver was reopened from the log, not begun fresh: the turn's own
        // events — the pair that opened it, plus the two rounds it got through.
        assert_eq!(*resumed_entries.lock().unwrap(), Some(4));

        // The continuation appended exactly one assistant message — the
        // interrupted turn's own user message still opens the pair.
        let session = SessionRepository::find(&*db, "cli:rs1")
            .await
            .unwrap()
            .unwrap();
        let roles: Vec<Role> = session.messages.iter().map(|m| m.role.clone()).collect();
        assert_eq!(roles, vec![Role::User, Role::Assistant]);
        assert_eq!(session.messages[1].content, "resumed reply");

        // The continuation is its own ledger run, linked back.
        let runs = RunRepository::list(&*db, 10).await.unwrap();
        let continuation = runs
            .iter()
            .find(|r| r.resumed_from.as_deref() == Some(original.id.as_str()))
            .expect("a continuation run linked to the original");
        assert_eq!(continuation.status, RunStatus::Done);

        // The turn's events live with the session, not with the run: nothing to
        // clear, and the conversation keeps the continuation's reply.
        let messages = MessageRepository::list_by_session(&*db, &original.session_id)
            .await
            .unwrap();
        assert_eq!(messages.last().unwrap().role, Role::Assistant);
    }

    #[tokio::test]
    async fn resume_refuses_a_transcript_that_already_ends_in_a_reply() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_resume_guard.db"))
                .await
                .unwrap(),
        );
        let original = seed_interrupted(&db, "cli:rs2", 1).await;
        // The reply actually landed (crash in the gap before the ledger
        // closed) — the transcript ends on an assistant message.
        say(&db, "cli:rs2", Message::assistant("already delivered")).await;

        let (rt, _) = scripted_runtime(
            db.clone(),
            vec![Step::Final("should not run".into())],
            vec![],
            30,
        );
        let rt = AgentRuntime { ..rt };

        let outcome = rt.resume_interrupted(&original).await.unwrap();
        assert!(outcome.is_none(), "must decline, not continue");
        // Nothing was appended to the transcript, and no ledger run opened.
        let session = SessionRepository::find(&*db, "cli:rs2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.messages.len(), 2);
        let runs = RunRepository::list(&*db, 10).await.unwrap();
        assert_eq!(runs.len(), 1, "only the original run exists");
    }

    #[tokio::test]
    async fn resume_without_journal_rows_fails_before_touching_anything() {
        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_resume_norows.db"))
                .await
                .unwrap(),
        );
        // An interrupted run whose turn left no events at all (a pre-log
        // build, or the appends failed) — the caller must fall back to the
        // digest path. The conversation is there; the turn is not.
        SessionRepository::save(&*db, &Session::new("cli:rs3"))
            .await
            .unwrap();
        say(&db, "cli:rs3", Message::user("do the thing")).await;
        let original = Run::start("cli:rs3", "do the thing");
        let (rt, _) = scripted_runtime(db.clone(), vec![], vec![], 30);
        let rt = AgentRuntime { ..rt };
        let outcome = rt.resume_interrupted(&original).await.unwrap();
        assert!(
            outcome.is_none(),
            "no rows ⇒ decline so the digest path runs"
        );
        let session = SessionRepository::find(&*db, "cli:rs3")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.messages.len(), 1, "transcript untouched");
    }

    /// The ordering fix, asserted end to end: learning is dispatched **after**
    /// `runs.finish`, so the episode it assembles is a finished one.
    ///
    /// The failure this guards against is silent, not loud. Dispatched from
    /// inside the turn — where the post-turn review used to live — the run is
    /// still `Running`, so it is not offered as an episode and the turn is
    /// simply never learned from. Nothing errors; komo just stops learning.
    #[tokio::test]
    async fn learning_sees_a_finished_run_because_it_is_dispatched_after_the_ledger_closes() {
        /// Records the status each episode carried when the extractor saw it.
        struct StatusSpy(Arc<Mutex<Vec<(String, RunStatus)>>>);
        #[async_trait]
        impl komo_core::domain::reviewer::Reviewer for StatusSpy {
            async fn review(
                &self,
                _session: &Session,
                episodes: &[komo_core::domain::episode::AssessedEpisode],
            ) -> anyhow::Result<komo_core::domain::reviewer::ReviewOutcome> {
                self.0.lock().unwrap().extend(
                    episodes
                        .iter()
                        .map(|e| (e.view.id().to_string(), e.view.run.status)),
                );
                Ok(Default::default())
            }
        }

        let db = Arc::new(
            Db::connect(&sqlite_url("komo_rt_learning_order.db"))
                .await
                .unwrap(),
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (mut rt, _) =
            scripted_runtime(db.clone(), vec![Step::Final("done".into())], Vec::new(), 30);
        rt.learning = Some(Arc::new(
            crate::learning_coordinator::LearningCoordinator::new(
                db.clone(),
                db.clone(),
                db.clone(),
                Arc::new(StatusSpy(seen.clone())),
                1,
            ),
        ));

        rt.handle_input("cli:s1", "hi".into()).await.unwrap();

        // Learning runs detached, so wait for it rather than racing it.
        for _ in 0..200 {
            if !seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let runs = RunRepository::list(&*db, 10).await.unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.len(),
            1,
            "the finished turn must reach the extractor — an empty list here \
             means learning ran while the run was still open"
        );
        assert_eq!(seen[0].0, runs[0].id);
        assert_eq!(seen[0].1, RunStatus::Done, "and it was already terminal");
    }
}
