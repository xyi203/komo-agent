//! Execution-context value types shared by the tool loop and the tools.
//!
//! These are pure values over domain traits (`ReplySink`, `RunRepository`,
//! `Approver`) — no I/O — so they live in `domain`. The tool-execution service
//! re-exports [`SessionContext`] and [`RunContext`] for path stability, adds the
//! per-turn [`ToolTurnContext`] bundle, and owns the ambient-session task-local
//! (now only the approvers read it — see that module). [`ToolContext`] is the
//! **explicit** per-call context handed to `Tool::call` (tool trait v2): every
//! tool reads its session and requests approval through `ctx`, never an ambient
//! scope.

use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use crate::domain::approval::{ApprovalRequest, Approver, Decision};
use crate::domain::cancel::CancelSignal;
use crate::domain::events::ToolEventSink;
use crate::domain::gateway::{InterjectSource, ReplySink};
use crate::domain::policy::LOCAL_CHANNEL;
use crate::domain::repository::SessionEventRepository;
use crate::domain::run::RunStep;
use crate::domain::session::ChannelPeer;
use crate::domain::session_event::{
    ApprovalRequestedEvent, ApprovalResolvedEvent, ResumedWait, SessionEvent, SessionEventKind,
    TurnWaits, Wakeup,
};

/// What is driving a turn, as far as **approval** is concerned.
///
/// Deliberately *not* expressed as "has an ambient session or not". A cron job's
/// turn genuinely has a session — a ledger run, a transcript, session-scoped
/// tools — and encoding "nobody is watching" as "there is no session" made the
/// two questions share one answer, so the policy engine's unattended branch
/// never actually ran in production (it read a session id it was never supposed
/// to see). This says the quiet part explicitly instead.
///
/// An enum rather than a `bool` because the callers already differ in more than
/// attendance: a cron job's turn is scoped to *one job*, the briefing's is not.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum SessionOrigin {
    /// A user-driven conversation — chat channel, CLI, TUI, HTTP API. Approval
    /// is evaluated against the session's channel, and a human may be reachable
    /// (that separate question is [`SessionContext::interactive`]).
    #[default]
    User,
    /// A scheduled cron job's turn (`CronJobSweep`).
    Cron,
    /// The daily briefing sweep's turn (`BriefingSweep`).
    Briefing,
    /// A sub-agent's scratch session, spawned by the `delegate` tool.
    ///
    /// Only ever the *session record's* value: a delegation deliberately runs
    /// inside the parent's ambient [`SessionContext`], so the turn's own origin
    /// is whatever the parent's was. It is here because it answers the same
    /// question the other three do — what is driving this conversation — and
    /// the alternative was a second enum overlapping this one three ways.
    Delegate,
}

impl SessionOrigin {
    /// Whether no human is behind this turn, so the permission policy must
    /// evaluate it channel-lessly (only an `unattended = true` allow rule
    /// grants; no default and no saved grant ever does).
    ///
    /// Matched exhaustively rather than written as "anything but `User`": a
    /// variant added later must not inherit an attendance answer nobody chose,
    /// in either direction — too strict silently breaks a sweep, too loose
    /// silently widens the gate.
    pub fn is_unattended(self) -> bool {
        match self {
            Self::User => false,
            Self::Cron | Self::Briefing => true,
            // A sub-agent runs inside its parent's turn and inherits its
            // context, so whoever was reachable for the parent is reachable
            // for it.
            Self::Delegate => false,
        }
    }

    /// Stable wire form for the session record's column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Cron => "cron",
            Self::Briefing => "briefing",
            Self::Delegate => "delegate",
        }
    }

    /// Read a stored value back. An unknown string reads as [`Self::User`]:
    /// this decides display and learning eligibility, never authorization, and
    /// an unreadable row should look like an ordinary conversation rather than
    /// vanish from the session list.
    pub fn parse(raw: &str) -> Self {
        match raw {
            "cron" => Self::Cron,
            "briefing" => Self::Briefing,
            "delegate" => Self::Delegate,
            _ => Self::User,
        }
    }

    /// Whether a turn on this session is a lesson worth extracting from.
    ///
    /// Only a real conversation is. A sweep (`Cron`/`Briefing`) restates what
    /// the agent already knows, and each run's session counts as a fresh
    /// "independent occasion" to the memory consolidator — extracting there
    /// lets the library corroborate itself on a timer. A `Delegate` session is
    /// the *parent* turn's own work done by a sub-agent, so learning from both
    /// counts one occasion twice, which is the same failure by another route.
    ///
    /// Matched exhaustively rather than written as "anything but `User`": a new
    /// origin has to decide this deliberately.
    pub fn is_learnable(self) -> bool {
        match self {
            Self::User => true,
            Self::Cron | Self::Briefing | Self::Delegate => false,
        }
    }
}

/// The session a tool is executing within: which conversation it belongs to
/// and how to talk back to that conversation. Set by the gateway dispatcher
/// around a turn (`agent::interaction`) and read by a chat-channel approver
/// when a tool needs mid-execution approval.
#[derive(Clone)]
pub struct SessionContext {
    pub session_id: String,
    /// Optional filesystem root selected by a local UI for this turn. HTTP
    /// adapters must resolve this from a server-owned workspace catalog; tools
    /// never accept a client-supplied path directly.
    pub workspace_root: Option<PathBuf>,
    pub sink: Arc<dyn ReplySink>,
    /// Whether a human can answer a mid-turn approval prompt on this channel.
    /// Chat channels set this `true`; non-interactive callers (the detached
    /// context, the HTTP API) set it `false` so a `Risk::Normal` /
    /// `Risk::Dangerous` request is denied immediately instead of waiting out
    /// the approval timeout against a sink no one is reading.
    pub interactive: bool,
    /// Whether approval-needing tool calls should be auto-approved without a
    /// prompt. Set only for a **trusted** turn — a `komo chat` routed over the
    /// gateway's loopback api channel, where the CLI user *is* the host
    /// operator (see [`SessionContext::trusted`]). The api channel gates this to
    /// loopback callers, so a publicly-bound api never reaches it. Leave `false`
    /// everywhere else.
    pub auto_approve: bool,
    /// Optional live event sink. When set (a streaming client is watching this
    /// turn), the tool executor emits [`TurnEvent`](crate::domain::events::TurnEvent)s
    /// as each tool starts and finishes. `None` for every ordinary turn — no
    /// watcher, no emission. Attached via [`with_event_sink`](Self::with_event_sink).
    pub event_sink: Option<Arc<dyn ToolEventSink>>,
    /// Optional cancellation signal for this turn. Set when the caller has a way
    /// to ask for a stop (the api channel's `/api/interactions/{session}/cancel`);
    /// `None` for turns nobody can interrupt — sweeps, cron, aux sub-agents.
    /// See [`CancelSignal`] for what "cancelled" does and does not stop.
    pub cancel: Option<Arc<dyn CancelSignal>>,
    /// Optional source of mid-turn user messages. Set by the gateway dispatcher
    /// (it owns the queue those messages land in); `None` for turns nobody can
    /// talk to while they run — sweeps, cron, aux sub-agents, the HTTP API.
    /// See [`InterjectSource`].
    pub interject: Option<Arc<dyn InterjectSource>>,
    /// What is driving this turn. [`SessionOrigin::User`] for every ordinary
    /// conversation; the sweeps that run agent turns with nobody watching set
    /// their own variant via [`with_origin`](Self::with_origin), which is what
    /// makes the permission policy treat them as unattended.
    pub origin: SessionOrigin,
    /// The correspondent this turn is talking to, when there is one. `None` for
    /// every local surface and for komo's own turns.
    ///
    /// Carried on the turn rather than looked up, because the two readers —
    /// the permission engine's channel scope and the memory writer's scope —
    /// run per turn and hold no repository handle. It used to be recovered by
    /// splitting `session_id` on a colon, which made the session id a schema
    /// and every client that could name its own id able to claim a channel.
    pub channel: Option<ChannelPeer>,
}

impl SessionContext {
    /// A context that knows the session but cannot talk back mid-turn (its sink
    /// is a no-op, and it is non-interactive). Used by any caller that has a
    /// session id but no channel to prompt on — enough for session-scoped tools
    /// like `todo`, while a mid-turn approval prompt is auto-denied.
    pub fn detached(session_id: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            workspace_root: None,
            sink: Arc::new(NoopSink),
            interactive: false,
            auto_approve: false,
            event_sink: None,
            cancel: None,
            interject: None,
            origin: SessionOrigin::User,
            channel: None,
        }
    }

    /// A trusted context: like [`detached`](SessionContext::detached) (no
    /// mid-turn prompting), but approval-needing tool calls are auto-approved.
    /// Used for a `komo chat` turn routed over the gateway's **loopback** api
    /// channel — the CLI user is the host operator, so there is no separate
    /// human to prompt. The api channel only builds this for loopback callers
    /// carrying the trusted header; a publicly-bound api keeps using `detached`.
    pub fn trusted(session_id: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            workspace_root: None,
            sink: Arc::new(NoopSink),
            interactive: false,
            auto_approve: true,
            event_sink: None,
            cancel: None,
            interject: None,
            origin: SessionOrigin::User,
            channel: None,
        }
    }

    /// An interactive HTTP context: `interactive` so approval / clarify prompts
    /// suspend the turn (rather than auto-denying), but the sink is a no-op — the
    /// prompt is surfaced out-of-band. Used for the GUI's turns over the
    /// gateway's **loopback** api channel (`X-Komo-Interactive`): it polls
    /// `GET /api/interactions/{session}` for the pending prompt and resolves it
    /// with a `POST`, so no reply sink is read. The api channel builds this only
    /// for loopback callers; a publicly-bound api keeps using `detached`.
    pub fn interactive_http(session_id: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            workspace_root: None,
            sink: Arc::new(NoopSink),
            interactive: true,
            auto_approve: false,
            event_sink: None,
            cancel: None,
            interject: None,
            origin: SessionOrigin::User,
            channel: None,
        }
    }

    /// Declare which correspondent this turn answers. Chat channels call this;
    /// every local surface leaves it unset.
    pub fn with_channel(mut self, channel: Option<ChannelPeer>) -> Self {
        self.channel = channel;
        self
    }

    /// The permission channel this turn is evaluated against — the platform it
    /// arrived on, or `cli` for a local surface that has no correspondent.
    pub fn channel_name(&self) -> &str {
        match &self.channel {
            Some(peer) => &peer.platform,
            None => LOCAL_CHANNEL,
        }
    }

    /// Declare what is driving this turn. Only the unattended sweeps call this —
    /// every ordinary construction is [`SessionOrigin::User`].
    pub fn with_origin(mut self, origin: SessionOrigin) -> Self {
        self.origin = origin;
        self
    }

    /// Whether no human is behind this turn — see [`SessionOrigin`].
    pub fn is_unattended(&self) -> bool {
        self.origin.is_unattended()
    }

    /// Attach a live [`ToolEventSink`] so the tool executor emits `TurnEvent`s
    /// for this turn (the streaming api path uses this to feed the SSE stream).
    pub fn with_event_sink(mut self, sink: Arc<dyn ToolEventSink>) -> Self {
        self.event_sink = Some(sink);
        self
    }

    /// Attach an [`InterjectSource`], letting the agent loop pick up what the
    /// user says while the turn is running.
    pub fn with_interject(mut self, interject: Arc<dyn InterjectSource>) -> Self {
        self.interject = Some(interject);
        self
    }

    /// Attach a [`CancelSignal`], making this turn interruptible.
    pub fn with_cancel(mut self, cancel: Arc<dyn CancelSignal>) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// Attach the server-resolved workspace selected for this turn.
    pub fn with_workspace(mut self, root: PathBuf) -> Self {
        self.workspace_root = Some(root);
        self
    }

    /// Has a stop been requested for this turn?
    pub fn is_cancelled(&self) -> bool {
        self.cancel.as_ref().is_some_and(|c| c.is_cancelled())
    }
}

/// A [`ReplySink`] that drops everything — see [`SessionContext::detached`].
struct NoopSink;

#[async_trait::async_trait]
impl ReplySink for NoopSink {
    async fn send(&self, _text: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

/// The wait a call raised instead of running.
///
/// Carried on the turn rather than returned from the call, because it is not
/// the *call's* outcome — the call did not happen. The executor reads it to
/// leave that call unsettled, and the loop reads it to stop the turn.
///
/// Raised by the approval gate when nobody has answered yet, and by a tool that
/// asked to wait ([`ToolContext::wait_for`]) — one channel, because from the
/// executor's and the loop's side those are the same event: this call stopped,
/// the turn is over for now, and the call comes back when the wake arrives.
#[derive(Debug, Clone)]
pub struct PendingSuspension {
    pub call_id: String,
    pub call_index: u32,
    pub wakeup: Wakeup,
    /// One line for the operator: what this turn is waiting for.
    pub summary: String,
    pub expires_at: Option<i64>,
}

/// The run-ledger handle for one turn (`domain/run.rs`, roadmap §7): created by
/// `AgentRuntime::run_turn` and passed **explicitly** down to the executor, so
/// ledgering and the per-turn call budget never depend on a caller having
/// established an ambient scope. Absent (`None` in [`ToolContext`]) for callers
/// without a ledger, so their tool use never pollutes it.
#[derive(Clone)]
pub struct RunContext {
    pub run_id: String,
    /// The steps this turn has settled, shared across clones.
    ///
    /// The ledger's step *rows* are a projection of the log now, written when
    /// the turn closes — so mid-turn there is nothing there to read back, and
    /// the one reader inside the turn (the tool note its closing message
    /// carries) keeps them here instead. Ephemeral: the durable record is the
    /// `tool/call-settled` event these are built from.
    steps: Arc<Mutex<Vec<RunStep>>>,
    /// The wait that stopped this turn, if one did. Written by the approval
    /// gate, read by the executor (which then leaves the call unsettled) and by
    /// the loop (which then ends the turn as suspended rather than failed).
    ///
    /// One per turn: the first call to raise one stops the turn, and a round's
    /// other calls settle normally around it.
    suspend: Arc<Mutex<Option<PendingSuspension>>>,
    /// The other direction: what this turn's chain has already waited for, and
    /// what ended the wait that brought this attempt back. Folded out of the
    /// log by the runtime when a continuation opens, so a tool can read its own
    /// wake without a log handle of its own — which is what makes waiting work
    /// in a runtime whose executor keeps no transcript (a routine's).
    waits: Arc<Mutex<TurnWaits>>,
    /// Monotonic step counter, shared across clones so steps within a run get a
    /// stable order even when tool calls run concurrently.
    seq: Arc<AtomicI64>,
    /// Where a mutating tool leaves the bytes a file held before this run
    /// touched it, so `komo run rollback` can put them back. Rides here rather
    /// than being injected into each file tool because this is already the
    /// thing that knows *which run* a mutation belongs to. `None` = no
    /// checkpointing (aux runtimes, tests).
    checkpoint: Option<Arc<dyn super::checkpoint::CheckpointStore>>,
}

impl RunContext {
    pub fn new(run_id: String) -> Self {
        Self {
            run_id,
            steps: Arc::new(Mutex::new(Vec::new())),
            suspend: Arc::new(Mutex::new(None)),
            waits: Arc::new(Mutex::new(TurnWaits::default())),
            seq: Arc::new(AtomicI64::new(0)),
            checkpoint: None,
        }
    }

    /// Attach the checkpoint store, making this run's file changes undoable.
    pub fn with_checkpoint(
        mut self,
        store: Option<Arc<dyn super::checkpoint::CheckpointStore>>,
    ) -> Self {
        self.checkpoint = store;
        self
    }

    /// The checkpoint store, if this run is being checkpointed.
    pub fn checkpoint(&self) -> Option<&Arc<dyn super::checkpoint::CheckpointStore>> {
        self.checkpoint.as_ref()
    }

    /// Claim the next step's sequence number.
    pub fn next_seq(&self) -> i64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// How many tool steps have been claimed so far (the post-turn count).
    pub fn steps_count(&self) -> i64 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Keep a settled step, as the log records it.
    pub fn record_step(&self, step: RunStep) {
        self.steps.lock().unwrap().push(step);
    }

    /// This turn's settled steps in claim order — calls run concurrently, so
    /// they arrive in completion order and are sorted back here.
    pub fn steps(&self) -> Vec<RunStep> {
        let mut steps = self.steps.lock().unwrap().clone();
        steps.sort_by_key(|step| step.seq);
        steps
    }

    /// Record the wait that stopped this turn. First one wins: two calls in a
    /// round both waiting on a person is one suspension, and the turn comes
    /// back to re-dispatch both.
    pub fn suspend(&self, pending: PendingSuspension) {
        let mut slot = self.suspend.lock().unwrap();
        if slot.is_none() {
            *slot = Some(pending);
        }
    }

    /// The wait that stopped this turn, if one did.
    pub fn suspension(&self) -> Option<PendingSuspension> {
        self.suspend.lock().unwrap().clone()
    }

    /// Install what the log says this turn has waited for. Called once, as a
    /// continuation opens: the runtime holds the events, the tool does not.
    pub fn resumed_with(&self, waits: TurnWaits) {
        *self.waits.lock().unwrap() = waits;
    }

    /// What this turn's chain has waited for so far.
    pub fn waits(&self) -> TurnWaits {
        self.waits.lock().unwrap().clone()
    }

    /// Whether *this call* is the one that stopped the turn — the executor's
    /// question, because a suspended call has no outcome to record.
    pub fn suspended_call(&self, call_id: &str) -> bool {
        self.suspend
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|pending| pending.call_id == call_id)
    }
}

/// The **explicit** per-call context handed to [`Tool::call`](crate::domain::tool::Tool::call).
///
/// A tool reads its session (`ctx.session`) and requests approval
/// (`ctx.approve(..)`) through this value rather than an ambient task-local or a
/// constructor-injected `Arc<dyn Approver>`. Owned (all fields cheap-clone) so
/// the executor can move it into the spawned tool task.
pub struct ToolContext {
    pub session: SessionContext,
    pub run: Option<RunContext>,
    approver: Arc<dyn Approver>,
    /// Makes this call's approval a durable fact. `None` for a turn with no
    /// event log (aux runtimes), which simply loses the not-started proof.
    approval: Option<ApprovalGate>,
    /// Which call in the round this is. Installed by the executor for every
    /// ledgered call — separately from the approval gate, which additionally
    /// needs an event log: a tool may stop to wait in a runtime that keeps no
    /// transcript of its tool calls, and the identity is all that stopping
    /// takes.
    call: Option<CallRef>,
    /// Who holds work that outlives this turn (`domain/background.rs`). `None`
    /// on a runtime whose turns are all there is — an aux completion, a
    /// sub-agent, a test — and a tool asked to detach then says so rather than
    /// pretending to.
    tasks: Option<Arc<dyn crate::domain::background::BackgroundTasks>>,
}

/// One call's place in its round: what a wait has to name to be re-dispatched
/// as the same call it stopped as.
#[derive(Debug, Clone)]
struct CallRef {
    call_id: String,
    call_index: u32,
}

/// Why a tool's request to wait was turned down. Both are the tool's to report
/// to the model — neither is an error, and neither stops the turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitRefused {
    /// This runtime cannot stop a turn and come back for it (no ledger, no
    /// call identity — an aux completion, a sub-agent, a test).
    Unsupported,
    /// Another call in the same round stopped the turn first. One suspension
    /// per turn: this call settles normally and the model may ask again.
    Superseded,
}

/// What one call needs to record its approval durably.
///
/// The gate exists because an approval is the **widest** crash window in a
/// turn: a person may take five minutes. Without it, a crash during that wait
/// is indistinguishable from a crash during the tool, so recovery has to assume
/// the effect may have landed — and tell the model to go verify something that
/// never happened.
#[derive(Clone)]
pub struct ApprovalGate {
    events: Arc<dyn SessionEventRepository>,
    session_id: String,
    turn_id: String,
    call_id: String,
    call_index: u32,
}

impl ApprovalGate {
    pub fn new(
        events: Arc<dyn SessionEventRepository>,
        session_id: &str,
        turn_id: &str,
        call_id: &str,
        call_index: u32,
    ) -> Self {
        Self {
            events,
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            call_id: call_id.to_string(),
            call_index,
        }
    }

    async fn append(&self, kind: SessionEventKind) -> anyhow::Result<()> {
        self.events.append(&self.session_id, vec![kind]).await?;
        Ok(())
    }

    async fn durable(&self) -> anyhow::Result<()> {
        self.events.durable_flush(&self.session_id).await
    }

    /// The answer already in the log for **this call**, if there is one.
    ///
    /// What makes a resumed turn re-dispatch its gated call without asking
    /// again: the answer arrived while the turn was suspended, and it is on
    /// record. Asking a second time would be asking the user to approve the
    /// same action twice — and for a wait that expired, would park the turn
    /// again forever.
    ///
    /// The newest resolution wins, and a log that cannot be read means "no
    /// answer yet": the gate then asks, which is the safe direction.
    /// Whether this call's approval **ran out** while the turn was suspended.
    ///
    /// A wait nobody answered comes back as a refusal, not as another prompt:
    /// asking again would park the turn on the same question forever, and the
    /// model needs to be told it was never answered so it can say so or take
    /// another route.
    async fn expired_earlier(&self, events: &[SessionEvent]) -> bool {
        let attempts = crate::domain::session_event::attempt_chain(events, &self.turn_id);
        events.iter().any(|event| {
            matches!(
                &event.kind,
                SessionEventKind::ApprovalExpired { turn_id, call_id, .. }
                    if attempts.contains(turn_id.as_str()) && *call_id == self.call_id
            )
        })
    }

    async fn resolved_earlier(&self) -> Option<ApprovalResolvedEvent> {
        let events = self.events.events(&self.session_id).await.ok()?;
        // Across the whole chain of attempts, not just this turn: the answer was
        // recorded against the turn that *asked*, and the turn asking now is its
        // continuation. Bounded to the chain rather than matched on `call_id`
        // alone, so an answer can never leak into an unrelated turn that
        // happened to reuse a call id.
        let attempts = crate::domain::session_event::attempt_chain(&events, &self.turn_id);
        events.iter().rev().find_map(|event| match &event.kind {
            SessionEventKind::ApprovalResolved(resolved)
                if attempts.contains(&resolved.turn_id) && resolved.call_id == self.call_id =>
            {
                Some(resolved.clone())
            }
            _ => None,
        })
    }
}

impl ToolContext {
    pub fn new(
        session: SessionContext,
        run: Option<RunContext>,
        approver: Arc<dyn Approver>,
    ) -> Self {
        Self {
            session,
            run,
            approver,
            approval: None,
            call: None,
            tasks: None,
        }
    }

    /// Install who runs work that outlives this turn. Set by the executor, on
    /// the runtimes that have somewhere for such work to live.
    pub fn with_background(
        mut self,
        tasks: Arc<dyn crate::domain::background::BackgroundTasks>,
    ) -> Self {
        self.tasks = Some(tasks);
        self
    }

    /// Who runs work that outlives this turn, if anyone does here.
    pub fn background(&self) -> Option<&Arc<dyn crate::domain::background::BackgroundTasks>> {
        self.tasks.as_ref()
    }

    /// The turn this call belongs to — what a spawned task is recorded against.
    pub fn turn_id(&self) -> Option<&str> {
        self.run.as_ref().map(|run| run.run_id.as_str())
    }

    /// Make this call's approvals durable facts. Installed by the executor,
    /// which is the only place a call's identity is known.
    pub fn with_approval_gate(mut self, gate: ApprovalGate) -> Self {
        self.approval = Some(gate);
        self
    }

    /// Name this call, so it can stop to wait and come back as itself.
    pub fn with_call(mut self, call_id: &str, call_index: u32) -> Self {
        self.call = Some(CallRef {
            call_id: call_id.to_string(),
            call_index,
        });
        self
    }

    /// Stop the turn here and come back when `wakeup` fires.
    ///
    /// The tool's half of the suspension primitive (docs/bot-runtime.md §3.4):
    /// the call is left unsettled — a call that stopped to wait **did not
    /// happen** — the loop ends the turn as suspended, and the runtime writes
    /// `turn/suspended` plus the registration that brings it back. What the
    /// tool returns after this is discarded, so it may be anything.
    ///
    /// The way back is [`resumed_wait`](Self::resumed_wait): the same call is
    /// re-dispatched with the wake attached, and returns *that* to the model.
    pub fn wait_for(
        &self,
        wakeup: Wakeup,
        summary: impl Into<String>,
        expires_at: Option<i64>,
    ) -> Result<(), WaitRefused> {
        let (Some(run), Some(call)) = (&self.run, &self.call) else {
            return Err(WaitRefused::Unsupported);
        };
        run.suspend(PendingSuspension {
            call_id: call.call_id.clone(),
            call_index: call.call_index,
            wakeup,
            summary: summary.into(),
            expires_at,
        });
        match run.suspended_call(&call.call_id) {
            true => Ok(()),
            false => Err(WaitRefused::Superseded),
        }
    }

    /// The wake that ended **this call's** wait, if this call is the
    /// re-dispatch of one that stopped.
    ///
    /// Matched on the call id the suspension recorded, so a turn that waited
    /// twice never hands one call the other's answer — and a tool that reads
    /// `Some` here must return it rather than wait again, which is what keeps
    /// a wait from re-arming itself forever.
    pub fn resumed_wait(&self) -> Option<ResumedWait> {
        let call = self.call.as_ref()?;
        self.run
            .as_ref()?
            .waits()
            .resumed
            .filter(|wake| wake.call_id == call.call_id)
    }

    /// Every wait this turn has already taken — the per-turn budget's counter,
    /// read from the log rather than from memory so it survives the suspension
    /// it is counting.
    pub fn waits_taken(&self) -> Vec<Wakeup> {
        self.run
            .as_ref()
            .map(|run| run.waits().taken)
            .unwrap_or_default()
    }

    /// Ask the wired approver to allow `request`, keeping only the yes/no. The
    /// executor installs the ambient session scope around the tool task, so the
    /// concrete approver (chat/CLI) still resolves the prompt against the right
    /// conversation.
    ///
    /// Use [`decide`](Self::decide) instead when the tool's refusal text should
    /// carry the user's reason back to the model.
    pub async fn approve(&self, request: &ApprovalRequest) -> bool {
        self.decide(request).await.is_allowed()
    }

    /// Ask the wired approver, keeping the full [`Decision`] — including the
    /// reason a denial carried, which the tool should pass to the model as
    /// [`ToolError::Denied`](crate::domain::tool::ToolError::Denied) so the next
    /// round can correct itself rather than retry verbatim.
    pub async fn decide(&self, request: &ApprovalRequest) -> Decision {
        let Some(gate) = &self.approval else {
            return self.approver.decide(request).await;
        };

        // An answer that arrived while this turn was suspended is *the* answer:
        // the call is being re-dispatched precisely because it came in.
        if let Some(resolved) = gate.resolved_earlier().await {
            return match resolved.allowed {
                true => Decision::Allow,
                false => match resolved.reason.is_empty() {
                    true => Decision::deny(),
                    false => Decision::deny_because(resolved.reason),
                },
            };
        }
        // Nobody answered in time. Also an answer, and the turn is told so
        // rather than asked again.
        if let Ok(events) = gate.events.events(&gate.session_id).await
            && gate.expired_earlier(&events).await
        {
            return Decision::deny_because(
                "the approval request expired without an answer; the action was not taken",
            );
        }
        let scope_key = request.scope_key.clone().unwrap_or_default();

        // Before the wait. A crash past this point but before a durable
        // `allow` proves the tool body never ran — the one thing that makes an
        // approval-gated call recoverable as *not started* rather than unknown.
        //
        // Fail closed: if the intent to ask did not survive, do not ask. A
        // prompt answered `yes` whose record was lost would let the effect
        // begin while the log still said it had not been approved.
        let requested = SessionEventKind::ApprovalRequested(ApprovalRequestedEvent {
            turn_id: gate.turn_id.clone(),
            call_id: gate.call_id.clone(),
            call_index: gate.call_index,
            scope_key: scope_key.clone(),
        });
        if let Err(error) = gate.append(requested).await {
            return Decision::deny_because(format!(
                "could not record the approval request durably ({error}); refusing rather than \
                 acting on an approval the log would not remember"
            ));
        }
        if let Err(error) = gate.durable().await {
            return Decision::deny_because(format!(
                "could not make the approval request durable ({error}); refusing rather than \
                 acting on an approval the log would not remember"
            ));
        }

        let asked_at = std::time::Instant::now();
        let (decision, decided_by) = self.approver.decide_reported(request).await;
        let waited_ms = asked_at.elapsed().as_millis() as i64;

        // Nobody has answered, and the answer is coming later: the prompt is
        // delivered, this turn stops here, and the *same* call is re-dispatched
        // when the wake arrives. Deliberately **no** `approval/resolved` — the
        // question is still open, and a resolution written now would be the
        // gate answering it itself.
        if decision.is_suspended() {
            let pending = PendingSuspension {
                call_id: gate.call_id.clone(),
                call_index: gate.call_index,
                wakeup: Wakeup::Approval {
                    call_id: gate.call_id.clone(),
                },
                summary: request.summary.clone(),
                expires_at: None,
            };
            match &self.run {
                Some(run) => run.suspend(pending),
                // No ledger handle means nothing can stop the turn on this
                // call's behalf, and a caller that cannot suspend must not act
                // as though it were allowed.
                None => {
                    return Decision::deny_because(
                        "this action needs approval and this runtime cannot wait for one",
                    );
                }
            }
            // The tool stops on a refusal like any other; the executor drops
            // this outcome, because a call that never ran has none.
            return Decision::deny_because(
                "waiting for approval — the turn will continue when it arrives",
            );
        }

        let allowed = decision.is_allowed();

        let resolved = SessionEventKind::ApprovalResolved(ApprovalResolvedEvent {
            turn_id: gate.turn_id.clone(),
            call_id: gate.call_id.clone(),
            call_index: gate.call_index,
            allowed,
            decided_by: decided_by.to_string(),
            reason: decision.feedback().unwrap_or_default().to_string(),
            waited_ms,
        });
        if let Err(error) = gate.append(resolved).await {
            return Decision::deny_because(format!("could not record the approval ({error})"));
        }
        // Only an *allow* has to be durable before it is acted on: it is what
        // licenses the side effect. A denial runs nothing, so it can ride the
        // next barrier like any other record.
        if allowed && let Err(error) = gate.durable().await {
            return Decision::deny_because(format!(
                "the approval could not be made durable ({error}); refusing rather than acting \
                 on one a crash would erase"
            ));
        }
        decision
    }

    /// Resolves once this turn has been cancelled — and **never** otherwise, so
    /// it is safe as a `select!` arm against real work. With no cancel signal
    /// (sweeps, cron, aux sub-agents) it pends forever, which makes that arm
    /// inert rather than instantly winning.
    ///
    /// Only a tool that can abandon its work **without leaving a mess** should
    /// race this: `shell` (kills its process group), `web_fetch` / `web_search`
    /// (drop the request, no side effect). A tool that mutates the filesystem
    /// deliberately does *not* — `apply_patch` writes several files in sequence,
    /// and stopping between two of them turns a patch that would have completed
    /// into a half-applied tree. Slower is better than inconsistent.
    pub async fn cancelled(&self) {
        match self.session.cancel.as_ref() {
            Some(signal) => signal.cancelled().await,
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod approval_gate_tests {
    use super::*;
    use crate::domain::session_event::SessionEvent;
    use std::sync::Mutex;

    /// An event store that records what reached it, and can be told to fail.
    #[derive(Default)]
    struct Recording {
        appended: Mutex<Vec<SessionEventKind>>,
        durable_calls: Mutex<usize>,
        fail_append: bool,
        fail_durable: bool,
    }

    #[async_trait::async_trait]
    impl SessionEventRepository for Recording {
        async fn session_ids(&self) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn surface(
            &self,
            _s: &str,
        ) -> anyhow::Result<Option<crate::domain::session_event::SurfaceProjection>> {
            Ok(None)
        }

        async fn append(
            &self,
            _s: &str,
            kinds: Vec<SessionEventKind>,
        ) -> anyhow::Result<Vec<SessionEvent>> {
            if self.fail_append {
                anyhow::bail!("disk full");
            }
            let mut all = self.appended.lock().unwrap();
            let appended = kinds
                .iter()
                .enumerate()
                .map(|(i, kind)| SessionEvent::now((all.len() + i) as u64, kind.clone()))
                .collect();
            all.extend(kinds);
            Ok(appended)
        }
        async fn durable_flush(&self, _s: &str) -> anyhow::Result<()> {
            *self.durable_calls.lock().unwrap() += 1;
            if self.fail_durable {
                anyhow::bail!("fsync failed");
            }
            Ok(())
        }
        async fn events(&self, _s: &str) -> anyhow::Result<Vec<SessionEvent>> {
            Ok(Vec::new())
        }
        async fn events_from(&self, _s: &str, _seq: u64) -> anyhow::Result<Vec<SessionEvent>> {
            Ok(Vec::new())
        }

        async fn turn_boundary(&self, _session_id: &str) -> anyhow::Result<bool> {
            Ok(false)
        }

        async fn retain(&self, _session_id: &str, _keep_from: u64) -> anyhow::Result<Option<u64>> {
            Ok(None)
        }
    }

    struct Fixed(Decision);

    #[async_trait::async_trait]
    impl Approver for Fixed {
        async fn decide(&self, _request: &ApprovalRequest) -> Decision {
            self.0.clone()
        }
    }

    fn ctx(store: Arc<Recording>, decision: Decision) -> ToolContext {
        ToolContext::new(
            SessionContext::detached("s"),
            None,
            Arc::new(Fixed(decision)),
        )
        .with_approval_gate(ApprovalGate::new(store, "s", "t1", "call-0", 0))
    }

    fn request() -> ApprovalRequest {
        ApprovalRequest::normal("write /tmp/x").with_scope_key("file:write")
    }

    #[tokio::test]
    async fn an_allow_is_durable_before_the_tool_may_act_on_it() {
        let store = Arc::new(Recording::default());
        let decision = ctx(store.clone(), Decision::Allow).decide(&request()).await;
        assert!(decision.is_allowed());

        let events = store.appended.lock().unwrap();
        assert!(matches!(events[0], SessionEventKind::ApprovalRequested(_)));
        assert!(matches!(events[1], SessionEventKind::ApprovalResolved(_)));
        // Twice: once before the wait, once before Allow is handed back. Those
        // are the two boundaries a crash has to be able to land between.
        assert_eq!(*store.durable_calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn a_denial_needs_no_fsync_of_its_own() {
        // A denial runs nothing, so it can ride the next barrier like any other
        // record — only an allow licenses a side effect.
        let store = Arc::new(Recording::default());
        let decision = ctx(store.clone(), Decision::deny_because("no"))
            .decide(&request())
            .await;
        assert!(!decision.is_allowed());
        assert_eq!(
            *store.durable_calls.lock().unwrap(),
            1,
            "only the pre-wait flush"
        );
    }

    #[tokio::test]
    async fn a_request_that_cannot_be_recorded_is_never_asked() {
        // Fail closed: a prompt answered `yes` whose record was lost would let
        // the effect begin while the log still said it was never approved.
        let store = Arc::new(Recording {
            fail_append: true,
            ..Recording::default()
        });
        let decision = ctx(store.clone(), Decision::Allow).decide(&request()).await;
        assert!(!decision.is_allowed(), "must refuse, not proceed");
        assert!(store.appended.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_allow_that_cannot_be_made_durable_is_refused() {
        let store = Arc::new(Recording {
            fail_durable: true,
            ..Recording::default()
        });
        let decision = ctx(store.clone(), Decision::Allow).decide(&request()).await;
        assert!(
            !decision.is_allowed(),
            "refusing beats acting on an approval a crash would erase"
        );
    }

    #[tokio::test]
    async fn a_turn_with_no_event_log_still_approves_normally() {
        // Aux runtimes keep no log. They lose the not-started proof and nothing
        // else — approval itself must not depend on being recordable.
        let decision = ToolContext::new(
            SessionContext::detached("s"),
            None,
            Arc::new(Fixed(Decision::Allow)),
        )
        .decide(&request())
        .await;
        assert!(decision.is_allowed());
    }
}
