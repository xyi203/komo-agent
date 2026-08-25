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
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::domain::approval::{ApprovalRequest, Approver, Decision};
use crate::domain::cancel::CancelSignal;
use crate::domain::events::ToolEventSink;
use crate::domain::gateway::{InterjectSource, ReplySink};
use crate::domain::run::RunRepository;

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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
}

impl SessionOrigin {
    /// Whether no human is behind this turn, so the permission policy must
    /// evaluate it channel-lessly (only an `unattended = true` allow rule
    /// grants; no default and no saved grant ever does).
    pub fn is_unattended(self) -> bool {
        !matches!(self, Self::User)
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

/// The run-ledger handle for one turn (`domain/run.rs`, roadmap §7): created by
/// `AgentRuntime::run_turn` and passed **explicitly** down to the executor, so
/// ledgering and the per-turn call budget never depend on a caller having
/// established an ambient scope. Absent (`None` in [`ToolContext`]) for callers
/// without a ledger, so their tool use never pollutes it.
#[derive(Clone)]
pub struct RunContext {
    pub run_id: String,
    pub repo: Arc<dyn RunRepository>,
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
    pub fn new(run_id: String, repo: Arc<dyn RunRepository>) -> Self {
        Self {
            run_id,
            repo,
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
        }
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
        self.approver.decide(request).await
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
