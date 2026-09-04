//! The session's authoritative event log — vocabulary, envelope, and the folds
//! that decide what a log *means*.
//!
//! A `SessionEvent` is "one thing that already happened in this session". It is
//! immutable: nothing ever edits an event, and a later fact is expressed by
//! appending another event. Two rules follow from that and are enforced here
//! rather than at each call site:
//!
//! **Fail closed on anything unreadable.** An event type this build does not
//! know may change how the rest of the log must be read, so meeting one is a
//! refusal to reconstruct the session — not a skipped line. The single escape
//! is [`SessionEvent::ignorable`], which a writer sets only on records whose
//! loss cannot affect model history, recovery, or side-effect judgement.
//! Defaulting to *required* means a forgotten marker over-refuses (an
//! inconvenience) instead of silently resuming a gutted session. The first
//! version marks nothing ignorable; the mechanism exists so a later one can.
//!
//! **`seq` is the only order.** It is contiguous and assigned by the session's
//! single writer. [`SessionEvent::at`] is for display and diagnostics: a reader
//! must never sort or reason about recovery by it.
//!
//! This module is pure — no I/O, no storage layout. The segment/manifest
//! machinery that makes a log durable lives in `komo-infra`; what a *reader*
//! may conclude from a log lives here, so both sides cannot disagree.

use serde::{Deserialize, Serialize};

use super::message::{Message, Role};
use super::run::RecalledMemories;
use time::OffsetDateTime;

/// Bumped when an event's envelope or an existing payload changes shape in a
/// way serde defaults cannot absorb. A reader meeting a higher version refuses
/// the log rather than guessing at it.
pub const SESSION_EVENT_VERSION: u32 = 1;

// ── header ───────────────────────────────────────────────────────────────────

/// Who this log belongs to. Identity metadata, not something that happened:
/// it is written once when the session materializes and never appears as an
/// event, so `deriveMessages`-style folds never see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHeader {
    pub session_id: String,
    /// What drives this conversation (`user` / `cron` / `briefing` /
    /// `delegate`) — the same value the session record carries.
    pub origin: String,
    /// Workspace root this session's tools are confined to, when it has one.
    #[serde(default)]
    pub workspace: Option<String>,
    /// RFC 3339, UTC.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// The `SESSION_EVENT_VERSION` this log was created under.
    pub format_version: u32,
}

// ── envelope ─────────────────────────────────────────────────────────────────

/// One immutable entry in the session log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionEvent {
    #[serde(rename = "v")]
    pub version: u32,
    /// Monotonic, contiguous position within the session. The fold order.
    pub seq: u64,
    /// When it happened, RFC 3339 in UTC. **Display and diagnostics only** —
    /// never an ordering or recovery input; that is `seq`'s job alone. Stored
    /// as text because this file is one an operator opens and greps, and a
    /// timestamp they cannot read at a glance is one they will convert by hand.
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    /// A reader that does not recognize [`kind`](Self::kind) may skip this
    /// event instead of refusing the log. Absent means required.
    #[serde(default, skip_serializing_if = "is_false")]
    pub ignorable: bool,
    #[serde(flatten)]
    pub kind: SessionEventKind,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl SessionEvent {
    /// A required event of this build's version at `seq`.
    pub fn new(seq: u64, at: OffsetDateTime, kind: SessionEventKind) -> Self {
        Self {
            version: SESSION_EVENT_VERSION,
            seq,
            at,
            ignorable: false,
            kind,
        }
    }

    /// A required event stamped with the current time.
    pub fn now(seq: u64, kind: SessionEventKind) -> Self {
        Self::new(seq, OffsetDateTime::now_utc(), kind)
    }

    /// The turn this event belongs to, when it belongs to one.
    pub fn turn_id(&self) -> Option<&str> {
        match &self.kind {
            SessionEventKind::UserMessage(m) => Some(&m.turn_id)
                .filter(|t| !t.is_empty())
                .map(|t| t.as_str()),
            SessionEventKind::AssistantMessage(m) => Some(m.turn_id.as_str()),
            _ => None,
        }
    }

    /// The turn this event belongs to, across every kind that records work —
    /// wider than [`turn_id`](Self::turn_id), which answers only for the two
    /// events that reach the conversation surface.
    pub fn turn_id_of_work(&self) -> Option<&str> {
        match &self.kind {
            SessionEventKind::UserMessage(m) => Some(m.turn_id.as_str()),
            SessionEventKind::AssistantMessage(m) => Some(m.turn_id.as_str()),
            SessionEventKind::AssistantRound(r) => Some(r.turn_id.as_str()),
            SessionEventKind::ToolCallStarted(c) => Some(c.turn_id.as_str()),
            SessionEventKind::ToolCallSettled(c) => Some(c.turn_id.as_str()),
            SessionEventKind::ApprovalRequested(a) => Some(a.turn_id.as_str()),
            SessionEventKind::ApprovalResolved(a) => Some(a.turn_id.as_str()),
            SessionEventKind::ApprovalExpired { turn_id, .. } => Some(turn_id.as_str()),
            SessionEventKind::TurnSuspended(s) => Some(s.turn_id.as_str()),
            SessionEventKind::WakeupFired(w) => Some(w.turn_id.as_str()),
            SessionEventKind::TurnStarted { turn_id, .. }
            | SessionEventKind::TurnCompleted { turn_id }
            | SessionEventKind::TurnFailed { turn_id, .. }
            | SessionEventKind::TurnCancelled { turn_id, .. }
            | SessionEventKind::TurnMemories { turn_id, .. } => Some(turn_id.as_str()),
            _ => None,
        }
    }

    /// The surface placement this event declares, or `None` when it produces no
    /// long-term conversation message.
    ///
    /// Only the two message-producing variants can answer with `Some`, and they
    /// carry the field in their own payload — which is what makes "a non-message
    /// event with a `surfaceOp`" unrepresentable rather than merely invalid.
    pub fn surface(&self) -> Option<&SurfacePlacement> {
        match &self.kind {
            SessionEventKind::UserMessage(m) => Some(&m.surface),
            SessionEventKind::AssistantMessage(m) => Some(&m.surface),
            _ => None,
        }
    }
}

// ── vocabulary ───────────────────────────────────────────────────────────────

/// The first version's event vocabulary.
///
/// Adjacently tagged so a line reads `{"v":1,"seq":42,"at":…,"type":"tool/call-started","data":{…}}`
/// — the `type` is greppable by eye in a file an operator opens.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum SessionEventKind {
    #[serde(rename = "session/title-changed")]
    SessionTitleChanged { title: String },
    #[serde(rename = "session/model-changed")]
    SessionModelChanged { model: String, effort: String },

    #[serde(rename = "turn/started")]
    TurnStarted {
        turn_id: String,
        /// The interrupted turn this one continues, when it is a continuation
        /// rather than a fresh turn. The audit link a resumed turn is otherwise
        /// indistinguishable from a first attempt by.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resumed_from: Option<String>,
    },
    #[serde(rename = "user/message")]
    UserMessage(UserMessageEvent),
    #[serde(rename = "request/header")]
    RequestHeader(RequestHeaderEvent),
    #[serde(rename = "request/context")]
    RequestContext(RequestContextEvent),
    #[serde(rename = "assistant/round")]
    AssistantRound(AssistantRoundEvent),
    #[serde(rename = "tool/call-started")]
    ToolCallStarted(ToolCallStartedEvent),
    #[serde(rename = "tool/call-settled")]
    ToolCallSettled(ToolCallSettledEvent),
    #[serde(rename = "assistant/message")]
    AssistantMessage(AssistantMessageEvent),
    #[serde(rename = "turn/completed")]
    TurnCompleted { turn_id: String },
    #[serde(rename = "turn/failed")]
    TurnFailed { turn_id: String, error: String },
    #[serde(rename = "turn/cancelled")]
    TurnCancelled {
        turn_id: String,
        /// The turn was stopped before it did anything worth remembering — no
        /// tool ran. Its user message leaves the surface, so the conversation
        /// reads as if the turn never happened while the log still knows it
        /// did. A cancel *after* work is not pristine: those effects happened.
        #[serde(default)]
        pristine: bool,
    },

    /// Which stored memories reached this turn's prompt. Its own event rather
    /// than a `request/header` field because the header is written only when the
    /// envelope *changes* — recall changes every turn, so folding it in would
    /// defeat that dedup and rewrite the whole envelope each time.
    #[serde(rename = "turn/memories")]
    TurnMemories {
        turn_id: String,
        memories: RecalledMemories,
    },
    #[serde(rename = "compaction/started")]
    CompactionStarted { turn_id: String },
    #[serde(rename = "compaction/completed")]
    CompactionCompleted { turn_id: String },
    #[serde(rename = "learning/completed")]
    LearningCompleted { turn_id: String },
    #[serde(rename = "learning/skipped")]
    LearningSkipped { turn_id: String, reason: String },
    #[serde(rename = "approval/requested")]
    ApprovalRequested(ApprovalRequestedEvent),
    #[serde(rename = "approval/resolved")]
    ApprovalResolved(ApprovalResolvedEvent),
    /// The third outcome an approval can have, beside allowed and denied:
    /// nobody answered in time. Recorded as its own event because "denied" and
    /// "expired" are different things to tell the model, and because a turn
    /// that expired was *waiting* — an operator reading the log a week later
    /// needs to see that nobody was there, not a refusal that never happened.
    #[serde(rename = "approval/expired")]
    ApprovalExpired {
        turn_id: String,
        call_id: String,
        call_index: u32,
    },

    /// The turn stopped to wait for something, and gave up its session slot.
    ///
    /// A **durable barrier**, like `approval/requested`: written and flushed
    /// before the wait begins, or a crash cannot tell a turn that died waiting
    /// from one that died working — and only the first is worth waking up.
    #[serde(rename = "turn/suspended")]
    TurnSuspended(TurnSuspendedEvent),
    /// What ended the wait. The causal link between a suspension and the
    /// continuation that follows it: without it "why did this turn come back to
    /// life at 08:00" is not a question the log can answer.
    #[serde(rename = "wakeup/fired")]
    WakeupFired(WakeupFiredEvent),

    /// A turn started work that outlives it — a background `shell`, a detached
    /// `delegate` (docs/bot-runtime.md §5.9).
    #[serde(rename = "task/spawned")]
    TaskSpawned(TaskSpawnedEvent),
    /// That work finished. **This may land long after the turn ended**, which
    /// is the whole difference between a background task and a tool call: a
    /// call settles inside the round that made it, and this one settles
    /// whenever the work does. It names no turn for the same reason — the turn
    /// that started it may be over, and attributing a step to it would put work
    /// inside a run that had already closed.
    #[serde(rename = "task/settled")]
    TaskSettled(TaskSettledEvent),

    /// `/new`: the operator drew a line under the conversation so far.
    ///
    /// One appended event, not a new session id — the home conversation is one
    /// ordered timeline (docs/bot-runtime.md §3.8), and rotating the id would
    /// break that. It is invisible to seq, recovery, approvals and every
    /// projection; the *only* thing it decides is how far back the model is
    /// replayed by default ([`SurfaceProjection::replayed`]).
    #[serde(rename = "conversation/boundary")]
    ConversationBoundary {
        /// The turn that wrote it, when a turn did. A chat `/new` is not part
        /// of any turn, so it usually has none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
    },
}

/// What a suspended turn is waiting for.
///
/// One vocabulary for four things that look different to a user and identical
/// to the runtime: an approval, a question, a timer, and a job it started. Each
/// is "stop here, and come back when X" — differing only in what X is and what
/// the turn is handed on the way back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Wakeup {
    /// A wall-clock instant. `komo wait 2h`, and a routine that checks back.
    At { at: i64 },
    /// One gated call's approval — `/approve`, `/deny`, or nobody in time.
    Approval { call_id: String },
    /// The user's next message in this conversation.
    UserReply,
    /// A background job this turn started (`task/spawned`).
    TaskDone { task_id: String },
    /// Something outside komo: a webhook, a message from a particular peer.
    Event { filter: EventFilter },
}

impl Wakeup {
    /// A stable short name for the projection and for operator surfaces.
    pub fn kind(&self) -> WakeupKind {
        match self {
            Self::At { .. } => WakeupKind::At,
            Self::Approval { .. } => WakeupKind::Approval,
            Self::UserReply => WakeupKind::UserReply,
            Self::TaskDone { .. } => WakeupKind::TaskDone,
            Self::Event { .. } => WakeupKind::Event,
        }
    }
}

/// A [`Wakeup`] with its payload dropped: which of the five kinds of waiting
/// this is. What a projection stores and what an operator surface renders — the
/// payload is the runtime's business, "what are we waiting for" is theirs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WakeupKind {
    At,
    Approval,
    UserReply,
    TaskDone,
    Event,
}

impl WakeupKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::At => "at",
            Self::Approval => "approval",
            Self::UserReply => "user-reply",
            Self::TaskDone => "task-done",
            Self::Event => "event",
        }
    }

    /// What the operator sees.
    pub fn label(&self) -> &'static str {
        match self {
            Self::At => "定时等待",
            Self::Approval => "等你审批",
            Self::UserReply => "等待回答",
            Self::TaskDone => "等后台任务",
            Self::Event => "等事件",
        }
    }
}

/// What an [`Wakeup::Event`] listens for.
///
/// Matched against the thing that arrived, never against a name someone typed:
/// `waiting_on: "张三"` is for a human to read, and a `ChannelPeer` is what an
/// inbound message can actually be compared with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "on", rename_all = "kebab-case")]
pub enum EventFilter {
    /// Any inbound message from this correspondent, on any of komo's channels.
    FromPeer { platform: String, peer_id: String },
    /// A named inbound webhook (`POST /api/hooks/{name}`).
    Webhook { name: String },
}

/// The turn stopped to wait. `expires_at` is not optional in spirit — every
/// variant has a default — because a question nobody ever answers must not
/// leave a turn suspended forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnSuspendedEvent {
    pub turn_id: String,
    pub wakeup: Wakeup,
    /// The call that stopped. A call that stopped to wait **did not happen**,
    /// so this is what tells a continuation to re-dispatch it — the same
    /// reading `approval/requested` without a `resolved` carries — and what
    /// lets the tool recognise its own wake on the way back.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub call_id: String,
    /// One line for the operator: what this turn is waiting for, in words.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

/// Why a suspended turn is being woken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WakeupCause {
    Approve,
    Deny,
    Reply,
    Time,
    Event,
    Task,
    /// The wait ran out. Not a silent drop: the turn comes back and is told
    /// nobody answered.
    Expired,
    /// The user said something else instead of answering — which is an answer
    /// about what they want, and takes the pending wait's place.
    MovedOn,
}

impl WakeupCause {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Deny => "deny",
            Self::Reply => "reply",
            Self::Time => "time",
            Self::Event => "event",
            Self::Task => "task",
            Self::Expired => "expired",
            Self::MovedOn => "moved-on",
        }
    }
}

/// The wait ended. What the turn is handed on its way back rides in `payload` —
/// the user's answer, the event that fired, the note that nobody replied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeupFiredEvent {
    pub turn_id: String,
    /// The registration this fired, so a wake can be traced back to what
    /// scheduled it. Empty when nothing was registered (a wait resolved inside
    /// the process that opened it).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub wakeup_id: String,
    pub cause: WakeupCause,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payload: String,
}

/// What kind of work a background task is. Two today, and the executor treats
/// them the same — the distinction is for the operator reading the log and for
/// the line the model is handed when the task settles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskKind {
    Shell,
    Delegate,
}

impl TaskKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Shell => "shell",
            Self::Delegate => "delegate",
        }
    }
}

/// The turn handed work off and kept going.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpawnedEvent {
    /// The turn that started it — which may well be over by the time the
    /// matching `task/settled` arrives.
    pub turn_id: String,
    pub task_id: String,
    pub kind: TaskKind,
    /// One line naming the work, for the operator and for the wake that
    /// eventually reports it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub label: String,
}

/// The work finished, one way or another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSettledEvent {
    pub task_id: String,
    /// [`ToolOutcome::Uncertain`] is the same claim it makes on a tool call:
    /// nobody knows whether the work landed. A background task inherits it
    /// wholesale on restart — the process group died with the process, and the
    /// command may have completed first.
    pub outcome: ToolOutcome,
    /// Where the full output is kept (the tool-output store's path). Empty when
    /// there is nothing to keep — an uncertain settle written by the restart
    /// check has no output to point at.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub result_ref: String,
    /// What the model is told when this wakes a turn: the outcome in a few
    /// lines, with `result_ref` for the rest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(default)]
    pub elapsed_ms: i64,
}

/// Where a `user/message` came from. A compaction summary enters the surface as
/// a user message, so the surface fold needs no second message shape — but a
/// human transcript still has to tell it from something a person typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageSource {
    User,
    Compaction,
    /// Context a tool or hook injected into the conversation.
    Injected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserMessageEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub turn_id: String,
    pub content: String,
    pub source: MessageSource,
    #[serde(flatten)]
    pub surface: SurfacePlacement,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantMessageEvent {
    pub turn_id: String,
    pub content: String,
    /// Model-facing footnote: what tools this turn ran.
    ///
    /// The raw rounds do **not** re-enter the surface — replaying them would
    /// make every later turn pay for work already summarized. But a turn that
    /// cannot tell tools ran at all answers from nothing or runs them again, so
    /// the digest stays, exactly as the transcript carries it today. Derived at
    /// turn end from this turn's `tool/call-settled` events.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool_note: String,
    #[serde(flatten)]
    pub surface: SurfacePlacement,
}

/// Why a full `request/header` snapshot was written.
///
/// The envelope is large (a rendered system prompt plus every tool schema), so
/// an unchanged one is *inherited* rather than copied per round — the habit the
/// old turn journal had, and the reason a failed turn's rows dwarfed the
/// transcript they described.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HeaderReason {
    /// A new session's first provider request.
    Initial,
    /// The first request of a resumed loop — written even when identical, so a
    /// continuation boundary is visible in the log.
    Resume,
    /// The canonical header differs from the latest snapshot.
    Change,
}

/// The stable inputs of a provider request, apart from the derived messages.
///
/// Compared field-wise after canonicalization; anything that is not a *request
/// input* (route capacity, for one) stays out, or a route change would register
/// as an envelope change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestHeaderEvent {
    pub reason: HeaderReason,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub effort: String,
    /// Rendered system prompt; empty for a system-less request.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub system: String,
    /// Tool schemas in assembly order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<String>,
    /// Provider-specific request extras, opaque here. Part of the envelope: a
    /// continuation that dropped them would not be re-issuing the same request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

impl RequestHeaderEvent {
    /// Whether a new snapshot is needed. Compares only the request inputs —
    /// `reason` is how the snapshot was decided, not part of what it says.
    pub fn differs_from(&self, latest: &Self) -> bool {
        self.provider != latest.provider
            || self.model != latest.model
            || self.effort != latest.effort
            || self.system != latest.system
            || self.tools != latest.tools
            || self.extra != latest.extra
    }
}

/// Route metadata, appended only when the provider, model, or advertised
/// capacity changes. Deliberately outside [`RequestHeaderEvent`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContextEvent {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
}

/// One provider completion, verbatim, so an interrupted turn can be continued
/// rather than re-run. `blocks` is provider-shaped JSON this crate deliberately
/// does not model — the one writer and the one reader both live in the llm
/// layer, so the shape cannot fork.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantRoundEvent {
    pub turn_id: String,
    pub round: u32,
    /// The provider's id for this completion. Echoed back verbatim on a
    /// continuation, which is what carries a reasoning model's chain of thought
    /// across the interruption.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub response_id: String,
    pub blocks: serde_json::Value,
    #[serde(default)]
    pub tokens_in: i64,
    #[serde(default)]
    pub tokens_out: i64,
    #[serde(default)]
    pub tokens_cached: i64,
}

/// A tool call the round dispatched. Written — and made durable — **before**
/// the round runs, because "never started" and "started, outcome unknown" need
/// different answers on recovery and are otherwise indistinguishable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallStartedEvent {
    pub turn_id: String,
    pub call_id: String,
    /// Position in the assistant round's call list, from 0.
    ///
    /// The round runs concurrently, so `tool/call-settled` lands in *completion*
    /// order; a continuation must reassemble results in **this** order or the
    /// rebuilt request stops matching the one the live turn sent.
    pub call_index: u32,
    pub tool: String,
    /// Redacted arguments, as the ledger records them.
    pub args: String,
}

/// What became of one call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolOutcome {
    Succeeded,
    Failed,
    /// The call did not confirm its result — it may still have taken effect.
    Uncertain,
    /// Refused before the tool body ran (policy, approval, spin).
    Denied,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallSettledEvent {
    pub turn_id: String,
    pub call_id: String,
    pub call_index: u32,
    pub outcome: ToolOutcome,
    /// Model-facing text, already capped.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub result: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default)]
    pub elapsed_ms: i64,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub structured: serde_json::Value,
    /// Files holding the full output when it was too large to inline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_paths: Vec<String>,
}

/// Recorded — and made durable — *before* the approver is asked, so a crash
/// during the wait proves the tool body never ran. Without it the widest crash
/// window in a turn (a human may take five minutes) is indistinguishable from
/// "the effect may have landed".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequestedEvent {
    pub turn_id: String,
    pub call_id: String,
    pub call_index: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope_key: String,
}

/// How an approval resolved, and by which rung of the ladder. `waited_ms` is
/// recorded because "it was allowed" and "a person thought about it for four
/// minutes and then allowed it" are different facts about the same grant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalResolvedEvent {
    pub turn_id: String,
    pub call_id: String,
    pub call_index: u32,
    pub allowed: bool,
    /// Which rung decided: `hardline` / `config-deny` / `job-grant` /
    /// `saved-grant` / `config-allow` / `default` / `auto-review` / `human`.
    pub decided_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default)]
    pub waited_ms: i64,
}

/// The request envelope this turn will send, and why it is (or is not) worth a
/// new snapshot.
///
/// Called once per provider request. `latest` is
/// [`fold_request_header`]'s answer over everything already logged.
pub fn header_snapshot_reason(
    latest: Option<&RequestHeaderEvent>,
    current: &RequestHeaderEvent,
    resuming: bool,
) -> Option<HeaderReason> {
    match latest {
        // Nothing logged yet: the first request of a session always records its
        // envelope, or nothing downstream can say what the model was sent.
        None => Some(HeaderReason::Initial),
        // A continuation records its envelope even when identical, so the
        // boundary between the interrupted loop and the one that picked it up
        // is visible in the log rather than inferred from a gap.
        Some(_) if resuming => Some(HeaderReason::Resume),
        Some(latest) if current.differs_from(latest) => Some(HeaderReason::Change),
        // The ordinary case, and the one this function exists for: an unchanged
        // envelope is inherited. Copying a rendered system prompt and every tool
        // schema into every round is what made the old turn journal dwarf the
        // transcript it described.
        Some(_) => None,
    }
}

/// The request envelope in force at `through_seq`: the latest full snapshot at
/// or below it. There are no deltas to apply — a snapshot is always complete,
/// so reconstructing one is a search, not a replay.
pub fn fold_request_header(
    events: &[SessionEvent],
    through_seq: u64,
) -> Option<&RequestHeaderEvent> {
    events
        .iter()
        .filter(|e| e.seq <= through_seq)
        .filter_map(|e| match &e.kind {
            SessionEventKind::RequestHeader(header) => Some(header),
            _ => None,
        })
        .next_back()
}

/// The route in force at `through_seq`. Separate from the envelope on purpose:
/// capacity describes where a request went, not what was in it, so a route
/// change must not register as an envelope change.
pub fn fold_request_context(
    events: &[SessionEvent],
    through_seq: u64,
) -> Option<&RequestContextEvent> {
    events
        .iter()
        .filter(|e| e.seq <= through_seq)
        .filter_map(|e| match &e.kind {
            SessionEventKind::RequestContext(context) => Some(context),
            _ => None,
        })
        .next_back()
}

// ── surface placement ────────────────────────────────────────────────────────

/// How a message-producing event joins the ordered conversation surface — the
/// history later turns replay.
///
/// `Replace` is what a compaction is: the summary shadows a contiguous run of
/// surface entries. The shadowed events stay in the authoritative log, which is
/// why a human transcript can still show what the summary covered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SurfaceOp {
    Append,
    Replace { start: u64, end: u64 },
}

/// A surface declaration plus the events it cites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfacePlacement {
    #[serde(rename = "surfaceOp")]
    pub op: SurfaceOp,
    /// Every surface node this event shadows. Required to be complete for a
    /// `Replace`; unused for an `Append`.
    #[serde(
        rename = "sourceEventSeqs",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub source_event_seqs: Vec<u64>,
}

impl SurfacePlacement {
    pub fn append() -> Self {
        Self {
            op: SurfaceOp::Append,
            source_event_seqs: Vec::new(),
        }
    }

    pub fn replace(start: u64, end: u64, shadowed: Vec<u64>) -> Self {
        Self {
            op: SurfaceOp::Replace { start, end },
            source_event_seqs: shadowed,
        }
    }
}

/// Every attempt at one logical turn: `turn_id` plus the ids it was resumed
/// from, transitively.
///
/// A continuation is a new turn in the log, linked back by `resumed_from`, so
/// A→B→C is three ids for one question. Rebuilding C from C alone loses A's and
/// B's rounds — safe (it just re-does the work) but not the semantics anyone
/// wants: one crash would be recoverable and two would not.
///
/// Bounded by the number of `turn/started` events, so a `resumed_from` cycle
/// (which the log's own ordering makes impossible, but nothing here proves)
/// cannot loop.
pub fn attempt_chain(events: &[SessionEvent], turn_id: &str) -> std::collections::HashSet<String> {
    let mut chain = std::collections::HashSet::new();
    chain.insert(turn_id.to_string());
    let mut current = turn_id.to_string();
    loop {
        let parent = events.iter().find_map(|event| match &event.kind {
            SessionEventKind::TurnStarted {
                turn_id,
                resumed_from: Some(from),
            } if *turn_id == current => Some(from.clone()),
            _ => None,
        });
        match parent {
            Some(from) if chain.insert(from.clone()) => current = from,
            _ => return chain,
        }
    }
}

/// What ended one call's wait, as the log recorded it.
///
/// Handed back to the call that stopped, so a `wait` that has come due returns
/// "the time you asked for arrived" instead of stopping the turn a second time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumedWait {
    /// The call that stopped — the one this wake belongs to.
    pub call_id: String,
    pub wakeup: Wakeup,
    pub cause: WakeupCause,
    /// What the wake brought: the user's answer, an event body, nothing for a
    /// timer.
    pub payload: String,
}

/// Everything one turn's chain of attempts has waited for.
///
/// Two answers from one fold, because a tool asks both at once: *have I already
/// waited too often this turn* — the budget, which has to survive a suspension
/// a per-process counter would lose — and *am I being re-dispatched because my
/// own wait ended*.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnWaits {
    /// Every wait this turn registered, oldest first, served or not.
    pub taken: Vec<Wakeup>,
    /// The wake that brought this attempt back, if one did.
    pub resumed: Option<ResumedWait>,
}

/// Fold a turn's waits out of its session log.
///
/// Read across the whole [`attempt_chain`], because a suspension is recorded
/// against the turn that *stopped* and the turn asking now is its continuation
/// — the same reason the approval gate reads the chain rather than one id. A
/// `wakeup/fired` is matched to the suspension it ended by position: a turn
/// stops at most once per attempt, so the fired wake belongs to whichever
/// suspension was standing when it landed.
pub fn fold_turn_waits(events: &[SessionEvent], turn_id: &str) -> TurnWaits {
    let attempts = attempt_chain(events, turn_id);
    let mut waits = TurnWaits::default();
    let mut standing: Option<TurnSuspendedEvent> = None;
    for event in events.iter().filter(|event| {
        event
            .turn_id_of_work()
            .is_some_and(|id| attempts.contains(id))
    }) {
        match &event.kind {
            SessionEventKind::TurnSuspended(suspended) => {
                waits.taken.push(suspended.wakeup.clone());
                standing = Some(suspended.clone());
            }
            SessionEventKind::WakeupFired(fired) => {
                if let Some(suspended) = standing.take() {
                    waits.resumed = Some(ResumedWait {
                        call_id: suspended.call_id,
                        wakeup: suspended.wakeup,
                        cause: fired.cause,
                        payload: fired.payload.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    waits
}

/// Project the conversation surface into the messages a later turn replays.
///
/// Only the surface is read, so a compaction's `replace` takes effect here with
/// no special case: the summary stands where the messages it covers used to.
/// Tool activity reaches the model as the assistant message's `tool_note`, not
/// as replayed rounds — see [`AssistantMessageEvent::tool_note`].
///
/// `dense_from` is the log's `truncated_before` — see [`fold_surface`].
pub fn derive_messages(
    events: &[SessionEvent],
    dense_from: u64,
) -> Result<Vec<Message>, FoldError> {
    SurfaceProjection::fold(events, dense_from)?.messages()
}

/// What one surface node contributes to the replayed conversation.
///
/// The log stays the authority on what an event *says*; this is that content
/// projected — which is what a checkpoint can carry across a process without
/// re-reading the events it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceContent {
    pub role: SurfaceRole,
    pub text: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool_note: String,
    /// The turn that put this node on the surface. Carried so a pristine cancel
    /// can take its own back even when the node it removes was folded by an
    /// earlier read — which is what makes a checkpoint safe to take anywhere.
    pub turn: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SurfaceRole {
    User,
    /// Something the user said while the turn was already running.
    Injected,
    Assistant,
}

/// One event's contribution to the surface, or `None` if it makes no message.
pub fn surface_content(event: &SessionEvent) -> Option<SurfaceContent> {
    match &event.kind {
        SessionEventKind::UserMessage(m) => Some(SurfaceContent {
            role: match m.source {
                MessageSource::Injected => SurfaceRole::Injected,
                _ => SurfaceRole::User,
            },
            text: m.content.clone(),
            tool_note: String::new(),
            turn: m.turn_id.clone(),
        }),
        SessionEventKind::AssistantMessage(m) => Some(SurfaceContent {
            role: SurfaceRole::Assistant,
            text: m.content.clone(),
            tool_note: m.tool_note.clone(),
            turn: m.turn_id.clone(),
        }),
        _ => None,
    }
}

/// Records one turn's events into its session's log.
///
/// Handed to the LLM backend for the duration of a turn, because that is the
/// only place provider-shaped state exists. Recording is best-effort by
/// contract: a failure costs resumability and nothing else — it must never fail
/// the turn it was describing.
#[async_trait::async_trait]
pub trait TurnRecorder: Send + Sync {
    /// The turn these events belong to. The recorder is bound to one turn, so
    /// the backend does not have to carry the id alongside it.
    fn turn_id(&self) -> &str;

    async fn record(&self, kinds: Vec<SessionEventKind>);
    /// Make everything recorded so far survive a crash. Called before an effect
    /// whose attribution depends on the record having landed.
    async fn durable(&self);
}

// ── folds ────────────────────────────────────────────────────────────────────

/// Why a log could not be read as a session.
///
/// Every variant is a refusal, not a warning: a reader that cannot account for
/// an event cannot know what the rest of the log means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldError {
    /// An event type this build does not know, and the writer did not mark it
    /// skippable.
    UnknownEventType { seq: u64, type_name: String },
    /// Written by a newer komo.
    UnsupportedVersion { seq: u64, version: u32 },
    /// `seq` is not contiguous — the log has a hole, so what is missing cannot
    /// be reasoned about.
    SeqGap { expected: u64, found: u64 },
    /// A replacement cited a range the surface does not hold, or did not cite
    /// everything it shadows.
    InvalidReplacement { seq: u64, reason: String },
    /// The line is not a readable event at all.
    Malformed { seq: Option<u64>, error: String },
}

impl std::fmt::Display for FoldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownEventType { seq, type_name } => write!(
                f,
                "event {seq} has type `{type_name}`, which this komo does not know \
                 and its writer did not mark ignorable — upgrade komo to read this session"
            ),
            Self::UnsupportedVersion { seq, version } => write!(
                f,
                "event {seq} was written by a newer komo (format {version}, this build reads \
                 {SESSION_EVENT_VERSION}) — upgrade komo to read this session"
            ),
            Self::SeqGap { expected, found } => {
                write!(f, "session log jumps from seq {expected} to {found}")
            }
            Self::InvalidReplacement { seq, reason } => {
                write!(
                    f,
                    "event {seq} is not a valid surface replacement: {reason}"
                )
            }
            Self::Malformed { seq, error } => match seq {
                Some(seq) => write!(f, "event {seq} is unreadable: {error}"),
                None => write!(f, "unreadable session event: {error}"),
            },
        }
    }
}

impl std::error::Error for FoldError {}

/// Decode one stored line.
///
/// `Ok(None)` means the line was an unrecognized event its writer marked
/// ignorable — the one case a reader may skip. Everything else is a refusal.
pub fn decode_event(line: &str) -> Result<Option<SessionEvent>, FoldError> {
    // Read the envelope first: whether an unknown type is fatal is the writer's
    // call, and that answer is in the envelope, not in the payload.
    let envelope: EventEnvelope =
        serde_json::from_str(line).map_err(|error| FoldError::Malformed {
            seq: None,
            error: error.to_string(),
        })?;
    if envelope.version > SESSION_EVENT_VERSION {
        return Err(FoldError::UnsupportedVersion {
            seq: envelope.seq,
            version: envelope.version,
        });
    }
    match serde_json::from_str::<SessionEvent>(line) {
        Ok(event) => Ok(Some(event)),
        Err(error) if envelope.ignorable => {
            tracing::debug!(seq = envelope.seq, %error, "skipped an ignorable session event");
            Ok(None)
        }
        Err(_) if !KNOWN_EVENT_TYPES.contains(&envelope.type_name.as_str()) => {
            Err(FoldError::UnknownEventType {
                seq: envelope.seq,
                type_name: envelope.type_name,
            })
        }
        Err(error) => Err(FoldError::Malformed {
            seq: Some(envelope.seq),
            error: error.to_string(),
        }),
    }
}

/// Just enough of a line to decide how to read the rest of it.
#[derive(Deserialize)]
struct EventEnvelope {
    #[serde(rename = "v", default = "default_version")]
    version: u32,
    seq: u64,
    #[serde(default)]
    ignorable: bool,
    #[serde(rename = "type")]
    type_name: String,
}

fn default_version() -> u32 {
    SESSION_EVENT_VERSION
}

/// Every type this build writes. An unrecognized type outside this list is what
/// the fail-closed rule is about; a *known* type that will not parse is a
/// malformed record instead.
pub const KNOWN_EVENT_TYPES: &[&str] = &[
    "session/title-changed",
    "session/model-changed",
    "turn/started",
    "user/message",
    "request/header",
    "request/context",
    "assistant/round",
    "tool/call-started",
    "tool/call-settled",
    "assistant/message",
    "turn/completed",
    "turn/failed",
    "turn/cancelled",
    "compaction/started",
    "compaction/completed",
    "learning/completed",
    "learning/skipped",
    "approval/requested",
    "approval/resolved",
    "approval/expired",
    "turn/suspended",
    "wakeup/fired",
    "task/spawned",
    "task/settled",
    "conversation/boundary",
];

/// The ordered conversation surface, folded from a log.
///
/// Holds seqs, not content: the log stays the authority on what an event says,
/// and this answers only *which* events a later turn replays, in what order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Surface {
    nodes: Vec<u64>,
    /// Committed positional replacements, so an incremental consumer can tell
    /// plain tail growth from a rewrite.
    replace_generation: u64,
    /// The seq of the most recent `conversation/boundary`, when one was drawn.
    ///
    /// Nothing leaves the surface for it: the conversation still happened, and
    /// the transcript still shows it. It only marks where the model's default
    /// replay starts — see [`SurfaceProjection::replayed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    boundary: Option<u64>,
}

impl Surface {
    pub fn nodes(&self) -> &[u64] {
        &self.nodes
    }

    pub fn replace_generation(&self) -> u64 {
        self.replace_generation
    }

    pub fn boundary(&self) -> Option<u64> {
        self.boundary
    }

    /// Drop every node the predicate rejects. Used by the pristine-cancel rule;
    /// deliberately not a general edit — nothing else may remove a node once it
    /// is on the surface except a replacement.
    fn retain(&mut self, keep: impl Fn(&u64) -> bool) {
        self.nodes.retain(|seq| keep(seq));
    }

    /// Apply one message-producing event's declaration.
    pub fn apply(&mut self, seq: u64, placement: &SurfacePlacement) -> Result<(), FoldError> {
        match placement.op {
            SurfaceOp::Append => {
                self.nodes.push(seq);
                Ok(())
            }
            SurfaceOp::Replace { start, end } => {
                let invalid = |reason: &str| FoldError::InvalidReplacement {
                    seq,
                    reason: reason.to_string(),
                };
                let from = self
                    .nodes
                    .iter()
                    .position(|n| *n == start)
                    .ok_or_else(|| invalid("`start` is not on the surface"))?;
                let to = self
                    .nodes
                    .iter()
                    .position(|n| *n == end)
                    .ok_or_else(|| invalid("`end` is not on the surface"))?;
                if from > to {
                    return Err(invalid("`start` comes after `end`"));
                }
                let shadowed: Vec<u64> = self.nodes[from..=to].to_vec();
                // Citing every shadowed node is what lets a human transcript
                // show what a summary covered. An incomplete citation would
                // lose that silently.
                if !shadowed
                    .iter()
                    .all(|n| placement.source_event_seqs.contains(n))
                {
                    return Err(invalid(
                        "`sourceEventSeqs` does not cite every shadowed surface node",
                    ));
                }
                self.nodes.splice(from..=to, [seq]);
                self.replace_generation += 1;
                Ok(())
            }
        }
    }
}

/// Fold a whole log: check `seq` contiguity from `dense_from` and build the
/// surface. `dense_from` is the log's `truncated_before` — 0 for a log that has
/// never been truncated.
///
/// Events **below** `dense_from` are a retention base's survivors, and their
/// seqs have holes on purpose: the base keeps what still matters and the events
/// the missing seqs named are gone. That is a truncation, not a log that lost
/// them, so only their order is checked. From `dense_from` on, a hole is a hole.
pub fn fold_surface(events: &[SessionEvent], dense_from: u64) -> Result<Surface, FoldError> {
    fold_surface_from(
        Surface::default(),
        &mut std::collections::HashMap::new(),
        events,
        dense_from,
        dense_from,
    )
}

/// Continue a surface fold: `events` must begin at `expected`, and `surface` is
/// what folding everything below it produced.
///
/// `owner` says which turn put each node already on `surface` there — the
/// pristine-cancel rule's input, and the reason a checkpoint may be taken at any
/// point rather than only at a turn boundary. It is extended as the fold goes.
pub fn fold_surface_from(
    surface: Surface,
    owner: &mut std::collections::HashMap<u64, String>,
    events: &[SessionEvent],
    dense_from: u64,
    expected: u64,
) -> Result<Surface, FoldError> {
    let mut surface = surface;
    let mut expected = expected;
    let mut previous: Option<u64> = None;
    for event in events {
        if event.seq < dense_from {
            if let Some(previous) = previous.filter(|p| event.seq <= *p) {
                return Err(FoldError::SeqGap {
                    expected: previous + 1,
                    found: event.seq,
                });
            }
        } else if event.seq != expected {
            return Err(FoldError::SeqGap {
                expected,
                found: event.seq,
            });
        } else {
            expected += 1;
        }
        previous = Some(event.seq);
        if let SessionEventKind::ConversationBoundary { .. } = &event.kind {
            surface.boundary = Some(event.seq);
        }
        if let Some(placement) = event.surface() {
            surface.apply(event.seq, placement)?;
            if let Some(turn) = event.turn_id() {
                owner.insert(event.seq, turn.to_string());
            }
        }
        // A turn stopped before it did anything is not a thing that was said.
        // The events stay in the log — the log never forgets that the user
        // asked and then stopped — but the conversation a later turn replays
        // must not carry a question nobody answered.
        if let SessionEventKind::TurnCancelled {
            turn_id,
            pristine: true,
        } = &event.kind
        {
            surface.retain(|seq| owner.get(seq) != Some(turn_id));
        }
    }
    Ok(surface)
}

/// Version of the surface projection's shape and meaning. A checkpoint written
/// by any other version is ignored and re-folded: the log is the authority, so
/// the cost of a mismatch is time, never a wrong history.
pub const SURFACE_PROJECTION_VERSION: u32 = 1;

/// The conversation surface, folded and carryable.
///
/// A **cache, never an authority.** Deleting it changes nothing but the time
/// the next read takes; the events it was folded from are still there. What it
/// buys is that a long-lived session stops re-parsing its whole log on every
/// turn just to hand the model the last few messages.
///
/// It carries content as well as node seqs, because the events below the tail
/// are exactly what an incremental read is trying not to read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceProjection {
    /// Shape version — see [`SURFACE_PROJECTION_VERSION`].
    pub v: u32,
    /// The highest seq folded in, or `None` for a log with no events yet.
    pub through_seq: Option<u64>,
    /// The log's `truncated_before` when this was folded. A retention cut moves
    /// it, and every seq below it means something different afterwards, so a
    /// checkpoint from before the cut is not resumable.
    pub dense_from: u64,
    pub surface: Surface,
    /// The content of each node **on the surface**, in no particular order.
    /// Shadowed nodes are dropped: what a summary covered is read from the log,
    /// not from here.
    pub content: Vec<(u64, SurfaceContent)>,
}

impl SurfaceProjection {
    /// Fold a whole log — the cold path, and the one a missing or unusable
    /// checkpoint falls back to.
    pub fn fold(events: &[SessionEvent], dense_from: u64) -> Result<Self, FoldError> {
        Self {
            v: SURFACE_PROJECTION_VERSION,
            through_seq: None,
            dense_from,
            surface: Surface::default(),
            content: Vec::new(),
        }
        .extend_from(events, dense_from)
    }

    /// Whether this checkpoint can be continued over a log whose
    /// `truncated_before` is `dense_from`.
    pub fn resumable(&self, dense_from: u64) -> bool {
        self.v == SURFACE_PROJECTION_VERSION && self.dense_from == dense_from
    }

    /// The seq a resuming read must ask the log for.
    pub fn read_from(&self) -> u64 {
        self.through_seq
            .map_or(self.dense_from, |through| through + 1)
    }

    /// Fold `tail` — which must begin at [`Self::read_from`] — onto this.
    pub fn extend(&self, tail: &[SessionEvent]) -> Result<Self, FoldError> {
        self.extend_from(tail, self.read_from())
    }

    fn extend_from(&self, events: &[SessionEvent], expected: u64) -> Result<Self, FoldError> {
        let mut content: std::collections::HashMap<u64, SurfaceContent> =
            self.content.iter().cloned().collect();
        let mut owner: std::collections::HashMap<u64, String> = content
            .iter()
            .map(|(seq, node)| (*seq, node.turn.clone()))
            .collect();
        let surface = fold_surface_from(
            self.surface.clone(),
            &mut owner,
            events,
            self.dense_from,
            expected,
        )?;
        for event in events {
            if let Some(node) = surface_content(event) {
                content.insert(event.seq, node);
            }
        }
        // Only what the surface still holds: a shadowed node's content is dead
        // weight in a file that is rewritten every turn.
        let live: Vec<(u64, SurfaceContent)> = surface
            .nodes()
            .iter()
            .filter_map(|seq| content.remove(seq).map(|node| (*seq, node)))
            .collect();
        Ok(Self {
            v: SURFACE_PROJECTION_VERSION,
            through_seq: events.last().map(|event| event.seq).or(self.through_seq),
            dense_from: self.dense_from,
            surface,
            content: live,
        })
    }

    /// The whole conversation, oldest first — the transcript.
    ///
    /// A `conversation/boundary` is deliberately invisible here: what the
    /// operator drew a line under still happened, and every reader of the
    /// *record* (the session tool, episodic indexing, the reviewer, a client
    /// hydrating a window) must still see it. What the boundary decides is
    /// [`replay`](Self::replay).
    pub fn messages(&self) -> Result<Vec<Message>, FoldError> {
        self.messages_of(self.surface.nodes())
    }

    /// The messages a later turn replays: everything after the most recent
    /// `conversation/boundary`.
    ///
    /// The cut lands on the **last assistant node at or before the boundary**,
    /// not on the boundary itself. A turn that was still open when the line was
    /// drawn — one suspended on an approval, say — has a user message on the
    /// surface with no answer under it, and hiding that would leave its
    /// continuation replying to a conversation it cannot see. `/new` ends a
    /// context, not a turn already in flight (docs/bot-runtime.md §3.8).
    pub fn replayed(&self) -> Vec<u64> {
        let nodes = self.surface.nodes();
        let Some(boundary) = self.surface.boundary() else {
            return nodes.to_vec();
        };
        let content: std::collections::HashMap<u64, &SurfaceContent> = self
            .content
            .iter()
            .map(|(seq, node)| (*seq, node))
            .collect();
        let cut = nodes.iter().rposition(|seq| {
            *seq <= boundary
                && content.get(seq).map(|node| node.role) == Some(SurfaceRole::Assistant)
        });
        match cut {
            Some(last) => nodes[last + 1..].to_vec(),
            None => nodes.to_vec(),
        }
    }

    /// [`messages`](Self::messages), cut at the conversation boundary.
    pub fn replay(&self) -> Result<Vec<Message>, FoldError> {
        self.messages_of(&self.replayed())
    }

    fn messages_of(&self, nodes: &[u64]) -> Result<Vec<Message>, FoldError> {
        let content: std::collections::HashMap<u64, &SurfaceContent> = self
            .content
            .iter()
            .map(|(seq, node)| (*seq, node))
            .collect();
        let mut out: Vec<Message> = Vec::with_capacity(nodes.len());
        for seq in nodes {
            let Some(node) = content.get(seq) else {
                // The surface only ever holds nodes this folded, so this is
                // unreachable through `fold` — but a caller assembling one by
                // hand must not silently get a short history.
                return Err(FoldError::InvalidReplacement {
                    seq: *seq,
                    reason: "surface node has no content".to_string(),
                });
            };
            match node.role {
                // Something the user said while the turn was already running
                // belongs to that turn's input, not to a turn of its own: two
                // consecutive user messages is exactly what a transcript may
                // not contain, and several providers reject it on replay.
                SurfaceRole::Injected => {
                    match out.last_mut().filter(|last| last.role == Role::User) {
                        Some(last) => {
                            last.content.push('\n');
                            last.content.push_str(&node.text);
                        }
                        None => out.push(Message::user(&node.text)),
                    }
                }
                SurfaceRole::User => out.push(Message::user(&node.text)),
                SurfaceRole::Assistant => {
                    out.push(Message::assistant(&node.text).with_tool_note(&node.tool_note))
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;

    fn at(text: &str) -> OffsetDateTime {
        OffsetDateTime::parse(text, &Rfc3339).unwrap()
    }

    fn user(seq: u64, text: &str) -> SessionEvent {
        SessionEvent::new(
            seq,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: "turn-1".into(),
                content: text.into(),
                source: MessageSource::User,
                surface: SurfacePlacement::append(),
            }),
        )
    }

    fn assistant(seq: u64, text: &str) -> SessionEvent {
        SessionEvent::new(
            seq,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::AssistantMessage(AssistantMessageEvent {
                turn_id: "turn-1".into(),
                content: text.into(),
                tool_note: String::new(),
                surface: SurfacePlacement::append(),
            }),
        )
    }

    #[test]
    fn an_event_round_trips_through_its_stored_line() {
        let event = SessionEvent::new(
            42,
            at("2026-09-01T10:30:00Z"),
            SessionEventKind::ToolCallStarted(ToolCallStartedEvent {
                turn_id: "turn-7".into(),
                call_id: "call-3".into(),
                call_index: 0,
                tool: "shell".into(),
                args: r#"{"command":"cargo test"}"#.into(),
            }),
        );
        let line = serde_json::to_string(&event).unwrap();
        // The type is a top-level, greppable field — an operator opening the
        // file should not have to know the payload shape to see what happened.
        assert!(line.contains(r#""type":"tool/call-started""#), "{line}");
        assert!(line.contains(r#""seq":42"#), "{line}");
        // Required is the default, so it costs no bytes.
        assert!(!line.contains("ignorable"), "{line}");
        assert_eq!(decode_event(&line).unwrap(), Some(event));
    }

    #[test]
    fn a_message_event_writes_its_surface_declaration_inline() {
        // Locks the stored shape: everything downstream (segments, folds, the
        // human transcript) reads these bytes, so a silent change here is a
        // silent change to every session on disk.
        let event = SessionEvent::new(
            4,
            at("2026-09-01T10:30:00Z"),
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: "turn-2".into(),
                content: "[summary]".into(),
                source: MessageSource::Compaction,
                surface: SurfacePlacement::replace(0, 1, vec![0, 1]),
            }),
        );
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"v":1,"seq":4,"at":"2026-09-01T10:30:00Z","type":"user/message","data":{"turn_id":"turn-2","content":"[summary]","source":"compaction","surfaceOp":{"replace":{"start":0,"end":1}},"sourceEventSeqs":[0,1]}}"#
        );

        let plain = user(0, "hi");
        assert_eq!(
            serde_json::to_string(&plain).unwrap(),
            r#"{"v":1,"seq":0,"at":"2026-08-31T00:00:00Z","type":"user/message","data":{"turn_id":"turn-1","content":"hi","source":"user","surfaceOp":"append"}}"#
        );
    }

    #[test]
    fn a_fresh_turn_costs_no_bytes_for_the_continuation_link() {
        // Most turns are not continuations, and `turn/started` is written once
        // per turn on every session — an always-present null would be pure
        // overhead on the most common event there is.
        let fresh = SessionEvent::new(
            0,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::TurnStarted {
                turn_id: "turn-1".into(),
                resumed_from: None,
            },
        );
        assert_eq!(
            serde_json::to_string(&fresh).unwrap(),
            r#"{"v":1,"seq":0,"at":"2026-08-31T00:00:00Z","type":"turn/started","data":{"turn_id":"turn-1"}}"#
        );

        let continued = SessionEvent::new(
            1,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::TurnStarted {
                turn_id: "turn-2".into(),
                resumed_from: Some("turn-1".into()),
            },
        );
        assert_eq!(
            serde_json::to_string(&continued).unwrap(),
            r#"{"v":1,"seq":1,"at":"2026-08-31T00:00:00Z","type":"turn/started","data":{"turn_id":"turn-2","resumed_from":"turn-1"}}"#
        );
        assert_eq!(
            decode_event(&serde_json::to_string(&continued).unwrap()).unwrap(),
            Some(continued)
        );
    }

    #[test]
    fn recall_is_its_own_event_so_the_envelope_stays_deduped() {
        // `request/header` is written only when the envelope changes; recall
        // changes every turn, so folding it in there would rewrite the whole
        // envelope — system prompt and tool schemas included — each time.
        let event = SessionEvent::new(
            7,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::TurnMemories {
                turn_id: "turn-1".into(),
                memories: RecalledMemories {
                    pinned: vec!["m1".into()],
                    recall: vec!["m2".into()],
                },
            },
        );
        let line = serde_json::to_string(&event).unwrap();
        assert!(line.contains(r#""type":"turn/memories""#), "{line}");
        assert_eq!(decode_event(&line).unwrap(), Some(event));
    }

    #[test]
    fn an_unknown_required_event_refuses_the_log() {
        // Written by a newer komo that added a type this build does not know.
        let line = r#"{"v":1,"seq":9,"at":"2026-08-31T00:00:00Z","type":"workflow/step-entered","data":{}}"#;
        assert_eq!(
            decode_event(line),
            Err(FoldError::UnknownEventType {
                seq: 9,
                type_name: "workflow/step-entered".into(),
            })
        );
    }

    #[test]
    fn an_unknown_ignorable_event_is_skipped_instead() {
        // The one escape: its writer promised losing it cannot change what the
        // rest of the log means.
        let line = r#"{"v":1,"seq":9,"at":"2026-08-31T00:00:00Z","ignorable":true,"type":"telemetry/first-token","data":{}}"#;
        assert_eq!(decode_event(line), Ok(None));
    }

    #[test]
    fn a_newer_format_version_refuses_before_the_payload_is_read() {
        let line = r#"{"v":2,"seq":9,"at":"2026-08-31T00:00:00Z","type":"turn/started","data":{"turn_id":"t"}}"#;
        assert_eq!(
            decode_event(line),
            Err(FoldError::UnsupportedVersion { seq: 9, version: 2 })
        );
    }

    #[test]
    fn the_first_version_marks_nothing_ignorable() {
        // The mechanism exists for a later version; shipping one now would mean
        // this build already tolerates losing something.
        let events = [
            user(0, "hi"),
            assistant(1, "hello"),
            SessionEvent::new(
                2,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::TurnStarted {
                    turn_id: "t".into(),
                    resumed_from: None,
                },
            ),
        ];
        assert!(events.iter().all(|e| !e.ignorable));
    }

    #[test]
    fn only_message_events_can_declare_a_surface_placement() {
        assert!(user(0, "hi").surface().is_some());
        assert!(assistant(1, "hello").surface().is_some());
        // Not "invalid" — unrepresentable: the field lives in the two message
        // payloads, so no other variant has one to set.
        let round = SessionEvent::new(
            2,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::AssistantRound(AssistantRoundEvent {
                turn_id: "t".into(),
                round: 0,
                response_id: String::new(),
                blocks: serde_json::json!([]),
                tokens_in: 0,
                tokens_out: 0,
                tokens_cached: 0,
            }),
        );
        assert!(round.surface().is_none());
    }

    #[test]
    fn derived_messages_are_the_surface_in_order() {
        let events = [user(0, "q1"), assistant(1, "a1"), user(2, "q2")];
        let messages = derive_messages(&events, 0).unwrap();
        assert_eq!(
            messages
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            vec!["q1", "a1", "q2"]
        );
        assert_eq!(messages[1].role, super::super::message::Role::Assistant);
    }

    #[test]
    fn a_compaction_summary_stands_where_the_messages_it_covers_used_to() {
        let mut events = vec![user(0, "q1"), assistant(1, "a1"), user(2, "q2")];
        events.push(SessionEvent::new(
            3,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: "turn-2".into(),
                content: "[summary of q1/a1]".into(),
                source: MessageSource::Compaction,
                surface: SurfacePlacement::replace(0, 1, vec![0, 1]),
            }),
        ));
        // No special case in the projection: the surface already resolved it.
        assert_eq!(
            derive_messages(&events, 0)
                .unwrap()
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            vec!["[summary of q1/a1]", "q2"]
        );
    }

    #[test]
    fn tool_activity_reaches_the_model_as_a_note_not_as_replayed_rounds() {
        // A round and its calls are durable, but they are not conversation: a
        // later turn must not pay for work its assistant message already
        // summarized — while still being able to tell that tools ran at all.
        let events = vec![
            user(0, "run the tests"),
            SessionEvent::new(
                1,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::AssistantRound(AssistantRoundEvent {
                    turn_id: "turn-1".into(),
                    round: 0,
                    response_id: String::new(),
                    blocks: serde_json::json!([{"tool_call": "shell"}]),
                    tokens_in: 100,
                    tokens_out: 20,
                    tokens_cached: 0,
                }),
            ),
            SessionEvent::new(
                2,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::ToolCallSettled(ToolCallSettledEvent {
                    turn_id: "turn-1".into(),
                    call_id: "c0".into(),
                    call_index: 0,
                    outcome: ToolOutcome::Succeeded,
                    result: "test result: ok".into(),
                    error: String::new(),
                    elapsed_ms: 4100,
                    structured: serde_json::Value::Null,
                    output_paths: vec![],
                }),
            ),
            SessionEvent::new(
                3,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::AssistantMessage(AssistantMessageEvent {
                    turn_id: "turn-1".into(),
                    content: "全部通过".into(),
                    tool_note: "1. shell → test result: ok".into(),
                    surface: SurfacePlacement::append(),
                }),
            ),
        ];
        let messages = derive_messages(&events, 0).unwrap();
        assert_eq!(
            messages.len(),
            2,
            "the round and the call are not conversation"
        );
        assert_eq!(messages[1].content, "全部通过");
        assert_eq!(messages[1].tool_note, "1. shell → test result: ok");
    }

    fn cancelled(seq: u64, turn: &str, pristine: bool) -> SessionEvent {
        SessionEvent::new(
            seq,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::TurnCancelled {
                turn_id: turn.into(),
                pristine,
            },
        )
    }

    fn boundary(seq: u64) -> SessionEvent {
        SessionEvent::new(
            seq,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::ConversationBoundary { turn_id: None },
        )
    }

    fn said(seq: u64, turn: &str, text: &str, source: MessageSource) -> SessionEvent {
        SessionEvent::new(
            seq,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: turn.into(),
                content: text.into(),
                source,
                surface: SurfacePlacement::append(),
            }),
        )
    }

    /// The property the checkpoint rests on: folding a prefix, carrying it, and
    /// folding the rest gives the history the cold read gives. Split at *every*
    /// point, including inside a turn and across a compaction, because a cache
    /// that is right only at convenient boundaries is a cache that hands the
    /// model a conversation that never happened.
    #[test]
    fn a_checkpoint_plus_its_tail_is_the_whole_log() {
        let events = vec![
            said(0, "turn-1", "q1", MessageSource::User),
            assistant(1, "a1"),
            said(2, "turn-2", "q2", MessageSource::User),
            said(3, "turn-2", "and also this", MessageSource::Injected),
            assistant(4, "a2"),
            // A compaction shadows the first exchange.
            SessionEvent::new(
                5,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::UserMessage(UserMessageEvent {
                    turn_id: "turn-3".into(),
                    content: "earlier: q1/a1".into(),
                    source: MessageSource::Compaction,
                    surface: SurfacePlacement::replace(0, 1, vec![0, 1]),
                }),
            ),
            // A turn that asked and was stopped before it did anything.
            said(6, "turn-4", "never mind".into(), MessageSource::User),
            cancelled(7, "turn-4", true),
            said(8, "turn-5", "q3", MessageSource::User),
            assistant(9, "a3"),
            // `/new`: nothing leaves the surface, so the transcript is
            // unchanged — but every split has to fold the same boundary.
            boundary(10),
            said(11, "turn-6", "q4", MessageSource::User),
            assistant(12, "a4"),
        ];

        // `Message` is not comparable, so compare what it carries.
        fn shape(messages: &[Message]) -> Vec<(String, String, String)> {
            messages
                .iter()
                .map(|m| {
                    (
                        format!("{:?}", m.role),
                        m.content.clone(),
                        m.tool_note.clone(),
                    )
                })
                .collect()
        }

        let cold = SurfaceProjection::fold(&events, 0).unwrap();
        let expected = shape(&cold.messages().unwrap());
        assert_eq!(
            expected
                .iter()
                .map(|(_, content, _)| content.as_str())
                .collect::<Vec<_>>(),
            vec![
                "earlier: q1/a1",
                "q2\nand also this",
                "a2",
                "q3",
                "a3",
                "q4",
                "a4"
            ],
        );
        let replayed: Vec<String> = cold
            .replay()
            .unwrap()
            .iter()
            .map(|m| m.content.clone())
            .collect();
        assert_eq!(
            replayed,
            vec!["q4", "a4"],
            "the model starts after the line"
        );

        for split in 0..=events.len() {
            let head = SurfaceProjection::fold(&events[..split], 0).unwrap();
            assert!(head.resumable(0));
            let warm = head
                .extend(&events[split..])
                .unwrap_or_else(|e| panic!("split at {split}: {e}"));
            assert_eq!(
                shape(&warm.messages().unwrap()),
                expected,
                "a checkpoint after {split} events must not change the history"
            );
            assert_eq!(
                shape(&warm.replay().unwrap()),
                shape(&cold.replay().unwrap()),
                "nor where the boundary puts the model's replay"
            );
            assert_eq!(warm.surface, cold.surface, "split at {split}");
        }
    }

    /// `/new` draws a line; it does not delete. The transcript keeps every
    /// word — that is what `komo run inspect`, episodic search and a client
    /// hydrating the window read — and only the model's replay moves.
    #[test]
    fn a_boundary_moves_the_replay_and_leaves_the_transcript_whole() {
        let mut events = vec![
            said(0, "turn-1", "q1", MessageSource::User),
            assistant(1, "a1"),
            boundary(2),
            said(3, "turn-2", "q2", MessageSource::User),
            assistant(4, "a2"),
        ];
        let folded = SurfaceProjection::fold(&events, 0).unwrap();
        assert_eq!(folded.surface.nodes(), &[0, 1, 3, 4], "nothing left");
        assert_eq!(folded.messages().unwrap().len(), 4);
        assert_eq!(
            folded
                .replay()
                .unwrap()
                .iter()
                .map(|m| m.content.clone())
                .collect::<Vec<_>>(),
            vec!["q2", "a2"],
        );

        // A second line supersedes the first.
        events.push(boundary(5));
        let folded = SurfaceProjection::fold(&events, 0).unwrap();
        assert!(folded.replay().unwrap().is_empty());
        assert_eq!(folded.messages().unwrap().len(), 4, "still all of it");
    }

    /// The one thing a boundary must not do: strand a turn that was still in
    /// flight when it was drawn. A turn suspended on an approval has a user
    /// message and no answer under it; hiding that would leave its continuation
    /// replying to a conversation it cannot see, and `/new` does not end turns.
    #[test]
    fn a_boundary_does_not_hide_a_turn_that_was_still_open() {
        let events = vec![
            said(0, "turn-1", "q1", MessageSource::User),
            assistant(1, "a1"),
            said(2, "turn-2", "删掉那个目录", MessageSource::User),
            // turn-2 suspends on an approval — no assistant message.
            boundary(3),
        ];
        let folded = SurfaceProjection::fold(&events, 0).unwrap();
        assert_eq!(
            folded
                .replay()
                .unwrap()
                .iter()
                .map(|m| m.content.clone())
                .collect::<Vec<_>>(),
            vec!["删掉那个目录"],
            "the unanswered question stays; the settled exchange before it goes"
        );
    }

    /// A checkpoint written before a retention cut is not resumable: every seq
    /// below the new `truncated_before` means something different afterwards.
    #[test]
    fn a_truncation_retires_the_checkpoint_that_predates_it() {
        let folded = SurfaceProjection::fold(&[said(0, "t", "q", MessageSource::User)], 0).unwrap();
        assert!(folded.resumable(0));
        assert!(!folded.resumable(4), "the log has been cut since");
        let stale = SurfaceProjection {
            v: SURFACE_PROJECTION_VERSION + 1,
            ..folded
        };
        assert!(!stale.resumable(0), "and another shape is another meaning");
    }

    #[test]
    fn a_pristine_cancel_takes_its_own_question_back_off_the_surface() {
        // The log keeps every event — an operator can still see that the user
        // asked and then stopped — but a later turn must not replay a question
        // nobody answered.
        let events = vec![
            said(0, "turn-1", "q1", MessageSource::User),
            assistant(1, "a1"),
            said(2, "turn-2", "oops, never mind", MessageSource::User),
            cancelled(3, "turn-2", true),
            said(4, "turn-3", "the real question", MessageSource::User),
        ];
        assert_eq!(
            derive_messages(&events, 0)
                .unwrap()
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            vec!["q1", "a1", "the real question"]
        );
    }

    #[test]
    fn a_cancel_after_work_keeps_its_question() {
        // Tools ran. Those effects happened, so the turn is part of the
        // conversation whatever the user pressed afterwards.
        let events = vec![
            said(
                0,
                "turn-1",
                "delete the stale migrations",
                MessageSource::User,
            ),
            cancelled(1, "turn-1", false),
        ];
        assert_eq!(derive_messages(&events, 0).unwrap().len(), 1);
    }

    #[test]
    fn an_interjection_joins_the_turn_it_interrupted() {
        // Not a second user message: several providers reject two in a row on
        // replay, and both halves really are one person's input for one turn.
        let events = vec![
            said(0, "turn-1", "看下 A", MessageSource::User),
            said(1, "turn-1", "顺便也看下 B", MessageSource::Injected),
            assistant(2, "两个都看了"),
        ];
        let messages = derive_messages(&events, 0).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content, "看下 A\n顺便也看下 B");
    }

    #[test]
    fn a_seq_gap_refuses_the_log() {
        let events = [user(0, "hi"), assistant(2, "hello")];
        assert_eq!(
            fold_surface(&events, 0),
            Err(FoldError::SeqGap {
                expected: 1,
                found: 2
            })
        );
    }

    #[test]
    fn a_truncated_log_folds_from_its_retention_base() {
        // After a truncate the log legitimately starts above zero; contiguity is
        // checked against the base, not against 0.
        let events = [user(100, "hi"), assistant(101, "hello")];
        let surface = fold_surface(&events, 100).unwrap();
        assert_eq!(surface.nodes(), &[100, 101]);
    }

    #[test]
    fn a_base_is_sparse_on_purpose_but_the_retained_tail_is_not() {
        // A retention base keeps what still matters, so the seqs it did not keep
        // are missing *by decision*. Above the cut the same absence means the
        // log lost something, and a reader that served it would be inventing a
        // conversation. One boundary tells the two apart.
        let base_then_tail = [user(0, "q1"), user(2, "q2"), user(4, "q3")];
        assert_eq!(
            fold_surface(&base_then_tail, 4).unwrap().nodes(),
            &[0, 2, 4]
        );
        // The same events read as a complete log: seq 1 is now a hole.
        assert_eq!(
            fold_surface(&base_then_tail, 0),
            Err(FoldError::SeqGap {
                expected: 1,
                found: 2
            })
        );
        // A hole in the retained tail is refused however the base was cut.
        let torn = [user(0, "q1"), user(4, "q3"), user(6, "q4")];
        assert_eq!(
            fold_surface(&torn, 4),
            Err(FoldError::SeqGap {
                expected: 5,
                found: 6
            })
        );
        // Sparse is not the same as unordered: the base is written in seq order
        // and read back in it, so a reversal is a corrupt file, not a cut.
        let unordered = [user(2, "q2"), user(0, "q1"), user(4, "q3")];
        assert!(fold_surface(&unordered, 4).is_err());
    }

    #[test]
    fn a_replacement_shadows_its_range_and_counts_as_a_rewrite() {
        let mut events = vec![
            user(0, "q1"),
            assistant(1, "a1"),
            user(2, "q2"),
            assistant(3, "a2"),
        ];
        events.push(SessionEvent::new(
            4,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: "turn-2".into(),
                content: "[summary of the first exchange]".into(),
                source: MessageSource::Compaction,
                surface: SurfacePlacement::replace(0, 1, vec![0, 1]),
            }),
        ));
        let surface = fold_surface(&events, 0).unwrap();
        // The summary stands where the two it covers used to; everything after
        // is untouched, and the shadowed events remain in the log.
        assert_eq!(surface.nodes(), &[4, 2, 3]);
        assert_eq!(surface.replace_generation(), 1);
    }

    #[test]
    fn a_second_compaction_can_replace_the_first_summary() {
        // The reason a reader cannot "stop at the first compaction": a later
        // summary may target an earlier one, so the ops have to be folded.
        let mut surface = Surface::default();
        for seq in 0..4 {
            surface.apply(seq, &SurfacePlacement::append()).unwrap();
        }
        surface
            .apply(4, &SurfacePlacement::replace(0, 1, vec![0, 1]))
            .unwrap();
        assert_eq!(surface.nodes(), &[4, 2, 3]);
        surface
            .apply(5, &SurfacePlacement::replace(4, 2, vec![4, 2]))
            .unwrap();
        assert_eq!(surface.nodes(), &[5, 3]);
        assert_eq!(surface.replace_generation(), 2);
    }

    #[test]
    fn an_invalid_replacement_refuses_rather_than_guessing() {
        let mut surface = Surface::default();
        for seq in 0..3 {
            surface.apply(seq, &SurfacePlacement::append()).unwrap();
        }

        // A range the surface does not hold.
        let off_surface = surface
            .clone()
            .apply(9, &SurfacePlacement::replace(7, 8, vec![7, 8]));
        assert!(matches!(
            off_surface,
            Err(FoldError::InvalidReplacement { seq: 9, .. })
        ));

        // Backwards.
        let backwards = surface
            .clone()
            .apply(9, &SurfacePlacement::replace(2, 0, vec![0, 1, 2]));
        assert!(matches!(
            backwards,
            Err(FoldError::InvalidReplacement { seq: 9, .. })
        ));

        // Covers three nodes but cites two: the uncited one would vanish from
        // the human transcript's account of what the summary replaced.
        let undercited = surface
            .clone()
            .apply(9, &SurfacePlacement::replace(0, 2, vec![0, 2]));
        assert!(matches!(
            undercited,
            Err(FoldError::InvalidReplacement { seq: 9, .. })
        ));
    }

    fn header(system: &str, tools: &[&str]) -> RequestHeaderEvent {
        RequestHeaderEvent {
            reason: HeaderReason::Initial,
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            effort: String::new(),
            system: system.into(),
            tools: tools.iter().map(|t| (*t).to_string()).collect(),
            extra: None,
        }
    }

    #[test]
    fn ten_identical_rounds_write_one_header_and_the_change_writes_a_second() {
        let steady = header("You are komo.", &["read", "shell"]);
        let mut log: Vec<SessionEvent> = Vec::new();
        let mut seq = 0u64;

        for _ in 0..10 {
            let latest = fold_request_header(&log, seq);
            if let Some(reason) = header_snapshot_reason(latest, &steady, false) {
                log.push(SessionEvent::new(
                    seq,
                    at("2026-08-31T00:00:00Z"),
                    SessionEventKind::RequestHeader(RequestHeaderEvent {
                        reason,
                        ..steady.clone()
                    }),
                ));
                seq += 1;
            }
            // Each round also logs *something*, so `seq` moves whether or not a
            // snapshot was written.
            log.push(user(seq, "another question"));
            seq += 1;
        }
        let headers: Vec<HeaderReason> = log
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::RequestHeader(h) => Some(h.reason),
                _ => None,
            })
            .collect();
        assert_eq!(
            headers,
            vec![HeaderReason::Initial],
            "ten rounds, one snapshot"
        );

        // A tool mounts: exactly one `change`.
        let widened = header("You are komo.", &["read", "shell", "edit"]);
        let reason = header_snapshot_reason(fold_request_header(&log, seq), &widened, false);
        assert_eq!(reason, Some(HeaderReason::Change));
        log.push(SessionEvent::new(
            seq,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::RequestHeader(RequestHeaderEvent {
                reason: HeaderReason::Change,
                ..widened.clone()
            }),
        ));
        let change_seq = seq;

        // And a continuation always marks itself, identical or not.
        assert_eq!(
            header_snapshot_reason(fold_request_header(&log, change_seq), &widened, true),
            Some(HeaderReason::Resume)
        );
    }

    #[test]
    fn a_header_fold_answers_with_the_envelope_in_force_at_that_seq() {
        let first = header("v1", &["read"]);
        let second = header("v2", &["read", "edit"]);
        let log = vec![
            SessionEvent::new(
                0,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::RequestHeader(first.clone()),
            ),
            user(1, "q"),
            SessionEvent::new(
                2,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::RequestHeader(second.clone()),
            ),
            user(3, "q2"),
        ];
        assert_eq!(fold_request_header(&log, 1).unwrap().system, "v1");
        assert_eq!(fold_request_header(&log, 3).unwrap().system, "v2");
        // Before any snapshot there is no envelope to report — not an empty one.
        assert!(fold_request_header(&log[1..], 1).is_none());
    }

    #[test]
    fn a_route_change_is_not_an_envelope_change() {
        // The whole reason capacity lives outside `RequestHeaderEvent`: a
        // provider advertising a different context window must not force the
        // system prompt and every tool schema to be copied again.
        let steady = header("You are komo.", &["read"]);
        let log = vec![SessionEvent::new(
            0,
            at("2026-08-31T00:00:00Z"),
            SessionEventKind::RequestHeader(steady.clone()),
        )];
        assert_eq!(
            header_snapshot_reason(fold_request_header(&log, 0), &steady, false),
            None
        );

        let routes = vec![
            SessionEvent::new(
                1,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::RequestContext(RequestContextEvent {
                    provider: "anthropic".into(),
                    model: "claude-sonnet-4-6".into(),
                    context_window: Some(200_000),
                }),
            ),
            SessionEvent::new(
                2,
                at("2026-08-31T00:00:00Z"),
                SessionEventKind::RequestContext(RequestContextEvent {
                    provider: "anthropic".into(),
                    model: "claude-sonnet-4-6".into(),
                    context_window: None,
                }),
            ),
        ];
        // A route that advertises nothing clears the older capacity rather than
        // leaving a stale number in force.
        assert_eq!(
            fold_request_context(&routes, 2).unwrap().context_window,
            None
        );
        assert_eq!(
            fold_request_context(&routes, 1).unwrap().context_window,
            Some(200_000)
        );
    }

    #[test]
    fn an_unchanged_request_header_needs_no_new_snapshot() {
        let initial = RequestHeaderEvent {
            reason: HeaderReason::Initial,
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
            effort: String::new(),
            system: "You are komo.".into(),
            tools: vec!["read".into(), "shell".into()],
            extra: None,
        };
        // Same inputs, different reason: still the same request envelope, so no
        // snapshot — this is what keeps a rendered system prompt out of every
        // round, the habit that made the old turn journal dwarf its transcript.
        let same = RequestHeaderEvent {
            reason: HeaderReason::Change,
            ..initial.clone()
        };
        assert!(!same.differs_from(&initial));

        let one_more_tool = RequestHeaderEvent {
            tools: vec!["read".into(), "shell".into(), "edit".into()],
            ..initial.clone()
        };
        assert!(one_more_tool.differs_from(&initial));
    }
}
