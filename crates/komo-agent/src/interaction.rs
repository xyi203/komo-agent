//! Interactive gateway layer: lets a chat-channel turn pause for the user's
//! approval mid-execution, and handles the chat control commands (`/new`,
//! `/approve`, `/deny`, `/sethome`, `/wechat login`, `/pair`).
//!
//! Borrowed from hermes-agent's gateway approval. Hermes runs the agent on a
//! worker thread that blocks on a `threading.Event` keyed by session while the
//! async message loop stays responsive and intercepts `/approve` to signal it.
//! komo's tokio-native equivalent:
//!
//!   - each turn is a **spawned task**, so the channel's receive loop keeps
//!     polling while the turn is in flight (no deadlock);
//!   - when a tool needs approval, [`ChatApprover`] sends the prompt to the
//!     chat and **awaits a `oneshot`** registered in [`ApprovalState`], keyed by
//!     session, with a timeout;
//!   - the loop sees the user's `/approve` / `/deny` reply as an ordinary
//!     inbound message, and [`GatewayDispatcher`] resolves the `oneshot` instead
//!     of starting a new turn.
//!
//! The turn's session context (id + reply sink) reaches the approver through
//! the task-local in `services::tool_execution`.

use komo_services::tool_execution::{
    SessionContext, SessionOrigin, current_session, with_job_grants, with_session,
};
use komo_services::triggers::TriggerMatcher;

use crate::daemon::{EventFanout, RoutineEventSource};
use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::sync::{Notify, watch};
use tracing::{info, warn};

use komo_core::domain::{
    approval::{ApprovalRequest, Approver, DECIDED_BY_HUMAN, Decision, Risk},
    cancel::CancelSignal,
    gateway::{InterjectSource, MessageHandler, ReplySink, WeChatLogin},
    home::HomeRepository,
    inbox::{InboundOrigin, InboxClaim, InboxRepository},
    notify::Notifier,
    pairing::{ApproveOutcome, PairingRepository, PairingStatus},
    policy::{Rule, RuleSpec},
    repository::{SessionEventRepository, SessionRepository},
    run::RunRepository,
    session::{InboundPeer, Session},
    session_event::{
        ApprovalResolvedEvent, MessageSource, SessionEventKind, SurfacePlacement, UserMessageEvent,
        Wakeup, WakeupCause, WakeupFiredEvent,
    },
    todo::SessionTodoRepository,
    trigger::ExternalEvent,
    wakeup::{WAIT_ID_PREFIX, WakeupDispatch, WakeupRegistration, WakeupRepository, is_suspended},
};

// How long an approval may go unanswered is the *wait's* lifetime now
// (`domain::wakeup::default_expiry_secs` — a day), not a timeout this process
// sits through: the turn gives up its slot and the answer may arrive after a
// restart.

/// The user's answer to an approval prompt.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Answer {
    /// Allow this one action.
    Once,
    /// Allow this action and remember its scope key for the rest of the session.
    Session,
    /// Allow, and save a narrow rule so this kind of action stops asking in
    /// future sessions too (`~/.komo/permissions.json`).
    Always,
    /// Refuse. `/deny <理由>` carries the reason through to the model (see
    /// [`Decision`]) so the next round can correct the call rather than repeat it.
    Deny(Option<String>),
}

/// The human-facing description of a pending approval, stored alongside the
/// reply channel so an out-of-band surface — the HTTP
/// `GET /api/interactions/{session}` the GUI polls — can render the prompt
/// without reading the chat reply sink. Chat channels still see the prompt text
/// via the sink; this is the structured mirror.
#[derive(Clone, Debug, serde::Serialize)]
pub struct PendingApproval {
    pub summary: String,
    pub detail: Option<String>,
    /// `"normal"` | `"dangerous"` (a `Risk::Safe` action never prompts).
    pub risk: String,
}

impl PendingApproval {
    fn from_request(request: &ApprovalRequest) -> Self {
        Self {
            summary: request.summary.clone(),
            detail: request.detail.clone(),
            risk: match request.risk {
                Risk::Dangerous => "dangerous",
                _ => "normal",
            }
            .to_string(),
        }
    }
}

/// Per-session cancellation, keyed like the approval state: the api
/// channel registers a signal when it starts an interruptible turn, the
/// `/api/interactions/{session}/cancel` endpoint flips it, and the agent loop
/// (which holds the matching [`CancelSignal`] on its [`SessionContext`]) stops at
/// its next await.
///
/// A `watch` channel rather than a `oneshot`: the signal is cloned into the turn
/// context and may be observed from several await points, and a cancel arriving
/// for a session with no turn in flight is simply a no-op.
///
/// **A session holds every registration, not one.** Only one turn *runs* per
/// session, but a second ingress can be parked in
/// [`claim_session`](GatewayDispatcher::claim_session) waiting for the slot, and
/// that caller has to be stoppable too: with one slot per session it could not
/// register without clobbering the running turn's signal, so Stop reached the
/// turn in flight and the queued one then ran the very work the user had just
/// stopped. `cancel` flips them all — what the user pressed Stop on is *this
/// conversation*, not one of the turns in it.
#[derive(Default)]
pub struct CancelState {
    pending: Mutex<HashMap<String, Vec<(u64, watch::Sender<bool>)>>>,
    next_token: AtomicU64,
}

impl CancelState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a cancellation slot, returning the ticket that owns it. The ticket
    /// retires its own slot when dropped — and only its own, so a queued
    /// caller's registration cannot take the running turn's away.
    ///
    /// Registered *before* the session slot is claimed, not after: the wait for
    /// the slot is unbounded, and a caller that cannot be stopped while waiting
    /// is a caller Stop does not reach.
    pub fn register(self: &Arc<Self>, session: &str) -> CancelTicket {
        let (tx, rx) = watch::channel(false);
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.pending
            .lock()
            .unwrap()
            .entry(session.to_string())
            .or_default()
            .push((token, tx));
        CancelTicket {
            state: self.clone(),
            session: session.to_string(),
            token,
            signal: Arc::new(WatchCancel { rx }),
        }
    }

    /// Request cancellation of everything registered for the session: the turn
    /// in flight and anything queued behind it. `false` when nothing is
    /// registered — no turn, or it already finished.
    pub fn cancel(&self, session: &str) -> bool {
        let pending = self.pending.lock().unwrap();
        let Some(slots) = pending.get(session) else {
            return false;
        };
        // `is_ok()` per slot: a receiver already dropped is a turn already gone,
        // and answering `true` because one of the others took it is right.
        slots
            .iter()
            .fold(false, |any, (_, tx)| tx.send(true).is_ok() || any)
    }

    fn retire(&self, session: &str, token: u64) {
        let mut pending = self.pending.lock().unwrap();
        let Some(slots) = pending.get_mut(session) else {
            return;
        };
        slots.retain(|(held, _)| *held != token);
        if slots.is_empty() {
            pending.remove(session);
        }
    }
}

/// One registration in [`CancelState`], retired when dropped.
///
/// RAII because the two things it must survive are the two ways a turn ends
/// early: an error path that returns before any cleanup line, and a cancel that
/// unwinds out of the middle of the turn.
pub struct CancelTicket {
    state: Arc<CancelState>,
    session: String,
    token: u64,
    signal: Arc<dyn CancelSignal>,
}

impl CancelTicket {
    /// The signal to hang on the turn's [`SessionContext`].
    pub fn signal(&self) -> Arc<dyn CancelSignal> {
        self.signal.clone()
    }

    /// Whether this registration has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.signal.is_cancelled()
    }

    /// Resolves when this registration is cancelled — what a caller waiting for
    /// the session slot races its wait against.
    pub async fn cancelled(&self) {
        self.signal.cancelled().await
    }
}

impl Drop for CancelTicket {
    fn drop(&mut self) {
        self.state.retire(&self.session, self.token);
    }
}

/// [`CancelSignal`] over a `watch` receiver.
struct WatchCancel {
    rx: watch::Receiver<bool>,
}

#[async_trait]
impl CancelSignal for WatchCancel {
    fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            // Sender gone (the turn's slot was dropped) — park forever rather
            // than resolve, since this races real work in a `select!`.
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Shared approval state, keyed by session: the pending prompt's reply channel
/// plus the set of scope keys the user has approved "for this session". Shared
/// between [`ChatApprover`] (registers/awaits) and [`GatewayDispatcher`]
/// (resolves on `/approve`).
pub struct ApprovalState {
    pending: Mutex<HashMap<String, PendingApproval>>,
    approved: Mutex<HashMap<String, HashSet<String>>>,
    /// Per-session serialization gate. A round's tool calls now run
    /// concurrently (`AgentRuntime::run_agent_loop`), so two side-effecting
    /// tools can ask for approval at once; holding this across the
    /// prompt→await→resolve cycle keeps the single `pending` slot from being
    /// raced (a second `register` would otherwise drop the first sender, denying
    /// it). Per session, not global, so a slow approver in one chat never blocks
    /// another chat's prompt.
    gates: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl ApprovalState {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            approved: Mutex::new(HashMap::new()),
            gates: Mutex::new(HashMap::new()),
        }
    }

    /// The approval gate for `session`, created on first use. Held by
    /// [`ChatApprover`] across an interactive prompt so concurrent approvals in
    /// the same session queue instead of racing the `pending` slot.
    fn gate(&self, session: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.gates
            .lock()
            .unwrap()
            .entry(session.to_string())
            .or_default()
            .clone()
    }

    /// Note what `session` is being asked, for the GUI's approval modal.
    ///
    /// A cache of the prompt, not the wait itself: the wait is
    /// `turn/suspended` plus its registration, and this is lost on a restart
    /// while those are not. Replaces any prior prompt for the session — the
    /// newest question is the one on screen.
    fn note_pending(&self, session: &str, info: PendingApproval) {
        self.pending
            .lock()
            .unwrap()
            .insert(session.to_string(), info);
    }

    /// Deliver `decision` to the approver waiting on `session`. Returns whether
    /// one was actually waiting (so the dispatcher can tell the user there was
    /// nothing to approve).
    pub fn resolve(&self, session: &str, decision: Answer) -> bool {
        self.resolve_scoped(session, decision).is_some()
    }

    /// Resolve, and report the scope the answer actually carried.
    ///
    /// A dangerous action is **never** approved beyond the one call it was
    /// asked about, whatever the user typed. "Session" and "always" widen an
    /// approval to *later* calls the user has not seen, and for an irreversible
    /// action the next one is a second deletion, not a repeat of the first. The
    /// narrowing happens here, at the single point every channel's answer flows
    /// through, rather than at each grant-recording site — one of those is easy
    /// to add and forget.
    pub fn resolve_scoped(&self, session: &str, decision: Answer) -> Option<Answer> {
        let info = self.pending.lock().unwrap().remove(session)?;
        Some(match info.risk == "dangerous" {
            true => narrow_unknown(decision),
            false => decision,
        })
    }

    /// The structured description of the approval pending for `session`, if any.
    /// Backs the HTTP `GET /api/interactions/{session}` poll the GUI uses to
    /// render an approval modal (chat channels instead see it via the sink).
    pub fn pending_info(&self, session: &str) -> Option<PendingApproval> {
        self.pending.lock().unwrap().get(session).cloned()
    }

    /// Drop any pending approval for `session` without resolving it (the waiter
    /// reads the dropped sender as a denial).
    fn forget_pending(&self, session: &str) {
        self.pending.lock().unwrap().remove(session);
    }

    fn is_session_approved(&self, session: &str, scope_key: &str) -> bool {
        self.approved
            .lock()
            .unwrap()
            .get(session)
            .is_some_and(|keys| keys.contains(scope_key))
    }

    fn remember(&self, session: &str, scope_key: &str) {
        self.approved
            .lock()
            .unwrap()
            .entry(session.to_string())
            .or_default()
            .insert(scope_key.to_string());
    }

    /// Reclaim the session's transient serialization gate between turns
    /// (recreated on demand by [`gate`](Self::gate)). Called when a turn
    /// finishes so the `gates` map doesn't accumulate one entry per session for
    /// the gateway's lifetime. The `approved` set is deliberately *not* touched
    /// — it is session-scoped and outlives a conversation boundary, because it
    /// is an answer about what komo may do rather than about what was discussed.
    fn release_gate(&self, session: &str) {
        self.gates.lock().unwrap().remove(session);
    }
}

impl Default for ApprovalState {
    fn default() -> Self {
        Self::new()
    }
}

/// Approver for chat channels: routes the approval prompt to the conversation
/// and awaits the user's `/approve` or `/deny` reply.
///
/// Mirrors `CliApprover`'s policy — `Risk::Safe` actions run without prompting;
/// `Normal`/`Dangerous` ask — but over chat instead of a TTY. Without a chat
/// session in context (maintenance sweeps, aux sub-agents) there is no one to
/// ask, so it denies, matching the old `DenyApprover` behavior there.
pub struct ChatApprover {
    state: Arc<ApprovalState>,
}

impl ChatApprover {
    pub fn new(state: Arc<ApprovalState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl Approver for ChatApprover {
    async fn decide(&self, request: &ApprovalRequest) -> Decision {
        self.decide_reported(request).await.0
    }

    /// Whatever this returns, a person in the conversation decided it — a
    /// `/approve`, a `/deny`, or the silence that times out.
    async fn decide_reported(&self, request: &ApprovalRequest) -> (Decision, &'static str) {
        (self.decide_inner(request).await, DECIDED_BY_HUMAN)
    }
}

impl ChatApprover {
    async fn decide_inner(&self, request: &ApprovalRequest) -> Decision {
        if request.risk == Risk::Safe {
            return Decision::Allow;
        }
        let Some(ctx) = current_session() else {
            warn!(summary = %request.summary, "approval auto-denied (no chat session in context)");
            return Decision::deny_because(
                "这一步需要用户批准，但当前上下文没有可以应答的会话（后台任务 / 子代理）",
            );
        };

        // Trusted turn (a `komo chat` routed over the gateway's loopback api
        // channel): the CLI user is the host operator, so run without prompting.
        // The api channel only builds a trusted context for loopback callers.
        if ctx.auto_approve {
            return Decision::Allow;
        }

        // No human to answer (HTTP API, detached REPL context): deny rather than
        // prompt a sink no one reads and wait out the timeout.
        if !ctx.interactive {
            warn!(summary = %request.summary, "approval auto-denied (non-interactive session)");
            return Decision::deny_because(
                "这一步需要用户批准，但当前会话是非交互的，没有人能应答",
            );
        }

        // Already approved this kind of action for the session?
        if let Some(key) = &request.scope_key
            && self.state.is_session_approved(&ctx.session_id, key)
        {
            return Decision::Allow;
        }

        // Serialize concurrent approvals for this session (a round's tools run
        // concurrently now) so they don't race the single `pending` slot. Held
        // until the decision resolves below.
        let gate = self.state.gate(&ctx.session_id);
        let _guard = gate.lock().await;
        // A concurrent approval may have granted this scope "for session" while
        // we waited on the gate — re-check so we don't prompt twice for it.
        if let Some(key) = &request.scope_key
            && self.state.is_session_approved(&ctx.session_id, key)
        {
            return Decision::Allow;
        }

        let channel = ctx.channel_name().to_string();
        if let Err(error) = ctx.sink.send(&prompt(request, &channel)).await {
            warn!(%error, "failed to send approval prompt; denying");
            return Decision::deny();
        }

        // The prompt is out; the answer is not this process's to wait for. The
        // turn stops here and gives up its session slot — `/approve` (or the
        // GUI's modal) writes the answer into the log, and the turn is
        // continued then, in this process or the next one.
        //
        // The pending info still goes into memory: it is what the GUI's
        // approval modal polls. It is a *cache* — a restart loses it, exactly
        // as it lost the whole approval before — while the answer itself is
        // durable.
        self.state
            .note_pending(&ctx.session_id, PendingApproval::from_request(request));
        Decision::Suspend
    }
}

fn prompt(request: &ApprovalRequest, channel: &str) -> String {
    let mut s = match request.risk {
        Risk::Dangerous => format!("🛑 需要审批（危险操作）：{}", request.summary),
        _ => format!("⚠️ 需要审批：{}", request.summary),
    };
    if let Some(detail) = &request.detail {
        s.push_str(&format!("\n（{detail}）"));
    }
    s.push_str(
        "\n回复 /approve 批准本次 · /approve session 批准本会话内同类操作 · \
         /deny 拒绝（可写理由：/deny 用 trash 代替 rm）",
    );
    // `always` is offered only when there is something to remember, and the rule
    // text is spelled out — the operator has to see how wide the grant is before
    // granting it. A dangerous action never offers it: the policy engine refuses
    // to read a saved grant for one, so the option would be a lie.
    if request.risk == Risk::Normal
        && let Some(rule) = request
            .action
            .as_ref()
            .and_then(|a| komo_core::domain::policy::Rule::narrowest_for(a, channel))
    {
        s.push_str(&format!(
            "\n· /approve always 以后都允许，将保存规则：{}",
            rule.describe()
        ));
    }
    s
}

/// A control command parsed from an inbound message, or plain text for the agent.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    /// Start a fresh session (clear context + approval state).
    New,
    /// Resolve a pending approval. The id names a wait in **another** session
    /// (a routine's, prompted into the home chat); `None` means this chat's own.
    Approve(Answer, Option<String>),
    /// Refuse a pending approval, optionally with a reason relayed to the agent,
    /// and optionally naming a wait in another session.
    Deny(Option<String>, Option<String>),
    /// Decline the question a suspended turn is waiting on: an answer that
    /// says "I am not going to say", so the turn continues on its own
    /// assumptions instead of standing for a week.
    Skip,
    /// Make this chat the home channel for proactive output.
    SetHome,
    /// Provision the WeChat channel by QR (delivered to this chat).
    WechatLogin,
    /// Approve/list/revoke pairings from chat — the gateway holds the db lock,
    /// so the `komo pair` CLI can't open it while the gateway runs.
    Pair(PairAction),
    /// Ordinary message — run a turn.
    Plain(String),
}

/// The sub-action of a `/pair` chat command.
#[derive(Debug, PartialEq, Eq)]
pub enum PairAction {
    List,
    Approve(String),
    Revoke(String),
    /// Unrecognized `/pair …` — reply with usage.
    Usage,
}

/// Classify an inbound message. No-arg commands match case-insensitively on the
/// whole (trimmed) message. `/pair …` takes an argument, so it is parsed from
/// the original text (the code/id keep their case); anything else is plain text.
pub fn classify(text: &str) -> Command {
    let trimmed = text.trim();
    let lower = trimmed.to_lowercase();

    if lower == "/pair" || lower.starts_with("/pair ") {
        // Split off the verb + argument from the *original* text so a revoke id
        // (`{platform}:{sender_id}`) and a code keep their case.
        let mut parts = trimmed.split_whitespace();
        let _ = parts.next(); // "/pair"
        let verb = parts.next().map(|v| v.to_lowercase());
        let arg = parts.next().map(|s| s.to_string());
        return Command::Pair(match (verb.as_deref(), arg) {
            (Some("list"), _) | (None, _) => PairAction::List,
            (Some("approve"), Some(code)) => PairAction::Approve(code),
            (Some("revoke"), Some(id)) => PairAction::Revoke(id),
            _ => PairAction::Usage,
        });
    }

    // `/deny <理由>` takes free text, so it is split off the *original* message
    // (the reason keeps its case) before the exact-match table below.
    let (verb, rest) = match trimmed.split_once(char::is_whitespace) {
        Some((verb, rest)) => (verb, rest.trim()),
        None => (trimmed, ""),
    };
    if matches!(verb.to_lowercase().as_str(), "/deny" | "/no" | "/n") {
        let (id, reason) = split_wait_id(rest);
        return Command::Deny((!reason.is_empty()).then(|| reason.to_string()), id);
    }
    if matches!(
        verb.to_lowercase().as_str(),
        "/approve" | "/yes" | "/y" | "/ok"
    ) {
        let (id, scope) = split_wait_id(rest);
        // Only a *recognised* argument makes this a command: "/approve the
        // budget" is somebody talking, not approving, and reading it as an
        // approval would grant something nobody was asked about.
        let answer = match scope.to_lowercase().as_str() {
            "" => Some(Answer::Once),
            "session" | "all" => Some(Answer::Session),
            "always" => Some(Answer::Always),
            _ => None,
        };
        if let Some(answer) = answer {
            return Command::Approve(answer, id);
        }
        if id.is_some() {
            // An id with an unrecognised scope word is still an approval —
            // nobody types a wait id by accident.
            return Command::Approve(Answer::Once, id);
        }
    }

    match lower.as_str() {
        "/new" | "/clear" | "/reset" => Command::New,
        "/skip" => Command::Skip,
        "/sethome" | "/home" => Command::SetHome,
        "/wechat" | "/wechat login" | "/weixin" => Command::WechatLogin,
        _ => Command::Plain(text.to_string()),
    }
}

/// What answering an approval reached.
#[derive(Debug, PartialEq, Eq)]
enum Answered {
    /// Nothing durable was waiting.
    Nothing,
    /// A wait in this chat's own session.
    Here,
    /// A wait belonging to another session — a routine's, answered from the
    /// home chat.
    Elsewhere(String),
}

fn answered_elsewhere(answered: &Answered) -> &'static str {
    match answered {
        Answered::Elsewhere(_) => "✅ 已答复，那个任务正在继续。",
        _ => "✅ 已答复，这一轮正在继续。",
    }
}

/// Narrow an answer given without knowing what was asked.
///
/// A restart loses the in-memory prompt, and with it the action's risk. "For
/// this session" and "always" widen an approval to *later* calls nobody has
/// seen; granting that on an action we can no longer read the risk of is the
/// one direction that cannot be taken back, so an answer with no context is
/// worth exactly the call it was given for.
fn narrow_unknown(answer: Answer) -> Answer {
    match answer {
        Answer::Session | Answer::Always => Answer::Once,
        other => other,
    }
}

fn now_secs() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

/// Split a leading wait id off an approval command's arguments.
///
/// An id is recognised by the prefix every registration carries, so
/// `/deny 太危险了` stays a reason and `/deny wk-0199 太危险了` names a wait in
/// another session and *then* gives a reason. Nothing else disambiguates them:
/// a reason is free text, and a chat cannot ask a follow-up question.
fn split_wait_id(rest: &str) -> (Option<String>, &str) {
    match rest.split_once(char::is_whitespace) {
        Some((first, tail)) if first.starts_with(WAIT_ID_PREFIX) => {
            (Some(first.to_string()), tail.trim())
        }
        None if rest.starts_with(WAIT_ID_PREFIX) => (Some(rest.to_string()), ""),
        _ => (None, rest),
    }
}

/// Front door for inbound chat messages: classifies control commands and,
/// for plain text, runs the agent turn off the channel's receive loop so the
/// loop can still deliver an `/approve` reply while the turn is suspended.
///
/// Channels build a [`ReplySink`] for the conversation and call
/// [`GatewayDispatcher::handle`]; the dispatcher owns replying (including the
/// turn's eventual answer), so channels no longer send agent replies directly.
/// What the dispatcher needs to bring a suspended turn back: the ledger row to
/// continue, the log to record why, and the waits to retire.
///
/// Held together because continuing a turn without any of the three is not a
/// partial feature: a continuation with no `wakeup/fired` leaves the log unable
/// to say what ended the wait, and one that does not retire the turn's other
/// waits gets woken again by them.
#[derive(Clone)]
pub struct WaitParts {
    pub runs: Arc<dyn RunRepository>,
    pub events: Arc<dyn SessionEventRepository>,
    pub wakeups: Arc<dyn WakeupRepository>,
}

pub struct GatewayDispatcher {
    handler: Arc<dyn MessageHandler>,
    /// The runtimes a woken turn may come back on, by what drives its session.
    /// [`handler`](Self::handler) is the conversation's and is not in here; a
    /// missing entry falls back to it. See [`handler_for`](Self::handler_for).
    by_origin: HashMap<SessionOrigin, Arc<dyn MessageHandler>>,
    /// Where an unattended turn's second question goes. `None` = nowhere, which
    /// is every fixture and the local surfaces.
    notifier: Option<Arc<dyn Notifier>>,
    /// `None` = this dispatcher cannot continue suspended turns (the test
    /// fixtures, which have no store behind them).
    waits: Option<WaitParts>,
    approvals: Arc<ApprovalState>,
    sessions: Arc<dyn SessionRepository>,
    home: Arc<dyn HomeRepository>,
    todos: Arc<dyn SessionTodoRepository>,
    /// Set when the WeChat channel is enabled — drives `/wechat login`.
    wechat_login: Option<Arc<dyn WeChatLogin>>,
    /// Backs the `/pair` chat commands (same store the `komo pair` CLI uses).
    pairings: Arc<dyn PairingRepository>,
    /// Durable dedupe for redelivered platform messages (`domain/inbox.rs`).
    inbox: Arc<dyn InboxRepository>,
    /// Standing wakes an inbound message can fire — a kanban Task waiting on
    /// the person who just wrote (docs/bot-runtime.md §3.7). `None` = nothing
    /// listens, which is what the test fixtures and the local TUI have.
    triggers: Option<Arc<TriggerMatcher>>,
    /// Where an ingress hands an event that is **not** a chat message — a
    /// webhook body, a group reaction, a changed file (docs/bot-runtime.md
    /// §5.12–5.14). Held here because the dispatcher is the one thing every
    /// channel is given, and those three arrive on channels that were built
    /// before the routine machinery existed.
    ///
    /// Attached after construction, like `TriggerMatcher`'s own dispatch and
    /// for the same reason: a routine's turn is woken through this dispatcher,
    /// so the source cannot exist before it. Absent ⇒ no routine ingress.
    routines: RwLock<Option<Arc<RoutineEventSource>>>,
    /// Per-session turn state. A session key is present iff a turn is in flight;
    /// its queue holds up to [`QUEUE_CAP`] messages that arrived mid-turn, drained
    /// FIFO as each turn finishes (so a quick follow-up is answered, not dropped).
    inflight: Mutex<HashMap<String, VecDeque<QueuedMessage>>>,
    /// Raised whenever a session leaves [`inflight`](Self::inflight), so a
    /// caller parked in [`claim_session`](Self::claim_session) can look again.
    /// One `Notify` for every session rather than one each: a release is rare,
    /// waiters are few, and a spurious wake costs one re-check of a `HashMap`.
    idle: Notify,
}

/// Waking a suspended turn: the scheduler's side of `turn/suspended`
/// (docs/bot-runtime.md §4.1).
///
/// A thin adapter, because the continuation itself belongs to whoever owns
/// turns: `/approve` has to do exactly the same thing without going through a
/// sweep, and two copies of "bring a turn back" would be two chances to forget
/// the `wakeup/fired`.
pub struct TurnWaker {
    dispatcher: Arc<GatewayDispatcher>,
}

impl TurnWaker {
    pub fn new(dispatcher: Arc<GatewayDispatcher>) -> Self {
        Self { dispatcher }
    }
}

#[async_trait]
impl WakeupDispatch for TurnWaker {
    async fn fire(
        &self,
        registration: &WakeupRegistration,
        cause: WakeupCause,
        payload: &str,
    ) -> anyhow::Result<()> {
        // No sink: a sweep tick and a settling background task have nobody
        // standing at a channel waiting for the answer — it lands in the
        // transcript, which is where the next reader looks.
        self.dispatcher
            .continue_turn_with(registration, cause, payload, None)
            .await
    }
}

/// The two kinds of wait a person can answer, told apart so a surface never
/// resolves the other one: `/approve` is not an answer to a question, and a
/// question's answer is not an approval.
fn is_approval(wakeup: &Wakeup) -> bool {
    matches!(wakeup, Wakeup::Approval { .. })
}

/// Whether this wait is a question to the user — the one a plain message, the
/// GUI's inline reply or `/skip` may answer. Public because the TUI's local
/// mode drives its own turns and has to find the same wait.
pub fn is_user_reply(wakeup: &Wakeup) -> bool {
    matches!(wakeup, Wakeup::UserReply)
}

/// Write down that a wait ended, and retire what was watching it.
///
/// Three records, in this order, because each makes the next honest:
///
/// 1. A wait that **ran out** is written as such (`approval/expired`) before
///    the turn sees it — the gate then reads it as a refusal instead of asking
///    the same question again and parking the turn forever.
/// 2. `wakeup/fired`, durably, carrying whatever the wake brought: the log has
///    to answer "what ended the wait, and what was it handed" long after.
/// 3. Every other wait that turn was holding is retired — an approval answered
///    by a person must not be woken a second time by the timer watching it.
///
/// Continuing the turn is deliberately **not** here: the gateway claims a
/// session slot and spawns, the TUI drives the turn itself. What must not fork
/// is this — two writers of `wakeup/fired` would be two chances to forget it.
pub async fn record_wake(
    waits: &WaitParts,
    registration: &WakeupRegistration,
    turn_id: &str,
    cause: WakeupCause,
    payload: &str,
) -> anyhow::Result<()> {
    let session_id = registration.session_id.clone();
    let mut closing = Vec::new();
    if cause == WakeupCause::Expired
        && let Wakeup::Approval { call_id } = &registration.wakeup
    {
        closing.push(SessionEventKind::ApprovalExpired {
            turn_id: turn_id.to_string(),
            call_id: call_id.clone(),
            call_index: requested_call_index(waits, &session_id, turn_id, call_id).await,
        });
    }
    closing.push(SessionEventKind::WakeupFired(WakeupFiredEvent {
        turn_id: turn_id.to_string(),
        wakeup_id: registration.id.clone(),
        cause,
        payload: payload.to_string(),
    }));
    waits.events.append(&session_id, closing).await?;
    waits.events.durable_flush(&session_id).await?;

    if let Err(error) = waits.wakeups.take_for_turn(&session_id, turn_id).await {
        warn!(%error, turn = %turn_id, "failed to retire a woken turn's other waits");
    }
    Ok(())
}

/// The `call_index` the gate recorded when it asked. Read back rather than
/// assumed: an expiry has to name the same call the request did, and only the
/// request knows where in its round it sat.
async fn requested_call_index(
    waits: &WaitParts,
    session_id: &str,
    turn_id: &str,
    call_id: &str,
) -> u32 {
    let Ok(events) = waits.events.events(session_id).await else {
        return 0;
    };
    events
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            SessionEventKind::ApprovalRequested(requested)
                if requested.turn_id == turn_id && requested.call_id == call_id =>
            {
                Some(requested.call_index)
            }
            _ => None,
        })
        .unwrap_or(0)
}

/// How many mid-turn messages a session may queue before further ones are
/// rejected. Small on purpose: it absorbs a rapid follow-up without letting a
/// spamming sender build an unbounded backlog.
const QUEUE_CAP: usize = 2;

/// A message that arrived while its session's turn was in flight, held for
/// dispatch when the turn finishes.
struct QueuedMessage {
    input: String,
    sink: Arc<dyn ReplySink>,
}

impl GatewayDispatcher {
    pub fn new(
        handler: Arc<dyn MessageHandler>,
        approvals: Arc<ApprovalState>,
        sessions: Arc<dyn SessionRepository>,
        home: Arc<dyn HomeRepository>,
        todos: Arc<dyn SessionTodoRepository>,
        wechat_login: Option<Arc<dyn WeChatLogin>>,
        pairings: Arc<dyn PairingRepository>,
        inbox: Arc<dyn InboxRepository>,
    ) -> Self {
        Self {
            handler,
            by_origin: HashMap::new(),
            notifier: None,
            approvals,
            sessions,
            home,
            todos,
            wechat_login,
            pairings,
            inbox,
            waits: None,
            triggers: None,
            routines: RwLock::new(None),
            inflight: Mutex::new(HashMap::new()),
            idle: Notify::new(),
        }
    }

    /// Declare the runtime turns on `origin`'s sessions run on.
    ///
    /// A builder rather than a constructor argument because the fixtures have
    /// exactly one runtime and the gateway has three, and a signature that made
    /// every caller name all of them would be nine `None`s in the tests.
    pub fn with_runtime(mut self, origin: SessionOrigin, handler: Arc<dyn MessageHandler>) -> Self {
        self.by_origin.insert(origin, handler);
        self
    }

    /// Where to tell the operator that an unattended turn stopped again.
    pub fn with_notifier(mut self, notifier: Arc<dyn Notifier>) -> Self {
        self.notifier = Some(notifier);
        self
    }

    /// Which runtime brings a turn on this session back — the one it ran on,
    /// not whichever is nearest.
    ///
    /// What a turn was is what it comes back as. A routine's continuation on
    /// the conversation's runtime gets a wider tool set, the user's memory
    /// library injected, and an approver that answers on behalf of a human who
    /// is not there — so a second ungranted action comes back *refused* instead
    /// of stopping to ask, which is the opposite of what an unattended turn is
    /// supposed to do (docs/bot-runtime.md §4.2).
    ///
    /// `None` = do not continue at all. That is [`SessionOrigin::Delegate`]: a
    /// delegation is the parent turn's own work done on a scratch session, and
    /// the `delegate` call that would have read its answer is long gone — so a
    /// continuation would spend a model round producing a reply with no reader.
    fn handler_for(&self, origin: SessionOrigin) -> Option<Arc<dyn MessageHandler>> {
        match origin {
            SessionOrigin::Delegate => None,
            SessionOrigin::User => Some(self.handler.clone()),
            SessionOrigin::Cron | SessionOrigin::Briefing => match self.by_origin.get(&origin) {
                Some(handler) => Some(handler.clone()),
                None => {
                    warn!(
                        origin = origin.as_str(),
                        "no runtime registered for this origin; using the conversation's"
                    );
                    Some(self.handler.clone())
                }
            },
        }
    }

    /// Number of sessions with a turn currently in flight. The gateway's
    /// bounded shutdown drain polls this so active turns get a chance to finish
    /// (and persist their reply + run) before teardown, leaving fewer runs
    /// marked `interrupted`.
    pub fn inflight_count(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }

    /// Claim a session's turn slot for a caller that drives the turn itself,
    /// waiting while another turn holds it.
    ///
    /// A chat channel never needs this: [`spawn_turn`](Self::spawn_turn) leaves
    /// its message in the queue and returns, and the answer finds its way back
    /// through a [`ReplySink`] that addresses the whole conversation. An HTTP
    /// turn can do neither — the caller is holding the connection and is owed
    /// *its own* reply — so it waits for the slot instead of queueing behind it.
    ///
    /// Both routes go through the one `inflight` map, which is what makes "one
    /// turn per session" true *across* ingresses rather than within each. Two
    /// turns on one session is not merely untidy: the second assembles its
    /// history before the first has written a word of its answer, so it starts
    /// over from the original question and re-runs every tool the first one is
    /// still paying for.
    ///
    /// The wait is deliberately unbounded. A turn can legitimately run for
    /// minutes, and refusing the message at some deadline would throw away what
    /// the user typed — strictly worse than making them wait for an answer that
    /// will have the previous turn's conclusions already in it.
    pub async fn claim_session(self: &Arc<Self>, session: &str) -> SessionClaim {
        loop {
            // Enlisted *before* the map is read: `notified()` does not register
            // a waiter until it is first polled, so a release landing between
            // the read and the await would be a wake nobody is listening for.
            let mut idle = std::pin::pin!(self.idle.notified());
            idle.as_mut().enable();
            {
                let mut inflight = self.inflight.lock().unwrap();
                if !inflight.contains_key(session) {
                    inflight.insert(session.to_string(), VecDeque::new());
                    return SessionClaim {
                        guard: TurnGuard {
                            dispatcher: self.clone(),
                            session: session.to_string(),
                            armed: true,
                        },
                    };
                }
            }
            idle.await;
        }
    }

    /// Give the dispatcher what it needs to continue suspended turns.
    ///
    /// Separate from `new` because it is the one capability a dispatcher can
    /// coherently lack: the test fixtures have no store behind them, and a
    /// dispatcher without it simply reports nothing to answer.
    pub fn with_waits(mut self, waits: WaitParts) -> Self {
        self.waits = Some(waits);
        self
    }

    /// Let inbound messages fire the standing wakes they match.
    pub fn with_triggers(mut self, triggers: Arc<TriggerMatcher>) -> Self {
        self.triggers = Some(triggers);
        self
    }

    /// Let ingresses hand over events that are not chat messages. Called once,
    /// during gateway wiring, after the waker the source fires through exists.
    pub fn attach_routines(&self, routines: Arc<RoutineEventSource>) {
        *self.routines.write().unwrap() = Some(routines);
    }

    /// One external event, from whichever ingress saw it: the routines it
    /// starts and the standing waits it ends (docs/bot-runtime.md §4.4).
    ///
    /// Every ingress goes through here rather than holding the source itself,
    /// because the dispatcher is what a `Channel` is handed — and because a
    /// gateway wired without routines must answer "nothing happened" rather
    /// than force each ingress to know whether the machinery exists.
    pub async fn on_external_event(&self, event: &ExternalEvent) -> EventFanout {
        let routines = self.routines.read().unwrap().clone();
        match routines {
            Some(routines) => routines.on_event(event).await,
            None => EventFanout::default(),
        }
    }

    /// Whether turning a chat reaction into an event is worth an ingress's
    /// trouble — see [`RoutineEventSource::wants_feishu_reactions`].
    pub async fn wants_feishu_reactions(&self) -> bool {
        let routines = self.routines.read().unwrap().clone();
        match routines {
            Some(routines) => routines.wants_feishu_reactions().await,
            None => false,
        }
    }

    /// Bring a suspended turn back.
    ///
    /// Four things, in this order, because each one is what makes the next
    /// honest:
    ///
    /// 1. A wait that **ran out** is written down as such (`approval/expired`)
    ///    before the turn sees it. The gate reads it as a refusal, so the turn
    ///    comes back and is told nobody answered — rather than asking again and
    ///    parking itself forever.
    /// 2. `wakeup/fired`, durably: the log has to be able to answer "what ended
    ///    the wait" long after the fact.
    /// 3. Every other wait that turn was holding is retired — an approval must
    ///    not be woken a second time by the timer that was watching it.
    /// 4. The continuation takes the session's turn slot like any other turn,
    ///    and runs spawned: the caller (a sweep tick, a `/approve` reply) must
    ///    not be held for however long the turn takes.
    pub async fn continue_turn(
        self: &Arc<Self>,
        registration: &WakeupRegistration,
        cause: WakeupCause,
    ) -> anyhow::Result<()> {
        self.continue_turn_with(registration, cause, "", None).await
    }

    /// The same, carrying what the wake brought and where to answer.
    ///
    /// `payload` is what the thing that happened produced — the user's answer
    /// to an `ask_user` question, a webhook's body. It rides on `wakeup/fired`,
    /// which is where the call that stopped reads it when it is re-dispatched:
    /// one record, so what woke the turn and what it was handed cannot
    /// disagree.
    ///
    /// `sink` is where the continuation's reply goes, and it is **the surface
    /// that answered, not the one that asked**. A turn suspended in the TUI and
    /// released by `/approve` from Telegram answers into Telegram — the
    /// operator is standing there, and a reply that only lands in a transcript
    /// nobody is looking at reads as silence. It lands in the log either way,
    /// which is what the TUI shows when it is opened again. `None` for a wake
    /// nobody is waiting on the other end of — a sweep firing a timer, a
    /// settling background task, the GUI's modal (which polls the transcript).
    pub async fn continue_turn_with(
        self: &Arc<Self>,
        registration: &WakeupRegistration,
        cause: WakeupCause,
        payload: &str,
        sink: Option<Arc<dyn ReplySink>>,
    ) -> anyhow::Result<()> {
        let Some(waits) = self.waits.clone() else {
            anyhow::bail!("this dispatcher has no store to continue a turn from");
        };
        let Some(turn_id) = registration.turn_id.clone() else {
            // A wake with nothing to continue *starts* something instead: a
            // background task that settled after its turn ended, a trigger.
            // What it brought is the whole message — a wake with no turn and
            // nothing to say would open a turn about nothing.
            return self
                .start_turn_with(&registration.session_id, payload, sink)
                .await;
        };
        let Some(run) = waits.runs.get(&turn_id).await? else {
            anyhow::bail!("no run `{turn_id}` to continue");
        };
        let session_id = registration.session_id.clone();
        // What the turn was is what it comes back as. A routine that stopped to
        // ask is still a routine: it comes back on the routine runtime, the
        // permission engine reads `origin` to know nobody is watching, and the
        // job's grants are what let it act at all — all three would be gone if
        // the continuation ran as a plain detached turn on the conversation's
        // runtime, which is a *widening* (`default_normal` would apply) as well
        // as a routine that comes back unable to do the work it was granted.
        let origin = self.session_origin(&session_id).await;
        let Some(handler) = self.handler_for(origin) else {
            warn!(
                turn = %turn_id,
                origin = origin.as_str(),
                "nothing continues a turn on this kind of session; dropping the wake"
            );
            return Ok(());
        };
        record_wake(&waits, registration, &turn_id, cause, payload).await?;

        let grants: Vec<Rule> = registration
            .grants
            .iter()
            .filter_map(RuleSpec::to_rule)
            .collect();

        let dispatcher = self.clone();
        tokio::spawn(async move {
            let claim = dispatcher.claim_session(&session_id).await;
            let ctx = SessionContext::detached(&session_id).with_origin(origin);
            let outcome =
                with_job_grants(grants, with_session(ctx, handler.resume_interrupted(&run))).await;
            claim.release();
            match outcome {
                Ok(Some(reply)) => {
                    if let Some(sink) = &sink
                        && let Err(error) = sink.send(&reply).await
                    {
                        warn!(%error, turn = %run.id, "failed to deliver a woken turn's reply");
                    }
                    info!(turn = %run.id, cause = cause.as_str(), "continued a woken turn")
                }
                // The continuation declined — the transcript already ends in a
                // reply, or the log has nothing for the turn. Not an error, and
                // not something to retry: the wait is gone and the turn is
                // whatever the log says it is.
                Ok(None) => warn!(turn = %run.id, "a woken turn was not continuable"),
                // It stopped again rather than failing: the routine met a second
                // action nobody has granted. Its new wait needs an operator, and
                // there is no sweep behind this turn to find one.
                Err(error) if is_suspended(&error) => {
                    dispatcher.announce_new_wait(origin, &session_id).await
                }
                Err(error) => warn!(%error, turn = %run.id, "a woken turn failed"),
            }
        });
        Ok(())
    }

    /// Open a turn on `session_id` with what a wake brought.
    ///
    /// The other half of `continue_turn_with`: a registration with no turn has
    /// nothing to pick up, so what it carries becomes the turn's user message —
    /// "the task you started has finished, here is what it produced". A wake
    /// carrying nothing opens nothing; there would be no message to run.
    ///
    /// No `wakeup/fired` is written, and deliberately: that event is the causal
    /// link between a suspension and *its* continuation, and it names the turn
    /// it brought back. Nothing was suspended here. What caused this turn is
    /// already a fact in the same log — the `task/settled` immediately before
    /// it — and the turn's own user message says what it was told.
    async fn start_turn_with(
        self: &Arc<Self>,
        session_id: &str,
        payload: &str,
        sink: Option<Arc<dyn ReplySink>>,
    ) -> anyhow::Result<()> {
        if payload.trim().is_empty() {
            warn!(session = %session_id, "a wake with no turn and nothing to say opens nothing");
            return Ok(());
        }
        let origin = self.session_origin(session_id).await;
        let Some(handler) = self.handler_for(origin) else {
            warn!(
                session = %session_id,
                origin = origin.as_str(),
                "nothing opens a turn on this kind of session; dropping the wake"
            );
            return Ok(());
        };
        let dispatcher = self.clone();
        let session = session_id.to_string();
        let input = payload.to_string();
        tokio::spawn(async move {
            // Takes the session's slot like any other turn: a task settling
            // while the conversation is mid-turn queues behind it rather than
            // running beside it.
            let claim = dispatcher.claim_session(&session).await;
            let ctx = SessionContext::detached(&session).with_origin(origin);
            let outcome = with_session(ctx, handler.handle(&session, input)).await;
            claim.release();
            match outcome {
                Ok(reply) => {
                    // Same rule as a continuation's: deliver where the wake was
                    // answered from when someone is there, and otherwise let the
                    // transcript be the record. A settling background task has
                    // nobody there, so this is usually `None`.
                    if let Some(sink) = &sink
                        && let Err(error) = sink.send(&reply).await
                    {
                        warn!(%error, session = %session, "failed to deliver a woken turn's reply");
                    }
                    info!(session = %session, "opened a turn with what a wake carried")
                }
                Err(error) if is_suspended(&error) => {
                    dispatcher.announce_new_wait(origin, &session).await
                }
                Err(error) => warn!(%error, session = %session, "a woken turn failed"),
            }
        });
        Ok(())
    }

    /// Tell the operator about the wait a *woken* unattended turn left behind.
    ///
    /// The sweep that starts a routine says what its turn stopped for; a turn
    /// that stops a second time has no sweep behind it, so a wait whose id
    /// nobody was given is a routine parked until it expires. Read back out of
    /// the two records the suspension left — the log says what it wants, the
    /// registration says how to name it — because the id does not exist until
    /// after the approver has already answered.
    ///
    /// Only `/approve <id>` is offered: `session` / `always` widen a grant, and
    /// an unattended turn's actions are approved one at a time or not at all.
    async fn announce_new_wait(&self, origin: SessionOrigin, session_id: &str) {
        // An attended turn's prompt went to the conversation as it was asked.
        if !origin.is_unattended() {
            return;
        }
        let (Some(notifier), Some(waits)) = (self.notifier.as_ref(), self.waits.as_ref()) else {
            warn!(session = %session_id, "a woken turn stopped again and nobody can be told");
            return;
        };
        let summary = match waits.events.events(session_id).await {
            Ok(events) => events.iter().rev().find_map(|event| match &event.kind {
                SessionEventKind::TurnSuspended(suspended) => Some(suspended.summary.clone()),
                _ => None,
            }),
            Err(error) => {
                warn!(%error, session = %session_id, "could not read what a woken turn stopped for");
                None
            }
        };
        let id = match waits.wakeups.list().await {
            Ok(registrations) => registrations
                .into_iter()
                .find(|r| r.session_id == session_id && is_approval(&r.wakeup))
                .map(|r| r.id),
            Err(error) => {
                warn!(%error, session = %session_id, "could not read a woken turn's new wait");
                None
            }
        };
        let (Some(summary), Some(id)) = (summary, id) else {
            warn!(
                session = %session_id,
                "a woken turn stopped again with no wait to answer; \
                 the next gateway start re-registers it"
            );
            return;
        };
        let body = format!(
            "⚠️ 继续执行后又需要审批：{summary}\n回复 /approve {id} 批准本次 · /deny {id} 拒绝"
        );
        if let Err(error) = notifier.notify("Komo routine 等待批准", &body).await {
            warn!(%error, session = %session_id, "failed to deliver a woken turn's new prompt");
        }
    }

    /// What is driving the session — the record is the authority, since the
    /// context the turn originally ran under died with the process that held
    /// it. A session we cannot read is treated as an ordinary conversation:
    /// that is the *narrower* answer, since an unattended turn's grants only
    /// apply where the engine sees `origin` say so.
    async fn session_origin(&self, session_id: &str) -> SessionOrigin {
        match self.sessions.find_windowed(session_id, 1).await {
            Ok(Some(session)) => session.origin,
            Ok(None) => SessionOrigin::default(),
            Err(error) => {
                warn!(%error, session = session_id, "could not read what drives this session");
                SessionOrigin::default()
            }
        }
    }

    /// `/new`: draw a context boundary in this conversation's log.
    async fn mark_boundary(&self, session_id: &str) -> anyhow::Result<()> {
        let Some(waits) = self.waits.as_ref() else {
            anyhow::bail!("this dispatcher has no log to write a boundary into");
        };
        komo_services::conversation::mark_boundary(
            waits.events.as_ref(),
            self.todos.as_ref(),
            session_id,
        )
        .await
    }

    /// A plain message while this session has an approval parked on it.
    ///
    /// Answers whether it was taken as an answer. The message is appended to
    /// the **suspended turn** as an interjection — the surface fold merges it
    /// into that turn's user message, so the transcript still alternates and
    /// the continuation replays it in place — and the approval is refused,
    /// citing what was said.
    ///
    /// Not "deny and start a new turn": the model would then answer the new
    /// message without knowing the action it asked about was dropped, and the
    /// user would see two turns for one exchange.
    async fn moved_on(
        self: &Arc<Self>,
        session_id: &str,
        input: &str,
        sink: Arc<dyn ReplySink>,
    ) -> bool {
        let Some(waits) = self.waits.clone() else {
            return false;
        };
        let Some(registration) = self
            .pending_wait(&waits, session_id, None, is_approval)
            .await
        else {
            return false;
        };
        let Some(turn_id) = registration.turn_id.clone() else {
            return false;
        };

        if let Err(error) = waits
            .events
            .append(
                session_id,
                vec![SessionEventKind::UserMessage(UserMessageEvent {
                    turn_id: turn_id.clone(),
                    content: input.to_string(),
                    source: MessageSource::Injected,
                    surface: SurfacePlacement::append(),
                })],
            )
            .await
        {
            warn!(%error, "failed to record what the user said instead; leaving the wait alone");
            return false;
        }

        let refusal = Answer::Deny(Some(format!(
            "the user said this instead of answering: {input}"
        )));
        self.approvals.resolve(session_id, refusal.clone());
        matches!(
            self.answer_suspended(session_id, None, &refusal, Some(sink))
                .await,
            Answered::Here | Answered::Elsewhere(_)
        )
    }

    /// Answer whatever `session` (or the wait named by `id`) is waiting on.
    ///
    /// The single entry both surfaces use — a chat `/approve` and the GUI's
    /// approval modal — so the two halves of an answer never drift apart: the
    /// in-memory prompt (what the modal polls, and what knows the action's
    /// risk) and the durable resolution a suspended turn is parked on.
    ///
    /// Answers whether anything was actually waiting.
    pub async fn answer_approval(
        self: &Arc<Self>,
        session_id: &str,
        id: Option<&str>,
        answer: Answer,
    ) -> bool {
        let granted = self.approvals.resolve_scoped(session_id, answer.clone());
        let narrowed = granted.clone().unwrap_or_else(|| narrow_unknown(answer));
        // No sink: the GUI reads the continuation's reply out of the transcript
        // it is already polling.
        let woken = self.answer_suspended(session_id, id, &narrowed, None).await;
        granted.is_some() || woken != Answered::Nothing
    }

    /// Write the answer to a **suspended** turn's approval, and bring the turn
    /// back.
    ///
    /// The durable half of `/approve` and `/deny`: the turn is not waiting on
    /// anything in this process — it gave up its slot and may well have been
    /// asked by a process that has since restarted — so the answer goes into
    /// the log, where the gate reads it when the call is re-dispatched.
    async fn answer_suspended(
        self: &Arc<Self>,
        session_id: &str,
        id: Option<&str>,
        answer: &Answer,
        sink: Option<Arc<dyn ReplySink>>,
    ) -> Answered {
        let Some(waits) = self.waits.clone() else {
            return Answered::Nothing;
        };
        let Some(registration) = self.pending_wait(&waits, session_id, id, is_approval).await
        else {
            return Answered::Nothing;
        };
        let Some(turn_id) = registration.turn_id.clone() else {
            return Answered::Nothing;
        };
        let Wakeup::Approval { call_id } = registration.wakeup.clone() else {
            return Answered::Nothing;
        };

        let (allowed, reason) = match answer {
            Answer::Deny(reason) => (false, reason.clone().unwrap_or_default()),
            _ => (true, String::new()),
        };
        let resolved = SessionEventKind::ApprovalResolved(ApprovalResolvedEvent {
            turn_id: turn_id.clone(),
            call_id: call_id.clone(),
            call_index: self
                .requested_call_index(&waits, &registration.session_id, &turn_id, &call_id)
                .await,
            allowed,
            decided_by: DECIDED_BY_HUMAN.to_string(),
            reason,
            // How long the person took, from the wait being registered. The
            // question is "did somebody think about this", and the registration
            // is when it was put in front of them.
            waited_ms: (now_secs() - registration.created_at).max(0) * 1_000,
        });
        if let Err(error) = waits
            .events
            .append(&registration.session_id, vec![resolved])
            .await
        {
            warn!(%error, turn = %turn_id, "failed to record an approval answer");
            return Answered::Nothing;
        }
        // Durable before the turn acts on it: an allow the log would forget is
        // an action nobody approved.
        if let Err(error) = waits.events.durable_flush(&registration.session_id).await {
            warn!(%error, turn = %turn_id, "an approval answer is not durable; not continuing");
            return Answered::Nothing;
        }

        // Claim it — the sweep may be reaching for the same wait — and only
        // then continue.
        match waits.wakeups.take(&registration.id).await {
            Ok(true) => {}
            Ok(false) => return Answered::Nothing,
            Err(error) => {
                warn!(%error, "failed to claim an answered wait");
                return Answered::Nothing;
            }
        }
        // "For this session" widens the grant to later calls of the same kind,
        // and the key that defines "same kind" is the one the gate recorded
        // when it asked — the in-memory prompt that used to carry it is gone
        // once the turn suspends.
        if matches!(answer, Answer::Session | Answer::Always)
            && let Some(key) = self
                .requested_scope_key(&waits, &registration.session_id, &turn_id, &call_id)
                .await
        {
            self.approvals.remember(&registration.session_id, &key);
        }

        let cause = match allowed {
            true => WakeupCause::Approve,
            false => WakeupCause::Deny,
        };
        if let Err(error) = self
            .continue_turn_with(&registration, cause, "", sink)
            .await
        {
            warn!(%error, turn = %turn_id, "failed to continue an answered turn");
        }
        match registration.session_id == session_id {
            true => Answered::Here,
            false => Answered::Elsewhere(registration.session_id),
        }
    }

    /// The wait an answer is for: named by id, or this chat's own.
    ///
    /// An id is how a routine's approval is answered from the home chat — the
    /// wait belongs to another session entirely. A prefix is enough, because
    /// nobody is going to type a UUIDv7 in full.
    ///
    /// `wanted` narrows it to the kind of wait the caller can actually answer:
    /// `/approve` answers an approval, a plain message answers a question, and
    /// neither may resolve the other.
    async fn pending_wait(
        &self,
        waits: &WaitParts,
        session_id: &str,
        id: Option<&str>,
        wanted: fn(&Wakeup) -> bool,
    ) -> Option<WakeupRegistration> {
        let registrations = waits.wakeups.list().await.ok()?;
        let matching = registrations
            .into_iter()
            .filter(|r| wanted(&r.wakeup) && r.turn_id.is_some());
        match id {
            Some(id) => matching.filter(|r| r.id.starts_with(id)).next(),
            None => matching.filter(|r| r.session_id == session_id).next(),
        }
    }

    /// The question this session's suspended turn is waiting on, if any.
    ///
    /// Read from the log (`turn/suspended`'s summary), not from memory: the
    /// turn that asked may have been in a process that is gone, and the
    /// question outlives it. Backs the GUI's interactions poll.
    pub async fn pending_question(&self, session_id: &str) -> Option<String> {
        let waits = self.waits.clone()?;
        let registration = self
            .pending_wait(&waits, session_id, None, is_user_reply)
            .await?;
        let turn_id = registration.turn_id.clone()?;
        let events = waits.events.events(&registration.session_id).await.ok()?;
        events.iter().rev().find_map(|event| match &event.kind {
            SessionEventKind::TurnSuspended(suspended)
                if suspended.turn_id == turn_id && !suspended.summary.is_empty() =>
            {
                Some(suspended.summary.clone())
            }
            _ => None,
        })
    }

    /// Answer the question a suspended turn asked, and continue it.
    ///
    /// The single entry for every surface that can carry an answer: the next
    /// plain chat message, the GUI's inline reply, `/skip` (an empty answer,
    /// which the tool reads as "nobody answered"). Answers whether anything was
    /// waiting.
    ///
    /// `sink` is where the continuation replies — the surface that answered,
    /// which need not be the one that asked. `None` for the GUI and the api,
    /// which read the reply out of the transcript they already poll.
    pub async fn answer_question(
        self: &Arc<Self>,
        session_id: &str,
        text: &str,
        sink: Option<Arc<dyn ReplySink>>,
    ) -> bool {
        let Some(waits) = self.waits.clone() else {
            return false;
        };
        let Some(registration) = self
            .pending_wait(&waits, session_id, None, is_user_reply)
            .await
        else {
            return false;
        };
        // Claim it first — a sweep may be reaching for the same wait with an
        // expiry — and only then wake the turn.
        match waits.wakeups.take(&registration.id).await {
            Ok(true) => {}
            Ok(false) => return false,
            Err(error) => {
                warn!(%error, "failed to claim an answered question");
                return false;
            }
        }
        // An answer and a decline are the same event with different contents:
        // `moved-on` says the user was asked and chose not to say, which the
        // tool degrades on exactly as it does on an expiry.
        let cause = match text.trim().is_empty() {
            true => WakeupCause::MovedOn,
            false => WakeupCause::Reply,
        };
        if let Err(error) = self
            .continue_turn_with(&registration, cause, text, sink)
            .await
        {
            warn!(%error, "failed to continue an answered turn");
            return false;
        }
        true
    }

    /// The scope key the gate recorded when it asked, if it had one.
    async fn requested_scope_key(
        &self,
        waits: &WaitParts,
        session_id: &str,
        turn_id: &str,
        call_id: &str,
    ) -> Option<String> {
        let events = waits.events.events(session_id).await.ok()?;
        events.iter().rev().find_map(|event| match &event.kind {
            SessionEventKind::ApprovalRequested(requested)
                if requested.turn_id == turn_id
                    && requested.call_id == call_id
                    && !requested.scope_key.is_empty() =>
            {
                Some(requested.scope_key.clone())
            }
            _ => None,
        })
    }

    /// The `call_index` the gate recorded when it asked.
    async fn requested_call_index(
        &self,
        waits: &WaitParts,
        session_id: &str,
        turn_id: &str,
        call_id: &str,
    ) -> u32 {
        requested_call_index(waits, session_id, turn_id, call_id).await
    }

    /// Handle one inbound message. Returns promptly: a plain message spawns its
    /// turn and returns, so the caller's receive loop is never blocked.
    ///
    /// This is the only entry a channel should use: it drops redeliveries
    /// before they reach [`dispatch`](Self::dispatch). Chat platforms deliver
    /// at-least-once, and the gate has to sit in front of *commands* too — a
    /// redelivered `/approve` would approve a second time, which is worse than
    /// a repeated question.
    ///
    /// Takes the *correspondent*, not a session id: a channel knows who wrote,
    /// and which conversation that is belongs to the store. Resolving it here
    /// rather than in each channel is what keeps three ingresses from growing
    /// three copies of "find or open the session".
    pub async fn handle(
        self: &Arc<Self>,
        from: &InboundPeer,
        origin: InboundOrigin,
        text: String,
        sink: Arc<dyn ReplySink>,
    ) {
        let session_id = match self.session_for(from).await {
            Ok(id) => id,
            Err(error) => {
                // Without a session there is nowhere to put the turn, so say so
                // rather than drop the message silently.
                warn!(%error, platform = %from.peer.platform, "could not open a session for this chat");
                let _ = sink.send("会话打开失败，请稍后再试。").await;
                return;
            }
        };
        let session_id = session_id.as_str();
        match self.inbox.claim(&origin, session_id, &text).await {
            Ok(InboxClaim::Duplicate) => {
                info!(
                    platform = %origin.platform,
                    message_id = %origin.message_id,
                    "dropped a redelivered message"
                );
                return;
            }
            Ok(InboxClaim::Fresh) => {}
            Err(error) => {
                // Losing the dedupe record is not a reason to lose the user's
                // message: answering twice is recoverable, silence is not.
                warn!(%error, "inbox claim failed; handling the message anyway");
            }
        }
        // What just arrived may be the reply a commitment has been waiting for.
        // Before the message is routed, and never *instead* of routing it: the
        // person is talking to komo on their own conversation, and a task
        // waking is a second turn elsewhere, not a redirection of this one
        // (docs/bot-runtime.md §6, "no Task router").
        if let Some(triggers) = &self.triggers {
            triggers.on_inbound(&from.peer, &text).await;
        }
        self.dispatch(session_id, from, text, sink).await;
        if let Err(error) = self.inbox.complete(&origin).await {
            // The row stays `claimed`. Harmless today (the key already blocks a
            // redelivery); it becomes the signal for crash re-delivery later.
            warn!(%error, "inbox complete failed (non-fatal)");
        }
    }

    /// Which conversation this message belongs to — the two-step resolution of
    /// docs/bot-runtime.md §3.8.
    ///
    /// **Principal first**: the channel's admission gate already knows whether
    /// the sender is the operator (`allow_from`) or somebody they paired with.
    /// **Then conversation**: the operator writing privately is always the one
    /// home conversation, whichever surface they picked up — a Telegram DM at
    /// lunch and the TUI in the afternoon are one continuous timeline, not two
    /// half-informed ones. Everything else is a conversation *with someone*, and
    /// keys on the correspondent exactly as it always did.
    ///
    /// A conversation's identity is its session id and nothing else, so the map
    /// from an address to that id is **stored**, not computed. It used to be
    /// computed — the session id *was* `feishu:{chat_id}` — which meant a
    /// conversation could not exist without an address, an address could not
    /// change, and anything able to name a session id could name a channel.
    async fn session_for(&self, from: &InboundPeer) -> anyhow::Result<String> {
        if from.is_home() {
            return self.home.home_session().await;
        }
        if let Some(existing) = self.sessions.find_by_peer(&from.peer).await? {
            return Ok(existing.id);
        }
        let session =
            Session::new(uuid::Uuid::now_v7().to_string()).with_channel(from.peer.clone());
        self.sessions.save(&session).await?;
        info!(
            platform = %from.peer.platform,
            session = %session.id,
            "opened a session for a new chat"
        );
        Ok(session.id)
    }

    /// Route one already-deduped message.
    async fn dispatch(
        self: &Arc<Self>,
        session_id: &str,
        from: &InboundPeer,
        text: String,
        sink: Arc<dyn ReplySink>,
    ) {
        match classify(&text) {
            Command::Approve(answer, id) => {
                let asked = answer.clone();
                // Two halves of one answer: the in-memory prompt (what the GUI
                // polls, and what knows the action's risk) and the durable wait
                // a suspended turn is parked on. Either may be absent — after a
                // restart only the second survives.
                let granted = self.approvals.resolve_scoped(session_id, answer.clone());
                let narrowed = granted.clone().unwrap_or_else(|| narrow_unknown(answer));
                let woken = self
                    .answer_suspended(session_id, id.as_deref(), &narrowed, Some(sink.clone()))
                    .await;
                if granted.is_none() && woken != Answered::Nothing {
                    let _ = sink.send(answered_elsewhere(&woken)).await;
                    return;
                }
                let reply = match (&granted, asked) {
                    (Some(Answer::Session), _) => "✅ 已批准（本会话内同类操作将自动放行）",
                    (Some(Answer::Always), _) => {
                        "✅ 已批准，并已记住（同类操作以后不再询问，可用 `komo policy saved list` 查看）"
                    }
                    // The answer was widened but the action is irreversible, so
                    // it was narrowed back. Say so — silently granting less than
                    // was asked for is how a user ends up believing a later
                    // deletion was pre-approved.
                    (Some(Answer::Once), Answer::Session | Answer::Always) => {
                        "✅ 已批准（仅此一次：危险操作不会记住，下次仍会询问）"
                    }
                    (Some(_), _) => "✅ 已批准",
                    (None, _) => "当前没有待审批的操作。",
                };
                let _ = sink.send(reply).await;
            }
            Command::Deny(reason, id) => {
                let explained = reason.is_some();
                let answer = Answer::Deny(reason);
                let in_memory = self.approvals.resolve(session_id, answer.clone());
                let woken = self
                    .answer_suspended(session_id, id.as_deref(), &answer, Some(sink.clone()))
                    .await;
                if !in_memory && woken != Answered::Nothing {
                    let _ = sink.send(answered_elsewhere(&woken)).await;
                    return;
                }
                let reply = if in_memory {
                    if explained {
                        "已拒绝，理由已转达。"
                    } else {
                        "已拒绝。"
                    }
                } else {
                    "当前没有待审批的操作。"
                };
                let _ = sink.send(reply).await;
            }
            Command::Skip => {
                let reply = match self
                    .answer_question(session_id, "", Some(sink.clone()))
                    .await
                {
                    true => "已跳过，这一轮正在继续。",
                    false => "当前没有待回答的问题。",
                };
                let _ = sink.send(reply).await;
            }
            Command::New => {
                // A line in the log, not a new session: the conversation is
                // one ordered timeline and rotating its id would break that
                // (§3.8). Nothing else here is touched — a turn suspended on a
                // question or an approval is still owed its answer,
                // `/approve session` grants are about permission rather than
                // about what was being discussed, and tasks and memories were
                // never this conversation's to end.
                let reply = match self.mark_boundary(session_id).await {
                    Ok(()) => {
                        info!(session = %session_id, "conversation boundary via /new");
                        "已开始新的上下文；之前的对话仍在这条会话里，只是不再默认带给模型。"
                    }
                    Err(error) => {
                        warn!(%error, "failed to record a conversation boundary");
                        "开始新上下文失败，请稍后再试。"
                    }
                };
                let _ = sink.send(reply).await;
            }
            Command::SetHome => {
                // The *address*, not the session: proactive output is delivered
                // to a correspondent, and a session id names no channel.
                let reply = match self.home.set(&from.peer.address()).await {
                    Ok(()) => {
                        info!(home = %from.peer.address(), "home channel set via /sethome");
                        "✅ 已将当前会话设为提醒与通知的接收频道。"
                    }
                    Err(error) => {
                        warn!(%error, "failed to set home channel");
                        "设置接收频道失败，请稍后再试。"
                    }
                };
                let _ = sink.send(reply).await;
            }
            Command::WechatLogin => self.spawn_wechat_login(sink),
            Command::Pair(action) => {
                let reply = self.handle_pair(action).await;
                let _ = sink.send(&reply).await;
            }
            Command::Plain(input) => {
                // A pending `ask_user` question eats the next plain message as
                // its answer — the suspended turn continues with it; no new
                // turn starts. Control commands above keep priority (`/deny`
                // etc. never reach here), and a second message while the turn
                // keeps running queues as usual via `spawn_turn`.
                if self
                    .answer_question(session_id, &input, Some(sink.clone()))
                    .await
                {
                    return;
                }
                // Same rule for an approval this session is parked on: the user
                // said something else, and a pending wait is **replaced** by the
                // next thing they say rather than kept beside it. The message
                // joins the suspended turn, the approval resolves as refused,
                // and the turn continues — so the model sees both the refusal
                // and what was actually said, in one turn instead of two.
                if self.moved_on(session_id, &input, sink.clone()).await {
                    return;
                }
                self.spawn_turn(session_id, input, sink)
            }
        }
    }

    /// Run a `/pair` command against the shared pairing store. Lives in the
    /// gateway (which holds the db lock) so admitting a new sender no longer
    /// needs the `komo pair` CLI — that CLI can't open the db while the
    /// gateway is running. Any already-admitted sender may run it (same trust
    /// level as `/sethome` and `/wechat login`).
    async fn handle_pair(&self, action: PairAction) -> String {
        match action {
            PairAction::Usage => {
                "用法：/pair list · /pair approve <code> · /pair revoke <platform:sender_id>"
                    .to_string()
            }
            PairAction::List => match self.pairings.list().await {
                Ok(list) if list.is_empty() => {
                    "暂无配对。陌生发送者首次联系时会收到一个配对码。".to_string()
                }
                Ok(list) => {
                    let now = time::OffsetDateTime::now_utc().unix_timestamp();
                    let mut out = String::from("配对列表：\n");
                    for p in list {
                        let state = match p.status {
                            PairingStatus::Approved => "approved",
                            PairingStatus::Pending if p.is_expired(now) => "expired",
                            PairingStatus::Pending => "pending",
                        };
                        out.push_str(&format!("· {} [{}]\n", p.id, state));
                    }
                    out.push_str("\n批准：/pair approve <发送者给你的 code>");
                    out
                }
                Err(error) => {
                    warn!(%error, "pair list via chat failed");
                    "读取配对列表失败，请稍后再试。".to_string()
                }
            },
            PairAction::Approve(code) => {
                let code = code.trim().to_uppercase();
                match self.pairings.approve_code(&code).await {
                    Ok(ApproveOutcome::Approved(req)) => {
                        info!(id = %req.id, "pairing approved via chat");
                        format!("✅ 已配对 {} —— 对方现在可以对话了。", req.id)
                    }
                    Ok(ApproveOutcome::NotFound) => {
                        format!(
                            "没有匹配 code {code} 的待批准配对（未知或已过期，见 /pair list）。"
                        )
                    }
                    Ok(ApproveOutcome::Locked { retry_after_secs }) => format!(
                        "失败次数过多，批准已锁定，请 {} 分钟后再试。",
                        (retry_after_secs + 59) / 60
                    ),
                    Err(error) => {
                        warn!(%error, "pair approve via chat failed");
                        "批准失败，请稍后再试。".to_string()
                    }
                }
            }
            PairAction::Revoke(id) => match self.pairings.revoke(&id).await {
                Ok(true) => {
                    info!(%id, "pairing revoked via chat");
                    format!("已解除配对 {id}。")
                }
                Ok(false) => format!("没有配对 {id}（见 /pair list）。"),
                Err(error) => {
                    warn!(%error, "pair revoke via chat failed");
                    "解除配对失败，请稍后再试。".to_string()
                }
            },
        }
    }

    /// Run the WeChat QR login off the receive loop: it blocks while the user
    /// scans, and the QR is delivered to this chat as a photo. On success the
    /// login pulses the channel's `ready` signal, bringing it online.
    fn spawn_wechat_login(self: &Arc<Self>, sink: Arc<dyn ReplySink>) {
        let Some(login) = self.wechat_login.clone() else {
            tokio::spawn(async move {
                let _ = sink
                    .send("微信通道未启用：先在 ~/.komo/config.toml 配置 [channels.wechat]。")
                    .await;
            });
            return;
        };
        tokio::spawn(async move {
            let _ = sink.send("正在生成微信登录二维码，请稍候…").await;
            match login.run(sink.clone()).await {
                Ok(user_id) => {
                    let _ = sink
                        .send(&format!("✅ 微信已连接（{user_id}），现在可以直接对话了。"))
                        .await;
                }
                Err(error) => {
                    warn!(%error, "wechat login via chat failed");
                    let _ = sink.send(&format!("微信登录失败：{error}")).await;
                }
            }
        });
    }

    fn spawn_turn(self: &Arc<Self>, session_id: &str, input: String, sink: Arc<dyn ReplySink>) {
        // One turn at a time per session (keeps a session's history
        // append-ordered). A message that arrives mid-turn is queued (bounded)
        // so a quick follow-up is answered after the current turn instead of
        // dropped; past the cap it's rejected with a hint to resend. (An
        // `/approve` reply is handled above and never reaches here.)
        {
            let mut inflight = self.inflight.lock().unwrap();
            if let Some(queue) = inflight.get_mut(session_id) {
                if queue.len() >= QUEUE_CAP {
                    let sink = sink.clone();
                    tokio::spawn(async move {
                        let _ = sink
                            .send("上一条还在处理、队列已满；这条未处理，请稍后重发。")
                            .await;
                    });
                } else {
                    queue.push_back(QueuedMessage { input, sink });
                }
                return;
            }
            // No turn in flight: mark the session busy (empty queue) and fall
            // through to dispatch.
            inflight.insert(session_id.to_string(), VecDeque::new());
        }
        self.dispatch_turn(session_id.to_string(), input, sink);
    }

    /// Run one turn on a spawned task. The session is already marked in-flight;
    /// [`TurnGuard`] guarantees the session is released (and the next queued
    /// message dispatched) on every exit path, including a panic or cancellation.
    fn dispatch_turn(self: &Arc<Self>, session: String, input: String, sink: Arc<dyn ReplySink>) {
        let this = self.clone();
        let ctx = SessionContext {
            session_id: session.clone(),
            workspace_root: None,
            sink: sink.clone(),
            // A chat channel has a human who can answer an approval prompt.
            interactive: true,
            // Real human prompting — not the trusted loopback-CLI shortcut.
            auto_approve: false,
            // Chat channels don't stream tool events (no live watcher wiring).
            event_sink: None,
            // No cancel affordance in a chat channel — there is no "stop"
            // message, and a turn ends on its own or times out.
            cancel: None,
            // A chat user can talk mid-turn, so let the loop pick those
            // messages up between rounds instead of making them wait.
            interject: Some(Arc::new(QueueInterjector {
                dispatcher: this.clone(),
                session: session.clone(),
            })),
            // A chat turn is user-driven: policy evaluates it against the
            // channel, and a human is reachable for an approval prompt.
            origin: SessionOrigin::User,
            // Filled in by `dispatch_turn`'s caller-supplied context below.
            channel: None,
        };
        tokio::spawn(async move {
            // Armed until normal completion below. If the task is cancelled
            // (e.g. gateway shutdown), its Drop releases the session so it is
            // never left wedged — see `TurnGuard`.
            let mut guard = TurnGuard {
                dispatcher: this.clone(),
                session: session.clone(),
                armed: true,
            };
            // Catch a panic in the turn (LLM client, a repository, etc.) so a
            // single bad turn neither wedges the session nor loses the queued
            // follow-ups: the session is advanced normally below either way.
            let outcome = AssertUnwindSafe(with_session(ctx, this.handler.handle(&session, input)))
                .catch_unwind()
                .await;
            let reply = match outcome {
                Ok(Ok(reply)) => reply,
                Ok(Err(error)) => {
                    warn!(%error, "message handling failed");
                    format!("处理消息时出错了: {error}")
                }
                Err(_panic) => {
                    warn!(session = %session, "turn panicked");
                    "处理消息时发生内部错误，请重试。".to_string()
                }
            };
            if let Err(error) = sink.send(&reply).await {
                warn!(%error, "failed to send reply");
            }
            // Normal completion: advance the queue ourselves (safe to spawn from
            // this async context) and disarm the guard's emergency path.
            guard.armed = false;
            this.finish_turn(&session);
        });
    }

    /// A turn finished normally: drop any approval it left pending, then either
    /// dispatch the next queued message or clear the session's in-flight flag.
    fn finish_turn(self: &Arc<Self>, session: &str) {
        // Any approval the turn abandoned (a tool call never resolved) is dropped,
        // and the transient serialization gate is reclaimed (the session-scoped
        // "approved for session" set stays for the session).
        self.approvals.forget_pending(session);
        self.approvals.release_gate(session);
        let next = {
            let mut inflight = self.inflight.lock().unwrap();
            let Some(queue) = inflight.get_mut(session) else {
                return;
            };
            if queue.is_empty() {
                // Queue drained: the session is now idle.
                inflight.remove(session);
                // Whoever is parked in `claim_session` may take it now. Raised
                // under the lock, so a waiter cannot observe the key gone and
                // still miss the wake.
                self.idle.notify_waiters();
                None
            } else {
                // Keeps the session marked in-flight for the next turn.
                Some(merge_queued(queue))
            }
        };
        if let Some(QueuedMessage { input, sink }) = next {
            self.dispatch_turn(session.to_string(), input, sink);
        }
    }
}

/// Take everything queued behind the turn that just finished as **one** input.
///
/// Chat users habitually split a single thought across several messages. Run as
/// separate turns each costs its own model round-trip and tool loop, and each
/// turn only ever sees a prefix of what the user meant — the first one answers
/// a half-stated question. The queue holds nothing but consecutive user
/// messages (chat commands are handled before `spawn_turn` and never reach it),
/// so joining them is the whole merge.
///
/// The reply goes to the **last** message's sink: it is the freshest reply
/// handle (WeChat's are short-lived, held in memory) and answering the newest
/// message is what a person would do.
fn merge_queued(queue: &mut VecDeque<QueuedMessage>) -> QueuedMessage {
    let mut inputs = Vec::with_capacity(queue.len());
    let mut last_sink = None;
    while let Some(QueuedMessage { input, sink }) = queue.pop_front() {
        inputs.push(input);
        last_sink = Some(sink);
    }
    QueuedMessage {
        input: inputs.join("\n"),
        sink: last_sink.expect("callers merge only a non-empty queue"),
    }
}

/// Feeds one session's queued messages to the turn currently running on it.
///
/// The same queue [`GatewayDispatcher::finish_turn`] drains, so a message goes
/// to exactly one place: whichever gets to it first. Taking it here is the
/// better outcome — the running turn can still act on it, while the next turn
/// can only react after the fact.
struct QueueInterjector {
    dispatcher: Arc<GatewayDispatcher>,
    session: String,
}

impl InterjectSource for QueueInterjector {
    fn take(&self) -> Vec<String> {
        let mut inflight = self.dispatcher.inflight.lock().unwrap();
        // The reply still goes to the running turn's own sink, so the queued
        // messages' sinks are dropped here — same conversation either way, and
        // a turn answers on the handle it started with.
        match inflight.get_mut(&self.session) {
            Some(queue) => queue.drain(..).map(|msg| msg.input).collect(),
            // No entry means the turn already finished; nothing to take.
            None => Vec::new(),
        }
    }
}

/// Told to a sender whose queued message died with the turn ahead of it (see
/// [`TurnGuard`]) — the message was never handled, so the user has to know to
/// resend rather than wait for an answer that isn't coming.
const QUEUED_MESSAGE_DROPPED: &str = "刚才那条消息没能处理（服务重启或任务中断），请重发。";

/// One self-driven turn's exclusive hold on its session, from
/// [`GatewayDispatcher::claim_session`].
///
/// Release it with [`release`](Self::release) when the turn ends — that is what
/// hands the session to whatever queued behind it. Dropping it instead (a
/// panic, a cancelled task) still frees the session, but a queued message dies
/// with it: there is no turn left to run it on, the same way a chat turn's
/// [`TurnGuard`] loses its queue on that path.
pub struct SessionClaim {
    guard: TurnGuard,
}

impl SessionClaim {
    /// The turn is over: drop what it left pending and dispatch whatever queued
    /// behind it.
    pub fn release(mut self) {
        self.guard.armed = false;
        let dispatcher = self.guard.dispatcher.clone();
        let session = self.guard.session.clone();
        drop(self);
        dispatcher.finish_turn(&session);
    }
}

/// Releases a session's turn state on the exit paths a normal completion can't
/// cover — a panic that escapes the catch, or task cancellation. On drop while
/// still `armed` it forgets any pending approval and clears the in-flight flag
/// (dropping any queued messages), so a session is never left permanently busy.
/// The normal path disarms it and calls [`GatewayDispatcher::finish_turn`], which
/// also advances the queue; the guard deliberately does *not* spawn from Drop.
struct TurnGuard {
    dispatcher: Arc<GatewayDispatcher>,
    session: String,
    armed: bool,
}

impl Drop for TurnGuard {
    fn drop(&mut self) {
        if self.armed {
            self.dispatcher.approvals.forget_pending(&self.session);
            self.dispatcher.approvals.release_gate(&self.session);
            let dropped: Vec<Arc<dyn ReplySink>> = {
                let mut inflight = self.dispatcher.inflight.lock().unwrap();
                let dropped = inflight
                    .remove(&self.session)
                    .map(|queue| queue.into_iter().map(|msg| msg.sink).collect())
                    .unwrap_or_default();
                // Same reason as `finish_turn`: the session just became free.
                self.dispatcher.idle.notify_waiters();
                dropped
            };
            // Queued messages can't be dispatched from Drop (no turn to run
            // them on), so they are lost. This path is effectively
            // cancellation-only — gateway shutdown — but the loss must not be
            // silent: the log is the reliable record, and each sender gets a
            // best-effort "resend it" notice so a swallowed follow-up doesn't
            // look like an ignored message.
            if !dropped.is_empty() {
                warn!(
                    session = %self.session,
                    dropped = dropped.len(),
                    "turn cancelled; queued messages discarded"
                );
                // Drop is sync, so the notice needs a task. During a runtime
                // teardown there may be no handle, or the task may never get to
                // run — hence best-effort, with the warn above as the record.
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(async move {
                        for sink in dropped {
                            let _ = sink.send(QUEUED_MESSAGE_DROPPED).await;
                        }
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::session::ChannelPeer;
    use komo_core::domain::session_event::SurfaceRole;
    use std::time::Duration;

    #[test]
    fn classify_matches_commands_case_insensitively() {
        assert_eq!(classify("/new"), Command::New);
        assert_eq!(classify("  /CLEAR "), Command::New);
        assert_eq!(classify("/approve"), Command::Approve(Answer::Once, None));
        assert_eq!(
            classify("/approve session"),
            Command::Approve(Answer::Session, None)
        );
        assert_eq!(classify("/deny"), Command::Deny(None, None));
        assert_eq!(classify("/sethome"), Command::SetHome);
        assert_eq!(classify(" /SetHome "), Command::SetHome);
        assert_eq!(classify("/wechat login"), Command::WechatLogin);
        assert_eq!(classify(" /WeChat "), Command::WechatLogin);
        assert_eq!(classify("hello"), Command::Plain("hello".to_string()));
        // A leading slash inside a longer message is plain text.
        assert_eq!(
            classify("/approve the budget"),
            Command::Plain("/approve the budget".to_string())
        );
    }

    #[test]
    fn deny_takes_a_free_text_reason_for_the_model() {
        assert_eq!(
            classify("/deny 用 trash 代替 rm"),
            Command::Deny(Some("用 trash 代替 rm".to_string()), None)
        );
        // The verb is case-insensitive; the reason keeps its case.
        assert_eq!(
            classify("/DENY Use Trash"),
            Command::Deny(Some("Use Trash".to_string()), None)
        );
        // Whitespace-only argument is the same as a bare `/deny`.
        assert_eq!(classify("/deny    "), Command::Deny(None, None));
    }

    /// A routine's approval is answered from the home chat, so the command has
    /// to be able to name a wait that belongs to another session — while
    /// `/deny <reason>` keeps taking free text.
    #[test]
    fn an_approval_command_can_name_the_wait_it_answers() {
        assert_eq!(
            classify("/approve wk-0199abc"),
            Command::Approve(Answer::Once, Some("wk-0199abc".to_string()))
        );
        assert_eq!(
            classify("/approve wk-0199abc session"),
            Command::Approve(Answer::Session, Some("wk-0199abc".to_string()))
        );
        assert_eq!(
            classify("/deny wk-0199abc 太危险了"),
            Command::Deny(Some("太危险了".to_string()), Some("wk-0199abc".to_string()))
        );
        // Without the prefix it is a reason, not an id.
        assert_eq!(
            classify("/deny 太危险了"),
            Command::Deny(Some("太危险了".to_string()), None)
        );
    }

    #[test]
    fn classify_parses_pair_subcommands_preserving_arg_case() {
        assert_eq!(classify("/pair"), Command::Pair(PairAction::List));
        assert_eq!(classify("/pair list"), Command::Pair(PairAction::List));
        // The verb is case-insensitive but the code/id keep their case.
        assert_eq!(
            classify("/PAIR approve aB12cD34"),
            Command::Pair(PairAction::Approve("aB12cD34".to_string()))
        );
        assert_eq!(
            classify("/pair revoke feishu:ou_AbC"),
            Command::Pair(PairAction::Revoke("feishu:ou_AbC".to_string()))
        );
        // Missing argument → usage, not a turn.
        assert_eq!(classify("/pair approve"), Command::Pair(PairAction::Usage));
        assert_eq!(
            classify("/pair frobnicate"),
            Command::Pair(PairAction::Usage)
        );
    }

    #[tokio::test]
    async fn resolve_returns_false_when_nothing_pending() {
        let state = ApprovalState::new();
        assert!(!state.resolve("s1", Answer::Once));
    }

    fn sample_pending() -> PendingApproval {
        PendingApproval {
            summary: "run shell command: ls".to_string(),
            detail: None,
            risk: "normal".to_string(),
        }
    }

    /// The prompt cache the GUI's modal polls: what is being asked, until it
    /// is answered.
    #[tokio::test]
    async fn a_noted_prompt_is_visible_until_it_is_answered() {
        let state = ApprovalState::new();
        state.note_pending("s1", sample_pending());
        assert_eq!(
            state.pending_info("s1").map(|p| p.summary),
            Some("run shell command: ls".to_string())
        );
        assert_eq!(
            state.resolve_scoped("s1", Answer::Session),
            Some(Answer::Session)
        );
        assert!(state.pending_info("s1").is_none());
        // And answering twice reports the second time as nothing pending.
        assert!(!state.resolve("s1", Answer::Once));
    }

    /// A dangerous action is approved for the one call it was asked about,
    /// whatever the user typed: widening pre-approves a *later* deletion nobody
    /// has seen.
    #[tokio::test]
    async fn a_dangerous_prompt_narrows_a_widening_answer() {
        let state = ApprovalState::new();
        state.note_pending(
            "s1",
            PendingApproval {
                risk: "dangerous".to_string(),
                ..sample_pending()
            },
        );
        assert_eq!(
            state.resolve_scoped("s1", Answer::Always),
            Some(Answer::Once)
        );
    }

    #[tokio::test]
    async fn session_approval_cache_remembers_scope_keys() {
        let state = ApprovalState::new();
        assert!(!state.is_session_approved("s1", "file:write"));
        state.remember("s1", "file:write");
        assert!(state.is_session_approved("s1", "file:write"));
        // Scoped per session.
        assert!(!state.is_session_approved("s2", "file:write"));
    }

    // --- GatewayDispatcher turn queue / panic recovery -----------------------

    use komo_core::domain::{
        pairing::PairingRequest, repository::SessionRepository, session::Session, todo::TodoItem,
    };
    use tokio::sync::{Semaphore, mpsc};

    /// A handler that announces each entered input on a channel and blocks until
    /// the test grants a completion permit — so a test can hold a turn "in
    /// flight" and observe dispatch order. Panics on the input `"boom"`.
    struct GateHandler {
        entered: mpsc::UnboundedSender<String>,
        permits: Arc<Semaphore>,
    }

    #[async_trait]
    impl MessageHandler for GateHandler {
        async fn handle(&self, _session_id: &str, input: String) -> anyhow::Result<String> {
            let _ = self.entered.send(input.clone());
            if input == "boom" {
                panic!("boom");
            }
            let permit = self.permits.acquire().await.unwrap();
            permit.forget();
            Ok(input)
        }
    }

    /// A turn that actually writes to the session log: reports the session it
    /// ran on, and appends the user/assistant pair a real turn would. Enough to
    /// prove two ingresses share one ordered timeline.
    struct SayingHandler {
        entered: mpsc::UnboundedSender<String>,
        events: Arc<dyn SessionEventRepository>,
    }

    #[async_trait]
    impl MessageHandler for SayingHandler {
        async fn handle(&self, session_id: &str, input: String) -> anyhow::Result<String> {
            said(
                self.events.as_ref(),
                session_id,
                "turn",
                SurfaceRole::User,
                &input,
            )
            .await;
            said(
                self.events.as_ref(),
                session_id,
                "turn",
                SurfaceRole::Assistant,
                "ok",
            )
            .await;
            let _ = self.entered.send(session_id.to_string());
            Ok("ok".to_string())
        }
    }

    /// Append one surface message to a session's log.
    async fn said(
        events: &dyn SessionEventRepository,
        session_id: &str,
        turn: &str,
        role: SurfaceRole,
        text: &str,
    ) {
        let kind = match role {
            SurfaceRole::Assistant => SessionEventKind::AssistantMessage(
                komo_core::domain::session_event::AssistantMessageEvent {
                    turn_id: turn.to_string(),
                    content: text.to_string(),
                    tool_note: String::new(),
                    surface: SurfacePlacement::append(),
                },
            ),
            _ => SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: turn.to_string(),
                content: text.to_string(),
                source: MessageSource::User,
                surface: SurfacePlacement::append(),
            }),
        };
        events.append(session_id, vec![kind]).await.unwrap();
        events.durable_flush(session_id).await.unwrap();
    }

    /// A real store under a scratch home, so a test can read a log back.
    async fn test_db(name: &str) -> Arc<komo_infra::persistence::db::Db> {
        let home = std::env::temp_dir().join(name);
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        Arc::new(
            komo_infra::persistence::db::Db::connect(&format!(
                "turso:{}",
                home.join("komo.db").display()
            ))
            .await
            .unwrap(),
        )
    }

    /// A sink that records every text sent through it.
    struct RecordingSink {
        sent: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ReplySink for RecordingSink {
        async fn send(&self, text: &str) -> anyhow::Result<()> {
            self.sent.lock().unwrap().push(text.to_string());
            Ok(())
        }
    }

    /// Just enough session store for the dispatcher: `handle` now resolves a
    /// correspondent to its session, so the tests need that mapping to behave —
    /// open one on first contact, return the same one after.
    #[derive(Default)]
    struct MemorySessions {
        rows: Mutex<Vec<Session>>,
    }

    impl MemorySessions {
        /// Pre-open `id` for `peer`, so a test can name the session a message
        /// will land in before it arrives.
        fn seeded(id: &str, peer: &ChannelPeer) -> Arc<Self> {
            let store = Self::default();
            store
                .rows
                .lock()
                .unwrap()
                .push(Session::new(id).with_channel(peer.clone()));
            Arc::new(store)
        }

        /// The session ids handed out so far, in order of creation.
        fn ids(&self) -> Vec<String> {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .map(|s| s.id.clone())
                .collect()
        }
    }

    #[async_trait]
    impl SessionRepository for MemorySessions {
        async fn find_by_peer(&self, channel: &ChannelPeer) -> anyhow::Result<Option<Session>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|s| s.channel.as_ref() == Some(channel))
                .cloned())
        }
        async fn find(&self, id: &str) -> anyhow::Result<Option<Session>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|s| s.id == id)
                .cloned())
        }
        async fn find_windowed(&self, id: &str, _limit: usize) -> anyhow::Result<Option<Session>> {
            SessionRepository::find(self, id).await
        }
        async fn list(&self) -> anyhow::Result<Vec<Session>> {
            Ok(self.rows.lock().unwrap().clone())
        }
        async fn save(&self, session: &Session) -> anyhow::Result<()> {
            let mut rows = self.rows.lock().unwrap();
            match rows.iter_mut().find(|s| s.id == session.id) {
                Some(existing) => *existing = session.clone(),
                None => rows.push(session.clone()),
            }
            Ok(())
        }
        async fn delete_empty_sessions(&self) -> anyhow::Result<usize> {
            unimplemented!()
        }
    }

    /// The home conversation's stored id, minted on first ask like the real one.
    #[derive(Default)]
    struct MemoryHome {
        address: Mutex<Option<String>>,
        session: Mutex<Option<String>>,
    }

    #[async_trait]
    impl HomeRepository for MemoryHome {
        async fn get(&self) -> anyhow::Result<Option<String>> {
            Ok(self.address.lock().unwrap().clone())
        }
        async fn set(&self, address: &str) -> anyhow::Result<()> {
            *self.address.lock().unwrap() = Some(address.to_string());
            Ok(())
        }
        async fn home_session(&self) -> anyhow::Result<String> {
            let mut held = self.session.lock().unwrap();
            Ok(held
                .get_or_insert_with(|| uuid::Uuid::now_v7().to_string())
                .clone())
        }
    }

    /// Records what a boundary cleared, so `/new`'s blast radius is testable.
    #[derive(Default)]
    struct MemoryTodos {
        cleared: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SessionTodoRepository for MemoryTodos {
        async fn get(&self, _session_id: &str) -> anyhow::Result<Vec<TodoItem>> {
            Ok(Vec::new())
        }
        async fn set(&self, _session_id: &str, _items: &[TodoItem]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn clear(&self, session_id: &str) -> anyhow::Result<()> {
            self.cleared.lock().unwrap().push(session_id.to_string());
            Ok(())
        }
    }

    struct UnusedPairings;
    #[async_trait]
    impl PairingRepository for UnusedPairings {
        async fn upsert(&self, _request: &PairingRequest) -> anyhow::Result<()> {
            unimplemented!()
        }
        async fn find(
            &self,
            _platform: &str,
            _sender_id: &str,
        ) -> anyhow::Result<Option<PairingRequest>> {
            unimplemented!()
        }
        async fn count_active_pending(&self, _platform: &str) -> anyhow::Result<usize> {
            unimplemented!()
        }
        async fn approve_code(&self, _code: &str) -> anyhow::Result<ApproveOutcome> {
            unimplemented!()
        }
        async fn list(&self) -> anyhow::Result<Vec<PairingRequest>> {
            unimplemented!()
        }
        async fn revoke(&self, _id: &str) -> anyhow::Result<bool> {
            unimplemented!()
        }
    }

    fn dispatcher_with(handler: Arc<GateHandler>) -> Arc<GatewayDispatcher> {
        dispatcher_with_parts(handler, Arc::new(AlwaysFreshInbox))
    }

    fn dispatcher_with_parts(
        handler: Arc<GateHandler>,
        inbox: Arc<dyn InboxRepository>,
    ) -> Arc<GatewayDispatcher> {
        dispatcher_with_sessions(handler, inbox, Arc::new(MemorySessions::default()))
    }

    fn dispatcher_with_sessions(
        handler: Arc<GateHandler>,
        inbox: Arc<dyn InboxRepository>,
        sessions: Arc<dyn SessionRepository>,
    ) -> Arc<GatewayDispatcher> {
        Arc::new(GatewayDispatcher::new(
            handler,
            Arc::new(ApprovalState::new()),
            sessions,
            Arc::new(MemoryHome::default()),
            Arc::new(MemoryTodos::default()),
            None,
            Arc::new(UnusedPairings),
            inbox,
        ))
    }

    /// A paired correspondent's own DM: private to *them*, not the operator's,
    /// so it keys on the peer exactly as every chat did before D6.
    fn peer() -> InboundPeer {
        InboundPeer::new(ChannelPeer::new("telegram", "1"), true, false)
    }

    /// The operator, writing from a private surface. Whichever one they pick
    /// up, it is the same conversation.
    fn operator_dm(platform: &str, peer_id: &str) -> InboundPeer {
        InboundPeer::new(ChannelPeer::new(platform, peer_id), true, true)
    }

    /// A chat with other people in it — a Feishu group.
    fn group(peer_id: &str) -> InboundPeer {
        InboundPeer::new(ChannelPeer::new("feishu", peer_id), false, false)
    }

    /// Dedupe has its own test below; every other test wants each message
    /// through.
    struct AlwaysFreshInbox;

    #[async_trait]
    impl InboxRepository for AlwaysFreshInbox {
        async fn claim(
            &self,
            _origin: &InboundOrigin,
            _session_id: &str,
            _text: &str,
        ) -> anyhow::Result<InboxClaim> {
            Ok(InboxClaim::Fresh)
        }

        async fn complete(&self, _origin: &InboundOrigin) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// The real repository's behaviour without a database.
    #[derive(Default)]
    struct DedupingInbox {
        seen: Mutex<HashSet<String>>,
    }

    #[async_trait]
    impl InboxRepository for DedupingInbox {
        async fn claim(
            &self,
            origin: &InboundOrigin,
            _session_id: &str,
            _text: &str,
        ) -> anyhow::Result<InboxClaim> {
            if self.seen.lock().unwrap().insert(origin.key()) {
                Ok(InboxClaim::Fresh)
            } else {
                Ok(InboxClaim::Duplicate)
            }
        }

        async fn complete(&self, _origin: &InboundOrigin) -> anyhow::Result<()> {
            Ok(())
        }
    }

    // A conversation's identity is its session id, and the map from a chat
    // address to that id is stored — so the same correspondent keeps reaching
    // the same conversation, and a different one never does. This used to be
    // arithmetic on strings (`feishu:{chat_id}` *was* the session id), which is
    // why nothing tested it.
    #[tokio::test]
    async fn a_correspondent_keeps_reaching_the_same_session() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let sessions = Arc::new(MemorySessions::default());
        let dispatcher = dispatcher_with_sessions(
            Arc::new(GateHandler {
                entered: entered_tx,
                permits: Arc::new(Semaphore::new(10)),
            }),
            Arc::new(AlwaysFreshInbox),
            sessions.clone(),
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        let alice = group("oc_alice");
        for text in ["第一句", "第二句"] {
            dispatcher
                .handle(&alice, InboundOrigin::local(), text.into(), sink.clone())
                .await;
            next_entered(&mut entered_rx).await;
        }
        assert_eq!(sessions.ids().len(), 1, "one correspondent, one session");

        // A different chat on the same platform is a different conversation.
        dispatcher
            .handle(
                &group("oc_bob"),
                InboundOrigin::local(),
                "你好".into(),
                sink,
            )
            .await;
        next_entered(&mut entered_rx).await;
        let ids = sessions.ids();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);

        // And the id itself carries nothing: it is a uuid, not an address.
        for id in ids {
            assert!(uuid::Uuid::parse_str(&id).is_ok(), "{id}");
        }
        // The address lives in a field, where one reader can find it.
        let stored = sessions.find_by_peer(&alice.peer).await.unwrap().unwrap();
        assert_eq!(stored.channel.as_ref(), Some(&alice.peer));
    }

    /// D6: same principal + private conversation => one ordered timeline.
    ///
    /// The operator's Telegram DM, their Feishu DM and the local client (which
    /// asks the store for the same id rather than minting one) are one
    /// conversation, and the messages land in one log with contiguous seqs. A
    /// Feishu group has someone else in it, so it is a conversation of its own.
    #[tokio::test]
    async fn every_private_surface_of_the_operator_is_one_conversation() {
        let db = test_db("komo-home-conversation").await;

        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let sessions = Arc::new(MemorySessions::default());
        let home_repo: Arc<dyn HomeRepository> = db.clone();
        let dispatcher = Arc::new(GatewayDispatcher::new(
            Arc::new(SayingHandler {
                entered: entered_tx,
                events: db.clone(),
            }),
            Arc::new(ApprovalState::new()),
            sessions.clone(),
            home_repo.clone(),
            Arc::new(MemoryTodos::default()),
            None,
            Arc::new(UnusedPairings),
            Arc::new(AlwaysFreshInbox),
        ));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        // The local client (TUI / desktop / web) opens the stored home id.
        let tui = home_repo.home_session().await.unwrap();

        for (from, text) in [
            (
                operator_dm("telegram", "42"),
                "\u{4e0a}\u{5348}\u{5728}\u{624b}\u{673a}\u{4e0a}\u{95ee}\u{7684}",
            ),
            (
                operator_dm("feishu", "ou_me"),
                "\u{4e2d}\u{5348}\u{6362}\u{98de}\u{4e66}\u{63a5}\u{7740}\u{8bf4}",
            ),
        ] {
            dispatcher
                .handle(&from, InboundOrigin::local(), text.into(), sink.clone())
                .await;
            assert_eq!(
                next_entered(&mut entered_rx).await,
                tui,
                "a private surface of the operator is the home conversation"
            );
        }

        // One log, one writer, no holes.
        let events = SessionEventRepository::events(db.as_ref(), &tui)
            .await
            .unwrap();
        assert_eq!(events.len(), 4, "two exchanges");
        assert!(
            events.iter().enumerate().all(|(i, e)| e.seq == i as u64),
            "seqs stay contiguous across ingresses: {:?}",
            events.iter().map(|e| e.seq).collect::<Vec<_>>()
        );
        assert!(
            sessions.ids().is_empty(),
            "the home conversation is not opened per peer"
        );

        // A group has other people in it, so it keys on the correspondent.
        dispatcher
            .handle(
                &group("oc_team"),
                InboundOrigin::local(),
                "\u{7fa4}\u{91cc}\u{95ee}\u{4e00}\u{53e5}".into(),
                sink,
            )
            .await;
        let group_session = next_entered(&mut entered_rx).await;
        assert_ne!(group_session, tui);
        assert_eq!(sessions.ids(), vec![group_session]);
    }

    /// `/new` is a line in the log, not a rotate and not a cleanup button.
    ///
    /// After it the model is replayed only what came since — but the transcript
    /// and the run ledger still hold everything before, and the approval the
    /// operator left parked is still answerable, still on the same session.
    #[tokio::test]
    async fn a_boundary_moves_the_replay_without_ending_anything() {
        use komo_core::domain::run::Run;

        let db = test_db("komo-boundary").await;
        let home_repo: Arc<dyn HomeRepository> = db.clone();
        let session = home_repo.home_session().await.unwrap();

        // One finished exchange, then a turn suspended on an approval.
        let mut run = Run::start(&session, "delete the tree");
        run.status = komo_core::domain::run::RunStatus::Suspended;
        komo_core::domain::run_projection::RunProjectionStore::commit(
            db.as_ref(),
            &session,
            &[komo_core::domain::run_projection::ProjectedRun {
                run: run.clone(),
                steps: Vec::new(),
                start_seq: 0,
            }],
            0,
        )
        .await
        .unwrap();
        said(
            db.as_ref(),
            &session,
            "turn-1",
            SurfaceRole::User,
            "\u{7b2c}\u{4e00}\u{8f6e}",
        )
        .await;
        said(
            db.as_ref(),
            &session,
            "turn-1",
            SurfaceRole::Assistant,
            "\u{7b54}\u{7b2c}\u{4e00}\u{8f6e}",
        )
        .await;
        SessionEventRepository::append(
            db.as_ref(),
            &session,
            vec![
                SessionEventKind::TurnStarted {
                    turn_id: run.id.clone(),
                    resumed_from: None,
                },
                SessionEventKind::UserMessage(UserMessageEvent {
                    turn_id: run.id.clone(),
                    content: "\u{5220}\u{6389}\u{90a3}\u{4e2a}\u{76ee}\u{5f55}".into(),
                    source: MessageSource::User,
                    surface: SurfacePlacement::append(),
                }),
                SessionEventKind::ApprovalRequested(
                    komo_core::domain::session_event::ApprovalRequestedEvent {
                        turn_id: run.id.clone(),
                        call_id: "c1".into(),
                        call_index: 0,
                        scope_key: String::new(),
                    },
                ),
            ],
        )
        .await
        .unwrap();
        SessionEventRepository::durable_flush(db.as_ref(), &session)
            .await
            .unwrap();
        let registration = WakeupRegistration::new(
            &session,
            Wakeup::Approval {
                call_id: "c1".into(),
            },
            1_000,
        )
        .continuing(&run.id);
        WakeupRepository::save(db.as_ref(), &registration)
            .await
            .unwrap();

        let todos = Arc::new(MemoryTodos::default());
        let resumed = Arc::new(RecordingResume(Mutex::new(Vec::new())));
        let dispatcher = Arc::new(
            GatewayDispatcher::new(
                resumed.clone(),
                Arc::new(ApprovalState::new()),
                Arc::new(MemorySessions::default()),
                home_repo.clone(),
                todos.clone(),
                None,
                Arc::new(UnusedPairings),
                Arc::new(AlwaysFreshInbox),
            )
            .with_waits(WaitParts {
                runs: db.clone(),
                events: db.clone(),
                wakeups: db.clone(),
            }),
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        dispatcher
            .handle(
                &operator_dm("telegram", "42"),
                InboundOrigin::local(),
                "/new".into(),
                sink,
            )
            .await;

        // The conversation keeps its id — this is the same session throughout.
        assert_eq!(home_repo.home_session().await.unwrap(), session);
        assert_eq!(
            todos.cleared.lock().unwrap().as_slice(),
            [session.clone()],
            "the working todo list is what a boundary retires"
        );

        let projection = SessionEventRepository::surface(db.as_ref(), &session)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            projection.messages().unwrap().len(),
            3,
            "the transcript still holds everything before the line"
        );
        assert_eq!(
            projection
                .replay()
                .unwrap()
                .iter()
                .map(|m| m.content.clone())
                .collect::<Vec<_>>(),
            vec!["\u{5220}\u{6389}\u{90a3}\u{4e2a}\u{76ee}\u{5f55}"],
            "the model sees nothing before the line — bar the turn still waiting"
        );

        // The ledger reads back every turn, boundary or no boundary.
        let events = SessionEventRepository::events(db.as_ref(), &session)
            .await
            .unwrap();
        let runs = komo_core::domain::run_projection::project_runs(&session, &events);
        assert!(
            runs.iter().any(|r| r.run.id == run.id),
            "the suspended turn is still in the ledger"
        );

        // And the approval left parked is still answerable.
        assert!(
            dispatcher
                .answer_approval(&session, None, Answer::Once)
                .await,
            "`/new` does not end a turn that is still owed an answer"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            resumed.0.lock().unwrap().as_slice(),
            [run.id.clone()],
            "the woken turn continued"
        );
    }

    // An HTTP turn takes the same per-session slot a chat turn takes, so two
    // clients on one session (a second TUI resuming it, the desktop app beside
    // the terminal) can never run turns side by side. They used to: the api
    // channel called the handler directly, and the later turn assembled its
    // history before the earlier one had written a word of its answer — so it
    // started over from the original question and re-ran everything the first
    // was still doing.
    #[tokio::test]
    async fn a_second_claim_on_one_session_waits_for_the_first() {
        let (entered_tx, _entered_rx) = mpsc::unbounded_channel();
        let dispatcher = dispatcher_with(Arc::new(GateHandler {
            entered: entered_tx,
            permits: Arc::new(Semaphore::new(0)),
        }));

        let first = dispatcher.claim_session("s1").await;

        let mut waiting = {
            let dispatcher = dispatcher.clone();
            tokio::spawn(async move { dispatcher.claim_session("s1").await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiting)
                .await
                .is_err(),
            "a second turn must not start while the first holds the session"
        );

        // The gate is per session, not global: an unrelated session is free.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), dispatcher.claim_session("s2"))
                .await
                .is_ok(),
            "another session must not be blocked by this one"
        );

        first.release();
        let second = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the slot must be handed over once the first turn releases")
            .expect("the waiting task must not panic");
        second.release();
    }

    /// The reply goes where the *answer* came from, not where the question was
    /// asked. Suspended in the TUI, released by `/approve` from Telegram: the
    /// continuation answers into Telegram, because that is where the operator
    /// is standing. The transcript has it either way.
    #[tokio::test]
    async fn a_woken_turn_answers_the_surface_that_released_it() {
        use komo_core::domain::run::Run;

        let db = test_db("komo-woken-reply").await;
        let home_repo: Arc<dyn HomeRepository> = db.clone();
        let session = home_repo.home_session().await.unwrap();

        let mut run = Run::start(&session, "delete the tree");
        run.status = komo_core::domain::run::RunStatus::Suspended;
        komo_core::domain::run_projection::RunProjectionStore::commit(
            db.as_ref(),
            &session,
            &[komo_core::domain::run_projection::ProjectedRun {
                run: run.clone(),
                steps: Vec::new(),
                start_seq: 0,
            }],
            0,
        )
        .await
        .unwrap();
        WakeupRepository::save(
            db.as_ref(),
            &WakeupRegistration::new(
                &session,
                Wakeup::Approval {
                    call_id: "c1".into(),
                },
                1_000,
            )
            .continuing(&run.id),
        )
        .await
        .unwrap();

        let dispatcher = Arc::new(
            GatewayDispatcher::new(
                Arc::new(RecordingResume(Mutex::new(Vec::new()))),
                Arc::new(ApprovalState::new()),
                Arc::new(MemorySessions::default()),
                home_repo,
                Arc::new(MemoryTodos::default()),
                None,
                Arc::new(UnusedPairings),
                Arc::new(AlwaysFreshInbox),
            )
            .with_waits(WaitParts {
                runs: db.clone(),
                events: db.clone(),
                wakeups: db.clone(),
            }),
        );

        // `/approve` arrives from Telegram — the same home conversation, a
        // different surface from the one that asked.
        let sent = Arc::new(Mutex::new(Vec::new()));
        let telegram = Arc::new(RecordingSink { sent: sent.clone() }) as Arc<dyn ReplySink>;
        dispatcher
            .handle(
                &operator_dm("telegram", "42"),
                InboundOrigin::local(),
                "/approve".into(),
                telegram,
            )
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let sent = sent.lock().unwrap().clone();
        assert!(
            sent.iter().any(|line| line == "continued"),
            "the continuation's answer reaches the surface that answered: {sent:?}"
        );
    }

    /// The waker's own three jobs: record *why* the turn came back, retire the
    /// other waits it was holding, and hand the run to whoever continues it.
    #[tokio::test]
    async fn waking_a_turn_records_the_cause_and_retires_its_other_waits() {
        use komo_core::domain::run::Run;
        use komo_core::domain::session_event::Wakeup;

        let home = std::env::temp_dir().join("komo-waker-fire");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let db = Arc::new(
            komo_infra::persistence::db::Db::connect(&format!(
                "turso:{}",
                home.join("komo.db").display()
            ))
            .await
            .unwrap(),
        );

        // A suspended turn in the ledger, with two waits on it: the approval
        // and the deadline watching the same wait.
        let mut run = Run::start("s1", "delete it");
        run.status = komo_core::domain::run::RunStatus::Suspended;
        let projected = komo_core::domain::run_projection::ProjectedRun {
            run: run.clone(),
            steps: Vec::new(),
            start_seq: 0,
        };
        komo_core::domain::run_projection::RunProjectionStore::commit(
            db.as_ref(),
            "s1",
            &[projected],
            0,
        )
        .await
        .unwrap();
        let approval = WakeupRegistration::new(
            "s1",
            Wakeup::Approval {
                call_id: "c1".into(),
            },
            1_000,
        )
        .continuing(&run.id);
        let deadline =
            WakeupRegistration::new("s1", Wakeup::At { at: 2_000 }, 1_000).continuing(&run.id);
        for registration in [&approval, &deadline] {
            WakeupRepository::save(db.as_ref(), registration)
                .await
                .unwrap();
        }

        let handler = Arc::new(RecordingResume(Mutex::new(Vec::new())));
        let dispatcher = Arc::new(
            GatewayDispatcher::new(
                handler.clone(),
                Arc::new(ApprovalState::new()),
                Arc::new(MemorySessions::default()),
                Arc::new(MemoryHome::default()),
                Arc::new(MemoryTodos::default()),
                None,
                Arc::new(UnusedPairings),
                Arc::new(AlwaysFreshInbox),
            )
            .with_waits(WaitParts {
                runs: db.clone(),
                events: db.clone(),
                wakeups: db.clone(),
            }),
        );
        let waker = TurnWaker::new(dispatcher);

        waker
            .fire(&approval, WakeupCause::Approve, "")
            .await
            .expect("the wake itself must land");

        // Why it came back is on the record, durably.
        let events = SessionEventRepository::events(db.as_ref(), "s1")
            .await
            .unwrap();
        let fired = events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::WakeupFired(fired) => Some(fired),
                _ => None,
            })
            .next()
            .expect("the log says what ended the wait");
        assert_eq!(fired.turn_id, run.id);
        assert_eq!(fired.cause, WakeupCause::Approve);
        assert_eq!(
            fired.wakeup_id, approval.id,
            "traceable to what scheduled it"
        );

        // Both waits are gone: the deadline must not wake the same turn again.
        assert!(
            WakeupRepository::list(db.as_ref())
                .await
                .unwrap()
                .is_empty(),
            "a woken turn takes every wait it was holding with it"
        );

        // And the continuation was handed the suspended run. Spawned, so give
        // it a moment.
        for _ in 0..200 {
            if !handler.0.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(*handler.0.lock().unwrap(), vec![run.id.clone()]);
    }

    /// Records the run it was asked to continue.
    struct RecordingResume(Mutex<Vec<String>>);

    #[async_trait]
    impl MessageHandler for RecordingResume {
        async fn handle(&self, _session_id: &str, input: String) -> anyhow::Result<String> {
            Ok(input)
        }
        async fn resume_interrupted(
            &self,
            run: &komo_core::domain::run::Run,
        ) -> anyhow::Result<Option<String>> {
            self.0.lock().unwrap().push(run.id.clone());
            Ok(Some("continued".to_string()))
        }
    }

    /// A pending wait is **replaced** by the next thing the user says, not kept
    /// beside it. The message joins the suspended turn, the approval is
    /// refused citing it, and no second turn starts — the model answers both in
    /// one turn.
    #[tokio::test]
    async fn saying_something_else_takes_the_place_of_a_pending_approval() {
        use komo_core::domain::run::Run;

        let home = std::env::temp_dir().join("komo-moved-on");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let db = Arc::new(
            komo_infra::persistence::db::Db::connect(&format!(
                "turso:{}",
                home.join("komo.db").display()
            ))
            .await
            .unwrap(),
        );

        let mut run = Run::start("s1", "delete the tree");
        run.status = komo_core::domain::run::RunStatus::Suspended;
        komo_core::domain::run_projection::RunProjectionStore::commit(
            db.as_ref(),
            "s1",
            &[komo_core::domain::run_projection::ProjectedRun {
                run: run.clone(),
                steps: Vec::new(),
                start_seq: 0,
            }],
            0,
        )
        .await
        .unwrap();
        WakeupRepository::save(
            db.as_ref(),
            &WakeupRegistration::new(
                "s1",
                Wakeup::Approval {
                    call_id: "c1".into(),
                },
                1_000,
            )
            .continuing(&run.id),
        )
        .await
        .unwrap();

        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let dispatcher = Arc::new(
            GatewayDispatcher::new(
                Arc::new(GateHandler {
                    entered: entered_tx,
                    permits: Arc::new(Semaphore::new(1)),
                }),
                Arc::new(ApprovalState::new()),
                Arc::new(MemorySessions::default()),
                Arc::new(MemoryHome::default()),
                Arc::new(MemoryTodos::default()),
                None,
                Arc::new(UnusedPairings),
                Arc::new(AlwaysFreshInbox),
            )
            .with_waits(WaitParts {
                runs: db.clone(),
                events: db.clone(),
                wakeups: db.clone(),
            }),
        );
        assert!(
            dispatcher
                .moved_on(
                    "s1",
                    "算了，先别删",
                    Arc::new(RecordingSink {
                        sent: Arc::new(Mutex::new(Vec::new())),
                    }),
                )
                .await,
            "the message answers the pending approval"
        );
        assert!(
            entered_rx.try_recv().is_err(),
            "and no second turn is started for it"
        );

        let events = SessionEventRepository::events(db.as_ref(), "s1")
            .await
            .unwrap();
        let said = events
            .iter()
            .find_map(|event| match &event.kind {
                SessionEventKind::UserMessage(m) => Some(m),
                _ => None,
            })
            .expect("what the user said is on the record");
        assert_eq!(said.turn_id, run.id, "it belongs to the suspended turn");
        assert_eq!(
            said.source,
            MessageSource::Injected,
            "as an interjection, so the surface still alternates"
        );

        let resolved = events
            .iter()
            .find_map(|event| match &event.kind {
                SessionEventKind::ApprovalResolved(resolved) => Some(resolved),
                _ => None,
            })
            .expect("the approval is answered, not left hanging");
        assert!(!resolved.allowed);
        assert!(
            resolved.reason.contains("算了，先别删"),
            "and the refusal cites what was said: {}",
            resolved.reason
        );
        assert!(
            WakeupRepository::list(db.as_ref())
                .await
                .unwrap()
                .is_empty(),
            "the wait is retired with it"
        );
    }

    /// A wait that ran out is written down as such *before* the turn comes
    /// back, so the gate reads it as a refusal instead of asking again.
    #[tokio::test]
    async fn an_expired_wait_records_the_expiry_before_continuing() {
        use komo_core::domain::run::Run;

        let home = std::env::temp_dir().join("komo-waker-expired");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let db = Arc::new(
            komo_infra::persistence::db::Db::connect(&format!(
                "turso:{}",
                home.join("komo.db").display()
            ))
            .await
            .unwrap(),
        );

        // The gate asked, and recorded where in its round the call sat.
        let mut run = Run::start("s1", "delete it");
        run.status = komo_core::domain::run::RunStatus::Suspended;
        komo_core::domain::run_projection::RunProjectionStore::commit(
            db.as_ref(),
            "s1",
            &[komo_core::domain::run_projection::ProjectedRun {
                run: run.clone(),
                steps: Vec::new(),
                start_seq: 0,
            }],
            0,
        )
        .await
        .unwrap();
        SessionEventRepository::append(
            db.as_ref(),
            "s1",
            vec![SessionEventKind::ApprovalRequested(
                komo_core::domain::session_event::ApprovalRequestedEvent {
                    turn_id: run.id.clone(),
                    call_id: "c1".into(),
                    call_index: 2,
                    scope_key: String::new(),
                },
            )],
        )
        .await
        .unwrap();
        SessionEventRepository::durable_flush(db.as_ref(), "s1")
            .await
            .unwrap();

        let registration = WakeupRegistration::new(
            "s1",
            Wakeup::Approval {
                call_id: "c1".into(),
            },
            1_000,
        )
        .continuing(&run.id);
        WakeupRepository::save(db.as_ref(), &registration)
            .await
            .unwrap();

        let handler = Arc::new(RecordingResume(Mutex::new(Vec::new())));
        let dispatcher = Arc::new(
            GatewayDispatcher::new(
                handler.clone(),
                Arc::new(ApprovalState::new()),
                Arc::new(MemorySessions::default()),
                Arc::new(MemoryHome::default()),
                Arc::new(MemoryTodos::default()),
                None,
                Arc::new(UnusedPairings),
                Arc::new(AlwaysFreshInbox),
            )
            .with_waits(WaitParts {
                runs: db.clone(),
                events: db.clone(),
                wakeups: db.clone(),
            }),
        );

        TurnWaker::new(dispatcher)
            .fire(&registration, WakeupCause::Expired, "")
            .await
            .unwrap();

        let events = SessionEventRepository::events(db.as_ref(), "s1")
            .await
            .unwrap();
        let expired = events
            .iter()
            .find_map(|event| match &event.kind {
                SessionEventKind::ApprovalExpired {
                    turn_id,
                    call_id,
                    call_index,
                } => Some((turn_id, call_id, *call_index)),
                _ => None,
            })
            .expect("an expiry the turn can read as a refusal");
        assert_eq!(expired.0, &run.id);
        assert_eq!(expired.1, "c1");
        assert_eq!(
            expired.2, 2,
            "it names the same call the request did, read back from the request"
        );
    }

    /// Stop is pressed on a conversation, so it has to reach the caller *queued*
    /// for the session as well as the one holding it — and that caller should
    /// give up its wait rather than run, once the turn it was queued behind
    /// finishes, the very work the user just stopped.
    #[tokio::test]
    async fn a_caller_queued_for_the_session_gives_up_when_it_is_cancelled() {
        let (entered_tx, _entered_rx) = mpsc::unbounded_channel();
        let dispatcher = dispatcher_with(Arc::new(GateHandler {
            entered: entered_tx,
            permits: Arc::new(Semaphore::new(0)),
        }));
        let cancels = Arc::new(CancelState::new());

        let running = dispatcher.claim_session("s1").await;
        // The api ingress's shape: register, then race the wait for the slot
        // against the signal.
        let ticket = cancels.register("s1");
        let queued = {
            let dispatcher = dispatcher.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _claim = dispatcher.claim_session("s1") => "ran",
                    () = ticket.cancelled() => "stopped",
                }
            })
        };
        // Parked, not running.
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(cancels.cancel("s1"), "the session has listeners");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), queued)
                .await
                .expect("a cancelled waiter must not keep waiting")
                .unwrap(),
            "stopped"
        );
        running.release();
    }

    // The other direction of the same invariant: a chat message that arrives
    // while a self-driven (HTTP) turn holds the session queues behind it and
    // runs when it finishes, rather than opening a concurrent turn.
    #[tokio::test]
    async fn a_chat_message_arriving_during_a_claimed_turn_runs_after_it() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let dispatcher = dispatcher_with(Arc::new(GateHandler {
            entered: entered_tx,
            permits: Arc::new(Semaphore::new(1)),
        }));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        let claim = dispatcher.claim_session("s1").await;
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "看下 A".into(), sink)
            .await;
        assert!(
            entered_rx.try_recv().is_err(),
            "the chat message must queue, not run beside the claimed turn"
        );

        claim.release();
        assert_eq!(next_entered(&mut entered_rx).await, "看下 A");
    }

    /// Wait for the next entered input, failing the test on timeout so a wedge
    /// surfaces as a failure rather than a hang.
    async fn next_entered(rx: &mut mpsc::UnboundedReceiver<String>) -> String {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for a turn to start")
            .expect("handler channel closed")
    }

    /// "Approve for the session" widens an approval to calls the user has not
    /// seen yet. For an irreversible action the next such call is a *second*
    /// deletion, not a repeat of the one that was shown — so the widening is
    /// refused and the user is told, rather than silently granted less than
    /// they asked for.
    #[tokio::test]
    async fn a_dangerous_action_is_never_approved_beyond_the_call_it_was_asked_about() {
        let state = ApprovalState::new();
        let dangerous = PendingApproval {
            summary: "rm -rf /data".to_string(),
            detail: None,
            risk: "dangerous".to_string(),
        };
        state.note_pending("s1", dangerous);
        assert_eq!(
            state.resolve_scoped("s1", Answer::Session),
            Some(Answer::Once),
            "a session-wide grant must narrow to this one call"
        );

        // `always` would have written a persisted rule; it narrows the same way.
        state.note_pending(
            "s2",
            PendingApproval {
                summary: "drop the table".to_string(),
                detail: None,
                risk: "dangerous".to_string(),
            },
        );
        assert_eq!(
            state.resolve_scoped("s2", Answer::Always),
            Some(Answer::Once)
        );

        // A normal action is untouched — that is what the scopes are for.
        state.note_pending(
            "s3",
            PendingApproval {
                summary: "write a file".to_string(),
                detail: None,
                risk: "normal".to_string(),
            },
        );
        assert_eq!(
            state.resolve_scoped("s3", Answer::Session),
            Some(Answer::Session)
        );
    }

    /// Chat platforms deliver at-least-once: Telegram redelivers a whole batch
    /// when the offset never got committed, Feishu retries what it thinks was
    /// not acked, and either survives a gateway restart. A redelivery must not
    /// run a second turn — and the gate sits in front of *commands* too, so a
    /// redelivered `/approve` cannot approve twice.
    #[tokio::test]
    async fn a_redelivered_message_never_runs_a_second_turn() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let dispatcher = dispatcher_with_parts(
            Arc::new(GateHandler {
                entered: entered_tx,
                permits: Arc::new(Semaphore::new(8)),
            }),
            Arc::new(DedupingInbox::default()),
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;
        let origin = InboundOrigin::new("telegram", "42");

        dispatcher
            .handle(&peer(), origin.clone(), "hello".into(), sink.clone())
            .await;
        assert_eq!(next_entered(&mut entered_rx).await, "hello");

        // The same platform message, delivered again.
        dispatcher
            .handle(&peer(), origin, "hello".into(), sink.clone())
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            entered_rx.try_recv().is_err(),
            "a redelivered message must not start a second turn"
        );

        // A genuinely new message still gets through.
        dispatcher
            .handle(
                &peer(),
                InboundOrigin::new("telegram", "43"),
                "and another".into(),
                sink,
            )
            .await;
        assert_eq!(next_entered(&mut entered_rx).await, "and another");
    }

    #[tokio::test]
    async fn mid_turn_messages_merge_into_one_turn_and_cap() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let permits = Arc::new(Semaphore::new(0));
        let handler = Arc::new(GateHandler {
            entered: entered_tx,
            permits: permits.clone(),
        });
        let dispatcher = dispatcher_with(handler);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent: sent.clone() }) as Arc<dyn ReplySink>;

        // m1 dispatches and blocks in the handler.
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "m1".into(), sink.clone())
            .await;
        assert_eq!(next_entered(&mut entered_rx).await, "m1");

        // m2, m3 queue behind it; m4 overflows the cap and is rejected.
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "m2".into(), sink.clone())
            .await;
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "m3".into(), sink.clone())
            .await;
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "m4".into(), sink.clone())
            .await;

        // Everything queued behind m1 runs as ONE turn, in order — a user who
        // splits a thought across messages gets one answer, not one per line.
        permits.add_permits(1);
        assert_eq!(next_entered(&mut entered_rx).await, "m2\nm3");
        permits.add_permits(1);

        // Let the final reply + rejection settle, then assert the overflow hint
        // was delivered and no fourth turn ever started.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let sent = sent.lock().unwrap();
        assert!(
            sent.iter().any(|t| t.contains("队列已满")),
            "m4 should be rejected with the queue-full hint, got {sent:?}"
        );
        assert!(entered_rx.try_recv().is_err(), "m4 must not have run");
    }

    /// The running turn takes queued messages out of the same queue the next
    /// turn would drain, so a message is delivered exactly once — and once
    /// taken, `finish_turn` finds nothing left to dispatch.
    #[tokio::test]
    async fn a_running_turn_takes_queued_messages_for_itself() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let permits = Arc::new(Semaphore::new(0));
        let dispatcher = dispatcher_with_sessions(
            Arc::new(GateHandler {
                entered: entered_tx,
                permits: permits.clone(),
            }),
            Arc::new(AlwaysFreshInbox),
            MemorySessions::seeded("s7", &peer().peer),
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        dispatcher
            .handle(&peer(), InboundOrigin::local(), "m1".into(), sink.clone())
            .await;
        assert_eq!(next_entered(&mut entered_rx).await, "m1");
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "m2".into(), sink.clone())
            .await;
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "m3".into(), sink.clone())
            .await;

        // What the agent loop does between rounds.
        let interjector = QueueInterjector {
            dispatcher: dispatcher.clone(),
            session: "s7".to_string(),
        };
        assert_eq!(interjector.take(), vec!["m2".to_string(), "m3".to_string()]);
        assert!(interjector.take().is_empty(), "taken exactly once");

        // The turn finishes with an empty queue, so no follow-up turn runs.
        permits.add_permits(1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            entered_rx.try_recv().is_err(),
            "the messages were already handled inside the first turn"
        );
        assert!(
            !dispatcher.inflight.lock().unwrap().contains_key("s7"),
            "the session should be idle once the turn ends"
        );
    }

    /// A turn killed mid-flight (gateway shutdown) can't run what queued behind
    /// it, but the sender must not be left waiting for an answer that will
    /// never come. Drives [`TurnGuard`]'s emergency path directly — the normal
    /// completion path drains the queue instead.
    #[tokio::test]
    async fn a_dropped_turn_tells_queued_senders_to_resend() {
        let (entered_tx, _entered_rx) = mpsc::unbounded_channel();
        let dispatcher = dispatcher_with(Arc::new(GateHandler {
            entered: entered_tx,
            permits: Arc::new(Semaphore::new(0)),
        }));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent: sent.clone() }) as Arc<dyn ReplySink>;

        // The state a turn in flight with one queued follow-up leaves behind.
        dispatcher.inflight.lock().unwrap().insert(
            "s8".to_string(),
            VecDeque::from(vec![QueuedMessage {
                input: "跟进的一条".into(),
                sink: sink.clone(),
            }]),
        );

        drop(TurnGuard {
            dispatcher: dispatcher.clone(),
            session: "s8".to_string(),
            armed: true,
        });

        // The notice is spawned, so give it a turn of the runtime to land.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            sent.lock().unwrap().iter().any(|t| t.contains("请重发")),
            "the queued sender should be told to resend, got {:?}",
            sent.lock().unwrap()
        );
        assert!(
            !dispatcher.inflight.lock().unwrap().contains_key("s8"),
            "the session must not be left wedged"
        );
    }

    #[tokio::test]
    async fn a_panicking_turn_does_not_wedge_the_session() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let permits = Arc::new(Semaphore::new(0));
        let handler = Arc::new(GateHandler {
            entered: entered_tx,
            permits: permits.clone(),
        });
        let dispatcher = dispatcher_with(handler);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent: sent.clone() }) as Arc<dyn ReplySink>;

        // First turn panics — the catch keeps the task alive and the guard/finish
        // path releases the session.
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "boom".into(), sink.clone())
            .await;
        assert_eq!(next_entered(&mut entered_rx).await, "boom");

        // A later message must still be handled (session not permanently busy).
        dispatcher
            .handle(
                &peer(),
                InboundOrigin::local(),
                "after".into(),
                sink.clone(),
            )
            .await;
        permits.add_permits(1);
        assert_eq!(next_entered(&mut entered_rx).await, "after");
    }

    // --- kanban Task ↔ Wakeup (docs/bot-runtime.md §3.7, §8 判据 9) ----------

    /// Records every turn it was asked to run, with the session it ran on.
    #[derive(Default)]
    struct RecordingTurns {
        turns: Mutex<Vec<(String, String)>>,
        resumed: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl MessageHandler for RecordingTurns {
        async fn handle(&self, session_id: &str, input: String) -> anyhow::Result<String> {
            self.turns
                .lock()
                .unwrap()
                .push((session_id.to_string(), input));
            Ok("ok".to_string())
        }
        async fn resume_interrupted(
            &self,
            run: &komo_core::domain::run::Run,
        ) -> anyhow::Result<Option<String>> {
            self.resumed.lock().unwrap().push(run.id.clone());
            Ok(Some("continued".to_string()))
        }
    }

    /// The dispatcher a triggered wake needs: a real store, a real waker, and
    /// the matcher wired to the same one.
    fn triggering_dispatcher(
        db: &Arc<komo_infra::persistence::db::Db>,
        handler: Arc<dyn MessageHandler>,
    ) -> Arc<GatewayDispatcher> {
        let triggers = Arc::new(komo_services::triggers::TriggerMatcher::new(
            db.clone(),
            db.clone(),
        ));
        let dispatcher = Arc::new(
            GatewayDispatcher::new(
                handler,
                Arc::new(ApprovalState::new()),
                Arc::new(MemorySessions::default()),
                Arc::new(MemoryHome::default()),
                Arc::new(MemoryTodos::default()),
                None,
                Arc::new(UnusedPairings),
                Arc::new(AlwaysFreshInbox),
            )
            .with_waits(WaitParts {
                runs: db.clone(),
                events: db.clone(),
                wakeups: db.clone(),
            })
            .with_triggers(triggers.clone()),
        );
        triggers.attach_dispatch(Arc::new(TurnWaker::new(dispatcher.clone())));
        dispatcher
    }

    /// Turns are spawned; give them a moment to land.
    async fn settle(done: impl Fn() -> bool) {
        for _ in 0..200 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The whole §5.10 loop: a commitment waiting on a Feishu peer, that peer
    /// writing, and the turn it opens on the session the commitment came from —
    /// beside the message's own turn, never instead of it. The task's status is
    /// left alone: whether that message discharges the commitment is a
    /// judgement, and the model makes it in the turn this opens.
    #[tokio::test]
    async fn a_waiting_tasks_peer_writing_opens_a_turn_where_the_task_came_from() {
        use komo_core::domain::task::{Task, TaskRepository, TaskStatus};

        let db = test_db("komo-task-trigger").await;
        let waiting = komo_services::task_waiting::TaskWaiting::new(db.clone(), db.clone());

        let mut task = Task::new("等张三的方案".into());
        task.status = TaskStatus::Waiting;
        task.waiting_on = "张三".into();
        task.waiting_on_peer = Some(ChannelPeer::new("feishu", "ou_x"));
        task.source = "s-task".into();
        waiting.sync(&mut task, 1_000).await.unwrap();
        TaskRepository::save(db.as_ref(), &task).await.unwrap();

        // Entering `Waiting` registered exactly one wake, and the task holds it.
        let rows = WakeupRepository::list(db.as_ref()).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].wakeup,
            Wakeup::Event {
                filter: komo_core::domain::session_event::EventFilter::FromPeer {
                    platform: "feishu".into(),
                    peer_id: "ou_x".into()
                }
            }
        );
        assert_eq!(rows[0].turn_id, None, "a reply opens a turn of its own");
        assert_eq!(task.wakeup_id.as_deref(), Some(rows[0].id.as_str()));

        let handler = Arc::new(RecordingTurns::default());
        let dispatcher = triggering_dispatcher(&db, handler.clone());
        let sink = Arc::new(RecordingSink {
            sent: Arc::new(Mutex::new(Vec::new())),
        }) as Arc<dyn ReplySink>;

        dispatcher
            .handle(
                &group("ou_x"),
                InboundOrigin::local(),
                "方案发你了".into(),
                sink.clone(),
            )
            .await;

        settle(|| handler.turns.lock().unwrap().len() >= 2).await;
        let turns = handler.turns.lock().unwrap().clone();
        let woken = turns
            .iter()
            .find(|(session, _)| session == "s-task")
            .expect("the commitment's own session got a turn");
        assert!(woken.1.contains("方案发你了"), "{}", woken.1);
        assert!(woken.1.contains("等张三的方案"), "{}", woken.1);
        assert!(
            turns.iter().any(|(session, _)| session != "s-task"),
            "the message still reaches the conversation it was sent to"
        );

        // Claimed, and the task says so — but nobody decided it is discharged.
        assert!(
            WakeupRepository::list(db.as_ref())
                .await
                .unwrap()
                .is_empty()
        );
        let stored = TaskRepository::find(db.as_ref(), &task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, TaskStatus::Waiting);
        assert_eq!(stored.wakeup_id, None);

        // Closed out, the same person writing again is just a message.
        let mut done = stored;
        done.status = TaskStatus::Done;
        waiting.sync(&mut done, 2_000).await.unwrap();
        TaskRepository::update(db.as_ref(), &done).await.unwrap();

        let before = handler.turns.lock().unwrap().len();
        dispatcher
            .handle(
                &group("ou_x"),
                InboundOrigin::local(),
                "还有件事".into(),
                sink,
            )
            .await;
        settle(|| handler.turns.lock().unwrap().len() > before).await;
        assert_eq!(
            handler
                .turns
                .lock()
                .unwrap()
                .iter()
                .filter(|(session, _)| session == "s-task")
                .count(),
            1,
            "a closed commitment is not woken again"
        );
    }

    /// The other way to consume the same wake: a turn parked on
    /// `wait { for_task }` is continued rather than a fresh one opened, and
    /// what it is handed is the message itself.
    #[tokio::test]
    async fn a_turn_waiting_on_a_task_is_continued_by_that_peers_message() {
        use komo_core::domain::run::Run;

        let db = test_db("komo-task-trigger-wait").await;
        let mut run = Run::start("s1", "等张三");
        run.status = komo_core::domain::run::RunStatus::Suspended;
        komo_core::domain::run_projection::RunProjectionStore::commit(
            db.as_ref(),
            "s1",
            &[komo_core::domain::run_projection::ProjectedRun {
                run: run.clone(),
                steps: Vec::new(),
                start_seq: 0,
            }],
            0,
        )
        .await
        .unwrap();
        let registration = WakeupRegistration::new(
            "s1",
            Wakeup::Event {
                filter: komo_core::domain::session_event::EventFilter::FromPeer {
                    platform: "feishu".into(),
                    peer_id: "ou_x".into(),
                },
            },
            1_000,
        )
        .continuing(&run.id);
        WakeupRepository::save(db.as_ref(), &registration)
            .await
            .unwrap();

        let handler = Arc::new(RecordingTurns::default());
        let dispatcher = triggering_dispatcher(&db, handler.clone());
        dispatcher
            .handle(
                &group("ou_x"),
                InboundOrigin::local(),
                "好了".into(),
                Arc::new(RecordingSink {
                    sent: Arc::new(Mutex::new(Vec::new())),
                }),
            )
            .await;

        settle(|| !handler.resumed.lock().unwrap().is_empty()).await;
        assert_eq!(*handler.resumed.lock().unwrap(), vec![run.id.clone()]);

        // What ended the wait is on the record, carrying what arrived — which
        // is what the `wait` call reads when it is re-dispatched.
        let events = SessionEventRepository::events(db.as_ref(), "s1")
            .await
            .unwrap();
        let fired = events
            .iter()
            .find_map(|event| match &event.kind {
                SessionEventKind::WakeupFired(fired) => Some(fired),
                _ => None,
            })
            .expect("the log says what ended the wait");
        assert_eq!(fired.cause, WakeupCause::Event);
        assert_eq!(fired.payload, "好了");
    }
}
