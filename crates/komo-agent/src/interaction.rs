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

use komo_services::clarify::ClarifyState;
use komo_services::tool_execution::{SessionContext, SessionOrigin, current_session, with_session};
use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::FutureExt;
use tokio::sync::{Notify, oneshot, watch};
use tracing::{info, warn};

use komo_core::domain::{
    approval::{ApprovalRequest, Approver, DECIDED_BY_HUMAN, Decision, Risk},
    cancel::CancelSignal,
    gateway::{InterjectSource, MessageHandler, ReplySink, WeChatLogin},
    home::HomeRepository,
    inbox::{InboundOrigin, InboxClaim, InboxRepository},
    pairing::{ApproveOutcome, PairingRepository, PairingStatus},
    repository::{SessionEventRepository, SessionRepository},
    run::RunRepository,
    session::{ChannelPeer, Session},
    session_event::{SessionEventKind, WakeupCause, WakeupFiredEvent},
    todo::SessionTodoRepository,
    wakeup::{WakeupDispatch, WakeupRegistration, WakeupRepository},
};

/// How long an approval prompt waits for a reply before auto-denying.
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(300);

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

/// Per-session cancellation, keyed like the approval/clarify state: the api
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
/// (resolves on `/approve`, clears on `/new`).
pub struct ApprovalState {
    pending: Mutex<HashMap<String, (oneshot::Sender<Answer>, PendingApproval)>>,
    approved: Mutex<HashMap<String, HashSet<String>>>,
    /// Per-session serialization gate. A round's tool calls now run
    /// concurrently (`AgentRuntime::run_agent_loop`), so two side-effecting
    /// tools can ask for approval at once; holding this across the
    /// prompt→await→resolve cycle keeps the single `pending` slot from being
    /// raced (a second `register` would otherwise drop the first sender, denying
    /// it). Per session, not global, so a slow approver in one chat never blocks
    /// another chat's prompt.
    gates: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    timeout: Duration,
}

impl ApprovalState {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            approved: Mutex::new(HashMap::new()),
            gates: Mutex::new(HashMap::new()),
            timeout: APPROVAL_TIMEOUT,
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

    /// Register a pending approval for `session`, returning the receiver the
    /// approver awaits. Replaces any prior pending approval (its sender drops,
    /// which the old waiter reads as a denial). `info` is the structured prompt
    /// stored for the interactions poll.
    fn register(&self, session: &str, info: PendingApproval) -> oneshot::Receiver<Answer> {
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap()
            .insert(session.to_string(), (tx, info));
        rx
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
        let (tx, info) = self.pending.lock().unwrap().remove(session)?;
        let narrowed = if info.risk == "dangerous" {
            match decision {
                Answer::Session | Answer::Always => Answer::Once,
                other => other,
            }
        } else {
            decision
        };
        tx.send(narrowed.clone()).ok()?;
        Some(narrowed)
    }

    /// The structured description of the approval pending for `session`, if any.
    /// Backs the HTTP `GET /api/interactions/{session}` poll the GUI uses to
    /// render an approval modal (chat channels instead see it via the sink).
    pub fn pending_info(&self, session: &str) -> Option<PendingApproval> {
        self.pending
            .lock()
            .unwrap()
            .get(session)
            .map(|(_, info)| info.clone())
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

    /// Forget all approval state for `session` (on `/new`): cancel any pending
    /// wait and drop the session's "allow for this session" set.
    pub fn clear(&self, session: &str) {
        self.forget_pending(session);
        self.approved.lock().unwrap().remove(session);
        self.gates.lock().unwrap().remove(session);
    }

    /// Reclaim the session's transient serialization gate between turns
    /// (recreated on demand by [`gate`](Self::gate)). Called when a turn
    /// finishes so the `gates` map doesn't accumulate one entry per session for
    /// the gateway's lifetime. The `approved` set is deliberately *not* touched
    /// — it is session-scoped and must survive until `/new`.
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

        let rx = self
            .state
            .register(&ctx.session_id, PendingApproval::from_request(request));
        match tokio::time::timeout(self.state.timeout, rx).await {
            Ok(Ok(Answer::Once)) => Decision::Allow,
            Ok(Ok(Answer::Session)) => {
                if let Some(key) = &request.scope_key {
                    self.state.remember(&ctx.session_id, key);
                }
                Decision::Allow
            }
            Ok(Ok(Answer::Always)) => {
                // Cache for the session too: the saved rule is narrow, so a
                // near-miss later in this conversation shouldn't re-prompt after
                // the user already said "always". `PolicyApprover` is what turns
                // this into a persisted grant.
                if let Some(key) = &request.scope_key {
                    self.state.remember(&ctx.session_id, key);
                }
                Decision::AllowAlways
            }
            Ok(Ok(Answer::Deny(feedback))) => Decision::Deny { feedback },
            // The sender was dropped (superseded / cleared).
            Ok(Err(_)) => Decision::deny(),
            Err(_) => {
                self.state.forget_pending(&ctx.session_id);
                let _ = ctx.sink.send("审批超时，已自动拒绝。").await;
                Decision::deny_because("审批超时（5 分钟内无人应答），已自动拒绝")
            }
        }
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
    /// Resolve a pending approval.
    Approve(Answer),
    /// Refuse a pending approval, optionally with a reason relayed to the agent.
    Deny(Option<String>),
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
        return Command::Deny((!rest.is_empty()).then(|| rest.to_string()));
    }

    match lower.as_str() {
        "/new" | "/clear" | "/reset" => Command::New,
        "/approve" | "/yes" | "/y" | "/ok" => Command::Approve(Answer::Once),
        "/approve session" | "/approve all" => Command::Approve(Answer::Session),
        "/approve always" => Command::Approve(Answer::Always),
        "/sethome" | "/home" => Command::SetHome,
        "/wechat" | "/wechat login" | "/weixin" => Command::WechatLogin,
        _ => Command::Plain(text.to_string()),
    }
}

/// Front door for inbound chat messages: classifies control commands and,
/// for plain text, runs the agent turn off the channel's receive loop so the
/// loop can still deliver an `/approve` reply while the turn is suspended.
///
/// Channels build a [`ReplySink`] for the conversation and call
/// [`GatewayDispatcher::handle`]; the dispatcher owns replying (including the
/// turn's eventual answer), so channels no longer send agent replies directly.
pub struct GatewayDispatcher {
    handler: Arc<dyn MessageHandler>,
    approvals: Arc<ApprovalState>,
    /// Pending `ask_user` questions (mirrors `approvals`): a plain inbound
    /// message resolves a pending question instead of starting a new turn.
    clarify: Arc<ClarifyState>,
    sessions: Arc<dyn SessionRepository>,
    home: Arc<dyn HomeRepository>,
    todos: Arc<dyn SessionTodoRepository>,
    /// Set when the WeChat channel is enabled — drives `/wechat login`.
    wechat_login: Option<Arc<dyn WeChatLogin>>,
    /// Backs the `/pair` chat commands (same store the `komo pair` CLI uses).
    pairings: Arc<dyn PairingRepository>,
    /// Durable dedupe for redelivered platform messages (`domain/inbox.rs`).
    inbox: Arc<dyn InboxRepository>,
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
/// Everything about the continuation itself is already built — a wake is
/// `resume_interrupted` on the suspended run, which replays the turn's rounds
/// and re-dispatches the call that stopped at the gate. What this adds is the
/// three things that have to happen around it: record *why* the turn came back,
/// retire the other waits it was holding, and take the session's turn slot so
/// the continuation does not run beside a live turn.
pub struct TurnWaker {
    dispatcher: Arc<GatewayDispatcher>,
    handler: Arc<dyn MessageHandler>,
    runs: Arc<dyn RunRepository>,
    events: Arc<dyn SessionEventRepository>,
    wakeups: Arc<dyn WakeupRepository>,
}

impl TurnWaker {
    pub fn new(
        dispatcher: Arc<GatewayDispatcher>,
        handler: Arc<dyn MessageHandler>,
        runs: Arc<dyn RunRepository>,
        events: Arc<dyn SessionEventRepository>,
        wakeups: Arc<dyn WakeupRepository>,
    ) -> Self {
        Self {
            dispatcher,
            handler,
            runs,
            events,
            wakeups,
        }
    }
}

#[async_trait]
impl WakeupDispatch for TurnWaker {
    async fn fire(
        &self,
        registration: &WakeupRegistration,
        cause: WakeupCause,
    ) -> anyhow::Result<()> {
        let Some(turn_id) = registration.turn_id.clone() else {
            // A wake that *starts* a turn is a trigger, not a continuation —
            // nothing writes one yet (docs/bot-runtime.md §3.3).
            warn!(id = %registration.id, "a wakeup with no turn to continue is not dispatchable yet");
            return Ok(());
        };
        let Some(run) = self.runs.get(&turn_id).await? else {
            anyhow::bail!("no run `{turn_id}` to continue");
        };

        // Why it came back, durably, before it comes back: without this the log
        // cannot answer "what ended the wait", and a crash mid-continuation
        // would read as a turn that woke for no reason.
        self.events
            .append(
                &registration.session_id,
                vec![SessionEventKind::WakeupFired(WakeupFiredEvent {
                    turn_id: turn_id.clone(),
                    wakeup_id: registration.id.clone(),
                    cause,
                    payload: String::new(),
                })],
            )
            .await?;
        self.events.durable_flush(&registration.session_id).await?;

        // Everything else this turn was waiting on goes with it: a turn woken
        // by an approval must not be woken again by the timer that was watching
        // the same wait.
        if let Err(error) = self
            .wakeups
            .take_for_turn(&registration.session_id, &turn_id)
            .await
        {
            warn!(%error, turn = %turn_id, "failed to retire a woken turn's other waits");
        }

        // The continuation is a turn like any other: it queues behind whatever
        // the session is already doing. Spawned so the sweep's tick is not held
        // for however long the turn takes — the wake itself is already durable.
        let dispatcher = self.dispatcher.clone();
        let handler = self.handler.clone();
        let session_id = registration.session_id.clone();
        tokio::spawn(async move {
            let claim = dispatcher.claim_session(&session_id).await;
            let ctx = SessionContext::detached(&session_id);
            let outcome = with_session(ctx, handler.resume_interrupted(&run)).await;
            claim.release();
            match outcome {
                Ok(Some(_)) => {
                    info!(turn = %run.id, cause = cause.as_str(), "continued a woken turn")
                }
                // The continuation declined — the transcript already ends in a
                // reply, or the log has nothing for the turn. Not an error, and
                // not something to retry: the wait is gone and the turn is
                // whatever the log says it is.
                Ok(None) => warn!(turn = %run.id, "a woken turn was not continuable"),
                Err(error) => warn!(%error, turn = %run.id, "a woken turn failed"),
            }
        });
        Ok(())
    }
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
        clarify: Arc<ClarifyState>,
        sessions: Arc<dyn SessionRepository>,
        home: Arc<dyn HomeRepository>,
        todos: Arc<dyn SessionTodoRepository>,
        wechat_login: Option<Arc<dyn WeChatLogin>>,
        pairings: Arc<dyn PairingRepository>,
        inbox: Arc<dyn InboxRepository>,
    ) -> Self {
        Self {
            handler,
            approvals,
            clarify,
            sessions,
            home,
            todos,
            wechat_login,
            pairings,
            inbox,
            inflight: Mutex::new(HashMap::new()),
            idle: Notify::new(),
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
        peer: &ChannelPeer,
        origin: InboundOrigin,
        text: String,
        sink: Arc<dyn ReplySink>,
    ) {
        let session_id = match self.session_for(peer).await {
            Ok(id) => id,
            Err(error) => {
                // Without a session there is nowhere to put the turn, so say so
                // rather than drop the message silently.
                warn!(%error, platform = %peer.platform, "could not open a session for this chat");
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
        self.dispatch(session_id, peer, text, sink).await;
        if let Err(error) = self.inbox.complete(&origin).await {
            // The row stays `claimed`. Harmless today (the key already blocks a
            // redelivery); it becomes the signal for crash re-delivery later.
            warn!(%error, "inbox complete failed (non-fatal)");
        }
    }

    /// The session that answers `peer`, opening one the first time that
    /// correspondent writes.
    ///
    /// A conversation's identity is its session id and nothing else, so the map
    /// from an address to that id is **stored**, not computed. It used to be
    /// computed — the session id *was* `feishu:{chat_id}` — which meant a
    /// conversation could not exist without an address, an address could not
    /// change, and anything able to name a session id could name a channel.
    async fn session_for(&self, peer: &ChannelPeer) -> anyhow::Result<String> {
        if let Some(existing) = self.sessions.find_by_peer(peer).await? {
            return Ok(existing.id);
        }
        let session = Session::new(uuid::Uuid::now_v7().to_string()).with_channel(peer.clone());
        self.sessions.save(&session).await?;
        info!(
            platform = %peer.platform,
            session = %session.id,
            "opened a session for a new chat"
        );
        Ok(session.id)
    }

    /// Route one already-deduped message.
    async fn dispatch(
        self: &Arc<Self>,
        session_id: &str,
        peer: &ChannelPeer,
        text: String,
        sink: Arc<dyn ReplySink>,
    ) {
        match classify(&text) {
            Command::Approve(answer) => {
                let asked = answer.clone();
                let granted = self.approvals.resolve_scoped(session_id, answer);
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
            Command::Deny(reason) => {
                let explained = reason.is_some();
                let reply = if self.approvals.resolve(session_id, Answer::Deny(reason)) {
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
            Command::New => {
                self.approvals.clear(session_id);
                // A pending clarify question belongs to the old conversation;
                // its waiter reads the dropped sender as "no answer".
                self.clarify.clear(session_id);
                // The working todo list is session-scoped; a fresh conversation
                // starts with an empty one. (The session id is reused across the
                // rotate, so the row must be cleared explicitly.)
                if let Err(error) = self.todos.clear(session_id).await {
                    warn!(%error, "failed to clear session todos (non-fatal)");
                }
                // Rotate (hermes-style): archive the old transcript, leave the
                // chat's session empty for a fresh conversation.
                match self.sessions.rotate(session_id).await {
                    Ok(archived) => {
                        info!(session = %session_id, ?archived, "session rotated via /new")
                    }
                    Err(error) => warn!(%error, "session rotate failed (non-fatal)"),
                }
                let _ = sink.send("已开始新会话，之前的上下文已归档。").await;
            }
            Command::SetHome => {
                // The *address*, not the session: proactive output is delivered
                // to a correspondent, and a session id names no channel.
                let reply = match self.home.set(&peer.address()).await {
                    Ok(()) => {
                        info!(home = %peer.address(), "home channel set via /sethome");
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
                if self.clarify.resolve(session_id, &input) {
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
            // Fresh clarify budget for this turn (and drop any stale question).
            this.clarify.begin_turn(&session);
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
        // "approved for session" set stays until `/new`).
        self.approvals.forget_pending(session);
        self.approvals.release_gate(session);
        // Same for a clarify question the turn never resolved (+ its budget).
        self.clarify.clear(session);
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
            self.dispatcher.clarify.clear(&self.session);
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

    #[test]
    fn classify_matches_commands_case_insensitively() {
        assert_eq!(classify("/new"), Command::New);
        assert_eq!(classify("  /CLEAR "), Command::New);
        assert_eq!(classify("/approve"), Command::Approve(Answer::Once));
        assert_eq!(
            classify("/approve session"),
            Command::Approve(Answer::Session)
        );
        assert_eq!(classify("/deny"), Command::Deny(None));
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
            Command::Deny(Some("用 trash 代替 rm".to_string()))
        );
        // The verb is case-insensitive; the reason keeps its case.
        assert_eq!(
            classify("/DENY Use Trash"),
            Command::Deny(Some("Use Trash".to_string()))
        );
        // Whitespace-only argument is the same as a bare `/deny`.
        assert_eq!(classify("/deny    "), Command::Deny(None));
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

    #[tokio::test]
    async fn register_then_resolve_delivers_the_decision() {
        let state = ApprovalState::new();
        let rx = state.register("s1", sample_pending());
        // The structured prompt is visible to the interactions poll while pending.
        assert_eq!(
            state.pending_info("s1").map(|p| p.summary),
            Some("run shell command: ls".to_string())
        );
        assert!(state.resolve("s1", Answer::Session));
        assert_eq!(rx.await.unwrap(), Answer::Session);
        // Cleared once resolved.
        assert!(state.pending_info("s1").is_none());
    }

    #[tokio::test]
    async fn clear_cancels_a_pending_wait() {
        let state = ApprovalState::new();
        let rx = state.register("s1", sample_pending());
        state.clear("s1");
        // Sender dropped → receiver errors → treated as denial.
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn session_approval_cache_remembers_scope_keys() {
        let state = ApprovalState::new();
        assert!(!state.is_session_approved("s1", "file:write"));
        state.remember("s1", "file:write");
        assert!(state.is_session_approved("s1", "file:write"));
        // Scoped per session.
        assert!(!state.is_session_approved("s2", "file:write"));
        state.clear("s1");
        assert!(!state.is_session_approved("s1", "file:write"));
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
        async fn rotate(&self, _session_id: &str) -> anyhow::Result<Option<String>> {
            unimplemented!()
        }
    }

    struct UnusedHome;
    #[async_trait]
    impl HomeRepository for UnusedHome {
        async fn get(&self) -> anyhow::Result<Option<String>> {
            unimplemented!()
        }
        async fn set(&self, _session_id: &str) -> anyhow::Result<()> {
            unimplemented!()
        }
    }

    struct UnusedTodos;
    #[async_trait]
    impl SessionTodoRepository for UnusedTodos {
        async fn get(&self, _session_id: &str) -> anyhow::Result<Vec<TodoItem>> {
            unimplemented!()
        }
        async fn set(&self, _session_id: &str, _items: &[TodoItem]) -> anyhow::Result<()> {
            unimplemented!()
        }
        async fn clear(&self, _session_id: &str) -> anyhow::Result<()> {
            unimplemented!()
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
        dispatcher_with_clarify(handler, Arc::new(ClarifyState::new()))
    }

    fn dispatcher_with_clarify(
        handler: Arc<GateHandler>,
        clarify: Arc<ClarifyState>,
    ) -> Arc<GatewayDispatcher> {
        dispatcher_with_parts(handler, clarify, Arc::new(AlwaysFreshInbox))
    }

    fn dispatcher_with_parts(
        handler: Arc<GateHandler>,
        clarify: Arc<ClarifyState>,
        inbox: Arc<dyn InboxRepository>,
    ) -> Arc<GatewayDispatcher> {
        dispatcher_with_sessions(handler, clarify, inbox, Arc::new(MemorySessions::default()))
    }

    fn dispatcher_with_sessions(
        handler: Arc<GateHandler>,
        clarify: Arc<ClarifyState>,
        inbox: Arc<dyn InboxRepository>,
        sessions: Arc<dyn SessionRepository>,
    ) -> Arc<GatewayDispatcher> {
        Arc::new(GatewayDispatcher::new(
            handler,
            Arc::new(ApprovalState::new()),
            clarify,
            sessions,
            Arc::new(UnusedHome),
            Arc::new(UnusedTodos),
            None,
            Arc::new(UnusedPairings),
            inbox,
        ))
    }

    /// The correspondent the dispatcher tests speak as.
    fn peer() -> ChannelPeer {
        ChannelPeer::new("telegram", "1")
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
            Arc::new(ClarifyState::new()),
            Arc::new(AlwaysFreshInbox),
            sessions.clone(),
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        let alice = ChannelPeer::new("feishu", "oc_alice");
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
                &ChannelPeer::new("feishu", "oc_bob"),
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
        let stored = sessions.find_by_peer(&alice).await.unwrap().unwrap();
        assert_eq!(stored.channel.as_ref(), Some(&alice));
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

    /// The waker's own three jobs: record *why* the turn came back, retire the
    /// other waits it was holding, and hand the run to whoever continues it.
    #[tokio::test]
    async fn waking_a_turn_records_the_cause_and_retires_its_other_waits() {
        use komo_core::domain::run::Run;
        use komo_core::domain::session_event::Wakeup;

        /// Records the run it was asked to continue.
        struct RecordingResume(Mutex<Vec<String>>);

        #[async_trait]
        impl MessageHandler for RecordingResume {
            async fn handle(&self, _session_id: &str, input: String) -> anyhow::Result<String> {
                Ok(input)
            }
            async fn resume_interrupted(&self, run: &Run) -> anyhow::Result<Option<String>> {
                self.0.lock().unwrap().push(run.id.clone());
                Ok(Some("continued".to_string()))
            }
        }

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
        let (entered_tx, _entered_rx) = mpsc::unbounded_channel();
        let dispatcher = dispatcher_with(Arc::new(GateHandler {
            entered: entered_tx,
            permits: Arc::new(Semaphore::new(0)),
        }));
        let waker = TurnWaker::new(
            dispatcher,
            handler.clone(),
            db.clone(),
            db.clone(),
            db.clone(),
        );

        waker
            .fire(&approval, WakeupCause::Approve)
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

    // A plain message answers a pending clarify question instead of starting a
    // new turn; once nothing is pending, plain messages dispatch normally.
    #[tokio::test]
    async fn plain_message_resolves_pending_clarify_not_a_new_turn() {
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let permits = Arc::new(Semaphore::new(0));
        let handler = Arc::new(GateHandler {
            entered: entered_tx,
            permits: permits.clone(),
        });
        let clarify = Arc::new(ClarifyState::new());
        let dispatcher = dispatcher_with_sessions(
            handler,
            clarify.clone(),
            Arc::new(AlwaysFreshInbox),
            MemorySessions::seeded("s1", &peer()),
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        // A turn is suspended on a question.
        let rx = clarify.register("s1", "什么颜色？");
        dispatcher
            .handle(
                &peer(),
                InboundOrigin::local(),
                "蓝色的".into(),
                sink.clone(),
            )
            .await;
        assert_eq!(rx.await.unwrap(), "蓝色的", "message became the answer");
        assert!(
            entered_rx.try_recv().is_err(),
            "the answer must not start a new turn"
        );

        // With nothing pending, the next message dispatches a turn as usual.
        dispatcher
            .handle(&peer(), InboundOrigin::local(), "next".into(), sink.clone())
            .await;
        assert_eq!(next_entered(&mut entered_rx).await, "next");
        permits.add_permits(1);
    }

    // Control commands keep priority over a pending clarify: `/deny` resolves
    // the approval path and is never eaten as the question's answer.
    #[tokio::test]
    async fn commands_keep_priority_over_pending_clarify() {
        let (entered_tx, _entered_rx) = mpsc::unbounded_channel();
        let handler = Arc::new(GateHandler {
            entered: entered_tx,
            permits: Arc::new(Semaphore::new(0)),
        });
        let clarify = Arc::new(ClarifyState::new());
        let dispatcher = dispatcher_with_clarify(handler, clarify.clone());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingSink { sent }) as Arc<dyn ReplySink>;

        let _rx = clarify.register("s1", "问题？");
        dispatcher
            .handle(
                &peer(),
                InboundOrigin::local(),
                "/deny".into(),
                sink.clone(),
            )
            .await;
        assert!(
            clarify.has_pending("s1"),
            "/deny must not consume the clarify question"
        );
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
        let rx = state.register("s1", dangerous);
        assert_eq!(
            state.resolve_scoped("s1", Answer::Session),
            Some(Answer::Once),
            "a session-wide grant must narrow to this one call"
        );
        assert_eq!(
            rx.await.unwrap(),
            Answer::Once,
            "the waiter sees the narrowing"
        );

        // `always` would have written a persisted rule; it narrows the same way.
        let rx = state.register(
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
        assert_eq!(rx.await.unwrap(), Answer::Once);

        // A normal action is untouched — that is what the scopes are for.
        let rx = state.register(
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
        assert_eq!(rx.await.unwrap(), Answer::Session);
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
            Arc::new(ClarifyState::new()),
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
            Arc::new(ClarifyState::new()),
            Arc::new(AlwaysFreshInbox),
            MemorySessions::seeded("s7", &peer()),
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
}
