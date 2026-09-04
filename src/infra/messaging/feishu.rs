//! Feishu (Lark) ingress channel.
//!
//! Receives `im.message.receive_v1` events over Feishu's WebSocket long
//! connection (no public callback URL needed — right for a laptop process),
//! routes each text message through the `MessageHandler` as one session turn,
//! and replies via the IM REST API with a plain reqwest call.
//!
//! openlark is used only for the long connection (the frames are
//! protobuf-encoded, which it handles); event payloads are consumed raw with
//! our own tolerant serde structs (the SDK's typed events once dropped whole
//! messages over null sender fields), and replies and token fetching go
//! through reqwest directly so the SDK surface we depend on stays minimal.
//!
//! Access control follows hermes-agent's feishu adapter: an `allow_from`
//! open_id allowlist (empty = open), a `require_mention` gate for group
//! chats (DMs always bypass), and an optional `home_chat` that receives
//! proactive output (reminders) via the shared `HomeNotifier`. The mention
//! gate matches the bot's *own* open_id, resolved once at startup: a group
//! message that @s somebody else is not addressed to the agent.

use komo_bot::gateway::Channel;
use komo_bot::interaction::GatewayDispatcher;
use komo_bot::pairing::{PairingGuard, Principal};
use komo_core::domain::inbox::InboundOrigin;
use komo_core::domain::session::{ChannelPeer, InboundPeer};
use komo_core::domain::trigger::{ExternalEvent, FeishuEvent};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
// The `openlark` package keeps `open_lark` as its lib name for compatibility.
use open_lark::{
    Config as LarkConfig,
    ws_client::{EventDispatcherHandler, EventHandler, LarkWsClient, WsClientError},
};
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc, watch};
use tracing::{error, info, warn};

use crate::{
    domain::{gateway::ReplySink, pairing::PairingRepository},
    infra::messaging::reconnect_backoff,
};
use komo_config::FeishuConfig;

const FEISHU_BASE_URL: &str = "https://open.feishu.cn";
/// Refresh the tenant token this long before Feishu's reported expiry.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Outbound side of the integration: tenant token cache + message send.
/// Shared by the ingress channel (replies) and the `HomeNotifier` (proactive
/// messages to the home chat).
pub struct FeishuSender {
    app_id: String,
    app_secret: String,
    http: reqwest::Client,
    token: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    value: String,
    expires_at: Instant,
}

impl FeishuSender {
    pub fn new(app_id: String, app_secret: String) -> Self {
        Self {
            app_id,
            app_secret,
            http: reqwest::Client::new(),
            token: Mutex::new(None),
        }
    }

    /// Fetch (or reuse) the tenant access token for REST calls.
    async fn tenant_access_token(&self) -> anyhow::Result<String> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached.as_ref()
            && Instant::now() < token.expires_at
        {
            return Ok(token.value.clone());
        }

        #[derive(Deserialize)]
        struct TokenResponse {
            code: i64,
            #[serde(default)]
            msg: String,
            #[serde(default)]
            tenant_access_token: String,
            #[serde(default)]
            expire: u64,
        }

        let response: TokenResponse = self
            .http
            .post(format!(
                "{FEISHU_BASE_URL}/open-apis/auth/v3/tenant_access_token/internal"
            ))
            .json(&serde_json::json!({
                "app_id": self.app_id,
                "app_secret": self.app_secret,
            }))
            .send()
            .await?
            .json()
            .await?;
        if response.code != 0 {
            anyhow::bail!(
                "feishu token request failed: code {} ({})",
                response.code,
                response.msg
            );
        }

        let ttl = Duration::from_secs(response.expire).saturating_sub(TOKEN_REFRESH_MARGIN);
        *cached = Some(CachedToken {
            value: response.tenant_access_token.clone(),
            expires_at: Instant::now() + ttl,
        });
        Ok(response.tenant_access_token)
    }

    /// The bot's own open_id, which the group mention gate compares each
    /// mention against. Resolved once at startup, not per message.
    async fn bot_open_id(&self) -> anyhow::Result<String> {
        #[derive(Deserialize)]
        struct InfoResponse {
            code: i64,
            #[serde(default)]
            msg: String,
            #[serde(default)]
            bot: BotInfo,
        }

        #[derive(Deserialize, Default)]
        struct BotInfo {
            #[serde(default)]
            open_id: String,
        }

        let token = self.tenant_access_token().await?;
        let response: InfoResponse = self
            .http
            .get(format!("{FEISHU_BASE_URL}/open-apis/bot/v3/info"))
            .bearer_auth(token)
            .send()
            .await?
            .json()
            .await?;
        if response.code != 0 {
            anyhow::bail!(
                "feishu bot info request failed: code {} ({})",
                response.code,
                response.msg
            );
        }
        if response.bot.open_id.is_empty() {
            anyhow::bail!("feishu bot info returned no open_id");
        }
        Ok(response.bot.open_id)
    }

    /// Which chat a message belongs to.
    ///
    /// Needed because `im.message.reaction.created_v1` carries the message id
    /// and no chat id, while a `Trigger::Feishu` is written about a chat —
    /// matching a reaction against a routine is impossible without this. Asked
    /// only when a reaction-watching routine actually exists, so a busy group
    /// with no such routine costs nothing.
    async fn message_chat_id(&self, message_id: &str) -> anyhow::Result<String> {
        #[derive(Deserialize)]
        struct MessageResponse {
            code: i64,
            #[serde(default)]
            msg: String,
            #[serde(default)]
            data: MessageData,
        }

        #[derive(Deserialize, Default)]
        struct MessageData {
            #[serde(default)]
            items: Vec<MessageItem>,
        }

        #[derive(Deserialize)]
        struct MessageItem {
            #[serde(default)]
            chat_id: String,
        }

        let token = self.tenant_access_token().await?;
        let response: MessageResponse = self
            .http
            .get(format!(
                "{FEISHU_BASE_URL}/open-apis/im/v1/messages/{message_id}"
            ))
            .bearer_auth(token)
            .send()
            .await?
            .json()
            .await?;
        if response.code != 0 {
            anyhow::bail!(
                "feishu message lookup failed: code {} ({})",
                response.code,
                response.msg
            );
        }
        response
            .data
            .items
            .into_iter()
            .map(|item| item.chat_id)
            .find(|chat| !chat.is_empty())
            .ok_or_else(|| anyhow::anyhow!("feishu message {message_id} named no chat"))
    }

    /// Send a plain text message into a chat (works for both p2p and group).
    pub async fn send_text(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        #[derive(Deserialize)]
        struct ApiResponse {
            code: i64,
            #[serde(default)]
            msg: String,
        }

        let token = self.tenant_access_token().await?;
        let response: ApiResponse = self
            .http
            .post(format!("{FEISHU_BASE_URL}/open-apis/im/v1/messages"))
            .query(&[("receive_id_type", "chat_id")])
            .bearer_auth(token)
            .json(&serde_json::json!({
                "receive_id": chat_id,
                "msg_type": "text",
                "content": serde_json::json!({ "text": text }).to_string(),
            }))
            .send()
            .await?
            .json()
            .await?;
        if response.code != 0 {
            anyhow::bail!(
                "feishu send failed: code {} ({})",
                response.code,
                response.msg
            );
        }
        Ok(())
    }
}

/// Sends a turn's output (and any mid-turn approval prompts) back to one chat.
struct FeishuReplySink {
    sender: Arc<FeishuSender>,
    chat_id: String,
}

#[async_trait]
impl ReplySink for FeishuReplySink {
    async fn send(&self, text: &str) -> anyhow::Result<()> {
        self.sender.send_text(&self.chat_id, text).await
    }
}

/// Which inbound messages the agent handles. Sender identity (allowlist /
/// pairing) is the `PairingGuard`'s job, checked in the async consumer.
#[derive(Clone, Default)]
struct AdmitPolicy {
    /// Group messages must @mention the bot itself (DMs always pass).
    require_mention: bool,
    /// The bot's own open_id, resolved once at serve time. Empty means the
    /// mention gate cannot match, which fails closed.
    bot_open_id: String,
}

pub struct FeishuChannel {
    sender: Arc<FeishuSender>,
    policy: AdmitPolicy,
    guard: PairingGuard,
}

/// One thing that arrived on the long connection.
///
/// A reaction is here for one reason: a `Trigger::Feishu { Reaction }` routine
/// (docs/bot-runtime.md §5.13). It is never chat input — nobody is talking to
/// komo by tapping an emoji — so it has no `Inbound` and never reaches the
/// dispatcher's message path.
enum Arrival {
    Message(Inbound),
    Reaction {
        message_id: String,
        sender_id: String,
        emoji: String,
    },
}

/// One inbound text message, reduced to what the agent needs.
struct Inbound {
    message_id: String,
    sender_id: String,
    chat_id: String,
    /// A `p2p` chat. Feishu's own word for it, carried through because which
    /// conversation a message belongs to depends on it (docs/bot-runtime.md
    /// §3.8): the operator's DM is their home conversation, a group is not.
    private: bool,
    text: String,
    /// The message @s the bot itself.
    mentions_bot: bool,
    /// Whether the *chat* path takes it: a group message must address the bot
    /// when `require_mention` is on.
    ///
    /// Kept as a flag instead of dropping the message, because a routine
    /// trigger is not addressed to the bot and never had to be — a keyword in a
    /// group nobody @s is exactly the case §5.13 exists for. The routine path
    /// sees every message; only the conversation is gated.
    admitted: bool,
}

impl FeishuChannel {
    pub fn new(
        sender: Arc<FeishuSender>,
        config: &FeishuConfig,
        pairings: Arc<dyn PairingRepository>,
    ) -> Self {
        Self {
            sender,
            policy: AdmitPolicy {
                require_mention: config.require_mention,
                bot_open_id: String::new(),
            },
            guard: PairingGuard::new("feishu", config.allow_from.clone(), pairings),
        }
    }

    /// One reaction, on its way to a `Trigger::Feishu { Reaction }` routine.
    ///
    /// The chat is looked up rather than read off the event, which does not
    /// carry one — and only when a routine is actually watching for a reaction,
    /// because every emoji in every chat the bot can see arrives here and the
    /// lookup is an API call. A gateway with no such routine spends nothing.
    ///
    /// Spawned for the same reason a message's triggers are: the consumer loop
    /// has to keep consuming while a routine's turn runs.
    fn on_reaction(
        &self,
        dispatcher: &Arc<GatewayDispatcher>,
        message_id: String,
        sender_id: String,
        emoji: String,
    ) {
        let dispatcher = dispatcher.clone();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            if !dispatcher.wants_feishu_reactions().await {
                return;
            }
            let chat = match sender.message_chat_id(&message_id).await {
                Ok(chat) => chat,
                Err(error) => {
                    warn!(%error, message = %message_id, "could not resolve a reaction's chat");
                    return;
                }
            };
            let fired = dispatcher
                .on_external_event(&ExternalEvent::Feishu(FeishuEvent {
                    chat,
                    sender: sender_id,
                    text: String::new(),
                    mention: false,
                    reaction: Some(emoji.clone()),
                }))
                .await;
            if fired.routines > 0 {
                info!(
                    %emoji,
                    routines = fired.routines,
                    "a reaction fired routines"
                );
            }
        });
    }
}

#[async_trait]
impl Channel for FeishuChannel {
    fn name(&self) -> &str {
        "feishu"
    }

    async fn serve(
        &self,
        dispatcher: Arc<GatewayDispatcher>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        // The mention gate matches against the bot's own open_id; without it a
        // mention of anyone would admit the message. Keep retrying until it
        // resolves — the ws connection needs the same network anyway, so
        // waiting here delays nothing that could otherwise proceed. Nothing
        // else needs the id, so a gate that is off skips the call entirely.
        let mut policy = self.policy.clone();
        if policy.require_mention {
            let mut backoff = 0usize;
            policy.bot_open_id = loop {
                tokio::select! {
                    _ = shutdown.changed() => return Ok(()),
                    result = self.sender.bot_open_id() => match result {
                        Ok(open_id) => break open_id,
                        Err(error) => {
                            warn!(%error, "feishu bot info fetch failed; retrying");
                            tokio::select! {
                                _ = shutdown.changed() => return Ok(()),
                                _ = tokio::time::sleep(reconnect_backoff(backoff)) => {}
                            }
                            backoff += 1;
                        }
                    }
                }
            };
            info!(bot = %policy.bot_open_id, "feishu bot identified");
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<Arrival>();

        // The long connection runs on its own thread with a single-threaded
        // runtime, keeping its heartbeat/reconnect loop isolated from the
        // main runtime. Events cross back over the mpsc channel.
        let ws_thread = spawn_ws_thread(
            self.sender.app_id.clone(),
            self.sender.app_secret.clone(),
            policy,
            tx,
            shutdown.clone(),
        );

        // Consumer: one arrival at a time, in order. The chat id keys the
        // session, so a p2p chat is one continuous conversation.
        let consume = async {
            while let Some(arrival) = rx.recv().await {
                let msg = match arrival {
                    Arrival::Message(msg) => msg,
                    Arrival::Reaction {
                        message_id,
                        sender_id,
                        emoji,
                    } => {
                        self.on_reaction(&dispatcher, message_id, sender_id, emoji);
                        continue;
                    }
                };
                // Routine triggers first, and unconditionally: a routine is set
                // off by *what was said in a chat*, not by who said it or by
                // whether they addressed the bot (docs/bot-runtime.md §8,
                // criterion 6). It opens its own turn on the routine's grants
                // and never touches this conversation.
                //
                // Spawned, because a routine turn is an agent turn: awaiting it
                // here would stop this loop consuming — including the
                // `/approve` somebody is typing — for as long as it runs.
                let triggered = ExternalEvent::Feishu(FeishuEvent {
                    chat: msg.chat_id.clone(),
                    sender: msg.sender_id.clone(),
                    text: msg.text.clone(),
                    mention: msg.mentions_bot,
                    reaction: None,
                });
                let routines = dispatcher.clone();
                tokio::spawn(async move { routines.on_external_event(&triggered).await });
                // Everything below is the *conversation*, which keeps every
                // gate it had: a group message that does not address the bot is
                // not talking to it, and an unknown sender pairs first.
                if !msg.admitted {
                    continue;
                }
                // Pairing gate: unknown senders get a pairing code instead of
                // the agent until `komo pair approve` runs on the host.
                let sender = self.sender.clone();
                let chat = msg.chat_id.clone();
                let Some(principal) = self
                    .guard
                    .admit(&msg.sender_id, &msg.chat_id, move |reply| async move {
                        sender.send_text(&chat, &reply).await
                    })
                    .await
                else {
                    continue;
                };

                info!(chat = %msg.chat_id, "feishu message received");
                let sink: Arc<dyn ReplySink> = Arc::new(FeishuReplySink {
                    sender: self.sender.clone(),
                    chat_id: msg.chat_id.clone(),
                });
                // Returns promptly: a turn runs on its own task so this loop can
                // keep consuming and deliver the user's `/approve` reply.
                // Feishu retries a message it believes was not acked, and the
                // gateway restarts. Both are the inbox's job now — the old
                // in-process `seen` set survived neither.
                let origin = InboundOrigin::new("feishu", msg.message_id.clone());
                dispatcher
                    .handle(
                        &InboundPeer::new(
                            ChannelPeer::new("feishu", &msg.chat_id),
                            msg.private,
                            principal == Principal::Operator,
                        ),
                        origin,
                        msg.text,
                        sink,
                    )
                    .await;
            }
        };

        tokio::select! {
            _ = shutdown.changed() => info!("feishu channel stopped"),
            _ = consume => {}
        }
        let _ = tokio::task::spawn_blocking(move || ws_thread.join()).await;
        Ok(())
    }
}

/// Run the reconnect loop on a dedicated thread. Holds the only sender, so
/// the consumer's `recv` ends when this thread exits.
fn spawn_ws_thread(
    app_id: String,
    app_secret: String,
    policy: AdmitPolicy,
    events: mpsc::UnboundedSender<Arrival>,
    mut shutdown: watch::Receiver<bool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(error) => {
                error!(%error, "failed to build feishu ws runtime");
                return;
            }
        };
        let ws_config = Arc::new(
            LarkConfig::builder()
                .app_id(app_id)
                .app_secret(app_secret)
                .build(),
        );
        runtime.block_on(async move {
            // Every event is acked by the session regardless of registration,
            // so subscribing to the two events we consume is enough — read
            // receipts and the rest are acked without a handler. Reactions are
            // here for routine triggers only (docs/bot-runtime.md §5.13); a
            // gateway with no reaction routine drops them a step later, having
            // spent one parse.
            let dispatcher = match EventDispatcherHandler::builder()
                .register_raw(
                    "im.message.receive_v1",
                    ReceiveHandler {
                        policy,
                        events: events.clone(),
                    },
                )
                .and_then(|builder| {
                    builder.register_raw(
                        "im.message.reaction.created_v1",
                        ReactionHandler { events },
                    )
                }) {
                Ok(builder) => builder.build(),
                Err(error) => {
                    error!(%error, "failed to register feishu event handler");
                    return;
                }
            };
            let mut backoff = 0usize;
            loop {
                if *shutdown.borrow() {
                    break;
                }
                let started = std::time::Instant::now();
                let connected = tokio::select! {
                    _ = shutdown.changed() => break,
                    result = LarkWsClient::open(ws_config.clone(), dispatcher.clone()) => match result {
                        // A session that terminates normally surfaces as
                        // `ConnectionClosed`, not `Ok`.
                        Ok(()) | Err(WsClientError::ConnectionClosed { .. }) => {
                            warn!("feishu connection closed; reconnecting");
                            true
                        }
                        Err(error) => {
                            warn!(%error, "feishu connection failed; reconnecting");
                            false
                        }
                    }
                };
                // A connection that was actually established AND stayed up a
                // while was healthy: reset the backoff so a later blip starts
                // from the short delay. Err never resets — a failed open that
                // merely took a long time (slow TLS/DNS/connect timeouts) is
                // still a failure, so a persistent outage keeps escalating.
                // The elapsed floor guards the Ok side against a server that
                // accepts and immediately drops us in a tight loop.
                if connected && started.elapsed() >= reconnect_backoff(0) {
                    backoff = 0;
                }
                tokio::select! {
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep(reconnect_backoff(backoff)) => {}
                }
                backoff += 1;
            }
        });
    })
}

/// Forwards admitted `im.message.receive_v1` payloads to the consumer task.
/// Parses the raw payload with our own tolerant structs — sender fields
/// arrive as null for bot mentions or without contact scope, and a strict
/// schema would drop the whole message.
struct ReceiveHandler {
    policy: AdmitPolicy,
    events: mpsc::UnboundedSender<Arrival>,
}

impl EventHandler for ReceiveHandler {
    fn handle(&self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match serde_json::from_slice::<ReceiveEvent>(payload) {
            Ok(event) => {
                if let Some(msg) = admit(event, &self.policy) {
                    let _ = self.events.send(Arrival::Message(msg));
                }
            }
            // Ack anyway: redelivery cannot fix a payload we cannot parse.
            Err(error) => warn!(%error, "feishu event payload failed to parse"),
        }
        Ok(())
    }
}

/// The `im.message.receive_v1` payload, reduced to the fields `admit` reads.
#[derive(Deserialize)]
struct ReceiveEvent {
    event: ReceiveBody,
}

#[derive(Deserialize)]
struct ReceiveBody {
    sender: EventSender,
    message: EventMessage,
}

#[derive(Deserialize)]
struct EventSender {
    #[serde(default)]
    sender_id: Option<SenderId>,
    #[serde(default)]
    sender_type: String,
}

#[derive(Deserialize)]
struct SenderId {
    #[serde(default)]
    open_id: Option<String>,
}

#[derive(Deserialize)]
struct EventMessage {
    message_id: String,
    chat_id: String,
    #[serde(default)]
    chat_type: String,
    #[serde(default)]
    message_type: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    mentions: Option<Vec<EventMention>>,
}

/// One `@` in the message text. `id` is absent for a mention Feishu cannot
/// resolve for this app, which can never be the bot itself.
#[derive(Deserialize)]
struct EventMention {
    #[serde(default)]
    id: Option<SenderId>,
}

/// Forwards `im.message.reaction.created_v1` — the ingress a
/// `Trigger::Feishu { Reaction }` routine listens on (docs/bot-runtime.md
/// §5.13). Reactions are not chat input, so they take no policy: whether one
/// means anything is entirely up to whether a routine is watching for it.
struct ReactionHandler {
    events: mpsc::UnboundedSender<Arrival>,
}

impl EventHandler for ReactionHandler {
    fn handle(&self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match serde_json::from_slice::<ReactionEvent>(payload) {
            Ok(event) => {
                if let Some(arrival) = reaction(event) {
                    let _ = self.events.send(arrival);
                }
            }
            Err(error) => warn!(%error, "feishu reaction payload failed to parse"),
        }
        Ok(())
    }
}

/// The `im.message.reaction.created_v1` payload. It names the message, never
/// the chat — resolving that is `message_chat_id`'s job.
#[derive(Deserialize)]
struct ReactionEvent {
    event: ReactionBody,
}

#[derive(Deserialize)]
struct ReactionBody {
    #[serde(default)]
    message_id: String,
    #[serde(default)]
    reaction_type: ReactionType,
    #[serde(default)]
    operator_type: String,
    #[serde(default)]
    user_id: Option<SenderId>,
}

#[derive(Deserialize, Default)]
struct ReactionType {
    #[serde(default)]
    emoji_type: String,
}

fn reaction(event: ReactionEvent) -> Option<Arrival> {
    let body = event.event;
    // A bot reacting to its own output must not set a routine off.
    if body.operator_type != "user" {
        return None;
    }
    if body.message_id.is_empty() || body.reaction_type.emoji_type.is_empty() {
        return None;
    }
    Some(Arrival::Reaction {
        message_id: body.message_id,
        sender_id: body.user_id.and_then(|id| id.open_id).unwrap_or_default(),
        emoji: body.reaction_type.emoji_type,
    })
}

/// Reduce a raw receive event to an `Inbound`, or `None` when nothing at all
/// can be done with it (non-text, non-user sender, empty after mention strip).
///
/// The group mention rule is recorded on the message (`admitted`) rather than
/// applied here: it decides whether the *conversation* takes the message, and a
/// routine trigger is a separate question with a separate answer.
fn admit(event: ReceiveEvent, policy: &AdmitPolicy) -> Option<Inbound> {
    let sender = event.event.sender;
    if sender.sender_type != "user" {
        return None;
    }
    // Pairing keys on the open_id; a user message without one cannot be
    // admitted or paired.
    let sender_id = sender.sender_id.and_then(|id| id.open_id)?;
    let message = event.event.message;
    if message.message_type != "text" {
        return None;
    }
    // Only a mention of the bot itself addresses the agent: a group message
    // that @s a colleague is delivered to us all the same (the app may hold
    // `im:message` rather than only `im:message.group_at_msg`), and answering
    // it talks over the conversation. An unresolved own open_id fails closed.
    let mentions_bot = !policy.bot_open_id.is_empty()
        && message.mentions.iter().flatten().any(|mention| {
            mention.id.as_ref().and_then(|id| id.open_id.as_deref())
                == Some(policy.bot_open_id.as_str())
        });
    let admitted = message.chat_type != "group" || !policy.require_mention || mentions_bot;
    let text = strip_mentions(&extract_text(&message.content)?);
    if text.is_empty() {
        return None;
    }
    Some(Inbound {
        message_id: message.message_id,
        sender_id,
        private: message.chat_type == "p2p",
        chat_id: message.chat_id,
        text,
        mentions_bot,
        admitted,
    })
}

/// Text message content arrives as `{"text": "..."}`.
fn extract_text(content: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct TextContent {
        text: String,
    }
    serde_json::from_str::<TextContent>(content)
        .ok()
        .map(|c| c.text)
}

/// Group @mentions appear inline as `@_user_N` placeholders; remove them so
/// the agent sees only the actual message.
fn strip_mentions(text: &str) -> String {
    const MENTION: &str = "@_user_";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find(MENTION) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + MENTION.len()..];
        rest = after.trim_start_matches(|c: char| c.is_ascii_digit());
    }
    out.push_str(rest);
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BOT: &str = "ou_bot";

    fn event(overrides: serde_json::Value) -> ReceiveEvent {
        let mut base = json!({
            "schema": "2.0",
            "header": { "event_type": "im.message.receive_v1" },
            "event": {
                "sender": {
                    "sender_id": { "union_id": "un_1", "user_id": "u_1", "open_id": "ou_1" },
                    "sender_type": "user",
                    "tenant_key": "t"
                },
                "message": {
                    "message_id": "om_1",
                    "create_time": "0",
                    "update_time": "0",
                    "chat_id": "oc_1",
                    "chat_type": "p2p",
                    "message_type": "text",
                    "content": "{\"text\":\"hello\"}"
                }
            }
        });
        merge(&mut base, overrides);
        serde_json::from_value(base).expect("event should deserialize")
    }

    fn merge(base: &mut serde_json::Value, patch: serde_json::Value) {
        if let (Some(base_map), serde_json::Value::Object(patch_map)) =
            (base.as_object_mut(), patch)
        {
            for (key, value) in patch_map {
                match base_map.get_mut(&key) {
                    Some(slot) if slot.is_object() && value.is_object() => merge(slot, value),
                    _ => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
    }

    #[test]
    fn extract_text_parses_text_content() {
        assert_eq!(
            extract_text(r#"{"text":"hello"}"#).as_deref(),
            Some("hello")
        );
        assert_eq!(extract_text("not json"), None);
    }

    #[test]
    fn strip_mentions_removes_placeholders() {
        assert_eq!(strip_mentions("@_user_1 现在几点"), "现在几点");
        assert_eq!(strip_mentions("前面 @_user_12 后面"), "前面  后面");
        assert_eq!(strip_mentions("没有提及"), "没有提及");
    }

    #[test]
    fn strip_mentions_keeps_multiline_text() {
        assert_eq!(strip_mentions("@_user_1 第一行\n第二行"), "第一行\n第二行");
    }

    #[test]
    fn admit_extracts_sender_id_for_the_pairing_gate() {
        let msg = admit(event(json!({})), &AdmitPolicy::default()).expect("admitted");
        assert_eq!(msg.chat_id, "oc_1");
        assert_eq!(msg.sender_id, "ou_1");
        assert_eq!(msg.text, "hello");
    }

    #[test]
    fn admit_tolerates_null_sender_fields() {
        // Feishu sends null user_id/union_id/tenant_key for bot mentions or
        // without contact scope; only the open_id is required.
        let msg = admit(
            event(json!({
                "event": { "sender": {
                    "sender_id": { "union_id": null, "user_id": null, "open_id": "ou_1" },
                    "tenant_key": null
                } }
            })),
            &AdmitPolicy::default(),
        )
        .expect("admitted");
        assert_eq!(msg.sender_id, "ou_1");
    }

    fn group_mentioning(open_id: serde_json::Value) -> ReceiveEvent {
        event(json!({
            "event": { "message": {
                "chat_type": "group",
                "content": "{\"text\":\"@_user_1 hi\"}",
                "mentions": [{
                    "key": "@_user_1",
                    "id": { "union_id": "un_b", "user_id": "u_b", "open_id": open_id },
                    "name": "someone",
                    "tenant_key": "t"
                }]
            } }
        }))
    }

    #[test]
    fn admit_requires_mention_in_groups_only() {
        let policy = AdmitPolicy {
            require_mention: true,
            bot_open_id: BOT.to_string(),
        };
        let unmentioned_group = event(json!({
            "event": { "message": { "chat_type": "group" } }
        }));
        // Still parsed — a routine keyword can match it (docs/bot-runtime.md
        // §5.13) — but the *conversation* does not take it.
        let overheard = admit(unmentioned_group, &policy).expect("overheard, not dropped");
        assert!(!overheard.admitted);
        assert!(!overheard.mentions_bot);

        let msg =
            admit(group_mentioning(json!(BOT)), &policy).expect("mentioned group message admitted");
        assert_eq!(msg.text, "hi");
        assert!(msg.admitted);
        assert!(msg.mentions_bot);

        // DMs bypass the mention gate entirely.
        assert!(admit(event(json!({})), &policy).expect("a dm").admitted);
    }

    #[test]
    fn admit_ignores_a_group_mention_of_someone_else() {
        let policy = AdmitPolicy {
            require_mention: true,
            bot_open_id: BOT.to_string(),
        };
        // A colleague @s another colleague: delivered to us, not addressed to
        // us. Answering it talks over the conversation.
        for open_id in [json!("ou_colleague"), json!(null)] {
            let msg = admit(group_mentioning(open_id), &policy).expect("overheard, not dropped");
            assert!(!msg.admitted);
            assert!(!msg.mentions_bot);
        }
    }

    /// A reaction is a routine trigger and nothing else: it never becomes chat
    /// input, and a bot's own reaction is not somebody asking for anything.
    #[test]
    fn a_users_reaction_becomes_an_arrival_and_a_bots_does_not() {
        let payload = |operator: &str| ReactionEvent {
            event: ReactionBody {
                message_id: "om_1".into(),
                reaction_type: ReactionType {
                    emoji_type: "THUMBSUP".into(),
                },
                operator_type: operator.into(),
                user_id: Some(SenderId {
                    open_id: Some("ou_stranger".into()),
                }),
            },
        };
        let Some(Arrival::Reaction {
            message_id,
            sender_id,
            emoji,
        }) = reaction(payload("user"))
        else {
            panic!("a user's reaction arrives");
        };
        assert_eq!(
            (message_id.as_str(), sender_id.as_str(), emoji.as_str()),
            ("om_1", "ou_stranger", "THUMBSUP")
        );
        assert!(reaction(payload("app")).is_none());
    }

    #[test]
    fn admit_skips_non_user_and_non_text() {
        let from_bot = event(json!({
            "event": { "sender": { "sender_type": "app" } }
        }));
        assert!(admit(from_bot, &AdmitPolicy::default()).is_none());

        let image = event(json!({
            "event": { "message": { "message_type": "image", "content": "{}" } }
        }));
        assert!(admit(image, &AdmitPolicy::default()).is_none());
    }
}
