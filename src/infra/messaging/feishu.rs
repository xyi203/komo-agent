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

use komo_agent::gateway::Channel;
use komo_agent::interaction::GatewayDispatcher;
use komo_agent::pairing::{PairingGuard, Principal};
use komo_core::domain::inbox::InboundOrigin;
use komo_core::domain::session::{ChannelPeer, InboundPeer};
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

        let (tx, mut rx) = mpsc::unbounded_channel::<Inbound>();

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

        // Consumer: one message at a time, in arrival order. The chat id keys
        // the session, so a p2p chat is one continuous conversation.
        let consume = async {
            while let Some(msg) = rx.recv().await {
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
    events: mpsc::UnboundedSender<Inbound>,
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
            // so subscribing to the one event we consume is enough — read
            // receipts and the rest are acked without a handler.
            let dispatcher = match EventDispatcherHandler::builder().register_raw(
                "im.message.receive_v1",
                ReceiveHandler { policy, events },
            ) {
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
    events: mpsc::UnboundedSender<Inbound>,
}

impl EventHandler for ReceiveHandler {
    fn handle(&self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match serde_json::from_slice::<ReceiveEvent>(payload) {
            Ok(event) => {
                if let Some(msg) = admit(event, &self.policy) {
                    let _ = self.events.send(msg);
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

/// Reduce a raw receive event to an `Inbound`, or `None` when the agent
/// should ignore it (policy rejection, non-text, non-user sender, empty
/// after mention strip).
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
    if message.chat_type == "group" && policy.require_mention {
        let mentions_bot = !policy.bot_open_id.is_empty()
            && message.mentions.iter().flatten().any(|mention| {
                mention.id.as_ref().and_then(|id| id.open_id.as_deref())
                    == Some(policy.bot_open_id.as_str())
            });
        if !mentions_bot {
            return None;
        }
    }
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
        assert!(admit(unmentioned_group, &policy).is_none());

        let msg =
            admit(group_mentioning(json!(BOT)), &policy).expect("mentioned group message admitted");
        assert_eq!(msg.text, "hi");

        // DMs bypass the mention gate entirely.
        assert!(admit(event(json!({})), &policy).is_some());
    }

    #[test]
    fn admit_ignores_a_group_mention_of_someone_else() {
        let policy = AdmitPolicy {
            require_mention: true,
            bot_open_id: BOT.to_string(),
        };
        // A colleague @s another colleague: delivered to us, not addressed to
        // us. Answering it talks over the conversation.
        assert!(admit(group_mentioning(json!("ou_colleague")), &policy).is_none());
        // A mention Feishu could not resolve is not the bot either.
        assert!(admit(group_mentioning(json!(null)), &policy).is_none());
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
