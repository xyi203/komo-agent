//! Telegram ingress channel.
//!
//! Receives messages via long polling (`getUpdates`) — like the feishu
//! WebSocket connection, no public callback URL is needed, which is right
//! for a laptop process. Each text message routes through the
//! `MessageHandler` as one session turn; replies go out via `sendMessage`.
//!
//! Everything is plain reqwest against the Bot API — no SDK dependency.
//!
//! Outbound replies are sent via `sendRichMessage` (Bot API 10.1) with the
//! agent's CommonMark passed straight through as the rich message's `markdown`
//! field — Telegram renders headings, lists, tables, block quotes, and inline
//! styles natively (up to 32768 chars), so no client-side conversion or
//! chunking is needed.
//!
//! Access control mirrors the feishu adapter: an `allow_from` user-id
//! allowlist (empty = open), a `require_mention` gate for group chats
//! (DMs always bypass), and an optional `home_chat` that receives
//! proactive output (reminders) via the shared `HomeNotifier`.

use komo_bot::gateway::Channel;
use komo_bot::interaction::GatewayDispatcher;
use komo_bot::pairing::{PairingGuard, Principal};
use komo_core::domain::inbox::InboundOrigin;
use komo_core::domain::session::{ChannelPeer, InboundPeer};
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::{
    domain::{gateway::ReplySink, pairing::PairingRepository},
    infra::messaging::reconnect_backoff,
};
use komo_config::TelegramConfig;

const TELEGRAM_BASE_URL: &str = "https://api.telegram.org";
/// Long-poll wait passed to `getUpdates`.
const POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Outbound side of the integration. Shared by the ingress channel (replies)
/// and the `HomeNotifier` (proactive messages to the home chat).
pub struct TelegramSender {
    bot_token: String,
    http: reqwest::Client,
}

/// Bot API envelope: `result` is present on `ok`, `description` on failure.
#[derive(Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    #[serde(default)]
    description: String,
    result: Option<T>,
}

#[derive(Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
}

#[derive(Deserialize)]
struct Message {
    message_id: i64,
    text: Option<String>,
    from: Option<User>,
    chat: Chat,
}

#[derive(Deserialize)]
struct User {
    id: i64,
    #[serde(default)]
    is_bot: bool,
}

#[derive(Deserialize)]
struct Chat {
    id: i64,
    #[serde(rename = "type")]
    kind: String,
}

impl TelegramSender {
    pub fn new(bot_token: String) -> Self {
        Self {
            bot_token,
            http: reqwest::Client::new(),
        }
    }

    fn url(&self, method: &str) -> String {
        format!("{TELEGRAM_BASE_URL}/bot{}/{method}", self.bot_token)
    }

    /// Send the agent's text as a Rich Message (Bot API 10.1): the CommonMark
    /// is passed straight through as the rich message's `markdown` field, which
    /// Telegram renders natively (headings, lists, tables, quotes, inline styles).
    pub async fn send_text(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        let body = serde_json::json!({
            "chat_id": chat_id,
            "rich_message": { "markdown": text },
        });
        let response: ApiResponse<serde_json::Value> = self
            .http
            .post(self.url("sendRichMessage"))
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if !response.ok {
            anyhow::bail!("telegram sendRichMessage failed: {}", response.description);
        }
        Ok(())
    }

    /// Send a PNG photo (e.g. the wechat login QR) via `sendPhoto` multipart.
    pub async fn send_photo(
        &self,
        chat_id: &str,
        png: Vec<u8>,
        caption: &str,
    ) -> anyhow::Result<()> {
        let part = reqwest::multipart::Part::bytes(png)
            .file_name("qr.png")
            .mime_str("image/png")?;
        let form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .text("caption", caption.to_string())
            .part("photo", part);
        let response: ApiResponse<serde_json::Value> = self
            .http
            .post(self.url("sendPhoto"))
            .multipart(form)
            .send()
            .await?
            .json()
            .await?;
        if !response.ok {
            anyhow::bail!("telegram sendPhoto failed: {}", response.description);
        }
        Ok(())
    }

    /// The bot's own username, needed for the group @mention gate.
    async fn bot_username(&self) -> anyhow::Result<String> {
        #[derive(Deserialize)]
        struct Me {
            username: Option<String>,
        }

        let response: ApiResponse<Me> = self
            .http
            .get(self.url("getMe"))
            .send()
            .await?
            .json()
            .await?;
        if !response.ok {
            anyhow::bail!("telegram getMe failed: {}", response.description);
        }
        response
            .result
            .and_then(|me| me.username)
            .ok_or_else(|| anyhow::anyhow!("telegram bot has no username"))
    }

    /// One long-poll round. `offset` acknowledges everything below it.
    async fn get_updates(&self, offset: i64) -> anyhow::Result<Vec<Update>> {
        let response: ApiResponse<Vec<Update>> = self
            .http
            .post(self.url("getUpdates"))
            // Outlive the server-side long-poll wait.
            .timeout(POLL_TIMEOUT + Duration::from_secs(10))
            .json(&serde_json::json!({
                "offset": offset,
                "timeout": POLL_TIMEOUT.as_secs(),
                "allowed_updates": ["message"],
            }))
            .send()
            .await?
            .json()
            .await?;
        if !response.ok {
            anyhow::bail!("telegram getUpdates failed: {}", response.description);
        }
        Ok(response.result.unwrap_or_default())
    }
}

/// Sends a turn's output (and any mid-turn approval prompts) back to one chat.
struct TelegramReplySink {
    sender: Arc<TelegramSender>,
    chat_id: String,
}

#[async_trait]
impl ReplySink for TelegramReplySink {
    async fn send(&self, text: &str) -> anyhow::Result<()> {
        self.sender.send_text(&self.chat_id, text).await
    }

    async fn send_photo(&self, png: Vec<u8>, caption: &str) -> anyhow::Result<()> {
        self.sender.send_photo(&self.chat_id, png, caption).await
    }
}

/// Which inbound messages the agent handles. Sender identity (allowlist /
/// pairing) is the `PairingGuard`'s job, not this struct's.
#[derive(Clone, Default)]
struct AdmitPolicy {
    /// Group messages must @mention the bot (DMs always pass).
    require_mention: bool,
    /// When non-empty, only handle group messages from these chat ids (DMs
    /// always pass). Mirrors hermes' `allowed_chats`.
    allowed_chats: Vec<String>,
}

pub struct TelegramChannel {
    sender: Arc<TelegramSender>,
    policy: AdmitPolicy,
    guard: PairingGuard,
}

/// One inbound text message, reduced to what the agent needs.
struct Inbound {
    /// Telegram's own id for the message, carried through for the inbox:
    /// `offset` only acknowledges a *batch*, so a crash between running a turn
    /// and committing the next offset redelivers everything in it.
    message_id: i64,
    sender_id: String,
    chat_id: String,
    /// A `private` chat. Which conversation a message belongs to depends on it
    /// (docs/bot-runtime.md §3.8): the operator's DM is their home
    /// conversation, a group is a conversation with other people.
    private: bool,
    text: String,
}

impl TelegramChannel {
    pub fn new(
        sender: Arc<TelegramSender>,
        config: &TelegramConfig,
        pairings: Arc<dyn PairingRepository>,
    ) -> Self {
        Self {
            sender,
            policy: AdmitPolicy {
                require_mention: config.require_mention,
                allowed_chats: config.allowed_chats.clone(),
            },
            guard: PairingGuard::new("telegram", config.allow_from.clone(), pairings),
        }
    }
}

#[async_trait]
impl Channel for TelegramChannel {
    fn name(&self) -> &str {
        "telegram"
    }

    async fn serve(
        &self,
        dispatcher: Arc<GatewayDispatcher>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        // The group mention gate needs the bot's username; keep retrying so a
        // gateway started offline comes up once the network does. Back off on
        // repeated failures (a bad token would otherwise hammer getMe).
        let mut backoff = 0usize;
        let username = loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                result = self.sender.bot_username() => match result {
                    Ok(name) => break name,
                    Err(error) => {
                        warn!(%error, "telegram getMe failed; retrying");
                        tokio::select! {
                            _ = shutdown.changed() => return Ok(()),
                            _ = tokio::time::sleep(reconnect_backoff(backoff)) => {}
                        }
                        backoff += 1;
                    }
                }
            }
        };
        info!(bot = %username, "telegram channel connected");

        // Messages are handled one at a time, in arrival order; the next poll
        // (offset past everything received) acknowledges the batch. The chat
        // id keys the session, so a private chat is one continuous
        // conversation.
        let mut offset = 0i64;
        let mut backoff = 0usize;
        loop {
            let updates = tokio::select! {
                _ = shutdown.changed() => break,
                result = self.sender.get_updates(offset) => match result {
                    // A successful long-poll (even an empty batch) clears the
                    // backoff so the next failure starts from the short delay.
                    Ok(updates) => {
                        backoff = 0;
                        updates
                    }
                    Err(error) => {
                        warn!(%error, "telegram polling failed; retrying");
                        tokio::select! {
                            _ = shutdown.changed() => break,
                            _ = tokio::time::sleep(reconnect_backoff(backoff)) => {}
                        }
                        backoff += 1;
                        continue;
                    }
                }
            };
            for update in updates {
                offset = offset.max(update.update_id + 1);
                let Some(message) = update.message else {
                    continue;
                };
                let Some(msg) = admit(message, &self.policy, &username) else {
                    continue;
                };
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
                info!(chat = %msg.chat_id, "telegram message received");
                let sink: Arc<dyn ReplySink> = Arc::new(TelegramReplySink {
                    sender: self.sender.clone(),
                    chat_id: msg.chat_id.clone(),
                });
                // Returns promptly: a turn runs on its own task so this loop can
                // keep polling and deliver the user's `/approve` reply.
                // `offset` acknowledges a batch, not a message: a crash after
                // running a turn but before the next poll redelivers the whole
                // batch. The inbox is what makes that a no-op.
                let origin = InboundOrigin::new("telegram", msg.message_id.to_string());
                dispatcher
                    .handle(
                        &InboundPeer::new(
                            ChannelPeer::new("telegram", msg.chat_id.to_string()),
                            msg.private,
                            principal == Principal::Operator,
                        ),
                        origin,
                        msg.text,
                        sink,
                    )
                    .await;
            }
        }
        info!("telegram channel stopped");
        Ok(())
    }
}

/// Reduce an update's message to an `Inbound`, or `None` when the agent
/// should ignore it (policy rejection, non-text, bot sender, empty after
/// mention strip).
fn admit(message: Message, policy: &AdmitPolicy, bot_username: &str) -> Option<Inbound> {
    let from = message.from?;
    if from.is_bot {
        return None;
    }
    let text = message.text?;
    let mention = format!("@{bot_username}");
    let is_group = matches!(message.chat.kind.as_str(), "group" | "supergroup");
    let private = message.chat.kind == "private";
    if is_group {
        // Group chat-id allowlist (when set): only handle whitelisted chats.
        if !policy.allowed_chats.is_empty()
            && !policy
                .allowed_chats
                .iter()
                .any(|c| c == &message.chat.id.to_string())
        {
            return None;
        }
        if policy.require_mention && !text.contains(&mention) {
            return None;
        }
    }
    let text = text
        .replace(&mention, " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if text.is_empty() {
        return None;
    }
    Some(Inbound {
        message_id: message.message_id,
        sender_id: from.id.to_string(),
        chat_id: message.chat.id.to_string(),
        private,
        text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(text: &str, chat_kind: &str, user_id: i64) -> Message {
        Message {
            message_id: 1,
            text: Some(text.to_string()),
            from: Some(User {
                id: user_id,
                is_bot: false,
            }),
            chat: Chat {
                id: 100,
                kind: chat_kind.to_string(),
            },
        }
    }

    #[test]
    fn admit_passes_private_text_message_with_sender_id() {
        let policy = AdmitPolicy {
            require_mention: true,
            ..Default::default()
        };
        let inbound = admit(message("hello", "private", 1), &policy, "komo_bot").unwrap();
        assert_eq!(inbound.chat_id, "100");
        assert_eq!(inbound.sender_id, "1");
        assert_eq!(inbound.text, "hello");
    }

    #[test]
    fn admit_requires_mention_in_groups_and_strips_it() {
        let policy = AdmitPolicy {
            require_mention: true,
            ..Default::default()
        };
        assert!(admit(message("hello", "supergroup", 1), &policy, "komo_bot").is_none());
        let inbound = admit(
            message("@komo_bot what time is it", "supergroup", 1),
            &policy,
            "komo_bot",
        )
        .unwrap();
        assert_eq!(inbound.text, "what time is it");
    }

    #[test]
    fn admit_enforces_group_chat_allowlist() {
        // chat id 100 is not in the allowlist → group message dropped.
        let policy = AdmitPolicy {
            require_mention: false,
            allowed_chats: vec!["999".to_string()],
        };
        assert!(admit(message("hi", "supergroup", 1), &policy, "komo_bot").is_none());

        // Same message in an allowlisted chat is admitted; DMs always pass.
        let policy = AdmitPolicy {
            require_mention: false,
            allowed_chats: vec!["100".to_string()],
        };
        assert!(admit(message("hi", "supergroup", 1), &policy, "komo_bot").is_some());
        assert!(admit(message("hi", "private", 1), &policy, "komo_bot").is_some());
    }

    #[test]
    fn admit_rejects_bot_senders() {
        let mut msg = message("hello", "private", 1);
        msg.from.as_mut().unwrap().is_bot = true;
        assert!(admit(msg, &AdmitPolicy::default(), "komo_bot").is_none());
    }
}
