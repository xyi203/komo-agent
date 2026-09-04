use async_trait::async_trait;

/// The "home" channel: where proactive output (reminders, task due notices, the
/// gateway's shutdown notice) is delivered. Borrowed from hermes-agent's
/// home-channel concept, but settable at runtime via the `/sethome` chat command
/// instead of only through config.
///
/// The value is a **channel address** — `{platform}:{chat_id}`, e.g.
/// `telegram:123456` — the same form the config's `home_chat` takes, so the
/// notifier can pick the matching channel sender. A `/sethome` override here
/// wins over the config value.
///
/// An address and not a session id: proactive output goes to a *correspondent*,
/// and a session is a conversation with one. They used to be the same string,
/// which is why this once read as "the home session id"; a session id is a UUID
/// now and names no channel to send through.
#[async_trait]
pub trait HomeRepository: Send + Sync {
    /// The current home address, or `None` when unset.
    async fn get(&self) -> anyhow::Result<Option<String>>;
    /// Set the home to `address` (`{platform}:{chat_id}`).
    async fn set(&self, address: &str) -> anyhow::Result<()>;

    /// The operator's **home conversation** — one session id, minted the first
    /// time anything asks for it and stored from then on.
    ///
    /// Every private surface the operator speaks to komo through lands here:
    /// the TUI, the desktop and web clients, a Telegram or Feishu DM, WeChat.
    /// That is the D6 invariant — same principal + private conversation ⇒ one
    /// ordered timeline (docs/bot-runtime.md §3.8) — and it is a stored id
    /// rather than a computed one for the same reason a chat's session id is:
    /// a conversation's identity is its id and nothing else.
    ///
    /// Distinct from [`get`](Self::get), which is an *address* to deliver
    /// proactive output to. The two are unrelated: a reminder can be sent to a
    /// Feishu group without that group being the home conversation.
    async fn home_session(&self) -> anyhow::Result<String>;
}
