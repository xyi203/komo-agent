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
}
