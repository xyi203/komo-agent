use async_trait::async_trait;

use super::{
    message::{Message, ToolEntry},
    session::{ChannelPeer, Session},
    skill::Skill,
};

#[async_trait]
pub trait SessionRepository: Send + Sync {
    /// Find a session by id. Returns None if it does not exist.
    ///
    /// Loads the *entire* transcript — the reflective reviewer depends on
    /// seeing every message. The per-turn agent loop, which only needs a recent
    /// window, should use [`find_windowed`](Self::find_windowed) instead.
    async fn find(&self, id: &str) -> anyhow::Result<Option<Session>>;
    /// Like [`find`](Self::find) but loads only the most recent `limit`
    /// messages (by timestamp), keeping the per-turn hot path off a full-
    /// transcript read for long-lived chat sessions. `limit == 0` means no
    /// window (load everything, same as `find`). The returned messages stay in
    /// chronological order.
    async fn find_windowed(&self, id: &str, limit: usize) -> anyhow::Result<Option<Session>>;
    /// The session that answers a given correspondent, if one exists.
    ///
    /// This is the only way a chat channel finds its way back to a
    /// conversation: an inbound message carries the platform's own chat id, not
    /// a session id. It used to be answered by *computing* the session id
    /// (`feishu:{chat_id}`), which made the id a derived value and left no room
    /// for a conversation to exist without an address — or for two ids to name
    /// the same one. Metadata only; the transcript is not loaded.
    async fn find_by_peer(&self, channel: &ChannelPeer) -> anyhow::Result<Option<Session>>;
    /// Return all sessions, ordered by creation time.
    async fn list(&self) -> anyhow::Result<Vec<Session>>;
    /// Persist a session (insert or update).
    async fn save(&self, session: &Session) -> anyhow::Result<()>;
    /// Delete every session that has zero messages. Returns the count removed.
    async fn delete_empty_sessions(&self) -> anyhow::Result<usize>;
    /// Rotate a session (hermes' `/new`): move its messages to a fresh archived
    /// id so `session_id` is left empty for a new conversation, while the old
    /// transcript is preserved (the reviewer can still see it). Returns the
    /// archived id, or `None` when there was nothing to archive.
    async fn rotate(&self, session_id: &str) -> anyhow::Result<Option<String>>;

    /// Set a session's display title (operator rename). No-op if the session
    /// does not exist. Default is a no-op so stores that don't support titling
    /// aren't forced to implement it.
    async fn set_title(&self, _session_id: &str, _title: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// Set a session's lifecycle status (`active` / `archive` / `deleted`) —
    /// a soft state; the list view hides `deleted`. No-op if the session does
    /// not exist. Default is a no-op.
    async fn set_status(&self, _session_id: &str, _status: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// Set a session's model override and reasoning effort (either empty = fall
    /// back to the gateway/provider default). Unlike the workspace this is not
    /// creation-locked: a conversation may switch models mid-thread, and the
    /// stored choice is what the next turn — and any other client opening the
    /// session — runs on. No-op if the session does not exist. Default is a
    /// no-op so stores without the columns aren't forced to implement it.
    async fn set_model(
        &self,
        _session_id: &str,
        _model: &str,
        _effort: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Delete a session and its messages outright (operator delete — the row
    /// disappears from the session list). Returns whether a session was
    /// removed. Default is a no-op returning `false`.
    async fn delete_session(&self, _session_id: &str) -> anyhow::Result<bool> {
        Ok(false)
    }
}

#[async_trait]
pub trait MessageRepository: Send + Sync {
    /// Return all messages for a session, ordered by timestamp.
    async fn list_by_session(&self, session_id: &str) -> anyhow::Result<Vec<Message>>;
    /// Append a message to a session.
    async fn save(&self, session_id: &str, message: &Message) -> anyhow::Result<()>;
    /// Record that the turn in flight was cancelled before it did anything.
    ///
    /// The pristine-cancel rollback: a turn cancelled before it produced
    /// anything should read as if it never happened. **The store records the
    /// fact and its projection decides what it means** — nothing is deleted, so
    /// the transcript a reader gets loses the turn while the store still knows
    /// it happened. Deliberately not a general "edit history" affordance.
    async fn cancel_last_turn(&self, session_id: &str) -> anyhow::Result<()>;
    /// Record something the user said while a turn was already running.
    ///
    /// What they said belongs to that turn's user input, not to a turn of its
    /// own: two consecutive user messages is exactly what a transcript may not
    /// contain (several providers reject it on replay). Recorded separately and
    /// merged by the projection, for the same reason as
    /// [`cancel_last_turn`](Self::cancel_last_turn) — a store that only ever
    /// appends cannot corrupt what it already wrote.
    async fn record_interjection(&self, session_id: &str, text: &str) -> anyhow::Result<()>;

    /// Record one tool call in the transcript.
    ///
    /// Never read back into the model's history — see [`ToolEntry`]. It makes
    /// the transcript a complete account of the conversation *including the
    /// work*, which is what an operator reading the file, or a client rendering
    /// it, actually needs. Best-effort at the call site, like the ledger:
    /// failing to record what a tool did must not fail the tool.
    ///
    /// Default no-op, so a store that keeps only what was said is still a valid
    /// transcript store.
    async fn record_tool(&self, _session_id: &str, _entry: &ToolEntry) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait SkillRepository: Send + Sync {
    async fn find(&self, name: &str) -> anyhow::Result<Option<Skill>>;
    async fn list(&self) -> anyhow::Result<Vec<Skill>>;
    async fn save(&self, skill: &Skill) -> anyhow::Result<()>;
}
