use async_trait::async_trait;

use super::{
    message::Message,
    session::{ChannelPeer, Session},
    session_event::{SessionEvent, SessionEventKind},
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

/// Read the conversation a session holds.
///
/// Reading only: everything that *happens* in a session is appended through
/// [`SessionEventRepository`], and the messages here are one projection of that
/// log — the one a later turn replays. A caller that wants to record something
/// records the event, not the message it will eventually project into.
#[async_trait]
pub trait MessageRepository: Send + Sync {
    /// Every message a later turn would replay, oldest first.
    async fn list_by_session(&self, session_id: &str) -> anyhow::Result<Vec<Message>>;
}

/// Append to a session's authoritative event log.
///
/// One writer per session, and it assigns the sequence numbers — a caller that
/// numbered its own events is how two ingresses end up committing the same
/// position.
///
/// [`append`](Self::append) buffers. A caller whose next step has an effect
/// that must be attributable after a crash — dispatching a tool, sending a
/// provider request — calls [`durable_flush`](Self::durable_flush) first, and
/// only then acts.
#[async_trait]
pub trait SessionEventRepository: Send + Sync {
    /// Assign and buffer a batch, returning the seqs it was given.
    async fn append(
        &self,
        session_id: &str,
        kinds: Vec<SessionEventKind>,
    ) -> anyhow::Result<Vec<u64>>;

    /// Make everything buffered survive a crash. Reaches the filesystem's
    /// durability boundary — flushing a userspace buffer would make the
    /// recovery rules claim more than the bytes support.
    async fn durable_flush(&self, session_id: &str) -> anyhow::Result<()>;

    /// The session's events, oldest first. Empty for a session with no log.
    async fn events(&self, session_id: &str) -> anyhow::Result<Vec<SessionEvent>>;

    /// A completed turn just became durable — a safe point for the log to do its
    /// own upkeep.
    ///
    /// Called here and nowhere else because a turn boundary is the only place
    /// the log may cut itself: its unit of deletion has to hold whole turns, or
    /// the recoverable half of a turn becomes unsplittable from the deletable
    /// one. Best-effort — the turn is already over and nothing here may fail it.
    async fn turn_boundary(&self, session_id: &str) -> anyhow::Result<()>;
}

#[async_trait]
pub trait SkillRepository: Send + Sync {
    async fn find(&self, name: &str) -> anyhow::Result<Option<Skill>>;
    async fn list(&self) -> anyhow::Result<Vec<Skill>>;
    async fn save(&self, skill: &Skill) -> anyhow::Result<()>;
}
