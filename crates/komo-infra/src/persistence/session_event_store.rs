//! Every session's event log, and the projections a caller actually asks for.
//!
//! [`SessionLog`] owns one session's bytes. This owns the set of them, keeps
//! one handle per session alive (the handle holds the write position, so two
//! handles to one session would hand out the same `seq` twice), and turns a log
//! into the things callers want: the messages a later turn replays, the window
//! of them a turn actually sends.
//!
//! It is deliberately not a `Repository` trait implementation. The repository
//! seam takes and returns `Message`, which has no turn — and a turn is exactly
//! what the log needs to attribute a cancel or an interjection. Callers that
//! know their turn append events; callers that only want history read messages.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use komo_core::domain::message::Message;
use komo_core::domain::session_event::{
    SESSION_EVENT_VERSION, SessionEvent, SessionEventKind, SessionHeader, derive_messages,
};
use tokio::sync::Mutex;

use super::session_log::{LogError, SessionLog};

type Result<T> = std::result::Result<T, LogError>;

pub struct SessionEventStore {
    root: PathBuf,
    /// One live handle per session. Opening a second would give it its own
    /// `next_seq` cursor, and two writers numbering their own events is the
    /// failure the single-writer rule exists to prevent.
    open: Mutex<HashMap<String, Arc<SessionLog>>>,
}

impl SessionEventStore {
    /// Sessions live under `<home>/sessions/<session-id>/`.
    pub fn new(home: &std::path::Path) -> Self {
        Self {
            root: home.join("sessions"),
            open: Mutex::new(HashMap::new()),
        }
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    /// The session's log, materializing it from `header` the first time.
    ///
    /// `header` is only consulted on first materialization: it is identity, and
    /// a session does not get a new one because a later caller described it
    /// differently.
    pub async fn open(&self, session_id: &str, header: SessionHeader) -> Result<Arc<SessionLog>> {
        let mut open = self.open.lock().await;
        if let Some(log) = open.get(session_id) {
            return Ok(log.clone());
        }
        let log = Arc::new(SessionLog::open_or_create(self.dir_for(session_id), header).await?);
        open.insert(session_id.to_string(), log.clone());
        Ok(log)
    }

    /// A log that already exists, or `None`. Never creates one — a reader
    /// asking about a session that was never opened is not a reason to make it.
    pub async fn existing(&self, session_id: &str) -> Result<Option<Arc<SessionLog>>> {
        {
            let open = self.open.lock().await;
            if let Some(log) = open.get(session_id) {
                return Ok(Some(log.clone()));
            }
        }
        if !self.dir_for(session_id).join("manifest.json").exists() {
            return Ok(None);
        }
        // The header is read back from the manifest, so the placeholder here is
        // never the one that lands on disk.
        self.open(session_id, placeholder_header(session_id))
            .await
            .map(Some)
    }

    /// Every message a later turn would replay, oldest first.
    pub async fn messages(&self, session_id: &str) -> Result<Vec<Message>> {
        let Some(log) = self.existing(session_id).await? else {
            return Ok(Vec::new());
        };
        let events = log.load().await?;
        Ok(derive_messages(&events, log.truncated_before().await)?)
    }

    /// The most recent `limit` messages, still oldest first. `0` means all.
    ///
    /// Derives the whole surface and then cuts. A window over an *event* log
    /// cannot be taken by reading the tail alone the way a message log's could:
    /// a compaction near the end can replace a range that began far earlier, so
    /// the last N events do not determine the last N messages. Bounding this is
    /// the projection checkpoint's job (batch 3), not a shortcut here — a
    /// wrong-but-fast window would hand the model a history that never existed.
    pub async fn windowed(&self, session_id: &str, limit: usize) -> Result<Vec<Message>> {
        let mut all = self.messages(session_id).await?;
        if limit > 0 && all.len() > limit {
            all.drain(..all.len() - limit);
        }
        Ok(all)
    }

    /// Append a batch to a session that is already open, returning the assigned
    /// seqs. The batch is buffered; a caller that needs it to have survived a
    /// crash calls [`SessionLog::durable_flush`] before the effect it describes.
    /// Roll the session's active segment if it has grown past its target.
    ///
    /// Returns whether a segment was sealed — the only moment the log's retained
    /// size can cross its budget, and so the only moment retention has anything
    /// new to consider.
    pub async fn seal(&self, session_id: &str) -> Result<bool> {
        match self.existing(session_id).await? {
            Some(log) => log.seal_if_full().await,
            None => Ok(false),
        }
    }

    pub async fn append(
        &self,
        session_id: &str,
        header: SessionHeader,
        kinds: Vec<SessionEventKind>,
    ) -> Result<Vec<u64>> {
        Ok(self
            .open(session_id, header)
            .await?
            .append_batch(kinds)
            .await)
    }

    /// Session ids with a log on disk.
    pub async fn session_ids(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut entries = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(error) => return Err(error.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().join("manifest.json").exists()
                && let Some(name) = entry.file_name().to_str()
            {
                out.push(decode_dir_name(name));
            }
        }
        out.sort();
        Ok(out)
    }

    /// Forget a session entirely: drop its handle, then its bytes.
    ///
    /// The handle goes first — deleting the directory under a live writer would
    /// leave it appending into a file nothing references.
    pub async fn delete(&self, session_id: &str) -> Result<()> {
        self.open.lock().await.remove(session_id);
        match tokio::fs::remove_dir_all(self.dir_for(session_id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Every event in a session, for the callers that need more than messages —
    /// turn continuation, the run-ledger projection, a human transcript.
    pub async fn events(&self, session_id: &str) -> Result<Vec<SessionEvent>> {
        match self.existing(session_id).await? {
            Some(log) => log.load().await,
            None => Ok(Vec::new()),
        }
    }

    fn dir_for(&self, session_id: &str) -> PathBuf {
        self.root.join(encode_dir_name(session_id))
    }
}

fn placeholder_header(session_id: &str) -> SessionHeader {
    SessionHeader {
        session_id: session_id.to_string(),
        origin: "user".to_string(),
        workspace: None,
        created_at: time::OffsetDateTime::now_utc(),
        format_version: SESSION_EVENT_VERSION,
    }
}

/// Session ids are UUIDs, so this is the identity mapping in practice. It stays
/// because a directory name is a filesystem path and an id that is not a UUID —
/// one from an older komo, one a test made up — must still not be able to name
/// a directory outside the root.
fn encode_dir_name(session_id: &str) -> String {
    let mut name = String::with_capacity(session_id.len());
    for byte in session_id.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'_' => name.push(byte as char),
            other => name.push_str(&format!("%{other:02X}")),
        }
    }
    name
}

fn decode_dir_name(name: &str) -> String {
    let bytes = name.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(byte) = u8::from_str_radix(&name[i + 1..i + 3], 16)
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::message::Role;
    use komo_core::domain::session_event::{
        AssistantMessageEvent, MessageSource, SurfacePlacement, UserMessageEvent,
    };

    fn store(name: &str) -> SessionEventStore {
        let home = std::env::temp_dir().join(format!("komo_event_store_{name}"));
        let _ = std::fs::remove_dir_all(&home);
        SessionEventStore::new(&home)
    }

    fn header(session_id: &str) -> SessionHeader {
        SessionHeader {
            session_id: session_id.into(),
            origin: "user".into(),
            workspace: None,
            created_at: time::OffsetDateTime::now_utc(),
            format_version: SESSION_EVENT_VERSION,
        }
    }

    fn said(turn: &str, text: &str, source: MessageSource) -> SessionEventKind {
        SessionEventKind::UserMessage(UserMessageEvent {
            turn_id: turn.into(),
            content: text.into(),
            source,
            surface: SurfacePlacement::append(),
        })
    }

    fn answered(turn: &str, text: &str) -> SessionEventKind {
        SessionEventKind::AssistantMessage(AssistantMessageEvent {
            turn_id: turn.into(),
            content: text.into(),
            tool_note: String::new(),
            surface: SurfacePlacement::append(),
        })
    }

    #[tokio::test]
    async fn messages_come_back_as_the_conversation_a_later_turn_replays() {
        let store = store("messages");
        let id = "019fad15-8199-7461-9d48-0a6c779f1c8d";
        store
            .append(
                id,
                header(id),
                vec![
                    said("t1", "q1", MessageSource::User),
                    answered("t1", "a1"),
                    said("t2", "q2", MessageSource::User),
                ],
            )
            .await
            .unwrap();
        store
            .open(id, header(id))
            .await
            .unwrap()
            .durable_flush()
            .await
            .unwrap();

        let messages = store.messages(id).await.unwrap();
        assert_eq!(
            messages
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            vec!["q1", "a1", "q2"]
        );
        assert_eq!(messages[1].role, Role::Assistant);
        // A window is the tail of that, still oldest first.
        assert_eq!(
            store
                .windowed(id, 2)
                .await
                .unwrap()
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            vec!["a1", "q2"]
        );
        assert_eq!(store.windowed(id, 0).await.unwrap().len(), 3);
        let _ = std::fs::remove_dir_all(store.root().parent().unwrap());
    }

    #[tokio::test]
    async fn one_session_gets_one_handle_so_seqs_are_never_handed_out_twice() {
        let store = store("one_handle");
        let id = "019fad15-8199-7461-9d48-0a6c779f1c8d";
        let a = store.open(id, header(id)).await.unwrap();
        let b = store.open(id, header(id)).await.unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "a second open must not open a second writer"
        );
        assert_eq!(
            a.append_batch(vec![said("t1", "x", MessageSource::User)])
                .await,
            vec![0]
        );
        assert_eq!(
            b.append_batch(vec![said("t1", "y", MessageSource::User)])
                .await,
            vec![1]
        );
        let _ = std::fs::remove_dir_all(store.root().parent().unwrap());
    }

    #[tokio::test]
    async fn a_session_that_was_never_opened_reads_as_empty_and_is_not_created() {
        let store = store("absent");
        let id = "019fad16-0000-7461-9d48-0a6c779f1c8d";
        assert!(store.messages(id).await.unwrap().is_empty());
        assert!(store.existing(id).await.unwrap().is_none());
        assert!(
            !store.root().join(id).exists(),
            "reading must not materialize a session"
        );
        let _ = std::fs::remove_dir_all(store.root().parent().unwrap());
    }

    #[tokio::test]
    async fn a_reopened_store_finds_the_sessions_on_disk() {
        let home = std::env::temp_dir().join("komo_event_store_reopen");
        let _ = std::fs::remove_dir_all(&home);
        let id = "019fad15-8199-7461-9d48-0a6c779f1c8d";
        {
            let store = SessionEventStore::new(&home);
            store
                .append(id, header(id), vec![said("t1", "hi", MessageSource::User)])
                .await
                .unwrap();
            store
                .open(id, header(id))
                .await
                .unwrap()
                .durable_flush()
                .await
                .unwrap();
        }
        let store = SessionEventStore::new(&home);
        assert_eq!(store.session_ids().await.unwrap(), vec![id.to_string()]);
        assert_eq!(store.messages(id).await.unwrap().len(), 1);
        // The header on disk wins: `existing` must not overwrite identity with
        // the placeholder it uses to reopen.
        assert_eq!(
            store
                .existing(id)
                .await
                .unwrap()
                .unwrap()
                .header()
                .await
                .session_id,
            id
        );
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn deleting_a_session_drops_its_handle_before_its_bytes() {
        let store = store("delete");
        let id = "019fad15-8199-7461-9d48-0a6c779f1c8d";
        store
            .append(id, header(id), vec![said("t1", "hi", MessageSource::User)])
            .await
            .unwrap();
        store
            .open(id, header(id))
            .await
            .unwrap()
            .durable_flush()
            .await
            .unwrap();
        store.delete(id).await.unwrap();

        assert!(store.existing(id).await.unwrap().is_none());
        assert!(store.messages(id).await.unwrap().is_empty());
        // Idempotent: deleting what is already gone is not an error.
        store.delete(id).await.unwrap();
        let _ = std::fs::remove_dir_all(store.root().parent().unwrap());
    }

    #[tokio::test]
    async fn an_id_that_is_not_a_uuid_cannot_name_a_directory_outside_the_root() {
        let store = store("escape");
        let id = "../../etc/passwd";
        store
            .append(id, header(id), vec![said("t1", "hi", MessageSource::User)])
            .await
            .unwrap();
        store
            .open(id, header(id))
            .await
            .unwrap()
            .durable_flush()
            .await
            .unwrap();
        // The whole id became one path segment, and it round-trips.
        assert_eq!(store.session_ids().await.unwrap(), vec![id.to_string()]);
        assert_eq!(store.messages(id).await.unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(store.root().parent().unwrap());
    }
}
