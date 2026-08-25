//! Transcripts as append-only JSONL — one file per session under
//! `~/.komo/sessions/`.
//!
//! ## Why this is not a table
//!
//! A transcript is the one thing in komo that is purely *appended*: a turn adds
//! a user message and an assistant message, and nothing ever revisits them. That
//! shape costs nothing in a table, but the schema does — a table's shape is
//! fixed at creation, and toasty's `push_schema` runs only for a new database
//! file, so a non-additive change to the message shape means deleting
//! `state.db`. A line of JSON has no shape to migrate: a new field reads as its
//! default on every line written before it existed, and a change too deep for
//! that is dispatched on [`Line::v`] rather than paid for with the file.
//!
//! It also drops a piece of bookkeeping the table needed. There, a message's
//! order comes from a UUIDv7 key, because `timestamp` is whole seconds and a
//! fast turn's user and assistant messages would otherwise be indistinguishable.
//! Here the order *is* the file: line N was appended before line N+1.
//!
//! Session metadata stays in the database, on purpose. Titles, status, the model
//! override and the review watermark are all *updated*, and a log is the wrong
//! shape for a value that changes — so each holds what it is good at, and
//! `SessionRepository` reads the two together.
//!
//! ## What holds it together
//!
//! **One writer.** komo runs one turn per session (the gateway dispatcher
//! enforces it), so a transcript has a single writer by construction. The
//! per-session lock here makes that true *within* the process as well.
//!
//! **Nothing is ever rewritten.** Two things used to go back and change what
//! was already written — a cancelled turn deleted its user message, and a
//! mid-turn interjection edited one. On a file both meant reading the whole
//! transcript, cutting it, and writing it out again. They are now facts
//! appended at the end ([`KIND_CANCELLED`], [`KIND_SAID_MORE`]) and resolved on
//! the way out by [`fold`], which is also the single place that keeps a
//! reader's view alternating user/assistant.
//!
//! **A partial line is dropped, never patched.** A process killed mid-append can
//! leave a truncated final line. Reads skip any line that does not parse and say
//! so in the log; nothing tries to repair one, because a half-written message is
//! not a message.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use komo_core::domain::message::{Message, Role, ToolEntry};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::warn;

/// Bumped when a line's shape changes in a way `serde` defaults cannot absorb.
/// A reader that meets a version it does not know skips the line rather than
/// guessing at it — the same rule the turn journal follows.
const LINE_VERSION: u32 = 1;

/// How much of a transcript's tail a windowed read looks at. Far more than any
/// history window needs (a window is tens of messages; this is thousands), so
/// the fallback to a full read is the rare case, not the common one.
const TAIL_BYTES: u64 = 512 * 1024;

/// Marks a line that records a tool call rather than something that was said.
///
/// Absent on a message line — which is also every line written before tool
/// activity was recorded here at all, so the older files read unchanged.
const KIND_TOOL: &str = "tool";

/// Marks a line that records what became of a turn rather than something that
/// was said.
///
/// These two exist so the log never has to go back and change what it already
/// wrote. A cancel used to delete the user's message and an interjection used
/// to edit it; on a file that meant reading the whole transcript, cutting it,
/// and writing it out again. Both are now facts appended at the end, and
/// [`fold`] is the single place that decides what they mean.
const KIND_CANCELLED: &str = "cancelled";
const KIND_SAID_MORE: &str = "said_more";

/// The part of a line that is common to both kinds, read first to decide how to
/// read the rest.
#[derive(Deserialize)]
struct LineHeader {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    kind: Option<String>,
}

/// A message line: the message's own fields, flattened in, plus a version.
/// Unknown fields are ignored by serde, which is what lets a newer komo's file
/// still load in an older one.
#[derive(Serialize, Deserialize)]
struct MessageLine {
    #[serde(default = "default_version")]
    v: u32,
    #[serde(flatten)]
    message: Message,
}

/// A tool line. Carries `kind` so a reader can tell it from a message without
/// guessing at which fields are present.
#[derive(Serialize, Deserialize)]
struct ToolLine {
    #[serde(default = "default_version")]
    v: u32,
    kind: String,
    #[serde(flatten)]
    entry: ToolEntry,
}

/// A turn that was cancelled before it did anything worth remembering.
#[derive(Serialize, Deserialize)]
struct CancelledLine {
    #[serde(default = "default_version")]
    v: u32,
    kind: String,
    timestamp: i64,
}

/// Something the user said while a turn was already running.
#[derive(Serialize, Deserialize)]
struct SaidMoreLine {
    #[serde(default = "default_version")]
    v: u32,
    kind: String,
    content: String,
    timestamp: i64,
}

/// What one line turned out to be.
enum Entry {
    Said(Message),
    Ran(ToolEntry),
    /// The turn that the preceding user message belongs to was cancelled before
    /// it did anything. Resolved by [`fold`].
    Cancelled,
    /// The user added this while that turn was in flight. Resolved by [`fold`].
    SaidMore(String),
}

fn default_version() -> u32 {
    LINE_VERSION
}

fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

/// Append-only transcript storage, one file per session.
pub struct MessageLog {
    dir: PathBuf,
    /// One lock per session file, so two appends to the same transcript cannot
    /// interleave. Nothing reads-then-writes any more, so that is all it has to
    /// guard.
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl MessageLog {
    /// Open (creating it if needed) the transcript directory under `home`.
    pub fn open(home: &Path) -> anyhow::Result<Self> {
        let dir = home.join("sessions");
        std::fs::create_dir_all(&dir).map_err(|e| {
            anyhow::anyhow!(
                "could not create the transcript directory {}: {e}",
                dir.display()
            )
        })?;
        Ok(Self {
            dir,
            locks: Mutex::new(HashMap::new()),
        })
    }

    /// The file a session's transcript lives in.
    ///
    /// Session ids are `{platform}:{chat_id}`, so they carry characters a file
    /// name cannot. Percent-encoding everything outside a conservative set keeps
    /// the mapping reversible and the names readable — `api:1234` becomes
    /// `api%3A1234`, which is still greppable by eye.
    pub fn path_for(&self, session_id: &str) -> PathBuf {
        let mut name = String::with_capacity(session_id.len());
        for byte in session_id.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'_' => {
                    name.push(byte as char)
                }
                other => name.push_str(&format!("%{other:02X}")),
            }
        }
        self.dir.join(format!("{name}.jsonl"))
    }

    async fn lock_for(&self, session_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().await;
        locks.entry(session_id.to_string()).or_default().clone()
    }

    /// Every line of a session's file, in order — what was said and what ran.
    async fn entries(&self, session_id: &str) -> anyhow::Result<Vec<Entry>> {
        let path = self.path_for(session_id);
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(text) => text,
            // A session with no transcript file has no transcript. That is the
            // normal state of a session id a client just minted, not an error.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "could not read the transcript {}: {error}",
                    path.display()
                ));
            }
        };
        Ok(parse_lines(&text, &path))
    }

    /// The lines as a reader should see them — [`fold`] applied.
    ///
    /// Every read path goes through here; `entries` is the raw log and stays
    /// private, so no caller can accidentally see a cancelled turn.
    async fn projected(&self, session_id: &str) -> anyhow::Result<Vec<Entry>> {
        Ok(fold(self.entries(session_id).await?))
    }

    /// Every message in a session, in the order they were appended.
    ///
    /// Tool lines are skipped: they are a record of the work, not part of what
    /// was said, and this is what feeds the model's history.
    pub async fn list(&self, session_id: &str) -> anyhow::Result<Vec<Message>> {
        Ok(self
            .projected(session_id)
            .await?
            .into_iter()
            .filter_map(|entry| match entry {
                Entry::Said(message) => Some(message),
                _ => None,
            })
            .collect())
    }

    /// The tool calls a session recorded, in order.
    pub async fn tools(&self, session_id: &str) -> anyhow::Result<Vec<ToolEntry>> {
        Ok(self
            .projected(session_id)
            .await?
            .into_iter()
            .filter_map(|entry| match entry {
                Entry::Ran(tool) => Some(tool),
                _ => None,
            })
            .collect())
    }

    /// Append one tool record.
    pub async fn append_tool(&self, session_id: &str, entry: &ToolEntry) -> anyhow::Result<()> {
        let line = serde_json::to_string(&ToolLine {
            v: LINE_VERSION,
            kind: KIND_TOOL.to_string(),
            entry: entry.clone(),
        })?;
        self.append_line(session_id, &line).await
    }

    /// The most recent `limit` messages, still in chronological order.
    /// `limit == 0` means the whole transcript.
    ///
    /// Reads only the **tail** of the file. This runs on every turn, and the
    /// whole point of a window is not to pay for a conversation's whole past —
    /// but reading the file to throw all but the last few messages away pays
    /// for it in IO and parsing anyway, on the reply path, growing for as long
    /// as the session lives. A session that is never rotated would get slower
    /// every day for a reason nobody could see.
    pub async fn window(&self, session_id: &str, limit: usize) -> anyhow::Result<Vec<Message>> {
        if limit == 0 {
            return self.list(session_id).await;
        }
        if let Some(tail) = self.tail_messages(session_id, limit).await? {
            return Ok(tail);
        }
        // The tail did not hold `limit` messages: fall back rather than answer
        // with a short window. Correctness first — the tail is an optimisation,
        // not a different contract.
        let mut all = self.list(session_id).await?;
        if all.len() > limit {
            all.drain(..all.len() - limit);
        }
        Ok(all)
    }

    /// The last `limit` messages read from at most [`TAIL_BYTES`] of the file,
    /// or `None` when that much of the file did not contain them.
    async fn tail_messages(
        &self,
        session_id: &str,
        limit: usize,
    ) -> anyhow::Result<Option<Vec<Message>>> {
        let path = self.path_for(session_id);
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Some(Vec::new()));
            }
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "could not read the transcript {}: {error}",
                    path.display()
                ));
            }
        };
        let size = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        let from = size.saturating_sub(TAIL_BYTES);
        if from > 0 {
            file.seek(std::io::SeekFrom::Start(from))
                .await
                .map_err(|e| anyhow::anyhow!("could not seek the transcript: {e}"))?;
        }
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)
            .await
            .map_err(|e| anyhow::anyhow!("could not read the transcript: {e}"))?;
        let mut text = String::from_utf8_lossy(&buf).into_owned();
        if from > 0 {
            // The seek landed mid-line; that fragment is not a record.
            match text.find('\n') {
                Some(nl) => text = text[nl + 1..].to_string(),
                None => return Ok(None),
            }
        }

        let mut messages: Vec<Message> = fold(parse_lines(&text, &path))
            .into_iter()
            .filter_map(|entry| match entry {
                Entry::Said(message) => Some(message),
                _ => None,
            })
            .collect();
        // Short only because the file itself is short is still the whole
        // answer; short because we cut the file is not.
        if messages.len() < limit && from > 0 {
            return Ok(None);
        }
        if messages.len() > limit {
            messages.drain(..messages.len() - limit);
        }
        Ok(Some(messages))
    }

    /// Append one message.
    pub async fn append(&self, session_id: &str, message: &Message) -> anyhow::Result<()> {
        let line = serde_json::to_string(&MessageLine {
            v: LINE_VERSION,
            message: message.clone(),
        })?;
        self.append_line(session_id, &line).await
    }

    async fn append_line(&self, session_id: &str, line: &str) -> anyhow::Result<()> {
        let path = self.path_for(session_id);
        let lock = self.lock_for(session_id).await;
        let _held = lock.lock().await;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| {
                anyhow::anyhow!("could not open the transcript {}: {e}", path.display())
            })?;
        // One write for the line and its terminator: a reader that catches the
        // file mid-append sees a whole line or none of it.
        file.write_all(format!("{line}\n").as_bytes())
            .await
            .map_err(|e| {
                anyhow::anyhow!("could not append to the transcript {}: {e}", path.display())
            })?;
        // Explicitly, because dropping a `tokio::fs::File` does not flush it —
        // the write is dispatched to a blocking pool and a dropped handle can
        // lose it. Without this the message is in the file only *eventually*,
        // and the next read of the same turn does not find it.
        file.flush().await.map_err(|e| {
            anyhow::anyhow!("could not flush the transcript {}: {e}", path.display())
        })?;
        Ok(())
    }

    /// Record that the turn in flight was cancelled before it did anything.
    ///
    /// Appends the fact; [`fold`] is what makes the transcript read as if the
    /// turn never happened. The line stays in the file, so an operator reading
    /// it can still see that the user asked and then stopped — the projection
    /// hides it, the log does not lose it.
    pub async fn record_cancelled_turn(&self, session_id: &str) -> anyhow::Result<()> {
        let line = serde_json::to_string(&CancelledLine {
            v: LINE_VERSION,
            kind: KIND_CANCELLED.to_string(),
            timestamp: now(),
        })?;
        self.append_line(session_id, &line).await
    }

    /// Record something the user said while a turn was already running.
    ///
    /// Appended as its own line rather than edited into the user message it
    /// belongs to; [`fold`] merges the two on the way out.
    pub async fn record_interjection(&self, session_id: &str, text: &str) -> anyhow::Result<()> {
        let line = serde_json::to_string(&SaidMoreLine {
            v: LINE_VERSION,
            kind: KIND_SAID_MORE.to_string(),
            content: text.to_string(),
            timestamp: now(),
        })?;
        self.append_line(session_id, &line).await
    }

    /// Move a transcript to another session id, as `/new` does when it archives
    /// the conversation it is rotating out of.
    ///
    /// A rename, where the table had to rewrite every row's foreign key.
    pub async fn rename(&self, from: &str, to: &str) -> anyhow::Result<()> {
        let source = self.path_for(from);
        if !source.exists() {
            return Ok(());
        }
        tokio::fs::rename(&source, self.path_for(to))
            .await
            .map_err(|e| {
                anyhow::anyhow!("could not archive the transcript {}: {e}", source.display())
            })
    }

    /// Whether the session has any messages at all — what decides an "empty"
    /// session.
    pub async fn is_empty(&self, session_id: &str) -> anyhow::Result<bool> {
        Ok(self.list(session_id).await?.is_empty())
    }

    /// Delete a session's transcript. Missing is success: the caller wanted it
    /// gone.
    pub async fn remove(&self, session_id: &str) -> anyhow::Result<()> {
        match tokio::fs::remove_file(self.path_for(session_id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(anyhow::anyhow!("could not remove the transcript: {error}")),
        }
    }
}

/// Turn the lines that were written into the transcript a reader should see.
///
/// The log records what happened; this decides what it means. That split is
/// what lets the file stay append-only — the alternative is going back to edit
/// what was already written, which on a file means rewriting all of it.
///
/// It also puts one invariant in one place. What a reader gets must alternate
/// user and assistant: several providers reject two consecutive user messages
/// on replay. Keeping that true at every *write* site took a delete here, an
/// edit there, and a placeholder somewhere else — and a rule maintained in
/// four places is a rule with a hole in it. Here it is a property of one
/// function, which is also why it can be tested without a database.
fn fold(entries: Vec<Entry>) -> Vec<Entry> {
    let mut out: Vec<Entry> = Vec::new();
    for entry in entries {
        match entry {
            // Rewind to just before that turn's user message. The tool lines
            // after it go too: they are the work of the turn being taken back,
            // and leaving them would attribute it to the turn before.
            Entry::Cancelled => {
                if let Some(at) = out
                    .iter()
                    .rposition(|e| matches!(e, Entry::Said(m) if m.role == Role::User))
                {
                    out.truncate(at);
                }
            }
            // Merge into that turn's user message rather than standing as a
            // second one. Both halves really are one user's input for one turn.
            Entry::SaidMore(text) => {
                if let Some(Entry::Said(message)) = out
                    .iter_mut()
                    .rev()
                    .find(|e| matches!(e, Entry::Said(m) if m.role == Role::User))
                {
                    message.content.push('\n');
                    message.content.push_str(&text);
                }
            }
            said_or_ran => out.push(said_or_ran),
        }
    }
    out
}

/// Parse a transcript, skipping anything that does not read as a message.
///
/// A line can fail to parse for two reasons, and both mean the same thing here:
/// the process died mid-append and left a truncated tail, or the line was
/// written by a komo whose format this one does not know. Neither is repairable
/// and neither should cost the rest of the transcript — but a dropped message is
/// a real loss, so it is never silent.
fn parse_lines(text: &str, path: &Path) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut skipped = 0usize;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // The header decides which shape to read; a line whose header will not
        // parse cannot be read as either.
        let Ok(header) = serde_json::from_str::<LineHeader>(line) else {
            warn!(path = %path.display(), "unreadable transcript line; skipped");
            skipped += 1;
            continue;
        };
        if header.v > LINE_VERSION {
            warn!(
                path = %path.display(),
                version = header.v,
                "transcript line written by a newer komo; skipped"
            );
            skipped += 1;
            continue;
        }
        let parsed = match header.kind.as_deref() {
            Some(KIND_TOOL) => serde_json::from_str::<ToolLine>(line).map(|l| Entry::Ran(l.entry)),
            Some(KIND_CANCELLED) => {
                serde_json::from_str::<CancelledLine>(line).map(|_| Entry::Cancelled)
            }
            Some(KIND_SAID_MORE) => {
                serde_json::from_str::<SaidMoreLine>(line).map(|l| Entry::SaidMore(l.content))
            }
            // A kind this build does not know is a line from a newer komo whose
            // `v` did not change. Skipping is the same rule as an unknown
            // version — never guess at a shape.
            Some(other) => {
                warn!(path = %path.display(), kind = other, "unknown transcript line kind; skipped");
                skipped += 1;
                continue;
            }
            None => serde_json::from_str::<MessageLine>(line).map(|l| Entry::Said(l.message)),
        };
        match parsed {
            Ok(entry) => entries.push(entry),
            Err(error) => {
                warn!(path = %path.display(), %error, "unreadable transcript line; skipped");
                skipped += 1;
            }
        }
    }
    if skipped > 0 {
        warn!(
            path = %path.display(),
            skipped,
            "transcript loaded with lines missing"
        );
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(tag: &str) -> (MessageLog, PathBuf) {
        let home = std::env::temp_dir().join(format!("komo-msglog-{tag}-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&home).unwrap();
        (MessageLog::open(&home).unwrap(), home)
    }

    fn assistant(content: &str) -> Message {
        Message {
            role: Role::Assistant,
            content: content.to_string(),
            timestamp: 0,
            tool_note: String::new(),
        }
    }

    #[tokio::test]
    async fn messages_come_back_in_the_order_they_were_appended() {
        let (log, _home) = log("order");
        // Same timestamp on purpose: in the table this is exactly the case that
        // forced a UUIDv7 key, because `timestamp` is whole seconds. Here the
        // file's order is the answer.
        for text in ["one", "two", "three"] {
            log.append("api:s", &assistant(text)).await.unwrap();
        }
        let got: Vec<String> = log
            .list("api:s")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(got, ["one", "two", "three"]);
    }

    #[tokio::test]
    async fn a_session_with_no_file_has_an_empty_transcript() {
        let (log, _home) = log("missing");
        assert!(log.list("api:never-used").await.unwrap().is_empty());
        assert!(log.is_empty("api:never-used").await.unwrap());
    }

    /// The tail read is an optimisation, so its only real contract is that it
    /// cannot be told apart from the full read. Sized past `TAIL_BYTES` so the
    /// seek genuinely lands mid-line and the partial fragment has to be dropped.
    #[tokio::test]
    async fn a_windowed_read_of_a_long_transcript_matches_reading_all_of_it() {
        let (log, _home) = log("tail");
        let filler = "x".repeat(2_000);
        // ~600 messages × ~2KB ≈ 1.2MB, comfortably past the 512KB tail.
        for i in 0..300 {
            log.append("api:s", &Message::user(format!("q{i} {filler}")))
                .await
                .unwrap();
            log.append("api:s", &assistant(&format!("a{i} {filler}")))
                .await
                .unwrap();
        }
        assert!(
            std::fs::metadata(log.path_for("api:s")).unwrap().len() > TAIL_BYTES,
            "the file must exceed the tail for this to test anything"
        );

        // The point of the change: this window is served from the tail, not by
        // reading a megabyte to discard all but 40 messages.
        assert!(
            log.tail_messages("api:s", 40).await.unwrap().is_some(),
            "a windowed read must be served from the tail"
        );
        // And a window the tail cannot hold falls back rather than answering
        // short — ~2KB a message, so 400 of them do not fit in 512KB.
        assert!(
            log.tail_messages("api:s", 400).await.unwrap().is_none(),
            "a window larger than the tail must fall back to the full read"
        );

        for limit in [1usize, 5, 40] {
            let windowed = log.window("api:s", limit).await.unwrap();
            let mut full = log.list("api:s").await.unwrap();
            full.drain(..full.len() - limit);
            assert_eq!(
                windowed.iter().map(|m| &m.content).collect::<Vec<_>>(),
                full.iter().map(|m| &m.content).collect::<Vec<_>>(),
                "tail read disagreed with the full read at limit {limit}"
            );
        }
    }

    /// The fold rules have to hold on the tail too — they resolve against the
    /// most recent user message, which a window always contains.
    #[tokio::test]
    async fn folding_still_applies_to_a_tail_read() {
        let (log, _home) = log("tail-fold");
        let filler = "y".repeat(2_000);
        for i in 0..300 {
            log.append("api:s", &Message::user(format!("q{i} {filler}")))
                .await
                .unwrap();
            log.append("api:s", &assistant(&format!("a{i} {filler}")))
                .await
                .unwrap();
        }
        log.append("api:s", &Message::user("cancel me"))
            .await
            .unwrap();
        log.record_cancelled_turn("api:s").await.unwrap();
        log.append("api:s", &Message::user("kept")).await.unwrap();
        log.record_interjection("api:s", "and this").await.unwrap();

        let windowed = log.window("api:s", 2).await.unwrap();
        assert_eq!(windowed.len(), 2);
        assert!(
            windowed[0].content.starts_with("a299"),
            "the cancelled turn must be gone, got {:?}",
            windowed[0].content
        );
        assert_eq!(windowed[1].content, "kept\nand this");
    }

    #[tokio::test]
    async fn the_window_keeps_the_last_n_in_order() {
        let (log, _home) = log("window");
        for text in ["a", "b", "c", "d"] {
            log.append("api:s", &assistant(text)).await.unwrap();
        }
        let got: Vec<String> = log
            .window("api:s", 2)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(got, ["c", "d"], "the most recent two, oldest first");

        // 0 is "no window", the same meaning `find_windowed` gives it.
        assert_eq!(log.window("api:s", 0).await.unwrap().len(), 4);
    }

    /// User messages a reader sees, counted off the projection — the shape
    /// assertions below are about what `fold` resolves to, not about the file.
    async fn user_turns(log: &MessageLog, session_id: &str) -> usize {
        log.list(session_id)
            .await
            .unwrap()
            .iter()
            .filter(|m| m.role == Role::User)
            .count()
    }

    #[tokio::test]
    async fn a_cancelled_turn_leaves_the_transcript_as_if_it_never_happened() {
        let (log, _home) = log("cancel");
        log.append("api:s", &Message::user("first")).await.unwrap();
        log.append("api:s", &assistant("answered")).await.unwrap();
        log.append("api:s", &Message::user("oops")).await.unwrap();

        log.record_cancelled_turn("api:s").await.unwrap();

        let left: Vec<String> = log
            .list("api:s")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(left, ["first", "answered"]);
        // What a reader sees never ends on a user message with no reply — that
        // is the shape providers reject on replay.
        assert_eq!(user_turns(&log, "api:s").await, 1);

        // The line is still in the file: the projection hides the turn, the log
        // does not lose it.
        let raw = tokio::fs::read_to_string(log.path_for("api:s"))
            .await
            .unwrap();
        assert!(
            raw.contains("\"oops\""),
            "the log still records what was said"
        );
        assert!(raw.contains(KIND_CANCELLED));
    }

    #[tokio::test]
    async fn a_cancelled_turn_takes_its_own_tool_work_with_it() {
        let (log, _home) = log("cancel_tools");
        log.append("api:s", &Message::user("go")).await.unwrap();
        log.append_tool("api:s", &tool("shell")).await.unwrap();
        log.record_cancelled_turn("api:s").await.unwrap();

        assert!(log.list("api:s").await.unwrap().is_empty());
        assert!(
            log.tools("api:s").await.unwrap().is_empty(),
            "tool lines after the cancelled user message belong to that turn"
        );
    }

    #[tokio::test]
    async fn a_cancel_with_nothing_to_cancel_is_a_no_op() {
        let (log, _home) = log("cancel_empty");
        log.record_cancelled_turn("api:s").await.unwrap();
        assert!(log.list("api:s").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_interjection_joins_the_last_user_message() {
        let (log, _home) = log("interject");
        log.append("api:s", &Message::user("do the thing"))
            .await
            .unwrap();
        log.append("api:s", &assistant("working")).await.unwrap();

        log.record_interjection("api:s", "wait, also this")
            .await
            .unwrap();

        let messages = log.list("api:s").await.unwrap();
        assert_eq!(messages[0].content, "do the thing\nwait, also this");
        assert_eq!(messages.len(), 2, "the assistant message is untouched");
        assert_eq!(
            messages[1].role,
            Role::Assistant,
            "an interjection never becomes a second user message"
        );

        // Nothing to merge into is not an error — the fact is recorded either
        // way, and the projection simply has nowhere to put it.
        log.record_interjection("api:other", "x").await.unwrap();
        assert!(log.list("api:other").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rotating_moves_the_transcript_to_the_archived_id() {
        let (log, _home) = log("rotate");
        log.append("api:s", &assistant("old talk")).await.unwrap();
        log.rename("api:s", "api:s-archived").await.unwrap();

        assert!(log.list("api:s").await.unwrap().is_empty());
        assert_eq!(
            log.list("api:s-archived").await.unwrap()[0].content,
            "old talk"
        );
    }

    fn tool(name: &str) -> ToolEntry {
        ToolEntry {
            name: name.to_string(),
            args: "{}".to_string(),
            result: "done".to_string(),
            ok: true,
            elapsed_ms: 5,
            timestamp: 0,
        }
    }

    /// The whole point of recording tools here: the file holds the work, and
    /// the model's history does not change by one byte because of it.
    #[tokio::test]
    async fn tool_records_share_the_file_but_not_the_history() {
        let (log, _home) = log("tools");
        log.append("api:s", &Message::user("count the files"))
            .await
            .unwrap();
        log.append_tool("api:s", &tool("shell")).await.unwrap();
        log.append_tool("api:s", &tool("read")).await.unwrap();
        log.append("api:s", &assistant("15")).await.unwrap();

        let said: Vec<String> = log
            .list("api:s")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(said, ["count the files", "15"], "tools are not messages");

        let ran: Vec<String> = log
            .tools("api:s")
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(ran, ["shell", "read"], "and the work is still on file");

        // The user-turn count the review cadence rides on must not see them.
        assert_eq!(user_turns(&log, "api:s").await, 1);
    }

    /// A window of N messages is N *messages*, however many tool lines sit
    /// between them — otherwise a tool-heavy turn would silently shrink the
    /// history the model is given.
    #[tokio::test]
    async fn the_window_counts_messages_not_lines() {
        let (log, _home) = log("window-tools");
        for text in ["a", "b", "c"] {
            log.append("api:s", &assistant(text)).await.unwrap();
            log.append_tool("api:s", &tool("shell")).await.unwrap();
        }
        let got: Vec<String> = log
            .window("api:s", 2)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(got, ["b", "c"]);
    }

    /// Taking a turn back takes its tool calls with it: they are that turn's
    /// work, and leaving them would attribute it to the turn before.
    #[tokio::test]
    async fn rolling_back_a_turn_drops_the_tools_it_ran() {
        let (log, _home) = log("rollback-tools");
        log.append("api:s", &Message::user("first")).await.unwrap();
        log.append_tool("api:s", &tool("early")).await.unwrap();
        log.append("api:s", &Message::user("cancelled"))
            .await
            .unwrap();
        log.append_tool("api:s", &tool("late")).await.unwrap();

        log.record_cancelled_turn("api:s").await.unwrap();
        let said: Vec<String> = log
            .list("api:s")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(said, ["first"]);
        let ran: Vec<String> = log
            .tools("api:s")
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(ran, ["early"], "the cancelled turn's tool call went too");
    }

    /// An interjection merges into the last user message and leaves the tool
    /// lines around it exactly where they were.
    #[tokio::test]
    async fn an_interjection_leaves_tool_records_in_place() {
        let (log, _home) = log("interject-tools");
        log.append("api:s", &Message::user("go")).await.unwrap();
        log.append_tool("api:s", &tool("shell")).await.unwrap();

        log.record_interjection("api:s", "and hurry").await.unwrap();
        assert_eq!(log.list("api:s").await.unwrap()[0].content, "go\nand hurry");
        assert_eq!(log.tools("api:s").await.unwrap().len(), 1);
    }

    /// A process killed mid-append leaves a truncated last line. The rest of the
    /// transcript has to survive it — losing one message is bad, losing the
    /// conversation because of one message is worse.
    #[tokio::test]
    async fn a_truncated_final_line_costs_only_itself() {
        let (log, _home) = log("torn");
        log.append("api:s", &assistant("first")).await.unwrap();
        log.append("api:s", &assistant("second")).await.unwrap();

        let path = log.path_for("api:s");
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str("{\"v\":1,\"role\":\"assist");
        std::fs::write(&path, text).unwrap();

        let got: Vec<String> = log
            .list("api:s")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(got, ["first", "second"]);
    }

    /// The point of the format: a field added later reads as its default on
    /// every line written before it existed, with no migration and no reset.
    #[tokio::test]
    async fn a_line_missing_a_later_field_still_loads() {
        let (log, _home) = log("additive");
        let path = log.path_for("api:s");
        // A line as an older komo would have written it: no `tool_note`.
        std::fs::write(
            &path,
            "{\"v\":1,\"role\":\"assistant\",\"content\":\"hi\",\"timestamp\":7}\n",
        )
        .unwrap();

        let messages = log.list("api:s").await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "hi");
        assert_eq!(messages[0].tool_note, "", "absent reads as the default");
    }

    /// A line from a komo that writes a shape this one does not understand is
    /// skipped rather than half-read.
    #[tokio::test]
    async fn a_line_from_a_newer_komo_is_skipped() {
        let (log, _home) = log("newer");
        let path = log.path_for("api:s");
        std::fs::write(
            &path,
            "{\"v\":99,\"role\":\"assistant\",\"content\":\"from the future\",\"timestamp\":0}\n\
             {\"v\":1,\"role\":\"assistant\",\"content\":\"mine\",\"timestamp\":0}\n",
        )
        .unwrap();

        let messages = log.list("api:s").await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "mine");
    }

    /// Session ids carry `:` and whatever a chat platform puts in an id; the
    /// file name has to survive that and stay reversible.
    #[test]
    fn session_ids_become_readable_file_names() {
        let (log, home) = log("names");
        assert_eq!(
            log.path_for("api:1234"),
            home.join("sessions").join("api%3A1234.jsonl")
        );
        assert_eq!(
            log.path_for("feishu:oc_9/x"),
            home.join("sessions").join("feishu%3Aoc_9%2Fx.jsonl")
        );
    }
}
