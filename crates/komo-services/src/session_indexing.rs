//! Semantic search over komo's own transcripts — episodic memory, as opposed
//! to the semantic memory in `memory.db` and the procedural memory in skills.
//!
//! The `session` tool could already scan a transcript for a substring, which
//! answers "find the message containing this exact string" and nothing else.
//! Two things it structurally cannot do:
//!
//! - **Match across languages.** A Chinese question and an English transcript
//!   share no terms by construction — CJK bigrams and ASCII words can never be
//!   equal. This is the same gap the semantic arm of memory recall exists to
//!   close, and it fails the same way: silently, as "no matches".
//! - **Look past one conversation.** "Why did we decide against rig?" is a
//!   question about *some* past session, and a search that needs the session id
//!   up front cannot answer it.
//!
//! So transcripts get the same treatment the note vault gets: chunk, embed,
//! and query a [`ChunkIndex`] hybrid (dense ∪ BM25). The index is derived and
//! disposable — every chunk is reproducible from the transcript files, which
//! are the store of record.
//!
//! **The unit is a turn, not a message.** "嗯，那就不用 rig 了" embeds into
//! almost nothing on its own; paired with the reply that prompted it, it
//! embeds into what the exchange was about. A chunk is therefore one user
//! message plus everything that followed it up to the next one.

use std::collections::HashMap;
use std::sync::Arc;

use komo_core::domain::{
    chunk_index::{ChunkHit, ChunkIndex, IndexedChunk},
    embedding::EmbeddingClient,
    message::{Message, Role},
    session::Session,
};

/// Chunks embedded per request — a batch is far faster than the same chunks one
/// at a time, and bounded so a long backlog cannot build one huge request.
const EMBED_BATCH: usize = 32;

/// Most chunks one search will bring the index up to date by.
///
/// Catch-up happens on the search path, so that a session komo never searches
/// costs nothing to keep indexed — the same bargain the note vault makes. The
/// bound is what keeps that from turning the first search after a long gap
/// into a multi-minute tool call: the backlog is worked newest-first, so the
/// turns most likely to be asked about are indexed first, and what is left over
/// is picked up by the next search.
const CATCHUP_CHUNK_BUDGET: usize = 200;

/// Longest text one turn-chunk carries. A turn that pasted a 200 KB file should
/// contribute its shape to the index, not its bulk.
const CHUNK_TEXT_CAP: usize = 2000;

/// Cosine floor for the dense arm, matching memory recall's
/// [`RECALL_SEMANTIC_FLOOR`](komo_core::domain::memory::RECALL_SEMANTIC_FLOOR):
/// the same embedding model, the same question of what counts as related.
const SEARCH_SEMANTIC_FLOOR: f32 = 0.45;

/// Turn a session's projected transcript into one chunk per turn.
///
/// `ordinal` is the **1-based index of the turn's opening user message**, which
/// is deliberately the same coordinate `session` `show` takes as its `offset`:
/// a hit can be read in full without translating between two numbering schemes.
///
/// Messages before the first user message (a system preamble) belong to no turn
/// and are skipped rather than attached to the first one.
pub fn chunk_session(session_id: &str, messages: &[Message]) -> Vec<IndexedChunk> {
    let mut chunks = Vec::new();
    let mut turn: Option<(usize, i64, String)> = None;

    let flush = |turn: Option<(usize, i64, String)>, chunks: &mut Vec<IndexedChunk>| {
        let Some((ordinal, timestamp, text)) = turn else {
            return;
        };
        let text = text.trim().to_string();
        if text.is_empty() {
            return;
        }
        chunks.push(IndexedChunk {
            id: IndexedChunk::make_id(session_id, ordinal),
            path: session_id.to_string(),
            // The turn's date, which is embedded along with the body (see
            // `flush`) and shown on the hit. "When" is a primary cue for
            // episodic recall — "that thing from August" has to have something
            // to match against — and it is the only locating information a
            // transcript chunk has, since it has no headings to sit under.
            heading_path: local_date(timestamp),
            ordinal,
            text: cap(&text),
            mtime: timestamp,
            embedding: Vec::new(),
            embedding_model: String::new(),
        });
    };

    for (idx, message) in messages.iter().enumerate() {
        match message.role {
            Role::User => {
                flush(turn.take(), &mut chunks);
                turn = Some((idx + 1, message.timestamp, render(message)));
            }
            _ => {
                if let Some((_, _, text)) = turn.as_mut() {
                    text.push('\n');
                    text.push_str(&render(message));
                }
            }
        }
    }
    flush(turn.take(), &mut chunks);
    chunks
}

/// One message as the index sees it: role-tagged, with the tool note folded in.
///
/// The note is included because it is often the only record that work happened
/// at all — "which command did we run to fix that?" is answerable from the note
/// and from nothing else in the transcript.
fn render(message: &Message) -> String {
    let role = match message.role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::System => "system",
        Role::Tool => "tool",
    };
    if message.tool_note.is_empty() {
        format!("{role}: {}", message.content)
    } else {
        format!("{role}: {}\n{}", message.content, message.tool_note)
    }
}

fn cap(text: &str) -> String {
    if text.chars().count() <= CHUNK_TEXT_CAP {
        return text.to_string();
    }
    text.chars().take(CHUNK_TEXT_CAP).collect::<String>() + " …[truncated]"
}

fn local_date(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%Y-%m-%d")
                .to_string()
        })
        .unwrap_or_default()
}

/// Hybrid search over every stored transcript, with the index brought up to
/// date first.
pub struct SessionSearch {
    index: Arc<dyn ChunkIndex>,
    embedder: Arc<dyn EmbeddingClient>,
}

impl SessionSearch {
    pub fn new(index: Arc<dyn ChunkIndex>, embedder: Arc<dyn EmbeddingClient>) -> Self {
        Self { index, embedder }
    }

    /// Index whatever `sessions` hold that the index does not, newest session
    /// first, stopping at [`CATCHUP_CHUNK_BUDGET`].
    ///
    /// Returns how many chunks were written. Callers treat a failure as "the
    /// index is as good as it is" and search anyway — a stale index answers
    /// worse, an unavailable embedder must not answer "no matches".
    pub async fn refresh(&self, sessions: &[Session]) -> anyhow::Result<usize> {
        let indexed = self.index.indexed().await?;
        let mut pending: Vec<IndexedChunk> = Vec::new();
        let mut budget = CATCHUP_CHUNK_BUDGET;

        // Newest first: a question about last week should not wait behind a
        // year of backlog.
        let mut ordered: Vec<&Session> = sessions.iter().collect();
        ordered.sort_by_key(|s| std::cmp::Reverse(s.created_at));

        let mut stale_paths: Vec<String> = Vec::new();
        for session in ordered {
            if budget == 0 {
                break;
            }
            let chunks = chunk_session(&session.id, &session.messages);
            let known = indexed.get(&session.id);
            let already = known.map(|f| f.chunks).unwrap_or(0);
            // A transcript only ever grows, so the first `already` chunks are
            // the ones already indexed — *unless* the projection shrank, which
            // `fold` can do when a turn is cancelled. Then the tail no longer
            // lines up and the session is re-chunked whole.
            let fresh: Vec<IndexedChunk> = if chunks.len() < already {
                stale_paths.push(session.id.clone());
                chunks
            } else {
                chunks.into_iter().skip(already).collect()
            };
            for chunk in fresh {
                if budget == 0 {
                    break;
                }
                budget -= 1;
                pending.push(chunk);
            }
        }
        if !stale_paths.is_empty() {
            self.index.delete_paths(&stale_paths).await?;
        }

        let mut written = 0usize;
        while !pending.is_empty() {
            let batch: Vec<IndexedChunk> =
                pending.drain(..EMBED_BATCH.min(pending.len())).collect();
            written += self.flush(batch).await?;
        }
        Ok(written)
    }

    async fn flush(&self, mut batch: Vec<IndexedChunk>) -> anyhow::Result<usize> {
        let texts: Vec<String> = batch
            .iter()
            .map(|c| format!("{}\n{}", c.heading_path, c.text))
            .collect();
        let vectors = self.embedder.embed(&texts).await?;
        if vectors.len() != batch.len() {
            anyhow::bail!(
                "embedding backend returned {} vectors for {} chunks",
                vectors.len(),
                batch.len()
            );
        }
        let model = self.embedder.model_id().to_string();
        for (chunk, vector) in batch.iter_mut().zip(vectors) {
            chunk.embedding = vector;
            chunk.embedding_model = model.clone();
        }
        let n = batch.len();
        self.index.upsert(&batch).await?;
        Ok(n)
    }

    /// Top `limit` turns for `query`, optionally narrowed to one session.
    pub async fn search(
        &self,
        query: &str,
        session: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<ChunkHit>> {
        let vector = self
            .embedder
            .embed(&[query.to_string()])
            .await?
            .into_iter()
            .next()
            .unwrap_or_default();
        // Over-fetch when narrowing, since the filter runs after retrieval: a
        // session-scoped search that fetched exactly `limit` would return
        // almost nothing once other sessions' hits are dropped.
        let fetch = match session {
            Some(_) => limit * 8,
            None => limit,
        };
        let hits = self
            .index
            .search(&vector, query, fetch, SEARCH_SEMANTIC_FLOOR)
            .await?;
        Ok(hits
            .into_iter()
            .filter(|h| session.is_none_or(|s| h.chunk.path == s))
            .take(limit)
            .collect())
    }

    /// How many turns are indexed, for diagnosis.
    pub async fn count(&self) -> anyhow::Result<usize> {
        self.index.count().await
    }

    /// What is indexed per session, keyed by session id.
    pub async fn indexed(
        &self,
    ) -> anyhow::Result<HashMap<String, komo_core::domain::chunk_index::IndexedFile>> {
        self.index.indexed().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::chunk_index::IndexedFile;
    use std::sync::Mutex;

    fn user(text: &str, ts: i64) -> Message {
        let mut m = Message::user(text);
        m.timestamp = ts;
        m
    }

    fn assistant(text: &str, ts: i64) -> Message {
        let mut m = Message::assistant(text);
        m.timestamp = ts;
        m
    }

    /// A system preamble — no constructor for one, since nothing but a test
    /// writes one into a transcript.
    fn system(text: &str) -> Message {
        Message {
            role: Role::System,
            content: text.into(),
            timestamp: 0,
            tool_note: String::new(),
        }
    }

    #[test]
    fn a_chunk_is_a_turn_not_a_message() {
        let messages = vec![
            user("要不要用 rig?", 100),
            assistant("不建议，会侵入 runtime", 101),
            user("那就不用", 102),
            assistant("好的", 103),
        ];
        let chunks = chunk_session("cli:s", &messages);

        assert_eq!(chunks.len(), 2, "two user turns, two chunks");
        assert!(chunks[0].text.contains("要不要用 rig?"));
        assert!(
            chunks[0].text.contains("侵入 runtime"),
            "the reply is what makes a short question searchable: {}",
            chunks[0].text
        );
        assert!(chunks[1].text.contains("那就不用"));
    }

    /// The hit → `show` handoff: a chunk's ordinal is the 1-based index of its
    /// opening user message, which is exactly what `show`'s `offset` takes.
    #[test]
    fn a_chunks_ordinal_is_its_user_messages_show_offset() {
        let messages = vec![
            system("preamble"),
            user("first", 100),
            assistant("reply", 101),
            user("second", 102),
        ];
        let chunks = chunk_session("cli:s", &messages);

        assert_eq!(chunks[0].ordinal, 2, "messages[1] is `show` offset 2");
        assert_eq!(chunks[1].ordinal, 4);
        assert!(
            !chunks[0].text.contains("preamble"),
            "a system preamble belongs to no turn"
        );
    }

    #[test]
    fn tool_notes_are_indexed_because_nothing_else_records_the_work() {
        let mut reply = assistant("done", 101);
        reply.tool_note = "1. shell cargo test → ok".into();
        let chunks = chunk_session("cli:s", &[user("run the tests", 100), reply]);

        assert!(chunks[0].text.contains("cargo test"), "{}", chunks[0].text);
    }

    #[test]
    fn a_session_with_no_user_turn_produces_nothing() {
        assert!(chunk_session("cli:s", &[system("only a preamble")]).is_empty());
        assert!(chunk_session("cli:s", &[]).is_empty());
    }

    #[test]
    fn ids_are_stable_so_reindexing_upserts_rather_than_duplicates() {
        let messages = vec![user("hi", 100), assistant("hello", 101)];
        let first = chunk_session("cli:s", &messages);
        let again = chunk_session("cli:s", &messages);
        assert_eq!(first[0].id, again[0].id);
        assert_eq!(first[0].id, "cli:s#1");
    }

    /// Records what it was asked to store and hands back canned hits.
    #[derive(Default)]
    struct FakeIndex {
        chunks: Mutex<Vec<IndexedChunk>>,
        deleted: Mutex<Vec<String>>,
        hits: Mutex<Vec<ChunkHit>>,
        last_query: Mutex<Option<(String, usize)>>,
    }

    #[async_trait]
    impl ChunkIndex for FakeIndex {
        async fn upsert(&self, chunks: &[IndexedChunk]) -> anyhow::Result<()> {
            self.chunks.lock().unwrap().extend_from_slice(chunks);
            Ok(())
        }
        async fn search(
            &self,
            _query: &[f32],
            query_text: &str,
            limit: usize,
            _min_score: f32,
        ) -> anyhow::Result<Vec<ChunkHit>> {
            *self.last_query.lock().unwrap() = Some((query_text.to_string(), limit));
            Ok(self.hits.lock().unwrap().clone())
        }
        async fn indexed(&self) -> anyhow::Result<HashMap<String, IndexedFile>> {
            let mut out: HashMap<String, IndexedFile> = HashMap::new();
            for chunk in self.chunks.lock().unwrap().iter() {
                let entry = out.entry(chunk.path.clone()).or_insert(IndexedFile {
                    mtime: 0,
                    chunks: 0,
                });
                entry.chunks += 1;
                entry.mtime = entry.mtime.max(chunk.mtime);
            }
            Ok(out)
        }
        async fn delete_paths(&self, paths: &[String]) -> anyhow::Result<()> {
            self.deleted.lock().unwrap().extend_from_slice(paths);
            self.chunks
                .lock()
                .unwrap()
                .retain(|c| !paths.contains(&c.path));
            Ok(())
        }
        async fn count(&self) -> anyhow::Result<usize> {
            Ok(self.chunks.lock().unwrap().len())
        }
        async fn reset(&self) -> anyhow::Result<()> {
            self.chunks.lock().unwrap().clear();
            Ok(())
        }
        async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>> {
            Ok(None)
        }
    }

    struct FakeEmbedder;

    #[async_trait]
    impl EmbeddingClient for FakeEmbedder {
        async fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
        }
        fn model_id(&self) -> &str {
            "fake-model"
        }
    }

    fn session_with(id: &str, created_at: i64, messages: Vec<Message>) -> Session {
        let mut s = Session::new(id);
        s.created_at = created_at;
        s.messages = messages;
        s
    }

    fn search_over(index: Arc<FakeIndex>) -> SessionSearch {
        SessionSearch::new(index, Arc::new(FakeEmbedder))
    }

    #[tokio::test]
    async fn refresh_indexes_only_the_turns_it_has_not_seen() {
        let index = Arc::new(FakeIndex::default());
        let search = search_over(index.clone());
        let mut messages = vec![user("one", 100), assistant("a", 101)];
        let sessions = vec![session_with("cli:s", 1, messages.clone())];

        assert_eq!(search.refresh(&sessions).await.unwrap(), 1);

        // A new turn arrives; only it is embedded.
        messages.push(user("two", 102));
        messages.push(assistant("b", 103));
        let sessions = vec![session_with("cli:s", 1, messages)];
        assert_eq!(
            search.refresh(&sessions).await.unwrap(),
            1,
            "an append must not re-embed the whole conversation"
        );
        assert_eq!(index.chunks.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn an_unchanged_session_costs_nothing() {
        let index = Arc::new(FakeIndex::default());
        let search = search_over(index.clone());
        let sessions = vec![session_with(
            "cli:s",
            1,
            vec![user("one", 100), assistant("a", 101)],
        )];

        search.refresh(&sessions).await.unwrap();
        assert_eq!(search.refresh(&sessions).await.unwrap(), 0);
    }

    /// `fold` can shrink a projection — a cancelled turn reads as if it never
    /// happened. The tail then no longer lines up with what was indexed, so the
    /// session is re-chunked whole rather than left with orphaned chunks.
    #[tokio::test]
    async fn a_shrunken_transcript_is_reindexed_whole() {
        let index = Arc::new(FakeIndex::default());
        let search = search_over(index.clone());
        let sessions = vec![session_with(
            "cli:s",
            1,
            vec![
                user("one", 100),
                assistant("a", 101),
                user("two", 102),
                assistant("b", 103),
            ],
        )];
        search.refresh(&sessions).await.unwrap();
        assert_eq!(index.chunks.lock().unwrap().len(), 2);

        // The second turn was cancelled and no longer projects.
        let shrunk = vec![session_with(
            "cli:s",
            1,
            vec![user("one", 100), assistant("a", 101)],
        )];
        search.refresh(&shrunk).await.unwrap();

        assert_eq!(*index.deleted.lock().unwrap(), vec!["cli:s".to_string()]);
        assert_eq!(
            index.chunks.lock().unwrap().len(),
            1,
            "the orphaned chunk is gone, not left to be searched"
        );
    }

    #[tokio::test]
    async fn catch_up_works_the_newest_sessions_first_and_stops_at_the_budget() {
        let index = Arc::new(FakeIndex::default());
        let search = search_over(index.clone());
        // Two sessions, each holding more turns than the whole budget.
        let many = |base: i64| {
            (0..CATCHUP_CHUNK_BUDGET)
                .flat_map(|i| {
                    let t = base + i as i64;
                    [user(&format!("q{i}"), t), assistant("a", t)]
                })
                .collect::<Vec<_>>()
        };
        let sessions = vec![
            session_with("cli:old", 1, many(0)),
            session_with("cli:new", 9, many(10_000)),
        ];

        let written = search.refresh(&sessions).await.unwrap();

        assert_eq!(written, CATCHUP_CHUNK_BUDGET, "the budget is a hard stop");
        let chunks = index.chunks.lock().unwrap();
        assert!(
            chunks.iter().all(|c| c.path == "cli:new"),
            "the newest conversation is indexed first — a question about last \
             week must not wait behind a year of backlog"
        );
    }

    #[tokio::test]
    async fn a_session_scoped_search_over_fetches_because_it_filters_after_retrieval() {
        let index = Arc::new(FakeIndex::default());
        let hit = |path: &str| ChunkHit {
            chunk: IndexedChunk {
                id: IndexedChunk::make_id(path, 1),
                path: path.into(),
                heading_path: String::new(),
                ordinal: 1,
                text: "t".into(),
                mtime: 0,
                embedding: Vec::new(),
                embedding_model: String::new(),
            },
            score: 1.0,
        };
        *index.hits.lock().unwrap() = vec![hit("cli:a"), hit("cli:b"), hit("cli:a")];
        let search = search_over(index.clone());

        let scoped = search.search("q", Some("cli:a"), 5).await.unwrap();
        assert_eq!(scoped.len(), 2);
        assert!(scoped.iter().all(|h| h.chunk.path == "cli:a"));
        assert_eq!(
            index.last_query.lock().unwrap().as_ref().unwrap().1,
            40,
            "narrowing has to over-fetch, or the filter empties the result"
        );

        let all = search.search("q", None, 5).await.unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(index.last_query.lock().unwrap().as_ref().unwrap().1, 5);
    }

    #[tokio::test]
    async fn the_raw_query_text_reaches_the_index_for_its_lexical_arm() {
        let index = Arc::new(FakeIndex::default());
        let search = search_over(index.clone());
        search.search("meta_x_request_id", None, 5).await.unwrap();
        assert_eq!(
            index.last_query.lock().unwrap().as_ref().unwrap().0,
            "meta_x_request_id",
            "an exact identifier is what the lexical arm is for"
        );
    }
}
