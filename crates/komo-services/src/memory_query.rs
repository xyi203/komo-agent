//! One hybrid query over the memory library, shared by every read path.
//!
//! Automatic L3 recall matched lexically **∪** semantically, while the model's
//! own `memory search` ran a substring scan over active memories only. Two
//! consequences, both of which defeat the point of giving the model a search
//! tool at all:
//!
//! * A memory recall had just injected — matched across languages, or matched as
//!   a candidate — could not be found again by searching for it. The model was
//!   shown a fact it had no way to reproduce or widen.
//! * Iterative retrieval was impossible. "Recall gave me something adjacent, let
//!   me re-query with better terms" only works when the second query is at least
//!   as capable as the first.
//!
//! So both paths build their query here. The embedding backend and its
//! degradation rules live here too, next to the ranking that consumes them —
//! including the background backfill, so *every* way a memory is written
//! (reviewer, tool, CLI, api) converges on one index-maintenance implementation
//! rather than one per write path.
//!
//! Aux relevance screening is deliberately **not** here: it belongs to automatic
//! injection, where nothing else can judge whether five facts earn their context.
//! On an explicit search the model is the screener — it sees the results and
//! decides — so an aux call there would buy latency and nothing else.

use std::sync::Arc;
use std::time::Duration;

use komo_core::domain::embedding::EmbeddingClient;
use komo_core::domain::memory::{
    Memory, MemoryContext, MemoryRepository, RecallQuery, ScoredMemory, select_recall,
};

/// Budget for embedding one query. Recall runs on the reply's critical path, so
/// past this the query proceeds lexical-only. Generous enough to absorb an Ollama
/// model reload (a warm one answers in ~100 ms); a turn that pays it once leaves
/// the model warm for the rest of the conversation.
const EMBED_TIMEOUT: Duration = Duration::from_secs(3);

/// Memories embedded per background backfill pass. Bounds one batch's work and
/// payload; a large library converges over several turns instead of blocking one
/// on a giant request.
const BACKFILL_BATCH: usize = 32;

/// Builds and runs memory queries. Cheap to clone-share (`Arc`); holds no state
/// beyond its handles.
pub struct MemoryQueryService {
    memories: Arc<dyn MemoryRepository>,
    /// Optional semantic arm. `None` — or any failure at call time — leaves every
    /// query exactly as lexical as it was before embeddings existed.
    embedder: Option<Arc<dyn EmbeddingClient>>,
    embed_timeout: Duration,
    backfill_batch: usize,
}

impl MemoryQueryService {
    pub fn new(memories: Arc<dyn MemoryRepository>) -> Self {
        Self {
            memories,
            embedder: None,
            embed_timeout: EMBED_TIMEOUT,
            backfill_batch: BACKFILL_BATCH,
        }
    }

    /// Attach an embedding backend, giving every query its cross-language arm.
    pub fn with_embedder(mut self, embedder: Arc<dyn EmbeddingClient>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Shrink the embed budget. For tests, which must not wait out the real one.
    pub fn with_embed_timeout(mut self, timeout: Duration) -> Self {
        self.embed_timeout = timeout;
        self
    }

    /// Build a query for `text`: lexical terms always, plus a vector when a
    /// backend is configured and answers in time. Every failure path — no
    /// backend, an error, a timeout, an empty reply — yields the lexical query.
    pub async fn build_query(&self, text: &str) -> RecallQuery {
        let Some(embedder) = &self.embedder else {
            return RecallQuery::lexical(text);
        };
        let batch = [text.to_string()];
        match tokio::time::timeout(self.embed_timeout, embedder.embed(&batch)).await {
            Ok(Ok(mut vectors)) if !vectors.is_empty() => {
                RecallQuery::semantic(text, vectors.remove(0), embedder.model_id())
            }
            Ok(Ok(_)) => {
                tracing::warn!("embedding backend returned no vector — query stays lexical");
                RecallQuery::lexical(text)
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "embedding the query failed — query stays lexical");
                RecallQuery::lexical(text)
            }
            Err(_) => {
                tracing::warn!("embedding the query timed out — query stays lexical");
                RecallQuery::lexical(text)
            }
        }
    }

    /// The in-scope memories most relevant to `text`, best first, at most
    /// `limit` (`0` = uncapped).
    ///
    /// Recall-eligible statuses only (see [`select_recall`]): active memories and
    /// candidates, never archived or rejected ones. Candidates being visible here
    /// is deliberate — they are visible to automatic recall, so a model told
    /// about one must be able to search for it.
    ///
    /// For the per-turn path, which already holds the whole library, use
    /// [`MemoryQueryService::build_query`] with `select_recall` directly instead:
    /// this method's `list()` would be a second full load of the same rows.
    pub async fn lookup(
        &self,
        ctx: &MemoryContext,
        text: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<ScoredMemory>> {
        let all = self.memories.list().await?;
        let query = self.build_query(text).await;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        Ok(select_recall(&all, ctx, &query, limit, now))
    }

    /// Embed up to `backfill_batch` memories that lack a vector for the current
    /// model, in the background.
    ///
    /// Best-effort in every direction: no backend, nothing missing, or a failed
    /// call all leave the store untouched and the affected memories lexical-only
    /// until a later pass retries. Driven from the read path rather than the write
    /// path on purpose — a reader already holds the whole library, and one
    /// implementation then covers every writer plus memories that predate
    /// embeddings entirely.
    pub fn spawn_backfill(&self, all: &[Memory]) {
        let Some(embedder) = self.embedder.clone() else {
            return;
        };
        let model = embedder.model_id().to_string();
        let stale: Vec<Memory> = all
            .iter()
            .filter(|m| m.embedding_for(&model).is_none())
            .take(self.backfill_batch)
            .cloned()
            .collect();
        if stale.is_empty() {
            return;
        }
        let repo = self.memories.clone();
        tokio::spawn(async move {
            let texts: Vec<String> = stale.iter().map(|m| m.content.clone()).collect();
            let vectors = match embedder.embed(&texts).await {
                Ok(vectors) => vectors,
                Err(error) => {
                    tracing::warn!(%error, "memory embedding backfill failed");
                    return;
                }
            };
            let mut written = 0usize;
            for (mut memory, vector) in stale.into_iter().zip(vectors) {
                memory.embedding = vector;
                memory.embedding_model = model.clone();
                // `updated_at` is deliberately untouched: embedding is an index
                // rebuild, not an edit, and bumping it would reset the
                // recency-decay signal for the whole library at once.
                if let Err(error) = repo.save(&memory).await {
                    tracing::warn!(%error, id = %memory.id, "failed to store memory embedding");
                    continue;
                }
                written += 1;
            }
            tracing::debug!(written, model = %model, "memory embedding backfill");
        });
    }

    /// Embed everything that still lacks a current vector, and wait for it.
    ///
    /// The background pass above is deliberately lazy — it embeds one batch per
    /// read so a turn never waits on the model. That is right for a store that
    /// gains memories a few at a time, and wrong for one that has never been
    /// embedded at all: at a batch per read, a library of any size stays
    /// half-lexical for days, and cross-language recall is broken the whole
    /// time. This is the operator's way to say "do it now".
    ///
    /// Returns how many memories were embedded. A batch that fails to embed
    /// stops the run rather than spinning: whatever went wrong with the model
    /// will go wrong with the next batch too.
    pub async fn backfill_all(&self) -> anyhow::Result<usize> {
        let Some(embedder) = self.embedder.clone() else {
            anyhow::bail!(
                "no embedding model is configured — set `[memory] embedding_model` \
                 (and `embedding_url`) in ~/.komo/config.toml"
            );
        };
        let model = embedder.model_id().to_string();
        let mut written = 0usize;

        loop {
            let all = self.memories.list().await?;
            let stale: Vec<Memory> = all
                .into_iter()
                .filter(|m| m.embedding_for(&model).is_none())
                .take(self.backfill_batch)
                .collect();
            if stale.is_empty() {
                break;
            }

            let texts: Vec<String> = stale.iter().map(|m| m.content.clone()).collect();
            let vectors = embedder
                .embed(&texts)
                .await
                .map_err(|e| anyhow::anyhow!("embedding failed after {written} memories: {e}"))?;

            let mut wrote_this_round = 0usize;
            for (mut memory, vector) in stale.into_iter().zip(vectors) {
                memory.embedding = vector;
                memory.embedding_model = model.clone();
                // Same reasoning as the background pass: an embedding is an
                // index rebuild, not an edit, so `updated_at` stays put.
                self.memories.save(&memory).await?;
                wrote_this_round += 1;
            }
            written += wrote_this_round;
            // Nothing moved despite stale rows: saving is not sticking, and
            // looping would never terminate.
            if wrote_this_round == 0 {
                break;
            }
        }
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::memory::{MemoryKind, MemoryStatus};
    use std::sync::Mutex;

    struct FakeStore {
        memories: Vec<Memory>,
        saved: Arc<Mutex<Vec<Memory>>>,
    }

    impl FakeStore {
        fn new(memories: Vec<Memory>) -> Self {
            Self {
                memories,
                saved: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl MemoryRepository for FakeStore {
        async fn save(&self, memory: &Memory) -> anyhow::Result<()> {
            self.saved.lock().unwrap().push(memory.clone());
            Ok(())
        }
        async fn list(&self) -> anyhow::Result<Vec<Memory>> {
            Ok(self.memories.clone())
        }
    }

    struct FakeEmbedder {
        vector: Vec<f32>,
        fail: bool,
        hang: bool,
    }

    #[async_trait]
    impl EmbeddingClient for FakeEmbedder {
        async fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            if self.hang {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            if self.fail {
                anyhow::bail!("backend down");
            }
            Ok(texts.iter().map(|_| self.vector.clone()).collect())
        }
        fn model_id(&self) -> &str {
            "fake-model"
        }
    }

    fn embedder(vector: Vec<f32>) -> Arc<dyn EmbeddingClient> {
        Arc::new(FakeEmbedder {
            vector,
            fail: false,
            hang: false,
        })
    }

    fn memory(content: &str, status: MemoryStatus) -> Memory {
        let mut m = Memory::new(MemoryKind::Fact, content);
        m.status = status;
        m
    }

    fn embedded(content: &str, status: MemoryStatus, vector: Vec<f32>) -> Memory {
        let mut m = memory(content, status);
        m.embedding = vector;
        m.embedding_model = "fake-model".into();
        m
    }

    fn ctx() -> MemoryContext {
        MemoryContext::local("s1")
    }

    #[tokio::test]
    async fn lookup_matches_lexically() {
        let service = MemoryQueryService::new(Arc::new(FakeStore::new(vec![
            memory(
                "durable kanban tasks live in kanban.db",
                MemoryStatus::Active,
            ),
            memory("unrelated weather note", MemoryStatus::Active),
        ])));
        let hits = service
            .lookup(&ctx(), "where do kanban tasks live?", 5)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].memory.content.contains("kanban.db"));
    }

    /// The defect this service exists to remove: the explicit search path now
    /// matches across languages, exactly as automatic recall does.
    #[tokio::test]
    async fn lookup_matches_across_languages_through_the_semantic_arm() {
        let store = FakeStore::new(vec![embedded(
            "User communicates in Chinese.",
            MemoryStatus::Active,
            vec![1.0, 0.0],
        )]);
        let service =
            MemoryQueryService::new(Arc::new(store)).with_embedder(embedder(vec![1.0, 0.0]));
        let hits = service
            .lookup(&ctx(), "我平时用什么语言跟你说话", 5)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "the semantic arm admits what lexical cannot");
    }

    /// Candidates are recallable, so they must be searchable — a model told
    /// about a candidate could previously never find it again.
    #[tokio::test]
    async fn lookup_sees_candidates_but_not_archived_or_rejected() {
        let store = FakeStore::new(vec![
            memory("kanban tasks live in kanban.db", MemoryStatus::Candidate),
            memory("kanban was archived long ago", MemoryStatus::Archived),
            memory("kanban was rejected outright", MemoryStatus::Rejected),
        ]);
        let hits = MemoryQueryService::new(Arc::new(store))
            .lookup(&ctx(), "kanban", 5)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].memory.status, MemoryStatus::Candidate);
    }

    /// Every way the backend can let a query down leaves it lexical, never worse.
    #[tokio::test]
    async fn an_embedding_failure_degrades_to_lexical() {
        for (fail, hang) in [(true, false), (false, true)] {
            let store = FakeStore::new(vec![memory(
                "durable kanban tasks live in kanban.db",
                MemoryStatus::Active,
            )]);
            let service = MemoryQueryService::new(Arc::new(store))
                .with_embedder(Arc::new(FakeEmbedder {
                    vector: vec![1.0, 0.0],
                    fail,
                    hang,
                }))
                .with_embed_timeout(Duration::from_millis(20));
            let hits = service.lookup(&ctx(), "kanban tasks", 5).await.unwrap();
            assert_eq!(hits.len(), 1, "lexical matching still works");
        }
    }

    #[tokio::test]
    async fn backfill_embeds_only_what_lacks_a_current_vector() {
        let store = FakeStore::new(Vec::new());
        let saved = store.saved.clone();
        let service =
            MemoryQueryService::new(Arc::new(store)).with_embedder(embedder(vec![0.0, 1.0]));

        let missing = memory("needs a vector", MemoryStatus::Active);
        let current = embedded("already has one", MemoryStatus::Active, vec![0.0, 1.0]);
        service.spawn_backfill(&[missing, current]);

        for _ in 0..50 {
            if !saved.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let saved = saved.lock().unwrap();
        assert_eq!(saved.len(), 1, "only the un-embedded memory is re-saved");
        assert_eq!(saved[0].content, "needs a vector");
        assert_eq!(saved[0].embedding_model, "fake-model");
    }

    #[tokio::test]
    async fn backfill_without_a_backend_does_nothing() {
        let store = FakeStore::new(Vec::new());
        let saved = store.saved.clone();
        let service = MemoryQueryService::new(Arc::new(store));
        service.spawn_backfill(&[memory("x", MemoryStatus::Active)]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(saved.lock().unwrap().is_empty());
    }
}
