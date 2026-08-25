//! A [`ChunkIndex`] that opens its backend on first use.
//!
//! Wiring is one-shot — the tool catalog is frozen once the agent is built — so
//! an index that refuses to open at startup otherwise costs `wiki_search` for
//! the whole life of the process. That failure is usually *outside* komo and
//! gets fixed while the gateway keeps running: a Qdrant that boots later than
//! the daemon, a NAS that is still coming up, or a local-network permission
//! macOS has not yet granted the launchd job. Opening lazily turns a permanent
//! loss into a per-call retry, because [`OnceCell::get_or_try_init`] caches a
//! success and never an error.
//!
//! Only the *index* is deferred. The embedding client is built synchronously
//! and does no I/O until it embeds, so it has nothing to fail at yet.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use komo_core::domain::chunk_index::{ChunkHit, ChunkIndex, IndexedChunk, IndexedFile};
use tokio::sync::OnceCell;

use crate::{WikiSettings, build_index};

pub struct LazyWikiIndex {
    settings: WikiSettings,
    inner: OnceCell<Arc<dyn ChunkIndex>>,
}

impl LazyWikiIndex {
    pub fn new(settings: WikiSettings) -> Self {
        Self {
            settings,
            inner: OnceCell::new(),
        }
    }

    /// The open index: opening it on the first call, and retrying on every call
    /// after one that failed.
    pub async fn get(&self) -> anyhow::Result<&Arc<dyn ChunkIndex>> {
        self.inner
            .get_or_try_init(|| build_index(&self.settings))
            .await
    }
}

#[async_trait]
impl ChunkIndex for LazyWikiIndex {
    async fn upsert(&self, chunks: &[IndexedChunk]) -> anyhow::Result<()> {
        self.get().await?.upsert(chunks).await
    }

    async fn search(
        &self,
        query: &[f32],
        query_text: &str,
        limit: usize,
        min_score: f32,
    ) -> anyhow::Result<Vec<ChunkHit>> {
        self.get()
            .await?
            .search(query, query_text, limit, min_score)
            .await
    }

    async fn indexed(&self) -> anyhow::Result<HashMap<String, IndexedFile>> {
        self.get().await?.indexed().await
    }

    async fn delete_paths(&self, paths: &[String]) -> anyhow::Result<()> {
        self.get().await?.delete_paths(paths).await
    }

    async fn count(&self) -> anyhow::Result<usize> {
        self.get().await?.count().await
    }

    async fn reset(&self) -> anyhow::Result<()> {
        self.get().await?.reset().await
    }

    async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>> {
        self.get().await?.vector_spec().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unopenable() -> LazyWikiIndex {
        LazyWikiIndex::new(WikiSettings {
            backend: crate::WikiBackend::Server,
            url: "http://127.0.0.1:1".to_string(),
            ..WikiSettings::default()
        })
    }

    /// The whole point: a failed open is not remembered, so the call after a
    /// fix succeeds without restarting the process.
    #[tokio::test]
    async fn a_failed_open_is_retried_rather_than_cached() {
        let lazy = unopenable();
        assert!(lazy.get().await.is_err(), "unreachable server should fail");
        assert!(
            lazy.inner.get().is_none(),
            "a failure must leave the cell empty so the next call retries"
        );
        assert!(lazy.get().await.is_err(), "second attempt still runs");
    }

    /// Construction alone must not touch the network — that is what lets wiring
    /// register the tool before the backend is known to be up.
    #[test]
    fn constructing_opens_nothing() {
        assert!(unopenable().inner.get().is_none());
    }
}
