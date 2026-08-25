//! Embedded backend: `qdrant-edge` running in-process.
//!
//! Two things shape this file.
//!
//! **The API is synchronous.** `EdgeShard` does blocking file I/O, so every call
//! is wrapped in `spawn_blocking`; calling it directly from the async context
//! would stall the executor for the duration of a search.
//!
//! **The shard is created lazily.** A shard's config fixes the vector width at
//! creation, and the width is only known once the first embedded chunk arrives.
//! So an index with no data yet holds `None`, and the first `upsert` creates the
//! shard with that batch's dimensionality. Every read against an
//! uncreated shard answers "empty" rather than failing — asking a
//! never-indexed vault a question is a legitimate state, not an error.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use komo_core::domain::chunk_index::{
    ChunkHit, ChunkIndex, DIVERSIFY_OVERFETCH, IndexedChunk, IndexedFile, MAX_CHUNKS_PER_FILE,
    diversify, reciprocal_rank_fusion,
};
use qdrant_edge::bm25_embed::{EdgeBm25, EdgeBm25Config};
use qdrant_edge::{
    Distance, EdgeConfig, EdgeShard, EdgeSparseVectorParams, EdgeVectorParams, Modifier,
    NamedQuery, Payload, PointId, PointInsertOperations, PointOperations, QueryEnum, ScrollRequest,
    SearchRequestBuilder, TokenizerType, UpdateOperation, VectorInternal, VectorPersisted,
    VectorStructPersisted, WithPayloadInterface, WithVector,
};

use crate::payload::{
    self, F_MODEL, F_MTIME, F_PATH, SPARSE_VECTOR_NAME, VECTOR_NAME, point_id, to_payload,
};

/// Page size for full scans. The request default is 10, which would turn one
/// `indexed()` call into hundreds of round trips.
const SCROLL_PAGE: usize = 1024;

/// Run one arm of a search. Blocking — callers wrap it in `spawn_blocking`.
fn run_query(
    shard: &EdgeShard,
    query: QueryEnum,
    limit: usize,
    min_score: Option<f32>,
) -> qdrant_edge::OperationResult<Vec<qdrant_edge::ScoredPoint>> {
    let mut request =
        SearchRequestBuilder::new(query, limit).with_payload(WithPayloadInterface::Bool(true));
    if let Some(min_score) = min_score {
        request = request.score_threshold(min_score);
    }
    shard.search(request.build())
}

/// Scored points → hits, dropping any whose payload will not decode. A
/// malformed point costs its own result, never the query.
fn to_hits(points: Vec<qdrant_edge::ScoredPoint>) -> Vec<ChunkHit> {
    points
        .into_iter()
        .filter_map(|point| {
            let value = serde_json::to_value(point.payload.as_ref()?).ok()?;
            Some(ChunkHit {
                chunk: payload::from_payload(&value)?,
                score: point.score,
            })
        })
        .collect()
}

pub struct EdgeIndex {
    path: PathBuf,
    /// `None` until the first upsert establishes the vector width — see the
    /// module docs.
    shard: Arc<RwLock<Option<Arc<EdgeShard>>>>,
    /// Lexical arm. Stateless — it turns text into term frequencies; the IDF
    /// half of BM25 is applied by the index itself via [`Modifier::Idf`], which
    /// is why indexing needs only one pass over the vault.
    bm25: EdgeBm25,
}

/// BM25 tuned for a note vault.
///
/// `Multilingual` is the load-bearing choice: the default `Word` tokenizer
/// splits on ASCII word boundaries and would treat a whole Chinese sentence as
/// one token, making the lexical arm useless on most of this vault. Multilingual
/// routes CJK through jieba.
fn bm25_config() -> EdgeBm25Config {
    EdgeBm25Config {
        tokenizer: TokenizerType::Multilingual,
        ..Default::default()
    }
}

impl EdgeIndex {
    /// Open the index under `data_dir/collection`, loading an existing shard if
    /// one is there.
    pub fn open(data_dir: &Path, collection: &str) -> anyhow::Result<Self> {
        let path = data_dir.join(collection);
        let existing = if path.join("segments").exists() || path.join("wal").exists() {
            Some(Arc::new(EdgeShard::load(&path, None).map_err(|e| {
                anyhow!("loading wiki index at {}: {e}", path.display())
            })?))
        } else {
            None
        };
        Ok(Self {
            path,
            shard: Arc::new(RwLock::new(existing)),
            bm25: EdgeBm25::new(bm25_config())
                .map_err(|e| anyhow!("building the BM25 tokenizer: {e}"))?,
        })
    }

    /// Does the open shard carry a sparse column?
    ///
    /// An index built before hybrid search has dense vectors only, and querying
    /// a vector name it does not have is an error. Checking lets an old index
    /// keep working as dense-only until it is rebuilt.
    fn has_sparse(shard: &EdgeShard) -> bool {
        shard
            .config()
            .sparse_vectors
            .contains_key(SPARSE_VECTOR_NAME)
    }

    fn snapshot(&self) -> Option<Arc<EdgeShard>> {
        self.shard.read().ok()?.clone()
    }

    /// Create the shard sized for `dim`, or return the existing one.
    ///
    /// An existing shard of a *different* width is a hard error, not a silent
    /// mismatch: vector width is fixed at creation, so this is what a change of
    /// embedding model looks like from here, and the message has to say what
    /// fixes it.
    fn ensure(&self, dim: usize) -> anyhow::Result<Arc<EdgeShard>> {
        if let Some(shard) = self.snapshot() {
            let existing = shard
                .config()
                .vectors
                .get(VECTOR_NAME)
                .map(|v| v.size)
                .unwrap_or(dim);
            if existing != dim {
                anyhow::bail!(
                    "index was built for {existing}-dim vectors but the embedding \
                     model produces {dim}-dim. Vector width is fixed when the index \
                     is created — run `komo wiki index --rebuild` to rebuild it."
                );
            }
            return Ok(shard);
        }
        let mut guard = self
            .shard
            .write()
            .map_err(|_| anyhow!("wiki index lock poisoned"))?;
        // Another writer may have created it between the read and the write.
        if let Some(shard) = guard.as_ref() {
            return Ok(shard.clone());
        }
        std::fs::create_dir_all(&self.path)
            .with_context(|| format!("creating {}", self.path.display()))?;
        let config = EdgeConfig {
            on_disk_payload: Some(false),
            vectors: HashMap::from([(
                VECTOR_NAME.to_string(),
                EdgeVectorParams {
                    size: dim,
                    // Vectors reach us L2-normalized (the `EmbeddingClient`
                    // contract), so a dot product *is* cosine similarity, and
                    // scores come back directly comparable to a caller's
                    // `min_score` with no conversion.
                    distance: Distance::Dot,
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                    on_disk: None,
                    hnsw_config: None,
                },
            )]),
            sparse_vectors: HashMap::from([(
                SPARSE_VECTOR_NAME.to_string(),
                EdgeSparseVectorParams {
                    full_scan_threshold: None,
                    on_disk: None,
                    // The index applies IDF at query time from its own document
                    // frequencies, so writes carry only term frequencies and one
                    // indexing pass is enough.
                    modifier: Some(Modifier::Idf),
                    datatype: None,
                },
            )]),
            hnsw_config: Default::default(),
            quantization_config: None,
            optimizers: Default::default(),
            wal_options: None,
            max_search_threads: None,
            search_pool_core: None,
        };
        let shard = Arc::new(
            EdgeShard::new(&self.path, config)
                .map_err(|e| anyhow!("creating wiki index at {}: {e}", self.path.display()))?,
        );
        *guard = Some(shard.clone());
        Ok(shard)
    }

    /// Read every point's payload. Both `indexed` and `delete_paths` need the
    /// full set, and at vault scale (a few thousand points) one scan is cheaper
    /// than maintaining a secondary index.
    fn scan(shard: &EdgeShard) -> anyhow::Result<Vec<(PointId, serde_json::Value)>> {
        let mut out = Vec::new();
        let mut offset = None;
        loop {
            let request = ScrollRequest {
                offset,
                limit: Some(SCROLL_PAGE),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: WithVector::Bool(false),
                order_by: None,
            };
            let (records, next) = shard
                .scroll(request)
                .map_err(|e| anyhow!("scanning wiki index: {e}"))?;
            for record in records {
                let value = record
                    .payload
                    .as_ref()
                    .and_then(|p| serde_json::to_value(p).ok())
                    .unwrap_or(serde_json::Value::Null);
                out.push((record.id, value));
            }
            match next {
                Some(next) => offset = Some(next),
                None => break,
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl ChunkIndex for EdgeIndex {
    async fn upsert(&self, chunks: &[IndexedChunk]) -> anyhow::Result<()> {
        let Some(dim) = chunks.iter().map(|c| c.embedding.len()).find(|n| *n > 0) else {
            return Ok(());
        };
        if let Some(bad) = chunks
            .iter()
            .find(|c| !c.embedding.is_empty() && c.embedding.len() != dim)
        {
            anyhow::bail!(
                "mixed vector widths in one batch ({} vs {} for {}) — the index stores one width",
                dim,
                bad.embedding.len(),
                bad.path
            );
        }

        let points: Vec<_> = chunks
            .iter()
            .filter(|c| !c.embedding.is_empty())
            .map(|c| {
                let mut named: HashMap<String, VectorPersisted> = HashMap::from([(
                    VECTOR_NAME.to_string(),
                    VectorPersisted::Dense(c.embedding.clone()),
                )]);
                // Indexed over the same text that was embedded (heading trail +
                // body), so both arms see one document.
                let sparse = self
                    .bm25
                    .embed_document(&format!("{}\n{}", c.heading_path, c.text));
                if !sparse.indices.is_empty() {
                    named.insert(
                        SPARSE_VECTOR_NAME.to_string(),
                        VectorPersisted::Sparse(sparse),
                    );
                }
                let vector = VectorStructPersisted::Named(named);
                let payload = match to_payload(c) {
                    serde_json::Value::Object(map) => Payload(map.into_iter().collect()),
                    _ => unreachable!("to_payload always builds an object"),
                };
                qdrant_edge::PointStructPersisted {
                    id: PointId::Uuid(point_id(&c.id)),
                    vector,
                    payload: Some(payload),
                }
            })
            .collect();

        let shard = self.ensure(dim)?;
        tokio::task::spawn_blocking(move || {
            shard.update(UpdateOperation::PointOperation(
                PointOperations::UpsertPoints(PointInsertOperations::PointsList(points)),
            ))
        })
        .await?
        .map_err(|e| anyhow!("writing to wiki index: {e}"))?;
        Ok(())
    }

    async fn search(
        &self,
        query: &[f32],
        query_text: &str,
        limit: usize,
        min_score: f32,
    ) -> anyhow::Result<Vec<ChunkHit>> {
        let Some(shard) = self.snapshot() else {
            return Ok(Vec::new());
        };
        if query.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        // A query of pure stopwords (or one against a pre-hybrid index) yields
        // no terms; the dense arm then carries the search alone.
        let sparse_query = Self::has_sparse(&shard)
            .then(|| self.bm25.embed_query(query_text))
            .filter(|sparse| !sparse.indices.is_empty());

        // Each arm is fetched deeper than it will contribute, because the cap
        // below throws hits away: without the headroom, a note that fills an
        // arm's top-k just yields a shorter run instead of a wider one.
        let depth = limit.saturating_mul(DIVERSIFY_OVERFETCH);
        let dense_query = query.to_vec();
        let (dense, sparse) = tokio::task::spawn_blocking(move || {
            let dense = run_query(
                &shard,
                QueryEnum::Nearest(NamedQuery {
                    query: VectorInternal::Dense(dense_query),
                    using: Some(VECTOR_NAME.into()),
                }),
                depth,
                Some(min_score),
            )?;
            let sparse = match sparse_query {
                // No floor on the lexical arm: BM25 scores are unbounded and
                // corpus-dependent, so a cosine threshold would be meaningless
                // here — depth and the per-note cap are what bound it.
                Some(sparse_query) => run_query(
                    &shard,
                    QueryEnum::Nearest(NamedQuery {
                        query: VectorInternal::Sparse(sparse_query),
                        using: Some(SPARSE_VECTOR_NAME.into()),
                    }),
                    depth,
                    None,
                )?,
                None => Vec::new(),
            };
            Ok::<_, qdrant_edge::OperationError>((dense, sparse))
        })
        .await?
        .map_err(|e| anyhow!("searching wiki index: {e}"))?;

        // Cap each arm before fusing: a note that owns most of one arm's top-k
        // is spending slots that fusion never gets to choose among. RRF scores
        // by rank, so discarding a note's 3rd chunk here promotes everything
        // below it rather than leaving a hole.
        let dense = diversify(to_hits(dense), limit, MAX_CHUNKS_PER_FILE);
        let sparse = diversify(to_hits(sparse), limit, MAX_CHUNKS_PER_FILE);
        // Dense-only: return cosine scores unchanged, so a vault without a
        // lexical arm behaves exactly as it did before hybrid existed.
        if sparse.is_empty() {
            return Ok(dense.into_iter().take(limit).collect());
        }
        Ok(reciprocal_rank_fusion(vec![dense, sparse], limit))
    }

    async fn indexed(&self) -> anyhow::Result<HashMap<String, IndexedFile>> {
        let Some(shard) = self.snapshot() else {
            return Ok(HashMap::new());
        };
        let rows = tokio::task::spawn_blocking(move || Self::scan(&shard)).await??;
        let mut out: HashMap<String, IndexedFile> = HashMap::new();
        for (_, value) in rows {
            let (Some(path), Some(mtime)) = (
                value.get(F_PATH).and_then(|v| v.as_str()),
                value.get(F_MTIME).and_then(|v| v.as_i64()),
            ) else {
                continue;
            };
            let entry = out
                .entry(path.to_string())
                .or_insert(IndexedFile { mtime, chunks: 0 });
            entry.chunks += 1;
            // A file's chunks all carry the same mtime; if a partial re-index
            // left a mix, the oldest is the honest answer — it forces a
            // re-index rather than skipping a half-updated file.
            entry.mtime = entry.mtime.min(mtime);
        }
        Ok(out)
    }

    async fn delete_paths(&self, paths: &[String]) -> anyhow::Result<()> {
        let Some(shard) = self.snapshot() else {
            return Ok(());
        };
        if paths.is_empty() {
            return Ok(());
        }
        let wanted: std::collections::HashSet<&str> = paths.iter().map(String::as_str).collect();
        let rows = {
            let shard = shard.clone();
            tokio::task::spawn_blocking(move || Self::scan(&shard)).await??
        };
        let ids: Vec<PointId> = rows
            .into_iter()
            .filter(|(_, v)| {
                v.get(F_PATH)
                    .and_then(|p| p.as_str())
                    .is_some_and(|p| wanted.contains(p))
            })
            .map(|(id, _)| id)
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        tokio::task::spawn_blocking(move || {
            shard.update(UpdateOperation::PointOperation(
                PointOperations::DeletePoints { ids },
            ))
        })
        .await?
        .map_err(|e| anyhow!("deleting from wiki index: {e}"))?;
        Ok(())
    }

    async fn count(&self) -> anyhow::Result<usize> {
        let Some(shard) = self.snapshot() else {
            return Ok(0);
        };
        let rows = tokio::task::spawn_blocking(move || Self::scan(&shard)).await??;
        Ok(rows.len())
    }

    async fn reset(&self) -> anyhow::Result<()> {
        // Drop the handle before touching the files: the shard holds a WAL and
        // mmapped segments, and removing those out from under a live handle is
        // how a half-deleted index survives to confuse the next run.
        {
            let mut guard = self
                .shard
                .write()
                .map_err(|_| anyhow!("wiki index lock poisoned"))?;
            *guard = None;
        }
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&path) {
            Ok(()) => Ok(()),
            // Already gone is the desired state, not a failure.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        })
        .await?
        .with_context(|| format!("removing wiki index at {}", self.path.display()))?;
        Ok(())
    }

    async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>> {
        let Some(shard) = self.snapshot() else {
            return Ok(None);
        };
        let dim = {
            let config = shard.config();
            config.vectors.get(VECTOR_NAME).map(|v| v.size as usize)
        };
        let Some(dim) = dim else { return Ok(None) };
        let rows = tokio::task::spawn_blocking(move || Self::scan(&shard)).await??;
        let model = rows
            .iter()
            .find_map(|(_, v)| v.get(F_MODEL).and_then(|m| m.as_str()))
            .unwrap_or_default()
            .to_string();
        Ok(Some((dim, model)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unit vectors, as the `EmbeddingClient` contract guarantees — the shard is
    /// configured for dot-product distance on that basis.
    fn chunk(path: &str, ordinal: usize, embedding: Vec<f32>) -> IndexedChunk {
        IndexedChunk {
            id: IndexedChunk::make_id(path, ordinal),
            path: path.to_string(),
            heading_path: format!("{path} > 节"),
            ordinal,
            text: format!("{path} 第{ordinal}段的正文"),
            mtime: 1780000000 + ordinal as i64,
            embedding,
            embedding_model: "test-model".into(),
        }
    }

    fn index(dir: &tempfile::TempDir) -> EdgeIndex {
        EdgeIndex::open(dir.path(), "wiki").unwrap()
    }

    /// A vault that was never indexed must answer "empty", not error — see the
    /// module docs on lazy creation.
    #[tokio::test]
    async fn uncreated_index_reads_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        assert_eq!(index.count().await.unwrap(), 0);
        assert!(
            index
                .search(&[1.0, 0.0], "q", 5, 0.0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(index.indexed().await.unwrap().is_empty());
        assert!(index.vector_spec().await.unwrap().is_none());
        index.delete_paths(&["a.md".into()]).await.unwrap();
    }

    #[tokio::test]
    async fn upsert_then_search_returns_the_nearest_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        index
            .upsert(&[
                chunk("a.md", 0, vec![1.0, 0.0]),
                chunk("b.md", 0, vec![0.0, 1.0]),
            ])
            .await
            .unwrap();

        assert_eq!(index.count().await.unwrap(), 2);
        assert_eq!(index.vector_spec().await.unwrap().unwrap().0, 2);

        let hits = index.search(&[1.0, 0.0], "q", 1, 0.0).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].chunk.path, "a.md");
        assert!(hits[0].score > 0.9, "score was {}", hits[0].score);
        // Payload survived the round trip; vectors deliberately did not.
        assert_eq!(hits[0].chunk.heading_path, "a.md > 节");
        assert!(hits[0].chunk.embedding.is_empty());
    }

    /// `min_score` must drop weak neighbours — an unrelated query always has a
    /// nearest point, and returning it is how a search tool invents relevance.
    #[tokio::test]
    async fn min_score_filters_weak_hits() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        index
            .upsert(&[chunk("a.md", 0, vec![1.0, 0.0])])
            .await
            .unwrap();
        // Orthogonal query: dot product 0.
        assert!(
            index
                .search(&[0.0, 1.0], "q", 5, 0.5)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            index.search(&[0.0, 1.0], "q", 5, -1.0).await.unwrap().len(),
            1
        );
    }

    /// Re-indexing an unchanged file must not duplicate its points — this is
    /// what the deterministic UUIDv5 point id buys.
    #[tokio::test]
    async fn upsert_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        let chunks = [
            chunk("a.md", 0, vec![1.0, 0.0]),
            chunk("a.md", 1, vec![0.0, 1.0]),
        ];
        index.upsert(&chunks).await.unwrap();
        index.upsert(&chunks).await.unwrap();
        assert_eq!(index.count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn indexed_groups_by_path_and_delete_removes_a_whole_file() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        index
            .upsert(&[
                chunk("a.md", 0, vec![1.0, 0.0]),
                chunk("a.md", 1, vec![0.0, 1.0]),
                chunk("b.md", 0, vec![0.0, 1.0]),
            ])
            .await
            .unwrap();

        let indexed = index.indexed().await.unwrap();
        assert_eq!(indexed.len(), 2);
        assert_eq!(indexed["a.md"].chunks, 2);
        assert_eq!(indexed["b.md"].chunks, 1);
        // Oldest mtime wins, so a half-updated file re-indexes.
        assert_eq!(indexed["a.md"].mtime, 1780000000);

        index.delete_paths(&["a.md".into()]).await.unwrap();
        assert_eq!(index.count().await.unwrap(), 1);
        assert!(index.indexed().await.unwrap().contains_key("b.md"));
    }

    fn chunk_with_text(path: &str, text: &str, embedding: Vec<f32>) -> IndexedChunk {
        let mut c = chunk(path, 0, embedding);
        c.text = text.to_string();
        c.heading_path = path.to_string();
        c
    }

    /// The entire reason hybrid exists: an exact token the dense arm cannot
    /// reach. The query vector points *away* from the note holding the id, so
    /// only the lexical arm can surface it.
    #[tokio::test]
    async fn lexical_arm_finds_an_exact_id_the_dense_arm_points_away_from() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        index
            .upsert(&[
                chunk_with_text(
                    "orders.md",
                    "订单 ORD-A1B2C3 在 complete 步骤连续提交失败",
                    vec![1.0, 0.0],
                ),
                chunk_with_text(
                    "unrelated.md",
                    "今天的天气很好，适合出门散步",
                    vec![0.0, 1.0],
                ),
            ])
            .await
            .unwrap();

        // Vector points at `unrelated.md`; the text names the id in `orders.md`.
        let hits = index
            .search(&[0.0, 1.0], "ORD-A1B2C3", 5, -1.0)
            .await
            .unwrap();
        let paths: Vec<&str> = hits.iter().map(|h| h.chunk.path.as_str()).collect();
        assert!(
            paths.contains(&"orders.md"),
            "lexical arm did not surface the exact id: {paths:?}"
        );
    }

    /// Measured on the real vault: one note held 9 of the dense arm's top 15,
    /// so fusion never saw the notes ranked behind it. Capping per arm is what
    /// puts them in front of it.
    #[tokio::test]
    async fn one_note_cannot_monopolize_an_arm_before_fusion() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        let mut chunks: Vec<IndexedChunk> = (0..6)
            .map(|i| {
                let mut c = chunk("hog.md", i, vec![1.0, 0.0]);
                c.text = "结账 服务 编排".into();
                c
            })
            .collect();
        chunks.push(chunk_with_text(
            "rival.md",
            "结账 服务 地图",
            vec![0.99, 0.01],
        ));
        index.upsert(&chunks).await.unwrap();

        // Dense-only, so this isolates the arm cap from anything lexical.
        let hits = index.search(&[1.0, 0.0], "", 5, -1.0).await.unwrap();
        let paths: Vec<&str> = hits.iter().map(|h| h.chunk.path.as_str()).collect();
        assert_eq!(
            paths.iter().filter(|p| **p == "hog.md").count(),
            MAX_CHUNKS_PER_FILE,
            "{paths:?}"
        );
        assert!(paths.contains(&"rival.md"), "{paths:?}");
    }

    /// CJK must tokenize, or the lexical arm is dead weight on this vault —
    /// the default `Word` tokenizer would treat a whole sentence as one token.
    #[tokio::test]
    async fn chinese_text_produces_lexical_terms() {
        let bm25 = EdgeBm25::new(bm25_config()).unwrap();
        let sparse = bm25.embed_document("订单创建失败的排查记录与链路还原");
        assert!(
            sparse.indices.len() > 1,
            "expected several CJK terms, got {}",
            sparse.indices.len()
        );
    }

    /// A pre-hybrid index has no sparse column; querying one that does not exist
    /// is an error, so search must fall back to dense instead of failing.
    #[tokio::test]
    async fn dense_only_search_still_works_and_keeps_cosine_scores() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        index
            .upsert(&[chunk("a.md", 0, vec![1.0, 0.0])])
            .await
            .unwrap();
        // A query with no usable terms leaves the lexical arm empty.
        let hits = index.search(&[1.0, 0.0], "", 5, 0.0).await.unwrap();
        assert_eq!(hits.len(), 1);
        // Cosine, not an RRF value (which would be ~0.016).
        assert!(hits[0].score > 0.9, "score was {}", hits[0].score);
    }

    /// Changing embedding model changes vector width, and the store cannot take
    /// the new one. This must say so, and say what fixes it.
    #[tokio::test]
    async fn a_different_vector_width_is_rejected_with_a_fix() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        index
            .upsert(&[chunk("a.md", 0, vec![1.0, 0.0])])
            .await
            .unwrap();

        let err = index
            .upsert(&[chunk("b.md", 0, vec![1.0, 0.0, 0.0])])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("2-dim") && err.contains("3-dim"), "{err}");
        assert!(err.contains("--rebuild"), "must name the fix: {err}");
    }

    /// `reset` is what makes a model change possible: after it, an index built
    /// for one width accepts another.
    #[tokio::test]
    async fn reset_allows_a_new_vector_width() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        index
            .upsert(&[chunk("a.md", 0, vec![1.0, 0.0])])
            .await
            .unwrap();
        assert_eq!(index.count().await.unwrap(), 1);

        index.reset().await.unwrap();
        assert_eq!(index.count().await.unwrap(), 0);

        index
            .upsert(&[chunk("a.md", 0, vec![0.0, 1.0, 0.0])])
            .await
            .unwrap();
        assert_eq!(index.vector_spec().await.unwrap().unwrap().0, 3);
        assert_eq!(index.count().await.unwrap(), 1);
    }

    /// Resetting a never-created index is the desired state, not an error.
    #[tokio::test]
    async fn reset_on_an_empty_index_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        index(&dir).reset().await.unwrap();
    }

    /// One index stores one vector width; a mixed batch is a bug upstream and
    /// must be rejected loudly rather than half-written.
    #[tokio::test]
    async fn mixed_vector_widths_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);
        let err = index
            .upsert(&[
                chunk("a.md", 0, vec![1.0, 0.0]),
                chunk("b.md", 0, vec![1.0, 0.0, 0.0]),
            ])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("mixed vector widths"), "{err}");
    }
}
