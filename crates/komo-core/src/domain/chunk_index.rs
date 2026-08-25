//! Hybrid vector + lexical search over a chunked corpus.
//!
//! Two corpora use this: the operator's note vault (`wiki_search`) and komo's
//! own conversation transcripts (`session` search). They share everything that
//! matters here — chunk identity, rank fusion, per-file diversification, the
//! mtime bookkeeping an incremental indexer needs — and differ only in what
//! produces the chunks, which is why the vocabulary is `path`/`ordinal` rather
//! than anything about notes or turns.
//!
//! Whatever the corpus, this is a *derived* index, never a store of record:
//! every chunk is reproducible from its source, which is why the backing files
//! are disposable (delete one, re-index, get it back). That is the distinction
//! from [`super::memory`] — memories are durable personal data with no source
//! to rebuild from.
//!
//! And unlike recall, which injects memories automatically, both corpora are
//! **pulled on demand**. Either one dwarfs the memory store, so injecting from
//! them every turn would spend context on material the turn never asked about.

use std::collections::HashMap;

use async_trait::async_trait;

/// One indexed slice of a source document — a note, or a conversation turn.
///
/// `id` is derived from `path` + `ordinal` rather than random, so re-indexing an
/// unchanged file produces the same ids and upsert stays idempotent — a random
/// id would duplicate every chunk on every run.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedChunk {
    pub id: String,
    /// Vault-relative path, e.g. `02-projects/checkout policy.md`. Relative so
    /// the index survives moving or renaming the vault directory itself.
    pub path: String,
    /// Markdown heading trail this chunk sits under (`设计 > 状态机`), empty for
    /// content before the first heading. Carried into the tool's output so a hit
    /// cites *where* in a 120 KB note it came from, not just which file.
    pub heading_path: String,
    /// Position within the file, 0-based. Part of `id`, and what orders chunks
    /// when several from one note are returned together.
    pub ordinal: usize,
    pub text: String,
    /// Source file's mtime at index time. The whole incremental story: a file
    /// whose mtime matches what is indexed is skipped without being read or
    /// embedded.
    pub mtime: i64,
    /// L2-normalized, per [`super::embedding::EmbeddingClient`]'s contract, so
    /// cosine similarity is a plain dot product.
    pub embedding: Vec<f32>,
    /// Model that produced `embedding`. Vectors from different models are not
    /// comparable, and the backing store fixes vector width at table creation,
    /// so a model change means rebuilding rather than mixing.
    pub embedding_model: String,
}

impl IndexedChunk {
    /// Stable id for a chunk: `<path>#<ordinal>`. Readable on purpose — it shows
    /// up in logs and `komo wiki` output, where a hash would say nothing.
    pub fn make_id(path: &str, ordinal: usize) -> String {
        format!("{path}#{ordinal}")
    }
}

/// A scored search hit.
#[derive(Debug, Clone)]
pub struct ChunkHit {
    pub chunk: IndexedChunk,
    /// Cosine similarity against the query vector (`[-1, 1]`) from a single
    /// retrieval arm, or — once [`reciprocal_rank_fusion`] has run — the fused
    /// rank-agreement score in `[0, 1]`.
    pub score: f32,
}

/// Rank constant for [`reciprocal_rank_fusion`]. 60 is the value from the
/// original RRF paper and the de-facto default; it damps the top ranks enough
/// that one run cannot dominate on its first hit alone.
pub const RRF_K: f32 = 60.0;

/// Merge several ranked result lists into one by Reciprocal Rank Fusion.
///
/// RRF scores by *rank*, not by score, which is exactly what mixing dense and
/// lexical retrieval needs: a cosine similarity (0..1, clustered near 0.65 in
/// practice) and a BM25 score (unbounded, scaled by corpus statistics) share no
/// units, so any weighted sum of the two would be arbitrary. Ranks are
/// comparable by construction.
///
/// A chunk found by both runs accumulates both contributions, so agreement
/// between lexical and semantic retrieval is what floats a result to the top.
///
/// The returned `score` is **not** a similarity: it is the fused value divided
/// by the most any chunk could score, so `1.0` means every run ranked it first
/// and `0.5` (with two runs) means only one arm found it at all. Raw RRF values
/// live in a range that reads as garbage next to a cosine — 0.033 for a perfect
/// hit — and the same field carries a real cosine on the dense-only path, so
/// normalizing is what keeps one displayed column from carrying two scales.
pub fn reciprocal_rank_fusion(runs: Vec<Vec<ChunkHit>>, limit: usize) -> Vec<ChunkHit> {
    if limit == 0 {
        return Vec::new();
    }
    let best_possible = runs.len() as f32 / (RRF_K + 1.0);
    let mut fused: HashMap<String, (f32, ChunkHit)> = HashMap::new();
    for run in runs {
        for (rank, hit) in run.into_iter().enumerate() {
            let contribution = 1.0 / (RRF_K + rank as f32 + 1.0);
            fused
                .entry(hit.chunk.id.clone())
                .and_modify(|(score, _)| *score += contribution)
                .or_insert((contribution, hit));
        }
    }
    let mut out: Vec<ChunkHit> = fused
        .into_values()
        .map(|(score, mut hit)| {
            hit.score = score / best_possible;
            hit
        })
        .collect();
    // Ties broken by id so the order is deterministic — `fused` is a HashMap,
    // whose iteration order is not.
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.chunk.id.cmp(&b.chunk.id))
    });
    out.truncate(limit);
    out
}

/// How many chunks one note may contribute to a result set.
///
/// Two, not one: a long note's sections are genuinely separate answers (a
/// troubleshooting write-up's "root cause" and "trace" both matter), and cutting
/// to one would drop the second. But without a cap a single long note takes the
/// whole page — observed in practice, where one note held 3 of 10 slots and six
/// files filled all ten between them.
pub const MAX_CHUNKS_PER_FILE: usize = 2;

/// Multiplier for how many hits to fetch before capping per file.
///
/// Capping throws hits away, so fetching exactly `limit` would return fewer than
/// asked. Three covers the worst realistic case (every candidate crowding into
/// `limit / MAX_CHUNKS_PER_FILE` files) without making the search itself
/// meaningfully more expensive at vault scale.
pub const DIVERSIFY_OVERFETCH: usize = 3;

/// Cap how many chunks any one file contributes, preserving score order.
///
/// Applied to an over-fetched result set: search ranks chunks, but a *reader*
/// wants coverage — five passages from five notes beat five from two. Ties are
/// resolved by the incoming order, so this never reorders equally-ranked hits.
///
/// Also applied to each retrieval arm *before* fusion, where it is doing a
/// different job: keeping the candidate pool wide. Measured on the real vault,
/// one note took 5 of the dense arm's top 15 for `checkout` and 9 of 15 for
/// `TiDB 连接数打满`, so fusion was choosing among three or four distinct notes
/// no matter how many candidates it was handed. Capping per arm spends those
/// slots on notes that would otherwise never reach the fusion at all.
pub fn diversify(hits: Vec<ChunkHit>, limit: usize, max_per_file: usize) -> Vec<ChunkHit> {
    if limit == 0 {
        return Vec::new();
    }
    let mut per_file: HashMap<String, usize> = HashMap::new();
    let mut out = Vec::with_capacity(limit.min(hits.len()));
    for hit in hits {
        if out.len() >= limit {
            break;
        }
        let seen = per_file.entry(hit.chunk.path.clone()).or_insert(0);
        if *seen >= max_per_file {
            continue;
        }
        *seen += 1;
        out.push(hit);
    }
    out
}

/// What is currently indexed for one note: its mtime, and how many chunks it
/// produced. The indexer diffs this against the filesystem to decide what to
/// re-embed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedFile {
    pub mtime: i64,
    pub chunks: usize,
}

/// Vector index over the vault.
///
/// Deliberately takes an already-embedded query vector rather than text: the
/// embedding backend lives a layer up (`EmbeddingClient`), so this trait stays
/// implementable by anything that can store and compare vectors, and a caller
/// that already has a vector never embeds twice.
#[async_trait]
pub trait ChunkIndex: Send + Sync {
    /// Insert or replace `chunks` by id.
    async fn upsert(&self, chunks: &[IndexedChunk]) -> anyhow::Result<()>;

    /// Top `limit` chunks for a query.
    ///
    /// `query` is the embedded query vector. `query_text` is the same query as
    /// the user wrote it, passed so a backend with lexical retrieval can run it
    /// as well and fuse the two — a vault is full of proper nouns (order ids,
    /// service names, error strings) that dense retrieval approximates and exact
    /// matching nails. A backend without lexical search ignores it.
    ///
    /// `min_score` applies to the **dense** arm only, as a cosine floor: an
    /// unrelated query always has a nearest neighbour, and returning it anyway is
    /// how a search tool starts fabricating relevance. It cannot gate a lexical
    /// arm, whose scores are on another scale entirely.
    ///
    /// The `score` on each hit is therefore not comparable across backends or
    /// across hybrid/dense modes — see [`reciprocal_rank_fusion`].
    async fn search(
        &self,
        query: &[f32],
        query_text: &str,
        limit: usize,
        min_score: f32,
    ) -> anyhow::Result<Vec<ChunkHit>>;

    /// Everything indexed, keyed by vault-relative path.
    async fn indexed(&self) -> anyhow::Result<HashMap<String, IndexedFile>>;

    /// Drop every chunk belonging to `paths`. Used both for notes deleted from
    /// the vault and, before re-indexing a changed note, to clear chunks a
    /// shorter version no longer produces.
    async fn delete_paths(&self, paths: &[String]) -> anyhow::Result<()>;

    /// Total chunk count, for `komo wiki status`.
    async fn count(&self) -> anyhow::Result<usize>;

    /// Drop the index entirely, so the next `upsert` builds it fresh.
    ///
    /// Required to change embedding model: vector width is fixed when the index
    /// is created, and a 1024-dim store cannot accept 2560-dim vectors. Deleting
    /// every point is not enough — the *store* has to go. Safe by construction,
    /// since the index is derived data that `komo wiki index` rebuilds.
    async fn reset(&self) -> anyhow::Result<()>;

    /// Vector width the index was created with, and the model that set it.
    /// `None` for an empty index, which adopts whatever the first upsert brings.
    async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(path: &str, ordinal: usize, score: f32) -> ChunkHit {
        ChunkHit {
            chunk: IndexedChunk {
                id: IndexedChunk::make_id(path, ordinal),
                path: path.into(),
                heading_path: String::new(),
                ordinal,
                text: String::new(),
                mtime: 0,
                embedding: Vec::new(),
                embedding_model: String::new(),
            },
            score,
        }
    }

    /// Agreement between the two arms is the whole point: a chunk both runs
    /// found must outrank one that only the top of a single run found.
    #[test]
    fn a_chunk_found_by_both_runs_outranks_either_run_leader() {
        let dense = vec![hit("only_dense.md", 0, 0.9), hit("both.md", 0, 0.8)];
        let lexical = vec![hit("only_lexical.md", 0, 12.0), hit("both.md", 0, 9.0)];
        let out = reciprocal_rank_fusion(vec![dense, lexical], 3);
        assert_eq!(out[0].chunk.path, "both.md", "{out:?}");
    }

    /// RRF must not care that BM25 scores are an order of magnitude larger than
    /// cosine ones — it ranks, it does not add.
    #[test]
    fn fusion_ignores_the_scale_of_input_scores() {
        let small = vec![hit("a.md", 0, 0.01)];
        let huge = vec![hit("b.md", 0, 900.0)];
        let out = reciprocal_rank_fusion(vec![small, huge], 2);
        assert_eq!(out.len(), 2);
        // Both were rank 0 in their run, so both get the same contribution.
        assert!((out[0].score - out[1].score).abs() < 1e-6, "{out:?}");
    }

    /// The score reaches a reader, so it has to mean something there: full
    /// agreement is 1.0, and a hit only one of two arms found is 0.5. Raw RRF
    /// would have made these 0.033 and 0.016.
    #[test]
    fn fused_scores_are_normalized_against_full_agreement() {
        let dense = vec![hit("both.md", 0, 0.9), hit("only_dense.md", 0, 0.8)];
        let lexical = vec![hit("both.md", 0, 12.0)];
        let out = reciprocal_rank_fusion(vec![dense, lexical], 3);
        assert_eq!(out[0].chunk.path, "both.md");
        assert!((out[0].score - 1.0).abs() < 1e-6, "{out:?}");
        let single = out
            .iter()
            .find(|h| h.chunk.path == "only_dense.md")
            .unwrap();
        assert!(
            single.score < 0.5,
            "rank-1 alone is the 0.5 ceiling: {single:?}"
        );
    }

    #[test]
    fn fusion_of_one_run_preserves_its_order() {
        let run = vec![
            hit("a.md", 0, 0.9),
            hit("b.md", 0, 0.8),
            hit("c.md", 0, 0.7),
        ];
        let out = reciprocal_rank_fusion(vec![run], 10);
        let paths: Vec<&str> = out.iter().map(|h| h.chunk.path.as_str()).collect();
        assert_eq!(paths, vec!["a.md", "b.md", "c.md"]);
    }

    #[test]
    fn fusion_of_nothing_is_nothing() {
        assert!(reciprocal_rank_fusion(vec![], 5).is_empty());
        assert!(reciprocal_rank_fusion(vec![vec![]], 5).is_empty());
        assert!(reciprocal_rank_fusion(vec![vec![hit("a.md", 0, 0.9)]], 0).is_empty());
    }

    /// The observed failure: one long note held three of ten slots.
    #[test]
    fn one_file_cannot_take_every_slot() {
        let hits = vec![
            hit("long.md", 0, 0.9),
            hit("long.md", 1, 0.89),
            hit("long.md", 2, 0.88),
            hit("other.md", 0, 0.87),
            hit("third.md", 0, 0.86),
        ];
        let out = diversify(hits, 3, MAX_CHUNKS_PER_FILE);
        assert_eq!(out.len(), 3);
        let paths: Vec<&str> = out.iter().map(|h| h.chunk.path.as_str()).collect();
        assert_eq!(paths, vec!["long.md", "long.md", "other.md"]);
    }

    /// Capping must not reorder what survives — search already ranked these.
    #[test]
    fn score_order_is_preserved() {
        let hits = vec![
            hit("a.md", 0, 0.9),
            hit("b.md", 0, 0.8),
            hit("a.md", 1, 0.7),
            hit("c.md", 0, 0.6),
        ];
        let out = diversify(hits, 10, MAX_CHUNKS_PER_FILE);
        let scores: Vec<f32> = out.iter().map(|h| h.score).collect();
        assert_eq!(scores, vec![0.9, 0.8, 0.7, 0.6]);
    }

    /// Fewer hits than the limit is the common case and must pass through whole.
    #[test]
    fn under_the_limit_everything_survives() {
        let hits = vec![hit("a.md", 0, 0.9), hit("b.md", 0, 0.8)];
        assert_eq!(diversify(hits, 5, MAX_CHUNKS_PER_FILE).len(), 2);
    }

    #[test]
    fn zero_limit_returns_nothing() {
        assert!(diversify(vec![hit("a.md", 0, 0.9)], 0, MAX_CHUNKS_PER_FILE).is_empty());
    }

    /// A vault where every hit is from one note still returns that note, capped —
    /// never an empty result.
    #[test]
    fn a_single_file_vault_still_returns_hits() {
        let hits = vec![
            hit("only.md", 0, 0.9),
            hit("only.md", 1, 0.8),
            hit("only.md", 2, 0.7),
        ];
        assert_eq!(
            diversify(hits, 10, MAX_CHUNKS_PER_FILE).len(),
            MAX_CHUNKS_PER_FILE
        );
    }

    #[test]
    fn chunk_id_is_stable_for_the_same_position() {
        assert_eq!(
            IndexedChunk::make_id("03-areas/oncall.md", 4),
            IndexedChunk::make_id("03-areas/oncall.md", 4)
        );
        assert_ne!(
            IndexedChunk::make_id("03-areas/oncall.md", 4),
            IndexedChunk::make_id("03-areas/oncall.md", 5)
        );
    }
}
