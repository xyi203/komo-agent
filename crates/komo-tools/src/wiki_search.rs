//! Search the operator's note vault.
//!
//! Pulled on demand rather than injected every turn like memory recall: a vault
//! is orders of magnitude larger than the memory store, so auto-injecting from
//! it would spend context on notes the turn never asked about. A turn that does
//! not search pays nothing.
//!
//! This tool holds only [`ChunkIndex`] and [`EmbeddingClient`], never a concrete
//! backend — which backend is running (embedded or Qdrant server) is decided in
//! config and invisible here.

use std::sync::Arc;

use async_trait::async_trait;
use komo_core::domain::{
    chunk_index::{ChunkIndex, DIVERSIFY_OVERFETCH, MAX_CHUNKS_PER_FILE, diversify},
    context::ToolContext,
    embedding::EmbeddingClient,
    tool::{Tool, ToolError, ToolOutput, parse_args},
};
use serde::Deserialize;
use serde_json::Value;

/// Cosine floor for a hit to be shown.
///
/// Every query has a nearest neighbour, so without a floor an unrelated question
/// still returns the vault's least-unrelated note — and a model handed
/// confident-looking context tends to use it. Set at the same level as memory
/// recall's semantic floor, which is tuned for the same embedding models.
const SCORE_FLOOR: f32 = 0.45;

const DEFAULT_LIMIT: usize = 5;
/// Ceiling on `limit`. Ten chunks is already ~8 KB of context; more is a sign
/// the model should refine the query instead of widening it.
const MAX_LIMIT: usize = 10;

#[derive(Deserialize)]
struct Args {
    query: String,
    #[serde(default)]
    limit: Option<usize>,
}

pub struct WikiSearchTool {
    index: Arc<dyn ChunkIndex>,
    embedder: Arc<dyn EmbeddingClient>,
}

impl WikiSearchTool {
    pub fn new(index: Arc<dyn ChunkIndex>, embedder: Arc<dyn EmbeddingClient>) -> Self {
        Self { index, embedder }
    }
}

#[async_trait]
impl Tool for WikiSearchTool {
    fn name(&self) -> &'static str {
        "wiki_search"
    }

    fn description(&self) -> &'static str {
        "Search the user's personal note vault (Obsidian) by meaning. Returns the \
         most relevant passages with their source file and heading. Use it when \
         the user refers to something they wrote down, asks what they concluded \
         or decided previously, or when their own notes would answer better than \
         general knowledge. Matches across languages — a Chinese question finds \
         an English note. If one search comes back thin, retry with different \
         wording or an adjacent angle before concluding the note does not exist.\n\
         This answers \"what did I write about X\", never \"what is in the \
         vault\". An inventory question has no passage that matches it: the \
         vault's own index, dashboard and README-style files outrank every real \
         note, so searching returns a table of contents in fragments and the \
         answer silently omits whatever those files forgot to list. To report \
         coverage, `read` the vault's index/dashboard file whole (the vault root \
         usually names one) and list the directories — do not assemble it from \
         search hits."
    }

    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to look for, as a natural-language phrase. \
                                    Prefer the user's own wording — the vault is \
                                    theirs and uses their terms."
                },
                "limit": {
                    "type": "integer",
                    "description": format!("Passages to return (default {DEFAULT_LIMIT}, max {MAX_LIMIT})."),
                    "minimum": 1,
                    "maximum": MAX_LIMIT
                }
            },
            "required": ["query"]
        })
    }

    async fn call(&self, input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: Args = parse_args(&input)?;
        let query = args.query.trim();
        if query.is_empty() {
            return Err(ToolError::InvalidInput("query is empty".into()));
        }
        let limit = args.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

        let vectors = self
            .embedder
            .embed(&[query.to_string()])
            .await
            .map_err(ToolError::Failed)?;
        let Some(vector) = vectors.into_iter().next().filter(|v| !v.is_empty()) else {
            // Unlike recall, there is no lexical arm to fall back to here, so an
            // embedding failure is a real failure — but a legible one.
            return Err(ToolError::Failed(anyhow::anyhow!(
                "the embedding backend returned no vector; note search is unavailable"
            )));
        };

        // Over-fetch, then cap per file: ranking alone lets one long note take
        // the whole page, which costs the reader coverage of other notes.
        let candidates = self
            .index
            .search(&vector, query, limit * DIVERSIFY_OVERFETCH, SCORE_FLOOR)
            .await
            .map_err(ToolError::Failed)?;
        let hits = diversify(candidates, limit, MAX_CHUNKS_PER_FILE);

        if hits.is_empty() {
            return Ok(ToolOutput::text(format!(
                "No passages in the vault matched “{query}”. The vault may not \
                 cover this, or it may not be indexed yet (`komo wiki index`)."
            )));
        }

        let mut text = format!("{} passage(s) for “{query}”:\n", hits.len());
        for hit in &hits {
            text.push_str(&format!(
                "\n── {} ({:.2})\n{}\n{}\n",
                hit.chunk.path, hit.score, hit.chunk.heading_path, hit.chunk.text
            ));
        }
        Ok(ToolOutput::text(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::chunk_index::{ChunkHit, IndexedChunk, IndexedFile};
    use std::collections::HashMap;

    struct FixedIndex(Vec<ChunkHit>);

    #[async_trait]
    impl ChunkIndex for FixedIndex {
        async fn upsert(&self, _: &[IndexedChunk]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn search(
            &self,
            _: &[f32],
            _: &str,
            limit: usize,
            _: f32,
        ) -> anyhow::Result<Vec<ChunkHit>> {
            Ok(self.0.iter().take(limit).cloned().collect())
        }
        async fn indexed(&self) -> anyhow::Result<HashMap<String, IndexedFile>> {
            Ok(HashMap::new())
        }
        async fn delete_paths(&self, _: &[String]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn count(&self) -> anyhow::Result<usize> {
            Ok(self.0.len())
        }
        async fn reset(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>> {
            Ok(None)
        }
    }

    struct FakeEmbedder(bool);

    #[async_trait]
    impl EmbeddingClient for FakeEmbedder {
        async fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            if !self.0 {
                anyhow::bail!("backend down");
            }
            Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
        }
        fn model_id(&self) -> &str {
            "fake"
        }
    }

    fn hit_at(path: &str, ordinal: usize, score: f32) -> ChunkHit {
        let mut h = hit(path, score);
        h.chunk.ordinal = ordinal;
        h.chunk.id = IndexedChunk::make_id(path, ordinal);
        h
    }

    fn hit(path: &str, score: f32) -> ChunkHit {
        ChunkHit {
            chunk: IndexedChunk {
                id: IndexedChunk::make_id(path, 0),
                path: path.into(),
                heading_path: format!("{path} > 设计"),
                ordinal: 0,
                text: "正文内容".into(),
                mtime: 0,
                embedding: Vec::new(),
                embedding_model: "fake".into(),
            },
            score,
        }
    }

    fn tool(hits: Vec<ChunkHit>, embedder_ok: bool) -> WikiSearchTool {
        WikiSearchTool::new(
            Arc::new(FixedIndex(hits)),
            Arc::new(FakeEmbedder(embedder_ok)),
        )
    }

    fn ctx() -> ToolContext {
        use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
        use komo_core::domain::context::SessionContext;
        struct DenyAll;
        #[async_trait]
        impl Approver for DenyAll {
            async fn decide(&self, _: &ApprovalRequest) -> Decision {
                Decision::deny()
            }
        }
        ToolContext::new(
            SessionContext::detached("cli:test"),
            None,
            Arc::new(DenyAll),
        )
    }

    #[tokio::test]
    async fn hits_are_rendered_with_source_and_score() {
        let out = tool(vec![hit("02-projects/a.md", 0.82)], true)
            .call(serde_json::json!({"query": "checkout 设计"}), &ctx())
            .await
            .unwrap();
        assert!(out.text.contains("02-projects/a.md"), "{}", out.text);
        assert!(out.text.contains("0.82"), "{}", out.text);
        assert!(out.text.contains("正文内容"), "{}", out.text);
    }

    /// An empty result must read as "nothing matched", not as an error, and
    /// should hint at the un-indexed case.
    #[tokio::test]
    async fn empty_result_explains_itself() {
        let out = tool(Vec::new(), true)
            .call(serde_json::json!({"query": "无关问题"}), &ctx())
            .await
            .unwrap();
        assert!(out.text.contains("No passages"), "{}", out.text);
        assert!(out.text.contains("komo wiki index"), "{}", out.text);
    }

    /// The observed failure this guards: one long note filled most of the page,
    /// crowding out other files.
    #[tokio::test]
    async fn one_file_cannot_crowd_out_the_rest() {
        let mut hits = vec![
            hit_at("long.md", 0, 0.90),
            hit_at("long.md", 1, 0.89),
            hit_at("long.md", 2, 0.88),
            hit_at("long.md", 3, 0.87),
        ];
        hits.push(hit_at("other.md", 0, 0.86));
        hits.push(hit_at("third.md", 0, 0.85));

        let out = tool(hits, true)
            .call(serde_json::json!({"query": "x", "limit": 4}), &ctx())
            .await
            .unwrap();
        // Count result headers, not bare paths — a path also appears in the
        // heading trail of its own result.
        assert_eq!(
            out.text.matches("── long.md").count(),
            MAX_CHUNKS_PER_FILE,
            "{}",
            out.text
        );
        assert!(out.text.contains("other.md"), "{}", out.text);
        assert!(out.text.contains("third.md"), "{}", out.text);
    }

    #[tokio::test]
    async fn limit_is_clamped() {
        let hits: Vec<_> = (0..20).map(|i| hit(&format!("n{i}.md"), 0.9)).collect();
        let out = tool(hits, true)
            .call(serde_json::json!({"query": "x", "limit": 99}), &ctx())
            .await
            .unwrap();
        assert_eq!(out.text.matches("──").count(), MAX_LIMIT);
    }

    /// No lexical fallback exists here, so this must surface rather than look
    /// like an empty vault.
    #[tokio::test]
    async fn embedding_failure_is_an_error_not_an_empty_result() {
        let err = tool(vec![hit("a.md", 0.9)], false)
            .call(serde_json::json!({"query": "x"}), &ctx())
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }

    #[tokio::test]
    async fn empty_query_is_invalid_input() {
        let err = tool(Vec::new(), true)
            .call(serde_json::json!({"query": "   "}), &ctx())
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
    }
}
