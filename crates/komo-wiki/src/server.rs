//! Server backend: a Qdrant instance over gRPC.
//!
//! The reason to pick this over [`crate::edge`] is sharing: an embedded index is
//! visible only to the process holding it, while a Qdrant instance can serve
//! komo here, komo on the NAS, and open-webui from one collection.
//!
//! The collection is created lazily for the same reason as in the embedded
//! backend — vector width is fixed at creation and only known once the first
//! embedded chunk arrives.

use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use komo_core::domain::chunk_index::{ChunkHit, ChunkIndex, IndexedChunk, IndexedFile};
use qdrant_client::Payload;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CountPointsBuilder, CreateCollectionBuilder, DeletePointsBuilder, Distance, PointStruct,
    PointsIdsList, ScrollPointsBuilder, SearchPointsBuilder, UpsertPointsBuilder, Value,
    VectorParamsBuilder, VectorsConfig, vectors_config,
};

use crate::payload::{self, F_MODEL, F_MTIME, F_PATH, VECTOR_NAME, point_id, to_payload};

/// Page size for full scans; the server default is small enough to turn one
/// `indexed()` into many round trips.
const SCROLL_PAGE: u32 = 1024;

pub struct ServerIndex {
    client: Qdrant,
    collection: String,
}

/// gRPC payload map → plain JSON, so [`crate::payload`] can decode it with the
/// same code the embedded backend uses.
fn to_json(map: HashMap<String, Value>) -> serde_json::Value {
    serde_json::Value::Object(map.into_iter().map(|(k, v)| (k, v.into_json())).collect())
}

impl ServerIndex {
    pub async fn connect(
        url: &str,
        collection: &str,
        api_key: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut builder = Qdrant::from_url(url);
        if let Some(key) = api_key.filter(|k| !k.trim().is_empty()) {
            builder = builder.api_key(key.to_string());
        }
        let client = builder
            .build()
            .with_context(|| format!("connecting to qdrant at {url}"))?;
        // Fail at startup rather than on the first search: a wrong URL should
        // look like a misconfiguration, not like an empty vault.
        client
            .health_check()
            .await
            .with_context(|| format!("qdrant at {url} is not reachable"))?;
        Ok(Self {
            client,
            collection: collection.to_string(),
        })
    }

    async fn ensure_collection(&self, dim: usize) -> anyhow::Result<()> {
        if self.client.collection_exists(&self.collection).await? {
            return Ok(());
        }
        let params = VectorParamsBuilder::new(dim as u64, Distance::Dot).build();
        let config = VectorsConfig {
            config: Some(vectors_config::Config::ParamsMap(
                qdrant_client::qdrant::VectorParamsMap {
                    map: HashMap::from([(VECTOR_NAME.to_string(), params)]),
                },
            )),
        };
        self.client
            .create_collection(
                CreateCollectionBuilder::new(&self.collection).vectors_config(config),
            )
            .await
            .with_context(|| format!("creating collection {}", self.collection))?;
        Ok(())
    }

    /// Page through every point's payload.
    async fn scan(
        &self,
    ) -> anyhow::Result<Vec<(qdrant_client::qdrant::PointId, serde_json::Value)>> {
        if !self.client.collection_exists(&self.collection).await? {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut offset = None;
        loop {
            let mut request = ScrollPointsBuilder::new(&self.collection)
                .limit(SCROLL_PAGE)
                .with_payload(true)
                .with_vectors(false);
            if let Some(next) = offset.take() {
                request = request.offset(next);
            }
            let response = self.client.scroll(request).await?;
            for point in response.result {
                let Some(id) = point.id else { continue };
                out.push((id, to_json(point.payload)));
            }
            match response.next_page_offset {
                Some(next) => offset = Some(next),
                None => break,
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl ChunkIndex for ServerIndex {
    async fn upsert(&self, chunks: &[IndexedChunk]) -> anyhow::Result<()> {
        let Some(dim) = chunks.iter().map(|c| c.embedding.len()).find(|n| *n > 0) else {
            return Ok(());
        };
        if let Some(bad) = chunks
            .iter()
            .find(|c| !c.embedding.is_empty() && c.embedding.len() != dim)
        {
            anyhow::bail!(
                "mixed vector widths in one batch ({} vs {} for {}) — the collection stores one width",
                dim,
                bad.embedding.len(),
                bad.path
            );
        }
        self.ensure_collection(dim).await?;

        let points: Vec<PointStruct> = chunks
            .iter()
            .filter(|c| !c.embedding.is_empty())
            .map(|c| {
                let payload: Payload = match to_payload(c) {
                    serde_json::Value::Object(map) => map.into(),
                    _ => unreachable!("to_payload always builds an object"),
                };
                let vectors = HashMap::from([(VECTOR_NAME.to_string(), c.embedding.clone())]);
                PointStruct::new(point_id(&c.id).to_string(), vectors, payload)
            })
            .collect();

        self.client
            .upsert_points(UpsertPointsBuilder::new(&self.collection, points).wait(true))
            .await
            .with_context(|| format!("writing to collection {}", self.collection))?;
        Ok(())
    }

    /// Dense-only.
    ///
    /// `query_text` is ignored: the BM25 tokenizer that gives the embedded
    /// backend its lexical arm lives in `qdrant-edge`, and `qdrant-client`
    /// offers no equivalent — computing sparse vectors for this backend would
    /// mean reimplementing the tokenizer and keeping the two in step, which
    /// would silently diverge. A shared-index deployment that wants hybrid
    /// should configure the server's own BM25 instead.
    async fn search(
        &self,
        query: &[f32],
        _query_text: &str,
        limit: usize,
        min_score: f32,
    ) -> anyhow::Result<Vec<ChunkHit>> {
        if query.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        if !self.client.collection_exists(&self.collection).await? {
            return Ok(Vec::new());
        }
        let response = self
            .client
            .search_points(
                SearchPointsBuilder::new(&self.collection, query.to_vec(), limit as u64)
                    .vector_name(VECTOR_NAME)
                    .with_payload(true)
                    .score_threshold(min_score),
            )
            .await
            .with_context(|| format!("searching collection {}", self.collection))?;

        Ok(response
            .result
            .into_iter()
            .filter_map(|hit| {
                Some(ChunkHit {
                    chunk: payload::from_payload(&to_json(hit.payload))?,
                    score: hit.score,
                })
            })
            .collect())
    }

    async fn indexed(&self) -> anyhow::Result<HashMap<String, IndexedFile>> {
        let mut out: HashMap<String, IndexedFile> = HashMap::new();
        for (_, value) in self.scan().await? {
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
            entry.mtime = entry.mtime.min(mtime);
        }
        Ok(out)
    }

    async fn delete_paths(&self, paths: &[String]) -> anyhow::Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let wanted: std::collections::HashSet<&str> = paths.iter().map(String::as_str).collect();
        let ids: Vec<_> = self
            .scan()
            .await?
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
        self.client
            .delete_points(
                DeletePointsBuilder::new(&self.collection)
                    .points(PointsIdsList { ids })
                    .wait(true),
            )
            .await
            .with_context(|| format!("deleting from collection {}", self.collection))?;
        Ok(())
    }

    async fn reset(&self) -> anyhow::Result<()> {
        if !self.client.collection_exists(&self.collection).await? {
            return Ok(());
        }
        // Dropping the collection, not its points: vector width lives in the
        // collection's config, so only recreating it can change the width.
        self.client
            .delete_collection(&self.collection)
            .await
            .with_context(|| format!("deleting collection {}", self.collection))?;
        Ok(())
    }

    async fn count(&self) -> anyhow::Result<usize> {
        if !self.client.collection_exists(&self.collection).await? {
            return Ok(0);
        }
        let response = self
            .client
            .count(CountPointsBuilder::new(&self.collection).exact(true))
            .await?;
        Ok(response.result.map(|r| r.count as usize).unwrap_or(0))
    }

    async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>> {
        if !self.client.collection_exists(&self.collection).await? {
            return Ok(None);
        }
        let info = self.client.collection_info(&self.collection).await?;
        let dim = info
            .result
            .and_then(|r| r.config)
            .and_then(|c| c.params)
            .and_then(|p| p.vectors_config)
            .and_then(|v| v.config)
            .and_then(|c| match c {
                vectors_config::Config::Params(p) => Some(p.size as usize),
                vectors_config::Config::ParamsMap(m) => {
                    m.map.get(VECTOR_NAME).map(|p| p.size as usize)
                }
            });
        let Some(dim) = dim else { return Ok(None) };
        let model = self
            .scan()
            .await?
            .iter()
            .find_map(|(_, v)| v.get(F_MODEL).and_then(|m| m.as_str()).map(str::to_string))
            .unwrap_or_default();
        Ok(Some((dim, model)))
    }
}
