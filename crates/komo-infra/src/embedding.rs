//! Ollama-backed embeddings for cross-language memory recall.
//!
//! Ollama is the backend because the alternative — a hosted embeddings API —
//! puts a network round trip on the reply critical path and needs a key komo's
//! own providers do not supply: Anthropic serves no embeddings endpoint, and
//! the `codex` provider authenticates by OAuth against a chat backend. A local
//! daemon costs no key, no quota, and no egress, and a warm small multilingual
//! model answers in well under the timeout below.
//!
//! Every failure here is non-fatal by contract: recall falls back to lexical
//! matching, which is what it did before this module existed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use komo_core::domain::embedding::{EmbeddingClient, normalize};
use serde::Deserialize;

/// How long a single embed call may take before the caller gives up.
///
/// Sized for the cold case, not the warm one: a warm model answers a short
/// query in ~100ms, but Ollama unloads an idle model and the reload costs a
/// couple of seconds. The query path applies its own, tighter budget — this is
/// the transport ceiling, so a backfill batch is not cut off by it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How long Ollama should keep the model resident after a call. Long enough
/// that a conversation's turns all hit a warm model instead of each paying the
/// reload.
const KEEP_ALIVE: &str = "30m";

/// An embedding backend speaking Ollama's `/api/embed`.
pub struct OllamaEmbedder {
    client: reqwest::Client,
    /// Base URL with no trailing slash, e.g. `http://127.0.0.1:11434`.
    base_url: String,
    model: String,
}

#[derive(Deserialize)]
struct EmbedResponse {
    #[serde(default)]
    embeddings: Vec<Vec<f32>>,
}

impl OllamaEmbedder {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            model: model.into(),
        })
    }

    /// Probe that the daemon is up and actually serves `model`, so a
    /// misconfigured or absent backend is reported once at startup rather than
    /// as a silent per-turn fallback that looks like "recall just doesn't
    /// work".
    pub async fn probe(&self) -> anyhow::Result<()> {
        let probe = vec!["ok".to_string()];
        let vectors = self.embed(&probe).await?;
        match vectors.first() {
            Some(v) if !v.is_empty() => Ok(()),
            _ => anyhow::bail!(
                "ollama at {} returned no vector for model `{}`",
                self.base_url,
                self.model
            ),
        }
    }
}

/// [`OllamaEmbedder`] behind a kill switch, so the startup probe can run in
/// the background instead of on boot's critical path — awaiting it there held
/// the first frame hostage to Ollama's cold model load, which takes seconds.
///
/// Calls pass through until the probe delivers a verdict; a failed probe flips
/// the switch, and every later call fails immediately (degrading its caller to
/// lexical) instead of re-paying a connection timeout per turn against a
/// backend already known to be down.
pub struct GatedEmbedder {
    inner: OllamaEmbedder,
    enabled: AtomicBool,
}

impl GatedEmbedder {
    pub fn new(inner: OllamaEmbedder) -> Self {
        Self {
            inner,
            enabled: AtomicBool::new(true),
        }
    }

    /// Run the backend probe, closing the gate on failure. Flipping the switch
    /// lives here rather than at the call site so a probe whose verdict is
    /// dropped still takes effect.
    pub async fn probe(&self) -> anyhow::Result<()> {
        let verdict = self.inner.probe().await;
        if verdict.is_err() {
            self.enabled.store(false, Ordering::Relaxed);
        }
        verdict
    }
}

#[async_trait]
impl EmbeddingClient for GatedEmbedder {
    async fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        // The empty-batch contract holds even with the gate closed: no inputs
        // means no vectors and no round trip, never an error to fall back on.
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        if !self.enabled.load(Ordering::Relaxed) {
            anyhow::bail!(
                "embedding backend disabled: startup probe against {} failed",
                self.inner.base_url
            );
        }
        self.inner.embed(texts).await
    }

    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
}

#[async_trait]
impl EmbeddingClient for OllamaEmbedder {
    async fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let response = self
            .client
            .post(format!("{}/api/embed", self.base_url))
            .json(&serde_json::json!({
                "model": self.model,
                "input": texts,
                "keep_alive": KEEP_ALIVE,
            }))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("ollama embed failed ({status}): {}", body.trim());
        }

        let mut parsed: EmbedResponse = response.json().await?;
        // A short batch would silently misalign vectors with memories, which is
        // worse than no embeddings at all — the store would attribute one
        // memory's meaning to another.
        if parsed.embeddings.len() != texts.len() {
            anyhow::bail!(
                "ollama returned {} vectors for {} inputs",
                parsed.embeddings.len(),
                texts.len()
            );
        }
        // Normalize at the boundary so `cosine` is a dot product everywhere and
        // stored vectors stay comparable — the `EmbeddingClient` contract.
        for vector in &mut parsed.embeddings {
            normalize(vector);
        }
        Ok(parsed.embeddings)
    }

    fn model_id(&self) -> &str {
        &self.model
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_trailing_slash_from_base_url() {
        let e = OllamaEmbedder::new("http://127.0.0.1:11434/", "m").unwrap();
        assert_eq!(e.base_url, "http://127.0.0.1:11434");
        assert_eq!(e.model_id(), "m");
    }

    /// An empty batch must not make a request — the backfill calls this
    /// whenever it finds nothing to do.
    #[tokio::test]
    async fn empty_batch_short_circuits() {
        // Port 1 is unbound; reaching the network here would error.
        let e = OllamaEmbedder::new("http://127.0.0.1:1", "m").unwrap();
        assert!(e.embed(&[]).await.unwrap().is_empty());
    }

    /// An unreachable daemon is an error the caller can fall back on, never a
    /// panic or a hang.
    #[tokio::test]
    async fn unreachable_daemon_is_an_error() {
        let e = OllamaEmbedder::new("http://127.0.0.1:1", "m").unwrap();
        assert!(e.embed(&["hi".to_string()]).await.is_err());
        assert!(e.probe().await.is_err());
    }

    /// A failed probe closes the gate: later calls must fail without touching
    /// the network, not retry a backend already known to be down.
    #[tokio::test]
    async fn failed_probe_closes_the_gate() {
        // Port 1 is unbound; the probe fails on connect.
        let gated = GatedEmbedder::new(OllamaEmbedder::new("http://127.0.0.1:1", "m").unwrap());
        assert!(gated.probe().await.is_err());
        let error = gated.embed(&["hi".to_string()]).await.unwrap_err();
        assert!(error.to_string().contains("disabled"), "{error}");
    }

    /// The empty-batch contract survives a closed gate — no inputs is "nothing
    /// to do", never a failure for the caller to degrade on.
    #[tokio::test]
    async fn empty_batch_is_ok_even_when_gated() {
        let gated = GatedEmbedder::new(OllamaEmbedder::new("http://127.0.0.1:1", "m").unwrap());
        assert!(gated.probe().await.is_err());
        assert!(gated.embed(&[]).await.unwrap().is_empty());
        assert_eq!(gated.model_id(), "m");
    }
}
