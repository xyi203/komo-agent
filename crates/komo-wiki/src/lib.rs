//! Vector index backends for the note vault, behind [`ChunkIndex`].
//!
//! Two backends, chosen by config at startup:
//!
//! - [`WikiBackend::Edge`] — `qdrant-edge`, in-process, no daemon. The default:
//!   a personal vault is a single-writer store, and an embedded index costs no
//!   service to run, no port to expose, and no network hop per query.
//! - [`WikiBackend::Server`] — a Qdrant instance over gRPC, for when more than
//!   one process must see the same index (komo here, komo on the NAS, and
//!   open-webui can all share one collection).
//!
//! **Switching backends is a restart, not a live swap.** The two store vectors
//! in different places, and nothing migrates between them — after a switch the
//! new backend is empty until `komo wiki index` refills it. A runtime swap would
//! therefore only ever expose an empty index, so it is deliberately not offered.
//!
//! Both speak the same Qdrant data model — a point per chunk, the chunk's fields
//! as payload, the same collection name — so an index built by one is readable
//! by the other. That is what makes the choice reversible.

use std::path::PathBuf;
use std::sync::Arc;

use komo_core::domain::chunk_index::ChunkIndex;

pub mod edge;
pub mod lazy;
pub mod payload;
pub mod server;

/// Which backend serves the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WikiBackend {
    /// In-process `qdrant-edge`.
    #[default]
    Edge,
    /// Remote Qdrant over gRPC.
    Server,
}

impl WikiBackend {
    /// Parse a config value. Unknown strings are an error rather than a silent
    /// fallback: quietly booting the embedded backend when the operator asked
    /// for a shared one would look like a working setup that indexes nowhere the
    /// other processes can see.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "edge" | "embedded" | "local" => Ok(Self::Edge),
            "server" | "remote" | "qdrant" => Ok(Self::Server),
            other => anyhow::bail!("unknown wiki backend `{other}` (expected `edge` or `server`)"),
        }
    }
}

/// Everything a backend needs to open its index.
///
/// Deliberately plain data rather than a borrow of `ConfigSnapshot`: this crate
/// depends only on `komo-core`, and the wiring layer maps config into it. That
/// keeps the heavy vector dependencies off `komo-config`'s compile path.
#[derive(Debug, Clone)]
pub struct WikiSettings {
    pub backend: WikiBackend,
    /// Where the embedded backend keeps its files (`~/.komo/wiki`). Disposable —
    /// deleting it costs one re-index, never the notes themselves.
    pub data_dir: PathBuf,
    /// Base URL of the Qdrant instance, for [`WikiBackend::Server`].
    pub url: String,
    /// Collection name, shared by both backends so an index is portable.
    pub collection: String,
    /// Qdrant API key. Comes from the environment, never `config.toml` — komo's
    /// standing rule is credentials in `.env`, behaviour in config.
    pub api_key: Option<String>,
}

impl Default for WikiSettings {
    fn default() -> Self {
        Self {
            backend: WikiBackend::Edge,
            data_dir: PathBuf::from("wiki"),
            url: "http://127.0.0.1:6334".to_string(),
            collection: "komo_wiki".to_string(),
            api_key: None,
        }
    }
}

/// Open the configured backend.
///
/// Mirrors `llm::build_llm`: one place where a config value becomes a concrete
/// implementation, so every caller downstream holds the trait and nothing else
/// in komo knows which backend is running.
pub async fn build_index(settings: &WikiSettings) -> anyhow::Result<Arc<dyn ChunkIndex>> {
    match settings.backend {
        WikiBackend::Edge => {
            let index = edge::EdgeIndex::open(&settings.data_dir, &settings.collection)?;
            tracing::info!(dir = %settings.data_dir.display(), "wiki index: embedded (qdrant-edge)");
            Ok(Arc::new(index))
        }
        WikiBackend::Server => {
            let index = server::ServerIndex::connect(
                &settings.url,
                &settings.collection,
                settings.api_key.as_deref(),
            )
            .await?;
            tracing::info!(url = %settings.url, collection = %settings.collection, "wiki index: qdrant server");
            Ok(Arc::new(index))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_parses_its_aliases() {
        assert_eq!(WikiBackend::parse("edge").unwrap(), WikiBackend::Edge);
        assert_eq!(WikiBackend::parse(" Local ").unwrap(), WikiBackend::Edge);
        assert_eq!(WikiBackend::parse("server").unwrap(), WikiBackend::Server);
        assert_eq!(WikiBackend::parse("QDRANT").unwrap(), WikiBackend::Server);
    }

    /// A typo must not silently boot the embedded backend — see
    /// [`WikiBackend::parse`].
    #[test]
    fn unknown_backend_is_an_error() {
        let err = WikiBackend::parse("qdrent").unwrap_err().to_string();
        assert!(
            err.contains("qdrent"),
            "error should name the bad value: {err}"
        );
    }
}
