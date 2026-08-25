//! The data model both backends share.
//!
//! `qdrant-edge` and `qdrant-client` do not share Rust types — one uses Qdrant's
//! internal segment types, the other protobuf-generated ones — but they do share
//! Qdrant's *data model*. Everything portable between them lives here: the
//! payload field names, the JSON shape, and the point-id derivation. An index
//! written by one backend is therefore readable by the other, which is what
//! makes the config switch reversible.

use komo_core::domain::chunk_index::IndexedChunk;
use uuid::Uuid;

pub const F_PATH: &str = "path";
pub const F_HEADING: &str = "heading_path";
pub const F_ORDINAL: &str = "ordinal";
pub const F_TEXT: &str = "text";
pub const F_MTIME: &str = "mtime";
pub const F_MODEL: &str = "embedding_model";

/// Name of the dense vector field. Qdrant supports several vectors per point;
/// naming ours leaves room to add a sparse (BM25) vector later without a
/// migration, which is the hybrid-search path.
pub const VECTOR_NAME: &str = "dense";

/// Name of the sparse (BM25) vector field.
///
/// Kept beside `dense` in the same points: a vault is full of proper nouns —
/// order ids, service names, verbatim error strings — that dense retrieval only
/// approximates while exact term matching nails them. The two arms are fused at
/// query time rather than either one being authoritative.
pub const SPARSE_VECTOR_NAME: &str = "sparse";

/// Namespace for chunk-id → point-id derivation. A fixed random UUID, as UUIDv5
/// requires; it never changes, or every point id would change with it.
const CHUNK_NAMESPACE: Uuid = Uuid::from_u128(0x6b6f_6d6f_7769_6b69_0000_0000_0000_0001);

/// Map a chunk's readable id (`02-projects/note.md#3`) onto a Qdrant point id.
///
/// Qdrant accepts only `u64` or UUID point ids — an arbitrary string is
/// rejected — so the readable id cannot be used directly. UUIDv5 is a
/// deterministic hash of it, which keeps upsert idempotent: re-indexing an
/// unchanged chunk targets the same point instead of inserting a duplicate. A
/// `u64` hash would be shorter but reintroduces a collision probability for no
/// benefit here. The readable id stays in the payload for humans.
pub fn point_id(chunk_id: &str) -> Uuid {
    Uuid::new_v5(&CHUNK_NAMESPACE, chunk_id.as_bytes())
}

/// Chunk → payload JSON. Both backends build their native payload from this.
pub fn to_payload(chunk: &IndexedChunk) -> serde_json::Value {
    serde_json::json!({
        "id": chunk.id,
        F_PATH: chunk.path,
        F_HEADING: chunk.heading_path,
        F_ORDINAL: chunk.ordinal as u64,
        F_TEXT: chunk.text,
        F_MTIME: chunk.mtime,
        F_MODEL: chunk.embedding_model,
    })
}

/// Payload JSON → chunk.
///
/// `embedding` is left empty: search never returns vectors (a 1024-dim vector is
/// 4 KB per hit that no caller reads), so reconstructing one would be a lie.
/// Returns `None` when a required field is missing or mistyped — a malformed
/// point must drop out of the results, not fail the whole query.
pub fn from_payload(payload: &serde_json::Value) -> Option<IndexedChunk> {
    let path = payload.get(F_PATH)?.as_str()?.to_string();
    let ordinal = payload.get(F_ORDINAL)?.as_u64()? as usize;
    Some(IndexedChunk {
        id: payload
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| IndexedChunk::make_id(&path, ordinal)),
        path,
        heading_path: payload
            .get(F_HEADING)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        ordinal,
        text: payload.get(F_TEXT)?.as_str()?.to_string(),
        mtime: payload.get(F_MTIME).and_then(|v| v.as_i64()).unwrap_or(0),
        embedding: Vec::new(),
        embedding_model: payload
            .get(F_MODEL)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk() -> IndexedChunk {
        IndexedChunk {
            id: "02-projects/note.md#3".into(),
            path: "02-projects/note.md".into(),
            heading_path: "note > 设计".into(),
            ordinal: 3,
            text: "正文内容".into(),
            mtime: 1780000000,
            embedding: vec![0.1, 0.2],
            embedding_model: "qwen3-embedding:4b".into(),
        }
    }

    /// Idempotent upsert depends on this: the same chunk must always map to the
    /// same point.
    #[test]
    fn point_id_is_deterministic_and_distinct() {
        assert_eq!(point_id("a.md#1"), point_id("a.md#1"));
        assert_ne!(point_id("a.md#1"), point_id("a.md#2"));
        assert_ne!(point_id("a.md#1"), point_id("b.md#1"));
    }

    #[test]
    fn payload_round_trips() {
        let original = chunk();
        let back = from_payload(&to_payload(&original)).unwrap();
        assert_eq!(back.id, original.id);
        assert_eq!(back.path, original.path);
        assert_eq!(back.heading_path, original.heading_path);
        assert_eq!(back.ordinal, original.ordinal);
        assert_eq!(back.text, original.text);
        assert_eq!(back.mtime, original.mtime);
        assert_eq!(back.embedding_model, original.embedding_model);
        // Vectors are deliberately not returned by search.
        assert!(back.embedding.is_empty());
    }

    #[test]
    fn malformed_payload_drops_out_instead_of_failing() {
        assert!(from_payload(&serde_json::json!({})).is_none());
        assert!(from_payload(&serde_json::json!({ F_PATH: "a.md" })).is_none());
        // ordinal as a string, not a number
        assert!(
            from_payload(&serde_json::json!({ F_PATH: "a.md", F_ORDINAL: "3", F_TEXT: "x" }))
                .is_none()
        );
    }
}
