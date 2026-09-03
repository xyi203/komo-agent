//! Long-term memories: `memory_records` in `komo.db`, and the
//! [`MemoryRepository`] over them.
//!
//! The strictest durability rule in the repository lives here: this table may
//! **only ever change additively**. It had its own file (`memory.db`) until
//! docs/adr/0004 moved that guarantee to the table; the file is gone, the rule
//! is not. Markdown (`md_memory.rs`) stays an import/export format, never the
//! canonical backend.
//!
//! Schema is laid out **schema-first**: governance/scope/usage columns land all
//! at once even before every consumer exists, because toasty's `push_schema`
//! is not idempotent. See `docs/personal-agent-roadmap.md`.

use std::path::Path;

use anyhow::Context;
use async_trait::async_trait;

use crate::memory::md_memory::MdMemoryStore;
use crate::persistence::db::Db;
use crate::persistence::with_write_retry;
use komo_core::domain::memory::{
    Evidence, Memory, MemoryRepository, MemoryScope, parse_belief_state, parse_memory_confidence,
    parse_memory_kind, parse_memory_provenance, parse_memory_status,
};

// Optional i64 fields use 0 as the "unset" sentinel (same convention as `Db`).
#[derive(Debug, toasty::Model)]
pub(crate) struct MemoryRecord {
    #[key]
    id: String,
    kind: String,
    content: String,
    status: String,
    confidence: String,
    importance: i64,
    pinned: bool,
    scope_type: String,
    scope_key: String,
    source: String,
    source_message_id: String,
    created_at: i64,
    updated_at: i64,
    expires_at: i64,
    last_used_at: i64,
    // Who the claim came from: `user` or `tool`. Additive column, defaulting to
    // `user` — everything written before it existed was extracted from a
    // conversation.
    provenance: String,
    // Truth signals — see `Memory`'s own docs for why they are kept apart from
    // `recall_count`.
    belief_state: String,
    support_count: i64,
    contradiction_count: i64,
    last_confirmed_at: i64,
    superseded_by: String,
    // JSON array of `Evidence`; empty when none. JSON rather than a child table
    // because the list is capped at a handful of entries and is always read with
    // its memory — a join would buy nothing and cost `list()`, which runs every
    // turn.
    evidence: String,
    recall_count: i64,
    // Base64 of the L2-normalized embedding's little-endian f32 bytes; empty
    // when not embedded. Base64 rather than a JSON array because a 1024-dim
    // vector is ~5.5 KB encoded against ~12 KB as text, and `list()` loads
    // every row on every turn.
    embedding: String,
    // Model that produced `embedding`; empty when not embedded.
    embedding_model: String,
}

/// Encode an embedding for storage: little-endian f32 bytes, base64. Empty
/// vector → empty string, so "not embedded" needs no sentinel.
fn encode_embedding(vector: &[f32]) -> String {
    use base64::Engine;
    if vector.is_empty() {
        return String::new();
    }
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Decode a stored embedding. Anything malformed — bad base64, a length that is
/// not a whole number of f32s — reads as *not embedded* rather than failing the
/// load: a corrupt vector must cost recall quality, never access to the memory
/// itself.
fn decode_embedding(encoded: &str) -> Vec<f32> {
    use base64::Engine;
    if encoded.is_empty() {
        return Vec::new();
    }
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return Vec::new();
    };
    if bytes.len() % 4 != 0 {
        return Vec::new();
    }
    bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Encode an evidence list as JSON. Empty list → empty string, so "no evidence"
/// needs no sentinel and costs no bytes on the many rows that have none.
fn encode_evidence(evidence: &[Evidence]) -> String {
    if evidence.is_empty() {
        return String::new();
    }
    serde_json::to_string(evidence).unwrap_or_default()
}

/// Decode a stored evidence list. Malformed JSON reads as *no evidence* rather
/// than failing the load — provenance is an audit aid, and losing it must never
/// cost access to the memory itself.
fn decode_evidence(encoded: &str) -> Vec<Evidence> {
    if encoded.is_empty() {
        return Vec::new();
    }
    serde_json::from_str(encoded).unwrap_or_default()
}

/// Connection to the memory database. Holds only `MemoryRecord`.
///
/// Backed by the Turso engine with a per-operation connection pool: `inner` is a
/// plain `Arc<toasty::Db>` (no outer `Mutex`), and every method checks out a
/// pooled `Connection`, so independent reads/writes run concurrently. Writes use
/// Turso's MVCC concurrent-write mode and retry on commit conflict (see
/// `infra::persistence::with_write_retry`).
/// Every memory in a legacy `memory.db`, for the one-time merge into
/// `komo.db`.
///
/// The old file is brought up to the current column set first — a `memory.db`
/// written before `belief_state` (or with the retired `recall_query_hashes`
/// still on it) cannot be read through today's model — and a pre-Turso SQLite
/// file is opened with the SQLite driver, because that per-store migration ran
/// here before the merge and dropping the path would strand anyone who had not
/// upgraded through it.
pub(crate) async fn import_from(path: &Path) -> anyhow::Result<Vec<Memory>> {
    let native = crate::persistence::turso_marker_path(path).exists();
    if native {
        ensure_columns(path).await?;
    }
    let url = match native {
        true => format!("turso:{}", path.display()),
        false => format!("sqlite:{}", path.display()),
    };
    let db = toasty::Db::builder()
        .models(toasty::models!(MemoryRecord))
        .connect(&url)
        .await
        .with_context(|| format!("opening {} to merge it in", path.display()))?;
    let mut conn = db.connection().await?;
    let rows = toasty::query!(MemoryRecord).exec(&mut conn).await?;
    Ok(rows.into_iter().map(memory_from_record).collect())
}

/// Bring an existing file's `memory_records` up to the current column set,
/// before toasty opens it.
pub(crate) async fn ensure_schema(path: &Path) -> anyhow::Result<()> {
    ensure_columns(path).await
}

impl Db {
    /// One-time migration: import every memory from a legacy markdown directory
    /// into a freshly-created db. No-op when the directory is absent or the db
    /// already holds memories (so it is safe to call on every startup). Returns
    /// the number imported.
    pub async fn import_legacy_markdown(&self, dir: &Path) -> anyhow::Result<usize> {
        // Only seed an empty db — never double-import or fight live writes.
        if !self.list().await?.is_empty() {
            return Ok(0);
        }
        let legacy = MdMemoryStore::new(dir.to_path_buf());
        let memories = legacy.read_all().await?;
        let count = memories.len();
        for memory in &memories {
            self.save(memory).await?;
        }
        Ok(count)
    }
}

fn record_from_memory(memory: &Memory) -> MemoryRecord {
    MemoryRecord {
        id: memory.id.clone(),
        kind: memory.kind.as_str().to_string(),
        content: memory.content.clone(),
        status: memory.status.as_str().to_string(),
        confidence: memory.confidence.as_str().to_string(),
        importance: memory.importance as i64,
        pinned: memory.pinned,
        scope_type: memory.scope.type_str().to_string(),
        scope_key: memory.scope.key(),
        source: memory.source.clone(),
        source_message_id: memory.source_message_id.clone(),
        created_at: memory.created_at,
        updated_at: memory.updated_at,
        expires_at: memory.expires_at.unwrap_or(0),
        last_used_at: memory.last_used_at.unwrap_or(0),
        provenance: memory.provenance.as_str().to_string(),
        belief_state: memory.belief.as_str().to_string(),
        support_count: memory.support_count,
        contradiction_count: memory.contradiction_count,
        last_confirmed_at: memory.last_confirmed_at.unwrap_or(0),
        superseded_by: memory.superseded_by.clone(),
        evidence: encode_evidence(&memory.evidence),
        recall_count: memory.recall_count,
        embedding: encode_embedding(&memory.embedding),
        embedding_model: memory.embedding_model.clone(),
    }
}

fn memory_from_record(record: MemoryRecord) -> Memory {
    let nonzero = |v: i64| (v != 0).then_some(v);
    Memory {
        id: record.id,
        kind: parse_memory_kind(&record.kind),
        content: record.content,
        status: parse_memory_status(&record.status),
        confidence: parse_memory_confidence(&record.confidence),
        importance: record.importance as i32,
        pinned: record.pinned,
        scope: MemoryScope::from_parts(&record.scope_type, &record.scope_key),
        source: record.source,
        source_message_id: record.source_message_id,
        created_at: record.created_at,
        updated_at: record.updated_at,
        expires_at: nonzero(record.expires_at),
        last_used_at: nonzero(record.last_used_at),
        belief: parse_belief_state(&record.belief_state),
        provenance: parse_memory_provenance(&record.provenance),
        support_count: record.support_count,
        contradiction_count: record.contradiction_count,
        last_confirmed_at: nonzero(record.last_confirmed_at),
        superseded_by: record.superseded_by,
        evidence: decode_evidence(&record.evidence),
        recall_count: record.recall_count,
        embedding: decode_embedding(&record.embedding),
        embedding_model: record.embedding_model,
    }
}

#[async_trait]
impl MemoryRepository for Db {
    async fn save(&self, memory: &Memory) -> anyhow::Result<()> {
        // MVCC: retry the whole transaction on a commit conflict. Each attempt
        // re-checks out its own pooled connection.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let r = record_from_memory(memory);
            // Overwrite on id collision (save is create-or-replace), mirroring
            // the markdown store's filename-keyed overwrite.
            if let Ok(mut existing) = MemoryRecord::get_by_id(&mut conn, &r.id).await {
                existing
                    .update()
                    .kind(r.kind)
                    .content(r.content)
                    .status(r.status)
                    .confidence(r.confidence)
                    .importance(r.importance)
                    .pinned(r.pinned)
                    .scope_type(r.scope_type)
                    .scope_key(r.scope_key)
                    .source(r.source)
                    .source_message_id(r.source_message_id)
                    .updated_at(r.updated_at)
                    .expires_at(r.expires_at)
                    .last_used_at(r.last_used_at)
                    .provenance(r.provenance)
                    .belief_state(r.belief_state)
                    .support_count(r.support_count)
                    .contradiction_count(r.contradiction_count)
                    .last_confirmed_at(r.last_confirmed_at)
                    .superseded_by(r.superseded_by)
                    .evidence(r.evidence)
                    .recall_count(r.recall_count)
                    .embedding(r.embedding)
                    .embedding_model(r.embedding_model)
                    .exec(&mut conn)
                    .await?;
                return Ok(());
            }
            toasty::create!(MemoryRecord {
                id: r.id,
                kind: r.kind,
                content: r.content,
                status: r.status,
                confidence: r.confidence,
                importance: r.importance,
                pinned: r.pinned,
                scope_type: r.scope_type,
                scope_key: r.scope_key,
                source: r.source,
                source_message_id: r.source_message_id,
                created_at: r.created_at,
                updated_at: r.updated_at,
                expires_at: r.expires_at,
                last_used_at: r.last_used_at,
                provenance: r.provenance,
                belief_state: r.belief_state,
                support_count: r.support_count,
                contradiction_count: r.contradiction_count,
                last_confirmed_at: r.last_confirmed_at,
                superseded_by: r.superseded_by,
                evidence: r.evidence,
                recall_count: r.recall_count,
                embedding: r.embedding,
                embedding_model: r.embedding_model,
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn list(&self) -> anyhow::Result<Vec<Memory>> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(MemoryRecord).exec(&mut conn).await?;
        let mut memories: Vec<Memory> = rows
            .into_iter()
            .map(memory_from_record)
            .filter(|m| !m.is_expired(now))
            .collect();
        memories.sort_by_key(|m| m.created_at);
        Ok(memories)
    }

    /// Fetch by id directly — unlike the default (which scans `list`), this
    /// sees expired and any-status rows, so governance can still operate on
    /// them.
    async fn get(&self, id: &str) -> anyhow::Result<Option<Memory>> {
        let mut conn = self.inner.connection().await?;
        Ok(MemoryRecord::get_by_id(&mut conn, id)
            .await
            .ok()
            .map(memory_from_record))
    }
}

/// Bring an existing `memory_records` table up to the current `MemoryRecord`
/// shape by adding any columns it lacks, in place (no data loss, idempotent) —
/// the shared additive migration in `infra/persistence/mod.rs`. When adding a
/// `MemoryRecord` field, extend this list (NOT NULL with a DEFAULT, or nullable).
async fn ensure_columns(path: &Path) -> anyhow::Result<()> {
    const EXPECTED: &[(&str, &str)] = &[
        (
            "recall_count",
            "\"recall_count\" integer NOT NULL DEFAULT 0",
        ),
        // `user` is what every row written before this column meant: they were
        // all extracted from conversations, never from fetched content.
        ("provenance", "\"provenance\" text NOT NULL DEFAULT 'user'"),
        // Truth signals. `belief_state` defaults to `current`, which is exactly
        // what every row written before the column existed means.
        (
            "belief_state",
            "\"belief_state\" text NOT NULL DEFAULT 'current'",
        ),
        (
            "support_count",
            "\"support_count\" integer NOT NULL DEFAULT 0",
        ),
        (
            "contradiction_count",
            "\"contradiction_count\" integer NOT NULL DEFAULT 0",
        ),
        (
            "last_confirmed_at",
            "\"last_confirmed_at\" integer NOT NULL DEFAULT 0",
        ),
        (
            "superseded_by",
            "\"superseded_by\" text NOT NULL DEFAULT ''",
        ),
        ("evidence", "\"evidence\" text NOT NULL DEFAULT ''"),
        ("embedding", "\"embedding\" text NOT NULL DEFAULT ''"),
        (
            "embedding_model",
            "\"embedding_model\" text NOT NULL DEFAULT ''",
        ),
    ];
    crate::persistence::ensure_columns(path, "memory_records", EXPECTED).await?;

    // Columns this komo no longer models. `recall_query_hashes` backed the
    // dream-promotion query-diversity signal, added 2026-07-03 and dropped when
    // promotion moved to truth signals (2026-08-12) — but dropping it from the
    // model left it in every store built in between, `NOT NULL` and with no
    // default, so every insert after the upgrade failed the constraint and the
    // store silently stopped accepting memories. Durable data may only change
    // additively (see AGENTS.md); this is the repair for the one time it did
    // not.
    const RETIRED: &[&str] = &["recall_query_hashes"];
    crate::persistence::drop_retired_columns(path, "memory_records", RETIRED).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::memory::{MemoryConfidence, MemoryContext, MemoryKind, MemoryStatus};

    /// A `komo.db` in a home directory of this test's own, wiped first. Its own
    /// directory because `Db::connect` scans the one it opens for legacy files
    /// to merge — two tests sharing a directory would merge each other's.
    fn turso_url(name: &str) -> String {
        let home = std::env::temp_dir().join(format!("komo-mdb-{name}"));
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        format!("turso:{}", home.join("komo.db").display())
    }

    /// A legacy SQLite `memory.db` — written by the rusqlite backend, two
    /// engines and one file merge ago — must still reach `komo.db`. That
    /// migration used to run per store; the merge is now the only path there
    /// is, so it has to cover the oldest file shape as well as the newest.
    #[tokio::test]
    async fn merges_a_legacy_sqlite_memory_db_into_komo_db() {
        let home = std::env::temp_dir().join("komo-mdb-legacy-sqlite");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let path = home.join("memory.db");

        // 1. Seed a legacy SQLite file with two memories via the SQLite driver.
        {
            let sdb = toasty::Db::builder()
                .models(toasty::models!(MemoryRecord))
                .connect(&format!("sqlite:{}", path.display()))
                .await
                .unwrap();
            sdb.push_schema().await.unwrap();
            let mut conn = sdb.connection().await.unwrap();
            for r in [
                record_from_memory(&Memory::new(MemoryKind::Project, "written in Rust")),
                record_from_memory(&Memory::new(MemoryKind::Fact, "likes coffee")),
            ] {
                toasty::create!(MemoryRecord {
                    id: r.id,
                    kind: r.kind,
                    content: r.content,
                    status: r.status,
                    confidence: r.confidence,
                    importance: r.importance,
                    pinned: r.pinned,
                    scope_type: r.scope_type,
                    scope_key: r.scope_key,
                    source: r.source,
                    source_message_id: r.source_message_id,
                    created_at: r.created_at,
                    updated_at: r.updated_at,
                    expires_at: r.expires_at,
                    last_used_at: r.last_used_at,
                    provenance: r.provenance,
                    belief_state: r.belief_state,
                    support_count: r.support_count,
                    contradiction_count: r.contradiction_count,
                    last_confirmed_at: r.last_confirmed_at,
                    superseded_by: r.superseded_by,
                    evidence: r.evidence,
                    recall_count: r.recall_count,
                    embedding: r.embedding,
                    embedding_model: r.embedding_model,
                })
                .exec(&mut conn)
                .await
                .unwrap();
            }
        }

        // 2. Open `komo.db` beside it: the merge imports the rows and retires
        //    the old file.
        let komo = format!("turso:{}", home.join("komo.db").display());
        let db = Db::connect(&komo).await.unwrap();
        let mut contents: Vec<String> = MemoryRepository::list(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        contents.sort();
        assert_eq!(contents, vec!["likes coffee", "written in Rust"]);
        assert!(
            !path.exists(),
            "the merged file is renamed, not left in place"
        );
        assert!(
            home.join("memory.db.merged-backup").exists(),
            "and kept: it was the only copy of durable data"
        );

        // 3. Add a row, reconnect: nothing re-imports (still 3, not 5).
        MemoryRepository::save(&db, &Memory::new(MemoryKind::Fact, "third"))
            .await
            .unwrap();
        drop(db);
        let db2 = Db::connect(&komo).await.unwrap();
        assert_eq!(
            MemoryRepository::list(&db2).await.unwrap().len(),
            3,
            "must not re-import"
        );
    }

    /// An existing memory.db created before `recall_count` and the truth-signal
    /// columns existed must gain them **in place** on connect — additive ALTER, no
    /// data loss — rather than force a destructive reset. Memories are durable
    /// personal data; "delete the file" is not an available migration.
    /// Every schema komo has ever shipped must still accept a write.
    ///
    /// This is the guard the `recall_query_hashes` outage did not have. Removing
    /// a field from the model removes the column from *new* files only; every
    /// store already on disk keeps it, and a column that is `NOT NULL` without a
    /// default then fails every insert that no longer mentions it. The store
    /// reports no schema problem — it just stops accepting memories.
    ///
    /// **When you remove a field from `MemoryRecord`, add its column name to
    /// `RETIRED` in `ensure_columns`.** A snapshot below still creates that
    /// column, so forgetting makes this test fail instead of making someone's
    /// memory store fail silently, days later.
    ///
    /// Adding a snapshot: paste the `CREATE TABLE` a released komo would have
    /// written, and never edit an existing one — each is a record of a file
    /// somebody may still be running.
    #[tokio::test]
    async fn every_shipped_schema_still_accepts_writes() {
        // The 15 columns before any of the recall/truth work.
        const ORIGINAL: &str = "\"id\" TEXT NOT NULL, \"kind\" TEXT NOT NULL, \"content\" TEXT NOT NULL, \
             \"status\" TEXT NOT NULL, \"confidence\" TEXT NOT NULL, \"importance\" BIGINT NOT NULL, \
             \"pinned\" BOOLEAN NOT NULL, \"scope_type\" TEXT NOT NULL, \"scope_key\" TEXT NOT NULL, \
             \"source\" TEXT NOT NULL, \"source_message_id\" TEXT NOT NULL, \"created_at\" BIGINT NOT NULL, \
             \"updated_at\" BIGINT NOT NULL, \"expires_at\" BIGINT NOT NULL, \"last_used_at\" BIGINT NOT NULL";

        let snapshots: &[(&str, String)] = &[
            ("2026-06 original", ORIGINAL.to_string()),
            (
                // 2026-07-03: dream promotion weighed query diversity. Retired
                // 2026-08-12 — this is the shape that stopped accepting writes.
                "2026-07-03 query diversity",
                format!(
                    "{ORIGINAL}, \"recall_count\" BIGINT NOT NULL, \"recall_query_hashes\" TEXT NOT NULL"
                ),
            ),
        ];

        for (name, columns) in snapshots {
            let home = std::env::temp_dir()
                .join(format!("komo-test-mem-schema-{}", name.replace(' ', "-")));
            std::fs::remove_dir_all(&home).ok();
            std::fs::create_dir_all(&home).expect("test home");
            let path = home.join("memory.db");

            {
                let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                    .build()
                    .await
                    .unwrap();
                let conn = db.connect().unwrap();
                conn.pragma_update("journal_mode", "'mvcc'").await.ok();
                conn.execute(
                    &format!("CREATE TABLE \"memory_records\" ({columns}, PRIMARY KEY (\"id\"))"),
                    (),
                )
                .await
                .unwrap();
            }
            std::fs::write(
                crate::persistence::turso_marker_path(&path),
                b"turso-native\n",
            )
            .unwrap();

            // Merged into a fresh `komo.db` beside it: the old file's columns
            // are brought up to date before it is read, which is what makes an
            // ancient store readable at all.
            let db = Db::connect(&format!("turso:{}", home.join("komo.db").display()))
                .await
                .unwrap_or_else(|e| panic!("`{name}` must still open: {e}"));
            let mut memory = Memory::new(MemoryKind::Fact, "written after the upgrade");
            memory.id = "mem-after".to_string();
            db.save(&memory).await.unwrap_or_else(|e| {
                panic!(
                    "`{name}` must still accept a write — add the retired column to RETIRED: {e}"
                )
            });

            let rows = db.list().await.unwrap();
            assert_eq!(rows.len(), 1, "`{name}`");
            assert_eq!(rows[0].content, "written after the upgrade", "`{name}`");
        }
    }

    #[tokio::test]
    async fn adds_missing_columns_in_place() {
        let home = std::env::temp_dir().join("komo-mdb-addcol");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let path = home.join("memory.db");

        // 1. Seed a turso file with the OLD 15-column schema (no recall_count)
        //    and one row, then drop the handle.
        {
            let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.ok();
            conn.execute(
                "CREATE TABLE \"memory_records\" (\
                 \"id\" TEXT NOT NULL, \"kind\" TEXT NOT NULL, \"content\" TEXT NOT NULL, \
                 \"status\" TEXT NOT NULL, \"confidence\" TEXT NOT NULL, \"importance\" BIGINT NOT NULL, \
                 \"pinned\" BOOLEAN NOT NULL, \"scope_type\" TEXT NOT NULL, \"scope_key\" TEXT NOT NULL, \
                 \"source\" TEXT NOT NULL, \"source_message_id\" TEXT NOT NULL, \"created_at\" BIGINT NOT NULL, \
                 \"updated_at\" BIGINT NOT NULL, \"expires_at\" BIGINT NOT NULL, \"last_used_at\" BIGINT NOT NULL, \
                 PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO \"memory_records\" VALUES \
                 ('mem-old', 'fact', 'a pre-migration memory', 'active', 'confirmed', 50, 0, \
                 'global', '', '', '', 100, 100, 0, 0)",
                (),
            )
            .await
            .unwrap();
        }
        // Mark it turso-native so it is read as one, not staged as sqlite.
        std::fs::write(
            crate::persistence::turso_marker_path(&path),
            b"turso-native\n",
        )
        .unwrap();

        // 2. Merge it into a fresh `komo.db`: the old file's missing columns are
        //    added in place first, which is what makes it readable at all.
        let db = Db::connect(&format!("turso:{}", home.join("komo.db").display()))
            .await
            .unwrap();
        let rows = MemoryRepository::list(&db).await.unwrap();
        assert_eq!(rows.len(), 1, "the pre-migration row survives");
        assert_eq!(rows[0].content, "a pre-migration memory");
        assert_eq!(rows[0].recall_count, 0, "new column defaults to 0");
        // The truth-signal columns are additive too, and their defaults have to
        // read as "believed, nothing recorded" — a pre-migration memory must not
        // arrive contested or superseded.
        assert_eq!(
            rows[0].belief,
            komo_core::domain::memory::BeliefState::Current
        );
        assert_eq!(rows[0].support_count, 0);
        assert_eq!(rows[0].contradiction_count, 0);
        assert_eq!(rows[0].last_confirmed_at, None);
        assert!(rows[0].superseded_by.is_empty());
        assert!(rows[0].evidence.is_empty());

        // 3. The added columns are fully usable: a recall bump persists.
        db.mark_used(&[rows[0].id.clone()], 9_000).await.unwrap();
        let after = db.get("mem-old").await.unwrap().unwrap();
        assert_eq!(after.recall_count, 1);

        // …and so are the new ones: evidence and a contest survive a write.
        let mut memory = after;
        memory.record_evidence(
            "s-1",
            komo_core::domain::memory::EvidenceRelation::Contradicts,
            "actually no",
            9_100,
        );
        memory.contest(9_100);
        db.save(&memory).await.unwrap();
        let reloaded = db.get("mem-old").await.unwrap().unwrap();
        assert_eq!(
            reloaded.belief,
            komo_core::domain::memory::BeliefState::Contested
        );
        assert_eq!(reloaded.contradiction_count, 1);
        assert_eq!(reloaded.evidence.len(), 1);
        assert_eq!(reloaded.evidence[0].session, "s-1");
        assert_eq!(reloaded.evidence[0].excerpt, "actually no");
    }

    #[tokio::test]
    async fn save_list_roundtrip_and_overwrite() {
        let db = Db::connect(&turso_url("komo_memory_db_roundtrip.db"))
            .await
            .unwrap();
        let mut m = Memory::new(MemoryKind::Preference, "prefers concise answers");
        m.pinned = true;
        m.confidence = MemoryConfidence::UserWritten;
        m.scope = MemoryScope::Channel {
            platform: "telegram".into(),
            chat_id: "42".into(),
        };
        db.save(&m).await.unwrap();

        let rows = db.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "prefers concise answers");
        assert!(rows[0].pinned);
        assert_eq!(rows[0].confidence, MemoryConfidence::UserWritten);

        // Overwrite same id.
        let mut updated = m.clone();
        updated.content = "prefers terse answers".into();
        db.save(&updated).await.unwrap();
        let rows = db.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "prefers terse answers");
    }

    #[tokio::test]
    async fn expired_hidden_from_list() {
        let db = Db::connect(&turso_url("komo_memory_db_expired.db"))
            .await
            .unwrap();
        db.save(&Memory::new(MemoryKind::Fact, "live"))
            .await
            .unwrap();
        let mut stale = Memory::new(MemoryKind::Fact, "stale");
        stale.expires_at = Some(1);
        db.save(&stale).await.unwrap();

        let rows = db.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "live");
    }

    #[tokio::test]
    async fn pinned_filters_by_eligibility_and_scope() {
        let db = Db::connect(&turso_url("komo_memory_db_pinned.db"))
            .await
            .unwrap();

        // Eligible: pinned, active, user_written, preference, global.
        let mut good = Memory::new(MemoryKind::Preference, "concise answers");
        good.pinned = true;
        good.confidence = MemoryConfidence::UserWritten;
        db.save(&good).await.unwrap();

        // Not pinned.
        db.save(&Memory::new(MemoryKind::Preference, "not pinned"))
            .await
            .unwrap();

        // Pinned but candidate → excluded.
        let mut cand = Memory::new(MemoryKind::Profile, "candidate");
        cand.pinned = true;
        cand.confidence = MemoryConfidence::UserWritten;
        cand.status = MemoryStatus::Candidate;
        db.save(&cand).await.unwrap();

        let ctx = MemoryContext::local("s1");
        let pinned = db.pinned(&ctx).await.unwrap();
        assert_eq!(pinned.len(), 1);
        assert_eq!(pinned[0].content, "concise answers");
    }

    #[tokio::test]
    async fn recall_returns_in_scope_active_and_candidate_matches() {
        let db = Db::connect(&turso_url("komo_memory_db_recall.db"))
            .await
            .unwrap();

        // Relevant, active, global → recalled.
        db.save(&Memory::new(
            MemoryKind::Project,
            "the komo project is written in Rust",
        ))
        .await
        .unwrap();
        // Irrelevant → excluded by term overlap.
        db.save(&Memory::new(MemoryKind::Fact, "the user likes coffee"))
            .await
            .unwrap();
        // Relevant candidate → INCLUDED (so it can earn its recall signal for
        // the dreaming loop), though it ranks below the active hit.
        let mut cand = Memory::new(MemoryKind::Fact, "the rust toolchain is pinned to nightly");
        cand.status = MemoryStatus::Candidate;
        db.save(&cand).await.unwrap();
        // Relevant but rejected → excluded by status.
        let mut rejected = Memory::new(MemoryKind::Fact, "rust borrow checker notes");
        rejected.status = MemoryStatus::Rejected;
        db.save(&rejected).await.unwrap();
        // Relevant but scoped to another channel → excluded by scope.
        let mut other = Memory::new(MemoryKind::Fact, "rust edition is 2021");
        other.scope = MemoryScope::Channel {
            platform: "feishu".into(),
            chat_id: "oc_other".into(),
        };
        db.save(&other).await.unwrap();

        let ctx = MemoryContext::local("s1");
        let hits = db
            .recall(&ctx, "what language is the rust project in", 5)
            .await
            .unwrap();
        // Active + candidate both recalled; rejected and out-of-scope excluded.
        assert_eq!(hits.len(), 2);
        assert!(
            hits.iter()
                .any(|h| h.memory.content.contains("written in Rust"))
        );
        assert!(
            hits.iter()
                .any(|h| h.memory.status == MemoryStatus::Candidate)
        );
        assert!(
            !hits
                .iter()
                .any(|h| h.memory.status == MemoryStatus::Rejected)
        );
    }

    #[tokio::test]
    async fn mark_used_sets_last_used_without_touching_updated_at() {
        let db = Db::connect(&turso_url("komo_memory_db_mark_used.db"))
            .await
            .unwrap();
        let mut m = Memory::new(MemoryKind::Fact, "recalled at least once");
        m.updated_at = 500;
        db.save(&m).await.unwrap();

        for at in [9_000, 9_100, 9_200, 9_300] {
            db.mark_used(&[m.id.clone()], at).await.unwrap();
        }

        let after = db.get(&m.id).await.unwrap().unwrap();
        assert_eq!(after.last_used_at, Some(9_300));
        assert_eq!(after.recall_count, 4, "each recall bumps the count");
        assert_eq!(after.updated_at, 500, "recall must not bump updated_at");
    }

    #[tokio::test]
    async fn import_legacy_seeds_empty_db_only_once() {
        let dir = std::env::temp_dir().join("komo_memory_db_import_src");
        let _ = std::fs::remove_dir_all(&dir);
        let legacy = MdMemoryStore::new(dir.clone());
        legacy
            .save(&Memory::new(MemoryKind::Project, "uses Rust"))
            .await
            .unwrap();

        let db = Db::connect(&turso_url("komo_memory_db_import.db"))
            .await
            .unwrap();
        assert_eq!(db.import_legacy_markdown(&dir).await.unwrap(), 1);
        assert_eq!(db.list().await.unwrap().len(), 1);
        // Second call is a no-op (db non-empty).
        assert_eq!(db.import_legacy_markdown(&dir).await.unwrap(), 0);
        assert_eq!(db.list().await.unwrap().len(), 1);
    }
}
