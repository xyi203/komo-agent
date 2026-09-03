//! Markdown-file memory store: one `.md` file per memory under a directory
//! (normally `~/.komo/memory/`), with a small frontmatter block for metadata.
//!
//! With the move to a canonical SQLite store (`infra/memory_db.rs`), this is no
//! longer the primary backend — it stays as a human-readable **import/export**
//! format and the source for the one-time legacy migration. Files written
//! before the governance fields existed (only `kind`/`created_at`/`source`/
//! `expiry`) still parse: absent governance fields take migration defaults —
//! a reviewer-sourced memory (`source` set) becomes a low-confidence
//! `Candidate`, a hand-written one an `Active`/`Confirmed` fact.

use std::path::PathBuf;

use async_trait::async_trait;

use komo_core::domain::memory::{
    BeliefState, Memory, MemoryConfidence, MemoryRepository, MemoryScope, MemoryStatus,
    parse_belief_state, parse_memory_confidence, parse_memory_kind, parse_memory_status,
};

pub struct MdMemoryStore {
    dir: PathBuf,
}

impl MdMemoryStore {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Read every memory file in the directory (including expired ones), for
    /// the one-time import into the canonical store. Unlike
    /// [`list`](MemoryRepository::list), this does not drop expired memories —
    /// migration preserves them; pruning is a separate act.
    pub async fn read_all(&self) -> anyhow::Result<Vec<Memory>> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(anyhow::anyhow!("failed to read {:?}: {e}", self.dir)),
        };
        let mut memories = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let text = tokio::fs::read_to_string(&path).await?;
            if let Some(memory) = parse_md(id, &text) {
                memories.push(memory);
            }
        }
        Ok(memories)
    }
}

fn render_md(memory: &Memory) -> String {
    let mut front = format!(
        "---\nkind: {}\nstatus: {}\nconfidence: {}\nimportance: {}\npinned: {}\ncreated_at: {}\nupdated_at: {}\n",
        memory.kind.as_str(),
        memory.status.as_str(),
        memory.confidence.as_str(),
        memory.importance,
        memory.pinned,
        memory.created_at,
        memory.updated_at,
    );
    if !matches!(memory.scope, MemoryScope::Global) {
        front.push_str(&format!("scope_type: {}\n", memory.scope.type_str()));
        front.push_str(&format!("scope_key: {}\n", memory.scope.key()));
    }
    if !memory.source.is_empty() {
        front.push_str(&format!("source: {}\n", memory.source));
    }
    if !memory.source_message_id.is_empty() {
        front.push_str(&format!(
            "source_message_id: {}\n",
            memory.source_message_id
        ));
    }
    if let Some(expires_at) = memory.expires_at {
        front.push_str(&format!("expires_at: {expires_at}\n"));
    }
    if let Some(last_used_at) = memory.last_used_at {
        front.push_str(&format!("last_used_at: {last_used_at}\n"));
    }
    if memory.recall_count > 0 {
        front.push_str(&format!("recall_count: {}\n", memory.recall_count));
    }
    // Belief and the truth signals **must** round-trip. Dropping an embedding or a
    // recall count costs quality; dropping `superseded` resurrects a belief someone
    // deliberately retired, and dropping a support count re-opens a promotion
    // decision that was already earned. Only the evidence *detail* is left out — a
    // JSON blob in frontmatter would be unreadable, and it is provenance, not a
    // decision input (see `Memory::evidence`).
    if memory.belief != BeliefState::Current {
        front.push_str(&format!("belief: {}\n", memory.belief.as_str()));
    }
    if !memory.superseded_by.is_empty() {
        front.push_str(&format!("superseded_by: {}\n", memory.superseded_by));
    }
    if memory.support_count > 0 {
        front.push_str(&format!("support_count: {}\n", memory.support_count));
    }
    if memory.contradiction_count > 0 {
        front.push_str(&format!(
            "contradiction_count: {}\n",
            memory.contradiction_count
        ));
    }
    if let Some(last_confirmed_at) = memory.last_confirmed_at {
        front.push_str(&format!("last_confirmed_at: {last_confirmed_at}\n"));
    }
    format!("{front}---\n\n{}\n", memory.content)
}

/// Parse a memory file. Returns `None` when the file lacks the frontmatter
/// format (a stray note) — such files are skipped, not fatal. Governance fields
/// absent (legacy files) take migration defaults keyed on whether `source` is
/// set (reviewer-distilled → `Candidate`/`Extracted`, else `Active`/`Confirmed`).
fn parse_md(id: &str, text: &str) -> Option<Memory> {
    let rest = text.strip_prefix("---\n")?;
    let (front, body) = rest.split_once("\n---\n")?;

    let mut kind = None;
    let mut created_at = None;
    let mut source = String::new();
    let mut source_message_id = String::new();
    let mut status: Option<MemoryStatus> = None;
    let mut confidence: Option<MemoryConfidence> = None;
    let mut importance: Option<i32> = None;
    let mut pinned = false;
    let mut scope_type = String::new();
    let mut scope_key = String::new();
    let mut updated_at: Option<i64> = None;
    let mut expires_at: Option<i64> = None;
    let mut last_used_at: Option<i64> = None;
    let mut recall_count: i64 = 0;
    let mut belief = BeliefState::Current;
    let mut superseded_by = String::new();
    let mut support_count: i64 = 0;
    let mut contradiction_count: i64 = 0;
    let mut last_confirmed_at: Option<i64> = None;

    for line in front.lines() {
        if let Some(v) = line.strip_prefix("kind:") {
            kind = Some(parse_memory_kind(v.trim()));
        } else if let Some(v) = line.strip_prefix("status:") {
            status = Some(parse_memory_status(v.trim()));
        } else if let Some(v) = line.strip_prefix("confidence:") {
            confidence = Some(parse_memory_confidence(v.trim()));
        } else if let Some(v) = line.strip_prefix("importance:") {
            importance = v.trim().parse::<i32>().ok();
        } else if let Some(v) = line.strip_prefix("pinned:") {
            pinned = v.trim() == "true";
        } else if let Some(v) = line.strip_prefix("scope_type:") {
            scope_type = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("scope_key:") {
            scope_key = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("created_at:") {
            created_at = v.trim().parse::<i64>().ok();
        } else if let Some(v) = line.strip_prefix("updated_at:") {
            updated_at = v.trim().parse::<i64>().ok();
        } else if let Some(v) = line.strip_prefix("source_message_id:") {
            source_message_id = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("source:") {
            source = v.trim().to_string();
        } else if let Some(v) = line
            .strip_prefix("expires_at:")
            .or(line.strip_prefix("expiry:"))
        {
            expires_at = v.trim().parse::<i64>().ok();
        } else if let Some(v) = line.strip_prefix("last_used_at:") {
            last_used_at = v.trim().parse::<i64>().ok();
        } else if let Some(v) = line.strip_prefix("recall_count:") {
            recall_count = v.trim().parse::<i64>().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("belief:") {
            belief = parse_belief_state(v.trim());
        } else if let Some(v) = line.strip_prefix("superseded_by:") {
            superseded_by = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("support_count:") {
            support_count = v.trim().parse::<i64>().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("contradiction_count:") {
            contradiction_count = v.trim().parse::<i64>().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("last_confirmed_at:") {
            last_confirmed_at = v.trim().parse::<i64>().ok();
        }
    }

    let kind = kind?;
    let created_at = created_at?;
    // Legacy migration defaults: reviewer-distilled (source set) is untrusted →
    // Candidate/Extracted; a hand-written file is Active/Confirmed.
    let (default_status, default_confidence) = if source.is_empty() {
        (MemoryStatus::Active, MemoryConfidence::Confirmed)
    } else {
        (MemoryStatus::Candidate, MemoryConfidence::Extracted)
    };

    Some(Memory {
        id: id.to_string(),
        kind,
        content: body.trim().to_string(),
        status: status.unwrap_or(default_status),
        confidence: confidence.unwrap_or(default_confidence),
        // The legacy markdown store predates tool-derived extraction entirely.
        provenance: komo_core::domain::memory::MemoryProvenance::User,
        importance: importance.unwrap_or(komo_core::domain::memory::DEFAULT_IMPORTANCE),
        pinned,
        scope: MemoryScope::from_parts(&scope_type, &scope_key),
        source,
        source_message_id,
        created_at,
        updated_at: updated_at.unwrap_or(created_at),
        expires_at,
        last_used_at,
        belief,
        support_count,
        contradiction_count,
        last_confirmed_at,
        superseded_by,
        // The counts round-trip but the per-observation detail does not: a legacy
        // file predates evidence entirely, and a rendered one deliberately omits
        // the blob. A memory read back therefore knows *how much* backed it up,
        // not *which sessions* — enough for every decision, short of the audit
        // trail the canonical store keeps.
        evidence: Vec::new(),
        recall_count,
        // Markdown carries no vector. Left empty so the backfill embeds it on
        // its own terms rather than importing a stale or foreign one.
        embedding: Vec::new(),
        embedding_model: String::new(),
    })
}

#[async_trait]
impl MemoryRepository for MdMemoryStore {
    async fn list(&self) -> anyhow::Result<Vec<Memory>> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let mut memories = self.read_all().await?;
        // Governance: expired memories are hidden from recall but their files
        // are left on disk — pruning is a separate, deliberate act.
        memories.retain(|m| !m.is_expired(now));
        memories.sort_by_key(|m| m.created_at);
        Ok(memories)
    }

    async fn save(&self, memory: &Memory) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(|e| anyhow::anyhow!("failed to create {:?}: {e}", self.dir))?;
        let path = self.dir.join(format!("{}.md", memory.id));
        tokio::fs::write(&path, render_md(memory))
            .await
            .map_err(|e| anyhow::anyhow!("failed to write {path:?}: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::memory::{EvidenceRelation, MemoryKind};

    fn temp_store(name: &str) -> MdMemoryStore {
        let dir = std::env::temp_dir().join(name);
        let _ = std::fs::remove_dir_all(&dir);
        MdMemoryStore::new(dir)
    }

    #[tokio::test]
    async fn save_then_list_roundtrips() {
        let store = temp_store("komo_md_memory_roundtrip");
        let memory = Memory::new(MemoryKind::Project, "项目用 Rust 写");
        store.save(&memory).await.unwrap();

        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, memory.id);
        assert_eq!(rows[0].kind, MemoryKind::Project);
        assert_eq!(rows[0].content, "项目用 Rust 写");
    }

    #[tokio::test]
    async fn full_model_roundtrips() {
        let store = temp_store("komo_md_memory_full");
        let mut memory = Memory::new(MemoryKind::Preference, "prefers concise replies");
        memory.status = MemoryStatus::Active;
        memory.confidence = MemoryConfidence::UserWritten;
        memory.importance = 90;
        memory.pinned = true;
        memory.scope = MemoryScope::Channel {
            platform: "telegram".into(),
            chat_id: "42".into(),
        };
        memory.source_message_id = "commit-abc".into();
        store.save(&memory).await.unwrap();

        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].confidence, MemoryConfidence::UserWritten);
        assert_eq!(rows[0].importance, 90);
        assert!(rows[0].pinned);
        assert_eq!(
            rows[0].scope,
            MemoryScope::Channel {
                platform: "telegram".into(),
                chat_id: "42".into()
            }
        );
        assert_eq!(rows[0].source_message_id, "commit-abc");
    }

    /// Belief and the truth signals have to survive a round-trip. Losing them is
    /// not a quality regression like a dropped embedding — a `superseded` memory
    /// that reads back as `current` is a retired belief coming back to life, and
    /// an earned support count reading back as zero re-opens a settled promotion.
    #[tokio::test]
    async fn belief_and_truth_signals_roundtrip() {
        let store = temp_store("komo_md_memory_belief");
        let now = 1_700_000_000;
        let mut memory = Memory::new(MemoryKind::Preference, "prefers Python examples");
        memory.record_evidence("s-1", EvidenceRelation::Supports, "I use Python", now);
        memory.record_evidence("s-2", EvidenceRelation::Contradicts, "Rust now", now);
        memory.last_confirmed_at = Some(now);
        memory.supersede("mem-rust", now);
        store.save(&memory).await.unwrap();

        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].belief, BeliefState::Superseded);
        assert!(!rows[0].is_injectable(), "a retired belief stays retired");
        assert_eq!(rows[0].superseded_by, "mem-rust");
        assert_eq!(rows[0].support_count, 1);
        assert_eq!(rows[0].contradiction_count, 1);
        assert_eq!(rows[0].last_confirmed_at, Some(now));
    }

    /// A believed memory writes no belief line — the common case must not pay for
    /// the exceptional one, and an absent line reads back as `current`.
    #[tokio::test]
    async fn a_current_belief_writes_no_frontmatter_line() {
        let memory = Memory::new(MemoryKind::Fact, "komo is written in Rust");
        let rendered = render_md(&memory);
        assert!(!rendered.contains("belief:"), "{rendered}");
        assert_eq!(
            parse_md("mem-1", &rendered).unwrap().belief,
            BeliefState::Current
        );
    }

    #[tokio::test]
    async fn legacy_reviewer_file_migrates_to_candidate() {
        let store = temp_store("komo_md_memory_legacy_reviewer");
        tokio::fs::create_dir_all(&store.dir).await.unwrap();
        // A pre-governance reviewer file: only old fields, source set.
        tokio::fs::write(
            store.dir.join("mem-legacy.md"),
            "---\nkind: user\ncreated_at: 100\nsource: telegram:42\nexpiry: 9999999999\n---\n\nuser likes blue\n",
        )
        .await
        .unwrap();

        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, MemoryKind::Profile); // user → profile
        assert_eq!(rows[0].status, MemoryStatus::Candidate);
        assert_eq!(rows[0].confidence, MemoryConfidence::Extracted);
        assert_eq!(rows[0].expires_at, Some(9999999999));
    }

    #[tokio::test]
    async fn legacy_handwritten_file_migrates_to_active_confirmed() {
        let store = temp_store("komo_md_memory_legacy_hand");
        tokio::fs::create_dir_all(&store.dir).await.unwrap();
        tokio::fs::write(
            store.dir.join("notes.md"),
            "---\nkind: feedback\ncreated_at: 100\n---\n\nbe terse\n",
        )
        .await
        .unwrap();

        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, MemoryStatus::Active);
        assert_eq!(rows[0].confidence, MemoryConfidence::Confirmed);
    }

    #[tokio::test]
    async fn malformed_files_are_skipped() {
        let store = temp_store("komo_md_memory_malformed");
        store
            .save(&Memory::new(MemoryKind::Profile, "valid"))
            .await
            .unwrap();
        std::fs::write(
            std::env::temp_dir().join("komo_md_memory_malformed/stray.md"),
            "just some text, no frontmatter",
        )
        .unwrap();

        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "valid");
    }

    #[tokio::test]
    async fn expired_memories_are_hidden_from_list() {
        let store = temp_store("komo_md_memory_expired");
        store
            .save(&Memory::new(MemoryKind::Profile, "still good"))
            .await
            .unwrap();
        let mut stale = Memory::new(MemoryKind::Profile, "long gone");
        stale.expires_at = Some(1);
        store.save(&stale).await.unwrap();

        let rows = store.list().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "still good");
        // read_all keeps the expired one (migration preserves everything).
        assert_eq!(store.read_all().await.unwrap().len(), 2);
    }
}
