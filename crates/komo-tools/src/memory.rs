use std::sync::Arc;

use async_trait::async_trait;
use komo_services::memory_enrichment::pinned_budget_usage;
use komo_services::memory_query::MemoryQueryService;
use komo_services::tool_execution::SessionContext;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    context::ToolContext,
    memory::{
        EvidenceRelation, Memory, MemoryConfidence, MemoryContext, MemoryKind, MemoryRepository,
        MemoryStatus, ScoredMemory, parse_memory_kind, parse_memory_status,
    },
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

/// Default cap on search results.
const SEARCH_LIMIT: usize = 10;

/// How many possibly-related existing memories a `save` reports back. Enough to
/// surface a contradiction, small enough not to bloat the tool result.
const RELATED_LIMIT: usize = 3;

#[derive(Deserialize)]
struct MemoryArgs {
    action: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    query: Option<String>,
    /// Target memory id (action=update/promote/reject/archive).
    #[serde(default)]
    id: Option<String>,
    /// New status (action=update).
    #[serde(default)]
    status: Option<String>,
    /// Pin/unpin (action=update). Pinning is the only path into L1 injection.
    #[serde(default)]
    pinned: Option<bool>,
    /// New ranking weight 0–100 (action=update).
    #[serde(default)]
    importance: Option<i32>,
    /// Optional TTL in days (action=save).
    #[serde(default)]
    expiry_days: Option<i64>,
    /// Ids of existing memories the new fact replaces (action=save); superseded
    /// in the same call so an outdated fact never coexists with its successor.
    #[serde(default)]
    supersedes: Option<Vec<String>>,
}

impl MemoryArgs {
    /// Some models fill every optional schema field with a placeholder instead
    /// of omitting it (`"id": ""`, `"status": ""`). An empty string is never a
    /// meaningful value for any of these, so normalize it to absent — otherwise
    /// `parse_memory_status("")` silently becomes an `active` filter and a
    /// `list` over an all-candidate store returns nothing.
    fn normalized(mut self) -> Self {
        for field in [
            &mut self.text,
            &mut self.kind,
            &mut self.query,
            &mut self.id,
            &mut self.status,
        ] {
            if field.as_deref().is_some_and(|s| s.trim().is_empty()) {
                *field = None;
            }
        }
        // Same placeholder problem in list form: `"supersedes": []` or `[""]`.
        if let Some(ids) = &mut self.supersedes {
            ids.retain(|id| !id.trim().is_empty());
            if ids.is_empty() {
                self.supersedes = None;
            }
        }
        self
    }
}

/// Long-term, cross-session memory with governance. The model `save`s facts,
/// `search`es them (scoped to the current chat/session), and curates the
/// library: `promote` a candidate to active, `reject`/`archive` it, or `update`
/// fields (including `pinned`, which gates L1 per-turn injection). Storage lives
/// behind [`MemoryRepository`] — the same store the reviewer writes to.
///
/// Searching goes through the same [`MemoryQueryService`] as automatic recall, so
/// what the model can find by asking is exactly what it can be handed
/// unprompted — including candidates and cross-language matches.
pub struct MemoryTool {
    memories: Arc<dyn MemoryRepository>,
    query: Arc<MemoryQueryService>,
}

impl MemoryTool {
    pub fn new(memories: Arc<dyn MemoryRepository>, query: Arc<MemoryQueryService>) -> Self {
        Self { memories, query }
    }

    /// A Hermes-style usage line for the L1 pinned profile — the one memory
    /// surface with a real, finite budget (it is injected verbatim every turn).
    /// Surfacing "how full is it" nudges the model to keep pinned compact and
    /// curate before adding. Returns `None` when nothing is pinned (no pressure
    /// to report). Best-effort: a load failure just omits the line.
    async fn pinned_usage_line(&self, scope: &MemoryContext) -> Option<String> {
        let pinned = self.memories.pinned(scope).await.ok()?;
        let (used, budget) = pinned_budget_usage(&pinned);
        if used == 0 {
            return None;
        }
        let pct = (used * 100) / budget;
        Some(format!(
            "L1 pinned profile: {used}/{budget} chars ({pct}%) used."
        ))
    }

    /// Load a memory by id or return a helpful error.
    /// Look up the memory an action names. A missing / unknown id is the model's
    /// mistake to fix, so both map to [`ToolError::InvalidInput`] rather than a
    /// retryable failure.
    async fn require(&self, id: &Option<String>) -> Result<Memory, ToolError> {
        let id = id.as_deref().ok_or_else(|| {
            ToolError::InvalidInput("`id` is required for this action".to_string())
        })?;
        self.memories
            .get(id)
            .await?
            .ok_or_else(|| ToolError::InvalidInput(format!("no memory with id `{id}`")))
    }
}

#[async_trait]
impl Tool for MemoryTool {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn description(&self) -> &'static str {
        "Persistent long-term memory across sessions, with governance. \
         action=\"save\" stores a fact (optional kind: profile | preference | feedback | \
         project | person | fact | decision | reference); action=\"search\" returns facts \
         matching a query (scoped to this chat) — by meaning as well as by wording, so \
         it matches across languages, and it is worth re-searching with different terms \
         when the memories you were handed are close but not enough; action=\"list\" \
         returns stored facts; \
         action=\"update\" changes a memory by id (status / pinned / importance / kind / \
         content); action=\"promote\" marks a candidate active; action=\"reject\" / \
         \"archive\" retire one. Pin a memory (update pinned=true) only when the user \
         confirms it as durable profile context. \
         Write each memory as a declarative fact, not an instruction (\"User prefers \
         concise replies\" ✓, \"Always reply concisely\" ✗), and prioritize what reduces \
         future steering. Do not save anything that will be stale within a week — task \
         progress, completed-work logs, PR/issue numbers, or commit SHAs do not belong here. \
         When a new fact replaces or contradicts a stored one (a changed preference, a \
         corrected fact), pass `supersedes: [ids]` on save: the outdated memory is \
         retired as history that points at its replacement, instead of coexisting with \
         it. `save` reports possibly related existing memories so you can catch the \
         conflict while you are still in context."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["save", "search", "list", "update", "promote", "reject", "archive"],
                    "description": "The memory operation to perform."
                },
                "text": { "type": "string", "description": "Fact to store (action=save) or new content (action=update)." },
                "kind": {
                    "type": "string",
                    "enum": ["profile", "preference", "feedback", "project", "person", "fact", "decision", "reference"],
                    "description": "Category (action=save, default profile; or action=update)."
                },
                "query": { "type": "string", "description": "Search term (action=search)." },
                "id": { "type": "string", "description": "Target memory id (action=update/promote/reject/archive)." },
                "status": { "type": "string", "enum": ["candidate", "active", "archived", "rejected"], "description": "New status (action=update)." },
                "pinned": { "type": "boolean", "description": "Pin/unpin for L1 injection (action=update). Only pin user-confirmed durable facts." },
                "importance": { "type": "integer", "description": "Ranking weight 0–100 (action=update)." },
                "expiry_days": { "type": "integer", "description": "Optional TTL in days (action=save); omit for permanent." },
                "supersedes": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Ids of stored memories the new fact replaces (action=save); they are retired as history in the same call, linked to the new memory."
                }
            },
            "required": ["action"]
        })
    }

    async fn call(&self, input: Value, tool_ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: MemoryArgs = parse_args::<MemoryArgs>(&input)?.normalized();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        // Scope comes from the *explicit* per-call context (tool trait v2), not
        // the ambient task-local: `memory` was the last tool reading that seam.
        let scope = memory_context(&tool_ctx.session);

        match args.action.as_str() {
            "save" => {
                let text = args.text.ok_or_else(|| {
                    ToolError::InvalidInput("`text` is required for action=save".to_string())
                })?;
                let kind = args
                    .kind
                    .as_deref()
                    .map(parse_memory_kind)
                    .unwrap_or(MemoryKind::Profile);
                // Validate every superseded id *before* any write, so a typo'd
                // id can never leave the save half-applied.
                let mut superseded: Vec<Memory> = Vec::new();
                for id in args.supersedes.as_deref().unwrap_or_default() {
                    superseded.push(self.require(&Some(id.clone())).await?);
                }
                // Look for possibly-conflicting memories *before* the save, so the
                // new memory cannot be reported against itself. Through the shared
                // query service, so this catches a cross-language near-duplicate —
                // exactly the case a lexical scan misses and the user then has to
                // correct by hand.
                let related = self
                    .query
                    .lookup(&scope, &text, RELATED_LIMIT + superseded.len())
                    .await
                    .map_err(ToolError::Failed)?;

                let mut memory = Memory::new(kind, text);
                // An explicit user save is the highest trust tier.
                memory.confidence = MemoryConfidence::UserWritten;
                // …and it is a confirmation, not merely an origin: the user asked
                // for this to be remembered. Stamped so the freshness clock starts
                // now rather than at "never vouched for", and recorded as evidence
                // so provenance is uniform across every write path.
                memory.last_confirmed_at = Some(now);
                memory.record_evidence(
                    &tool_ctx.session.session_id,
                    EvidenceRelation::Supports,
                    &memory.content.clone(),
                    now,
                );
                // Scope to the current chat so a channel fact does not leak elsewhere.
                memory.scope = scope.write_scope();
                if let Some(days) = args.expiry_days.filter(|d| *d > 0) {
                    memory.expires_at = Some(now + days * 86_400);
                }
                self.memories.save(&memory).await?;
                let mut out = format!("Saved memory {}.", memory.id);

                let superseded_ids: Vec<String> = superseded.iter().map(|m| m.id.clone()).collect();
                if !superseded.is_empty() {
                    for mut old in superseded.drain(..) {
                        // `supersede`, not `Archived`: the two express different
                        // things, and conflating them loses both. Archived means
                        // "retired, nobody needed it"; superseded means "was true,
                        // this replaced it" — it carries a forward link, stays
                        // queryable as history ("what did I use to prefer"), and is
                        // already barred from injection by `is_injectable`. It is
                        // also what the reviewer-side consolidation seam writes, so
                        // the explicit and the automated path now say the same
                        // thing about the same event.
                        old.supersede(&memory.id, now);
                        self.memories.save(&old).await?;
                    }
                    out.push_str(&format!("\nSuperseded: {}.", superseded_ids.join(", ")));
                }

                // Surface possibly related existing memories so a contradiction is
                // caught while the model is still in context. The consolidation seam
                // only sees a conversation *after* it ends, so on this path the
                // model is the conflict detector.
                let related: Vec<&ScoredMemory> = related
                    .iter()
                    .filter(|h| !superseded_ids.contains(&h.memory.id))
                    .take(RELATED_LIMIT)
                    .collect();
                if !related.is_empty() {
                    out.push_str(
                        "\nPossibly related existing memories — if the new fact replaces one, \
                         save again with `supersedes: [id]`, or archive it (action=archive):",
                    );
                    for hit in &related {
                        out.push('\n');
                        out.push_str(&render_one(&hit.memory));
                    }
                }

                if let Some(usage) = self.pinned_usage_line(&scope).await {
                    out.push('\n');
                    out.push_str(&usage);
                }
                Ok(ToolOutput::text(out).with_structured(json!({ "id": memory.id })))
            }
            "list" => {
                let mut memories = self.memories.list().await?;
                let total = memories.len();
                let breakdown = status_breakdown(&memories);
                if let Some(status) = args.status.as_deref().map(parse_memory_status) {
                    memories.retain(|m| m.status == status);
                }
                let mut out = if memories.is_empty() && total > 0 {
                    // A status filter that matched nothing must not read as "the
                    // store is empty" — say where the memories actually are so
                    // the model can re-list instead of concluding there are none.
                    format!(
                        "No memories with that status, but {total} exist: {breakdown}. \
                         Call list without `status` to see them."
                    )
                } else {
                    render(&memories)
                };
                if let Some(usage) = self.pinned_usage_line(&scope).await {
                    out.push_str("\n\n");
                    out.push_str(&usage);
                }
                Ok(ToolOutput::text(out).with_title(format!("{} memories", memories.len())))
            }
            "search" => {
                let text = args.query.ok_or_else(|| {
                    ToolError::InvalidInput("`query` is required for action=search".to_string())
                })?;
                let hits = self
                    .query
                    .lookup(&scope, &text, SEARCH_LIMIT)
                    .await
                    .map_err(ToolError::Failed)?;
                Ok(ToolOutput::text(render_scored(&hits))
                    .with_title(format!("{} matches", hits.len())))
            }
            "update" => {
                let mut memory = self.require(&args.id).await?;
                if let Some(text) = args.text {
                    memory.content = text;
                }
                if let Some(kind) = args.kind.as_deref() {
                    memory.kind = parse_memory_kind(kind);
                }
                if let Some(status) = args.status.as_deref() {
                    memory.status = parse_memory_status(status);
                }
                if let Some(pinned) = args.pinned {
                    memory.pinned = pinned;
                    // Pinning requires high confidence to actually surface in L1.
                    if pinned && memory.confidence == MemoryConfidence::Extracted {
                        memory.confidence = MemoryConfidence::Confirmed;
                    }
                }
                if let Some(importance) = args.importance {
                    memory.importance = importance.clamp(0, 100);
                }
                memory.updated_at = now;
                self.memories.save(&memory).await?;
                Ok(ToolOutput::text(format!("Updated memory {}.", memory.id)))
            }
            "promote" => {
                let mut memory = self.require(&args.id).await?;
                memory.promote(now);
                self.memories.save(&memory).await?;
                Ok(ToolOutput::text(format!(
                    "Promoted memory {} to active.",
                    memory.id
                )))
            }
            "reject" => set_status(self, &args.id, MemoryStatus::Rejected, now).await,
            "archive" => set_status(self, &args.id, MemoryStatus::Archived, now).await,
            other => Err(ToolError::InvalidInput(format!(
                "unknown action `{other}` (expected save/search/list/update/promote/reject/archive)"
            ))),
        }
    }
}

/// The memory context for this call: the turn's own session, plus its
/// correspondent's channel when the turn has one (a chat turn does, a local one
/// does not).
fn memory_context(session: &SessionContext) -> MemoryContext {
    MemoryContext::new(&session.session_id, session.channel.as_ref())
}

async fn set_status(
    tool: &MemoryTool,
    id: &Option<String>,
    status: MemoryStatus,
    now: i64,
) -> Result<ToolOutput, ToolError> {
    let mut memory = tool.require(id).await?;
    memory.status = status;
    memory.updated_at = now;
    tool.memories.save(&memory).await?;
    Ok(ToolOutput::text(format!(
        "Set memory {} to {}.",
        memory.id,
        status.as_str()
    )))
}

/// Count memories per status, e.g. `candidate=24, archived=2`.
fn status_breakdown(memories: &[Memory]) -> String {
    let mut counts: Vec<(&str, usize)> = Vec::new();
    for m in memories {
        let name = m.status.as_str();
        match counts.iter_mut().find(|(n, _)| *n == name) {
            Some((_, c)) => *c += 1,
            None => counts.push((name, 1)),
        }
    }
    counts
        .iter()
        .map(|(n, c)| format!("{n}={c}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render(memories: &[Memory]) -> String {
    if memories.is_empty() {
        return "(no memories)".to_string();
    }
    memories
        .iter()
        .map(render_one)
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_one(m: &Memory) -> String {
    let pin = if m.pinned { " 📌" } else { "" };
    // Belief is shown only when it is *not* `current`, so the common line stays
    // short — but a contested or superseded memory can never be read as an
    // ordinary fact, which is the whole point of it being searchable at all.
    let belief = if m.is_injectable() {
        String::new()
    } else {
        format!("/{}", m.belief.as_str())
    };
    let mut line = format!(
        "[{}/{}/{}{}{}] {}: {}",
        m.kind.as_str(),
        m.status.as_str(),
        m.scope.type_str(),
        belief,
        pin,
        m.id,
        m.content
    );
    if !m.superseded_by.is_empty() {
        line.push_str(&format!(" (replaced by {})", m.superseded_by));
    }
    if !m.source.is_empty() {
        line.push_str(&format!(" (from {})", m.source));
    }
    line
}

fn render_scored(hits: &[ScoredMemory]) -> String {
    if hits.is_empty() {
        return "(no matches)".to_string();
    }
    hits.iter()
        .map(|h| render_one(&h.memory))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_infra::memory::md_memory::MdMemoryStore;

    fn temp_tool(name: &str) -> MemoryTool {
        let dir = std::env::temp_dir().join(name);
        let _ = std::fs::remove_dir_all(&dir);
        let store: Arc<dyn MemoryRepository> = Arc::new(MdMemoryStore::new(dir));
        // No embedding backend: the lexical arm alone, which is what a machine
        // with no Ollama running gets.
        let query = Arc::new(MemoryQueryService::new(store.clone()));
        MemoryTool::new(store, query)
    }

    /// A CLI-shaped session: global + session scope, no channel scope.
    fn ctx() -> ToolContext {
        crate::test_support::detached_ctx("cli:test")
    }

    #[tokio::test]
    async fn save_list_search_roundtrip() {
        let tool = temp_tool("komo_mem_tool_test");

        tool.call(json!({ "action": "save", "text": "用户喜欢蓝色" }), &ctx())
            .await
            .unwrap();
        tool.call(
            json!({ "action": "save", "text": "项目用 Rust 写", "kind": "project" }),
            &ctx(),
        )
        .await
        .unwrap();

        let list = tool
            .call(json!({ "action": "list" }), &ctx())
            .await
            .unwrap()
            .text;
        assert!(list.contains("蓝色"));
        assert!(list.contains("Rust"));
        assert!(list.contains("[project/"));

        let hit = tool
            .call(json!({ "action": "search", "query": "rust" }), &ctx())
            .await
            .unwrap()
            .text;
        assert!(hit.contains("Rust"));
        assert!(!hit.contains("蓝色"));
    }

    /// The exact call shape observed from a model that fills every optional
    /// field with a placeholder (run-019fc562): `status: "active"` over an
    /// all-candidate store must not read as "the store is empty".
    #[tokio::test]
    async fn list_filtered_to_nothing_reports_where_memories_are() {
        let tool = temp_tool("komo_mem_tool_filler");
        let mut cand = Memory::new(MemoryKind::Fact, "user prefers rebase before push");
        cand.status = MemoryStatus::Candidate;
        tool.memories.save(&cand).await.unwrap();

        let out = tool
            .call(
                json!({
                    "action": "list", "status": "active", "kind": "fact",
                    "id": "", "query": "", "text": "",
                    "importance": 0, "pinned": false, "expiry_days": 0
                }),
                &ctx(),
            )
            .await
            .unwrap()
            .text;
        assert!(!out.contains("(no memories)"));
        assert!(out.contains("candidate=1"));
    }

    /// An empty-string `status` is a placeholder, not an `active` filter
    /// (`parse_memory_status("")` would otherwise default to Active).
    #[tokio::test]
    async fn empty_string_args_are_treated_as_absent() {
        let tool = temp_tool("komo_mem_tool_empty_args");
        let mut cand = Memory::new(MemoryKind::Fact, "protoc lives in /opt/homebrew/bin");
        cand.status = MemoryStatus::Candidate;
        tool.memories.save(&cand).await.unwrap();

        let out = tool
            .call(json!({ "action": "list", "status": "", "id": "" }), &ctx())
            .await
            .unwrap()
            .text;
        assert!(out.contains("protoc"));
    }

    /// The preference-evolution case: the new fact archives the outdated one in
    /// the same call, so the two never coexist in recall.
    #[tokio::test]
    async fn save_with_supersedes_archives_the_outdated_memory() {
        let tool = temp_tool("komo_mem_tool_supersede");
        let mut old = Memory::new(MemoryKind::Preference, "User prefers Python for scripting");
        old.status = MemoryStatus::Active;
        tool.memories.save(&old).await.unwrap();

        let out = tool
            .call(
                json!({
                    "action": "save",
                    "text": "User mainly uses Rust for scripting now",
                    "kind": "preference",
                    "supersedes": [old.id]
                }),
                &ctx(),
            )
            .await
            .unwrap()
            .text;
        assert!(out.contains("Superseded"));
        assert!(out.contains(&old.id));
        // The superseded line must not double as a "possibly related" hint.
        assert!(!out.contains("Possibly related"));

        // Retired as *history*, not archived: no longer injectable, still
        // queryable, and pointing at what replaced it. Same mechanism the
        // reviewer-side consolidation seam uses.
        let retired = tool.memories.get(&old.id).await.unwrap().unwrap();
        assert_eq!(
            retired.belief,
            komo_core::domain::memory::BeliefState::Superseded
        );
        assert!(!retired.is_injectable());
        assert!(
            !retired.superseded_by.is_empty(),
            "history has to point at its replacement"
        );
    }

    /// A save that overlaps a stored fact reports it, so the model can catch a
    /// contradiction while still in context.
    #[tokio::test]
    async fn save_reports_possibly_related_existing_memories() {
        let tool = temp_tool("komo_mem_tool_related");
        let mut old = Memory::new(MemoryKind::Preference, "User prefers Python for scripting");
        old.status = MemoryStatus::Active;
        tool.memories.save(&old).await.unwrap();

        let out = tool
            .call(
                json!({ "action": "save", "text": "User mainly uses Rust for scripting now" }),
                &ctx(),
            )
            .await
            .unwrap()
            .text;
        assert!(out.contains("Possibly related existing memories"));
        assert!(out.contains(&old.id));
        assert!(out.contains("Python"));
        // Nothing was archived — surfacing is a hint, not an action.
        assert_eq!(
            tool.memories.get(&old.id).await.unwrap().unwrap().status,
            MemoryStatus::Active
        );
    }

    /// An unrelated save stays quiet — the hint must not fire on every write.
    #[tokio::test]
    async fn save_with_no_overlap_reports_nothing_related() {
        let tool = temp_tool("komo_mem_tool_unrelated");
        let mut old = Memory::new(MemoryKind::Preference, "User prefers Python for scripting");
        old.status = MemoryStatus::Active;
        tool.memories.save(&old).await.unwrap();

        let out = tool
            .call(
                json!({ "action": "save", "text": "团队周会安排在星期二" }),
                &ctx(),
            )
            .await
            .unwrap()
            .text;
        assert!(!out.contains("Possibly related"));
    }

    /// A typo'd supersede id fails the whole call before any write — the new
    /// memory is not saved, nothing is archived.
    #[tokio::test]
    async fn save_with_unknown_supersede_id_writes_nothing() {
        let tool = temp_tool("komo_mem_tool_supersede_unknown");
        let err = tool
            .call(
                json!({
                    "action": "save",
                    "text": "User mainly uses Rust now",
                    "supersedes": ["mem-nope"]
                }),
                &ctx(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no memory with id"));
        assert!(tool.memories.list().await.unwrap().is_empty());
    }

    /// Placeholder shapes (`[]`, `[""]`) mean "no supersede", not an error.
    #[tokio::test]
    async fn empty_supersedes_placeholders_are_ignored() {
        let tool = temp_tool("komo_mem_tool_supersede_empty");
        let out = tool
            .call(
                json!({ "action": "save", "text": "用户喜欢蓝色", "supersedes": [""] }),
                &ctx(),
            )
            .await
            .unwrap()
            .text;
        assert!(out.contains("Saved memory"));
    }

    #[tokio::test]
    async fn promote_then_pin_via_update() {
        let tool = temp_tool("komo_mem_tool_promote");
        // A candidate (simulating a reviewer extraction).
        let mut cand = Memory::new(MemoryKind::Preference, "prefers concise answers");
        cand.status = MemoryStatus::Candidate;
        cand.confidence = MemoryConfidence::Extracted;
        tool.memories.save(&cand).await.unwrap();

        tool.call(json!({ "action": "promote", "id": cand.id }), &ctx())
            .await
            .unwrap();
        let after = tool.memories.get(&cand.id).await.unwrap().unwrap();
        assert_eq!(after.status, MemoryStatus::Active);
        assert_eq!(after.confidence, MemoryConfidence::Confirmed);

        tool.call(
            json!({ "action": "update", "id": cand.id, "pinned": true }),
            &ctx(),
        )
        .await
        .unwrap();
        let pinned = tool.memories.get(&cand.id).await.unwrap().unwrap();
        assert!(pinned.pinned);
    }

    #[tokio::test]
    async fn reject_and_archive_set_status() {
        let tool = temp_tool("komo_mem_tool_reject");
        let m = Memory::new(MemoryKind::Fact, "ephemeral");
        tool.memories.save(&m).await.unwrap();

        tool.call(json!({ "action": "reject", "id": m.id }), &ctx())
            .await
            .unwrap();
        assert_eq!(
            tool.memories.get(&m.id).await.unwrap().unwrap().status,
            MemoryStatus::Rejected
        );
    }

    #[tokio::test]
    async fn update_unknown_id_errors() {
        let tool = temp_tool("komo_mem_tool_unknown");
        let err = tool
            .call(json!({ "action": "promote", "id": "nope" }), &ctx())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no memory with id"));
    }
}
