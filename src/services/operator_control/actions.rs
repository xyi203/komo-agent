//! Shared operator behavior: the projections and transitions that must be
//! identical whether an operator action runs inside the gateway (behind the
//! HTTP api channel) or in-process against directly-opened stores.
//!
//! Everything here is parameterized by domain repositories/values, never by a
//! transport — the api handlers and the direct adapter both call these, so the
//! business result can't fork between the two paths.

use anyhow::Context;
use komo_core::domain::episode::{OutcomeAssessment, OutcomeVerdict};
use komo_core::domain::session::is_subagent_session;
use komo_services::cron_actions;
/// Re-exported so every operator-control caller keeps naming one place for
/// the unknown-job message, wherever the implementation lives.
pub use komo_services::cron_actions::no_cron_job_message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::domain::cron::{CronJob, CronJobRepository, CronJobSpec};
use crate::domain::home::HomeRepository;
use crate::domain::memory::{
    DreamVerdict, Memory, MemoryRepository, MemoryScope, MemoryStatus, dream_score, dream_verdict,
    is_durable_channel,
};
use crate::domain::message::Message;
use crate::domain::pairing::{ApproveOutcome, PairingRepository, PairingRequest, PairingStatus};
use crate::domain::reminder::{Reminder, ReminderRepository};
use crate::domain::repository::{MessageRepository, SessionRepository, SkillRepository};
use crate::domain::run::{
    MemoryUse, Run, RunRepository, RunStep, resume_prompt, skill_viewed, step_views_skill,
};
use crate::domain::session::Session;
use crate::domain::skill::Skill;
use crate::domain::task::{Task, TaskRepository};

use super::now;
use super::request::{
    DreamItem, DreamReport, MemoryTransitionAction, PairingView, SessionSummary, SkillInvocation,
    SkillUsage, WikiHitView, WikiIndexView, WikiStatusView,
};
use komo_core::domain::chunk_index::{
    ChunkIndex, DIVERSIFY_OVERFETCH, MAX_CHUNKS_PER_FILE, diversify,
};
use komo_core::domain::embedding::EmbeddingClient;
use komo_services::wiki_indexing::WikiIndexRunner;

/// Cosine floor for a wiki hit, kept equal to the `wiki_search` tool's own so
/// `komo wiki search` predicts what a turn would get rather than approximating it.
const WIKI_SCORE_FLOOR: f32 = 0.45;

/// Everything the operator surface needs to serve the note vault.
///
/// Held here rather than opened per request because the gateway already has the
/// index open — that exclusive handle is exactly why these commands cannot run
/// in the CLI's own process while the gateway is up.
pub struct WikiOps {
    /// The one gate every indexing run goes through — this surface's, the
    /// `wiki_index` tool's, and any scheduled job's. Shared so a rebuild started
    /// from a conversation and one started with `komo wiki index --rebuild`
    /// cannot interleave over the same store.
    pub runner: Arc<WikiIndexRunner>,
    pub backend: String,
    pub collection: String,
    /// Human-facing location: the data directory, or the server URL.
    pub location: String,
}

impl WikiOps {
    fn store(&self) -> &Arc<dyn ChunkIndex> {
        self.runner.index()
    }

    fn embedder(&self) -> &Arc<dyn EmbeddingClient> {
        self.runner.embedder()
    }

    fn vault(&self) -> &std::path::Path {
        self.runner.vault()
    }

    fn model(&self) -> &str {
        self.runner.embedding_model()
    }
}

/// The operator use-case implementation over the gateway's repositories: one
/// bundle the HTTP api channel delegates to, so its transport state stops
/// doubling as a dependency list. Methods mirror the operations both
/// transports must agree on and delegate to the same pure helpers the direct
/// adapter uses.
pub struct OperatorActions {
    pub sessions: Arc<dyn SessionRepository>,
    pub messages: Arc<dyn MessageRepository>,
    pub tasks: Arc<dyn TaskRepository>,
    pub memories: Arc<dyn MemoryRepository>,
    pub runs: Arc<dyn RunRepository>,
    pub reminders: Arc<dyn ReminderRepository>,
    /// The concrete store, not `SkillRepository`: that trait carries only the
    /// automated write path (find/list/save), while every governance transition
    /// — promote, archive, expire — is an inherent method on the store.
    pub skills: Arc<komo_infra::skills::FsSkillStore>,
    pub pairings: Arc<dyn PairingRepository>,
    pub home: Arc<dyn HomeRepository>,
    pub cron_jobs: Arc<dyn CronJobRepository>,
    /// `None` when `[wiki]` is missing, or present but unusable as written —
    /// wiki operations then fail with that as the reason rather than looking
    /// like an empty vault. A vault that is merely *unreachable* is still
    /// `Some`: opening retries per call, so it must not read as unconfigured.
    pub wiki: Option<WikiOps>,
    /// The hybrid query service, when an embedder is configured. `None` leaves
    /// `memory_backfill` reporting that there is nothing to embed *with* —
    /// which is the honest answer, not an empty run.
    pub memory_query: Option<Arc<komo_services::memory_query::MemoryQueryService>>,
}

impl OperatorActions {
    fn wiki(&self) -> anyhow::Result<&WikiOps> {
        // Both causes are named because this used to claim the first one only,
        // which sent an operator who *had* configured a vault off to add the
        // section they already had.
        self.wiki.as_ref().context(
            "wiki unavailable: ~/.komo/config.toml has no [wiki] section, or has one \
             komo could not use.\n\n\
             [wiki]\n\
             vault = \"~/notes\"\n\n\
             If it is configured, the gateway log gives the reason — `komo logs | \
             grep wiki`.\n",
        )
    }

    pub async fn wiki_search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<WikiHitView>> {
        self.wiki()?.search(query, limit).await
    }

    pub async fn wiki_status(&self) -> anyhow::Result<WikiStatusView> {
        self.wiki()?.status().await
    }

    pub async fn wiki_index(&self, rebuild: bool) -> anyhow::Result<WikiIndexView> {
        self.wiki()?.index(rebuild).await
    }
}

impl WikiOps {
    pub async fn search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<WikiHitView>> {
        let wiki = self;
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }
        let vectors = wiki.embedder().embed(&[query.to_string()]).await?;
        let vector = vectors
            .into_iter()
            .next()
            .filter(|v| !v.is_empty())
            .context("the embedding backend returned no vector")?;
        // Same over-fetch-then-cap as the tool, so this predicts what a turn gets.
        let candidates = wiki
            .store()
            .search(
                &vector,
                query,
                limit * DIVERSIFY_OVERFETCH,
                WIKI_SCORE_FLOOR,
            )
            .await?;
        Ok(diversify(candidates, limit, MAX_CHUNKS_PER_FILE)
            .into_iter()
            .map(|hit| WikiHitView {
                path: hit.chunk.path,
                heading_path: hit.chunk.heading_path,
                text: hit.chunk.text,
                score: hit.score,
            })
            .collect())
    }

    pub async fn status(&self) -> anyhow::Result<WikiStatusView> {
        let wiki = self;
        let indexed = wiki.store().indexed().await?;
        let spec = wiki.store().vector_spec().await?;
        Ok(WikiStatusView {
            vault: wiki.vault().display().to_string(),
            backend: wiki.backend.clone(),
            collection: wiki.collection.clone(),
            location: wiki.location.clone(),
            model: wiki.model().to_string(),
            files: indexed.len(),
            chunks: wiki.store().count().await?,
            dims: spec.as_ref().map(|(dims, _)| *dims),
            indexed_by: spec
                .map(|(_, model)| model)
                .filter(|model| !model.is_empty()),
        })
    }

    /// Minutes-long by nature. Progress goes to the tracing log rather than back
    /// over the wire: the operator protocol is request/response, and adding a
    /// streaming channel for one command would not pay for itself — `komo logs
    /// -f` already shows it.
    pub async fn index(&self, rebuild: bool) -> anyhow::Result<WikiIndexView> {
        let wiki = self;
        // Through the shared runner, so this refuses rather than racing a run
        // the agent (or a scheduled job) already has going. It logs its own
        // progress and outcome.
        let outcome = wiki.runner.run(rebuild, now()).await.map_err(|busy| {
            anyhow::anyhow!(
                "an index run is already in progress (started {}s ago{}) — wait for it, \
                     or watch it with `komo logs -f`",
                now().saturating_sub(busy.since),
                if busy.rebuild { ", a full rebuild" } else { "" }
            )
        })??;
        Ok(WikiIndexView {
            files_seen: outcome.files_seen,
            files_changed: outcome.files_changed,
            files_removed: outcome.files_removed,
            chunks_written: outcome.chunks_written,
            chunks_total: outcome.chunks_total,
            skipped: outcome.skipped,
        })
    }
}

impl OperatorActions {
    pub async fn session_summaries(&self) -> anyhow::Result<Vec<SessionSummary>> {
        Ok(session_summaries(self.sessions.list().await?))
    }

    pub async fn session_messages(&self, id: &str) -> anyhow::Result<Vec<Message>> {
        self.messages.list_by_session(id).await
    }

    pub async fn open_tasks(&self) -> anyhow::Result<Vec<Task>> {
        self.tasks.list_open().await
    }

    pub async fn list_memories(&self, status: Option<MemoryStatus>) -> anyhow::Result<Vec<Memory>> {
        let mut memories = self.memories.list().await?;
        if let Some(want) = status {
            memories.retain(|m| m.status == want);
        }
        Ok(memories)
    }

    pub async fn memory_transition(
        &self,
        id: &str,
        action: MemoryTransitionAction,
    ) -> anyhow::Result<TransitionOutcome> {
        apply_memory_transition(self.memories.as_ref(), id, action, now()).await
    }

    /// Widen memories stranded in an ephemeral `api` channel scope to `Global`.
    pub async fn repair_memory_scopes(&self) -> anyhow::Result<usize> {
        repair_memory_scopes(self.memories.as_ref()).await
    }

    /// Ranked memory search — the operator's view of the same hybrid query
    /// recall and the model's `memory search` run.
    pub async fn memory_search(&self, query: &str, limit: usize) -> anyhow::Result<Vec<Memory>> {
        let Some(service) = &self.memory_query else {
            anyhow::bail!("memory search is served by the gateway's query service");
        };
        search_memories(service, self.memories.as_ref(), query, limit).await
    }

    /// Embed every memory still missing a current vector.
    pub async fn memory_backfill(&self) -> anyhow::Result<usize> {
        let Some(query) = &self.memory_query else {
            anyhow::bail!(
                "no embedding model is configured — set `[memory] embedding_model` in ~/.komo/config.toml"
            );
        };
        query.backfill_all().await
    }

    pub async fn runs(&self, limit: usize) -> anyhow::Result<Vec<Run>> {
        self.runs.list(limit).await
    }

    pub async fn run(&self, id: &str) -> anyhow::Result<Option<(Run, Vec<RunStep>)>> {
        Ok(match self.runs.get(id).await? {
            Some(run) => {
                let steps = self.runs.steps(&run.id).await?;
                Some((run, steps))
            }
            None => None,
        })
    }

    pub async fn prune_runs(&self, cutoff: i64) -> anyhow::Result<usize> {
        self.runs.prune(cutoff).await
    }

    pub async fn clean_sessions(&self) -> anyhow::Result<usize> {
        self.sessions.delete_empty_sessions().await
    }

    pub async fn set_session_title(&self, id: &str, title: &str) -> anyhow::Result<()> {
        self.sessions.set_title(id, title).await
    }

    pub async fn set_session_status(&self, id: &str, status: &str) -> anyhow::Result<()> {
        self.sessions.set_status(id, status).await
    }

    pub async fn delete_session(&self, id: &str) -> anyhow::Result<bool> {
        self.sessions.delete_session(id).await
    }

    pub async fn pending_reminders(&self) -> anyhow::Result<Vec<Reminder>> {
        let mut pending = self.reminders.list_pending().await?;
        pending.sort_by_key(|r| r.run_at);
        Ok(pending)
    }

    pub async fn list_skills(&self) -> anyhow::Result<Vec<Skill>> {
        let mut skills = self.skills.list().await?;
        skills.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(skills)
    }

    pub async fn skill_audit(&self, name: &str) -> anyhow::Result<Vec<SkillInvocation>> {
        let steps = self.runs.steps_by_tool("skill", AUDIT_SCAN_LIMIT).await?;
        Ok(skill_invocations(
            steps,
            name,
            AUDIT_RESULT_CAP,
            &self.run_verdicts().await?,
        ))
    }

    /// How each recent run turned out, for attributing a skill's loads.
    ///
    /// One bounded read rather than a lookup per step: a skill loaded in fifty
    /// turns would otherwise be fifty key reads to answer one report. A run
    /// outside the window is absent, which reads as `Unknown` — the same thing
    /// it would say if it were there and unsettled.
    async fn run_verdicts(&self) -> anyhow::Result<HashMap<String, OutcomeVerdict>> {
        Ok(run_verdicts(self.runs.list(AUDIT_SCAN_LIMIT).await?))
    }

    /// Which turns a memory reached the prompt of, newest first.
    ///
    /// The direction that gets asked after a memory turns out to be wrong:
    /// what did it already shape? `Run.memories` answers the other way round
    /// (this turn used these memories) and cannot be read backwards without
    /// scanning every run.
    pub async fn memory_used(
        &self,
        memory_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<MemoryUse>> {
        self.runs.runs_using_memory(memory_id, limit).await
    }

    /// Every active skill ranked coldest-first — the aggregate the per-name
    /// audit could not answer ("which of these has nobody used?").
    pub async fn skill_usage(&self) -> anyhow::Result<Vec<SkillUsage>> {
        let steps = self.runs.steps_by_tool("skill", AUDIT_SCAN_LIMIT).await?;
        let names = self.skills.list().await?.into_iter().map(|s| s.name);
        Ok(skill_usage(names, steps, &self.run_verdicts().await?))
    }

    pub async fn pairing_views(&self) -> anyhow::Result<Vec<PairingView>> {
        Ok(pairing_views(self.pairings.list().await?, now()))
    }

    pub async fn pair_approve(&self, code: &str) -> anyhow::Result<ApproveOutcome> {
        self.pairings.approve_code(code).await
    }

    pub async fn pair_revoke(&self, id: &str) -> anyhow::Result<bool> {
        self.pairings.revoke(id).await
    }

    pub async fn dream_preview(&self) -> anyhow::Result<DreamReport> {
        let now = now();
        let mut report = dream_classify(&self.memories.list().await?, now);
        let (expire_skills, skill_candidate_count) =
            dream_classify_skills(&self.skills.list_candidates(), now);
        report.expire_skills = expire_skills;
        report.skill_candidate_count = skill_candidate_count;
        Ok(report)
    }

    pub async fn home_override(&self) -> anyhow::Result<Option<String>> {
        self.home.get().await
    }

    pub async fn list_cron_jobs(&self) -> anyhow::Result<Vec<CronJob>> {
        self.cron_jobs.list().await
    }

    pub async fn add_cron_job(&self, spec: CronJobSpec) -> anyhow::Result<CronJob> {
        cron_actions::add_cron_job(self.cron_jobs.as_ref(), spec, now()).await
    }

    pub async fn remove_cron_job(&self, name: &str) -> anyhow::Result<bool> {
        self.cron_jobs.delete(name).await
    }

    pub async fn set_cron_enabled(
        &self,
        name: &str,
        enabled: bool,
    ) -> anyhow::Result<Option<CronJob>> {
        cron_actions::set_cron_enabled(self.cron_jobs.as_ref(), name, enabled, now()).await
    }

    pub async fn trigger_cron_job(&self, name: &str) -> anyhow::Result<Option<CronJob>> {
        cron_actions::trigger_cron_job(self.cron_jobs.as_ref(), name, now()).await
    }
}

/// How many `skill`-tool ledger steps one audit request scans, and how many
/// matches it returns.
pub const AUDIT_SCAN_LIMIT: usize = 500;
pub const AUDIT_RESULT_CAP: usize = 50;

/// How many recent runs a no-id `run resume` scans for the latest recoverable.
pub const RESUME_SCAN_LIMIT: usize = 100;

/// The message a no-id resume gets when nothing is recoverable.
pub const NO_RECOVERABLE: &str =
    "no recoverable runs — nothing was interrupted, or it was already resumed";

/// One uniform not-recoverable message (the gateway's 409 body and the direct
/// path's error must read identically).
pub fn not_recoverable_message(id: &str, status: &str) -> String {
    format!(
        "run `{id}` is not recoverable (status: {status} — it finished normally, \
         failed without interruption, or was already resumed)"
    )
}

/// A memory governance transition's result: applied, or no such id (each
/// transport maps `NotFound` to its own shape — 404 vs. a CLI error).
pub enum TransitionOutcome {
    Applied(Box<Memory>),
    NotFound,
}

/// Apply one governance transition — the domain owns the semantics
/// (`Memory::promote/reject/pin`), so both transports share one definition.
pub async fn apply_memory_transition(
    memories: &dyn MemoryRepository,
    id: &str,
    action: MemoryTransitionAction,
    now: i64,
) -> anyhow::Result<TransitionOutcome> {
    let Some(mut memory) = memories.get(id).await? else {
        return Ok(TransitionOutcome::NotFound);
    };
    (action.apply())(&mut memory, now);
    memories.save(&memory).await?;
    Ok(TransitionOutcome::Applied(Box::new(memory)))
}

/// Widen every memory stuck in an ephemeral `api` channel scope to `Global`,
/// returning how many were moved.
///
/// A one-shot repair for memories written before `MemoryContext::write_scope`
/// learned that `api` chat ids are per-conversation (see
/// `komo_core::domain::memory::is_durable_channel`). Those memories name a
/// conversation that has ended, so no later turn can ever recall them — they
/// are invisible rather than private, and widening them to `Global` restores
/// exactly the reach they were meant to have.
///
/// Deliberately operator-invoked rather than run at startup: this rewrites
/// durable personal data, so it is the operator's call, not a silent migration.
/// Idempotent — a second run finds nothing left to move. Only `api` scopes are
/// touched; a real chat channel's scope is a privacy boundary and stays put.
/// Ranked memory search over the whole library, one implementation for both
/// operator paths (gateway and direct).
///
/// Reuses recall's own query building and ranking (`select_recall`) rather than
/// a private matcher, which is what retired the CLI's old substring scan: a
/// substring cannot cross languages and cannot even find "智能设备" in a memory
/// that says 智能插座. One deliberate difference from recall — the scope
/// context is built from the scopes the library actually holds, so nothing is
/// hidden: this is an operator surface, like the `Memories` listing above it.
pub async fn search_memories(
    service: &komo_services::memory_query::MemoryQueryService,
    memories: &dyn MemoryRepository,
    text: &str,
    limit: usize,
) -> anyhow::Result<Vec<Memory>> {
    let all = memories.list().await?;
    let mut allowed_scopes: Vec<komo_core::domain::memory::MemoryScope> = Vec::new();
    for memory in &all {
        if !allowed_scopes.contains(&memory.scope) {
            allowed_scopes.push(memory.scope.clone());
        }
    }
    let ctx = komo_core::domain::memory::MemoryContext { allowed_scopes };
    let query = service.build_query(text).await;
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    Ok(
        komo_core::domain::memory::select_recall(&all, &ctx, &query, limit, now)
            .into_iter()
            .map(|scored| scored.memory)
            .collect(),
    )
}

pub async fn repair_memory_scopes(memories: &dyn MemoryRepository) -> anyhow::Result<usize> {
    let mut repaired = 0usize;
    for mut memory in memories.list().await? {
        let MemoryScope::Channel { platform, .. } = &memory.scope else {
            continue;
        };
        if is_durable_channel(platform) {
            continue;
        }
        memory.scope = MemoryScope::Global;
        // `updated_at` untouched: this corrects where a memory is visible, not
        // what it says, and the recency signal should keep reflecting the last
        // real edit.
        memories.save(&memory).await?;
        repaired += 1;
    }
    Ok(repaired)
}

/// An explicit-id resume request's eligibility, plus the priming input when
/// it is resumable. Shared by the gateway's resume endpoint and the direct
/// in-process path, so eligibility rules and the digest never fork.
pub enum ResumeTarget {
    Missing,
    NotRecoverable {
        status: String,
    },
    Ready {
        run: Run,
        steps: Vec<RunStep>,
        input: String,
    },
}

/// Resolve one run id to its resume eligibility and priming input.
pub async fn resolve_resume(runs: &dyn RunRepository, id: &str) -> anyhow::Result<ResumeTarget> {
    let Some(run) = runs.get(id).await? else {
        return Ok(ResumeTarget::Missing);
    };
    if !run.recoverable {
        return Ok(ResumeTarget::NotRecoverable {
            status: run.status.as_str().to_string(),
        });
    }
    let steps = runs.steps(id).await?;
    let input = resume_prompt(&run, &steps);
    Ok(ResumeTarget::Ready { run, steps, input })
}

/// Summaries only — a list view never dumps full transcripts.
pub fn session_summaries(sessions: Vec<Session>) -> Vec<SessionSummary> {
    sessions
        .into_iter()
        // Hide soft-deleted sessions from the list; active + archived stay.
        .filter(|s| s.status != komo_core::domain::session::SESSION_STATUS_DELETED)
        // A sub-agent's session (`delegate` tool) is scratch work, not a
        // conversation: one delegation per handoff would flood a list whose whole
        // purpose is "conversations you can reopen". The work is not hidden — each
        // one is its own ledger run, which is the right lens for it
        // (`komo run list` / `run inspect`).
        .filter(|s| !is_subagent_session(&s.id))
        .map(|s| SessionSummary {
            created_at: s.created_at,
            messages: s.messages.len(),
            user_turns: s.user_turns(),
            title: s.title,
            status: s.status,
            id: s.id,
            workspace: s.workspace,
            model: s.model,
            effort: s.effort,
        })
        .collect()
}

/// Hash-free pairing rows — the salted code hash and per-row salt never leave
/// the host, on either path.
pub fn pairing_views(pairings: Vec<PairingRequest>, now: i64) -> Vec<PairingView> {
    pairings
        .into_iter()
        .map(|p| {
            let status = match p.status {
                PairingStatus::Approved => "approved",
                PairingStatus::Pending if p.is_expired(now) => "expired",
                PairingStatus::Pending => "pending",
            };
            PairingView {
                id: p.id,
                status: status.to_string(),
                created_at: p.created_at,
            }
        })
        .collect()
}

/// Index runs by how they turned out. Shared by both operator transports so
/// the two cannot disagree about what a stored assessment means — an
/// unparseable or absent one reads as `Unknown`, never as success.
pub fn run_verdicts(runs: Vec<komo_core::domain::run::Run>) -> HashMap<String, OutcomeVerdict> {
    runs.into_iter()
        .map(|run| {
            let verdict = serde_json::from_str::<OutcomeAssessment>(&run.outcome)
                .map(|a| a.verdict)
                .unwrap_or_default();
            (run.id, verdict)
        })
        .collect()
}

/// Filter `skill`-tool steps down to the views of one skill (newest-first in,
/// newest-first out). A skill "used" is exactly a `skill view` step — nothing
/// stores usage counters; the audit is always derived from the ledger.
pub fn skill_invocations(
    steps: Vec<RunStep>,
    name: &str,
    cap: usize,
    verdicts: &HashMap<String, OutcomeVerdict>,
) -> Vec<SkillInvocation> {
    steps
        .into_iter()
        .filter(|s| step_views_skill(s, name))
        .take(cap)
        .map(|s| SkillInvocation {
            verdict: verdicts.get(&s.run_id).copied().unwrap_or_default(),
            run_id: s.run_id,
            seq: s.seq,
            started_at: s.started_at,
            ok: s.ok,
        })
        .collect()
}

/// Roll `skill`-tool steps up into one row per skill, coldest first (never
/// loaded, then least recently loaded). Skills are named by the caller rather
/// than discovered from the steps, so a skill nobody has ever touched — the
/// whole point of the report — still gets a row.
pub fn skill_usage(
    names: impl IntoIterator<Item = String>,
    steps: Vec<RunStep>,
    verdicts: &HashMap<String, OutcomeVerdict>,
) -> Vec<SkillUsage> {
    let mut rows: BTreeMap<String, SkillUsage> = names
        .into_iter()
        .map(|name| {
            (
                name.clone(),
                SkillUsage {
                    name,
                    views: 0,
                    last_at: None,
                    succeeded: 0,
                    failed: 0,
                    unknown: 0,
                },
            )
        })
        .collect();
    let mut counted: HashSet<(String, String)> = HashSet::new();
    for step in steps {
        let Some(name) = skill_viewed(&step) else {
            continue;
        };
        // A view of a skill that is no longer active still counts: it says the
        // ledger knows this name, which is exactly what a `restore` decision
        // wants. It just has no row unless the caller named it.
        let Some(row) = rows.get_mut(&name) else {
            continue;
        };
        row.views += 1;
        row.last_at = Some(
            row.last_at
                .map_or(step.started_at, |at| at.max(step.started_at)),
        );
        // Per run, not per view: a skill loaded twice in one turn is one piece
        // of evidence about that turn.
        if counted.insert((name.clone(), step.run_id.clone())) {
            match verdicts.get(&step.run_id).copied().unwrap_or_default() {
                OutcomeVerdict::Success => row.succeeded += 1,
                OutcomeVerdict::Failure => row.failed += 1,
                OutcomeVerdict::Unknown => row.unknown += 1,
            }
        }
    }
    let mut rows: Vec<SkillUsage> = rows.into_values().collect();
    // `None` sorts before `Some`, so never-used lands first; ties keep the
    // BTreeMap's name order so the report is stable between runs.
    rows.sort_by(|a, b| a.last_at.cmp(&b.last_at).then_with(|| a.name.cmp(&b.name)));
    rows
}

/// Classify the memory library into the dreaming dry-run report: which
/// candidates would promote (strongest case first) and which would archive.
/// The same `dream_verdict` the sweep applies — this only *previews* it.
/// The skill half of the preview: which proposals this cycle would withdraw,
/// and how many are waiting on a verdict at all.
pub fn dream_classify_skills(
    candidates: &[komo_core::domain::skill::Skill],
    now: i64,
) -> (Vec<String>, usize) {
    use komo_core::domain::skill::candidate_expired;
    let expiring = candidates
        .iter()
        .filter(|skill| candidate_expired(skill, now))
        .map(|skill| skill.name.clone())
        .collect();
    (expiring, candidates.len())
}

pub fn dream_classify(memories: &[Memory], now: i64) -> DreamReport {
    let mut report = DreamReport::default();
    for m in memories {
        if m.status == MemoryStatus::Candidate {
            report.candidate_count += 1;
        }
        let item = DreamItem {
            id: m.id.clone(),
            support_count: m.support_count,
            contradiction_count: m.contradiction_count,
            belief: m.belief.as_str().to_string(),
            recall_count: m.recall_count,
            score: dream_score(m, now),
            content: m.content.clone(),
        };
        match dream_verdict(m, now) {
            DreamVerdict::Promote => report.promote.push(item),
            DreamVerdict::Archive => report.archive.push(item),
            DreamVerdict::Keep => {}
        }
    }
    report.promote.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::memory::MemoryKind;
    use komo_core::domain::session::{SESSION_STATUS_ARCHIVE, SESSION_STATUS_DELETED};

    fn session(id: &str, status: &str) -> Session {
        let mut s = Session::new(id);
        s.status = status.to_string();
        s
    }

    fn skill_step(args: String, at: i64) -> RunStep {
        RunStep {
            run_id: "run".to_string(),
            seq: 1,
            tool_name: "skill".to_string(),
            args,
            result: "done".to_string(),
            error: String::new(),
            ok: true,
            uncertain: false,
            started_at: at,
            ended_at: at,
            elapsed_ms: 1,
            structured: serde_json::Value::Null,
            output_paths: Vec::new(),
        }
    }

    fn view_step(skill: &str, at: i64) -> RunStep {
        skill_step(format!(r#"{{"action":"view","name":"{skill}"}}"#), at)
    }

    /// The report's reason to exist: a skill nobody has loaded still gets a row,
    /// and it sorts first.
    #[test]
    fn skill_usage_ranks_never_used_skills_first() {
        let names = ["cold", "warm", "hot"].map(str::to_string);
        let steps = vec![
            view_step("hot", 300),
            view_step("hot", 100),
            view_step("warm", 200),
            // A view of a skill that is no longer active is not invented into a row.
            view_step("archived-one", 400),
        ];

        let rows = skill_usage(names, steps, &HashMap::new());
        let shape: Vec<_> = rows
            .iter()
            .map(|r| (r.name.as_str(), r.views, r.last_at))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("cold", 0, None),
                ("warm", 1, Some(200)),
                ("hot", 2, Some(300)),
            ]
        );
    }

    /// Non-`view` skill calls (`learn`, `install`) are not uses of a skill.
    #[test]
    fn skill_usage_counts_only_view_steps() {
        let learn = skill_step(r#"{"action":"learn","name":"warm"}"#.to_string(), 500);

        let rows = skill_usage(["warm".to_string()], vec![learn], &HashMap::new());
        assert_eq!(rows[0].views, 0);
        assert_eq!(rows[0].last_at, None);
    }

    /// The repair widens only the ephemeral `api` scopes, leaves a real chat
    /// channel's privacy boundary alone, and is safe to run twice.
    #[tokio::test]
    async fn repairing_scopes_widens_only_ephemeral_api_channels() {
        use std::sync::Mutex;

        struct Store(Mutex<Vec<Memory>>);
        #[async_trait::async_trait]
        impl MemoryRepository for Store {
            async fn save(&self, memory: &Memory) -> anyhow::Result<()> {
                let mut rows = self.0.lock().unwrap();
                if let Some(slot) = rows.iter_mut().find(|m| m.id == memory.id) {
                    *slot = memory.clone();
                }
                Ok(())
            }
            async fn list(&self) -> anyhow::Result<Vec<Memory>> {
                Ok(self.0.lock().unwrap().clone())
            }
        }

        let scoped = |scope: MemoryScope| {
            let mut m = Memory::new(MemoryKind::Fact, "a fact");
            m.scope = scope;
            m
        };
        let store = Store(Mutex::new(vec![
            scoped(MemoryScope::Channel {
                platform: "api".into(),
                chat_id: "019fb0ce-9f7a-7c23".into(),
            }),
            scoped(MemoryScope::Channel {
                platform: "feishu".into(),
                chat_id: "ou_445299e2".into(),
            }),
            scoped(MemoryScope::Global),
        ]));

        assert_eq!(repair_memory_scopes(&store).await.unwrap(), 1);
        let rows = store.list().await.unwrap();
        assert_eq!(rows[0].scope, MemoryScope::Global, "api scope widened");
        assert_eq!(
            rows[1].scope,
            MemoryScope::Channel {
                platform: "feishu".into(),
                chat_id: "ou_445299e2".into(),
            },
            "a chat channel's scope is a privacy boundary and must survive"
        );
        assert_eq!(rows[2].scope, MemoryScope::Global);

        // Idempotent: a second run finds nothing left to move.
        assert_eq!(repair_memory_scopes(&store).await.unwrap(), 0);
    }

    #[test]
    fn the_session_list_hides_deleted_and_subagent_sessions() {
        let rows = session_summaries(vec![
            session("api:real", "active"),
            session("api:archived", SESSION_STATUS_ARCHIVE),
            session("api:gone", SESSION_STATUS_DELETED),
            // A `delegate` sub-agent's scratch session: real work, but not a
            // conversation — it belongs in the run ledger, not this list.
            session("delegate:019fa7f2-50e7-7f42-9430-ea0e1d88c81e", "active"),
        ]);
        let ids: Vec<_> = rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["api:real", "api:archived"]);
    }

    #[test]
    fn a_session_summary_carries_its_model_choice() {
        let mut s = session("api:one", "active");
        s.model = "deepseek:deepseek-chat".into();
        s.effort = "high".into();
        let rows = session_summaries(vec![s]);
        assert_eq!(rows[0].model, "deepseek:deepseek-chat");
        assert_eq!(rows[0].effort, "high");
    }

    #[test]
    fn dream_preview_counts_candidates_that_remain_under_observation() {
        let now = 1_000_000;
        let mut young = Memory::new(MemoryKind::Fact, "still being observed");
        young.status = MemoryStatus::Candidate;
        young.created_at = now - 86_400;

        let active = Memory::new(MemoryKind::Fact, "already active");
        let report = dream_classify(&[young, active], now);

        assert!(!report.has_actions());
        assert_eq!(report.candidate_count, 1);
        assert_eq!(report.observing_count(), 1);
    }

    fn viewed_in(run_id: &str, seq: i64, name: &str, at: i64) -> RunStep {
        RunStep {
            run_id: run_id.into(),
            seq,
            tool_name: "skill".into(),
            args: format!(r#"{{"action":"view","name":"{name}"}}"#),
            result: String::new(),
            error: String::new(),
            ok: true,
            uncertain: false,
            started_at: at,
            ended_at: at,
            elapsed_ms: 0,
            structured: serde_json::Value::Null,
            output_paths: Vec::new(),
        }
    }

    fn verdicts(pairs: &[(&str, OutcomeVerdict)]) -> HashMap<String, OutcomeVerdict> {
        pairs.iter().map(|(id, v)| (id.to_string(), *v)).collect()
    }

    /// A skill loaded twice inside one turn is one piece of evidence about that
    /// turn. Counting the views would let a single failure look like two.
    #[test]
    fn one_turn_counts_once_however_many_times_it_loaded_the_skill() {
        let steps = vec![
            viewed_in("run-1", 1, "deploy", 100),
            viewed_in("run-1", 5, "deploy", 110),
        ];
        let rows = skill_usage(
            ["deploy".to_string()],
            steps,
            &verdicts(&[("run-1", OutcomeVerdict::Failure)]),
        );
        assert_eq!(rows[0].views, 2, "both loads are still visible");
        assert_eq!(rows[0].failed, 1, "but they are one failing turn");
        assert_eq!(rows[0].succeeded + rows[0].unknown, 0);
    }

    /// An unsettled turn is not a quiet success — which is also where a skill
    /// that was loaded but never actually followed ends up, since the ledger
    /// cannot see adoption.
    #[test]
    fn an_unsettled_turn_is_never_counted_as_a_success() {
        let rows = skill_usage(
            ["deploy".to_string()],
            vec![
                viewed_in("run-1", 1, "deploy", 100),
                viewed_in("run-2", 1, "deploy", 200),
            ],
            // run-2 is outside the verdict window entirely.
            &verdicts(&[("run-1", OutcomeVerdict::Unknown)]),
        );
        assert_eq!(rows[0].succeeded, 0);
        assert_eq!(rows[0].unknown, 2);
    }

    #[test]
    fn turns_are_bucketed_by_how_they_ended_not_by_whether_the_load_worked() {
        let rows = skill_usage(
            ["deploy".to_string()],
            vec![
                viewed_in("run-1", 1, "deploy", 100),
                viewed_in("run-2", 1, "deploy", 200),
                viewed_in("run-3", 1, "deploy", 300),
            ],
            &verdicts(&[
                ("run-1", OutcomeVerdict::Success),
                ("run-2", OutcomeVerdict::Failure),
                ("run-3", OutcomeVerdict::Unknown),
            ]),
        );
        assert_eq!(
            (rows[0].succeeded, rows[0].failed, rows[0].unknown),
            (1, 1, 1)
        );
    }

    #[test]
    fn an_invocation_carries_its_turns_verdict() {
        let hits = skill_invocations(
            vec![viewed_in("run-1", 1, "deploy", 100)],
            "deploy",
            10,
            &verdicts(&[("run-1", OutcomeVerdict::Failure)]),
        );
        assert_eq!(hits[0].verdict, OutcomeVerdict::Failure);
        assert!(hits[0].ok, "the load itself still succeeded");
    }
}
