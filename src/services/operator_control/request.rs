//! Typed operator requests and replies.
//!
//! These are the view types the operator surface exchanges — serialized by the
//! gateway's HTTP endpoints, deserialized by the CLI's gateway adapter, and
//! produced directly by the in-process adapter — so they live here as the
//! single source of truth, not in either transport.

use crate::domain::{
    cron::{CronJob, CronJobSpec},
    memory::Memory,
    reminder::Reminder,
    run::{MemoryUse, Run, RunStep},
    task::Task,
};

// The pure view DTOs (no domain dependency) live in `komo-core` so HTTP clients
// — the CLI gateway adapter and the Dioxus GUI — share one definition. Re-export
// them here so `operator_control::{SessionSummary, …}` paths are unchanged.
pub use komo_core::operator_view::{
    DreamItem, DreamReport, PairingView, ResumeOutcome, SessionSummary, SkillInvocation,
    SkillUsage, WikiHitView, WikiIndexView, WikiStatusView,
};

/// A read-only operator request. One `query` call per CLI render — the CLI
/// never knows which transport answers it.
#[derive(Debug)]
pub enum OperatorQuery {
    /// Pending reminders, soonest first.
    Reminders,
    /// Open tasks (inbox/todo/waiting).
    Tasks,
    /// Recent runs, newest first.
    Runs { limit: usize },
    /// One run with its tool steps (`None` = no such run).
    Run { id: String },
    /// Session summaries (never full transcripts).
    Sessions,
    /// The whole memory library (operator view — no scope enforcement).
    Memories,
    /// Ranked memory search over the same hybrid query recall uses. Routed like
    /// every other operator read so a running gateway lends its embedder;
    /// without one the same scoring runs lexical-only.
    MemorySearch { query: String, limit: usize },
    /// Which turns a memory reached the prompt of.
    MemoryUsed { id: String, limit: usize },
    /// Hash-free pairing rows.
    Pairings,
    /// The dreaming dry-run classification.
    DreamPreview,
    /// Which turns loaded a skill (derived from the run ledger).
    SkillAudit { name: String },
    /// Every active skill ranked coldest-first (same ledger derivation).
    SkillUsage,
    /// The `/sethome` runtime override (`None` when unset).
    HomeOverride,
    /// Note-vault search. Routed like every other operator read so it works
    /// while the gateway holds the index open.
    WikiSearch { query: String, limit: usize },
    /// What the note-vault index currently holds.
    WikiStatus,
    /// Every scheduled cron job (enabled or not), by name.
    CronJobs,
}

/// The reply to an [`OperatorQuery`], variant-for-variant. Callers match
/// exhaustively — transport JSON shapes never become the caller interface.
#[derive(Debug)]
pub enum OperatorQueryResult {
    Reminders(Vec<Reminder>),
    Tasks(Vec<Task>),
    Runs(Vec<Run>),
    Run(Option<(Run, Vec<RunStep>)>),
    Sessions(Vec<SessionSummary>),
    Memories(Vec<Memory>),
    MemorySearch(Vec<Memory>),
    MemoryUsed(Vec<MemoryUse>),
    Pairings(Vec<PairingView>),
    DreamPreview(DreamReport),
    SkillAudit(Vec<SkillInvocation>),
    SkillUsage(Vec<SkillUsage>),
    HomeOverride(Option<String>),
    CronJobs(Vec<CronJob>),
    WikiHits(Vec<WikiHitView>),
    WikiStatus(WikiStatusView),
}

/// A state-changing operator action (host-operator writes; the gateway serves
/// these only to loopback callers).
#[derive(Debug)]
pub enum OperatorCommand {
    /// Apply one memory governance transition.
    MemoryTransition {
        id: String,
        action: MemoryTransitionAction,
    },
    /// Drop runs (and their steps) started before `cutoff`.
    PruneRuns { cutoff: i64 },
    /// Delete every session with no messages.
    CleanSessions,
    /// Approve the pending pairing bearing `code`.
    PairApprove { code: String },
    /// Remove a pairing by id.
    PairRevoke { id: String },
    /// Run one dreaming consolidation cycle.
    DreamApply,
    /// Widen memories stranded in an ephemeral `api` channel scope to `Global`.
    MemoryRepairScopes,
    /// Embed every memory that still lacks a current vector, and wait for it.
    /// Minutes-long on a library that has never been embedded: the gateway
    /// adapter gives this the same long timeout `ChunkIndex` gets.
    MemoryBackfill,
    /// Index the note vault. Minutes-long: the gateway adapter gives this
    /// command its own, far longer timeout than every other operator call.
    ChunkIndex { rebuild: bool },
    /// Create a scheduled cron job (validated; duplicate names refused).
    CronAdd { spec: CronJobSpec },
    /// Delete a cron job by name.
    CronRemove { name: String },
    /// Enable or disable a cron job. Re-enabling recomputes `next_run_at`
    /// from now, so a long-disabled job doesn't fire immediately off its
    /// stale slot.
    CronSetEnabled { name: String, enabled: bool },
    /// Make a job due now — it fires on the gateway's next sweep tick
    /// (within a minute). With no gateway running, it fires once one starts.
    CronTrigger { name: String },
}

/// The reply to an [`OperatorCommand`], variant-for-variant.
#[derive(Debug)]
pub enum OperatorCommandResult {
    /// The transition applied (an unknown id is an `Err`, identical on both
    /// transports).
    MemoryTransitioned,
    WikiIndexed(WikiIndexView),
    RunsPruned {
        removed: usize,
    },
    SessionsCleaned {
        removed: usize,
    },
    PairApproved(PairApproveOutcome),
    PairRevoked {
        revoked: bool,
    },
    DreamApplied {
        promoted: usize,
        archived: usize,
        /// Skill proposals withdrawn for want of a verdict.
        skills_expired: usize,
    },
    /// How many memories were widened to `Global`.
    MemoryScopesRepaired {
        repaired: usize,
    },
    /// How many memories gained an embedding.
    MemoryBackfilled {
        embedded: usize,
    },
    /// The created job (with its computed `next_run_at`).
    CronAdded(Box<CronJob>),
    CronRemoved,
    /// The job after an enable/disable/trigger update.
    CronUpdated(Box<CronJob>),
}

/// A memory governance transition. The domain owns the semantics
/// (`Memory::promote/reject/pin`); this only names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryTransitionAction {
    Promote,
    Reject,
    Pin,
}

impl MemoryTransitionAction {
    /// The api route leg (`/api/memories/{id}/<route>`).
    pub fn route(self) -> &'static str {
        match self {
            MemoryTransitionAction::Promote => "promote",
            MemoryTransitionAction::Reject => "reject",
            MemoryTransitionAction::Pin => "pin",
        }
    }

    /// The domain method this action names.
    pub fn apply(self) -> fn(&mut Memory, i64) {
        match self {
            MemoryTransitionAction::Promote => Memory::promote,
            MemoryTransitionAction::Reject => Memory::reject,
            MemoryTransitionAction::Pin => Memory::pin,
        }
    }
}

/// The outcome of a pairing approval, identical on both transports.
#[derive(Debug)]
pub enum PairApproveOutcome {
    Approved { id: String },
    NotFound,
    Locked { retry_after_secs: i64 },
}
