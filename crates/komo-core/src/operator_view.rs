//! Operator view DTOs — the serialized shapes the gateway's HTTP endpoints emit
//! and the CLI / GUI clients deserialize.
//!
//! These carry no domain dependency (plain rows over `String`/`i64`/`f64`), so
//! they live in `komo-core` where any HTTP client can reuse them as the single
//! source of truth. The richer operator request/reply enums that *do* wrap
//! domain types stay in `komo::services::operator_control::request`, which
//! re-exports these for path stability.

use serde::{Deserialize, Serialize};

use crate::domain::awaiting::Awaiting;

/// A session list row (full transcripts are never dumped in a list view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub id: String,
    /// Immutable workspace id selected when the session was created.
    #[serde(default = "default_workspace")]
    pub workspace: String,
    pub created_at: i64,
    pub messages: usize,
    pub user_turns: usize,
    /// Operator-set display name (empty = untitled). `default` so a payload from
    /// an older gateway still parses.
    #[serde(default)]
    pub title: String,
    /// Lifecycle status: `active` / `archive` (`deleted` sessions are omitted
    /// from the list). `default` for older-gateway compatibility.
    #[serde(default)]
    pub status: String,
    /// Per-session model override (empty = the gateway default). Switchable
    /// mid-conversation, unlike `workspace`.
    #[serde(default)]
    pub model: String,
    /// Per-session reasoning effort (empty = the provider default).
    #[serde(default)]
    pub effort: String,
    /// The wait this conversation is stopped in, when it is stopped in one — a
    /// suspended turn is otherwise indistinguishable from an idle chat.
    #[serde(default)]
    pub awaiting: Option<Awaiting>,
}

fn default_workspace() -> String {
    "__default__".to_string()
}

/// A pairing row without the salted code hash / salt (never leaves the host).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairingView {
    pub id: String,
    /// `pending` | `approved` | `expired`.
    pub status: String,
    pub created_at: i64,
}

/// One `skill view` step from the run ledger (backs `komo skills audit`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInvocation {
    pub run_id: String,
    pub seq: i64,
    pub started_at: i64,
    pub ok: bool,
    /// How the turn that loaded this skill turned out. `ok` above says the
    /// *load* worked, which is a different and much weaker claim.
    #[serde(default)]
    pub verdict: crate::domain::episode::OutcomeVerdict,
}

/// One skill's usage across the ledger scan window (backs the aggregate
/// `komo skills audit`). Derived on every call — no counter is stored anywhere,
/// so this reflects exactly what the ledger still holds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillUsage {
    pub name: String,
    /// `skill view` steps seen in the scan window.
    pub views: usize,
    /// When it was last loaded; `None` = not once in the window.
    pub last_at: Option<i64>,
    /// Distinct turns that loaded it, bucketed by how they turned out. Counted
    /// per **run**, not per view: a skill loaded twice in one turn is one piece
    /// of evidence about that turn, not two.
    ///
    /// `unknown` is the honest majority and is not a silent success: a turn
    /// whose outcome nothing settled says nothing about whether the skill
    /// helped — and a skill that was loaded but never actually followed lands
    /// here too, since the ledger cannot see adoption.
    #[serde(default)]
    pub succeeded: usize,
    #[serde(default)]
    pub failed: usize,
    #[serde(default)]
    pub unknown: usize,
}

/// The result of resuming an interrupted run, consumed by `komo run resume`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeOutcome {
    pub run_id: String,
    pub session_id: String,
    /// How many completed steps the priming digest handed to the model.
    pub steps: usize,
    pub reply: String,
    /// `true` when the turn was *continued* from its journal (exact state,
    /// no re-run); `false` for the digest-primed fresh turn. `default` so an
    /// older gateway's response still parses.
    #[serde(default)]
    pub continued: bool,
}

/// One candidate in the dreaming preview, carrying the signals behind its
/// verdict: the truth signals that decide promotion, and the usage signal that
/// decides retention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DreamItem {
    pub id: String,
    /// Independent occasions of support — what promotion actually reads.
    /// `default` so a payload from an older gateway still parses.
    #[serde(default)]
    pub support_count: i64,
    #[serde(default)]
    pub contradiction_count: i64,
    /// `current` / `contested` / `superseded`. Anything but `current` blocks
    /// promotion.
    #[serde(default)]
    pub belief: String,
    /// Retrieval frequency. Retention only — never evidence that a memory is
    /// true, which is why it no longer appears in the promote gate.
    pub recall_count: i64,
    pub score: f64,
    pub content: String,
}

/// The dreaming dry-run classification: which candidates would promote
/// (strongest case first), which would archive, and how many remain under
/// observation this cycle.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DreamReport {
    pub promote: Vec<DreamItem>,
    pub archive: Vec<DreamItem>,
    /// Every candidate considered by the sweep, including ones that remain in
    /// the observation window. A missing field from an older gateway means the
    /// CLI can still render the actionable buckets safely.
    #[serde(default)]
    pub candidate_count: usize,
    /// Skill proposals this cycle would withdraw, by name — the other half of
    /// dreaming's governance. Names only: a proposal's body is `komo skills
    /// inspect`'s business, and the preview's job is to say what will move.
    #[serde(default)]
    pub expire_skills: Vec<String>,
    /// Skill candidates awaiting triage, expiring or not — the denominator the
    /// withdrawal count means nothing without.
    #[serde(default)]
    pub skill_candidate_count: usize,
}

impl DreamReport {
    /// Whether this cycle has any state transition to apply.
    pub fn has_actions(&self) -> bool {
        !self.promote.is_empty() || !self.archive.is_empty() || !self.expire_skills.is_empty()
    }

    /// Memory candidates that are neither ready to promote nor old and cold
    /// enough to archive yet.
    pub fn observing_count(&self) -> usize {
        self.candidate_count
            .saturating_sub(self.promote.len() + self.archive.len())
    }

    /// Skill proposals still inside their window, waiting on a human verdict.
    pub fn skills_awaiting_count(&self) -> usize {
        self.skill_candidate_count
            .saturating_sub(self.expire_skills.len())
    }

    /// Backwards-compatible spelling for callers that mean “no state changes”,
    /// not “there are no candidate memories”.
    pub fn is_empty(&self) -> bool {
        !self.has_actions()
    }
}

/// One note-vault search hit, without its vector (4 KB nobody reads).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiHitView {
    /// Vault-relative path.
    pub path: String,
    /// Heading trail within the note.
    pub heading_path: String,
    pub text: String,
    /// Ranking score. **Not** a similarity when hybrid retrieval is on — it is
    /// then a fused rank value, so it is only comparable within one result set.
    pub score: f32,
}

/// What `komo wiki status` reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiStatusView {
    pub vault: String,
    pub backend: String,
    pub collection: String,
    /// Where the embedded backend keeps its files, or the server URL.
    pub location: String,
    /// Embedding model from config.
    pub model: String,
    pub files: usize,
    pub chunks: usize,
    /// Vector width and the model that wrote it, once anything is indexed.
    /// A model here that differs from `model` means the index is stale.
    pub dims: Option<usize>,
    pub indexed_by: Option<String>,
}

/// What one indexing run did.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiIndexView {
    pub files_seen: usize,
    pub files_changed: usize,
    pub files_removed: usize,
    pub chunks_written: usize,
    pub chunks_total: usize,
    /// Notes that could not be read, with the reason.
    pub skipped: Vec<String>,
}
