use std::collections::HashSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::session::ChannelPeer;

/// A long-term memory: a durable fact, preference, or note about the user, a
/// project, a person, or a decision. Memories are governed (status/confidence)
/// and scoped (where they may surface) so the agent can be injected with a
/// conservative profile (L1), recall relevant facts (L3), and let the user
/// curate the full library (L2). See `docs/personal-agent-roadmap.md`.
///
/// Not `Eq`: `embedding` is `Vec<f32>`, and float equality is not an
/// equivalence relation. Nothing keys a collection on a whole `Memory` — `id`
/// is the identity — so `PartialEq` (assertions, dedup checks) is enough.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Memory {
    pub id: String,
    pub kind: MemoryKind,
    pub content: String,

    /// Lifecycle state. Automated extraction lands as `Candidate`; only
    /// user-confirmed/written memories become high-confidence `Active`.
    pub status: MemoryStatus,
    /// Whether komo believes this *right now* — a different axis from `status`.
    /// See [`BeliefState`].
    pub belief: BeliefState,
    /// How much the memory can be trusted, by origin.
    pub confidence: MemoryConfidence,
    /// **Who** the claim came from — the user, or something a tool returned.
    /// See [`MemoryProvenance`]: this is the one axis that decides whether a
    /// claim may promote on its own accumulated support.
    pub provenance: MemoryProvenance,
    /// 0–100 ranking weight; ties broken by recency. Default 50.
    pub importance: i32,
    /// Eligible for L1 pinned-profile injection (every turn). Only ever set by
    /// the user / explicit confirmation, never by automated extraction.
    pub pinned: bool,

    /// Where this memory may surface. Scope is enforced at the query layer, not
    /// the render layer, so a channel-scoped memory never leaks into another
    /// chat. See [`MemoryContext`].
    pub scope: MemoryScope,

    /// Session this memory was distilled from (`telegram:{chat_id}`, a cli
    /// session uuid, …). Empty = written outside any session.
    pub source: String,
    /// Content-derived dedup key set on automated extraction (FNV-1a over the
    /// normalized content), so re-reviewing a session never duplicates it.
    pub source_message_id: String,

    pub created_at: i64,
    pub updated_at: i64,
    /// Optional governance TTL: a unix timestamp past which the memory is
    /// treated as stale and hidden from recall. `None` = never expires.
    pub expires_at: Option<i64>,
    /// Last time this memory surfaced in recall, for usage-based
    /// retention signals. `None` = never used.
    pub last_used_at: Option<i64>,

    // ── truth signals ────────────────────────────────────────────────────────
    //
    // Deliberately separate from the usage signals below. `recall_count` proves
    // a memory keeps being *relevant*; only these prove it is *true*. Promotion
    // reads these, retention reads those — conflating them is what let a wrong
    // memory promote itself by being repeatedly retrieved.
    /// Independent occasions on which the user said something supporting this
    /// claim. Independence is per session — see [`Memory::record_evidence`].
    pub support_count: i64,
    /// Independent occasions on which the user said something conflicting with
    /// it. Any unresolved contradiction is what `Contested` expresses.
    pub contradiction_count: i64,
    /// When the user last explicitly confirmed this (triage promote, or an
    /// unambiguous restatement). The strongest truth signal there is, and the
    /// freshness that matters for a memory about to drive an action.
    pub last_confirmed_at: Option<i64>,
    /// Id of the memory that replaced this one, when `belief` is `Superseded`.
    /// Empty otherwise.
    pub superseded_by: String,
    /// Capped, summarized provenance: *why* komo believes this. See [`Evidence`].
    #[serde(default)]
    pub evidence: Vec<Evidence>,
    /// How many times this memory has surfaced in L3 recall — a **utility**
    /// signal, not a truth signal. It says the retriever keeps finding this
    /// relevant, which is exactly what retention should be decided on, and is
    /// exactly what promotion must *not* be decided on: a wrong memory that
    /// happens to be relevant to a recurring question would otherwise promote
    /// itself. See [`dream_verdict`].
    pub recall_count: i64,
    /// L2-normalized semantic vector of [`content`](Self::content), or empty
    /// when none has been computed yet (no embedding backend configured, or the
    /// backfill has not reached this memory). This is what lets a Chinese
    /// question recall an English memory — see [`super::embedding`].
    ///
    /// Filled in the background, never on the write path: a memory is always
    /// usable lexically the moment it is saved.
    #[serde(default)]
    pub embedding: Vec<f32>,
    /// Which model produced [`embedding`](Self::embedding). Vectors from
    /// different models live in incomparable spaces, so recall only uses an
    /// embedding whose model matches the *current* backend; a mismatch is
    /// treated as "not embedded yet" and re-embedded by the backfill.
    #[serde(default)]
    pub embedding_model: String,
}

/// Default ranking weight for a new memory.
pub const DEFAULT_IMPORTANCE: i32 = 50;

/// How long a memory can go unvouched-for before an injected line flags it.
///
/// Six months: long enough that a settled preference is not nagged about every
/// turn, short enough that a stale one is questioned before it drives an action.
pub const MEMORY_STALE_AFTER_DAYS: i64 = 180;

impl Memory {
    /// A new memory with conservative defaults: `Active` status, `Inferred`
    /// confidence, global scope, not pinned. Callers (the `memory` tool, the
    /// reviewer) override status/confidence/scope to match their trust level.
    pub fn new(kind: MemoryKind, content: impl Into<String>) -> Self {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        Self {
            id: format!("mem-{}", uuid::Uuid::now_v7()),
            kind,
            content: content.into(),
            status: MemoryStatus::Active,
            belief: BeliefState::Current,
            confidence: MemoryConfidence::Inferred,
            // The permissive default is the honest one for everything komo
            // wrote before provenance existed: it was all extracted from
            // conversations. A tool-derived claim is marked by whoever creates
            // it, and that is the only writer that knows.
            provenance: MemoryProvenance::User,
            importance: DEFAULT_IMPORTANCE,
            pinned: false,
            scope: MemoryScope::Global,
            source: String::new(),
            source_message_id: String::new(),
            created_at: now,
            updated_at: now,
            expires_at: None,
            last_used_at: None,
            // Zero, not one: the count means "recorded evidence", so the
            // observation that created a memory is registered by whoever creates
            // it (`record_evidence`), never assumed here.
            support_count: 0,
            contradiction_count: 0,
            last_confirmed_at: None,
            superseded_by: String::new(),
            evidence: Vec::new(),
            recall_count: 0,
            embedding: Vec::new(),
            embedding_model: String::new(),
        }
    }

    /// This memory's vector, but only if it was produced by `model` — vectors
    /// from another model are not comparable, so they read as absent. `None`
    /// also when nothing has been embedded yet.
    pub fn embedding_for<'a>(&'a self, model: &str) -> Option<&'a [f32]> {
        (!self.embedding.is_empty() && self.embedding_model == model)
            .then_some(self.embedding.as_slice())
    }

    /// Whether this memory has expired as of `now` (a unix timestamp).
    pub fn is_expired(&self, now: i64) -> bool {
        self.expires_at.is_some_and(|e| e <= now)
    }

    // Governance transitions (the triage ladder). Shared by the CLI, the api
    // channel, and the `memory` tool so the semantics can never drift between
    // surfaces.

    /// Promote a candidate to an active, **confirmed** memory (a human vouched
    /// for it — unlike dreaming's evidence-driven promote, which caps at
    /// inferred).
    ///
    /// A human vouching *is* an explicit confirmation, so this also stamps
    /// `last_confirmed_at` and clears any contest: whatever conflict was
    /// outstanding, the operator has now ruled on it.
    pub fn promote(&mut self, now: i64) {
        self.status = MemoryStatus::Active;
        self.confidence = MemoryConfidence::Confirmed;
        self.last_confirmed_at = Some(now);
        self.belief = BeliefState::Current;
        self.superseded_by.clear();
        self.updated_at = now;
    }

    /// Record an observation bearing on this memory. Returns whether it counted.
    ///
    /// **Independence is per session.** An observation from a session already
    /// present in the evidence list is dropped, which is what makes
    /// `support_count` mean "N separate occasions" instead of "N sentences" — a
    /// user restating a preference three times in one conversation must not look
    /// like three independent confirmations.
    ///
    /// The evidence *list* is capped at [`EVIDENCE_CAP`] (most recent kept) while
    /// the counts keep rising, so a long-lived memory cannot grow its row without
    /// bound. Consequence, deliberately accepted: once the cap is reached, a
    /// session evicted from the list can be counted again. Every threshold that
    /// reads these counts sits far below the cap, so this cannot change a verdict.
    pub fn record_evidence(
        &mut self,
        session: &str,
        relation: EvidenceRelation,
        excerpt: &str,
        now: i64,
    ) -> bool {
        if self.evidence.iter().any(|e| e.session == session) {
            return false;
        }
        match relation {
            EvidenceRelation::Supports => self.support_count += 1,
            EvidenceRelation::Contradicts => self.contradiction_count += 1,
        }
        self.evidence.push(Evidence {
            session: session.to_string(),
            observed_at: now,
            relation,
            excerpt: truncate_excerpt(excerpt),
        });
        if self.evidence.len() > EVIDENCE_CAP {
            let drop = self.evidence.len() - EVIDENCE_CAP;
            self.evidence.drain(..drop);
        }
        self.updated_at = now;
        true
    }

    /// Something now conflicts with this memory and nothing has resolved which
    /// wins. It stops being injected until a confirmation or triage rules on it.
    pub fn contest(&mut self, now: i64) {
        self.belief = BeliefState::Contested;
        self.updated_at = now;
    }

    /// This memory was true and has been replaced by `by`. Kept as history
    /// rather than deleted — "what did I use to prefer" is a real question, and
    /// the replacement's provenance points back here.
    pub fn supersede(&mut self, by: &str, now: i64) {
        self.belief = BeliefState::Superseded;
        self.superseded_by = by.to_string();
        self.updated_at = now;
    }

    /// When this memory's content was last *vouched for*: an explicit
    /// confirmation, else the most recent recorded observation, else the day it
    /// was created.
    ///
    /// Deliberately not `updated_at`, which is an edit clock — it also moves when
    /// importance is retuned or the belief is contested, neither of which says
    /// anything about whether the claim still holds. This is the clock that
    /// answers "how long since anyone actually backed this up", which is what a
    /// memory about to drive an action should be judged on.
    pub fn vouched_at(&self) -> i64 {
        let newest_evidence = self.evidence.iter().map(|e| e.observed_at).max();
        [self.last_confirmed_at, newest_evidence]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or(self.created_at)
    }

    /// When something last conflicted with this memory and nothing has ruled on
    /// it since — `None` when nothing conflicts, or when a confirmation came
    /// after the conflict (the operator has since decided the question).
    ///
    /// Read off the contradicting evidence, falling back to `updated_at` for a
    /// conflict that left no entry: `contest`/`supersede` write none, and the
    /// oldest entries drain out of the capped list. The fallback is an edit
    /// clock, so it can only ever read *later* than the real refutation — which
    /// delays retiring a refuted claim, never hastens it.
    pub fn unresolved_refutation_at(&self) -> Option<i64> {
        if self.contradiction_count == 0 && self.belief == BeliefState::Current {
            return None;
        }
        let refuted_at = self
            .evidence
            .iter()
            .filter(|e| e.relation == EvidenceRelation::Contradicts)
            .map(|e| e.observed_at)
            .max()
            .unwrap_or(self.updated_at);
        if self.last_confirmed_at.is_some_and(|c| c >= refuted_at) {
            return None;
        }
        Some(refuted_at)
    }

    /// Whether nothing has vouched for this memory in [`MEMORY_STALE_AFTER_DAYS`].
    /// Injected memories say so, because acting on a long-unconfirmed preference
    /// is how an assistant gets corrected.
    pub fn is_stale(&self, now: i64) -> bool {
        (now - self.vouched_at()).max(0) / 86_400 >= MEMORY_STALE_AFTER_DAYS
    }

    /// Whether independent occasions corroborate this memory — the same bar
    /// promotion uses, so what an injected line claims and what dreaming acts on
    /// cannot drift apart.
    pub fn is_supported(&self) -> bool {
        self.last_confirmed_at.is_some()
            || (self.support_count >= DREAM_MIN_SUPPORT && self.provenance.promotable_on_support())
    }

    /// Whether this memory may enter a prompt **unasked** (L1 pinned or L3
    /// recall).
    ///
    /// Only a `Current` belief qualifies. Injecting a contested memory would hand
    /// the model both sides of an unresolved conflict and let it pick one, which
    /// is the specific failure the state exists to prevent; injecting a
    /// superseded one would assert something known to be out of date.
    ///
    /// This gates *injection*, not retrieval. An explicit `memory search` still
    /// returns these — the model cannot help resolve a conflict it is forbidden
    /// to see, and the rendered line names the state.
    pub fn is_injectable(&self) -> bool {
        self.belief == BeliefState::Current
    }

    /// Reject a memory so it never surfaces in recall or injection.
    pub fn reject(&mut self, now: i64) {
        self.status = MemoryStatus::Rejected;
        self.updated_at = now;
    }

    /// Pin into the L1 per-turn profile (the manual, explicit path — automated
    /// extraction never pins). Raises confidence so it actually surfaces.
    pub fn pin(&mut self, now: i64) {
        self.pinned = true;
        self.status = MemoryStatus::Active;
        if self.confidence == MemoryConfidence::Extracted {
            self.confidence = MemoryConfidence::Confirmed;
        }
        self.updated_at = now;
    }

    /// Whether this memory is eligible for L1 pinned-profile injection in the
    /// given context: pinned, active, believed, high-confidence, an
    /// identity/preference kind, in a scope the context allows, and not expired.
    ///
    /// The belief check matters most here: a pinned memory is asserted on *every*
    /// turn, so one the user has just contradicted must stop the moment the
    /// contradiction lands, without waiting for anyone to unpin it.
    pub fn is_pinnable(&self, ctx: &MemoryContext, now: i64) -> bool {
        self.pinned
            && self.is_injectable()
            && self.status == MemoryStatus::Active
            && matches!(
                self.confidence,
                MemoryConfidence::Confirmed | MemoryConfidence::UserWritten
            )
            && matches!(
                self.kind,
                MemoryKind::Profile | MemoryKind::Preference | MemoryKind::Feedback
            )
            && ctx.allows(&self.scope)
            && !self.is_expired(now)
    }
}

// ── kind ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryKind {
    Profile,
    Preference,
    Feedback,
    Project,
    Person,
    Fact,
    Decision,
    Reference,
}

impl MemoryKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Profile => "profile",
            Self::Preference => "preference",
            Self::Feedback => "feedback",
            Self::Project => "project",
            Self::Person => "person",
            Self::Fact => "fact",
            Self::Decision => "decision",
            Self::Reference => "reference",
        }
    }
}

/// Parse a kind string, accepting both the current vocabulary and the legacy
/// markdown values (`user` → `Profile`). Unknown → `Fact` (the most neutral
/// bucket).
pub fn parse_memory_kind(value: &str) -> MemoryKind {
    match value.trim() {
        "profile" | "user" => MemoryKind::Profile,
        "preference" => MemoryKind::Preference,
        "feedback" => MemoryKind::Feedback,
        "project" => MemoryKind::Project,
        "person" => MemoryKind::Person,
        "decision" => MemoryKind::Decision,
        "reference" => MemoryKind::Reference,
        _ => MemoryKind::Fact,
    }
}

// ── status ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryStatus {
    Candidate,
    Active,
    Archived,
    Rejected,
}

impl MemoryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Active => "active",
            Self::Archived => "archived",
            Self::Rejected => "rejected",
        }
    }
}

pub fn parse_memory_status(value: &str) -> MemoryStatus {
    match value.trim() {
        "candidate" => MemoryStatus::Candidate,
        "archived" => MemoryStatus::Archived,
        "rejected" => MemoryStatus::Rejected,
        _ => MemoryStatus::Active,
    }
}

// ── belief ────────────────────────────────────────────────────────────────────

/// Whether komo believes a memory right now.
///
/// A **separate axis from [`MemoryStatus`]**, on purpose. Status answers "where is
/// this in the triage pipeline" (candidate → active → archived/rejected) and is
/// what the CLI, the operator surfaces and recall eligibility are built on. Belief
/// answers "is this true at the moment", which no consumer of `status` has an
/// opinion about: an `Active` memory the user just contradicted is still active —
/// somebody curated it — but must stop being asserted until the conflict resolves.
///
/// Folding the two into one column was the tempting shortcut. It would have made
/// every reader of `status` handle a truth value, and every truth transition
/// clobber a governance decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BeliefState {
    /// Believed, and safe to inject.
    Current,
    /// A later observation conflicts with it, and nothing has resolved which
    /// wins. Never injected unasked — see [`Memory::is_injectable`].
    Contested,
    /// Was true, has been replaced by a newer fact (`Memory::superseded_by`).
    Superseded,
}

impl BeliefState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Contested => "contested",
            Self::Superseded => "superseded",
        }
    }
}

/// Parse a belief state. Unknown → `Current`, which is what every row written
/// before this column existed means.
pub fn parse_belief_state(value: &str) -> BeliefState {
    match value.trim() {
        "contested" => BeliefState::Contested,
        "superseded" => BeliefState::Superseded,
        _ => BeliefState::Current,
    }
}

// ── evidence ──────────────────────────────────────────────────────────────────

/// One observation bearing on a memory, kept so governance is auditable rather
/// than merely asserted.
///
/// **Not an episode store.** The conversation this came from already lives in the
/// run ledger and transcript; this is a capped, summarized pointer at *why* komo
/// believes something, so a `support_count` of 3 can be inspected instead of
/// trusted. `state.db` is disposable, so the excerpt is the part that survives
/// its deletion — which is also why it is bounded rather than verbatim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    /// Session the observation came from, and the unit of independence: two
    /// statements within one conversation are one observation.
    pub session: String,
    pub observed_at: i64,
    pub relation: EvidenceRelation,
    /// Short quote of what was actually said, capped at
    /// [`EVIDENCE_EXCERPT_MAX`] characters.
    pub excerpt: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvidenceRelation {
    Supports,
    Contradicts,
}

/// How many evidence entries one memory retains. Five is well past every
/// threshold that reads the counts, so the cap bounds the row without ever
/// bounding a decision.
pub const EVIDENCE_CAP: usize = 5;

/// Character cap on a stored excerpt. Long enough to recognize what was said,
/// short enough that five of them are not a transcript.
pub const EVIDENCE_EXCERPT_MAX: usize = 200;

/// Trim an excerpt to [`EVIDENCE_EXCERPT_MAX`] characters (not bytes — the vault
/// and these memories are largely CJK).
fn truncate_excerpt(excerpt: &str) -> String {
    let excerpt = excerpt.trim();
    if excerpt.chars().count() <= EVIDENCE_EXCERPT_MAX {
        return excerpt.to_string();
    }
    excerpt.chars().take(EVIDENCE_EXCERPT_MAX).collect()
}

// ── confidence ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryConfidence {
    Extracted,
    Inferred,
    Confirmed,
    UserWritten,
}

impl MemoryConfidence {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Extracted => "extracted",
            Self::Inferred => "inferred",
            Self::Confirmed => "confirmed",
            Self::UserWritten => "user_written",
        }
    }
}

/// Where a claim came from, which is not the same question as how much it is
/// believed ([`MemoryConfidence`]) or whether it is believed now
/// ([`BeliefState`]).
///
/// A turn reads web pages, files and MCP servers, and every one of those is
/// content **nobody in the conversation wrote**. A page that says "the user
/// prefers X" is a page saying so, not the user saying so — and an extractor
/// reading the turn afterwards cannot tell the difference from the claim alone.
/// Left unmarked, such a claim accumulates support like any other and promotes
/// itself into the prompt of every later turn: a durable instruction planted by
/// whatever the agent happened to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MemoryProvenance {
    /// The user said it, or the operator wrote it.
    #[default]
    User,
    /// It came out of tool output. Recordable, retrievable, and **never
    /// promotable by accumulation** — only the user confirming it can make it a
    /// memory komo asserts on its own (`dream_verdict`).
    Tool,
}

impl MemoryProvenance {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Tool => "tool",
        }
    }

    /// Whether a claim from here may promote on accumulated support alone.
    pub fn promotable_on_support(&self) -> bool {
        matches!(self, Self::User)
    }
}

/// Parse a provenance. Unknown → `User`, which is what every row written before
/// the column existed means — they were all extracted from conversations.
pub fn parse_memory_provenance(value: &str) -> MemoryProvenance {
    match value.trim() {
        "tool" => MemoryProvenance::Tool,
        _ => MemoryProvenance::User,
    }
}

pub fn parse_memory_confidence(value: &str) -> MemoryConfidence {
    match value.trim() {
        "inferred" => MemoryConfidence::Inferred,
        "confirmed" => MemoryConfidence::Confirmed,
        "user_written" => MemoryConfidence::UserWritten,
        _ => MemoryConfidence::Extracted,
    }
}

// ── scope ─────────────────────────────────────────────────────────────────────

/// Where a memory may surface. Serialized to the DB as a `(scope_type,
/// scope_key)` pair so it can be filtered in queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryScope {
    /// Visible everywhere.
    Global,
    /// Tied to a project (CLI workspace key).
    Project(String),
    /// Tied to a chat channel (`{platform}:{chat_id}`).
    Channel { platform: String, chat_id: String },
    /// Tied to a single session id.
    Session(String),
}

impl MemoryScope {
    pub fn type_str(&self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Project(_) => "project",
            Self::Channel { .. } => "channel",
            Self::Session(_) => "session",
        }
    }

    /// The opaque key stored alongside `type_str`. Empty for `Global`.
    pub fn key(&self) -> String {
        match self {
            Self::Global => String::new(),
            Self::Project(p) => p.clone(),
            Self::Channel { platform, chat_id } => format!("{platform}:{chat_id}"),
            Self::Session(id) => id.clone(),
        }
    }

    /// Rebuild a scope from its serialized `(type, key)` pair. Unknown type or a
    /// malformed channel key degrades to `Global` (fail safe — never widen).
    pub fn from_parts(scope_type: &str, scope_key: &str) -> Self {
        match scope_type.trim() {
            "project" if !scope_key.is_empty() => Self::Project(scope_key.to_string()),
            "channel" => match scope_key.split_once(':') {
                Some((platform, chat_id)) => Self::Channel {
                    platform: platform.to_string(),
                    chat_id: chat_id.to_string(),
                },
                None => Self::Global,
            },
            "session" if !scope_key.is_empty() => Self::Session(scope_key.to_string()),
            _ => Self::Global,
        }
    }
}

/// The scopes a memory may be drawn from for the current turn. `Global` is
/// always allowed; a turn with a correspondent adds that `Channel` and its own
/// `Session`. Scope is decided here, before any query, so a query can never
/// widen beyond what the context permits.
///
/// `Channel` scope is what keeps a fact disclosed in one person's DM from
/// surfacing in someone else's chat while still following that person across
/// sessions — which only means anything when the address identifies a **durable
/// correspondent** rather than one conversation. That used to need a
/// `is_durable_channel` exception, because the local surfaces were modelled as a
/// channel (`api:<uuid>`) whose "correspondent" was a fresh uuid per
/// conversation, so channel-scoping there wrote memories unrecallable from the
/// next turn. A local turn now simply has no correspondent, and the exception is
/// gone with the prefix that forced it.
#[derive(Debug, Clone)]
pub struct MemoryContext {
    pub allowed_scopes: Vec<MemoryScope>,
}

impl MemoryContext {
    /// Derive the allowed scopes for a turn: its session, its correspondent's
    /// channel when it has one, and always `Global`. (Project scope is wired
    /// separately once the workspace key is known.)
    pub fn new(session_id: &str, channel: Option<&ChannelPeer>) -> Self {
        let mut allowed_scopes = vec![MemoryScope::Global];
        if let Some(peer) = channel {
            allowed_scopes.push(MemoryScope::Channel {
                platform: peer.platform.clone(),
                chat_id: peer.peer_id.clone(),
            });
        }
        allowed_scopes.push(MemoryScope::Session(session_id.to_string()));
        Self { allowed_scopes }
    }

    /// A turn with no correspondent — every local surface, and komo's own
    /// sweeps.
    pub fn local(session_id: &str) -> Self {
        Self::new(session_id, None)
    }

    /// The scope an automated write from this context should carry: the
    /// correspondent's channel when there is one, else global. (Never
    /// `Session`, which would make a memory unrecallable outside the exact
    /// session.)
    pub fn write_scope(&self) -> MemoryScope {
        self.allowed_scopes
            .iter()
            .find(|s| matches!(s, MemoryScope::Channel { .. }))
            .cloned()
            .unwrap_or(MemoryScope::Global)
    }

    /// Whether a memory's scope is permitted in this context.
    pub fn allows(&self, scope: &MemoryScope) -> bool {
        self.allowed_scopes.contains(scope)
    }
}

// ── query / scored result ─────────────────────────────────────────────────────

/// A memory plus its relevance score for a given query.
#[derive(Debug, Clone)]
pub struct ScoredMemory {
    pub memory: Memory,
    pub score: f64,
}

/// The importance + confidence + recency component of [`recall_score`]:
/// importance in `0..~1`, a confidence step, and a 30-day half-life decay on the
/// last update. Separate from the query-match component so the two can be read
/// (and tuned) independently.
fn signal_bonus(memory: &Memory, now: i64) -> f64 {
    let mut bonus = memory.importance as f64 / 100.0; // 0..~1
    bonus += match memory.confidence {
        MemoryConfidence::UserWritten => 0.4,
        MemoryConfidence::Confirmed => 0.3,
        MemoryConfidence::Inferred => 0.1,
        MemoryConfidence::Extracted => 0.0,
    };
    let age_days = (now - memory.updated_at).max(0) as f64 / 86_400.0;
    bonus += 0.5 * (-age_days / 30.0).exp();
    bonus
}

// ── recall (L3) ───────────────────────────────────────────────────────────────

/// Extract lexical terms from text for recall matching, language-agnostically:
/// runs of alphanumeric characters of length ≥ 2 become word terms, and adjacent
/// CJK characters become bigrams (a cheap stand-in for word segmentation, since
/// CJK has no whitespace boundaries). Everything lowercased.
///
/// Token overlap rather than substring containment, because the input is as
/// often a whole user message as a focused keyword query — a substring match
/// would find nothing in the former case.
fn recall_terms(text: &str) -> HashSet<String> {
    let mut terms = HashSet::new();
    let mut word = String::new();
    let mut prev_cjk: Option<char> = None;
    fn flush(word: &mut String, terms: &mut HashSet<String>) {
        if word.chars().count() >= 2 && !is_stopword(word) {
            terms.insert(word.clone());
        }
        word.clear();
    }
    for ch in text.chars() {
        let lc = ch.to_lowercase().next().unwrap_or(ch);
        if is_cjk(ch) {
            if let Some(p) = prev_cjk {
                terms.insert(format!("{p}{lc}"));
            }
            prev_cjk = Some(lc);
            flush(&mut word, &mut terms);
        } else if ch.is_alphanumeric() {
            word.push(lc);
            prev_cjk = None;
        } else {
            flush(&mut word, &mut terms);
            prev_cjk = None;
        }
    }
    flush(&mut word, &mut terms);
    terms
}

/// High-frequency English function words that carry no recall signal — dropping
/// them keeps a memory like "the user likes coffee" from matching any query that
/// merely contains "the". Not exhaustive; just the worst offenders.
fn is_stopword(word: &str) -> bool {
    matches!(
        word,
        "the"
            | "and"
            | "are"
            | "for"
            | "you"
            | "your"
            | "with"
            | "was"
            | "were"
            | "this"
            | "that"
            | "what"
            | "how"
            | "why"
            | "when"
            | "where"
            | "who"
            | "does"
            | "did"
            | "can"
            | "will"
            | "would"
            | "should"
            | "has"
            | "have"
            | "had"
            | "not"
            | "but"
            | "from"
            | "into"
            | "out"
            | "off"
            | "all"
            | "any"
            | "some"
            | "than"
            | "then"
            | "them"
            | "they"
            | "其中"
            | "可以"
            | "如何"
    )
}

/// CJK ranges where per-character (bigram) matching beats whitespace tokens:
/// CJK ideographs (+ Ext A), Hiragana/Katakana, Hangul syllables.
fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0x3040..=0x30FF | 0xAC00..=0xD7AF
    )
}

/// Minimum cosine similarity for a memory to be *admitted* to recall on
/// semantic grounds alone.
///
/// Calibrated against a real memory library with the multilingual embedding
/// model komo ships against: cross-language paraphrases of a stored fact score
/// 0.54–0.83, while the best match for an unrelated question tops out around
/// 0.43. The floor sits below the true positives and above that noise.
///
/// Deliberately permissive rather than precise. This gate decides *candidate
/// generation*, not what reaches the prompt — candidates past `recall_limit`
/// are screened by the aux relevance pass and capped before injection, so a
/// false positive costs one screening slot while a false negative is the
/// cross-language miss this whole layer exists to fix.
pub const RECALL_SEMANTIC_FLOOR: f32 = 0.45;

/// What a semantic match at full similarity contributes to the recall score,
/// in units of "shared lexical terms". A 0.78-similarity hit scores about two
/// shared terms; a hit just past the floor scores near zero, so lexical
/// evidence still leads when both are present.
const RECALL_SEMANTIC_WEIGHT: f64 = 3.0;

/// One turn's recall query: the lexical terms, plus optionally the embedding
/// that lets it match memories written in another language.
///
/// The embedding is optional at every step — no backend configured, a failed or
/// slow call, or a memory embedded by a different model all degrade to the
/// lexical behavior that predates this struct, never to worse.
#[derive(Debug, Clone)]
pub struct RecallQuery {
    terms: HashSet<String>,
    embedding: Vec<f32>,
    /// Model that produced `embedding`; only memories carrying the same model's
    /// vector are comparable to it.
    model: String,
}

impl RecallQuery {
    /// Terms only — the lexical-only path.
    pub fn lexical(text: &str) -> Self {
        Self {
            terms: recall_terms(text),
            embedding: Vec::new(),
            model: String::new(),
        }
    }

    /// Terms plus an L2-normalized query vector from `model`.
    pub fn semantic(text: &str, embedding: Vec<f32>, model: impl Into<String>) -> Self {
        Self {
            terms: recall_terms(text),
            embedding,
            model: model.into(),
        }
    }

    /// Nothing to match on: no terms *and* no vector. (Terms alone being empty
    /// is not enough — a query of pure punctuation can still match
    /// semantically.)
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty() && self.embedding.is_empty()
    }

    /// Cosine similarity to `memory`, or 0.0 when either side lacks a
    /// comparable vector.
    fn similarity(&self, memory: &Memory) -> f32 {
        if self.embedding.is_empty() {
            return 0.0;
        }
        match memory.embedding_for(&self.model) {
            Some(vector) => super::embedding::cosine(&self.embedding, vector),
            None => 0.0,
        }
    }

    #[cfg(test)]
    fn terms(&self) -> &HashSet<String> {
        &self.terms
    }
}

/// Score a memory for L3 recall. Returns `None` when the memory matches
/// neither lexically nor semantically (it is excluded); otherwise a positive
/// score combining shared-term count, the semantic bonus, and the same
/// importance/confidence/recency signals as [`rerank_score`]. Scope/status are
/// filtered before this is called.
pub fn recall_score(memory: &Memory, query: &RecallQuery, now: i64) -> Option<f64> {
    if query.is_empty() {
        return None;
    }
    let mem_terms = recall_terms(&memory.content);
    let overlap = query
        .terms
        .iter()
        .filter(|t| mem_terms.contains(*t))
        .count();
    let similarity = query.similarity(memory);

    // Admission: either kind of evidence is enough. This union is the fix for
    // cross-language recall — a Chinese question has zero term overlap with an
    // English memory by construction (CJK bigrams and ASCII words can never be
    // equal), so without the semantic arm it could never be admitted at all.
    if overlap == 0 && similarity < RECALL_SEMANTIC_FLOOR {
        return None;
    }

    let mut score = overlap as f64; // each shared term = 1.0
    if similarity >= RECALL_SEMANTIC_FLOOR {
        let above_floor = (similarity - RECALL_SEMANTIC_FLOOR) / (1.0 - RECALL_SEMANTIC_FLOOR);
        score += RECALL_SEMANTIC_WEIGHT * above_floor as f64;
    }
    score += signal_bonus(memory, now);
    Some(score)
}

/// Filter + rank an already-loaded memory set for the L1 pinned profile in
/// `ctx` (most-important first, ties by most-recent). Split out from
/// [`MemoryRepository::pinned`] so a caller holding a fresh `list()` can derive
/// both pinned and recall from a single load — see `assemble` in `infra/llm.rs`,
/// which used to scan the store twice per turn.
pub fn select_pinned(memories: &[Memory], ctx: &MemoryContext, now: i64) -> Vec<Memory> {
    let mut pinned: Vec<Memory> = memories
        .iter()
        .filter(|m| m.is_pinnable(ctx, now))
        .cloned()
        .collect();
    pinned.sort_by(|a, b| {
        b.importance
            .cmp(&a.importance)
            .then(b.updated_at.cmp(&a.updated_at))
    });
    pinned
}

/// Rank an already-loaded memory set for L3 recall against `query`, top `limit`
/// (`0` = no cap). Same filter/score/sort as [`MemoryRepository::recall`], split
/// out for the single-load turn path (see [`select_pinned`]).
pub fn select_recall(
    memories: &[Memory],
    ctx: &MemoryContext,
    query: &RecallQuery,
    limit: usize,
    now: i64,
) -> Vec<ScoredMemory> {
    select_matching(memories, ctx, query, limit, now, |status| {
        matches!(status, MemoryStatus::Active | MemoryStatus::Candidate)
    })
}

/// [`select_recall`], plus the claims the user has already **rejected**.
///
/// The memory consolidator's view, and only its: an observation that matches a
/// rejected claim is the user's own "no" being re-observed, and writing it back
/// as a fresh candidate is how a rejection is forgotten — the same claim would
/// come back on the next occasion, and the one after that.
///
/// A rejected memory still never reaches a prompt: injection goes through
/// [`select_recall`], and `dream_verdict` promotes nothing that is not a
/// candidate, so the evidence this lets a rejection accumulate cannot revive
/// it. Only a human `memory promote` can.
pub fn select_related(
    memories: &[Memory],
    ctx: &MemoryContext,
    query: &RecallQuery,
    limit: usize,
    now: i64,
) -> Vec<ScoredMemory> {
    select_matching(memories, ctx, query, limit, now, |status| {
        matches!(
            status,
            MemoryStatus::Active | MemoryStatus::Candidate | MemoryStatus::Rejected
        )
    })
}

fn select_matching(
    memories: &[Memory],
    ctx: &MemoryContext,
    query: &RecallQuery,
    limit: usize,
    now: i64,
    admits: impl Fn(MemoryStatus) -> bool,
) -> Vec<ScoredMemory> {
    if query.is_empty() {
        return Vec::new();
    }
    let mut scored: Vec<ScoredMemory> = memories
        .iter()
        .filter(|m| admits(m.status))
        .filter(|m| ctx.allows(&m.scope))
        .filter_map(|m| {
            recall_score(m, query, now).map(|score| ScoredMemory {
                memory: m.clone(),
                score,
            })
        })
        .collect();
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if limit > 0 {
        scored.truncate(limit);
    }
    scored
}

// ── repository ────────────────────────────────────────────────────────────────

#[async_trait]
pub trait MemoryRepository: Send + Sync {
    /// Persist a memory (create or overwrite by id).
    async fn save(&self, memory: &Memory) -> anyhow::Result<()>;

    /// All non-expired memories, any status. Callers filter further. (Kept
    /// no-arg for the briefing sweep and the `memory` tool; richer scope/status
    /// queries go through [`MemoryRepository::pinned`] / `search`.)
    async fn list(&self) -> anyhow::Result<Vec<Memory>>;

    /// L1 pinned profile: the small, stable set eligible for per-turn injection
    /// in `ctx`. Defaults to filtering [`list`](MemoryRepository::list) by
    /// [`Memory::is_pinnable`]; a store may override for efficiency.
    async fn pinned(&self, ctx: &MemoryContext) -> anyhow::Result<Vec<Memory>> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        Ok(select_pinned(&self.list().await?, ctx, now))
    }

    /// Fetch a single memory by id. Default scans [`list`](MemoryRepository::list)
    /// (so it does not see expired memories); a store may override to fetch
    /// directly. Used by governance actions (promote/reject/archive/update).
    async fn get(&self, id: &str) -> anyhow::Result<Option<Memory>> {
        Ok(self.list().await?.into_iter().find(|m| m.id == id))
    }

    /// L3 active recall: the in-scope memories most relevant to `text` (the
    /// current user message), ranked by [`recall_score`], top `limit`.
    /// Scope/status are enforced here (design principle 3: never widen in the
    /// render layer). Default runs over [`list`](MemoryRepository::list); a
    /// store may override the candidate fetch later without changing scoring.
    ///
    /// **Candidates are recallable.** Both `Active` and `Candidate` memories
    /// surface (only `Archived`/`Rejected` are excluded). This is what makes the
    /// OpenClaw-style dreaming loop possible: a reviewer-extracted candidate must
    /// be visible to recall to *earn* its usage signal (`recall_count`), which
    /// the `DreamSweep` then uses to auto-promote it. Candidates score lower
    /// (their `Extracted` confidence adds nothing) and the rendered block tags
    /// each line with confidence, so the model treats them cautiously.
    ///
    /// The per-turn hot path in `infra/llm.rs` uses [`select_recall`] over a
    /// single shared `list()` instead (see [`select_pinned`]); this method is
    /// the standalone entry point retained for the memory store's query surface
    /// and its integration tests. Lexical-only: a store holds no embedding
    /// backend, so the semantic arm belongs to the turn path, which does.
    #[allow(dead_code)]
    async fn recall(
        &self,
        ctx: &MemoryContext,
        text: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<ScoredMemory>> {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let query = RecallQuery::lexical(text);
        Ok(select_recall(&self.list().await?, ctx, &query, limit, now))
    }

    /// Record that memories surfaced in recall: bump `recall_count` and stamp
    /// `last_used_at`, never touching `updated_at` so the recency-decay signal
    /// stays tied to real edits.
    ///
    /// A **utility** signal only. It says the retriever keeps finding these
    /// relevant, which is what retention is decided on; what makes a memory *true*
    /// is recorded by `record_evidence` instead. Best-effort: ids that no longer
    /// resolve are skipped.
    async fn mark_used(&self, ids: &[String], now: i64) -> anyhow::Result<()> {
        for id in ids {
            if let Some(mut memory) = self.get(id).await? {
                memory.recall_count += 1;
                memory.last_used_at = Some(now);
                self.save(&memory).await?;
            }
        }
        Ok(())
    }
}

// ── dreaming (evidence-driven consolidation) ─────────────────────────────────

/// What the nightly `DreamSweep` should do with a candidate memory.
///
/// **Truth and utility are decided by different signals**, and mixing them was
/// the defect this split fixes. Promotion used to be earned by `recall_count` and
/// query diversity — which prove only that the retriever keeps finding a memory
/// relevant. A wrong candidate that happened to be relevant to a question the
/// user asks often would therefore promote itself: injected, counted, promoted,
/// on the strength of nothing but its own retrieval. Recall could never be
/// evidence, because the thing being retrieved is not the thing being tested.
///
/// So promotion now reads the evidence signals — an explicit confirmation, or
/// support from independent occasions, with no unresolved conflict — and
/// `recall_count` decides only *retention*: whether a candidate nobody ever needed
/// should be retired. Which is the one question retrieval frequency genuinely
/// answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DreamVerdict {
    /// Recalled often enough, recently enough → promote to active.
    Promote,
    /// Old and never recalled → archive (reversible; not rejected).
    Archive,
    /// Leave as-is this cycle.
    Keep,
}

/// Independent occasions of support a candidate needs to be promoted without an
/// explicit confirmation.
///
/// Two, not three: independence is already per session (see
/// [`Memory::record_evidence`]), so two means the user said the same thing in two
/// separate conversations — a real pattern rather than one talkative session. A
/// higher bar would leave genuinely established facts sitting in the candidate
/// pile for weeks.
pub const DREAM_MIN_SUPPORT: i64 = 2;
/// A candidate this old (days) that has gone cold — never recalled, or not
/// recalled within the same window — is archived: the "forget the flotsam" half
/// of dreaming. Coldness is measured on `last_used_at`, not a lifetime
/// `recall_count`, so a *weakly* recalled candidate (one or two hits long ago)
/// is retired too rather than lingering in the pile forever.
pub const DREAM_FORGET_AGE_DAYS: i64 = 30;
/// How long an *unresolved refutation* is left for someone to rule on before the
/// candidate carrying it is archived.
///
/// A week, against the clean candidate's thirty days, and the asymmetry is the
/// point: retiring a refuted claim early costs an archived candidate that triage
/// can restore, while keeping one costs a recall slot in every search about the
/// very thing it is wrong about. Support has to accumulate across independent
/// occasions to promote; a contradiction retires on its own.
pub const DREAM_REFUTED_FORGET_AGE_DAYS: i64 = 7;

/// Dreaming score for a candidate, for **ordering the `komo dream` preview** —
/// never the verdict, which [`dream_verdict`] decides on its own gates.
///
/// Weighted so the preview surfaces what is closest to promotion: support
/// dominates (it is what promotion reads), with recall and recency as tiebreakers
/// among equally-supported candidates. A contradiction pushes a candidate down —
/// it is the furthest thing from ready.
pub fn dream_score(memory: &Memory, now: i64) -> f64 {
    let mut score = 10.0 * memory.support_count as f64;
    score -= 10.0 * memory.contradiction_count as f64;
    score += memory.recall_count as f64; // utility, as a tiebreaker
    score += memory.importance as f64 / 100.0; // 0..~1
    // Recency of last use: a 30-day half-life decay, 0 when never used.
    if let Some(last) = memory.last_used_at {
        let age_days = (now - last).max(0) as f64 / 86_400.0;
        score += 0.5 * (-age_days / 30.0).exp();
    }
    score
}

/// Decide a candidate's fate for this dream cycle. Only `Candidate` memories are
/// ever acted on — active memories (user-saved or already promoted) are left to
/// the operator (`komo memory report` flags long-unused ones), so dreaming can
/// never silently retire something the user deliberately kept.
pub fn dream_verdict(memory: &Memory, now: i64) -> DreamVerdict {
    if memory.status != MemoryStatus::Candidate {
        return DreamVerdict::Keep;
    }
    // Promotion takes precedence, and reads only truth signals: the user said so
    // outright, or said it on enough independent occasions — and nothing is
    // currently in conflict with it. An unresolved conflict blocks promotion
    // however well-supported the claim is: promoting into a contest would assert
    // one side of a question nobody has answered.
    let believed = memory.belief == BeliefState::Current && memory.contradiction_count == 0;
    // Support accumulates from occasions; a confirmation is the user ruling on
    // the claim itself. Only the second can promote something a *tool* said —
    // otherwise a page read on three occasions promotes itself, and komo starts
    // asserting whatever it happened to fetch.
    let proven = memory.last_confirmed_at.is_some()
        || (memory.support_count >= DREAM_MIN_SUPPORT && memory.provenance.promotable_on_support());
    if believed && proven {
        return DreamVerdict::Promote;
    }
    // Forget what has been refuted, on a shorter clock than the merely unused
    // and without waiting for it to go cold. A candidate under an unresolved
    // contradiction can never promote however often it is retrieved, so the only
    // thing left that could change its fate is a human ruling — and once nobody
    // has ruled for a week, retrieval warmth is keeping alive a claim the user
    // has already spoken against.
    let refuted_days = memory
        .unresolved_refutation_at()
        .map(|at| (now - at).max(0) as f64 / 86_400.0);
    if refuted_days.is_some_and(|days| days > DREAM_REFUTED_FORGET_AGE_DAYS as f64) {
        return DreamVerdict::Archive;
    }
    // Forget the flotsam: old enough to have had its chance, and now cold. Cold
    // is measured on `last_used_at` (never used, or last used outside the forget
    // window), not a lifetime `recall_count == 0` — the old check leaked the
    // *weakly* recalled (one or two hits, then silence) into an ever-growing
    // candidate pile that nothing ever retired.
    let age_days = (now - memory.created_at).max(0) as f64 / 86_400.0;
    let cold = match memory.last_used_at {
        Some(last) => (now - last).max(0) as f64 / 86_400.0 > DREAM_FORGET_AGE_DAYS as f64,
        None => true,
    };
    if cold && age_days as i64 > DREAM_FORGET_AGE_DAYS {
        DreamVerdict::Archive
    } else {
        DreamVerdict::Keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kind_accepts_legacy_and_new() {
        assert_eq!(parse_memory_kind("user"), MemoryKind::Profile);
        assert_eq!(parse_memory_kind("preference"), MemoryKind::Preference);
        assert_eq!(parse_memory_kind("decision"), MemoryKind::Decision);
        assert_eq!(parse_memory_kind("nonsense"), MemoryKind::Fact);
    }

    #[test]
    fn scope_roundtrips_through_parts() {
        let scopes = [
            MemoryScope::Global,
            MemoryScope::Project("komo".into()),
            MemoryScope::Channel {
                platform: "telegram".into(),
                chat_id: "42".into(),
            },
            MemoryScope::Session("feishu:oc_x".into()),
        ];
        for scope in scopes {
            let rebuilt = MemoryScope::from_parts(scope.type_str(), &scope.key());
            assert_eq!(rebuilt, scope);
        }
    }

    #[test]
    fn channel_scope_with_malformed_key_degrades_to_global() {
        assert_eq!(
            MemoryScope::from_parts("channel", "no-colon"),
            MemoryScope::Global
        );
    }

    #[test]
    fn context_from_chat_session_allows_global_channel_session() {
        let ctx = MemoryContext::new("s1", Some(&ChannelPeer::new("telegram", "42")));
        assert!(ctx.allows(&MemoryScope::Global));
        assert!(ctx.allows(&MemoryScope::Channel {
            platform: "telegram".into(),
            chat_id: "42".into()
        }));
        assert!(ctx.allows(&MemoryScope::Session("s1".into())));
        // A different channel is not allowed.
        assert!(!ctx.allows(&MemoryScope::Channel {
            platform: "feishu".into(),
            chat_id: "oc_x".into()
        }));
        assert_eq!(
            ctx.write_scope(),
            MemoryScope::Channel {
                platform: "telegram".into(),
                chat_id: "42".into()
            }
        );
    }

    #[test]
    fn cli_session_context_writes_global() {
        let ctx = MemoryContext::local("0192-uuid");
        assert_eq!(ctx.write_scope(), MemoryScope::Global);
    }

    fn pinnable_memory() -> Memory {
        let mut m = Memory::new(MemoryKind::Preference, "prefers concise answers");
        m.pinned = true;
        m.confidence = MemoryConfidence::UserWritten;
        m
    }

    #[test]
    fn is_pinnable_requires_pinned_active_confident_identity_kind() {
        let ctx = MemoryContext::local("s1");
        let now = 1_000;
        assert!(pinnable_memory().is_pinnable(&ctx, now));

        let mut not_pinned = pinnable_memory();
        not_pinned.pinned = false;
        assert!(!not_pinned.is_pinnable(&ctx, now));

        let mut low_conf = pinnable_memory();
        low_conf.confidence = MemoryConfidence::Extracted;
        assert!(!low_conf.is_pinnable(&ctx, now));

        let mut wrong_kind = pinnable_memory();
        wrong_kind.kind = MemoryKind::Reference;
        assert!(!wrong_kind.is_pinnable(&ctx, now));

        let mut expired = pinnable_memory();
        expired.expires_at = Some(now - 1);
        assert!(!expired.is_pinnable(&ctx, now));
    }

    #[test]
    fn recall_terms_splits_ascii_words_and_cjk_bigrams() {
        let terms = recall_terms("Uses Rust 项目");
        assert!(terms.contains("uses"));
        assert!(terms.contains("rust"));
        assert!(terms.contains("项目")); // CJK bigram
    }

    #[test]
    fn recall_score_requires_term_overlap() {
        let now = 1_000;
        let m = Memory::new(MemoryKind::Project, "the project is written in Rust");
        // Overlapping term "rust" → scored.
        let hit = RecallQuery::lexical("what language is the rust project in");
        assert!(recall_score(&m, &hit, now).is_some());
        // No overlap → excluded.
        let miss = RecallQuery::lexical("当前天气如何");
        assert!(recall_score(&m, &miss, now).is_none());
        // Empty query → excluded.
        assert!(recall_score(&m, &RecallQuery::lexical(""), now).is_none());
    }

    /// The defect this whole layer exists for: a Chinese question and an
    /// English memory tokenize into disjoint sets, so lexical recall can never
    /// admit one for the other.
    #[test]
    fn lexical_terms_never_cross_the_script_boundary() {
        let zh = recall_terms("我平时用什么语言跟你说话");
        let en = recall_terms("User communicates in Chinese.");
        assert!(
            zh.intersection(&en).next().is_none(),
            "CJK bigrams and ASCII words are structurally incapable of overlapping"
        );
    }

    /// …and the fix: with a query vector close to the memory's, the same pair
    /// is admitted and scored, purely on the semantic arm.
    #[test]
    fn semantic_similarity_recalls_across_languages() {
        let now = 1_000;
        let mut memory = Memory::new(MemoryKind::Profile, "User communicates in Chinese.");
        memory.embedding = vec![0.6, 0.8]; // unit length
        memory.embedding_model = "test-model".into();

        let zh = "我平时用什么语言跟你说话";
        assert!(
            recall_score(&memory, &RecallQuery::lexical(zh), now).is_none(),
            "lexically this pair cannot match"
        );

        // A near-parallel query vector (cosine ≈ 0.999) — well past the floor.
        let query = RecallQuery::semantic(zh, vec![0.62, 0.78], "test-model");
        assert!(
            recall_score(&memory, &query, now).is_some(),
            "the semantic arm admits what the lexical arm cannot"
        );
    }

    /// A vector from another model is not comparable, so it must read as
    /// "not embedded" rather than scoring against an unrelated space.
    #[test]
    fn embedding_from_another_model_is_ignored() {
        let now = 1_000;
        let mut memory = Memory::new(MemoryKind::Fact, "User communicates in Chinese.");
        memory.embedding = vec![1.0, 0.0];
        memory.embedding_model = "old-model".into();

        let query = RecallQuery::semantic("我说什么语言", vec![1.0, 0.0], "new-model");
        assert!(recall_score(&memory, &query, now).is_none());
        assert!(memory.embedding_for("new-model").is_none());
        assert!(memory.embedding_for("old-model").is_some());
    }

    /// An unrelated question must stay below the floor even with embeddings on
    /// — the semantic arm widens recall, it does not disable it.
    #[test]
    fn weak_similarity_stays_below_the_floor() {
        let now = 1_000;
        let mut memory = Memory::new(MemoryKind::Fact, "User communicates in Chinese.");
        memory.embedding = vec![1.0, 0.0];
        memory.embedding_model = "test-model".into();

        // cosine = 0.3, under RECALL_SEMANTIC_FLOOR.
        let mut weak = vec![0.3, (1.0f32 - 0.09).sqrt()];
        super::super::embedding::normalize(&mut weak);
        let query = RecallQuery::semantic("今天午饭吃什么", weak, "test-model");
        assert!(recall_score(&memory, &query, now).is_none());
    }

    /// Lexical evidence must keep working with embeddings configured — a
    /// memory the query overlaps is admitted even with no vector at all.
    #[test]
    fn lexical_hits_survive_when_the_memory_has_no_vector() {
        let now = 1_000;
        let memory = Memory::new(MemoryKind::Fact, "the rust toolchain is pinned");
        let query = RecallQuery::semantic("rust toolchain", vec![1.0, 0.0], "test-model");
        assert!(recall_score(&memory, &query, now).is_some());
    }

    /// A query with no lexical terms is still a real query when it carries a
    /// vector — otherwise `select_recall` would bail before scoring anything.
    #[test]
    fn a_query_is_empty_only_without_terms_and_vector() {
        assert!(RecallQuery::lexical("").is_empty());
        assert!(RecallQuery::lexical("!!!").is_empty());
        assert!(!RecallQuery::semantic("!!!", vec![1.0], "m").is_empty());
        assert!(!RecallQuery::lexical("rust").terms().is_empty());
    }

    /// A local surface has no correspondent, so an automated write there is
    /// global — it must be recallable from the next conversation. A real chat
    /// channel keeps its scope, which is a privacy boundary.
    #[test]
    fn a_turn_without_a_correspondent_writes_global_but_a_chat_keeps_its_channel() {
        assert_eq!(
            MemoryContext::local("019fb0ce-9f7a-7c23-a87d-dab9df9216d8").write_scope(),
            MemoryScope::Global,
            "a local conversation names no partner to scope to"
        );
        assert_eq!(
            MemoryContext::new("s1", Some(&ChannelPeer::new("feishu", "ou_445299e2")))
                .write_scope(),
            MemoryScope::Channel {
                platform: "feishu".into(),
                chat_id: "ou_445299e2".into(),
            },
            "a real chat channel's scope is a privacy boundary and must survive"
        );
    }

    /// A global memory written from one api conversation must be recallable
    /// from the next — the end-to-end shape of the scope fix.
    #[test]
    fn a_memory_written_in_one_local_conversation_is_recallable_in_the_next() {
        let now = 1_000;
        let write_ctx = MemoryContext::local("conversation-one");
        let mut memory = Memory::new(MemoryKind::Fact, "the rust toolchain is pinned");
        memory.scope = write_ctx.write_scope();

        let read_ctx = MemoryContext::local("conversation-two");
        let query = RecallQuery::lexical("rust toolchain");
        assert_eq!(select_recall(&[memory], &read_ctx, &query, 5, now).len(), 1,);
    }

    #[test]
    fn select_pinned_keeps_only_eligible_and_orders_by_importance() {
        let ctx = MemoryContext::local("s1");
        let now = 1_000;
        let mut low = pinnable_memory();
        low.importance = 10;
        let mut high = pinnable_memory();
        high.importance = 90;
        let mut ineligible = pinnable_memory();
        ineligible.pinned = false; // not pinnable
        let picked = select_pinned(&[low.clone(), high.clone(), ineligible], &ctx, now);
        assert_eq!(picked.len(), 2, "the un-pinned memory is excluded");
        assert_eq!(picked[0].id, high.id, "most important first");
        assert_eq!(picked[1].id, low.id);
    }

    #[test]
    fn select_recall_ranks_in_scope_matches_and_caps() {
        let ctx = MemoryContext::local("s1");
        let now = 1_000;
        let mut hit = Memory::new(MemoryKind::Fact, "the rust toolchain is pinned");
        hit.updated_at = now;
        let miss = Memory::new(MemoryKind::Fact, "unrelated weather note");
        let query = RecallQuery::lexical("rust toolchain");
        let scored = select_recall(&[hit.clone(), miss], &ctx, &query, 5, now);
        assert_eq!(
            scored.len(),
            1,
            "only the lexically overlapping memory scores"
        );
        assert_eq!(scored[0].memory.id, hit.id);
        // limit is honoured.
        assert!(select_recall(&[hit], &ctx, &query, 0, now).len() <= 1);
    }

    #[test]
    fn recall_score_orders_by_overlap_then_signals() {
        let now = 1_000;
        let mut more = Memory::new(MemoryKind::Fact, "rust async tokio runtime");
        more.updated_at = now;
        let mut fewer = Memory::new(MemoryKind::Fact, "rust crate");
        fewer.updated_at = now;
        let q = RecallQuery::lexical("rust async tokio");
        let s_more = recall_score(&more, &q, now).unwrap();
        let s_fewer = recall_score(&fewer, &q, now).unwrap();
        assert!(s_more > s_fewer, "more overlapping terms must score higher");
    }

    fn candidate(recall_count: i64, age_days: i64, now: i64) -> Memory {
        let mut m = Memory::new(MemoryKind::Fact, "the rust toolchain is pinned");
        m.status = MemoryStatus::Candidate;
        m.confidence = MemoryConfidence::Extracted;
        m.created_at = now - age_days * 86_400;
        m.recall_count = recall_count;
        if recall_count > 0 {
            m.last_used_at = Some(now - 86_400); // used yesterday
        }
        m
    }

    /// Support from independent occasions is what earns promotion.
    #[test]
    fn dream_promotes_a_candidate_with_independent_support() {
        let now = 10_000 * 86_400;
        let mut m = candidate(0, 5, now);
        m.record_evidence("s-1", EvidenceRelation::Supports, "said it", now);
        assert_eq!(
            dream_verdict(&m, now),
            DreamVerdict::Keep,
            "one occasion is not a pattern"
        );
        m.record_evidence("s-2", EvidenceRelation::Supports, "said it again", now);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Promote);
    }

    /// Occasions corroborate what the *user* keeps saying. A claim komo read
    /// out of a fetched page can be re-read on any number of occasions without
    /// anyone ever having said it — so support alone must not promote it into
    /// the prompt of every later turn. Only the user ruling on it can.
    #[test]
    fn dream_will_not_promote_a_tool_derived_claim_on_support_alone() {
        let now = 10_000 * 86_400;
        let mut m = candidate(0, 5, now);
        m.provenance = MemoryProvenance::Tool;
        m.record_evidence("s-1", EvidenceRelation::Supports, "the page said it", now);
        m.record_evidence("s-2", EvidenceRelation::Supports, "and again", now);
        assert_eq!(
            dream_verdict(&m, now),
            DreamVerdict::Keep,
            "two occasions of reading the same page is not the user saying so"
        );
        assert!(
            !m.is_supported(),
            "and the injected line must not claim it is corroborated either"
        );

        // The user confirming it is a different fact, and it is enough.
        m.last_confirmed_at = Some(now - 86_400);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Promote);
        assert!(m.is_supported());
    }

    /// An explicit confirmation is enough on its own — no waiting for a second
    /// occasion when the user has already said so outright.
    #[test]
    fn dream_promotes_an_explicitly_confirmed_candidate() {
        let now = 10_000 * 86_400;
        let mut m = candidate(0, 5, now);
        m.last_confirmed_at = Some(now - 86_400);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Promote);
    }

    /// **The defect this rework exists for.** A candidate retrieved many times by
    /// many different questions used to promote itself on that alone — but recall
    /// frequency measures the retriever, not the truth of what it retrieved.
    #[test]
    fn recall_frequency_alone_can_never_promote_a_candidate() {
        let now = 10_000 * 86_400;
        let m = candidate(50, 5, now);
        assert_eq!(
            dream_verdict(&m, now),
            DreamVerdict::Keep,
            "heavily recalled, never corroborated — it stays a candidate"
        );
        // …and it is not archived either: it is plainly still useful.
        let mut cold_but_hot = m.clone();
        cold_but_hot.created_at = now - (DREAM_FORGET_AGE_DAYS + 10) * 86_400;
        assert_eq!(dream_verdict(&cold_but_hot, now), DreamVerdict::Keep);
    }

    /// An unresolved conflict blocks promotion however well-supported the claim
    /// is: promoting into a contest asserts one side of an open question.
    #[test]
    fn dream_never_promotes_a_contested_or_contradicted_candidate() {
        let now = 10_000 * 86_400;
        let mut m = candidate(0, 5, now);
        m.record_evidence("s-1", EvidenceRelation::Supports, "a", now);
        m.record_evidence("s-2", EvidenceRelation::Supports, "b", now);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Promote);

        // A contradiction from a third occasion stops it, even before anything
        // marks the belief contested.
        let mut contradicted = m.clone();
        contradicted.record_evidence("s-3", EvidenceRelation::Contradicts, "no", now);
        assert_eq!(dream_verdict(&contradicted, now), DreamVerdict::Keep);

        let mut contested = m.clone();
        contested.contest(now);
        assert_eq!(dream_verdict(&contested, now), DreamVerdict::Keep);

        let mut superseded = m.clone();
        superseded.supersede("mem-new", now);
        assert_eq!(dream_verdict(&superseded, now), DreamVerdict::Keep);
    }

    /// The asymmetry: a refutation nobody rules on retires the candidate well
    /// before the thirty-day cold rule would, and *regardless* of how warm
    /// retrieval keeps it — a claim the user has spoken against cannot earn its
    /// recall slot back by being relevant.
    #[test]
    fn dream_archives_a_candidate_left_refuted() {
        let now = 10_000 * 86_400;
        let stale = now - (DREAM_REFUTED_FORGET_AGE_DAYS + 1) * 86_400;

        // Warm (recalled yesterday) and young — neither saves it.
        let mut contradicted = candidate(20, 5, now);
        contradicted.record_evidence("s-1", EvidenceRelation::Contradicts, "no", stale);
        assert_eq!(dream_verdict(&contradicted, now), DreamVerdict::Archive);

        // `contest`/`supersede` write no evidence entry; the edit clock stands in.
        let mut superseded = candidate(20, 5, now);
        superseded.supersede("mem-new", stale);
        assert_eq!(dream_verdict(&superseded, now), DreamVerdict::Archive);
    }

    /// A refutation is a question for the operator first. It only becomes a
    /// retirement once a week has passed with nobody answering it.
    #[test]
    fn dream_leaves_a_fresh_refutation_for_the_operator() {
        let now = 10_000 * 86_400;
        let mut m = candidate(20, 5, now);
        m.record_evidence(
            "s-1",
            EvidenceRelation::Contradicts,
            "no",
            now - (DREAM_REFUTED_FORGET_AGE_DAYS - 1) * 86_400,
        );
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Keep);
    }

    /// A confirmation *after* the conflict is the ruling the window was waiting
    /// for — the candidate goes back to being judged on support alone.
    #[test]
    fn dream_does_not_retire_a_refutation_a_confirmation_has_settled() {
        let now = 10_000 * 86_400;
        let mut m = candidate(0, 5, now);
        m.record_evidence(
            "s-1",
            EvidenceRelation::Contradicts,
            "no",
            now - (DREAM_REFUTED_FORGET_AGE_DAYS + 1) * 86_400,
        );
        m.last_confirmed_at = Some(now - 86_400);
        assert_eq!(m.unresolved_refutation_at(), None);
        // Still not promoted — an outstanding contradiction count blocks that —
        // but no longer on the refuted clock either.
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Keep);
    }

    /// The preview ordering has to put what is closest to promotion on top, or
    /// `komo dream` stops being a useful triage queue.
    /// Golden case: being retrieved a lot must never make a memory *true*.
    ///
    /// This is the self-confirming loop the truth/utility split exists to break.
    /// Promotion once read the recall counters, so a wrong memory that happened
    /// to be relevant to a recurring question would keep being injected, keep
    /// scoring, and promote itself on the strength of nothing but its own
    /// retrieval. The thing retrieved is not the thing tested.
    ///
    /// Ranking is checked below; this checks the *gate*, which is what actually
    /// decides promotion.
    #[test]
    fn retrieval_alone_never_makes_a_memory_supported() {
        let now = 1_800_000_000;

        // Recalled relentlessly, corroborated by nobody.
        let mut popular = candidate(500, 10, now);
        popular.last_used_at = Some(now);
        assert!(
            !popular.is_supported(),
            "500 recalls and no evidence must not clear the promotion bar"
        );

        // One independent occasion is still not enough — the bar is two.
        let mut once = candidate(0, 10, now);
        once.record_evidence("session-a", EvidenceRelation::Supports, "said it once", now);
        assert!(!once.is_supported(), "one occasion is not corroboration");

        // A second, independent occasion clears it.
        let mut twice = once.clone();
        twice.record_evidence(
            "session-b",
            EvidenceRelation::Supports,
            "said it again",
            now,
        );
        assert!(
            twice.is_supported(),
            "two independent occasions corroborate"
        );

        // ...but the same session saying it twice is one occasion, however
        // talkative it is. Otherwise one conversation corroborates itself.
        let mut echo = once.clone();
        echo.record_evidence("session-a", EvidenceRelation::Supports, "and again", now);
        assert!(
            !echo.is_supported(),
            "one session cannot be two independent occasions"
        );
    }

    #[test]
    fn dream_score_ranks_supported_candidates_above_merely_recalled_ones() {
        let now = 10_000 * 86_400;
        let mut supported = candidate(0, 5, now);
        supported.record_evidence("s-1", EvidenceRelation::Supports, "a", now);
        let recalled = candidate(5, 5, now);
        assert!(
            dream_score(&supported, now) > dream_score(&recalled, now),
            "one real corroboration outranks five retrievals"
        );

        let mut contradicted = supported.clone();
        contradicted.record_evidence("s-2", EvidenceRelation::Contradicts, "no", now);
        assert!(
            dream_score(&contradicted, now) < dream_score(&supported, now),
            "a contradiction pushes a candidate down the queue"
        );
    }

    #[test]
    fn governance_transitions_set_status_confidence_and_updated_at() {
        let now = 9_000;
        let mut m = candidate(0, 1, 8_000);
        m.promote(now);
        assert_eq!(m.status, MemoryStatus::Active);
        assert_eq!(m.confidence, MemoryConfidence::Confirmed);
        assert_eq!(m.updated_at, now);

        let mut m = candidate(0, 1, 8_000);
        m.reject(now);
        assert_eq!(m.status, MemoryStatus::Rejected);

        let mut m = candidate(0, 1, 8_000);
        m.pin(now);
        assert!(m.pinned);
        assert_eq!(m.status, MemoryStatus::Active);
        assert_eq!(
            m.confidence,
            MemoryConfidence::Confirmed,
            "pin raises extracted to confirmed so it can surface in L1"
        );
        // Pinning must never *lower* confidence.
        let mut written = candidate(0, 1, 8_000);
        written.confidence = MemoryConfidence::UserWritten;
        written.pin(now);
        assert_eq!(written.confidence, MemoryConfidence::UserWritten);
    }

    #[test]
    fn dream_keeps_under_recalled_candidate() {
        let now = 10_000 * 86_400;
        // Two recalls — below the threshold of three — and still young.
        let m = candidate(2, 5, now);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Keep);
    }

    #[test]
    fn dream_archives_old_never_recalled_candidate() {
        let now = 10_000 * 86_400;
        let m = candidate(0, DREAM_FORGET_AGE_DAYS + 1, now);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Archive);
    }

    #[test]
    fn dream_keeps_young_never_recalled_candidate() {
        let now = 10_000 * 86_400;
        // Never recalled but still within the forget window — give it time.
        let m = candidate(0, DREAM_FORGET_AGE_DAYS - 1, now);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Keep);
    }

    #[test]
    fn dream_archives_weakly_recalled_gone_cold() {
        let now = 10_000 * 86_400;
        // Two recalls long ago, then silence: below the promote bar, and cold
        // (last used outside the forget window) — this is the leak the old
        // `recall_count == 0` archive check let linger forever. Now retired.
        let mut m = candidate(2, DREAM_FORGET_AGE_DAYS + 10, now);
        m.last_used_at = Some(now - (DREAM_FORGET_AGE_DAYS + 5) * 86_400);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Archive);
    }

    #[test]
    fn dream_keeps_weakly_recalled_still_warm() {
        let now = 10_000 * 86_400;
        // Old, only two recalls — but recalled recently, so it is still earning
        // its keep and might yet reach the promote bar. Not archived.
        let mut m = candidate(2, DREAM_FORGET_AGE_DAYS + 10, now);
        m.last_used_at = Some(now - 5 * 86_400);
        assert_eq!(dream_verdict(&m, now), DreamVerdict::Keep);
    }

    #[test]
    fn dream_never_touches_active_memories() {
        let now = 10_000 * 86_400;
        // An active memory recalled a lot is still left alone (no auto-archive of
        // user-kept memories), and an old unused active is not archived either.
        let mut hot = candidate(99, 1, now);
        hot.status = MemoryStatus::Active;
        assert_eq!(dream_verdict(&hot, now), DreamVerdict::Keep);
        let mut cold = candidate(0, DREAM_FORGET_AGE_DAYS + 100, now);
        cold.status = MemoryStatus::Active;
        assert_eq!(dream_verdict(&cold, now), DreamVerdict::Keep);
    }

    // ---- belief state and evidence ----

    /// Independence is per session: restating a fact three times in one
    /// conversation is one observation, not three.
    #[test]
    fn evidence_from_the_same_session_counts_once() {
        let now = 1_000;
        let mut m = Memory::new(MemoryKind::Preference, "user prefers rebase");
        assert!(m.record_evidence("s-1", EvidenceRelation::Supports, "I rebase", now));
        assert!(!m.record_evidence("s-1", EvidenceRelation::Supports, "always rebase", now + 5));
        assert_eq!(m.support_count, 1);
        assert_eq!(m.evidence.len(), 1);

        // A different session is a genuinely independent observation.
        assert!(m.record_evidence("s-2", EvidenceRelation::Supports, "rebase again", now + 10));
        assert_eq!(m.support_count, 2);
    }

    #[test]
    fn contradicting_evidence_counts_separately() {
        let now = 1_000;
        let mut m = Memory::new(MemoryKind::Preference, "user prefers rebase");
        m.record_evidence("s-1", EvidenceRelation::Supports, "I rebase", now);
        m.record_evidence("s-2", EvidenceRelation::Contradicts, "merge now", now);
        assert_eq!(m.support_count, 1);
        assert_eq!(m.contradiction_count, 1);
    }

    /// The list is bounded while the counts keep rising, so a long-lived memory
    /// cannot grow its row without limit.
    #[test]
    fn the_evidence_list_is_capped_but_the_count_is_not() {
        let now = 1_000;
        let mut m = Memory::new(MemoryKind::Fact, "x");
        for i in 0..(EVIDENCE_CAP + 3) {
            m.record_evidence(
                &format!("s-{i}"),
                EvidenceRelation::Supports,
                "said so",
                now + i as i64,
            );
        }
        assert_eq!(m.evidence.len(), EVIDENCE_CAP);
        assert_eq!(m.support_count, (EVIDENCE_CAP + 3) as i64);
        // The most recent survive — they are the ones that speak to "still true".
        assert_eq!(m.evidence.last().unwrap().session, "s-7");
    }

    #[test]
    fn an_excerpt_is_truncated_by_characters_not_bytes() {
        let now = 1_000;
        let mut m = Memory::new(MemoryKind::Fact, "x");
        let long = "语".repeat(EVIDENCE_EXCERPT_MAX + 50);
        m.record_evidence("s-1", EvidenceRelation::Supports, &long, now);
        assert_eq!(
            m.evidence[0].excerpt.chars().count(),
            EVIDENCE_EXCERPT_MAX,
            "counted in chars, so CJK is not cut to a third"
        );
    }

    /// The core of the belief axis: a contested memory stays retrievable but
    /// stops being assertable.
    #[test]
    fn contested_and_superseded_memories_are_not_injectable() {
        let now = 1_000;
        let mut m = Memory::new(MemoryKind::Preference, "user writes Python");
        assert!(m.is_injectable());

        m.contest(now);
        assert!(!m.is_injectable());
        assert_eq!(m.belief, BeliefState::Contested);
        // Governance is untouched — contesting is not a triage decision.
        assert_eq!(m.status, MemoryStatus::Active);

        let mut m = Memory::new(MemoryKind::Preference, "user writes Python");
        m.supersede("mem-rust", now);
        assert!(!m.is_injectable());
        assert_eq!(m.superseded_by, "mem-rust");
    }

    /// A pinned memory is asserted every single turn, so a contradiction has to
    /// silence it immediately — without anyone unpinning it first.
    #[test]
    fn a_contested_pinned_memory_leaves_the_l1_profile() {
        let ctx = MemoryContext::local("s1");
        let now = 1_000;
        let mut m = pinnable_memory();
        assert!(m.is_pinnable(&ctx, now));
        m.contest(now);
        assert!(!m.is_pinnable(&ctx, now));
        assert_eq!(select_pinned(&[m], &ctx, now).len(), 0);
    }

    /// Retrieval is deliberately belief-agnostic: the scoring layer must keep
    /// returning a contested memory so an explicit search can surface it. The
    /// injection filter lives with the injector.
    #[test]
    fn recall_scoring_still_returns_a_contested_memory() {
        let ctx = MemoryContext::local("s1");
        let now = 1_000;
        let mut m = Memory::new(MemoryKind::Fact, "the rust toolchain is pinned");
        m.contest(now);
        let query = RecallQuery::lexical("rust toolchain");
        assert_eq!(
            select_recall(&[m], &ctx, &query, 5, now).len(),
            1,
            "the query layer finds it; only injection refuses it"
        );
    }

    /// An operator promote is an explicit ruling: it confirms the memory and
    /// clears whatever conflict was outstanding.
    #[test]
    fn promote_confirms_and_resolves_a_contest() {
        let now = 9_000;
        let mut m = candidate(0, 1, 8_000);
        m.contest(now - 100);
        m.superseded_by = "mem-other".into();
        m.promote(now);
        assert_eq!(m.belief, BeliefState::Current);
        assert!(m.superseded_by.is_empty());
        assert_eq!(m.last_confirmed_at, Some(now));
        assert!(m.is_injectable());
    }

    #[test]
    fn belief_state_round_trips_and_unknown_reads_as_current() {
        for state in [
            BeliefState::Current,
            BeliefState::Contested,
            BeliefState::Superseded,
        ] {
            assert_eq!(parse_belief_state(state.as_str()), state);
        }
        // Every row written before the column existed.
        assert_eq!(parse_belief_state(""), BeliefState::Current);
        assert_eq!(parse_belief_state("nonsense"), BeliefState::Current);
    }

    #[test]
    fn pinnable_excludes_out_of_scope() {
        let ctx = MemoryContext::new("s1", Some(&ChannelPeer::new("telegram", "42")));
        let mut other_channel = pinnable_memory();
        other_channel.scope = MemoryScope::Channel {
            platform: "feishu".into(),
            chat_id: "oc_x".into(),
        };
        assert!(!other_channel.is_pinnable(&ctx, 1_000));
    }
}
