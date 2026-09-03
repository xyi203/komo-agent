use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use toasty_driver_turso::Turso;
use tracing::info;

use crate::persistence::{
    DEFAULT_POOL_SIZE, drop_retired_columns, ensure_columns, ensure_table, prepare_turso_path,
    session_event_store::SessionEventStore, turso_marker_path, with_write_retry,
};

use komo_core::domain::{
    briefing::BriefingMarkRepository,
    context::SessionOrigin,
    home::HomeRepository,
    inbox::{InboundOrigin, InboxClaim, InboxRepository},
    message::Message,
    pairing::{
        APPROVE_LOCKOUT_SECS, APPROVE_MAX_FAILURES, ApproveOutcome, PAIRING_CODE_TTL_SECS,
        PairingRepository, PairingRequest, PairingStatus, parse_pairing_status, verify_code,
    },
    reminder::{Reminder, ReminderRepository, ReminderStatus, parse_reminder_status},
    repository::{MessageRepository, SessionEventRepository, SessionRepository},
    run::{INTERRUPTED_ERROR, MemoryUse, Run, RunRepository, RunStatus, RunStep, parse_run_status},
    run_projection::{ProjectedRun, RunProjectionStore, project_runs},
    session::{ChannelPeer, Session},
    session_event::{
        SESSION_EVENT_VERSION, SessionEvent, SessionEventKind, SessionHeader, SurfaceProjection,
    },
    skill::Skill,
    todo::{SessionTodoRepository, TodoItem},
};

// ── toasty models (infra-internal) ───────────────────────────────────────────

#[derive(Debug, toasty::Model)]
struct SessionRecord {
    #[key]
    id: String,
    created_at: i64,
    /// Immutable workspace identity chosen when the session is created.
    workspace: String,

    /// Operator-set display name (empty = untitled). Added additively via
    /// `SESSION_COLUMNS`; set through `SessionRepository::set_title`.
    title: String,

    /// Lifecycle status (`active` / `archive` / `deleted`). Additive column;
    /// set through `SessionRepository::set_status`. The list view hides
    /// `deleted`.
    status: String,

    /// Per-session model override (empty = the gateway's configured model) and
    /// reasoning effort (empty = the provider default). Additive columns; set
    /// through `SessionRepository::set_model`. Unlike `workspace` these are not
    /// creation-locked — a conversation may switch models mid-thread.
    model: String,
    effort: String,

    /// The correspondent this conversation answers, split into the chat
    /// platform and that platform's own id for the peer. Both empty for every
    /// local surface and for komo's own sessions. Additive columns; a channel
    /// finds its session by this pair (`find_by_peer`), which is what replaced
    /// deriving the session id from the address.
    channel_platform: String,
    channel_peer_id: String,

    /// What drives this conversation (`user` / `cron` / `briefing` /
    /// `delegate`). Additive column; decides titling, list visibility and
    /// learning eligibility. Was encoded in the id as a prefix.
    origin: String,
}

#[derive(Debug, toasty::Model)]
struct SkillRecord {
    #[key]
    name: String,
    description: String,
    instructions: String,
    protected: bool,
}

#[derive(Debug, toasty::Model)]
struct ReminderRecord {
    #[key]
    id: String,
    message: String,
    run_at: i64,
    status: String,   // "pending" | "fired" | "missed" | "cancelled"
    schedule: String, // reserved for v2 cron expressions; always "" in v1
    created_at: i64,
}

/// Session-scoped working todo list (`domain/todo.rs`). One row per session;
/// `items` is the JSON-serialized `Vec<TodoItem>`. Disposable working state —
/// cleared on `/new` rotate.
#[derive(Debug, toasty::Model)]
struct SessionTodoRecord {
    #[key]
    session_id: String,
    items: String, // JSON array of TodoItem
    updated_at: i64,
}

#[derive(Debug, toasty::Model)]
struct PairingRecord {
    /// One row per sender: `{platform}:{sender_id}`.
    #[key]
    id: String,
    platform: String,
    sender_id: String,
    chat_id: String,
    code_hash: String, // salted SHA-256 of the code; plaintext never stored
    salt: String,
    status: String, // "pending" | "approved"
    created_at: i64,
}

/// Failure-lockout counter for the `komo pair approve` path. A singleton row
/// (`id = "approve"`); mirrors hermes' per-platform approve lockout.
#[derive(Debug, toasty::Model)]
struct LockoutRecord {
    #[key]
    id: String,
    failed_count: i64,
    locked_until: i64,
}

/// Generic key/value settings. One row per setting (`id` is the key); the home
/// channel set via `/sethome` lives under `id = "home_chat"`.
#[derive(Debug, toasty::Model)]
struct SettingRecord {
    #[key]
    id: String,
    value: String,
}

/// One agent turn in the run ledger (`domain/run.rs`, roadmap §7). `ended_at`
/// uses 0 as the "still running" sentinel (same convention as other optional
/// i64s here).
#[derive(Debug, toasty::Model)]
struct RunRecord {
    #[key]
    id: String,
    session_id: String,
    input: String,
    plan: String,
    status: String, // "running" | "done" | "failed"
    final_output: String,
    error: String,
    recoverable: bool,
    started_at: i64,
    ended_at: i64,

    /// Tokens the turn's model round-trips spent. Additive columns (see
    /// `RUN_COLUMNS`); 0 = unknown, which is what a pre-column row reads as.
    tokens_in: i64,
    tokens_out: i64,
    /// Cache-served part of `tokens_in`. Additive column; 0 = unknown.
    tokens_cached: i64,

    /// The memories that reached this run's prompt, as `RecalledMemories` JSON
    /// (`""` = none, which is also what a pre-column row reads as). Additive.
    memories: String,

    /// Run id this run continued from (journal resume). Additive column;
    /// empty = none, same convention as `structured`.
    resumed_from: String,

    /// The learning pass has consumed this run. Additive column; a pre-column
    /// row reads as `false`, which offers it to the pass once — the extractor's
    /// own dedup makes a re-read harmless.
    learned: bool,

    /// Serialized `OutcomeAssessment`. Additive column; empty = never assessed.
    outcome: String,
}

/// One tool invocation within a run. `run_id` indexes back to [`RunRecord`];
/// `seq` orders steps within a run.
#[derive(Debug, toasty::Model)]
struct RunStepRecord {
    // UUIDv7 string key: MVCC rejects AUTOINCREMENT.
    // Assigned at insert (`RunRepository::append_step`).
    #[key]
    id: String,

    #[index]
    run_id: String,

    seq: i64,
    tool_name: String,
    args: String,
    result: String,
    error: String,
    ok: bool,

    /// `!ok` but the call may still have taken effect (`domain::run::RunStep`).
    /// Additive column.
    uncertain: bool,

    started_at: i64,
    ended_at: i64,

    /// Measured call duration in milliseconds. Additive column (see
    /// `STEP_COLUMNS`); `started_at`/`ended_at` are whole seconds and can't
    /// express a sub-second call.
    elapsed_ms: i64,

    /// `ToolOutput::structured` as JSON text; empty string = none (which is also
    /// what a row written before the column reads as). Additive column.
    structured: String,

    /// Newline-separated paths of stored full outputs; empty = none. Additive
    /// column. A list, not JSON: the entries are paths, and `split('\n')` on the
    /// read side beats a nested parse.
    output_paths: String,
}

/// One inbound message the gateway has seen (`domain/inbox.rs`). The key is
/// `<platform>:<message_id>` rather than the UUIDv7 used everywhere else in
/// this file: dedupe wants the collision, and the primary key is what makes
/// "already handled" atomic instead of a check the next delivery can race.
#[derive(Debug, toasty::Model)]
struct InboxRecord {
    #[key]
    id: String,

    session_id: String,
    /// The message body, kept so a claimed-but-uncompleted row can be
    /// re-delivered after a crash. Nothing reads it back yet — see
    /// `InboxRepository::claim`.
    text: String,
    status: String, // "claimed" | "completed"
    claimed_at: i64,
    /// 0 until `complete` runs.
    completed_at: i64,
}

/// The exact DDL `push_schema` emits for [`InboxRecord`], for the same reason
/// [`JOURNAL_TABLE_DDL`] exists. Byte-parity is locked by
/// `inbox_table_ddl_matches_push_schema`.
const INBOX_TABLE: &str = "inbox_records";
const INBOX_TABLE_DDL: &[&str] = &[
    "CREATE TABLE \"inbox_records\" (\"id\" TEXT NOT NULL, \"session_id\" TEXT NOT NULL, \
     \"text\" TEXT NOT NULL, \"status\" TEXT NOT NULL, \"claimed_at\" BIGINT NOT NULL, \
     \"completed_at\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
];

/// One `(memory, run)` link — the reverse index behind `runs_using_memory`.
/// Written when the run finishes, because that is when the turn's injected
/// memories are known.
#[derive(Debug, toasty::Model)]
struct RunMemoryRecord {
    #[key]
    id: String,

    #[index]
    memory_id: String,

    run_id: String,
    session_id: String,
    pinned: bool,
    started_at: i64,
}

/// DDL for [`RunMemoryRecord`], for a state.db that predates it. Byte-parity is
/// locked by `run_memory_table_ddl_matches_push_schema`.
const RUN_MEMORY_TABLE: &str = "run_memory_records";
const RUN_MEMORY_TABLE_DDL: &[&str] = &[
    "CREATE TABLE \"run_memory_records\" (\"id\" TEXT NOT NULL, \"memory_id\" TEXT NOT NULL, \
     \"run_id\" TEXT NOT NULL, \"session_id\" TEXT NOT NULL, \"pinned\" BOOLEAN NOT NULL, \
     \"started_at\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
    "CREATE INDEX \"index_run_memory_records_by_memory_id\" ON \"run_memory_records\" (\"memory_id\")",
];

const INBOX_STATUS_CLAIMED: &str = "claimed";
const INBOX_STATUS_COMPLETED: &str = "completed";

/// Setting key for the runtime home channel (`/sethome`).
const HOME_SETTING_KEY: &str = "home_chat";
/// Setting key for the briefing watermark (local date last handled).
const BRIEFING_MARK_KEY: &str = "briefing_last_handled";

/// Settings key holding how far one session's run projection is committed.
/// Per session rather than one global cursor: the fold is per session, and a
/// log that has not changed must be skippable on its own.
fn run_projection_key(session_id: &str) -> String {
    format!("projection:runs:{session_id}")
}

/// The prune tombstone: runs that started before this are deliberately gone.
///
/// Projection control state, not a session event — `run prune` is an operator
/// deleting an index, and broadcasting it into N session logs would make the
/// authoritative record of what happened depend on what someone later chose
/// not to keep. One fence rather than a row per run because prune's own unit is
/// a cutoff: `started_at < cutoff` is exactly the set it deleted.
const RUN_PRUNED_BEFORE_KEY: &str = "projection:runs:pruned_before";

// ── Db ───────────────────────────────────────────────────────────────────────

/// The disposable session/run/pairing store, over the Turso engine with a
/// per-operation connection pool: `inner` is a plain `Arc<toasty::Db>` (no outer
/// `Mutex`), so every method checks out a pooled `Connection` and independent
/// reads/writes run concurrently. Concurrently-written tables (the run ledger)
/// use [`with_write_retry`] for MVCC commit conflicts.
pub struct Db {
    inner: Arc<toasty::Db>,
    /// The session event logs — files rather than rows, one directory per
    /// session. Session *metadata* is still a row here: it is updated (title,
    /// status, model), and a log is the wrong shape for a value that changes.
    events: SessionEventStore,
}

impl Db {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        // `url` is `turso:<path>` (or `turso::memory:`). state.db is disposable
        // (sessions, messages, runs, pairings, settings): a legacy SQLite file
        // can't be reopened under Turso's MVCC mode, so `prepare_turso_path`
        // stages it aside to a `.sqlite-backup` (kept as a safety net) and we
        // start fresh. Durable personal data lives in memory.db / kanban.db,
        // which migrate their rows instead of resetting.
        let (path, is_new) = prepare_turso_path(url)?;

        // Additive in-place migration for an EXISTING db: `push_schema` only
        // runs for new files, so a column added to a model after the file was
        // created would otherwise be missing and every query on that table
        // would fail — turning "disposable, delete to reset" into "broken on
        // upgrade until the operator remembers to delete". Same mechanism as
        // memory.db's ensure_columns; when adding a column to a model here,
        // extend this list (NOT NULL with a DEFAULT, or nullable) — a new
        // *table* still needs the delete-to-reset.
        if !is_new && let Some(p) = &path {
            const SESSION_COLUMNS: &[(&str, &str)] = &[
                ("title", "\"title\" text NOT NULL DEFAULT ''"),
                ("status", "\"status\" text NOT NULL DEFAULT 'active'"),
                (
                    "workspace",
                    "\"workspace\" text NOT NULL DEFAULT '__default__'",
                ),
                ("model", "\"model\" text NOT NULL DEFAULT ''"),
                ("effort", "\"effort\" text NOT NULL DEFAULT ''"),
                (
                    "channel_platform",
                    "\"channel_platform\" text NOT NULL DEFAULT ''",
                ),
                (
                    "channel_peer_id",
                    "\"channel_peer_id\" text NOT NULL DEFAULT ''",
                ),
                ("origin", "\"origin\" text NOT NULL DEFAULT 'user'"),
            ];
            ensure_columns(p, "session_records", SESSION_COLUMNS).await?;
            // Columns this komo no longer models. `reviewed_through` was the
            // review sweep's per-session watermark until the watermark moved to
            // `Run.learned` — but dropping it from the model left it in every
            // file whose push_schema ran while it existed, `NOT NULL` and with
            // no default, so creating any new session failed the constraint.
            // Same repair as memory.db's `recall_query_hashes`.
            const SESSION_RETIRED: &[&str] = &["reviewed_through"];
            drop_retired_columns(p, "session_records", SESSION_RETIRED).await?;
            const RUN_COLUMNS: &[(&str, &str)] = &[
                (
                    "recoverable",
                    "\"recoverable\" boolean NOT NULL DEFAULT false",
                ),
                ("tokens_in", "\"tokens_in\" integer NOT NULL DEFAULT 0"),
                ("tokens_out", "\"tokens_out\" integer NOT NULL DEFAULT 0"),
                (
                    "tokens_cached",
                    "\"tokens_cached\" integer NOT NULL DEFAULT 0",
                ),
                ("resumed_from", "\"resumed_from\" text NOT NULL DEFAULT ''"),
                ("memories", "\"memories\" text NOT NULL DEFAULT ''"),
                // `DEFAULT true` backfills history, and only history: every run
                // the learning pass could act on is inserted with an explicit
                // `learned: false`, so this default is reached exactly once per
                // row that predates the column. Defaulting to `false` instead
                // would offer the entire existing ledger to the pass on the
                // upgrade that adds it — thousands of old turns re-extracted at
                // once, each an "independent occasion" to the consolidator.
                ("learned", "\"learned\" boolean NOT NULL DEFAULT true"),
                ("outcome", "\"outcome\" text NOT NULL DEFAULT ''"),
            ];
            ensure_columns(p, "run_records", RUN_COLUMNS).await?;
            const STEP_COLUMNS: &[(&str, &str)] = &[
                ("elapsed_ms", "\"elapsed_ms\" integer NOT NULL DEFAULT 0"),
                ("uncertain", "\"uncertain\" boolean NOT NULL DEFAULT false"),
                ("structured", "\"structured\" text NOT NULL DEFAULT ''"),
                ("output_paths", "\"output_paths\" text NOT NULL DEFAULT ''"),
            ];
            ensure_columns(p, "run_step_records", STEP_COLUMNS).await?;
            ensure_table(p, INBOX_TABLE, INBOX_TABLE_DDL).await?;
            ensure_table(p, RUN_MEMORY_TABLE, RUN_MEMORY_TABLE_DDL).await?;
        }

        // MVCC concurrent-writes on (UUID keys throughout, so no AUTOINCREMENT).
        let driver = match &path {
            Some(p) => Turso::file(p).concurrent_writes(),
            None => Turso::in_memory().concurrent_writes(),
        };
        let db = toasty::Db::builder()
            .models(toasty::models!(
                SessionRecord,
                SkillRecord,
                ReminderRecord,
                SessionTodoRecord,
                PairingRecord,
                LockoutRecord,
                SettingRecord,
                RunRecord,
                RunStepRecord,
                InboxRecord,
                RunMemoryRecord
            ))
            .max_pool_size(DEFAULT_POOL_SIZE)
            .build(driver)
            .await?;

        if is_new {
            db.push_schema().await?;
            // Mark the file Turso-native so a future run never mistakes it for a
            // legacy SQLite file to stage aside.
            if let Some(p) = &path {
                std::fs::write(turso_marker_path(p), b"turso-native\n").ok();
            }
        }

        // Transcripts sit beside state.db, so `KOMO_HOME` carries them without
        // this needing to know about it. An in-memory db (tests) gets a
        // directory of its own per connection, which is what keeps two tests
        // from reading each other's transcripts.
        let transcript_home = match &path {
            Some(p) => p
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
            None => std::env::temp_dir().join(format!("komo-mem-{}", uuid::Uuid::now_v7())),
        };
        let events = SessionEventStore::new(&transcript_home);

        let this = Self {
            inner: Arc::new(db),
            events,
        };
        Ok(this)
    }
}

// ── legacy skills (read-only) ─────────────────────────────────────────────────

impl Db {
    /// The skills a pre-filesystem komo accumulated in `komo.db` (the
    /// reviewer used to write here; the runtime never read it). Read-only:
    /// skills now live as files under `~/.komo/skills` (`infra/skills.rs`),
    /// and this backs the one-time candidate import at wiring time. The
    /// `SkillRecord` table stays in the schema only so old dbs remain readable.
    pub async fn export_legacy_skills(&self) -> anyhow::Result<Vec<Skill>> {
        let mut conn = self.inner.connection().await?;
        let mut rows = toasty::query!(SkillRecord).exec(&mut conn).await?;
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(rows.into_iter().map(skill_from_record).collect())
    }
}

// ── SessionRepository ─────────────────────────────────────────────────────────

#[async_trait]
impl SessionRepository for Db {
    async fn find(&self, id: &str) -> anyhow::Result<Option<Session>> {
        let mut conn = self.inner.connection().await?;
        let Ok(record) = SessionRecord::get_by_id(&mut conn, id).await else {
            return Ok(None);
        };
        let messages = self.events.messages(id).await?;
        Ok(Some(session_from_record(record, messages)))
    }

    async fn find_windowed(&self, id: &str, limit: usize) -> anyhow::Result<Option<Session>> {
        // `limit == 0` means "no window" — fall back to the full load.
        if limit == 0 {
            return SessionRepository::find(self, id).await;
        }
        let mut conn = self.inner.connection().await?;
        let Ok(record) = SessionRecord::get_by_id(&mut conn, id).await else {
            return Ok(None);
        };
        // Derived from the event log's conversation surface. A window cannot be
        // taken from the file's tail the way a message log's could: a compaction
        // near the end can replace a range that began far earlier, so the last N
        // events do not determine the last N messages.
        let messages = self.events.windowed(id, limit).await?;
        Ok(Some(session_from_record(record, messages)))
    }

    async fn find_by_peer(&self, channel: &ChannelPeer) -> anyhow::Result<Option<Session>> {
        // A session with no correspondent stores an empty address, so a query
        // for one would match every local conversation and hand them each
        // other's turns. An empty address is not an address.
        if channel.platform.is_empty() || channel.peer_id.is_empty() {
            return Ok(None);
        }
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(SessionRecord).exec(&mut conn).await?;
        // Metadata only — a channel asks this on every inbound message just to
        // learn which conversation it is, and loading a transcript to answer
        // that would pay a turn's read before the turn starts.
        let found = rows
            .into_iter()
            .filter(|r| {
                r.channel_platform == channel.platform && r.channel_peer_id == channel.peer_id
            })
            // Newest wins. There should only ever be one, but a session the
            // operator deleted and a channel then recreated would leave two,
            // and answering with the stale one would strand the conversation.
            .max_by_key(|r| r.created_at);
        Ok(found.map(|record| session_from_record(record, Vec::new())))
    }

    async fn list(&self) -> anyhow::Result<Vec<Session>> {
        let mut conn = self.inner.connection().await?;
        let mut rows = toasty::query!(SessionRecord).exec(&mut conn).await?;
        rows.sort_by_key(|r| r.created_at);

        let mut sessions = Vec::with_capacity(rows.len());
        for record in rows {
            let messages = self.events.messages(&record.id).await?;
            sessions.push(session_from_record(record, messages));
        }
        Ok(sessions)
    }

    async fn save(&self, session: &Session) -> anyhow::Result<()> {
        // Idempotent create (save runs on every load-or-create). The old form
        // `let _ = create!(...)` swallowed *every* error — including an MVCC
        // write conflict, which left the session uncreated and the very next
        // MessageRepository::save failing with a phantom "session not found".
        // Pre-check existence, then insert only when absent; a conflict retries
        // and any real error surfaces.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            if SessionRecord::get_by_id(&mut conn, &session.id)
                .await
                .is_ok()
            {
                return Ok(());
            }
            let created = toasty::create!(SessionRecord {
                id: session.id.clone(),
                created_at: session.created_at,
                workspace: session.workspace.clone(),
                title: session.title.clone(),
                status: session.status.clone(),
                model: session.model.clone(),
                effort: session.effort.clone(),
                channel_platform: session
                    .channel
                    .as_ref()
                    .map(|c| c.platform.clone())
                    .unwrap_or_default(),
                channel_peer_id: session
                    .channel
                    .as_ref()
                    .map(|c| c.peer_id.clone())
                    .unwrap_or_default(),
                origin: session.origin.as_str().to_string(),
            })
            .exec(&mut conn)
            .await;
            if let Err(error) = created {
                // Concurrent create of the same brand-new id: the dispatcher
                // serializes chat turns per session, but the api channel calls
                // the handler directly, so two first-requests can race here.
                // If the winner committed, Turso reports a UNIQUE-constraint
                // violation (not a retryable busy/conflict) — the row exists,
                // which is all save() promises, so treat it as success. A
                // genuinely absent row means a real failure: propagate.
                if SessionRecord::get_by_id(&mut conn, &session.id)
                    .await
                    .is_ok()
                {
                    return Ok(());
                }
                return Err(error.into());
            }
            Ok(())
        })
        .await
    }

    async fn delete_empty_sessions(&self) -> anyhow::Result<usize> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(SessionRecord).exec(&mut conn).await?;

        let mut removed = 0usize;
        for record in rows {
            if self.events.messages(&record.id).await?.is_empty() {
                // No transcript file to remove — that is what empty means here.
                record.delete().exec(&mut conn).await?;
                removed += 1;
            }
        }

        if removed > 0 {
            info!(removed, "pruned empty sessions");
        }
        Ok(removed)
    }

    async fn rotate(&self, session_id: &str) -> anyhow::Result<Option<String>> {
        // `/new` used to move the transcript to an archived id and leave the
        // live id empty, because the chat's session id *was* its address and so
        // could not change. It is a UUID now and the address is a field, which
        // makes the whole move unnecessary: retiring a conversation is dropping
        // its correspondent. The session keeps its id, its log and its history;
        // it simply stops answering for that chat, and the next message from
        // the peer opens a new one (`find_by_peer` finds none).
        //
        // Nothing moves, so there is no half-done state to reason about — the
        // whole rotation is one row update.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let Ok(mut record) = SessionRecord::get_by_id(&mut conn, session_id).await else {
                return Ok(None);
            };
            // A session with no correspondent has nothing to be retired *from*:
            // a local surface rotates by minting a new id client-side.
            if record.channel_platform.is_empty() && record.channel_peer_id.is_empty() {
                return Ok(None);
            }
            record
                .update()
                .channel_platform(String::new())
                .channel_peer_id(String::new())
                .exec(&mut conn)
                .await?;
            Ok(Some(session_id.to_string()))
        })
        .await
    }

    async fn set_title(&self, session_id: &str, title: &str) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let Ok(mut record) = SessionRecord::get_by_id(&mut conn, session_id).await else {
                return Ok(()); // no such session — nothing to rename
            };
            record
                .update()
                .title(title.to_string())
                .exec(&mut conn)
                .await?;
            Ok(())
        })
        .await
    }

    async fn set_model(&self, session_id: &str, model: &str, effort: &str) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let Ok(mut record) = SessionRecord::get_by_id(&mut conn, session_id).await else {
                return Ok(()); // no such session
            };
            // Skip the write when nothing moved: the chat endpoint sends the
            // client's current selection on *every* turn, so an unchanged
            // selection would otherwise be a pointless write per turn.
            if record.model == model && record.effort == effort {
                return Ok(());
            }
            record
                .update()
                .model(model.to_string())
                .effort(effort.to_string())
                .exec(&mut conn)
                .await?;
            Ok(())
        })
        .await
    }

    async fn set_status(&self, session_id: &str, status: &str) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let Ok(mut record) = SessionRecord::get_by_id(&mut conn, session_id).await else {
                return Ok(()); // no such session
            };
            record
                .update()
                .status(status.to_string())
                .exec(&mut conn)
                .await?;
            Ok(())
        })
        .await
    }

    async fn delete_session(&self, session_id: &str) -> anyhow::Result<bool> {
        // Transactional cascade: remove the session's messages then the session
        // row itself, so a mid-sequence failure rolls back cleanly (mirrors
        // `rotate` / `RunRepository::prune`). Runs/todos keyed by this session
        // are left as harmless orphans — they never surface in the session list.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            let Ok(record) = SessionRecord::get_by_id(&mut tx, session_id).await else {
                return Ok(false);
            };
            record.delete().exec(&mut tx).await?;
            tx.commit().await?;
            Ok(true)
        })
        .await
    }
}

// ── MessageRepository ─────────────────────────────────────────────────────────

#[async_trait]
impl MessageRepository for Db {
    async fn list_by_session(&self, session_id: &str) -> anyhow::Result<Vec<Message>> {
        Ok(self.events.messages(session_id).await?)
    }
}

// ── SessionEventRepository ────────────────────────────────────────────────────

#[async_trait]
impl SessionEventRepository for Db {
    async fn append(
        &self,
        session_id: &str,
        kinds: Vec<SessionEventKind>,
    ) -> anyhow::Result<Vec<SessionEvent>> {
        // The header is only consulted when the log does not exist yet, so this
        // describes a session at its first event and never overwrites identity.
        let header = self.session_header(session_id).await;
        Ok(self.events.append(session_id, header, kinds).await?)
    }

    async fn durable_flush(&self, session_id: &str) -> anyhow::Result<()> {
        if let Some(log) = self.events.existing(session_id).await? {
            log.durable_flush().await?;
        }
        Ok(())
    }

    async fn events(&self, session_id: &str) -> anyhow::Result<Vec<SessionEvent>> {
        Ok(self.events.events(session_id).await?)
    }

    async fn events_from(&self, session_id: &str, seq: u64) -> anyhow::Result<Vec<SessionEvent>> {
        Ok(self.events.events_from(session_id, seq).await?)
    }

    async fn surface(&self, session_id: &str) -> anyhow::Result<Option<SurfaceProjection>> {
        Ok(self.events.surface(session_id).await?)
    }

    async fn turn_boundary(&self, session_id: &str) -> anyhow::Result<bool> {
        // Refresh the surface checkpoint here and nowhere else: a turn boundary
        // is where the log is quiet, and the next turn's history read is what
        // the checkpoint is for. Best-effort — the log already holds everything
        // it describes, so a failed write costs the next read a full fold.
        if let Err(error) = self.events.checkpoint_surface(session_id).await {
            info!(%error, session_id, "could not refresh the surface checkpoint (non-fatal)");
        }
        Ok(self.events.seal(session_id).await?)
    }

    async fn retain(&self, session_id: &str, keep_from: u64) -> anyhow::Result<Option<u64>> {
        Ok(self.events.retain(session_id, keep_from).await?)
    }
}

impl Db {
    /// Identity for a log being materialized: taken from the session row when
    /// there is one, so a session's origin and workspace reach its log without
    /// every caller having to carry them.
    async fn session_header(&self, session_id: &str) -> SessionHeader {
        let row = SessionRepository::find_windowed(self, session_id, 1)
            .await
            .ok()
            .flatten();
        SessionHeader {
            session_id: session_id.to_string(),
            origin: row
                .as_ref()
                .map(|s| s.origin.as_str().to_string())
                .unwrap_or_else(|| SessionOrigin::User.as_str().to_string()),
            workspace: row.as_ref().and_then(|s| {
                Some(s.workspace.clone())
                    .filter(|w| w != komo_core::domain::session::DEFAULT_WORKSPACE)
            }),
            created_at: time::OffsetDateTime::now_utc(),
            format_version: SESSION_EVENT_VERSION,
        }
    }
}

fn run_from_record(record: RunRecord) -> anyhow::Result<Run> {
    Ok(Run {
        id: record.id,
        session_id: record.session_id,
        input: record.input,
        plan: record.plan,
        status: parse_run_status(&record.status)?,
        final_output: record.final_output,
        error: record.error,
        recoverable: record.recoverable,
        started_at: record.started_at,
        ended_at: (record.ended_at != 0).then_some(record.ended_at),
        tokens_in: record.tokens_in,
        tokens_out: record.tokens_out,
        tokens_cached: record.tokens_cached,
        resumed_from: (!record.resumed_from.is_empty()).then_some(record.resumed_from),
        // A malformed cell reads as "none recorded": the ledger is an audit
        // record, and one bad row must not fail the read of a whole run.
        memories: serde_json::from_str(&record.memories).unwrap_or_default(),
        learned: record.learned,
        outcome: record.outcome,
    })
}

// ── ReminderRepository ────────────────────────────────────────────────────────

#[async_trait]
impl ReminderRepository for Db {
    async fn save(&self, reminder: &Reminder) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            toasty::create!(ReminderRecord {
                id: reminder.id.clone(),
                message: reminder.message.clone(),
                run_at: reminder.run_at,
                status: reminder.status.as_str().to_string(),
                schedule: reminder.schedule.clone(),
                created_at: reminder.created_at,
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn list_pending(&self) -> anyhow::Result<Vec<Reminder>> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(ReminderRecord).exec(&mut conn).await?;
        let pending = rows
            .into_iter()
            .filter(|r| r.status == "pending")
            .map(reminder_from_record)
            .collect();
        Ok(pending)
    }

    async fn set_status(&self, id: &str, status: ReminderStatus) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut record = ReminderRecord::get_by_id(&mut conn, id).await?;
            record
                .update()
                .status(status.as_str().to_string())
                .exec(&mut conn)
                .await?;
            Ok(())
        })
        .await
    }

    async fn reschedule(&self, id: &str, next_run_at: i64) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut record = ReminderRecord::get_by_id(&mut conn, id).await?;
            record.update().run_at(next_run_at).exec(&mut conn).await?;
            Ok(())
        })
        .await
    }
}

// ── SessionTodoRepository ─────────────────────────────────────────────────────

#[async_trait]
impl SessionTodoRepository for Db {
    async fn get(&self, session_id: &str) -> anyhow::Result<Vec<TodoItem>> {
        let mut conn = self.inner.connection().await?;
        match SessionTodoRecord::get_by_session_id(&mut conn, session_id).await {
            Ok(record) => Ok(serde_json::from_str(&record.items).unwrap_or_default()),
            Err(_) => Ok(Vec::new()),
        }
    }

    async fn set(&self, session_id: &str, items: &[TodoItem]) -> anyhow::Result<()> {
        let json = serde_json::to_string(items)?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            match SessionTodoRecord::get_by_session_id(&mut conn, session_id).await {
                Ok(mut record) => {
                    record
                        .update()
                        .items(json.clone())
                        .updated_at(now)
                        .exec(&mut conn)
                        .await?;
                }
                Err(_) => {
                    toasty::create!(SessionTodoRecord {
                        session_id: session_id.to_string(),
                        items: json.clone(),
                        updated_at: now,
                    })
                    .exec(&mut conn)
                    .await?;
                }
            }
            Ok(())
        })
        .await
    }

    async fn clear(&self, session_id: &str) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            if let Ok(record) = SessionTodoRecord::get_by_session_id(&mut conn, session_id).await {
                record.delete().exec(&mut conn).await?;
            }
            Ok(())
        })
        .await
    }
}

// ── PairingRepository ─────────────────────────────────────────────────────────

#[async_trait]
impl PairingRepository for Db {
    async fn upsert(&self, request: &PairingRequest) -> anyhow::Result<()> {
        // delete-if-exists + create: the delete is conditional on the row being
        // present, so a conflict-retry of the whole closure re-reads cleanly
        // (an already-deleted row is simply skipped on the next attempt).
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            if let Ok(record) = PairingRecord::get_by_id(&mut conn, &request.id).await {
                record.delete().exec(&mut conn).await?;
            }
            toasty::create!(PairingRecord {
                id: request.id.clone(),
                platform: request.platform.clone(),
                sender_id: request.sender_id.clone(),
                chat_id: request.chat_id.clone(),
                code_hash: request.code_hash.clone(),
                salt: request.salt.clone(),
                status: request.status.as_str().to_string(),
                created_at: request.created_at,
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn find(
        &self,
        platform: &str,
        sender_id: &str,
    ) -> anyhow::Result<Option<PairingRequest>> {
        let mut conn = self.inner.connection().await?;
        let id = format!("{platform}:{sender_id}");
        match PairingRecord::get_by_id(&mut conn, &id).await {
            Ok(record) => Ok(Some(pairing_from_record(record))),
            Err(_) => Ok(None),
        }
    }

    async fn count_active_pending(&self, platform: &str) -> anyhow::Result<usize> {
        let mut conn = self.inner.connection().await?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let rows = toasty::query!(PairingRecord).exec(&mut conn).await?;
        Ok(rows
            .iter()
            .filter(|r| {
                r.platform == platform
                    && r.status == "pending"
                    && now - r.created_at <= PAIRING_CODE_TTL_SECS
            })
            .count())
    }

    async fn approve_code(&self, code: &str) -> anyhow::Result<ApproveOutcome> {
        const LOCK_ID: &str = "approve";
        // Transactional: the code-match status flip and the failure-counter
        // update are two writes that must land together — a mid-sequence failure
        // used to leave "approved but counter not cleared" (or vice versa).
        // with_write_retry re-runs the whole closure on an MVCC conflict; the
        // rolled-back transaction makes that safe.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            let now = time::OffsetDateTime::now_utc().unix_timestamp();

            // Honor an active lockout before testing the code (read-only path:
            // returning here rolls the empty transaction back).
            let lock = LockoutRecord::get_by_id(&mut tx, LOCK_ID).await.ok();
            if let Some(l) = &lock
                && l.locked_until > now
            {
                return Ok(ApproveOutcome::Locked {
                    retry_after_secs: l.locked_until - now,
                });
            }

            let rows = toasty::query!(PairingRecord).exec(&mut tx).await?;
            let matched = rows.into_iter().find(|r| {
                r.status == "pending"
                    && now - r.created_at <= PAIRING_CODE_TTL_SECS
                    && verify_code(&r.salt, &r.code_hash, code)
            });

            let outcome = match matched {
                Some(mut record) => {
                    record
                        .update()
                        .status(PairingStatus::Approved.as_str().to_string())
                        .exec(&mut tx)
                        .await?;
                    // Success clears the failure counter.
                    if let Some(mut l) = lock {
                        l.update()
                            .failed_count(0)
                            .locked_until(0)
                            .exec(&mut tx)
                            .await?;
                    }
                    ApproveOutcome::Approved(pairing_from_record(record))
                }
                None => {
                    let mut count = lock.as_ref().map(|l| l.failed_count).unwrap_or(0) + 1;
                    let mut locked_until = 0;
                    if count >= APPROVE_MAX_FAILURES {
                        locked_until = now + APPROVE_LOCKOUT_SECS;
                        count = 0; // reset the counter once locked
                    }
                    match lock {
                        Some(mut l) => {
                            l.update()
                                .failed_count(count)
                                .locked_until(locked_until)
                                .exec(&mut tx)
                                .await?;
                        }
                        None => {
                            toasty::create!(LockoutRecord {
                                id: LOCK_ID.to_string(),
                                failed_count: count,
                                locked_until,
                            })
                            .exec(&mut tx)
                            .await?;
                        }
                    }
                    if locked_until > now {
                        ApproveOutcome::Locked {
                            retry_after_secs: locked_until - now,
                        }
                    } else {
                        ApproveOutcome::NotFound
                    }
                }
            };
            tx.commit().await?;
            Ok(outcome)
        })
        .await
    }

    async fn list(&self) -> anyhow::Result<Vec<PairingRequest>> {
        let mut conn = self.inner.connection().await?;
        let mut rows = toasty::query!(PairingRecord).exec(&mut conn).await?;
        rows.sort_by_key(|r| r.created_at);
        Ok(rows.into_iter().map(pairing_from_record).collect())
    }

    async fn revoke(&self, id: &str) -> anyhow::Result<bool> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            match PairingRecord::get_by_id(&mut conn, id).await {
                Ok(record) => {
                    record.delete().exec(&mut conn).await?;
                    Ok(true)
                }
                Err(_) => Ok(false),
            }
        })
        .await
    }
}

// ── Settings (HomeRepository, BriefingMarkRepository) ────────────────────────

impl Db {
    /// Read one settings row; empty value reads as unset.
    async fn setting_get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut conn = self.inner.connection().await?;
        match SettingRecord::get_by_id(&mut conn, key).await {
            Ok(record) => Ok(Some(record.value).filter(|v| !v.is_empty())),
            Err(_) => Ok(None),
        }
    }

    /// Upsert one settings row.
    async fn setting_set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            match SettingRecord::get_by_id(&mut conn, key).await {
                Ok(mut record) => {
                    record
                        .update()
                        .value(value.to_string())
                        .exec(&mut conn)
                        .await?;
                }
                Err(_) => {
                    toasty::create!(SettingRecord {
                        id: key.to_string(),
                        value: value.to_string(),
                    })
                    .exec(&mut conn)
                    .await?;
                }
            }
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl HomeRepository for Db {
    async fn get(&self) -> anyhow::Result<Option<String>> {
        self.setting_get(HOME_SETTING_KEY).await
    }

    async fn set(&self, session_id: &str) -> anyhow::Result<()> {
        self.setting_set(HOME_SETTING_KEY, session_id).await
    }
}

#[async_trait]
impl BriefingMarkRepository for Db {
    async fn last_handled(&self) -> anyhow::Result<Option<String>> {
        self.setting_get(BRIEFING_MARK_KEY).await
    }

    async fn mark_handled(&self, date: &str) -> anyhow::Result<()> {
        self.setting_set(BRIEFING_MARK_KEY, date).await
    }
}

// ── RunRepository ─────────────────────────────────────────────────────────────

#[async_trait]
impl RunRepository for Db {
    async fn list(&self, limit: usize) -> anyhow::Result<Vec<Run>> {
        let mut conn = self.inner.connection().await?;
        // Most-recent-first ordering and the cap are pushed down to SQL, so a
        // large ledger doesn't get fully materialized just to take the head.
        let rows = toasty::query!(RunRecord ORDER BY .started_at DESC LIMIT #limit)
            .exec(&mut conn)
            .await?;
        rows.into_iter().map(run_from_record).collect()
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<Run>> {
        let mut conn = self.inner.connection().await?;
        match RunRecord::get_by_id(&mut conn, id).await {
            Ok(record) => Ok(Some(run_from_record(record)?)),
            Err(_) => Ok(None),
        }
    }

    async fn steps(&self, run_id: &str) -> anyhow::Result<Vec<RunStep>> {
        let mut conn = self.inner.connection().await?;
        // Use the `run_id` index instead of scanning the whole step table.
        let rows = toasty::query!(RunStepRecord FILTER .run_id == #run_id)
            .exec(&mut conn)
            .await?;
        let mut steps: Vec<RunStep> = rows.into_iter().map(step_from_record).collect();
        steps.sort_by_key(|s| s.seq);
        Ok(steps)
    }

    async fn prune(&self, cutoff: i64) -> anyhow::Result<usize> {
        // Transactional: each run and all its steps drop together — a partial
        // prune used to orphan steps whose run was already deleted (or vice
        // versa). with_write_retry re-runs cleanly after a rolled-back conflict.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            // Select the stale runs with the cutoff pushed down to SQL, then drop
            // each run's steps via the `run_id` index — no full step-table scan.
            let stale = toasty::query!(RunRecord FILTER .started_at < #cutoff)
                .exec(&mut tx)
                .await?;
            let count = stale.len();
            let mut newest_pruned: Option<i64> = None;
            for run in stale {
                newest_pruned =
                    Some(newest_pruned.map_or(run.started_at, |at: i64| at.max(run.started_at)));
                let run_id = run.id.clone();
                let steps = toasty::query!(RunStepRecord FILTER .run_id == #run_id)
                    .exec(&mut tx)
                    .await?;
                for step in steps {
                    step.delete().exec(&mut tx).await?;
                }
                // The memory index drops with its run, or `komo memory used`
                // would keep citing turns whose transcript and ledger are gone.
                let mem_run_id = run.id.clone();
                let links = toasty::query!(RunMemoryRecord FILTER .run_id == #mem_run_id)
                    .exec(&mut tx)
                    .await?;
                for link in links {
                    link.delete().exec(&mut tx).await?;
                }
                run.delete().exec(&mut tx).await?;
            }
            // The tombstone, in the same transaction as the deletes: a rebuild
            // reads the log, and without this every pruned run comes back.
            //
            // Bounded by what was *actually* deleted rather than by `cutoff`,
            // which `--before` will happily accept in the future: the newest run
            // that went is the exact edge of the deleted set, since everything
            // that survived started at or after the cutoff above it.
            if let Some(newest) = newest_pruned {
                let fence = newest + 1;
                match SettingRecord::get_by_id(&mut tx, RUN_PRUNED_BEFORE_KEY).await {
                    // Monotonic: a later prune with an older cutoff must not
                    // unfence what an earlier one deleted.
                    Ok(mut record) => {
                        let held = record.value.parse::<i64>().unwrap_or(i64::MIN);
                        if fence > held {
                            record
                                .update()
                                .value(fence.to_string())
                                .exec(&mut tx)
                                .await?;
                        }
                    }
                    Err(_) => {
                        toasty::create!(SettingRecord {
                            id: RUN_PRUNED_BEFORE_KEY.to_string(),
                            value: fence.to_string(),
                        })
                        .exec(&mut tx)
                        .await?;
                    }
                }
            }
            tx.commit().await?;
            Ok(count)
        })
        .await
    }

    async fn reconcile_interrupted(&self, now: i64) -> anyhow::Result<usize> {
        // Transactional: flip every crash-residue "running" run to failed as one
        // unit, so a failure partway doesn't leave some rows stuck "running"
        // (they'd never be reconciled on a later startup). Retry-safe.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            let running = RunStatus::Running.as_str();
            // Only the still-"running" rows are touched — filter pushed to SQL.
            let rows = toasty::query!(RunRecord FILTER .status == #running)
                .exec(&mut tx)
                .await?;
            let mut reconciled = 0;
            for mut record in rows {
                record
                    .update()
                    .status(RunStatus::Failed.as_str().to_string())
                    .error(INTERRUPTED_ERROR.to_string())
                    .recoverable(true)
                    .ended_at(now)
                    .exec(&mut tx)
                    .await?;
                reconciled += 1;
            }
            tx.commit().await?;
            Ok(reconciled)
        })
        .await
    }

    async fn runs_using_memory(
        &self,
        memory_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<MemoryUse>> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(
            RunMemoryRecord FILTER .memory_id == #memory_id ORDER BY .started_at DESC LIMIT #limit
        )
        .exec(&mut conn)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| MemoryUse {
                memory_id: r.memory_id,
                run_id: r.run_id,
                session_id: r.session_id,
                pinned: r.pinned,
                started_at: r.started_at,
            })
            .collect())
    }

    async fn steps_by_tool(&self, tool_name: &str, limit: usize) -> anyhow::Result<Vec<RunStep>> {
        let mut conn = self.inner.connection().await?;
        // Filter, ordering, and cap pushed to SQL (tool_name is unindexed — a
        // scan bounded by the pruned ledger's size, audit-frequency only).
        let rows = toasty::query!(
            RunStepRecord FILTER .tool_name == #tool_name ORDER BY .started_at DESC LIMIT #limit
        )
        .exec(&mut conn)
        .await?;
        Ok(rows.into_iter().map(step_from_record).collect())
    }

    async fn unlearned(&self, session_id: Option<&str>, limit: usize) -> anyhow::Result<Vec<Run>> {
        let mut conn = self.inner.connection().await?;
        // The `learned` filter and the cap are pushed to SQL: once the ledger
        // is mostly learned, a scan that filtered in Rust would spend its whole
        // limit on already-consumed rows and report an empty backlog that isn't.
        // Oldest first — learning replays a conversation forwards, so a
        // correction is extracted after the claim it corrects.
        let rows = match session_id {
            Some(session) => {
                toasty::query!(
                    RunRecord FILTER .learned == false AND .session_id == #session
                    ORDER BY .started_at LIMIT #limit
                )
                .exec(&mut conn)
                .await?
            }
            None => {
                toasty::query!(
                    RunRecord FILTER .learned == false ORDER BY .started_at LIMIT #limit
                )
                .exec(&mut conn)
                .await?
            }
        };
        rows.into_iter()
            .map(run_from_record)
            // A turn still in flight is not an episode. Filtered here rather
            // than in the query because the crash residue it guards against is
            // rare and short-lived (`reconcile_interrupted` clears it at every
            // startup), so it never eats a meaningful share of the limit.
            .filter(|run| !matches!(run, Ok(r) if matches!(r.status, RunStatus::Running)))
            .collect()
    }

    async fn set_outcome(&self, run_id: &str, outcome: &str) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut record = RunRecord::get_by_id(&mut conn, run_id).await?;
            record
                .update()
                .outcome(outcome.to_string())
                .exec(&mut conn)
                .await?;
            Ok(())
        })
        .await
    }

    async fn previous_in_session(&self, run_id: &str) -> anyhow::Result<Option<Run>> {
        let mut conn = self.inner.connection().await?;
        let Ok(current) = RunRecord::get_by_id(&mut conn, run_id).await else {
            return Ok(None);
        };
        let session = current.session_id.clone();
        let started = current.started_at;
        // Strictly earlier, newest first — the turn whose work a follow-up
        // message is most plausibly about.
        let rows = toasty::query!(
            RunRecord FILTER .session_id == #session AND .started_at < #started
            ORDER BY .started_at DESC LIMIT 1usize
        )
        .exec(&mut conn)
        .await?;
        rows.into_iter().next().map(run_from_record).transpose()
    }

    async fn mark_learned(&self, run_ids: &[String]) -> anyhow::Result<()> {
        if run_ids.is_empty() {
            return Ok(());
        }
        // One transaction inside the retry: a conflicting commit rolls the whole
        // batch back and re-runs it, so a partial mark can never make half a
        // learning pass look complete.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            for id in run_ids {
                let mut record = RunRecord::get_by_id(&mut tx, id).await?;
                record.update().learned(true).exec(&mut tx).await?;
            }
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}

// ── RunProjectionStore ────────────────────────────────────────────────────────

#[async_trait]
impl RunProjectionStore for Db {
    async fn commit(
        &self,
        session_id: &str,
        runs: &[ProjectedRun],
        through: u64,
    ) -> anyhow::Result<()> {
        let key = run_projection_key(session_id);
        let committed = self
            .setting_get(&key)
            .await?
            .and_then(|value| value.parse::<u64>().ok());
        if committed.is_some_and(|at| at >= through) {
            return Ok(());
        }
        self.write_projection(runs).await?;
        // The watermark lands *after* the rows, so a crash between the two
        // re-commits a fold the tables already hold — which is the one thing a
        // commit is allowed to do twice.
        self.setting_set(&key, &through.to_string()).await
    }
}

impl Db {
    /// Re-commit every session's ledger out of its log, watermarks ignored.
    ///
    /// The repair path: these tables are disposable, and this is what makes
    /// that true of the ledger too. Answers how many runs it wrote.
    pub async fn rebuild_run_projection(&self) -> anyhow::Result<usize> {
        let mut total = 0;
        for session_id in self.events.session_ids().await? {
            let events = self.events.events(&session_id).await?;
            let runs = project_runs(&session_id, &events);
            if runs.is_empty() {
                continue;
            }
            let through = events.last().map(|event| event.seq).unwrap_or(0);
            self.write_projection(&runs).await?;
            self.setting_set(&run_projection_key(&session_id), &through.to_string())
                .await?;
            total += runs.len();
        }
        Ok(total)
    }

    /// Write one session's fold as rows, in a single transaction.
    ///
    /// Every write is an upsert and nothing is deleted: a settled call and a
    /// finished turn are immutable in the log, so an existing row already
    /// agrees with the fold, and a row the fold no longer produces would mean
    /// the log lost events — which is a gap the loader rejects, not something
    /// to clean up here.
    async fn write_projection(&self, runs: &[ProjectedRun]) -> anyhow::Result<()> {
        if runs.is_empty() {
            return Ok(());
        }
        // Runs an operator pruned are not resurrected, however long their log
        // outlives them.
        let pruned_before = self
            .setting_get(RUN_PRUNED_BEFORE_KEY)
            .await?
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(i64::MIN);
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            for projected in runs {
                let run = &projected.run;
                if run.started_at < pruned_before {
                    continue;
                }
                let memories = if run.memories.is_empty() {
                    String::new()
                } else {
                    serde_json::to_string(&run.memories).unwrap_or_default()
                };
                match RunRecord::get_by_id(&mut tx, &run.id).await {
                    Ok(mut record) => {
                        // `outcome` is not here at all, and `learned` only ever
                        // advances: both are row-held, and the fold overwriting
                        // them would drop a verdict the user gave after the turn.
                        let learned = record.learned || run.learned;
                        // A turn with no terminal event folds as *running*, and
                        // whether it is running or dead is the one thing the log
                        // cannot say — the startup reconciler rules on that and
                        // writes it here. So the fold's silence never un-decides
                        // it: without this, the next turn in the session would
                        // put every interrupted run back to "running".
                        let undecided = matches!(run.status, RunStatus::Running)
                            && record.status != RunStatus::Running.as_str();
                        let status = if undecided {
                            record.status.clone()
                        } else {
                            run.status.as_str().to_string()
                        };
                        let error = if undecided {
                            record.error.clone()
                        } else {
                            run.error.clone()
                        };
                        let ended_at = if undecided {
                            record.ended_at
                        } else {
                            run.ended_at.unwrap_or(0)
                        };
                        record
                            .update()
                            .input(run.input.clone())
                            .plan(run.plan.clone())
                            .status(status)
                            .final_output(run.final_output.clone())
                            .error(error)
                            .recoverable(run.recoverable)
                            .started_at(run.started_at)
                            .ended_at(ended_at)
                            .tokens_in(run.tokens_in)
                            .tokens_out(run.tokens_out)
                            .tokens_cached(run.tokens_cached)
                            .memories(memories.clone())
                            .resumed_from(run.resumed_from.clone().unwrap_or_default())
                            .learned(learned)
                            .exec(&mut tx)
                            .await?;
                    }
                    Err(_) => {
                        toasty::create!(RunRecord {
                            id: run.id.clone(),
                            session_id: run.session_id.clone(),
                            input: run.input.clone(),
                            plan: run.plan.clone(),
                            status: run.status.as_str().to_string(),
                            final_output: run.final_output.clone(),
                            error: run.error.clone(),
                            recoverable: run.recoverable,
                            started_at: run.started_at,
                            ended_at: run.ended_at.unwrap_or(0),
                            tokens_in: run.tokens_in,
                            tokens_out: run.tokens_out,
                            tokens_cached: run.tokens_cached,
                            memories: memories.clone(),
                            resumed_from: run.resumed_from.clone().unwrap_or_default(),
                            learned: run.learned,
                            outcome: String::new(),
                        })
                        .exec(&mut tx)
                        .await?;
                    }
                }

                // Only the calls that settled become rows — that is what the
                // ledger has always held, and an unsettled call is the fold's
                // own answer to recovery, not a step anyone ran.
                let run_id = run.id.clone();
                let existing = toasty::query!(RunStepRecord FILTER .run_id == #run_id)
                    .exec(&mut tx)
                    .await?;
                for step in projected
                    .steps
                    .iter()
                    .filter(|s| s.settled)
                    .map(|s| &s.step)
                {
                    if existing.iter().any(|row| row.seq == step.seq) {
                        continue;
                    }
                    toasty::create!(RunStepRecord {
                        id: uuid::Uuid::now_v7().to_string(),
                        run_id: step.run_id.clone(),
                        seq: step.seq,
                        tool_name: step.tool_name.clone(),
                        args: step.args.clone(),
                        result: step.result.clone(),
                        error: step.error.clone(),
                        ok: step.ok,
                        uncertain: step.uncertain,
                        started_at: step.started_at,
                        ended_at: step.ended_at,
                        elapsed_ms: step.elapsed_ms,
                        structured: match &step.structured {
                            serde_json::Value::Null => String::new(),
                            value => value.to_string(),
                        },
                        output_paths: step.output_paths.join("\n"),
                    })
                    .exec(&mut tx)
                    .await?;
                }

                // The reverse index `komo memory used` reads. Same upsert rule:
                // a link is one turn's use of one memory, and the log states it
                // once.
                let mem_run_id = run.id.clone();
                let links = toasty::query!(RunMemoryRecord FILTER .run_id == #mem_run_id)
                    .exec(&mut tx)
                    .await?;
                for (memory_id, pinned) in run
                    .memories
                    .pinned
                    .iter()
                    .map(|id| (id, true))
                    .chain(run.memories.recall.iter().map(|id| (id, false)))
                {
                    if links
                        .iter()
                        .any(|link| link.memory_id == *memory_id && link.pinned == pinned)
                    {
                        continue;
                    }
                    toasty::create!(RunMemoryRecord {
                        id: uuid::Uuid::now_v7().to_string(),
                        memory_id: memory_id.clone(),
                        run_id: run.id.clone(),
                        session_id: run.session_id.clone(),
                        pinned,
                        started_at: run.started_at,
                    })
                    .exec(&mut tx)
                    .await?;
                }
            }
            tx.commit().await?;
            Ok(())
        })
        .await
    }
}

// ── InboxRepository ──────────────────────────────────────────────────────────

#[async_trait]
impl InboxRepository for Db {
    async fn claim(
        &self,
        origin: &InboundOrigin,
        session_id: &str,
        text: &str,
    ) -> anyhow::Result<InboxClaim> {
        let id = origin.key();
        let lookup = id.as_str();
        let mut conn = self.inner.connection().await?;
        let seen = toasty::query!(InboxRecord FILTER .id == #lookup)
            .exec(&mut conn)
            .await?;
        if !seen.is_empty() {
            return Ok(InboxClaim::Duplicate);
        }
        drop(conn);
        // Each channel consumes its own messages one at a time, so two claims
        // for the same id never race here. If that ever changes, the primary
        // key still refuses the second insert — loudly, rather than by letting
        // both through.
        let session_id = session_id.to_string();
        let text = text.to_string();
        with_write_retry(|| {
            let id = id.clone();
            let session_id = session_id.clone();
            let text = text.clone();
            async move {
                let mut conn = self.inner.connection().await?;
                toasty::create!(InboxRecord {
                    id,
                    session_id,
                    text,
                    status: INBOX_STATUS_CLAIMED.to_string(),
                    claimed_at: time::OffsetDateTime::now_utc().unix_timestamp(),
                    completed_at: 0,
                })
                .exec(&mut conn)
                .await?;
                Ok(())
            }
        })
        .await?;
        Ok(InboxClaim::Fresh)
    }

    async fn complete(&self, origin: &InboundOrigin) -> anyhow::Result<()> {
        let id = origin.key();
        with_write_retry(|| {
            let id = id.clone();
            async move {
                let mut conn = self.inner.connection().await?;
                toasty::query!(InboxRecord FILTER .id == #id)
                    .update()
                    .status(INBOX_STATUS_COMPLETED)
                    .completed_at(time::OffsetDateTime::now_utc().unix_timestamp())
                    .exec(&mut conn)
                    .await?;
                Ok(())
            }
        })
        .await
    }
}

fn step_from_record(record: RunStepRecord) -> RunStep {
    RunStep {
        run_id: record.run_id,
        seq: record.seq,
        tool_name: record.tool_name,
        args: record.args,
        result: record.result,
        error: record.error,
        ok: record.ok,
        uncertain: record.uncertain,
        started_at: record.started_at,
        ended_at: record.ended_at,
        elapsed_ms: record.elapsed_ms,
        // Empty (a tool with no structured view, or a pre-column row) reads back
        // as `Null` — absence, not an empty object. Unparseable text does too:
        // the ledger is an audit record, and a malformed cell must not fail a read.
        structured: serde_json::from_str(&record.structured).unwrap_or(serde_json::Value::Null),
        output_paths: record
            .output_paths
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect(),
    }
}

fn session_from_record(record: SessionRecord, messages: Vec<Message>) -> Session {
    let id = record.id.clone();
    let workspace = record.workspace.clone();
    let created_at = record.created_at;
    let title = record.title.clone();
    let status = record.status.clone();
    let model = record.model.clone();
    let effort = record.effort.clone();
    // Both halves or neither: a half-written address names no correspondent.
    let channel = (!record.channel_platform.is_empty() && !record.channel_peer_id.is_empty())
        .then(|| ChannelPeer::new(&record.channel_platform, &record.channel_peer_id));
    let origin = SessionOrigin::parse(&record.origin);
    Session {
        id,
        workspace,
        messages,
        created_at,
        title,
        status,
        model,
        effort,
        channel,
        origin,
    }
}

fn skill_from_record(record: SkillRecord) -> Skill {
    Skill {
        name: record.name,
        description: record.description,
        instructions: record.instructions,
        protected: record.protected,
        disabled: false,
        // Every db-era skill was a reviewer extraction (there was no other
        // writer); tag it so the imported candidate shows its provenance.
        source: komo_core::domain::skill::SOURCE_REVIEWER.to_string(),
        // The db schema predates offer gating: ungated, like any skill that
        // declares neither key.
        platforms: Vec::new(),
        requires_tools: Vec::new(),
        // Stamped when the import writes the file, not carried from the row.
        updated_at: None,
    }
}

fn pairing_from_record(record: PairingRecord) -> PairingRequest {
    PairingRequest {
        id: record.id,
        platform: record.platform,
        sender_id: record.sender_id,
        chat_id: record.chat_id,
        code_hash: record.code_hash,
        salt: record.salt,
        status: parse_pairing_status(&record.status),
        created_at: record.created_at,
    }
}

fn reminder_from_record(record: ReminderRecord) -> Reminder {
    Reminder {
        id: record.id,
        message: record.message,
        run_at: record.run_at,
        status: parse_reminder_status(&record.status),
        schedule: record.schedule,
        created_at: record.created_at,
    }
}

#[cfg(test)]
mod tests {
    use komo_core::domain::message::Role;

    /// Append one message as an event and make it durable — what a turn does,
    /// condensed for a fixture that only cares that the message is there.
    async fn say(db: &Db, session_id: &str, message: &Message) {
        let kind = match message.role {
            Role::Assistant => SessionEventKind::AssistantMessage(
                komo_core::domain::session_event::AssistantMessageEvent {
                    turn_id: "t".into(),
                    content: message.content.clone(),
                    tool_note: message.tool_note.clone(),
                    surface: komo_core::domain::session_event::SurfacePlacement::append(),
                },
            ),
            _ => {
                SessionEventKind::UserMessage(komo_core::domain::session_event::UserMessageEvent {
                    turn_id: "t".into(),
                    content: message.content.clone(),
                    source: komo_core::domain::session_event::MessageSource::User,
                    surface: komo_core::domain::session_event::SurfacePlacement::append(),
                })
            }
        };
        SessionEventRepository::append(db, session_id, vec![kind])
            .await
            .unwrap();
        SessionEventRepository::durable_flush(db, session_id)
            .await
            .unwrap();
    }
    use super::*;
    use komo_core::domain::reminder::ReminderStatus;
    use komo_core::domain::run_projection::ProjectedStep;

    /// A komo home of this test's own, wiped first.
    ///
    /// The whole directory, not just the db file: a home now holds transcripts
    /// beside `state.db`, and two tests sharing a directory would read each
    /// other's conversations.
    fn sqlite_url(name: &str) -> String {
        let home = std::env::temp_dir().join(format!("komo-test-{name}"));
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        format!("turso:{}", home.join("state.db").display())
    }

    /// Every `CREATE …` statement sqlite_master holds for one table.
    async fn table_schema_sql(path: &std::path::Path, table: &str) -> Vec<String> {
        let raw = turso::Builder::new_local(path.to_string_lossy().as_ref())
            .build()
            .await
            .unwrap();
        let conn = raw.connect().unwrap();
        let mut rows = conn
            .query(
                &format!(
                    "SELECT sql FROM sqlite_master \
                     WHERE tbl_name = '{table}' AND sql IS NOT NULL \
                     ORDER BY name"
                ),
                (),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            if let turso::Value::Text(sql) = row.get_value(0).unwrap() {
                out.push(sql);
            }
        }
        out
    }

    #[tokio::test]
    async fn a_correspondent_resolves_to_one_session_and_carries_no_transcript() {
        let db = Db::connect(&sqlite_url("komo_find_by_peer.db"))
            .await
            .unwrap();
        let alice = ChannelPeer::new("feishu", "oc_alice");
        let bob = ChannelPeer::new("feishu", "oc_bob");
        // A platform's ids are its own: the same string on telegram is a
        // different correspondent.
        let elsewhere = ChannelPeer::new("telegram", "oc_alice");

        assert!(
            SessionRepository::find_by_peer(&db, &alice)
                .await
                .unwrap()
                .is_none(),
            "nobody has written yet"
        );

        let session =
            Session::new("019fad15-8199-7461-9d48-0a6c779f1c8d").with_channel(alice.clone());
        SessionRepository::save(&db, &session).await.unwrap();
        say(&db, &session.id, &Message::user("在吗")).await;

        let found = SessionRepository::find_by_peer(&db, &alice)
            .await
            .unwrap()
            .expect("alice's session");
        assert_eq!(found.id, session.id);
        assert_eq!(found.channel.as_ref(), Some(&alice));
        // Metadata only: a channel asks this on every inbound message just to
        // learn which conversation it is, and loading the transcript to answer
        // that would pay a turn's read before the turn starts.
        assert!(
            found.messages.is_empty(),
            "find_by_peer must not load the transcript"
        );

        for stranger in [&bob, &elsewhere] {
            assert!(
                SessionRepository::find_by_peer(&db, stranger)
                    .await
                    .unwrap()
                    .is_none(),
                "{stranger:?} is a different correspondent"
            );
        }

        // A local session has no correspondent at all, and must not answer for
        // one — an empty address is not an address.
        SessionRepository::save(&db, &Session::new("019fad16-0000-7461-9d48-0a6c779f1c8d"))
            .await
            .unwrap();
        assert!(
            SessionRepository::find_by_peer(&db, &ChannelPeer::new("", ""))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn inbox_claims_once_and_reports_every_redelivery() {
        let db = Db::connect(&sqlite_url("komo_inbox_claim.db"))
            .await
            .unwrap();
        let origin = InboundOrigin::new("telegram", "42");

        assert_eq!(
            db.claim(&origin, "telegram:7", "hi").await.unwrap(),
            InboxClaim::Fresh
        );
        db.complete(&origin).await.unwrap();
        assert_eq!(
            db.claim(&origin, "telegram:7", "hi").await.unwrap(),
            InboxClaim::Duplicate
        );

        // A claim that never completed still blocks its own redelivery: the row
        // exists from the moment it is claimed, which is what makes a crash
        // mid-turn safe.
        let midturn = InboundOrigin::new("telegram", "43");
        assert_eq!(
            db.claim(&midturn, "telegram:7", "second").await.unwrap(),
            InboxClaim::Fresh
        );
        assert_eq!(
            db.claim(&midturn, "telegram:7", "second").await.unwrap(),
            InboxClaim::Duplicate
        );

        // The key is the pair: platforms number their messages independently,
        // so the same id elsewhere is a different message.
        assert_eq!(
            db.claim(&InboundOrigin::new("feishu", "42"), "feishu:9", "hi")
                .await
                .unwrap(),
            InboxClaim::Fresh
        );

        // Local input has no platform to redeliver it — never a duplicate.
        for _ in 0..2 {
            assert_eq!(
                db.claim(&InboundOrigin::local(), "cli:1", "run")
                    .await
                    .unwrap(),
                InboxClaim::Fresh
            );
        }
    }

    /// The reverse direction: which turns did this memory shape? Written from
    /// the same value as `Run.memories` at the same moment, so the two cannot
    /// disagree — and dropped with the run, so a pruned turn stops being cited.
    #[tokio::test]
    async fn a_memory_can_be_traced_back_to_the_turns_it_shaped() {
        use komo_core::domain::run::{RecalledMemories, Run, RunStatus};
        let db = Db::connect(&sqlite_url("komo_memory_used_test.db"))
            .await
            .unwrap();

        let finish = |id: &str, at: i64, mem: RecalledMemories| {
            let mut run = Run::start("api:s", id);
            run.started_at = at;
            run.memories = mem;
            run.status = RunStatus::Done;
            run
        };
        let older = finish(
            "first",
            1_000,
            RecalledMemories {
                pinned: vec!["mem-p".into()],
                recall: vec!["mem-a".into()],
            },
        );
        let newer = finish(
            "second",
            2_000,
            RecalledMemories {
                pinned: Vec::new(),
                recall: vec!["mem-a".into()],
            },
        );
        for (through, run) in [&older, &newer].into_iter().enumerate() {
            commit_run(&db, run, &[], through as u64).await;
        }

        let uses = RunRepository::runs_using_memory(&db, "mem-a", 10)
            .await
            .unwrap();
        assert_eq!(uses.len(), 2);
        assert_eq!(uses[0].run_id, newer.id, "newest first");
        assert!(!uses[0].pinned, "mem-a was recalled, not pinned");

        // The tier is kept, because "it was pinned then" and "it matched the
        // question" are different reasons for a memory to be in a prompt.
        let pinned = RunRepository::runs_using_memory(&db, "mem-p", 10)
            .await
            .unwrap();
        assert_eq!(pinned.len(), 1);
        assert!(pinned[0].pinned);

        // A memory nothing used has no history — not an error.
        assert!(
            RunRepository::runs_using_memory(&db, "mem-never", 10)
                .await
                .unwrap()
                .is_empty()
        );

        // Pruning a run takes its links: citing a turn whose ledger row is gone
        // would send the operator to a `run inspect` that finds nothing.
        RunRepository::prune(&db, 1_500).await.unwrap();
        let after = RunRepository::runs_using_memory(&db, "mem-a", 10)
            .await
            .unwrap();
        assert_eq!(after.len(), 1, "the pruned run's link went with it");
        assert_eq!(after[0].run_id, newer.id);
    }

    #[tokio::test]
    async fn run_memory_table_ddl_matches_push_schema() {
        let fresh = std::env::temp_dir().join("komo_run_memory_ddl_fresh.db");
        crate::persistence::reset_test_db(&fresh);
        let db = Db::connect(&format!("turso:{}", fresh.display()))
            .await
            .unwrap();
        drop(db);
        let reference = table_schema_sql(&fresh, RUN_MEMORY_TABLE).await;
        assert!(!reference.is_empty(), "push_schema created the table");

        let old = std::env::temp_dir().join("komo_run_memory_ddl_old.db");
        crate::persistence::reset_test_db(&old);
        let db = Db::connect(&format!("turso:{}", old.display()))
            .await
            .unwrap();
        drop(db);
        {
            let raw = turso::Builder::new_local(old.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = raw.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.ok();
            conn.execute("DROP TABLE \"run_memory_records\"", ())
                .await
                .unwrap();
        }
        let db = Db::connect(&format!("turso:{}", old.display()))
            .await
            .unwrap();
        drop(db);
        assert_eq!(table_schema_sql(&old, RUN_MEMORY_TABLE).await, reference);
    }

    #[tokio::test]
    async fn inbox_table_ddl_matches_push_schema() {
        let fresh = std::env::temp_dir().join("komo_inbox_ddl_fresh.db");
        crate::persistence::reset_test_db(&fresh);
        let db = Db::connect(&format!("turso:{}", fresh.display()))
            .await
            .unwrap();
        drop(db);
        let reference = table_schema_sql(&fresh, INBOX_TABLE).await;
        assert!(!reference.is_empty(), "push_schema created the table");

        // Simulate a state.db that predates the table: drop it, reconnect, and
        // `ensure_table` must rebuild it byte-identically.
        let old = std::env::temp_dir().join("komo_inbox_ddl_old.db");
        crate::persistence::reset_test_db(&old);
        let db = Db::connect(&format!("turso:{}", old.display()))
            .await
            .unwrap();
        drop(db);
        {
            let raw = turso::Builder::new_local(old.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = raw.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.ok();
            conn.execute("DROP TABLE \"inbox_records\"", ())
                .await
                .unwrap();
        }
        let db = Db::connect(&format!("turso:{}", old.display()))
            .await
            .unwrap();
        drop(db);
        assert_eq!(table_schema_sql(&old, INBOX_TABLE).await, reference);
    }

    /// The link from an answer back to the memories that shaped it. Stored as
    /// ids so the ledger cannot drift from what a memory now says, and kept
    /// even when the memory is later edited or archived — the turn was still
    /// built with it.
    #[tokio::test]
    async fn a_runs_memories_roundtrip() {
        use komo_core::domain::run::{RecalledMemories, Run};
        let db = Db::connect(&sqlite_url("komo_run_memories_test.db"))
            .await
            .unwrap();

        // Recall reaches the row from the turn's own `turn/memories` event, so
        // the projection carries whatever the fold saw — including nothing.
        let mut run = Run::start("api:s", "why did you say that");
        run.memories = RecalledMemories {
            pinned: vec!["mem-pinned".into()],
            recall: vec!["mem-a".into(), "mem-b".into()],
        };
        run.status = komo_core::domain::run::RunStatus::Done;
        commit_run(&db, &run, &[], 10).await;

        let back = RunRepository::get(&db, &run.id).await.unwrap().unwrap();
        assert_eq!(back.memories.pinned, ["mem-pinned"]);
        assert_eq!(back.memories.recall, ["mem-a", "mem-b"]);

        // A turn that used none records none.
        let mut plain = Run::start("api:s", "hi");
        plain.status = komo_core::domain::run::RunStatus::Done;
        commit_run(&db, &plain, &[], 11).await;
        assert!(
            RunRepository::get(&db, &plain.id)
                .await
                .unwrap()
                .unwrap()
                .memories
                .is_empty()
        );
    }

    #[tokio::test]
    async fn run_resumed_from_roundtrips() {
        use komo_core::domain::run::Run;
        let db = Db::connect(&sqlite_url("komo_resumed_from_test.db"))
            .await
            .unwrap();
        let mut run = Run::start("cli:s", "continue");
        run.resumed_from = Some("run-original".to_string());
        commit_run(&db, &run, &[], 0).await;
        let back = RunRepository::get(&db, &run.id).await.unwrap().unwrap();
        assert_eq!(back.resumed_from.as_deref(), Some("run-original"));
    }

    #[tokio::test]
    async fn run_ledger_roundtrips_with_ordered_steps() {
        use komo_core::domain::run::{Run, RunStatus, RunStep};
        let db = Db::connect(&sqlite_url("komo_run_repo_test.db"))
            .await
            .unwrap();

        let mut run = Run::start("cli:session-1", "do the thing");

        // Two steps out of seq order; `steps` must return them sorted.
        let step = |seq: i64, tool: &str, ok: bool| RunStep {
            run_id: run.id.clone(),
            seq,
            tool_name: tool.to_string(),
            args: format!("{{\"a\":{seq}}}"),
            result: if ok { "ok".into() } else { String::new() },
            error: if ok { String::new() } else { "boom".into() },
            ok,
            uncertain: false,
            started_at: 100 + seq,
            ended_at: 101 + seq,
            elapsed_ms: 250 + seq,
            structured: if ok {
                serde_json::json!({ "exit": 0 })
            } else {
                serde_json::Value::Null
            },
            output_paths: if ok {
                vec!["/tmp/komo/out.txt".to_string()]
            } else {
                Vec::new()
            },
        };
        run.plan = "multistep:2".into();
        run.status = RunStatus::Done;
        run.final_output = "all done".into();
        run.ended_at = Some(999);
        commit_run(
            &db,
            &run,
            &[step(1, "time", true), step(0, "shell", false)],
            0,
        )
        .await;

        let got = RunRepository::get(&db, &run.id).await.unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Done);
        assert_eq!(got.final_output, "all done");
        assert_eq!(got.plan, "multistep:2");
        assert_eq!(got.ended_at, Some(999));

        let steps = RunRepository::steps(&db, &run.id).await.unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].seq, 0); // sorted by seq
        assert_eq!(steps[0].tool_name, "shell");
        assert!(!steps[0].ok);
        assert_eq!(steps[0].error, "boom");
        assert_eq!(steps[1].seq, 1);
        assert!(steps[1].ok);
        // The additive columns round-trip, and an absent structured view reads
        // back as `Null` — absence, never an empty object.
        assert_eq!(steps[1].structured, serde_json::json!({ "exit": 0 }));
        assert_eq!(steps[1].output_paths, vec!["/tmp/komo/out.txt".to_string()]);
        assert!(steps[0].structured.is_null());
        assert!(steps[0].output_paths.is_empty());

        let recent = RunRepository::list(&db, 10).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].id, run.id);
    }

    #[tokio::test]
    async fn run_prune_drops_old_runs_and_their_steps() {
        use komo_core::domain::run::{Run, RunStatus, RunStep};
        let db = Db::connect(&sqlite_url("komo_run_prune_test.db"))
            .await
            .unwrap();

        // Three runs at increasing start times, each with one step.
        let make = |id: &str, started_at: i64| Run {
            id: id.to_string(),
            session_id: "cli:s".to_string(),
            input: "x".to_string(),
            plan: String::new(),
            status: RunStatus::Done,
            final_output: String::new(),
            error: String::new(),
            recoverable: false,
            started_at,
            ended_at: Some(started_at + 1),
            tokens_in: 0,
            tokens_out: 0,
            tokens_cached: 0,
            resumed_from: None,
            memories: Default::default(),
            learned: false,
            outcome: String::new(),
        };
        for (through, (id, t)) in [("run-a", 100), ("run-b", 200), ("run-c", 300)]
            .into_iter()
            .enumerate()
        {
            let run = make(id, t);
            let step = RunStep {
                run_id: id.to_string(),
                seq: 0,
                tool_name: "time".into(),
                args: "{}".into(),
                result: "ok".into(),
                error: String::new(),
                ok: true,
                uncertain: false,
                started_at: t,
                ended_at: t + 1,
                elapsed_ms: 12,
                structured: serde_json::Value::Null,
                output_paths: Vec::new(),
            };
            commit_run(&db, &run, &[step], through as u64).await;
        }

        // Cutoff drops run-a (100) and run-b (200), keeps run-c (300).
        let removed = RunRepository::prune(&db, 250).await.unwrap();
        assert_eq!(removed, 2);

        let remaining = RunRepository::list(&db, 10).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "run-c");
        // Steps of pruned runs are gone; the survivor's step stays.
        assert!(RunRepository::steps(&db, "run-a").await.unwrap().is_empty());
        assert_eq!(RunRepository::steps(&db, "run-c").await.unwrap().len(), 1);

        // Nothing older than the floor → no-op.
        assert_eq!(RunRepository::prune(&db, 0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn reconcile_interrupted_fails_only_running_runs() {
        use komo_core::domain::run::{INTERRUPTED_ERROR, Run, RunStatus};
        let db = Db::connect(&sqlite_url("komo_run_reconcile_test.db"))
            .await
            .unwrap();

        // A run left mid-flight (status stays `Running`, as on a crash).
        let stuck = Run::start("cli:crashed", "long task");
        commit_run(&db, &stuck, &[], 0).await;

        // A run that finished cleanly before the restart — must be untouched.
        let mut done = Run::start("cli:ok", "quick task");
        done.status = RunStatus::Done;
        done.final_output = "reply".into();
        done.ended_at = Some(500);
        commit_run(&db, &done, &[], 0).await;

        let reconciled = RunRepository::reconcile_interrupted(&db, 1234)
            .await
            .unwrap();
        assert_eq!(reconciled, 1);

        let stuck = RunRepository::get(&db, &stuck.id).await.unwrap().unwrap();
        assert_eq!(stuck.status, RunStatus::Failed);
        assert_eq!(stuck.error, INTERRUPTED_ERROR);
        assert_eq!(stuck.ended_at, Some(1234));
        assert!(stuck.recoverable, "interrupted run must become resumable");

        let done = RunRepository::get(&db, &done.id).await.unwrap().unwrap();
        assert_eq!(done.status, RunStatus::Done);
        assert_eq!(done.final_output, "reply");
        assert!(!done.recoverable);

        // Idempotent: a second pass finds nothing still running.
        assert_eq!(
            RunRepository::reconcile_interrupted(&db, 9999)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn session_repository_lists_sessions() {
        let db = Db::connect(&sqlite_url("komo_session_repo_test.db"))
            .await
            .unwrap();
        let first = Session::with_workspace("first", "alpha");
        let second = Session::new("second");

        SessionRepository::save(&db, &first).await.unwrap();
        // A later attempt to reuse the id with another workspace must not
        // rebind the existing conversation.
        SessionRepository::save(&db, &Session::with_workspace("first", "beta"))
            .await
            .unwrap();
        say(&db, "first", &Message::user("hello")).await;
        SessionRepository::save(&db, &second).await.unwrap();

        let rows = SessionRepository::list(&db).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "first");
        assert_eq!(rows[0].workspace, "alpha");
        assert_eq!(rows[0].user_turns(), 1);
        assert_eq!(rows[1].id, "second");
    }

    #[tokio::test]
    async fn delete_empty_sessions_prunes_only_sessions_without_messages() {
        let db = Db::connect(&sqlite_url("komo_delete_empty_test.db"))
            .await
            .unwrap();

        // Session with messages — must survive.
        let keep = Session::new("keep");
        SessionRepository::save(&db, &keep).await.unwrap();
        say(&db, "keep", &Message::user("hello")).await;

        // Empty session — must be pruned.
        let drop = Session::new("drop");
        SessionRepository::save(&db, &drop).await.unwrap();

        // Another empty session.
        let drop2 = Session::new("drop2");
        SessionRepository::save(&db, &drop2).await.unwrap();

        let removed = SessionRepository::delete_empty_sessions(&db).await.unwrap();
        assert_eq!(removed, 2);

        let survivors = SessionRepository::list(&db).await.unwrap();
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].id, "keep");
    }

    #[tokio::test]
    async fn delete_empty_sessions_returns_zero_when_none_empty() {
        let db = Db::connect(&sqlite_url("komo_delete_none_test.db"))
            .await
            .unwrap();

        let s = Session::new("only");
        SessionRepository::save(&db, &s).await.unwrap();
        say(&db, "only", &Message::user("hi")).await;

        let removed = SessionRepository::delete_empty_sessions(&db).await.unwrap();
        assert_eq!(removed, 0);
        assert_eq!(SessionRepository::list(&db).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn db_reminder_schedule_roundtrip() {
        let db = Db::connect(&sqlite_url("komo_reminder_schedule_test.db"))
            .await
            .unwrap();
        let now_unix = chrono::Utc::now().timestamp();
        let reminder = komo_core::domain::reminder::Reminder::recurring(
            "take medication".to_string(),
            now_unix + 3600,
            "0 9 * * *".to_string(),
        );

        ReminderRepository::save(&db, &reminder).await.unwrap();
        let pending = ReminderRepository::list_pending(&db).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].schedule, "0 9 * * *");
        assert_eq!(pending[0].status, ReminderStatus::Pending);

        let new_run_at = now_unix + 90_000;
        ReminderRepository::reschedule(&db, &reminder.id, new_run_at)
            .await
            .unwrap();

        let pending = ReminderRepository::list_pending(&db).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].run_at, new_run_at);
        assert_eq!(pending[0].status, ReminderStatus::Pending);
    }

    #[tokio::test]
    async fn db_reminder_roundtrip() {
        let db = Db::connect(&sqlite_url("komo_reminder_repo_test.db"))
            .await
            .unwrap();
        let reminder = Reminder::new("drink water".to_string(), 9999999999);

        ReminderRepository::save(&db, &reminder).await.unwrap();
        let pending = ReminderRepository::list_pending(&db).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message, "drink water");
        assert_eq!(pending[0].status, ReminderStatus::Pending);

        ReminderRepository::set_status(&db, &reminder.id, ReminderStatus::Fired)
            .await
            .unwrap();
        let pending = ReminderRepository::list_pending(&db).await.unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn db_session_todo_set_get_clear() {
        use komo_core::domain::todo::{TodoItem, TodoStatus};
        let db = Db::connect(&sqlite_url("komo_session_todo_test.db"))
            .await
            .unwrap();

        // Absent session reads as empty.
        assert!(
            SessionTodoRepository::get(&db, "s1")
                .await
                .unwrap()
                .is_empty()
        );

        let items = vec![
            TodoItem {
                content: "step one".to_string(),
                status: TodoStatus::InProgress,
                active_form: "doing step one".to_string(),
            },
            TodoItem {
                content: "step two".to_string(),
                status: TodoStatus::Pending,
                active_form: String::new(),
            },
        ];
        SessionTodoRepository::set(&db, "s1", &items).await.unwrap();
        let got = SessionTodoRepository::get(&db, "s1").await.unwrap();
        assert_eq!(got, items);

        // set replaces the whole list (upsert, not append).
        let replaced = vec![TodoItem {
            content: "only step".to_string(),
            status: TodoStatus::Completed,
            active_form: String::new(),
        }];
        SessionTodoRepository::set(&db, "s1", &replaced)
            .await
            .unwrap();
        assert_eq!(
            SessionTodoRepository::get(&db, "s1").await.unwrap(),
            replaced
        );

        // Scoped per session.
        assert!(
            SessionTodoRepository::get(&db, "s2")
                .await
                .unwrap()
                .is_empty()
        );

        SessionTodoRepository::clear(&db, "s1").await.unwrap();
        assert!(
            SessionTodoRepository::get(&db, "s1")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn db_pairing_upsert_approve_revoke_roundtrip() {
        use komo_core::domain::pairing::ApproveOutcome;

        let db = Db::connect(&sqlite_url("komo_pairing_repo_test.db"))
            .await
            .unwrap();
        let (request, code) = PairingRequest::mint("telegram", "777", "777");

        PairingRepository::upsert(&db, &request).await.unwrap();
        let found = PairingRepository::find(&db, "telegram", "777")
            .await
            .unwrap()
            .unwrap();
        // The plaintext code is never persisted — only the salted hash.
        assert_eq!(found.code_hash, request.code_hash);
        assert_ne!(found.code_hash, code);
        assert_eq!(
            found.status,
            komo_core::domain::pairing::PairingStatus::Pending
        );
        assert_eq!(
            PairingRepository::count_active_pending(&db, "telegram")
                .await
                .unwrap(),
            1
        );

        // Upsert with a fresh code replaces the row (one row per sender).
        let (refreshed, refreshed_code) = PairingRequest::mint("telegram", "777", "777");
        PairingRepository::upsert(&db, &refreshed).await.unwrap();
        assert_eq!(PairingRepository::list(&db).await.unwrap().len(), 1);

        assert!(matches!(
            PairingRepository::approve_code(&db, "NOSUCHCD")
                .await
                .unwrap(),
            ApproveOutcome::NotFound
        ));
        let ApproveOutcome::Approved(approved) =
            PairingRepository::approve_code(&db, &refreshed_code)
                .await
                .unwrap()
        else {
            panic!("expected approval");
        };
        assert_eq!(approved.sender_id, "777");
        let found = PairingRepository::find(&db, "telegram", "777")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            found.status,
            komo_core::domain::pairing::PairingStatus::Approved
        );

        assert!(
            PairingRepository::revoke(&db, "telegram:777")
                .await
                .unwrap()
        );
        assert!(
            !PairingRepository::revoke(&db, "telegram:777")
                .await
                .unwrap()
        );
        assert!(
            PairingRepository::find(&db, "telegram", "777")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn db_pairing_locks_out_after_repeated_bad_codes() {
        use komo_core::domain::pairing::{APPROVE_MAX_FAILURES, ApproveOutcome};

        let db = Db::connect(&sqlite_url("komo_pairing_lockout_test.db"))
            .await
            .unwrap();

        // The first APPROVE_MAX_FAILURES - 1 wrong codes are NotFound; the
        // attempt that reaches the limit locks out.
        for _ in 0..APPROVE_MAX_FAILURES - 1 {
            assert!(matches!(
                PairingRepository::approve_code(&db, "BADCODE1")
                    .await
                    .unwrap(),
                ApproveOutcome::NotFound
            ));
        }
        assert!(matches!(
            PairingRepository::approve_code(&db, "BADCODE1")
                .await
                .unwrap(),
            ApproveOutcome::Locked { .. }
        ));
    }

    #[tokio::test]
    async fn rotate_retires_a_chat_session_by_dropping_its_correspondent() {
        // `/new` used to move the transcript to an archived id and leave the
        // live id empty, because a chat's session id *was* its address. The id
        // is a UUID now and the address is a field, so retiring a conversation
        // is dropping the correspondent — nothing moves.
        let db = Db::connect(&sqlite_url("komo_rotate_test.db"))
            .await
            .unwrap();
        let peer = ChannelPeer::new("telegram", "rot");
        let sid = "019fad18-0000-7461-9d48-0a6c779f1c8d";
        SessionRepository::save(&db, &Session::new(sid).with_channel(peer.clone()))
            .await
            .unwrap();
        say(&db, sid, &Message::user("hi")).await;
        say(&db, sid, &Message::assistant("hello")).await;

        assert_eq!(
            SessionRepository::rotate(&db, sid).await.unwrap(),
            Some(sid.to_string())
        );

        // The conversation keeps its id and every word of it.
        assert_eq!(
            MessageRepository::list_by_session(&db, sid)
                .await
                .unwrap()
                .len(),
            2
        );
        // But it no longer answers for that chat, so the peer's next message
        // opens a fresh session.
        assert!(
            SessionRepository::find_by_peer(&db, &peer)
                .await
                .unwrap()
                .is_none(),
            "a retired session must not keep answering for its correspondent"
        );
        // Rotating it again finds nothing left to retire.
        assert_eq!(SessionRepository::rotate(&db, sid).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rotate_is_a_no_op_for_a_session_with_no_correspondent() {
        // A local surface rotates by minting a new id client-side; there is no
        // address to drop, so the store has nothing to do.
        let db = Db::connect(&sqlite_url("komo_rotate_local.db"))
            .await
            .unwrap();
        let sid = "019fad19-0000-7461-9d48-0a6c779f1c8d";
        SessionRepository::save(&db, &Session::new(sid))
            .await
            .unwrap();
        say(&db, sid, &Message::user("hi")).await;
        assert_eq!(SessionRepository::rotate(&db, sid).await.unwrap(), None);
        assert_eq!(
            MessageRepository::list_by_session(&db, sid)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn home_repository_roundtrips_and_overwrites() {
        let db = Db::connect(&sqlite_url("komo_home_repo_test.db"))
            .await
            .unwrap();

        assert!(HomeRepository::get(&db).await.unwrap().is_none());

        HomeRepository::set(&db, "telegram:123456").await.unwrap();
        assert_eq!(
            HomeRepository::get(&db).await.unwrap().as_deref(),
            Some("telegram:123456")
        );

        // /sethome from another chat replaces the home (one row per key).
        HomeRepository::set(&db, "feishu:oc_home").await.unwrap();
        assert_eq!(
            HomeRepository::get(&db).await.unwrap().as_deref(),
            Some("feishu:oc_home")
        );
    }

    #[tokio::test]
    async fn legacy_skills_export_reads_old_rows() {
        // Skills now live as files (`infra/skills.rs`); the db only backs the
        // one-time candidate import. Seed a legacy row directly and check the
        // export maps it with reviewer provenance.
        let db = Db::connect(&sqlite_url("komo_skill_repo_test.db"))
            .await
            .unwrap();
        let mut conn = db.inner.connection().await.unwrap();
        toasty::create!(SkillRecord {
            name: "debug-builds".to_string(),
            description: "Debug build failures".to_string(),
            instructions: "Check compiler errors first.".to_string(),
            protected: true,
        })
        .exec(&mut conn)
        .await
        .unwrap();
        drop(conn);

        let rows = db.export_legacy_skills().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "debug-builds");
        assert!(rows[0].protected);
        assert_eq!(rows[0].source, komo_core::domain::skill::SOURCE_REVIEWER);
    }

    #[tokio::test]
    async fn find_windowed_returns_recent_messages_in_order() {
        let db = Db::connect(&sqlite_url("komo_find_windowed_test.db"))
            .await
            .unwrap();
        let sid = "telegram:win";
        SessionRepository::save(&db, &Session::new(sid))
            .await
            .unwrap();
        // All six messages deliberately share one second-precision timestamp,
        // the way a fast turn's user/assistant pair does. Insertion order must
        // still survive, which is what ordering by the UUIDv7 id buys.
        for i in 0..6i64 {
            let msg = Message {
                role: if i % 2 == 0 {
                    Role::User
                } else {
                    Role::Assistant
                },
                content: format!("m{i}"),
                timestamp: 1_000,
                tool_note: String::new(),
            };
            say(&db, sid, &msg).await;
        }

        // Window of 3 keeps the three most recent, still chronological.
        let windowed = SessionRepository::find_windowed(&db, sid, 3)
            .await
            .unwrap()
            .unwrap();
        let contents: Vec<_> = windowed.messages.iter().map(|m| &m.content).collect();
        assert_eq!(contents, ["m3", "m4", "m5"]);

        // limit == 0 loads the whole transcript (same as `find`).
        let full = SessionRepository::find_windowed(&db, sid, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(full.messages.len(), 6);

        // A window larger than the transcript returns everything.
        let all = SessionRepository::find_windowed(&db, sid, 100)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all.messages.len(), 6);

        assert!(
            SessionRepository::find_windowed(&db, "nope", 3)
                .await
                .unwrap()
                .is_none()
        );
    }

    /// A state.db created before the session columns existed must gain them
    /// **in place** on connect (additive ALTER, like memory.db's
    /// ensure_columns) — an upgraded gateway must not hard-fail every session
    /// query until the operator remembers the delete-to-reset convention.
    /// An upgrading komo keeps its conversations: rows in the old table are
    /// moved into the log on connect, and the rows go away only once the file
    /// holds them. Re-connecting must not duplicate what it already moved.

    #[tokio::test]
    async fn adds_missing_session_columns_in_place() {
        // Its own home, so a shared directory cannot carry a previous run's
        // session logs into this one.
        let home = std::env::temp_dir().join("komo-test-db-addcol");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let path = home.join("state.db");

        // 1. Seed a turso file with the OLD session_records shape (without the
        //    added columns), then drop the handle. (connect skips push_schema
        //    for an existing file, so every table a session query touches must
        //    pre-exist, as it would in a real old db.)
        {
            let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.ok();
            conn.execute(
                "CREATE TABLE \"session_records\" (\
                 \"id\" TEXT NOT NULL, \"created_at\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO \"session_records\" VALUES ('cli:old', 100)",
                (),
            )
            .await
            .unwrap();
        }
        // Mark it turso-native so connect() does not stage it as a sqlite backup.
        std::fs::write(turso_marker_path(&path), b"turso-native\n").unwrap();

        // 2. Connect via Db: ensure_columns adds the session columns in place.
        let db = Db::connect(&format!("turso:{}", path.display()))
            .await
            .unwrap();
        let session = SessionRepository::find(&db, "cli:old").await.unwrap();
        let session = session.expect("the pre-column session survives");

        // 3. An added column is fully usable: it reads as its default and is
        //    writable straight away.
        assert!(session.title.is_empty(), "new column defaults to empty");
        SessionRepository::set_title(&db, "cli:old", "old chat")
            .await
            .unwrap();
        let retitled = SessionRepository::find(&db, "cli:old")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retitled.title, "old chat");
    }

    /// A state.db whose `push_schema` ran while `reviewed_through` was still a
    /// model field carries the column `NOT NULL` with no default — so once the
    /// field left the model, every new-session insert failed the constraint.
    /// Connect must drop the retired column in place and accept writes again.
    #[tokio::test]
    async fn drops_retired_reviewed_through_column_in_place() {
        let home = std::env::temp_dir().join("komo-test-db-dropcol");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let path = home.join("state.db");

        // Seed the shape push_schema wrote in the reviewed_through era: the
        // watermark column NOT NULL and defaultless, beside a live session.
        {
            let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.ok();
            conn.execute(
                "CREATE TABLE \"session_records\" (\
                 \"id\" TEXT NOT NULL, \"created_at\" BIGINT NOT NULL, \
                 \"reviewed_through\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE \"message_records\" (\
                 \"id\" TEXT NOT NULL, \"session_id\" TEXT NOT NULL, \"role\" TEXT NOT NULL, \
                 \"content\" TEXT NOT NULL, \"timestamp\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO \"session_records\" VALUES ('cli:old', 100, 3)",
                (),
            )
            .await
            .unwrap();
        }
        std::fs::write(turso_marker_path(&path), b"turso-native\n").unwrap();

        let db = Db::connect(&format!("turso:{}", path.display()))
            .await
            .unwrap();

        // The pre-existing session survives the drop…
        assert!(
            SessionRepository::find(&db, "cli:old")
                .await
                .unwrap()
                .is_some(),
            "pre-migration session survives the column drop"
        );
        // …and the store accepts new sessions again, which is exactly what the
        // leftover NOT NULL column used to fail.
        SessionRepository::save(&db, &Session::new("cli:new"))
            .await
            .expect("a new session inserts once the retired column is gone");
        assert!(
            SessionRepository::find(&db, "cli:new")
                .await
                .unwrap()
                .is_some()
        );
    }

    /// A state.db created before `recoverable` existed must gain the column
    /// **in place** on connect, like the session columns above — otherwise an
    /// upgraded gateway 500s every run-ledger read ("no such column:
    /// recoverable") until the operator remembers the delete-to-reset.
    #[tokio::test]
    async fn adds_missing_run_columns_in_place() {
        let path = std::env::temp_dir().join("komo_db_addcol_runs.db");
        crate::persistence::reset_test_db(&path);

        // 1. Seed a turso file with the OLD run_records shape (no recoverable):
        //    one crash-residue row, still `running` with the ended_at sentinel.
        {
            let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.ok();
            conn.execute(
                "CREATE TABLE \"run_records\" (\
                 \"id\" TEXT NOT NULL, \"session_id\" TEXT NOT NULL, \
                 \"input\" TEXT NOT NULL, \"plan\" TEXT NOT NULL, \
                 \"status\" TEXT NOT NULL, \"final_output\" TEXT NOT NULL, \
                 \"error\" TEXT NOT NULL, \"started_at\" BIGINT NOT NULL, \
                 \"ended_at\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO \"run_records\" VALUES \
                 ('r-old', 'cli:old', 'hi', 'respond', 'running', '', '', 100, 0)",
                (),
            )
            .await
            .unwrap();
            // And the settings row the projection keeps its watermark in.
            conn.execute(
                "CREATE TABLE \"setting_records\" (\
                 \"id\" TEXT NOT NULL, \"value\" TEXT NOT NULL, PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            // The step table in its old shape too — the projection writes both,
            // so a migrated file has to be writable in both.
            conn.execute(
                "CREATE TABLE \"run_step_records\" (\
                 \"id\" TEXT NOT NULL, \"run_id\" TEXT NOT NULL, \
                 \"seq\" BIGINT NOT NULL, \"tool_name\" TEXT NOT NULL, \
                 \"args\" TEXT NOT NULL, \"result\" TEXT NOT NULL, \
                 \"error\" TEXT NOT NULL, \"ok\" BOOLEAN NOT NULL, \
                 \"started_at\" BIGINT NOT NULL, \"ended_at\" BIGINT NOT NULL, \
                 PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
        }
        std::fs::write(turso_marker_path(&path), b"turso-native\n").unwrap();

        // 2. Connect via Db: ensure_columns adds `recoverable` in place, and
        //    run-ledger reads work again.
        let db = Db::connect(&format!("turso:{}", path.display()))
            .await
            .unwrap();
        let runs = RunRepository::list(&db, 10).await.unwrap();
        assert_eq!(runs.len(), 1, "pre-migration run survives");
        assert!(!runs[0].recoverable, "new column defaults to false");

        // 3. The added column is fully writable: startup reconciliation flips
        //    the crash residue to failed + recoverable.
        let flipped = RunRepository::reconcile_interrupted(&db, 200)
            .await
            .unwrap();
        assert_eq!(flipped, 1);
        let runs = RunRepository::list(&db, 10).await.unwrap();
        assert!(runs[0].recoverable, "interrupted run became resumable");
        assert_eq!(
            (runs[0].tokens_in, runs[0].tokens_out, runs[0].tokens_cached),
            (0, 0, 0),
            "pre-column rows read as unknown usage, not as a free turn"
        );

        // 4. The token columns are writable on the same connection.
        let mut fresh = Run::start("cli:old", "how much did that cost");
        fresh.tokens_in = 900;
        fresh.tokens_out = 120;
        fresh.tokens_cached = 700;
        fresh.status = RunStatus::Done;
        let step = RunStep {
            run_id: fresh.id.clone(),
            seq: 0,
            tool_name: "time".into(),
            args: "{}".into(),
            result: "09:00".into(),
            error: String::new(),
            ok: true,
            uncertain: false,
            started_at: 100,
            ended_at: 101,
            elapsed_ms: 12,
            structured: serde_json::Value::Null,
            output_paths: Vec::new(),
        };
        commit_run(&db, &fresh, &[step], 0).await;
        let stored = RunRepository::get(&db, &fresh.id).await.unwrap().unwrap();
        assert_eq!(
            (stored.tokens_in, stored.tokens_out, stored.tokens_cached),
            (900, 120, 700)
        );
        // The step table's own added columns are writable on the same file.
        let steps = RunRepository::steps(&db, &fresh.id).await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].elapsed_ms, 12);
    }

    /// The learning watermark: `unlearned` offers finished, not-yet-learned runs
    /// oldest first, `mark_learned` retires them, and a turn still in flight is
    /// never offered.
    #[tokio::test]
    async fn unlearned_offers_finished_runs_until_they_are_marked() {
        let db = Db::connect(&sqlite_url("komo_unlearned.db")).await.unwrap();

        // Committed newest-first, so the watermark cannot be the run's own
        // start: it has to advance per commit or the older ones are skipped.
        let through = std::cell::Cell::new(0u64);
        let save = async |id: &str, session: &str, status: RunStatus, at: i64| {
            let mut run = Run::start(session, "q");
            run.id = id.to_string();
            run.started_at = at;
            run.status = status;
            through.set(through.get() + 1);
            commit_run(&db, &run, &[], through.get()).await;
        };
        // Inserted newest-first to prove the ordering is the query's, not the
        // insertion order's.
        save("run-c", "cli:a", RunStatus::Done, 300).await;
        save("run-b", "cli:b", RunStatus::Failed, 200).await;
        save("run-a", "cli:a", RunStatus::Done, 100).await;

        let ids = |runs: Vec<Run>| runs.into_iter().map(|r| r.id).collect::<Vec<_>>();

        assert_eq!(
            ids(RunRepository::unlearned(&db, None, 10).await.unwrap()),
            ["run-a", "run-b", "run-c"],
            "oldest first, so a correction is learned after the claim it corrects"
        );
        assert_eq!(
            ids(RunRepository::unlearned(&db, Some("cli:a"), 10)
                .await
                .unwrap()),
            ["run-a", "run-c"],
            "scoping to one conversation is the query's job, not the caller's"
        );

        RunRepository::mark_learned(&db, &["run-a".to_string(), "run-c".to_string()])
            .await
            .unwrap();
        assert_eq!(
            ids(RunRepository::unlearned(&db, None, 10).await.unwrap()),
            ["run-b"],
            "a retired run is never offered again"
        );
        assert!(
            RunRepository::get(&db, "run-a")
                .await
                .unwrap()
                .unwrap()
                .learned
        );

        // A run still in flight has no decided outcome and no complete step
        // list, so it is not an episode yet.
        let running = Run::start("cli:a", "in flight");
        commit_run(&db, &running, &[], through.get() + 1).await;
        assert_eq!(
            ids(RunRepository::unlearned(&db, None, 10).await.unwrap()),
            ["run-b"]
        );
    }

    // ── run projection ───────────────────────────────────────────────────────

    /// One finished turn's worth of events: a question, a round, a tool call
    /// that settled, the recall that shaped it, the reply, and the terminal
    /// event. Enough that every projected table has something in it.
    async fn log_a_finished_turn(db: &Db, session_id: &str, turn: &str, memory: &str) {
        use komo_core::domain::session_event::{
            AssistantMessageEvent, AssistantRoundEvent, MessageSource, SurfacePlacement,
            ToolCallSettledEvent, ToolCallStartedEvent, ToolOutcome, UserMessageEvent,
        };
        let kinds = vec![
            SessionEventKind::TurnStarted {
                turn_id: turn.into(),
                resumed_from: None,
            },
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: turn.into(),
                content: "what time is it".into(),
                source: MessageSource::User,
                surface: SurfacePlacement::append(),
            }),
            SessionEventKind::AssistantRound(AssistantRoundEvent {
                turn_id: turn.into(),
                round: 0,
                response_id: "resp-1".into(),
                blocks: serde_json::json!([]),
                tokens_in: 120,
                tokens_out: 30,
                tokens_cached: 100,
            }),
            SessionEventKind::ToolCallStarted(ToolCallStartedEvent {
                turn_id: turn.into(),
                call_id: "c1".into(),
                call_index: 0,
                tool: "time".into(),
                args: "{}".into(),
            }),
            SessionEventKind::ToolCallSettled(ToolCallSettledEvent {
                turn_id: turn.into(),
                call_id: "c1".into(),
                call_index: 0,
                outcome: ToolOutcome::Succeeded,
                result: "09:00".into(),
                error: String::new(),
                elapsed_ms: 12,
                structured: serde_json::Value::Null,
                output_paths: Vec::new(),
            }),
            SessionEventKind::TurnMemories {
                turn_id: turn.into(),
                memories: komo_core::domain::run::RecalledMemories {
                    pinned: Vec::new(),
                    recall: vec![memory.to_string()],
                },
            },
            SessionEventKind::AssistantMessage(AssistantMessageEvent {
                turn_id: turn.into(),
                content: "it is 09:00".into(),
                tool_note: String::new(),
                surface: SurfacePlacement::append(),
            }),
            SessionEventKind::TurnCompleted {
                turn_id: turn.into(),
            },
        ];
        SessionEventRepository::append(db, session_id, kinds)
            .await
            .unwrap();
        SessionEventRepository::durable_flush(db, session_id)
            .await
            .unwrap();
    }

    /// Write a run and its steps the only way anything writes them now: as a
    /// committed projection. `through` is the watermark, which every commit for
    /// one session has to advance.
    async fn commit_run(db: &Db, run: &Run, steps: &[RunStep], through: u64) {
        let projected = ProjectedRun {
            run: run.clone(),
            steps: steps
                .iter()
                .map(|step| ProjectedStep {
                    step: step.clone(),
                    settled: true,
                })
                .collect(),
            start_seq: 0,
        };
        RunProjectionStore::commit(db, &run.session_id, &[projected], through)
            .await
            .unwrap();
    }

    /// Commit the session's whole log as the projector would after a turn.
    async fn project(db: &Db, session_id: &str) {
        let events = SessionEventRepository::events(db, session_id)
            .await
            .unwrap();
        let through = events.last().map(|e| e.seq).unwrap_or(0);
        let runs = project_runs(session_id, &events);
        RunProjectionStore::commit(db, session_id, &runs, through)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_ledger_rebuilds_from_the_log_alone() {
        let db = Db::connect(&sqlite_url("komo_projection.db"))
            .await
            .unwrap();
        log_a_finished_turn(&db, "s-proj", "t1", "m1").await;

        project(&db, "s-proj").await;

        let runs = RunRepository::list(&db, 10).await.unwrap();
        assert_eq!(runs.len(), 1, "the turn the log holds");
        let run = &runs[0];
        assert_eq!(run.id, "t1");
        assert_eq!(run.session_id, "s-proj");
        assert_eq!(run.input, "what time is it");
        assert_eq!(run.final_output, "it is 09:00");
        assert_eq!(run.status, RunStatus::Done);
        assert!(!run.recoverable, "a completed turn is not resumable");
        assert_eq!(
            (run.tokens_in, run.tokens_out, run.tokens_cached),
            (120, 30, 100)
        );
        assert_eq!(run.memories.recall, vec!["m1".to_string()]);

        let steps = RunRepository::steps(&db, "t1").await.unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].tool_name, "time");
        assert_eq!(steps[0].result, "09:00");
        assert_eq!(steps[0].elapsed_ms, 12);

        // The two derived indexes every operator surface reads.
        let uses = RunRepository::runs_using_memory(&db, "m1", 10)
            .await
            .unwrap();
        assert_eq!(uses.len(), 1);
        assert_eq!(uses[0].run_id, "t1");
        let audit = RunRepository::steps_by_tool(&db, "time", 10).await.unwrap();
        assert_eq!(audit.len(), 1);
        let pending = RunRepository::unlearned(&db, None, 10).await.unwrap();
        assert_eq!(pending.len(), 1, "nobody has learned from it yet");
    }

    /// A commit runs after every turn and again on a rebuild, over rows it has
    /// already written. Duplicating a step would double the tool's history in
    /// `skills audit`, and duplicating a link would double `memory used`.
    #[tokio::test]
    async fn committing_the_same_fold_twice_changes_nothing() {
        let db = Db::connect(&sqlite_url("komo_projection_idem.db"))
            .await
            .unwrap();
        log_a_finished_turn(&db, "s-idem", "t1", "m1").await;

        project(&db, "s-idem").await;
        // Same watermark: the second call is the one that must not double-write.
        project(&db, "s-idem").await;
        // And once more with the watermark ignored, as a rebuild does.
        db.rebuild_run_projection().await.unwrap();

        assert_eq!(RunRepository::list(&db, 10).await.unwrap().len(), 1);
        assert_eq!(RunRepository::steps(&db, "t1").await.unwrap().len(), 1);
        assert_eq!(
            RunRepository::runs_using_memory(&db, "m1", 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// The two row-held fields. `outcome` is revised by a *later* turn and the
    /// log never carries it; `learned` is a watermark that may predate the
    /// events that now record it. A rebuild that overwrote either would throw
    /// away the user's own verdict, or re-extract every turn ever learned from.
    #[tokio::test]
    async fn a_rebuild_keeps_what_the_log_does_not_know() {
        let db = Db::connect(&sqlite_url("komo_projection_merge.db"))
            .await
            .unwrap();
        log_a_finished_turn(&db, "s-merge", "t1", "m1").await;
        project(&db, "s-merge").await;

        RunRepository::set_outcome(&db, "t1", "{\"verdict\":\"success\"}")
            .await
            .unwrap();
        RunRepository::mark_learned(&db, &["t1".to_string()])
            .await
            .unwrap();

        db.rebuild_run_projection().await.unwrap();

        let run = RunRepository::get(&db, "t1").await.unwrap().unwrap();
        assert_eq!(run.outcome, "{\"verdict\":\"success\"}");
        assert!(run.learned, "a learned turn must not return to the backlog");
        assert!(
            RunRepository::unlearned(&db, None, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// Two turns half a month apart, as bare open/close events. Timestamps a
    /// prune cutoff can actually fall between — which the real log cannot give
    /// a test, since it stamps every append with the same second.
    fn folded_turns(session: &str, turns: &[(&str, i64)]) -> Vec<ProjectedRun> {
        let events: Vec<_> = turns
            .iter()
            .enumerate()
            .flat_map(|(i, (turn, at))| {
                let at = time::OffsetDateTime::from_unix_timestamp(*at).unwrap();
                [
                    SessionEvent::new(
                        i as u64 * 2,
                        at,
                        SessionEventKind::TurnStarted {
                            turn_id: (*turn).into(),
                            resumed_from: None,
                        },
                    ),
                    SessionEvent::new(
                        i as u64 * 2 + 1,
                        at,
                        SessionEventKind::TurnCompleted {
                            turn_id: (*turn).into(),
                        },
                    ),
                ]
            })
            .collect();
        project_runs(session, &events)
    }

    /// The completion criterion for making these rows a projection: `state.db`
    /// is disposable, so deleting it entirely must cost nothing but the time to
    /// fold the logs back. Every query an operator surface makes has to answer
    /// the same afterwards.
    #[tokio::test]
    async fn a_deleted_state_db_rebuilds_the_ledger_from_the_logs() {
        let url = sqlite_url("komo_projection_rebuild.db");
        let path = std::path::PathBuf::from(url.trim_start_matches("turso:"));

        async fn snapshot(db: &Db) -> String {
            let runs = RunRepository::list(db, 10).await.unwrap();
            let mut out = format!("{runs:?}");
            for run in &runs {
                out.push_str(&format!(
                    "{:?}",
                    RunRepository::steps(db, &run.id).await.unwrap()
                ));
            }
            out.push_str(&format!(
                "{:?}{:?}{:?}",
                RunRepository::runs_using_memory(db, "m1", 10)
                    .await
                    .unwrap(),
                RunRepository::steps_by_tool(db, "time", 10).await.unwrap(),
                RunRepository::unlearned(db, None, 10).await.unwrap(),
            ));
            out
        }

        let before = {
            let db = Db::connect(&url).await.unwrap();
            log_a_finished_turn(&db, "s-a", "t1", "m1").await;
            log_a_finished_turn(&db, "s-b", "t2", "m1").await;
            project(&db, "s-a").await;
            project(&db, "s-b").await;
            snapshot(&db).await
        };

        // Drop the whole file — rows, watermarks and all. The logs under
        // `sessions/` are untouched, and they are the authority.
        crate::persistence::reset_test_db(&path);
        let db = Db::connect(&url).await.unwrap();
        assert!(
            RunRepository::list(&db, 10).await.unwrap().is_empty(),
            "a fresh state.db holds no ledger"
        );

        assert_eq!(db.rebuild_run_projection().await.unwrap(), 2);
        assert_eq!(snapshot(&db).await, before);
    }

    /// A pruned run must stay gone. Its log outlives it — retention keeps
    /// whatever is still resumable or unlearned — so without a tombstone the
    /// next commit hands the operator back exactly what they deleted.
    #[tokio::test]
    async fn a_pruned_run_is_never_projected_again() {
        let db = Db::connect(&sqlite_url("komo_projection_prune.db"))
            .await
            .unwrap();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let folded = folded_turns("s-prune", &[("t-old", now - 30 * 86_400), ("t-new", now)]);
        RunProjectionStore::commit(&db, "s-prune", &folded, 3)
            .await
            .unwrap();
        assert_eq!(RunRepository::list(&db, 10).await.unwrap().len(), 2);

        assert_eq!(
            RunRepository::prune(&db, now - 86_400).await.unwrap(),
            1,
            "only the older turn is stale"
        );

        // The same fold, offered again with a watermark that advances — which is
        // what a rebuild, or the next turn in this session, does.
        RunProjectionStore::commit(&db, "s-prune", &folded, 99)
            .await
            .unwrap();

        let runs = RunRepository::list(&db, 10).await.unwrap();
        assert_eq!(runs.len(), 1, "the tombstone outranks the fold");
        assert_eq!(
            runs[0].id, "t-new",
            "and the turn nobody pruned is still here"
        );
    }

    /// Whether an open turn is running or dead is the one fact the log cannot
    /// hold — the startup reconciler rules on it. The fold's silence must not
    /// overturn that ruling, or the next turn in the session puts every
    /// interrupted run back to "running" and nothing is ever resumable.
    #[tokio::test]
    async fn a_reconciled_run_is_not_reopened_by_the_next_commit() {
        let db = Db::connect(&sqlite_url("komo_projection_interrupted.db"))
            .await
            .unwrap();
        // An open turn: `turn/started` with no terminal event.
        let events = vec![SessionEvent::new(
            0,
            time::OffsetDateTime::now_utc(),
            SessionEventKind::TurnStarted {
                turn_id: "t-open".into(),
                resumed_from: None,
            },
        )];
        let folded = project_runs("s-open", &events);
        RunProjectionStore::commit(&db, "s-open", &folded, 0)
            .await
            .unwrap();
        assert_eq!(
            RunRepository::get(&db, "t-open")
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Running
        );

        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        assert_eq!(
            RunRepository::reconcile_interrupted(&db, now)
                .await
                .unwrap(),
            1
        );

        // The same fold again, as the session's next turn would commit it.
        RunProjectionStore::commit(&db, "s-open", &folded, 5)
            .await
            .unwrap();

        let run = RunRepository::get(&db, "t-open").await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error, INTERRUPTED_ERROR);
        assert!(
            run.recoverable,
            "and it is still the turn resume can pick up"
        );
    }

    /// `run prune --before` takes any date, including one past every run there
    /// is. The fence still has to describe what was deleted rather than the
    /// cutoff asked for, or every later turn falls behind it.
    #[tokio::test]
    async fn a_prune_of_everything_does_not_fence_off_later_turns() {
        let db = Db::connect(&sqlite_url("komo_projection_prune_all.db"))
            .await
            .unwrap();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let old = folded_turns("s-all", &[("t-old", now - 30 * 86_400)]);
        RunProjectionStore::commit(&db, "s-all", &old, 1)
            .await
            .unwrap();

        assert_eq!(
            RunRepository::prune(&db, now + 86_400).await.unwrap(),
            1,
            "a future cutoff prunes everything that exists"
        );

        let later = folded_turns("s-all", &[("t-later", now)]);
        RunProjectionStore::commit(&db, "s-all", &later, 9)
            .await
            .unwrap();
        let runs = RunRepository::list(&db, 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, "t-later");
    }

    /// The watermark exists so a session whose log has not moved costs nothing
    /// to re-commit. A stale one must still be able to catch up.
    #[tokio::test]
    async fn a_commit_that_would_not_advance_the_watermark_is_skipped() {
        let db = Db::connect(&sqlite_url("komo_projection_mark.db"))
            .await
            .unwrap();
        log_a_finished_turn(&db, "s-mark", "t1", "m1").await;
        project(&db, "s-mark").await;

        // A fold the projection has already committed, offered again with a
        // *lower* watermark: it must not be treated as new.
        let events = SessionEventRepository::events(&db, "s-mark").await.unwrap();
        let mut folded = project_runs("s-mark", &events);
        folded[0].run.input = "rewritten behind the log's back".into();
        RunProjectionStore::commit(&db, "s-mark", &folded, 0)
            .await
            .unwrap();
        let run = RunRepository::get(&db, "t1").await.unwrap().unwrap();
        assert_eq!(run.input, "what time is it");

        // A second turn moves the log on, and the projection follows.
        log_a_finished_turn(&db, "s-mark", "t2", "m2").await;
        project(&db, "s-mark").await;
        assert_eq!(RunRepository::list(&db, 10).await.unwrap().len(), 2);
    }
}
