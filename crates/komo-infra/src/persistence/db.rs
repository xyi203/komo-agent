use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use toasty_driver_turso::Turso;
use tracing::info;

use crate::persistence::{
    DEFAULT_POOL_SIZE, ensure_columns, ensure_table, message_log::MessageLog, prepare_turso_path,
    turso_marker_path, with_write_retry,
};

use komo_core::domain::{
    briefing::BriefingMarkRepository,
    home::HomeRepository,
    inbox::{InboundOrigin, InboxClaim, InboxRepository},
    message::{Message, Role, ToolEntry},
    pairing::{
        APPROVE_LOCKOUT_SECS, APPROVE_MAX_FAILURES, ApproveOutcome, PAIRING_CODE_TTL_SECS,
        PairingRepository, PairingRequest, PairingStatus, parse_pairing_status, verify_code,
    },
    reminder::{Reminder, ReminderRepository, ReminderStatus, parse_reminder_status},
    repository::{MessageRepository, SessionRepository},
    run::{INTERRUPTED_ERROR, MemoryUse, Run, RunRepository, RunStatus, RunStep, parse_run_status},
    session::Session,
    skill::Skill,
    todo::{SessionTodoRepository, TodoItem},
    turn_journal::{JournalEntry, TurnJournalRepository, parse_journal_kind},
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

    #[has_many]
    messages: toasty::Deferred<Vec<MessageRecord>>,
}

/// Transcripts as they were stored before they became files.
///
/// Nothing writes here any more — `MessageRepository` is the log
/// (`super::message_log`). The model stays declared so `connect` can read an
/// upgrading komo's rows once and move them out; a db whose table is empty has
/// nothing left to migrate, which is why the migration needs no marker.
#[derive(Debug, toasty::Model)]
struct MessageRecord {
    // UUIDv7 string key (time-ordered) rather than `#[auto]` autoincrement:
    // Turso's MVCC concurrent-write mode rejects AUTOINCREMENT. Assigned at
    // insert (`MessageRepository::save`).
    #[key]
    id: String,

    #[index]
    session_id: String,

    #[belongs_to(key = session_id, references = id)]
    session_record: toasty::Deferred<SessionRecord>,

    role: String,
    content: String,
    timestamp: i64,

    /// The turn's tool-activity digest for an assistant message (`domain/run.rs
    /// ::tool_digest`), carried into later turns' model history. Empty = none,
    /// which is also what a row written before the column reads as. Additive
    /// column (see `MESSAGE_COLUMNS`).
    tool_note: String,
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
    // UUIDv7 string key (see `MessageRecord`): MVCC rejects AUTOINCREMENT.
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

/// One turn-journal row (`domain/turn_journal.rs`): the persisted twin of an
/// in-flight turn's provider-level state, keyed to its ledger run. `payload`
/// is JSON owned by the writer in `komo-agent`'s llm layer.
#[derive(Debug, toasty::Model)]
struct TurnJournalRecord {
    // UUIDv7 string key (see `MessageRecord`): MVCC rejects AUTOINCREMENT.
    #[key]
    id: String,

    #[index]
    run_id: String,

    seq: i64,
    kind: String, // "envelope" | "assistant" | "results"
    payload: String,
    created_at: i64,
}

/// The exact DDL `push_schema` emits for [`TurnJournalRecord`], for creating
/// the table in place on a db file that predates it (`push_schema` only runs
/// for new files, and is not idempotent — but deleting state.db to pick up a
/// new table would throw away every chat transcript). Byte-parity with
/// `push_schema`'s output is locked by `journal_table_ddl_matches_push_schema`.
const JOURNAL_TABLE: &str = "turn_journal_records";
const JOURNAL_TABLE_DDL: &[&str] = &[
    "CREATE TABLE \"turn_journal_records\" (\"id\" TEXT NOT NULL, \"run_id\" TEXT NOT NULL, \
     \"seq\" BIGINT NOT NULL, \"kind\" TEXT NOT NULL, \"payload\" TEXT NOT NULL, \
     \"created_at\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
    "CREATE INDEX \"index_turn_journal_records_by_run_id\" ON \"turn_journal_records\" (\"run_id\")",
];

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

// ── Db ───────────────────────────────────────────────────────────────────────

/// The disposable session/run/pairing store, over the Turso engine with a
/// per-operation connection pool: `inner` is a plain `Arc<toasty::Db>` (no outer
/// `Mutex`), so every method checks out a pooled `Connection` and independent
/// reads/writes run concurrently. Concurrently-written tables (the run ledger)
/// use [`with_write_retry`] for MVCC commit conflicts.
pub struct Db {
    inner: Arc<toasty::Db>,
    /// Transcripts, which are files rather than rows — see
    /// [`message_log`](super::message_log) for why. Session *metadata* is still
    /// a row here: it is updated (title, status, model, watermark), and a log is
    /// the wrong shape for a value that changes.
    messages: MessageLog,
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
            ];
            ensure_columns(p, "session_records", SESSION_COLUMNS).await?;
            const MESSAGE_COLUMNS: &[(&str, &str)] =
                &[("tool_note", "\"tool_note\" text NOT NULL DEFAULT ''")];
            ensure_columns(p, "message_records", MESSAGE_COLUMNS).await?;
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
            ensure_table(p, JOURNAL_TABLE, JOURNAL_TABLE_DDL).await?;
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
                MessageRecord,
                SkillRecord,
                ReminderRecord,
                SessionTodoRecord,
                PairingRecord,
                LockoutRecord,
                SettingRecord,
                RunRecord,
                RunStepRecord,
                TurnJournalRecord,
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
        let messages = MessageLog::open(&transcript_home)?;

        let this = Self {
            inner: Arc::new(db),
            messages,
        };
        // Move any transcript still in the table out to its file. One-time and
        // idempotent, the same shape as the legacy-SQLite staging above: a komo
        // that upgrades keeps its conversations without the operator being told
        // to delete anything.
        if !is_new {
            this.migrate_messages_to_log().await?;
        }
        Ok(this)
    }

    /// Move transcripts out of `message_records` and into their files, once.
    ///
    /// Runs on every connect to an existing db and does nothing when the table
    /// is already empty, so there is no marker to keep in sync — the table being
    /// empty *is* the marker. Rows are deleted only after their file is written,
    /// so an interrupted migration re-runs rather than losing a transcript.
    async fn migrate_messages_to_log(&self) -> anyhow::Result<()> {
        let mut conn = self.inner.connection().await?;
        let Ok(rows) = toasty::query!(MessageRecord).exec(&mut conn).await else {
            // No such table: a db from before messages were rows at all.
            return Ok(());
        };
        if rows.is_empty() {
            return Ok(());
        }

        // Group by session, preserving the id order the table used for ordering
        // (UUIDv7, millisecond-precise) — that order becomes the file's order,
        // which is what the log uses from then on.
        let mut by_session: std::collections::BTreeMap<String, Vec<MessageRecord>> =
            std::collections::BTreeMap::new();
        for row in rows {
            by_session
                .entry(row.session_id.clone())
                .or_default()
                .push(row);
        }

        let mut moved = 0usize;
        for (session_id, mut rows) in by_session {
            // A transcript already on disk means a previous run got this far;
            // appending again would duplicate it.
            if !self.messages.is_empty(&session_id).await? {
                continue;
            }
            rows.sort_by(|a, b| a.id.cmp(&b.id));
            for row in &rows {
                self.messages
                    .append(
                        &session_id,
                        &Message {
                            role: parse_role(&row.role),
                            content: row.content.clone(),
                            timestamp: row.timestamp,
                            tool_note: row.tool_note.clone(),
                        },
                    )
                    .await?;
            }
            moved += rows.len();
            for row in rows {
                row.delete().exec(&mut conn).await?;
            }
        }
        if moved > 0 {
            info!(
                messages = moved,
                "moved transcripts out of state.db into ~/.komo/sessions"
            );
        }
        Ok(())
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
        let messages = self.messages.list(id).await?;
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
        // The window is the tail of the file. What the table needed a UUIDv7 key
        // to reconstruct — the order of two messages written inside one second —
        // the log gets from the order they were appended in.
        let messages = self.messages.window(id, limit).await?;
        Ok(Some(session_from_record(record, messages)))
    }

    async fn list(&self) -> anyhow::Result<Vec<Session>> {
        let mut conn = self.inner.connection().await?;
        let mut rows = toasty::query!(SessionRecord).exec(&mut conn).await?;
        rows.sort_by_key(|r| r.created_at);

        let mut sessions = Vec::with_capacity(rows.len());
        for record in rows {
            let messages = self.messages.list(&record.id).await?;
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
            if self.messages.is_empty(&record.id).await? {
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
        // Nothing to archive if the session is absent or already empty. Checked
        // before anything moves, because the transcript move below is not part
        // of the transaction that follows it.
        {
            let mut conn = self.inner.connection().await?;
            if SessionRecord::get_by_id(&mut conn, session_id)
                .await
                .is_err()
                || self.messages.is_empty(session_id).await?
            {
                return Ok(None);
            }
        }
        // The transcript moves *first*, and deliberately. The two steps cannot
        // be made atomic — one is a rename, the other a database transaction —
        // so the question is which half-done state is safer. This order leaves,
        // at worst, a transcript filed under an id with no metadata row: `/new`
        // did what the user asked (the conversation is cleared) and the history
        // is recoverable by hand. The other order leaves the live conversation
        // still readable after `/new` said it was archived, which is the failure
        // that lies to the user.
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let archived_id = format!("{session_id}#{now}");
        self.messages.rename(session_id, &archived_id).await?;

        // Wrapped in with_write_retry: a transaction that loses an MVCC commit
        // rolls back cleanly, so re-running the whole closure never
        // double-applies.
        let archived_id = with_write_retry(|| async {
            let archived_id = archived_id.clone();
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            let Ok(live) = SessionRecord::get_by_id(&mut tx, session_id).await else {
                return Ok(None);
            };

            // Give the moved transcript a session row, preserving the original's
            // start time; the live row stays and is now empty for the next
            // conversation. Rotation carries no learning state: the watermark is
            // per-run and a run keeps the session id it was recorded under, so
            // moving a transcript cannot make an episode look unlearned again.
            toasty::create!(SessionRecord {
                id: archived_id.clone(),
                created_at: live.created_at,
                workspace: live.workspace.clone(),
                title: live.title.clone(),
                status: live.status.clone(),
                model: live.model.clone(),
                effort: live.effort.clone(),
            })
            .exec(&mut tx)
            .await?;
            tx.commit().await?;
            Ok(Some(archived_id))
        })
        .await?;
        Ok(archived_id)
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
            for msg in record.messages().exec(&mut tx).await? {
                msg.delete().exec(&mut tx).await?;
            }
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
    // Every method here is the log's; the table is gone. What used to be a
    // query with an index, an ORDER BY and a UUIDv7 key is a file read — see
    // `super::message_log` for why that trade is worth making.
    async fn list_by_session(&self, session_id: &str) -> anyhow::Result<Vec<Message>> {
        self.messages.list(session_id).await
    }

    async fn save(&self, session_id: &str, message: &Message) -> anyhow::Result<()> {
        self.messages.append(session_id, message).await
    }

    async fn cancel_last_turn(&self, session_id: &str) -> anyhow::Result<()> {
        self.messages.record_cancelled_turn(session_id).await
    }

    async fn record_interjection(&self, session_id: &str, text: &str) -> anyhow::Result<()> {
        self.messages.record_interjection(session_id, text).await
    }

    async fn record_tool(&self, session_id: &str, entry: &ToolEntry) -> anyhow::Result<()> {
        self.messages.append_tool(session_id, entry).await
    }
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
    async fn start(&self, run: &Run) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
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
                resumed_from: run.resumed_from.clone().unwrap_or_default(),
                memories: if run.memories.is_empty() {
                    String::new()
                } else {
                    serde_json::to_string(&run.memories).unwrap_or_default()
                },
                // Explicit, so the column's backfill default never applies to a
                // run the learning pass is meant to see.
                learned: run.learned,
                outcome: run.outcome.clone(),
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn append_step(&self, step: &RunStep) -> anyhow::Result<()> {
        // A round's tool calls run concurrently (`run_agent_loop`), so several
        // steps of the same run can be appended at once — retry on MVCC conflict.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
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
                // `Null` is "no structured view" — store it as the empty string
                // rather than the four bytes of `null`, so the column reads the
                // same for a tool without one and a row written before it existed.
                structured: match &step.structured {
                    serde_json::Value::Null => String::new(),
                    value => value.to_string(),
                },
                output_paths: step.output_paths.join("\n"),
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn finish(&self, run: &Run) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut record = RunRecord::get_by_id(&mut conn, &run.id).await?;
            record
                .update()
                .plan(run.plan.clone())
                .status(run.status.as_str().to_string())
                .final_output(run.final_output.clone())
                .error(run.error.clone())
                .ended_at(run.ended_at.unwrap_or(0))
                .tokens_in(run.tokens_in)
                .tokens_out(run.tokens_out)
                .tokens_cached(run.tokens_cached)
                // Written here, not at `start`: the enricher runs inside the
                // turn, so at `start` there is nothing to record yet.
                .memories(if run.memories.is_empty() {
                    String::new()
                } else {
                    serde_json::to_string(&run.memories).unwrap_or_default()
                })
                .exec(&mut conn)
                .await?;
            // The reverse index, written from the same value and at the same
            // moment, so the two can never disagree about what a turn used.
            for (memory_id, pinned) in run
                .memories
                .pinned
                .iter()
                .map(|id| (id, true))
                .chain(run.memories.recall.iter().map(|id| (id, false)))
            {
                toasty::create!(RunMemoryRecord {
                    id: uuid::Uuid::now_v7().to_string(),
                    memory_id: memory_id.clone(),
                    run_id: run.id.clone(),
                    session_id: run.session_id.clone(),
                    pinned,
                    started_at: run.started_at,
                })
                .exec(&mut conn)
                .await?;
            }
            Ok(())
        })
        .await
    }

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
            for run in stale {
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
                let journal_run_id = run.id.clone();
                let journal = toasty::query!(TurnJournalRecord FILTER .run_id == #journal_run_id)
                    .exec(&mut tx)
                    .await?;
                for row in journal {
                    row.delete().exec(&mut tx).await?;
                }
                run.delete().exec(&mut tx).await?;
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

    async fn mark_resumed(&self, id: &str) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut record = RunRecord::get_by_id(&mut conn, id).await?;
            record.update().recoverable(false).exec(&mut conn).await?;
            Ok(())
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

// ── TurnJournalRepository ─────────────────────────────────────────────────────

#[async_trait]
impl TurnJournalRepository for Db {
    async fn append(&self, run_id: &str, entry: &JournalEntry) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            toasty::create!(TurnJournalRecord {
                id: uuid::Uuid::now_v7().to_string(),
                run_id: run_id.to_string(),
                seq: entry.seq,
                kind: entry.kind.as_str().to_string(),
                payload: entry.payload.clone(),
                created_at: time::OffsetDateTime::now_utc().unix_timestamp(),
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn load(&self, run_id: &str) -> anyhow::Result<Vec<JournalEntry>> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(TurnJournalRecord FILTER .run_id == #run_id)
            .exec(&mut conn)
            .await?;
        let mut entries = rows
            .into_iter()
            .map(|row| {
                Ok(JournalEntry {
                    seq: row.seq,
                    kind: parse_journal_kind(&row.kind)?,
                    payload: row.payload,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        entries.sort_by_key(|e| e.seq);
        Ok(entries)
    }

    async fn delete(&self, run_id: &str) -> anyhow::Result<usize> {
        // Transactional for the same reason as `prune`: a run's journal rows
        // drop together or not at all — a half-deleted journal would rebuild
        // into a corrupt history instead of failing cleanly.
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            let rows = toasty::query!(TurnJournalRecord FILTER .run_id == #run_id)
                .exec(&mut tx)
                .await?;
            let count = rows.len();
            for row in rows {
                row.delete().exec(&mut tx).await?;
            }
            tx.commit().await?;
            Ok(count)
        })
        .await
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

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

fn parse_role(s: &str) -> Role {
    match s {
        "system" => Role::System,
        "assistant" => Role::Assistant,
        "tool" => Role::Tool,
        _ => Role::User,
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
    Session {
        id,
        workspace,
        messages,
        created_at,
        title,
        status,
        model,
        effort,
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
    use super::*;
    use komo_core::domain::reminder::ReminderStatus;

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

    /// Every `CREATE …` statement sqlite_master holds for `turn_journal_records`,
    /// ordered by object name so the comparison is deterministic.
    async fn journal_schema_sql(path: &std::path::Path) -> Vec<String> {
        table_schema_sql(path, "turn_journal_records").await
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
        for run in [&older, &newer] {
            RunRepository::start(&db, run).await.unwrap();
            RunRepository::finish(&db, run).await.unwrap();
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

    #[tokio::test]
    async fn journal_table_ddl_matches_push_schema() {
        // A fresh file gets the table from push_schema — the reference shape.
        let fresh = std::env::temp_dir().join("komo_journal_ddl_fresh.db");
        crate::persistence::reset_test_db(&fresh);
        let db = Db::connect(&format!("turso:{}", fresh.display()))
            .await
            .unwrap();
        drop(db);
        let reference = journal_schema_sql(&fresh).await;
        assert!(!reference.is_empty(), "push_schema created the table");

        // An "old" file: connect, drop the journal objects to simulate a db
        // created before the table existed, then reconnect — ensure_table must
        // recreate them with byte-identical DDL.
        let old = std::env::temp_dir().join("komo_journal_ddl_old.db");
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
            conn.execute("DROP TABLE \"turn_journal_records\"", ())
                .await
                .unwrap();
        }
        let db = Db::connect(&format!("turso:{}", old.display()))
            .await
            .unwrap();
        drop(db);
        assert_eq!(journal_schema_sql(&old).await, reference);
    }

    #[tokio::test]
    async fn turn_journal_roundtrips_ordered_and_deletes_whole() {
        use komo_core::domain::turn_journal::{JournalEntry, JournalKind, TurnJournalRepository};
        let db = Db::connect(&sqlite_url("komo_turn_journal_test.db"))
            .await
            .unwrap();

        let entry = |seq: i64, kind: JournalKind, payload: &str| JournalEntry {
            seq,
            kind,
            payload: payload.to_string(),
        };
        // Append out of seq order; `load` must return them sorted.
        for e in [
            entry(1, JournalKind::Assistant, r#"{"blocks":[]}"#),
            entry(0, JournalKind::Envelope, r#"{"version":1}"#),
            entry(2, JournalKind::Results, r#"{"outcomes":[]}"#),
        ] {
            TurnJournalRepository::append(&db, "run-j1", &e)
                .await
                .unwrap();
        }
        // A second run's rows must not bleed in.
        TurnJournalRepository::append(&db, "run-j2", &entry(0, JournalKind::Envelope, "{}"))
            .await
            .unwrap();

        let loaded = TurnJournalRepository::load(&db, "run-j1").await.unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(
            loaded.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(loaded[0].kind, JournalKind::Envelope);
        assert_eq!(loaded[2].payload, r#"{"outcomes":[]}"#);

        let removed = TurnJournalRepository::delete(&db, "run-j1").await.unwrap();
        assert_eq!(removed, 3);
        assert!(
            TurnJournalRepository::load(&db, "run-j1")
                .await
                .unwrap()
                .is_empty()
        );
        // The other run's journal survives.
        assert_eq!(
            TurnJournalRepository::load(&db, "run-j2")
                .await
                .unwrap()
                .len(),
            1
        );
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

        // The real order, which is the whole point: a run is opened *before*
        // the turn runs, so at `start` there is nothing to record — the
        // enricher has not run yet. Recording only at `start` (as this first
        // did) leaves the column empty forever in production while a
        // storage-roundtrip test passes happily.
        let mut run = Run::start("api:s", "why did you say that");
        assert!(run.memories.is_empty());
        RunRepository::start(&db, &run).await.unwrap();

        run.memories = RecalledMemories {
            pinned: vec!["mem-pinned".into()],
            recall: vec!["mem-a".into(), "mem-b".into()],
        };
        run.status = komo_core::domain::run::RunStatus::Done;
        RunRepository::finish(&db, &run).await.unwrap();

        let back = RunRepository::get(&db, &run.id).await.unwrap().unwrap();
        assert_eq!(back.memories.pinned, ["mem-pinned"]);
        assert_eq!(back.memories.recall, ["mem-a", "mem-b"]);

        // A turn that used none records none.
        let mut plain = Run::start("api:s", "hi");
        RunRepository::start(&db, &plain).await.unwrap();
        plain.status = komo_core::domain::run::RunStatus::Done;
        RunRepository::finish(&db, &plain).await.unwrap();
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
        RunRepository::start(&db, &run).await.unwrap();
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
        RunRepository::start(&db, &run).await.unwrap();

        // Append two steps out of seq order; `steps` must return them sorted.
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
        RunRepository::append_step(&db, &step(1, "time", true))
            .await
            .unwrap();
        RunRepository::append_step(&db, &step(0, "shell", false))
            .await
            .unwrap();

        run.plan = "multistep:2".into();
        run.status = RunStatus::Done;
        run.final_output = "all done".into();
        run.ended_at = Some(999);
        RunRepository::finish(&db, &run).await.unwrap();

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
        for (id, t) in [("run-a", 100), ("run-b", 200), ("run-c", 300)] {
            let run = make(id, t);
            RunRepository::start(&db, &run).await.unwrap();
            RunRepository::append_step(
                &db,
                &RunStep {
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
                },
            )
            .await
            .unwrap();
            // A journal row per run: pruning a run must take its journal too.
            use komo_core::domain::turn_journal::{
                JournalEntry, JournalKind, TurnJournalRepository,
            };
            TurnJournalRepository::append(
                &db,
                id,
                &JournalEntry {
                    seq: 0,
                    kind: JournalKind::Envelope,
                    payload: "{}".into(),
                },
            )
            .await
            .unwrap();
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
        // Same for the journals.
        use komo_core::domain::turn_journal::TurnJournalRepository;
        assert!(
            TurnJournalRepository::load(&db, "run-a")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            TurnJournalRepository::load(&db, "run-c")
                .await
                .unwrap()
                .len(),
            1
        );

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
        RunRepository::start(&db, &stuck).await.unwrap();

        // A run that finished cleanly before the restart — must be untouched.
        let mut done = Run::start("cli:ok", "quick task");
        done.status = RunStatus::Done;
        done.final_output = "reply".into();
        done.ended_at = Some(500);
        RunRepository::start(&db, &done).await.unwrap();
        RunRepository::finish(&db, &done).await.unwrap();

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

        // Resuming clears the flag, so a second resume finds nothing.
        RunRepository::mark_resumed(&db, &stuck.id).await.unwrap();
        let stuck = RunRepository::get(&db, &stuck.id).await.unwrap().unwrap();
        assert!(!stuck.recoverable);
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
        MessageRepository::save(&db, "first", &Message::user("hello"))
            .await
            .unwrap();
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
        MessageRepository::save(&db, "keep", &Message::user("hello"))
            .await
            .unwrap();

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
        MessageRepository::save(&db, "only", &Message::user("hi"))
            .await
            .unwrap();

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
    async fn rotate_archives_transcript_and_empties_live_session() {
        let db = Db::connect(&sqlite_url("komo_rotate_test.db"))
            .await
            .unwrap();
        let sid = "telegram:rot";
        SessionRepository::save(&db, &Session::new(sid))
            .await
            .unwrap();
        MessageRepository::save(&db, sid, &Message::user("hi"))
            .await
            .unwrap();
        MessageRepository::save(&db, sid, &Message::assistant("hello"))
            .await
            .unwrap();

        let archived = SessionRepository::rotate(&db, sid)
            .await
            .unwrap()
            .expect("a non-empty session rotates");
        assert_ne!(archived, sid);

        // Live session is now empty; the archive holds the transcript.
        assert!(
            MessageRepository::list_by_session(&db, sid)
                .await
                .unwrap()
                .is_empty()
        );
        let archived_msgs = MessageRepository::list_by_session(&db, &archived)
            .await
            .unwrap();
        assert_eq!(archived_msgs.len(), 2);
        assert_eq!(archived_msgs[0].content, "hi");

        // Rotating an empty session is a no-op.
        assert!(SessionRepository::rotate(&db, sid).await.unwrap().is_none());
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
            MessageRepository::save(&db, sid, &msg).await.unwrap();
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
    async fn transcripts_in_the_old_table_move_into_the_log_once() {
        let home = std::env::temp_dir().join("komo-test-msg-migration");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let path = home.join("state.db");

        // Seed a db shaped like one written before transcripts were files.
        {
            let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.execute(
                "CREATE TABLE \"session_records\" (\"id\" TEXT NOT NULL, \"created_at\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "CREATE TABLE \"message_records\" (\"id\" TEXT NOT NULL, \"session_id\" TEXT NOT NULL, \"role\" TEXT NOT NULL, \"content\" TEXT NOT NULL, \"timestamp\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
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
            // Ids out of insertion order on purpose: the table's order was its
            // UUIDv7 key, and that order is what the file must inherit.
            for (id, role, body) in [
                ("m2", "assistant", "hi there"),
                ("m1", "user", "hello"),
                ("m3", "user", "again"),
            ] {
                conn.execute(
                    &format!(
                        "INSERT INTO \"message_records\" VALUES ('{id}', 'cli:old', '{role}', '{body}', 100)"
                    ),
                    (),
                )
                .await
                .unwrap();
            }
        }
        std::fs::write(turso_marker_path(&path), b"turso-native\n").unwrap();

        let url = format!("turso:{}", path.display());
        let db = Db::connect(&url).await.unwrap();
        let moved: Vec<String> = MessageRepository::list_by_session(&db, "cli:old")
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.content)
            .collect();
        assert_eq!(moved, ["hello", "hi there", "again"], "in key order");
        assert_eq!(
            moved.iter().filter(|c| c.as_str() != "hi there").count(),
            2,
            "both user turns moved out of the table"
        );
        drop(db);

        // Connecting again finds an empty table and leaves the file alone.
        let db = Db::connect(&url).await.unwrap();
        assert_eq!(
            MessageRepository::list_by_session(&db, "cli:old")
                .await
                .unwrap()
                .len(),
            3,
            "a second connect must not duplicate the transcript"
        );
    }

    #[tokio::test]
    async fn adds_missing_session_columns_in_place() {
        // Its own home: `connect` now moves transcripts out of the table into
        // `<home>/sessions`, so a shared directory would carry a previous run's
        // migrated messages into this one.
        let home = std::env::temp_dir().join("komo-test-db-addcol");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let path = home.join("state.db");

        // 1. Seed a turso file with the OLD session_records shape (no
        //    the added columns) plus its messages table, then drop the handle.
        //    (connect skips push_schema for an existing file, so every table a
        //    session query touches must pre-exist, as it would in a real old db.)
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
                "CREATE TABLE \"message_records\" (\
                 \"id\" TEXT NOT NULL, \"session_id\" TEXT NOT NULL, \"role\" TEXT NOT NULL, \
                 \"content\" TEXT NOT NULL, \"timestamp\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
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
            conn.execute(
                "INSERT INTO \"message_records\" VALUES ('m1', 'cli:old', 'user', 'hello', 100)",
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
        let session = session.expect("pre-migration session survives");
        assert_eq!(session.messages.len(), 1, "transcript intact");

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

        // 4. Same contract for `message_records.tool_note`: a pre-migration row
        //    reads as "no note", and the column is writable straight away.
        assert!(
            session.messages[0].tool_note.is_empty(),
            "a pre-column message has no tool note"
        );
        MessageRepository::save(
            &db,
            "cli:old",
            &Message::assistant("done").with_tool_note("[tools used] read foo"),
        )
        .await
        .unwrap();
        // Located by content, not position: the seeded legacy row's key is the
        // literal `m1`, which sorts *after* a UUIDv7 id.
        let messages = MessageRepository::list_by_session(&db, "cli:old")
            .await
            .unwrap();
        let saved = messages
            .iter()
            .find(|m| m.content == "done")
            .expect("the note-bearing message round-trips");
        assert!(saved.tool_note.contains("read foo"));
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
        RunRepository::start(&db, &fresh).await.unwrap();
        fresh.status = RunStatus::Done;
        RunRepository::finish(&db, &fresh).await.unwrap();
        let stored = RunRepository::get(&db, &fresh.id).await.unwrap().unwrap();
        assert_eq!(
            (stored.tokens_in, stored.tokens_out, stored.tokens_cached),
            (900, 120, 700)
        );
    }

    /// The learning watermark: `unlearned` offers finished, not-yet-learned runs
    /// oldest first, `mark_learned` retires them, and a turn still in flight is
    /// never offered.
    #[tokio::test]
    async fn unlearned_offers_finished_runs_until_they_are_marked() {
        let db = Db::connect(&sqlite_url("komo_unlearned.db")).await.unwrap();

        let save = async |id: &str, session: &str, status: RunStatus, at: i64| {
            let mut run = Run::start(session, "q");
            run.id = id.to_string();
            run.started_at = at;
            RunRepository::start(&db, &run).await.unwrap();
            run.status = status;
            RunRepository::finish(&db, &run).await.unwrap();
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
        RunRepository::start(&db, &running).await.unwrap();
        assert_eq!(
            ids(RunRepository::unlearned(&db, None, 10).await.unwrap()),
            ["run-b"]
        );
    }
}
