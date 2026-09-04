//! Kanban tasks: `task_records` in `komo.db`, and the [`TaskRepository`] over
//! them.
//!
//! Tasks are **durable** personal data — schema changes here are additive and
//! nothing ever deletes the table to reset it. That used to be expressed by
//! keeping them in their own file (`kanban.db`); it is a table-level rule now
//! (docs/adr/0004), so this module holds the model, the queries and the
//! one-time import from the old file, and `Db` owns the connection.

use std::path::Path;

use anyhow::Context;
use async_trait::async_trait;

use super::db::Db;
use crate::persistence::with_write_retry;
use komo_core::domain::session::ChannelPeer;
use komo_core::domain::task::{Task, TaskRepository, parse_task_status};

// Optional i64 fields use 0 as the "unset" sentinel (same convention as `Db`).
#[derive(Debug, toasty::Model)]
pub(crate) struct TaskRecord {
    #[key]
    id: String,
    title: String,
    note: String,
    status: String, // "inbox" | "todo" | "waiting" | "done" | "cancelled"
    waiting_on: String,
    due_at: i64,
    source: String,
    source_message_id: String,
    /// `waiting_on_peer` flattened, same shape `session_records` uses for its
    /// channel; both empty = the task carries only a name, so nothing can wake it.
    waiting_on_platform: String,
    waiting_on_peer_id: String,
    /// The standing registration this task holds while it waits; empty = none.
    wakeup_id: String,
    board: String,
    due_notified_at: i64,
    created_at: i64,
    completed_at: i64,
}

/// Columns added to `task_records` after a `komo.db` was created. Extend this
/// for every new [`TaskRecord`] column: the table is durable, so it is migrated
/// in place and never dropped to be rebuilt.
const EXPECTED: &[(&str, &str)] = &[
    (
        "waiting_on_platform",
        "\"waiting_on_platform\" text NOT NULL DEFAULT ''",
    ),
    (
        "waiting_on_peer_id",
        "\"waiting_on_peer_id\" text NOT NULL DEFAULT ''",
    ),
    ("wakeup_id", "\"wakeup_id\" text NOT NULL DEFAULT ''"),
];

/// Bring an existing file's `task_records` up to the current column set, before
/// toasty opens it.
pub(crate) async fn ensure_schema(path: &Path) -> anyhow::Result<()> {
    crate::persistence::ensure_columns(path, "task_records", EXPECTED).await
}

/// Every task in a legacy `kanban.db`, for the one-time merge into `komo.db`.
///
/// Reads through the same model the live store uses, so a file written by any
/// build that had these columns is readable. A pre-Turso SQLite file is opened
/// with the SQLite driver instead — that migration ran per-store before the
/// merge, and dropping the path would strand anyone who had not upgraded
/// through it.
pub(crate) async fn import_from(path: &Path) -> anyhow::Result<Vec<Task>> {
    // The old file gets the same upkeep first: a `kanban.db` written before
    // these columns existed cannot be opened with the current model.
    ensure_schema(path).await.ok();
    let url = match super::turso_marker_path(path).exists() {
        true => format!("turso:{}", path.display()),
        false => format!("sqlite:{}", path.display()),
    };
    let db = toasty::Db::builder()
        .models(toasty::models!(TaskRecord))
        .connect(&url)
        .await
        .with_context(|| format!("opening {} to merge it in", path.display()))?;
    let mut conn = db.connection().await?;
    let rows = toasty::query!(TaskRecord).exec(&mut conn).await?;
    // Closed tasks included: the merge preserves the whole board, not the open
    // subset.
    rows.into_iter().map(task_from_record).collect()
}

#[async_trait]
impl TaskRepository for Db {
    async fn save(&self, task: &Task) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            toasty::create!(TaskRecord {
                id: task.id.clone(),
                title: task.title.clone(),
                note: task.note.clone(),
                status: task.status.as_str().to_string(),
                waiting_on: task.waiting_on.clone(),
                due_at: task.due_at.unwrap_or(0),
                source: task.source.clone(),
                source_message_id: task.source_message_id.clone(),
                waiting_on_platform: peer_platform(task),
                waiting_on_peer_id: peer_id(task),
                wakeup_id: task.wakeup_id.clone().unwrap_or_default(),
                board: task.board.clone(),
                due_notified_at: task.due_notified_at.unwrap_or(0),
                created_at: task.created_at,
                completed_at: task.completed_at.unwrap_or(0),
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn find(&self, id: &str) -> anyhow::Result<Option<Task>> {
        let mut conn = self.inner.connection().await?;
        match TaskRecord::get_by_id(&mut conn, id).await {
            Ok(record) => Ok(Some(task_from_record(record)?)),
            Err(_) => Ok(None),
        }
    }

    async fn list_open(&self) -> anyhow::Result<Vec<Task>> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(TaskRecord).exec(&mut conn).await?;
        let mut open: Vec<Task> = rows
            .into_iter()
            .map(task_from_record)
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .filter(|t| t.status.is_open())
            .collect();
        open.sort_by_key(|t| t.created_at);
        Ok(open)
    }

    async fn update(&self, task: &Task) -> anyhow::Result<()> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut record = TaskRecord::get_by_id(&mut conn, &task.id).await?;
            record
                .update()
                .title(task.title.clone())
                .note(task.note.clone())
                .status(task.status.as_str().to_string())
                .waiting_on(task.waiting_on.clone())
                .waiting_on_platform(peer_platform(task))
                .waiting_on_peer_id(peer_id(task))
                .wakeup_id(task.wakeup_id.clone().unwrap_or_default())
                .due_at(task.due_at.unwrap_or(0))
                .board(task.board.clone())
                .due_notified_at(task.due_notified_at.unwrap_or(0))
                .completed_at(task.completed_at.unwrap_or(0))
                .exec(&mut conn)
                .await?;
            Ok(())
        })
        .await
    }

    async fn find_by_source_message_id(
        &self,
        source: &str,
        source_message_id: &str,
    ) -> anyhow::Result<Option<Task>> {
        // Dedup keys are only set on automated captures, so an empty key can
        // never match a real extraction — bail before scanning.
        if source_message_id.is_empty() {
            return Ok(None);
        }
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(TaskRecord).exec(&mut conn).await?;
        for record in rows {
            if record.source == source && record.source_message_id == source_message_id {
                return Ok(Some(task_from_record(record)?));
            }
        }
        Ok(None)
    }

    async fn find_by_wakeup_id(&self, wakeup_id: &str) -> anyhow::Result<Option<Task>> {
        if wakeup_id.is_empty() {
            return Ok(None);
        }
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(TaskRecord).exec(&mut conn).await?;
        for record in rows {
            if record.wakeup_id == wakeup_id {
                return Ok(Some(task_from_record(record)?));
            }
        }
        Ok(None)
    }
}

fn peer_platform(task: &Task) -> String {
    task.waiting_on_peer
        .as_ref()
        .map(|p| p.platform.clone())
        .unwrap_or_default()
}

fn peer_id(task: &Task) -> String {
    task.waiting_on_peer
        .as_ref()
        .map(|p| p.peer_id.clone())
        .unwrap_or_default()
}

fn task_from_record(record: TaskRecord) -> anyhow::Result<Task> {
    let nonzero = |v: i64| (v != 0).then_some(v);
    Ok(Task {
        id: record.id,
        title: record.title,
        note: record.note,
        status: parse_task_status(&record.status)?,
        waiting_on: record.waiting_on,
        due_at: nonzero(record.due_at),
        source: record.source,
        source_message_id: record.source_message_id,
        waiting_on_peer: (!record.waiting_on_platform.is_empty()
            && !record.waiting_on_peer_id.is_empty())
        .then(|| ChannelPeer::new(record.waiting_on_platform, record.waiting_on_peer_id)),
        wakeup_id: (!record.wakeup_id.is_empty()).then_some(record.wakeup_id),
        board: record.board,
        due_notified_at: nonzero(record.due_notified_at),
        created_at: record.created_at,
        completed_at: nonzero(record.completed_at),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::task::TaskStatus;

    /// A `komo.db` in a home of this test's own — `Db::connect` scans its
    /// directory for legacy files to merge.
    fn sqlite_url(name: &str) -> String {
        let home = std::env::temp_dir().join(format!("komo-kb-{name}"));
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        format!("turso:{}", home.join("komo.db").display())
    }

    #[tokio::test]
    async fn task_roundtrip_and_update() {
        let db = Db::connect(&sqlite_url("komo_kanban_repo_test.db"))
            .await
            .unwrap();
        let mut task = Task::new("send weekly report".to_string());
        task.due_at = Some(9999999999);
        task.waiting_on = "boss".to_string();
        task.board = "work".to_string();

        db.save(&task).await.unwrap();
        let open = db.list_open().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].title, "send weekly report");
        assert_eq!(open[0].status, TaskStatus::Inbox);
        assert_eq!(open[0].due_at, Some(9999999999));
        assert_eq!(open[0].waiting_on, "boss");
        assert_eq!(open[0].board, "work");
        assert_eq!(open[0].due_notified_at, None);

        let mut updated = open[0].clone();
        updated.status = TaskStatus::Done;
        updated.completed_at = Some(123);
        db.update(&updated).await.unwrap();

        assert!(db.list_open().await.unwrap().is_empty());
        let found = db.find(&task.id).await.unwrap().unwrap();
        assert_eq!(found.status, TaskStatus::Done);
        assert_eq!(found.completed_at, Some(123));
    }

    /// The two columns §3.7 added, through every path that touches them —
    /// including the one that clears them, since a spent wake left on the row
    /// would make `komo task list` claim somebody is still listening.
    #[tokio::test]
    async fn a_waiting_tasks_peer_and_wake_survive_a_write() {
        let db = Db::connect(&sqlite_url("komo_kanban_peer_test.db"))
            .await
            .unwrap();
        let mut task = Task::new("等张三的方案".to_string());
        task.status = TaskStatus::Waiting;
        task.waiting_on = "张三".to_string();
        task.waiting_on_peer = Some(ChannelPeer::new("feishu", "ou_x"));
        task.wakeup_id = Some("wk-1".to_string());
        db.save(&task).await.unwrap();

        let stored = db.find(&task.id).await.unwrap().unwrap();
        assert_eq!(
            stored.waiting_on_peer,
            Some(ChannelPeer::new("feishu", "ou_x"))
        );
        assert_eq!(stored.wakeup_id.as_deref(), Some("wk-1"));
        assert!(stored.is_wakeable());

        let mut spent = stored;
        spent.wakeup_id = None;
        db.update(&spent).await.unwrap();
        let reread = db.find(&task.id).await.unwrap().unwrap();
        assert_eq!(reread.wakeup_id, None);
        assert_eq!(
            reread.waiting_on_peer,
            Some(ChannelPeer::new("feishu", "ou_x")),
            "who it waits on outlives the wake that watched for them"
        );
    }

    /// A commitment carrying only a name has no address to match an inbound
    /// message against — stored as absent, never as an empty half-address.
    #[tokio::test]
    async fn a_task_naming_only_a_person_stores_no_address() {
        let db = Db::connect(&sqlite_url("komo_kanban_nopeer_test.db"))
            .await
            .unwrap();
        let mut task = Task::new("周报".to_string());
        task.status = TaskStatus::Waiting;
        task.waiting_on = "boss".to_string();
        db.save(&task).await.unwrap();

        let stored = db.find(&task.id).await.unwrap().unwrap();
        assert_eq!(stored.waiting_on_peer, None);
        assert_eq!(stored.wakeup_id, None);
        assert!(!stored.is_wakeable());
    }

    #[tokio::test]
    async fn find_by_wakeup_id_finds_the_task_that_registered_it() {
        let db = Db::connect(&sqlite_url("komo_kanban_wake_test.db"))
            .await
            .unwrap();
        let mut task = Task::new("等张三".to_string());
        task.status = TaskStatus::Waiting;
        task.waiting_on_peer = Some(ChannelPeer::new("feishu", "ou_x"));
        task.wakeup_id = Some("wk-7".to_string());
        db.save(&task).await.unwrap();

        assert_eq!(
            db.find_by_wakeup_id("wk-7").await.unwrap().map(|t| t.id),
            Some(task.id)
        );
        assert!(db.find_by_wakeup_id("wk-nope").await.unwrap().is_none());
        assert!(db.find_by_wakeup_id("").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn find_returns_none_for_unknown_id() {
        let db = Db::connect(&sqlite_url("komo_kanban_find_test.db"))
            .await
            .unwrap();
        assert!(db.find("task-nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn find_by_source_message_id_matches_source_and_key() {
        let db = Db::connect(&sqlite_url("komo_kanban_dedup_test.db"))
            .await
            .unwrap();
        let mut task = Task::new("call Bob".to_string());
        task.source = "telegram:1".to_string();
        task.source_message_id = "commit-abc".to_string();
        db.save(&task).await.unwrap();

        // Match on source + key.
        assert!(
            db.find_by_source_message_id("telegram:1", "commit-abc")
                .await
                .unwrap()
                .is_some()
        );
        // Same key, different source → no match.
        assert!(
            db.find_by_source_message_id("telegram:2", "commit-abc")
                .await
                .unwrap()
                .is_none()
        );
        // Empty key never matches.
        assert!(
            db.find_by_source_message_id("telegram:1", "")
                .await
                .unwrap()
                .is_none()
        );
    }
}
