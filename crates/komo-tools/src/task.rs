//! The `task` tool: durable cross-session tasks (roadmap §2).
//!
//! Four actions, deliberately minimal: `capture` collects into the inbox,
//! `list` shows open tasks, `update` retriages (status / due / waiting_on),
//! `complete` closes. No `plan_today` — daily planning is the briefing
//! sweep's job, where the model reads this list and organizes it itself.
//!
//! `waiting` is the one status with a runtime consequence: a task that names
//! *who* it waits on as an address registers a standing wake, and their next
//! message opens a turn about the commitment (docs/bot-runtime.md §3.7). Every
//! write here that can enter or leave `waiting` goes through
//! [`TaskWaiting::sync`] rather than touching registrations itself.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    context::ToolContext,
    session::ChannelPeer,
    task::{Task, TaskRepository, TaskStatus, parse_task_status},
    tool::{Tool, ToolError, ToolOutput, parse_args},
};
use komo_services::task_waiting::TaskWaiting;

#[derive(Deserialize)]
struct TaskArgs {
    action: String,
    title: Option<String>,
    note: Option<String>,
    status: Option<String>,
    waiting_on: Option<String>,
    waiting_on_peer: Option<PeerArgs>,
    due: Option<String>,
    id: Option<String>,
    board: Option<String>,
}

/// The correspondent to listen for, as the model reads it off the conversation
/// — never derived from `waiting_on`, which is a name.
#[derive(Deserialize)]
struct PeerArgs {
    platform: String,
    peer_id: String,
}

pub struct TaskTool {
    tasks: Arc<dyn TaskRepository>,
    /// Registers and retires the standing wake a `waiting` task holds. `None` =
    /// this runtime has no wakeup store, and `waiting` is a label again.
    waiting: Option<Arc<TaskWaiting>>,
}

impl TaskTool {
    pub fn new(tasks: Arc<dyn TaskRepository>) -> Self {
        Self {
            tasks,
            waiting: None,
        }
    }

    pub fn with_waiting(mut self, waiting: Arc<TaskWaiting>) -> Self {
        self.waiting = Some(waiting);
        self
    }

    /// Bring the task's wake in line with what it now says, before it is
    /// written — one task change, one row write. A failure here is reported as
    /// the tool's error: a `waiting` task the store never registered looks
    /// identical to one that will be woken, and the model has to know which.
    async fn sync_wait(&self, task: &mut Task) -> Result<(), ToolError> {
        let Some(waiting) = &self.waiting else {
            return Ok(());
        };
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        waiting
            .sync(task, now)
            .await
            .map_err(|e| ToolError::Failed(e.context("could not update this task's wake")))
    }
}

fn local_time(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| dt.with_timezone(&chrono::Local).to_rfc3339())
        .unwrap_or_else(|| unix.to_string())
}

fn parse_due(due: &str) -> anyhow::Result<i64> {
    chrono::DateTime::parse_from_rfc3339(due)
        .map(|dt| dt.timestamp())
        .map_err(|e| anyhow::anyhow!("invalid `due` time `{due}` (expected RFC3339): {e}"))
}

/// One task as a display line (shared by list responses and confirmations).
fn render(task: &Task) -> String {
    let mut line = format!("{} [{}] {}", task.id, task.status.as_str(), task.title);
    if !task.board.is_empty() {
        line.push_str(&format!(" #{}", task.board));
    }
    if !task.waiting_on.is_empty() {
        line.push_str(&format!(" (waiting on: {})", task.waiting_on));
    }
    if task.status == TaskStatus::Waiting && task.waiting_on_peer.is_none() {
        line.push_str(" [不可唤醒]");
    }
    if let Some(due) = task.due_at {
        line.push_str(&format!(" (due {})", local_time(due)));
    }
    if !task.note.is_empty() {
        line.push_str(&format!(" — {}", task.note));
    }
    line
}

#[async_trait]
impl Tool for TaskTool {
    fn name(&self) -> &'static str {
        "task"
    }

    fn description(&self) -> &'static str {
        "Durable task list that persists across sessions (unlike this conversation). \
         action=\"capture\" collects a task or idea into the inbox (use status=\"todo\" \
         when it is already actionable, waiting_on when it is a commitment to/from \
         someone); action=\"list\" shows open tasks; action=\"update\" changes \
         status/due/waiting_on/title/note/board by id; action=\"complete\" marks a task done. \
         With status=\"waiting\", pass waiting_on_peer when you know the channel address of \
         the person being waited on — their next message then reopens this task. \
         Optional `board` groups tasks by project; pass it on list to filter. \
         Tasks with a due time are delivered as notifications by the gateway."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["capture", "list", "update", "complete"],
                    "description": "The task operation."
                },
                "title": {
                    "type": "string",
                    "description": "Short task title (action=capture; optional rename on update)."
                },
                "note": {
                    "type": "string",
                    "description": "Free-form details (optional)."
                },
                "status": {
                    "type": "string",
                    "enum": ["inbox", "todo", "waiting", "done", "cancelled"],
                    "description": "Task status. capture defaults to \"inbox\"; use \"waiting\" with waiting_on when blocked on someone."
                },
                "waiting_on": {
                    "type": "string",
                    "description": "Who this task waits on / was promised to (optional)."
                },
                "waiting_on_peer": {
                    "type": "object",
                    "properties": {
                        "platform": { "type": "string", "description": "Channel the person writes on (\"feishu\", \"telegram\", \"wechat\")." },
                        "peer_id": { "type": "string", "description": "That channel's id for them, as it appears on their messages." }
                    },
                    "required": ["platform", "peer_id"],
                    "description": "The address of the person this waits on, taken from the conversation — NOT guessed from their name. With it, their next message brings this task back; without it the task still waits, but nothing will wake it."
                },
                "due": {
                    "type": "string",
                    "description": "RFC3339 due time, e.g. \"2026-06-20T18:00:00+08:00\" (optional)."
                },
                "id": {
                    "type": "string",
                    "description": "Task id (action=update/complete)."
                },
                "board": {
                    "type": "string",
                    "description": "Project/grouping label (optional). On list, filters to this board."
                }
            },
            "required": ["action"]
        })
    }

    async fn call(&self, input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: TaskArgs = parse_args(&input)?;

        match args.action.as_str() {
            "capture" => {
                let title = args.title.ok_or_else(|| {
                    ToolError::InvalidInput("`title` is required for action=capture".to_string())
                })?;
                let mut task = Task::new(title);
                if let Some(note) = args.note {
                    task.note = note;
                }
                if let Some(status) = args.status {
                    task.status = parse_task_status(&status)
                        .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
                }
                if let Some(waiting_on) = args.waiting_on {
                    task.waiting_on = waiting_on;
                }
                if let Some(peer) = args.waiting_on_peer {
                    task.waiting_on_peer = Some(ChannelPeer::new(peer.platform, peer.peer_id));
                }
                if let Some(due) = args.due {
                    task.due_at =
                        Some(parse_due(&due).map_err(|e| ToolError::InvalidInput(e.to_string()))?);
                }
                if let Some(board) = args.board {
                    task.board = board;
                }
                self.sync_wait(&mut task).await?;
                self.tasks.save(&task).await?;
                Ok(ToolOutput::text(format!("Captured: {}", render(&task)))
                    .with_structured(json!({ "id": task.id, "status": task.status.as_str() })))
            }

            "list" => {
                let mut open = self.tasks.list_open().await?;
                if let Some(status) = args.status {
                    let wanted = parse_task_status(&status)
                        .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
                    open.retain(|t| t.status == wanted);
                }
                if let Some(board) = &args.board {
                    open.retain(|t| &t.board == board);
                }
                if open.is_empty() {
                    return Ok(ToolOutput::text("No open tasks."));
                }
                Ok(
                    ToolOutput::text(open.iter().map(render).collect::<Vec<_>>().join("\n"))
                        .with_title(format!("{} open tasks", open.len())),
                )
            }

            "update" => {
                let id = args.id.ok_or_else(|| {
                    ToolError::InvalidInput("`id` is required for action=update".to_string())
                })?;
                let mut task =
                    self.tasks.find(&id).await?.ok_or_else(|| {
                        ToolError::InvalidInput(format!("no task with id `{id}`"))
                    })?;
                if let Some(title) = args.title {
                    task.title = title;
                }
                if let Some(note) = args.note {
                    task.note = note;
                }
                if let Some(status) = args.status {
                    task.status = parse_task_status(&status)
                        .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
                    if task.status == TaskStatus::Done && task.completed_at.is_none() {
                        task.completed_at = Some(time::OffsetDateTime::now_utc().unix_timestamp());
                    }
                }
                if let Some(waiting_on) = args.waiting_on {
                    task.waiting_on = waiting_on;
                }
                if let Some(peer) = args.waiting_on_peer {
                    task.waiting_on_peer = Some(ChannelPeer::new(peer.platform, peer.peer_id));
                }
                if let Some(due) = args.due {
                    task.due_at =
                        Some(parse_due(&due).map_err(|e| ToolError::InvalidInput(e.to_string()))?);
                    // A moved deadline should notify again.
                    task.due_notified_at = None;
                }
                if let Some(board) = args.board {
                    task.board = board;
                }
                self.sync_wait(&mut task).await?;
                self.tasks.update(&task).await?;
                Ok(ToolOutput::text(format!("Updated: {}", render(&task)))
                    .with_structured(json!({ "id": task.id, "status": task.status.as_str() })))
            }

            "complete" => {
                let id = args.id.ok_or_else(|| {
                    ToolError::InvalidInput("`id` is required for action=complete".to_string())
                })?;
                let mut task =
                    self.tasks.find(&id).await?.ok_or_else(|| {
                        ToolError::InvalidInput(format!("no task with id `{id}`"))
                    })?;
                task.status = TaskStatus::Done;
                task.completed_at = Some(time::OffsetDateTime::now_utc().unix_timestamp());
                self.sync_wait(&mut task).await?;
                self.tasks.update(&task).await?;
                Ok(ToolOutput::text(format!("Completed: {}", task.title))
                    .with_structured(json!({ "id": task.id, "status": "done" })))
            }

            other => Err(ToolError::InvalidInput(format!(
                "unknown action `{other}` (expected capture/list/update/complete)"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-memory repository so tool behavior is testable without SQLite.
    #[derive(Default)]
    struct MemTasks {
        rows: Mutex<Vec<Task>>,
    }

    #[async_trait]
    impl TaskRepository for MemTasks {
        async fn save(&self, task: &Task) -> anyhow::Result<()> {
            self.rows.lock().unwrap().push(task.clone());
            Ok(())
        }
        async fn find(&self, id: &str) -> anyhow::Result<Option<Task>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|t| t.id == id)
                .cloned())
        }
        async fn list_open(&self) -> anyhow::Result<Vec<Task>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|t| t.status.is_open())
                .cloned()
                .collect())
        }
        async fn update(&self, task: &Task) -> anyhow::Result<()> {
            let mut rows = self.rows.lock().unwrap();
            let slot = rows
                .iter_mut()
                .find(|t| t.id == task.id)
                .ok_or_else(|| anyhow::anyhow!("not found"))?;
            *slot = task.clone();
            Ok(())
        }
        async fn find_by_source_message_id(
            &self,
            source: &str,
            source_message_id: &str,
        ) -> anyhow::Result<Option<Task>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|t| t.source == source && t.source_message_id == source_message_id)
                .cloned())
        }
        async fn find_by_wakeup_id(&self, wakeup_id: &str) -> anyhow::Result<Option<Task>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|t| t.wakeup_id.as_deref() == Some(wakeup_id))
                .cloned())
        }
    }

    fn tool() -> (TaskTool, Arc<MemTasks>) {
        let repo = Arc::new(MemTasks::default());
        (TaskTool::new(repo.clone()), repo)
    }

    fn ctx() -> ToolContext {
        crate::test_support::detached_ctx("cli:test")
    }

    #[tokio::test]
    async fn capture_defaults_to_inbox() {
        let (tool, repo) = tool();
        let reply = tool
            .call(json!({"action":"capture","title":"review PR"}), &ctx())
            .await
            .unwrap()
            .text;
        assert!(reply.contains("[inbox] review PR"), "{reply}");
        assert_eq!(repo.rows.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn capture_with_waiting_on_records_commitment() {
        let (tool, repo) = tool();
        tool.call(
            json!({"action":"capture","title":"weekly report","status":"waiting","waiting_on":"boss"}),
            &ctx(),
        )
        .await
        .unwrap();
        let rows = repo.rows.lock().unwrap();
        assert_eq!(rows[0].status, TaskStatus::Waiting);
        assert_eq!(rows[0].waiting_on, "boss");
    }

    #[tokio::test]
    async fn complete_sets_done_and_completed_at() {
        let (tool, repo) = tool();
        tool.call(json!({"action":"capture","title":"x"}), &ctx())
            .await
            .unwrap();
        let id = repo.rows.lock().unwrap()[0].id.clone();
        let reply = tool
            .call(json!({"action":"complete","id":id}), &ctx())
            .await
            .unwrap()
            .text;
        assert!(reply.contains("Completed"), "{reply}");
        let rows = repo.rows.lock().unwrap();
        assert_eq!(rows[0].status, TaskStatus::Done);
        assert!(rows[0].completed_at.is_some());
    }

    #[tokio::test]
    async fn update_due_resets_notification_guard() {
        let (tool, repo) = tool();
        tool.call(json!({"action":"capture","title":"x"}), &ctx())
            .await
            .unwrap();
        let id = {
            let mut rows = repo.rows.lock().unwrap();
            rows[0].due_notified_at = Some(100);
            rows[0].id.clone()
        };
        tool.call(
            json!({"action":"update","id":id,"due":"2099-01-01T09:00:00+08:00"}),
            &ctx(),
        )
        .await
        .unwrap();
        let rows = repo.rows.lock().unwrap();
        assert!(rows[0].due_at.is_some());
        assert_eq!(rows[0].due_notified_at, None);
    }

    #[tokio::test]
    async fn list_filters_by_status_and_hides_closed() {
        let (tool, _repo) = tool();
        tool.call(json!({"action":"capture","title":"a"}), &ctx())
            .await
            .unwrap();
        tool.call(
            json!({"action":"capture","title":"b","status":"todo"}),
            &ctx(),
        )
        .await
        .unwrap();
        let reply = tool
            .call(json!({"action":"list","status":"todo"}), &ctx())
            .await
            .unwrap()
            .text;
        assert!(reply.contains("b"), "{reply}");
        assert!(!reply.contains("[inbox] a"), "{reply}");
    }

    #[tokio::test]
    async fn unknown_status_is_invalid_input() {
        let (tool, _repo) = tool();
        let err = tool
            .call(
                json!({"action":"capture","title":"x","status":"urgent"}),
                &ctx(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
        assert!(err.to_string().contains("unknown task status"));
    }

    // --- the standing wake a `waiting` task holds (§3.7) ---------------------

    #[derive(Default)]
    struct MemWakeups(std::sync::Mutex<Vec<komo_core::domain::wakeup::WakeupRegistration>>);

    #[async_trait]
    impl komo_core::domain::wakeup::WakeupRepository for MemWakeups {
        async fn save(
            &self,
            registration: &komo_core::domain::wakeup::WakeupRegistration,
        ) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(registration.clone());
            Ok(())
        }
        async fn list(&self) -> anyhow::Result<Vec<komo_core::domain::wakeup::WakeupRegistration>> {
            Ok(self.0.lock().unwrap().clone())
        }
        async fn take(&self, id: &str) -> anyhow::Result<bool> {
            let mut rows = self.0.lock().unwrap();
            let before = rows.len();
            rows.retain(|r| r.id != id);
            Ok(rows.len() != before)
        }
        async fn take_for_turn(&self, _session: &str, _turn: &str) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    struct MemHome;

    #[async_trait]
    impl komo_core::domain::home::HomeRepository for MemHome {
        async fn get(&self) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
        async fn set(&self, _address: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn home_session(&self) -> anyhow::Result<String> {
            Ok("home-session".to_string())
        }
    }

    fn waking_tool() -> (TaskTool, Arc<MemTasks>, Arc<MemWakeups>) {
        let repo = Arc::new(MemTasks::default());
        let wakeups = Arc::new(MemWakeups::default());
        let waiting = Arc::new(TaskWaiting::new(wakeups.clone(), Arc::new(MemHome)));
        (
            TaskTool::new(repo.clone()).with_waiting(waiting),
            repo,
            wakeups,
        )
    }

    /// An address makes a commitment wakeable; the id of the wake it registered
    /// lives on the task, and completing it takes the wake with it.
    #[tokio::test]
    async fn a_commitment_with_an_address_registers_a_wake_and_completing_it_retires_it() {
        use komo_core::domain::wakeup::WakeupRepository;

        let (tool, repo, wakeups) = waking_tool();
        tool.call(
            json!({
                "action": "capture",
                "title": "等张三的方案",
                "status": "waiting",
                "waiting_on": "张三",
                "waiting_on_peer": { "platform": "feishu", "peer_id": "ou_x" }
            }),
            &ctx(),
        )
        .await
        .unwrap();

        let (id, wake) = {
            let rows = repo.rows.lock().unwrap();
            (rows[0].id.clone(), rows[0].wakeup_id.clone())
        };
        let registered = wakeups.list().await.unwrap();
        assert_eq!(registered.len(), 1);
        assert_eq!(wake.as_deref(), Some(registered[0].id.as_str()));
        assert_eq!(
            registered[0].wakeup,
            komo_core::domain::session_event::Wakeup::Event {
                filter: komo_core::domain::session_event::EventFilter::FromPeer {
                    platform: "feishu".into(),
                    peer_id: "ou_x".into()
                }
            }
        );

        tool.call(json!({"action":"complete","id":id}), &ctx())
            .await
            .unwrap();
        assert!(wakeups.list().await.unwrap().is_empty());
        assert_eq!(repo.rows.lock().unwrap()[0].wakeup_id, None);
    }

    /// A name is not an address. The commitment stands, nothing listens, and
    /// the listing says so rather than implying somebody is watching.
    #[tokio::test]
    async fn a_commitment_naming_only_a_person_is_listed_as_unwakeable() {
        use komo_core::domain::wakeup::WakeupRepository;

        let (tool, _repo, wakeups) = waking_tool();
        tool.call(
            json!({"action":"capture","title":"周报","status":"waiting","waiting_on":"boss"}),
            &ctx(),
        )
        .await
        .unwrap();
        assert!(wakeups.list().await.unwrap().is_empty());

        let listed = tool
            .call(json!({"action":"list"}), &ctx())
            .await
            .unwrap()
            .text;
        assert!(listed.contains("[不可唤醒]"), "{listed}");
    }
}
