//! Durable cross-session tasks — the "kanban layer" of the roadmap's task &
//! commitment model (docs/personal-agent-roadmap.md §2). One table covers
//! inbox items (status = inbox) and commitments (`waiting_on` set); session-
//! scoped work breakdown stays out of this model.

use async_trait::async_trait;

use super::session::ChannelPeer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Captured but not yet triaged (replaces a separate InboxItem model).
    Inbox,
    Todo,
    /// Blocked on someone or something external (see `waiting_on`).
    Waiting,
    Done,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::Todo => "todo",
            Self::Waiting => "waiting",
            Self::Done => "done",
            Self::Cancelled => "cancelled",
        }
    }

    /// Open = still actionable (shows up in lists and the due sweep).
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Inbox | Self::Todo | Self::Waiting)
    }
}

pub fn parse_task_status(s: &str) -> anyhow::Result<TaskStatus> {
    match s {
        "inbox" => Ok(TaskStatus::Inbox),
        "todo" => Ok(TaskStatus::Todo),
        "waiting" => Ok(TaskStatus::Waiting),
        "done" => Ok(TaskStatus::Done),
        "cancelled" => Ok(TaskStatus::Cancelled),
        other => Err(anyhow::anyhow!(
            "unknown task status `{other}` (expected inbox/todo/waiting/done/cancelled)"
        )),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Task {
    pub id: String,
    pub title: String,
    /// Free-form details. Empty string = none.
    pub note: String,
    pub status: TaskStatus,
    /// Who this is waiting on / promised to. Empty string = nobody — a task
    /// with this set is what the roadmap calls a commitment.
    pub waiting_on: String,
    pub due_at: Option<i64>,
    /// Session id this task came from (`telegram:{chat_id}`, `feishu:{chat_id}`,
    /// a cli session uuid). Empty string = captured outside any session.
    pub source: String,
    /// Dedup key for automated extraction (reviewer); empty for manual captures.
    pub source_message_id: String,
    /// Who is being waited on, as an address an inbound message can be compared
    /// with. `None` = `waiting_on` is only words, and this task cannot be woken
    /// — never guessed from the name, which is for a human to read
    /// (docs/bot-runtime.md §3.7).
    pub waiting_on_peer: Option<ChannelPeer>,
    /// The standing wake registered while this task is `Waiting`; retired when
    /// it leaves. `None` = nothing is watching for a reply.
    pub wakeup_id: Option<String>,
    /// Optional project/grouping label. Empty string = the default board. A
    /// plain string (not a separate model) — multi-project grouping without the
    /// weight of a Project entity (the roadmap §2 escape hatch, as hermes does).
    pub board: String,
    /// When the due notification went out (at-most-once delivery guard).
    pub due_notified_at: Option<i64>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
}

impl Task {
    pub fn new(title: String) -> Self {
        Self {
            id: format!("task-{}", uuid::Uuid::now_v7()),
            title,
            note: String::new(),
            status: TaskStatus::Inbox,
            waiting_on: String::new(),
            due_at: None,
            source: String::new(),
            source_message_id: String::new(),
            waiting_on_peer: None,
            wakeup_id: None,
            board: String::new(),
            due_notified_at: None,
            created_at: time::OffsetDateTime::now_utc().unix_timestamp(),
            completed_at: None,
        }
    }

    /// Whether a reply from the person this waits on could bring it back —
    /// `Waiting` plus an address to match against. What `komo task list` marks
    /// when it is false.
    pub fn is_wakeable(&self) -> bool {
        self.status == TaskStatus::Waiting && self.waiting_on_peer.is_some()
    }
}

#[async_trait]
pub trait TaskRepository: Send + Sync {
    async fn save(&self, task: &Task) -> anyhow::Result<()>;
    async fn find(&self, id: &str) -> anyhow::Result<Option<Task>>;
    /// All tasks with an open status (inbox / todo / waiting), oldest first.
    async fn list_open(&self) -> anyhow::Result<Vec<Task>>;
    /// Overwrite every mutable field of the row matching `task.id`.
    async fn update(&self, task: &Task) -> anyhow::Result<()>;
    /// Find an existing task by its automated-extraction dedup key
    /// (`source` + `source_message_id`), across *all* statuses — so the reviewer
    /// never re-captures a commitment the user already triaged, completed, or
    /// cancelled. Returns `None` when nothing matches.
    async fn find_by_source_message_id(
        &self,
        source: &str,
        source_message_id: &str,
    ) -> anyhow::Result<Option<Task>>;

    /// The task that registered this wake, across every status. Read when a
    /// registration fires, to say which commitment the arriving message is
    /// about and to clear the id it just spent.
    async fn find_by_wakeup_id(&self, wakeup_id: &str) -> anyhow::Result<Option<Task>>;
}
