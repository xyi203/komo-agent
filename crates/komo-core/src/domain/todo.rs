//! Session-scoped working todo — the agent's *current focus list* for one
//! conversation, distinct from the durable cross-session `task` store
//! (`domain/task.rs`). This is the roadmap's "会话内 todo" layer (§2/§8):
//! ephemeral, ordered (position = priority), at most one item `in_progress`.
//!
//! Modeled on the精简 intersection of hermes-agent's `TodoStore` and Claude
//! Code's `TodoWrite`: `{content, status, active_form}`, full-list replace on
//! write. Unlike those (which keep it purely in process memory), komo reloads
//! a session per turn, so the list is persisted keyed by session id — but it is
//! still disposable working state, cleared when the session rotates (`/new`).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TodoStatus {
    Pending,
    InProgress,
    Completed,
    Cancelled,
}

impl TodoStatus {
    /// Active = still on the agent's plate (counts toward the list summary the
    /// model re-reads). Completed/cancelled are done.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Pending | Self::InProgress)
    }
}

pub fn parse_todo_status(s: &str) -> anyhow::Result<TodoStatus> {
    match s {
        "pending" => Ok(TodoStatus::Pending),
        "in_progress" => Ok(TodoStatus::InProgress),
        "completed" => Ok(TodoStatus::Completed),
        "cancelled" => Ok(TodoStatus::Cancelled),
        other => Err(anyhow::anyhow!(
            "unknown todo status `{other}` (expected pending/in_progress/completed/cancelled)"
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoItem {
    /// Imperative description of the step ("Write the parser").
    pub content: String,
    pub status: TodoStatus,
    /// Present-continuous form shown while the step runs ("Writing the parser").
    /// Optional; empty string = none.
    #[serde(default)]
    pub active_form: String,
}

/// Per-session storage for the working todo list. Keyed by session id; an
/// absent session reads as an empty list.
#[async_trait]
pub trait SessionTodoRepository: Send + Sync {
    async fn get(&self, session_id: &str) -> anyhow::Result<Vec<TodoItem>>;
    async fn set(&self, session_id: &str, items: &[TodoItem]) -> anyhow::Result<()>;
    /// Drop the list for `session_id` (on a `/new` conversation boundary).
    async fn clear(&self, session_id: &str) -> anyhow::Result<()>;
}
