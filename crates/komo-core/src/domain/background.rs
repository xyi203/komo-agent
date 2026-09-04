//! Background tasks: work a turn starts and does not wait for
//! (docs/bot-runtime.md §3.5, §5.9).
//!
//! A tool call is bounded by its round — the turn cannot end until it settles.
//! A background task is the opposite: `task/spawned` is written, the call
//! returns an id, and the turn is free to finish. `task/settled` lands whenever
//! the work does, which may be minutes after the conversation moved on.
//!
//! Two records, both facts in the session log and nothing else — no status
//! table. "Which tasks are still running" is a fold: a `task/spawned` with no
//! `task/settled` beside it ([`unsettled`]). That is also what the per-session
//! cap counts, and what the startup check reads to settle the ones a dead
//! process left behind.
//!
//! **A restart settles every running task as
//! [`Uncertain`](crate::domain::session_event::ToolOutcome::Uncertain)**, never
//! re-runs it. The process group is gone, and whether the command finished
//! first is not knowable — which is exactly the claim a tool call makes when it
//! cannot confirm its own effect, and it has to reach the model rather than
//! quietly becoming a failure.

use std::future::Future;
use std::pin::Pin;

use super::session_event::{SessionEvent, SessionEventKind, TaskKind};

/// How many background tasks one session may have running at once. Small on
/// purpose: each is a process (or a whole sub-agent turn) with nobody watching
/// it, and a model that can start them without limit will.
pub const MAX_BACKGROUND_TASKS_PER_SESSION: usize = 3;

/// What a caller says about the work before it starts.
#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub kind: TaskKind,
    /// One line naming the work — the command, the delegated task.
    pub label: String,
}

/// What the work reports when it is done.
pub struct TaskReport {
    pub outcome: super::session_event::ToolOutcome,
    /// The few lines the model is handed when this wakes a turn.
    pub summary: String,
    /// Everything the work produced, kept in the tool-output store and pointed
    /// at by `task/settled.result_ref`.
    pub full: String,
}

/// The work itself, owned by whoever runs it: a future that outlives the call
/// that handed it over, and therefore the turn.
pub type TaskWork = Pin<Box<dyn Future<Output = TaskReport> + Send>>;

/// Who actually holds a background task while it runs.
///
/// The tool's side of §5.9, reached through [`ToolContext`] the same way an
/// approval is: a tool builds the work and hands it over, and everything after
/// — the id, the two events, the store, the wake when it settles — belongs to
/// the runtime. `None` on a context whose runtime cannot outlive its turns (an
/// aux completion, a sub-agent, a test), and the tool then says so rather than
/// pretending to detach.
///
/// [`ToolContext`]: crate::domain::context::ToolContext
#[async_trait::async_trait]
pub trait BackgroundTasks: Send + Sync {
    /// Record the spawn and take ownership of settling it. Answers the task id.
    ///
    /// Refuses when the session is already at
    /// [`MAX_BACKGROUND_TASKS_PER_SESSION`]; the error text is what the model
    /// is told, so it says what to do instead.
    async fn spawn(
        &self,
        session_id: &str,
        turn_id: &str,
        spec: TaskSpec,
        work: TaskWork,
    ) -> anyhow::Result<String>;
}

/// One spawned task the log has no settle for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsettledTask {
    pub task_id: String,
    pub turn_id: String,
    pub kind: TaskKind,
    pub label: String,
}

/// Every task this session started and never settled, oldest first.
///
/// The one reader of "what is running": the per-session cap counts it, and the
/// startup check settles what it finds. Folded rather than stored because the
/// log already says both halves, and a second record of the same thing is a
/// second thing to keep in sync.
pub fn unsettled(events: &[SessionEvent]) -> Vec<UnsettledTask> {
    let mut open: Vec<UnsettledTask> = Vec::new();
    for event in events {
        match &event.kind {
            SessionEventKind::TaskSpawned(spawned) => open.push(UnsettledTask {
                task_id: spawned.task_id.clone(),
                turn_id: spawned.turn_id.clone(),
                kind: spawned.kind,
                label: spawned.label.clone(),
            }),
            SessionEventKind::TaskSettled(settled) => {
                open.retain(|task| task.task_id != settled.task_id)
            }
            _ => {}
        }
    }
    open
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::session_event::{
        SessionEvent, TaskSettledEvent, TaskSpawnedEvent, ToolOutcome,
    };

    fn event(seq: u64, kind: SessionEventKind) -> SessionEvent {
        SessionEvent {
            version: crate::domain::session_event::SESSION_EVENT_VERSION,
            seq,
            at: time::OffsetDateTime::from_unix_timestamp(1_700_000_000 + seq as i64).unwrap(),
            kind,
            ignorable: false,
        }
    }

    fn spawned(seq: u64, task_id: &str) -> SessionEvent {
        event(
            seq,
            SessionEventKind::TaskSpawned(TaskSpawnedEvent {
                turn_id: "t1".into(),
                task_id: task_id.into(),
                kind: TaskKind::Shell,
                label: "sleep 1".into(),
            }),
        )
    }

    fn settled(seq: u64, task_id: &str) -> SessionEvent {
        event(
            seq,
            SessionEventKind::TaskSettled(TaskSettledEvent {
                task_id: task_id.into(),
                outcome: ToolOutcome::Succeeded,
                result_ref: String::new(),
                summary: "done".into(),
                elapsed_ms: 10,
            }),
        )
    }

    #[test]
    fn running_is_what_the_log_started_and_never_finished() {
        let log = vec![spawned(1, "a"), spawned(2, "b"), settled(3, "a")];
        let open = unsettled(&log);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].task_id, "b");
    }

    /// A settle may arrive long after the turn that spawned it ended — and out
    /// of spawn order, since tasks finish when they finish.
    #[test]
    fn a_settle_matches_by_id_not_by_adjacency() {
        let log = vec![
            spawned(1, "a"),
            spawned(2, "b"),
            settled(3, "b"),
            settled(4, "a"),
        ];
        assert!(unsettled(&log).is_empty());
    }
}
