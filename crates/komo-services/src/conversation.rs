//! `/new`: an explicit context boundary inside one conversation.
//!
//! Not a rotate. The operator's home conversation is a single ordered timeline
//! (docs/bot-runtime.md §2 D6), so drawing a line under it is one appended
//! event — nothing is archived, renamed or deleted. What the boundary decides
//! is where the model's default replay starts; everything else about the
//! session (its id, its log, its runs, a turn suspended inside it) carries on.
//!
//! Lives here because three surfaces write it — the chat `/new` command, the
//! TUI, and the api route the desktop client uses — and "which state dies with
//! a boundary" must not be answered three times.

use komo_core::domain::repository::SessionEventRepository;
use komo_core::domain::session_event::SessionEventKind;
use komo_core::domain::todo::SessionTodoRepository;

/// Draw a context boundary in `session_id`, and retire the working context that
/// belongs to the stretch of conversation it closes.
///
/// **Retired**: the todo list — a session-scoped statement of what the agent is
/// working on right now, which is exactly the thing a fresh context does not
/// inherit.
///
/// **Kept**: everything with a lifecycle of its own. Kanban tasks and memories
/// outlive any conversation; policy grants (`/approve session`) are answers
/// about what komo may do, not about what it was talking about; a suspended
/// turn and its wakeup registration are still owed an answer. `/new` used to
/// end the session, which coupled all three lifecycles to one keystroke.
pub async fn mark_boundary(
    events: &dyn SessionEventRepository,
    todos: &dyn SessionTodoRepository,
    session_id: &str,
) -> anyhow::Result<()> {
    events
        .append(
            session_id,
            vec![SessionEventKind::ConversationBoundary { turn_id: None }],
        )
        .await?;
    // Durable before it is acknowledged: a boundary the log forgets is a
    // conversation that silently keeps its old context.
    events.durable_flush(session_id).await?;
    todos.clear(session_id).await?;
    Ok(())
}
