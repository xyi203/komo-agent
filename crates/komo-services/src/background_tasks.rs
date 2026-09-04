//! Who holds a background task while it runs, and what happens when it settles
//! (docs/bot-runtime.md §5.9).
//!
//! The tool builds the work and hands it over; everything after belongs here:
//! the id, the two log events, the full output in the tool-output store, and
//! the wake that tells somebody the work is done. One implementation for
//! `shell` and `delegate` alike, because "a turn started something and walked
//! away" is one thing however the work is shaped.
//!
//! **The work runs in a task of the process's own**, not the turn's. That is
//! the point: the executor aborts a call at its wall-clock limit and the loop
//! ends the turn, and neither may reach into work that was explicitly detached
//! from both. It also decides the restart rule — the tasks die with the
//! process, so [`reconcile_orphans`](BackgroundTaskRuntime::reconcile_orphans)
//! settles every one it finds still open as `Uncertain` and never re-runs it.
//!
//! Settling wakes somebody in exactly one of three ways, in this order:
//!
//! 1. A turn suspended on `wait { for_task }` — a standing registration with a
//!    turn — is continued, handed the result.
//! 2. That registration exists but its turn is no longer waiting (it failed, or
//!    something else woke it): the result still has to arrive, so it opens a
//!    fresh turn instead of being dropped.
//! 3. Nobody registered at all — the ordinary case, since a turn that spawns a
//!    task usually just finishes: a fresh turn on the same session, told what
//!    finished and what it produced.
//!
//! The claim comes first in every case (`take` answers `false` when the row is
//! already gone), so a settle racing the sweep wakes the turn once.

use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tracing::{info, warn};

use komo_core::domain::{
    background::{
        BackgroundTasks, MAX_BACKGROUND_TASKS_PER_SESSION, TaskSpec, TaskWork, unsettled,
    },
    repository::SessionEventRepository,
    run::RunStatus,
    run_projection::project_runs,
    session_event::{
        SessionEventKind, TaskSettledEvent, TaskSpawnedEvent, ToolOutcome, Wakeup, WakeupCause,
    },
    wakeup::{WakeupDispatch, WakeupRegistration, WakeupRepository},
};

use crate::tool_output_store::ToolOutputStore;

/// How many recent sessions the restart check reads, mirroring the
/// suspended-turn check beside it: the sessions worth looking at are the ones
/// that were live when the process died.
pub const ORPHAN_RECHECK_SESSIONS: usize = 20;

/// Shared by every executor that has one *and* by each detached task, which is
/// why the state sits behind a single `Arc`: a task started an hour ago and the
/// executor that started it read the same dispatch, not two copies of it.
struct Inner {
    events: Arc<dyn SessionEventRepository>,
    wakeups: Arc<dyn WakeupRepository>,
    outputs: Arc<ToolOutputStore>,
    /// Whoever knows how to wake a turn. Attached after construction, because
    /// the dispatcher that implements it needs the runtime whose executor holds
    /// this — the same late binding the sweep's `WakeupWiring` has. Absent ⇒ a
    /// task still settles in the log, and nothing is woken.
    dispatch: RwLock<Option<Arc<dyn WakeupDispatch>>>,
}

pub struct BackgroundTaskRuntime {
    inner: Arc<Inner>,
}

impl BackgroundTaskRuntime {
    pub fn new(
        events: Arc<dyn SessionEventRepository>,
        wakeups: Arc<dyn WakeupRepository>,
        outputs: Arc<ToolOutputStore>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                events,
                wakeups,
                outputs,
                dispatch: RwLock::new(None),
            }),
        }
    }

    /// Install who wakes a turn when a task settles. Called once, during
    /// gateway wiring, as soon as the dispatcher exists.
    pub fn attach_dispatch(&self, dispatch: Arc<dyn WakeupDispatch>) {
        *self.inner.dispatch.write().unwrap() = Some(dispatch);
    }

    /// Settle every task a dead process left running, as `Uncertain`.
    ///
    /// Not "failed": the process group died with the process, and whether the
    /// command finished first is not knowable. That is the same claim a tool
    /// call makes when it cannot confirm its own effect, and it has to reach
    /// the model — a turn waiting on one is woken and told, and nothing is
    /// re-run.
    ///
    /// Best-effort throughout; nothing here may keep the gateway from starting.
    pub async fn reconcile_orphans(&self, limit: usize, now: i64) -> usize {
        self.inner.reconcile_orphans(limit, now).await
    }
}

impl Inner {
    async fn reconcile_orphans(&self, limit: usize, now: i64) -> usize {
        let ids = match self.events.session_ids().await {
            Ok(ids) => ids,
            Err(error) => {
                warn!(%error, "could not list sessions; skipping the background-task check");
                return 0;
            }
        };
        // Newest first — the ids are UUIDv7, so their order is chronological.
        let recent: Vec<String> = ids.into_iter().rev().take(limit).collect();
        let mut settled = 0;
        for session_id in recent {
            let log = match self.events.events(&session_id).await {
                Ok(log) => log,
                Err(error) => {
                    warn!(%error, session = %session_id, "could not read a session log; skipping it");
                    continue;
                }
            };
            for task in unsettled(&log) {
                warn!(
                    session = %session_id,
                    task = %task.task_id,
                    kind = task.kind.as_str(),
                    "settling a background task the restart lost as uncertain"
                );
                self.settle(
                    &session_id,
                    &task.task_id,
                    &task.label,
                    TaskSettledEvent {
                        task_id: task.task_id.clone(),
                        outcome: ToolOutcome::Uncertain,
                        result_ref: String::new(),
                        summary: format!(
                            "komo restarted while this task was running, so whether \
                             `{}` finished is unknown. Nothing was re-run — check the \
                             effect before acting on it.",
                            task.label
                        ),
                        elapsed_ms: 0,
                    },
                    now,
                )
                .await;
                settled += 1;
            }
        }
        settled
    }

    /// Write the settle, then wake whoever was waiting.
    async fn settle(
        &self,
        session_id: &str,
        task_id: &str,
        label: &str,
        event: TaskSettledEvent,
        now: i64,
    ) {
        let summary = event.summary.clone();
        let result_ref = event.result_ref.clone();
        let outcome = event.outcome;
        if let Err(error) = self
            .events
            .append(session_id, vec![SessionEventKind::TaskSettled(event)])
            .await
        {
            warn!(%error, task = %task_id, "failed to record a background task's settle");
            return;
        }
        if let Err(error) = self.events.durable_flush(session_id).await {
            warn!(%error, task = %task_id, "a background task's settle is not durable");
        }
        self.wake(
            session_id,
            task_id,
            label,
            outcome,
            &summary,
            &result_ref,
            now,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn wake(
        &self,
        session_id: &str,
        task_id: &str,
        label: &str,
        outcome: ToolOutcome,
        summary: &str,
        result_ref: &str,
        now: i64,
    ) {
        let Some(dispatch) = self.dispatch.read().unwrap().clone() else {
            info!(task = %task_id, "a background task settled with nothing wired to wake");
            return;
        };

        // A standing wait for exactly this task, claimed before anything is
        // fired: a sweep expiring it at the same moment must not fire it too.
        let waiting = match self.wakeups.list().await {
            Ok(rows) => rows
                .into_iter()
                .find(|r| matches!(&r.wakeup, Wakeup::TaskDone { task_id: id } if id == task_id)),
            Err(error) => {
                warn!(%error, task = %task_id, "could not read standing waits for a settled task");
                None
            }
        };
        if let Some(registration) = &waiting {
            match self.wakeups.take(&registration.id).await {
                Ok(true) => {}
                // Somebody else got there first. Not ours to fire, and not ours
                // to report either — they are reporting it.
                Ok(false) => {
                    info!(task = %task_id, "a standing wait for this task was already claimed");
                    return;
                }
                Err(error) => {
                    warn!(%error, task = %task_id, "could not claim a standing wait")
                }
            }
        }

        // The turn's own wake: it is parked on this call and comes back to it.
        if let Some(registration) = waiting.as_ref()
            && self.still_waiting(registration).await
        {
            let payload = continuation_payload(outcome, summary, result_ref);
            match dispatch
                .fire(registration, WakeupCause::Task, &payload)
                .await
            {
                Ok(()) => info!(task = %task_id, "woke the turn waiting on a background task"),
                Err(error) => warn!(%error, task = %task_id, "failed to wake a waiting turn"),
            }
            return;
        }

        // Nobody is parked on it (or whoever was has moved on), so the result
        // opens a turn of its own. The registration is built and fired, never
        // stored: nothing would ever come back for a row written here, and a
        // `TaskDone` row has no clock to expire it.
        let fresh = WakeupRegistration::new(
            session_id,
            Wakeup::TaskDone {
                task_id: task_id.to_string(),
            },
            now,
        );
        let prompt = fresh_turn_prompt(label, outcome, summary, result_ref);
        match dispatch.fire(&fresh, WakeupCause::Task, &prompt).await {
            Ok(()) => info!(task = %task_id, "opened a turn with a background task's result"),
            Err(error) => warn!(%error, task = %task_id, "failed to report a background task"),
        }
    }

    /// Whether the log still says this registration's turn is parked.
    ///
    /// Same rule the sweep applies before firing: a registration says *when* to
    /// come back, never what the turn is doing, and continuing a turn that
    /// already moved on would redo work it has done. A registration with no
    /// turn, and an unreadable log, both answer no — the result then arrives as
    /// a fresh turn, which says the same thing without redoing anything.
    async fn still_waiting(&self, registration: &WakeupRegistration) -> bool {
        let Some(turn_id) = &registration.turn_id else {
            return false;
        };
        let Ok(events) = self.events.events(&registration.session_id).await else {
            return false;
        };
        project_runs(&registration.session_id, &events)
            .iter()
            .find(|projected| projected.run.id == *turn_id)
            .is_some_and(|projected| projected.run.status == RunStatus::Suspended)
    }
}

/// What the `wait` call that stopped is handed on its way back.
fn continuation_payload(outcome: ToolOutcome, summary: &str, result_ref: &str) -> String {
    let mut text = format!("[{}] {summary}", outcome_word(outcome));
    if !result_ref.is_empty() {
        text.push_str(&format!("\nFull output: {result_ref}"));
    }
    text
}

/// The message a task's result arrives as when no turn is waiting for it.
fn fresh_turn_prompt(label: &str, outcome: ToolOutcome, summary: &str, result_ref: &str) -> String {
    let mut text = format!(
        "The background task you started earlier (`{label}`) has finished — {}.\n\n{summary}",
        outcome_word(outcome)
    );
    if !result_ref.is_empty() {
        text.push_str(&format!("\n\nFull output: {result_ref}"));
    }
    text
}

fn outcome_word(outcome: ToolOutcome) -> &'static str {
    match outcome {
        ToolOutcome::Succeeded => "ok",
        ToolOutcome::Failed => "failed",
        ToolOutcome::Uncertain => "uncertain — it may or may not have taken effect",
        ToolOutcome::Denied => "refused",
    }
}

#[async_trait]
impl BackgroundTasks for BackgroundTaskRuntime {
    async fn spawn(
        &self,
        session_id: &str,
        turn_id: &str,
        spec: TaskSpec,
        work: TaskWork,
    ) -> anyhow::Result<String> {
        // The cap is a fold, not a counter: "still running" is a `task/spawned`
        // the log has no `task/settled` for, which stays true across a restart
        // and whoever ends up settling the task.
        let log = self
            .inner
            .events
            .events(session_id)
            .await
            .unwrap_or_default();
        let running = unsettled(&log).len();
        if running >= MAX_BACKGROUND_TASKS_PER_SESSION {
            anyhow::bail!(
                "this conversation already has {running} background tasks running (the limit \
                 is {MAX_BACKGROUND_TASKS_PER_SESSION}). Wait for one to finish — `wait` with \
                 its `for_task` id — or run this in the foreground instead."
            );
        }

        let task_id = uuid::Uuid::now_v7().to_string();
        self.inner
            .events
            .append(
                session_id,
                vec![SessionEventKind::TaskSpawned(TaskSpawnedEvent {
                    turn_id: turn_id.to_string(),
                    task_id: task_id.clone(),
                    kind: spec.kind,
                    label: spec.label.clone(),
                })],
            )
            .await?;
        // Durable before the work starts, for the same reason a round's dispatch
        // intent is: a crash in the gap would leave work running that nothing
        // knows about, which is the one thing the restart check cannot repair.
        self.inner.events.durable_flush(session_id).await?;

        let inner = self.inner.clone();
        let session = session_id.to_string();
        let id = task_id.clone();
        let label = spec.label;
        let kind = spec.kind;
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let report = work.await;
            let elapsed_ms = started.elapsed().as_millis() as i64;
            let result_ref = inner
                .outputs
                .store(&session, &id, &report.full)
                .map(|path| path.display().to_string())
                .unwrap_or_default();
            let now = time::OffsetDateTime::now_utc().unix_timestamp();
            inner
                .settle(
                    &session,
                    &id,
                    &label,
                    TaskSettledEvent {
                        task_id: id.clone(),
                        outcome: report.outcome,
                        result_ref,
                        summary: report.summary,
                        elapsed_ms,
                    },
                    now,
                )
                .await;
            info!(task = %id, kind = kind.as_str(), elapsed_ms, "a background task settled");
        });
        Ok(task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_uncertain_task_says_so_where_the_model_will_read_it() {
        let text = continuation_payload(ToolOutcome::Uncertain, "komo restarted", "");
        assert!(text.contains("uncertain"), "{text}");
        assert!(text.contains("may or may not"), "{text}");
    }

    #[test]
    fn a_fresh_turn_names_the_task_and_points_at_its_output() {
        let text = fresh_turn_prompt(
            "cargo test",
            ToolOutcome::Succeeded,
            "exit 0",
            "/tmp/out.txt",
        );
        assert!(text.contains("cargo test"), "{text}");
        assert!(text.contains("/tmp/out.txt"), "{text}");
    }
}
