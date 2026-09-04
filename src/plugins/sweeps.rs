//! Scheduled maintenance as plugins — one per sweep, so `[plugins.dream]
//! enabled = false` and friends mean the same thing they mean for every other
//! plugin. Schedules are parsed by the host (the startup banner and the
//! sweeps must never disagree about what's in effect) and ride in on
//! [`SweepCx`].

use std::sync::Arc;

use async_trait::async_trait;

use komo_agent::daemon::{
    BriefingSweep, DreamSweep, Maintenance, MemoryMonitorSweep, ReminderSweep, ReviewSweep,
    Schedule, TaskSweep, WorkdayGated,
};
use komo_agent::gateway::MaintenanceService;
use komo_infra::workday::HolidayCalendar;

use super::{Plugin, SweepCx, SweepRegistry};
use crate::domain::briefing::BriefingMarkRepository;
use crate::domain::reminder::ReminderRepository;
use crate::domain::task::TaskRepository;

pub struct ReviewPlugin;

#[async_trait]
impl Plugin for ReviewPlugin {
    fn name(&self) -> &'static str {
        "review"
    }

    async fn setup_sweeps(&self, reg: &mut SweepRegistry, cx: &SweepCx<'_>) -> anyhow::Result<()> {
        reg.sweep(MaintenanceService {
            name: "review".to_string(),
            schedule: cx.maintenance_schedule.clone(),
            maintenance: Arc::new(ReviewSweep {
                review: cx.review.clone(),
            }),
            alert: Some(cx.notifier.clone()),
        });
        Ok(())
    }
}

pub struct RemindersPlugin;

#[async_trait]
impl Plugin for RemindersPlugin {
    fn name(&self) -> &'static str {
        "reminders"
    }

    async fn setup_sweeps(&self, reg: &mut SweepRegistry, cx: &SweepCx<'_>) -> anyhow::Result<()> {
        let reminders: Arc<dyn ReminderRepository> = cx.db.clone();
        reg.sweep(MaintenanceService {
            name: "reminders".to_string(),
            schedule: Schedule::parse("* * * * *")?,
            maintenance: Arc::new(ReminderSweep {
                reminders,
                notifier: cx.notifier.clone(),
            }),
            alert: Some(cx.notifier.clone()),
        });
        Ok(())
    }
}

pub struct TasksPlugin;

#[async_trait]
impl Plugin for TasksPlugin {
    fn name(&self) -> &'static str {
        "tasks"
    }

    async fn setup_sweeps(&self, reg: &mut SweepRegistry, cx: &SweepCx<'_>) -> anyhow::Result<()> {
        let tasks: Arc<dyn TaskRepository> = cx.kanban.clone();
        reg.sweep(MaintenanceService {
            name: "tasks".to_string(),
            schedule: Schedule::parse("* * * * *")?,
            maintenance: Arc::new(TaskSweep {
                tasks,
                notifier: cx.notifier.clone(),
            }),
            alert: Some(cx.notifier.clone()),
        });
        Ok(())
    }
}

/// Always-on RSS observability. Reads only the process's own resident set, so
/// it's infallible — no breaker alert.
pub struct MemoryMonitorPlugin;

#[async_trait]
impl Plugin for MemoryMonitorPlugin {
    fn name(&self) -> &'static str {
        "memory-monitor"
    }

    async fn setup_sweeps(&self, reg: &mut SweepRegistry, _cx: &SweepCx<'_>) -> anyhow::Result<()> {
        reg.sweep(MaintenanceService {
            name: "memory-monitor".to_string(),
            schedule: Schedule::parse("*/5 * * * *")?,
            maintenance: Arc::new(MemoryMonitorSweep::new()),
            alert: None,
        });
        Ok(())
    }
}

/// Daily briefing — mounts only when the user opted in with
/// `briefing_schedule`. Reads tasks + memories, composes on the aux LLM,
/// delivers via the same home notifier as reminders.
pub struct BriefingPlugin;

#[async_trait]
impl Plugin for BriefingPlugin {
    fn name(&self) -> &'static str {
        "briefing"
    }

    async fn setup_sweeps(&self, reg: &mut SweepRegistry, cx: &SweepCx<'_>) -> anyhow::Result<()> {
        let Some(schedule) = cx.briefing_schedule.clone() else {
            return Ok(());
        };
        let marks: Arc<dyn BriefingMarkRepository> = cx.db.clone();
        let mut sweep: Arc<dyn Maintenance> = Arc::new(BriefingSweep {
            tasks: cx.kanban.clone(),
            memories: cx.memories.clone(),
            llm: cx.aux_llm.clone(),
            notifier: cx.notifier.clone(),
            // Tool-capable agent turn (read-only tools + unattended policy
            // gating); the sweep degrades to the plain compose on error.
            runtime: Some(cx.briefing_runtime.clone()),
            marks: Some(marks.clone()),
        });
        // Opt-in: only fire on Chinese working days (statutory holidays and
        // 调休-adjusted weekends respected). The calendar is built only when
        // gating is on, so the holiday API is never touched otherwise.
        if cx.config.runtime.briefing_workdays_only {
            let calendar = Arc::new(HolidayCalendar::new(komo_config::workday_cache_dir()));
            sweep = Arc::new(WorkdayGated {
                inner: sweep,
                calendar,
            });
        }
        // Startup catch-up: a gateway that was down (restart, upgrade) across
        // today's slot runs the briefing late, once — the same rule a cron job
        // gets from its stored `next_run_at`. Goes through the workday-gated
        // sweep, so a holiday still skips it.
        if let Some(expr) = cx.briefing_expr.clone() {
            let catch_up = sweep.clone();
            tokio::spawn(async move {
                let handled = marks.last_handled().await.unwrap_or_default();
                if komo_agent::daemon::briefing_catchup_due(
                    &expr,
                    handled.as_deref(),
                    chrono::Local::now(),
                ) {
                    tracing::info!(
                        "briefing: today's slot passed while the gateway was down; catching up"
                    );
                    if let Err(error) = catch_up.run().await {
                        tracing::warn!(%error, "briefing catch-up failed");
                    }
                }
            });
        }
        reg.sweep(MaintenanceService {
            name: "briefing".to_string(),
            schedule,
            maintenance: sweep,
            alert: Some(cx.notifier.clone()),
        });
        Ok(())
    }
}

/// Routines (`komo cron add`, stored in `cron_job_records`): one every-minute
/// sweep reads the store and executes the ones whose slot has come, so jobs
/// added/removed/toggled while the gateway runs take effect on the next tick —
/// no restart.
///
/// It is the *clock* half only: the event-triggered routines (§5.12–5.14) fire
/// from their own ingresses, through the same shared
/// [`RoutineEventSource`](komo_agent::daemon::RoutineEventSource) the host
/// built and handed here.
pub struct CronJobsPlugin;

#[async_trait]
impl Plugin for CronJobsPlugin {
    fn name(&self) -> &'static str {
        "cron-jobs"
    }

    async fn setup_sweeps(&self, reg: &mut SweepRegistry, cx: &SweepCx<'_>) -> anyhow::Result<()> {
        reg.sweep(MaintenanceService {
            name: "cron-jobs".to_string(),
            schedule: Schedule::parse("* * * * *")?,
            maintenance: Arc::new(cx.routines.sweep()),
            alert: Some(cx.notifier.clone()),
        });
        Ok(())
    }
}

/// Dreaming — mounts only when `dream_schedule` is in effect. Reads the whole
/// memory library, promotes well-supported candidates, archives cold and refuted
/// ones, and withdraws skill proposals nobody ruled on.
pub struct DreamPlugin;

#[async_trait]
impl Plugin for DreamPlugin {
    fn name(&self) -> &'static str {
        "dream"
    }

    async fn setup_sweeps(&self, reg: &mut SweepRegistry, cx: &SweepCx<'_>) -> anyhow::Result<()> {
        let Some(schedule) = cx.dream_schedule.clone() else {
            return Ok(());
        };
        reg.sweep(MaintenanceService {
            name: "dreaming".to_string(),
            schedule,
            maintenance: Arc::new(DreamSweep {
                memories: cx.memories.clone(),
                skills: cx.skill_store.clone(),
            }),
            alert: Some(cx.notifier.clone()),
        });
        Ok(())
    }
}
