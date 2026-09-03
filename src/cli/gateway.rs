use komo_agent::daemon::Schedule;
use komo_agent::gateway::Gateway;
use komo_agent::interaction::{ApprovalState, ChatApprover, GatewayDispatcher};
use komo_infra::persistence::db::Db;
use std::sync::Arc;

use crate::{
    cli::wiring,
    domain::{
        approval::Approver, cron::CronJobRepository, gateway::MessageHandler, home::HomeRepository,
        notify::Notifier, pairing::PairingRepository, repository::SessionRepository,
        run::RunRepository, task::TaskRepository, todo::SessionTodoRepository,
    },
    infra::messaging::{
        api::ApiChannel, home_notifier::HomeNotifier, macos_notifier::MacosNotifier,
    },
    plugins::{self, ChannelCx, ChannelRegistry, SweepCx, SweepRegistry},
    services::operator_control::actions::OperatorActions,
};
use komo_config::{ConfigSnapshot, IssueSeverity};

/// Run the always-on gateway: a persistent process hosting the maintenance
/// scheduler and the config-declared ingress channels. Runs until Ctrl-C.
/// Everything is read from the caller's one resolved `config` snapshot.
///
/// Channels and sweeps come from the plugin roster (`crate::plugins`, phases
/// 2 and 3); the host keeps what plugins depend on or what must never be
/// disableable — storage, the dispatcher, the home notifier, and the api
/// channel (the CLI's only path to a running gateway).
pub async fn run(config: &ConfigSnapshot) -> anyhow::Result<()> {
    // The gateway hosts every surface, so any fatal config issue (unusable
    // model, enabled-but-credential-less channel) stops startup here, before
    // the db is opened. Warnings are logged and tolerated.
    config.validate_gateway()?;
    for issue in &config.report.issues {
        if issue.severity == IssueSeverity::Warning {
            tracing::warn!(path = issue.path, "{}", issue.message);
        }
    }
    let rt = &config.runtime;

    // A cron typo must not crash-loop the always-on gateway (same principle as
    // the missing-credential warnings above): the maintenance schedule degrades
    // to the built-in default cadence, an opt-in sweep (briefing/dream) is
    // disabled — each with a warning naming the bad expression. Parsed here,
    // once, so the startup banner and the sweeps can never disagree.
    let (review_schedule, schedule_expr) = schedule_or_default(&rt.maintenance_schedule);
    let (briefing_schedule, briefing_expr) =
        optional_schedule(rt.briefing_schedule.as_deref(), "briefing_schedule");
    let (dream_schedule, dream_expr) =
        optional_schedule(rt.dream_schedule.as_deref(), "dream_schedule");

    let db = Arc::new(Db::connect(&rt.db_url).await?);
    // Reconcile runs left `Running` by a crashed earlier process (launchd
    // restarts the gateway): flip them to failed/"interrupted" so the ledger is
    // truthful. Best-effort — a reconciliation failure must not block startup.
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    match RunRepository::reconcile_interrupted(&*db, now).await {
        Ok(0) => {}
        Ok(n) => tracing::info!(count = n, "reconciled interrupted runs on startup"),
        Err(error) => tracing::warn!(%error, "failed to reconcile interrupted runs"),
    }
    // Tasks and cron jobs are tables in the same database now (docs/adr/0004);
    // the sweeps still take them as their own repositories.
    let kanban: Arc<dyn TaskRepository> = db.clone();
    let cron_jobs: Arc<dyn CronJobRepository> = db.clone();

    // Tool actions that need approval are gated over the chat channel: the
    // agent sends an approval prompt and waits for the user's `/approve` (or
    // `/deny`) reply. Shared with the dispatcher so the reply resolves the wait.
    let approvals = Arc::new(ApprovalState::new());
    let approver: Arc<dyn Approver> = Arc::new(ChatApprover::new(approvals.clone()));
    let mut wired = wiring::build(config, db.clone(), approver).await?;

    // Expire stored tool outputs once, here. Not a `Maintenance` sweep on
    // purpose: the list of scheduled sweeps is long already, and a scratch file
    // living a few hours past its week costs nothing (the store also re-sweeps
    // at most hourly whenever it writes).
    match wired.output_store.sweep() {
        0 => {}
        n => tracing::info!(removed = n, "expired stored tool outputs"),
    }

    let roster = plugins::builtin();
    let gate = plugins::PluginGate::new(config, &roster);

    // ── Plugin phase 2: ingress channels ─────────────────────────────────────
    // Senders outside `allow_from` go through the pairing handshake; the
    // pairing store is shared with the `komo pair` CLI via the same db.
    let pairings: Arc<dyn PairingRepository> = db.clone();
    let mut channel_reg = ChannelRegistry::default();
    let channel_cx = ChannelCx {
        config,
        pairings: pairings.clone(),
    };
    plugins::run_channel_phase(&roster, &gate, &mut channel_reg, &channel_cx).await?;

    // A single home notifier delivers all proactive output (reminders, task
    // due notices, the shutdown notice). It resolves the home chat at
    // notify-time — a `/sethome` override (db) wins over the config `home_chat`
    // (plugin order preserves the feishu-first priority) — and degrades to the
    // local macOS notifier when no chat home resolves.
    let config_home = channel_reg.config_home();
    let home_repo: Arc<dyn HomeRepository> = db.clone();
    let notifier: Arc<dyn Notifier> = Arc::new(HomeNotifier::new(
        channel_reg.senders(),
        home_repo.clone(),
        config_home.clone(),
        Arc::new(MacosNotifier),
    ));

    // Taken before the runtime moves into the handler: /health reports what the
    // main catalog has mounted, live.
    let tool_catalog = wired.runtime.tool_executor.catalog().clone();
    let handler: Arc<dyn MessageHandler> = Arc::new(wired.runtime);
    let sessions: Arc<dyn SessionRepository> = db.clone();
    let todos: Arc<dyn SessionTodoRepository> = db.clone();
    let dispatcher = Arc::new(GatewayDispatcher::new(
        handler.clone(),
        approvals.clone(),
        wired.clarify.clone(),
        sessions,
        home_repo,
        todos,
        channel_reg.wechat_login.clone(),
        db.clone(),
        db.clone(),
    ));

    // ── Plugin phase 3: scheduled sweeps ─────────────────────────────────────
    let mut sweep_reg = SweepRegistry::default();
    let sweep_cx = SweepCx {
        config,
        db: db.clone(),
        kanban: kanban.clone(),
        cron_jobs: cron_jobs.clone(),
        notifier: notifier.clone(),
        review: wired.review.clone(),
        memories: wired.memories.clone(),
        skill_store: wired.skills.clone(),
        aux_llm: wired.aux_llm.clone(),
        briefing_runtime: wired.briefing_runtime.clone(),
        cron_runtime: wired.cron_runtime.clone(),
        maintenance_schedule: review_schedule,
        briefing_schedule,
        briefing_expr: briefing_expr.clone(),
        dream_schedule,
    };
    plugins::run_sweep_phase(&roster, &gate, &mut sweep_reg, &sweep_cx).await?;

    let mut gateway = Gateway::new(dispatcher.clone());
    for service in sweep_reg.into_sweeps() {
        gateway = gateway.with_maintenance(service);
    }

    // Whether an interactive chat channel exists — gates the shutdown notice.
    // Only chat channels register in phase 2; the api channel below is not one.
    let mut channels = channel_reg.names();
    let has_chat_channel = !channel_reg.is_empty();
    for channel in channel_reg.into_channels() {
        gateway = gateway.add_channel(channel);
    }

    // For the startup banner.
    let cron_job_count = cron_jobs.list().await.map(|j| j.len()).unwrap_or(0);

    // HTTP API channel: serves the local dashboard UI and any OpenAI-compatible
    // client. It calls the handler directly (synchronous request/response), so
    // it needs the repositories rather than just the dispatcher. Host-mounted,
    // never a plugin: it is how the local `komo` CLI reaches this gateway while
    // we hold the exclusive Turso db lock, so no `[plugins]` toggle may remove
    // it. By default it is loopback-only on an ephemeral port (published in the
    // rendezvous file); `[channels.api] enabled = true` widens it to an external
    // bind/port for Open WebUI / the dashboard.
    let api = rt
        .api
        .ready()
        .ok_or_else(|| anyhow::anyhow!("api channel misconfigured"))?;
    {
        let enabled = {
            let mut names = channels.clone();
            names.push("api".to_string());
            names
        };
        // The operator use cases behind the /api/* routes — the same shared
        // definitions the CLI's direct adapter runs, here over the gateway's
        // repositories.
        let actions = Arc::new(OperatorActions {
            sessions: db.clone(),
            messages: db.clone(),
            tasks: kanban.clone(),
            memories: wired.memories.clone(),
            runs: db.clone(),
            reminders: db.clone(),
            skills: wired.skills.clone(),
            pairings: pairings.clone(),
            home: db.clone(),
            cron_jobs: cron_jobs.clone(),
            memory_query: Some(wired.memory_query.clone()),
            wiki: wired.wiki.take(),
        });
        gateway = gateway.add_channel(Box::new(ApiChannel::new(
            api,
            handler.clone(),
            dispatcher.clone(),
            actions,
            tool_catalog,
            enabled,
            config_home.clone(),
            crate::infra::messaging::api::ModelMenu::from_config(&rt.model),
            approvals.clone(),
            wired.clarify.clone(),
            rt.home.join("workspaces"),
            std::env::current_dir().unwrap_or_else(|_| rt.home.clone()),
        )));
        channels.push("api".to_string());
    }

    // Send the offline notice on shutdown only when a chat channel exists; with
    // none, the home notifier would fall back to a macOS popup, which is noise
    // on a foreground Ctrl-C.
    if has_chat_channel {
        gateway = gateway.with_shutdown_notice(notifier);
    }

    let fmt_opt = |e: &Option<String>| {
        e.as_deref()
            .map(|e| format!("`{e}`"))
            .unwrap_or_else(|| "off".to_string())
    };
    println!(
        "Komo gateway — maintenance `{}`, reminders every minute, briefing {}, dreaming {}, jobs: {}, channels: {}. Ctrl-C to stop.\n",
        schedule_expr,
        fmt_opt(&briefing_expr),
        fmt_opt(&dream_expr),
        format!("{cron_job_count} in cron.db"),
        if channels.is_empty() {
            "none".to_string()
        } else {
            channels.join(", ")
        }
    );

    gateway.run(shutdown_signal()).await
}

/// Parse the maintenance cron, degrading a typo to the built-in default
/// cadence: an always-on gateway must not crash-loop over a config typo.
/// Returns the schedule plus the expression actually in effect (for display).
fn schedule_or_default(expr: &str) -> (Schedule, String) {
    match Schedule::parse(expr) {
        Ok(schedule) => (schedule, expr.to_string()),
        Err(error) => {
            tracing::warn!(%error, default = komo_config::DEFAULT_MAINTENANCE_SCHEDULE,
                "invalid maintenance schedule; falling back to the default");
            let default = komo_config::DEFAULT_MAINTENANCE_SCHEDULE;
            (
                Schedule::parse(default).expect("built-in default cron is valid"),
                default.to_string(),
            )
        }
    }
}

/// Parse an opt-in sweep's cron; a typo disables that sweep with a warning
/// (never the whole gateway). Returns the schedule plus the effective
/// expression (`None` = the sweep is off, for the startup banner).
fn optional_schedule(expr: Option<&str>, what: &str) -> (Option<Schedule>, Option<String>) {
    match expr {
        None => (None, None),
        Some(expr) => match Schedule::parse(expr) {
            Ok(schedule) => (Some(schedule), Some(expr.to_string())),
            Err(error) => {
                tracing::warn!(%error, config = what, "invalid schedule; sweep disabled");
                (None, None)
            }
        },
    }
}

/// Resolve when the process is asked to stop. Catches both Ctrl-C (SIGINT, the
/// foreground case) and SIGTERM — the signal `launchctl bootout` sends when
/// `komo gateway stop`/`restart` tears the job down. Without the SIGTERM arm
/// launchd would kill the process before the shutdown notice could be sent.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(term) => term,
            Err(error) => {
                tracing::warn!(%error, "failed to install SIGTERM handler; relying on Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_schedule_typo_degrades_to_default() {
        let (_, expr) = schedule_or_default("not a cron");
        assert_eq!(expr, komo_config::DEFAULT_MAINTENANCE_SCHEDULE);
        let (_, expr) = schedule_or_default("*/5 * * * *");
        assert_eq!(expr, "*/5 * * * *");
    }

    #[test]
    fn optional_schedule_typo_disables_the_sweep() {
        let (schedule, expr) = optional_schedule(Some("not a cron"), "briefing_schedule");
        assert!(schedule.is_none());
        assert!(expr.is_none());
        let (schedule, expr) = optional_schedule(Some("0 3 * * *"), "dream_schedule");
        assert!(schedule.is_some());
        assert_eq!(expr.as_deref(), Some("0 3 * * *"));
        let (schedule, _) = optional_schedule(None, "briefing_schedule");
        assert!(schedule.is_none());
    }
}
