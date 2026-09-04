//! `komo doctor` — config & gateway health aggregation (roadmap §9).
//!
//! A single read-only snapshot of "what is komo configured to do, and what is
//! missing": the active model/provider and whether its API key is present, the
//! sweep schedules, each ingress channel's enabled+credentials state, the
//! resolved home channel, and the run ledger's recent failures.
//!
//! Everything config-derived renders from the one [`ConfigSnapshot`] the whole
//! process shares — the same resolved truth the gateway boots from — so doctor
//! can never disagree with the gateway about precedence or credential
//! semantics. Resolution never aborts: problems arrive as `ConfigIssue`s and
//! are all shown, not just the first.
//!
//! The two db-backed sections (home override, run ledger) follow the standard
//! CLI read path: a reachable gateway → `GET /api/*` (it holds the exclusive
//! db lock); none → open the db directly.

use crate::infra::rendezvous;
use crate::services::operator_control::{OperatorControl, OperatorQuery, OperatorQueryResult};
use komo_config::{ChannelState, ConfigSnapshot, IssueSeverity, wechat_cred_path};

/// Status glyph for a channel/credential line.
const OK: &str = "✓";
const OFF: &str = "·";
const BAD: &str = "✗";

fn local_time(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| dt.with_timezone(&chrono::Local).to_rfc3339())
        .unwrap_or_else(|| unix.to_string())
}

pub async fn doctor(config: &ConfigSnapshot, control: &OperatorControl) -> anyhow::Result<()> {
    println!("home: {}", config.runtime.home.display());

    // The operator backend was resolved once by the caller; the db-backed
    // sections below reuse it, and the gateway line reports which side it hit.
    let health = gateway_health(control.via_gateway()).await;

    issue_health(config);
    model_health(config);
    memory_health(config, control).await;
    plugin_health(config, health.as_ref());
    schedule_health(config);
    policy_health(config, control).await;
    println!("\nchannels:");
    channel_health(config).await;
    home_channel_health(control, config).await;
    cron_health(control).await;
    run_health(control).await;
    Ok(())
}

/// Scheduled cron jobs (cron.db): count, disabled ones, and any whose last run
/// failed — the operator's "is my weekly job actually running" glance.
async fn cron_health(control: &OperatorControl) {
    use crate::domain::cron::{CronJobStatus, RoutineRunStatus};
    println!("\ncron jobs:");
    let fetched = control
        .query(OperatorQuery::CronJobs)
        .await
        .map(|r| match r {
            OperatorQueryResult::CronJobs(jobs) => jobs,
            _ => unreachable!("CronJobs query answers with CronJobs"),
        });
    let jobs = match fetched {
        Ok(jobs) => jobs,
        Err(e) => {
            println!("  {BAD} could not read cron store: {e:#}");
            return;
        }
    };
    if jobs.is_empty() {
        println!("  (none — `komo cron add <name> <schedule> <command>`)");
        return;
    }
    for job in &jobs {
        let mark = if job.status != CronJobStatus::Active {
            OFF
        } else if job.last_run().map(|r| r.status) == Some(RoutineRunStatus::Error) {
            BAD
        } else {
            OK
        };
        let state = match job.status {
            CronJobStatus::Active => format!("next {}", local_time(job.next_run_at)),
            CronJobStatus::Paused => "paused".to_string(),
            CronJobStatus::Done => "done".to_string(),
        };
        let last = match job.last_run() {
            Some(run) => format!(
                ", last {} {}",
                run.status.as_str(),
                local_time(run.started_at)
            ),
            None => String::new(),
        };
        println!(
            "  {mark} {}  [{}]  {state}{last}",
            job.name,
            job.trigger.describe()
        );
    }
}

/// Is a gateway process actually running and answering? (The channel lines
/// below describe *configuration*; this is the live process.)
async fn gateway_health(reachable: bool) -> Option<serde_json::Value> {
    match (rendezvous::read(), reachable) {
        (Some(info), true) => {
            println!(
                "\ngateway: {OK} running (pid {}, api {}:{})",
                info.pid, info.bind, info.port
            );
            let health = crate::infra::gateway_client::GatewayClient::advertised_health().await;
            // The comparison the build stamp exists for. The two processes are
            // installed by separate steps, so drift is routine — and without
            // this line it surfaces as a deserialization error somewhere deep,
            // days later, with both sides claiming "0.1.0".
            match health
                .as_ref()
                .and_then(|h| h.get("version"))
                .and_then(|v| v.as_str())
            {
                Some(server) if server != crate::cli::VERSION => println!(
                    "  {BAD} gateway is {server} but this CLI is {} — `komo gateway restart` syncs them",
                    crate::cli::VERSION
                ),
                // Two builds stamped `unknown` compare equal whether or not
                // they are the same build — which is the drift-invisibility
                // this line exists to prevent. Say so instead of vouching.
                Some(server) if server.contains("unknown") => println!(
                    "  ! version {server} on both sides, but an unknown stamp cannot \
                     tell two builds apart — build with KOMO_BUILD set (see Dockerfile)"
                ),
                Some(server) => println!("  {OK} version {server} (matches this CLI)"),
                None => println!("  ! gateway did not report a version"),
            }
            return health;
        }
        (Some(info), false) => println!(
            "\ngateway: {BAD} advertised (pid {}) but not answering — stale {} or mid-restart?",
            info.pid,
            rendezvous::path().display()
        ),
        (None, _) => println!("\ngateway: {OFF} not running (db opened directly)"),
    }
    None
}

/// The python plugin host — on by default (the gateway creates the plugins
/// directory at startup), so what needs checking is the *running* gateway's
/// live state: a config opt-out, an older gateway, or a missing interpreter
/// all leave run_code and every py__ tool silently absent, indistinguishable
/// from working unless something asks the live catalog.
fn plugin_health(config: &ConfigSnapshot, health: Option<&serde_json::Value>) {
    println!("\nplugins:");
    let dir = config.runtime.home.join("plugins");
    let plugins = health.and_then(|h| h.get("plugins"));
    let run_code = plugins
        .and_then(|p| p.get("run_code"))
        .and_then(|v| v.as_bool());
    let mounted = plugins
        .and_then(|p| p.get("tools"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    match run_code {
        Some(true) => println!(
            "  {OK} host wired, run_code + {mounted} python tool(s) mounted ({})",
            dir.display()
        ),
        // The gateway answered and run_code is not in its catalog: opted out,
        // predates default-on, or python3 is missing (the gateway log says
        // which).
        Some(false) => println!(
            "  {BAD} python host not wired — [plugins.pyhost] disabled, python3 missing, \
             or a pre-default-on gateway; `komo logs` says which, `komo gateway restart` \
             picks up changes"
        ),
        None if dir.is_dir() => {
            println!("  ! live state unknown (gateway not answering or older than this CLI)")
        }
        None => println!(
            "  {OFF} {} absent — created on the next gateway start",
            dir.display()
        ),
    }
}

/// The memory store's semantic arm: configured, and actually covering the
/// library. Both halves failed silently this year — no `[memory]` section
/// leaves recall lexical-only (cross-language recall structurally broken, no
/// error anywhere), and a store written before embeddings were configured
/// stays unembedded for weeks because backfill is lazy. Neither is visible
/// unless something counts.
async fn memory_health(config: &ConfigSnapshot, control: &OperatorControl) {
    println!("\nmemory:");
    let Some(embedding) = &config.runtime.embedding else {
        println!(
            "  ! embeddings not configured — recall is lexical-only, so a Chinese \
             question cannot reach an English memory; set [memory] embedding_model"
        );
        return;
    };
    let memories = match control.query(OperatorQuery::Memories).await {
        Ok(OperatorQueryResult::Memories(memories)) => memories,
        Ok(_) => unreachable!("Memories query answers with Memories"),
        Err(error) => {
            println!("  {BAD} could not read the memory store: {error:#}");
            return;
        }
    };
    if memories.is_empty() {
        println!("  {OK} empty store (model {})", embedding.model);
        return;
    }
    let covered = memories
        .iter()
        .filter(|m| m.embedding_for(&embedding.model).is_some())
        .count();
    if covered == memories.len() {
        println!(
            "  {OK} embeddings {covered}/{} (model {})",
            memories.len(),
            embedding.model
        );
    } else {
        println!(
            "  {BAD} embeddings {covered}/{} for model {} — run `komo memory backfill`",
            memories.len(),
            embedding.model
        );
    }
}

/// Every problem resolution recorded, fatal first in resolution order. The
/// gateway refuses to start on a fatal issue; warnings are safe to run with.
fn issue_health(config: &ConfigSnapshot) {
    let issues = &config.report.issues;
    if issues.is_empty() {
        return;
    }
    println!("\nconfig issues:");
    for issue in issues {
        let mark = match issue.severity {
            IssueSeverity::Fatal => BAD,
            IssueSeverity::Warning => "!",
        };
        println!("  {mark} {}: {}", issue.path, issue.message);
    }
}

/// The resolved provider/model and whether its credential is present.
fn model_health(config: &ConfigSnapshot) {
    // An unparsable provider is already listed under config issues; the model
    // line then shows the fallback resolution actually in effect.
    let model = &config.runtime.model;
    let provider = model.provider;
    println!("\nmodel: {} / {}", provider.name(), model.model);
    if provider.uses_api_key() {
        let has_key = config.report.key_present(provider);
        let mark = if has_key { OK } else { BAD };
        println!(
            "  {mark} {} {}",
            provider.api_key_var(),
            if has_key { "set" } else { "MISSING" }
        );
    } else {
        // Codex authenticates from ~/.codex/auth.json — validate that
        // login rather than looking for an env key.
        match komo_infra::codex::CodexAuth::load() {
            Ok(_) => println!("  {OK} Codex OAuth (~/.codex/auth.json)"),
            Err(e) => println!("  {BAD} Codex auth: {e}"),
        }
    }
}

/// Maintenance cron, daily briefing (opt-in), dreaming, and the workday gate.
fn schedule_health(config: &ConfigSnapshot) {
    let rt = &config.runtime;
    println!("\nsweeps:");
    println!("  maintenance  {}", rt.maintenance_schedule);
    match &rt.briefing_schedule {
        Some(s) => {
            let gate = if rt.briefing_workdays_only {
                " (Chinese workdays only)"
            } else {
                ""
            };
            println!("  briefing     {s}{gate}");
        }
        None => println!("  briefing     {OFF} disabled (set briefing_schedule to enable)"),
    }
    match &rt.dream_schedule {
        Some(s) => println!("  dreaming     {s}"),
        None => println!("  dreaming     {OFF} disabled"),
    }
    println!("  reminders    every minute");
    println!("  tasks        every minute");
    println!("  cron jobs    every minute (see `komo cron list`)");
}

/// The permission policy: configured?, rule count, load errors, and the two
/// runtime grant sources (saved prompts, scheduled jobs).
async fn policy_health(config: &ConfigSnapshot, control: &OperatorControl) {
    use crate::domain::policy::{PolicyMode, Verdict};
    let report = &config.runtime.policy;
    println!("\npolicy:");
    // Saved grants are reported whether or not a [policy] table exists — they are
    // accumulated at runtime, so an operator with no config can still have them.
    let saved = komo_infra::permissions_store::PermissionsStore::load(&config.runtime.home);
    if !saved.is_empty() {
        println!(
            "  {OK} {} saved grant(s) from approval prompts  (see `komo policy saved list`)",
            saved.len()
        );
    }
    // Job grants likewise: a job created purely in conversation carries
    // unattended permissions with no [policy] table anywhere.
    if let Ok(OperatorQueryResult::CronJobs(jobs)) = control.query(OperatorQuery::CronJobs).await {
        let granting = jobs.iter().filter(|j| !j.grants.is_empty()).count();
        if granting > 0 {
            println!(
                "  {OK} {granting} job(s) with their own unattended grants  \
                 (see `komo policy list`)"
            );
        }
    }
    if !report.configured {
        println!("  {OFF} no [policy] table — Normal/Dangerous actions ask interactively");
        return;
    }
    let d = match report.policy.default_normal() {
        Verdict::Allow => "allow",
        Verdict::Deny => "deny",
        Verdict::Ask => "ask",
    };
    println!(
        "  {OK} {} rule(s), default_normal = {d}  (see `komo policy list`)",
        report.policy.rules().len()
    );
    // Worth a line of its own: in auto mode a prompt the rules produced may be
    // answered by the aux reviewer instead of reaching the operator, which is
    // exactly the kind of thing someone reads `doctor` to find out.
    if report.mode == PolicyMode::Auto {
        println!("  {OK} mode = auto — the aux reviewer may auto-allow prompts (never deny)");
    }
    if !report.invalid.is_empty() {
        println!(
            "  {BAD} {} invalid rule(s) ignored — fix [[policy.rule]] in config.toml",
            report.invalid.len()
        );
    }
}

/// One line per ingress channel: enabled?, credentials present?
async fn channel_health(config: &ConfigSnapshot) {
    let rt = &config.runtime;
    // Enabled is a statement about the config; these lines are about the world.
    // A channel whose credential the platform rejects on every poll must not
    // print {OK} — that is how a dead telegram token stayed invisible for a day.
    match &rt.feishu {
        ChannelState::Ready(_) => match super::channel::check_feishu_live(config).await {
            Ok(()) => println!("  {OK} {:<14} enabled, credentials accepted", "feishu"),
            Err(e) => println!("  {BAD} {:<14} enabled but failing: {e:#}", "feishu"),
        },
        ChannelState::Disabled => println!("  {OFF} {:<14} disabled", "feishu"),
        ChannelState::Misconfigured(e) => println!("  {BAD} {:<14} {e}", "feishu"),
    }
    match &rt.telegram {
        ChannelState::Ready(_) => match super::channel::check_telegram_live(config).await {
            Ok(bot) => println!("  {OK} {:<14} enabled, @{bot} answers", "telegram"),
            Err(e) => println!("  {BAD} {:<14} enabled but failing: {e:#}", "telegram"),
        },
        ChannelState::Disabled => println!("  {OFF} {:<14} disabled", "telegram"),
        ChannelState::Misconfigured(e) => println!("  {BAD} {:<14} {e}", "telegram"),
    }
    // The api channel is always on (it's how the CLI reaches a running gateway);
    // `enabled` only widens it from loopback-only to externally reachable.
    match &rt.api {
        ChannelState::Ready(cfg) if cfg.port != 0 => {
            println!(
                "  {OK} {:<14} enabled (external {}:{})",
                "api", cfg.bind, cfg.port
            )
        }
        ChannelState::Ready(_) => println!("  {OK} {:<14} on (loopback-only, CLI)", "api"),
        ChannelState::Misconfigured(e) => println!("  {BAD} {:<14} {e}", "api"),
        ChannelState::Disabled => unreachable!("the api channel is always on"),
    }

    // WeChat resolves with no credential check (login is QR-based, creds in a
    // separate file), so verify the file ourselves.
    match &rt.wechat {
        ChannelState::Ready(_) => {
            if wechat_cred_path().exists() {
                println!("  {OK} {:<14} enabled", "wechat");
            } else {
                println!(
                    "  {BAD} {:<14} enabled but not logged in (run `komo channel wechat login`)",
                    "wechat"
                );
            }
        }
        ChannelState::Disabled => println!("  {OFF} {:<14} disabled", "wechat"),
        ChannelState::Misconfigured(e) => println!("  {BAD} {:<14} {e}", "wechat"),
    }

    // The homeassistant tool (agent queries/controls HA on demand).
    let ha_tool = if rt.homeassistant_tool.is_some() {
        format!("{OK} HASS_TOKEN set")
    } else {
        format!("{OFF} HASS_TOKEN unset (homeassistant tool not registered)")
    };
    println!("  {ha_tool}");
}

/// Resolved proactive-output home: the `/sethome` runtime override (db) wins
/// over the config `home_chat` fallback (feishu first).
async fn home_channel_health(control: &OperatorControl, config: &ConfigSnapshot) {
    println!("\nhome channel (proactive output):");
    let over = control
        .query(OperatorQuery::HomeOverride)
        .await
        .map(|r| match r {
            OperatorQueryResult::HomeOverride(over) => over,
            _ => unreachable!("HomeOverride query answers with HomeOverride"),
        });
    match over {
        Ok(Some(session)) => println!("  {OK} /sethome override → {session}"),
        Ok(None) => match config_home_chat(config) {
            Some((platform, chat)) => {
                println!("  {OK} config home_chat → {platform}:{chat}")
            }
            None => {
                println!("  {OFF} none set — proactive output falls back to the macOS notifier")
            }
        },
        Err(e) => println!("  {BAD} could not read home setting: {e:#}"),
    }
}

/// The config `home_chat` fallback, feishu-first (matches `HomeNotifier`).
fn config_home_chat(config: &ConfigSnapshot) -> Option<(&'static str, String)> {
    let rt = &config.runtime;
    if let Some(chat) = rt.feishu.ready().and_then(|c| c.home_chat.clone()) {
        return Some(("feishu", chat));
    }
    if let Some(chat) = rt.telegram.ready().and_then(|c| c.home_chat.clone()) {
        return Some(("telegram", chat));
    }
    if let Some(chat) = rt.wechat.ready().and_then(|c| c.home_chat.clone()) {
        return Some(("wechat", chat));
    }
    None
}

/// Recent run-ledger health: how many of the last 50 turns failed, with the
/// most recent few. The roadmap §9 "last error" view.
async fn run_health(control: &OperatorControl) {
    println!("\nrecent runs:");
    let fetched = control
        .query(OperatorQuery::Runs { limit: 50 })
        .await
        .map(|r| match r {
            OperatorQueryResult::Runs(runs) => runs,
            _ => unreachable!("Runs query answers with Runs"),
        });
    let runs = match fetched {
        Ok(r) => r,
        Err(e) => {
            println!("  {BAD} could not read run ledger: {e:#}");
            return;
        }
    };
    if runs.is_empty() {
        println!("  (no runs recorded)");
        return;
    }
    let failed: Vec<_> = runs
        .iter()
        .filter(|r| r.status == crate::domain::run::RunStatus::Failed)
        .collect();
    println!("  last {} turns, {} failed", runs.len(), failed.len());
    for r in failed.iter().take(3) {
        let msg = if r.error.is_empty() { "—" } else { &r.error };
        println!("  {BAD} {} {} {}", r.id, local_time(r.started_at), msg);
    }
}
