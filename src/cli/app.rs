use clap::{Parser, Subcommand};

use super::{
    channel, doctor, dream, gateway, health, init, inspect, journey, logs, memory, model, pair,
    policy, resume, service, skill, upgrade, wechat, wiki, workday,
};

/// The version every surface reports: the crate version plus the commit it was
/// built from (see `build.rs`).
///
/// One string, because the whole use of it is comparing two processes — a CLI
/// that printed the crate version while the gateway printed something else
/// would be worse than useless.
pub const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("KOMO_BUILD"));

#[derive(Parser)]
#[command(name = "komo", version = VERSION, about = "Personal agent framework")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Bootstrap ~/.komo: write a commented default config.toml and a .env
    /// credential template. Existing files are never overwritten.
    Init,
    /// Start an interactive chat session (full-screen TUI; needs a terminal)
    Chat,
    /// Resume an existing chat session (shortcut for `komo session resume`)
    Resume {
        /// Session id; an API session may be given without its `api:` prefix
        id: String,
    },
    /// Run the always-on gateway: maintenance scheduler (and, later,
    /// config-declared ingress channels). Maintenance cron comes from
    /// `schedule` in ~/.komo/config.toml (or KOMO_SCHEDULE); default hourly.
    /// With no action, runs in the foreground (this is what launchd invokes).
    Gateway {
        #[command(subcommand)]
        action: Option<GatewayAction>,
    },
    /// Pull the latest source, rebuild + reinstall the binary, and restart the
    /// gateway so the new build goes live (komo's analog of `hermes update`)
    Upgrade {
        /// Rebuild and reinstall, but don't restart the gateway
        #[arg(long)]
        no_restart: bool,
    },
    /// Inspect scheduled reminders (recurring crons and one-shots)
    Cron {
        #[command(subcommand)]
        action: CronAction,
    },
    /// Inspect stored chat sessions
    Session {
        #[command(subcommand)]
        action: SessionAction,
    },
    /// Inspect the durable task list
    Task {
        #[command(subcommand)]
        action: TaskAction,
    },
    /// Inspect the run ledger (every agent turn and its tool steps)
    Run {
        #[command(subcommand)]
        action: RunAction,
    },
    /// Inspect and govern the long-term memory library
    Memory {
        #[command(subcommand)]
        action: MemoryAction,
    },
    /// Run (or preview) usage-driven memory consolidation: promote well-recalled
    /// candidates to active, archive ones that never earned a recall
    Dream {
        /// Apply the cycle (mutate the store). Without it, this is a dry run.
        #[arg(long)]
        apply: bool,
    },
    /// Build and inspect the note-vault search index (`[wiki]` in config.toml)
    Wiki {
        #[command(subcommand)]
        action: WikiAction,
    },
    /// Inspect and govern skills
    Skills {
        #[command(subcommand)]
        action: SkillsAction,
    },
    /// Timeline of what komo has learned: memories (born/promoted/archived)
    /// and skills (proposed/activated), newest first
    Journey {
        /// Maximum number of events to show
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Only show events on or after this date (YYYY-MM-DD, local time)
        #[arg(long)]
        since: Option<String>,
    },
    /// Config & gateway health: model, schedules, channels, home, recent failures
    Doctor,
    /// One-line gateway liveness probe (exit 0 = healthy). This is the Docker
    /// HEALTHCHECK command; `doctor` is the full human report.
    Health,
    /// Manage channel pairing: unknown senders must be approved from this
    /// host before the agent talks to them
    Pair {
        #[command(subcommand)]
        action: PairAction,
    },
    /// Inspect and dry-run the permission policy ([policy] in config.toml)
    Policy {
        #[command(subcommand)]
        action: PolicyAction,
    },
    /// Show or switch the active LLM provider and model
    Model {
        #[command(subcommand)]
        action: ModelAction,
    },
    /// Channel operator commands (per-platform provisioning/maintenance)
    Channel {
        #[command(subcommand)]
        action: ChannelAction,
    },
    /// Check the Chinese working-day calendar (statutory holidays + 调休).
    /// Reports whether a date is a workday, fetching+caching its year if needed.
    Workday {
        /// Date to check (YYYY-MM-DD); defaults to today
        date: Option<String>,
    },
    /// Print the gateway log (the launchd-captured tracing output)
    Logs {
        /// Number of trailing lines to print
        #[arg(short = 'n', long, default_value_t = 100)]
        lines: usize,
        /// Follow the log, streaming new lines until Ctrl-C
        #[arg(short, long)]
        follow: bool,
        /// Show the stdout log (`gateway.log`) instead of the tracing log
        #[arg(long)]
        stdout: bool,
    },
    /// Print the komo version
    Version,
}

#[derive(Subcommand)]
enum ChannelAction {
    /// List configured channels and whether the running gateway has loaded them
    List {
        /// Emit machine-readable channel summaries
        #[arg(long)]
        json: bool,
    },
    /// Check credentials and connectivity without sending a message
    Probe {
        /// Channel: feishu | telegram | wechat | api
        channel: String,
    },
    /// Interactively configure an ingress channel and its credentials
    Setup {
        /// Channel: feishu | telegram | wechat
        channel: String,
    },
    /// WeChat (微信) channel operator commands
    Wechat {
        #[command(subcommand)]
        action: WechatAction,
    },
}

#[derive(Subcommand)]
enum WechatAction {
    /// Provision iLink credentials by scanning a login QR (run on the host)
    Login,
}

#[derive(Subcommand)]
enum PolicyAction {
    /// Show the resolved rules (as the approver applies them) and defaults
    List,
    /// Dry-run one action: which verdict, and which rule decided it
    Check {
        /// Action category: shell | file | network | homeassistant
        category: String,
        /// The target: a command, path, URL, or `domain.service`
        target: String,
        /// Evaluate as this channel (feishu | telegram | wechat | cli | …)
        #[arg(long)]
        channel: Option<String>,
        /// Classify the action as Risk::Dangerous (shell)
        #[arg(long)]
        dangerous: bool,
        /// file only: check the write path instead of read
        #[arg(long)]
        write: bool,
    },
    /// Grants saved by answering `a` at an approval prompt (permissions.json)
    Saved {
        #[command(subcommand)]
        action: SavedAction,
    },
}

#[derive(Subcommand)]
enum SavedAction {
    /// List the saved grants, numbered as `forget` takes them
    List,
    /// Stop honoring a saved grant, so the action asks again
    Forget {
        /// Index from `saved list`
        index: Option<usize>,
        /// Forget every saved grant
        #[arg(long)]
        all: bool,
    },
}

#[derive(Subcommand)]
enum ModelAction {
    /// Show the current provider/model and list available providers
    List,
    /// Switch provider (and optionally model); persists to config.toml
    Set {
        /// Provider, or a Codex model shortcut such as gpt-5.5
        provider: String,
        /// Model id (defaults to the provider's default model)
        model: Option<String>,
    },
}

#[derive(Subcommand)]
enum CronAction {
    /// List scheduled jobs (cron.db) and pending reminders
    List,
    /// Add a command job: a fixed command the gateway runs on a cron
    /// schedule, its stdout delivered to the home channel (deterministic, no LLM)
    Add {
        /// Unique job name (e.g. weekly-alarmhandler-rotation)
        name: String,
        /// 5-field cron expression in local time (e.g. "0 14 * * 5"), or
        /// "@at YYYY-MM-DD HH:MM" for a one-shot that completes after firing
        schedule: String,
        /// Program to execute (absolute path; run directly, not via a shell)
        command: String,
        /// Arguments passed to the command (after `--`)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
        /// Working directory for the command
        #[arg(long)]
        workdir: Option<String>,
        /// Wall-clock budget in seconds (default 900; process killed past it)
        #[arg(long)]
        timeout_secs: Option<u64>,
        /// Never run a slot the gateway slept through. Default: run it late,
        /// once, if it is late by less than the job's own interval.
        #[arg(long)]
        skip_missed: bool,
    },
    /// Add an agent job: the gateway runs a prompt through an unattended,
    /// tool-capable agent turn on schedule and delivers the reply. Side effects
    /// are refused unless granted — per job with `--grant`, or globally with an
    /// `unattended = true` [policy] rule.
    AddAgent {
        /// Unique job name
        name: String,
        /// 5-field cron expression in local time (e.g. "0 8 * * *"), or
        /// "@at YYYY-MM-DD HH:MM" for a one-shot that completes after firing
        schedule: String,
        /// The instruction the agent runs each fire
        prompt: String,
        /// Skill(s) to load before running the prompt (repeatable)
        #[arg(long = "skill")]
        skills: Vec<String>,
        /// Directory the job's file and shell tools are confined to. Must
        /// already exist; defaults to the gateway's own workspace.
        #[arg(long)]
        workspace: Option<String>,
        /// Action this job may take unattended, as `<category>:<match>:<value>`
        /// (e.g. `homeassistant:exact:climate.set_temperature`, `shell:prefix:git `,
        /// `file:prefix:/srv/backups/:write`). Repeatable; scoped to this job
        /// only, and removed with it.
        #[arg(long = "grant")]
        grants: Vec<String>,
        /// Never run a slot the gateway slept through (see `cron add`).
        #[arg(long)]
        skip_missed: bool,
    },
    /// Remove a scheduled job by name
    Remove { name: String },
    /// Re-enable a disabled job (next fire recomputed from now)
    Enable { name: String },
    /// Disable a job without deleting it
    Disable { name: String },
    /// Fire a job now: it becomes due and runs on the gateway's next sweep
    /// tick (within a minute)
    Run { name: String },
}

#[derive(Subcommand)]
enum TaskAction {
    /// List open tasks (inbox / todo / waiting), grouped by status
    List,
}

#[derive(Subcommand)]
enum RunAction {
    /// List recent runs (most recent first)
    List {
        /// Maximum number of runs to show
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Show one run in full, including every tool step
    Inspect {
        /// Run id (as shown by `run list`)
        id: String,
    },
    /// Resume an interrupted run: re-dispatch its input in the original
    /// session, primed with the tool steps that had completed
    Resume {
        /// Run id (as shown by `run list`); defaults to the most recent
        /// recoverable run
        id: Option<String>,
    },
    /// Prune old runs (and their tool steps) from the ledger. Pass exactly one
    /// of --before or --keep.
    Prune {
        /// Delete runs started before this date (YYYY-MM-DD, local time)
        #[arg(long, conflicts_with = "keep")]
        before: Option<String>,
        /// Keep only the N most recent runs, deleting everything older
        #[arg(long, conflicts_with = "before")]
        keep: Option<usize>,
    },
}

#[derive(Subcommand)]
enum WikiAction {
    /// Index the vault. Incremental by default: files whose mtime is unchanged
    /// are skipped without being read or embedded.
    Index {
        /// Re-embed every file, ignoring what is already indexed. Needed after
        /// changing the embedding model — vectors from different models are not
        /// comparable.
        #[arg(long)]
        rebuild: bool,
    },
    /// Query the index the way the `wiki_search` tool does — same embedding,
    /// same score floor — to see what a turn would get back
    Search {
        /// What to look for
        query: String,
        /// Passages to show
        #[arg(long, default_value_t = 5)]
        limit: usize,
    },
    /// Show what is indexed: backend, model, file and chunk counts
    Status,
}

#[derive(Subcommand)]
enum MemoryAction {
    /// List stored memories (optionally filter by status), grouped by status
    List {
        /// Only show this status: candidate | active | archived | rejected
        #[arg(long)]
        status: Option<String>,
    },
    /// Which turns a memory reached the prompt of, newest first — the
    /// question to ask after correcting one: what did it already shape?
    Used {
        /// Memory id (as shown by `memory list`)
        id: String,
        /// How many turns to show
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Substring search across all memories
    Search {
        /// Text to match in memory content
        query: String,
    },
    /// Promote candidates to active, confirmed memories
    Promote {
        /// Memory ids (as shown by `memory list`)
        #[arg(required = true)]
        ids: Vec<String>,
    },
    /// Reject candidates so they never surface
    Reject {
        /// Memory ids
        #[arg(required = true)]
        ids: Vec<String>,
    },
    /// Pin a memory into the L1 per-turn profile (the manual, explicit path)
    Pin {
        /// Memory id
        id: String,
    },
    /// Interactively triage the candidate pile (oldest first): p=promote,
    /// r=reject, s=skip, q=quit
    Triage,
    /// Quality report: counts by status/confidence + piles needing triage
    Report,
    /// Widen memories stranded in a per-conversation `api` scope to global, so
    /// recall can reach them again. Safe to re-run; chat-channel scopes are
    /// left alone.
    RepairScopes,
    /// Embed every memory that still lacks a current vector.
    ///
    /// Recall embeds lazily — one small batch per read — which leaves a library
    /// that has never been embedded lexical-only for a long time, and lexical
    /// matching cannot cross languages. This does the whole library at once.
    Backfill,
}

#[derive(Subcommand)]
enum SkillsAction {
    /// List managed skills, shared ~/.agents/skills, and reviewer candidates
    List,
    /// Install a skill from a git repo or a raw SKILL.md URL into the active
    /// store (owner/repo, owner/repo/subpath, a GitHub URL, a *.git/git@ URL,
    /// or a link straight to a SKILL.md)
    Install {
        /// Where to fetch the skill from
        source: String,
    },
    /// Accept a reviewer candidate into the active store
    Promote {
        /// Skill name (as shown under `candidates` in `skills list`)
        name: String,
    },
    /// Discard a reviewer candidate
    Reject {
        /// Skill name
        name: String,
    },
    /// Mark a skill operator-edit-only (the reviewer stops proposing changes)
    Protect {
        /// Skill name
        name: String,
    },
    /// Clear the protected flag
    Unprotect {
        /// Skill name
        name: String,
    },
    /// Re-enable a disabled skill
    Enable {
        /// Skill name
        name: String,
    },
    /// Hide a skill from the agent without deleting it
    Disable {
        /// Skill name
        name: String,
    },
    /// Retire an active skill: move it out of the catalog, keep the files
    Archive {
        /// Skill name
        name: String,
    },
    /// Bring an archived skill back into the active catalog
    Restore {
        /// Skill name
        name: String,
    },
    /// Show one skill in full: status, provenance, path, history, body
    Inspect {
        /// Skill name
        name: String,
    },
    /// Which turns loaded this skill; without a name, every skill coldest-first
    Audit {
        /// Skill name (omit for the whole ranking)
        name: Option<String>,
    },
}

#[derive(Subcommand)]
enum PairAction {
    /// List pending pairing requests (with codes) and approved senders
    List,
    /// Approve a pending request by its pairing code
    Approve {
        /// The code the bot sent to the unpaired chat
        code: String,
    },
    /// Remove a pairing by id (`platform:sender_id`, as shown by `pair list`)
    Revoke {
        /// Pairing id to remove
        id: String,
    },
}

#[derive(Subcommand)]
enum SessionAction {
    /// List stored sessions with creation time and message counts
    List,
    /// Resume an existing session: reopen the chat TUI bound to its id, so its
    /// history is loaded and the conversation continues where it left off
    Resume {
        /// Session id (a bare UUID also resolves an API session)
        id: String,
    },
    /// Delete sessions that contain no messages
    Clean,
}

#[derive(Subcommand)]
enum GatewayAction {
    /// macOS only: install and start the gateway under launchd
    Start,
    /// macOS only: stop the gateway and remove it from launchd
    Stop,
    /// macOS only: restart the launchd gateway
    Restart,
    /// macOS only: show launchd state for the gateway
    Status,
}

/// The chat TUI owns the terminal, so it needs a real one on both ends.
/// Piped/scripted invocations get a clear pointer to the api channel instead —
/// that is the scripting surface (roadmap §8), not an interactive chat.
/// Must stay in sync with `main.rs::will_run_tui`, which picks the tracing
/// writer before the CLI parses.
fn require_terminal() -> anyhow::Result<()> {
    use std::io::IsTerminal;
    if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
        return Ok(());
    }
    anyhow::bail!(
        "`komo` (or `komo chat`) is a full-screen TUI and needs a terminal.\n\
         For scripted access, POST to the gateway's api channel instead \
         (`/v1/chat/completions`; address and key in ~/.komo/gateway.json)."
    )
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // One resolved snapshot for the whole invocation: every source (config.toml,
    // KOMO_* env, .env secrets) is read exactly once. All paths live under the
    // config home; use KOMO_HOME to point at a different one (e.g. for tests
    // or a second instance).
    let config = komo_config::ConfigSnapshot::load();
    match cli.command {
        // The interactive chat is komo's primary surface. Keep `chat` as an
        // explicit, script-friendly spelling, but make a bare `komo` open it.
        None | Some(Commands::Chat) => {
            require_terminal()?;
            crate::tui::run(&config).await
        }
        Some(Commands::Init) => init::run(),
        Some(Commands::Resume { id }) => {
            require_terminal()?;
            crate::tui::resume(&config, &id).await
        }
        Some(Commands::Gateway { action }) => match action {
            None => gateway::run(&config).await,
            Some(GatewayAction::Start) => service::start(),
            Some(GatewayAction::Stop) => service::stop(),
            Some(GatewayAction::Restart) => service::restart(),
            Some(GatewayAction::Status) => service::status(),
        },
        Some(Commands::Upgrade { no_restart }) => upgrade::run(no_restart),
        Some(Commands::Cron { action }) => match action {
            CronAction::List => inspect::cron_list(&operator(&config).await?).await,
            CronAction::Add {
                name,
                schedule,
                command,
                args,
                workdir,
                timeout_secs,
                skip_missed,
            } => {
                inspect::cron_add(
                    &operator(&config).await?,
                    crate::domain::cron::CronJobSpec {
                        name,
                        schedule,
                        action: crate::domain::cron::CronAction::Command {
                            command,
                            args,
                            workdir,
                            timeout_secs: timeout_secs.unwrap_or(0),
                        },
                        // A command job runs the program directly with no
                        // approver in the loop — nothing to grant.
                        grants: Vec::new(),
                        catch_up: catch_up_of(skip_missed),
                    },
                )
                .await
            }
            CronAction::AddAgent {
                name,
                schedule,
                prompt,
                skills,
                workspace,
                grants,
                skip_missed,
            } => {
                let grants = grants
                    .iter()
                    .map(|g| policy::parse_grant(g))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                inspect::cron_add(
                    &operator(&config).await?,
                    crate::domain::cron::CronJobSpec {
                        name,
                        schedule,
                        action: crate::domain::cron::CronAction::Agent {
                            prompt,
                            skills,
                            workspace,
                        },
                        grants,
                        catch_up: catch_up_of(skip_missed),
                    },
                )
                .await
            }
            CronAction::Remove { name } => {
                inspect::cron_remove(&operator(&config).await?, &name).await
            }
            CronAction::Enable { name } => {
                inspect::cron_set_enabled(&operator(&config).await?, &name, true).await
            }
            CronAction::Disable { name } => {
                inspect::cron_set_enabled(&operator(&config).await?, &name, false).await
            }
            CronAction::Run { name } => inspect::cron_run(&operator(&config).await?, &name).await,
        },
        Some(Commands::Session { action }) => match action {
            SessionAction::List => inspect::session_list(&operator(&config).await?).await,
            SessionAction::Resume { id } => {
                require_terminal()?;
                crate::tui::resume(&config, &id).await
            }
            SessionAction::Clean => inspect::session_clean(&operator(&config).await?).await,
        },
        Some(Commands::Task { action }) => match action {
            TaskAction::List => inspect::task_list(&operator(&config).await?).await,
        },
        Some(Commands::Run { action }) => match action {
            RunAction::List { limit } => inspect::run_list(&operator(&config).await?, limit).await,
            RunAction::Inspect { id } => inspect::run_inspect(&operator(&config).await?, &id).await,
            RunAction::Resume { id } => resume::run(&config, &operator(&config).await?, id).await,
            RunAction::Prune { before, keep } => {
                run_prune(&operator(&config).await?, before, keep).await
            }
        },
        Some(Commands::Memory { action }) => {
            let control = operator(&config).await?;
            match action {
                MemoryAction::List { status } => memory::list(&control, status).await,
                MemoryAction::Search { query } => memory::search(&control, &query).await,
                MemoryAction::Used { id, limit } => memory::used(&control, &id, limit).await,
                MemoryAction::Promote { ids } => memory::promote(&control, &ids).await,
                MemoryAction::Reject { ids } => memory::reject(&control, &ids).await,
                MemoryAction::Pin { id } => memory::pin(&control, &id).await,
                MemoryAction::Triage => memory::triage(&control).await,
                MemoryAction::Report => memory::report(&control).await,
                MemoryAction::RepairScopes => memory::repair_scopes(&control).await,
                MemoryAction::Backfill => memory::backfill(&control).await,
            }
        }
        Some(Commands::Dream { apply }) => dream::run(&operator(&config).await?, apply).await,
        Some(Commands::Wiki { action }) => {
            let control = operator(&config).await?;
            match action {
                WikiAction::Index { rebuild } => wiki::index(&control, rebuild).await,
                WikiAction::Search { query, limit } => wiki::search(&control, &query, limit).await,
                WikiAction::Status => wiki::status(&control).await,
            }
        }
        Some(Commands::Skills { action }) => match action {
            SkillsAction::List => skill::list(),
            SkillsAction::Install { source } => skill::install(&source).await,
            SkillsAction::Promote { name } => skill::promote(&name),
            SkillsAction::Reject { name } => skill::reject(&name),
            SkillsAction::Protect { name } => skill::protect(&name, true),
            SkillsAction::Unprotect { name } => skill::protect(&name, false),
            SkillsAction::Enable { name } => skill::set_enabled(&name, true),
            SkillsAction::Disable { name } => skill::set_enabled(&name, false),
            SkillsAction::Archive { name } => skill::archive(&name),
            SkillsAction::Restore { name } => skill::restore(&name),
            SkillsAction::Inspect { name } => skill::inspect(&name),
            SkillsAction::Audit { name } => {
                skill::audit(&operator(&config).await?, name.as_deref()).await
            }
        },
        Some(Commands::Journey { limit, since }) => {
            journey::journey(&operator(&config).await?, limit, since).await
        }
        Some(Commands::Doctor) => doctor::doctor(&config, &operator(&config).await?).await,
        Some(Commands::Health) => health::run().await,
        Some(Commands::Pair { action }) => {
            let control = operator(&config).await?;
            match action {
                PairAction::List => pair::list(&control).await,
                PairAction::Approve { code } => pair::approve(&control, &code).await,
                PairAction::Revoke { id } => pair::revoke(&control, &id).await,
            }
        }
        Some(Commands::Policy { action }) => match action {
            PolicyAction::List => {
                // Job grants live in cron.db, which the gateway may hold open —
                // so they come through operator control like every other job
                // read. A failure here degrades the listing (one section marked
                // unavailable) rather than failing a command whose config and
                // saved sections are perfectly readable.
                let jobs = match operator(&config).await {
                    Ok(control) => match control
                        .query(crate::services::operator_control::OperatorQuery::CronJobs)
                        .await
                    {
                        Ok(crate::services::operator_control::OperatorQueryResult::CronJobs(
                            jobs,
                        )) => Some(jobs),
                        _ => None,
                    },
                    Err(_) => None,
                };
                policy::list(&config, jobs.as_deref())
            }
            PolicyAction::Check {
                category,
                target,
                channel,
                dangerous,
                write,
            } => policy::check(
                &config,
                &category,
                &target,
                channel.as_deref(),
                dangerous,
                write,
            ),
            PolicyAction::Saved { action } => match action {
                SavedAction::List => policy::saved_list(&config),
                SavedAction::Forget { index, all } => policy::saved_forget(&config, index, all),
            },
        },
        Some(Commands::Model { action }) => match action {
            ModelAction::List => model::list(&config).await,
            ModelAction::Set { provider, model } => model::set(&config, &provider, model).await,
        },
        Some(Commands::Channel { action }) => match action {
            ChannelAction::List { json } => channel::list(&config, json).await,
            ChannelAction::Probe { channel: name } => channel::probe(&config, &name).await,
            ChannelAction::Setup { channel: name } => channel::setup(&config, &name).await,
            ChannelAction::Wechat { action } => match action {
                WechatAction::Login => wechat::login().await,
            },
        },
        Some(Commands::Workday { date }) => workday::check(date).await,
        Some(Commands::Logs {
            lines,
            follow,
            stdout,
        }) => logs::run(lines, follow, stdout),
        Some(Commands::Version) => {
            println!("komo {VERSION}");
            Ok(())
        }
    }
}

/// Resolve one operator backend for this invocation: the gateway is probed
/// exactly once, and every read/write the command performs reuses it.
async fn operator(
    config: &komo_config::ConfigSnapshot,
) -> anyhow::Result<crate::services::operator_control::OperatorControl> {
    crate::services::operator_control::OperatorControl::connect(
        crate::services::operator_control::StoreUrls::from_config(&config.runtime),
    )
    .await
}

/// Resolve `run prune`'s `--before <date>` / `--keep N` into a cutoff timestamp,
/// then prune. Exactly one of the two must be given (clap enforces mutual
/// exclusion, but not presence).
async fn run_prune(
    control: &crate::services::operator_control::OperatorControl,
    before: Option<String>,
    keep: Option<usize>,
) -> anyhow::Result<()> {
    let cutoff = match (before, keep) {
        (Some(date), None) => parse_local_date(&date)?,
        (None, Some(keep)) => match inspect::run_keep_cutoff(control, keep).await? {
            Some(cutoff) => cutoff,
            None => {
                println!("Fewer than {} runs; nothing to prune.", keep + 1);
                return Ok(());
            }
        },
        _ => anyhow::bail!("pass exactly one of --before <YYYY-MM-DD> or --keep <N>"),
    };
    inspect::run_prune(control, cutoff).await
}

/// Parse a `YYYY-MM-DD` date as local-time midnight, returning a unix timestamp.
pub(crate) fn parse_local_date(s: &str) -> anyhow::Result<i64> {
    use chrono::TimeZone;
    let date = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
        .map_err(|e| anyhow::anyhow!("invalid date `{s}` (expected YYYY-MM-DD): {e}"))?;
    let midnight = date.and_hms_opt(0, 0, 0).expect("valid midnight");
    match chrono::Local.from_local_datetime(&midnight).single() {
        Some(dt) => Ok(dt.timestamp()),
        None => anyhow::bail!("ambiguous local time for `{s}`"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skills_is_the_cli_command_name() {
        assert!(Cli::try_parse_from(["komo", "skills", "list"]).is_ok());
        assert!(Cli::try_parse_from(["komo", "skill", "list"]).is_err());
    }

    #[test]
    fn bare_command_defaults_to_chat() {
        let cli = Cli::try_parse_from(["komo"]).expect("bare komo should parse");
        assert!(cli.command.is_none());
    }

    #[test]
    fn channel_list_accepts_json_output() {
        assert!(Cli::try_parse_from(["komo", "channel", "list", "--json"]).is_ok());
    }

    #[test]
    fn channel_probe_and_setup_accept_supported_channel_names() {
        assert!(Cli::try_parse_from(["komo", "channel", "probe", "telegram"]).is_ok());
        assert!(Cli::try_parse_from(["komo", "channel", "setup", "telegram"]).is_ok());
    }

    #[test]
    fn root_resume_parses_a_bare_session_id() {
        assert!(
            Cli::try_parse_from(["komo", "resume", "019fad15-8199-7461-9d48-0a6c779f1c8d",])
                .is_ok()
        );
    }

    #[test]
    fn session_resume_also_parses_a_bare_api_session_id() {
        assert!(
            Cli::try_parse_from([
                "komo",
                "session",
                "resume",
                "019fad15-8199-7461-9d48-0a6c779f1c8d",
            ])
            .is_ok()
        );
    }
}

/// `--skip-missed` as a [`CatchUp`]. A flag rather than a value because there
/// are two behaviours, and "skip" is the one that needs asking for.
fn catch_up_of(skip_missed: bool) -> crate::domain::cron::CatchUp {
    if skip_missed {
        crate::domain::cron::CatchUp::Skip
    } else {
        crate::domain::cron::CatchUp::Late
    }
}
