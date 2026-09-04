//! komo's plugin layer — the deepseek-harness "everything is a plugin"
//! mechanism, adapted to static Rust composition.
//!
//! One [`Plugin`] trait covers every feature domain. A plugin contributes
//! through three phases whose order encodes the real dependency chain (the
//! static equivalent of Cordis' `inject`-driven activation):
//!
//! 1. [`Plugin::setup_tools`] — tools and hooks, scoped per runtime. Runs in
//!    every process (chat TUI and gateway) during `wiring::build`.
//! 2. [`Plugin::setup_channels`] — ingress channels and their senders.
//!    Gateway only; senders feed the host-built `HomeNotifier`.
//! 3. [`Plugin::setup_sweeps`] — scheduled maintenance. Gateway only, after
//!    the notifier exists.
//!
//! The roster is [`builtin`], the single place a plugin is listed. Enable /
//! disable is uniform: `[plugins.<name>] enabled = false` silences a plugin's
//! contributions across all three phases, whatever they are. Failure is
//! per-plugin [`FailureMode`]: `Degrade` logs and boots without the plugin
//! (MCP, wiki — optional integrations), `Fatal` stops startup (a channel the
//! operator explicitly configured must not silently vanish).
//!
//! What is deliberately NOT ported from Cordis: hot-plug / reversible
//! registration. Composition happens once at wiring — the maximally
//! prompt-cache-friendly choice (registries feed byte-stable, name-sorted
//! catalogs). Hooks observe or veto on the request *suffix* only (tool
//! outcomes are append-only); nothing here can mutate the prompt prefix or
//! the schema set mid-session.

pub mod channels;
pub mod mcp;
pub mod pyhost;
pub mod sweeps;
pub mod tools;
pub mod wiki;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use komo_agent::daemon::{Schedule, WakeupWiring};
use komo_agent::gateway::{Channel, MaintenanceService};
use komo_agent::learning_coordinator::LearningCoordinator;
use komo_agent::runtime::AgentRuntime;
use komo_config::ConfigSnapshot;
use komo_core::domain::catalog::ToolCatalog;
use komo_core::domain::hooks::{StepHook, ToolHook, TurnHook};
use komo_core::domain::tool::Tool;
use komo_infra::persistence::db::Db;
use komo_infra::skills::FsSkillStore;
use komo_services::memory_query::MemoryQueryService;
use komo_services::skill_registry::SkillRegistry;
use komo_services::tool_execution::ToolExecutor;

use crate::domain::{
    cron::CronJobRepository, gateway::WeChatLogin, llm::LlmClient, memory::MemoryRepository,
    notify::Notifier, pairing::PairingRepository, task::TaskRepository, workspace::Workspace,
};
use crate::infra::messaging::home_notifier::TextSender;
use crate::services::operator_control::actions::WikiOps;

/// Which of the four runtimes a registration is visible to. A tiny bitset —
/// not the `bitflags` crate for two constants' worth of use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope(u8);

impl Scope {
    /// The user-facing conversation runtime.
    pub const MAIN: Scope = Scope(0b0001);
    /// The `delegate` tool's sub-agent.
    pub const SUBAGENT: Scope = Scope(0b0010);
    /// The unattended cron-job runtime.
    pub const CRON: Scope = Scope(0b0100);
    /// The read-only briefing runtime.
    pub const BRIEFING: Scope = Scope(0b1000);
    /// The three tool-wielding agent runtimes (everything but briefing) —
    /// the default scope for a tool that can mutate state.
    pub const AGENTIC: Scope = Scope(0b0111);
    /// Every runtime, briefing included — safe reads only.
    pub const ALL: Scope = Scope(0b1111);

    pub fn contains(self, member: Scope) -> bool {
        self.0 & member.0 == member.0
    }
}

impl std::ops::BitOr for Scope {
    type Output = Scope;
    fn bitor(self, rhs: Scope) -> Scope {
        Scope(self.0 | rhs.0)
    }
}

/// What a plugin's `setup_*` failure means for startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureMode {
    /// Log a warning and boot without this plugin's contributions — the rule
    /// for optional external integrations (MCP, wiki, HA).
    Degrade,
    /// Refuse to start. For surfaces the operator explicitly configured and
    /// would otherwise silently lose (ingress channels).
    Fatal,
}

/// One feature domain. Implementations contribute registrations; they hold no
/// state of their own — construction inputs come from the per-phase contexts.
#[async_trait]
pub trait Plugin: Send + Sync {
    /// The `[plugins.<name>]` key, and the log label.
    fn name(&self) -> &'static str;

    /// What a setup failure means for startup. Default: degrade.
    fn failure(&self) -> FailureMode {
        FailureMode::Degrade
    }

    /// Phase 1: contribute tools and hooks. Chat and gateway both run this.
    async fn setup_tools(&self, _reg: &mut ToolRegistry, _cx: &ToolCx<'_>) -> anyhow::Result<()> {
        Ok(())
    }

    /// Phase 2: contribute ingress channels and senders. Gateway only.
    async fn setup_channels(
        &self,
        _reg: &mut ChannelRegistry,
        _cx: &ChannelCx<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Phase 3: contribute scheduled sweeps. Gateway only; the notifier the
    /// sweeps alert through already exists.
    async fn setup_sweeps(
        &self,
        _reg: &mut SweepRegistry,
        _cx: &SweepCx<'_>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// The plugin roster, in phase-relevant order. The single place a plugin is
/// listed; wiring runs phase 1 over it, the gateway phases 2 and 3.
///
/// Order matters twice: channel order fixes the `home_chat` fallback priority
/// (feishu first, preserving the pre-plugin behavior), and sweep order is the
/// startup-banner order.
pub fn builtin() -> Vec<Arc<dyn Plugin>> {
    vec![
        Arc::new(tools::CoreToolsPlugin),
        Arc::new(tools::WebPlugin),
        Arc::new(wiki::WikiPlugin),
        Arc::new(tools::HomeAssistantPlugin),
        Arc::new(mcp::McpPlugin),
        Arc::new(pyhost::PyHostPlugin),
        Arc::new(channels::FeishuPlugin),
        Arc::new(channels::TelegramPlugin),
        Arc::new(channels::WeChatPlugin),
        Arc::new(sweeps::ReviewPlugin),
        Arc::new(sweeps::RemindersPlugin),
        Arc::new(sweeps::TasksPlugin),
        Arc::new(sweeps::MemoryMonitorPlugin),
        Arc::new(sweeps::BriefingPlugin),
        Arc::new(sweeps::CronJobsPlugin),
        Arc::new(sweeps::DreamPlugin),
    ]
}

/// The uniform `[plugins]` kill switches, resolved once against a roster.
///
/// The phase drivers take this rather than the whole [`ConfigSnapshot`]: what
/// they decide is only "does this plugin run", and a narrow value is also what
/// lets the mechanism be tested without a config snapshot.
#[derive(Debug, Default, Clone)]
pub struct PluginGate {
    toggles: std::collections::BTreeMap<String, bool>,
}

impl PluginGate {
    /// Read the toggles out of the snapshot and warn about keys that name no
    /// known plugin — the config layer can't know the roster, so the check
    /// lives here, where it does.
    pub fn new(config: &ConfigSnapshot, plugins: &[Arc<dyn Plugin>]) -> Self {
        for name in config.runtime.plugin_toggles.keys() {
            if !plugins.iter().any(|p| p.name() == name) {
                tracing::warn!(
                    plugin = %name,
                    "[plugins.{name}] names no known plugin; the toggle has no effect"
                );
            }
        }
        Self {
            toggles: config.runtime.plugin_toggles.clone(),
        }
    }

    /// Absent = enabled: a roster needs no entry per plugin to run.
    pub fn enabled(&self, name: &str) -> bool {
        self.toggles.get(name).copied().unwrap_or(true)
    }
}

/// Drive one phase over the roster: skip disabled plugins, apply each
/// plugin's failure mode.
macro_rules! run_phase {
    ($fn_name:ident, $reg:ty, $cx:ty, $method:ident, $phase:literal) => {
        pub async fn $fn_name(
            plugins: &[Arc<dyn Plugin>],
            gate: &PluginGate,
            reg: &mut $reg,
            cx: &$cx,
        ) -> anyhow::Result<()> {
            for plugin in plugins {
                let name = plugin.name();
                if !gate.enabled(name) {
                    tracing::info!(
                        plugin = name,
                        phase = $phase,
                        "plugin disabled by [plugins]"
                    );
                    continue;
                }
                if let Err(error) = plugin.$method(reg, cx).await {
                    match plugin.failure() {
                        FailureMode::Fatal => {
                            return Err(
                                error.context(format!("plugin `{name}` failed ({})", $phase))
                            );
                        }
                        FailureMode::Degrade => tracing::warn!(
                            plugin = name,
                            phase = $phase,
                            error = format!("{error:#}"),
                            "plugin setup failed; booting without it"
                        ),
                    }
                }
            }
            Ok(())
        }
    };
}

run_phase!(
    run_tool_phase,
    ToolRegistry,
    ToolCx<'_>,
    setup_tools,
    "tools"
);
run_phase!(
    run_channel_phase,
    ChannelRegistry,
    ChannelCx<'_>,
    setup_channels,
    "channels"
);
run_phase!(
    run_sweep_phase,
    SweepRegistry,
    SweepCx<'_>,
    setup_sweeps,
    "sweeps"
);

// ── Phase 1: tools ───────────────────────────────────────────────────────────

/// One tool catalog per runtime.
///
/// Separate rather than shared because the runtimes deliberately differ: the
/// briefing agent gets read-only tools, the others get the full set. A plugin
/// mounting at runtime picks which of them it belongs in — and a `Scope` is how
/// it says so, the same vocabulary the static registrations use.
pub struct ScopedCatalogs {
    main: Arc<ToolCatalog>,
    subagent: Arc<ToolCatalog>,
    cron: Arc<ToolCatalog>,
    briefing: Arc<ToolCatalog>,
}

impl Default for ScopedCatalogs {
    fn default() -> Self {
        Self {
            main: Arc::new(ToolCatalog::new()),
            subagent: Arc::new(ToolCatalog::new()),
            cron: Arc::new(ToolCatalog::new()),
            briefing: Arc::new(ToolCatalog::new()),
        }
    }
}

impl ScopedCatalogs {
    /// The catalog for one runtime.
    pub fn of(&self, runtime: Scope) -> &Arc<ToolCatalog> {
        match runtime {
            Scope::SUBAGENT => &self.subagent,
            Scope::CRON => &self.cron,
            Scope::BRIEFING => &self.briefing,
            // MAIN, and any composite — a caller asking for "the catalog" of a
            // multi-runtime scope means the conversation's.
            _ => &self.main,
        }
    }

    /// Every catalog `scope` covers, for a plugin mounting into all of them at
    /// once (`Scope::AGENTIC` is the usual one: everything but the unattended
    /// briefing).
    pub fn covered_by(&self, scope: Scope) -> Vec<Arc<ToolCatalog>> {
        [Scope::MAIN, Scope::SUBAGENT, Scope::CRON, Scope::BRIEFING]
            .into_iter()
            .filter(|runtime| scope.contains(*runtime))
            .map(|runtime| self.of(runtime).clone())
            .collect()
    }
}

/// Shared dependencies phase-1 plugins construct tools from. All host-built:
/// the host owns storage, workspace and the memory/skill services; plugins
/// own what is made of them.
pub struct ToolCx<'a> {
    pub config: &'a ConfigSnapshot,
    /// The per-runtime catalogs, created before this phase so a plugin that
    /// mounts tools *later* (a plugin host connecting, a server appearing) has
    /// something to mount into. Static contributions go through
    /// [`ToolRegistry::tool`] instead — wiring fills the catalogs from it.
    pub catalogs: Arc<ScopedCatalogs>,
    pub db: Arc<Db>,
    pub kanban: Arc<dyn TaskRepository>,
    pub cron_jobs: Arc<dyn CronJobRepository>,
    pub workspace: Arc<Workspace>,
    pub memory_repo: Arc<dyn MemoryRepository>,
    pub memory_query: Arc<MemoryQueryService>,
    /// Hybrid search over stored transcripts. `None` = no embedding backend (or
    /// an unopenable index), which leaves `session` search on its substring scan.
    pub episodic: Option<Arc<komo_services::session_indexing::SessionSearch>>,
    pub skills: Arc<SkillRegistry>,
    pub skill_store: Arc<FsSkillStore>,
}

/// Builds a tool that needs the executor it will be registered in.
pub type ToolFactory = Box<dyn Fn(&ToolExecutor) -> Arc<dyn Tool> + Send + Sync>;

/// Phase-1 registry: tools and hooks, each tagged with the runtimes that see
/// it. Wiring materializes one `ToolExecutor` per runtime from this.
#[derive(Default)]
pub struct ToolRegistry {
    tools: Vec<(Arc<dyn Tool>, Scope)>,
    /// Tools that can only be built once their executor exists — one that
    /// dispatches *other* tools needs a handle to the thing dispatching it.
    /// Wiring runs these after the static registrations, per runtime.
    factories: Vec<(ToolFactory, Scope)>,
    tool_hooks: Vec<(Arc<dyn ToolHook>, Scope)>,
    turn_hooks: Vec<(Arc<dyn TurnHook>, Scope)>,
    step_hooks: Vec<(Arc<dyn StepHook>, Scope)>,
    /// Note-vault operator handles, produced by the wiki plugin and consumed
    /// by the host (`komo wiki` over the operator channel).
    pub wiki_ops: Option<WikiOps>,
}

impl ToolRegistry {
    pub fn tool(&mut self, scope: Scope, tool: Arc<dyn Tool>) {
        self.tools.push((tool, scope));
    }

    /// Contribute a tool built from the executor it will live in.
    ///
    /// For the one shape a plain registration cannot express: a tool that
    /// dispatches other tools (`run_code`) needs a handle to its own executor,
    /// which does not exist until the static tools are in.
    pub fn tool_from_executor(&mut self, scope: Scope, build: ToolFactory) {
        self.factories.push((build, scope));
    }

    /// Contribute a tool hook. No built-in plugin registers one yet — the
    /// executor side of the seam is what komo's own tools needed first — so
    /// this is exercised only by tests until a plugin brings hooks of its own.
    #[allow(dead_code)]
    pub fn tool_hook(&mut self, scope: Scope, hook: Arc<dyn ToolHook>) {
        self.tool_hooks.push((hook, scope));
    }

    /// Contribute a turn-lifecycle hook. Unused by the built-in roster for the
    /// same reason as [`tool_hook`](Self::tool_hook).
    #[allow(dead_code)]
    pub fn turn_hook(&mut self, scope: Scope, hook: Arc<dyn TurnHook>) {
        self.turn_hooks.push((hook, scope));
    }

    /// Contribute a between-round hook.
    pub fn step_hook(&mut self, scope: Scope, hook: Arc<dyn StepHook>) {
        self.step_hooks.push((hook, scope));
    }

    /// The tools visible to `runtime`, in registration order (the executor
    /// name-sorts on registration, so order here carries no cache weight).
    pub fn tools_for(&self, runtime: Scope) -> impl Iterator<Item = &Arc<dyn Tool>> {
        self.tools
            .iter()
            .filter(move |(_, scope)| scope.contains(runtime))
            .map(|(tool, _)| tool)
    }

    /// The executor-dependent tools for `runtime`, built against `executor`.
    pub fn build_for(&self, runtime: Scope, executor: &ToolExecutor) -> Vec<Arc<dyn Tool>> {
        self.factories
            .iter()
            .filter(|(_, scope)| scope.contains(runtime))
            .map(|(build, _)| build(executor))
            .collect()
    }

    pub fn tool_hooks_for(&self, runtime: Scope) -> impl Iterator<Item = &Arc<dyn ToolHook>> {
        self.tool_hooks
            .iter()
            .filter(move |(_, scope)| scope.contains(runtime))
            .map(|(hook, _)| hook)
    }

    pub fn turn_hooks_for(&self, runtime: Scope) -> Vec<Arc<dyn TurnHook>> {
        self.turn_hooks
            .iter()
            .filter(|(_, scope)| scope.contains(runtime))
            .map(|(hook, _)| hook.clone())
            .collect()
    }

    pub fn step_hooks_for(&self, runtime: Scope) -> Vec<Arc<dyn StepHook>> {
        self.step_hooks
            .iter()
            .filter(|(_, scope)| scope.contains(runtime))
            .map(|(hook, _)| hook.clone())
            .collect()
    }
}

// ── Phase 2: channels ────────────────────────────────────────────────────────

pub struct ChannelCx<'a> {
    pub config: &'a ConfigSnapshot,
    pub pairings: Arc<dyn PairingRepository>,
}

/// Phase-2 registry. Channel `name`s double as the platform key everywhere
/// (`{platform}:{chat_id}` session ids, the api status list), so they come
/// from the plugin rather than the trait object — the channel isn't started
/// yet when the list is needed.
#[derive(Default)]
pub struct ChannelRegistry {
    channels: Vec<(&'static str, Box<dyn Channel>)>,
    senders: HashMap<String, Arc<dyn TextSender>>,
    /// `home_chat` candidates in plugin order — first wins, preserving the
    /// pre-plugin feishu > telegram > wechat priority.
    home_candidates: Vec<String>,
    /// The WeChat QR-login coordinator, when that channel mounted. Consumed
    /// by the host's dispatcher (`/wechat login`).
    pub wechat_login: Option<Arc<dyn WeChatLogin>>,
}

impl ChannelRegistry {
    pub fn channel(&mut self, name: &'static str, channel: Box<dyn Channel>) {
        self.channels.push((name, channel));
    }

    pub fn sender(&mut self, platform: &str, sender: Arc<dyn TextSender>) {
        self.senders.insert(platform.to_string(), sender);
    }

    pub fn home_candidate(&mut self, target: String) {
        self.home_candidates.push(target);
    }

    /// Names of the mounted chat channels — replaces the hand-maintained
    /// parallel `Vec<&str>` the gateway used to carry.
    pub fn names(&self) -> Vec<String> {
        self.channels.iter().map(|(n, _)| n.to_string()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    pub fn senders(&self) -> HashMap<String, Arc<dyn TextSender>> {
        self.senders.clone()
    }

    pub fn config_home(&self) -> Option<String> {
        self.home_candidates.first().cloned()
    }

    pub fn into_channels(self) -> Vec<Box<dyn Channel>> {
        self.channels.into_iter().map(|(_, c)| c).collect()
    }
}

// ── Phase 3: sweeps ──────────────────────────────────────────────────────────

/// Everything a sweep plugin may need: storage handles, the wired agent
/// pieces, the notifier, and the host-parsed schedules (parsing stays in the
/// host so the startup banner and the sweeps can never disagree about what's
/// in effect).
pub struct SweepCx<'a> {
    pub config: &'a ConfigSnapshot,
    pub db: Arc<Db>,
    pub kanban: Arc<dyn TaskRepository>,
    pub cron_jobs: Arc<dyn CronJobRepository>,
    pub notifier: Arc<dyn Notifier>,
    pub review: Arc<LearningCoordinator>,
    pub memories: Arc<dyn MemoryRepository>,
    /// The governed skill store — dreaming's proposal half withdraws lapsed
    /// candidates from it.
    pub skill_store: Arc<FsSkillStore>,
    pub aux_llm: Arc<dyn LlmClient>,
    pub briefing_runtime: Arc<AgentRuntime>,
    pub cron_runtime: Arc<AgentRuntime>,
    pub maintenance_schedule: Schedule,
    /// `None` = the opt-in sweep is off (unset or a typo'd cron, already
    /// warned about by the host).
    pub briefing_schedule: Option<Schedule>,
    pub briefing_expr: Option<String>,
    pub dream_schedule: Option<Schedule>,
    /// What the cron sweep needs to fire a standing wait — the registrations,
    /// the log to check them against, and the waker that continues the turn.
    pub wakeups: WakeupWiring,
}

#[derive(Default)]
pub struct SweepRegistry {
    sweeps: Vec<MaintenanceService>,
}

impl SweepRegistry {
    pub fn sweep(&mut self, service: MaintenanceService) {
        self.sweeps.push(service);
    }

    pub fn into_sweeps(self) -> Vec<MaintenanceService> {
        self.sweeps
    }
}

#[cfg(test)]
mod tests {
    //! The registry's scope filtering and the phase driver's enable/failure
    //! semantics. The plugins themselves are exercised through the wiring they
    //! feed; what is asserted here is the mechanism every plugin rides on.

    use super::*;
    use komo_core::domain::context::ToolContext;
    use komo_core::domain::tool::{ToolError, ToolOutput};

    struct NamedTool(&'static str);

    #[async_trait]
    impl Tool for NamedTool {
        fn name(&self) -> &'static str {
            self.0
        }
        fn description(&self) -> &'static str {
            "stand-in"
        }
        async fn call(
            &self,
            _input: serde_json::Value,
            _ctx: &ToolContext,
        ) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("ok"))
        }
    }

    fn names(reg: &ToolRegistry, runtime: Scope) -> Vec<&'static str> {
        reg.tools_for(runtime).map(|t| t.name()).collect()
    }

    /// `AGENTIC` is the three tool-wielding runtimes; `ALL` adds briefing.
    /// Getting this wrong hands the unattended briefing agent a shell.
    #[test]
    fn scope_membership_matches_the_runtimes_each_alias_names() {
        assert!(Scope::AGENTIC.contains(Scope::MAIN));
        assert!(Scope::AGENTIC.contains(Scope::SUBAGENT));
        assert!(Scope::AGENTIC.contains(Scope::CRON));
        assert!(
            !Scope::AGENTIC.contains(Scope::BRIEFING),
            "briefing is read-only; AGENTIC must not reach it"
        );
        for runtime in [Scope::MAIN, Scope::SUBAGENT, Scope::CRON, Scope::BRIEFING] {
            assert!(Scope::ALL.contains(runtime));
        }
        // A composed scope reaches exactly its members.
        let pair = Scope::MAIN | Scope::BRIEFING;
        assert!(pair.contains(Scope::MAIN) && pair.contains(Scope::BRIEFING));
        assert!(!pair.contains(Scope::CRON));
    }

    /// One registry, four runtimes: the filter is what replaced the four
    /// hand-written registration lists, so a `Scope::ALL` tool must reach the
    /// briefing runtime and an `AGENTIC` one must not.
    #[test]
    fn a_runtime_sees_exactly_the_tools_scoped_to_it() {
        let mut reg = ToolRegistry::default();
        reg.tool(Scope::ALL, Arc::new(NamedTool("time")));
        reg.tool(Scope::AGENTIC, Arc::new(NamedTool("shell")));
        reg.tool(Scope::MAIN, Arc::new(NamedTool("main_only")));

        assert_eq!(names(&reg, Scope::MAIN), vec!["time", "shell", "main_only"]);
        assert_eq!(names(&reg, Scope::CRON), vec!["time", "shell"]);
        assert_eq!(
            names(&reg, Scope::BRIEFING),
            vec!["time"],
            "the briefing runtime gets safe reads only"
        );
    }

    struct NamedHook(&'static str);

    #[async_trait]
    impl ToolHook for NamedHook {
        fn name(&self) -> &'static str {
            self.0
        }
    }

    #[async_trait]
    impl TurnHook for NamedHook {
        fn name(&self) -> &'static str {
            self.0
        }
    }

    /// Hooks carry the same scope tag as tools, so a hook meant for the
    /// conversation runtime never runs on an unattended sweep's turns.
    #[test]
    fn hooks_are_scope_filtered_like_tools() {
        let mut reg = ToolRegistry::default();
        reg.tool_hook(Scope::MAIN, Arc::new(NamedHook("main_gate")));
        reg.tool_hook(Scope::ALL, Arc::new(NamedHook("everywhere")));
        reg.turn_hook(Scope::AGENTIC, Arc::new(NamedHook("agentic_turns")));

        let main: Vec<&str> = reg.tool_hooks_for(Scope::MAIN).map(|h| h.name()).collect();
        assert_eq!(main, vec!["main_gate", "everywhere"]);
        let briefing: Vec<&str> = reg
            .tool_hooks_for(Scope::BRIEFING)
            .map(|h| h.name())
            .collect();
        assert_eq!(briefing, vec!["everywhere"]);

        assert_eq!(reg.turn_hooks_for(Scope::CRON).len(), 1);
        assert!(
            reg.turn_hooks_for(Scope::BRIEFING).is_empty(),
            "AGENTIC must not reach the briefing runtime"
        );
    }

    /// A plugin that contributes nothing to a phase is not a failure — the
    /// default `setup_*` bodies are what let a channel plugin ignore phases 1
    /// and 3 entirely.
    struct Contributing {
        name: &'static str,
        failure: FailureMode,
        fail: bool,
    }

    #[async_trait]
    impl Plugin for Contributing {
        fn name(&self) -> &'static str {
            self.name
        }
        fn failure(&self) -> FailureMode {
            self.failure
        }
        async fn setup_tools(
            &self,
            reg: &mut ToolRegistry,
            _cx: &ToolCx<'_>,
        ) -> anyhow::Result<()> {
            if self.fail {
                anyhow::bail!("{} could not start", self.name);
            }
            reg.tool(Scope::ALL, Arc::new(NamedTool(self.name)));
            Ok(())
        }
    }

    fn plugin(name: &'static str, failure: FailureMode, fail: bool) -> Arc<dyn Plugin> {
        Arc::new(Contributing {
            name,
            failure,
            fail,
        })
    }

    fn gate_with(toggles: &[(&str, bool)]) -> PluginGate {
        PluginGate {
            toggles: toggles
                .iter()
                .map(|(name, enabled)| (name.to_string(), *enabled))
                .collect(),
        }
    }

    /// A `ToolCx` is a bag of host-built handles; a phase-driver test needs one
    /// only because the signature demands it, and the stand-in plugins above
    /// never read it.
    async fn tool_cx(config: &ConfigSnapshot) -> ToolCx<'_> {
        let db = Arc::new(
            komo_infra::persistence::db::Db::connect("turso::memory:")
                .await
                .unwrap(),
        );
        let memory_repo: Arc<dyn MemoryRepository> = db.clone();
        ToolCx {
            config,
            catalogs: Arc::new(ScopedCatalogs::default()),
            db: db.clone(),
            kanban: db.clone(),
            cron_jobs: db.clone(),
            workspace: Arc::new(Workspace::current_dir().unwrap()),
            memory_query: Arc::new(MemoryQueryService::new(memory_repo.clone())),
            memory_repo,
            episodic: None,
            skills: Arc::new(SkillRegistry::load_from_dirs(&[])),
            skill_store: Arc::new(komo_infra::skills::FsSkillStore::new(
                std::env::temp_dir().join("komo-plugin-test-skills"),
            )),
        }
    }

    fn test_config() -> ConfigSnapshot {
        ConfigSnapshot::defaults_for_test(std::env::temp_dir().join("komo-plugin-test-home"))
    }

    /// The uniform kill switch: a disabled plugin contributes nothing, and the
    /// rest of the roster is unaffected.
    #[tokio::test]
    async fn a_disabled_plugin_contributes_nothing() {
        let config = test_config();
        let cx = tool_cx(&config).await;
        let roster = vec![
            plugin("alpha", FailureMode::Degrade, false),
            plugin("beta", FailureMode::Degrade, false),
            plugin("gamma", FailureMode::Degrade, false),
        ];

        let mut reg = ToolRegistry::default();
        run_tool_phase(&roster, &gate_with(&[("beta", false)]), &mut reg, &cx)
            .await
            .unwrap();
        assert_eq!(names(&reg, Scope::MAIN), vec!["alpha", "gamma"]);
    }

    /// `Degrade` is the rule for optional integrations: the failure is a
    /// warning and the roster keeps going. This is what keeps an unreachable
    /// MCP server or a broken vault from costing the whole boot.
    #[tokio::test]
    async fn a_degrading_plugin_failure_leaves_the_rest_of_the_roster_intact() {
        let config = test_config();
        let cx = tool_cx(&config).await;
        let roster = vec![
            plugin("alpha", FailureMode::Degrade, false),
            plugin("broken", FailureMode::Degrade, true),
            plugin("gamma", FailureMode::Degrade, false),
        ];

        let mut reg = ToolRegistry::default();
        run_tool_phase(&roster, &PluginGate::default(), &mut reg, &cx)
            .await
            .unwrap();
        assert_eq!(names(&reg, Scope::MAIN), vec!["alpha", "gamma"]);
    }

    /// `Fatal` is for a surface the operator explicitly configured and would
    /// otherwise silently lose — it stops startup, naming the plugin.
    #[tokio::test]
    async fn a_fatal_plugin_failure_stops_the_phase() {
        let config = test_config();
        let cx = tool_cx(&config).await;
        let roster = vec![
            plugin("alpha", FailureMode::Degrade, false),
            plugin("critical", FailureMode::Fatal, true),
        ];

        let mut reg = ToolRegistry::default();
        let error = run_tool_phase(&roster, &PluginGate::default(), &mut reg, &cx)
            .await
            .expect_err("a fatal plugin failure must stop the phase");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("critical"), "{rendered}");
    }

    /// A disabled plugin cannot fail the boot, whatever its failure mode —
    /// turning something off is how an operator recovers from it being broken.
    #[tokio::test]
    async fn disabling_a_fatal_plugin_skips_it_rather_than_failing() {
        let config = test_config();
        let cx = tool_cx(&config).await;
        let roster = vec![plugin("critical", FailureMode::Fatal, true)];

        let mut reg = ToolRegistry::default();
        run_tool_phase(&roster, &gate_with(&[("critical", false)]), &mut reg, &cx)
            .await
            .unwrap();
        assert!(names(&reg, Scope::MAIN).is_empty());
    }

    /// An absent toggle means enabled — a roster runs with no `[plugins]`
    /// table at all, which is the default deployment.
    #[test]
    fn an_empty_gate_enables_everything() {
        let gate = PluginGate::default();
        assert!(gate.enabled("anything"));
        assert!(gate.enabled("core-tools"));
    }

    /// The refactor's regression guard: the roster must hand each runtime the
    /// same tools the four hand-written registration lists did.
    ///
    /// Asserted against a defaults-only config, so the config-gated plugins
    /// (wiki, homeassistant, mcp) contribute nothing and what is left is the
    /// unconditional set. `delegate` is absent from both sides — the host
    /// registers it directly, outside the roster.
    #[tokio::test]
    async fn the_roster_reproduces_the_pre_plugin_tool_sets() {
        let config = test_config();
        let cx = tool_cx(&config).await;
        let roster = builtin();

        let mut reg = ToolRegistry::default();
        run_tool_phase(&roster, &PluginGate::default(), &mut reg, &cx)
            .await
            .unwrap();

        let sorted = |runtime: Scope| {
            let mut names = names(&reg, runtime);
            names.sort_unstable();
            names
        };

        // What `build_full_tools` registered, minus the config-gated tools.
        let agentic = vec![
            "apply_patch",
            "ask_user",
            "cron",
            "edit",
            "glob",
            "grep",
            "logs",
            "memory",
            "read",
            "reminder",
            "session",
            "shell",
            "skill",
            "task",
            "time",
            "todo",
            // Everywhere an agent turn runs, unattended ones included: a
            // routine that waits two hours and checks again is the point.
            "wait",
            "web_fetch",
            "web_search",
            "write",
        ];
        assert_eq!(sorted(Scope::MAIN), agentic);
        assert_eq!(
            sorted(Scope::SUBAGENT),
            agentic,
            "the sub-agent shared the main tool set"
        );
        assert_eq!(
            sorted(Scope::CRON),
            agentic,
            "the cron runtime shared it too — only its approver differed"
        );

        // The briefing runtime's second, hand-copied list.
        assert_eq!(
            sorted(Scope::BRIEFING),
            vec!["skill", "time", "wait", "web_fetch", "web_search"],
        );
    }

    /// Every plugin the roster ships must answer to a `[plugins.<name>]` key,
    /// and the names must be unique — two plugins sharing one would make the
    /// toggle ambiguous.
    #[test]
    fn the_builtin_roster_has_unique_names() {
        let roster = builtin();
        let mut names: Vec<&str> = roster.iter().map(|p| p.name()).collect();
        names.sort_unstable();
        let unique: std::collections::BTreeSet<&str> = names.iter().copied().collect();
        assert_eq!(
            names.len(),
            unique.len(),
            "duplicate plugin name: {names:?}"
        );
        assert!(!names.is_empty());
    }
}
