//! Shared construction of a fully-wired `AgentRuntime`.
//!
//! Both the chat REPL (`cli/chat.rs`) and the gateway (`cli/gateway.rs`) need
//! the same agent: identical tools, skills, LLM, and reviewer. The only thing
//! that differs is the `Approver` — interactive at a TTY vs. auto-deny in the
//! unattended gateway — so it is passed in.
//!
//! Tools and hooks come from the plugin roster (`crate::plugins`): the host
//! builds the shared services (storage, workspace, memory, skills), phase 1
//! of the plugin layer contributes every tool tagged with the runtimes that
//! see it, and this module materializes the four executors from that one
//! registry — so a runtime's tool set can never drift from the roster.

use komo_agent::delegate::DelegateTool;
use komo_agent::learning_coordinator::LearningCoordinator;
use komo_agent::llm::{PreambleFn, build_llm};
use komo_agent::reviewer::ReflectiveReviewer;
use komo_agent::runtime::AgentRuntime;
use komo_agent::system_prompt::SystemPromptBuilder;
use komo_core::domain::checkpoint::CheckpointStore;
use komo_core::domain::embedding::EmbeddingClient;
use komo_core::domain::skill::SkillOffer;
use komo_core::domain::turn_journal::TurnJournalRepository;
use komo_infra::embedding::{GatedEmbedder, OllamaEmbedder};
use komo_infra::memory::memory_db::MemoryDb;
use komo_infra::permissions_store::PermissionsStore;
use komo_infra::persistence::{db::Db, kanban::KanbanDb};
use komo_infra::skills::FsSkillStore;
use komo_services::clarify::ClarifyState;
use komo_services::memory_enrichment::MemoryEnricher;
use komo_services::skill_registry::SkillRegistry;
use komo_services::tool_execution::{ToolExecutionConfig, ToolExecutor};
use komo_services::tool_output_store::ToolOutputStore;
use std::sync::Arc;

use crate::domain::{
    approval::Approver, cron::CronJobRepository, llm::LlmClient, memory::MemoryRepository,
    repository::SkillRepository, reviewer::Reviewer, workspace::Workspace,
};
use crate::plugins::{self, Scope, ToolCx, ToolRegistry};
use komo_config::ConfigSnapshot;

/// A wired agent plus the handles background work needs (sessions for sweeping,
/// the reviewer the sweep invokes).
pub struct Wiring {
    pub runtime: AgentRuntime,
    /// The shared review coordinator (post-turn + scheduled), for the
    /// gateway's `ReviewSweep`.
    pub review: Arc<LearningCoordinator>,
    /// The auxiliary (cheaper) LLM, reused by the daily briefing sweep.
    pub aux_llm: Arc<dyn LlmClient>,
    /// The markdown memory store, also read by the briefing sweep.
    pub memories: Arc<dyn MemoryRepository>,
    /// The hybrid query service, so the operator surface can drive an embedding
    /// backfill through the same one recall uses.
    pub memory_query: Arc<komo_services::memory_query::MemoryQueryService>,
    /// The governed skill store (`~/.komo/skills`, files — roadmap §9), shared
    /// with the gateway's api channel.
    pub skills: Arc<FsSkillStore>,
    /// Mid-turn clarify state: the `ask_user` tool waits on it; the gateway
    /// dispatcher (and the TUI) resolve an inbound message into it.
    pub clarify: Arc<ClarifyState>,
    /// The briefing sweep's tool-capable agent (roadmap §2): aux model over a
    /// read-only tool set, policy-gated with a deny-all inner approver — only
    /// explicit `unattended` policy rules can grant a `Risk::Normal` action.
    pub briefing_runtime: Arc<AgentRuntime>,
    /// The cron sweep's agent for `CronAction::Agent` jobs: the full tool set
    /// (unlike briefing) but the same unattended policy gating. Main model, no
    /// memory enricher.
    pub cron_runtime: Arc<AgentRuntime>,
    /// Where over-limit tool results are stored in full. Exposed so the gateway
    /// can run the retention sweep once at startup — the store re-sweeps at most
    /// hourly on its own, and this is deliberately not a cron schedule: expiring
    /// a scratch file does not need to happen on the minute.
    pub output_store: Arc<ToolOutputStore>,
    /// Note-vault handles, shared with the operator surface so `komo wiki` works
    /// while the gateway holds the index open.
    pub wiki: Option<crate::services::operator_control::actions::WikiOps>,
}

/// What distinguishes one runtime from another.
///
/// Five runtimes exist — interactive, delegate, cron, briefing, and the
/// reviewer's aux calls — and they were four near-identical struct literals
/// whose *differences* were three fields buried among nine identical ones.
/// Everything shared (the stores, the history window, the learning
/// coordinator) is supplied by [`RuntimeParts`]; a profile states only what is
/// its own.
///
/// The load-bearing field is `scope`. It used to be written twice per runtime,
/// once for each hook lookup, with nothing checking the two agreed or that
/// either matched the executor's own scope — so a copy-pasted `Scope::MAIN`
/// would silently give a sweep the conversation's hooks. Named once here, that
/// cannot be spelled wrong in only one of the places.
struct CapabilityProfile {
    /// Which runtime this is. Selects the tool catalog's hooks, and must be the
    /// same scope `tools` was built for.
    scope: Scope,
    llm: Arc<dyn LlmClient>,
    tools: ToolExecutor,
    max_turns: usize,
    /// Learns from its own finished turns. **False for every aux runtime**: a
    /// sub-agent's transcript is scratch work, and a sweep restates what komo
    /// already knows, so extracting from either feeds the memory pipeline its
    /// own output. (`LearningCoordinator` also refuses sweep sessions by id —
    /// this is the half that stops them being offered at all.)
    learns: bool,
    /// Journals each turn's provider state so an interrupted one can be
    /// continued. True only for conversations: an aux turn is re-dispatched
    /// whole, so a journal for one would only ever be written and deleted.
    resumable: bool,
    /// Keeps the pre-image of every file its turns change, so
    /// `komo run rollback` can undo one. On wherever the runtime can write:
    /// a delegation and a cron job produce final file state exactly like a
    /// conversation does, and are *less* watched while doing it.
    checkpoints: bool,
}

/// What every runtime shares. Held once so [`CapabilityProfile`] can be read as
/// a list of differences.
struct RuntimeParts<'a> {
    db: Arc<Db>,
    registry: &'a plugins::ToolRegistry,
    /// Mirrors the LLM's own history window, so a turn loads exactly what the
    /// model will replay and no long transcript is read in full.
    history_window: usize,
    learning: Arc<LearningCoordinator>,
    checkpoint: Option<Arc<dyn CheckpointStore>>,
}

impl RuntimeParts<'_> {
    fn build(&self, profile: CapabilityProfile) -> AgentRuntime {
        AgentRuntime {
            llm: profile.llm,
            sessions: self.db.clone(),
            messages: self.db.clone(),
            // Every runtime shares the run ledger, which is what makes a
            // delegation, a cron job and a briefing each auditable through
            // `komo run list` alongside ordinary turns.
            runs: self.db.clone(),
            tool_executor: profile.tools,
            max_turns: profile.max_turns,
            history_window: self.history_window,
            learning: profile.learns.then(|| self.learning.clone()),
            journal: profile
                .resumable
                .then(|| self.db.clone() as Arc<dyn TurnJournalRepository>),
            checkpoint: profile
                .checkpoints
                .then(|| self.checkpoint.clone())
                .flatten(),
            turn_hooks: self.registry.turn_hooks_for(profile.scope),
            step_hooks: self.registry.step_hooks_for(profile.scope),
        }
    }
}

/// A warning, never a fatal — the same call komo makes for a missing model key
/// or a token-less HA channel. Recall keeps working without it.
///
/// The probe runs in the background, not here: awaiting it held the first TUI
/// frame hostage to Ollama's cold model load (seconds), and its verdict is a
/// diagnostic plus a kill switch, never a precondition — every embed caller
/// already degrades to lexical on failure. A failed probe closes the
/// [`GatedEmbedder`]'s gate so later calls fail instantly instead of re-paying
/// a network timeout per turn; a successful one doubles as a model warm-up, so
/// the first recall of the session usually hits a resident model.
pub(crate) fn build_embedder(
    config: Option<&komo_config::EmbeddingConfig>,
) -> Option<Arc<dyn EmbeddingClient>> {
    let config = config?;
    let embedder = match OllamaEmbedder::new(&config.url, &config.model) {
        Ok(embedder) => embedder,
        Err(error) => {
            tracing::warn!(%error, "memory embedding backend unusable — recall stays lexical");
            return None;
        }
    };
    let gated = Arc::new(GatedEmbedder::new(embedder));
    let probe = gated.clone();
    let (url, model) = (config.url.clone(), config.model.clone());
    tokio::spawn(async move {
        match probe.probe().await {
            Ok(()) => tracing::info!(model = %model, "memory embedding backend ready"),
            Err(error) => tracing::warn!(
                %error,
                url = %url,
                model = %model,
                "memory embedding backend unreachable — recall stays lexical"
            ),
        }
    });
    Some(gated)
}

/// Build the agent against `db` (sessions/messages/etc.), `kanban` (durable
/// tasks, a separate file) and `cron_jobs` (durable scheduled jobs, ditto),
/// gating side-effecting tools through `approver`. Every setting comes from the
/// caller's one resolved `config` snapshot — wiring never re-reads config.toml,
/// the env, or `.env`.
///
/// The stores are passed in rather than opened here because Turso takes an
/// exclusive lock per file: the gateway already holds all three open and must
/// hand its own handles over.
pub async fn build(
    config: &ConfigSnapshot,
    db: Arc<Db>,
    kanban: Arc<KanbanDb>,
    cron_jobs: Arc<dyn CronJobRepository>,
    approver: Arc<dyn Approver>,
) -> anyhow::Result<Wiring> {
    // An unusable model selection (bad KOMO_* value, unknown provider,
    // missing API key) can't produce a working agent — fail here like the old
    // strict resolver did.
    config.validate_agent()?;
    let model_config = &config.runtime.model;

    // Approvals the operator chose to make durable (`a` at the prompt →
    // ~/.komo/permissions.json). The store's list is *shared* with the policy, so
    // a grant applies to the next decision without a restart.
    let permissions = Arc::new(PermissionsStore::load(&config.runtime.home));
    let interactive_policy = config
        .runtime
        .policy
        .policy
        .clone()
        .with_saved(permissions.rules());

    // Over-limit tool output is kept in full under ~/.komo/tool-output; the model
    // gets a head+tail preview naming the file (roadmap item 10).
    let output_store = Arc::new(ToolOutputStore::new(
        config.runtime.home.join("tool-output"),
    ));

    // Mutations and shell workdirs remain confined to the current working
    // directory. Local files are readable from any directory (subject to the
    // file-read permission policy); managed tool output is retained as an
    // explicit root for session-derived workspaces as well.
    let mut readonly_roots = config.runtime.readable_roots.clone();
    readonly_roots.push(output_store.root().to_path_buf());
    let workspace = Arc::new(
        Workspace::current_dir()?
            .with_readonly(readonly_roots)
            .with_unrestricted_reads(),
    );

    // ── Shared dependencies (built once, used by every tool set) ─────────────
    // Mid-turn clarification (roadmap §7): the sentinel tool suspends the turn
    // on a question; whoever routes inbound messages (gateway dispatcher, TUI)
    // resolves the answer through this shared state.
    let clarify = Arc::new(ClarifyState::new());

    // Memories live in their own SQLite file (~/.komo/memory.db), shared by the
    // `memory` tool, the reflective reviewer, the L1 pinned injection, and the
    // briefing sweep. On first run it seeds itself from any legacy markdown
    // memories under ~/.komo/memory/ (a one-time, no-op-once-populated import).
    let memory_db = MemoryDb::connect(&config.runtime.memory_db_url).await?;
    let imported = memory_db
        .import_legacy_markdown(&config.runtime.home.join("memory"))
        .await
        .unwrap_or(0);
    if imported > 0 {
        tracing::info!(imported, "migrated legacy markdown memories into memory.db");
    }
    let memory_repo: Arc<dyn MemoryRepository> = Arc::new(memory_db);

    // The delegate tool runs a separate, tool-less sub-agent on the (optionally
    // cheaper) aux model. It gets a minimal identity-only preamble — no tools,
    // skills, or project context — rebuilt per turn like the main agent.
    let aux_config = model_config.aux_variant();
    let aux_builder = Arc::new(SystemPromptBuilder::new(&aux_config));
    let aux_preamble: PreambleFn = Arc::new(move || aux_builder.build());
    // Aux/delegate sub-agents must not be fed the user's memory library — and
    // the aux agent never gets an aux of its own (no recursion).
    let aux_llm = build_llm(&aux_config, None, aux_preamble, None, Some("aux"))?;

    // ── The attended approval chain ──────────────────────────────────────────
    // Built here rather than at the top of `build` because its middle rung needs
    // the aux model above.
    //
    // `[policy] mode = "auto"` inserts a second-opinion reviewer between the
    // policy's `Ask` and the human: it may auto-allow an action the operator's
    // own request plainly covers, or hand it over — it can never deny. In `ask`
    // mode (the default) the decorator is absent, so this is byte-identical to
    // what the chain was before the mode existed.
    //
    // Attended runtimes only. Cron and briefing build their own `PolicyApprover`
    // over a deny-all inner further down and deliberately skip this: an
    // unattended turn grants through rules approved in advance, never a live
    // judgement call (ADR 0002 / 0003).
    let approver = match config.runtime.policy.mode {
        komo_core::domain::policy::PolicyMode::Auto => {
            tracing::info!("permission policy: auto mode (aux reviewer may auto-allow prompts)");
            komo_agent::auto_reviewer::AutoReviewApprover::wrap(
                aux_llm.clone(),
                db.clone(),
                approver,
            )
        }
        komo_core::domain::policy::PolicyMode::Ask => approver,
    };

    // Wrap that in the configurable permission policy (roadmap §3): the policy
    // auto-allows / hard-denies per `[policy]` rules and only escalates when it
    // says "ask". With no `[policy]` table this is the empty policy — identical
    // to the bare interactive approver.
    let approver = komo_agent::policy_approver::PolicyApprover::wrap_with_store(
        interactive_policy,
        approver,
        permissions.clone(),
    );

    // The governed skill store: `~/.komo/skills` is the komo-owned home for
    // durable skills (files, not db — roadmap §9). Reviewer proposals land in
    // its `.candidates/` for triage; a one-time import moves any skills a
    // pre-filesystem komo accumulated in komo.db into that triage pile.
    let skill_store = Arc::new(FsSkillStore::new(FsSkillStore::default_root()));
    match db.export_legacy_skills().await {
        Ok(rows) if !rows.is_empty() => match skill_store.import_legacy_db(rows) {
            Ok(0) => {}
            Ok(n) => tracing::info!(n, "imported legacy komo.db skills as candidates"),
            Err(error) => tracing::warn!(%error, "legacy skill import failed"),
        },
        Ok(_) => {}
        Err(error) => tracing::warn!(%error, "failed to read legacy db skills"),
    }

    // Skills load from, in priority order (first to define a name wins):
    //   KOMO_SKILLS_PATH (colon-separated), <workspace>/skills,
    //   <workspace>/.claude/skills, the governed ~/.komo/skills store, then the
    //   user-global ~/.agents/skills and ~/.claude/skills shared by other agents.
    let root = workspace.roots().first().cloned().unwrap_or_default();
    let skill_dirs = komo_infra::skills::runtime_skill_dirs(
        &config.runtime.skills_path,
        &root,
        skill_store.root(),
        dirs::home_dir().as_deref(),
    );
    let skills = Arc::new(SkillRegistry::load_from_dirs(&skill_dirs));

    // One memory query service, shared by the `memory` tool's explicit search and
    // the enricher's automatic recall — that sharing is the point: a model handed
    // a memory unprompted must be able to find the same memory by asking. Built
    // before the tool set because the tool holds it.
    // Built once and shared: the same backend serves memory recall and the
    // episodic index over transcripts, and its background probe (see
    // `build_embedder`) then covers both.
    let embedder = build_embedder(config.runtime.embedding.as_ref());
    let mut memory_query =
        komo_services::memory_query::MemoryQueryService::new(memory_repo.clone());
    if let Some(embedder) = &embedder {
        memory_query = memory_query.with_embedder(embedder.clone());
    }
    let memory_query = Arc::new(memory_query);

    // Episodic search over komo's own transcripts. Its own collection, beside
    // the wiki's and independent of `[wiki]`: this corpus is komo's, exists
    // whether or not the operator keeps a note vault, and needs only the
    // embedding backend memory recall already asked for.
    //
    // Absent when there is no embedder, or when the store cannot be opened —
    // `session` search then falls back to the substring scan it had before,
    // which is worse but never silent.
    let episodic = match &embedder {
        Some(embedder) => {
            let dir = komo_config::komo_home().join("session-index");
            match komo_wiki::edge::EdgeIndex::open(&dir, "komo_sessions") {
                Ok(index) => Some(Arc::new(
                    komo_services::session_indexing::SessionSearch::new(
                        Arc::new(index),
                        embedder.clone(),
                    ),
                )),
                Err(error) => {
                    tracing::warn!(%error, dir = %dir.display(),
                        "session index unusable — `session` search stays lexical");
                    None
                }
            }
        }
        None => None,
    };

    // ── Plugin phase 1: every tool and hook, tagged per runtime ──────────────
    // The roster is `plugins::builtin()`; `[plugins.<name>] enabled = false`
    // silences a plugin uniformly. MCP/wiki failures degrade (warn, boot on).
    let roster = plugins::builtin();
    let gate = plugins::PluginGate::new(config, &roster);
    // The catalogs exist before phase 1 so a plugin that mounts tools *later*
    // — the python plugin host, which can gain a tool the moment a file is
    // written — has something to mount into. Static contributions still go
    // through the registry; wiring fills each catalog from it below.
    let catalogs = Arc::new(plugins::ScopedCatalogs::default());
    let tool_cx = ToolCx {
        config,
        catalogs: catalogs.clone(),
        db: db.clone(),
        kanban: kanban.clone(),
        cron_jobs: cron_jobs.clone(),
        workspace: workspace.clone(),
        clarify: clarify.clone(),
        memory_repo: memory_repo.clone(),
        episodic: episodic.clone(),
        memory_query: memory_query.clone(),
        skills: skills.clone(),
        skill_store: skill_store.clone(),
    };
    let mut registry = ToolRegistry::default();
    plugins::run_tool_phase(&roster, &gate, &mut registry, &tool_cx).await?;
    let wiki_ops = registry.wiki_ops.take();

    // Keep the always-on preamble small: list a bounded catalog, the rest is
    // discoverable on demand via the `skill` tool.
    //
    // Built per runtime rather than once, because the catalog is gated on what
    // *that* runtime offers: a skill restricted to another OS, or one requiring
    // a tool this runtime never registered (config-absent, or dropped by a
    // policy deny), is not worth a prompt line every turn. Offer-time only —
    // `skill` view/list and every `komo skills` command ignore the gating, so a
    // skill left out of the preamble still loads the moment it's named.
    const SKILL_CATALOG_CAP: usize = 30;
    let skills_note_for = |tool_names: &[String]| -> Option<String> {
        let catalog = skills.catalog_capped(
            SKILL_CATALOG_CAP,
            &SkillOffer::here(tool_names.iter().cloned()),
        );
        (!catalog.is_empty()).then(|| {
            format!(
                "You have skills (instruction playbooks) available. To use one, call the \
                 `skill` tool with action=view and the skill name to load its instructions, \
                 then follow them. Available skills:\n{catalog}"
            )
        })
    };

    // Materialize one executor per runtime from the registry: the plugin
    // roster is the single definition, scope filtering replaces the four
    // hand-written registration lists (briefing's included). `delegate` is
    // passed in rather than registered by a plugin because the sub-agent it
    // runs needs an executor of its own — built by this same closure with
    // `delegate: None`, which is the structural guard against recursion.
    let executor_for = |scope: Scope,
                        approver: Arc<dyn Approver>,
                        delegate: Option<Arc<DelegateTool>>|
     -> ToolExecutor {
        let mut tools = ToolExecutor::with_catalog(
            catalogs.of(scope).clone(),
            ToolExecutionConfig::with_result_cap(model_config.max_tool_result_bytes)
                .with_turn_budget(model_config.max_turn_result_bytes)
                .with_call_timeout_secs(model_config.tool_timeout_secs),
        )
        .with_approver(approver)
        .with_output_store(output_store.clone());
        // Only the main runtime records its tool calls in a transcript: every
        // other scope runs on a synthetic session (delegate, cron, briefing),
        // where a file per one-shot turn is litter rather than history. Set
        // here, not on the returned executor — registering a tool shares the
        // core, and the setters take `Arc::get_mut`.
        if scope == Scope::MAIN {
            tools = tools.with_transcript(db.clone());
        }
        for tool in registry.tools_for(scope) {
            tools.register(tool.clone());
        }
        for hook in registry.tool_hooks_for(scope) {
            tools.add_hook(hook.clone());
        }
        if let Some(delegate) = delegate {
            tools.register(delegate);
        }
        // Tools built from the executor itself, now that the rest are in.
        for tool in registry.build_for(scope, &tools) {
            tools.register(tool);
        }
        // A tool the policy denies outright never gets advertised: it would
        // otherwise cost a schema, a prompt entry, and a whole round-trip per
        // attempt, all to be refused. Runs before the catalog is read, so the
        // prompt's tool list and the model's schemas agree by construction.
        let dropped = tools.drop_policy_denied(&config.runtime.policy.policy);
        if !dropped.is_empty() {
            tracing::info!(tools = %dropped.join(", "), "tools withheld by a policy deny rule");
        }
        tools
    };
    // The `run_code` API listing, when that runtime loaded the tool. Rendered
    // from the same catalog the schemas come from, so the two can never
    // disagree about what a program may call.
    let code_note_for = |tools: &ToolExecutor| -> Option<String> {
        let snapshot = tools.snapshot();
        let note = snapshot
            .get("run_code")
            .is_some()
            .then(|| komo_tools::run_code::sdk_note(&snapshot))
            .flatten()?;
        // The *actual* plugins directory, because the model otherwise guesses:
        // "~/.komo/plugins" is only the default, and under Docker the home is
        // /data — a deployment lost a round of turns to exactly that guess.
        // Byte-stable per deployment (the home never changes at runtime), so
        // the prompt cache is untouched.
        Some(format!(
            "{note}\nDurable composition belongs in a plugin: a `*.py` file with \
             `@tool` functions saved into `{}` is hot-loaded within seconds and \
             becomes a `py__<name>` tool — no restart.",
            config.runtime.home.join("plugins").display()
        ))
    };
    let tool_names_of = |tools: &ToolExecutor| -> Vec<String> {
        tools
            .definitions()
            .iter()
            .map(|t| t.name().to_string())
            .collect()
    };

    // ── Sub-agent runtime (the `delegate` tool's worker) ─────────────────────
    // A real agent turn, not a bare completion: the full tool set, so a delegated
    // subtask can actually search/read/edit — and `delegate`'s `model` argument
    // picks which model does it (plan on one, apply on another).
    //
    // Safety comes from three places, none of them a new mechanism:
    //   - it is built WITHOUT `delegate`, so a sub-agent cannot spawn another;
    //   - it shares the **main approver**, and the parent's ambient session
    //     context is inherited (`AgentRuntime::handle_input` never overrides one),
    //     so every side effect still prompts the human in the real conversation
    //     and still resolves against the parent's workspace root;
    //   - it shares the run ledger, so each delegation is auditable on its own.
    // No memory enricher: a sub-agent is a worker, not the user's assistant.
    let subagent_tools = executor_for(Scope::SUBAGENT, approver.clone(), None);
    let subagent_tool_names = tool_names_of(&subagent_tools);
    let subagent_note = skills_note_for(&subagent_tool_names);
    let subagent_builder = Arc::new(
        SystemPromptBuilder::new(model_config)
            .tools(subagent_tool_names)
            .skills_note(subagent_note)
            .code_note(code_note_for(&subagent_tools))
            .workspace_root(Some(root.clone())),
    );
    let subagent_preamble: PreambleFn = Arc::new(move || subagent_builder.build());
    let subagent_llm = build_llm(
        model_config,
        Some(&subagent_tools),
        subagent_preamble,
        None,
        Some("delegate"),
    )?;
    let skill_repo: Arc<dyn SkillRepository> = skill_store.clone();
    // The seam every extracted observation goes through. It shares the query
    // service with recall, so "which existing claims might this be about" is
    // answered by the same hybrid matching that decides what gets injected.
    let consolidator = Arc::new(
        komo_services::memory_consolidation::MemoryConsolidator::new(
            memory_repo.clone(),
            aux_llm.clone(),
            memory_query.clone(),
        ),
    );
    let reviewer: Arc<dyn Reviewer> = Arc::new(ReflectiveReviewer::new(
        aux_llm.clone(),
        consolidator,
        skill_repo,
        kanban.clone(),
    ));
    // One coordinator instance shared by the runtime's post-run trigger and
    // the gateway's scheduled sweep — that sharing is what makes its
    // per-session in-flight guard effective across the two paths.
    let review = Arc::new(
        LearningCoordinator::new(
            db.clone(),
            db.clone(),
            reviewer,
            config.runtime.review_interval,
        )
        // Reads the user's next message as a verdict on the previous turn.
        // Without it every outcome stays `Unknown`, since nothing observable
        // when a turn ends tells success from silence.
        .with_feedback(aux_llm.clone()),
    );

    // Pre-images for `komo run rollback`. Swept once here rather than on a
    // schedule: the directory is read only on demand, and a gateway start is
    // the natural moment to drop what has aged out.
    let checkpoint_store = Arc::new(komo_services::checkpoint_store::FsCheckpointStore::new(
        komo_config::komo_home().join("checkpoints"),
    ));
    checkpoint_store.sweep();
    let checkpoint: Option<Arc<dyn CheckpointStore>> = Some(checkpoint_store.clone());

    // Built before the runtimes because every one of them is assembled from it.
    let parts = RuntimeParts {
        db: db.clone(),
        registry: &registry,
        history_window: model_config.max_history_messages,
        learning: review.clone(),
        checkpoint,
    };

    let subagent_runtime = Arc::new(parts.build(CapabilityProfile {
        scope: Scope::SUBAGENT,
        llm: subagent_llm,
        tools: subagent_tools,
        max_turns: model_config.max_turns,
        learns: false,
        resumable: false,
        checkpoints: true,
    }));
    let delegate = Arc::new(DelegateTool::new(
        subagent_runtime,
        db.clone(),
        model_config.menu(),
        model_config.model.clone(),
    ));

    // Only the main runtime records its tool calls in a transcript. Every other
    // scope runs on a synthetic session (delegate, cron, briefing), and a
    // transcript file per one-shot turn is litter, not history.
    let tools = executor_for(Scope::MAIN, approver.clone(), Some(delegate));

    // Assemble the tiered system prompt: stable identity + tool-aware guidance
    // (gated on the tools actually loaded) + skills catalog, then the workspace
    // project-instruction file, then the day-precision volatile footer. Wrapped
    // in a factory so `complete` rebuilds it per turn (per session) rather than
    // freezing the date at process start — important for the long-lived gateway.
    let tool_names = tool_names_of(&tools);
    let main_note = skills_note_for(&tool_names);
    let prompt_builder = Arc::new(
        SystemPromptBuilder::new(model_config)
            .tools(tool_names)
            .skills_note(main_note)
            .code_note(code_note_for(&tools))
            .workspace_root(Some(root.clone()))
            // The main agent fields "how do I configure Komo" questions, so it
            // gets the built-in platform manual (wechat login, pairing, …).
            // Aux/delegate/briefing builders deliberately don't.
            .operations_manual()
            // …and the operator-authored user profile (~/.komo/USER.md), for the
            // same reason the aux/reviewer/briefing builders don't get it.
            .user_profile()
            // …and their machine-wide agent instructions (~/.agents/AGENTS.md),
            // shared with whatever other agents read that directory.
            .global_instructions(),
    );
    let preamble: PreambleFn = Arc::new(move || prompt_builder.build());

    // Hand the same tool instances to the LLM so the model can call them, plus
    // the memory enricher (main agent only): the memory store for pinned/recall
    // selection and the aux agent for recall screening, behind one interface.
    let enricher = Arc::new(MemoryEnricher::new(
        memory_repo.clone(),
        Some(aux_llm.clone()),
        memory_query.clone(),
    ));
    let llm = build_llm(model_config, Some(&tools), preamble, Some(enricher), None)?;

    // The conversation: the only runtime that learns from what it did, and the
    // only one whose turns are worth resuming.
    let runtime = parts.build(CapabilityProfile {
        scope: Scope::MAIN,
        llm,
        // The in-house agent loop hands each round to this executor; the LLM
        // above was handed the same catalog's schemas, declaration only.
        tools,
        max_turns: model_config.max_turns,
        learns: true,
        resumable: true,
        checkpoints: true,
    });

    // ── Cron agent runtime (general cron, agent mode) ────────────────────────
    // Runs `CronAction::Agent` jobs: the SAME full tool set as the main agent
    // (so a scheduled job can act — shell, git, skills), but with the briefing's
    // unattended safety model — a `PolicyApprover` over a deny-all inner, so a
    // `Risk::Normal` action passes only through an explicit `unattended = true`
    // policy rule. Main model (jobs can be arbitrarily complex), no memory
    // enricher (sweeps aren't fed the user's memory library), and the run ledger
    // is shared so every job execution is auditable via `komo run list`.
    // Deliberately `wrap`, not `wrap_with_store`: saved grants were accumulated
    // interactively and must not leak into an unattended context, where only an
    // explicit `unattended = true` config rule may grant. (The engine enforces
    // this again for a channel-less decision — two floors, on purpose.)
    let cron_approver = komo_agent::policy_approver::PolicyApprover::wrap(
        config.runtime.policy.policy.clone(),
        Arc::new(UnattendedDeny),
    );
    // No `delegate`: the sub-agent runtime carries the *interactive* approver, and
    // handing that to an unattended job mixes trust models — a cron turn has no
    // ambient session, so the sub-agent's Risk::Normal actions would be auto-denied
    // anyway, just less legibly. A cron job that needs a sub-agent should say so
    // explicitly (its own runtime with the unattended approver), not inherit one.
    let cron_tools = executor_for(Scope::CRON, cron_approver, None);
    let cron_tool_names = tool_names_of(&cron_tools);
    // No operations_manual / user_profile: the cron agent is a background task
    // executor, not the user-facing assistant.
    let cron_note = skills_note_for(&cron_tool_names);
    let cron_builder = Arc::new(
        SystemPromptBuilder::new(model_config)
            .tools(cron_tool_names)
            .skills_note(cron_note)
            .code_note(code_note_for(&cron_tools))
            .workspace_root(Some(root.clone())),
    );
    let cron_preamble: PreambleFn = Arc::new(move || cron_builder.build());
    let cron_llm = build_llm(
        model_config,
        Some(&cron_tools),
        cron_preamble,
        None,
        Some("cron"),
    )?;
    let cron_runtime = Arc::new(parts.build(CapabilityProfile {
        scope: Scope::CRON,
        llm: cron_llm,
        tools: cron_tools,
        max_turns: model_config.max_turns,
        learns: false,
        resumable: false,
        checkpoints: true,
    }));

    // ── Briefing runtime (roadmap §2) ────────────────────────────────────────
    // A second, deliberately small agent the BriefingSweep drives: aux model,
    // read-only tool set (the plugins' `Scope::ALL` registrations — no
    // shell/file/task/memory writes), and a policy approver whose inner is
    // deny-all — there is never a human to prompt, so a `Risk::Normal` action
    // passes only through an explicit `unattended` policy rule. Safe reads
    // (web_fetch, skill view) work out of the box.
    // Sharing the run ledger (`runs: db`) makes every briefing execution
    // auditable via `komo run list`.
    // No saved grants here either — see the cron approver above.
    let briefing_approver = komo_agent::policy_approver::PolicyApprover::wrap(
        config.runtime.policy.policy.clone(),
        Arc::new(UnattendedDeny),
    );
    let briefing_tools = executor_for(Scope::BRIEFING, briefing_approver, None);
    let briefing_tool_names = tool_names_of(&briefing_tools);
    let briefing_note = skills_note_for(&briefing_tool_names);
    let briefing_builder = Arc::new(
        SystemPromptBuilder::new(&aux_config)
            .tools(briefing_tool_names)
            .skills_note(briefing_note),
    );
    let briefing_preamble: PreambleFn = Arc::new(move || briefing_builder.build());
    // No memory enricher: sweeps must not be fed the user's memory library.
    let briefing_llm = build_llm(
        &aux_config,
        Some(&briefing_tools),
        briefing_preamble,
        None,
        Some("briefing"),
    )?;
    let briefing_runtime = Arc::new(parts.build(CapabilityProfile {
        scope: Scope::BRIEFING,
        llm: briefing_llm,
        tools: briefing_tools,
        // A briefing is an aggregation read, not a long-running job.
        max_turns: BRIEFING_MAX_TURNS,
        learns: false,
        resumable: false,
        checkpoints: false,
    }));

    Ok(Wiring {
        runtime,
        review,
        aux_llm,
        memories: memory_repo,
        memory_query: memory_query.clone(),
        skills: skill_store,
        clarify,
        briefing_runtime,
        cron_runtime,
        output_store,
        wiki: wiki_ops,
    })
}

/// Round budget for the briefing runtime: enough for "list skills → load one →
/// fetch its data → compose", never a long-running loop.
const BRIEFING_MAX_TURNS: usize = 4;

/// Inner approver for the unattended briefing runtime: there is never a human
/// watching, so anything the policy didn't explicitly grant is denied.
struct UnattendedDeny;

#[async_trait::async_trait]
impl Approver for UnattendedDeny {
    async fn decide(
        &self,
        request: &crate::domain::approval::ApprovalRequest,
    ) -> crate::domain::approval::Decision {
        tracing::warn!(summary = %request.summary,
            "briefing: denied (unattended; add an `unattended = true` policy rule to grant)");
        crate::domain::approval::Decision::deny_because(
            "这是无人值守的后台任务，没有人能批准这一步。只有配置了 \
             `unattended = true` 的 [policy] 允许规则才会放行；请改用不需要审批的做法。",
        )
    }
}
