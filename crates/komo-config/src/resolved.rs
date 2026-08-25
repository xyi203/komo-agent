//! Pure resolution: raw [`ConfigSources`] → one [`RuntimeConfig`] snapshot.
//!
//! Precedence is applied here exactly once — built-in defaults < `config.toml`
//! < `KOMO_*` env — and every problem found becomes a [`ConfigIssue`] instead
//! of an early error, so diagnostic consumers (`doctor`) always see the whole
//! picture while startup paths fail fast via `ConfigSnapshot::validate_*`.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use super::report::{ConfigIssue, ConfigReport, IssueSeverity, Origin};
use super::sources::{ConfigSources, KomoEnv, PolicyFileConfig, PolicyRuleFileConfig};
use super::{Provider, split_model_id};

/// Built-in default for `max_turns` when neither `KOMO_MAX_TURNS` nor
/// config.toml sets one — the number of model **round-trips** one turn may
/// spend, not the number of tool calls (a round can call several tools at once).
///
/// Sized for the way real work actually goes with the search/edit tools:
/// locate (`grep`) → read a few pages → edit → re-read → run tests → fix. That
/// is easily 20 rounds for one file and 60+ across several, and hitting the
/// ceiling costs the whole turn's momentum — the model is forced to answer from
/// wherever it happens to be. The cheap protections against a runaway loop are
/// the per-turn output budget and the per-call timeout, not a tight round count.
pub const DEFAULT_MAX_TURNS: usize = 120;

/// Built-in per-completion timeout (seconds) when neither `KOMO_LLM_TIMEOUT_SECS`
/// nor config.toml sets one. A backstop so a hung provider request (rig's
/// default reqwest client sets no timeout) fails the turn cleanly instead of
/// wedging it in `running` forever — long enough for a slow tool-using
/// completion, short enough that a stalled request can't hang a turn all day.
pub const DEFAULT_LLM_TIMEOUT_SECS: u64 = 180;

/// Built-in default byte cap on a tool result handed back to the LLM, when
/// neither `KOMO_MAX_TOOL_RESULT_BYTES` nor config.toml sets one. Sized above
/// the per-tool self-caps (web_fetch / homeassistant trim to 8 KB) so it only
/// catches tools that don't self-trim.
pub const DEFAULT_MAX_TOOL_RESULT_BYTES: usize = 16 * 1024;

/// Built-in default per-turn cap on the *cumulative* bytes of tool output fed
/// back to the model, when neither `KOMO_MAX_TURN_RESULT_BYTES` nor config.toml
/// sets one. `max_tool_result_bytes` bounds a *single* result; this bounds the
/// whole turn, so a long tool chain (dozens of rounds, each returning a capped
/// result) can't silently accumulate past the context window and fail the turn
/// only after all the side effects have already run. `0` disables the budget.
///
/// 512 KB is roughly 128k tokens of text — at the edge of a modern context
/// window, which is the real constraint. Lower would cut off legitimate work
/// (a dozen paged reads plus a test run), higher would just move the failure
/// from "budget note" to "provider rejects the request".
pub const DEFAULT_MAX_TURN_RESULT_BYTES: usize = 512 * 1024;

/// Built-in per-tool-call wall-clock timeout (seconds) when neither
/// `KOMO_TOOL_TIMEOUT_SECS` nor config.toml sets one. A backstop so a tool that
/// hangs forever (a shell command waiting on stdin, a `reqwest` client with no
/// timeout of its own) fails the call cleanly instead of wedging the whole turn
/// — and, since the loop can't finish, the session — indefinitely. Generous
/// enough for a slow build or a large download; `0` disables the timeout.
///
/// This is only the **default**: a tool that legitimately takes longer overrides
/// it with [`Tool::max_duration`](komo_core::domain::tool::Tool::max_duration)
/// — `delegate` runs a whole sub-agent completion, `shell` honors its own
/// `timeout` argument up to ten minutes, and every approval-gated tool has to
/// outlast a human reading a prompt.
pub const DEFAULT_TOOL_TIMEOUT_SECS: u64 = 120;

/// Built-in default for `max_history_messages` when neither
/// `KOMO_MAX_HISTORY_MESSAGES` nor config.toml sets one. Counts prior messages
/// (user + assistant alternating, so ~25 turns), enough context for a chat
/// assistant while keeping a long-lived session's per-turn cost bounded. `0`
/// disables the window (replay the whole transcript, the pre-windowing behavior).
pub const DEFAULT_MAX_HISTORY_MESSAGES: usize = 50;

/// Built-in default for `max_history_bytes`. The companion bound to
/// [`DEFAULT_MAX_HISTORY_MESSAGES`]: a message *count* says nothing about size, so
/// fifty messages of pasted build logs overflow a context that fifty chat lines
/// sit comfortably inside. 256 KB is roughly 64k tokens of history — generous for
/// a chat assistant, and well clear of every menu model's window once the system
/// prompt and this turn's tool output are added. `0` disables the byte bound.
pub const DEFAULT_MAX_HISTORY_BYTES: usize = 256 * 1024;

/// Built-in reviewer cadence: run the reflective reviewer every N user turns
/// when `KOMO_REVIEW_INTERVAL` doesn't set one.
pub const DEFAULT_REVIEW_INTERVAL: usize = 10;

/// Default maintenance cron when neither `KOMO_SCHEDULE` nor config.toml
/// `schedule` sets one: hourly.
pub const DEFAULT_MAINTENANCE_SCHEDULE: &str = "0 * * * *";

/// Default dreaming-sweep schedule: nightly at 3am, mirroring OpenClaw's
/// dreaming. Unlike the briefing (proactive notifications → opt-in), dreaming is
/// internal memory housekeeping with no user-facing output, so it is **on by
/// default**.
pub const DEFAULT_DREAM_SCHEDULE: &str = "0 3 * * *";

/// Default Home Assistant URL when `HASS_URL` is unset.
const DEFAULT_HASS_URL: &str = "http://homeassistant.local:8123";

/// Default loopback bind address for the HTTP API channel. Loopback-only by
/// default so the API isn't reachable off-host without an explicit override.
const DEFAULT_API_BIND: &str = "127.0.0.1";
/// Default API port (kept distinct from hermes' 8642 to avoid a same-host clash).
const DEFAULT_API_PORT: u16 = 8765;

/// Everything the running program needs, fully resolved. Callers consume this
/// (via `ConfigSnapshot`) instead of the raw file/env/secret sources.
///
/// No `Debug` impl on purpose: several fields carry credentials.
pub struct RuntimeConfig {
    /// The `~/.komo` home directory the snapshot was resolved against.
    pub home: PathBuf,
    /// `turso:` URL of the disposable session/state db (`state.db`).
    pub db_url: String,
    /// `turso:` URL of the durable task db (`kanban.db`).
    pub kanban_db_url: String,
    /// `turso:` URL of the durable memory db (`memory.db`).
    pub memory_db_url: String,
    /// `turso:` URL of the durable scheduled-job db (`cron.db`).
    pub cron_db_url: String,
    /// Provider/model selection plus the agent-loop knobs that ride with it.
    pub model: ModelConfig,
    /// Reviewer cadence: run the reflective reviewer every N user turns.
    pub review_interval: usize,
    /// Maintenance sweep cron (5-field Unix).
    pub maintenance_schedule: String,
    /// Daily briefing cron; `None` = opt-in feature disabled.
    pub briefing_schedule: Option<String>,
    /// Gate the briefing to Chinese working days.
    pub briefing_workdays_only: bool,
    /// Dreaming sweep cron; `None` = explicitly disabled (default is on).
    pub dream_schedule: Option<String>,
    /// Embedding backend for cross-language memory recall; `None` = off, recall
    /// stays lexical-only.
    pub embedding: Option<EmbeddingConfig>,
    /// Note-vault search (`[wiki]`); `None` = not configured, the `wiki_search`
    /// tool is not registered.
    pub wiki: Option<WikiConfig>,
    /// The permission policy plus its load diagnostics.
    pub policy: PolicyReport,
    /// Extra skill directories from `KOMO_SKILLS_PATH` (colon-separated),
    /// highest priority first.
    pub skills_path: Vec<PathBuf>,
    /// Operator-configured directories that filesystem tools may read in
    /// addition to the session workspace. Each entry is canonicalized during
    /// config resolution and is never a write root.
    pub readable_roots: Vec<PathBuf>,
    /// The `homeassistant` *tool* credentials (`HASS_TOKEN`/`HASS_URL`);
    /// `None` = token unset, tool not registered.
    pub homeassistant_tool: Option<HomeAssistantConfig>,
    /// External MCP servers to connect at startup, already filtered to the
    /// usable ones (a server missing its url, token, or tool list is dropped
    /// here with a warning rather than failing the boot).
    pub mcp_servers: Vec<McpServerConfig>,
    pub feishu: ChannelState<FeishuConfig>,
    pub telegram: ChannelState<TelegramConfig>,
    pub wechat: ChannelState<WeChatConfig>,
    /// The HTTP api channel is always on (the CLI reaches a running gateway
    /// through it), so this is never `Disabled` — only `Ready` (loopback or
    /// external) or `Misconfigured` (external without a key).
    pub api: ChannelState<ApiConfig>,
    /// Explicit `[plugins.<name>] enabled` toggles. Absence = enabled; the
    /// wiring layer (which knows the plugin roster) warns about unknown names.
    pub plugin_toggles: std::collections::BTreeMap<String, bool>,
}

impl RuntimeConfig {
    /// Whether the uniform `[plugins]` overlay leaves `name` enabled.
    pub fn plugin_enabled(&self, name: &str) -> bool {
        self.plugin_toggles.get(name).copied().unwrap_or(true)
    }
}

/// One resolved MCP server: reachable-looking config with its token already
/// read out of the environment.
///
/// No `Debug`: `token` is a credential.
#[derive(Clone)]
pub struct McpServerConfig {
    /// The operator's name for the server (the `[mcp.servers.<name>]` key).
    /// Namespaces its tools in the catalog, so it must be stable.
    pub name: String,
    pub url: String,
    pub token: Option<String>,
    /// Tools to mount; empty means "everything the server advertises"
    /// (`all_tools = true`). Resolution rejects the empty-and-not-all case, so
    /// an empty list here is always deliberate.
    pub tools: Vec<String>,
}

/// Resolved embedding backend for L3 memory recall.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingConfig {
    /// Ollama model id; also stored on each memory so vectors from a different
    /// model are never compared against each other.
    pub model: String,
    /// Ollama base URL.
    pub url: String,
}

/// Resolved `[wiki]` note-vault search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WikiConfig {
    /// Root of the note vault, `~` expanded. The switch for the whole feature:
    /// no vault, no wiki.
    pub vault: PathBuf,
    /// Backend selector, validated by `komo-wiki` (`edge` or `server`). Kept as
    /// a string here so `komo-config` does not depend on the vector crate.
    pub backend: String,
    /// Where the embedded backend keeps its files (`~/.komo/wiki`). Disposable.
    pub data_dir: PathBuf,
    /// Qdrant endpoint, used only by the `server` backend.
    pub url: String,
    /// Collection name, shared by both backends so an index is portable.
    pub collection: String,
    /// Embedding backend for the vault. Falls back to `[memory]`'s when `[wiki]`
    /// declares no model, so the common case configures one model, and a vault
    /// that wants a bigger one can say so without touching recall.
    pub embedding: EmbeddingConfig,
}

/// One ingress channel's resolved state.
pub enum ChannelState<T> {
    /// Not declared, or declared with `enabled = false`.
    Disabled,
    /// Enabled and fully configured.
    Ready(T),
    /// Enabled but unusable; the message names what is missing. Resolution
    /// also records a fatal [`ConfigIssue`], so `validate_gateway` fails.
    Misconfigured(String),
}

impl<T> ChannelState<T> {
    /// The config when the channel is ready to serve.
    pub fn ready(&self) -> Option<&T> {
        match self {
            ChannelState::Ready(cfg) => Some(cfg),
            _ => None,
        }
    }
}

/// One selectable model, resolved from a (possibly provider-qualified) menu id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelEntry {
    /// The menu id as configured — qualified (`deepseek:deepseek-chat`) or not.
    /// This is what a client sends back and what is stored on a session.
    pub id: String,
    pub provider: Provider,
    /// The bare model id handed to the provider.
    pub model: String,
    /// Reasoning-effort levels valid for *this* entry's provider.
    pub efforts: &'static [&'static str],
}

/// Resolved model selection: provider, model id, API key, and optional overrides.
pub struct ModelConfig {
    pub provider: Provider,
    pub model: String,
    /// The models a client may switch a session to, `model` first. Always
    /// non-empty (it contains at least `model`). An entry may be **qualified**
    /// (`deepseek:deepseek-chat`) to name a different backend than `provider`, so
    /// one menu can span providers — see [`split_model_id`] for the syntax and
    /// [`Self::menu`] for the resolved form. The api channel advertises it over
    /// `/api/models` and validates a client's request against it, so a typo can
    /// never reach a provider.
    pub models: Vec<String>,
    /// API keys for **every** provider that has one configured, not just
    /// `provider` — a cross-provider menu needs a client per backend. Codex is
    /// absent by design (OAuth, see [`Provider::uses_api_key`]).
    pub keys: HashMap<Provider, String>,
    /// Empty for Codex (OAuth via `~/.codex/auth.json`) and when the key is
    /// missing — the latter is recorded as a fatal issue.
    pub api_key: String,
    /// Optional base-URL override for OpenAI-compatible endpoints.
    pub base_url: Option<String>,
    /// Optional cheaper model for auxiliary sub-tasks.
    pub aux_model: Option<String>,
    /// Maximum tool-calling round-trips per user turn.
    pub max_turns: usize,
    /// Byte cap on a single tool result handed back to the LLM (global backstop).
    pub max_tool_result_bytes: usize,
    /// Cumulative per-turn cap on tool output fed back to the model (`0` =
    /// unlimited). Bounds a whole tool chain, not one result.
    pub max_turn_result_bytes: usize,
    /// Per-tool-call wall-clock timeout in seconds — a hung tool fails the call
    /// cleanly rather than wedging the turn forever (`0` = no timeout).
    pub tool_timeout_secs: u64,
    /// Max prior messages replayed as history per turn (`0` = unlimited).
    pub max_history_messages: usize,
    /// Byte budget for that replayed history (`0` = unlimited). Applied after the
    /// count window, trimming from the oldest end.
    pub max_history_bytes: usize,
    /// Per-completion timeout in seconds — a hung provider request fails the
    /// turn cleanly rather than wedging it forever (`0` = no timeout).
    pub llm_timeout_secs: u64,
}

impl fmt::Debug for ModelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelConfig")
            .field("provider", &self.provider)
            .field("model", &self.model)
            .field("models", &self.models)
            .field("api_key", &mask_secret(&self.api_key))
            .field("base_url", &self.base_url)
            .field("aux_model", &self.aux_model)
            .field("max_turns", &self.max_turns)
            .field("max_tool_result_bytes", &self.max_tool_result_bytes)
            .field("max_turn_result_bytes", &self.max_turn_result_bytes)
            .field("tool_timeout_secs", &self.tool_timeout_secs)
            .field("max_history_messages", &self.max_history_messages)
            .field("max_history_bytes", &self.max_history_bytes)
            .field("llm_timeout_secs", &self.llm_timeout_secs)
            .finish()
    }
}

/// The switchable-model menu, `default_model` always first.
///
/// `KOMO_MODELS` (comma-separated) wins over config.toml `models`; with neither,
/// the menu is the configured model plus `aux_model` — enough for the common
/// "one strong model, one cheap one" setup without any config at all. Blanks and
/// duplicates are dropped, and the default is force-included so a menu that
/// omits it can't leave the running model unselectable.
fn resolve_model_menu(
    default_model: &str,
    aux_model: Option<&str>,
    env_models: Option<&str>,
    file_models: Option<&[String]>,
) -> Vec<String> {
    let declared: Vec<String> = match env_models {
        Some(csv) => csv.split(',').map(|s| s.trim().to_string()).collect(),
        None => match file_models {
            Some(list) => list.iter().map(|s| s.trim().to_string()).collect(),
            None => aux_model.into_iter().map(str::to_string).collect(),
        },
    };
    let mut menu = vec![default_model.to_string()];
    for name in declared {
        if !name.is_empty() && !menu.contains(&name) {
            menu.push(name);
        }
    }
    menu
}

/// Show first 3 and last 4 chars; fully mask short keys.
fn mask_secret(s: &str) -> String {
    if s.len() <= 7 {
        return "***".to_string();
    }
    format!("{}...{}", &s[..3], &s[s.len() - 4..])
}

impl ModelConfig {
    /// The menu resolved into one entry per selectable model, in declared order.
    ///
    /// Entries whose provider has no usable credential are **dropped**: offering
    /// a model that errors on every turn is worse than not offering it. The
    /// configured `model` is the exception — it always survives, because hiding
    /// the model the gateway is actually running would misreport reality (its
    /// missing key is already a startup warning, and the turn's reply says so).
    pub fn menu(&self) -> Vec<ModelEntry> {
        let mut out: Vec<ModelEntry> = Vec::new();
        for id in &self.models {
            let (qualified, bare) = split_model_id(id);
            let provider = qualified.unwrap_or(self.provider);
            let is_default = id == &self.model;
            if !is_default && !self.has_credential(provider) {
                continue;
            }
            out.push(ModelEntry {
                id: id.clone(),
                provider,
                model: bare.to_string(),
                efforts: provider.efforts(),
            });
        }
        out
    }

    /// Is this provider usable — a key present, or Codex (OAuth, validated
    /// separately by `komo doctor`)?
    pub fn has_credential(&self, provider: Provider) -> bool {
        !provider.uses_api_key() || self.keys.contains_key(&provider)
    }

    /// Every provider the resolved menu actually needs a client for.
    pub fn menu_providers(&self) -> Vec<Provider> {
        let mut out = Vec::new();
        for entry in self.menu() {
            if !out.contains(&entry.provider) {
                out.push(entry.provider);
            }
        }
        if !out.contains(&self.provider) {
            out.push(self.provider);
        }
        out
    }

    /// This config re-pointed at `provider`, running `model`. Used to build one
    /// backend per provider behind the cross-provider router: the agent-loop
    /// knobs carry over, only the identity and credential change.
    ///
    /// `base_url` deliberately does **not** carry over to a non-default provider —
    /// it overrides one specific endpoint, and applying it to every backend would
    /// silently point deepseek at an OpenAI-compatible proxy.
    pub fn for_provider(&self, provider: Provider, model: String) -> ModelConfig {
        let default_provider = provider == self.provider;
        ModelConfig {
            provider,
            models: vec![model.clone()],
            model,
            api_key: self.keys.get(&provider).cloned().unwrap_or_default(),
            keys: self.keys.clone(),
            base_url: default_provider.then(|| self.base_url.clone()).flatten(),
            aux_model: self.aux_model.clone(),
            max_turns: self.max_turns,
            max_tool_result_bytes: self.max_tool_result_bytes,
            max_turn_result_bytes: self.max_turn_result_bytes,
            tool_timeout_secs: self.tool_timeout_secs,
            max_history_messages: self.max_history_messages,
            max_history_bytes: self.max_history_bytes,
            llm_timeout_secs: self.llm_timeout_secs,
        }
    }

    /// A variant using the cheaper `aux_model`, falling back to the main model.
    pub fn aux_variant(&self) -> ModelConfig {
        let model = self.aux_model.clone().unwrap_or_else(|| self.model.clone());
        ModelConfig {
            provider: self.provider,
            // The aux agent is not switchable: it runs the configured aux model,
            // period. A one-entry menu keeps `allows_model` honest for it.
            models: vec![model.clone()],
            model,
            api_key: self.api_key.clone(),
            keys: self.keys.clone(),
            base_url: self.base_url.clone(),
            aux_model: self.aux_model.clone(),
            max_turns: self.max_turns,
            max_tool_result_bytes: self.max_tool_result_bytes,
            max_turn_result_bytes: self.max_turn_result_bytes,
            tool_timeout_secs: self.tool_timeout_secs,
            max_history_messages: self.max_history_messages,
            max_history_bytes: self.max_history_bytes,
            llm_timeout_secs: self.llm_timeout_secs,
        }
    }
}

/// The resolved policy plus load diagnostics (for `komo policy list` / doctor).
pub struct PolicyReport {
    pub policy: komo_core::domain::policy::Policy,
    /// Who answers a `Verdict::Ask` — the human, or the aux reviewer first.
    /// Carried beside the policy rather than inside it: the rule engine's
    /// verdicts do not depend on it (see `domain::policy::PolicyMode`).
    pub mode: komo_core::domain::policy::PolicyMode,
    /// Config indices (0-based `[[policy.rule]]` order) of ignored invalid rules.
    pub invalid: Vec<usize>,
    /// Whether a `[policy]` table was present at all.
    pub configured: bool,
}

/// Resolved Feishu channel settings.
pub struct FeishuConfig {
    pub app_id: String,
    pub app_secret: String,
    pub allow_from: Vec<String>,
    pub require_mention: bool,
    pub home_chat: Option<String>,
}

/// Resolved Telegram channel settings.
pub struct TelegramConfig {
    pub bot_token: String,
    pub allow_from: Vec<String>,
    pub allowed_chats: Vec<String>,
    pub require_mention: bool,
    pub home_chat: Option<String>,
}

/// Resolved WeChat channel settings.
pub struct WeChatConfig {
    pub allow_from: Vec<String>,
    pub home_chat: Option<String>,
}

/// Resolved Home Assistant settings for the `homeassistant` tool.
pub struct HomeAssistantConfig {
    pub base_url: String,
    pub token: String,
}

/// Resolved HTTP API channel settings.
pub struct ApiConfig {
    pub bind: String,
    /// `0` means "let the OS assign an ephemeral port" — the actual port is read
    /// back after bind and published in the rendezvous file for the CLI.
    pub port: u16,
    pub server_key: String,
    /// Optional built web SPA to serve same-origin (static assets public, api
    /// key-gated). `None` = no static serving.
    pub web_dir: Option<String>,
    /// Allow keyed remote callers to use interactive turns + resolve
    /// approval/clarify (see [`crate::sources::ApiFileConfig`]).
    pub remote_interactive: bool,
}

/// Resolve one consistent read of the sources into the runtime snapshot plus
/// its redacted report. Never fails: problems become [`ConfigIssue`]s.
pub(super) fn resolve(sources: ConfigSources) -> (RuntimeConfig, ConfigReport) {
    let ConfigSources {
        home,
        file,
        env,
        secrets,
        env_error,
    } = sources;

    let skills_path = skills_dirs(&env);
    let mut issues = Vec::new();
    if let Some(message) = env_error {
        issues.push(ConfigIssue {
            path: "env",
            severity: IssueSeverity::Fatal,
            message,
        });
    }

    // Provider/model, with provenance for `doctor` / `model list`.
    let (provider_str, provider_origin) = pick(env.provider, file.provider, || {
        Provider::DeepSeek.name().to_string()
    });
    let provider = Provider::parse(&provider_str).unwrap_or_else(|e| {
        issues.push(ConfigIssue {
            path: "model.provider",
            severity: IssueSeverity::Fatal,
            message: e.to_string(),
        });
        Provider::DeepSeek
    });
    let (model, model_origin) = pick(env.model, file.model, || {
        provider.default_model().to_string()
    });

    // Codex authenticates from `~/.codex/auth.json`, not an env key — its
    // `api_key` stays empty and is resolved in `infra/codex.rs`.
    //
    // A missing key is a *warning*, not a fatal issue: a fresh install (first
    // gateway boot in Docker, `komo init` before any credential exists) must
    // come up rather than crash-loop. `build_llm` degrades to a client whose
    // every call reports this same fix, so turns fail with guidance instead.
    let api_key = if provider.uses_api_key() {
        match secrets.key(provider) {
            Some(key) => key.to_string(),
            None => {
                issues.push(ConfigIssue {
                    path: "model.api_key",
                    severity: IssueSeverity::Warning,
                    message: format!(
                        "{} is not set (required for {provider:?}) — agent turns will \
                         fail until it is added to ~/.komo/.env (see `komo init`)",
                        provider.api_key_var()
                    ),
                });
                String::new()
            }
        }
    } else {
        String::new()
    };

    let provider_key_present = Provider::ALL
        .iter()
        .map(|p| (*p, secrets.key(*p).is_some()))
        .collect();

    // Every configured key, not just the active provider's: a cross-provider
    // `models` menu needs a client per backend (see `ModelConfig::for_provider`).
    let keys: HashMap<Provider, String> = Provider::ALL
        .iter()
        .filter_map(|p| secrets.key(*p).map(|k| (*p, k.to_string())))
        .collect();

    let aux_model = env.aux_model.or(file.aux_model);
    let models = resolve_model_menu(
        &model,
        aux_model.as_deref(),
        env.models.as_deref(),
        file.models.as_deref(),
    );
    let model = ModelConfig {
        provider,
        models,
        model,
        api_key,
        keys,
        base_url: env.base_url.or(file.base_url),
        aux_model,
        max_turns: env
            .max_turns
            .or(file.max_turns)
            .unwrap_or(DEFAULT_MAX_TURNS),
        max_tool_result_bytes: env
            .max_tool_result_bytes
            .or(file.max_tool_result_bytes)
            .unwrap_or(DEFAULT_MAX_TOOL_RESULT_BYTES),
        max_turn_result_bytes: env
            .max_turn_result_bytes
            .or(file.max_turn_result_bytes)
            .unwrap_or(DEFAULT_MAX_TURN_RESULT_BYTES),
        tool_timeout_secs: env
            .tool_timeout_secs
            .or(file.tool_timeout_secs)
            .unwrap_or(DEFAULT_TOOL_TIMEOUT_SECS),
        max_history_messages: env
            .max_history_messages
            .or(file.max_history_messages)
            .unwrap_or(DEFAULT_MAX_HISTORY_MESSAGES),
        max_history_bytes: env
            .max_history_bytes
            .or(file.max_history_bytes)
            .unwrap_or(DEFAULT_MAX_HISTORY_BYTES),
        llm_timeout_secs: env
            .llm_timeout_secs
            .or(file.llm_timeout_secs)
            .unwrap_or(DEFAULT_LLM_TIMEOUT_SECS),
    };

    let policy = match file.policy {
        Some(cfg) => build_policy(cfg, &mut issues),
        None => PolicyReport {
            policy: Default::default(),
            mode: Default::default(),
            invalid: Vec::new(),
            configured: false,
        },
    };

    // Reading outside the workspace is deliberately opt-in. Canonicalize only
    // existing directories so aliases and duplicate entries do not create
    // surprising prefixes in the Workspace allow-list.
    let readable_roots = resolve_readable_roots(file.readable_roots, &mut issues);

    // The homeassistant tool credentials.
    let homeassistant_tool = secrets
        .hass_token
        .clone()
        .filter(|s| !s.is_empty())
        .map(|token| HomeAssistantConfig {
            // Trim a trailing slash so `{base_url}/api/...` never doubles up.
            base_url: secrets
                .hass_url
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| DEFAULT_HASS_URL.to_string())
                .trim_end_matches('/')
                .to_string(),
            token,
        });

    let channels = file.channels.unwrap_or_default();
    let feishu = match channels.feishu.filter(|c| c.enabled) {
        None => ChannelState::Disabled,
        Some(cfg) => {
            let creds = require_secret(&secrets.feishu_app_id, "feishu", "FEISHU_APP_ID").and_then(
                |app_id| {
                    require_secret(&secrets.feishu_app_secret, "feishu", "FEISHU_APP_SECRET")
                        .map(|app_secret| (app_id, app_secret))
                },
            );
            match creds {
                Ok((app_id, app_secret)) => ChannelState::Ready(FeishuConfig {
                    app_id,
                    app_secret,
                    allow_from: cfg.allow_from,
                    require_mention: cfg.require_mention.unwrap_or(true),
                    home_chat: cfg.home_chat,
                }),
                Err(message) => misconfigured(&mut issues, "channels.feishu", message),
            }
        }
    };
    let telegram = match channels.telegram.filter(|c| c.enabled) {
        None => ChannelState::Disabled,
        Some(cfg) => match require_secret(
            &secrets.telegram_bot_token,
            "telegram",
            "TELEGRAM_BOT_TOKEN",
        ) {
            Ok(bot_token) => ChannelState::Ready(TelegramConfig {
                bot_token,
                allow_from: cfg.allow_from,
                allowed_chats: cfg.allowed_chats,
                require_mention: cfg.require_mention.unwrap_or(true),
                home_chat: cfg.home_chat,
            }),
            Err(message) => misconfigured(&mut issues, "channels.telegram", message),
        },
    };
    // WeChat has no credential to check here — login is QR-based and the token
    // lives in `~/.komo/wechat/credentials.json`, verified at serve time.
    let wechat = match channels.wechat.filter(|c| c.enabled) {
        None => ChannelState::Disabled,
        Some(cfg) => ChannelState::Ready(WeChatConfig {
            allow_from: cfg.allow_from,
            home_chat: cfg.home_chat,
        }),
    };
    let api_file = channels.api.unwrap_or_default();
    // Shared by both branches (external and loopback-only).
    let api_web_dir = api_file.web_dir.clone().filter(|s| !s.is_empty());
    let api_remote_interactive = api_file.remote_interactive.unwrap_or(false);
    let api = if api_file.enabled {
        // Externally reachable: honor the configured bind/port and require a key.
        match require_secret(&secrets.api_server_key, "api", "API_SERVER_KEY") {
            Ok(server_key) => ChannelState::Ready(ApiConfig {
                bind: api_file
                    .bind
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| DEFAULT_API_BIND.to_string()),
                port: api_file.port.unwrap_or(DEFAULT_API_PORT),
                server_key,
                web_dir: api_web_dir.clone(),
                remote_interactive: api_remote_interactive,
            }),
            Err(message) => misconfigured(&mut issues, "channels.api", message),
        }
    } else {
        // Always-on, loopback-only, CLI-facing: ephemeral port (discovered via
        // the rendezvous file), and the configured key if any, else a generated
        // one. Loopback-only, so a v4 token is ample.
        ChannelState::Ready(ApiConfig {
            bind: DEFAULT_API_BIND.to_string(),
            port: 0,
            server_key: secrets
                .api_server_key
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string()),
            web_dir: api_web_dir,
            remote_interactive: api_remote_interactive,
        })
    };

    let db_url = |file: &str| format!("turso:{}", home.join(file).display());
    // Resolved before the struct literal because `wiki` falls back to the
    // `[memory]` backend when it declares no model of its own.
    let embedding = resolve_embedding(file.memory);
    let wiki = resolve_wiki(file.wiki, embedding.as_ref(), &home, &mut issues);
    let briefing_enabled = env
        .briefing_schedule_enabled
        .or(file.briefing_schedule_enabled)
        .unwrap_or(true);
    let dream_enabled = env
        .dream_schedule_enabled
        .or(file.dream_schedule_enabled)
        .unwrap_or(true);

    let runtime = RuntimeConfig {
        db_url: db_url("state.db"),
        kanban_db_url: db_url("kanban.db"),
        memory_db_url: db_url("memory.db"),
        cron_db_url: db_url("cron.db"),
        home,
        model,
        review_interval: env
            .review_interval
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_REVIEW_INTERVAL),
        maintenance_schedule: env
            .schedule
            .or(file.schedule)
            .unwrap_or_else(|| DEFAULT_MAINTENANCE_SCHEDULE.to_string()),
        briefing_schedule: enabled_then(
            briefing_enabled,
            env.briefing_schedule.or(file.briefing_schedule),
        ),
        briefing_workdays_only: env
            .briefing_workdays_only
            .or(file.briefing_workdays_only)
            .unwrap_or(false),
        dream_schedule: enabled_then(
            dream_enabled,
            resolve_dream_schedule(env.dream_schedule.or(file.dream_schedule)),
        ),
        embedding,
        wiki,
        policy,
        skills_path,
        readable_roots,
        homeassistant_tool,
        mcp_servers: resolve_mcp_servers(file.mcp, &mut issues),
        feishu,
        telegram,
        wechat,
        api,
        plugin_toggles: file
            .plugins
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(name, cfg)| cfg.enabled.map(|enabled| (name, enabled)))
            .collect(),
    };
    let report = ConfigReport {
        issues,
        provider_origin,
        model_origin,
        provider_key_present,
    };
    (runtime, report)
}

/// Resolve `[mcp.servers.*]` into the servers worth attempting a connection to.
///
/// Every rejection is a **warning**, never fatal: an MCP server is an optional
/// external integration, and a typo in one table must not stop the gateway from
/// booting (the same call komo makes for a missing model key or a token-less HA
/// channel). The affected server's tools are simply absent.
///
/// This is the one place `std::env::var` is read for a *dynamically named*
/// variable — `Secrets` is an `envy` struct over a fixed field set, and
/// `token_env` names its variable at runtime.
fn resolve_mcp_servers(
    mcp: Option<crate::sources::McpFileConfig>,
    issues: &mut Vec<ConfigIssue>,
) -> Vec<McpServerConfig> {
    const PATH: &str = "mcp.servers";
    let mut warn = |message: String| {
        issues.push(ConfigIssue {
            path: PATH,
            severity: IssueSeverity::Warning,
            message,
        });
    };

    let Some(mcp) = mcp else {
        return Vec::new();
    };
    let mut resolved = Vec::new();
    for (name, cfg) in mcp.servers {
        if !cfg.enabled.unwrap_or(true) {
            continue;
        }
        let url = cfg.url.trim().to_string();
        if url.is_empty() {
            warn(format!("[mcp.servers.{name}] has no `url`; server skipped"));
            continue;
        }
        // Naming a `token_env` states the server needs auth. Connecting anyway
        // would trade a clear config warning for a 401 at call time, which
        // reads like a bad token rather than an unset one.
        let token = match cfg
            .token_env
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            None => None,
            Some(var) => match std::env::var(var) {
                Ok(value) if !value.trim().is_empty() => Some(value),
                _ => {
                    warn(format!(
                        "[mcp.servers.{name}] names token_env = \"{var}\" but it is not set \
                         (put it in ~/.komo/.env); server skipped"
                    ));
                    continue;
                }
            },
        };
        let tools: Vec<String> = cfg
            .tools
            .into_iter()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect();
        // Closed by default, like the HA channel's event filters: a server can
        // advertise dozens of tools, and every mounted one costs a schema on
        // every round. Mounting them all because the operator forgot to choose
        // is the expensive failure mode, so it must be asked for.
        if tools.is_empty() && !cfg.all_tools {
            warn(format!(
                "[mcp.servers.{name}] lists no `tools`; nothing is mounted. \
                 List the tool names, or set `all_tools = true` to mount everything."
            ));
            continue;
        }
        resolved.push(McpServerConfig {
            name,
            url,
            token,
            tools: if cfg.all_tools { Vec::new() } else { tools },
        });
    }
    resolved
}

/// Default Ollama endpoint — the daemon's own default bind.
const DEFAULT_EMBEDDING_URL: &str = "http://127.0.0.1:11434";

/// Resolve the `[memory]` embedding backend. The model name is the switch: no
/// `[memory]` table, or an empty/absent `embedding_model`, means embeddings are
/// off and recall stays lexical-only.
fn resolve_embedding(memory: Option<crate::sources::MemoryFileConfig>) -> Option<EmbeddingConfig> {
    let memory = memory?;
    let model = memory.embedding_model?;
    let model = model.trim();
    if model.is_empty() {
        return None;
    }
    Some(EmbeddingConfig {
        model: model.to_string(),
        url: memory
            .embedding_url
            .map(|u| u.trim().to_string())
            .filter(|u| !u.is_empty())
            .unwrap_or_else(|| DEFAULT_EMBEDDING_URL.to_string()),
    })
}

/// Default Qdrant gRPC endpoint (the server's own default bind).
const DEFAULT_QDRANT_URL: &str = "http://127.0.0.1:6334";
/// Default collection name, shared by both backends.
const DEFAULT_WIKI_COLLECTION: &str = "komo_wiki";

/// Expand a leading `~/` against the **real** home, not `KOMO_HOME`: an
/// operator-typed path names their own directory and does not move when komo's
/// home is relocated.
pub fn expand_home(path: &str) -> PathBuf {
    match path.strip_prefix("~/") {
        Some(rest) => dirs::home_dir()
            .map(|h| h.join(rest))
            .unwrap_or_else(|| PathBuf::from(path)),
        None => PathBuf::from(path),
    }
}

/// Resolve the `[wiki]` table.
///
/// `vault` is the switch: no `[wiki]`, or no vault path, means the feature is
/// off and `wiki_search` is never registered — the same shape as `[memory]`'s
/// `embedding_model`.
///
/// A vault with no embedding backend anywhere is a *warning*, not a fatal:
/// booting without note search is survivable, and a fatal here would take the
/// whole gateway down over an optional feature. It returns `None` so nothing
/// downstream sees a half-configured wiki.
fn resolve_wiki(
    wiki: Option<crate::sources::WikiFileConfig>,
    memory_embedding: Option<&EmbeddingConfig>,
    home: &std::path::Path,
    issues: &mut Vec<ConfigIssue>,
) -> Option<WikiConfig> {
    let wiki = wiki?;
    let vault = wiki
        .vault
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())?;

    let own_model = wiki
        .embedding_model
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty());
    let own_url = wiki
        .embedding_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty());

    let embedding = match (own_model, memory_embedding) {
        // An explicit `[wiki] embedding_model` wins; its url falls back to
        // `[memory]`'s so pointing both at one Ollama host stays a one-liner.
        (Some(model), inherited) => EmbeddingConfig {
            model: model.to_string(),
            url: own_url
                .map(str::to_string)
                .or_else(|| inherited.map(|e| e.url.clone()))
                .unwrap_or_else(|| DEFAULT_EMBEDDING_URL.to_string()),
        },
        (None, Some(inherited)) => inherited.clone(),
        (None, None) => {
            issues.push(ConfigIssue {
                path: "wiki",
                severity: IssueSeverity::Warning,
                message: "[wiki] declares a vault but no embedding model, and \
                          [memory] has none to inherit — note search is off"
                    .to_string(),
            });
            return None;
        }
    };

    Some(WikiConfig {
        vault: expand_home(vault),
        backend: wiki
            .backend
            .as_deref()
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .unwrap_or("edge")
            .to_string(),
        data_dir: home.join("wiki"),
        url: wiki
            .url
            .as_deref()
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .unwrap_or(DEFAULT_QDRANT_URL)
            .to_string(),
        collection: wiki
            .collection
            .as_deref()
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .unwrap_or(DEFAULT_WIKI_COLLECTION)
            .to_string(),
        embedding,
    })
}

fn resolve_readable_roots(
    configured: Option<Vec<PathBuf>>,
    issues: &mut Vec<ConfigIssue>,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for path in configured.unwrap_or_default() {
        match canonical_directory(&path) {
            Some(path) if !roots.contains(&path) => roots.push(path),
            Some(_) => {}
            None => issues.push(ConfigIssue {
                path: "readable_roots",
                severity: IssueSeverity::Warning,
                message: format!(
                    "{} is not an existing directory; ignoring it",
                    path.display()
                ),
            }),
        }
    }
    roots
}

fn canonical_directory(path: &Path) -> Option<PathBuf> {
    let canonical = path.canonicalize().ok()?;
    canonical.is_dir().then_some(canonical)
}

/// env > file > default, tagging where the value came from.
fn pick(
    env: Option<String>,
    file: Option<String>,
    default: impl FnOnce() -> String,
) -> (String, Origin) {
    match (env, file) {
        (Some(v), _) => (v, Origin::Env),
        (None, Some(v)) => (v, Origin::File),
        (None, None) => (default(), Origin::Default),
    }
}

/// Record the fatal issue an enabled-but-broken channel produces and return its
/// state. One message serves both surfaces: the state (doctor's channel line)
/// and the issue (`validate_gateway`'s fail-fast error).
fn misconfigured<T>(
    issues: &mut Vec<ConfigIssue>,
    path: &'static str,
    message: String,
) -> ChannelState<T> {
    issues.push(ConfigIssue {
        path,
        severity: IssueSeverity::Fatal,
        message: message.clone(),
    });
    ChannelState::Misconfigured(message)
}

/// Resolve a required channel credential read from `~/.komo/.env`. Channels
/// keep secrets in the environment, never in `config.toml`; an enabled channel
/// missing its secret gets one uniform message.
fn require_secret(value: &Option<String>, channel: &str, var: &str) -> Result<String, String> {
    value
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            format!("[channels.{channel}] is enabled but {var} is not set (put it in ~/.komo/.env)")
        })
}

/// A `*_schedule_enabled = false` wins over whatever cron is configured: the
/// switch exists so a deployment can silence a sweep without erasing the
/// schedule it should come back to.
fn enabled_then(enabled: bool, schedule: Option<String>) -> Option<String> {
    enabled.then_some(schedule).flatten()
}

/// Pure resolution of the dreaming schedule from its configured value: unset →
/// the default (dreaming is on by default); empty or `off`/`none`/`disabled` →
/// `None` (off); anything else is taken as the cron expression.
fn resolve_dream_schedule(configured: Option<String>) -> Option<String> {
    match configured {
        Some(s)
            if s.trim().is_empty()
                || matches!(
                    s.trim().to_ascii_lowercase().as_str(),
                    "off" | "none" | "disabled"
                ) =>
        {
            None
        }
        Some(s) => Some(s),
        None => Some(DEFAULT_DREAM_SCHEDULE.to_string()),
    }
}

/// `KOMO_SKILLS_PATH` (colon-separated) → extra skill dirs, order preserved.
fn skills_dirs(env: &KomoEnv) -> Vec<PathBuf> {
    env.skills_path
        .as_deref()
        .map(|extra| {
            extra
                .split(':')
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_default()
}

fn build_policy(cfg: PolicyFileConfig, issues: &mut Vec<ConfigIssue>) -> PolicyReport {
    use komo_core::domain::policy::{Policy, PolicyMode, Verdict};

    let default_normal = cfg
        .default_normal
        .as_deref()
        .and_then(Verdict::parse_default)
        .unwrap_or(Verdict::Ask);

    // An unreadable mode falls back to `ask` with a warning rather than
    // silently enabling the reviewer: a typo must never widen the gate.
    let mode = match cfg.mode.as_deref() {
        None => PolicyMode::Ask,
        Some(raw) => PolicyMode::parse(raw).unwrap_or_else(|| {
            issues.push(ConfigIssue {
                path: "policy.mode",
                severity: IssueSeverity::Warning,
                message: format!("[policy] mode = \"{raw}\" is not ask/auto, using ask"),
            });
            PolicyMode::Ask
        }),
    };

    let mut rules = Vec::new();
    let mut invalid = Vec::new();
    for (i, r) in cfg.rule.into_iter().enumerate() {
        match build_rule(r) {
            Some(rule) => rules.push(rule),
            None => {
                issues.push(ConfigIssue {
                    path: "policy.rule",
                    severity: IssueSeverity::Warning,
                    message: format!("[[policy.rule]] #{i} is invalid, ignoring it"),
                });
                invalid.push(i);
            }
        }
    }
    PolicyReport {
        policy: Policy::new(rules, default_normal),
        mode,
        invalid,
        configured: true,
    }
}

#[cfg(test)]
mod policy_rule_tests {
    use super::*;
    use komo_core::domain::policy::{Access, Category, Matcher, PolicyMode};

    fn mode_of(raw: Option<&str>) -> (PolicyMode, usize) {
        let mut issues = Vec::new();
        let report = build_policy(
            PolicyFileConfig {
                mode: raw.map(str::to_string),
                ..Default::default()
            },
            &mut issues,
        );
        (report.mode, issues.len())
    }

    /// A typo must never turn the reviewer on: an unreadable mode falls back to
    /// `ask` **and** says so, rather than silently widening the gate.
    #[test]
    fn policy_mode_defaults_to_ask_and_warns_on_a_bad_value() {
        assert_eq!(mode_of(None), (PolicyMode::Ask, 0));
        assert_eq!(mode_of(Some("ask")), (PolicyMode::Ask, 0));
        assert_eq!(mode_of(Some(" AUTO ")), (PolicyMode::Auto, 0));
        assert_eq!(mode_of(Some("yolo")), (PolicyMode::Ask, 1));
    }

    fn rule(category: &str, matcher: &str, value: &str) -> PolicyRuleFileConfig {
        PolicyRuleFileConfig {
            category: category.to_string(),
            matcher: matcher.to_string(),
            value: value.to_string(),
            effect: "deny".to_string(),
            ..Default::default()
        }
    }

    /// `category = "shell", effect = "deny"` with no `match`/`value` is the
    /// whole-category form — it must survive parsing, since it's what takes a
    /// tool out of the model's catalog.
    #[test]
    fn a_rule_without_match_or_value_is_a_wildcard() {
        let parsed = build_rule(rule("shell", "", "")).expect("wildcard rule is valid");
        assert_eq!(parsed.matcher, Matcher::Any);
        assert_eq!(parsed.category, Category::Shell);

        // Still scopable by access — deny every write, leave reads alone.
        let mut r = rule("file", "", "");
        r.access = Some("write".to_string());
        let parsed = build_rule(r).unwrap();
        assert_eq!(parsed.matcher, Matcher::Any);
        assert_eq!(parsed.access, Some(Access::Write));
    }

    /// A matcher with nothing to compare is a config mistake, not a wildcard:
    /// reading `prefix ""` as "everything" would be the worst possible way for
    /// the operator to discover the typo.
    #[test]
    fn a_matcher_without_a_value_stays_invalid() {
        assert!(build_rule(rule("shell", "prefix", "")).is_none());
        assert!(build_rule(rule("shell", "nonsense", "x")).is_none());
        assert!(build_rule(rule("nonsense", "prefix", "x")).is_none());
    }
}

fn build_rule(r: PolicyRuleFileConfig) -> Option<komo_core::domain::policy::Rule> {
    use komo_core::domain::policy::{Access, Category, Effect, Matcher, Rule};

    // No `match` and no `value` is the wildcard form — "this whole category" —
    // which is what lets a `category = "shell", effect = "deny"` rule take the
    // tool out of the model's catalog entirely. A `match` *without* a value stays
    // invalid: a `prefix` with nothing to compare is a config mistake, and
    // silently reading it as "everything" would be the worst way to find out.
    let wildcard = r.matcher.trim().is_empty() && r.value.is_empty();
    if r.value.is_empty() && !wildcard {
        return None;
    }
    Some(Rule {
        channels: r.channels.filter(|c| !c.is_empty()),
        category: Category::parse(&r.category)?,
        matcher: if wildcard {
            Matcher::Any
        } else {
            Matcher::parse(&r.matcher)?
        },
        value: r.value,
        access: match r.access {
            Some(a) => Some(Access::parse(&a)?),
            None => None,
        },
        effect: Effect::parse(&r.effect)?,
        include_dangerous: r.include_dangerous.unwrap_or(false),
        unattended: r.unattended.unwrap_or(false),
    })
}

#[cfg(test)]
mod tests {
    use super::super::ConfigSnapshot;
    use super::super::sources::{
        ApiFileConfig, ChannelsFileConfig, FileConfig, McpFileConfig, McpServerFileConfig, Secrets,
        TelegramFileConfig,
    };
    use super::*;
    use std::path::PathBuf;

    fn sources() -> ConfigSources {
        ConfigSources {
            home: PathBuf::from("/tmp/komo-test-home"),
            file: FileConfig::default(),
            env: KomoEnv::default(),
            secrets: Secrets::default(),
            env_error: None,
        }
    }

    fn with_deepseek_key(mut s: ConfigSources) -> ConfigSources {
        s.secrets.deepseek_api_key = Some("sk-test".into());
        s
    }

    /// `[plugins.<name>] enabled` is the one uniform kill switch: absent means
    /// enabled, so a plugin roster needs no entry per plugin to run.
    #[test]
    fn plugin_toggles_default_to_enabled_and_honor_an_explicit_false() {
        use crate::sources::PluginFileConfig;

        let mut s = with_deepseek_key(sources());
        s.file.plugins = Some(
            [
                (
                    "wiki".to_string(),
                    PluginFileConfig {
                        enabled: Some(false),
                    },
                ),
                (
                    "mcp".to_string(),
                    PluginFileConfig {
                        enabled: Some(true),
                    },
                ),
                // An entry with no `enabled` says nothing — it must not be
                // mistaken for a disable.
                ("web".to_string(), PluginFileConfig { enabled: None }),
            ]
            .into_iter()
            .collect(),
        );
        let rt = ConfigSnapshot::from_sources(s).runtime;

        assert!(!rt.plugin_enabled("wiki"), "explicit false disables");
        assert!(rt.plugin_enabled("mcp"), "explicit true enables");
        assert!(
            rt.plugin_enabled("web"),
            "an entry without `enabled` is a no-op"
        );
        assert!(
            rt.plugin_enabled("core-tools"),
            "an absent plugin is enabled"
        );
        // Only the explicit toggles are recorded, so the wiring layer's
        // unknown-name warning fires on what the operator actually wrote.
        assert_eq!(rt.plugin_toggles.len(), 2);
    }

    #[test]
    fn defaults_resolve_without_file_or_env() {
        let snap = ConfigSnapshot::from_sources(with_deepseek_key(sources()));
        let rt = &snap.runtime;
        assert_eq!(rt.model.provider, Provider::DeepSeek);
        assert_eq!(rt.model.model, "deepseek-chat");
        assert_eq!(rt.model.max_turns, DEFAULT_MAX_TURNS);
        assert_eq!(rt.maintenance_schedule, DEFAULT_MAINTENANCE_SCHEDULE);
        assert_eq!(rt.briefing_schedule, None, "briefing stays opt-in");
        assert_eq!(
            rt.dream_schedule.as_deref(),
            Some(DEFAULT_DREAM_SCHEDULE),
            "dreaming is on by default"
        );
        assert_eq!(rt.review_interval, DEFAULT_REVIEW_INTERVAL);
        assert_eq!(snap.report.provider_origin, Origin::Default);
        assert_eq!(snap.report.model_origin, Origin::Default);
        assert!(snap.report.fatal().is_none());
        assert!(snap.validate_gateway().is_ok());
    }

    #[test]
    fn readable_roots_are_canonicalized_and_invalid_entries_are_reported() {
        let valid = std::env::temp_dir();
        let missing = valid.join(format!("komo-missing-{}", uuid::Uuid::new_v4()));
        let mut s = with_deepseek_key(sources());
        s.file.readable_roots = Some(vec![valid.clone(), missing]);

        let snap = ConfigSnapshot::from_sources(s);
        assert_eq!(
            snap.runtime.readable_roots,
            vec![valid.canonicalize().unwrap()]
        );
        assert!(snap.report.issues.iter().any(|issue| {
            issue.path == "readable_roots" && issue.severity == IssueSeverity::Warning
        }));
    }

    #[test]
    fn precedence_is_default_then_file_then_env() {
        let mut s = sources();
        s.secrets.openai_api_key = Some("sk-env".into());
        s.file.provider = Some("deepseek".into());
        s.file.model = Some("file-model".into());
        s.file.max_turns = Some(7);
        s.env.provider = Some("openai".into());
        s.env.model = Some("env-model".into());
        let snap = ConfigSnapshot::from_sources(s);
        assert_eq!(snap.runtime.model.provider, Provider::OpenAi);
        assert_eq!(snap.runtime.model.model, "env-model");
        assert_eq!(snap.runtime.model.max_turns, 7, "file wins over default");
        assert_eq!(snap.report.provider_origin, Origin::Env);
        assert_eq!(snap.report.model_origin, Origin::Env);
    }

    #[test]
    fn file_model_reports_file_origin() {
        let mut s = with_deepseek_key(sources());
        s.file.model = Some("deepseek-reasoner".into());
        let snap = ConfigSnapshot::from_sources(s);
        assert_eq!(snap.report.model_origin, Origin::File);
        assert_eq!(snap.report.provider_origin, Origin::Default);
    }

    #[test]
    fn missing_api_key_warns_but_does_not_block_startup() {
        let snap = ConfigSnapshot::from_sources(sources());
        assert_eq!(snap.runtime.model.api_key, "");
        // Degraded, not dead: a fresh install must boot (build_llm degrades to
        // an every-call-errors client), so the issue is a warning.
        assert!(snap.report.fatal().is_none());
        let issue = snap
            .report
            .issues
            .iter()
            .find(|i| i.path == "model.api_key")
            .expect("missing key is reported");
        assert_eq!(issue.severity, IssueSeverity::Warning);
        assert!(issue.message.contains("DEEPSEEK_API_KEY"));
        assert!(snap.validate_gateway().is_ok());
        assert!(snap.validate_agent().is_ok());
    }

    #[test]
    fn codex_needs_no_api_key() {
        let mut s = sources();
        s.file.provider = Some("codex".into());
        let snap = ConfigSnapshot::from_sources(s);
        assert_eq!(snap.runtime.model.provider, Provider::Codex);
        assert_eq!(snap.runtime.model.model, "gpt-5.5");
        assert!(
            snap.report.fatal().is_none(),
            "codex auth is OAuth, not an env key"
        );
        assert!(!snap.report.key_present(Provider::Codex));
    }

    #[test]
    fn invalid_provider_is_fatal_and_falls_back() {
        let mut s = sources();
        s.file.provider = Some("nonsense".into());
        let snap = ConfigSnapshot::from_sources(s);
        let fatal = snap.report.fatal().expect("bad provider is fatal");
        assert_eq!(fatal.path, "model.provider");
        assert_eq!(
            snap.runtime.model.provider,
            Provider::DeepSeek,
            "resolution continues on the default provider"
        );
        assert!(snap.validate_agent().is_err());
    }

    #[test]
    fn env_error_is_fatal_for_startup_not_diagnostics() {
        let mut s = with_deepseek_key(sources());
        s.env_error = Some("invalid KOMO_* environment variable: bad".into());
        let snap = ConfigSnapshot::from_sources(s);
        let fatal = snap.report.fatal().expect("env error is fatal");
        assert_eq!(fatal.path, "env");
        assert!(snap.validate_agent().is_err());
        // Diagnostics still get a fully-resolved snapshot.
        assert_eq!(snap.runtime.model.provider, Provider::DeepSeek);
    }

    #[test]
    fn disabled_channel_missing_secret_is_not_an_issue() {
        let mut s = with_deepseek_key(sources());
        s.file.channels = Some(ChannelsFileConfig {
            telegram: Some(TelegramFileConfig {
                enabled: false,
                ..Default::default()
            }),
            ..Default::default()
        });
        let snap = ConfigSnapshot::from_sources(s);
        assert!(matches!(snap.runtime.telegram, ChannelState::Disabled));
        assert!(snap.report.fatal().is_none());
    }

    #[test]
    fn enabled_channel_missing_secret_is_one_fatal_issue() {
        let mut s = with_deepseek_key(sources());
        s.file.channels = Some(ChannelsFileConfig {
            telegram: Some(TelegramFileConfig {
                enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        let snap = ConfigSnapshot::from_sources(s);
        let ChannelState::Misconfigured(msg) = &snap.runtime.telegram else {
            panic!("enabled without token must be misconfigured");
        };
        assert!(msg.contains("TELEGRAM_BOT_TOKEN"));
        assert_eq!(
            snap.report
                .issues
                .iter()
                .filter(|i| i.path == "channels.telegram")
                .count(),
            1
        );
        // The gateway fails fast; a chat turn doesn't need the channel.
        assert!(snap.validate_gateway().is_err());
        assert!(snap.validate_agent().is_ok());
    }

    #[test]
    fn api_defaults_to_loopback_ephemeral_with_auto_key() {
        let snap = ConfigSnapshot::from_sources(with_deepseek_key(sources()));
        let api = snap.runtime.api.ready().expect("api is always on");
        assert_eq!(api.bind, "127.0.0.1");
        assert_eq!(api.port, 0, "ephemeral port by default");
        assert!(!api.server_key.is_empty(), "auto-generated key");
    }

    #[test]
    fn external_api_requires_a_key() {
        let mut s = with_deepseek_key(sources());
        s.file.channels = Some(ChannelsFileConfig {
            api: Some(ApiFileConfig {
                enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        let snap = ConfigSnapshot::from_sources(s);
        assert!(matches!(snap.runtime.api, ChannelState::Misconfigured(_)));
        assert!(snap.validate_gateway().is_err());

        let mut s = with_deepseek_key(sources());
        s.secrets.api_server_key = Some("k".into());
        s.file.channels = Some(ChannelsFileConfig {
            api: Some(ApiFileConfig {
                enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        let snap = ConfigSnapshot::from_sources(s);
        let api = snap
            .runtime
            .api
            .ready()
            .expect("keyed external api is ready");
        assert_eq!(api.port, 8765, "stable default port when external");
        assert_eq!(api.server_key, "k");
    }

    #[test]
    fn report_never_contains_secret_values() {
        let mut s = sources();
        s.secrets.deepseek_api_key = Some("sk-super-secret-value".into());
        s.secrets.telegram_bot_token = Some("123:telegram-secret".into());
        s.file.channels = Some(ChannelsFileConfig {
            telegram: Some(TelegramFileConfig {
                enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        let snap = ConfigSnapshot::from_sources(s);
        let dump = format!("{:?}", snap.report);
        assert!(!dump.contains("sk-super-secret-value"));
        assert!(!dump.contains("telegram-secret"));
        assert!(snap.report.key_present(Provider::DeepSeek));
    }

    #[test]
    fn dream_schedule_defaults_on_and_can_be_disabled() {
        // Unset → on by default at the nightly slot.
        assert_eq!(
            resolve_dream_schedule(None).as_deref(),
            Some(DEFAULT_DREAM_SCHEDULE)
        );
        // A custom cron is taken verbatim.
        assert_eq!(
            resolve_dream_schedule(Some("0 4 * * *".into())).as_deref(),
            Some("0 4 * * *")
        );
        // Empty or off-like values disable it.
        for off in ["", "  ", "off", "OFF", "none", "disabled"] {
            assert_eq!(
                resolve_dream_schedule(Some(off.into())),
                None,
                "`{off}` should disable dreaming"
            );
        }
    }

    #[test]
    fn enabled_switches_override_a_configured_schedule() {
        let mut s = with_deepseek_key(sources());
        s.file.briefing_schedule = Some("30 8 * * *".into());
        s.file.dream_schedule = Some("0 3 * * *".into());
        s.env.briefing_schedule_enabled = Some(false);
        s.env.dream_schedule_enabled = Some(false);

        let rt = ConfigSnapshot::from_sources(s).runtime;
        assert_eq!(rt.briefing_schedule, None);
        assert_eq!(rt.dream_schedule, None);
    }

    #[test]
    fn enabled_switches_default_on_and_env_beats_file() {
        let mut s = with_deepseek_key(sources());
        s.file.briefing_schedule = Some("30 8 * * *".into());
        s.file.briefing_schedule_enabled = Some(false);
        s.file.dream_schedule_enabled = Some(false);
        s.env.briefing_schedule_enabled = Some(true);

        let rt = ConfigSnapshot::from_sources(s).runtime;
        assert_eq!(rt.briefing_schedule.as_deref(), Some("30 8 * * *"));
        assert_eq!(
            rt.dream_schedule, None,
            "file switch still disables dreaming"
        );
    }

    #[test]
    fn skills_path_splits_on_colons() {
        let mut s = with_deepseek_key(sources());
        s.env.skills_path = Some("/a/skills:/b/skills:".into());
        let snap = ConfigSnapshot::from_sources(s);
        assert_eq!(
            snap.runtime.skills_path,
            vec![PathBuf::from("/a/skills"), PathBuf::from("/b/skills")]
        );
    }

    #[test]
    fn db_urls_derive_from_home() {
        let snap = ConfigSnapshot::from_sources(with_deepseek_key(sources()));
        assert_eq!(snap.runtime.db_url, "turso:/tmp/komo-test-home/state.db");
        assert_eq!(
            snap.runtime.kanban_db_url,
            "turso:/tmp/komo-test-home/kanban.db"
        );
        assert_eq!(
            snap.runtime.memory_db_url,
            "turso:/tmp/komo-test-home/memory.db"
        );
        assert_eq!(
            snap.runtime.cron_db_url,
            "turso:/tmp/komo-test-home/cron.db"
        );
    }

    #[test]
    fn model_menu_defaults_to_the_model_plus_aux() {
        assert_eq!(
            resolve_model_menu("deepseek-chat", Some("deepseek-chat-lite"), None, None),
            vec!["deepseek-chat", "deepseek-chat-lite"]
        );
        // No aux model configured: the menu is just the one model.
        assert_eq!(
            resolve_model_menu("deepseek-chat", None, None, None),
            vec!["deepseek-chat"]
        );
        // An aux model equal to the main one must not appear twice.
        assert_eq!(
            resolve_model_menu("deepseek-chat", Some("deepseek-chat"), None, None),
            vec!["deepseek-chat"]
        );
    }

    #[test]
    fn model_menu_always_offers_the_running_model_first() {
        // A declared menu that forgot the configured model would otherwise leave
        // the model the gateway is actually running unselectable.
        let file = vec!["b".to_string(), "c".to_string()];
        assert_eq!(
            resolve_model_menu("a", None, None, Some(&file)),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn env_model_menu_wins_over_the_file_and_tolerates_sloppy_csv() {
        let file = vec!["ignored".to_string()];
        assert_eq!(
            resolve_model_menu("a", Some("aux"), Some(" b , a ,, c "), Some(&file)),
            vec!["a", "b", "c"],
            "env wins; blanks and the duplicate default are dropped, aux is not appended"
        );
    }

    /// A codex-default config whose menu also names a deepseek model.
    fn cross_provider_config(with_deepseek_key: bool) -> ModelConfig {
        let mut keys = HashMap::new();
        if with_deepseek_key {
            keys.insert(Provider::DeepSeek, "sk-ds".to_string());
        }
        ModelConfig {
            provider: Provider::Codex,
            model: "gpt-5.6-terra".into(),
            models: vec![
                "gpt-5.6-terra".into(),
                "deepseek:deepseek-chat".into(),
                "gpt-5.4-mini".into(),
            ],
            keys,
            api_key: String::new(),
            base_url: Some("https://proxy.example".into()),
            aux_model: None,
            max_turns: DEFAULT_MAX_TURNS,
            max_tool_result_bytes: DEFAULT_MAX_TOOL_RESULT_BYTES,
            max_turn_result_bytes: DEFAULT_MAX_TURN_RESULT_BYTES,
            tool_timeout_secs: DEFAULT_TOOL_TIMEOUT_SECS,
            max_history_messages: DEFAULT_MAX_HISTORY_MESSAGES,
            max_history_bytes: DEFAULT_MAX_HISTORY_BYTES,
            llm_timeout_secs: DEFAULT_LLM_TIMEOUT_SECS,
        }
    }

    #[test]
    fn menu_resolves_qualified_ids_to_their_own_provider_and_efforts() {
        let menu = cross_provider_config(true).menu();
        let ids: Vec<_> = menu.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(
            ids,
            ["gpt-5.6-terra", "deepseek:deepseek-chat", "gpt-5.4-mini"]
        );

        let deepseek = &menu[1];
        assert_eq!(deepseek.provider, Provider::DeepSeek);
        assert_eq!(deepseek.model, "deepseek-chat", "the prefix is stripped");
        assert!(
            deepseek.efforts.is_empty(),
            "deepseek exposes no effort scale, unlike the codex entries"
        );
        // An unqualified entry inherits the configured provider.
        assert_eq!(menu[2].provider, Provider::Codex);
        assert_eq!(menu[0].efforts, ["low", "medium", "high"]);
    }

    #[test]
    fn menu_drops_models_whose_provider_has_no_credential() {
        // Offering one would mean a model that errors on every single turn.
        let menu = cross_provider_config(false).menu();
        let ids: Vec<_> = menu.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["gpt-5.6-terra", "gpt-5.4-mini"]);
    }

    #[test]
    fn the_running_model_survives_even_without_its_credential() {
        // Hiding it would misreport what the gateway is actually running; the
        // missing key is already a startup warning.
        let mut config = cross_provider_config(false);
        config.provider = Provider::OpenAi;
        config.model = "gpt-4.1".into();
        config.models = vec!["gpt-4.1".into(), "deepseek:deepseek-chat".into()];
        let ids: Vec<_> = config.menu().iter().map(|e| e.id.clone()).collect();
        assert_eq!(ids, ["gpt-4.1"]);
    }

    #[test]
    fn menu_providers_covers_every_backend_plus_the_default() {
        let config = cross_provider_config(true);
        assert_eq!(
            config.menu_providers(),
            vec![Provider::Codex, Provider::DeepSeek]
        );
        // With no credential the deepseek entry is gone, so no client is built.
        assert_eq!(
            cross_provider_config(false).menu_providers(),
            vec![Provider::Codex]
        );
    }

    #[test]
    fn for_provider_carries_base_url_only_to_the_default_provider() {
        let config = cross_provider_config(true);
        // base_url overrides one specific endpoint; applying it to deepseek would
        // silently point it at an unrelated OpenAI-compatible proxy.
        let other = config.for_provider(Provider::DeepSeek, "deepseek-chat".into());
        assert_eq!(other.base_url, None);
        assert_eq!(other.api_key, "sk-ds", "each backend gets its own key");

        let same = config.for_provider(Provider::Codex, "gpt-5.6-terra".into());
        assert_eq!(same.base_url.as_deref(), Some("https://proxy.example"));
    }

    #[test]
    fn debug_output_masks_api_key() {
        let cfg = ModelConfig {
            provider: Provider::DeepSeek,
            model: "deepseek-chat".into(),
            models: vec!["deepseek-chat".into()],
            keys: Default::default(),
            api_key: "sk-abcdefghijklmnopqr".into(),
            base_url: None,
            aux_model: None,
            max_turns: DEFAULT_MAX_TURNS,
            max_tool_result_bytes: DEFAULT_MAX_TOOL_RESULT_BYTES,
            max_turn_result_bytes: DEFAULT_MAX_TURN_RESULT_BYTES,
            tool_timeout_secs: DEFAULT_TOOL_TIMEOUT_SECS,
            max_history_messages: DEFAULT_MAX_HISTORY_MESSAGES,
            max_history_bytes: DEFAULT_MAX_HISTORY_BYTES,
            llm_timeout_secs: DEFAULT_LLM_TIMEOUT_SECS,
        };
        let s = format!("{cfg:?}");
        assert!(
            !s.contains("sk-abcdefghijklmnopqr"),
            "full key must not appear in Debug output"
        );
        assert!(s.contains("sk-"), "prefix should be visible");
    }

    /// One `[mcp.servers.<name>]` table plus the resolved snapshot it produces.
    ///
    /// Deliberately never sets an env var: `resolve_mcp_servers` reads the
    /// process environment, which is shared by every test running in parallel.
    fn with_mcp(name: &str, server: McpServerFileConfig) -> ConfigSnapshot {
        let mut s = with_deepseek_key(sources());
        s.file.mcp = Some(McpFileConfig {
            servers: [(name.to_string(), server)].into_iter().collect(),
        });
        ConfigSnapshot::from_sources(s)
    }

    fn mcp_issues(snap: &ConfigSnapshot) -> Vec<&str> {
        snap.report
            .issues
            .iter()
            .filter(|i| i.path == "mcp.servers")
            .map(|i| i.message.as_str())
            .collect()
    }

    #[test]
    fn mcp_server_with_an_explicit_tool_list_resolves() {
        let snap = with_mcp(
            "memos",
            McpServerFileConfig {
                url: "https://memos.example.com/mcp".into(),
                tools: vec!["create_memo".into(), "list_memos".into()],
                ..Default::default()
            },
        );
        assert_eq!(snap.runtime.mcp_servers.len(), 1);
        let server = &snap.runtime.mcp_servers[0];
        assert_eq!(server.name, "memos");
        assert_eq!(server.tools, ["create_memo", "list_memos"]);
        assert!(server.token.is_none());
        assert!(mcp_issues(&snap).is_empty());
    }

    #[test]
    fn mcp_server_without_a_tool_list_mounts_nothing() {
        // Closed by default, like the HA channel's event filters: a server can
        // advertise dozens of tools and each one costs a schema every round.
        let snap = with_mcp(
            "memos",
            McpServerFileConfig {
                url: "https://memos.example.com/mcp".into(),
                ..Default::default()
            },
        );
        assert!(snap.runtime.mcp_servers.is_empty());
        let issues = mcp_issues(&snap);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("all_tools"), "{}", issues[0]);
        // A warning, never fatal — an optional integration must not block boot.
        assert!(snap.validate_gateway().is_ok());
        assert!(snap.validate_agent().is_ok());
    }

    #[test]
    fn mcp_all_tools_opts_out_of_the_allowlist() {
        let snap = with_mcp(
            "memos",
            McpServerFileConfig {
                url: "https://memos.example.com/mcp".into(),
                all_tools: true,
                ..Default::default()
            },
        );
        assert_eq!(snap.runtime.mcp_servers.len(), 1);
        assert!(
            snap.runtime.mcp_servers[0].tools.is_empty(),
            "an empty allowlist is how wiring spells `mount everything`"
        );
        assert!(mcp_issues(&snap).is_empty());
    }

    #[test]
    fn mcp_server_naming_an_unset_token_var_is_skipped_with_a_warning() {
        // Naming token_env states the server needs auth; connecting anyway
        // would turn a clear config warning into a 401 at call time.
        let snap = with_mcp(
            "memos",
            McpServerFileConfig {
                url: "https://memos.example.com/mcp".into(),
                token_env: Some("KOMO_TEST_DEFINITELY_UNSET_TOKEN".into()),
                tools: vec!["create_memo".into()],
                ..Default::default()
            },
        );
        assert!(snap.runtime.mcp_servers.is_empty());
        let issues = mcp_issues(&snap);
        assert_eq!(issues.len(), 1);
        assert!(
            issues[0].contains("KOMO_TEST_DEFINITELY_UNSET_TOKEN"),
            "{}",
            issues[0]
        );
        assert!(snap.validate_gateway().is_ok());
    }

    #[test]
    fn disabled_mcp_server_is_skipped_silently() {
        let snap = with_mcp(
            "memos",
            McpServerFileConfig {
                enabled: Some(false),
                url: "https://memos.example.com/mcp".into(),
                tools: vec!["create_memo".into()],
                ..Default::default()
            },
        );
        assert!(snap.runtime.mcp_servers.is_empty());
        assert!(mcp_issues(&snap).is_empty());
    }

    #[test]
    fn no_mcp_table_means_no_servers_and_no_issues() {
        let snap = ConfigSnapshot::from_sources(with_deepseek_key(sources()));
        assert!(snap.runtime.mcp_servers.is_empty());
        assert!(mcp_issues(&snap).is_empty());
    }
}
