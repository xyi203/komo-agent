//! HTTP API ingress channel.
//!
//! Exposes the agent over a loopback HTTP port for local UIs (the Tauri
//! dashboard) and any OpenAI-compatible client (Open WebUI, LobeChat, …). Two
//! families of endpoints:
//!
//!   - **OpenAI-compatible** (`/v1/*`): `chat/completions` (streaming and not)
//!     and `models`, so third-party chat frontends connect by pointing at
//!     `http://127.0.0.1:8765/v1` with the bearer key.
//!   - **dashboard** (`/api/*`): read views over the same repositories the
//!     `komo` CLI uses — sessions, tasks, memories, runs, plus a `status`
//!     aggregate. These back the desktop control panel (roadmap §9).
//!
//! Unlike the chat channels, an HTTP request is synchronous request/response,
//! so it calls the [`MessageHandler`] directly and awaits the reply rather than
//! going through the spawn-and-return [`GatewayDispatcher`]. The turn runs in a
//! **non-interactive** session context ([`SessionContext::detached`]), so a tool
//! that needs approval is denied immediately — there is no human on an HTTP
//! request to answer a `/approve` prompt.
//!
//! Auth is a single bearer key (`API_SERVER_KEY`); the listener binds loopback
//! by default. `/health` is unauthenticated so a probe can check liveness.
//!
//! A **CORS layer** ([`cors_layer`]) allows browser clients whose page origin
//! isn't this port: the Electron renderer (a Vite dev origin in development,
//! `file://` — origin `null` — when packaged) and a cross-origin browser dev
//! server. Without it those fetches are blocked before auth ever runs, since a
//! request carrying `Authorization` is preflighted and the preflight would hit
//! the bearer-key middleware. Only loopback origins are allowed and credentials
//! are off, so the bearer key remains the sole thing that grants access.

use komo_agent::daemon::DreamSweep;
use komo_agent::gateway::Channel;
use komo_agent::interaction::{Answer, ApprovalState, CancelState, GatewayDispatcher};
use komo_services::clarify::ClarifyState;
use komo_services::tool_execution::{SessionContext, with_session};
use std::path::PathBuf;
use std::sync::Arc;
use std::{convert::Infallible, time::Duration};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{ConnectInfo, Path, Query, Request, State},
    http::{
        HeaderName, HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use base64::Engine as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    services::{ServeDir, ServeFile},
};
use tracing::{info, warn};

use crate::{
    domain::{
        cancel::{CANCELLED_REPLY, is_cancelled},
        cron::CronJobSpec,
        events::{ToolEventSink, TurnEvent},
        gateway::MessageHandler,
        memory::{MemoryStatus, parse_memory_status},
        pairing::ApproveOutcome,
        session::{DEFAULT_WORKSPACE, Session},
    },
    services::operator_control::{
        MemoryTransitionAction, ResumeOutcome,
        actions::{
            OperatorActions, TransitionOutcome, no_cron_job_message, not_recoverable_message,
            resolve_resume,
        },
    },
};
use komo_config::{ApiConfig, ModelEntry};
use std::net::SocketAddr;

/// What the HTTP transport itself needs, cheaply cloned per request (all
/// `Arc`): the bearer key, the chat handler, the operator use cases, and the
/// two `/api/status` facts. Operator behavior lives in [`OperatorActions`] —
/// this is transport state, not a dependency list.
#[derive(Clone)]
struct AppState {
    api_key: Arc<String>,
    /// The main runtime's tool catalog — read per request, so `/health` reports
    /// what is mounted *now* (python plugins mount and unmount live).
    tools: Arc<komo_core::domain::catalog::ToolCatalog>,
    handler: Arc<dyn MessageHandler>,
    actions: Arc<OperatorActions>,
    /// Channel names enabled on this gateway (for `/api/status`).
    channels: Arc<Vec<String>>,
    /// Resolved config `home_chat` fallback, if any (for `/api/status`).
    home: Option<String>,
    /// Resolved main model identity (safe, non-secret status metadata).
    provider: Arc<String>,
    model: Arc<String>,
    /// The models a client may switch a session to, each carrying its own
    /// provider and reasoning-effort levels — with a cross-provider menu those
    /// differ per entry (codex has three levels, deepseek none). Advertised over
    /// `GET /api/models`; the chat path validates a request against them so an
    /// unknown id falls back to `model` instead of reaching a provider.
    models: Arc<Vec<ModelEntry>>,
    /// Shared with the gateway dispatcher and the `ChatApprover`: lets a
    /// loopback interactive HTTP turn (the GUI) surface a pending approval over
    /// `GET /api/interactions/{session}` and resolve it over `POST`.
    approvals: Arc<ApprovalState>,
    /// Shared with the `ask_user` tool: same, for mid-turn clarify questions.
    clarify: Arc<ClarifyState>,
    /// Cancellation slots for in-flight interruptible turns, keyed by session.
    /// Owned here (not shared with the gateway dispatcher) because the api
    /// channel is the only surface with a stop affordance.
    cancels: Arc<CancelState>,
    /// Allow keyed remote (non-loopback) callers to run interactive turns and
    /// resolve approval/clarify prompts. Off by default — those paths assume a
    /// host operator behind a loopback socket. `X-Komo-Trusted` (auto-approve)
    /// stays loopback-only regardless of this flag.
    remote_interactive: bool,
    /// Server-owned workspace catalog exposed to the authenticated local GUI.
    workspace_home: Arc<PathBuf>,
    /// Process workspace used when the UI selects the default entry.
    default_workspace: Arc<PathBuf>,
}

/// The model identity plus the switchable menu the api advertises.
///
/// Derived once from the resolved [`ModelConfig`] so `/api/status`,
/// `/api/models`, and the chat path's validation can never disagree about what
/// is on offer. Everything here is non-secret status metadata.
pub struct ModelMenu {
    /// The default provider's name (status metadata; each entry names its own).
    provider: String,
    default_model: String,
    /// Selectable models, the configured default first (always non-empty). Each
    /// entry carries its provider and that provider's effort levels.
    models: Vec<ModelEntry>,
}

impl ModelMenu {
    pub fn from_config(config: &komo_config::ModelConfig) -> Self {
        Self {
            provider: config.provider.name().to_string(),
            default_model: config.model.clone(),
            // `menu()` drops entries whose provider has no usable credential, so
            // the UI never offers a model that would error on every turn.
            models: config.menu(),
        }
    }
}

/// The HTTP API channel. Holds the listen config and the shared handler state.
pub struct ApiChannel {
    bind: String,
    port: u16,
    /// Optional built web SPA served same-origin (static public, api key-gated).
    web_dir: Option<String>,
    state: AppState,
}

impl ApiChannel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &ApiConfig,
        handler: Arc<dyn MessageHandler>,
        actions: Arc<OperatorActions>,
        tools: Arc<komo_core::domain::catalog::ToolCatalog>,
        channels: Vec<String>,
        home: Option<String>,
        models: ModelMenu,
        approvals: Arc<ApprovalState>,
        clarify: Arc<ClarifyState>,
        workspace_home: PathBuf,
        default_workspace: PathBuf,
    ) -> Self {
        Self {
            bind: config.bind.clone(),
            port: config.port,
            web_dir: config.web_dir.clone(),
            state: AppState {
                api_key: Arc::new(config.server_key.clone()),
                handler,
                actions,
                tools,
                channels: Arc::new(channels),
                home,
                provider: Arc::new(models.provider),
                model: Arc::new(models.default_model),
                models: Arc::new(models.models),
                approvals,
                clarify,
                cancels: Arc::new(CancelState::new()),
                remote_interactive: config.remote_interactive,
                workspace_home: Arc::new(workspace_home),
                default_workspace: Arc::new(default_workspace),
            },
        }
    }
}

#[async_trait]
impl Channel for ApiChannel {
    fn name(&self) -> &str {
        "api"
    }

    async fn serve(
        &self,
        _dispatcher: Arc<GatewayDispatcher>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.bind, self.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        // With an ephemeral bind (port 0, the loopback-only default) the real
        // port is only known after bind — read it back and advertise it.
        let local = listener.local_addr()?;
        info!(addr = %local, "api channel listening");
        // Publish how to reach this gateway so the local `komo` CLI can route
        // to it instead of opening the db (which Turso's exclusive lock forbids
        // while the gateway holds it). Removed again on graceful shutdown.
        crate::infra::rendezvous::write(&crate::infra::rendezvous::GatewayInfo {
            pid: std::process::id(),
            bind: self.bind.clone(),
            port: local.port(),
            key: self.state.api_key.as_ref().clone(),
        });
        let app = build_router(self.state.clone(), self.web_dir.as_deref());
        let graceful = async move {
            let _ = shutdown.changed().await;
        };
        // `into_make_service_with_connect_info` so handlers can see the peer
        // address — the trusted-chat path is gated to loopback callers.
        let result = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(graceful)
        .await;
        crate::infra::rendezvous::clear();
        result?;
        info!("api channel stopped");
        Ok(())
    }
}

/// Is this `Origin` a local page? Loopback hosts on any port, plus the opaque
/// `null` origin a packaged Electron renderer sends from `file://`.
fn is_local_origin(origin: &HeaderValue) -> bool {
    let Ok(text) = origin.to_str() else {
        return false;
    };
    // Packaged Electron loads the renderer over file://, whose origin is opaque.
    if text == "null" {
        return true;
    }
    let Some(host_port) = text
        .strip_prefix("http://")
        .or_else(|| text.strip_prefix("https://"))
    else {
        return false;
    };
    // Bracketed IPv6 (`[::1]:5273`) keeps its colons inside the brackets.
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        match rest.split_once(']') {
            Some((host, _)) => host,
            None => return false,
        }
    } else {
        host_port.split(':').next().unwrap_or_default()
    };
    if host == "localhost" {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

/// CORS for local browser clients. Applied as the **outermost** layer so a
/// preflight is answered here and never reaches [`require_auth`] (which would
/// 401 it, blocking the real request that follows).
///
/// Deliberately narrow: loopback/`null` origins only, no credentials (the key
/// travels in a header, not a cookie), and the small fixed set of headers this
/// API reads. A page on the open web gets no CORS grant — and even a local one
/// still needs the bearer key.
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(|origin, _parts| {
            is_local_origin(origin)
        }))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            AUTHORIZATION,
            CONTENT_TYPE,
            HeaderName::from_static("x-komo-session-id"),
            HeaderName::from_static("x-komo-trusted"),
            HeaderName::from_static("x-komo-interactive"),
            HeaderName::from_static("x-komo-workspace"),
            HeaderName::from_static("x-komo-model"),
            HeaderName::from_static("x-komo-effort"),
        ])
        .max_age(std::time::Duration::from_secs(600))
}

/// Build the router: `/health` and (if configured) the static web SPA are
/// public; everything else sits behind the bearer-key middleware.
fn build_router(state: AppState, web_dir: Option<&str>) -> Router {
    // Control-plane writes: host-operator actions (memory governance, run
    // prune, session clean, pairing admission, dream apply). Loopback-gated as
    // a *layer*, not per-handler checks, so a write route added here is gated
    // by construction — a publicly-bound api (`[channels.api] enabled = true`)
    // never reaches these, valid key or not.
    let operator_writes = Router::new()
        .route("/api/memories/{id}/promote", post(memory_promote))
        .route("/api/memories/{id}/reject", post(memory_reject))
        .route("/api/memories/{id}/pin", post(memory_pin))
        .route("/api/runs/prune", post(prune_runs))
        .route("/api/sessions/clean", post(clean_sessions))
        .route("/api/sessions/{id}/title", post(set_session_title))
        .route("/api/sessions/{id}/status", post(set_session_status))
        .route("/api/sessions/{id}/delete", post(delete_session))
        .route("/api/pairings/approve", post(pair_approve))
        .route("/api/pairings/{id}/revoke", post(pair_revoke))
        .route("/api/dream/apply", post(dream_apply))
        .route("/api/memories/repair-scopes", post(memory_repair_scopes))
        .route("/api/memories/backfill", post(memory_backfill))
        .route("/api/memories/search", post(memory_search))
        .route("/api/memories/used", post(memory_used))
        .route("/api/wiki/search", post(wiki_search))
        .route("/api/wiki/status", get(wiki_status))
        .route("/api/wiki/index", post(wiki_index))
        .route("/api/cron/add", post(cron_add))
        .route("/api/cron/{name}/remove", post(cron_remove))
        .route("/api/cron/{name}/enable", post(cron_enable))
        .route("/api/cron/{name}/disable", post(cron_disable))
        .route("/api/cron/{name}/trigger", post(cron_trigger))
        .route_layer(middleware::from_fn(require_loopback));

    // Interactive resolution (the GUI's approval modal + clarify answer). Always
    // allowed over loopback; reachable by keyed remote callers only when
    // `remote_interactive` is set (a remote GUI resolving its own prompts). The
    // `require_auth` layer below still applies via the merge into `protected`.
    let interactive_writes = Router::new()
        .route(
            "/api/interactions/{session}/approval",
            post(resolve_approval),
        )
        .route("/api/interactions/{session}/answer", post(answer_question))
        .route("/api/interactions/{session}/cancel", post(cancel_turn))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_interactive_access,
        ));

    let protected = Router::new()
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/api/status", get(status))
        .route("/api/models", get(list_model_menu))
        .route("/api/workspaces", get(list_workspaces))
        .route("/api/home", get(get_home))
        .route("/api/sessions", get(list_sessions))
        .route("/api/sessions/{id}/messages", get(session_messages))
        .route("/api/tasks", get(list_tasks))
        .route("/api/memories", get(list_memories))
        .route("/api/runs", get(list_runs))
        .route("/api/runs/{id}", get(get_run))
        .route("/api/runs/{id}/resume", post(resume_run))
        .route("/api/reminders", get(list_reminders))
        .route("/api/cron", get(list_cron_jobs))
        .route("/api/skills", get(list_skills))
        .route("/api/skills/{name}/audit", get(skill_audit))
        .route("/api/skills/usage", get(skill_usage))
        .route("/api/pairings", get(list_pairings))
        .route("/api/dream", get(dream_preview))
        .route("/api/interactions/{session}", get(get_interactions))
        .merge(operator_writes)
        .merge(interactive_writes)
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    let mut router = Router::new().route("/health", get(health)).merge(protected);

    // Serve the built web SPA same-origin as the unauthenticated fallback: the
    // bundle isn't secret (the key it then uses is), and same-origin means no
    // CORS. Unknown non-API paths fall back to index.html for the SPA. `/api`
    // and `/v1` are matched routes above, so they never reach this fallback.
    if let Some(dir) = web_dir {
        let index = std::path::Path::new(dir).join("index.html");
        router = router.fallback_service(ServeDir::new(dir).fallback(ServeFile::new(index)));
    }

    // Outermost: preflights are answered before auth/loopback gating runs.
    router.layer(cors_layer()).with_state(state)
}

/// Gate the interactive-resolution endpoints: always allow loopback (the local
/// GUI / CLI); allow keyed remote callers only when `remote_interactive` is on.
/// Auth is enforced separately by the `require_auth` layer.
async fn require_interactive_access(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    if peer.ip().is_loopback() || state.remote_interactive {
        return next.run(req).await;
    }
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "interactive endpoints are loopback-only \
                      (set [channels.api] remote_interactive = true to allow keyed remote access)"
        })),
    )
        .into_response()
}

/// Reject any control-plane write not arriving over loopback. These are
/// host-operator actions — like the trusted chat path, they must be unreachable
/// on an external bind regardless of the bearer key.
async fn require_loopback(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    if !peer.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "operator write endpoints are loopback-only" })),
        )
            .into_response();
    }
    next.run(req).await
}

/// Reject any request whose `Authorization: Bearer <key>` does not match.
async fn require_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(token) if bearer_matches(token, state.api_key.as_str()) => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

/// Constant-time bearer-token check. Both sides are SHA-256'd to a fixed-size
/// digest (so neither length nor content leaks) and compared with the shared
/// constant-time primitive (`domain::pairing::ct_eq`) — a plain `==` on the
/// tokens would let a timing side-channel probe the key byte by byte when the
/// api channel is bound externally (`[channels.api] enabled = true`), where
/// the key is the only auth.
fn bearer_matches(presented: &str, expected: &str) -> bool {
    use sha2::{Digest, Sha256};
    let digest_hex = |s: &str| -> String {
        Sha256::digest(s.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    crate::domain::pairing::ct_eq(&digest_hex(presented), &digest_hex(expected))
}

/// Maps any handler error to a 500 with a JSON body.
struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // `{:#}` renders the whole context chain. The outermost line alone is
        // the *least* specific thing known about the failure — "qdrant is not
        // reachable" hides the "no route to host" underneath that says which
        // kind of unreachable it was.
        let message = format!("{:#}", self.0);
        warn!(error = %message, "api request failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": message })),
        )
            .into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

// ---- OpenAI-compatible endpoints -------------------------------------------

#[derive(Deserialize)]
struct ChatCompletionRequest {
    #[serde(default)]
    model: String,
    #[serde(default)]
    messages: Vec<ChatMessage>,
    #[serde(default)]
    stream: bool,
}

#[derive(Deserialize)]
struct ChatMessage {
    #[serde(default)]
    role: String,
    #[serde(default)]
    content: String,
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    // Plugin state rides along so `komo doctor` can tell "not enabled" from
    // "enabled but the running gateway predates the plugins directory" — the
    // second is invisible from the filesystem alone. Counts only, no names:
    // /health answers unauthenticated.
    let snapshot = state.tools.snapshot();
    let run_code = snapshot.get("run_code").is_some();
    let plugin_tools = snapshot.names().filter(|n| n.starts_with("py__")).count();
    Json(json!({
        "status": "ok",
        "version": crate::cli::VERSION,
        "plugins": { "run_code": run_code, "tools": plugin_tools },
    }))
}

async fn list_models() -> impl IntoResponse {
    Json(json!({
        "object": "list",
        "data": [{
            "id": "komo",
            "object": "model",
            "created": 0,
            "owned_by": "komo",
        }],
    }))
}

async fn chat_completions(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: axum::http::HeaderMap,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, ApiError> {
    let (session_id, stateful) = resolve_session(&headers);
    let input = build_input(&req.messages, stateful);
    let model = if req.model.is_empty() {
        "komo".to_string()
    } else {
        req.model.clone()
    };

    let is_loopback = peer.ip().is_loopback();
    let requested_workspace = requested_workspace(&state, &headers, is_loopback);
    // A session's workspace is a creation-time choice. Once a row exists, its
    // stored id wins over every later header so an old conversation can never
    // silently run tools in a different directory.
    let workspace = bind_session_workspace(&state, &session_id, &requested_workspace).await?;

    // The model / effort choice, unlike the workspace, is *not* creation-locked:
    // a conversation may switch models mid-thread. The client sends its current
    // selection on every turn and we persist it, so the choice travels with the
    // session — the turn itself reads it back off the session row (see
    // `infra::llm::RigLlm::agent_for`), which is also what makes it visible to
    // another client opening the same conversation.
    if let Some(selection) = requested_model(&state.models, &state.model, &headers) {
        state
            .actions
            .sessions
            .set_model(&session_id, &selection.model, &selection.effort)
            .await?;
    }

    // Loopback callers may opt into one of two richer contexts (both ignored on
    // an external bind, where there is no host operator behind the socket):
    //   - `X-Komo-Trusted`: auto-approve side-effecting tools (the CLI user is
    //     the host operator — this is what `komo chat` uses).
    //   - `X-Komo-Interactive`: prompt for approval / clarify and suspend the
    //     turn, exactly like a chat channel, but resolved out-of-band over the
    //     `/api/interactions/*` endpoints (this is what the GUI uses). The reply
    //     sink is a no-op — the GUI reads the pending prompt by polling.
    // Trusted wins over interactive if a caller somehow sets both; anyone else
    // gets the detached (auto-deny) context.
    // Trusted (auto-approve) is loopback-only; interactive may also be granted to
    // keyed remote callers when `remote_interactive` is configured (they resolve
    // approvals/clarify out-of-band, same as the local GUI).
    let trusted = is_loopback && headers.contains_key("x-komo-trusted");
    let interactive =
        (is_loopback || state.remote_interactive) && headers.contains_key("x-komo-interactive");
    let mut ctx = if trusted {
        SessionContext::trusted(&session_id)
    } else if interactive {
        SessionContext::interactive_http(&session_id)
    } else {
        SessionContext::detached(&session_id)
    };
    // Workspace selection is local-operator-only, like trusted mode: the client
    // sends an opaque id, resolved either through the gateway's own catalog or —
    // for a folder the desktop shell picked via the native dialog — decoded from
    // it. A remote caller can therefore never widen its filesystem root.
    if is_loopback {
        if let Some(root) = resolve_workspace_id(&state, &workspace) {
            ctx = ctx.with_workspace(root);
        }
    }
    // Every api turn is interruptible: the caller holds the connection, so it is
    // the one surface with somewhere to put a stop button. The slot is dropped
    // when the turn ends (below / in `stream_turn`) so a late cancel can't hit
    // the next turn's signal.
    let ctx = ctx.with_cancel(state.cancels.register(&session_id));

    let id = format!("chatcmpl-{}", uuid::Uuid::now_v7());
    let created = now();

    if req.stream {
        // Live path: run the turn on a spawned task and stream tool-call events
        // (started/finished) as they happen, then the final reply + [DONE]. The
        // reply itself isn't token-incremental (rig's tool loop has no token
        // stream) — this streams the *tool-call process*, which is the point.
        Ok(stream_turn(
            state.handler.clone(),
            state.cancels.clone(),
            ctx,
            session_id,
            input,
            id,
            created,
            model,
        ))
    } else {
        // Synchronous: drive the turn and await the full reply.
        let reply = with_session(ctx, state.handler.handle(&session_id, input)).await;
        state.cancels.finish(&session_id);
        let reply = match reply {
            Err(error) if is_cancelled(&error) => CANCELLED_REPLY.to_string(),
            other => other?,
        };
        Ok(Json(json!({
            "id": id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": reply },
                "finish_reason": "stop",
            }],
        }))
        .into_response())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkspaceEntry {
    id: String,
    name: String,
    path: PathBuf,
}

fn workspace_entries(state: &AppState) -> Vec<WorkspaceEntry> {
    let mut entries = vec![WorkspaceEntry {
        id: "__default__".to_string(),
        name: state
            .default_workspace
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("默认 workspace")
            .to_string(),
        path: state.default_workspace.as_ref().clone(),
    }];
    if let Ok(children) = std::fs::read_dir(state.workspace_home.as_ref()) {
        for child in children.flatten() {
            let path = child.path();
            if !path.is_dir() {
                continue;
            }
            let Some(id) = child.file_name().to_str().map(str::to_string) else {
                continue;
            };
            // Header values are deliberately conservative. The path itself is
            // always discovered server-side and is never copied from the id.
            if id.is_empty()
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            {
                continue;
            }
            entries.push(WorkspaceEntry {
                name: id.clone(),
                id,
                path,
            });
        }
    }
    entries[1..].sort_by(|a, b| a.name.cmp(&b.name));
    entries
}

fn resolve_workspace_id(state: &AppState, id: &str) -> Option<PathBuf> {
    let catalogued = workspace_entries(state)
        .into_iter()
        .find_map(|entry| (entry.id == id).then_some(entry.path));
    if catalogued.is_some() {
        return catalogued;
    }
    // Not in the catalog: the desktop shell may have attached a folder the
    // operator chose through the native OS dialog. Only the loopback branch
    // above calls this, so widening the root stays a host-operator act — the
    // same boundary that grants `X-Komo-Trusted`.
    resolve_folder_workspace(id)
}

/// A validated per-session model selection. Either field may be empty, meaning
/// "back to the gateway/provider default".
struct ModelSelection {
    model: String,
    effort: String,
}

/// Read `X-Komo-Model` / `X-Komo-Effort` off a chat request, validated against
/// what this gateway actually advertises.
///
/// `None` = neither header present, so the session keeps whatever it already
/// stored (an OpenAI-compatible client that knows nothing about these headers
/// must not silently reset a conversation's model). Present-but-unknown values
/// resolve to empty — i.e. the default — rather than being forwarded verbatim,
/// so a stale UI or a typo can't push a bogus model id at a provider.
///
/// Effort is validated against **the model that will actually run**, not against
/// a gateway-wide list: switching a session to a provider with no effort scale
/// clears a stale level instead of storing one that silently does nothing.
fn requested_model(
    models: &[ModelEntry],
    default_model: &str,
    headers: &axum::http::HeaderMap,
) -> Option<ModelSelection> {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
    };
    let model = header("x-komo-model");
    let effort = header("x-komo-effort");
    if model.is_none() && effort.is_none() {
        return None;
    }
    // Empty = "run the gateway default", which is a legitimate selection.
    let chosen = model
        .filter(|want| models.iter().any(|entry| entry.id == *want))
        .unwrap_or_default();
    let effective = if chosen.is_empty() {
        default_model
    } else {
        chosen
    };
    let allowed = models
        .iter()
        .find(|entry| entry.id == effective)
        .map_or(&[][..], |entry| entry.efforts);
    Some(ModelSelection {
        model: chosen.to_string(),
        effort: effort
            .filter(|level| allowed.contains(level))
            .unwrap_or_default()
            .to_string(),
    })
}

fn requested_workspace(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    is_loopback: bool,
) -> String {
    if !is_loopback {
        return DEFAULT_WORKSPACE.to_string();
    }
    headers
        .get("x-komo-workspace")
        .and_then(|value| value.to_str().ok())
        .filter(|id| resolve_workspace_id(state, id).is_some())
        .unwrap_or(DEFAULT_WORKSPACE)
        .to_string()
}

async fn bind_session_workspace(
    state: &AppState,
    session_id: &str,
    requested_workspace: &str,
) -> Result<String, ApiError> {
    if let Some(session) = state.actions.sessions.find(session_id).await? {
        return Ok(session.workspace);
    }
    state
        .actions
        .sessions
        .save(&Session::with_workspace(session_id, requested_workspace))
        .await?;
    // `save` is idempotent under a concurrent first request. Read back so both
    // requests honor the workspace chosen by the insert that won the race.
    Ok(state
        .actions
        .sessions
        .find(session_id)
        .await?
        .map(|session| session.workspace)
        .unwrap_or_else(|| requested_workspace.to_string()))
}

/// Decode a `folder:<base64url path>` workspace id into an existing directory.
///
/// The opaque encoding is what lets an arbitrary Unicode path ride in an
/// ASCII-only header. Canonicalizing before the `is_dir` check means a
/// nonexistent path, a file, or anything that doesn't decode yields `None`, and
/// the caller falls back to the process workspace.
fn resolve_folder_workspace(id: &str) -> Option<PathBuf> {
    let encoded = id.strip_prefix("folder:")?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .ok()?;
    let path = PathBuf::from(String::from_utf8(bytes).ok()?);
    let canonical = path.canonicalize().ok()?;
    canonical.is_dir().then_some(canonical)
}

/// What a session may be switched to: the model menu (default first, each with
/// its best-known context window) and the provider's reasoning-effort levels.
/// An empty `efforts` means this provider has no effort knob — the client should
/// say so rather than offer a switch.
async fn list_model_menu(State(state): State<AppState>) -> Json<Value> {
    let models = state
        .models
        .iter()
        .map(|entry| {
            json!({
                "id": entry.id,
                "provider": entry.provider.name(),
                "model": entry.model,
                "context_window": model_context_window(&entry.model),
                // Per entry, not per gateway: a cross-provider menu mixes models
                // that have an effort scale with ones that don't.
                "efforts": entry.efforts,
            })
        })
        .collect::<Vec<_>>();
    Json(json!({
        "provider": state.provider.as_ref(),
        "default_model": state.model.as_ref(),
        "models": models,
    }))
}

async fn list_workspaces(State(state): State<AppState>) -> Json<Value> {
    let workspaces = workspace_entries(&state)
        .into_iter()
        .map(|entry| json!({ "id": entry.id, "name": entry.name, "path": entry.path }))
        .collect::<Vec<_>>();
    Json(json!({ "workspaces": workspaces }))
}

/// One item on the SSE stream for a streaming turn.
enum SseMsg {
    /// A live tool-call event (emitted from the executor via the sink).
    Tool(TurnEvent),
    /// The turn's final assistant reply (or an error rendered as text).
    Final(String),
}

/// State for the SSE `unfold`: draining the channel, then a one-shot `[DONE]`.
enum SseState {
    Live(mpsc::UnboundedReceiver<SseMsg>),
    Done,
}

/// [`ToolEventSink`] that forwards each `TurnEvent` onto the SSE channel.
struct ChannelEventSink {
    tx: mpsc::UnboundedSender<SseMsg>,
}

impl ToolEventSink for ChannelEventSink {
    fn emit(&self, event: TurnEvent) {
        // Best-effort: if the client hung up, the receiver is gone — drop it.
        let _ = self.tx.send(SseMsg::Tool(event));
    }
}

/// Run the turn on a spawned task and return an SSE response that streams live
/// tool-call events as they happen, then the final reply and `[DONE]`.
///
/// Tool events go out as SSE frames with `event: tool` and a JSON `TurnEvent`
/// body; the final reply goes out as an OpenAI-style `chat.completion.chunk`
/// (default `message` event) carrying the whole text with `finish_reason:stop`.
/// The reply is not token-incremental — rig's tool loop has no token stream —
/// so this streams the tool-call process, not the assistant text.
#[allow(clippy::too_many_arguments)]
fn stream_turn(
    handler: Arc<dyn MessageHandler>,
    cancels: Arc<CancelState>,
    ctx: SessionContext,
    session_id: String,
    input: String,
    id: String,
    created: i64,
    model: String,
) -> Response {
    let (tx, rx) = mpsc::unbounded_channel::<SseMsg>();
    // Attach the event sink so the executor emits tool events onto the channel.
    let ctx = ctx.with_event_sink(Arc::new(ChannelEventSink { tx: tx.clone() }));

    // Drive the turn; on completion push the final reply, then drop every sender
    // (this `tx` and the sink's clone inside `ctx`) so the receiver closes.
    tokio::spawn(async move {
        let outcome = with_session(ctx, handler.handle(&session_id, input)).await;
        cancels.finish(&session_id);
        let final_msg = match outcome {
            Ok(text) => text,
            // A cancel is the caller's own doing, not a failure to report as one.
            Err(error) if is_cancelled(&error) => CANCELLED_REPLY.to_string(),
            Err(error) => format!("请求失败：{error:#}"),
        };
        let _ = tx.send(SseMsg::Final(final_msg));
    });

    let stream = futures_util::stream::unfold(SseState::Live(rx), move |state| {
        let id = id.clone();
        let model = model.clone();
        async move {
            match state {
                SseState::Live(mut rx) => match rx.recv().await {
                    Some(SseMsg::Tool(event)) => {
                        let data = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
                        let ev = Event::default().event("tool").data(data);
                        Some((Ok::<Event, Infallible>(ev), SseState::Live(rx)))
                    }
                    Some(SseMsg::Final(text)) => {
                        let chunk = json!({
                            "id": id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model,
                            "choices": [{
                                "index": 0,
                                "delta": { "role": "assistant", "content": text },
                                "finish_reason": "stop",
                            }],
                        });
                        let ev = Event::default().data(chunk.to_string());
                        Some((Ok::<Event, Infallible>(ev), SseState::Live(rx)))
                    }
                    None => Some((
                        Ok::<Event, Infallible>(Event::default().data("[DONE]")),
                        SseState::Done,
                    )),
                },
                SseState::Done => None,
            }
        }
    });
    // A turn can spend minutes in one completion before it emits a tool event.
    // Keep the response alive through that quiet period so a healthy long turn
    // is not mistaken for an idle connection by the TUI or an intermediary.
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
}

/// Continue an existing conversation only when the client opts in with
/// `X-Komo-Session-Id`. Without it, mint an ephemeral session so no server-side
/// history accrues — the client manages its own context.
fn resolve_session(headers: &axum::http::HeaderMap) -> (String, bool) {
    if let Some(id) = headers
        .get("x-komo-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        // Dashboard clients may persist the server-returned id. It is already
        // namespaced, so adding `api:` again creates an empty conversation.
        // Strip every accidental legacy prefix too, so `api:api:<uuid>` repairs
        // itself on the next request instead of becoming a third identity.
        (format!("api:{}", id.trim_start_matches("api:")), true)
    } else {
        (format!("api:{}", uuid::Uuid::now_v7()), false)
    }
}

/// Reduce the OpenAI `messages` array to one input string for the turn.
///
/// Stateful (header given): the agent already has its history in the db, so we
/// pass only the latest user message. Stateless: the client owns the history,
/// so we flatten the whole exchange into the single ephemeral turn.
fn build_input(messages: &[ChatMessage], stateful: bool) -> String {
    if stateful {
        messages
            .iter()
            .rev()
            .find(|m| m.role == "user")
            .or_else(|| messages.last())
            .map(|m| m.content.clone())
            .unwrap_or_default()
    } else {
        messages
            .iter()
            .filter(|m| !m.content.trim().is_empty())
            .map(|m| format!("{}: {}", m.role, m.content))
            .collect::<Vec<_>>()
            .join("\n\n")
    }
}

// ---- dashboard endpoints ---------------------------------------------------

async fn status(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let open_tasks = state.actions.open_tasks().await?.len();
    let sessions = state.actions.session_summaries().await?.len();
    Ok(Json(json!({
        "ok": true,
        "version": crate::cli::VERSION,
        "channels": state.channels.as_ref(),
        "home_chat": state.home,
        "provider": state.provider.as_ref(),
        "model": state.model.as_ref(),
        "context_window": model_context_window(state.model.as_ref()),
        // The provider adapters do not currently expose per-turn token usage.
        "token_usage": Value::Null,
        "open_tasks": open_tasks,
        "sessions": sessions,
    })))
}

/// Best-known capacities for model families supported by Komo. Unknown model
/// ids deliberately return null rather than showing an invented limit.
fn model_context_window(model: &str) -> Option<u64> {
    let model = model.to_ascii_lowercase();
    if model.starts_with("gpt-4.1") {
        Some(1_047_576)
    } else if model.starts_with("gpt-5") || model.contains("codex") {
        Some(400_000)
    } else if model.starts_with("claude-") {
        Some(200_000)
    } else if model.starts_with("gemini-2.5") || model.starts_with("gemini-3") {
        Some(1_048_576)
    } else if model.starts_with("deepseek") {
        Some(128_000)
    } else {
        None
    }
}

/// The `/sethome` runtime override (`None` when unset). The config `home_chat`
/// fallback is *not* resolved here — the CLI derives it from the same
/// config.toml locally; only the db-held override needs the gateway.
async fn get_home(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let over = state.actions.home_override().await?;
    Ok(Json(json!({ "override": over })))
}

async fn list_sessions(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    // Summaries only — never dump every transcript in a list view.
    let sessions = state.actions.session_summaries().await?;
    Ok(Json(json!({ "sessions": sessions })))
}

async fn session_messages(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let messages = state.actions.session_messages(&id).await?;
    Ok(Json(json!({ "session_id": id, "messages": messages })))
}

async fn list_tasks(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    // `Task` serializes verbatim (snake_case status), so the CLI deserializes it
    // straight back into the domain type and reuses its existing renderer.
    let tasks = state.actions.open_tasks().await?;
    Ok(Json(json!({ "tasks": tasks })))
}

#[derive(Deserialize)]
struct MemoryQueryParams {
    status: Option<String>,
}

async fn list_memories(
    State(state): State<AppState>,
    Query(params): Query<MemoryQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let status: Option<MemoryStatus> = params
        .status
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(parse_memory_status);
    // Memory derives Serialize, so it serializes verbatim.
    let memories = state.actions.list_memories(status).await?;
    Ok(Json(json!({ "memories": memories })))
}

// Memory governance writes (`komo memory promote/reject/pin` while the gateway
// holds the db lock). Host-operator actions — loopback-gated by the
// `require_loopback` layer on the operator-writes router.

async fn memory_promote(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    memory_transition(&state, &id, MemoryTransitionAction::Promote).await
}

async fn memory_reject(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    memory_transition(&state, &id, MemoryTransitionAction::Reject).await
}

async fn memory_pin(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    memory_transition(&state, &id, MemoryTransitionAction::Pin).await
}

/// Apply one governance transition (the shared operator definition — the
/// domain owns the semantics) and return the updated memory. 404 on an
/// unknown id.
async fn memory_transition(
    state: &AppState,
    id: &str,
    action: MemoryTransitionAction,
) -> Result<Response, ApiError> {
    match state.actions.memory_transition(id, action).await? {
        TransitionOutcome::Applied(memory) => Ok(Json(json!({ "memory": memory })).into_response()),
        TransitionOutcome::NotFound => Ok((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no memory with id `{id}`") })),
        )
            .into_response()),
    }
}

#[derive(Deserialize)]
struct RunsQueryParams {
    limit: Option<usize>,
}

async fn list_runs(
    State(state): State<AppState>,
    Query(params): Query<RunsQueryParams>,
) -> Result<Json<Value>, ApiError> {
    let limit = params.limit.unwrap_or(50).clamp(1, 500);
    let runs = state.actions.runs(limit).await?;
    Ok(Json(json!({ "runs": runs })))
}

async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    // `Run` and `RunStep` serialize verbatim; the CLI reuses its run renderer.
    match state.actions.run(&id).await? {
        Some((run, steps)) => Ok(Json(json!({ "run": run, "steps": steps })).into_response()),
        None => Ok((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "run not found" })),
        )
            .into_response()),
    }
}

/// Resume an interrupted run (backs `komo run resume` while the gateway holds
/// the db lock): compose the priming input from the ledger and drive one normal
/// turn in the run's original session, then clear the `recoverable` flag.
/// Trust follows the chat rule — loopback + `X-Komo-Trusted` auto-approves
/// (the CLI user is the host operator); anyone else runs detached (auto-deny).
async fn resume_run(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    // Eligibility + priming come from the shared operator definition, so this
    // endpoint and the CLI's direct path can never disagree.
    let (run, steps, input) = match resolve_resume(state.actions.runs.as_ref(), &id).await? {
        crate::services::operator_control::actions::ResumeTarget::Missing => {
            return Ok((
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "run not found" })),
            )
                .into_response());
        }
        crate::services::operator_control::actions::ResumeTarget::NotRecoverable { status } => {
            return Ok((
                StatusCode::CONFLICT,
                Json(json!({ "error": not_recoverable_message(&id, &status) })),
            )
                .into_response());
        }
        crate::services::operator_control::actions::ResumeTarget::Ready { run, steps, input } => {
            (run, steps, input)
        }
    };

    let trusted = peer.ip().is_loopback() && headers.contains_key("x-komo-trusted");
    let ctx = if trusted {
        SessionContext::trusted(&run.session_id)
    } else {
        SessionContext::detached(&run.session_id)
    };
    // Journal-continue first: the interrupted turn picks up exactly where it
    // stopped, tool rounds already paid for replayed instead of re-run. Only
    // when the run isn't continuable (no journal, transcript already answered)
    // does the digest-primed fresh turn run — yesterday's behavior, verbatim.
    let (reply, continued) =
        match with_session(ctx.clone(), state.handler.resume_interrupted(&run)).await? {
            Some(reply) => (reply, true),
            None => (
                with_session(ctx, state.handler.handle(&run.session_id, input)).await?,
                false,
            ),
        };

    if let Err(error) = state.actions.runs.mark_resumed(&id).await {
        warn!(%error, run_id = %id, "failed to clear recoverable flag after resume");
    }
    info!(run_id = %id, session = %run.session_id, continued, "run resumed");
    Ok(Json(ResumeOutcome {
        run_id: id,
        session_id: run.session_id,
        steps: steps.len(),
        reply,
        continued,
    })
    .into_response())
}

// ---- control-plane write endpoints ------------------------------------------
//
// These back the maintenance CLIs (`run prune`, `session clean`,
// `pair approve|revoke`, `dream --apply`) while the gateway holds the db lock.
// All of them are loopback-gated by the `require_loopback` layer on the
// operator-writes router (see `build_router`) — not per-handler checks.

#[derive(Deserialize)]
struct PruneParams {
    cutoff: i64,
}

/// Drop runs (and their steps) started before `cutoff`. The client resolves
/// `--before`/`--keep` into the cutoff (it can read runs over `/api/runs`).
async fn prune_runs(
    State(state): State<AppState>,
    Query(params): Query<PruneParams>,
) -> Result<Response, ApiError> {
    let removed = state.actions.prune_runs(params.cutoff).await?;
    Ok(Json(json!({ "removed": removed })).into_response())
}

#[derive(Deserialize)]
struct WikiSearchBody {
    query: String,
    #[serde(default = "default_wiki_limit")]
    limit: usize,
}

fn default_wiki_limit() -> usize {
    5
}

/// Note-vault search (backs `komo wiki search`). Routed through the gateway
/// because it holds the index open — the CLI cannot open it concurrently.
async fn wiki_search(
    State(state): State<AppState>,
    Json(body): Json<WikiSearchBody>,
) -> Result<Response, ApiError> {
    let hits = state.actions.wiki_search(&body.query, body.limit).await?;
    Ok(Json(json!({ "hits": hits })).into_response())
}

/// What the note-vault index holds (backs `komo wiki status`).
async fn wiki_status(State(state): State<AppState>) -> Result<Response, ApiError> {
    let status = state.actions.wiki_status().await?;
    Ok(Json(json!({ "status": status })).into_response())
}

#[derive(Deserialize)]
struct WikiIndexBody {
    #[serde(default)]
    rebuild: bool,
}

/// Index the vault (backs `komo wiki index`).
///
/// Runs to completion inside the request — minutes for a full rebuild. The
/// client calls this on its no-timeout HTTP client for exactly that reason;
/// progress is visible in the gateway log rather than in the response.
async fn wiki_index(
    State(state): State<AppState>,
    Json(body): Json<WikiIndexBody>,
) -> Result<Response, ApiError> {
    let outcome = state.actions.wiki_index(body.rebuild).await?;
    Ok(Json(json!({ "outcome": outcome })).into_response())
}

/// Delete every session with no messages (backs `komo session clean`).
async fn clean_sessions(State(state): State<AppState>) -> Result<Response, ApiError> {
    let removed = state.actions.clean_sessions().await?;
    Ok(Json(json!({ "removed": removed })).into_response())
}

#[derive(Deserialize)]
struct TitleBody {
    title: String,
}

/// Rename a session (operator/GUI). Loopback-gated.
async fn set_session_title(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<TitleBody>,
) -> Result<Json<Value>, ApiError> {
    state.actions.set_session_title(&id, &body.title).await?;
    Ok(Json(json!({ "ok": true })))
}

#[derive(Deserialize)]
struct StatusBody {
    /// `"active"` | `"archive"` | `"deleted"`.
    status: String,
}

/// Set a session's lifecycle status — archive / unarchive / soft-delete. A
/// `deleted` session is hidden from the list but its rows remain. Loopback-gated.
async fn set_session_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<StatusBody>,
) -> Result<Json<Value>, ApiError> {
    state.actions.set_session_status(&id, &body.status).await?;
    Ok(Json(json!({ "ok": true })))
}

/// Delete a session and its messages so it drops off the list. Loopback-gated.
async fn delete_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let removed = state.actions.delete_session(&id).await?;
    Ok(Json(json!({ "removed": removed })))
}

#[derive(Deserialize)]
struct ApproveParams {
    code: String,
}

/// Approve the pending pairing bearing `code` (backs `komo pair approve`). The
/// outcome variant is echoed so the CLI prints the same message it would locally.
async fn pair_approve(
    State(state): State<AppState>,
    Json(body): Json<ApproveParams>,
) -> Result<Response, ApiError> {
    let json = match state.actions.pair_approve(&body.code).await? {
        ApproveOutcome::Approved(request) => json!({ "outcome": "approved", "id": request.id }),
        ApproveOutcome::NotFound => json!({ "outcome": "not_found" }),
        ApproveOutcome::Locked { retry_after_secs } => {
            json!({ "outcome": "locked", "retry_after_secs": retry_after_secs })
        }
    };
    Ok(Json(json).into_response())
}

/// Remove a pairing by id (backs `komo pair revoke`).
async fn pair_revoke(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let revoked = state.actions.pair_revoke(&id).await?;
    Ok(Json(json!({ "revoked": revoked })).into_response())
}

/// Run one dreaming consolidation cycle (backs `komo dream --apply`) — the same
/// `DreamSweep` the gateway schedules.
async fn dream_apply(State(state): State<AppState>) -> Result<Response, ApiError> {
    let summary = DreamSweep {
        memories: state.actions.memories.clone(),
        skills: state.actions.skills.clone(),
    }
    .apply()
    .await?;
    Ok(Json(json!({
        "promoted": summary.memories_promoted,
        "archived": summary.memories_archived,
        "skills_expired": summary.skill_candidates_expired,
    }))
    .into_response())
}

/// Ranked memory search (backs `komo memory search`): the same hybrid query
/// recall runs, with the gateway's embedder.
#[derive(serde::Deserialize)]
struct MemorySearchBody {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}
fn default_search_limit() -> usize {
    20
}
async fn memory_search(
    State(state): State<AppState>,
    Json(body): Json<MemorySearchBody>,
) -> Result<Response, ApiError> {
    let memories = state.actions.memory_search(&body.query, body.limit).await?;
    Ok(Json(json!({ "memories": memories })).into_response())
}

#[derive(serde::Deserialize)]
struct MemoryUsedBody {
    id: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}
async fn memory_used(
    State(state): State<AppState>,
    Json(body): Json<MemoryUsedBody>,
) -> Result<Response, ApiError> {
    let uses = state.actions.memory_used(&body.id, body.limit).await?;
    Ok(Json(json!({ "uses": uses })).into_response())
}

/// Embed every memory still missing a current vector (backs
/// `komo memory backfill`). Slow by nature: one model call per batch.
async fn memory_backfill(State(state): State<AppState>) -> Result<Response, ApiError> {
    let embedded = state.actions.memory_backfill().await?;
    Ok(Json(json!({ "embedded": embedded })).into_response())
}

/// Widen memories stranded in an ephemeral `api` channel scope to `Global`
/// (backs `komo memory repair-scopes`).
async fn memory_repair_scopes(State(state): State<AppState>) -> Result<Response, ApiError> {
    let repaired = state.actions.repair_memory_scopes().await?;
    Ok(Json(json!({ "repaired": repaired })).into_response())
}

// ---- control-plane read endpoints (CLI ↔ gateway) --------------------------

/// Pending reminders (backs `komo cron list`), soonest first.
async fn list_reminders(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let pending = state.actions.pending_reminders().await?;
    Ok(Json(json!({ "reminders": pending })))
}

// ---- cron-job endpoints (backs `komo cron`) ---------------------------------

/// Every scheduled cron job, by name.
async fn list_cron_jobs(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let jobs = state.actions.list_cron_jobs().await?;
    Ok(Json(json!({ "jobs": jobs })))
}

/// The uniform 404 for an unknown job name — body carries the same message the
/// direct path bails with, so the CLI prints one thing on either transport.
fn cron_not_found(name: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": no_cron_job_message(name) })),
    )
        .into_response()
}

/// Create a job. Validation failures (bad cron expression, duplicate name,
/// missing command) are the caller's to fix → 400 with the message.
async fn cron_add(State(state): State<AppState>, Json(spec): Json<CronJobSpec>) -> Response {
    match state.actions.add_cron_job(spec).await {
        Ok(job) => Json(json!({ "job": job })).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn cron_remove(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    Ok(if state.actions.remove_cron_job(&name).await? {
        Json(json!({ "removed": true })).into_response()
    } else {
        cron_not_found(&name)
    })
}

async fn cron_enable(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    cron_set_enabled(state, name, true).await
}

async fn cron_disable(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    cron_set_enabled(state, name, false).await
}

async fn cron_set_enabled(
    state: AppState,
    name: String,
    enabled: bool,
) -> Result<Response, ApiError> {
    Ok(
        match state.actions.set_cron_enabled(&name, enabled).await? {
            Some(job) => Json(json!({ "job": job })).into_response(),
            None => cron_not_found(&name),
        },
    )
}

/// Make a job due now (fires on the sweep's next tick). A disabled job is a
/// 400 (the shared action refuses it), an unknown one a 404.
async fn cron_trigger(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.actions.trigger_cron_job(&name).await {
        Ok(Some(job)) => Json(json!({ "job": job })).into_response(),
        Ok(None) => cron_not_found(&name),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Registered skills (backs `komo skills list`), by name.
async fn list_skills(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let skills = state.actions.list_skills().await?;
    Ok(Json(json!({ "skills": skills })))
}

/// Which turns loaded a skill (backs `komo skills audit` while the gateway
/// holds the db lock). Derived from the run ledger via the shared operator
/// projection.
async fn skill_audit(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let invocations = state.actions.skill_audit(&name).await?;
    Ok(Json(json!({ "invocations": invocations })))
}

/// Every active skill ranked coldest-first (backs the name-less `komo skills
/// audit`). Same ledger derivation, rolled up instead of filtered.
async fn skill_usage(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let usage = state.actions.skill_usage().await?;
    Ok(Json(json!({ "usage": usage })))
}

/// Pairings (backs `komo pair list`). A hash-free view — the salted code hash
/// and per-row salt are never serialized off the host.
async fn list_pairings(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let pairings = state.actions.pairing_views().await?;
    Ok(Json(json!({ "pairings": pairings })))
}

/// The dreaming dry-run classification (backs `komo dream`, no `--apply`):
/// which candidates would promote / archive, with their scores, plus the full
/// candidate count so a no-op does not look like an empty memory library.
async fn dream_preview(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let report = state.actions.dream_preview().await?;
    Ok(Json(json!({
        "promote": report.promote,
        "archive": report.archive,
        "candidate_count": report.candidate_count,
    })))
}

// ---- interactive approval / clarify (for the GUI) --------------------------
//
// An interactive HTTP turn (`X-Komo-Interactive`) suspends on approval and
// clarify prompts just like a chat channel, but there is no reply sink a human
// reads — the GUI polls `GET /api/interactions/{session}` for the pending
// prompt and resolves it with a `POST`. The GET is an ordinary protected read;
// the two POSTs sit behind `require_interactive_access` (loopback always;
// keyed remote only with `[channels.api] remote_interactive = true`).

/// The prompt(s) a suspended interactive turn is currently waiting on. Either
/// field is `null` when nothing of that kind is pending.
async fn get_interactions(
    State(state): State<AppState>,
    Path(session): Path<String>,
) -> Json<Value> {
    let approval = state.approvals.pending_info(&session);
    let question = state.clarify.pending_question(&session);
    Json(json!({ "approval": approval, "question": question }))
}

/// Stop the session's in-flight turn.
///
/// Order matters: a turn parked on an approval prompt or a clarify question is
/// not at an await the cancel signal can interrupt, so resolve those first (deny
/// / a stop answer) and the loop reaches its next cancellation check
/// immediately, instead of sitting out the 5-minute prompt timeout.
///
/// Cancelling stops further rounds and further tool calls; a tool call already
/// executing runs to completion (see `domain::cancel`). The turn's reply becomes
/// `CANCELLED_REPLY`, which is what lands in the transcript.
async fn cancel_turn(State(state): State<AppState>, Path(session): Path<String>) -> Json<Value> {
    let denied = state.approvals.resolve(&session, Answer::Deny(None));
    let answered = state.clarify.resolve(&session, CANCELLED_REPLY);
    let cancelled = state.cancels.cancel(&session);
    if cancelled {
        info!(%session, denied, answered, "turn cancelled by client");
    }
    Json(json!({
        "cancelled": cancelled,
        "denied_pending_approval": denied,
        "answered_pending_question": answered,
    }))
}

#[derive(Deserialize)]
struct ApprovalDecisionBody {
    /// `"once"` | `"session"` | `"deny"`.
    decision: String,
    /// Optional reason for a denial, relayed to the model so it can correct the
    /// call instead of retrying it (ignored for the two allow decisions).
    #[serde(default)]
    feedback: Option<String>,
}

/// Map the wire decision string to an [`Answer`], attaching `feedback` to a
/// denial. `None` = unrecognized.
fn parse_decision(s: &str, feedback: Option<String>) -> Option<Answer> {
    match s {
        "once" => Some(Answer::Once),
        "session" => Some(Answer::Session),
        "deny" => Some(Answer::Deny(
            feedback.filter(|text| !text.trim().is_empty()),
        )),
        _ => None,
    }
}

/// Resolve a pending approval for `session` (the GUI's approval modal).
async fn resolve_approval(
    State(state): State<AppState>,
    Path(session): Path<String>,
    Json(body): Json<ApprovalDecisionBody>,
) -> Result<Json<Value>, ApiError> {
    let decision = parse_decision(&body.decision, body.feedback).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown decision `{}` (want once|session|deny)",
            body.decision
        )
    })?;
    let resolved = state.approvals.resolve(&session, decision);
    Ok(Json(json!({ "resolved": resolved })))
}

#[derive(Deserialize)]
struct AnswerBody {
    text: String,
}

/// Answer a pending clarify question for `session` (the GUI's inline reply).
async fn answer_question(
    State(state): State<AppState>,
    Path(session): Path<String>,
    Json(body): Json<AnswerBody>,
) -> Result<Json<Value>, ApiError> {
    let resolved = state.clarify.resolve(&session, &body.text);
    Ok(Json(json!({ "resolved": resolved })))
}

/// Unix seconds, for OpenAI `created` fields.
fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stateful_input_takes_last_user_message() {
        let messages = vec![
            ChatMessage {
                role: "user".into(),
                content: "first".into(),
            },
            ChatMessage {
                role: "assistant".into(),
                content: "reply".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "second".into(),
            },
        ];
        assert_eq!(build_input(&messages, true), "second");
    }

    #[test]
    fn stateless_input_flattens_conversation() {
        let messages = vec![
            ChatMessage {
                role: "user".into(),
                content: "hi".into(),
            },
            ChatMessage {
                role: "assistant".into(),
                content: "hello".into(),
            },
        ];
        assert_eq!(
            build_input(&messages, false),
            "user: hi\n\nassistant: hello"
        );
    }

    #[test]
    fn resolve_session_is_ephemeral_without_header() {
        let headers = axum::http::HeaderMap::new();
        let (id, stateful) = resolve_session(&headers);
        assert!(id.starts_with("api:"));
        assert!(!stateful);
    }

    #[test]
    fn resolve_session_uses_header_when_present() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-komo-session-id", "panel-1".parse().unwrap());
        let (id, stateful) = resolve_session(&headers);
        assert_eq!(id, "api:panel-1");
        assert!(stateful);
    }

    #[test]
    fn resolve_session_does_not_double_prefix_a_server_id() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-komo-session-id", "api:panel-1".parse().unwrap());
        let (id, stateful) = resolve_session(&headers);
        assert_eq!(id, "api:panel-1");
        assert!(stateful);
    }

    #[test]
    fn resolve_session_repairs_repeated_legacy_prefixes() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-komo-session-id",
            "api:api:019fad15-8199-7461-9d48-0a6c779f1c8d"
                .parse()
                .unwrap(),
        );
        let (id, stateful) = resolve_session(&headers);
        assert_eq!(id, "api:019fad15-8199-7461-9d48-0a6c779f1c8d");
        assert!(stateful);
    }

    #[test]
    fn local_origins_are_allowed_for_cors() {
        for origin in [
            "http://127.0.0.1:5273", // Electron renderer, vite dev
            "http://localhost:5274", // web build, vite dev
            "http://[::1]:5273",     // IPv6 loopback
            "http://127.0.0.2:8080", // any loopback address
            "null",                  // packaged Electron (file://)
        ] {
            assert!(
                is_local_origin(&HeaderValue::from_static(origin)),
                "{origin} should be allowed"
            );
        }
    }

    #[test]
    fn remote_origins_are_refused_for_cors() {
        for origin in [
            "https://evil.example",
            "http://127.0.0.1.evil.example", // loopback as a subdomain label
            "http://192.168.1.20:5273",      // LAN, not loopback
            "file://",                       // not an origin we grant
            "ws://127.0.0.1:5273",           // only http(s) is granted
            "http://[::1",                   // malformed
        ] {
            assert!(
                !is_local_origin(&HeaderValue::from_static(origin)),
                "{origin} should be refused"
            );
        }
    }

    /// A router shaped like the real one: a protected route whose auth layer
    /// rejects everything, with the CORS layer outermost.
    fn cors_test_router() -> Router {
        Router::new()
            .route("/api/status", get(|| async { "ok" }))
            .route_layer(middleware::from_fn(
                |_req: Request, _next: Next| async move { StatusCode::UNAUTHORIZED },
            ))
            .layer(cors_layer())
    }

    fn preflight(origin: &str) -> Request {
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/status")
            .header("origin", origin)
            .header("access-control-request-method", "GET")
            .header("access-control-request-headers", "authorization")
            .body(axum::body::Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn preflight_is_answered_before_auth() {
        use tower::ServiceExt;

        // The whole point of layering CORS outermost: an `Authorization`-bearing
        // request is preflighted, and if the preflight reached the bearer-key
        // middleware it would 401 — killing the real request behind it.
        let res = cors_test_router()
            .oneshot(preflight("http://127.0.0.1:5273"))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get("access-control-allow-origin").unwrap(),
            "http://127.0.0.1:5273"
        );
        let allowed = res.headers().get("access-control-allow-headers").unwrap();
        assert!(allowed.to_str().unwrap().contains("authorization"));
    }

    #[tokio::test]
    async fn preflight_from_a_remote_origin_gets_no_grant() {
        use tower::ServiceExt;

        let res = cors_test_router()
            .oneshot(preflight("https://evil.example"))
            .await
            .unwrap();
        assert!(res.headers().get("access-control-allow-origin").is_none());
    }

    #[test]
    fn folder_workspace_id_resolves_an_existing_directory() {
        let path = std::env::current_dir().unwrap().canonicalize().unwrap();
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(path.to_string_lossy().as_bytes());
        assert_eq!(
            resolve_folder_workspace(&format!("folder:{encoded}")),
            Some(path)
        );
    }

    #[test]
    fn folder_workspace_id_rejects_non_directories_and_garbage() {
        assert_eq!(resolve_folder_workspace("not-a-folder-id"), None);
        assert_eq!(resolve_folder_workspace("folder:not+base64"), None);

        // A path that doesn't exist: the id decodes, the canonicalize doesn't.
        let missing = std::env::temp_dir().join(format!("komo-missing-{}", uuid::Uuid::now_v7()));
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(missing.to_string_lossy().as_bytes());
        assert_eq!(resolve_folder_workspace(&format!("folder:{encoded}")), None);

        // An existing *file* is not a workspace either.
        let file = std::env::current_dir().unwrap().join("Cargo.toml");
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(file.to_string_lossy().as_bytes());
        assert_eq!(resolve_folder_workspace(&format!("folder:{encoded}")), None);
    }

    /// A cross-provider menu: the default (codex, three effort levels), another
    /// codex model, and a deepseek one — which has **no** effort scale. That
    /// asymmetry is what the effort rules below turn on.
    fn menu() -> Vec<ModelEntry> {
        vec![
            ModelEntry {
                id: "gpt-5.5".into(),
                provider: komo_config::Provider::Codex,
                model: "gpt-5.5".into(),
                efforts: &["low", "medium", "high"],
            },
            ModelEntry {
                id: "gpt-5.4-mini".into(),
                provider: komo_config::Provider::Codex,
                model: "gpt-5.4-mini".into(),
                efforts: &["low", "medium", "high"],
            },
            ModelEntry {
                id: "deepseek:deepseek-chat".into(),
                provider: komo_config::Provider::DeepSeek,
                model: "deepseek-chat".into(),
                efforts: &[],
            },
        ]
    }

    const DEFAULT_MODEL: &str = "gpt-5.5";

    fn model_headers(pairs: &[(&str, &str)]) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        headers
    }

    #[test]
    fn no_model_headers_leaves_the_stored_selection_alone() {
        // An OpenAI-compatible client that knows nothing about these headers must
        // not silently reset a conversation's model.
        assert!(requested_model(&menu(), DEFAULT_MODEL, &model_headers(&[])).is_none());
    }

    #[test]
    fn advertised_model_and_effort_are_accepted() {
        let selection = requested_model(
            &menu(),
            DEFAULT_MODEL,
            &model_headers(&[("x-komo-model", "gpt-5.4-mini"), ("x-komo-effort", "high")]),
        )
        .expect("headers present");
        assert_eq!(selection.model, "gpt-5.4-mini");
        assert_eq!(selection.effort, "high");
    }

    #[test]
    fn a_qualified_cross_provider_id_is_accepted_verbatim() {
        // The stored value keeps its `provider:` prefix — that is what routes the
        // turn to the other backend (`infra::llm::RoutingLlm`).
        let selection = requested_model(
            &menu(),
            DEFAULT_MODEL,
            &model_headers(&[("x-komo-model", "deepseek:deepseek-chat")]),
        )
        .expect("headers present");
        assert_eq!(selection.model, "deepseek:deepseek-chat");
    }

    #[test]
    fn effort_is_validated_against_the_model_that_will_run() {
        // deepseek has no effort scale, so a level valid for the codex default
        // must not survive the switch — storing it would silently do nothing.
        let selection = requested_model(
            &menu(),
            DEFAULT_MODEL,
            &model_headers(&[
                ("x-komo-model", "deepseek:deepseek-chat"),
                ("x-komo-effort", "high"),
            ]),
        )
        .expect("headers present");
        assert_eq!(selection.model, "deepseek:deepseek-chat");
        assert_eq!(
            selection.effort, "",
            "an effort level the target provider doesn't support must be dropped"
        );
    }

    #[test]
    fn effort_alone_is_validated_against_the_gateway_default_model() {
        // No model header: the default runs, so *its* levels decide.
        let selection = requested_model(
            &menu(),
            DEFAULT_MODEL,
            &model_headers(&[("x-komo-effort", "medium")]),
        )
        .expect("headers present");
        assert_eq!(selection.model, "");
        assert_eq!(selection.effort, "medium");
    }

    #[test]
    fn unadvertised_values_resolve_to_the_default_not_the_provider() {
        let selection = requested_model(
            &menu(),
            DEFAULT_MODEL,
            &model_headers(&[
                ("x-komo-model", "definitely-not-a-model"),
                ("x-komo-effort", "extreme"),
            ]),
        )
        .expect("headers present");
        assert_eq!(
            selection.model, "",
            "an unknown id must not reach the provider"
        );
        assert_eq!(selection.effort, "");
    }

    #[test]
    fn one_header_alone_clears_the_other() {
        // Sending only a model is a full selection: effort resets to the default.
        let selection = requested_model(
            &menu(),
            DEFAULT_MODEL,
            &model_headers(&[("x-komo-model", "gpt-5.5")]),
        )
        .expect("headers present");
        assert_eq!(selection.model, "gpt-5.5");
        assert_eq!(selection.effort, "");
    }

    #[test]
    fn parse_decision_maps_known_strings_and_rejects_others() {
        assert_eq!(parse_decision("once", None), Some(Answer::Once));
        assert_eq!(parse_decision("session", None), Some(Answer::Session));
        assert_eq!(parse_decision("deny", None), Some(Answer::Deny(None)));
        assert_eq!(parse_decision("approve", None), None);
        assert_eq!(parse_decision("", None), None);
    }

    #[test]
    fn deny_feedback_rides_along_but_blank_is_dropped() {
        assert_eq!(
            parse_decision("deny", Some("用 trash".into())),
            Some(Answer::Deny(Some("用 trash".into())))
        );
        assert_eq!(
            parse_decision("deny", Some("   ".into())),
            Some(Answer::Deny(None))
        );
        // An allow ignores feedback entirely.
        assert_eq!(
            parse_decision("once", Some("ignored".into())),
            Some(Answer::Once)
        );
    }

    // The interactions state round-trip (register → pending_info/pending_question
    // visible → resolve delivers the decision/answer) is covered at the state
    // layer in `agent::interaction` and `services::clarify`; the handlers here
    // are thin wrappers over those, and `require_loopback` (shared with every
    // operator write) gates the two POST routes by construction.
}
