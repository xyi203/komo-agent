//! HTTP client the `komo` CLI uses to reach a **running gateway**.
//!
//! Turso takes an exclusive cross-process lock on each db file, so while the
//! gateway runs the CLI can't open the db itself. Instead it talks to the
//! gateway's always-on loopback api channel (`infra/messaging/api.rs`), which
//! the gateway advertises in `~/.komo/gateway.json` (see `infra/rendezvous`).
//!
//! [`GatewayClient::try_connect`] is the single "is a gateway reachable?" check
//! every CLI command makes: `Some` → route over HTTP, `None` → open the db
//! directly (today's path). The read methods return the **domain types** the
//! endpoints serialize verbatim, so the existing CLI renderers are reused.

use std::{path::Path, time::Duration};

use anyhow::Context;
use base64::Engine as _;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::domain::{
    cron::{CronJob, CronJobSpec},
    events::TurnEvent,
    memory::Memory,
    message::Message,
    reminder::Reminder,
    run::{Run, RunStep},
    task::Task,
};
use crate::infra::rendezvous::{self, GatewayInfo};
use crate::services::operator_control::{
    DreamItem, DreamReport, PairingView, ResumeOutcome, SessionSummary, SkillInvocation, SkillUsage,
};

/// How long to wait for the gateway to answer a request (a turn can take a
/// while — chat goes through the full agent loop server-side).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
/// The liveness probe must be quick: a stale rendezvous file (crashed gateway)
/// should fall back to the db fast, not hang the CLI.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Fail with the reason the gateway gave, not merely its status code.
///
/// `reqwest`'s `error_for_status` throws the response body away — and the body
/// is exactly where `ApiError` puts the reason. Every gateway-side failure
/// therefore reached the operator as a bare "500 Internal Server Error" with
/// nothing to act on, while the real cause sat in the gateway log.
///
/// Callers that special-case a status (404, 409) still check it first; this only
/// has to turn *unhandled* failures into something readable.
async fn checked(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
    let status = resp.status();
    if !status.is_client_error() && !status.is_server_error() {
        return Ok(resp);
    }
    let body = resp.text().await.unwrap_or_default();
    let reason = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v.get("error")?.as_str().map(str::to_string))
        .unwrap_or(body);
    match reason.trim() {
        "" => anyhow::bail!("gateway returned {status}"),
        reason => anyhow::bail!("{reason}"),
    }
}

/// Encode a locally chosen directory for the gateway's workspace header.
///
/// The gateway accepts this form only from loopback callers, canonicalizes it,
/// and persists the resulting opaque id when the session is first created.  It
/// deliberately carries a path as base64url rather than exposing path syntax in
/// an HTTP header (which also keeps Unicode paths valid).
pub fn folder_workspace_id(dir: &Path) -> anyhow::Result<String> {
    let dir = dir
        .canonicalize()
        .with_context(|| format!("cannot resolve workspace `{}`", dir.display()))?;
    if !dir.is_dir() {
        anyhow::bail!("workspace `{}` is not a directory", dir.display());
    }
    let path = dir.to_str().context("workspace path is not valid UTF-8")?;
    Ok(format!(
        "folder:{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(path.as_bytes())
    ))
}

pub struct GatewayClient {
    base: String,
    key: String,
    /// Bounded client for ordinary control-plane requests.
    http: reqwest::Client,
    /// Streaming turns can legitimately outlive `REQUEST_TIMEOUT`: one turn
    /// may contain several bounded LLM completions and tool calls. Their
    /// lifetime is enforced server-side; applying a whole-response timeout in
    /// the client would instead cut off a healthy SSE body mid-turn.
    streaming_http: reqwest::Client,
}

/// The lightweight live snapshot published by the gateway. It deliberately
/// reports mounted channels rather than claiming provider connectivity.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct GatewayStatus {
    #[serde(default)]
    pub channels: Vec<String>,
}

/// Result of a gateway-routed `pair approve`, mirroring the db path's
/// `ApproveOutcome` so the CLI prints the same message either way.
pub enum PairApprove {
    Approved(String),
    NotFound,
    Locked(i64),
}

impl GatewayClient {
    /// Reachable gateway → `Some`; no rendezvous file, unparseable, or the probe
    /// fails (stale file / crashed gateway) → `None` (caller falls back to db).
    pub async fn try_connect() -> Option<GatewayClient> {
        Self::from_info(rendezvous::read()?).await
    }

    /// Build a client for an advertised gateway and confirm it answers `/health`.
    /// Split out from [`try_connect`] so it is testable without a rendezvous file.
    async fn from_info(info: GatewayInfo) -> Option<GatewayClient> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .ok()?;
        let streaming_http = reqwest::Client::builder().build().ok()?;
        let base = info.base_url();
        Self::health_ok(&http, &base).await.then(|| GatewayClient {
            base,
            key: info.key,
            http,
            streaming_http,
        })
    }

    /// Whatever an advertised gateway reports on `/health`, if one answers.
    ///
    /// For `komo doctor`: the payload carries the gateway's build (the CLI and
    /// the gateway are installed by separate steps, so they drift routinely,
    /// and a mismatch surfaces as a deserialization error deep in some command
    /// rather than as anything naming the cause) and its plugin state (mounted
    /// live, invisible from the filesystem). `None` means no gateway is
    /// advertised or it did not answer; the absence of a report is not itself
    /// a failure.
    pub async fn advertised_health() -> Option<Value> {
        let info = rendezvous::read()?;
        let http = reqwest::Client::builder()
            .timeout(PROBE_TIMEOUT)
            .build()
            .ok()?;
        http.get(format!("{}/health", info.base_url()))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()
    }

    /// One quick unauthenticated `/health` probe. Shared by [`from_info`] and
    /// `komo health` (the Docker HEALTHCHECK command).
    pub async fn health_ok(http: &reqwest::Client, base: &str) -> bool {
        http.get(format!("{base}/health"))
            .timeout(PROBE_TIMEOUT)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    /// GET `path` and pull `key` out of the `{ "<key>": T }` envelope.
    async fn get_field<T: DeserializeOwned>(&self, path: &str, key: &str) -> anyhow::Result<T> {
        let resp = self
            .http
            .get(self.url(path))
            .bearer_auth(&self.key)
            .send()
            .await?;
        let mut map: Map<String, Value> = checked(resp).await?.json().await?;
        let val = map
            .remove(key)
            .with_context(|| format!("gateway response missing `{key}`"))?;
        Ok(serde_json::from_value(val)?)
    }

    pub async fn memories(&self) -> anyhow::Result<Vec<Memory>> {
        self.get_field("/api/memories", "memories").await
    }

    pub async fn status(&self) -> anyhow::Result<GatewayStatus> {
        self.get_field("/api/status", "channels")
            .await
            .map(|channels| GatewayStatus { channels })
    }

    pub async fn tasks(&self) -> anyhow::Result<Vec<Task>> {
        self.get_field("/api/tasks", "tasks").await
    }

    pub async fn runs(&self, limit: usize) -> anyhow::Result<Vec<Run>> {
        self.get_field(&format!("/api/runs?limit={limit}"), "runs")
            .await
    }

    /// One run with its steps; `None` if the gateway has no such run (404).
    pub async fn run(&self, id: &str) -> anyhow::Result<Option<(Run, Vec<RunStep>)>> {
        let resp = self
            .http
            .get(self.url(&format!("/api/runs/{id}")))
            .bearer_auth(&self.key)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let mut map: Map<String, Value> = checked(resp).await?.json().await?;
        let run: Run = serde_json::from_value(
            map.remove("run")
                .context("gateway response missing `run`")?,
        )?;
        let steps: Vec<RunStep> =
            serde_json::from_value(map.remove("steps").unwrap_or_else(|| Value::Array(vec![])))?;
        Ok(Some((run, steps)))
    }

    /// Resume an interrupted run server-side: the gateway composes the priming
    /// input from its ledger, drives the turn (trusted — loopback + the marker
    /// header, same as `chat`), and clears the `recoverable` flag. 404 and 409
    /// come back as clear errors rather than raw HTTP failures.
    pub async fn resume(&self, id: &str) -> anyhow::Result<ResumeOutcome> {
        let resp = self
            .http
            .post(self.url(&format!("/api/runs/{id}/resume")))
            .bearer_auth(&self.key)
            .header("X-Komo-Trusted", "1")
            .send()
            .await?;
        match resp.status() {
            reqwest::StatusCode::NOT_FOUND => anyhow::bail!("no run with id `{id}`"),
            reqwest::StatusCode::CONFLICT => {
                let v: Value = resp.json().await.unwrap_or_default();
                let msg = v
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("run is not recoverable")
                    .to_string();
                anyhow::bail!(msg);
            }
            _ => {}
        }
        Ok(checked(resp).await?.json().await?)
    }

    pub async fn sessions(&self) -> anyhow::Result<Vec<SessionSummary>> {
        self.get_field("/api/sessions", "sessions").await
    }

    /// The operator's home conversation, opened on first ask. A local client
    /// starts here rather than minting its own id, which is what makes the TUI
    /// and a Telegram DM one thread (docs/bot-runtime.md §2 D6).
    pub async fn home_session(&self) -> anyhow::Result<String> {
        self.get_field("/api/home-session", "session").await
    }

    /// `/new`: draw a context boundary in `session`.
    pub async fn conversation_boundary(&self, session: &str) -> anyhow::Result<bool> {
        self.post_field(
            &format!("/api/sessions/{session}/boundary"),
            json!({}),
            "ok",
        )
        .await
    }

    /// Transcript entries for one known session, used to hydrate a resumed TUI.
    pub async fn session_messages(&self, id: &str) -> anyhow::Result<Vec<Message>> {
        let mut url = reqwest::Url::parse(&self.base)?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("gateway base URL cannot contain a path"))?
            .extend(["api", "sessions", id, "messages"]);
        let resp = self.http.get(url).bearer_auth(&self.key).send().await?;
        let mut map: Map<String, Value> = checked(resp).await?.json().await?;
        let messages = map
            .remove("messages")
            .context("gateway response missing `messages`")?;
        Ok(serde_json::from_value(messages)?)
    }

    pub async fn reminders(&self) -> anyhow::Result<Vec<Reminder>> {
        self.get_field("/api/reminders", "reminders").await
    }

    /// Which turns loaded a skill (derived from the run ledger server-side).
    pub async fn skill_audit(&self, name: &str) -> anyhow::Result<Vec<SkillInvocation>> {
        self.get_field(&format!("/api/skills/{name}/audit"), "invocations")
            .await
    }

    /// Every active skill ranked coldest-first (same server-side derivation).
    pub async fn skill_usage(&self) -> anyhow::Result<Vec<SkillUsage>> {
        self.get_field("/api/skills/usage", "usage").await
    }

    pub async fn pairings(&self) -> anyhow::Result<Vec<PairingView>> {
        self.get_field("/api/pairings", "pairings").await
    }

    /// The `/sethome` runtime override (`None` when unset). The config
    /// `home_chat` fallback is derived locally from the same config.toml.
    pub async fn home_override(&self) -> anyhow::Result<Option<String>> {
        self.get_field("/api/home", "override").await
    }

    /// The dreaming dry-run: actionable candidate lists and the total number
    /// under observation. `candidate_count` defaults for an older gateway.
    pub async fn dream_preview(&self) -> anyhow::Result<DreamReport> {
        let resp = self
            .http
            .get(self.url("/api/dream"))
            .bearer_auth(&self.key)
            .send()
            .await?;
        let mut map: Map<String, Value> = checked(resp).await?.json().await?;
        let take = |map: &mut Map<String, Value>, k: &str| -> anyhow::Result<Vec<DreamItem>> {
            Ok(serde_json::from_value(
                map.remove(k).unwrap_or_else(|| Value::Array(vec![])),
            )?)
        };
        let promote = take(&mut map, "promote")?;
        let archive = take(&mut map, "archive")?;
        let candidate_count = serde_json::from_value(
            map.remove("candidate_count")
                .unwrap_or_else(|| Value::from(promote.len() + archive.len())),
        )?;
        // An older gateway reports no skill half at all: an empty list and a
        // zero count, which renders as "no proposals" rather than as a claim
        // that none of them would expire.
        let expire_skills: Vec<String> = serde_json::from_value(
            map.remove("expire_skills")
                .unwrap_or_else(|| Value::Array(vec![])),
        )?;
        let skill_candidate_count = serde_json::from_value(
            map.remove("skill_candidate_count")
                .unwrap_or_else(|| Value::from(expire_skills.len())),
        )?;
        Ok(DreamReport {
            promote,
            archive,
            candidate_count,
            expire_skills,
            skill_candidate_count,
        })
    }

    /// Apply a memory governance transition (`promote` | `reject` | `pin`)
    /// through the gateway (which holds the db lock). The endpoint is
    /// loopback-gated server-side; a 404 becomes a clear "no such id" error.
    pub async fn memory_transition(&self, id: &str, action: &str) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(self.url(&format!("/api/memories/{id}/{action}")))
            .bearer_auth(&self.key)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!("no memory with id `{id}`");
        }
        checked(resp).await?;
        Ok(())
    }

    /// POST a loopback-gated control-plane write and return the JSON reply
    /// object. The one request path all the maintenance write routes share —
    /// auth, error mapping, and the version-skew case live here: a running
    /// gateway from before the endpoint existed answers 404, which would
    /// otherwise surface as an opaque reqwest error with no db fallback
    /// possible (the old gateway holds the lock), so it becomes an actionable
    /// "restart the gateway" message instead.
    async fn post_json(&self, path: &str, body: Value) -> anyhow::Result<Map<String, Value>> {
        let resp = self
            .http
            .post(self.url(path))
            .bearer_auth(&self.key)
            .json(&body)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            anyhow::bail!(
                "the running gateway doesn't serve `{path}` — it predates this command.\n\
                 Restart it onto the current binary (`komo gateway restart`) and retry."
            );
        }
        Ok(checked(resp).await?.json().await?)
    }

    /// [`post_json`], pulling one field out of the `{ "<field>": T }` envelope.
    async fn post_field<T: DeserializeOwned>(
        &self,
        path: &str,
        body: Value,
        field: &str,
    ) -> anyhow::Result<T> {
        let mut map = self.post_json(path, body).await?;
        let val = map
            .remove(field)
            .with_context(|| format!("gateway response missing `{field}`"))?;
        Ok(serde_json::from_value(val)?)
    }

    /// Stop the turn in flight on `session`. `true` when a turn was actually
    /// signalled; `false` when there was nothing to stop (it had already
    /// finished, or never started).
    ///
    /// One request covers all three ways a turn can be stuck: the endpoint
    /// denies a pending approval and answers a pending `ask_user` question
    /// before flipping the cancel signal. A turn parked on either of those would
    /// not observe the signal at all until it was resolved, so the order matters
    /// and is the server's to own — see `api::cancel_turn`.
    pub async fn cancel_turn(&self, session: &str) -> anyhow::Result<bool> {
        self.post_field(
            &format!("/api/interactions/{session}/cancel"),
            json!({}),
            "cancelled",
        )
        .await
    }

    /// Prune runs started before `cutoff` server-side; returns the count removed.
    pub async fn prune_runs(&self, cutoff: i64) -> anyhow::Result<usize> {
        self.post_field(
            &format!("/api/runs/prune?cutoff={cutoff}"),
            json!({}),
            "removed",
        )
        .await
    }

    pub async fn wiki_search(
        &self,
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<komo_core::operator_view::WikiHitView>> {
        self.post_field(
            "/api/wiki/search",
            json!({ "query": query, "limit": limit }),
            "hits",
        )
        .await
    }

    pub async fn wiki_status(&self) -> anyhow::Result<komo_core::operator_view::WikiStatusView> {
        self.get_field("/api/wiki/status", "status").await
    }

    /// Index the vault server-side.
    ///
    /// Uses `streaming_http` rather than `http` for one reason: a full vault
    /// rebuild takes minutes and would blow past `REQUEST_TIMEOUT`. That client
    /// exists precisely for calls whose duration the server, not the client,
    /// should bound. Progress goes to the gateway log (`komo logs -f`).
    pub async fn wiki_index(
        &self,
        rebuild: bool,
    ) -> anyhow::Result<komo_core::operator_view::WikiIndexView> {
        let resp = self
            .streaming_http
            .post(self.url("/api/wiki/index"))
            .bearer_auth(&self.key)
            .json(&json!({ "rebuild": rebuild }))
            .send()
            .await?;
        let mut map: Map<String, Value> = checked(resp).await?.json().await?;
        let val = map
            .remove("outcome")
            .context("gateway reply had no `outcome`")?;
        Ok(serde_json::from_value(val)?)
    }

    /// Delete empty sessions server-side; returns the count removed.
    pub async fn clean_sessions(&self) -> anyhow::Result<usize> {
        self.post_field("/api/sessions/clean", json!({}), "removed")
            .await
    }

    /// Approve a pending pairing by code server-side.
    pub async fn pair_approve(&self, code: &str) -> anyhow::Result<PairApprove> {
        let v = self
            .post_json("/api/pairings/approve", json!({ "code": code }))
            .await?;
        match v.get("outcome").and_then(|o| o.as_str()) {
            Some("approved") => Ok(PairApprove::Approved(
                v.get("id")
                    .and_then(|i| i.as_str())
                    .unwrap_or("")
                    .to_string(),
            )),
            Some("locked") => Ok(PairApprove::Locked(
                v.get("retry_after_secs")
                    .and_then(|s| s.as_i64())
                    .unwrap_or(0),
            )),
            _ => Ok(PairApprove::NotFound),
        }
    }

    /// Revoke a pairing by id server-side; returns whether a row was removed.
    pub async fn pair_revoke(&self, id: &str) -> anyhow::Result<bool> {
        self.post_field(&format!("/api/pairings/{id}/revoke"), json!({}), "revoked")
            .await
    }

    /// Every scheduled cron job (backs `komo cron list`).
    pub async fn cron_jobs(&self) -> anyhow::Result<Vec<CronJob>> {
        self.get_field("/api/cron", "jobs").await
    }

    /// POST a cron write and surface the endpoint's own error message (bad
    /// cron expression, duplicate name, unknown job) instead of a raw HTTP
    /// failure. A body-less 404 still gets the version-skew hint — an old
    /// gateway holds the db lock, so there is no fallback.
    async fn cron_post(&self, path: &str, body: Value) -> anyhow::Result<Map<String, Value>> {
        let resp = self
            .http
            .post(self.url(path))
            .bearer_auth(&self.key)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let v: Value = resp.json().await.unwrap_or_default();
            match v.get("error").and_then(|e| e.as_str()) {
                Some(msg) => anyhow::bail!(msg.to_string()),
                None if status == reqwest::StatusCode::NOT_FOUND => anyhow::bail!(
                    "the running gateway doesn't serve `{path}` — it predates this command.\n\
                     Restart it onto the current binary (`komo gateway restart`) and retry."
                ),
                None => anyhow::bail!("gateway answered {status}"),
            }
        }
        Ok(resp.json().await?)
    }

    /// [`cron_post`], pulling the updated/created job out of the reply.
    async fn cron_post_job(&self, path: &str, body: Value) -> anyhow::Result<CronJob> {
        let mut map = self.cron_post(path, body).await?;
        let val = map
            .remove("job")
            .context("gateway response missing `job`")?;
        Ok(serde_json::from_value(val)?)
    }

    /// Create a cron job server-side (validated there; see `actions::add_cron_job`).
    pub async fn cron_add(&self, spec: &CronJobSpec) -> anyhow::Result<CronJob> {
        self.cron_post_job("/api/cron/add", serde_json::to_value(spec)?)
            .await
    }

    pub async fn cron_remove(&self, name: &str) -> anyhow::Result<()> {
        self.cron_post(&format!("/api/cron/{name}/remove"), json!({}))
            .await?;
        Ok(())
    }

    pub async fn cron_set_enabled(&self, name: &str, enabled: bool) -> anyhow::Result<CronJob> {
        let action = if enabled { "enable" } else { "disable" };
        self.cron_post_job(&format!("/api/cron/{name}/{action}"), json!({}))
            .await
    }

    pub async fn cron_trigger(&self, name: &str) -> anyhow::Result<CronJob> {
        self.cron_post_job(&format!("/api/cron/{name}/trigger"), json!({}))
            .await
    }

    /// Run one dreaming consolidation cycle server-side; returns
    /// `(promoted, archived)` counts.
    /// One dreaming cycle: `(promoted, archived, skills_expired)`. A missing
    /// field reads as 0, so an older gateway that ran no skill half still parses.
    pub async fn dream_apply(&self) -> anyhow::Result<(usize, usize, usize)> {
        let mut map = self.post_json("/api/dream/apply", json!({})).await?;
        let mut take = |k: &str| -> anyhow::Result<usize> {
            Ok(serde_json::from_value(
                map.remove(k).unwrap_or(Value::from(0)),
            )?)
        };
        let promoted = take("promoted")?;
        let archived = take("archived")?;
        let skills_expired = take("skills_expired")?;
        Ok((promoted, archived, skills_expired))
    }

    /// Ranked memory search server-side, where the embedder lives.
    pub async fn memory_search(
        &self,
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<komo_core::domain::memory::Memory>> {
        self.post_field(
            "/api/memories/search",
            json!({ "query": query, "limit": limit }),
            "memories",
        )
        .await
    }

    /// Which turns a memory reached the prompt of — read from the ledger, which
    /// only the gateway process can open while it runs.
    pub async fn memory_used(
        &self,
        id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<komo_core::domain::run::MemoryUse>> {
        self.post_field(
            "/api/memories/used",
            json!({ "id": id, "limit": limit }),
            "uses",
        )
        .await
    }

    /// Embed every memory still missing a current vector, server-side; returns
    /// how many gained one. Slow by nature — a never-embedded library calls the
    /// model once per batch — so it rides the long timeout, like wiki indexing.
    pub async fn memory_backfill(&self) -> anyhow::Result<usize> {
        // `streaming_http` for the same reason wiki indexing uses it: this calls
        // the embedding model once per batch, so it outlives the ordinary
        // operator timeout on a library that has never been embedded.
        let resp = self
            .streaming_http
            .post(self.url("/api/memories/backfill"))
            .bearer_auth(&self.key)
            .json(&json!({}))
            .send()
            .await?;
        let mut map: Map<String, Value> = checked(resp).await?.json().await?;
        Ok(serde_json::from_value(
            map.remove("embedded").unwrap_or(Value::from(0)),
        )?)
    }

    /// Widen memories stranded in an ephemeral `api` channel scope to `Global`
    /// server-side; returns how many moved.
    pub async fn memory_repair_scopes(&self) -> anyhow::Result<usize> {
        let mut map = self
            .post_json("/api/memories/repair-scopes", json!({}))
            .await?;
        Ok(serde_json::from_value(
            map.remove("repaired").unwrap_or(Value::from(0)),
        )?)
    }

    /// Run one chat turn server-side and return the reply. Sends the stable
    /// session id (so history threads) and the trusted marker (so the gateway
    /// auto-approves side-effecting tools — it is gated to loopback callers).
    #[allow(dead_code)]
    pub async fn chat(&self, session_id: &str, message: &str) -> anyhow::Result<String> {
        self.chat_streaming(session_id, message, |_| {}).await
    }

    /// Like [`chat`](Self::chat), but asks the gateway to **stream** the turn
    /// (`stream: true`) and invokes `on_event` for each live [`TurnEvent`] (a
    /// tool starting / finishing) as it arrives, returning the final reply.
    ///
    /// The gateway's `/v1/chat/completions` streams SSE frames: `event: tool`
    /// frames carry a JSON [`TurnEvent`]; the final default-event frame is an
    /// OpenAI-style `chat.completion.chunk` whose `delta.content` is the whole
    /// reply; a trailing `[DONE]` closes it (see `infra/messaging/api.rs`).
    /// rig's tool loop has no token stream, so this streams the *tool-call
    /// process*, not the assistant text token-by-token.
    pub async fn chat_streaming(
        &self,
        session_id: &str,
        message: &str,
        on_event: impl FnMut(TurnEvent),
    ) -> anyhow::Result<String> {
        self.chat_streaming_with_workspace(session_id, message, None, on_event)
            .await
    }

    /// As [`chat_streaming`](Self::chat_streaming), but binds a **new** session
    /// to the caller's startup directory. Existing sessions retain their stored
    /// workspace server-side, regardless of this header.
    pub async fn chat_streaming_in_workspace(
        &self,
        session_id: &str,
        message: &str,
        workspace: &str,
        on_event: impl FnMut(TurnEvent),
    ) -> anyhow::Result<String> {
        self.chat_streaming_with_workspace(session_id, message, Some(workspace), on_event)
            .await
    }

    async fn chat_streaming_with_workspace(
        &self,
        session_id: &str,
        message: &str,
        workspace: Option<&str>,
        mut on_event: impl FnMut(TurnEvent),
    ) -> anyhow::Result<String> {
        let body = json!({
            "model": "komo",
            "stream": true,
            "messages": [{ "role": "user", "content": message }],
        });
        let request = self
            .streaming_http
            .post(self.url("/v1/chat/completions"))
            .bearer_auth(&self.key)
            .header("X-Komo-Session-Id", session_id)
            .header("X-Komo-Trusted", "1");
        let request = if let Some(workspace) = workspace {
            request.header("X-Komo-Workspace", workspace)
        } else {
            request
        };
        let mut resp = checked(request.json(&body).send().await?).await?;

        let mut reply = String::new();
        let mut buf = String::new();
        // SSE frames are separated by a blank line; read the body incrementally
        // (reqwest's `chunk()` needs no extra feature) and dispatch whole frames.
        while let Some(chunk) = resp.chunk().await? {
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(pos) = buf.find("\n\n") {
                let frame: String = buf.drain(..pos + 2).collect();
                parse_sse_frame(&frame, &mut on_event, &mut reply);
            }
        }
        if !buf.trim().is_empty() {
            parse_sse_frame(&buf, &mut on_event, &mut reply);
        }
        Ok(reply)
    }
}

#[cfg(test)]
mod workspace_tests {
    use super::*;

    #[test]
    fn folder_workspace_id_is_base64url_encoded() {
        let dir = std::env::temp_dir();
        let id = folder_workspace_id(&dir).unwrap();
        let encoded = id.strip_prefix("folder:").unwrap();
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .unwrap();
        assert_eq!(
            Path::new(std::str::from_utf8(&decoded).unwrap()),
            dir.canonicalize().unwrap()
        );
    }
}

/// Parse one SSE frame from the chat stream: dispatch a `TurnEvent` (on the
/// `tool` event) via `on_event`, or capture the final reply from a
/// `chat.completion.chunk` delta. `[DONE]` and anything unrecognized are
/// ignored. Multiple `data:` lines in a frame are joined with newlines per the
/// SSE spec (komo's payloads are single-line, but this stays spec-correct).
fn parse_sse_frame(frame: &str, on_event: &mut impl FnMut(TurnEvent), reply: &mut String) {
    let mut event_name = "message".to_string();
    let mut data = String::new();
    for line in frame.lines() {
        if let Some(rest) = line.strip_prefix("event:") {
            event_name = rest.trim().to_string();
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data.is_empty() || data == "[DONE]" {
        return;
    }
    if event_name == "tool" {
        if let Ok(event) = serde_json::from_str::<TurnEvent>(&data) {
            on_event(event);
        }
        return;
    }
    // Default event: the OpenAI-style chunk carrying the final reply text.
    if let Ok(v) = serde_json::from_str::<Value>(&data)
        && let Some(text) = v
            .pointer("/choices/0/delta/content")
            .and_then(|c| c.as_str())
    {
        *reply = text.to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn from_info_returns_none_when_nothing_listening() {
        // Port 1 is privileged and (essentially) never has a listener → the
        // health probe fails fast and we fall back to the db.
        let info = GatewayInfo {
            pid: 0,
            bind: "127.0.0.1".into(),
            port: 1,
            key: "k".into(),
        };
        assert!(GatewayClient::from_info(info).await.is_none());
    }
}
