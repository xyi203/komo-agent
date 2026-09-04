use komo_infra::codex::{CODEX_BASE_URL, CodexAuth, codex_static_headers};
use komo_services::memory_enrichment::MemoryEnricher;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use serde_json::{Value, json};
use tracing::warn;

use komo_config::{ModelConfig, Provider, split_model_id};
use komo_core::domain::{
    catalog::ToolCatalog,
    llm::{DeltaSink, LlmClient, Step, TokenUsage, ToolCallReq, ToolOutcome, TurnDriver},
    message::{Message, Role},
    run::RecalledMemories,
    session::Session,
    session_event::{
        AssistantRoundEvent, HeaderReason, MessageSource, RequestHeaderEvent, SessionEvent,
        SessionEventKind, SurfacePlacement, TurnRecorder, UserMessageEvent, fold_request_header,
    },
};
use komo_provider::{
    AssistantBlock, Auth, Completion, Delta, Endpoint, LlmError, LlmErrorKind, ProviderClient,
    ToolSchema, Turn, UserBlock, Wire,
};

/// Produces the system prompt (preamble) on demand. Called once per user turn
/// so the prompt is rebuilt per session rather than baked once at startup —
/// the gateway is a long-lived process, so a baked prompt would freeze the
/// volatile tier (date) at boot. The factory's output is day-precision, so it
/// stays byte-identical across turns within a day (upstream prompt cache stays
/// warm) and self-heals across midnight.
pub type PreambleFn = Arc<dyn Fn() -> String + Send + Sync>;

/// Stand-in for a provider whose API key is missing (see [`build_llm`]):
/// construction always succeeds so a fresh install boots, and every call —
/// `begin_turn` inherits the default one-shot driver over `complete` — fails
/// with the fix. The error text reaches the user as the turn's reply.
struct UnconfiguredLlm {
    message: String,
}

#[async_trait]
impl LlmClient for UnconfiguredLlm {
    async fn complete(&self, _session: &Session) -> anyhow::Result<String> {
        anyhow::bail!("{}", self.message)
    }
}

/// A [`LlmClient`] over one provider, via komo's own provider layer
/// (`infra::provider`).
///
/// komo owns the tool loop (`run_agent_loop`), so what this needs from a
/// provider is exactly one completion per call. Everything a client library used
/// to hold for us — the model handle, the preamble, the tool schemas — is a plain
/// field here, and switching model within a provider is a `String` swap rather
/// than minting a new typed handle.
pub struct ProviderLlm {
    client: Arc<ProviderClient>,
    /// The catalog whose schemas are advertised to the provider. Only the
    /// *declaration* goes over the wire: komo dispatches every requested call
    /// itself in `ToolExecutor::execute_round`.
    ///
    /// Read per turn rather than copied once at wiring, so a tool mounted while
    /// the process runs actually reaches the model. Rendering is name-sorted
    /// and therefore byte-stable for an unchanged set — mounting something is
    /// what costs the provider's cached prefix, not re-reading the catalog.
    /// `None` for a tool-less backend (aux, delegate, reviewer).
    tools: Option<Arc<ToolCatalog>>,
    /// The configured model: what a session with no override runs on.
    default_model: String,
    /// Which provider this is, for mapping a session's reasoning-effort level
    /// onto request params (see [`reasoning_params`]).
    provider: Provider,
    /// Prompt-cache family this backend's turns belong to, when it is not the
    /// session (see [`ProviderLlm::model_for`]). `None` — the main agent — keys
    /// the cache by session id; a backend whose sessions are one-shot but whose
    /// prompt prefix is always the same names its family here so those turns
    /// share one warm prefix instead of each cold-starting.
    cache_family: Option<String>,
    /// Rebuilds the system prompt each turn (see [`PreambleFn`]).
    preamble: PreambleFn,
    /// Max prior messages replayed as history per turn (config
    /// `max_history_messages`; `0` = unlimited). The backstop against a
    /// long-lived chat session sending its entire transcript every turn — see
    /// [`ProviderLlm::assemble`].
    max_history_messages: usize,
    /// Byte budget for the replayed history (`0` = unlimited). The message-count
    /// window alone can't bound context: a handful of pasted logs or diffs blows
    /// past any token limit while sitting well inside the count. Applied after the
    /// count window, trimming from the oldest end.
    max_history_bytes: usize,
    /// Optional per-turn memory enrichment. `Some` only for the main agent —
    /// aux/delegate sub-agents must not be fed the user's memory library. The
    /// enricher owns the whole memory policy (selection, screening, rendering,
    /// usage tracking); this adapter only appends the finished prefix.
    enricher: Option<Arc<MemoryEnricher>>,
    /// Per-completion timeout, bounding all attempts of one round together.
    /// `None` = no timeout (config `llm_timeout_secs = 0`).
    timeout: Option<Duration>,
}

/// Total attempts for one model round-trip whose failure classifies as transient
/// (1 initial + retries). A constant rather than config, for the same reason the
/// tool executor's is: transient retry is an internal robustness backstop.
const LLM_RETRY_MAX_ATTEMPTS: usize = 4;
/// Local backoff before each retry, indexed by retry number (last entry reused).
///
/// A *fallback*: when the provider tells us how long to wait
/// ([`LlmError::retry_after`]) that always wins, because a server reporting when
/// its limit clears is more accurate than any table. This covers the failures
/// that carry no such hint — connection resets, 5xx, a stalled stream — and is
/// sized for the one that does not: a rate limit with no `Retry-After` takes
/// seconds to tens of seconds to clear, so a quarter-second-then-two-seconds
/// table would run out its attempts while the limit was still in force.
///
/// Total backoff (21s) stays well inside the round's `llm_timeout_secs` budget,
/// which bounds all attempts together (see [`with_retry`]).
const LLM_RETRY_BACKOFF_MS: [u64; 3] = [1_000, 5_000, 15_000];

/// Re-run `attempt` while its failure is retryable, bounded by
/// [`LLM_RETRY_MAX_ATTEMPTS`].
///
/// Retryability is the error's own answer ([`LlmError::is_retryable`]) rather
/// than a guess from its text, and the delay is the server's when it gave one.
/// A completion has no side effect that could double-apply, so re-sending is
/// safe by construction.
///
/// Deliberately nested *inside* [`with_timeout`] by every caller: the configured
/// timeout is then a budget for the whole round (attempts included), so retrying
/// can't multiply a turn's worst-case latency.
async fn with_retry<F, Fut, T>(mut attempt: F) -> Result<T, LlmError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, LlmError>>,
{
    let mut retries = 0usize;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        if retries + 1 >= LLM_RETRY_MAX_ATTEMPTS || !error.is_retryable() {
            return Err(error);
        }
        // The provider's own answer beats the table. This is the whole point of
        // carrying `retry_after` on the error: under a real rate limit the
        // server knows when it clears and we do not.
        let delay = error.retry_after.unwrap_or_else(|| {
            Duration::from_millis(LLM_RETRY_BACKOFF_MS[retries.min(LLM_RETRY_BACKOFF_MS.len() - 1)])
        });
        tracing::warn!(
            attempt = retries + 1,
            delay_ms = delay.as_millis(),
            kind = ?error.kind,
            server_paced = error.retry_after.is_some(),
            error = %error,
            "retryable LLM failure; retrying the completion"
        );
        tokio::time::sleep(delay).await;
        retries += 1;
    }
}

/// Run `fut` under `timeout` (if set), turning a stall into a clean error rather
/// than an indefinite await. Wraps [`with_retry`], so the budget covers every
/// attempt of one round rather than each attempt separately.
async fn with_timeout<F, T>(timeout: Option<Duration>, fut: F) -> Result<T, LlmError>
where
    F: Future<Output = Result<T, LlmError>>,
{
    match timeout {
        Some(d) => match tokio::time::timeout(d, fut).await {
            Ok(result) => result,
            Err(_) => Err(LlmError::new(
                LlmErrorKind::Timeout,
                format!(
                    "LLM completion timed out after {}s (provider unresponsive; \
                     failing the turn instead of leaving it running — raise \
                     `llm_timeout_secs` / `KOMO_LLM_TIMEOUT_SECS` if this is too tight)",
                    d.as_secs()
                ),
            )),
        },
        None => fut.await,
    }
}

/// Cross-provider dispatcher: one backend per provider, selected by the
/// session's model id.
///
/// A qualified id (`deepseek:deepseek-chat`) picks the backend here; the bare
/// remainder picks the model inside it.
///
/// An unqualified id — or one naming a provider this gateway has no client for —
/// falls through to the default backend rather than failing the turn: the api
/// channel already validates a client's choice against the advertised menu, so
/// reaching here with something unroutable means config changed under a stored
/// session, and running on the default is the recoverable answer.
struct RoutingLlm {
    by_provider: Vec<(Provider, Arc<dyn LlmClient>)>,
    default_provider: Provider,
}

impl RoutingLlm {
    fn route(&self, session: &Session) -> &Arc<dyn LlmClient> {
        let wanted = session
            .model_override()
            .and_then(|id| split_model_id(id).0)
            .unwrap_or(self.default_provider);
        self.backend(wanted)
            .or_else(|| self.backend(self.default_provider))
            .expect("routing llm always holds its default provider's backend")
    }

    fn backend(&self, provider: Provider) -> Option<&Arc<dyn LlmClient>> {
        self.by_provider
            .iter()
            .find(|(p, _)| *p == provider)
            .map(|(_, backend)| backend)
    }
}

#[async_trait]
impl LlmClient for RoutingLlm {
    async fn complete(&self, session: &Session) -> anyhow::Result<String> {
        self.route(session).complete(session).await
    }

    async fn begin_turn(
        &self,
        session: &Session,
        deltas: Option<Arc<dyn DeltaSink>>,
        recorder: Option<Arc<dyn TurnRecorder>>,
    ) -> anyhow::Result<Box<dyn TurnDriver>> {
        self.route(session)
            .begin_turn(session, deltas, recorder)
            .await
    }

    async fn resume_turn(
        &self,
        session: &Session,
        events: &[SessionEvent],
        turn_id: &str,
        deltas: Option<Arc<dyn DeltaSink>>,
        recorder: Option<Arc<dyn TurnRecorder>>,
    ) -> anyhow::Result<Box<dyn TurnDriver>> {
        // Route on the provider the *interrupted turn* recorded, not the
        // session's current model override: continuing on a different backend
        // would replay one provider's opaque state (reasoning blobs, item ids)
        // into another. No backend for that provider anymore ⇒ error out, and
        // the caller falls back to the digest-primed fresh turn.
        let last_seq = events.last().map(|e| e.seq).unwrap_or(0);
        let header = fold_request_header(events, last_seq)
            .context("the interrupted turn recorded no request header")?;
        let provider = Provider::parse(&header.provider)?;
        let backend = self
            .backend(provider)
            .with_context(|| format!("no configured backend for provider `{}`", header.provider))?;
        backend
            .resume_turn(session, events, turn_id, deltas, recorder)
            .await
    }
}

/// Extra answer budget granted on top of an Anthropic thinking budget, so the
/// model has room to write a reply after it finishes reasoning.
const THINKING_ANSWER_HEADROOM: u64 = 8_192;

/// Map a reasoning-effort level onto the provider's request params, or `None`
/// when this provider/level pair has no effect.
///
/// Which levels a provider offers is [`Provider::efforts`]; this is the other
/// half — how a level is actually spelled on the wire.
fn reasoning_params(provider: Provider, effort: &str) -> Option<Value> {
    let level = match effort.trim() {
        level @ ("low" | "medium" | "high") => level,
        _ => return None,
    };
    match provider {
        // Every Responses-API provider takes `reasoning.effort` verbatim.
        Provider::OpenAi | Provider::OpenRouter | Provider::Codex => {
            Some(json!({ "reasoning": { "effort": level } }))
        }
        // Anthropic has no effort scale — it budgets thinking in tokens, so the
        // levels map onto budgets. The caller must also raise `max_tokens` above
        // the budget (thinking is charged against it): see `model_for`.
        Provider::Anthropic => {
            let budget = match level {
                "low" => 4_096,
                "medium" => 10_240,
                _ => 24_576,
            };
            Some(json!({ "thinking": { "type": "enabled", "budget_tokens": budget } }))
        }
        // Only a thinking on/off flag; see `Provider::efforts`.
        Provider::DeepSeek => None,
    }
}

/// Shallow-merge `extra`'s top-level keys into `base` (extra wins).
fn merge_params(base: Option<Value>, extra: Value) -> Value {
    match (base, extra) {
        (Some(Value::Object(mut base)), Value::Object(extra)) => {
            base.extend(extra);
            Value::Object(base)
        }
        (_, extra) => extra,
    }
}

impl ProviderLlm {
    /// This turn's tool declarations, rendered from the shared catalog.
    ///
    /// Name-sorted (the catalog is), so the block is byte-identical between
    /// turns whose tool set did not change — which is what keeps the provider's
    /// cached prefix valid across a conversation. Read per turn rather than
    /// copied at wiring, so a tool mounted while the process runs is one the
    /// model can actually see.
    fn tool_schemas(&self) -> Vec<ToolSchema> {
        let Some(catalog) = &self.tools else {
            return Vec::new();
        };
        catalog
            .snapshot()
            .tools()
            .map(|tool| ToolSchema {
                name: tool.name().to_string(),
                description: tool.description().to_string(),
                parameters: tool.parameters_schema(),
            })
            .collect()
    }

    /// Assemble this turn's `(preamble, prompt, history)`: split the session
    /// into the latest user prompt + prior history, rebuild the system prompt,
    /// and inject the memory blocks (main agent only) — pinned into the
    /// preamble, recall in front of the prompt (see below for why they split).
    /// Run once per turn — never per tool-loop round (recall is keyed on the
    /// user message, and re-running it each round would churn the prompt).
    ///
    /// # Invariant: a stored message renders the same bytes forever
    ///
    /// komo does not store provider-shaped history (that is what lets a session
    /// switch models, even across providers, mid-conversation) — it re-renders
    /// the transcript every turn. The price of that freedom is this rule:
    /// **rendering a message must be a pure function of that message's stored
    /// fields.** Nothing may depend on how far it sits from the end of the
    /// window, or on anything else that moves as the conversation grows.
    ///
    /// Break it and the provider prefix cache dies quietly: a message whose
    /// bytes change is a divergence point, and everything after it is recomputed
    /// on every request for the rest of the turn (see [`to_turn`], which used to
    /// break exactly this way). The two places that legitimately vary per turn —
    /// where the window *starts*, and the memory blocks — are handled so that
    /// they don't rewrite anything: the cut snaps to a content-derived anchor
    /// ([`window_history`]), and recall is appended at the tail rather than
    /// folded into the prefix (below).
    async fn assemble(
        &self,
        session: &Session,
    ) -> anyhow::Result<(String, String, Vec<Turn>, RecalledMemories)> {
        // The current prompt is the most recent user message; everything before
        // it forms the conversation history sent to the model.
        let last_user_idx = session
            .messages
            .iter()
            .rposition(|m| m.role == Role::User)
            .context("no user message to respond to")?;
        let prompt = session.messages[last_user_idx].content.clone();

        // Window the replayed history to the most recent `max_history_messages`
        // (0 = keep everything). Without this a long-lived chat session
        // (telegram/feishu/wechat are keyed by chat id and only rotate on an
        // explicit `/new`) would resend its entire transcript every turn —
        // unbounded token cost and latency, eventually overflowing the context
        // window. The stable system-prompt + memory prefix is untouched, so the
        // upstream prompt cache is unaffected by trimming the tail.
        let window = window_history(
            &session.messages[..last_user_idx],
            self.max_history_messages,
            self.max_history_bytes,
        );
        let history: Vec<Turn> = window.iter().flat_map(to_turns).collect();

        // Rebuild the system prompt for this turn. It rides on the per-turn
        // request rather than on shared state, so concurrent sessions in the
        // gateway stay independent.
        let mut preamble = (self.preamble)();

        // Memory injection (main agent only). The two tiers land in different
        // places, and the split is what keeps the provider prompt cache warm:
        // pinned is cross-turn stable so it may join the system prompt, but
        // recall is keyed on this turn's user message — putting it in the
        // system prompt would rewrite the cached prefix every turn and
        // invalidate everything after it (which is exactly what it used to
        // do). Recall rides in front of the turn's user message instead: new
        // bytes were arriving at the tail anyway, so it costs the cache
        // nothing. Render-time only — the stored transcript keeps the user's
        // raw words. Enrichment failure is absorbed inside the enricher
        // (memory is background context — it must never fail a reply).
        let mut prompt = prompt;
        let mut memories = RecalledMemories::default();
        if let Some(enricher) = &self.enricher
            && let Some(injection) = enricher
                .enrich(session, &prompt, &session.messages[..last_user_idx])
                .await
        {
            if let Some(pinned) = injection.pinned {
                preamble.push_str("\n\n");
                preamble.push_str(&pinned);
            }
            if let Some(recall) = injection.recall {
                prompt = format!("{recall}\n\n{prompt}");
            }
            memories = injection.used;
        }

        Ok((preamble, prompt, history, memories))
    }

    /// Resolve this turn's model settings: the assembled preamble, then the
    /// session's own model / reasoning-effort choices.
    ///
    /// Only the *main* agent is ever handed a stored session: every aux path
    /// (reviewer, delegate, recall screening, sweeps) builds a synthetic
    /// `Session`, whose overrides are empty. That is what keeps a conversation's
    /// model choice from leaking onto the aux model.
    fn model_for(&self, preamble: String, session: &Session) -> TurnModel {
        // A session's model may be provider-qualified (`deepseek:deepseek-chat`).
        // Routing on the prefix is `RoutingLlm`'s job — by the time we get here
        // the provider is already decided, so only the bare id matters.
        let model = session
            .model_override()
            .map(|id| split_model_id(id).1.to_string())
            .unwrap_or_else(|| self.default_model.clone());
        let mut turn = TurnModel {
            model,
            preamble,
            extra: None,
        };

        // The Responses API caches by prefix automatically, but shard routing is
        // best-effort; `prompt_cache_key` pins related requests to the same
        // cache shard — the Codex CLI itself sends its session id here for the
        // same reason.
        //
        // What the key must identify is the *prefix family*, not the
        // conversation: two requests share a cache only if their
        // system-prompt + tool-definition bytes match. For the main agent the
        // two coincide (one conversation = one prefix), so the session id is
        // right. For a backend whose sessions are one-shot — delegate
        // (`delegate:<uuid>`), cron (`cron:<name>:<ts>`), briefing — the
        // session id is a *different* key every time even though every one of
        // those turns opens with identical bytes, so each would cold-start.
        // Those backends declare a family at wiring instead. Anchoring them on
        // the *parent's* key would be the other mistake: a frequent side query
        // would evict the conversation's own prefix.
        //
        // Anthropic has no such parameter (it caches from explicit
        // `cache_control` breakpoints, which `provider::messages` marks) and
        // rejects unknown request fields, so it is excluded.
        if self.client.wire == Wire::Responses {
            turn.extra = Some(merge_params(
                turn.extra.take(),
                json!({
                    "prompt_cache_key": cache_key(self.cache_family.as_deref(), &session.id)
                }),
            ));
        }

        if let Some(params) = session
            .effort_override()
            .and_then(|effort| reasoning_params(self.provider, effort))
        {
            // Anthropic charges thinking against `max_tokens`, so a budget above
            // the cap is rejected outright — raise the cap to clear it.
            if let Some(budget) = params
                .get("thinking")
                .and_then(|thinking| thinking.get("budget_tokens"))
                .and_then(Value::as_u64)
            {
                turn.extra = Some(merge_params(
                    turn.extra.take(),
                    json!({ "max_tokens": budget + THINKING_ANSWER_HEADROOM }),
                ));
            }
            turn.extra = Some(merge_params(turn.extra.take(), params));
        }
        turn
    }
}

/// One turn's model settings: which model runs it, the system prompt assembled
/// for it, and the request knobs the session's reasoning-effort choice implies.
///
/// Requests are built off this round by round, so a round is exactly one
/// provider completion and komo's loop stays in charge of what happens between
/// rounds.
struct TurnModel {
    model: String,
    preamble: String,
    /// Extra top-level request fields, merged over the codec's defaults.
    extra: Option<Value>,
}

#[async_trait]
impl LlmClient for ProviderLlm {
    async fn complete(&self, session: &Session) -> anyhow::Result<String> {
        // Tool-less by contract: this is the single-shot path for aux callers
        // (reviewer / recall screening / briefing fallback), and it advertises no
        // tools at all — nothing here would dispatch a call the model made, so it
        // must not be able to ask for one. One completion is the whole answer.
        let (preamble, prompt, history, _) = self.assemble(session).await?;
        let turn = self.model_for(preamble, session);
        let mut history = history;
        history.push(Turn::user(prompt));
        let completion = with_timeout(
            self.timeout,
            with_retry(|| {
                self.client.complete(
                    &turn.model,
                    &turn.preamble,
                    &history,
                    &[],
                    turn.extra.as_ref(),
                    // An aux completion has no watcher by construction — it is a
                    // side query on a synthetic session, not the conversation.
                    None,
                )
            }),
        )
        .await?;
        Ok(completion.text())
    }

    async fn begin_turn(
        &self,
        session: &Session,
        deltas: Option<Arc<dyn DeltaSink>>,
        recorder: Option<Arc<dyn TurnRecorder>>,
    ) -> anyhow::Result<Box<dyn TurnDriver>> {
        let (preamble, prompt, history, memories) = self.assemble(session).await?;
        let turn_loop = TurnLoop {
            client: self.client.clone(),
            turn: self.model_for(preamble, session),
            // Taken once here, then re-sent unchanged every round: a turn
            // declares one set of tools from its first round to its last.
            tools: self.tool_schemas(),
            history,
            start: TurnStart::Prompt(Turn::user(prompt)),
            recorder,
            provider_name: self.provider.name(),
            timeout: self.timeout,
            usage: TokenUsage::default(),
            memories,
            degraded: false,
            replayed: Vec::new(),
            deltas,
            rounds: 0,
        };
        turn_loop.record_header(HeaderReason::Initial).await;
        Ok(Box::new(turn_loop))
    }

    async fn resume_turn(
        &self,
        session: &Session,
        events: &[SessionEvent],
        turn_id: &str,
        deltas: Option<Arc<dyn DeltaSink>>,
        recorder: Option<Arc<dyn TurnRecorder>>,
    ) -> anyhow::Result<Box<dyn TurnDriver>> {
        // Which tools recovery may simply run again. Read off the same catalog
        // the round was dispatched from, so a tool that stops being idempotent
        // stops being replayed with it.
        let catalog = self.tools.as_ref().map(|tools| tools.snapshot());
        let idempotent = |name: &str| {
            catalog
                .as_ref()
                .and_then(|catalog| catalog.get(name))
                .is_some_and(|tool| tool.idempotent())
        };
        let rebuilt = rebuild_from_events(session, events, turn_id, &idempotent)?;
        let turn_loop = TurnLoop {
            client: self.client.clone(),
            turn: TurnModel {
                model: rebuilt.model,
                preamble: rebuilt.preamble,
                extra: rebuilt.extra,
            },
            tools: self.tool_schemas(),
            history: rebuilt.history,
            start: rebuilt.start,
            recorder,
            provider_name: self.provider.name(),
            timeout: self.timeout,
            usage: TokenUsage::default(),
            // A resumed turn reopens the recorded prompt rather than
            // assembling one, so there is no fresh enrichment to report; the
            // interrupted run's row already holds what was injected.
            memories: RecalledMemories::default(),
            degraded: false,
            replayed: Vec::new(),
            deltas,
            rounds: 0,
        };
        // The continuation journals itself from its rebuilt state, so a second
        // interruption resumes from here rather than replaying this rebuild.
        turn_loop.record_header(HeaderReason::Resume).await;
        Ok(Box::new(turn_loop))
    }
}

/// An interrupted turn, rebuilt from its journal: the model settings and
/// history to reopen with, and how to pick up (see [`TurnStart`]).
struct RebuiltTurn {
    model: String,
    preamble: String,
    extra: Option<Value>,
    history: Vec<Turn>,
    start: TurnStart,
}

/// Rebuild the state an interrupted turn died with, from its session's events.
///
/// The history is **derived**, not stored: `session.messages` is the
/// conversation projected out of the same log, and resume's own precondition is
/// that it still ends on the interrupted turn's user message. Only what a later
/// request cannot re-derive — the model settings and the rendered prompt — was
/// snapshotted, in `request/header`.
///
/// Rounds are replayed in `seq` order, and each round's **results are ordered by
/// its own recorded blocks**, never by the seq their settle landed on: a round
/// runs concurrently, so settle order is completion order, and rebuilding in it
/// would hand the provider a different request than the live turn sent.
///
/// `idempotent` answers whether a tool may simply be run again — the newest
/// round's unsettled calls are re-dispatched through it rather than reported as
/// lost (see [`TurnStart::Replay`]).
///
/// A turn resumed **twice** is rebuilt from every attempt at it, not just the
/// last one: each continuation is its own turn in the log, so the rounds a
/// second crash has to replay are spread across the chain, and reading only the
/// newest id would drop the work the first attempt already paid for and answer
/// the question from scratch. The chain is walked through
/// `turn/started{resumed_from}`.
fn rebuild_from_events(
    session: &Session,
    events: &[SessionEvent],
    turn_id: &str,
    idempotent: &dyn Fn(&str) -> bool,
) -> anyhow::Result<RebuiltTurn> {
    let last_seq = events.last().map(|e| e.seq).unwrap_or(0);
    let header = fold_request_header(events, last_seq)
        .context("the interrupted turn recorded no request header")?
        .clone();

    let mut history: Vec<Turn> = session.messages.iter().flat_map(to_turns).collect();

    let attempts = komo_core::domain::session_event::attempt_chain(events, turn_id);

    // One round's calls: what was dispatched, and what came back.
    struct Round {
        id: String,
        blocks: Vec<AssistantBlock>,
        settled: std::collections::HashMap<String, String>,
    }
    let mut rounds: Vec<Round> = Vec::new();
    // Calls that reached the approval gate. A durable `approval/requested` with
    // no settle beside it **proves the tool body never ran** — that is the whole
    // point of writing it before the wait — so such a call is re-dispatched on
    // the way back regardless of whether it is idempotent. Without this, a turn
    // that stopped for approval on a `shell` would come back and tell the model
    // the command may or may not have landed, which is the one thing the barrier
    // exists to rule out.
    let mut gated: std::collections::HashSet<String> = std::collections::HashSet::new();
    for event in events
        .iter()
        .filter(|e| e.turn_id_of_work().is_some_and(|id| attempts.contains(id)))
    {
        match &event.kind {
            SessionEventKind::ApprovalRequested(approval) => {
                gated.insert(approval.call_id.clone());
            }
            // A call that stopped to wait did not happen either — the tool
            // asked to be woken instead of running (`wait`, `ask_user`). Same
            // reading, same unconditional re-dispatch: idempotency has nothing
            // to say about a body that never ran.
            SessionEventKind::TurnSuspended(suspended) if !suspended.call_id.is_empty() => {
                gated.insert(suspended.call_id.clone());
            }
            SessionEventKind::AssistantRound(round) => rounds.push(Round {
                id: round.response_id.clone(),
                blocks: serde_json::from_value(round.blocks.clone())
                    .context("parsing a recorded assistant round")?,
                settled: std::collections::HashMap::new(),
            }),
            SessionEventKind::ToolCallSettled(call) => {
                if let Some(round) = rounds.last_mut() {
                    let text = if call.error.is_empty() {
                        call.result.clone()
                    } else {
                        call.error.clone()
                    };
                    round.settled.insert(call.call_id.clone(), text);
                }
            }
            SessionEventKind::UserMessage(m) if m.source == MessageSource::Injected => {
                // Recorded when it entered history mid-turn; it belongs after
                // the results of the round it interrupted.
                if let Some(Turn::User(blocks)) = history.last_mut() {
                    blocks.push(UserBlock::Text(format!(
                        "{INTERJECTION_PREFIX}{}",
                        m.content
                    )));
                }
            }
            _ => {}
        }
    }

    let mut lost_calls = false;
    let mut replay: Option<(Vec<ToolCallReq>, Vec<ReplaySlot>)> = None;
    let last = rounds.len().saturating_sub(1);
    for (n, round) in rounds.iter().enumerate() {
        history.push(Turn::Assistant {
            id: (!round.id.is_empty()).then(|| round.id.clone()),
            blocks: round.blocks.clone(),
        });
        // The round's own blocks are the call list, not its `tool/call-started`
        // events: the blocks are what the model sent — verbatim, in provider
        // order, with unredacted arguments — so a call rebuilt from them is the
        // call the live turn made. The started events are the ledger's redacted
        // copy, which is the wrong thing to re-issue.
        let Step::ToolCalls { calls, .. } = blocks_to_step(&round.blocks) else {
            continue;
        };
        let mut slots: Vec<ReplaySlot> = Vec::new();
        let mut rerun = Vec::new();
        for call in calls {
            let key = call.call_id.clone().unwrap_or_else(|| call.id.clone());
            let known = match round.settled.get(&key) {
                Some(text) => Some(text.clone()),
                // Unsettled. Only the newest round is still in flight — an
                // earlier round's results must have been sent or the round
                // after it would not exist — and only an idempotent tool may
                // simply be run again. Anything else is the model's call to
                // make, so it gets told.
                // Never started: either the gate holds it (see `gated`), or it
                // is the newest round's call and the tool may simply be run
                // again.
                None if n == last && (gated.contains(&key) || idempotent(&call.name)) => {
                    rerun.push(call);
                    None
                }
                None => {
                    lost_calls = true;
                    Some(INTERRUPTED_RESULT_NOTE.clone())
                }
            };
            slots.push((
                key.clone(),
                known.map(|text| UserBlock::ToolResult {
                    id: key.clone(),
                    call_id: Some(key),
                    text,
                }),
            ));
        }
        if rerun.is_empty() {
            history.push(Turn::User(
                slots.into_iter().filter_map(|(_, block)| block).collect(),
            ));
        } else {
            // Held back: this round's results message is only complete once the
            // replayed calls have answered, and it is `step` that assembles it.
            replay = Some((rerun, slots));
        }
    }

    // Where did the interruption land? A round whose calls all settled and that
    // produced no further round is a turn that had answered and only failed to
    // land it; anything else continues.
    let start = if let Some((calls, slots)) = replay {
        TurnStart::Replay { calls, slots }
    } else {
        match history.last() {
            None => anyhow::bail!("nothing to resume: the turn recorded no history"),
            Some(Turn::Assistant { blocks, .. }) if !lost_calls => {
                let text = blocks
                    .iter()
                    .filter_map(|block| match block {
                        AssistantBlock::Text(t) => Some(t.as_str()),
                        _ => None,
                    })
                    .collect::<String>();
                anyhow::ensure!(
                    !text.trim().is_empty(),
                    "interrupted turn ended on an empty assistant round"
                );
                TurnStart::Final(text)
            }
            _ => TurnStart::Continue,
        }
    };

    Ok(RebuiltTurn {
        model: header.model,
        preamble: header.system,
        extra: header.extra,
        history,
        start,
    })
}

/// Marks a mid-turn user message inside the tool-results message, so the model
/// can tell "the human just told me something" from tool output. Without it the
/// text sits next to a pile of results and reads as more data.
const INTERJECTION_PREFIX: &str = "The user sent this while you were working — \
     take it into account before your next step:\n";

/// What a tool call whose result was lost to the interruption gets fed back as
/// on resume. This is the answer for a tool that is *not* idempotent — one that
/// is gets re-dispatched instead ([`TurnStart::Replay`]). A mutation cannot be
/// assumed repeatable, so whether to re-issue it stays the model's decision.
static INTERRUPTED_RESULT_NOTE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    format!(
        "[This call's result was lost when the process was interrupted before it finished. {}]",
        komo_core::domain::tool::UNCERTAIN_OUTCOME_ADVICE
    )
});

/// One call's place in a replayed round's results message: `None` is the hole a
/// re-dispatched call fills, `Some` a result the interrupted process already had.
type ReplaySlot = (String, Option<UserBlock>);

/// Close a replayed round: drop what just ran into the holes the interrupted
/// process left, in the order the model issued the calls.
///
/// Order is the point. The results could be concatenated in any order and still
/// answer every call, but the round's message would then differ from the one the
/// live turn was assembling — and `rebuild == live` is what lets a second
/// interruption resume from here, and what keeps the provider's cached prefix.
fn fill_replay_slots(slots: Vec<ReplaySlot>, mut fresh: Vec<UserBlock>) -> Vec<UserBlock> {
    let mut blocks = Vec::with_capacity(slots.len());
    for (id, known) in slots {
        if let Some(block) = known {
            blocks.push(block);
            continue;
        }
        let at = fresh.iter().position(|block| match block {
            UserBlock::ToolResult { id: got, .. } => *got == id,
            _ => false,
        });
        match at {
            Some(at) => blocks.push(fresh.remove(at)),
            // The executor answers every call it is handed, so this is
            // unreachable — but an open hole is the one shape a provider
            // rejects outright, so it is filled rather than left.
            None => blocks.push(UserBlock::ToolResult {
                id: id.clone(),
                call_id: Some(id),
                text: INTERRUPTED_RESULT_NOTE.clone(),
            }),
        }
    }
    // A result that matched no slot would otherwise be dropped, and a dropped
    // tool result is the other shape a provider rejects.
    blocks.append(&mut fresh);
    blocks
}

/// How a [`TurnLoop`] opens: a fresh turn sends its prompt; a resumed one
/// picks up from whatever state the journal ended in.
enum TurnStart {
    /// Fresh turn — push the opening prompt, then complete.
    Prompt(Turn),
    /// Resumed with the history already ending on a user turn (the journal's
    /// last row was tool results, or the envelope's own prompt) — complete
    /// over it as it stands.
    Continue,
    /// Resumed past the finish line: the interrupted turn had already produced
    /// its final answer, it just never reached the transcript. No request at
    /// all — hand the answer back.
    Final(String),
    /// Resumed into a round that was still running: `calls` never settled and
    /// their tools are idempotent, so they are simply re-dispatched instead of
    /// costing the model a round to be told their results were lost.
    ///
    /// `slots` is the round's results message with a hole where each of those
    /// calls goes — held back because a round sends *one* results message, and
    /// `step` can only assemble it once the re-dispatched calls have answered.
    /// Keeping the holes in place is what makes the message it finally sends
    /// byte-identical to the one the interrupted turn was building.
    Replay {
        calls: Vec<ToolCallReq>,
        slots: Vec<ReplaySlot>,
    },
    /// `first()` already ran.
    Started,
}

/// A [`TurnDriver`] over a per-turn [`TurnModel`]. Holds the growing conversation
/// history so each round is a single provider completion — one round-trip per
/// round, komo owns the loop.
struct TurnLoop {
    client: Arc<ProviderClient>,
    turn: TurnModel,
    /// Tool schemas re-sent every round (see [`ProviderLlm::tools`]).
    tools: Vec<ToolSchema>,
    history: Vec<Turn>,
    /// The opening move; consumed by `first()`, then [`TurnStart::Started`].
    start: TurnStart,
    /// Journal for this turn's provider-level state, written in lockstep with
    /// `history` (envelope at construction, one row per round-trip and one per
    /// results feed-back). `None` — every aux path — journals nothing.
    recorder: Option<Arc<dyn TurnRecorder>>,
    /// This backend's provider, as recorded in the journal envelope.
    provider_name: &'static str,
    /// Per-round completion timeout (see [`ProviderLlm::timeout`]).
    timeout: Option<Duration>,
    /// Tokens spent so far this turn, summed over rounds; read by the runtime for
    /// the ledger once the turn ends.
    usage: TokenUsage,
    /// The memories prompt assembly injected, carried to the ledger at turn end
    /// (see `TurnDriver::memories`). Set once when the turn starts and never
    /// touched again — a resumed turn assembles nothing, so it reports none.
    memories: RecalledMemories,
    /// Whether this turn already spent its one context-overflow degrade (see
    /// [`TurnLoop::degrade_for_overflow`]). Once used, a second overflow
    /// fails the turn: the first degrade is a real reclaim, so if the request
    /// is *still* too large the shortfall is structural and retrying only burns
    /// another round-trip on a request that cannot fit.
    degraded: bool,
    /// A resumed round's results message under construction — see
    /// [`TurnStart::Replay`]. Empty for every turn but one resumed into a round
    /// that was still running.
    replayed: Vec<ReplaySlot>,
    /// Where to stream this turn's output as it is produced. `None` when nothing
    /// is watching, which is most turns — and then no per-chunk work happens at
    /// all.
    deltas: Option<Arc<dyn DeltaSink>>,
    /// Model round-trips this turn has made, for the per-round token log.
    rounds: usize,
}

/// Bytes of a tool result kept when a turn is degraded for overflow — the head
/// and tail of that size each, on the reasoning that a result's beginning
/// (what it is) and end (how it concluded) carry most of the signal. The full
/// text is already on disk in the tool-output store when it mattered, so this
/// discards a copy, not the only copy.
const OVERFLOW_TOOL_RESULT_KEEP: usize = 4 * 1024;

impl TurnLoop {
    /// Record this turn's events. Best-effort by contract: recording buys
    /// resumability, and a broken store must cost exactly that.
    async fn record(&self, kinds: Vec<SessionEventKind>) {
        if let Some(recorder) = &self.recorder {
            recorder.record(kinds).await;
        }
    }

    fn turn_id(&self) -> String {
        self.recorder
            .as_ref()
            .map(|r| r.turn_id().to_string())
            .unwrap_or_default()
    }

    /// Record the request envelope — model settings, the rendered system
    /// prompt, the assembled tool schemas.
    ///
    /// Deliberately **not** the history: that is derived from the session's own
    /// events, so copying it here would put a rendered prompt and every tool
    /// schema into the log once per round. Only the parts a later request
    /// cannot re-derive are stored, and only when they change — an unchanged
    /// envelope is inherited (`header_snapshot_reason`).
    ///
    /// A resumed turn always writes one, identical or not: the boundary between
    /// the interrupted loop and the one that picked it up has to be visible in
    /// the log rather than inferred from a gap.
    async fn record_header(&self, reason: HeaderReason) {
        self.record(vec![SessionEventKind::RequestHeader(RequestHeaderEvent {
            reason,
            provider: self.provider_name.to_string(),
            model: self.turn.model.clone(),
            effort: String::new(),
            system: self.turn.preamble.clone(),
            tools: self.tools.iter().map(|t| t.name.clone()).collect(),
            extra: self.turn.extra.clone(),
        })])
        .await;
    }

    /// Send one round-trip: complete over `history`, then commit the assistant
    /// turn (verbatim — text + tool calls + reasoning together) to history so the
    /// next round sees a provider-correct transcript.
    ///
    /// Committing reasoning verbatim is what carries a reasoning model's chain of
    /// thought across the tool loop: the provider hands back an opaque blob, and
    /// echoing it into the next request is the only way the model picks up where
    /// it left off instead of re-deriving its plan every round.
    async fn run(&mut self, prompt: Turn) -> anyhow::Result<Step> {
        self.history.push(prompt);
        self.complete_committed().await
    }

    /// The round-trip itself, over `history` as it stands (which must end on a
    /// user turn). Split from [`run`](Self::run) so a resumed turn — whose
    /// history was rebuilt already ending on a user turn — can complete without
    /// pushing anything.
    async fn complete_committed(&mut self) -> anyhow::Result<Step> {
        let completion = match self.complete_round().await {
            Ok(completion) => completion,
            // Overflowing the context window is not transient — `with_retry`
            // correctly refuses to re-send the same oversized request — but it
            // is recoverable, because most of what fills a long turn is tool
            // output the model has already read once. Reclaim that and try the
            // round again rather than losing every round of work before it.
            Err(error) if error.is_context_overflow() && self.degrade_for_overflow() => {
                tracing::warn!(
                    "context window exceeded; retrying this round on a degraded history"
                );
                self.complete_round().await?
            }
            Err(error) => return Err(error.into()),
        };

        self.usage.add(TokenUsage {
            input: completion.usage.input,
            output: completion.usage.output,
            cached_input: completion.usage.cached_input,
        });
        self.rounds += 1;
        // Per-round token accounting, which is the only honest way to tune the
        // context knobs: `max_history_bytes` and `max_turn_result_bytes` are
        // *byte* budgets, and bytes are a poor proxy for tokens (CJK spends ~3
        // bytes per token, code closer to 3.5), so the caps can only be set from
        // data. `cached` is the payoff of the prefix-cache work in `assemble` —
        // a round where it stays near zero across a tool loop means the prefix is
        // being invalidated and something upstream broke the render invariant.
        tracing::debug!(
            round = self.rounds,
            input = completion.usage.input,
            output = completion.usage.output,
            cached = completion.usage.cached_input,
            turn_input = self.usage.input,
            turn_output = self.usage.output,
            "model round completed"
        );
        self.record(vec![SessionEventKind::AssistantRound(
            AssistantRoundEvent {
                turn_id: self.turn_id(),
                round: self.rounds as u32,
                response_id: completion.id.clone().unwrap_or_default(),
                blocks: serde_json::to_value(&completion.blocks).unwrap_or(Value::Null),
                tokens_in: completion.usage.input,
                tokens_out: completion.usage.output,
                tokens_cached: completion.usage.cached_input,
            },
        )])
        .await;
        let step = blocks_to_step(&completion.blocks);
        self.history.push(Turn::Assistant {
            id: completion.id,
            blocks: completion.blocks,
        });
        Ok(step)
    }

    /// One provider round-trip over the history as it currently stands. Split out
    /// of [`run`] so an overflow can re-issue the identical call against a
    /// reclaimed history.
    ///
    /// [`run`]: Self::run
    async fn complete_round(&self) -> Result<Completion, LlmError> {
        // Bridge the domain sink onto the provider layer's callback. Built per
        // round rather than held, so a retry re-streams into the same watcher
        // without the provider layer ever learning what a session is.
        let forward = self.deltas.as_ref().map(|sink| {
            let sink = sink.clone();
            move |delta: Delta<'_>| match delta {
                Delta::Text(text) => sink.text(text),
                Delta::Reasoning(text) => sink.reasoning(text),
            }
        });
        let forward = forward
            .as_ref()
            .map(|f| f as &(dyn Fn(Delta<'_>) + Send + Sync));
        with_timeout(
            self.timeout,
            with_retry(|| {
                self.client.complete(
                    &self.turn.model,
                    &self.turn.preamble,
                    &self.history,
                    &self.tools,
                    self.turn.extra.as_ref(),
                    forward,
                )
            }),
        )
        .await
    }

    /// Reclaim context after an overflow, in place, on this turn's in-memory
    /// history only — the stored transcript is never touched, so a degrade
    /// costs the model's working set for the rest of this turn and nothing
    /// afterwards.
    ///
    /// Two steps, cheapest first: shrink the tool results this turn accumulated
    /// (the usual cause — a turn that read several large files), and only if
    /// that reclaims nothing, drop the oldest half of the replayed history (the
    /// case where the turn was already too big before it made a single call).
    /// Returns whether anything was actually reclaimed; `false` means there is
    /// nothing left to give and the caller must surface the failure.
    ///
    /// At most once per turn — see [`TurnLoop::degraded`].
    fn degrade_for_overflow(&mut self) -> bool {
        if self.degraded {
            return false;
        }
        self.degraded = reclaim_context(&mut self.history);
        self.degraded
    }
}

/// Shrink `history` in place, returning whether anything was reclaimed. Free
/// function rather than a method so the policy can be tested without standing
/// up a provider client — see [`TurnLoop::degrade_for_overflow`] for what it is
/// for.
fn reclaim_context(history: &mut Vec<Turn>) -> bool {
    let mut reclaimed = false;
    for turn in history.iter_mut() {
        let Turn::User(blocks) = turn else {
            continue;
        };
        for block in blocks.iter_mut() {
            if let UserBlock::ToolResult { text, .. } = block
                && text.len() > OVERFLOW_TOOL_RESULT_KEEP * 2
            {
                *text = head_tail(text, OVERFLOW_TOOL_RESULT_KEEP);
                reclaimed = true;
            }
        }
    }
    if reclaimed {
        return true;
    }
    // Nothing bulky to shrink: the weight is in the replayed conversation
    // itself. Drop the older half, keeping the window opening on a user
    // message (a leading assistant message is rejected outright by some
    // providers).
    let cut = history.len() / 2;
    if cut == 0 {
        return false;
    }
    let mut rest = history.split_off(cut);
    // The cut can land inside a tool round, so keep peeling until the window
    // opens on real user text. A leading assistant message is rejected
    // outright by some providers, and a tool result whose function_call went
    // with the dropped half is an orphan strict providers reject the same way
    // (DeepSeek: 400). The two strips alternate because each can create the
    // other's condition: removing an assistant turn with tool calls orphans
    // the results right after it.
    while let Some(first) = rest.first_mut() {
        match first {
            Turn::Assistant { .. } => {
                rest.remove(0);
            }
            Turn::User(blocks) => {
                // Any tool result this far forward pairs with an assistant
                // turn strictly before it — dropped by the cut or by a prior
                // iteration either way. Plain text (the user's ask, or a
                // mid-turn interjection) is kept.
                blocks.retain(|block| !matches!(block, UserBlock::ToolResult { .. }));
                if blocks.is_empty() {
                    rest.remove(0);
                } else {
                    break;
                }
            }
        }
    }
    if rest.is_empty() {
        // Dropping would leave nothing to send; let the turn fail honestly
        // instead of sending an empty request.
        return false;
    }
    *history = rest;
    true
}

/// The provider cache key for a turn: the backend's declared prefix family when
/// it has one, else the session. See [`ProviderLlm::model_for`] for why the two
/// differ.
fn cache_key(family: Option<&str>, session_id: &str) -> String {
    format!("komo:{}", family.unwrap_or(session_id))
}

/// `s` shortened to its first and last `keep` bytes with a marker between,
/// cut on char boundaries. Used only for the overflow degrade.
fn head_tail(s: &str, keep: usize) -> String {
    let head_end = floor_char_boundary(s, keep);
    let tail_start = ceil_char_boundary(s, s.len() - keep);
    format!(
        "{}\n\n…[{} bytes elided to fit the context window; the full result was \
         already delivered earlier this turn]…\n\n{}",
        &s[..head_end],
        tail_start - head_end,
        &s[tail_start..]
    )
}

fn floor_char_boundary(s: &str, mut at: usize) -> usize {
    while at > 0 && !s.is_char_boundary(at) {
        at -= 1;
    }
    at
}

fn ceil_char_boundary(s: &str, mut at: usize) -> usize {
    while at < s.len() && !s.is_char_boundary(at) {
        at += 1;
    }
    at
}

#[async_trait]
impl TurnDriver for TurnLoop {
    async fn first(&mut self) -> anyhow::Result<Step> {
        match std::mem::replace(&mut self.start, TurnStart::Started) {
            TurnStart::Prompt(prompt) => self.run(prompt).await,
            TurnStart::Continue => self.complete_committed().await,
            TurnStart::Final(text) => Ok(Step::Final(text)),
            TurnStart::Replay { calls, slots } => {
                self.replayed = slots;
                // No narration: the text that went with these calls is already
                // in history on the round that issued them, and repeating it
                // would show the user the same sentence twice.
                Ok(Step::ToolCalls {
                    calls,
                    text: String::new(),
                })
            }
            TurnStart::Started => anyhow::bail!("turn driver already started"),
        }
    }

    async fn step(
        &mut self,
        results: Vec<ToolOutcome>,
        interjected: Option<String>,
    ) -> anyhow::Result<Step> {
        // One user message carrying every tool result. A komo tool's model-facing
        // result is plain text by contract (`domain::tool::ToolOutput::text`), so
        // each goes over as one text payload — no sniffing for an image or
        // multipart envelope.
        let mut blocks: Vec<UserBlock> = results
            .into_iter()
            .map(|r| UserBlock::ToolResult {
                id: r.id,
                call_id: r.call_id,
                text: r.content,
            })
            .collect();
        if !self.replayed.is_empty() {
            blocks = fill_replay_slots(std::mem::take(&mut self.replayed), blocks);
        }
        // What the user said while this round ran, appended to the same user
        // message as a plain text block — after the results, so the model reads
        // the outcome first and the new instruction last (the position it acts
        // on). Labelled, or a bare sentence next to tool output reads as data.
        let interjected_text = interjected.clone();
        if let Some(text) = interjected {
            blocks.push(UserBlock::Text(format!("{INTERJECTION_PREFIX}{text}")));
        }
        if blocks.is_empty() {
            anyhow::bail!("no tool results to send back");
        }
        // The tool results are already in the log — the executor appends one
        // `tool/call-settled` as each call settles. What is *not* yet there is
        // anything the user said mid-turn: record it at the moment it enters
        // history, not at turn end, or a turn that fails after acting on it
        // loses it entirely.
        if let Some(text) = interjected_text {
            self.record(vec![SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: self.turn_id(),
                content: text,
                source: MessageSource::Injected,
                surface: SurfacePlacement::append(),
            })])
            .await;
        }
        self.run(Turn::User(blocks)).await
    }

    fn usage(&self) -> TokenUsage {
        self.usage
    }
    fn memories(&self) -> RecalledMemories {
        self.memories.clone()
    }
}

/// Split a model's assistant turn into komo's [`Step`]: any tool call makes it
/// a [`Step::ToolCalls`]; otherwise the concatenated text is the final answer.
/// Reasoning blocks are ignored for control flow (the driver still echoes them
/// back into history verbatim).
///
/// Text found *alongside* tool calls travels with them rather than being dropped:
/// it is the model narrating what it is about to do, which is the only account of
/// its reasoning a watcher gets and the honest thing to fall back on if the round
/// budget ends the turn early.
fn blocks_to_step(blocks: &[AssistantBlock]) -> Step {
    let mut calls = Vec::new();
    let mut text = String::new();
    for block in blocks {
        match block {
            AssistantBlock::ToolCall {
                id,
                call_id,
                name,
                args,
            } => calls.push(ToolCallReq {
                id: id.clone(),
                call_id: call_id.clone(),
                name: name.clone(),
                args: args.clone(),
            }),
            AssistantBlock::Text(t) => text.push_str(t),
            AssistantBlock::Reasoning(_) => {}
        }
    }
    if calls.is_empty() {
        if let Some(marker) = FABRICATED_CALL_MARKERS
            .iter()
            .find(|m| text.contains(**m))
            .filter(|_| !text.is_empty())
        {
            warn!(
                marker,
                "the model wrote tool-call syntax as prose and issued no call — \
                 the answer it is about to give is very likely fabricated"
            );
        }
        Step::Final(text)
    } else {
        Step::ToolCalls { calls, text }
    }
}

/// Tool-call syntax that must never appear in a model's *prose*. Finding it in a
/// round that issued no call means the model narrated a tool call instead of
/// making one, and the answer built on it is invented — the failure that led here
/// had a turn quoting a plausible JSON result for a command it never ran, with an
/// empty ledger and nothing in any log to say so. Detection cannot be a hard error
/// (the strings are model-specific and a legitimate reply could quote one while
/// discussing tooling), so this only leaves a breadcrumb: `komo logs` now names
/// the failure that otherwise has to be reconstructed from an empty run ledger.
///
/// The last entry is komo's own digest fence (`domain::run::tool_digest`): the
/// model emitting it means it is echoing the shape of its replayed history rather
/// than acting.
const FABRICATED_CALL_MARKERS: [&str; 5] = [
    "tool▁calls▁begin",
    "｜DSML｜",
    "<|tool_calls_begin|>",
    "<tool_call>",
    "<previous_turn_tools>",
];

/// Build an LLM client covering every provider the configured `models` menu
/// spans, exposing `tools` via function calling.
///
/// With a single-provider menu this is exactly one backend. With a
/// cross-provider one it is a [`RoutingLlm`] over one backend per provider, and
/// a session's qualified model id (`deepseek:deepseek-chat`) selects among them —
/// so switching provider is the same mechanism as switching model, decided per
/// turn off the session.
///
/// `preamble` is a factory (see [`PreambleFn`]) invoked once per turn to
/// (re)assemble the system prompt — typically wrapping a
/// [`crate::system_prompt::SystemPromptBuilder`]. `enricher` is the
/// optional per-turn memory enrichment — `Some` only for the main agent, `None`
/// for aux/delegate sub-agents (they must not be fed the user's memory library).
pub fn build_llm(
    config: &ModelConfig,
    tools: Option<&komo_services::tool_execution::ToolExecutor>,
    preamble: PreambleFn,
    enricher: Option<Arc<MemoryEnricher>>,
    cache_family: Option<&str>,
) -> anyhow::Result<Arc<dyn LlmClient>> {
    let providers = config.menu_providers();
    // The common case: everything on the menu runs on one provider, so there is
    // nothing to route between.
    if providers.len() < 2 {
        return build_provider_llm(config, tools, preamble, enricher, cache_family);
    }

    let mut by_provider = Vec::with_capacity(providers.len());
    for provider in providers {
        // Each backend's own default model is the first menu entry naming it —
        // for the configured provider that is `model` itself (the resolver force-
        // includes it first), so the default backend keeps its exact identity.
        let default_model = config
            .menu()
            .into_iter()
            .find(|entry| entry.provider == provider)
            .map(|entry| entry.model)
            .unwrap_or_else(|| provider.default_model().to_string());
        let scoped = config.for_provider(provider, default_model);
        by_provider.push((
            provider,
            build_provider_llm(
                &scoped,
                tools,
                preamble.clone(),
                enricher.clone(),
                cache_family,
            )?,
        ));
    }
    Ok(Arc::new(RoutingLlm {
        by_provider,
        default_provider: config.provider,
    }))
}

/// Build the backend for exactly one provider.
fn build_provider_llm(
    config: &ModelConfig,
    tools: Option<&komo_services::tool_execution::ToolExecutor>,
    preamble: PreambleFn,
    enricher: Option<Arc<MemoryEnricher>>,
    cache_family: Option<&str>,
) -> anyhow::Result<Arc<dyn LlmClient>> {
    // A missing API key degrades instead of failing construction: a fresh
    // install (first Docker boot, pre-`komo init`) must still bring the
    // gateway up — channels serve, pairing works — while every LLM call
    // reports the fix. Config resolution records the matching warning.
    if config.provider.uses_api_key() && config.api_key.is_empty() {
        return Ok(Arc::new(UnconfiguredLlm {
            message: format!(
                "{} is not set (required for {:?}). Add it to ~/.komo/.env \
                 (run `komo init` to scaffold one) or the container \
                 environment, then restart the gateway.",
                config.provider.api_key_var(),
                config.provider
            ),
        }));
    }

    // Only the schemas cross to the provider: the executor stays the single
    // dispatcher, so there is exactly one execution semantics (retry/ledger/cap)
    // for every tool call.
    // The catalog, not a copy of its schemas: `ProviderLlm::tool_schemas`
    // renders it per turn, so a tool mounted after wiring is one the model can
    // see without a restart.
    let tool_catalog = tools.map(|executor| executor.catalog().clone());

    let wire = wire_for(config.provider);
    // Auth and the static headers are resolved together because Codex's headers
    // depend on its credentials (the account id rides in one of them).
    let (auth, headers) = match config.provider {
        // Codex authenticates from the Codex CLI's OAuth file, and the token
        // rotates hourly — so it is resolved per request rather than captured
        // here. Missing/broken credentials degrade like a missing API key: the
        // gateway must boot (a fresh box, or a container without ~/.codex
        // mounted) instead of crash-looping, with every LLM call reporting the
        // fix as the turn's reply.
        Provider::Codex => match CodexAuth::load() {
            Ok(auth) => {
                let headers = codex_static_headers(auth.account_id());
                (Auth::Dynamic(auth), headers)
            }
            Err(error) => {
                tracing::warn!(%error, "Codex credentials unavailable; LLM degraded");
                return Ok(Arc::new(UnconfiguredLlm {
                    message: format!(
                        "Codex credentials unavailable: {error:#}. Run `codex` to log \
                         in (it writes ~/.codex/auth.json; $CODEX_HOME honored), then \
                         restart the gateway."
                    ),
                }));
            }
        },
        // Anthropic versions its API by header, not by URL.
        Provider::Anthropic => (
            Auth::ApiKey(config.api_key.clone()),
            vec![(
                "anthropic-version".to_string(),
                komo_provider::messages::ANTHROPIC_VERSION.to_string(),
            )],
        ),
        _ => (Auth::Bearer(config.api_key.clone()), Vec::new()),
    };

    let endpoint = Endpoint {
        url: endpoint_url(config.provider, config.base_url.as_deref()),
        auth,
        headers,
        client: reqwest::Client::new(),
    };

    Ok(Arc::new(ProviderLlm {
        client: Arc::new(ProviderClient { endpoint, wire }),
        tools: tool_catalog,
        default_model: config.model.clone(),
        provider: config.provider,
        cache_family: cache_family.map(str::to_string),
        preamble,
        max_history_messages: config.max_history_messages,
        max_history_bytes: config.max_history_bytes,
        enricher,
        // Cap each completion so a hung provider request fails the turn instead
        // of wedging it in `running`. `0` = off.
        timeout: (config.llm_timeout_secs > 0)
            .then(|| Duration::from_secs(config.llm_timeout_secs)),
    }))
}

/// Which wire protocol a provider speaks.
///
/// Four of the five are Responses; Anthropic serves no such endpoint, which is
/// the only reason komo carries a second codec at all.
fn wire_for(provider: Provider) -> Wire {
    match provider {
        Provider::Anthropic => Wire::Messages,
        Provider::DeepSeek | Provider::OpenAi | Provider::OpenRouter | Provider::Codex => {
            Wire::Responses
        }
    }
}

/// The completion endpoint for a provider.
///
/// `base_url` overrides the API root (config `base_url` — an OpenAI-compatible
/// proxy, a self-hosted gateway); the wire's path is appended to it, so callers
/// configure a root and never a full endpoint.
fn endpoint_url(provider: Provider, base_url: Option<&str>) -> String {
    let root = base_url.unwrap_or(match provider {
        Provider::DeepSeek => "https://api.deepseek.com/v1",
        Provider::OpenAi => "https://api.openai.com/v1",
        Provider::Anthropic => "https://api.anthropic.com/v1",
        Provider::OpenRouter => "https://openrouter.ai/api/v1",
        Provider::Codex => CODEX_BASE_URL,
    });
    let path = match wire_for(provider) {
        Wire::Responses => "responses",
        Wire::Messages => "messages",
    };
    format!("{}/{path}", root.trim_end_matches('/'))
}

/// Trim `prior` (the transcript before this turn's prompt) to the slice replayed
/// as model history, under two independent bounds.
///
/// Without a window, a long-lived chat session — telegram/feishu/wechat are keyed
/// by chat id and only rotate on an explicit `/new` — resends its whole transcript
/// every turn. `max_messages` is the count bound (`0` = keep everything);
/// `max_bytes` is the size bound (`0` = no size limit), and it exists because a
/// count says nothing about volume: twenty messages of pasted build output
/// overflow a context that two hundred chat lines sit inside. Both trim from the
/// oldest end, so the stable system prompt and memory prefix are untouched and the
/// upstream prompt cache is unaffected.
fn window_history(prior: &[Message], max_messages: usize, max_bytes: usize) -> &[Message] {
    let mut window = match max_messages {
        0 => prior,
        n => &prior[prior.len().saturating_sub(n)..],
    };
    // Once the transcript is at the count cap, the naive cut advances every
    // turn (each turn pushes the oldest message out), so the replayed history
    // opens with different bytes every turn and the provider prompt cache
    // misses everything after the system prompt. Snap the cut forward to the
    // nearest *anchor* message instead: anchors are a deterministic property
    // of the message itself (a hash of its stored bytes), so consecutive
    // turns keep opening the window on the same message until the cap
    // genuinely passes it — the prefix then stays byte-identical for
    // ~[`WINDOW_ANCHOR_SPACING`] turns at a stretch, at the cost of a
    // slightly shorter window. No anchor in the window ⇒ keep the naive cut
    // (slides, but never under-delivers history).
    if max_messages > 0 && prior.len() >= max_messages {
        if let Some(offset) = window.iter().position(is_window_anchor) {
            window = &window[offset..];
        }
    }
    if max_bytes > 0 {
        let size = |m: &Message| m.content.len() + m.tool_note.len();
        let mut total: usize = window.iter().map(size).sum();
        let mut start = 0;
        // A single message over the whole budget still gets dropped (the loop runs
        // to the end): sending it would blow the context on its own, and the turn's
        // own prompt is never part of this slice, so the model is not left mute.
        while start < window.len() && total > max_bytes {
            total -= size(&window[start]);
            start += 1;
        }
        window = &window[start..];
    }
    // The transcript strictly alternates user/assistant, so either cut can open on
    // an assistant message; drop it so history starts on a user turn (Anthropic
    // rejects a leading assistant message). Applied after both bounds, since
    // either one can be the cut that lands there.
    if window.first().is_some_and(|m| m.role == Role::Assistant) {
        window = &window[1..];
    }
    window
}

/// Average anchor spacing, in *user* messages: a user message is an anchor
/// when its hash lands in `1/WINDOW_ANCHOR_SPACING` of the space, so the
/// window start advances roughly once per this many turns instead of every
/// turn. Larger = warmer cache but a shorter average window (the cut snaps
/// further forward); 6 trades ≤ ~12 messages of tail for a prefix that holds
/// still ~6 turns at a time.
const WINDOW_ANCHOR_SPACING: u64 = 6;

/// Whether `m` is a window-anchor message (see [`window_history`]): a user
/// message whose FNV-1a hash of its stored bytes selects it. Keyed on stored
/// fields only (content + timestamp — never the render-time tool-note
/// decision), so every turn, in every process, agrees on which messages are
/// anchors. User role so a snapped window always opens on a user message,
/// which providers require anyway.
fn is_window_anchor(m: &Message) -> bool {
    if m.role != Role::User {
        return false;
    }
    let mut h: u64 = 0xcbf29ce484222325;
    for b in m
        .content
        .as_bytes()
        .iter()
        .chain(m.timestamp.to_le_bytes().iter())
    {
        h = (h ^ u64::from(*b)).wrapping_mul(0x100000001b3);
    }
    h % WINDOW_ANCHOR_SPACING == 0
}

/// Map a komo message into provider chat-history turns. The system prompt is
/// supplied via the preamble, and tool outputs are folded into the following
/// assistant reply, so both `System` and `Tool` roles are skipped here.
///
/// A note-bearing assistant message renders as **two** turns: the reply itself,
/// then the tool digest as a *user* turn. The digest is what lets the next turn
/// know tools ran at all, but it must not look like assistant output — rendered
/// inside the assistant's own text (as it was until this changed), it reads as a
/// worked example of an assistant narrating tool calls in prose, and a model that
/// copies the shape gets a turn that reports invented commands and invented
/// results with nothing in the ledger. Attributing it to the other side of the
/// conversation, plus `tool_digest`'s fence, is what breaks the pattern. The
/// user-visible `content` is untouched either way — it stays exactly what every
/// client renders.
///
/// Two user turns in a row can result (digest, then the next real user message).
/// Both wires handle it: `messages` merges same-role neighbours because Anthropic
/// demands strict alternation, and `responses` flattens turns into an input list
/// that never required it.
///
/// **The rendering is a pure function of the message.** Nothing here may depend
/// on where the message sits in the window (see [`ProviderLlm::assemble`]) — which
/// is why the digest becomes its own turn rather than a prefix on the *following*
/// user message: `window_history` always cuts so the window opens on a user
/// message, so a prefixed digest would appear or vanish depending on whether its
/// assistant survived the cut. Emitted as a pair, the two turns live and die
/// together. This also used to carry the note only for the last three
/// note-bearing turns, which meant every tool turn silently rewrote an older
/// message's bytes and cost the provider prefix cache everything from ~3 turns
/// back, every turn. Always attaching is both simpler and cheaper: the digest is
/// already capped when it is written (`domain::run::tool_digest`), and
/// [`window_history`]'s byte budget has always counted `tool_note` for every
/// message in the window regardless of whether it was rendered — so the
/// accounting matches what is actually sent.
fn to_turns(msg: &Message) -> Vec<Turn> {
    match msg.role {
        Role::User => vec![Turn::user(msg.content.clone())],
        Role::Assistant if !msg.tool_note.is_empty() => vec![
            Turn::assistant(msg.content.clone()),
            Turn::user(msg.tool_note.clone()),
        ],
        Role::Assistant => vec![Turn::assistant(msg.content.clone())],
        Role::System | Role::Tool => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_style_providers_send_reasoning_effort() {
        for provider in [Provider::OpenAi, Provider::OpenRouter, Provider::Codex] {
            assert_eq!(
                reasoning_params(provider, "high"),
                Some(json!({ "reasoning": { "effort": "high" } })),
                "{provider:?} should carry reasoning.effort"
            );
        }
    }

    #[test]
    fn anthropic_maps_effort_onto_a_thinking_budget() {
        let low = reasoning_params(Provider::Anthropic, "low").unwrap();
        let high = reasoning_params(Provider::Anthropic, "high").unwrap();
        let budget = |v: &Value| v["thinking"]["budget_tokens"].as_u64().unwrap();
        assert_eq!(low["thinking"]["type"], "enabled");
        assert!(
            budget(&low) < budget(&high),
            "a higher effort must buy more thinking"
        );
    }

    #[test]
    fn deepseek_and_unknown_levels_change_nothing() {
        // DeepSeek exposes no effort scale (`Provider::efforts` is empty), so a
        // level arriving anyway must not invent request params.
        assert_eq!(reasoning_params(Provider::DeepSeek, "high"), None);
        for level in ["", "  ", "auto", "xhigh", "HIGH"] {
            assert_eq!(reasoning_params(Provider::OpenAi, level), None, "{level:?}");
        }
    }

    #[test]
    fn every_advertised_effort_level_actually_maps() {
        // The menu a client is shown (`Provider::efforts`) and what reaches the
        // wire must agree — otherwise the UI offers a switch that does nothing.
        for provider in Provider::ALL {
            for level in provider.efforts() {
                assert!(
                    reasoning_params(provider, level).is_some(),
                    "{provider:?} advertises `{level}` but sends nothing"
                );
            }
        }
    }

    /// Four providers on one codec is the reason this layer is small; Anthropic
    /// is the one exception, because it serves no Responses endpoint.
    #[test]
    fn every_provider_but_anthropic_speaks_responses() {
        for provider in Provider::ALL {
            let expected = if provider == Provider::Anthropic {
                Wire::Messages
            } else {
                Wire::Responses
            };
            assert_eq!(wire_for(provider), expected, "{provider:?}");
        }
    }

    #[test]
    fn endpoint_urls_append_the_wires_path_to_the_root() {
        assert_eq!(
            endpoint_url(Provider::DeepSeek, None),
            "https://api.deepseek.com/v1/responses"
        );
        assert_eq!(
            endpoint_url(Provider::Anthropic, None),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            endpoint_url(Provider::Codex, None),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        // A configured root points the same wire at a proxy, trailing slash or
        // not.
        assert_eq!(
            endpoint_url(Provider::OpenAi, Some("http://localhost:8080/v1/")),
            "http://localhost:8080/v1/responses"
        );
    }

    /// A backend that reports which provider it was routed to.
    struct Tagged(&'static str);

    #[async_trait]
    impl LlmClient for Tagged {
        async fn complete(&self, _session: &Session) -> anyhow::Result<String> {
            Ok(self.0.to_string())
        }

        async fn resume_turn(
            &self,
            _session: &Session,
            _events: &[SessionEvent],
            _turn_id: &str,
            _deltas: Option<Arc<dyn DeltaSink>>,
            _recorder: Option<Arc<dyn TurnRecorder>>,
        ) -> anyhow::Result<Box<dyn TurnDriver>> {
            // Reports which backend the router picked, via the error text.
            anyhow::bail!("resumed-on:{}", self.0)
        }
    }

    use komo_core::domain::session_event::{
        ToolCallSettledEvent, ToolCallStartedEvent, ToolOutcome as SettledOutcome,
    };

    // ── rebuilding an interrupted turn from its events ───────────────────────

    fn at(text: &str) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).unwrap()
    }

    fn ev(seq: u64, kind: SessionEventKind) -> SessionEvent {
        SessionEvent::new(seq, at("2026-09-01T10:00:00Z"), kind)
    }

    fn header_event(seq: u64) -> SessionEvent {
        ev(
            seq,
            SessionEventKind::RequestHeader(RequestHeaderEvent {
                reason: HeaderReason::Initial,
                provider: "openai".into(),
                model: "gpt-test".into(),
                effort: String::new(),
                system: "SYSTEM".into(),
                tools: vec![],
                extra: Some(json!({ "prompt_cache_key": "komo:s" })),
            }),
        )
    }

    fn round_event(seq: u64, text: &str, calls: &[&str]) -> SessionEvent {
        let mut blocks = vec![AssistantBlock::Reasoning(komo_provider::types::Reasoning {
            id: Some("rs".into()),
            summary: vec![],
            encrypted: Some("blob".into()),
        })];
        if !text.is_empty() {
            blocks.push(AssistantBlock::Text(text.into()));
        }
        for name in calls {
            blocks.push(AssistantBlock::ToolCall {
                id: format!("item-{name}"),
                call_id: Some(format!("call-{name}")),
                name: (*name).into(),
                args: "{}".into(),
            });
        }
        ev(
            seq,
            SessionEventKind::AssistantRound(AssistantRoundEvent {
                turn_id: "t1".into(),
                round: 0,
                response_id: "msg".into(),
                blocks: serde_json::to_value(&blocks).unwrap(),
                tokens_in: 0,
                tokens_out: 0,
                tokens_cached: 0,
            }),
        )
    }

    fn started_event(seq: u64, name: &str, index: u32) -> SessionEvent {
        ev(
            seq,
            SessionEventKind::ToolCallStarted(ToolCallStartedEvent {
                turn_id: "t1".into(),
                call_id: format!("call-{name}"),
                call_index: index,
                tool: name.into(),
                args: "{}".into(),
            }),
        )
    }

    fn settled_event(seq: u64, name: &str, index: u32, result: &str) -> SessionEvent {
        ev(
            seq,
            SessionEventKind::ToolCallSettled(ToolCallSettledEvent {
                turn_id: "t1".into(),
                call_id: format!("call-{name}"),
                call_index: index,
                outcome: SettledOutcome::Succeeded,
                result: result.into(),
                error: String::new(),
                elapsed_ms: 1,
                structured: Value::Null,
                output_paths: vec![],
            }),
        )
    }

    fn asked(text: &str) -> Session {
        let mut session = Session::new("s");
        session.messages.push(Message::user(text));
        session
    }

    /// A turn resumed **twice**: A died after a round, B (resumed from A) died
    /// after another, and C picks up B. C has to replay both rounds — reading
    /// only B's would send the model back to the question, so one crash would
    /// be recoverable and two would not.
    #[test]
    fn a_second_resume_replays_every_attempt_before_it() {
        let session = asked("go");
        let opened = |seq: u64, turn: &str, from: Option<&str>| {
            ev(
                seq,
                SessionEventKind::TurnStarted {
                    turn_id: turn.into(),
                    resumed_from: from.map(str::to_string),
                },
            )
        };
        let round_of = |seq: u64, turn: &str, id: &str| {
            ev(
                seq,
                SessionEventKind::AssistantRound(AssistantRoundEvent {
                    turn_id: turn.into(),
                    round: 0,
                    response_id: id.into(),
                    blocks: serde_json::to_value(vec![AssistantBlock::Text("working".into())])
                        .unwrap(),
                    tokens_in: 0,
                    tokens_out: 0,
                    tokens_cached: 0,
                }),
            )
        };
        let events = vec![
            opened(0, "A", None),
            header_event(1),
            round_of(2, "A", "msg-a"),
            opened(3, "B", Some("A")),
            round_of(4, "B", "msg-b"),
            opened(5, "C", Some("B")),
        ];

        let rebuilt = rebuild_from_events(&session, &events, "C", &|_| false).unwrap();

        let replayed: Vec<String> = rebuilt
            .history
            .iter()
            .filter_map(|turn| match turn {
                Turn::Assistant { id, .. } => id.clone(),
                _ => None,
            })
            .collect();
        assert_eq!(
            replayed,
            vec!["msg-a".to_string(), "msg-b".to_string()],
            "both attempts' rounds, in the order they happened"
        );
        assert_eq!(
            rebuilt.history.len(),
            3,
            "the question, then the two rounds"
        );
    }

    #[test]
    fn a_rebuild_replays_the_rounds_and_their_results() {
        let session = asked("go");
        let events = vec![
            header_event(0),
            round_event(1, "", &["read"]),
            started_event(2, "read", 0),
            settled_event(3, "read", 0, "file contents"),
        ];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|_| false).unwrap();
        // The envelope came from the header, not from a stored copy of history.
        assert_eq!(rebuilt.model, "gpt-test");
        assert_eq!(rebuilt.preamble, "SYSTEM");
        assert!(rebuilt.extra.is_some());
        // user → assistant round → its results.
        assert_eq!(rebuilt.history.len(), 3);
        assert!(matches!(rebuilt.history[1], Turn::Assistant { .. }));
        assert!(matches!(rebuilt.start, TurnStart::Continue));
    }

    #[test]
    fn results_rebuild_in_call_order_not_in_the_order_they_settled() {
        // A round runs concurrently, so settle order is completion order.
        // Rebuilding in it would hand the provider a different request than the
        // live turn sent — and `rebuild == live` is the whole point.
        let session = asked("go");
        let events = vec![
            header_event(0),
            round_event(1, "", &["a", "b", "c"]),
            started_event(2, "a", 0),
            started_event(3, "b", 1),
            started_event(4, "c", 2),
            settled_event(5, "c", 2, "third"),
            settled_event(6, "a", 0, "first"),
            settled_event(7, "b", 1, "second"),
        ];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|_| false).unwrap();
        let Some(Turn::User(blocks)) = rebuilt.history.last() else {
            panic!("the round's results close the history");
        };
        let texts: Vec<&str> = blocks
            .iter()
            .filter_map(|b| match b {
                UserBlock::ToolResult { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts, vec!["first", "second", "third"]);
    }

    #[test]
    fn a_call_that_never_settled_comes_back_as_an_uncertain_outcome() {
        // The tool is *not* re-run: a mutation cannot be assumed idempotent, so
        // whether to re-issue it is the model's decision — told plainly.
        let session = asked("go");
        let events = vec![
            header_event(0),
            round_event(1, "", &["a", "b"]),
            started_event(2, "a", 0),
            started_event(3, "b", 1),
            settled_event(4, "a", 0, "landed"),
        ];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|_| false).unwrap();
        let Some(Turn::User(blocks)) = rebuilt.history.last() else {
            panic!("results close the history");
        };
        let texts: Vec<&str> = blocks
            .iter()
            .filter_map(|b| match b {
                UserBlock::ToolResult { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts[0], "landed", "a settled call keeps its real result");
        assert!(texts[1].contains("interrupted"));
        assert!(matches!(rebuilt.start, TurnStart::Continue));
    }

    #[test]
    fn an_unsettled_idempotent_call_is_re_dispatched_instead_of_reported_lost() {
        // The interrupted turn's own answer to "did this run?" — for a tool
        // that can simply be run again, re-running it is cheaper and more
        // accurate than spending a model round to say the result was lost.
        let session = asked("go");
        let events = vec![
            header_event(0),
            round_event(1, "", &["a", "b"]),
            started_event(2, "a", 0),
            started_event(3, "b", 1),
            settled_event(4, "a", 0, "landed"),
        ];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|name| name == "b").unwrap();
        let TurnStart::Replay { calls, slots } = rebuilt.start else {
            panic!("the unsettled idempotent call should be re-dispatched");
        };
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "b");
        // The round's results message is held back whole: it is one message and
        // it cannot be sent until every call in it has answered.
        assert!(matches!(
            rebuilt.history.last(),
            Some(Turn::Assistant { .. })
        ));
        assert_eq!(slots.len(), 2);
        assert!(slots[0].1.is_some(), "the settled call keeps its result");
        assert!(slots[1].1.is_none(), "the replayed call leaves a hole");
    }

    #[test]
    fn a_non_idempotent_call_in_the_same_round_still_gets_the_note() {
        // Replay is per tool, not per round: one call being safe to repeat says
        // nothing about the one beside it.
        let session = asked("go");
        let events = vec![
            header_event(0),
            round_event(1, "", &["read", "shell"]),
            started_event(2, "read", 0),
            started_event(3, "shell", 1),
        ];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|name| name == "read").unwrap();
        let TurnStart::Replay { calls, slots } = rebuilt.start else {
            panic!("expected a replay");
        };
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read");
        let Some(UserBlock::ToolResult { text, .. }) = &slots[1].1 else {
            panic!("the non-idempotent call keeps a recorded result");
        };
        assert!(text.contains("interrupted"));
    }

    #[test]
    fn only_the_newest_round_is_replayed() {
        // An earlier round's results must have been sent, or the round after it
        // would not exist. An unsettled call there is a lost event, not work in
        // flight, and re-running it would change a request already answered.
        let session = asked("go");
        let events = vec![
            header_event(0),
            round_event(1, "", &["a"]),
            started_event(2, "a", 0),
            round_event(3, "", &["b"]),
            started_event(4, "b", 1),
        ];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|_| true).unwrap();
        let TurnStart::Replay { calls, .. } = rebuilt.start else {
            panic!("expected a replay of the newest round");
        };
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "b");
        // The stale round closed with the note rather than being re-run.
        let stale = rebuilt
            .history
            .iter()
            .filter_map(|turn| match turn {
                Turn::User(blocks) => Some(blocks),
                _ => None,
            })
            .nth(1)
            .expect("the first round's results are in history");
        assert!(matches!(
            stale.first(),
            Some(UserBlock::ToolResult { text, .. }) if text.contains("interrupted")
        ));
    }

    #[test]
    fn a_replayed_round_closes_in_the_order_the_model_issued_the_calls() {
        // Not in the order the results arrived: the message has to match the
        // one the interrupted turn was assembling.
        let block = |id: &str, text: &str| UserBlock::ToolResult {
            id: id.into(),
            call_id: Some(id.into()),
            text: text.into(),
        };
        let slots: Vec<ReplaySlot> = vec![
            ("a".into(), Some(block("a", "first"))),
            ("b".into(), None),
            ("c".into(), Some(block("c", "third"))),
            ("d".into(), None),
        ];
        // The replay answered d before b.
        let fresh = vec![block("d", "fourth"), block("b", "second")];
        let texts: Vec<String> = fill_replay_slots(slots, fresh)
            .into_iter()
            .map(|b| match b {
                UserBlock::ToolResult { text, .. } => text,
                _ => panic!("results only"),
            })
            .collect();
        assert_eq!(texts, vec!["first", "second", "third", "fourth"]);
    }

    #[test]
    fn a_replayed_call_that_came_back_with_nothing_still_fills_its_slot() {
        // Unreachable in practice — the executor answers every call it is
        // handed — but an open hole is the one shape a provider rejects.
        let slots: Vec<ReplaySlot> = vec![("a".into(), None)];
        let blocks = fill_replay_slots(slots, vec![]);
        assert_eq!(blocks.len(), 1);
        assert!(matches!(
            &blocks[0],
            UserBlock::ToolResult { text, .. } if text.contains("interrupted")
        ));
    }

    #[test]
    fn a_turn_that_had_already_answered_hands_the_answer_back() {
        // The reply was produced and only the ledger close was lost. Re-issuing
        // the request would pay for an answer that already exists.
        let session = asked("go");
        let events = vec![header_event(0), round_event(1, "all done", &[])];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|_| false).unwrap();
        match rebuilt.start {
            TurnStart::Final(text) => assert_eq!(text, "all done"),
            TurnStart::Continue => panic!("expected the answer back, not a continuation"),
            _ => panic!("expected the answer back"),
        }
    }

    #[test]
    fn a_turn_with_no_recorded_header_cannot_be_rebuilt() {
        // Without the envelope the continuation would be a *different* request:
        // a re-assembled prompt whose memory recall and clock have moved on.
        let session = asked("go");
        assert!(
            rebuild_from_events(&session, &[round_event(0, "hi", &[])], "t1", &|_| false).is_err()
        );
    }

    #[test]
    fn another_turns_events_are_not_replayed_into_this_one() {
        let session = asked("go");
        let mut other = round_event(1, "", &["read"]);
        if let SessionEventKind::AssistantRound(round) = &mut other.kind {
            round.turn_id = "t0".into();
        }
        let events = vec![header_event(0), other];
        let rebuilt = rebuild_from_events(&session, &events, "t1", &|_| false).unwrap();
        assert_eq!(rebuilt.history.len(), 1, "only the user message survives");
    }

    #[tokio::test]
    async fn resume_routes_on_the_recorded_provider_not_the_session() {
        let router = router();
        // The session says codex (the default), but the interrupted turn ran on
        // deepseek — continuing it anywhere else would replay one provider's
        // opaque state into another.
        let events = vec![ev(
            0,
            SessionEventKind::RequestHeader(RequestHeaderEvent {
                reason: HeaderReason::Initial,
                provider: "deepseek".into(),
                model: "deepseek-v4".into(),
                effort: String::new(),
                system: String::new(),
                tools: vec![],
                extra: None,
            }),
        )];
        let error = router
            .resume_turn(
                &session_on("codex:gpt-5.3-codex"),
                &events,
                "t1",
                None,
                None,
            )
            .await
            .err()
            .expect("the tagged stub always errors");
        assert!(error.to_string().contains("resumed-on:deepseek"));
    }

    fn router() -> RoutingLlm {
        RoutingLlm {
            by_provider: vec![
                (
                    Provider::Codex,
                    Arc::new(Tagged("codex")) as Arc<dyn LlmClient>,
                ),
                (
                    Provider::DeepSeek,
                    Arc::new(Tagged("deepseek")) as Arc<dyn LlmClient>,
                ),
            ],
            default_provider: Provider::Codex,
        }
    }

    fn session_on(model: &str) -> Session {
        let mut session = Session::new("s");
        session.model = model.to_string();
        session
    }

    #[tokio::test]
    async fn a_qualified_id_routes_to_that_provider() {
        let router = router();
        assert_eq!(
            router
                .complete(&session_on("deepseek:deepseek-chat"))
                .await
                .unwrap(),
            "deepseek"
        );
    }

    #[tokio::test]
    async fn an_unqualified_or_default_id_stays_on_the_default_provider() {
        let router = router();
        for model in ["", "gpt-5.5", "codex:gpt-5.6-sol"] {
            assert_eq!(
                router.complete(&session_on(model)).await.unwrap(),
                "codex",
                "model {model:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_provider_with_no_backend_falls_back_to_the_default() {
        // Config can change under a stored session (a key removed, an entry
        // dropped), and running on the default beats failing the turn.
        let router = router();
        assert_eq!(
            router
                .complete(&session_on("anthropic:claude-sonnet-4-5"))
                .await
                .unwrap(),
            "codex"
        );
    }

    fn turn(user: &str, assistant: &str, note: &str) -> Vec<Message> {
        vec![
            Message::user(user),
            Message::assistant(assistant).with_tool_note(note),
        ]
    }

    fn retryable(message: &str) -> LlmError {
        LlmError::new(LlmErrorKind::Overloaded, message)
    }

    /// A rate limit takes seconds to clear, so the local fallback table has to
    /// outlast one: four attempts spanning 21s, versus the three-in-2.5s that
    /// used to run out while the limit was still in force.
    #[tokio::test(start_paused = true)]
    async fn the_retry_budget_outlasts_a_rate_limit() {
        assert_eq!(LLM_RETRY_MAX_ATTEMPTS, LLM_RETRY_BACKOFF_MS.len() + 1);
        assert!(
            LLM_RETRY_BACKOFF_MS.iter().sum::<u64>() >= 20_000,
            "the table has to cover a rate limit that lasts tens of seconds"
        );

        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result = with_retry(|| async {
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if n < 3 {
                return Err(retryable("429 Too Many Requests"));
            }
            Ok("answered")
        })
        .await
        .unwrap();
        assert_eq!(result, "answered");
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::Relaxed),
            LLM_RETRY_MAX_ATTEMPTS,
            "a fourth attempt has to exist for the last backoff to be worth having"
        );
    }

    /// The payoff of carrying `retry_after` on the error: when the server says
    /// when its limit clears, we wait exactly that long instead of guessing.
    #[tokio::test(start_paused = true)]
    async fn a_servers_own_delay_beats_the_local_table() {
        let started = tokio::time::Instant::now();
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result = with_retry(|| async {
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if n == 0 {
                return Err(LlmError::new(LlmErrorKind::RateLimited, "slow down")
                    .with_retry_after(Some(Duration::from_millis(200))));
            }
            Ok(())
        })
        .await;
        assert!(result.is_ok());
        let waited = started.elapsed();
        assert!(
            waited < Duration::from_millis(900),
            "waited {waited:?} — the server said 200ms, the table's first entry is 1s"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_terminal_failure_is_not_retried() {
        // An auth or schema error will fail identically forever; retrying it just
        // delays the message the user needs to see.
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let error = with_retry(|| async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err::<(), _>(LlmError::new(LlmErrorKind::Auth, "invalid api key"))
        })
        .await
        .expect_err("terminal errors surface");
        assert!(error.message.contains("invalid api key"));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    /// An overflow is recoverable, but not by re-sending: the driver has to
    /// shrink the history first, so the retry layer must pass it straight
    /// through to the degrade path.
    #[tokio::test(start_paused = true)]
    async fn an_overflow_is_not_retried_but_reaches_the_driver() {
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let error = with_retry(|| async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err::<(), _>(LlmError::new(LlmErrorKind::ContextOverflow, "too long"))
        })
        .await
        .expect_err("an overflow surfaces");
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(error.is_context_overflow());
    }

    #[tokio::test(start_paused = true)]
    async fn completion_retries_are_bounded() {
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let _ = with_retry(|| async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err::<(), _>(LlmError::transport("connection refused"))
        })
        .await
        .expect_err("a permanently down provider still fails");
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::Relaxed),
            LLM_RETRY_MAX_ATTEMPTS
        );
    }

    /// The retry budget lives *inside* the timeout, so a flapping provider can't
    /// multiply a turn's worst-case latency by the attempt count.
    #[tokio::test(start_paused = true)]
    async fn the_timeout_bounds_every_retry_together() {
        let started = tokio::time::Instant::now();
        let error = with_timeout(
            Some(Duration::from_secs(1)),
            with_retry(|| async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Err::<(), _>(LlmError::transport("connection refused"))
            }),
        )
        .await
        .expect_err("the round times out");
        assert_eq!(error.kind, LlmErrorKind::Timeout);
        assert!(
            !error.is_retryable(),
            "the budget it exceeded covers every attempt, so there is nothing left to retry"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    fn tool_result_turn(text: &str) -> Turn {
        Turn::User(vec![UserBlock::ToolResult {
            id: "call-1".into(),
            call_id: Some("call-1".into()),
            text: text.into(),
        }])
    }

    /// The usual overflow is one turn that read several large things. Shrinking
    /// those reclaims the context without touching the conversation, and the
    /// full text is still on disk in the tool-output store.
    #[test]
    fn reclaiming_shrinks_bulky_tool_results_and_keeps_the_conversation() {
        let mut history = vec![
            Turn::user("find the bug"),
            tool_result_turn(&"x".repeat(200_000)),
            Turn::assistant("reading further"),
        ];
        let before = history.len();

        assert!(reclaim_context(&mut history));
        assert_eq!(history.len(), before, "no message is dropped");
        let rendered = format!("{history:?}");
        assert!(rendered.contains("elided"), "the big result was shrunk");
        assert!(rendered.contains("find the bug"), "the ask is still there");
    }

    /// When there is nothing bulky to shrink the weight is the conversation
    /// itself, so the oldest half goes — and what is left still opens on a user
    /// message, which several providers require.
    #[test]
    fn reclaiming_falls_back_to_dropping_the_oldest_half() {
        let mut history: Vec<Turn> = (0..8)
            .flat_map(|i| {
                [
                    Turn::user(format!("q{i}")),
                    Turn::assistant(format!("a{i}")),
                ]
            })
            .collect();

        assert!(reclaim_context(&mut history));
        assert!(history.len() <= 8);
        assert!(
            !matches!(history.first(), Some(Turn::Assistant { .. })),
            "history must still open on a user message"
        );
        let rendered = format!("{history:?}");
        assert!(!rendered.contains("q0"), "the oldest exchange is gone");
        assert!(rendered.contains("q7"), "the newest is kept");
    }

    /// The cut can land inside a tool round; the kept half must not open on
    /// tool results whose function_calls went with the dropped half — strict
    /// providers (DeepSeek) reject such orphans with a 400.
    #[test]
    fn reclaiming_never_leaves_orphaned_tool_results_at_the_window_head() {
        let call_turn = Turn::Assistant {
            id: None,
            blocks: vec![AssistantBlock::ToolCall {
                id: "fc_1".into(),
                call_id: Some("call_1".into()),
                name: "read".into(),
                args: "{}".into(),
            }],
        };
        let mut history = vec![
            Turn::user("q0"),
            Turn::assistant("a0"),
            Turn::user("q1"),
            call_turn,
            // Small on purpose: big results are shrunk by the first step and
            // never reach the half-drop this test is about.
            tool_result_turn("ok"),
            Turn::assistant("a1"),
            Turn::user("q2"),
            Turn::assistant("a2"),
        ];

        assert!(reclaim_context(&mut history));
        // The cut (len 8 → 4) lands right between the call and its result.
        let rendered = format!("{history:?}");
        assert!(
            !rendered.contains("ToolResult"),
            "no orphaned tool result may survive: {rendered}"
        );
        assert!(
            matches!(history.first(), Some(Turn::User(blocks))
                if matches!(blocks.first(), Some(UserBlock::Text(t)) if t == "q2")),
            "the window must open on real user text: {rendered}"
        );
    }

    /// Nothing left to give: the caller has to surface the failure rather than
    /// re-send an empty request.
    #[test]
    fn reclaiming_reports_failure_when_there_is_nothing_to_reclaim() {
        let mut history = vec![Turn::user("hi")];
        assert!(!reclaim_context(&mut history));
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn head_tail_keeps_both_ends_and_cuts_on_char_boundaries() {
        let text = "前".repeat(4_000); // 12 KB of 3-byte chars
        let cut = head_tail(&text, 1_024);
        assert!(cut.len() < text.len() / 4);
        assert!(cut.starts_with('前') && cut.ends_with('前'));
        assert!(cut.contains("elided"));
    }

    /// A backend whose sessions are one-shot but whose prompt prefix never
    /// changes has to key the cache by that prefix, or every delegation and
    /// every cron firing pays a cold start. The main agent keeps keying by
    /// session, where one conversation really is one prefix.
    #[test]
    fn a_declared_family_keys_the_cache_instead_of_the_one_shot_session() {
        let first = cache_key(Some("delegate"), "delegate:0198-aaaa");
        let second = cache_key(Some("delegate"), "delegate:0198-bbbb");
        assert_eq!(first, second, "two delegations share one warm prefix");

        let a = cache_key(None, "telegram:644");
        let b = cache_key(None, "telegram:900");
        assert_ne!(a, b, "separate conversations must not share a key");
        assert_eq!(a, "komo:telegram:644");
    }

    #[test]
    fn the_byte_budget_trims_where_the_count_window_cannot() {
        // Two turns, the first carrying a pasted log. Both fit the count window,
        // so only the byte bound can keep the big one out — the case the count
        // window was blind to.
        let mut prior = turn("here is the log", &"x".repeat(5_000), "");
        prior.extend(turn("and now?", "short answer", ""));

        let counted = window_history(&prior, 50, 0);
        assert_eq!(counted.len(), 4, "the count window keeps everything");

        let bounded = window_history(&prior, 50, 1_000);
        assert_eq!(
            bounded.iter().map(|m| &m.content).collect::<Vec<_>>(),
            vec!["and now?", "short answer"],
            "the oversized turn is trimmed from the oldest end"
        );
    }

    #[test]
    fn a_window_never_opens_on_an_assistant_message() {
        // Whichever bound makes the cut, a leading assistant message must go:
        // Anthropic rejects one outright.
        let mut prior = turn("q1", "a1", "");
        prior.extend(turn("q2", "a2", ""));

        for window in [
            window_history(&prior, 3, 0), // count cut lands on "a1"
            window_history(&prior, 0, 5), // byte cut lands on "a1"
        ] {
            assert_eq!(
                window.first().map(|m| m.role.clone()),
                Some(Role::User),
                "history must start on a user turn, got {:?}",
                window.first().map(|m| &m.content)
            );
        }
    }

    #[test]
    fn the_window_cut_holds_still_between_anchor_jumps() {
        // A long transcript with fixed timestamps (anchor selection hashes
        // stored bytes, so pinning them makes the test deterministic), replayed
        // as a growing session at the count cap — the shape of every long-lived
        // chat session. Without the anchor snap the window start advances every
        // turn and the replayed prefix is never the same twice.
        let mut all = Vec::new();
        for i in 0..120 {
            let mut user = Message::user(format!("question number {i}"));
            user.timestamp = 1_700_000_000 + i;
            let mut assistant = Message::assistant(format!("answer number {i}"));
            assistant.timestamp = 1_700_000_000 + i;
            all.push(user);
            all.push(assistant);
        }

        let max = 50;
        let mut starts = Vec::new();
        for end in (max..=all.len()).step_by(2) {
            // What the DB layer hands the adapter each turn: the last `max`.
            let prior = &all[end - max..end];
            let window = window_history(prior, max, 0);
            assert!(window.len() <= max, "budget respected");
            assert!(!window.is_empty(), "the cut must not starve the model");
            assert_eq!(window.first().unwrap().role, Role::User);
            starts.push(window.first().unwrap().content.clone());
        }

        let turns = starts.len();
        let changes = starts.windows(2).filter(|w| w[0] != w[1]).count();
        assert!(
            changes * 2 < turns,
            "window start moved {changes} times over {turns} turns — \
             the anchor snap should hold it still most turns"
        );
    }

    #[test]
    fn the_byte_budget_is_off_at_zero_and_counts_tool_notes() {
        let prior = turn("q", "a", &"n".repeat(5_000));
        assert_eq!(window_history(&prior, 0, 0).len(), 2, "0 = unlimited");
        // The note is real context sent to the model, so it has to be weighed.
        assert!(
            window_history(&prior, 0, 1_000).is_empty(),
            "a note over the budget must be trimmed like content"
        );
    }

    /// The prefix-cache invariant `assemble` documents: a stored message renders
    /// the same bytes no matter where it sits, so growing the conversation never
    /// rewrites an earlier message. This used to fail — only the last three
    /// note-bearing turns carried their note, so every tool turn silently
    /// changed an older message and cost the cache everything after it.
    #[test]
    fn a_message_renders_the_same_bytes_wherever_it_sits() {
        let mut prior = Vec::new();
        for i in 0..5 {
            prior.extend(turn(
                &format!("q{i}"),
                &format!("a{i}"),
                &format!("note{i}"),
            ));
        }
        let render = |msgs: &[Message]| -> Vec<String> {
            msgs.iter()
                .flat_map(to_turns)
                .map(|m| format!("{m:?}"))
                .collect()
        };

        // Every note is carried, oldest included — nothing ages out.
        let early = render(&prior);
        for i in 0..5 {
            let note = format!("note{i}");
            assert!(
                early.iter().any(|m| m.contains(&note)),
                "{note} must ride along"
            );
        }

        // Two more turns arrive. The messages that were already there must
        // render byte-identically; only the new ones are added.
        prior.extend(turn("q5", "a5", "note5"));
        prior.extend(turn("q6", "a6", "note6"));
        let later = render(&prior);
        assert_eq!(
            early,
            later[..early.len()],
            "appending a turn must not rewrite an earlier message"
        );
    }

    #[test]
    fn a_tool_note_never_touches_the_user_visible_content() {
        let msg = Message::assistant("the answer").with_tool_note("[tools used] read foo.rs");
        // The model sees the note; `content` stays exactly the reply.
        let rendered: Vec<String> = to_turns(&msg).iter().map(|t| format!("{t:?}")).collect();
        assert!(rendered.iter().any(|t| t.contains("the answer")));
        assert!(rendered.iter().any(|t| t.contains("read foo.rs")));
        // And the stored message itself is untouched — every client renders this.
        assert_eq!(msg.content, "the answer");
        let plain = Message::assistant("just talk");
        let rendered = format!("{:?}", to_turns(&plain)[0]);
        assert!(rendered.contains("just talk"));
    }

    /// The regression this whole change exists for: a tool digest replayed inside
    /// the assistant's own text is a worked example of narrating tool calls in
    /// prose, and a model that copies it answers from invented results with no
    /// tool steps in the ledger. The digest must reach the model as the *other*
    /// speaker's words, and the assistant turn must carry nothing but the reply.
    #[test]
    fn a_tool_note_reaches_the_model_as_a_user_turn() {
        let msg = Message::assistant("the answer").with_tool_note("[tools used] read foo.rs");
        let turns = to_turns(&msg);
        assert_eq!(turns.len(), 2, "reply plus digest: {turns:?}");
        match &turns[0] {
            Turn::Assistant { .. } => {
                let rendered = format!("{:?}", turns[0]);
                assert!(
                    !rendered.contains("read foo.rs"),
                    "the digest must not ride in the assistant turn: {rendered}"
                );
            }
            other => panic!("expected the reply first, got {other:?}"),
        }
        match &turns[1] {
            Turn::User(_) => {
                let rendered = format!("{:?}", turns[1]);
                assert!(rendered.contains("read foo.rs"), "{rendered}");
            }
            other => panic!("expected the digest as a user turn, got {other:?}"),
        }
    }

    #[test]
    fn text_alongside_tool_calls_survives_the_step_split() {
        let blocks = vec![
            AssistantBlock::Text("Let me check the config first.".into()),
            AssistantBlock::ToolCall {
                id: "call-1".into(),
                call_id: Some("call-1".into()),
                name: "read".into(),
                args: r#"{"path":"config.toml"}"#.into(),
            },
        ];

        match blocks_to_step(&blocks) {
            Step::ToolCalls { calls, text } => {
                assert_eq!(calls.len(), 1);
                assert_eq!(text, "Let me check the config first.");
            }
            Step::Final(_) => panic!("a tool call must not read as a final answer"),
        }
    }

    /// Reasoning is echoed back into history verbatim but must not be mistaken
    /// for the model's answer — a round that only reasoned is not a final reply.
    #[test]
    fn reasoning_never_becomes_the_answer() {
        let blocks = vec![
            AssistantBlock::Reasoning(komo_provider::types::Reasoning {
                id: Some("rs_1".into()),
                summary: vec!["thinking".into()],
                encrypted: Some("OPAQUE".into()),
            }),
            AssistantBlock::Text("the answer".into()),
        ];
        match blocks_to_step(&blocks) {
            Step::Final(text) => assert_eq!(text, "the answer"),
            Step::ToolCalls { .. } => panic!("no tool was called"),
        }
    }

    #[test]
    fn merging_params_keeps_unrelated_keys_and_overrides_collisions() {
        let merged = merge_params(
            Some(json!({ "store": false, "reasoning": { "effort": "low" } })),
            json!({ "reasoning": { "effort": "high" } }),
        );
        assert_eq!(merged["store"], false);
        assert_eq!(merged["reasoning"]["effort"], "high");
        // No prior params, or a non-object one, is simply replaced.
        assert_eq!(merge_params(None, json!({ "a": 1 })), json!({ "a": 1 }));
        assert_eq!(
            merge_params(Some(Value::Null), json!({ "a": 1 })),
            json!({ "a": 1 })
        );
    }
}
