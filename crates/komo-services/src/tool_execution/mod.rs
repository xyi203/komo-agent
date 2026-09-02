//! Tool execution as one deep module (architecture deepening plan §6).
//!
//! [`ToolExecutor`] owns the whole execution pipeline the agent loop used to
//! assemble by hand: catalog lookup, per-turn call budget, arg redaction,
//! panic-isolated spawning, transient-error retry, run-ledger recording, the
//! LLM-facing result cap, and error→outcome mapping. Callers hand it a round
//! of model-requested calls plus an explicit [`ToolTurnContext`] and get back
//! `ToolOutcome`s ready for `TurnDriver::step` — they never see lookup, retry,
//! ledger, or cap decisions. Execution policy (result cap, call budget) is
//! **instance-owned** [`ToolExecutionConfig`], not process globals, so two
//! executors can carry different policies.
//!
//! Every execution path funnels through the one internal core, reached via
//! [`ToolExecutor::execute_round`]: the LLM backend only ever *declares* the
//! tools to the provider, so nothing else can run one.

pub mod context;
mod result;
/// Transient-error classification. `pub(crate)` because the LLM adapter retries
/// its completions on the same classification (`infra::llm::with_retry`) — one
/// definition of "transient", so the tool path and the model path can't drift.
pub(crate) mod retry;

use std::sync::Arc;
use std::time::Duration;

use tracing::{Instrument, info, info_span, warn};

pub use context::{
    RunContext, SessionContext, SessionOrigin, SpinDetector, SpinVerdict, ToolContext,
    ToolTurnContext, TurnResultBudget, current_job_grants, current_session, with_job_grants,
    with_session,
};

use crate::tool_output_store::{Bounded, ToolOutputStore};
use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
use komo_core::domain::catalog::{CatalogSnapshot, ToolCatalog};
use komo_core::domain::context::ApprovalGate;
use komo_core::domain::events::TurnEvent;
use komo_core::domain::hooks::{HookDecision, ToolHook};
use komo_core::domain::llm::{ToolCallReq, ToolOutcome};
use komo_core::domain::policy::{Access, Category, Policy};
use komo_core::domain::repository::SessionEventRepository;
use komo_core::domain::run::{RunStep, STEP_FIELD_CAP, truncate};
use komo_core::domain::session_event::{
    SessionEventKind, ToolCallSettledEvent, ToolCallStartedEvent, ToolOutcome as SettledOutcome,
};
use komo_core::domain::tool::{Tool, ToolError};

/// Live `TurnEvent` args/result use the **ledger's** cap, not a smaller one of
/// their own. A watcher renders a running call from the stream and the same call
/// from the ledger after a reload with one component, so a tighter live cap
/// showed a truncated result that silently grew on reload. The stream is
/// loopback SSE — a couple of KB per call costs nothing worth that.
const EVENT_SUMMARY_CAP: usize = STEP_FIELD_CAP;

use context::{JOB_GRANTS, SESSION};
use result::cap_tool_result;
use retry::{TOOL_RETRY_BACKOFF_MS, TOOL_RETRY_MAX_ATTEMPTS, settle, should_retry};

/// Soft per-turn tool-call budget default (backstop). The runtime's
/// `max_turns` bounds *round-trips*, but a single round can request many tools
/// at once; this caps the *total* calls per turn so a runaway loop can't fan
/// out unbounded. Set generously above any legitimate turn. Enforced against
/// the run-ledger seq, so it applies only to ledgered turns (the main agent),
/// never to callers without a run context.
const DEFAULT_MAX_TOOL_CALLS_PER_TURN: i64 = 500;

/// Hard ceiling on how many tool calls a *single* round may actually execute.
/// The per-turn budget bounds the total, but a single malformed round can
/// request thousands of calls at once; without this each one would spawn a
/// task and write a ledger step, flooding both. Calls past the ceiling get a
/// short note instead — logged, never silently dropped. Set well above any
/// legitimate parallel tool use in one round.
const MAX_CALLS_PER_ROUND: usize = 32;

/// A structured view over [`STEP_FIELD_CAP`] is replaced, not cut: half a JSON
/// document fails to parse, so every reader would have to treat a truncated cell
/// as corrupt. The marker keeps the cell valid and says what happened.
fn cap_structured(structured: serde_json::Value) -> serde_json::Value {
    if structured.is_null() {
        return structured;
    }
    let rendered = structured.to_string();
    if rendered.len() <= STEP_FIELD_CAP {
        return structured;
    }
    serde_json::json!({ "_elided": "structured view over the field cap", "bytes": rendered.len() })
}

/// Which `[policy]` category (and, for files, which access kind) a tool's
/// actions fall under — the mapping behind [`ToolExecutor::drop_policy_denied`].
///
/// `None` means "not subject to the permission policy": `time`, `todo`, `task`,
/// `memory`, `skill`, … carry no [`ActionRef`], so no rule can ever deny them and
/// they are never filtered. A tool added without an entry here defaults to that
/// safe side — it stays advertised.
///
/// [`ActionRef`]: komo_core::domain::approval::ActionRef
fn policy_scope(name: &str) -> Option<(Category, Option<Access>)> {
    match name {
        "shell" => Some((Category::Shell, None)),
        "read" | "grep" | "glob" | "logs" => Some((Category::File, Some(Access::Read))),
        "write" | "edit" | "apply_patch" => Some((Category::File, Some(Access::Write))),
        "web_fetch" | "web_search" => Some((Category::Network, None)),
        "homeassistant" => Some((Category::HomeAssistant, None)),
        "wiki_index" | "wiki_read" => Some((Category::Wiki, None)),
        // Every mounted MCP tool is named `mcp__<server>__<tool>`, so one
        // prefix covers them all — a `deny mcp any` rule drops the lot from the
        // catalog rather than paying a schema each to refuse them per call.
        name if name.starts_with("mcp__") => Some((Category::Mcp, None)),
        // Same trick for plugin-registered tools (`py__<tool>`): one prefix
        // covers a set whose members are only known at runtime, so a
        // `deny plugin any` rule is enforceable even though the names were not.
        name if name.starts_with("py__") => Some((Category::Plugin, None)),
        _ => None,
    }
}

/// Instance-owned execution policy.
#[derive(Debug, Clone, Copy)]
pub struct ToolExecutionConfig {
    /// Byte cap on a single tool result handed back to the LLM.
    pub max_result_bytes: usize,
    /// Cumulative per-turn cap on tool output fed back to the model (`0` =
    /// unlimited). Enforced via the turn's [`TurnResultBudget`].
    pub max_turn_result_bytes: usize,
    /// Per-turn cap on ledgered tool calls (logical calls, not retry attempts).
    pub max_calls_per_turn: i64,
    /// Wall-clock timeout for one tool call (`None` = no timeout). A hung tool
    /// is aborted and the call fails cleanly rather than wedging the turn.
    pub max_call_duration: Option<Duration>,
}

impl Default for ToolExecutionConfig {
    fn default() -> Self {
        Self {
            max_result_bytes: komo_config::DEFAULT_MAX_TOOL_RESULT_BYTES,
            max_turn_result_bytes: komo_config::DEFAULT_MAX_TURN_RESULT_BYTES,
            max_calls_per_turn: DEFAULT_MAX_TOOL_CALLS_PER_TURN,
            max_call_duration: Some(Duration::from_secs(komo_config::DEFAULT_TOOL_TIMEOUT_SECS)),
        }
    }
}

impl ToolExecutionConfig {
    /// The default policy with a specific single-result cap (the most commonly
    /// tuned setting, via `max_tool_result_bytes`).
    pub fn with_result_cap(max_result_bytes: usize) -> Self {
        Self {
            max_result_bytes,
            ..Self::default()
        }
    }

    /// Set the cumulative per-turn output budget (`0` = unlimited).
    pub fn with_turn_budget(mut self, max_turn_result_bytes: usize) -> Self {
        self.max_turn_result_bytes = max_turn_result_bytes;
        self
    }

    /// Set the per-call wall-clock timeout (`0` seconds = no timeout).
    pub fn with_call_timeout_secs(mut self, secs: u64) -> Self {
        self.max_call_duration = (secs > 0).then(|| Duration::from_secs(secs));
        self
    }
}

/// The tool-execution module's external interface. Cheap to clone (one `Arc`);
/// every caller shares the same core, so all execution paths carry identical
/// retry/ledger/cap semantics.
#[derive(Clone)]
pub struct ToolExecutor {
    core: Arc<ToolExecutionCore>,
}

/// The shared implementation: the catalog plus the execution policy and the
/// approver every migrated tool reaches through its [`ToolContext`].
pub struct ToolExecutionCore {
    /// What the model may call. Shared with whoever declares the schemas, so
    /// the set the model is told about and the set this dispatches against are
    /// the same object — see [`ToolCatalog`].
    ///
    /// Dispatch reads a [`CatalogSnapshot`], not this: within a turn the
    /// catalog may change under us, and a call the model was invited to make
    /// must not answer "unknown tool" a round later.
    catalog: Arc<ToolCatalog>,
    /// Set when this executor is pinned to one turn's view
    /// ([`ToolExecutor::pin`]). `None` — the wiring-time and test executors —
    /// reads the catalog's current snapshot per round.
    pinned: Option<Arc<CatalogSnapshot>>,
    config: ToolExecutionConfig,
    /// The approver placed into each call's [`ToolContext`]. Defaults to
    /// deny-all; wiring installs the real (policy-wrapped) approver via
    /// [`ToolExecutor::with_approver`].
    approver: Arc<dyn Approver>,
    /// Where an over-limit result is kept in full. `None` ⇒ over-limit results
    /// are truncated, with the tail lost — the behavior before roadmap item 10.
    output_store: Option<Arc<ToolOutputStore>>,
    /// Tool hooks, run around every call in registration order (first `Deny`
    /// wins). Registered during wiring; a pinned executor carries the same set.
    hooks: Vec<Arc<dyn ToolHook>>,
    /// Where a tool call is recorded in the session's event log, so the log
    /// holds the work and not only what was said. `None` (tests, aux executors)
    /// ⇒ nothing is recorded, which is what the transcript looked like before.
    events: Option<Arc<dyn SessionEventRepository>>,
}

impl ToolExecutor {
    pub fn new(config: ToolExecutionConfig) -> Self {
        Self::with_catalog(Arc::new(ToolCatalog::new()), config)
    }

    /// An executor over an existing catalog — the wiring path, where the model
    /// backend needs the same catalog to declare schemas from.
    pub fn with_catalog(catalog: Arc<ToolCatalog>, config: ToolExecutionConfig) -> Self {
        Self {
            core: Arc::new(ToolExecutionCore {
                catalog,
                pinned: None,
                config,
                approver: Arc::new(DenyAllApprover),
                output_store: None,
                hooks: Vec::new(),
                events: None,
            }),
        }
    }

    /// The catalog this executor dispatches against, for a caller that mounts
    /// tools into it or declares its schemas.
    pub fn catalog(&self) -> &Arc<ToolCatalog> {
        &self.core.catalog
    }

    /// An executor pinned to the catalog as it is *now*, for one turn.
    ///
    /// The runtime pins at turn start and uses the result for every round: the
    /// model is handed one set of schemas, so the executor has to keep
    /// dispatching against that set even if a plugin mounts or unmounts
    /// mid-turn. The mutation is not lost — the next turn pins the new set.
    pub fn pin(&self) -> Self {
        let snapshot = self.snapshot();
        Self {
            core: Arc::new(ToolExecutionCore {
                catalog: self.core.catalog.clone(),
                pinned: Some(snapshot),
                config: self.core.config,
                approver: self.core.approver.clone(),
                output_store: self.core.output_store.clone(),
                hooks: self.core.hooks.clone(),
                events: self.core.events.clone(),
            }),
        }
    }

    /// The catalog view this executor reads: its pinned turn snapshot, else the
    /// catalog's current one.
    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.core.snapshot()
    }

    /// A non-owning handle, for a tool that needs to dispatch *other* tools.
    ///
    /// Weak on purpose: such a tool lives in the very catalog this executor
    /// reads, so an owning handle would be a cycle that never drops. Nothing is
    /// lost by it — an executor gone while one of its own tools is running
    /// cannot happen, and if it somehow did, [`WeakToolExecutor::upgrade`]
    /// saying so beats a leak.
    pub fn downgrade(&self) -> WeakToolExecutor {
        WeakToolExecutor {
            core: Arc::downgrade(&self.core),
        }
    }

    /// Install the approver handed to every tool via its [`ToolContext`]. Called
    /// during wiring before the executor is shared (like [`register`]).
    pub fn with_approver(mut self, approver: Arc<dyn Approver>) -> Self {
        let core = Arc::get_mut(&mut self.core)
            .expect("set the approver during wiring, before the executor is shared");
        core.approver = approver;
        self
    }

    /// Install the store that keeps an over-limit result in full, so the model
    /// gets a head+tail preview and a path instead of a one-sided truncation.
    /// Absent (tests, and any executor wiring hasn't given one) ⇒ plain
    /// truncation, the previous behavior.
    pub fn with_output_store(mut self, store: Arc<ToolOutputStore>) -> Self {
        let core = Arc::get_mut(&mut self.core)
            .expect("set the output store during wiring, before the executor is shared");
        core.output_store = Some(store);
        self
    }

    /// Install the transcript a tool call is recorded in. Absent ⇒ calls are
    /// not recorded there, which is every aux executor: their sessions are
    /// synthetic and a file per one-shot turn is litter.
    pub fn with_events(mut self, events: Arc<dyn SessionEventRepository>) -> Self {
        let core = Arc::get_mut(&mut self.core)
            .expect("set the transcript during wiring, before the executor is shared");
        core.events = Some(events);
        self
    }

    /// Register a tool hook, run around every call in registration order.
    /// Wiring-time only, like [`register`](Self::register) — frozen once the
    /// executor is shared.
    pub fn add_hook(&mut self, hook: Arc<dyn ToolHook>) {
        let core = Arc::get_mut(&mut self.core)
            .expect("register hooks during wiring, before the executor is shared");
        core.hooks.push(hook);
    }

    /// Add a tool for the life of the process (the wiring path). A tool that
    /// can be taken back out is mounted on the [`catalog`](Self::catalog)
    /// instead, which hands back a guard.
    ///
    /// Takes `&mut self` although the catalog no longer needs it: registering
    /// through an executor is the wiring-time gesture, and the borrow keeps it
    /// from being reached for once the executor is shared.
    pub fn register(&mut self, tool: Arc<dyn Tool>) {
        self.core.catalog.register(tool);
    }

    /// The catalog as the model adapter needs it (schemas for function
    /// calling), name-sorted so the serialized tool block is byte-stable — a
    /// provider prompt cache matches on exact bytes. A read-only view —
    /// execution always goes through the executor.
    pub fn definitions(&self) -> Vec<Arc<dyn Tool>> {
        self.snapshot().tools().cloned().collect()
    }

    /// Drop the tools `policy` denies outright, returning their names (sorted) so
    /// wiring can log what it removed. Called during wiring, right after
    /// registration and before the catalog is read — the prompt's tool-name list
    /// and the model's function schemas both come from [`definitions`], so
    /// filtering here keeps them from ever disagreeing about what exists.
    ///
    /// Only a wholly-denied tool goes (see [`Policy::wholly_denied`]): a tool
    /// that *can* act, just not everywhere, stays advertised and refuses the
    /// individual call — the model gets an explanation it can work with, which a
    /// missing tool never is.
    ///
    /// [`definitions`]: Self::definitions
    /// [`Policy::wholly_denied`]: komo_core::domain::policy::Policy::wholly_denied
    pub fn drop_policy_denied(&mut self, policy: &Policy) -> Vec<String> {
        let mut removed = self.core.catalog.retain(|name| {
            policy_scope(name)
                .is_some_and(|(category, access)| policy.wholly_denied(category, access))
        });
        removed.sort();
        removed
    }

    /// The cumulative per-turn tool-output budget this executor enforces (`0` =
    /// unlimited). The runtime seeds each turn's [`TurnResultBudget`] from it.
    pub fn turn_result_cap(&self) -> usize {
        self.core.config.max_turn_result_bytes
    }

    /// Execute one round of model-requested tool calls concurrently, preserving
    /// order. Unknown tools and tool errors are mapped into the outcome content
    /// (the model can recover); nothing here aborts the turn.
    ///
    /// Concurrency is safe for approval prompts: the interactive approver
    /// serializes them per session, so two side-effecting tools in one round
    /// still prompt one at a time.
    pub async fn execute_round(
        &self,
        calls: &[ToolCallReq],
        context: &ToolTurnContext,
    ) -> Vec<ToolOutcome> {
        // One view for the whole round, so two calls in it can never see
        // different catalogs. On a pinned executor this is the turn's view.
        let catalog = self.snapshot();
        // Bound the per-round fan-out: a single malformed round can request far
        // more calls than any real parallel tool use. Calls past the ceiling get
        // a note without spawning a task or writing a ledger step, so a runaway
        // round can't flood either. Never silent — log what was skipped.
        if calls.len() > MAX_CALLS_PER_ROUND {
            warn!(
                requested = calls.len(),
                ceiling = MAX_CALLS_PER_ROUND,
                "tool round exceeded the per-round call ceiling; extra calls skipped"
            );
        }
        // Decide the spin verdicts here, in dispatch order, before anything is
        // spawned: the calls themselves run concurrently, so asking mid-flight
        // would make "three in a row" depend on which future happened to get
        // there first. See `SpinDetector`.
        let verdicts: Vec<SpinVerdict> = calls
            .iter()
            .map(|call| context.spin.observe(&call.name, &call.args))
            .collect();

        // Every call in the round is dispatched at once below, so the whole
        // round's intent is one append and **one** durable flush — not an fsync
        // per call. Written before anything runs, because "the tool never
        // started" and "it started and we lost the answer" need different
        // answers on recovery and are otherwise indistinguishable.
        //
        // Best-effort like the ledger, with one difference: a failed flush
        // means the round's intent did not survive, so recovery will read those
        // calls as never-dispatched. Logged loudly for that reason.
        if let (Some(events), Some(run)) = (&self.core.events, &context.run) {
            let started: Vec<SessionEventKind> = calls
                .iter()
                .take(MAX_CALLS_PER_ROUND)
                .enumerate()
                .map(|(i, call)| {
                    SessionEventKind::ToolCallStarted(ToolCallStartedEvent {
                        turn_id: run.run_id.clone(),
                        call_id: call.call_id.clone().unwrap_or_else(|| call.id.clone()),
                        call_index: i as u32,
                        tool: call.name.clone(),
                        args: catalog
                            .get(&call.name)
                            .map(|tool| tool.redact_args(&call.args))
                            .unwrap_or_else(|| call.args.clone()),
                    })
                })
                .collect();
            let session = context.session.session_id.clone();
            if let Err(error) = events.append(&session, started).await {
                warn!(%error, "failed to record the round's dispatch intent (non-fatal)");
            } else if let Err(error) = events.durable_flush(&session).await {
                warn!(%error, "the round's dispatch intent is not durable; a crash now would read these calls as never started");
            }
        }

        let catalog = &catalog;
        let futures = calls.iter().zip(&verdicts).enumerate().map(
            |(i, (call, verdict))| async move {
                // Only a call that actually reached its tool has one; every
                // refusal below answers with text alone.
                let mut structured = serde_json::Value::Null;
                let content = if i >= MAX_CALLS_PER_ROUND {
                    format!(
                        "error: too many tool calls in one round (limit {MAX_CALLS_PER_ROUND}); \
                         this call was skipped. Request fewer tools per round."
                    )
                } else if matches!(verdict, SpinVerdict::Refuse | SpinVerdict::Stop) {
                    warn!(
                        tool = %call.name,
                        "identical tool call repeated; refusing to run it again"
                    );
                    format!(
                        "error: `{}` was already called twice with these exact arguments and \
                         returned the same result; running it again cannot change anything. \
                         Use what those calls returned, try a different approach, or answer \
                         the user with what you have.",
                        call.name
                    )
                } else if call.args.trim().is_empty() {
                    // Empty arguments mean the model's output was cut off
                    // mid-call — a tool that genuinely takes none still gets
                    // `{}`. Running it would act on whatever the defaults are
                    // rather than on what was asked.
                    format!(
                        "error: the arguments for `{}` arrived empty, which usually means the \
                         response was truncated. Re-issue the call with its arguments.",
                        call.name
                    )
                } else if let Some(denial) = self.core.pre_hooks(call).await {
                    // A hook refused the call. The message rides back as the
                    // outcome content — append-only, so the model can adjust
                    // without the prompt prefix ever changing.
                    denial
                } else {
                    match catalog.get(&call.name) {
                        Some(tool) => match self
                            .core
                            .execute(
                                tool.clone(),
                                call.args.clone(),
                                context,
                                call.call_id.as_deref().unwrap_or(&call.id),
                                i as u32,
                            )
                            .await
                        {
                            Ok((out, view)) => {
                                structured = view;
                                out
                            }
                            Err(error) => format!("tool `{}` failed: {error:#}", call.name),
                        },
                        None => format!("error: unknown tool `{}`", call.name),
                    }
                };
                let outcome = ToolOutcome {
                    id: call.id.clone(),
                    call_id: call.call_id.clone(),
                    content,
                    structured,
                };
                // Post hooks observe what the model will see — including
                // refusals and error content — for every call in the round.
                for hook in &self.core.hooks {
                    hook.post_execute(call, &outcome).await;
                }
                outcome
            },
        );
        futures_util::future::join_all(futures).await
    }
}

/// A [`ToolExecutor`] handle that does not keep it alive. See
/// [`ToolExecutor::downgrade`].
#[derive(Clone)]
pub struct WeakToolExecutor {
    core: std::sync::Weak<ToolExecutionCore>,
}

impl WeakToolExecutor {
    /// The executor, if it still exists.
    pub fn upgrade(&self) -> Option<ToolExecutor> {
        self.core.upgrade().map(|core| ToolExecutor { core })
    }
}

impl ToolExecutionCore {
    /// This core's catalog view: the pinned turn snapshot when there is one,
    /// else whatever the catalog holds right now.
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        match &self.pinned {
            Some(pinned) => pinned.clone(),
            None => self.catalog.snapshot(),
        }
    }

    /// Consult the tool hooks before a call runs. Registration order; the
    /// first `Deny` short-circuits (waterfall semantics) and its message is
    /// returned as the call's outcome content.
    async fn pre_hooks(&self, call: &ToolCallReq) -> Option<String> {
        for hook in &self.hooks {
            if let HookDecision::Deny(reason) = hook.pre_execute(call).await {
                warn!(tool = %call.name, hook = hook.name(), "tool call denied by hook");
                return Some(reason);
            }
        }
        None
    }

    /// Run one tool call through the full pipeline. The invariant order:
    ///
    /// 1. claim a ledger seq (budget counts logical calls, not attempts)
    /// 2. redact args for the audit record
    /// 3. execute on an isolated, panic-catching task with the session context
    ///    installed and a `tool` tracing span
    /// 4. map panics/cancellation to errors
    /// 5. retry per the transient classification (typed hint first)
    /// 6. record the (original, truncated) step — best-effort
    /// 7. cap the LLM-facing result
    ///
    /// Answers with the model-facing text *and* the tool's structured view: the
    /// text has been capped and may have been swapped for a preview, so a caller
    /// that needs the result as data cannot recover it by parsing what comes
    /// back. `Null` for a tool that reports no structured view.
    pub async fn execute(
        &self,
        tool: Arc<dyn Tool>,
        input: String,
        context: &ToolTurnContext,
        call_id: &str,
        call_index: u32,
    ) -> anyhow::Result<(String, serde_json::Value)> {
        let name = tool.name();

        // Ledger bookkeeping (only when this turn is recorded). Capture the
        // redacted args and seq up front: the raw `input` is cloned per attempt
        // below, and the seq must be claimed before the tool runs so the span
        // and the persisted step agree.
        let ledger = context.run.as_ref().map(|r| (r, r.next_seq()));
        let redacted_args = ledger.as_ref().map(|_| tool.redact_args(&input));
        let started_at = now();
        // Wall-clock timestamps (`now()`) are integer unix seconds — fine for
        // the ledger's started/ended fields, but differencing them only yields
        // whole seconds, so any sub-second tool would log `elapsed_ms = 0`.
        // Measure the duration off a monotonic `Instant` instead.
        let started_instant = std::time::Instant::now();
        let seq_field = ledger.as_ref().map(|(_, s)| *s).unwrap_or(-1);

        // Live event: a watcher (streaming client) sees the call start. Args are
        // the redacted form when ledgered, else redacted on the spot — never the
        // raw input. No-op when no sink is attached (the common case).
        if let Some(sink) = &context.session.event_sink {
            let args = redacted_args
                .clone()
                .unwrap_or_else(|| tool.redact_args(&input));
            sink.emit(TurnEvent::ToolStarted {
                seq: seq_field,
                name: name.to_string(),
                args: truncate(&args, EVENT_SUMMARY_CAP),
                started_at_ms: now_ms(),
            });
        }

        // Parse the model's JSON arguments once, here, so every tool sees a
        // typed `Value` and `parse_args` can produce the canonical
        // `InvalidInput` error. Args that aren't JSON at all (a model emitting
        // bare text) become a `Value::String`, which every tool's `parse_args`
        // rejects with that same canonical error — the text is preserved in it
        // so the model can see what it sent.
        let value = serde_json::from_str::<serde_json::Value>(&input)
            .unwrap_or_else(|_| serde_json::Value::String(input.clone()));

        // Filled in by a successful call below; stays `Null` for a failure or a
        // tool that has no structured view.
        let mut structured = serde_json::Value::Null;

        // Soft tool-call budget (backstop): once this turn has reached the cap,
        // refuse further calls with an error the model sees instead of
        // executing them. Inactive without a run ledger (seq_field = -1).
        let result: anyhow::Result<String> = if seq_field >= self.config.max_calls_per_turn {
            warn!(
                tool = name,
                seq = seq_field,
                budget = self.config.max_calls_per_turn,
                "tool-call budget reached for this turn; refusing"
            );
            Err(anyhow::anyhow!(
                "tool-call budget of {} reached for this turn; \
                 stop calling tools and answer the user with what you already have.",
                self.config.max_calls_per_turn
            ))
        } else {
            let mut attempt: usize = 0;
            let outcome: Result<komo_core::domain::tool::ToolOutput, ToolError> = loop {
                // Span so the tool's own logs carry the run's `seq`/`name`.
                // Spans don't cross `tokio::spawn` on their own — instrument
                // the spawned future. A fresh span per attempt keeps each
                // retry's logs distinct.
                let span = info_span!("tool", name, seq = seq_field, attempt);
                let tool_attempt = tool.clone();
                let value_attempt = value.clone();
                // Build the explicit per-call context, and also install the
                // turn's session and job grants as the ambient scope for the
                // spawned task — the approvers still read them (they don't
                // take a context parameter), and a fresh task doesn't inherit
                // task-locals.
                let mut ctx = ToolContext::new(
                    context.session.clone(),
                    context.run.clone(),
                    self.approver.clone(),
                );
                // Makes this call's approval a durable fact — the widest crash
                // window in a turn is a person deciding.
                if let (Some(events), Some(run)) = (&self.events, &context.run) {
                    ctx = ctx.with_approval_gate(ApprovalGate::new(
                        events.clone(),
                        &context.session.session_id,
                        &run.run_id,
                        call_id,
                        call_index,
                    ));
                }
                let scope = context.session.clone();
                let grants = current_job_grants();
                let join = tokio::spawn(
                    SESSION
                        .scope(
                            scope,
                            JOB_GRANTS.scope(grants, async move {
                                tool_attempt.call(value_attempt, &ctx).await
                            }),
                        )
                        .instrument(span),
                );
                // Wall-clock timeout backstop: a tool that hangs forever (a
                // shell command waiting on stdin, a timeout-less HTTP client)
                // would otherwise await indefinitely and wedge the turn — and
                // the session, since the loop can't finish. On elapse, abort the
                // task (kill_on_drop tools reap their child) and fail the call.
                // The message deliberately avoids the "timeout/timed out"
                // markers so the retry classifier treats it as terminal — a
                // wall-clock exhaustion won't succeed on an immediate retry.
                let abort = join.abort_handle();
                // A tool that legitimately waits (a sub-agent completion, a human
                // reading an approval prompt, a build it was given ten minutes
                // for) declares its own ceiling; the config default only applies
                // to tools for which waiting means hanging.
                let limit = tool.max_duration().or(self.config.max_call_duration);
                let joined = match limit {
                    Some(d) => match tokio::time::timeout(d, join).await {
                        Ok(r) => r,
                        Err(_) => {
                            abort.abort();
                            let elapsed = anyhow::anyhow!(
                                "did not report back within its {}s execution limit and was aborted",
                                d.as_secs()
                            );
                            // Aborting stops us waiting; it does not undo
                            // whatever the tool had already done. For an
                            // idempotent tool that distinction costs nothing —
                            // for any other, saying "failed" would invite the
                            // model to apply the effect a second time.
                            break Err(if tool.idempotent() {
                                ToolError::Failed(elapsed)
                            } else {
                                ToolError::Uncertain(elapsed)
                            });
                        }
                    },
                    None => join.await,
                };
                let attempt_result: Result<komo_core::domain::tool::ToolOutput, ToolError> =
                    match joined {
                        Ok(result) => result,
                        Err(join_err) if join_err.is_panic() => {
                            let panic = join_err.into_panic();
                            let msg = panic
                                .downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| panic.downcast_ref::<&str>().copied())
                                .unwrap_or("unknown panic");
                            Err(ToolError::Failed(anyhow::anyhow!(
                                "tool `{name}` panicked: {msg}"
                            )))
                        }
                        Err(join_err) => Err(ToolError::Failed(anyhow::anyhow!(
                            "tool `{name}` was cancelled: {join_err}"
                        ))),
                    };

                match &attempt_result {
                    // Only genuine failures retry; InvalidInput/Denied are
                    // recoverable and terminal.
                    Err(ToolError::Failed(error))
                        if attempt + 1 < TOOL_RETRY_MAX_ATTEMPTS
                            && should_retry(error, tool.idempotent()) =>
                    {
                        let delay =
                            TOOL_RETRY_BACKOFF_MS[attempt.min(TOOL_RETRY_BACKOFF_MS.len() - 1)];
                        warn!(
                            tool = name,
                            seq = seq_field,
                            attempt = attempt + 1,
                            delay_ms = delay,
                            error = %format!("{error:#}"),
                            "transient tool error; retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        attempt += 1;
                    }
                    // Not retrying is not the same as knowing nothing happened.
                    // An ambiguous error on a non-idempotent tool is exactly the
                    // case the retry classifier declined to touch — and until
                    // now the model was told "failed" and re-issued the call
                    // itself, which is the double-apply the classifier existed
                    // to prevent.
                    _ => break settle(attempt_result, tool.idempotent()),
                }
            };

            // Classify into the ledger-facing `anyhow::Result<String>`:
            // recoverable errors become model-facing content (never retried);
            // a genuine failure stays an `Err` so the ledger marks the step
            // failed and `execute_round` surfaces it.
            match outcome {
                Ok(out) => {
                    // The tool's machine-readable view rides to the ledger, not
                    // to the model — it never pays tokens for it.
                    structured = out.structured;
                    Ok(out.text)
                }
                Err(ToolError::InvalidInput(m)) => Ok(format!(
                    "invalid input for tool `{name}`: {m}. \
                     Rewrite the arguments to match the tool's schema."
                )),
                Err(ToolError::Denied(m)) => Ok(m),
                Err(ToolError::Failed(e)) => Err(e),
                // Still an `Err` — the call did not confirm success, so the
                // ledger marks the step failed. What changes is what the model
                // is told: the reply below composes into "tool `x` failed: did
                // not confirm …", which asks for a check rather than a retry.
                Err(ToolError::Uncertain(e)) => Err(anyhow::Error::new(
                    komo_core::domain::tool::UncertainOutcome::new(format!(
                        "did not confirm its result ({e:#}). It may or may not have taken \
                             effect — check the target's state before calling it again; repeating \
                             it blindly can apply the same change twice."
                    )),
                )),
            }
        };

        // Measured once and shared by the live event and the ledger step, so a
        // watcher and `run inspect` report the same duration for the same call.
        let elapsed_ms = started_instant.elapsed().as_millis() as i64;

        // Live event: the call finished (after retries collapse). Emitted
        // regardless of ledger state so a watcher sees every call resolve.
        if let Some(sink) = &context.session.event_sink {
            let (ok, summary) = match &result {
                Ok(out) => (true, truncate(out, EVENT_SUMMARY_CAP)),
                Err(e) => (false, truncate(&format!("{e:#}"), EVENT_SUMMARY_CAP)),
            };
            sink.emit(TurnEvent::ToolFinished {
                seq: seq_field,
                name: name.to_string(),
                ok,
                summary,
                elapsed_ms,
            });
        }

        // The ledger's view of the outcome, taken from the *original* result —
        // the audit record keeps what the model was not shown.
        let (ok, result_s, error_s) = match &result {
            Ok(out) => (true, truncate(out, STEP_FIELD_CAP), String::new()),
            Err(e) => (
                false,
                String::new(),
                truncate(&format!("{e:#}"), STEP_FIELD_CAP),
            ),
        };
        // Not `ok`, but not the same as failed either: the call may have landed
        // and only its answer was lost. An operator asking "did that go
        // through?" a week later needs the two told apart, and by here the
        // `ToolError` variant is gone — the marker rides in the error chain.
        let uncertain = result
            .as_ref()
            .err()
            .is_some_and(komo_core::domain::tool::UncertainOutcome::marks);

        // Size the model's view. Over the cap, the full output is written out and
        // the model gets a head+tail preview naming that file — so this has to
        // run before the step is recorded, which is what carries the path.
        let bounded = result.map(|out| self.bound(out, context, seq_field));

        // Record the step — best-effort, never affecting the tool's own result.
        // Retries collapse into this one step: the retry is a robustness
        // detail, not extra audit rows.
        if let (Some((run, seq)), Some(args)) = (ledger, redacted_args) {
            let ended_at = now();
            if ok {
                info!(tool = name, seq, elapsed_ms, "tool ok");
            } else {
                warn!(tool = name, seq, error = %error_s, "tool failed");
            }
            let step = RunStep {
                run_id: run.run_id.clone(),
                seq,
                tool_name: name.to_string(),
                args: truncate(&args, STEP_FIELD_CAP),
                result: result_s,
                error: error_s,
                ok,
                uncertain,
                started_at,
                ended_at,
                elapsed_ms,
                structured: cap_structured(structured.clone()),
                output_paths: bounded
                    .as_ref()
                    .map(|b| {
                        b.output_paths
                            .iter()
                            .map(|p| p.display().to_string())
                            .collect()
                    })
                    .unwrap_or_default(),
            };
            // The turn's own copy, for the tool note its closing message
            // carries. The ledger's step rows are a projection of the event
            // below, committed when the turn closes — so there is nothing to
            // read back from them while the turn is still running.
            run.record_step(step.clone());

            // Settled, in the session's own log — **per call**, the moment it
            // settles. Not once per round: a round of three that crashes with
            // two finished has two real results, and a single batched record
            // written at the end would report all three as lost. Written from
            // the step's values so the log inherits the ledger's redaction and
            // cap. Best-effort, like the step: the record of the work must
            // never cost the work.
            if let (Some(events), Some(run)) = (&self.events, &context.run) {
                let settled = SessionEventKind::ToolCallSettled(ToolCallSettledEvent {
                    turn_id: run.run_id.clone(),
                    call_id: call_id.to_string(),
                    call_index,
                    outcome: if step.uncertain {
                        SettledOutcome::Uncertain
                    } else if step.ok {
                        SettledOutcome::Succeeded
                    } else {
                        SettledOutcome::Failed
                    },
                    result: step.result.clone(),
                    error: step.error.clone(),
                    elapsed_ms,
                    structured: step.structured.clone(),
                    output_paths: step.output_paths.clone(),
                });
                if let Err(error) = events
                    .append(&context.session.session_id, vec![settled])
                    .await
                {
                    warn!(%error, tool = name, "failed to record the settled call (non-fatal)");
                }
            }
        }

        // Charge the bounded result against the turn's cumulative budget: once
        // the turn is over budget, it is swapped for a short note so a long tool
        // chain can't quietly overflow the context window (the ledger — and, for
        // an over-limit result, the stored file — still have the real thing).
        let text = bounded.map(|b| match context.budget.admit(b.text) {
            Ok(out) => out,
            Err(note) => note,
        })?;
        Ok((text, structured))
    }

    /// Size one result for the model: the store's head+tail preview when a store
    /// is wired and this is a ledgered turn, else the old one-sided truncation.
    ///
    /// The store is skipped without a ledger seq (aux sub-agents, sweeps): those
    /// have no run to point an operator back at and no `read`-capable follow-up
    /// turn, so a file on disk nobody will open is just litter.
    fn bound(&self, out: String, context: &ToolTurnContext, seq: i64) -> Bounded {
        let cap = self.config.max_result_bytes;
        match (&self.output_store, seq >= 0) {
            (Some(store), true) => store.bound(
                &context.session.session_id,
                &format!(
                    "{}-{seq:04}",
                    context
                        .run
                        .as_ref()
                        .map(|r| r.run_id.as_str())
                        .unwrap_or("run")
                ),
                out,
                cap,
            ),
            _ => Bounded {
                text: cap_tool_result(out, cap),
                output_paths: Vec::new(),
            },
        }
    }
}

/// The executor's default approver until wiring installs the real one
/// (`ToolExecutor::with_approver`): deny everything. A tool that reaches
/// `ctx.approve(..)` before an approver is set is refused rather than silently
/// allowed — matters only in tests that don't exercise a migrated gated tool.
struct DenyAllApprover;

#[async_trait::async_trait]
impl Approver for DenyAllApprover {
    async fn decide(&self, _request: &ApprovalRequest) -> Decision {
        Decision::deny_because("没有配置审批入口（executor 未安装 approver）")
    }
}

fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

/// Unix milliseconds. The ledger keeps whole seconds, but a live watcher renders
/// a ticking duration off the start instant, and whole seconds make every
/// sub-second call read as zero.
fn now_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}

#[cfg(test)]
mod tests {
    //! Behavior tests through the `ToolExecutor` interface — a round of calls
    //! in, outcomes out. The executor owns lookup/retry/ledger/cap, so that is
    //! where they are asserted.

    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::tool::ToolOutput;
    use serde_json::Value;

    /// Render args as the tool's *payload* rather than as JSON: these tests pass
    /// bare text (the non-JSON arg path), and asserting on `"x"` with quotes
    /// would be asserting about serde, not about the executor.
    fn arg_text(v: &Value) -> String {
        v.as_str()
            .map(str::to_string)
            .unwrap_or_else(|| v.to_string())
    }
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EchoTool;
    #[async_trait]
    impl Tool for EchoTool {
        fn name(&self) -> &'static str {
            "echo"
        }
        fn description(&self) -> &'static str {
            "echoes its input"
        }
        async fn call(&self, input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text(format!("echoed: {}", arg_text(&input))))
        }
    }

    /// A stand-in registered under a real tool's name, so the catalog filter is
    /// tested against the names it actually maps (`policy_scope`).
    struct NamedTool(&'static str);
    #[async_trait]
    impl Tool for NamedTool {
        fn name(&self) -> &'static str {
            self.0
        }
        fn description(&self) -> &'static str {
            "stand-in"
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("ok"))
        }
    }

    fn catalog_with(names: &[&'static str]) -> ToolExecutor {
        let mut tools = ToolExecutor::new(ToolExecutionConfig::default());
        for name in names {
            tools.register(Arc::new(NamedTool(name)));
        }
        tools
    }

    fn wildcard_deny(category: Category, access: Option<Access>) -> Policy {
        use komo_core::domain::policy::{Effect, Matcher, Rule, Verdict};
        Policy::new(
            vec![Rule {
                channels: None,
                category,
                matcher: Matcher::Any,
                value: String::new(),
                access,
                effect: Effect::Deny,
                include_dangerous: false,
                unattended: false,
            }],
            Verdict::Ask,
        )
    }

    const FILE_AND_SHELL: &[&str] = &[
        "read",
        "grep",
        "glob",
        "write",
        "edit",
        "apply_patch",
        "shell",
        "time",
        "memory",
    ];

    #[test]
    fn a_wholly_denied_tool_leaves_the_catalog() {
        let mut tools = catalog_with(FILE_AND_SHELL);
        assert_eq!(
            tools.drop_policy_denied(&wildcard_deny(Category::Shell, None)),
            vec!["shell".to_string()]
        );
        let left: std::collections::BTreeSet<String> = tools
            .definitions()
            .iter()
            .map(|t| t.name().into())
            .collect();
        assert!(!left.contains("shell"));
        assert!(left.contains("read"), "only the denied tool goes");
        assert!(left.contains("time"));
    }

    /// Banning writes must not take the readers away — the file category is the
    /// one that splits, and losing `read`/`grep` here would blind the model.
    #[test]
    fn denying_file_writes_keeps_the_readers() {
        let mut tools = catalog_with(FILE_AND_SHELL);
        let dropped = tools.drop_policy_denied(&wildcard_deny(
            Category::File,
            Some(komo_core::domain::policy::Access::Write),
        ));
        assert_eq!(dropped, vec!["apply_patch", "edit", "write"]);
        let left: std::collections::BTreeSet<String> = tools
            .definitions()
            .iter()
            .map(|t| t.name().into())
            .collect();
        assert!(left.contains("read") && left.contains("grep") && left.contains("glob"));
        assert!(left.contains("shell"), "shell is its own category");
    }

    #[test]
    fn an_empty_policy_drops_nothing() {
        let mut tools = catalog_with(FILE_AND_SHELL);
        assert!(tools.drop_policy_denied(&Policy::default()).is_empty());
        assert_eq!(tools.definitions().len(), FILE_AND_SHELL.len());
    }

    struct SecretTool;
    #[async_trait]
    impl Tool for SecretTool {
        fn name(&self) -> &'static str {
            "secretive"
        }
        fn description(&self) -> &'static str {
            "redacts its args"
        }
        fn redact_args(&self, _args: &str) -> String {
            "[redacted]".to_string()
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("done"))
        }
    }

    struct PanickingTool;
    #[async_trait]
    impl Tool for PanickingTool {
        fn name(&self) -> &'static str {
            "boom"
        }
        fn description(&self) -> &'static str {
            "always panics"
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            panic!("kaboom");
        }
    }

    /// A tool that fails its first `fail_times` calls (with `error_msg`) then
    /// succeeds, counting every call. Lets a test assert how many attempts the
    /// retry loop made.
    struct FlakyTool {
        calls: Arc<AtomicUsize>,
        fail_times: usize,
        error_msg: &'static str,
        idempotent: bool,
    }

    #[async_trait]
    impl Tool for FlakyTool {
        fn name(&self) -> &'static str {
            "flaky"
        }
        fn description(&self) -> &'static str {
            "fails a few times then succeeds"
        }
        fn idempotent(&self) -> bool {
            self.idempotent
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed);
            if n < self.fail_times {
                Err(ToolError::Failed(anyhow::anyhow!("{}", self.error_msg)))
            } else {
                Ok(ToolOutput::text("ok"))
            }
        }
    }

    fn flaky(
        fail_times: usize,
        error_msg: &'static str,
        idempotent: bool,
    ) -> (Arc<FlakyTool>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let tool = Arc::new(FlakyTool {
            calls: calls.clone(),
            fail_times,
            error_msg,
            idempotent,
        });
        (tool, calls)
    }

    fn executor(tools: Vec<Arc<dyn Tool>>, config: ToolExecutionConfig) -> ToolExecutor {
        let mut executor = ToolExecutor::new(config);
        for t in tools {
            executor.register(t);
        }
        executor
    }

    fn call(name: &str, args: &str) -> ToolCallReq {
        ToolCallReq {
            id: format!("id-{name}"),
            call_id: None,
            name: name.to_string(),
            args: args.to_string(),
        }
    }

    fn ledgered() -> ToolTurnContext {
        ToolTurnContext {
            session: SessionContext::detached("cli:test"),
            run: Some(RunContext::new("run-1".into())),
            budget: TurnResultBudget::unlimited(),
            spin: SpinDetector::default(),
        }
    }

    fn unledgered() -> ToolTurnContext {
        ToolTurnContext {
            session: SessionContext::detached("cli:test"),
            run: None,
            budget: TurnResultBudget::unlimited(),
            spin: SpinDetector::default(),
        }
    }

    async fn one(
        executor: &ToolExecutor,
        req: ToolCallReq,
        context: &ToolTurnContext,
    ) -> ToolOutcome {
        executor
            .execute_round(std::slice::from_ref(&req), context)
            .await
            .remove(0)
    }

    #[test]
    fn the_catalog_is_name_sorted_regardless_of_registration_order() {
        // The tool schemas are serialized into every request; a provider
        // prompt cache matches on exact bytes, so the order must be stable
        // across restarts no matter how wiring happens to register.
        let executor = executor(
            vec![
                Arc::new(NamedTool("zeta")),
                Arc::new(NamedTool("alpha")),
                Arc::new(NamedTool("mid")),
            ],
            ToolExecutionConfig::default(),
        );
        let names: Vec<&str> = executor.definitions().iter().map(|t| t.name()).collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    }

    /// A model that keeps re-issuing one call gets refused rather than served:
    /// the first two answers are already in the transcript, so a third run
    /// cannot say anything new — it would only burn rounds.
    #[tokio::test]
    async fn an_identical_call_repeated_is_refused_instead_of_run() {
        let (tool, calls) = flaky(0, "unused", false);
        let executor = executor(vec![tool], ToolExecutionConfig::default());
        let context = unledgered();

        for _ in 0..2 {
            let out = one(&executor, call("flaky", "{}"), &context).await;
            assert!(!out.content.starts_with("error:"), "{}", out.content);
        }
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        let refused = one(&executor, call("flaky", "{}"), &context).await;
        assert!(
            refused.content.contains("already called twice"),
            "{}",
            refused.content
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "the refused call must not reach the tool"
        );
        assert!(!context.spin.should_stop(), "one refusal is not a stop yet");

        // Asking again after the refusal ends the turn; the loop reads this.
        let _ = one(&executor, call("flaky", "{}"), &context).await;
        assert!(context.spin.should_stop());
    }

    /// Same tool, different arguments is progress — and anything else in
    /// between resets the streak, so a poll interleaved with real work never
    /// trips the detector.
    #[tokio::test]
    async fn differing_arguments_and_interleaved_calls_never_trip_the_detector() {
        let executor = executor(
            vec![Arc::new(EchoTool), Arc::new(NamedTool("other"))],
            ToolExecutionConfig::default(),
        );
        let context = unledgered();

        for i in 0..5 {
            let out = one(&executor, call("echo", &format!("{{\"n\":{i}}}")), &context).await;
            assert!(!out.content.contains("already called twice"));
        }
        // Alternating: each repeat is broken by the other tool.
        for _ in 0..5 {
            let echoed = one(&executor, call("echo", "{}"), &context).await;
            assert!(!echoed.content.contains("already called twice"));
            let _ = one(&executor, call("other", "{}"), &context).await;
        }
        assert!(!context.spin.should_stop());
    }

    /// Empty arguments mean the model's output was cut off mid-call. Running it
    /// would act on defaults rather than on what was asked — but a tool that
    /// genuinely takes none still sends `{}`, which must go through.
    #[tokio::test]
    async fn empty_arguments_are_refused_but_an_empty_object_is_not() {
        let (tool, calls) = flaky(0, "unused", false);
        let executor = executor(vec![tool], ToolExecutionConfig::default());
        let context = unledgered();

        let truncated = one(&executor, call("flaky", "   "), &context).await;
        assert!(
            truncated.content.contains("arrived empty"),
            "{}",
            truncated.content
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0, "nothing should have run");

        let no_args = one(&executor, call("flaky", "{}"), &context).await;
        assert!(
            !no_args.content.starts_with("error:"),
            "{}",
            no_args.content
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// A truncated call must not take the rest of its round down with it: the
    /// calls streamed before it are complete.
    #[tokio::test]
    async fn a_truncated_call_does_not_block_the_rest_of_its_round() {
        let executor = executor(vec![Arc::new(EchoTool)], ToolExecutionConfig::default());
        let outcomes = executor
            .execute_round(
                &[
                    call("echo", "{\"a\":1}"),
                    call("echo", ""),
                    call("echo", "{\"b\":2}"),
                ],
                &unledgered(),
            )
            .await;
        assert!(!outcomes[0].content.contains("arrived empty"));
        assert!(outcomes[1].content.contains("arrived empty"));
        assert!(!outcomes[2].content.contains("arrived empty"));
    }

    #[tokio::test]
    async fn round_preserves_order_and_maps_unknown_tools() {
        let executor = executor(vec![Arc::new(EchoTool)], ToolExecutionConfig::default());
        let outcomes = executor
            .execute_round(
                &[call("echo", "a"), call("nope", "{}"), call("echo", "b")],
                &unledgered(),
            )
            .await;
        assert_eq!(outcomes.len(), 3);
        assert_eq!(outcomes[0].content, "echoed: a");
        assert_eq!(outcomes[1].content, "error: unknown tool `nope`");
        assert_eq!(outcomes[2].content, "echoed: b");
        assert_eq!(outcomes[1].id, "id-nope", "ids line up with calls");
    }

    #[tokio::test]
    async fn ledgered_call_records_one_step() {
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let executor = executor(vec![Arc::new(EchoTool)], ToolExecutionConfig::default());
        let out = one(&executor, call("echo", "hi"), &context).await;
        assert_eq!(out.content, "echoed: hi");

        let steps = run.steps();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].run_id, "run-1");
        assert_eq!(steps[0].seq, 0);
        assert_eq!(steps[0].tool_name, "echo");
        assert!(steps[0].ok);
        assert!(steps[0].result.contains("echoed: hi"));
        assert!(steps[0].error.is_empty());
    }

    /// "Did that go through?" is the question an operator asks a week later,
    /// and a run the model was told was uncertain must not read as a plain
    /// failure in the ledger — by then the `ToolError` variant is long gone, so
    /// the marker has to survive the trip through `anyhow`.
    #[tokio::test]
    async fn an_uncertain_call_is_recorded_as_uncertain_not_merely_failed() {
        struct FlakyWriter;
        #[async_trait]
        impl Tool for FlakyWriter {
            fn name(&self) -> &'static str {
                "flaky_write"
            }
            fn description(&self) -> &'static str {
                "mutates something, ambiguously"
            }
            async fn call(&self, _i: Value, _c: &ToolContext) -> Result<ToolOutput, ToolError> {
                Err(ToolError::Failed(anyhow::anyhow!(
                    "upstream returned HTTP 503: service unavailable"
                )))
            }
        }

        let context = ledgered();
        let run = context.run.clone().unwrap();
        let executor = executor(vec![Arc::new(FlakyWriter)], ToolExecutionConfig::default());
        let out = one(&executor, call("flaky_write", "{}"), &context).await;
        assert!(
            out.content.contains("may or may not have taken effect"),
            "got: {}",
            out.content
        );

        let steps = run.steps();
        assert!(!steps[0].ok);
        assert!(
            steps[0].uncertain,
            "the ledger must keep failed and may-have-landed apart"
        );
    }

    #[tokio::test]
    async fn redaction_happens_before_the_ledger() {
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let executor = executor(vec![Arc::new(SecretTool)], ToolExecutionConfig::default());
        one(&executor, call("secretive", "token=hunter2"), &context).await;
        let steps = run.steps();
        assert_eq!(steps[0].args, "[redacted]");
        assert!(!steps[0].args.contains("hunter2"));
    }

    #[tokio::test]
    async fn panicking_tool_becomes_an_error_outcome_and_error_step() {
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let executor = executor(
            vec![Arc::new(PanickingTool)],
            ToolExecutionConfig::default(),
        );
        let out = one(&executor, call("boom", "{}"), &context).await;
        assert!(out.content.contains("panicked"), "got: {}", out.content);
        assert!(out.content.contains("kaboom"));

        let steps = run.steps();
        assert_eq!(steps.len(), 1);
        assert!(!steps[0].ok);
        assert!(steps[0].error.contains("panicked"));
        assert!(steps[0].result.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn connection_error_is_retried_even_for_non_idempotent_tool() {
        // The request never reached the server, so a side effect can't have
        // landed — safe to retry regardless of idempotency.
        let (tool, calls) = flaky(2, "connection refused", false);
        let executor = executor(vec![tool], ToolExecutionConfig::default());
        let out = one(&executor, call("flaky", "{}"), &unledgered()).await;
        assert_eq!(out.content, "ok");
        assert_eq!(calls.load(Ordering::Relaxed), 3); // 2 failures + 1 success
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_error_is_not_retried() {
        let (tool, calls) = flaky(usize::MAX, "invalid arguments: bad json", true);
        let executor = executor(vec![tool], ToolExecutionConfig::default());
        let out = one(&executor, call("flaky", "{}"), &unledgered()).await;
        assert!(out.content.contains("invalid arguments"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_error_is_retried_only_for_idempotent_tool() {
        // Idempotent → retried.
        let (tool, calls) = flaky(1, "operation timed out", true);
        let executor = self::executor(vec![tool], ToolExecutionConfig::default());
        let out = one(&executor, call("flaky", "{}"), &unledgered()).await;
        assert_eq!(out.content, "ok");
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        // Non-idempotent → a timeout might have applied server-side; don't retry.
        let (tool, calls) = flaky(usize::MAX, "operation timed out", false);
        let executor = self::executor(vec![tool], ToolExecutionConfig::default());
        let _ = one(&executor, call("flaky", "{}"), &unledgered()).await;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_are_bounded_then_error_surfaces() {
        let (tool, calls) = flaky(usize::MAX, "connection refused", false);
        let executor = executor(vec![tool], ToolExecutionConfig::default());
        let out = one(&executor, call("flaky", "{}"), &unledgered()).await;
        assert!(out.content.to_lowercase().contains("connection refused"));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            retry::TOOL_RETRY_MAX_ATTEMPTS
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_collapses_into_a_single_ledger_step() {
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let (tool, calls) = flaky(1, "connection refused", false);
        let executor = executor(vec![tool], ToolExecutionConfig::default());
        let out = one(&executor, call("flaky", "{}"), &context).await;
        assert_eq!(out.content, "ok");
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        let steps = run.steps();
        assert_eq!(
            steps.len(),
            1,
            "retries must record one step, not one per attempt"
        );
        assert!(steps[0].ok);
        assert_eq!(steps[0].seq, 0);
    }

    #[tokio::test]
    async fn budget_counts_logical_calls_and_refuses_past_the_cap() {
        let (tool, calls) = flaky(0, "unused", false); // never fails; just counts
        let executor = executor(
            vec![tool],
            ToolExecutionConfig {
                max_calls_per_turn: 5,
                ..Default::default()
            },
        );
        let context = ledgered();
        let run = context.run.clone().unwrap();
        // Distinct arguments per call: this exercises the *budget*, and calls
        // that are byte-identical would be stopped earlier by the spin detector.
        for i in 0..5 {
            let out = one(
                &executor,
                call("flaky", &format!("{{\"i\":{i}}}")),
                &context,
            )
            .await;
            assert_eq!(out.content, "ok");
        }
        // The next call is refused without ever reaching the tool.
        let out = one(&executor, call("flaky", "{\"i\":9}"), &context).await;
        assert!(out.content.contains("budget"), "got: {}", out.content);
        assert_eq!(calls.load(Ordering::Relaxed), 5);

        // The refusal is still recorded as a failed step, for audit visibility.
        let steps = run.steps();
        assert_eq!(steps.len(), 6);
        assert!(!steps.last().unwrap().ok);
        assert!(steps.last().unwrap().error.contains("budget"));
    }

    struct BigTool;
    #[async_trait]
    impl Tool for BigTool {
        fn name(&self) -> &'static str {
            "big"
        }
        fn description(&self) -> &'static str {
            "returns a large result"
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("x".repeat(10_000)))
        }
    }

    /// A structured view rides to the ledger and nowhere near the model — the
    /// whole point of the third view is that the context window doesn't pay for it.
    struct StructuredTool;
    #[async_trait]
    impl Tool for StructuredTool {
        fn name(&self) -> &'static str {
            "structured"
        }
        fn description(&self) -> &'static str {
            "returns a structured view"
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("done")
                .with_structured(serde_json::json!({ "exit": 0, "truncated": false })))
        }
    }

    #[tokio::test]
    async fn a_structured_view_reaches_the_ledger_but_not_the_model() {
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let executor = executor(
            vec![Arc::new(StructuredTool)],
            ToolExecutionConfig::default(),
        );
        let out = one(&executor, call("structured", "{}"), &context).await;

        assert_eq!(out.content, "done", "the model sees text only");
        let steps = run.steps();
        assert_eq!(
            steps[0].structured,
            serde_json::json!({ "exit": 0, "truncated": false })
        );
    }

    /// A tool that claims the cancel signal (as `shell` does) ends the call, and
    /// the step it leaves must read like the run's own stop — one wording for one
    /// event — and must **not** be retried: a deliberate stop is not a transient
    /// failure, and `web_fetch` is `idempotent`, so the classifier is what stands
    /// between a cancel and two more attempts.
    #[tokio::test]
    async fn a_claimed_cancel_ends_the_call_once_and_reads_as_cancelled() {
        use komo_core::domain::cancel::{CANCELLED_ERROR, CancelSignal, Cancelled};

        struct AlreadyCancelled;
        #[async_trait]
        impl CancelSignal for AlreadyCancelled {
            fn is_cancelled(&self) -> bool {
                true
            }
            async fn cancelled(&self) {}
        }

        /// Waits for the signal like `shell` does, counting attempts.
        struct Claiming(Arc<AtomicUsize>);
        #[async_trait]
        impl Tool for Claiming {
            fn name(&self) -> &'static str {
                "claiming"
            }
            fn description(&self) -> &'static str {
                "waits for cancellation"
            }
            fn idempotent(&self) -> bool {
                true
            }
            async fn call(
                &self,
                _input: Value,
                ctx: &ToolContext,
            ) -> Result<ToolOutput, ToolError> {
                self.0.fetch_add(1, Ordering::Relaxed);
                ctx.cancelled().await;
                Err(ToolError::Failed(Cancelled.into()))
            }
        }

        let attempts = Arc::new(AtomicUsize::new(0));
        let executor = executor(
            vec![Arc::new(Claiming(attempts.clone()))],
            ToolExecutionConfig::default(),
        );
        let context = ToolTurnContext {
            session: SessionContext::detached("cli:test").with_cancel(Arc::new(AlreadyCancelled)),
            run: Some(RunContext::new("run-1".into())),
            budget: TurnResultBudget::unlimited(),
            spin: SpinDetector::default(),
        };
        let run = context.run.clone().unwrap();

        let out = one(&executor, call("claiming", "{}"), &context).await;
        assert!(out.content.contains(CANCELLED_ERROR), "{}", out.content);
        assert_eq!(attempts.load(Ordering::Relaxed), 1, "a cancel is terminal");
        let steps = run.steps();
        assert!(!steps[0].ok);
        assert_eq!(steps[0].error, CANCELLED_ERROR);
    }

    #[tokio::test]
    async fn a_failed_call_records_no_structured_view() {
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let executor = executor(
            vec![Arc::new(PanickingTool)],
            ToolExecutionConfig::default(),
        );
        one(&executor, call("boom", "{}"), &context).await;
        let steps = run.steps();
        assert!(!steps[0].ok);
        assert!(steps[0].structured.is_null());
    }

    #[test]
    fn an_oversized_structured_view_is_replaced_rather_than_cut() {
        let big = serde_json::json!({ "blob": "x".repeat(STEP_FIELD_CAP) });
        let capped = cap_structured(big);
        assert!(capped["_elided"].is_string(), "{capped}");
        // Still valid JSON — a reader must never have to handle half a document.
        assert!(capped.is_object());
        // Under the cap it passes through untouched.
        let small = serde_json::json!({ "exit": 1 });
        assert_eq!(cap_structured(small.clone()), small);
        assert!(cap_structured(serde_json::Value::Null).is_null());
    }

    /// 10's core promise: an over-limit result keeps its tail, the full text is on
    /// disk, and the step says where.
    #[tokio::test]
    async fn an_over_limit_result_is_stored_and_previewed_with_its_path_on_the_step() {
        struct Chatty;
        #[async_trait]
        impl Tool for Chatty {
            fn name(&self) -> &'static str {
                "chatty"
            }
            fn description(&self) -> &'static str {
                "returns many lines"
            }
            async fn call(
                &self,
                _input: Value,
                _ctx: &ToolContext,
            ) -> Result<ToolOutput, ToolError> {
                Ok(ToolOutput::text(
                    (0..400).map(|i| format!("line {i}\n")).collect::<String>(),
                ))
            }
        }

        let root = std::env::temp_dir().join("komo_exec_output_store");
        let _ = std::fs::remove_dir_all(&root);
        let store = Arc::new(crate::tool_output_store::ToolOutputStore::new(root.clone()));
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let mut executor = ToolExecutor::new(ToolExecutionConfig {
            max_result_bytes: 512,
            ..Default::default()
        })
        .with_output_store(store);
        executor.register(Arc::new(Chatty));

        let out = one(&executor, call("chatty", "{}"), &context).await;
        assert!(out.content.contains("line 0"));
        assert!(out.content.contains("line 399"), "the tail must survive");

        let steps = run.steps();
        let stored = &steps[0].output_paths[0];
        assert!(out.content.contains(stored), "the preview names the file");
        assert!(
            std::fs::read_to_string(stored)
                .unwrap()
                .contains("line 200")
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    /// No ledger (aux sub-agent, a sweep) ⇒ no file: there is no run to point an
    /// operator at and no follow-up turn to `read` it, so it would be litter.
    #[tokio::test]
    async fn an_unledgered_call_truncates_instead_of_storing() {
        let root = std::env::temp_dir().join("komo_exec_output_store_unledgered");
        let _ = std::fs::remove_dir_all(&root);
        let store = Arc::new(crate::tool_output_store::ToolOutputStore::new(root.clone()));
        let mut executor = ToolExecutor::new(ToolExecutionConfig {
            max_result_bytes: 1024,
            ..Default::default()
        })
        .with_output_store(store);
        executor.register(Arc::new(BigTool));

        let out = one(&executor, call("big", "{}"), &unledgered()).await;
        assert!(out.content.contains("truncated"));
        assert!(!root.exists(), "nothing should be written without a ledger");
    }

    #[tokio::test]
    async fn two_executors_carry_different_result_caps() {
        // The cap is instance policy, not a process global: the same tool
        // through two executors gets two different ceilings.
        let tight = executor(
            vec![Arc::new(BigTool)],
            ToolExecutionConfig {
                max_result_bytes: 1024,
                ..Default::default()
            },
        );
        let roomy = executor(
            vec![Arc::new(BigTool)],
            ToolExecutionConfig {
                max_result_bytes: 64 * 1024,
                ..Default::default()
            },
        );
        let capped = one(&tight, call("big", "{}"), &unledgered()).await;
        assert!(capped.content.len() <= 1024 + 200);
        assert!(capped.content.contains("truncated"));

        let free = one(&roomy, call("big", "{}"), &unledgered()).await;
        assert_eq!(free.content.len(), 10_000, "no truncation under the cap");
    }

    #[tokio::test]
    async fn unledgered_context_records_nothing_and_still_works() {
        let executor = executor(vec![Arc::new(EchoTool)], ToolExecutionConfig::default());
        let out = one(&executor, call("echo", "x"), &unledgered()).await;
        assert_eq!(out.content, "echoed: x");
    }

    /// Reports the ambient job-grant count from *inside* a tool call — the
    /// vantage point of an approver consulted mid-tool.
    struct GrantProbe;
    #[async_trait]
    impl Tool for GrantProbe {
        fn name(&self) -> &'static str {
            "grant_probe"
        }
        fn description(&self) -> &'static str {
            "reports the ambient job grants"
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text(current_job_grants().len().to_string()))
        }
    }

    /// The cron-job regression: each tool call runs on a spawned task, and a
    /// task-local does not cross `tokio::spawn` — so unless the executor
    /// re-installs the turn's job grants (like it does the session), a grant
    /// approved at job creation never reaches the approver at run time.
    #[tokio::test]
    async fn job_grants_reach_a_tool_across_its_spawn() {
        use komo_core::domain::policy::{Category, Effect, Matcher, Rule};
        let executor = executor(vec![Arc::new(GrantProbe)], ToolExecutionConfig::default());
        let grant = Rule {
            channels: None,
            category: Category::HomeAssistant,
            matcher: Matcher::Exact,
            value: "climate.set_temperature".to_string(),
            access: None,
            effect: Effect::Allow,
            include_dangerous: false,
            unattended: true,
        };
        let out = with_job_grants(
            vec![grant],
            one(&executor, call("grant_probe", "{}"), &unledgered()),
        )
        .await;
        assert_eq!(out.content, "1", "the job's grant must be visible in-tool");

        // …and outside the scope the same tool sees none.
        let out = one(&executor, call("grant_probe", "{}"), &unledgered()).await;
        assert_eq!(out.content, "0");
    }

    /// A tool that never returns on its own — stands in for a hung shell command
    /// or a timeout-less HTTP client.
    struct HangingTool;
    #[async_trait]
    impl Tool for HangingTool {
        fn name(&self) -> &'static str {
            "hang"
        }
        fn description(&self) -> &'static str {
            "never returns"
        }
        async fn call(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(ToolOutput::text("unreachable"))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hung_tool_is_aborted_by_the_call_timeout() {
        // Without a timeout this call would await forever and wedge the turn.
        let executor = executor(
            vec![Arc::new(HangingTool)],
            ToolExecutionConfig {
                max_call_duration: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        );
        let out = one(&executor, call("hang", "{}"), &unledgered()).await;
        assert!(
            out.content.contains("did not report back within its 1s"),
            "got: {}",
            out.content
        );
        // `HangingTool` takes the default `idempotent() == false`, so aborting
        // the wait says nothing about whether the work landed. The model has to
        // be told that, or it will simply call the tool again.
        assert!(
            out.content.contains("may or may not have taken effect"),
            "an aborted non-idempotent call must not read as a plain failure, got: {}",
            out.content
        );
    }

    /// The same abort on a tool that can be safely repeated stays an ordinary
    /// failure — there is nothing to check first.
    #[tokio::test(start_paused = true)]
    async fn a_hung_idempotent_tool_is_just_a_failure() {
        struct HangingReader;
        #[async_trait]
        impl Tool for HangingReader {
            fn name(&self) -> &'static str {
                "hang_ro"
            }
            fn description(&self) -> &'static str {
                "never returns, changes nothing"
            }
            fn idempotent(&self) -> bool {
                true
            }
            async fn call(
                &self,
                _input: Value,
                _ctx: &ToolContext,
            ) -> Result<ToolOutput, ToolError> {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(ToolOutput::text("unreachable"))
            }
        }

        let executor = executor(
            vec![Arc::new(HangingReader)],
            ToolExecutionConfig {
                max_call_duration: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        );
        let out = one(&executor, call("hang_ro", "{}"), &unledgered()).await;
        assert!(
            out.content.contains("did not report back within its 1s"),
            "got: {}",
            out.content
        );
        assert!(
            !out.content.contains("may or may not have taken effect"),
            "a read-only tool has nothing to check, got: {}",
            out.content
        );
    }

    /// A tool that legitimately waits (a sub-agent completion, a human at an
    /// approval prompt) declares its own ceiling, and the executor honors it over
    /// the config default — the bug being that `delegate` was killed at 120s
    /// mid-completion.
    #[tokio::test(start_paused = true)]
    async fn a_tools_own_ceiling_overrides_the_config_default() {
        struct PatientTool;
        #[async_trait]
        impl Tool for PatientTool {
            fn name(&self) -> &'static str {
                "patient"
            }
            fn description(&self) -> &'static str {
                "waits longer than the default allows"
            }
            fn max_duration(&self) -> Option<Duration> {
                Some(Duration::from_secs(600))
            }
            async fn call(&self, _i: Value, _c: &ToolContext) -> Result<ToolOutput, ToolError> {
                tokio::time::sleep(Duration::from_secs(300)).await;
                Ok(ToolOutput::text("finished"))
            }
        }

        let executor = executor(
            vec![Arc::new(PatientTool)],
            ToolExecutionConfig {
                // The default would abort this at 1s.
                max_call_duration: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        );
        let out = one(&executor, call("patient", "{}"), &unledgered()).await;
        assert_eq!(out.content, "finished");
    }

    /// …but its ceiling is still a ceiling: a genuine hang inside a patient tool
    /// is caught, just later.
    #[tokio::test(start_paused = true)]
    async fn a_patient_tool_is_still_bounded() {
        struct PatientButHung;
        #[async_trait]
        impl Tool for PatientButHung {
            fn name(&self) -> &'static str {
                "patient_hang"
            }
            fn description(&self) -> &'static str {
                "never returns, but claims patience"
            }
            fn max_duration(&self) -> Option<Duration> {
                Some(Duration::from_secs(5))
            }
            async fn call(&self, _i: Value, _c: &ToolContext) -> Result<ToolOutput, ToolError> {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(ToolOutput::text("unreachable"))
            }
        }

        let executor = executor(
            vec![Arc::new(PatientButHung)],
            ToolExecutionConfig::default(),
        );
        let out = one(&executor, call("patient_hang", "{}"), &unledgered()).await;
        assert!(
            out.content.contains("did not report back within its 5s"),
            "the tool's own ceiling is what bounds it, got: {}",
            out.content
        );
    }

    #[tokio::test]
    async fn round_caps_fan_out_and_notes_the_overflow() {
        // A single round requesting far more calls than the ceiling must not
        // execute (or ledger) them all — the overflow gets a note.
        let executor = executor(vec![Arc::new(EchoTool)], ToolExecutionConfig::default());
        let calls: Vec<ToolCallReq> = (0..MAX_CALLS_PER_ROUND + 5)
            .map(|i| call("echo", &format!("{i}")))
            .collect();
        let outcomes = executor.execute_round(&calls, &unledgered()).await;
        assert_eq!(outcomes.len(), calls.len());
        assert!(outcomes[0].content.starts_with("echoed:"));
        assert!(
            outcomes[MAX_CALLS_PER_ROUND - 1]
                .content
                .starts_with("echoed:")
        );
        assert!(
            outcomes[MAX_CALLS_PER_ROUND]
                .content
                .contains("too many tool calls"),
            "got: {}",
            outcomes[MAX_CALLS_PER_ROUND].content
        );
    }

    fn budgeted(cap: usize) -> ToolTurnContext {
        ToolTurnContext {
            session: SessionContext::detached("cli:test"),
            run: None,
            budget: TurnResultBudget::new(cap),
            spin: SpinDetector::default(),
        }
    }

    #[tokio::test]
    async fn turn_budget_omits_results_once_the_turn_is_over_budget() {
        // BigTool returns 10 KB. With a 5 KB per-turn budget, the first call is
        // admitted (nothing consumed yet) and the second is omitted with a note —
        // so a long tool chain can't quietly overflow the context window.
        let executor = executor(vec![Arc::new(BigTool)], ToolExecutionConfig::default());
        let ctx = budgeted(5 * 1024);
        let first = one(&executor, call("big", "{}"), &ctx).await;
        assert_eq!(first.content.len(), 10_000, "first result admitted in full");
        let second = one(&executor, call("big", "{}"), &ctx).await;
        assert!(
            second.content.contains("per-turn budget"),
            "second result should be omitted once over budget, got len {}",
            second.content.len()
        );
    }

    #[tokio::test]
    async fn unlimited_turn_budget_never_omits() {
        let executor = executor(vec![Arc::new(BigTool)], ToolExecutionConfig::default());
        let ctx = budgeted(0); // 0 = unlimited
        // Distinct arguments per call, as above: the budget is what is under
        // test here, not the spin detector.
        for i in 0..5 {
            let out = one(&executor, call("big", &format!("{{\"i\":{i}}}")), &ctx).await;
            assert_eq!(out.content.len(), 10_000);
        }
    }

    // ── Runtime mounting (domain::catalog) ───────────────────────────────────

    /// A tool mounted after the executor was built is callable, and callable
    /// through a *clone* — every executor over one catalog sees the same set,
    /// which is what lets a plugin host mount into a running process.
    #[tokio::test]
    async fn a_tool_mounted_at_runtime_is_dispatchable_through_every_clone() {
        let executor = executor(vec![], ToolExecutionConfig::default());
        let shared = executor.clone();
        let context = unledgered();

        // Distinct arguments throughout: what is under test is the catalog,
        // and byte-identical repeats would be stopped by the spin detector
        // before they ever reached it.
        let missing = one(&executor, call("echo", "before"), &context).await;
        assert_eq!(missing.content, "error: unknown tool `echo`");

        let mounted = executor.catalog().mount(Arc::new(EchoTool));
        assert_eq!(
            shared.definitions().len(),
            1,
            "a clone shares the catalog, not a copy of it"
        );
        let out = one(&shared, call("echo", "mounted"), &context).await;
        assert_eq!(out.content, "echoed: mounted");

        // And unmounting takes it back out of both.
        drop(mounted);
        let gone = one(&shared, call("echo", "after"), &context).await;
        assert_eq!(gone.content, "error: unknown tool `echo`");
        assert!(executor.definitions().is_empty());
    }

    /// A pinned executor keeps dispatching against the set it pinned. This is
    /// the turn-scoped guarantee the runtime relies on: the model was handed
    /// those schemas, so those are the tools that must answer.
    #[tokio::test]
    async fn a_pinned_executor_ignores_later_mounts_and_unmounts() {
        let executor = executor(vec![Arc::new(EchoTool)], ToolExecutionConfig::default());
        let pinned = executor.pin();
        let context = unledgered();

        let removed = executor.catalog().retain(|name| name == "echo");
        assert_eq!(removed, vec!["echo"]);
        let _late = executor.catalog().mount(Arc::new(NamedTool("late")));

        // The pinned view still has echo and still does not have `late`.
        let out = one(&pinned, call("echo", "hi"), &context).await;
        assert_eq!(out.content, "echoed: hi");
        let unseen = one(&pinned, call("late", "{}"), &context).await;
        assert_eq!(unseen.content, "error: unknown tool `late`");

        // A freshly pinned executor — the next turn — sees the new set.
        let next = executor.pin();
        assert_eq!(
            next.definitions()
                .iter()
                .map(|t| t.name())
                .collect::<Vec<_>>(),
            vec!["late"]
        );
    }

    // ── Tool hooks (domain::hooks) ───────────────────────────────────────────

    /// Records what it saw, and optionally refuses every call.
    struct RecordingHook {
        label: &'static str,
        deny: Option<&'static str>,
        seen_pre: Arc<Mutex<Vec<String>>>,
        seen_post: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl RecordingHook {
        fn new(label: &'static str, deny: Option<&'static str>) -> Arc<Self> {
            Arc::new(Self {
                label,
                deny,
                seen_pre: Arc::new(Mutex::new(Vec::new())),
                seen_post: Arc::new(Mutex::new(Vec::new())),
            })
        }
    }

    #[async_trait]
    impl ToolHook for RecordingHook {
        fn name(&self) -> &'static str {
            self.label
        }
        async fn pre_execute(&self, call: &ToolCallReq) -> HookDecision {
            self.seen_pre.lock().unwrap().push(call.name.clone());
            match self.deny {
                Some(reason) => HookDecision::Deny(reason.to_string()),
                None => HookDecision::Continue,
            }
        }
        async fn post_execute(&self, call: &ToolCallReq, outcome: &ToolOutcome) {
            self.seen_post
                .lock()
                .unwrap()
                .push((call.name.clone(), outcome.content.clone()));
        }
    }

    fn hooked(tools: Vec<Arc<dyn Tool>>, hooks: Vec<Arc<dyn ToolHook>>) -> ToolExecutor {
        let mut executor = executor(tools, ToolExecutionConfig::default());
        for hook in hooks {
            executor.add_hook(hook);
        }
        executor
    }

    /// A hook that allows sees the call and its outcome; the tool still runs.
    #[tokio::test]
    async fn an_observing_hook_sees_the_call_and_its_outcome() {
        let hook = RecordingHook::new("observer", None);
        let executor = hooked(vec![Arc::new(EchoTool)], vec![hook.clone()]);

        let out = one(&executor, call("echo", "hi"), &unledgered()).await;
        assert_eq!(
            out.content, "echoed: hi",
            "an allowing hook changes nothing"
        );
        assert_eq!(hook.seen_pre.lock().unwrap().clone(), vec!["echo"]);
        assert_eq!(
            hook.seen_post.lock().unwrap().clone(),
            vec![("echo".to_string(), "echoed: hi".to_string())]
        );
    }

    /// A veto never reaches the tool, and its reason rides back as the call's
    /// outcome content — append-only, so the prompt prefix is untouched and the
    /// model can adjust, exactly like a policy denial.
    #[tokio::test]
    async fn a_denying_hook_short_circuits_and_the_reason_reaches_the_model() {
        let (tool, calls) = flaky(0, "unused", false);
        let hook = RecordingHook::new("gate", Some("not allowed right now"));
        let executor = hooked(vec![tool], vec![hook.clone()]);

        let out = one(&executor, call("flaky", "{}"), &unledgered()).await;
        assert_eq!(out.content, "not allowed right now");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "a vetoed call must not reach the tool"
        );
        // The veto is still an outcome, so post hooks observe what the model saw.
        assert_eq!(
            hook.seen_post.lock().unwrap().clone(),
            vec![("flaky".to_string(), "not allowed right now".to_string())]
        );
    }

    /// Registration order, first `Deny` wins: the hook after the refusing one
    /// is never consulted (waterfall short-circuit).
    #[tokio::test]
    async fn the_first_denying_hook_wins_and_later_hooks_are_not_consulted() {
        let first = RecordingHook::new("first", None);
        let denier = RecordingHook::new("denier", Some("refused"));
        let after = RecordingHook::new("after", None);
        let executor = hooked(
            vec![Arc::new(EchoTool)],
            vec![first.clone(), denier.clone(), after.clone()],
        );

        let out = one(&executor, call("echo", "hi"), &unledgered()).await;
        assert_eq!(out.content, "refused");
        assert_eq!(first.seen_pre.lock().unwrap().len(), 1);
        assert_eq!(denier.seen_pre.lock().unwrap().len(), 1);
        assert!(
            after.seen_pre.lock().unwrap().is_empty(),
            "a hook after the refusal must not run"
        );
        // Every hook still observes the outcome the model was handed.
        assert_eq!(after.seen_post.lock().unwrap().len(), 1);
    }

    /// A ledgered veto leaves no run step: nothing executed, so there is no
    /// tool call to audit — the refusal lives in the transcript instead.
    #[tokio::test]
    async fn a_vetoed_call_records_no_ledger_step() {
        let context = ledgered();
        let run = context.run.clone().unwrap();
        let hook = RecordingHook::new("gate", Some("refused"));
        let executor = hooked(vec![Arc::new(EchoTool)], vec![hook]);

        one(&executor, call("echo", "hi"), &context).await;
        assert!(run.steps().is_empty());
    }

    /// An unknown tool still reaches the hooks: a hook that maps a name onto
    /// something else has to see the call the model actually made.
    #[tokio::test]
    async fn hooks_see_a_call_for_an_unknown_tool_too() {
        let hook = RecordingHook::new("observer", None);
        let executor = hooked(vec![], vec![hook.clone()]);

        let out = one(&executor, call("nope", "{}"), &unledgered()).await;
        assert_eq!(out.content, "error: unknown tool `nope`");
        assert_eq!(hook.seen_pre.lock().unwrap().clone(), vec!["nope"]);
        assert_eq!(hook.seen_post.lock().unwrap().len(), 1);
    }
}
