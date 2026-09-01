use std::sync::Arc;

use async_trait::async_trait;

use super::session::Session;
use super::session_event::{SessionEvent, TurnRecorder};

/// One model round-trip's outcome inside komo's own tool loop. The loop lives
/// in `AgentRuntime` (not rig — roadmap §7), so it can insert control points
/// between rounds: either the model produced a final answer, or it requested
/// tools the runtime must execute and feed back.
pub enum Step {
    Final(String),
    ToolCalls {
        calls: Vec<ToolCallReq>,
        /// Text the model wrote in the *same* assistant turn as the calls — the
        /// "let me check the config first" narration providers emit alongside
        /// tool use. Not part of the final reply (the turn hasn't answered yet),
        /// but the only place a watcher can see the model's reasoning as it
        /// works, and the honest thing to say when the round budget cuts the
        /// turn short. Empty for a model that only emits calls.
        text: String,
    },
}

/// Tokens one turn spent, accumulated across its model round-trips. Zero means
/// *unknown* (a provider that reports no usage) as much as it means "none" —
/// the ledger treats both as absent, same convention as `elapsed_ms`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenUsage {
    /// Total prompt tokens, cache hits included (the provider layer normalizes
    /// the wire formats to this one meaning).
    pub input: i64,
    pub output: i64,
    /// The part of `input` the provider served from its prefix cache — always a
    /// subset, so [`hit_rate`](Self::hit_rate) is a real ratio.
    pub cached_input: i64,
}

impl TokenUsage {
    /// Fold another round's usage in. Saturating: a provider reporting nonsense
    /// must not panic a turn in release *or* debug.
    pub fn add(&mut self, other: TokenUsage) {
        self.input = self.input.saturating_add(other.input);
        self.output = self.output.saturating_add(other.output);
        self.cached_input = self.cached_input.saturating_add(other.cached_input);
    }

    pub fn is_zero(&self) -> bool {
        self.input == 0 && self.output == 0
    }

    /// What fraction of the prompt the provider's cache served, or `None` when
    /// there is nothing to divide by — a turn that reported no input at all,
    /// which is *unknown* rather than a 0% hit rate.
    ///
    /// The number worth watching: a tool loop whose later rounds don't climb
    /// means something upstream is changing the request prefix between rounds,
    /// and every round is paying full price to re-send the same context.
    pub fn hit_rate(&self) -> Option<f64> {
        (self.input > 0).then(|| self.cached_input as f64 / self.input as f64)
    }
}

/// A tool call the model requested. Rig-agnostic on purpose — the seam carries
/// no rig types. `id`/`call_id` are the provider's correlation handles, echoed
/// back verbatim in the tool result (Anthropic keys on `id`, OpenAI on
/// `call_id`); `args` is the JSON arguments object for the tool's `execute`.
pub struct ToolCallReq {
    pub id: String,
    pub call_id: Option<String>,
    pub name: String,
    pub args: String,
}

/// The result of executing one [`ToolCallReq`], threaded back to the model on
/// the next round. Carries the same correlation handles back.
pub struct ToolOutcome {
    pub id: String,
    pub call_id: Option<String>,
    pub content: String,
    /// The tool's machine-readable view, `Null` when it has none.
    ///
    /// Never sent to the model — `content` is what costs tokens. It exists for
    /// the readers that are not the model: the ledger, post-execute hooks, and
    /// a `run_code` program, which has to *compute* on a result whose text was
    /// laid out to be read.
    pub structured: serde_json::Value,
}

/// Receives assistant output as the provider produces it, mid-round.
///
/// A round's [`Step`] only exists once the round *finishes*, which for a
/// reasoning model on a long tool chain can be tens of seconds of silence. This
/// is the seam that lets a watching client see the work as it happens instead:
/// the backend calls it per streamed chunk, and the runtime forwards those onto
/// the turn's event sink.
///
/// Fire-and-forget and synchronous, like
/// [`ToolEventSink`](crate::domain::events::ToolEventSink): it is called from
/// inside the provider's stream loop, so it must never block or await. Absent
/// (`None`) for every turn with no watcher, which is the common case — an
/// unwatched turn pays nothing for this.
pub trait DeltaSink: Send + Sync {
    /// A chunk of the assistant's visible answer.
    fn text(&self, delta: &str);
    /// A chunk of the model's reasoning, when the provider streams a summary of
    /// it. Defaulted to a no-op: this is the interesting one to watch on a
    /// reasoning model (most of a round's latency is here, before any visible
    /// text exists), but a sink that only cares about the answer can ignore it.
    fn reasoning(&self, _delta: &str) {}
}

/// Drives one user turn as a sequence of model round-trips. Created by
/// [`LlmClient::begin_turn`], which assembles the per-turn system prompt and
/// memory injection *once* (not per round). The runtime calls [`first`] to get
/// the opening round, executes any requested tools, then [`step`]s their
/// results back until a [`Step::Final`].
///
/// [`first`]: TurnDriver::first
/// [`step`]: TurnDriver::step
#[async_trait]
pub trait TurnDriver: Send {
    /// The first model round-trip for this turn.
    async fn first(&mut self) -> anyhow::Result<Step>;
    /// Feed the previous round's tool results back and get the next round-trip.
    ///
    /// `interjected` carries anything the user said while the round was running
    /// (see [`InterjectSource`](crate::domain::gateway::InterjectSource)),
    /// already merged into one message. It rides in the **same** user message as
    /// the tool results rather than a separate one: a turn's history has to stay
    /// user/assistant-alternating, and appending bytes to the message that was
    /// being written anyway costs the provider's prompt cache nothing.
    async fn step(
        &mut self,
        results: Vec<ToolOutcome>,
        interjected: Option<String>,
    ) -> anyhow::Result<Step>;
    /// Tokens spent across every round this driver has run so far. Read by the
    /// runtime once the turn ends, for the ledger. Defaults to zero — a backend
    /// whose provider reports no usage simply records nothing.
    fn usage(&self) -> TokenUsage {
        TokenUsage::default()
    }
    /// The memories this turn's prompt was assembled with. Read by the runtime
    /// at turn end for the ledger, exactly like [`usage`](Self::usage) — the
    /// enricher runs deep inside prompt assembly, and this is the seam that
    /// already carries per-turn facts back out.
    fn memories(&self) -> crate::domain::run::RecalledMemories {
        crate::domain::run::RecalledMemories::default()
    }
}

/// Abstraction over a large-language-model backend.
///
/// The domain layer only knows this trait; concrete providers (DeepSeek,
/// OpenAI, an internal gateway, ...) live in `infra/`.
#[async_trait]
pub trait LlmClient: Send + Sync {
    /// Produce an assistant reply for a tool-less sub-agent conversation — the
    /// `delegate` tool, the reflective reviewer, the briefing sweep. These
    /// expose no tools, so the whole exchange is a single completion.
    async fn complete(&self, session: &Session) -> anyhow::Result<String>;

    /// Begin a tool-using turn for the main agent. The returned [`TurnDriver`]
    /// lets `AgentRuntime` own the multi-step tool loop, so planner control
    /// points (budget, clarify, resume — roadmap §7) live there.
    ///
    /// `deltas` is where the backend streams this turn's output as it is
    /// produced; `None` when nothing is watching. A backend is free to ignore it
    /// (the default one-shot driver does).
    ///
    /// `journal` is the turn journal to record this turn's provider-level state
    /// into (see `domain::session_event::TurnRecorder`); `None` for callers that don't keep
    /// one (aux paths, tests). Backends without provider-shaped state ignore it.
    ///
    /// The default is a single-shot driver wrapping [`complete`](LlmClient::complete):
    /// it answers in one round with no tool calls. Tool-less backends (and test
    /// stubs) inherit this for free; the real provider client overrides it with a
    /// tool-looping driver.
    async fn begin_turn(
        &self,
        session: &Session,
        _deltas: Option<Arc<dyn DeltaSink>>,
        _recorder: Option<Arc<dyn TurnRecorder>>,
    ) -> anyhow::Result<Box<dyn TurnDriver>> {
        let reply = self.complete(session).await?;
        Ok(Box::new(OneShotDriver(Some(reply))))
    }

    /// Reopen an interrupted turn from its session's events, returning a driver
    /// that continues from exactly where the turn stopped: same model, same
    /// prompt bytes, and the tool rounds already paid for replayed from the log
    /// rather than re-run.
    ///
    /// `events` is the session's whole log and `turn_id` names the interrupted
    /// turn within it. `recorder` records the *continuation*, so a second
    /// interruption resumes from the continuation rather than from scratch.
    ///
    /// Default: unsupported — the caller falls back to the digest-primed fresh
    /// turn (`domain::run::resume_prompt`). Only the provider-backed client
    /// can rebuild provider-shaped state.
    async fn resume_turn(
        &self,
        _session: &Session,
        _events: &[SessionEvent],
        _turn_id: &str,
        _deltas: Option<Arc<dyn DeltaSink>>,
        _recorder: Option<Arc<dyn TurnRecorder>>,
    ) -> anyhow::Result<Box<dyn TurnDriver>> {
        anyhow::bail!("this backend cannot resume a turn from its session log")
    }
}

/// The default [`TurnDriver`]: yields one [`Step::Final`] (the precomputed
/// `complete` reply) and never requests tools.
struct OneShotDriver(Option<String>);

#[async_trait]
impl TurnDriver for OneShotDriver {
    async fn first(&mut self) -> anyhow::Result<Step> {
        Ok(Step::Final(self.0.take().unwrap_or_default()))
    }
    async fn step(
        &mut self,
        _results: Vec<ToolOutcome>,
        _interjected: Option<String>,
    ) -> anyhow::Result<Step> {
        Ok(Step::Final(self.0.take().unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rounds fold together, cache hits included — a turn's rate is over the
    /// whole turn, not just its last round.
    #[test]
    fn usage_accumulates_every_count_across_rounds() {
        let mut usage = TokenUsage::default();
        usage.add(TokenUsage {
            input: 1_000,
            output: 100,
            cached_input: 0,
        });
        usage.add(TokenUsage {
            input: 1_500,
            output: 50,
            cached_input: 1_000,
        });
        assert_eq!(usage.input, 2_500);
        assert_eq!(usage.output, 150);
        assert_eq!(usage.cached_input, 1_000);
        assert_eq!(usage.hit_rate(), Some(0.4));
    }

    /// Nothing to divide by is *unknown*, not a 0% hit rate — the same
    /// convention every zero in this struct follows.
    #[test]
    fn a_turn_that_reported_no_prompt_has_no_hit_rate() {
        assert_eq!(TokenUsage::default().hit_rate(), None);
        assert_eq!(
            TokenUsage {
                input: 0,
                output: 40,
                cached_input: 0,
            }
            .hit_rate(),
            None
        );
    }

    /// A saturating fold: a provider reporting nonsense must not panic a turn.
    #[test]
    fn accumulation_saturates_rather_than_overflowing() {
        let mut usage = TokenUsage {
            input: i64::MAX,
            output: i64::MAX,
            cached_input: i64::MAX,
        };
        usage.add(TokenUsage {
            input: 10,
            output: 10,
            cached_input: 10,
        });
        assert_eq!(usage.input, i64::MAX);
        assert_eq!(usage.cached_input, i64::MAX);
    }
}
