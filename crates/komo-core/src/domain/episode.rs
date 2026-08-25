//! The episode read model — one finished [`Run`] plus the [`RunStep`]s it
//! produced, which is what the learning pass consumes
//! (docs/episode-learning-framework.md §4).
//!
//! There is deliberately no episode *store*: a `Run` already is "one user
//! request driven agent turn" and a `RunStep` already is "one tool call in it".
//! Copying them into a second table would give the learning pass its own
//! version of facts the ledger is already the authority on. [`EpisodeView`] is
//! assembled on demand and thrown away.
//!
//! Two axes that look like one and are not (§4.2):
//!
//! ```text
//! Execution status  Running | Done | Failed      — did the turn deliver?
//! Goal outcome      Success | Failure | Unknown  — did the user get what they asked for?
//! ```
//!
//! `Done` is not `Success`: an agent that replies "done" without verifying
//! anything has delivered a reply and proved nothing. Collapsing the two is how
//! a learning loop starts treating its own claims as evidence for itself.

use crate::domain::cancel::CANCELLED_ERROR;
use crate::domain::run::{Run, RunStatus, RunStep};

/// One finished turn, read as a unit: the request, what the tools did, and the
/// reply. Assembled from the ledger (already redacted and truncated there — the
/// learning path never reaches around that).
#[derive(Debug, Clone)]
pub struct EpisodeView {
    pub run: Run,
    /// The run's tool calls, ordered by `seq`.
    pub steps: Vec<RunStep>,
}

impl EpisodeView {
    pub fn id(&self) -> &str {
        &self.run.id
    }

    /// The user stopped this turn. Distinct from an ordinary failure: nothing
    /// broke, the work is simply incomplete on purpose.
    pub fn was_cancelled(&self) -> bool {
        matches!(self.run.status, RunStatus::Failed) && self.run.error == CANCELLED_ERROR
    }

    /// A step that never confirmed its result — a wall-clock abort, or an
    /// ambiguous transport failure on a non-idempotent tool. "It may still have
    /// taken effect" is why such a turn can never be assessed `Success`.
    pub fn uncertain_steps(&self) -> impl Iterator<Item = &RunStep> {
        self.steps.iter().filter(|s| s.uncertain)
    }

    /// Whether this episode may produce learning at all (§7).
    ///
    /// A cancelled turn is not a lesson: the work stopped part-way by the
    /// user's choice, so whatever it half-did is not a procedure worth keeping
    /// and its silence is not evidence about the goal. It stays in the ledger as
    /// audit either way — that is what "we don't learn from it" costs, and all
    /// it costs.
    pub fn learning_eligible(&self) -> bool {
        !self.was_cancelled()
    }
}

/// An episode paired with what the evidence so far says about how it went —
/// the unit the learning extractor reads.
///
/// The two travel together because neither is enough alone: the steps say what
/// was done, the assessment says whether it worked, and a lesson drawn from one
/// without the other is either an unverified procedure or a verdict with no
/// subject.
#[derive(Debug, Clone)]
pub struct AssessedEpisode {
    pub view: EpisodeView,
    pub outcome: OutcomeAssessment,
}

impl AssessedEpisode {
    /// Assess `view` from what is deterministically knowable and pair them.
    pub fn deterministic(view: EpisodeView, now: i64) -> Self {
        let outcome = OutcomeAssessment::deterministic(&view, now);
        Self { view, outcome }
    }

    /// Prefer a stored assessment over recomputing one.
    ///
    /// The stored one is the better answer whenever it exists, because it may
    /// carry evidence that did not exist when the turn ended — the user's next
    /// message saying it worked, or did not. Recomputing would silently discard
    /// exactly the evidence worth having. Unparseable stored text falls back to
    /// the deterministic reading rather than failing: a mangled cell must not
    /// cost the episode its assessment.
    pub fn stored_or_deterministic(view: EpisodeView, stored: &str, now: i64) -> Self {
        match serde_json::from_str::<OutcomeAssessment>(stored) {
            Ok(outcome) => Self { view, outcome },
            Err(_) => Self::deterministic(view, now),
        }
    }
}

/// What the evidence currently says about whether the user's goal was met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeVerdict {
    Success,
    Failure,
    /// Not "no result" — *not enough evidence to say*. The honest default, and
    /// the one a conflict falls back to.
    #[default]
    Unknown,
}

impl OutcomeVerdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Unknown => "unknown",
        }
    }
}

/// Where one piece of outcome evidence came from. The variant *is* its
/// strength: the ordering below is the rule that a weaker source may never
/// overturn a stronger one (§4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeEvidenceKind {
    /// The user said it worked.
    UserConfirmed,
    /// The user said it did not.
    UserRejected,
    /// A step never confirmed its effect. Always votes [`OutcomeVerdict::Unknown`],
    /// and sits *above* deterministic checks on purpose: a passing test says
    /// the code is right, not that a non-idempotent call applied exactly once.
    UncertainEffect,
    /// A test run, a check command, a structured assertion — something that
    /// answered the question by executing it.
    DeterministicCheck,
    /// A tool's own machine-readable verdict (`ToolOutput::structured`).
    StructuredToolResult,
    /// The turn delivered / failed / was stopped.
    ExecutionStatus,
    /// The agent said so in its final reply. The weakest source there is: it is
    /// the claim under test, not evidence for it.
    AgentClaim,
}

impl OutcomeEvidenceKind {
    /// Higher wins. Only the strongest kind present decides the verdict.
    fn strength(&self) -> u8 {
        match self {
            Self::UserConfirmed | Self::UserRejected => 5,
            Self::UncertainEffect => 4,
            Self::DeterministicCheck => 3,
            Self::StructuredToolResult => 2,
            Self::ExecutionStatus => 1,
            Self::AgentClaim => 0,
        }
    }
}

/// One observation about how the episode turned out.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutcomeEvidence {
    pub kind: OutcomeEvidenceKind,
    /// What this one piece of evidence, on its own, points to.
    pub verdict: OutcomeVerdict,
    /// Human-readable reason, short enough to render in a prompt or `run inspect`.
    pub detail: String,
    /// Where it came from — `"run"`, `"step 3"`, or a later run's id.
    pub source: String,
    pub observed_at: i64,
}

impl OutcomeEvidence {
    pub fn new(
        kind: OutcomeEvidenceKind,
        verdict: OutcomeVerdict,
        detail: impl Into<String>,
        source: impl Into<String>,
        observed_at: i64,
    ) -> Self {
        Self {
            kind,
            verdict,
            detail: detail.into(),
            source: source.into(),
            observed_at,
        }
    }
}

/// The episode's goal outcome as the evidence so far supports it.
///
/// Provisional by construction: most work is confirmed or refuted by what the
/// user says *next*, not by anything observable when the turn ends. Phase 2
/// appends later evidence and re-resolves; nothing here assumes the list is
/// final.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutcomeAssessment {
    pub run_id: String,
    pub verdict: OutcomeVerdict,
    pub evidence: Vec<OutcomeEvidence>,
    pub evaluated_at: i64,
}

impl OutcomeAssessment {
    /// Resolve `evidence` into one verdict: take only the strongest kind
    /// present, and let it decide **only if it is unanimous**. Disagreement at
    /// the same strength means the evidence does not answer the question, which
    /// is [`OutcomeVerdict::Unknown`] — not a coin flip, and not a tie broken by
    /// counting, since two weak agreeing sources are not stronger than one
    /// disagreeing peer.
    pub fn resolve(run_id: impl Into<String>, evidence: Vec<OutcomeEvidence>, now: i64) -> Self {
        let verdict = match evidence.iter().map(|e| e.kind.strength()).max() {
            None => OutcomeVerdict::Unknown,
            Some(top) => {
                let mut strongest = evidence
                    .iter()
                    .filter(|e| e.kind.strength() == top)
                    .map(|e| e.verdict);
                let first = strongest.next().unwrap_or_default();
                if strongest.all(|v| v == first) {
                    first
                } else {
                    OutcomeVerdict::Unknown
                }
            }
        };
        Self {
            run_id: run_id.into(),
            verdict,
            evidence,
            evaluated_at: now,
        }
    }

    /// The deterministic reading of a finished episode (§5.3 step 1–2): what can
    /// be known without asking a model anything.
    ///
    /// It reports what went wrong and what stayed unconfirmed, and it never
    /// reports success — nothing observable at the end of a turn distinguishes
    /// "the goal was met" from "the agent stopped talking". A `Done` run with
    /// clean steps therefore yields *no* evidence and stays `Unknown`, which is
    /// the accurate answer rather than a missing one.
    pub fn deterministic(episode: &EpisodeView, now: i64) -> Self {
        let mut evidence = Vec::new();

        if episode.was_cancelled() {
            evidence.push(OutcomeEvidence::new(
                OutcomeEvidenceKind::ExecutionStatus,
                OutcomeVerdict::Unknown,
                "the user stopped this turn before it finished",
                "run",
                now,
            ));
        } else if matches!(episode.run.status, RunStatus::Failed) {
            // Failed execution is not a failed goal: a provider that dropped the
            // connection says nothing about whether the task is achievable, and
            // the next attempt may well deliver it.
            evidence.push(OutcomeEvidence::new(
                OutcomeEvidenceKind::ExecutionStatus,
                OutcomeVerdict::Unknown,
                format!("the turn failed: {}", episode.run.error),
                "run",
                now,
            ));
        }

        for step in episode.uncertain_steps() {
            evidence.push(OutcomeEvidence::new(
                OutcomeEvidenceKind::UncertainEffect,
                OutcomeVerdict::Unknown,
                format!(
                    "`{}` did not confirm its result — it may still have taken effect",
                    step.tool_name
                ),
                format!("step {}", step.seq),
                now,
            ));
        }

        Self::resolve(episode.run.id.clone(), evidence, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(status: RunStatus, error: &str) -> Run {
        let mut r = Run::start("cli:s", "do the thing");
        r.status = status;
        r.error = error.to_string();
        r.final_output = "done!".into();
        r
    }

    fn step(seq: i64, tool: &str, ok: bool, uncertain: bool) -> RunStep {
        RunStep {
            run_id: "run-1".into(),
            seq,
            tool_name: tool.into(),
            args: "{}".into(),
            result: String::new(),
            error: String::new(),
            ok,
            uncertain,
            started_at: 0,
            ended_at: 0,
            elapsed_ms: 0,
            structured: serde_json::Value::Null,
            output_paths: Vec::new(),
        }
    }

    fn episode(status: RunStatus, error: &str, steps: Vec<RunStep>) -> EpisodeView {
        EpisodeView {
            run: run(status, error),
            steps,
        }
    }

    /// Golden case 1: the agent said it finished and verified nothing.
    #[test]
    fn a_delivered_turn_without_verification_is_unknown_not_success() {
        let e = episode(RunStatus::Done, "", vec![step(1, "shell", true, false)]);
        let a = OutcomeAssessment::deterministic(&e, 100);

        assert_eq!(a.verdict, OutcomeVerdict::Unknown);
        assert!(
            a.evidence.is_empty(),
            "a clean delivered turn has nothing to say about the goal, \
             and saying nothing is the honest record"
        );
    }

    /// Golden case 4: a non-idempotent call that never confirmed.
    #[test]
    fn an_uncertain_step_is_recorded_and_keeps_the_verdict_unknown() {
        let e = episode(
            RunStatus::Done,
            "",
            vec![step(1, "read", true, false), step(2, "shell", false, true)],
        );
        let a = OutcomeAssessment::deterministic(&e, 100);

        assert_eq!(a.verdict, OutcomeVerdict::Unknown);
        assert_eq!(a.evidence.len(), 1);
        assert_eq!(a.evidence[0].kind, OutcomeEvidenceKind::UncertainEffect);
        assert_eq!(a.evidence[0].source, "step 2");
    }

    #[test]
    fn an_ordinary_failure_is_unknown_and_says_why() {
        let e = episode(RunStatus::Failed, "provider stream ended", Vec::new());
        let a = OutcomeAssessment::deterministic(&e, 100);

        assert_eq!(
            a.verdict,
            OutcomeVerdict::Unknown,
            "a broken turn is not a refuted goal"
        );
        assert!(a.evidence[0].detail.contains("provider stream ended"));
    }

    /// Golden cases 11 and 12: cancelled turns are audit, not lessons —
    /// whether or not they got as far as a side effect.
    #[test]
    fn cancelled_episodes_are_not_eligible_for_learning() {
        let pristine = episode(RunStatus::Failed, CANCELLED_ERROR, Vec::new());
        let with_effects = episode(
            RunStatus::Failed,
            CANCELLED_ERROR,
            vec![step(1, "write", true, false)],
        );

        assert!(!pristine.learning_eligible());
        assert!(!with_effects.learning_eligible());
        assert_eq!(
            OutcomeAssessment::deterministic(&with_effects, 100).verdict,
            OutcomeVerdict::Unknown
        );
    }

    #[test]
    fn a_delivered_turn_is_eligible_even_when_it_failed() {
        // Failure is where corrections happen — the doc's reason for not
        // gating learning on success (§4.4).
        assert!(episode(RunStatus::Failed, "tool exploded", Vec::new()).learning_eligible());
        assert!(episode(RunStatus::Done, "", Vec::new()).learning_eligible());
    }

    #[test]
    fn the_strongest_evidence_decides_and_weaker_evidence_cannot_overturn_it() {
        let a = OutcomeAssessment::resolve(
            "run-1",
            vec![
                OutcomeEvidence::new(
                    OutcomeEvidenceKind::AgentClaim,
                    OutcomeVerdict::Success,
                    "I fixed it",
                    "run",
                    1,
                ),
                OutcomeEvidence::new(
                    OutcomeEvidenceKind::UserRejected,
                    OutcomeVerdict::Failure,
                    "still broken",
                    "run-2",
                    2,
                ),
            ],
            3,
        );
        assert_eq!(a.verdict, OutcomeVerdict::Failure);
    }

    #[test]
    fn an_uncertain_effect_outranks_a_passing_check() {
        // The test passing does not tell us whether the non-idempotent call
        // applied once or twice.
        let a = OutcomeAssessment::resolve(
            "run-1",
            vec![
                OutcomeEvidence::new(
                    OutcomeEvidenceKind::DeterministicCheck,
                    OutcomeVerdict::Success,
                    "cargo test passed",
                    "step 4",
                    1,
                ),
                OutcomeEvidence::new(
                    OutcomeEvidenceKind::UncertainEffect,
                    OutcomeVerdict::Unknown,
                    "shell did not confirm",
                    "step 2",
                    1,
                ),
            ],
            2,
        );
        assert_eq!(a.verdict, OutcomeVerdict::Unknown);
    }

    #[test]
    fn peers_that_disagree_resolve_to_unknown_rather_than_a_majority() {
        let a = OutcomeAssessment::resolve(
            "run-1",
            vec![
                OutcomeEvidence::new(
                    OutcomeEvidenceKind::DeterministicCheck,
                    OutcomeVerdict::Success,
                    "unit tests passed",
                    "step 1",
                    1,
                ),
                OutcomeEvidence::new(
                    OutcomeEvidenceKind::DeterministicCheck,
                    OutcomeVerdict::Success,
                    "lint clean",
                    "step 2",
                    1,
                ),
                OutcomeEvidence::new(
                    OutcomeEvidenceKind::DeterministicCheck,
                    OutcomeVerdict::Failure,
                    "integration suite failed",
                    "step 3",
                    1,
                ),
            ],
            2,
        );
        assert_eq!(
            a.verdict,
            OutcomeVerdict::Unknown,
            "counting agreeing peers is not how conflicting checks are settled"
        );
    }

    #[test]
    fn no_evidence_is_unknown() {
        let a = OutcomeAssessment::resolve("run-1", Vec::new(), 5);
        assert_eq!(a.verdict, OutcomeVerdict::Unknown);
        assert_eq!(a.evaluated_at, 5);
    }
}
