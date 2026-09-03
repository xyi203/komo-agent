//! The run ledger, read out of the session event log.
//!
//! [`Run`] and [`RunStep`] are rows in a disposable database, and every fact
//! they hold is already a durable event: a turn opens with `turn/started`, its
//! calls bracket as `tool/call-started` / `tool/call-settled`, and it closes
//! with one of the three terminal events. Keeping the rows as a *second*
//! authoritative write meant two records of the same turn that could disagree —
//! and after a crash they routinely did, because the row's update is not part of
//! the append that made the event durable.
//!
//! So the rows become a query index over this fold. Nothing here reads the
//! database, and the fold is total: an event log alone reproduces the ledger.
//!
//! Two fields are **not** produced here, and each for its own reason:
//!
//! - [`Run::outcome`] is revisable by what the user says in a *later* turn, so
//!   it is row-held state merged over the projection, never derived from it.
//! - [`Run::recoverable`] folds as *interrupted and unclaimed*, which is not
//!   the same as the reconciled row: a turn with no terminal event is either
//!   running right now or died, and only the process can tell those apart.
//!   Translating one into `Failed` with an interrupted error is the
//!   reconciler's job at startup, not a fact the log states.

use async_trait::async_trait;

use super::cancel::CANCELLED_ERROR;
use super::run::{RUN_FIELD_CAP, Run, RunStatus, RunStep, truncate};
use super::session_event::{MessageSource, SessionEvent, SessionEventKind, ToolOutcome};

/// Where a folded ledger lands: the query tables `komo run`, `skills audit` and
/// `memory used` read.
///
/// The write half of this module. A commit is **idempotent** — it is replayed
/// after every turn and again by a full rebuild, over rows that may already
/// hold most of what it is committing.
#[async_trait]
pub trait RunProjectionStore: Send + Sync {
    /// Commit one session's folded runs, which the log holds `through` this seq.
    ///
    /// `through` is the projection's watermark, and a commit that would not
    /// advance it is skipped: a session gains events constantly and the fold is
    /// over the whole log, so re-committing an unchanged one is pure cost.
    ///
    /// Row-held fields are **merged, never overwritten**: [`Run::outcome`] is
    /// revised by a later turn and the log does not carry it, and
    /// [`Run::learned`] only ever advances — a rebuild must not un-retire a
    /// turn whose watermark event predates the log it can still read.
    async fn commit(
        &self,
        session_id: &str,
        runs: &[ProjectedRun],
        through: u64,
    ) -> anyhow::Result<()>;
}

/// One turn, as the log records it.
#[derive(Debug, Clone)]
pub struct ProjectedRun {
    pub run: Run,
    pub steps: Vec<ProjectedStep>,
    /// Where this turn begins in the log — the seq of its `turn/started`.
    ///
    /// Retention deletes whole segments, so the question it has to answer is
    /// "from which seq on must the log survive intact", and a run's id says
    /// nothing about that.
    pub start_seq: u64,
}

/// One call, and whether the log ever saw it finish.
///
/// The ledger cannot express this: a step row is written *at settle*, so a call
/// the process died in the middle of leaves no row at all — the record says the
/// turn never made the call. The log brackets every call, so the projection can
/// tell "never dispatched" from "dispatched and we lost the answer", which is
/// the question recovery has to answer and the reason the two halves are
/// separate events.
#[derive(Debug, Clone)]
pub struct ProjectedStep {
    pub step: RunStep,
    pub settled: bool,
}

/// The seq a turn's log has to survive from: its own start, and the start of
/// every earlier attempt at it.
///
/// A continuation is rebuilt from its whole `resumed_from` chain, so cutting an
/// ancestor's rounds away would leave a turn that still reads as resumable but
/// silently re-runs the work those rounds already paid for. Retention asks this
/// rather than `start_seq` for exactly that reason.
///
/// Bounded by the number of runs, so a `resumed_from` cycle cannot loop.
pub fn replay_floor(runs: &[ProjectedRun], run: &ProjectedRun) -> u64 {
    let mut floor = run.start_seq;
    let mut current = run.run.resumed_from.clone();
    for _ in 0..runs.len() {
        let Some(id) = current else { break };
        let Some(parent) = runs.iter().find(|other| other.run.id == id) else {
            break;
        };
        floor = floor.min(parent.start_seq);
        current = parent.run.resumed_from.clone();
    }
    floor
}

/// Fold one session's events into the runs they record, oldest first.
///
/// Events for a turn that never opened with `turn/started` are ignored rather
/// than synthesizing a run: the log is contiguous and checked on read, so their
/// absence is not a gap to paper over.
pub fn project_runs(session_id: &str, events: &[SessionEvent]) -> Vec<ProjectedRun> {
    let mut runs: Vec<ProjectedRun> = Vec::new();
    // A call's own `tool/call-started` and `tool/call-settled` are separated by
    // however long the tool took, and the settle lands in completion order, so
    // the two halves are matched by id rather than by adjacency.
    let mut open: Vec<(String, String, usize, usize)> = Vec::new();
    // The turn's reply, staged until the turn *completes*. A cancelled or failed
    // turn also leaves an `assistant/message` — a placeholder that keeps the
    // transcript alternating — but that is what the conversation shows, not an
    // answer the turn produced, and the ledger has always left `final_output`
    // empty for both.
    let mut replies: Vec<String> = Vec::new();

    for event in events {
        // The learning watermark. Decided by the sweep after the turn is over,
        // so it arrives past the run's terminal event and is not part of its
        // work — but it is the same fact the row's `learned` flag held, and
        // *skipped* has to advance it too, or a turn the sweep considered and
        // declined is offered again forever.
        if let SessionEventKind::LearningCompleted { turn_id }
        | SessionEventKind::LearningSkipped { turn_id, .. } = &event.kind
        {
            if let Some(projected) = runs.iter_mut().find(|p| p.run.id == *turn_id) {
                projected.run.learned = true;
            }
            continue;
        }

        let Some(turn_id) = event.turn_id_of_work() else {
            continue;
        };
        let at = event.at.unix_timestamp();

        if let SessionEventKind::TurnStarted {
            turn_id,
            resumed_from,
        } = &event.kind
        {
            let mut run = Run::start(session_id, "");
            run.id = turn_id.clone();
            run.started_at = at;
            run.resumed_from = resumed_from.clone();
            // Every turn opens interrupted and is cleared by its own terminal
            // event. A run left this way is the residue of a process that died
            // mid-turn — which is exactly what recovery is looking for.
            run.recoverable = true;
            runs.push(ProjectedRun {
                run,
                steps: Vec::new(),
                start_seq: event.seq,
            });
            replies.push(String::new());
            continue;
        }

        let Some(at_index) = runs.iter().position(|p| p.run.id == turn_id) else {
            continue;
        };
        let projected = &mut runs[at_index];

        match &event.kind {
            SessionEventKind::UserMessage(message) if message.source == MessageSource::User => {
                projected.run.input = truncate(&message.content, RUN_FIELD_CAP);
            }
            SessionEventKind::TurnMemories { memories, .. } => {
                projected.run.memories = memories.clone();
            }
            SessionEventKind::AssistantRound(round) => {
                projected.run.tokens_in += round.tokens_in;
                projected.run.tokens_out += round.tokens_out;
                projected.run.tokens_cached += round.tokens_cached;
            }
            SessionEventKind::AssistantMessage(message) => {
                replies[at_index] = truncate(&message.content, RUN_FIELD_CAP);
            }
            SessionEventKind::ToolCallStarted(call) => {
                let seq = projected.steps.len() as i64;
                projected.steps.push(ProjectedStep {
                    settled: false,
                    step: RunStep {
                        run_id: turn_id.to_string(),
                        seq,
                        tool_name: call.tool.clone(),
                        args: call.args.clone(),
                        result: String::new(),
                        error: String::new(),
                        ok: false,
                        uncertain: false,
                        started_at: at,
                        ended_at: at,
                        elapsed_ms: 0,
                        structured: serde_json::Value::Null,
                        output_paths: Vec::new(),
                        approved_by: String::new(),
                        approval_waited_ms: 0,
                    },
                });
                open.push((
                    turn_id.to_string(),
                    call.call_id.clone(),
                    at_index,
                    projected.steps.len() - 1,
                ));
            }
            // The audit half of an approval-gated call: which rung let it
            // happen and how long the answer took. Matched by `call_id` like a
            // settle — the approval resolves while the call is still open, so
            // adjacency says nothing.
            SessionEventKind::ApprovalResolved(approval) => {
                let found = open
                    .iter()
                    .find(|(run_id, call_id, ..)| run_id == turn_id && *call_id == approval.call_id)
                    .map(|(.., step_index)| *step_index);
                if let Some(step_index) = found
                    && let Some(projected_step) = projected.steps.get_mut(step_index)
                {
                    projected_step.step.approved_by = approval.decided_by.clone();
                    projected_step.step.approval_waited_ms = approval.waited_ms;
                }
            }
            SessionEventKind::ToolCallSettled(call) => {
                let found = open.iter().position(|(run_id, call_id, ..)| {
                    run_id == turn_id && *call_id == call.call_id
                });
                let Some(found) = found else {
                    continue;
                };
                let (.., step_index) = open.remove(found);
                projected.steps[step_index].settled = true;
                let step = &mut projected.steps[step_index].step;
                step.result = call.result.clone();
                step.error = call.error.clone();
                step.ok = call.outcome == ToolOutcome::Succeeded;
                step.uncertain = call.outcome == ToolOutcome::Uncertain;
                step.ended_at = at;
                step.elapsed_ms = call.elapsed_ms;
                step.structured = call.structured.clone();
                step.output_paths = call.output_paths.clone();
            }
            SessionEventKind::TurnCompleted { .. } => {
                projected.run.final_output = std::mem::take(&mut replies[at_index]);
                projected.run.status = RunStatus::Done;
                projected.run.ended_at = Some(at);
                projected.run.recoverable = false;
            }
            SessionEventKind::TurnFailed { error, .. } => {
                projected.run.status = RunStatus::Failed;
                projected.run.error = truncate(error, RUN_FIELD_CAP);
                projected.run.ended_at = Some(at);
                projected.run.recoverable = false;
            }
            SessionEventKind::TurnCancelled { .. } => {
                // Cancelled, not broken — and deliberately not recoverable:
                // there is nothing to resume, the user asked it to stop.
                projected.run.status = RunStatus::Failed;
                projected.run.error = CANCELLED_ERROR.to_string();
                projected.run.ended_at = Some(at);
                projected.run.recoverable = false;
            }
            _ => {}
        }
    }

    // A continuation's own `turn/started` **is** the claim on the turn it picked
    // up: seq assignment is what serializes two would-be resumers, so the log
    // decides which of them owns the recovery rather than a row update racing
    // another reader. Once claimed, a turn is not offered again — resuming a
    // turn twice re-runs work the first continuation already did.
    let claimed: Vec<String> = runs
        .iter()
        .filter_map(|projected| projected.run.resumed_from.clone())
        .collect();

    // A continuation appends no user message of its own — the question is the
    // one the interrupted turn was already answering — so it inherits it. The
    // row is what an operator reads in `run list`, and a turn there with no
    // input reads as a turn about nothing.
    let inherited: Vec<(String, String)> = runs
        .iter()
        .filter(|projected| projected.run.input.is_empty())
        .filter_map(|projected| {
            let from = projected.run.resumed_from.as_deref()?;
            let parent = runs.iter().find(|other| other.run.id == from)?;
            Some((projected.run.id.clone(), parent.run.input.clone()))
        })
        .collect();

    for projected in &mut runs {
        if claimed.contains(&projected.run.id) {
            projected.run.recoverable = false;
        }
        if let Some((_, input)) = inherited.iter().find(|(id, _)| *id == projected.run.id) {
            projected.run.input = input.clone();
        }
        // The LLM owns tool dispatch, so the plan is a description of what the
        // turn turned out to do, not a decision made before it ran.
        projected.run.plan = match projected.steps.len() {
            0 => "respond".to_string(),
            n => format!("{n} tool call(s)"),
        };
    }
    runs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::run::RecalledMemories;
    use crate::domain::session_event::{
        AssistantMessageEvent, AssistantRoundEvent, SurfacePlacement, ToolCallSettledEvent,
        ToolCallStartedEvent, UserMessageEvent,
    };
    use time::OffsetDateTime;

    fn at(secs: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(secs).unwrap()
    }

    fn ev(seq: u64, secs: i64, kind: SessionEventKind) -> SessionEvent {
        SessionEvent::new(seq, at(secs), kind)
    }

    fn started(seq: u64, secs: i64, turn: &str) -> SessionEvent {
        ev(
            seq,
            secs,
            SessionEventKind::TurnStarted {
                turn_id: turn.into(),
                resumed_from: None,
            },
        )
    }

    fn asked(seq: u64, secs: i64, turn: &str, text: &str) -> SessionEvent {
        ev(
            seq,
            secs,
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: turn.into(),
                content: text.into(),
                source: MessageSource::User,
                surface: SurfacePlacement::append(),
            }),
        )
    }

    fn call_started(seq: u64, secs: i64, turn: &str, id: &str, tool: &str) -> SessionEvent {
        ev(
            seq,
            secs,
            SessionEventKind::ToolCallStarted(ToolCallStartedEvent {
                turn_id: turn.into(),
                call_id: id.into(),
                call_index: 0,
                tool: tool.into(),
                args: "{}".into(),
            }),
        )
    }

    fn call_settled(
        seq: u64,
        secs: i64,
        turn: &str,
        id: &str,
        outcome: ToolOutcome,
    ) -> SessionEvent {
        ev(
            seq,
            secs,
            SessionEventKind::ToolCallSettled(ToolCallSettledEvent {
                turn_id: turn.into(),
                call_id: id.into(),
                call_index: 0,
                outcome,
                result: "done".into(),
                error: String::new(),
                elapsed_ms: 12,
                structured: serde_json::Value::Null,
                output_paths: vec![],
            }),
        )
    }

    #[test]
    fn a_finished_turn_projects_to_the_row_it_used_to_be_written_as() {
        let events = vec![
            started(0, 100, "t1"),
            asked(1, 100, "t1", "go"),
            ev(
                2,
                101,
                SessionEventKind::AssistantRound(AssistantRoundEvent {
                    turn_id: "t1".into(),
                    round: 0,
                    response_id: "r1".into(),
                    blocks: serde_json::Value::Null,
                    tokens_in: 30,
                    tokens_out: 4,
                    tokens_cached: 20,
                }),
            ),
            call_started(3, 101, "t1", "c1", "read"),
            call_settled(4, 102, "t1", "c1", ToolOutcome::Succeeded),
            ev(
                5,
                103,
                SessionEventKind::AssistantMessage(AssistantMessageEvent {
                    turn_id: "t1".into(),
                    content: "here you go".into(),
                    tool_note: String::new(),
                    surface: SurfacePlacement::append(),
                }),
            ),
            ev(
                6,
                103,
                SessionEventKind::TurnCompleted {
                    turn_id: "t1".into(),
                },
            ),
        ];
        let projected = project_runs("s1", &events);
        assert_eq!(projected.len(), 1);
        let ProjectedRun { run, steps, .. } = &projected[0];
        assert_eq!(run.id, "t1");
        assert_eq!(run.session_id, "s1");
        assert_eq!(run.input, "go");
        assert_eq!(run.status, RunStatus::Done);
        assert_eq!(run.final_output, "here you go");
        assert_eq!(run.plan, "1 tool call(s)");
        assert_eq!(
            (run.tokens_in, run.tokens_out, run.tokens_cached),
            (30, 4, 20)
        );
        assert_eq!(run.started_at, 100);
        assert_eq!(run.ended_at, Some(103));
        assert!(!run.recoverable);
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].step.tool_name, "read");
        assert!(steps[0].step.ok);
        assert_eq!(steps[0].step.elapsed_ms, 12);
    }

    #[test]
    fn a_turn_with_no_terminal_event_is_the_one_recovery_can_resume() {
        // The whole point of the fold: "interrupted" used to be a column another
        // process had to flip at startup, and a run whose row update was lost
        // read as still running forever. Here it is the absence of an event.
        let events = vec![
            started(0, 100, "t1"),
            asked(1, 100, "t1", "go"),
            call_started(2, 101, "t1", "c1", "shell"),
        ];
        let projected = project_runs("s1", &events);
        assert_eq!(projected[0].run.status, RunStatus::Running);
        assert!(projected[0].run.recoverable);
        assert_eq!(projected[0].run.ended_at, None);
        // The call is there, and marked as never having finished — the ledger
        // could not say this at all: its step row is written at settle, so a
        // call the process died inside leaves no row, and the record claims the
        // turn never made it.
        assert_eq!(projected[0].steps.len(), 1);
        assert!(!projected[0].steps[0].settled);
        assert!(!projected[0].steps[0].step.ok);
    }

    #[test]
    fn a_cancelled_turn_is_failed_and_not_offered_for_resume() {
        // The user asked it to stop; there is nothing to hand back.
        let events = vec![
            started(0, 100, "t1"),
            asked(1, 100, "t1", "go"),
            ev(
                2,
                101,
                SessionEventKind::TurnCancelled {
                    turn_id: "t1".into(),
                    pristine: false,
                },
            ),
        ];
        let run = &project_runs("s1", &events)[0].run;
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error, CANCELLED_ERROR);
        assert!(!run.recoverable);
    }

    #[test]
    fn an_approval_lands_on_the_call_it_gated() {
        use crate::domain::session_event::ApprovalResolvedEvent;

        // Two calls in flight; only one of them was gated. The approval belongs
        // to its own call, and it resolves while both are still open — so the
        // match is by id, like a settle.
        let events = vec![
            started(0, 100, "t1"),
            call_started(1, 100, "t1", "c1", "read"),
            call_started(2, 100, "t1", "c2", "shell"),
            ev(
                3,
                101,
                SessionEventKind::ApprovalResolved(ApprovalResolvedEvent {
                    turn_id: "t1".into(),
                    call_id: "c2".into(),
                    call_index: 1,
                    allowed: true,
                    decided_by: "human".into(),
                    reason: String::new(),
                    waited_ms: 4_200,
                }),
            ),
            call_settled(4, 102, "t1", "c2", ToolOutcome::Succeeded),
            call_settled(5, 102, "t1", "c1", ToolOutcome::Succeeded),
        ];

        let steps = &project_runs("s1", &events)[0].steps;
        assert_eq!(steps[0].step.tool_name, "read");
        assert!(
            steps[0].step.approved_by.is_empty(),
            "a call nobody gated says nothing about approval"
        );
        assert_eq!(steps[1].step.tool_name, "shell");
        assert_eq!(steps[1].step.approved_by, "human");
        assert_eq!(steps[1].step.approval_waited_ms, 4_200);
    }

    #[test]
    fn settles_match_their_call_by_id_not_by_arrival_order() {
        // A round runs concurrently, so the settles come back in completion
        // order. Pairing them by adjacency would file each result under the
        // wrong tool.
        let events = vec![
            started(0, 100, "t1"),
            call_started(1, 100, "t1", "a", "read"),
            call_started(2, 100, "t1", "b", "shell"),
            call_settled(3, 101, "t1", "b", ToolOutcome::Uncertain),
            call_settled(4, 102, "t1", "a", ToolOutcome::Succeeded),
        ];
        let steps = &project_runs("s1", &events)[0].steps;
        assert_eq!(steps[0].step.tool_name, "read");
        assert!(steps[0].step.ok);
        assert!(steps.iter().all(|s| s.settled));
        assert_eq!(steps[1].step.tool_name, "shell");
        assert!(
            steps[1].step.uncertain,
            "an uncertain call may still have landed"
        );
        assert!(!steps[1].step.ok);
    }

    #[test]
    fn one_log_projects_every_turn_it_holds() {
        let events = vec![
            started(0, 100, "t1"),
            asked(1, 100, "t1", "first"),
            ev(
                2,
                101,
                SessionEventKind::TurnCompleted {
                    turn_id: "t1".into(),
                },
            ),
            started(3, 200, "t2"),
            asked(4, 200, "t2", "second"),
            ev(
                5,
                201,
                SessionEventKind::TurnFailed {
                    turn_id: "t2".into(),
                    error: "boom".into(),
                },
            ),
        ];
        let projected = project_runs("s1", &events);
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].run.input, "first");
        assert_eq!(projected[1].run.status, RunStatus::Failed);
        assert_eq!(projected[1].run.error, "boom");
        assert_eq!(projected[1].run.plan, "respond");
    }

    #[test]
    fn a_continuation_projects_its_link_back_to_the_turn_it_picked_up() {
        let events = vec![
            started(0, 100, "t1"),
            ev(
                1,
                200,
                SessionEventKind::TurnStarted {
                    turn_id: "t2".into(),
                    resumed_from: Some("t1".into()),
                },
            ),
        ];
        let projected = project_runs("s1", &events);
        assert_eq!(projected[0].run.resumed_from, None);
        assert_eq!(projected[1].run.resumed_from, Some("t1".into()));
    }

    #[test]
    fn recall_reaches_the_row_it_shaped() {
        let events = vec![
            started(0, 100, "t1"),
            ev(
                1,
                100,
                SessionEventKind::TurnMemories {
                    turn_id: "t1".into(),
                    memories: RecalledMemories {
                        pinned: vec!["m1".into()],
                        recall: vec!["m2".into()],
                    },
                },
            ),
        ];
        let run = &project_runs("s1", &events)[0].run;
        assert_eq!(run.memories.pinned, vec!["m1".to_string()]);
        assert_eq!(run.memories.recall, vec!["m2".to_string()]);
    }

    #[test]
    fn events_for_a_turn_that_never_opened_are_ignored() {
        // The log is contiguous and checked on read, so a turn with no
        // `turn/started` is not a gap to paper over with a synthesized run.
        let events = vec![
            asked(0, 100, "ghost", "go"),
            call_started(1, 100, "ghost", "c1", "read"),
        ];
        assert!(project_runs("s1", &events).is_empty());
    }

    #[test]
    fn a_claimed_turn_is_not_offered_for_resume_again() {
        // The continuation's own `turn/started` is the claim. Without it a
        // crashed turn stays resumable forever and every restart re-runs it.
        let events = vec![
            started(0, 100, "t1"),
            asked(1, 100, "t1", "go"),
            ev(
                2,
                200,
                SessionEventKind::TurnStarted {
                    turn_id: "t2".into(),
                    resumed_from: Some("t1".into()),
                },
            ),
        ];
        let runs = project_runs("s1", &events);
        assert!(
            !runs[0].run.recoverable,
            "t1 has a continuation, so it is not the log's open turn any more"
        );
        assert!(
            runs[1].run.recoverable,
            "t2 has no terminal event of its own yet"
        );
        assert_eq!(runs[1].run.resumed_from.as_deref(), Some("t1"));
        assert_eq!(
            runs[1].run.input, "go",
            "a continuation is answering the same question, and the row says so"
        );
    }

    #[test]
    fn a_resumable_turn_holds_the_log_back_to_its_first_attempt() {
        // A→B→C, C still open: retention may not cut A's rounds away, because
        // rebuilding C replays them.
        let events = vec![
            started(0, 100, "A"),
            asked(1, 100, "A", "go"),
            ev(
                2,
                200,
                SessionEventKind::TurnStarted {
                    turn_id: "B".into(),
                    resumed_from: Some("A".into()),
                },
            ),
            ev(
                3,
                300,
                SessionEventKind::TurnStarted {
                    turn_id: "C".into(),
                    resumed_from: Some("B".into()),
                },
            ),
        ];
        let runs = project_runs("s1", &events);
        let open = runs.iter().find(|p| p.run.id == "C").unwrap();
        assert!(open.run.recoverable, "C is the attempt still in flight");
        assert_eq!(open.start_seq, 3);
        assert_eq!(
            replay_floor(&runs, open),
            0,
            "the floor reaches back to the turn that first asked the question"
        );
        // A turn nobody resumed answers with its own start.
        let first = runs.iter().find(|p| p.run.id == "A").unwrap();
        assert_eq!(replay_floor(&runs, first), 0);
    }

    #[test]
    fn the_learning_watermark_folds_from_either_verdict() {
        // Both verdicts retire the turn. "Considered and declined" has to read
        // as learned, or every sweep offers the same turn again forever.
        for verdict in [
            SessionEventKind::LearningCompleted {
                turn_id: "t1".into(),
            },
            SessionEventKind::LearningSkipped {
                turn_id: "t1".into(),
                reason: "cancelled turn".into(),
            },
        ] {
            let events = vec![
                started(0, 100, "t1"),
                ev(
                    1,
                    101,
                    SessionEventKind::TurnCompleted {
                        turn_id: "t1".into(),
                    },
                ),
                ev(2, 102, verdict),
            ];
            assert!(project_runs("s1", &events)[0].run.learned);
        }
    }

    #[test]
    fn a_turn_nobody_has_learned_from_folds_unlearned() {
        let events = vec![
            started(0, 100, "t1"),
            ev(
                1,
                101,
                SessionEventKind::TurnCompleted {
                    turn_id: "t1".into(),
                },
            ),
            // Another turn's watermark says nothing about this one.
            started(2, 102, "t2"),
            ev(
                3,
                103,
                SessionEventKind::LearningCompleted {
                    turn_id: "t2".into(),
                },
            ),
        ];
        let runs = project_runs("s1", &events);
        assert!(!runs[0].run.learned);
        assert!(runs[1].run.learned);
    }
}
