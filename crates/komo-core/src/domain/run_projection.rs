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
//! What it deliberately cannot reproduce is [`Run::outcome`] — the outcome
//! assessment is *revisable* by what the user says in a later turn, so it is
//! held on the row and merged over the projection rather than derived from it.

use super::cancel::CANCELLED_ERROR;
use super::run::{RUN_FIELD_CAP, Run, RunStatus, RunStep, truncate};
use super::session_event::{MessageSource, SessionEvent, SessionEventKind, ToolOutcome};

/// One turn, as the log records it.
#[derive(Debug, Clone)]
pub struct ProjectedRun {
    pub run: Run,
    pub steps: Vec<ProjectedStep>,
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
                    },
                });
                open.push((
                    turn_id.to_string(),
                    call.call_id.clone(),
                    at_index,
                    projected.steps.len() - 1,
                ));
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
            SessionEventKind::LearningCompleted { .. }
            | SessionEventKind::LearningSkipped { .. } => {
                projected.run.learned = true;
            }
            _ => {}
        }
    }

    for projected in &mut runs {
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
        let ProjectedRun { run, steps } = &projected[0];
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
}
