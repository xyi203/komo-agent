//! What a session is waiting for, read out of the session event log.
//!
//! A suspended turn is invisible today: it holds no session slot, writes no
//! assistant message, and its row says `suspended` in a ledger nobody watches.
//! The conversation it stopped in looks idle — which is the one thing it is not.
//!
//! [`Awaiting`] is that state, and like the run ledger it is a **projection**:
//! `turn/suspended` says a turn stopped and what for, `wakeup/fired` says the
//! wait is over, and folding the two is the whole definition. The session row's
//! column is a query index over this fold, never a second place the fact lives —
//! clear it and [`project_awaiting`] puts it back.

use serde::{Deserialize, Serialize};

use super::session_event::{SessionEvent, SessionEventKind, WakeupKind};

/// One session's open wait: what it is for, since when, and until when.
///
/// `turn_id` is what makes the fold composable — a `wakeup/fired` names the turn
/// it wakes, so a tail folded onto an earlier state can tell "the wait I am
/// holding just ended" from "some other turn's did".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Awaiting {
    pub turn_id: String,
    pub kind: WakeupKind,
    /// When the turn suspended.
    pub since: i64,
    /// One line for the operator: what it is waiting for, in words.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

impl Awaiting {
    /// The operator-facing line: `等你审批 · 已 3h`.
    ///
    /// One implementation for every surface — the session list, the TUI status
    /// row — because a wait that reads differently in two places reads as two
    /// waits.
    pub fn label(&self, now: i64) -> String {
        format!("{} · 已 {}", self.kind.label(), elapsed(now - self.since))
    }
}

/// A duration in the coarsest unit that still says something: a wait is read to
/// decide whether to go answer it, and `已 3h` decides that as well as
/// `已 3h 12min 40s` does.
fn elapsed(seconds: i64) -> String {
    let s = seconds.max(0);
    match s {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}min", s / 60),
        s if s < 86_400 => format!("{}h", s / 3600),
        s => format!("{}d", s / 86_400),
    }
}

/// Fold `events` onto the wait a previous fold left.
///
/// Folding a prefix and then the rest gives what folding everything gives, so a
/// turn's own tail is enough to keep the projection current — the same property
/// the surface checkpoint rests on. Pass `prior = None` with a whole log to
/// rebuild from scratch.
///
/// A wait ends the moment anything says so: the wake that fired it, the
/// continuation that picked the turn up, or the turn ending some other way.
/// Nothing here waits for a terminal event, because a suspended turn never gets
/// one — its continuation is a turn of its own.
pub fn project_awaiting(prior: Option<Awaiting>, events: &[SessionEvent]) -> Option<Awaiting> {
    let mut awaiting = prior;
    for event in events {
        let holding = |turn_id: &str| {
            awaiting
                .as_ref()
                .is_some_and(|open: &Awaiting| open.turn_id == turn_id)
        };
        match &event.kind {
            SessionEventKind::TurnSuspended(suspended) => {
                awaiting = Some(Awaiting {
                    turn_id: suspended.turn_id.clone(),
                    kind: suspended.wakeup.kind(),
                    since: event.at.unix_timestamp(),
                    summary: suspended.summary.clone(),
                    expires_at: suspended.expires_at,
                });
            }
            SessionEventKind::WakeupFired(fired) if holding(&fired.turn_id) => awaiting = None,
            SessionEventKind::TurnStarted {
                resumed_from: Some(from),
                ..
            } if holding(from) => awaiting = None,
            SessionEventKind::TurnCompleted { turn_id }
            | SessionEventKind::TurnFailed { turn_id, .. }
            | SessionEventKind::TurnCancelled { turn_id, .. }
                if holding(turn_id) =>
            {
                awaiting = None
            }
            _ => {}
        }
    }
    awaiting
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::session_event::{TurnSuspendedEvent, Wakeup, WakeupCause, WakeupFiredEvent};

    fn event(seq: u64, at: i64, kind: SessionEventKind) -> SessionEvent {
        SessionEvent::new(
            seq,
            time::OffsetDateTime::from_unix_timestamp(at).unwrap(),
            kind,
        )
    }

    fn suspended(seq: u64, at: i64) -> SessionEvent {
        event(
            seq,
            at,
            SessionEventKind::TurnSuspended(TurnSuspendedEvent {
                turn_id: "t1".to_string(),
                wakeup: Wakeup::Approval {
                    call_id: "c1".to_string(),
                },
                call_id: "c1".to_string(),
                summary: "rm -rf build".to_string(),
                expires_at: Some(at + 86_400),
            }),
        )
    }

    fn fired(seq: u64, at: i64, cause: WakeupCause) -> SessionEvent {
        event(
            seq,
            at,
            SessionEventKind::WakeupFired(WakeupFiredEvent {
                turn_id: "t1".to_string(),
                wakeup_id: "wk-1".to_string(),
                cause,
                payload: String::new(),
            }),
        )
    }

    #[test]
    fn a_suspended_turn_is_a_session_that_is_waiting() {
        let awaiting = project_awaiting(None, &[suspended(1, 1_000)]).expect("waiting");
        assert_eq!(awaiting.turn_id, "t1");
        assert_eq!(awaiting.kind, WakeupKind::Approval);
        assert_eq!(awaiting.since, 1_000);
        assert_eq!(awaiting.summary, "rm -rf build");
        assert_eq!(awaiting.expires_at, Some(87_400));
        assert_eq!(awaiting.label(1_000 + 3 * 3600), "等你审批 · 已 3h");
    }

    #[test]
    fn an_answered_approval_ends_the_wait() {
        let events = [suspended(1, 1_000), fired(2, 2_000, WakeupCause::Approve)];
        assert_eq!(project_awaiting(None, &events), None);
    }

    /// Running out is an ending like any other: the turn comes back to be told
    /// nobody answered, and a session still showing "等你审批" would be pointing
    /// at a question that is over.
    #[test]
    fn a_wait_that_ran_out_ends_the_wait() {
        let events = [suspended(1, 1_000), fired(2, 90_000, WakeupCause::Expired)];
        assert_eq!(project_awaiting(None, &events), None);
    }

    /// The tail a turn's settle folds starts at that turn's own `turn/started`,
    /// so an unrelated turn running while another waits says nothing about the
    /// wait — and must not clear it.
    #[test]
    fn another_turn_running_leaves_the_wait_alone() {
        let prior = project_awaiting(None, &[suspended(1, 1_000)]);
        let tail = [
            event(
                2,
                2_000,
                SessionEventKind::TurnStarted {
                    turn_id: "t2".to_string(),
                    resumed_from: None,
                },
            ),
            event(
                3,
                2_100,
                SessionEventKind::TurnCompleted {
                    turn_id: "t2".to_string(),
                },
            ),
        ];
        assert_eq!(project_awaiting(prior.clone(), &tail), prior);
    }

    /// The continuation claims the turn under its own `turn/started`, which the
    /// ledger already treats as the claim — so the badge comes off when the work
    /// restarts, not when it finishes.
    #[test]
    fn the_continuation_that_picks_the_turn_up_ends_the_wait() {
        let prior = project_awaiting(None, &[suspended(1, 1_000)]);
        let tail = [event(
            2,
            2_000,
            SessionEventKind::TurnStarted {
                turn_id: "t2".to_string(),
                resumed_from: Some("t1".to_string()),
            },
        )];
        assert_eq!(project_awaiting(prior, &tail), None);
    }

    #[test]
    fn every_kind_says_what_it_is_waiting_for() {
        assert_eq!(WakeupKind::Approval.label(), "等你审批");
        assert_eq!(WakeupKind::UserReply.label(), "等待回答");
        assert_eq!(WakeupKind::At.label(), "定时等待");
        assert_eq!(WakeupKind::TaskDone.label(), "等后台任务");
        assert_eq!(WakeupKind::Event.label(), "等事件");
    }

    #[test]
    fn elapsed_scales_to_the_unit_that_still_says_something() {
        assert_eq!(elapsed(-5), "0s");
        assert_eq!(elapsed(41), "41s");
        assert_eq!(elapsed(2_400), "40min");
        assert_eq!(elapsed(3 * 3600 + 700), "3h");
        assert_eq!(elapsed(3 * 86_400), "3d");
    }
}
