//! The `wait` tool: the model's own way to stop a turn until something happens
//! (docs/bot-runtime.md §3.4).
//!
//! Same shape as `ask_user`, and the same primitive underneath: the call asks
//! to be woken, the turn suspends without settling it, and when the wake
//! arrives that **same call** is re-dispatched — this time returning what
//! ended the wait. Nothing here polls, sleeps, or holds a session slot: a turn
//! waiting two hours costs nothing but a row, and survives a restart because
//! the log says it is waiting and the registration says when to come back.
//!
//! Three ways to name what to wait for, exactly one per call:
//!
//! - `until` — a relative delay (`2h`) or a local wall-clock time
//!   (`2026-09-03 09:00`). The sweep fires it; this is what lets a routine
//!   check something, wait, and check again inside one turn.
//! - `for_task` — a background task this turn started. The task runtime is
//!   §5.9 and does not exist yet, so **nothing fires this today**: the
//!   registration is written and, having no deadline of its own (the task's
//!   own timeout is meant to be the deadline), it stands until something
//!   settles the task.
//! - `for_event` — a named inbound webhook. The ingress is §5.12 and does not
//!   exist yet either, so today this stands until its 30-day expiry brings the
//!   turn back saying nothing arrived.
//!
//! **An unattended turn may wait too** — a routine's grants ride across the
//! wait on the registration, so it comes back able to act.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    context::{ToolContext, WaitRefused},
    session_event::{EventFilter, Wakeup, WakeupCause},
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

use crate::reminder::parse_after;

/// How many times one turn may stop to wait. A turn that keeps waiting is a
/// turn that never answers, and `wait` must not become a way to stay alive
/// forever; past the budget the model is told to finish or schedule a job
/// instead.
pub const WAIT_BUDGET_PER_TURN: usize = 4;

#[derive(Deserialize)]
struct WaitArgs {
    #[serde(default)]
    until: Option<String>,
    #[serde(default)]
    for_task: Option<String>,
    #[serde(default)]
    for_event: Option<EventArgs>,
}

#[derive(Deserialize)]
struct EventArgs {
    webhook: String,
}

pub struct WaitTool;

#[async_trait]
impl Tool for WaitTool {
    fn name(&self) -> &'static str {
        "wait"
    }

    fn description(&self) -> &'static str {
        "Stop this turn until a moment arrives, then continue from here with \
         everything you have already done still in hand. The turn is suspended, \
         not blocked: it survives a restart, and the answer you eventually give \
         lands in this same conversation. Use it for \"check back later\" work \
         (deploy finishing, a build settling). Do NOT use it as a sleep before \
         an answer you could give now, and do not use it for something that \
         should happen on a schedule — that is the `cron` tool."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "until": {
                    "type": "string",
                    "description": "A delay (\"45s\", \"5m\", \"2h\", \"1d\") or a local wall-clock time (\"2026-09-03 09:00\", must be in the future)."
                },
                "for_task": {
                    "type": "string",
                    "description": "Id of a background task to wait for."
                },
                "for_event": {
                    "type": "object",
                    "properties": {
                        "webhook": { "type": "string", "description": "Name of the inbound webhook to wait for." }
                    },
                    "required": ["webhook"],
                    "description": "Wait for a named external event."
                }
            }
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: WaitArgs = parse_args(&input)?;

        // Back from the wait this call asked for. Returned, never re-armed:
        // waiting again here would park the turn on the same question forever.
        if let Some(wake) = ctx.resumed_wait() {
            return Ok(
                ToolOutput::text(describe(&wake.wakeup, wake.cause, &wake.payload))
                    .with_structured(
                        json!({ "cause": wake.cause.as_str(), "kind": wake.wakeup.kind() }),
                    ),
            );
        }

        if taken(ctx) >= WAIT_BUDGET_PER_TURN {
            return Ok(ToolOutput::text(format!(
                "Wait budget exhausted for this turn ({WAIT_BUDGET_PER_TURN} waits max). \
                 Finish with what you have, or schedule the follow-up as a cron job \
                 instead of waiting again."
            )));
        }

        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let wakeup = match (args.until, args.for_task, args.for_event) {
            (Some(until), None, None) => Wakeup::At {
                at: parse_until(&until, now).map_err(|e| ToolError::InvalidInput(e.to_string()))?,
            },
            (None, Some(task_id), None) => Wakeup::TaskDone { task_id },
            (None, None, Some(event)) => Wakeup::Event {
                filter: EventFilter::Webhook {
                    name: event.webhook,
                },
            },
            _ => {
                return Err(ToolError::InvalidInput(
                    "pass exactly one of `until`, `for_task`, `for_event`".into(),
                ));
            }
        };

        let summary = summarize(&wakeup);
        match ctx.wait_for(wakeup, summary.clone(), None) {
            // Discarded: the turn ends here, and this call is re-dispatched
            // when the wake arrives.
            Ok(()) => Ok(ToolOutput::text(format!("Waiting {summary}."))),
            Err(WaitRefused::Unsupported) => Ok(ToolOutput::text(
                "This runtime cannot suspend a turn, so there is no way to wait here. \
                 Continue without waiting, or schedule the follow-up as a cron job.",
            )),
            Err(WaitRefused::Superseded) => Ok(ToolOutput::text(
                "Another call in this round stopped the turn first, so this wait was not \
                 registered. Ask for it again if you still need it.",
            )),
        }
    }
}

/// How many waits of this tool's kinds the turn has already taken. An approval
/// or a question is somebody being asked something — not this budget's
/// business.
fn taken(ctx: &ToolContext) -> usize {
    ctx.waits_taken()
        .iter()
        .filter(|wakeup| {
            matches!(
                wakeup,
                Wakeup::At { .. } | Wakeup::TaskDone { .. } | Wakeup::Event { .. }
            )
        })
        .count()
}

/// `2h` / `45m` / `1d`, or a local wall-clock `YYYY-MM-DD HH:MM`, as a Unix
/// instant. The absolute form is the cron one-shot parser, so "already past"
/// and the DST gap are rejected here exactly as they are for `@at` jobs.
fn parse_until(until: &str, now: i64) -> anyhow::Result<i64> {
    let until = until.trim();
    match parse_after(until) {
        Ok(delay) if delay.as_secs() > 0 => Ok(now + delay.as_secs() as i64),
        Ok(_) => anyhow::bail!("`until` is zero; nothing to wait for"),
        Err(_) => komo_core::domain::cron::next_occurrence_local(
            &format!("{}{until}", komo_core::domain::cron::ONCE_PREFIX),
            now,
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "invalid `until` `{until}`: expected a delay like `2h` or a future local \
                 time like `2026-09-03 09:00` ({e})"
            )
        }),
    }
}

/// One line for the operator (`komo session list`, the suspension event) and,
/// reused, for the model's own "waiting …" note.
fn summarize(wakeup: &Wakeup) -> String {
    match wakeup {
        Wakeup::At { at } => format!("until {}", local_time(*at)),
        Wakeup::TaskDone { task_id } => format!("for background task {task_id}"),
        Wakeup::Event {
            filter: EventFilter::Webhook { name },
        } => format!("for the `{name}` webhook"),
        Wakeup::Event {
            filter: EventFilter::FromPeer { platform, peer_id },
        } => format!("for a message from {platform}:{peer_id}"),
        Wakeup::Approval { .. } => "for an approval".to_string(),
        Wakeup::UserReply => "for the user's reply".to_string(),
    }
}

/// What the model is handed on the way back. Every cause is named, including
/// the one that means nobody came: an expiry that read as silence would leave
/// the model to guess whether the thing it waited for happened.
fn describe(wakeup: &Wakeup, cause: WakeupCause, payload: &str) -> String {
    let detail = match payload.trim() {
        "" => String::new(),
        text => format!(" {text}"),
    };
    match cause {
        WakeupCause::Time => format!(
            "The wait is over — {} has arrived. Continue.",
            summarize(wakeup).trim_start_matches("until ")
        ),
        WakeupCause::Task => format!("The task you waited for finished.{detail}"),
        WakeupCause::Event => format!("The event you waited for arrived.{detail}"),
        WakeupCause::Reply | WakeupCause::MovedOn => {
            format!("The user spoke instead of waiting it out.{detail}")
        }
        WakeupCause::Expired => format!(
            "Nothing happened before this wait ran out ({}). It will not arrive — \
             say so and continue without it.",
            summarize(wakeup)
        ),
        WakeupCause::Approve | WakeupCause::Deny => {
            format!("The wait ended with an approval decision.{detail}")
        }
    }
}

fn local_time(at: i64) -> String {
    chrono::DateTime::from_timestamp(at, 0)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| at.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
    use komo_core::domain::context::{RunContext, SessionContext, ToolContext};
    use komo_core::domain::session_event::{ResumedWait, TurnWaits};
    use std::sync::Arc;

    struct DenyAll;

    #[async_trait]
    impl Approver for DenyAll {
        async fn decide(&self, _request: &ApprovalRequest) -> Decision {
            Decision::deny()
        }
    }

    fn ctx() -> ToolContext {
        ToolContext::new(
            SessionContext::detached("s1"),
            Some(RunContext::new("t1".into())),
            Arc::new(DenyAll),
        )
        .with_call("c1", 0)
    }

    fn v(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[tokio::test]
    async fn a_delay_stops_the_turn_on_a_timer() {
        let ctx = ctx();
        WaitTool.call(v(r#"{"until":"2h"}"#), &ctx).await.unwrap();
        let pending = ctx
            .run
            .as_ref()
            .unwrap()
            .suspension()
            .expect("the call stopped the turn");
        assert_eq!(pending.call_id, "c1");
        assert!(matches!(pending.wakeup, Wakeup::At { .. }));
        assert_eq!(
            pending.expires_at, None,
            "a timer's own instant is its deadline"
        );
    }

    /// The way back: the same call, re-dispatched with the wake, reports it
    /// rather than registering a second wait — otherwise a `wait` could never
    /// end.
    #[tokio::test]
    async fn a_timer_that_came_due_reports_instead_of_waiting_again() {
        let ctx = ctx();
        ctx.run.as_ref().unwrap().resumed_with(TurnWaits {
            taken: vec![Wakeup::At { at: 1_000 }],
            resumed: Some(ResumedWait {
                call_id: "c1".into(),
                wakeup: Wakeup::At { at: 1_000 },
                cause: WakeupCause::Time,
                payload: String::new(),
            }),
        });
        let out = WaitTool.call(v(r#"{"until":"2h"}"#), &ctx).await.unwrap();
        assert!(out.text.contains("The wait is over"), "{}", out.text);
        assert!(ctx.run.as_ref().unwrap().suspension().is_none());
    }

    /// A turn that keeps waiting never answers. Counted from what the log says
    /// this turn already waited for, so the count survives the suspensions it
    /// is counting.
    #[tokio::test]
    async fn a_spent_budget_reports_instead_of_stopping_the_turn() {
        let ctx = ctx();
        ctx.run.as_ref().unwrap().resumed_with(TurnWaits {
            taken: vec![Wakeup::At { at: 1 }; WAIT_BUDGET_PER_TURN],
            resumed: None,
        });
        let out = WaitTool.call(v(r#"{"until":"2h"}"#), &ctx).await.unwrap();
        assert!(out.text.contains("budget exhausted"), "{}", out.text);
        assert!(
            ctx.run.as_ref().unwrap().suspension().is_none(),
            "over budget, the turn keeps going"
        );
    }

    #[tokio::test]
    async fn exactly_one_of_the_three_ways_to_wait() {
        let ctx = ctx();
        for bad in [r#"{}"#, r#"{"until":"2h","for_task":"t1"}"#] {
            assert!(
                matches!(
                    WaitTool.call(v(bad), &ctx).await,
                    Err(ToolError::InvalidInput(_))
                ),
                "{bad}"
            );
        }
        assert!(ctx.run.as_ref().unwrap().suspension().is_none());
    }

    /// Nothing fires a task or an event wait yet (§5.9 / §5.12), so the
    /// registration's shape is all there is to check — and an event wait must
    /// carry a deadline, or the turn would stand forever.
    #[tokio::test]
    async fn an_event_wait_is_registered_with_its_filter() {
        let ctx = ctx();
        WaitTool
            .call(v(r#"{"for_event":{"webhook":"ci-done"}}"#), &ctx)
            .await
            .unwrap();
        let pending = ctx.run.as_ref().unwrap().suspension().unwrap();
        assert_eq!(
            pending.wakeup,
            Wakeup::Event {
                filter: EventFilter::Webhook {
                    name: "ci-done".into()
                }
            }
        );
        assert!(
            komo_core::domain::wakeup::default_expiry_secs(&pending.wakeup).is_some(),
            "an event that never arrives has to come back as expired"
        );
    }

    #[test]
    fn a_relative_delay_lands_that_far_ahead() {
        assert_eq!(parse_until("2h", 1_000).unwrap(), 1_000 + 7_200);
        assert_eq!(parse_until(" 45s ", 0).unwrap(), 45);
    }

    #[test]
    fn an_absolute_time_must_be_in_the_future() {
        let now = chrono::Local::now().timestamp();
        let future = chrono::DateTime::from_timestamp(now + 86_400, 0)
            .unwrap()
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M")
            .to_string();
        assert!(parse_until(&future, now).unwrap() > now);
        assert!(
            parse_until("2020-01-01 08:00", now).is_err(),
            "a time already past is a typo, not a wait"
        );
        assert!(parse_until("next tuesday", now).is_err());
    }

    /// The expiry text has to say the wait failed, not stay silent about it:
    /// "nothing arrived" and "I was never told" look identical to the model
    /// otherwise.
    #[test]
    fn an_expired_wait_says_nothing_came() {
        let wakeup = Wakeup::Event {
            filter: EventFilter::Webhook { name: "ci".into() },
        };
        let text = describe(&wakeup, WakeupCause::Expired, "");
        assert!(text.contains("ran out"), "{text}");
        assert!(text.contains("ci"), "{text}");
    }
}
