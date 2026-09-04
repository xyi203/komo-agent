//! The `ask_user` sentinel tool: pause the current turn on a question to the
//! user and continue with their answer (docs/bot-runtime.md §4.3).
//!
//! This is the mid-turn clarification path — unlike ending the turn with a
//! question, the work already done stays done: the call that asked is left
//! unsettled, the turn suspends, and when the answer arrives that same call is
//! re-dispatched and returns it.
//!
//! It waits on the log, not in memory. A question used to be a `oneshot` this
//! tool awaited for ten minutes, which meant the turn held its session slot
//! while a person thought, and a restart lost both the question and the work
//! behind it. Now the wait is a `turn/suspended{UserReply}` plus a registration
//! (seven days), so the answer can arrive in another process — and the user can
//! say something else in the meantime without queueing behind a turn that is
//! only waiting for them.
//!
//! "The next plain message is the answer" is unchanged; `/skip` declines
//! explicitly. Every ending that is not an answer — nobody able to answer, the
//! budget spent, the question undeliverable, seven days of silence — returns
//! the same guidance text, so the model's recovery is uniform: state the
//! assumption and continue, or wrap up.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    context::{ToolContext, WaitRefused},
    session_event::{Wakeup, WakeupCause},
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

/// How many times one turn may ask the user (anti-interrogation cap).
pub const CLARIFY_BUDGET_PER_TURN: usize = 2;

#[derive(Deserialize)]
struct AskArgs {
    question: String,
    /// Optional candidate answers, rendered as a numbered list (the user may
    /// reply with a number or free text).
    #[serde(default)]
    options: Vec<String>,
}

/// What the model is told when nobody can answer — same wording for the
/// no-session, non-interactive, expired and skipped cases so the recovery
/// behavior is uniform: state the assumption and continue, or wrap up.
const NO_ANSWER: &str = "No answer from the user (unavailable or did not reply in time). \
     Proceed with your best assumption, stating it explicitly in your reply — \
     or conclude the turn if you cannot proceed safely.";

#[derive(Default)]
pub struct AskUserTool;

impl AskUserTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for AskUserTool {
    fn name(&self) -> &'static str {
        "ask_user"
    }

    fn description(&self) -> &'static str {
        "Ask the user one clarifying question mid-task and wait for their answer. \
         Use when a key parameter is ambiguous, the target of an action is unclear, \
         or an irreversible action's intent is uncertain — BEFORE guessing. \
         Do not use it for things you can safely infer or look up yourself. \
         Budget: at most 2 questions per turn."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The question to ask, in the user's language, specific enough to be answered in one message."
                },
                "options": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Optional candidate answers, shown as a numbered list."
                }
            },
            "required": ["question"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: AskArgs = parse_args(&input)?;

        // Back from the wait: this call is being re-dispatched because the
        // question was answered, declined, or ran out. Never asked twice — the
        // arguments are the same ones, so a second prompt would be the same
        // question again.
        if let Some(wake) = ctx.resumed_wait() {
            return Ok(ToolOutput::text(match wake.cause {
                WakeupCause::Reply if !wake.payload.trim().is_empty() => {
                    format!("User answered: {}", pick(&args.options, &wake.payload))
                }
                _ => NO_ANSWER.to_string(),
            }));
        }

        // Someone must be able to answer: a real chat session with a human
        // watching. Sweeps, aux sub-agents and detached contexts are
        // non-interactive and get the degrade text instead of a wait nobody
        // will end.
        let sc = &ctx.session;
        if !sc.interactive {
            return Ok(ToolOutput::text(NO_ANSWER));
        }
        if asked(ctx) >= CLARIFY_BUDGET_PER_TURN {
            return Ok(ToolOutput::text(
                "Clarify budget exhausted for this turn (2 questions max). Proceed with \
                 your best assumption, stating it explicitly, or conclude the turn.",
            ));
        }

        let mut prompt = format!("❓ {}", args.question.trim());
        for (i, option) in args.options.iter().enumerate() {
            prompt.push_str(&format!("\n{}. {}", i + 1, option));
        }
        if !args.options.is_empty() {
            prompt.push_str("\n（回复编号或直接输入答案）");
        }

        // Registered before the prompt goes out, so an instant reply cannot
        // land on a wait that does not exist yet.
        match ctx.wait_for(Wakeup::UserReply, args.question.trim(), None) {
            Ok(()) => {}
            // Nothing here can stop the turn, so nobody could ever bring it
            // back with an answer.
            Err(WaitRefused::Unsupported | WaitRefused::Superseded) => {
                return Ok(ToolOutput::text(NO_ANSWER));
            }
        }
        if let Err(error) = sc.sink.send(&prompt).await {
            // The question never reached anyone. The wait stands until it
            // expires — but the model is told now, so it can proceed on an
            // assumption rather than sit behind a question nobody saw.
            return Ok(ToolOutput::text(format!(
                "Could not deliver the question ({error}). {NO_ANSWER}"
            )));
        }
        // Discarded: the turn ends here and this call comes back with the
        // answer.
        Ok(ToolOutput::text("Waiting for the user's answer."))
    }
}

/// How many questions this turn has already asked, counted from the log so a
/// suspension does not reset it.
fn asked(ctx: &ToolContext) -> usize {
    ctx.waits_taken()
        .iter()
        .filter(|wakeup| matches!(wakeup, Wakeup::UserReply))
        .count()
}

/// Echo a numbered-option pick back as its text, so the model never has to
/// re-map "2" onto the option list.
fn pick(options: &[String], answer: &str) -> String {
    options
        .iter()
        .enumerate()
        .find(|(i, _)| answer.trim() == (i + 1).to_string())
        .map(|(_, option)| option.clone())
        .unwrap_or_else(|| answer.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
    use komo_core::domain::context::{RunContext, SessionContext, ToolContext};
    use komo_core::domain::gateway::ReplySink;
    use komo_core::domain::session_event::{ResumedWait, TurnWaits};
    use std::sync::{Arc, Mutex};

    struct RecordingSink {
        sent: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ReplySink for RecordingSink {
        async fn send(&self, text: &str) -> anyhow::Result<()> {
            self.sent.lock().unwrap().push(text.to_string());
            Ok(())
        }
    }

    struct DenyAll;

    #[async_trait]
    impl Approver for DenyAll {
        async fn decide(&self, _request: &ApprovalRequest) -> Decision {
            Decision::deny()
        }
    }

    fn interactive_ctx(session: &str, sent: Arc<Mutex<Vec<String>>>) -> ToolContext {
        let sc = SessionContext {
            session_id: session.to_string(),
            workspace_root: None,
            sink: Arc::new(RecordingSink { sent }),
            interactive: true,
            auto_approve: false,
            event_sink: None,
            cancel: None,
            interject: None,
            channel: None,
            origin: Default::default(),
        };
        ToolContext::new(sc, Some(RunContext::new("t1".into())), Arc::new(DenyAll))
            .with_call("c1", 0)
    }

    fn v(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[tokio::test]
    async fn asking_suspends_the_turn_on_the_question() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let ctx = interactive_ctx("s1", sent.clone());
        let out = AskUserTool::new()
            .call(v(r#"{"question":"红的还是蓝的?"}"#), &ctx)
            .await
            .unwrap();
        assert!(out.text.contains("Waiting"), "{}", out.text);
        assert!(sent.lock().unwrap()[0].contains("红的还是蓝的"));
        let pending = ctx
            .run
            .as_ref()
            .unwrap()
            .suspension()
            .expect("the call stopped the turn to wait");
        assert_eq!(pending.wakeup, Wakeup::UserReply);
        assert_eq!(pending.call_id, "c1");
    }

    /// The way back: the same call, re-dispatched with the answer, returns it
    /// instead of asking again.
    #[tokio::test]
    async fn the_answer_comes_back_through_the_same_call() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let ctx = interactive_ctx("s2", sent.clone());
        ctx.run.as_ref().unwrap().resumed_with(TurnWaits {
            taken: vec![Wakeup::UserReply],
            resumed: Some(ResumedWait {
                call_id: "c1".into(),
                wakeup: Wakeup::UserReply,
                cause: WakeupCause::Reply,
                payload: "2".into(),
            }),
        });
        let out = AskUserTool::new()
            .call(
                v(r#"{"question":"which?","options":["apple","banana"]}"#),
                &ctx,
            )
            .await
            .unwrap();
        assert_eq!(out.text, "User answered: banana");
        assert!(
            sent.lock().unwrap().is_empty(),
            "a re-dispatched question is not asked again"
        );
        assert!(ctx.run.as_ref().unwrap().suspension().is_none());
    }

    #[tokio::test]
    async fn an_unanswered_question_comes_back_as_no_answer() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let ctx = interactive_ctx("s3", sent.clone());
        ctx.run.as_ref().unwrap().resumed_with(TurnWaits {
            taken: vec![Wakeup::UserReply],
            resumed: Some(ResumedWait {
                call_id: "c1".into(),
                wakeup: Wakeup::UserReply,
                cause: WakeupCause::Expired,
                payload: String::new(),
            }),
        });
        let out = AskUserTool::new()
            .call(v(r#"{"question":"?"}"#), &ctx)
            .await
            .unwrap();
        assert_eq!(out.text, NO_ANSWER);
    }

    /// The budget is read from what the log says this turn already asked, so it
    /// survives the suspension between question and answer.
    #[tokio::test]
    async fn budget_exhaustion_reports_instead_of_asking() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let ctx = interactive_ctx("s4", sent.clone());
        ctx.run.as_ref().unwrap().resumed_with(TurnWaits {
            taken: vec![Wakeup::UserReply, Wakeup::UserReply],
            resumed: None,
        });
        let out = AskUserTool::new()
            .call(v(r#"{"question":"?"}"#), &ctx)
            .await
            .unwrap();
        assert!(out.text.contains("budget exhausted"), "{}", out.text);
        assert!(sent.lock().unwrap().is_empty(), "no prompt sent");
        assert!(ctx.run.as_ref().unwrap().suspension().is_none());
    }

    #[tokio::test]
    async fn no_one_to_ask_degrades_instead_of_waiting() {
        let sc = SessionContext::detached("s5");
        let ctx = ToolContext::new(sc, Some(RunContext::new("t1".into())), Arc::new(DenyAll))
            .with_call("c1", 0);
        let out = AskUserTool::new()
            .call(v(r#"{"question":"?"}"#), &ctx)
            .await
            .unwrap();
        assert_eq!(out.text, NO_ANSWER);
        assert!(ctx.run.as_ref().unwrap().suspension().is_none());
    }
}
