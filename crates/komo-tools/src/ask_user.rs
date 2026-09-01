//! The `ask_user` sentinel tool (roadmap §7 "clarify"): pause the current turn
//! on a question to the user and resume with their answer.
//!
//! This is the mid-turn clarification path — unlike ending the turn with a
//! question, the turn's tool-call context (which lives only in the driver's
//! memory) survives, so the model doesn't redo completed work after the user
//! answers. The suspension machinery mirrors chat approvals: the question goes
//! out through the session's `ReplySink`, the tool awaits a per-session
//! `oneshot` in [`ClarifyState`], and the dispatcher (or the TUI) resolves it
//! with the user's next plain message.
//!
//! Degrades instead of erroring: no session / non-interactive context /
//! timeout / exhausted budget all return guidance text the model can act on
//! (proceed on stated assumptions, or conclude). `Risk::Safe` — asking is not
//! a side effect — but each ask is a normal `RunStep`, so the ledger shows
//! what was asked and answered.

use komo_services::clarify::ClarifyState;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    context::ToolContext,
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

#[derive(Deserialize)]
struct AskArgs {
    question: String,
    /// Optional candidate answers, rendered as a numbered list (the user may
    /// reply with a number or free text).
    #[serde(default)]
    options: Vec<String>,
}

/// What the model is told when nobody can answer — same wording for the
/// no-session, non-interactive, and timeout cases so the recovery behavior is
/// uniform: state the assumption and continue, or wrap up.
const NO_ANSWER: &str = "No answer from the user (unavailable or did not reply in time). \
     Proceed with your best assumption, stating it explicitly in your reply — \
     or conclude the turn if you cannot proceed safely.";

pub struct AskUserTool {
    clarify: Arc<ClarifyState>,
}

impl AskUserTool {
    pub fn new(clarify: Arc<ClarifyState>) -> Self {
        Self { clarify }
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

        // Someone must be able to answer: a real chat session with a human
        // watching. Sweeps, aux sub-agents, the HTTP API, and detached
        // contexts are non-interactive and get the degrade text instead of a
        // dead wait.
        let sc = &ctx.session;
        if !sc.interactive {
            return Ok(ToolOutput::text(NO_ANSWER));
        }
        if !self.clarify.try_claim_budget(&sc.session_id) {
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

        // One question at a time per session, and the whole thing under one
        // deadline. The gate wait is *inside* the timeout on purpose: a second
        // question queued behind a slow first one must still be bounded by
        // `CLARIFY_TIMEOUT` rather than stacking another full wait on top of it
        // (which would outlive the tool's advertised `max_duration`).
        let asked = tokio::time::timeout(self.clarify.timeout, async {
            let gate = self.clarify.gate(&sc.session_id);
            let _guard = gate.lock().await;

            // Register BEFORE sending, so an instant reply can't race the window
            // between the prompt landing and the waiter existing. The prompt text
            // is stored too, so a non-sink surface (the GUI's interactions poll)
            // can render the question.
            let rx = self.clarify.register(&sc.session_id, &prompt);
            if let Err(error) = sc.sink.send(&prompt).await {
                self.clarify.forget_pending(&sc.session_id);
                return Asked::Undeliverable(error.to_string());
            }
            match rx.await {
                Ok(answer) => Asked::Answered(answer),
                // Superseded or cleared (e.g. `/new`).
                Err(_) => Asked::NoAnswer,
            }
        })
        .await;

        match asked {
            Ok(Asked::Answered(answer)) => {
                // Echo numbered-option picks back as their text so the model
                // never has to re-map "2" to the option list.
                let answer = args
                    .options
                    .iter()
                    .enumerate()
                    .find(|(i, _)| answer.trim() == (i + 1).to_string())
                    .map(|(_, opt)| opt.clone())
                    .unwrap_or(answer);
                Ok(ToolOutput::text(format!("User answered: {answer}")))
            }
            Ok(Asked::Undeliverable(error)) => Ok(ToolOutput::text(format!(
                "Could not deliver the question ({error}). {NO_ANSWER}"
            ))),
            Ok(Asked::NoAnswer) => Ok(ToolOutput::text(NO_ANSWER)),
            // Deadline hit — either waiting on the user, or on the gate ahead
            // of us. Same degrade either way.
            Err(_) => {
                self.clarify.forget_pending(&sc.session_id);
                Ok(ToolOutput::text(NO_ANSWER))
            }
        }
    }

    fn max_duration(&self) -> Option<std::time::Duration> {
        // Must outlast `CLARIFY_TIMEOUT`, or the executor's default per-call
        // timeout (120s) aborts the question long before the user's 10 minutes
        // are up — the same contract approval-gated tools have via
        // `APPROVAL_BOUND`.
        Some(komo_services::clarify::CLARIFY_BOUND)
    }
}

/// How an `ask_user` wait ended, so the outcome text is chosen after the
/// deadline wrapper rather than inside it.
enum Asked {
    Answered(String),
    Undeliverable(String),
    NoAnswer,
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
    use komo_core::domain::context::{SessionContext, ToolContext};
    use komo_core::domain::gateway::ReplySink;
    use std::sync::Mutex;

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
        async fn decide(&self, _r: &ApprovalRequest) -> Decision {
            Decision::deny()
        }
    }

    /// An interactive `ToolContext` whose sink records what was sent.
    fn interactive_ctx(session: &str, sent: Arc<Mutex<Vec<String>>>) -> ToolContext {
        let session = SessionContext {
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
        ToolContext::new(session, None, Arc::new(DenyAll))
    }

    fn v(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[tokio::test]
    async fn non_interactive_context_degrades_to_guidance() {
        let tool = AskUserTool::new(Arc::new(ClarifyState::new()));
        // A detached context is non-interactive: nobody can answer.
        let ctx = ToolContext::new(SessionContext::detached("s0"), None, Arc::new(DenyAll));
        let out = tool
            .call(v(r#"{"question":"which one?"}"#), &ctx)
            .await
            .unwrap();
        assert!(out.text.contains("No answer from the user"));
    }

    #[tokio::test]
    async fn answer_flows_back_and_prompt_reaches_sink() {
        let clarify = Arc::new(ClarifyState::new());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let ctx = interactive_ctx("s1", sent.clone());

        let tool = AskUserTool::new(clarify.clone());
        let fut = tool.call(v(r#"{"question":"红的还是蓝的?"}"#), &ctx);
        let answerer = {
            let clarify = clarify.clone();
            tokio::spawn(async move {
                // Wait until the question is registered, then answer.
                for _ in 0..100 {
                    if clarify.has_pending("s1") {
                        assert!(clarify.resolve("s1", "蓝的"));
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                panic!("question never registered");
            })
        };
        let out = fut.await.unwrap();
        answerer.await.unwrap();
        assert_eq!(out.text, "User answered: 蓝的");
        assert!(sent.lock().unwrap()[0].contains("红的还是蓝的"));
    }

    #[tokio::test]
    async fn numbered_option_pick_maps_to_option_text() {
        let clarify = Arc::new(ClarifyState::new());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let ctx = interactive_ctx("s2", sent.clone());
        let tool = AskUserTool::new(clarify.clone());
        let fut = tool.call(
            v(r#"{"question":"which?","options":["apple","banana"]}"#),
            &ctx,
        );
        let clarify2 = clarify.clone();
        let answerer = tokio::spawn(async move {
            for _ in 0..100 {
                if clarify2.has_pending("s2") {
                    clarify2.resolve("s2", "2");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
        let out = fut.await.unwrap();
        answerer.await.unwrap();
        assert_eq!(out.text, "User answered: banana");
        let prompt = &sent.lock().unwrap()[0];
        assert!(prompt.contains("1. apple") && prompt.contains("2. banana"));
    }

    /// Two `ask_user` calls in one round — the executor runs a round's calls
    /// concurrently, and the per-turn budget allows two. They must queue on the
    /// session gate and both get answered; before the gate the second
    /// `register` dropped the first waiter's sender, so question one could
    /// never be answered and the user only ever saw question two.
    #[tokio::test]
    async fn concurrent_questions_queue_instead_of_clobbering_each_other() {
        let clarify = Arc::new(ClarifyState::new());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let tool = Arc::new(AskUserTool::new(clarify.clone()));

        let spawn_ask = |question: &'static str| {
            let (tool, sent) = (tool.clone(), sent.clone());
            tokio::spawn(async move {
                let ctx = interactive_ctx("s5", sent);
                tool.call(v(&format!(r#"{{"question":"{question}"}}"#)), &ctx)
                    .await
                    .unwrap()
                    .text
            })
        };
        let (first, second) = (spawn_ask("Q1"), spawn_ask("Q2"));

        // Answer whatever is pending, one at a time: the second asker only gets
        // its question registered once the first releases the gate.
        let answerer = {
            let clarify = clarify.clone();
            tokio::spawn(async move {
                let mut served = 0;
                for _ in 0..600 {
                    if let Some(question) = clarify.pending_question("s5") {
                        let answer = if question.contains("Q1") { "A1" } else { "A2" };
                        assert!(clarify.resolve("s5", answer));
                        served += 1;
                        if served == 2 {
                            return;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                panic!("only {served} of 2 questions were ever pending");
            })
        };

        let (first, second) = (first.await.unwrap(), second.await.unwrap());
        answerer.await.unwrap();

        // Each asker got *its own* answer — neither was silently superseded.
        assert_eq!(first, "User answered: A1");
        assert_eq!(second, "User answered: A2");
        let asked = sent.lock().unwrap().clone();
        assert_eq!(asked.len(), 2, "both questions reached the user: {asked:?}");
    }

    #[tokio::test]
    async fn budget_exhaustion_reports_instead_of_asking() {
        let clarify = Arc::new(ClarifyState::new());
        clarify.try_claim_budget("s3");
        clarify.try_claim_budget("s3");
        let sent = Arc::new(Mutex::new(Vec::new()));
        let ctx = interactive_ctx("s3", sent.clone());
        let tool = AskUserTool::new(clarify);
        let out = tool.call(v(r#"{"question":"?"}"#), &ctx).await.unwrap();
        assert!(out.text.contains("budget exhausted"));
        assert!(sent.lock().unwrap().is_empty(), "no prompt sent");
    }
}
