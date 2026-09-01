//! Approver for the TUI: routes an approval request to the event loop as a
//! modal and awaits the user's keypress.
//!
//! Mirrors `CliApprover`'s policy (`Risk::Safe` runs without asking; `y`
//! allows once, `s` allows and remembers the scope key for the session,
//! anything else denies) — but where `CliApprover` blocks reading stdin, this
//! sends an [`ApprovalPrompt`] over a channel and suspends on a `oneshot`, so
//! the terminal stays owned by the TUI. Concurrent requests (a round's tool
//! calls run in parallel) simply queue in the channel; the event loop shows
//! one modal at a time.

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::domain::approval::{ApprovalRequest, Approver, Decision, Risk};

/// The user's answer to an approval modal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// Allow this one action.
    Once,
    /// Allow and remember the scope key for the rest of the session.
    Session,
    /// Allow, and save a narrow rule so this stops asking in future sessions.
    Always,
    /// Refuse, optionally with a reason the agent is told (typed after `n`).
    Deny(Option<String>),
}

/// One approval rendered as a modal. `reply` is taken (`Option`) when the
/// user answers; dropping it unanswered reads as a denial on the waiting side.
pub struct ApprovalPrompt {
    pub summary: String,
    pub detail: Option<String>,
    pub dangerous: bool,
    /// The rule an "always" answer would save, already described. `None` when
    /// there is nothing to generalize (no action on the request) or when the
    /// action is dangerous — the policy engine refuses to read a saved grant for
    /// one, so the modal must not offer the key.
    pub always_rule: Option<String>,
    pub reply: Option<oneshot::Sender<Answer>>,
}

pub struct TuiApprover {
    session_allowed: Mutex<HashSet<String>>,
    prompts: mpsc::UnboundedSender<ApprovalPrompt>,
}

impl TuiApprover {
    pub fn new(prompts: mpsc::UnboundedSender<ApprovalPrompt>) -> Self {
        Self {
            session_allowed: Mutex::new(HashSet::new()),
            prompts,
        }
    }
}

#[async_trait]
impl Approver for TuiApprover {
    async fn decide(&self, request: &ApprovalRequest) -> Decision {
        if request.risk == Risk::Safe {
            return Decision::Allow;
        }
        if let Some(key) = &request.scope_key
            && self.session_allowed.lock().unwrap().contains(key)
        {
            return Decision::Allow;
        }

        let (tx, rx) = oneshot::channel();
        let prompt = ApprovalPrompt {
            summary: request.summary.clone(),
            detail: request.detail.clone(),
            dangerous: request.risk == Risk::Dangerous,
            always_rule: (request.risk == Risk::Normal)
                .then(|| always_rule(request))
                .flatten(),
            reply: Some(tx),
        };
        // The TUI gone (channel closed) means no one can answer: deny.
        if self.prompts.send(prompt).is_err() {
            return Decision::deny();
        }
        match rx.await {
            Ok(Answer::Once) => Decision::Allow,
            Ok(Answer::Session) => {
                if let Some(key) = &request.scope_key {
                    self.session_allowed.lock().unwrap().insert(key.clone());
                }
                Decision::Allow
            }
            Ok(Answer::Always) => {
                if let Some(key) = &request.scope_key {
                    self.session_allowed.lock().unwrap().insert(key.clone());
                }
                Decision::AllowAlways
            }
            Ok(Answer::Deny(feedback)) => Decision::Deny { feedback },
            // The modal was dropped unanswered (quit).
            Err(_) => Decision::deny(),
        }
    }
}

/// The rule an `always` answer would save, described for the modal.
fn always_rule(request: &ApprovalRequest) -> Option<String> {
    let session = komo_services::tool_execution::current_session()?;
    let channel = session.channel_name().to_string();
    let action = request.action.as_ref()?;
    Some(crate::domain::policy::Rule::narrowest_for(action, &channel)?.describe())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::approval::ApprovalRequest;

    fn normal(summary: &str, scope_key: Option<&str>) -> ApprovalRequest {
        let mut r = ApprovalRequest::normal(summary);
        r.scope_key = scope_key.map(str::to_string);
        r
    }

    #[tokio::test]
    async fn safe_requests_never_prompt() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let approver = TuiApprover::new(tx);
        assert!(
            approver
                .decide(&ApprovalRequest::safe("read"))
                .await
                .is_allowed()
        );
        assert!(rx.try_recv().is_err(), "no modal for a safe action");
    }

    #[tokio::test]
    async fn session_answer_caches_the_scope_key() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let approver = std::sync::Arc::new(TuiApprover::new(tx));

        // First request prompts; answer "session".
        let a = approver.clone();
        let fut = tokio::spawn(async move { a.decide(&normal("run", Some("shell:ls"))).await });
        let mut prompt = rx.recv().await.expect("modal shown");
        prompt.reply.take().unwrap().send(Answer::Session).unwrap();
        assert!(fut.await.unwrap().is_allowed());

        // Same scope key again: allowed with no modal.
        assert!(
            approver
                .decide(&normal("run", Some("shell:ls")))
                .await
                .is_allowed()
        );
        assert!(rx.try_recv().is_err(), "cached scope must not re-prompt");
    }

    #[tokio::test]
    async fn dropped_modal_reads_as_denial() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let approver = std::sync::Arc::new(TuiApprover::new(tx));
        let a = approver.clone();
        let fut = tokio::spawn(async move { a.decide(&normal("rm -rf", None)).await });
        let prompt = rx.recv().await.expect("modal shown");
        drop(prompt); // quit without answering
        assert_eq!(fut.await.unwrap(), Decision::deny());
    }

    #[tokio::test]
    async fn a_denial_reason_reaches_the_caller() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let approver = std::sync::Arc::new(TuiApprover::new(tx));
        let a = approver.clone();
        let fut = tokio::spawn(async move { a.decide(&normal("rm x", None)).await });
        let mut prompt = rx.recv().await.expect("modal shown");
        prompt
            .reply
            .take()
            .unwrap()
            .send(Answer::Deny(Some("用 trash".into())))
            .unwrap();
        assert_eq!(fut.await.unwrap().feedback(), Some("用 trash"));
    }
}
