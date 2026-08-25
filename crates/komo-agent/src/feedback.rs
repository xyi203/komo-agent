//! Reading the user's next message as a verdict on the previous turn
//! (docs/episode-learning-framework.md §4.3, Phase 2).
//!
//! Most work is not settled when the turn ends. The agent edits a file and says
//! it is done; whether it *is* done is something only the next message reveals
//! — "可以了" or "还是不行". Without this, the strongest evidence komo can ever
//! get about its own work is the one source that cannot corroborate it: its own
//! claim.
//!
//! Deliberately narrow. This asks one question — *did the user say the last
//! turn's work succeeded or failed?* — and answers `None` for everything else,
//! which is nearly every message. A new request, a follow-up question, a thank
//! you, a change of subject: none of those are verdicts, and reading them as
//! verdicts would attach confident evidence to a turn nobody assessed.
//!
//! Fail-closed at every step, on the same terms as the auto-policy reviewer: a
//! model error, a timeout, an unparseable answer, or an ambiguous one all mean
//! "no verdict". The cost of a missed confirmation is a run that stays
//! `Unknown`, which is what it was already; the cost of a wrong one is komo
//! believing a broken change worked.

use std::sync::Arc;
use std::time::Duration;

use komo_core::domain::{
    episode::{OutcomeEvidence, OutcomeEvidenceKind, OutcomeVerdict},
    llm::LlmClient,
    message::Message,
    run::Run,
    session::Session,
};

/// How long the classifier may take before the turn's feedback is treated as
/// "no verdict". Matches the auto-policy reviewer's budget: this runs off the
/// reply path, but an aux model that has stopped answering must not keep a
/// background task alive indefinitely.
const CLASSIFY_TIMEOUT: Duration = Duration::from_secs(20);

/// Longest excerpt of the previous turn shown to the classifier. It needs to
/// know what was claimed, not to re-read the work.
const EXCERPT: usize = 600;

const PROMPT: &str = "\
You are deciding one narrow question about a conversation, and nothing else.

The assistant did some work and reported back. Then the user replied. Does the \
user's reply state that the work SUCCEEDED, state that it FAILED, or neither?

Answer with exactly one word on the first line:
  SUCCESS  - the user confirms it worked (\"可以了\", \"perfect, thanks\", \"that fixed it\")
  FAILURE  - the user says it did not work (\"还是不行\", \"still broken\", \"that's wrong\")
  NEITHER  - anything else

NEITHER is the common answer and the right one whenever you are unsure. A new \
request, a follow-up question, a clarification, praise for the explanation \
rather than the result, or simply moving on are all NEITHER. Do not infer \
success from the absence of a complaint: a user who says nothing about the work \
and asks for something else has not confirmed anything.

Judge only the user's reply. The assistant's own claim that it finished is what \
is being tested here, so it is never evidence for itself.";

/// The user's reply read as a verdict, or `None` when it is not one.
pub async fn classify(
    llm: &Arc<dyn LlmClient>,
    previous: &Run,
    reply: &str,
    now: i64,
) -> Option<OutcomeEvidence> {
    let reply = reply.trim();
    if reply.is_empty() || previous.final_output.is_empty() {
        return None;
    }
    let prompt = format!(
        "{PROMPT}\n\n\
         The user originally asked:\n{}\n\n\
         The assistant reported back:\n{}\n\n\
         The user's reply:\n{reply}",
        excerpt(&previous.input),
        excerpt(&previous.final_output),
    );
    // Empty model/effort, like every other aux path: the conversation's model
    // choice must not leak onto the aux model.
    let session = Session {
        id: format!("feedback-{}", previous.id),
        workspace: String::new(),
        messages: vec![Message::user(prompt)],
        created_at: now,
        title: String::new(),
        status: String::new(),
        model: String::new(),
        effort: String::new(),
    };

    let answer = match tokio::time::timeout(CLASSIFY_TIMEOUT, llm.complete(&session)).await {
        Ok(Ok(answer)) => answer,
        Ok(Err(error)) => {
            tracing::warn!(%error, "feedback classification failed — no verdict");
            return None;
        }
        Err(_) => {
            tracing::warn!("feedback classification timed out — no verdict");
            return None;
        }
    };

    let (kind, verdict) = match parse(&answer)? {
        OutcomeVerdict::Success => (OutcomeEvidenceKind::UserConfirmed, OutcomeVerdict::Success),
        OutcomeVerdict::Failure => (OutcomeEvidenceKind::UserRejected, OutcomeVerdict::Failure),
        OutcomeVerdict::Unknown => return None,
    };
    Some(OutcomeEvidence::new(
        kind,
        verdict,
        format!("the user replied: {}", excerpt(reply)),
        "next turn",
        now,
    ))
}

/// Parse the verdict word, on the same strict terms as the auto-policy
/// reviewer's: it must lead the first line **and** be the only verdict named on
/// it. A line that reasons its way to an answer ("SUCCESS would be wrong here;
/// NEITHER") has not given one, and taking its first word would inverte it.
fn parse(answer: &str) -> Option<OutcomeVerdict> {
    let line = answer.trim().lines().next()?.trim().to_uppercase();
    let named: Vec<&str> = ["SUCCESS", "FAILURE", "NEITHER"]
        .into_iter()
        .filter(|word| line.contains(word))
        .collect();
    if named.len() != 1 {
        return None;
    }
    let word = named[0];
    if !line.starts_with(word) {
        return None;
    }
    match word {
        "SUCCESS" => Some(OutcomeVerdict::Success),
        "FAILURE" => Some(OutcomeVerdict::Failure),
        _ => Some(OutcomeVerdict::Unknown),
    }
}

fn excerpt(text: &str) -> String {
    komo_core::domain::run::truncate(&text.replace('\n', " "), EXCERPT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct FixedLlm {
        reply: String,
        prompts: Mutex<Vec<String>>,
        fail: bool,
        stall: bool,
    }

    impl FixedLlm {
        fn new(reply: &str) -> Arc<Self> {
            Arc::new(Self {
                reply: reply.into(),
                prompts: Mutex::new(Vec::new()),
                fail: false,
                stall: false,
            })
        }
    }

    #[async_trait]
    impl LlmClient for FixedLlm {
        async fn complete(&self, session: &Session) -> anyhow::Result<String> {
            self.prompts
                .lock()
                .unwrap()
                .push(session.messages[0].content.clone());
            if self.stall {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            if self.fail {
                anyhow::bail!("aux model unavailable");
            }
            Ok(self.reply.clone())
        }
    }

    fn previous() -> Run {
        let mut run = Run::start("cli:s", "fix the failing test");
        run.final_output = "Fixed — the assertion was inverted.".into();
        run
    }

    async fn verdict_of(reply: &str, model_says: &str) -> Option<OutcomeEvidence> {
        let llm: Arc<dyn LlmClient> = FixedLlm::new(model_says);
        classify(&llm, &previous(), reply, 100).await
    }

    #[tokio::test]
    async fn a_confirmation_is_the_strongest_evidence_there_is() {
        let evidence = verdict_of("可以了，谢谢", "SUCCESS").await.unwrap();
        assert_eq!(evidence.kind, OutcomeEvidenceKind::UserConfirmed);
        assert_eq!(evidence.verdict, OutcomeVerdict::Success);
        assert!(evidence.detail.contains("可以了"));
    }

    #[tokio::test]
    async fn a_rejection_overturns_the_agents_own_report() {
        let evidence = verdict_of("还是不行", "FAILURE").await.unwrap();
        assert_eq!(evidence.kind, OutcomeEvidenceKind::UserRejected);
        assert_eq!(evidence.verdict, OutcomeVerdict::Failure);
    }

    #[tokio::test]
    async fn an_ordinary_next_request_is_not_a_verdict() {
        assert!(
            verdict_of("现在帮我改一下 README", "NEITHER")
                .await
                .is_none()
        );
    }

    /// The failure mode that matters: everything unclear must land on "no
    /// verdict", never on a confident one.
    #[tokio::test]
    async fn every_unusable_answer_means_no_verdict() {
        for answer in [
            "",
            "  ",
            "I think it probably worked?",
            // Reasons its way to the opposite of its first word.
            "SUCCESS would be wrong here; NEITHER",
            // Names two verdicts.
            "SUCCESS or FAILURE, hard to say",
            // Right word, wrong position.
            "The answer is SUCCESS",
            "{\"verdict\": \"SUCCESS\"}",
        ] {
            assert!(
                verdict_of("可以了", answer).await.is_none(),
                "answer {answer:?} must not produce a verdict"
            );
        }
    }

    #[tokio::test]
    async fn a_model_error_means_no_verdict() {
        let llm: Arc<dyn LlmClient> = Arc::new(FixedLlm {
            reply: String::new(),
            prompts: Mutex::new(Vec::new()),
            fail: true,
            stall: false,
        });
        assert!(classify(&llm, &previous(), "可以了", 100).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_model_means_no_verdict() {
        let llm: Arc<dyn LlmClient> = Arc::new(FixedLlm {
            reply: "SUCCESS".into(),
            prompts: Mutex::new(Vec::new()),
            fail: false,
            stall: true,
        });
        assert!(classify(&llm, &previous(), "可以了", 100).await.is_none());
    }

    /// Nothing was claimed, so there is nothing for a reply to confirm — and no
    /// reason to spend an aux call finding that out.
    #[tokio::test]
    async fn a_turn_that_reported_nothing_is_never_assessed_from_feedback() {
        let llm: Arc<dyn LlmClient> = FixedLlm::new("SUCCESS");
        let mut run = previous();
        run.final_output = String::new();
        assert!(classify(&llm, &run, "可以了", 100).await.is_none());

        assert!(classify(&llm, &previous(), "   ", 100).await.is_none());
    }

    #[tokio::test]
    async fn the_classifier_sees_the_claim_it_is_testing_and_the_reply() {
        let llm = FixedLlm::new("NEITHER");
        let dynamic: Arc<dyn LlmClient> = llm.clone();
        classify(&dynamic, &previous(), "下一步呢", 100).await;

        let prompt = &llm.prompts.lock().unwrap()[0];
        assert!(prompt.contains("fix the failing test"));
        assert!(prompt.contains("the assertion was inverted"));
        assert!(prompt.contains("下一步呢"));
        assert!(
            prompt.contains("never evidence for itself"),
            "the prompt has to say whose claim is under test"
        );
    }
}
