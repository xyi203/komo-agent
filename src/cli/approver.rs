use std::collections::HashSet;
use std::io::{self, Write};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::domain::approval::{ApprovalRequest, Approver, Decision, Risk};

/// What the user answered at the approval prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    /// Allow this one action only.
    Once,
    /// Allow this action and remember its scope key for the rest of the session.
    Session,
    /// Allow, and save a narrow rule so this kind of action stops asking in
    /// future sessions too (`~/.komo/permissions.json`).
    Always,
    /// Refuse. A denial answered as `n: <reason>` carries the reason to the
    /// model, so it can correct the call instead of retrying it verbatim.
    Deny(Option<String>),
}

fn parse_answer(input: &str) -> Answer {
    let trimmed = input.trim();
    // A reason may follow the verb after `:` or whitespace (`n: use trash`).
    let (verb, rest) = match trimmed.split_once([':', ' ']) {
        Some((verb, rest)) => (verb, rest.trim()),
        None => (trimmed, ""),
    };
    match verb.to_lowercase().as_str() {
        "y" | "yes" => Answer::Once,
        "s" | "session" => Answer::Session,
        "a" | "always" => Answer::Always,
        _ => Answer::Deny((!rest.is_empty()).then(|| rest.to_string())),
    }
}

/// Interactive approver, modeled on hermes-agent's approval policy:
/// - `Risk::Safe` actions (read-only commands) are allowed without prompting.
/// - everything else prompts with `[y/s/N]`, where `s` allows the action and
///   caches its scope key so the same kind of action skips the prompt for the
///   rest of the session.
pub struct CliApprover {
    session_allowed: Mutex<HashSet<String>>,
    /// Serializes the interactive prompt. A round's tool calls run concurrently
    /// (`AgentRuntime::run_agent_loop`), so two could prompt at once; without
    /// this their stdin reads would interleave into one garbled prompt.
    prompt_gate: tokio::sync::Mutex<()>,
}

impl CliApprover {
    pub fn new() -> Self {
        Self {
            session_allowed: Mutex::new(HashSet::new()),
            prompt_gate: tokio::sync::Mutex::new(()),
        }
    }
}

impl Default for CliApprover {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Approver for CliApprover {
    async fn decide(&self, request: &ApprovalRequest) -> Decision {
        if request.risk == Risk::Safe {
            return Decision::Allow;
        }

        // Session cache: the user already said "allow for this session" for
        // this kind of action.
        if let Some(key) = &request.scope_key
            && self.session_allowed.lock().unwrap().contains(key)
        {
            println!("✓ auto-approved (session): {}", request.summary);
            return Decision::Allow;
        }

        // Serialize concurrent prompts onto the single TTY (a round's tools run
        // concurrently) so their stdin reads don't interleave. Held across the
        // blocking read below.
        let _guard = self.prompt_gate.lock().await;
        // A concurrent prompt may have just cached "session" for this scope.
        if let Some(key) = &request.scope_key
            && self.session_allowed.lock().unwrap().contains(key)
        {
            return Decision::Allow;
        }

        // The prompt + stdin read is blocking; run it off the async runtime.
        let prompt = prompt_text(request);
        let answer = tokio::task::spawn_blocking(move || {
            print!("{prompt}");
            if io::stdout().flush().is_err() {
                return Answer::Deny(None);
            }
            let mut answer = String::new();
            if io::stdin().read_line(&mut answer).is_err() {
                return Answer::Deny(None);
            }
            parse_answer(&answer)
        })
        .await
        .unwrap_or(Answer::Deny(None));

        // Same rule the chat path applies in `ApprovalState::resolve_scoped`: a
        // dangerous action is approved for this call only. Widening it would
        // pre-approve a *later* irreversible action the operator never saw.
        let answer = match (request.risk, answer) {
            (Risk::Dangerous, Answer::Session | Answer::Always) => {
                println!("（危险操作仅批准本次；下次仍会询问）");
                Answer::Once
            }
            (_, other) => other,
        };

        match answer {
            Answer::Once => Decision::Allow,
            Answer::Session => {
                if let Some(key) = &request.scope_key {
                    self.session_allowed.lock().unwrap().insert(key.clone());
                }
                Decision::Allow
            }
            Answer::Always => {
                // Also cache for the session: the saved rule is narrow, so a
                // near-miss later in this conversation shouldn't re-prompt after
                // the user already said "always".
                if let Some(key) = &request.scope_key {
                    self.session_allowed.lock().unwrap().insert(key.clone());
                }
                Decision::AllowAlways
            }
            Answer::Deny(feedback) => Decision::Deny { feedback },
        }
    }
}

/// The interactive prompt text for a non-`Safe` request.
///
/// `[a]lways` is offered only when there is something to remember — a
/// `Risk::Normal` request carrying an action to generalize from. A dangerous
/// action never offers it (the policy engine refuses to read a saved grant for
/// one, so the key would be a lie), and the rule text is spelled out so the
/// operator sees how wide the grant is *before* answering.
fn prompt_text(request: &ApprovalRequest) -> String {
    let saveable = (request.risk == Risk::Normal)
        .then(|| always_rule(request))
        .flatten();
    let choices = match &saveable {
        Some(rule) => format!(
            "[y]es once / [s]ession / [a]lways (saves: {rule}) / [N]o (`n: reason` tells the agent why) "
        ),
        None => "[y]es once / [s]ession / [N]o (`n: reason` tells the agent why) ".to_string(),
    };
    match request.risk {
        Risk::Safe => unreachable!("handled before prompting"),
        Risk::Dangerous => {
            let mut s = format!("\n🛑 DANGEROUS — request to {}", request.summary);
            if let Some(detail) = &request.detail {
                s.push_str(&format!("\n   ({detail})"));
            }
            s.push_str(&format!("\n   Approve? {choices}"));
            s
        }
        Risk::Normal => format!("\n⚠  Approve request to {}? {choices}", request.summary),
    }
}

/// The rule an `always` answer would save, described for the prompt. `None` when
/// there is nothing to generalize (no action, or no session to scope it to).
fn always_rule(request: &ApprovalRequest) -> Option<String> {
    let session = komo_services::tool_execution::current_session()?;
    let channel = session.channel_name().to_string();
    let action = request.action.as_ref()?;
    Some(crate::domain::policy::Rule::narrowest_for(action, &channel)?.describe())
}

// The gateway uses `agent::interaction::ChatApprover`, which routes the prompt
// to the chat channel and denies when there is no chat session in context
// (maintenance sweeps, aux sub-agents) — so a separate deny-only approver is no
// longer needed.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answers_parse_to_once_session_or_deny() {
        assert_eq!(parse_answer("y\n"), Answer::Once);
        assert_eq!(parse_answer("YES"), Answer::Once);
        assert_eq!(parse_answer("s\n"), Answer::Session);
        assert_eq!(parse_answer("Session"), Answer::Session);
        assert_eq!(parse_answer("a\n"), Answer::Always);
        assert_eq!(parse_answer("ALWAYS"), Answer::Always);
        assert_eq!(parse_answer(""), Answer::Deny(None));
        assert_eq!(parse_answer("n"), Answer::Deny(None));
        assert_eq!(parse_answer("whatever"), Answer::Deny(None));
    }

    #[test]
    fn a_denial_can_carry_a_reason_for_the_model() {
        assert_eq!(
            parse_answer("n: use trash instead of rm\n"),
            Answer::Deny(Some("use trash instead of rm".to_string()))
        );
        // Whitespace separator works too, and the reason keeps its case.
        assert_eq!(
            parse_answer("no Use Trash"),
            Answer::Deny(Some("Use Trash".to_string()))
        );
    }

    #[tokio::test]
    async fn safe_requests_skip_the_prompt() {
        let approver = CliApprover::new();
        assert!(
            approver
                .decide(&ApprovalRequest::safe("run shell command: ls"))
                .await
                .is_allowed()
        );
    }

    #[tokio::test]
    async fn session_cache_short_circuits_the_prompt() {
        let approver = CliApprover::new();
        approver
            .session_allowed
            .lock()
            .unwrap()
            .insert("file:write".to_string());
        let request =
            ApprovalRequest::normal("write 5 bytes to /tmp/x").with_scope_key("file:write");
        assert!(approver.decide(&request).await.is_allowed());
    }
}
