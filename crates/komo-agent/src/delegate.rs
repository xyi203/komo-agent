use komo_core::domain::context::SessionOrigin;

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::runtime::AgentRuntime;
use komo_config::ModelEntry;
use komo_core::domain::{
    background::{TaskReport, TaskSpec},
    context::ToolContext,
    repository::SessionRepository,
    session::Session,
    session_event::{TaskKind, ToolOutcome},
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

#[derive(Deserialize)]
struct DelegateArgs {
    task: String,
    /// Menu id of the model to run the sub-agent on; omitted = the gateway
    /// default. Validated against the configured menu.
    #[serde(default)]
    model: Option<String>,
    /// Let the sub-agent run past this turn, reporting back when it is done.
    #[serde(default)]
    detach: bool,
}

/// Runs a self-contained subtask on a fresh sub-agent and returns its answer.
///
/// The sub-agent is a **real agent turn**, not a bare completion: it has the full
/// tool set, so it can search, read, and modify — which is the point of handing
/// off "执行修改" rather than only "做计划". `model` picks which model does the
/// work, so a plan and its execution can run on different ones.
///
/// Three things make that safe rather than a runaway:
///
///  - **The parent's session context is inherited, not replaced.**
///    `AgentRuntime::handle_input` leaves an existing ambient context alone, and
///    `run_agent_loop` reads it, so the sub-agent's tools resolve approval
///    prompts against the *parent conversation* (a human still approves every
///    side effect), stay confined to the parent's workspace root, and stop when
///    the parent turn is cancelled.
///  - **No recursion.** The sub-agent's tool set is built without this tool, so
///    a sub-agent cannot spawn another. That is a structural guard, not a depth
///    counter that could be miscounted.
///  - **Auditable.** Each delegation is its own session (`delegate:<uuid>`) and
///    its own ledger run, so `komo run list` / `run inspect` show exactly which
///    tools the sub-agent called.
///
/// One consequence worth knowing: session-scoped tools (`todo`) see the *parent's*
/// session id, because that is what the inherited context carries — a sub-agent
/// shares the conversation's working list rather than keeping a private one.
pub struct DelegateTool {
    /// Sub-agent runtime: full tools minus this one, main approver, shared ledger.
    runtime: Arc<AgentRuntime>,
    /// Needed to create the sub-session carrying the chosen model before the turn
    /// runs — the model travels on the session row, same as for a chat session.
    sessions: Arc<dyn SessionRepository>,
    /// The models a sub-agent may be pointed at (the configured menu).
    models: Vec<ModelEntry>,
    /// The gateway default, named in the tool description so the model knows what
    /// it gets by omitting `model`.
    default_model: String,
}

impl DelegateTool {
    pub fn new(
        runtime: Arc<AgentRuntime>,
        sessions: Arc<dyn SessionRepository>,
        models: Vec<ModelEntry>,
        default_model: String,
    ) -> Self {
        Self {
            runtime,
            sessions,
            models,
            default_model,
        }
    }

    fn model_ids(&self) -> Vec<&str> {
        model_ids(&self.models)
    }
}

fn model_ids(models: &[ModelEntry]) -> Vec<&str> {
    models.iter().map(|entry| entry.id.as_str()).collect()
}

/// Resolve a requested model to the value stored on the sub-session:
/// `None`/blank = the gateway default (empty string).
///
/// An id outside the menu is an **error**, not a silent fallback — the model
/// asked for a specific one, so tell it what exists rather than quietly running
/// something else and reporting success. A free function so it is testable
/// without standing up a whole sub-agent runtime.
fn resolve_model(models: &[ModelEntry], requested: Option<&str>) -> Result<String, ToolError> {
    let Some(want) = requested.map(str::trim).filter(|m| !m.is_empty()) else {
        return Ok(String::new());
    };
    if models.iter().any(|entry| entry.id == want) {
        return Ok(want.to_string());
    }
    Err(ToolError::InvalidInput(format!(
        "unknown model `{want}`; available: {}",
        model_ids(models).join(", ")
    )))
}

#[async_trait]
impl Tool for DelegateTool {
    fn name(&self) -> &'static str {
        "delegate"
    }

    fn description(&self) -> &'static str {
        "Delegate a focused, self-contained subtask to a sub-agent that has the \
         full tool set (it can search, read, edit and run commands) and return \
         its result. Provide all needed context in `task`; the sub-agent does not \
         see the main conversation. Optionally pick which model does the work \
         with `model` — e.g. a stronger model to plan, a faster one to apply \
         changes. The sub-agent cannot delegate further."
    }

    /// A sub-agent now runs a whole *tool-using* turn, not one completion, so it
    /// legitimately takes far longer than a single round-trip. Each of its own
    /// tool calls is still individually bounded by the executor, and its model
    /// round-trips by `llm_timeout_secs`; this only has to be above the total.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(30 * 60))
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "Fully self-contained instruction for the sub-agent."
                },
                "model": {
                    "type": "string",
                    "description": format!(
                        "Model to run the sub-agent on. Omit to use the default \
                         ({}). Available: {}.",
                        self.default_model,
                        self.model_ids().join(", ")
                    ),
                    "enum": self.model_ids(),
                },
                "detach": {
                    "type": "boolean",
                    "description": "Let the sub-agent run past this turn: the call returns a task id at once and its answer comes back to this conversation when it finishes. For work measured in minutes — never for something you need in order to answer now. Nobody is watching a detached sub-agent, so any action of its that needs the user's approval is refused rather than waiting for one: delegate work that will need permission without `detach`."
                }
            },
            "required": ["task"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: DelegateArgs = parse_args(&input)?;
        let model = resolve_model(&self.models, args.model.as_deref())?;

        // Create the sub-session up front carrying the model choice: the turn
        // reads it back off the session row (`infra::llm`), which is the same
        // mechanism a chat session uses — no separate plumbing for sub-agents.
        // `origin` is what marks it a sub-agent's scratch session — the session
        // list filters on that, not on the shape of the id.
        let session_id = uuid::Uuid::now_v7().to_string();
        let mut session = Session::new(&session_id).with_origin(SessionOrigin::Delegate);
        session.model = model;
        self.sessions.save(&session).await?;

        if args.detach {
            return self.detached(&session_id, args.task, ctx).await;
        }

        // Deliberately *not* wrapped in a fresh session context: inheriting the
        // parent's is what keeps approvals answerable and the workspace correct.
        let reply = self.runtime.handle_input(&session_id, args.task).await?;
        Ok(ToolOutput::text(reply))
    }
}

impl DelegateTool {
    /// Hand the sub-agent turn to the background runtime and answer with its id.
    ///
    /// A detached delegation is the same delegation — same sub-session, same
    /// recursion guard, same tools, each gated as usual. What changes is who is
    /// there for it: the work runs in a task of the *process's*, outside any
    /// conversation, so **a tool of its that needs approval is refused**, in as
    /// many words, rather than parked on a wait.
    ///
    /// Prompting the parent conversation instead — which is what a detached
    /// sub-agent inheriting its context would do — needs three things that do
    /// not exist. The prompt would have to name a `wk-` id that only comes into
    /// being *after* the approver has answered (the reason a routine's prompt is
    /// sent by its sweep and not by its approver). The task would have to settle
    /// twice, once to say it stopped and once to carry the answer the
    /// continuation eventually produces, where `task/spawned` ↔ `task/settled`
    /// allows one. And the approval slot is per session, so a background prompt
    /// would displace the one the operator is answering in that conversation.
    /// Until those exist, detachment is declared as what it is — in the tool's
    /// own description, so the model chooses accordingly: work that needs
    /// permission is work to delegate *without* `detach`.
    async fn detached(
        &self,
        session_id: &str,
        task: String,
        ctx: &ToolContext,
    ) -> Result<ToolOutput, ToolError> {
        let (Some(tasks), Some(turn_id)) = (ctx.background(), ctx.turn_id()) else {
            return Ok(ToolOutput::text(
                "This runtime cannot detach a sub-agent — nothing here outlives the \
                 turn. Delegate without `detach` and wait for the answer.",
            ));
        };
        let label = first_line(&task);
        let spec = TaskSpec {
            kind: TaskKind::Delegate,
            label: label.clone(),
        };
        let runtime = self.runtime.clone();
        let sub_session = session_id.to_string();
        let work = Box::pin(async move {
            match runtime.handle_input(&sub_session, task).await {
                Ok(reply) => TaskReport {
                    outcome: ToolOutcome::Succeeded,
                    summary: reply.clone(),
                    full: reply,
                },
                Err(error) => TaskReport {
                    outcome: ToolOutcome::Failed,
                    summary: format!("The sub-agent failed: {error}"),
                    full: error.to_string(),
                },
            }
        });
        match tasks
            .spawn(&ctx.session.session_id, turn_id, spec, work)
            .await
        {
            Ok(task_id) => Ok(ToolOutput::text(format!(
                "Delegated `{label}` to a detached sub-agent as task {task_id}. This turn \
                 does not wait for it: finish what you are doing and answer. You will be \
                 told its result in this conversation when it finishes — or stop and wait \
                 for it now with `wait` and `for_task: {task_id}`."
            ))
            .with_structured(json!({ "task_id": task_id, "session": session_id }))),
            Err(error) => Ok(ToolOutput::text(error.to_string())),
        }
    }
}

/// A one-line name for a task written as a paragraph — what the operator sees
/// in the log and what the eventual wake calls it.
fn first_line(task: &str) -> String {
    const CAP: usize = 80;
    let line = task.lines().find(|l| !l.trim().is_empty()).unwrap_or(task);
    match line.char_indices().nth(CAP) {
        Some((at, _)) => format!("{}…", &line[..at]),
        None => line.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_config::Provider;

    fn menu() -> Vec<ModelEntry> {
        vec![
            ModelEntry {
                id: "gpt-5.6-sol".into(),
                provider: Provider::Codex,
                model: "gpt-5.6-sol".into(),
                efforts: &["low", "medium", "high"],
            },
            ModelEntry {
                id: "deepseek:deepseek-chat".into(),
                provider: Provider::DeepSeek,
                model: "deepseek-chat".into(),
                efforts: &[],
            },
        ]
    }

    #[test]
    fn an_omitted_model_means_the_gateway_default() {
        assert_eq!(resolve_model(&menu(), None).unwrap(), "");
        assert_eq!(resolve_model(&menu(), Some("   ")).unwrap(), "");
    }

    #[test]
    fn a_menu_model_resolves_to_its_id_verbatim() {
        // The qualified form must survive: it is what routes the sub-session's
        // turn to the other provider (`infra::llm::RoutingLlm`).
        assert_eq!(
            resolve_model(&menu(), Some("deepseek:deepseek-chat")).unwrap(),
            "deepseek:deepseek-chat"
        );
        assert_eq!(
            resolve_model(&menu(), Some(" gpt-5.6-sol ")).unwrap(),
            "gpt-5.6-sol",
            "surrounding whitespace is tolerated"
        );
    }

    #[test]
    fn an_unknown_model_errors_and_lists_the_choices() {
        let msg = resolve_model(&menu(), Some("gpt-4.1"))
            .unwrap_err()
            .to_string();
        assert!(msg.contains("gpt-4.1"), "names the bad id: {msg}");
        assert!(msg.contains("gpt-5.6-sol"), "lists what exists: {msg}");
        assert!(
            msg.contains("deepseek:deepseek-chat"),
            "including cross-provider ids: {msg}"
        );
    }
}
