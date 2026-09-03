use std::path::PathBuf;

/// Risk level of an action, used to decide how prominently to warn the user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Risk {
    /// Read-only or otherwise harmless action. An interactive approver may
    /// allow these without prompting; non-interactive approvers still deny.
    Safe,
    Normal,
    Dangerous,
}

/// A structured, machine-matchable description of the action being approved.
///
/// Distinct from [`ApprovalRequest::summary`] (which is for humans) and
/// [`ApprovalRequest::scope_key`] (a coarse "remember this kind" cache key):
/// `ActionRef` carries the *resource* — the command, path, URL, or service — so
/// the configurable permission policy (`domain::policy`) can match on directory
/// prefixes, command prefixes, and domains rather than parsing the summary
/// string. Optional: a request without one degrades to risk/scope-only matching.
#[derive(Debug, Clone)]
pub enum ActionRef {
    /// A shell command (`shell` tool). Matched against the full command line.
    Shell { command: String },
    /// A filesystem access (`read` / `write`). Matched against the path.
    File { path: PathBuf, write: bool },
    /// An outbound network fetch (`web_fetch`). Matched against the URL's host
    /// at dot boundaries, so a `network` deny rule can blackhole a domain
    /// without catching look-alikes.
    Network { url: String },
    /// A Home Assistant service call, matched as `domain.service`.
    Service { domain: String, service: String },
    /// A tool call on an external MCP server, matched as `server.tool`.
    ///
    /// `server` is the operator's `[mcp.servers.<name>]` key, not anything the
    /// server told us — a remote must never be able to rename itself into a
    /// policy rule written for a different one.
    Mcp { server: String, tool: String },
    /// A note-vault index maintenance action (`wiki_index`), matched against the
    /// action name (`status` / `refresh` / `rebuild`).
    ///
    /// The action, not the vault path: the vault is a single configured
    /// location, so a rule scoped to it would say nothing, while the three
    /// actions differ enormously in cost and consequence — `rebuild` drops the
    /// index before refilling it.
    Wiki { action: String },
    /// A tool a local plugin registered, matched against the plugin's own tool
    /// name (un-namespaced, so a rule reads `value = "strlen"` rather than
    /// carrying komo's `py__` catalog prefix).
    Plugin { tool: String },
}

/// A request for the user to approve a side-effecting action.
pub struct ApprovalRequest {
    /// Human-readable description of the action, e.g. `run shell command: ls`.
    pub summary: String,
    pub risk: Risk,
    /// Optional extra context, e.g. why a command was flagged dangerous.
    pub detail: Option<String>,
    /// Stable key identifying the *kind* of action (e.g. the matched dangerous
    /// pattern, or `file:write`). An approver can cache an "allow for this
    /// session" answer under this key so repeats don't prompt again.
    pub scope_key: Option<String>,
    /// Structured resource the permission policy matches on (see [`ActionRef`]).
    pub action: Option<ActionRef>,
}

impl ApprovalRequest {
    pub fn safe(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            risk: Risk::Safe,
            detail: None,
            scope_key: None,
            action: None,
        }
    }

    pub fn normal(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            risk: Risk::Normal,
            detail: None,
            scope_key: None,
            action: None,
        }
    }

    pub fn dangerous(summary: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            risk: Risk::Dangerous,
            detail: Some(detail.into()),
            scope_key: None,
            action: None,
        }
    }

    /// Attach a session-scope key (see [`ApprovalRequest::scope_key`]).
    pub fn with_scope_key(mut self, key: impl Into<String>) -> Self {
        self.scope_key = Some(key.into());
        self
    }

    /// Attach the structured resource the policy matches on (see [`ActionRef`]).
    pub fn with_action(mut self, action: ActionRef) -> Self {
        self.action = Some(action);
        self
    }

    /// Attach the expanded explanation shown under the summary. `dangerous`
    /// requires one up front; this is for a `normal` request that still has
    /// something the operator must read before answering.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

/// The answer to an [`ApprovalRequest`]: proceed, or don't — and when the user
/// declined, optionally *why*.
///
/// The feedback arm is the point of this type existing rather than a bare
/// `bool`. A denial with no explanation tells the model only that it failed;
/// one carrying "别用 `rm`，用 `trash`" tells it what to do instead, so the next
/// round is a corrected attempt rather than a retry of the same call. Borrowed
/// from opencode v2's `PermissionV2.CorrectedError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// Allow, **and remember** this kind of action for future sessions — the
    /// interactive `a` answer. Only the interactive approvers produce it, and
    /// only `PolicyApprover` acts on it (it synthesizes the narrowest matching
    /// rule and persists it), so every other layer can treat it as `Allow`.
    AllowAlways,
    Deny {
        feedback: Option<String>,
    },
    /// **Nobody has answered yet, and the answer is coming later.** The prompt
    /// has been delivered somewhere a person will see it — a chat, the home
    /// channel — and the turn stops here rather than holding a session slot for
    /// five minutes or guessing.
    ///
    /// Not a denial: the action is neither refused nor taken. The gate turns
    /// this into a `turn/suspended` and a standing wakeup, and the same call is
    /// re-dispatched when the answer arrives (docs/bot-runtime.md §4.1).
    Suspend,
}

impl Decision {
    /// Deny with no explanation (a bare `n`, a timeout, an auto-deny).
    pub fn deny() -> Self {
        Decision::Deny { feedback: None }
    }

    /// Deny, handing the model a reason it can act on.
    pub fn deny_because(reason: impl Into<String>) -> Self {
        Decision::Deny {
            feedback: Some(reason.into()),
        }
    }

    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow | Decision::AllowAlways)
    }

    /// Whether the answer is still coming. Distinct from `!is_allowed()`: a
    /// denial is an answer, and this is the absence of one.
    pub fn is_suspended(&self) -> bool {
        matches!(self, Decision::Suspend)
    }

    /// The denial's explanation, if the user (or a policy rule) gave one.
    pub fn feedback(&self) -> Option<&str> {
        match self {
            Decision::Deny {
                feedback: Some(text),
            } => Some(text),
            _ => None,
        }
    }
}

impl From<bool> for Decision {
    fn from(allowed: bool) -> Self {
        if allowed {
            Decision::Allow
        } else {
            Decision::deny()
        }
    }
}

/// Rungs of the permission ladder, as `approval/resolved` records them. The
/// vocabulary is closed and the strings are stable: they are read back out of
/// logs written by older builds.
pub const DECIDED_BY_APPROVER: &str = "approver";
pub const DECIDED_BY_CONFIG_DENY: &str = "config-deny";
pub const DECIDED_BY_CONFIG_ALLOW: &str = "config-allow";
pub const DECIDED_BY_DEFAULT: &str = "default";
pub const DECIDED_BY_SAVED_GRANT: &str = "saved-grant";
pub const DECIDED_BY_JOB_GRANT: &str = "job-grant";
pub const DECIDED_BY_AUTO_REVIEW: &str = "auto-review";
pub const DECIDED_BY_HUMAN: &str = "human";

/// Gate for sensitive, side-effecting actions (e.g. running a shell command or
/// writing a file).
///
/// The domain layer only knows this trait; the interface layer provides a
/// concrete implementation that prompts the user. Tools that perform risky
/// actions depend on an `Arc<dyn Approver>` rather than on any I/O directly.
///
/// `decide` is async: an interactive approver reads a TTY, but a chat-channel
/// approver sends an approval prompt to the conversation and awaits the user's
/// reply on a later turn (see `agent::interaction::ChatApprover`).
#[async_trait::async_trait]
pub trait Approver: Send + Sync {
    /// Ask the user to approve `request`. See [`Decision`] — a denial may carry
    /// a reason for the model.
    async fn decide(&self, request: &ApprovalRequest) -> Decision;

    /// [`decide`](Approver::decide), plus **which rung decided** — the audit
    /// half of an approval, recorded on `approval/resolved`.
    ///
    /// "Why did this go through?" is asked long after the fact, and `allowed`
    /// alone cannot answer it: a config rule, a grant the operator saved months
    /// ago, the auto-reviewer and a person at a prompt all produce the same
    /// `true`. Only the layer that decided knows which, so it reports it here.
    ///
    /// Defaults to [`DECIDED_BY_APPROVER`] — an approver that does not say is
    /// one whose answer came from wherever it was wired, which is all the log
    /// can honestly claim about it. Every rung of the real ladder overrides it.
    async fn decide_reported(&self, request: &ApprovalRequest) -> (Decision, &'static str) {
        (self.decide(request).await, DECIDED_BY_APPROVER)
    }

    /// [`decide`](Approver::decide) reduced to a yes/no, for callers that have
    /// nothing to do with a denial's reason.
    ///
    /// A pure projection — **do not override it**. Anything that varies the
    /// answer belongs in `decide`, or the two entry points disagree.
    async fn approve(&self, request: &ApprovalRequest) -> bool {
        self.decide(request).await.is_allowed()
    }
}
