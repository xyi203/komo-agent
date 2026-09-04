//! Shared plumbing for the filesystem tools (`read`, `write`, and — next —
//! `edit` / `apply_patch`).
//!
//! Three things every one of them needs, in the same order every time:
//! resolve the model's path against the workspace, ask the approver with the
//! right [`ActionRef`] (so `[policy]` rules keep matching on category/access),
//! and turn a refusal into text the model can act on.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use komo_core::domain::{
    approval::{ActionRef, ApprovalRequest, Decision},
    context::ToolContext,
    tool::ToolError,
    workspace::Workspace,
};

/// Resolve a model-supplied path inside `workspace`. Relative paths anchor to
/// the workspace root; anything that lands outside it is refused as
/// [`ToolError::Denied`] — the workspace whitelist is a floor, not a prompt (no
/// approval unlocks it, matching `shell`'s hardline patterns).
///
/// This is the **mutating** resolver. A read goes through
/// [`resolve_readable`], which also admits komo's read-only managed roots.
pub fn resolve(
    workspace: &Arc<Workspace>,
    ctx: &ToolContext,
    path: &str,
) -> Result<PathBuf, ToolError> {
    let effective = effective(workspace, ctx);
    effective.resolve_contained(Path::new(path)).ok_or_else(|| {
        ToolError::Denied(format!(
            "path `{path}` is outside the workspace and was blocked. \
                 Only paths under {} are available.",
            roots_note(effective.roots())
        ))
    })
}

/// [`resolve`] for a read: the workspace plus its read-only roots, or any local
/// path when unrestricted reads are enabled. A preview hands the model a managed
/// tool-output path, so `read`, `grep`, and `glob` have to be able to open it;
/// nothing else can, because every mutating tool resolves through [`resolve`].
pub fn resolve_readable(
    workspace: &Arc<Workspace>,
    ctx: &ToolContext,
    path: &str,
) -> Result<PathBuf, ToolError> {
    let effective = effective(workspace, ctx);
    effective.resolve_readable(Path::new(path)).ok_or_else(|| {
        ToolError::Denied(format!(
            "path `{path}` is outside the workspace and was blocked. \
                 Only paths under {} are available.",
            roots_note(
                &effective
                    .roots()
                    .iter()
                    .chain(effective.readonly_roots())
                    .cloned()
                    .collect::<Vec<_>>()
            )
        ))
    })
}

/// The workspace this turn actually resolves against: the session's own root when
/// it picked one, else the wired default. A session-selected root still carries
/// komo's own managed roots — the read-only tool-output store and the writable
/// artifacts directory are komo's, not the workspace's, so moving where a turn
/// works must not take them away.
///
/// `shell` resolves its `workdir` through this too, which is what lets a turn run
/// a command inside its artifacts directory.
pub(crate) fn effective<'a>(
    workspace: &'a Arc<Workspace>,
    ctx: &ToolContext,
) -> std::borrow::Cow<'a, Workspace> {
    match ctx.session.workspace_root.as_ref() {
        Some(root) => {
            let derived = Workspace::new(vec![root.clone()])
                .with_readonly(workspace.readonly_roots().to_vec());
            let derived = match workspace.artifacts_root() {
                Some(artifacts) => derived.with_artifacts(artifacts.to_path_buf()),
                None => derived,
            };
            let derived = if workspace.has_unrestricted_reads() {
                derived.with_unrestricted_reads()
            } else {
                derived
            };
            std::borrow::Cow::Owned(derived)
        }
        None => std::borrow::Cow::Borrowed(workspace.as_ref()),
    }
}

fn roots_note(roots: &[PathBuf]) -> String {
    roots
        .iter()
        .map(|r| r.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Consult the approver for a **read**. Reads are `Risk::Safe`, so an
/// interactive approver never prompts — but a `category = "file", access =
/// "read"` deny rule still blackholes the path (the exfiltration guard). Returns
/// the refusal text when blocked.
pub async fn allow_read(ctx: &ToolContext, path: &Path) -> Option<String> {
    let request =
        ApprovalRequest::safe(format!("read {}", path.display())).with_action(ActionRef::File {
            path: path.to_path_buf(),
            write: false,
        });
    let decision = ctx.decide(&request).await;
    if decision.is_allowed() {
        return None;
    }
    Some(match decision.feedback() {
        Some(reason) => format!(
            "Read of {} blocked: {reason}. Nothing was read.",
            path.display()
        ),
        None => format!(
            "Read of {} blocked by the permission policy; nothing was read.",
            path.display()
        ),
    })
}

/// Consult the approver for a **write** (`Risk::Normal` — it prompts).
/// `summary` describes the mutation for the human. Returns the refusal text,
/// carrying the user's reason when they gave one, when denied.
pub async fn allow_write(ctx: &ToolContext, path: &Path, summary: String) -> Option<String> {
    let request = ApprovalRequest::normal(summary)
        .with_scope_key("file:write")
        .with_action(ActionRef::File {
            path: path.to_path_buf(),
            write: true,
        });
    let decision = ctx.decide(&request).await;
    if decision.is_allowed() {
        return None;
    }
    Some(match decision.feedback() {
        Some(reason) => format!(
            "Rejected by the user; {} was not changed. They said: {reason}",
            path.display()
        ),
        None => format!("Rejected by user; {} was not changed.", path.display()),
    })
}

/// Approve a mutation that spans **several** files with a single prompt.
///
/// `summary` should name every target, because that is the one thing the human
/// sees. The subtlety is the second pass: after the human grants the batch, each
/// remaining path is still evaluated at `Risk::Safe` — never prompting, but
/// keeping `ActionRef::File{write:true}`, so a `[policy]` deny rule covering any
/// one of them still blocks it. Without that pass, approving a patch would be
/// approving paths the policy fences off; with a prompt per path, a five-file
/// patch would ask five times.
///
/// Returns the refusal text naming the path that was blocked.
pub async fn allow_write_batch(
    ctx: &ToolContext,
    paths: &[PathBuf],
    summary: String,
) -> Option<String> {
    let first = paths.first()?;
    if let Some(refusal) = allow_write(ctx, first, summary).await {
        return Some(refusal);
    }
    for path in paths.iter().skip(1) {
        let request = ApprovalRequest::safe(format!("write {}", path.display())).with_action(
            ActionRef::File {
                path: path.clone(),
                write: true,
            },
        );
        if let Decision::Deny { feedback } = ctx.decide(&request).await {
            return Some(match feedback {
                Some(reason) => format!(
                    "Blocked before anything was written: {} is not writable ({reason}).",
                    path.display()
                ),
                None => format!(
                    "Blocked before anything was written: {} is not writable \
                     under the permission policy.",
                    path.display()
                ),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::detached_ctx;

    fn ws(root: &str) -> Arc<Workspace> {
        Arc::new(Workspace::new(vec![PathBuf::from(root)]))
    }

    #[test]
    fn relative_paths_anchor_to_the_workspace_root() {
        let resolved = resolve(&ws("/home/u/p"), &detached_ctx("test"), "src/main.rs").unwrap();
        assert_eq!(resolved, PathBuf::from("/home/u/p/src/main.rs"));
    }

    #[test]
    fn escapes_are_denied_not_merely_reported() {
        let err = resolve(&ws("/home/u/p"), &detached_ctx("test"), "../secret").unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)));
        // The message names the allowed root so the model can retry sensibly.
        assert!(err.to_string().contains("/home/u/p"));
    }

    #[test]
    fn selected_workspace_overrides_the_process_default() {
        let mut session = komo_core::domain::context::SessionContext::detached("test");
        session.workspace_root = Some(PathBuf::from("/home/u/selected"));
        let ctx = komo_core::domain::context::ToolContext::new(
            session,
            None,
            std::sync::Arc::new(crate::test_support::SafeOnly),
        );
        let resolved = resolve(&ws("/home/u/default"), &ctx, "src/main.rs").unwrap();
        assert_eq!(resolved, PathBuf::from("/home/u/selected/src/main.rs"));
    }
}
