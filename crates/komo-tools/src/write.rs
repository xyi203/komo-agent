//! The `write` tool: create or overwrite one file, gated by approval.
//!
//! Split out of the old `file{action:"write"}`. Behavior is the same except for
//! two additions: the user's reason for refusing reaches the model, and the
//! write is **stale-checked** against the moment the prompt went up
//! (`services::file_mutation`), so an edit that landed during a slow chat
//! approval is not silently discarded.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::fs_common;
use komo_core::domain::{
    context::ToolContext,
    tool::{Tool, ToolError, ToolOutput, parse_args},
    workspace::Workspace,
};
use komo_services::file_mutation;

#[derive(Deserialize)]
struct WriteArgs {
    path: String,
    content: String,
}

/// Writes files inside a [`Workspace`], with user approval.
pub struct WriteTool {
    workspace: Arc<Workspace>,
}

impl WriteTool {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &'static str {
        "write"
    }

    fn description(&self) -> &'static str {
        "Create a file, or replace an existing file's entire contents (requires \
         user approval). Relative paths resolve inside the workspace. To change \
         part of a file, read it first — replacing the whole body to alter a few \
         lines risks losing the rest."
    }

    /// This call can park on an approval prompt, so it must outlast one.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(komo_core::domain::tool::APPROVAL_BOUND)
    }

    /// Drop the write body before it reaches the run ledger — it can be
    /// arbitrarily large and may contain secrets. The action, path and a byte
    /// count keep the step readable.
    fn redact_args(&self, args: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(args) {
            Ok(mut v) => {
                if let Some(content) = v.get("content").and_then(|c| c.as_str()) {
                    let len = content.len();
                    v["content"] = serde_json::json!(format!("<{len} bytes redacted>"));
                }
                v.to_string()
            }
            // Unparseable args: keep nothing rather than risk leaking a body.
            Err(_) => "<write args redacted>".to_string(),
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File path, absolute or relative to the workspace root."
                },
                "content": {
                    "type": "string",
                    "description": "The file's complete new contents."
                }
            },
            "required": ["path", "content"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: WriteArgs = parse_args(&input)?;
        let path = fs_common::resolve(&self.workspace, ctx, &args.path)?;

        // Snapshot *before* prompting: that is what makes the post-approval
        // comparison meaningful.
        let before = file_mutation::snapshot(&path).await?;
        let existed = before.existed();

        let summary = format!(
            "{} {} with {} bytes",
            if existed { "overwrite" } else { "create" },
            path.display(),
            args.content.len()
        );
        if let Some(refusal) = fs_common::allow_write(ctx, &path, summary).await {
            return Ok(ToolOutput::text(refusal));
        }

        file_mutation::write_if_unchanged(&path, &before, &args.content, ctx.run.as_ref()).await?;

        Ok(ToolOutput::text(format!(
            "{} {} ({} bytes).",
            if existed { "Overwrote" } else { "Created" },
            path.display(),
            args.content.len()
        ))
        .with_title(format!("write {}", path.display()))
        .with_structured(json!({
            "path": path.display().to_string(),
            "bytes": args.content.len(),
            "existed": existed,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{approving_ctx, detached_ctx};
    use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
    use komo_core::domain::context::{SessionContext, ToolContext};
    use komo_services::artifact_store::ArtifactStore;
    use std::path::PathBuf;

    fn tool_in(tag: &str) -> (WriteTool, PathBuf) {
        let dir = std::env::temp_dir().join(format!("komo_write_{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        (
            WriteTool::new(Arc::new(Workspace::new(vec![dir.clone()]))),
            dir,
        )
    }

    /// komo's managed tool-output store is readable (see `read`) but must never
    /// be writable — a read-only root widens reads only, and an approving
    /// approver must not change that: it is a floor, not a prompt.
    #[tokio::test]
    async fn refuses_to_write_into_a_readonly_managed_root() {
        let base = std::env::temp_dir().join("komo_write_managed");
        let _ = std::fs::remove_dir_all(&base);
        let workspace_dir = base.join("project");
        let managed = base.join("tool-output");
        std::fs::create_dir_all(&workspace_dir).unwrap();
        std::fs::create_dir_all(&managed).unwrap();

        let tool = WriteTool::new(Arc::new(
            Workspace::new(vec![workspace_dir]).with_readonly(vec![managed.clone()]),
        ));
        let target = managed.join("sneaky.txt");
        let err = tool
            .call(
                json!({ "path": target.display().to_string(), "content": "x" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)), "{err}");
        assert!(!target.exists());

        let _ = std::fs::remove_dir_all(&base);
    }

    /// The mirror of the case above: komo's artifacts directory is outside the
    /// workspace and **is** writable, because it is where a turn's own output
    /// belongs. The session's subdirectory does not exist until something lands
    /// in it, and a traversal out of the root is still refused.
    #[tokio::test]
    async fn writes_into_the_artifacts_root_and_still_refuses_to_leave_it() {
        let base = std::env::temp_dir().join("komo_write_artifacts");
        let _ = std::fs::remove_dir_all(&base);
        let workspace_dir = base.join("project");
        let artifacts = base.join("artifacts");
        std::fs::create_dir_all(&workspace_dir).unwrap();

        let store = ArtifactStore::new(artifacts.clone());
        let tool = WriteTool::new(Arc::new(
            Workspace::new(vec![workspace_dir]).with_artifacts(artifacts.clone()),
        ));
        let session_dir = store.session_dir("cli:t");
        assert!(!session_dir.exists(), "nothing is created up front");

        let report = session_dir.join("report.md");
        let out = tool
            .call(
                json!({ "path": report.display().to_string(), "content": "# done" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap();
        assert!(out.text.contains("Created"), "{}", out.text);
        assert!(session_dir.is_dir(), "the first write creates it");
        assert_eq!(std::fs::read_to_string(&report).unwrap(), "# done");

        let escape = artifacts.join("../permissions.json");
        let err = tool
            .call(
                json!({ "path": escape.display().to_string(), "content": "x" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)), "{err}");
        assert!(!base.join("permissions.json").exists());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn creates_then_overwrites() {
        let (tool, dir) = tool_in("roundtrip");
        let ctx = approving_ctx("cli:t");

        let out = tool
            .call(json!({ "path": "a.txt", "content": "hello" }), &ctx)
            .await
            .unwrap();
        assert!(out.text.contains("Created"), "{}", out.text);
        assert_eq!(std::fs::read_to_string(dir.join("a.txt")).unwrap(), "hello");

        let out = tool
            .call(json!({ "path": "a.txt", "content": "bye" }), &ctx)
            .await
            .unwrap();
        assert!(out.text.contains("Overwrote"), "{}", out.text);
        assert_eq!(out.structured["existed"], true);
    }

    #[tokio::test]
    async fn missing_content_is_invalid_input() {
        let (tool, _dir) = tool_in("nocontent");
        let err = tool
            .call(json!({ "path": "a.txt" }), &approving_ctx("cli:t"))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn a_denied_write_changes_nothing_and_relays_the_reason() {
        struct DenyWithReason;
        #[async_trait]
        impl Approver for DenyWithReason {
            async fn decide(&self, _r: &ApprovalRequest) -> Decision {
                Decision::deny_because("那个文件是手写的，别覆盖")
            }
        }
        let (tool, dir) = tool_in("denied");
        std::fs::write(dir.join("keep.txt"), "original").unwrap();
        let ctx = ToolContext::new(
            SessionContext::detached("cli:t"),
            None,
            Arc::new(DenyWithReason),
        );
        let out = tool
            .call(json!({ "path": "keep.txt", "content": "clobbered" }), &ctx)
            .await
            .unwrap();
        assert!(out.text.contains("别覆盖"), "{}", out.text);
        assert_eq!(
            std::fs::read_to_string(dir.join("keep.txt")).unwrap(),
            "original"
        );
    }

    /// The stale guard, end to end: the approver edits the file while it
    /// "thinks", standing in for a slow chat approval.
    #[tokio::test]
    async fn a_write_racing_an_external_edit_is_refused() {
        struct ApproveButEdit(PathBuf);
        #[async_trait]
        impl Approver for ApproveButEdit {
            async fn decide(&self, _r: &ApprovalRequest) -> Decision {
                std::fs::write(&self.0, "someone else's edit").unwrap();
                Decision::Allow
            }
        }
        let (tool, dir) = tool_in("stale");
        let target = dir.join("race.txt");
        std::fs::write(&target, "original").unwrap();
        let ctx = ToolContext::new(
            SessionContext::detached("cli:t"),
            None,
            Arc::new(ApproveButEdit(target.clone())),
        );

        let err = tool
            .call(json!({ "path": "race.txt", "content": "mine" }), &ctx)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("changed after the approval"),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "someone else's edit",
            "the concurrent edit must survive"
        );
    }

    #[tokio::test]
    async fn paths_outside_the_workspace_are_denied_before_prompting() {
        let (tool, _dir) = tool_in("escape");
        let err = tool
            .call(
                json!({ "path": "/etc/passwd", "content": "x" }),
                // A deny-all approver: reaching it at all would be the bug.
                &detached_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)));
        assert!(err.to_string().contains("outside the workspace"));
    }

    #[tokio::test]
    async fn redact_args_drops_the_body_but_keeps_the_path() {
        let (tool, _dir) = tool_in("redact");
        let args = json!({ "path": "/x/y.txt", "content": "secret-body" }).to_string();
        let redacted = tool.redact_args(&args);
        assert!(!redacted.contains("secret-body"));
        assert!(redacted.contains("redacted"));
        assert!(redacted.contains("/x/y.txt"));
    }
}
