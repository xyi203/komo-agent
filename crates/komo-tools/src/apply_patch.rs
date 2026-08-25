//! The `apply_patch` tool: add, update and delete several files in one call.
//!
//! What it buys over N `edit` calls: one approval showing the whole blast radius,
//! and one model round-trip instead of one per file. What it deliberately does
//! *not* buy: atomicity. Operations apply in order, and a failure part-way
//! through leaves the earlier ones on disk — so the error names exactly what
//! landed, because a model that doesn't know which half applied will make things
//! worse. (opencode v2 makes the same trade.)
//!
//! A half-applied patch is recoverable after the fact, though not during:
//! every file this touches is checkpointed, so `komo run rollback` puts the
//! turn's changes back the way they were.

use std::path::PathBuf;
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
use komo_services::{diff, file_mutation, patch};

#[derive(Deserialize)]
struct PatchArgs {
    #[serde(rename = "patchText")]
    patch_text: String,
}

pub struct ApplyPatchTool {
    workspace: Arc<Workspace>,
}

impl ApplyPatchTool {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[async_trait]
impl Tool for ApplyPatchTool {
    fn name(&self) -> &'static str {
        "apply_patch"
    }

    fn description(&self) -> &'static str {
        "Apply one patch that adds, updates and/or deletes several files, with a \
         single approval (requires user approval). Use this instead of many `edit` \
         calls when a change spans files. Format:\n\
         *** Begin Patch\n\
         *** Add File: path\n\
         +new line\n\
         *** Update File: path\n\
         @@ optional context line\n\
         -removed line\n\
         +added line\n\
         *** Delete File: path\n\
         *** End Patch\n\
         Context lines start with a space. No line numbers — chunks are located by \
         their context, so copy enough surrounding lines to be unambiguous. Moves \
         are not supported: add the new file and delete the old one."
    }

    /// This call can park on an approval prompt, so it must outlast one.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(komo_core::domain::tool::APPROVAL_BOUND)
    }

    /// The patch body is the change itself — arbitrarily large, possibly
    /// secret-bearing. Keep only its shape in the ledger.
    fn redact_args(&self, args: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(args) {
            Ok(v) => {
                let len = v
                    .get("patchText")
                    .and_then(|t| t.as_str())
                    .map(str::len)
                    .unwrap_or(0);
                json!({ "patchText": format!("<{len} bytes redacted>") }).to_string()
            }
            Err(_) => "<apply_patch args redacted>".to_string(),
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "patchText": {
                    "type": "string",
                    "description": "The complete patch, between `*** Begin Patch` and `*** End Patch`."
                }
            },
            "required": ["patchText"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: PatchArgs = parse_args(&input)?;
        if args.patch_text.trim().is_empty() {
            return Err(ToolError::InvalidInput(
                "`patchText` is required".to_string(),
            ));
        }
        let hunks = patch::parse(&args.patch_text).map_err(ToolError::InvalidInput)?;
        if hunks.is_empty() {
            return Err(ToolError::InvalidInput(
                "patch rejected: it contains no file operations".to_string(),
            ));
        }

        // Resolve every target before touching anything: an out-of-workspace path
        // anywhere in the patch refuses the whole patch, with nothing applied.
        let mut targets: Vec<(PathBuf, &patch::Hunk)> = Vec::with_capacity(hunks.len());
        for hunk in &hunks {
            targets.push((fs_common::resolve(&self.workspace, ctx, hunk.path())?, hunk));
        }

        // One prompt for the whole blast radius.
        let listing = targets
            .iter()
            .map(|(path, hunk)| format!("{} {}", mark(hunk), path.display()))
            .collect::<Vec<_>>()
            .join(", ");
        let paths: Vec<PathBuf> = targets.iter().map(|(p, _)| p.clone()).collect();
        let summary = format!("apply a patch to {} file(s): {listing}", targets.len());
        if let Some(refusal) = fs_common::allow_write_batch(ctx, &paths, summary).await {
            return Ok(ToolOutput::text(refusal));
        }

        let mut applied: Vec<String> = Vec::new();
        let mut files: Vec<Value> = Vec::new();
        for (path, hunk) in &targets {
            match self.apply_one(path, hunk, ctx).await {
                Ok(entry) => {
                    applied.push(format!("{} {}", mark(hunk), path.display()));
                    files.push(entry);
                }
                // Report what already landed: the model must not re-apply those.
                Err(error) => {
                    let prefix = if applied.is_empty() {
                        format!("Failed to apply the patch at {}", path.display())
                    } else {
                        format!(
                            "Patch partially applied, then failed at {}. \
                             ALREADY APPLIED (do not repeat these): {}",
                            path.display(),
                            applied.join(", ")
                        )
                    };
                    return Err(ToolError::Failed(anyhow::anyhow!("{prefix}: {error}")));
                }
            }
        }

        Ok(
            ToolOutput::text(format!("Applied patch:\n{}", applied.join("\n")))
                .with_title(format!("apply_patch ({} files)", applied.len()))
                .with_structured(json!({ "files": files })),
        )
    }
}

impl ApplyPatchTool {
    /// Apply one hunk, returning its structured record.
    async fn apply_one(
        &self,
        path: &PathBuf,
        hunk: &patch::Hunk,
        ctx: &ToolContext,
    ) -> anyhow::Result<Value> {
        match hunk {
            patch::Hunk::Add { contents, .. } => {
                let before = file_mutation::snapshot(path).await?;
                if before.existed() {
                    anyhow::bail!(
                        "{} already exists — use an update hunk to change it",
                        path.display()
                    );
                }
                // `contents` comes from `+` lines, so it needs its final newline.
                let body = ensure_newline(contents);
                file_mutation::write_if_unchanged(path, &before, &body, ctx.run.as_ref()).await?;
                let stats = diff::unified(&path.display().to_string(), "", &body);
                Ok(json!({
                    "type": "add",
                    "path": path.display().to_string(),
                    "additions": stats.additions,
                    "deletions": 0,
                    "patch": stats.patch,
                }))
            }
            patch::Hunk::Delete { .. } => {
                file_mutation::delete_existing(path, ctx.run.as_ref()).await?;
                Ok(json!({ "type": "delete", "path": path.display().to_string() }))
            }
            patch::Hunk::Update { chunks, .. } => {
                let before = file_mutation::snapshot(path).await?;
                if !before.existed() {
                    anyhow::bail!(
                        "{} does not exist — use an add hunk to create it",
                        path.display()
                    );
                }
                let source = before
                    .text()
                    .ok_or_else(|| anyhow::anyhow!("{} is not valid UTF-8 text", path.display()))?;
                let label = path.display().to_string();
                let updated =
                    patch::apply(&label, &source, chunks).map_err(|e| anyhow::anyhow!("{e}"))?;
                file_mutation::write_if_unchanged(path, &before, &updated, ctx.run.as_ref())
                    .await?;
                let stats = diff::unified(&label, &source, &updated);
                Ok(json!({
                    "type": "update",
                    "path": label,
                    "additions": stats.additions,
                    "deletions": stats.deletions,
                    "patch": stats.patch,
                }))
            }
        }
    }
}

/// `A` / `M` / `D`, the git-familiar shorthand.
fn mark(hunk: &patch::Hunk) -> &'static str {
    match hunk {
        patch::Hunk::Add { .. } => "A",
        patch::Hunk::Update { .. } => "M",
        patch::Hunk::Delete { .. } => "D",
    }
}

fn ensure_newline(text: &str) -> String {
    if text.is_empty() || text.ends_with('\n') {
        text.to_string()
    } else {
        format!("{text}\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::approving_ctx;
    use komo_core::domain::approval::{ActionRef, ApprovalRequest, Approver, Decision};
    use komo_core::domain::context::{SessionContext, ToolContext};

    fn tool_in(tag: &str) -> (ApplyPatchTool, PathBuf) {
        let dir = std::env::temp_dir().join(format!("komo_patch_{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        (
            ApplyPatchTool::new(Arc::new(Workspace::new(vec![dir.clone()]))),
            dir,
        )
    }

    #[tokio::test]
    async fn applies_add_update_and_delete_in_one_call() {
        let (tool, dir) = tool_in("all_three");
        std::fs::write(dir.join("main.rs"), "fn main() {\n    old();\n}\n").unwrap();
        std::fs::write(dir.join("stale.rs"), "gone\n").unwrap();

        let patch_text = "\
*** Begin Patch
*** Add File: new.rs
+fn hello() {}
*** Update File: main.rs
@@ fn main() {
-    old();
+    new();
*** Delete File: stale.rs
*** End Patch";
        let out = tool
            .call(json!({ "patchText": patch_text }), &approving_ctx("cli:t"))
            .await
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.join("new.rs")).unwrap(),
            "fn hello() {}\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("main.rs")).unwrap(),
            "fn main() {\n    new();\n}\n"
        );
        assert!(!dir.join("stale.rs").exists());
        assert!(out.text.contains("A "), "{}", out.text);
        assert!(out.text.contains("M "), "{}", out.text);
        assert!(out.text.contains("D "), "{}", out.text);
        assert_eq!(out.structured["files"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn one_prompt_covers_the_whole_patch() {
        /// Counts how many times a human was asked (`Risk::Normal`), ignoring the
        /// policy-only `Safe` re-checks.
        struct CountPrompts(std::sync::Mutex<usize>);
        #[async_trait]
        impl Approver for CountPrompts {
            async fn decide(&self, r: &ApprovalRequest) -> Decision {
                if r.risk != komo_core::domain::approval::Risk::Safe {
                    *self.0.lock().unwrap() += 1;
                }
                Decision::Allow
            }
        }
        let (tool, dir) = tool_in("one_prompt");
        for name in ["a.rs", "b.rs", "c.rs"] {
            std::fs::write(dir.join(name), "x\n").unwrap();
        }
        let counter = Arc::new(CountPrompts(std::sync::Mutex::new(0)));
        let ctx = ToolContext::new(SessionContext::detached("cli:t"), None, counter.clone());

        let patch_text = "\
*** Begin Patch
*** Update File: a.rs
@@
-x
+A
*** Update File: b.rs
@@
-x
+B
*** Update File: c.rs
@@
-x
+C
*** End Patch";
        tool.call(json!({ "patchText": patch_text }), &ctx)
            .await
            .unwrap();
        assert_eq!(*counter.0.lock().unwrap(), 1, "three files, one prompt");
    }

    #[tokio::test]
    async fn a_denied_patch_changes_nothing() {
        struct Deny;
        #[async_trait]
        impl Approver for Deny {
            async fn decide(&self, _r: &ApprovalRequest) -> Decision {
                Decision::deny_because("先给我看 diff")
            }
        }
        let (tool, dir) = tool_in("denied");
        std::fs::write(dir.join("a.rs"), "x\n").unwrap();
        let ctx = ToolContext::new(SessionContext::detached("cli:t"), None, Arc::new(Deny));
        let out = tool
            .call(
                json!({ "patchText": "*** Begin Patch\n*** Update File: a.rs\n@@\n-x\n+y\n*** End Patch" }),
                &ctx,
            )
            .await
            .unwrap();
        assert!(out.text.contains("先给我看 diff"), "{}", out.text);
        assert_eq!(std::fs::read_to_string(dir.join("a.rs")).unwrap(), "x\n");
    }

    /// A policy deny on *any* path in the patch stops the whole thing before a
    /// single byte is written.
    #[tokio::test]
    async fn a_policy_deny_on_one_path_blocks_the_batch() {
        struct DenyLocked;
        #[async_trait]
        impl Approver for DenyLocked {
            async fn decide(&self, r: &ApprovalRequest) -> Decision {
                let locked = matches!(&r.action, Some(ActionRef::File { path, .. })
                    if path.to_string_lossy().contains("locked"));
                if locked {
                    Decision::deny_because("这个文件受保护")
                } else {
                    Decision::Allow
                }
            }
        }
        let (tool, dir) = tool_in("policy_deny");
        std::fs::write(dir.join("free.rs"), "x\n").unwrap();
        std::fs::write(dir.join("locked.rs"), "x\n").unwrap();
        let ctx = ToolContext::new(
            SessionContext::detached("cli:t"),
            None,
            Arc::new(DenyLocked),
        );
        let out = tool
            .call(
                json!({ "patchText": "*** Begin Patch\n*** Update File: free.rs\n@@\n-x\n+y\n*** Update File: locked.rs\n@@\n-x\n+y\n*** End Patch" }),
                &ctx,
            )
            .await
            .unwrap();
        assert!(out.text.contains("locked.rs"), "{}", out.text);
        assert!(out.text.contains("受保护"), "{}", out.text);
        // Neither file moved: the check runs before any write.
        assert_eq!(std::fs::read_to_string(dir.join("free.rs")).unwrap(), "x\n");
    }

    /// No rollback, so the error has to say what already landed.
    #[tokio::test]
    async fn a_mid_patch_failure_reports_what_was_applied() {
        let (tool, dir) = tool_in("partial");
        std::fs::write(dir.join("first.rs"), "x\n").unwrap();
        std::fs::write(dir.join("second.rs"), "totally different\n").unwrap();

        let patch_text = "\
*** Begin Patch
*** Update File: first.rs
@@
-x
+DONE
*** Update File: second.rs
@@
-x
+never
*** End Patch";
        let err = tool
            .call(json!({ "patchText": patch_text }), &approving_ctx("cli:t"))
            .await
            .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("partially applied"), "{text}");
        assert!(text.contains("ALREADY APPLIED"), "{text}");
        assert!(text.contains("first.rs"), "{text}");
        // The first file really did change; the second did not.
        assert_eq!(
            std::fs::read_to_string(dir.join("first.rs")).unwrap(),
            "DONE\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("second.rs")).unwrap(),
            "totally different\n"
        );
    }

    #[tokio::test]
    async fn an_out_of_workspace_path_refuses_the_whole_patch() {
        let (tool, dir) = tool_in("escape");
        std::fs::write(dir.join("ok.rs"), "x\n").unwrap();
        let err = tool
            .call(
                json!({ "patchText": "*** Begin Patch\n*** Update File: ok.rs\n@@\n-x\n+y\n*** Delete File: /etc/passwd\n*** End Patch" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)));
        assert_eq!(std::fs::read_to_string(dir.join("ok.rs")).unwrap(), "x\n");
    }

    #[tokio::test]
    async fn adding_over_an_existing_file_is_refused() {
        let (tool, dir) = tool_in("add_exists");
        std::fs::write(dir.join("a.rs"), "already here\n").unwrap();
        let err = tool
            .call(
                json!({ "patchText": "*** Begin Patch\n*** Add File: a.rs\n+new\n*** End Patch" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(
            std::fs::read_to_string(dir.join("a.rs")).unwrap(),
            "already here\n"
        );
    }

    #[tokio::test]
    async fn malformed_and_empty_patches_are_invalid_input() {
        let (tool, _dir) = tool_in("malformed");
        let ctx = approving_ctx("cli:t");
        for body in ["", "   ", "not a patch", "*** Begin Patch\n*** End Patch"] {
            let err = tool
                .call(json!({ "patchText": body }), &ctx)
                .await
                .unwrap_err();
            assert!(
                matches!(err, ToolError::InvalidInput(_)),
                "{body:?} → {err}"
            );
        }
    }

    #[tokio::test]
    async fn redact_args_drops_the_patch_body() {
        let (tool, _dir) = tool_in("redact");
        let args = json!({ "patchText": "*** Begin Patch\nsecret" }).to_string();
        let redacted = tool.redact_args(&args);
        assert!(!redacted.contains("secret"));
        assert!(redacted.contains("redacted"));
    }
}
