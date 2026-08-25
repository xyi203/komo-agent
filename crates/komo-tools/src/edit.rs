//! The `edit` tool: replace exact text in one file.
//!
//! The gap this closes: without it, changing one line meant `write`-ing the
//! whole file back — every byte through the model, and any part it
//! misremembered silently lost. Modeled on opencode v2's `edit`
//! (`packages/core/src/tool/edit.ts`), including its refusal texts, which are
//! themselves the instructions the model needs to fix its call.
//!
//! **No fuzzy matching**, deliberately: v2 dropped the line-trimmed /
//! block-anchor / indentation-correcting fallbacks and refuses with an
//! explanation instead. A near-miss edit applied to the wrong place is worse
//! than one that didn't apply.

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
use komo_services::{diff, file_mutation};

/// Lines of `-`/`+` context shown back to the model per edit.
const PREVIEW_LINES: usize = 6;
/// Per-preview-line cap.
const PREVIEW_LINE_CHARS: usize = 240;

#[derive(Deserialize)]
struct EditArgs {
    path: String,
    #[serde(rename = "oldString")]
    old_string: String,
    #[serde(rename = "newString")]
    new_string: String,
    #[serde(default, rename = "replaceAll")]
    replace_all: bool,
}

pub struct EditTool {
    workspace: Arc<Workspace>,
}

impl EditTool {
    pub fn new(workspace: Arc<Workspace>) -> Self {
        Self { workspace }
    }
}

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &'static str {
        "edit"
    }

    fn description(&self) -> &'static str {
        "Replace an exact piece of text in one file (requires user approval). \
         `oldString` must match the file byte for byte, including whitespace and \
         indentation — read the file first. It must also be unique: include \
         surrounding lines until it is, or set `replaceAll` to change every \
         occurrence. Prefer this over `write` for changing part of a file."
    }

    /// This call can park on an approval prompt, so it must outlast one.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(komo_core::domain::tool::APPROVAL_BOUND)
    }

    /// The replacement text can be large and may carry secrets; keep the shape
    /// of the edit in the ledger, not its content.
    fn redact_args(&self, args: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(args) {
            Ok(mut v) => {
                for field in ["oldString", "newString"] {
                    if let Some(text) = v.get(field).and_then(|c| c.as_str()) {
                        let len = text.len();
                        v[field] = json!(format!("<{len} bytes redacted>"));
                    }
                }
                v.to_string()
            }
            Err(_) => "<edit args redacted>".to_string(),
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "File to edit, absolute or relative to the workspace root."
                },
                "oldString": {
                    "type": "string",
                    "description": "The exact text to replace, including indentation."
                },
                "newString": {
                    "type": "string",
                    "description": "The replacement text. Must differ from oldString."
                },
                "replaceAll": {
                    "type": "boolean",
                    "description": "Replace every occurrence instead of requiring a unique match. Default false."
                }
            },
            "required": ["path", "oldString", "newString"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: EditArgs = parse_args(&input)?;

        // Cheap argument checks first — no filesystem, no prompt.
        if args.old_string == args.new_string {
            return Err(ToolError::InvalidInput(
                "No changes to apply: oldString and newString are identical.".to_string(),
            ));
        }
        if args.old_string.is_empty() {
            return Err(ToolError::InvalidInput(
                "oldString must not be empty. Use `write` to create or overwrite a file."
                    .to_string(),
            ));
        }

        let path = fs_common::resolve(&self.workspace, ctx, &args.path)?;

        // Snapshot before prompting: both the match source and the stale guard.
        let before = file_mutation::snapshot(&path).await?;
        if !before.existed() {
            return Err(ToolError::InvalidInput(format!(
                "{} does not exist. Use `write` to create it.",
                path.display()
            )));
        }
        let source = before.text().ok_or_else(|| {
            ToolError::InvalidInput(format!(
                "{} is not valid UTF-8 text; it cannot be edited.",
                path.display()
            ))
        })?;

        // Match in the file's own line-ending style: a model that read a CRLF
        // file still sends `\n`, and refusing that would be pedantry.
        let ending = if source.contains("\r\n") {
            "\r\n"
        } else {
            "\n"
        };
        let old = to_ending(&args.old_string, ending);
        let new = to_ending(&args.new_string, ending);

        let occurrences = source.matches(old.as_str()).count();
        if occurrences == 0 {
            return Err(ToolError::InvalidInput(format!(
                "Could not find oldString in {}. It must match exactly, including \
                 whitespace and indentation — read the file and copy the text verbatim.",
                path.display()
            )));
        }
        if occurrences > 1 && !args.replace_all {
            return Err(ToolError::InvalidInput(format!(
                "Found {occurrences} matches for oldString in {}. Provide more \
                 surrounding context to make it unique, or set replaceAll to true.",
                path.display()
            )));
        }

        let replaced = if args.replace_all {
            source.replace(old.as_str(), &new)
        } else {
            source.replacen(old.as_str(), &new, 1)
        };

        let summary = format!(
            "edit {} ({} replacement{})",
            path.display(),
            occurrences,
            if occurrences == 1 { "" } else { "s" }
        );
        if let Some(refusal) = fs_common::allow_write(ctx, &path, summary).await {
            return Ok(ToolOutput::text(refusal));
        }

        file_mutation::write_if_unchanged(&path, &before, &replaced, ctx.run.as_ref()).await?;

        let stats = diff::unified(&path.display().to_string(), &source, &replaced);
        let mut text = format!(
            "Edited {} — {} replacement{}, +{} -{}\n```diff\n",
            path.display(),
            occurrences,
            if occurrences == 1 { "" } else { "s" },
            stats.additions,
            stats.deletions
        );
        for line in preview(&args.old_string, '-') {
            text.push_str(&line);
            text.push('\n');
        }
        for line in preview(&args.new_string, '+') {
            text.push_str(&line);
            text.push('\n');
        }
        text.push_str("```");

        Ok(ToolOutput::text(text)
            .with_title(format!("edit {}", path.display()))
            .with_structured(json!({
                "path": path.display().to_string(),
                "replacements": occurrences,
                "additions": stats.additions,
                "deletions": stats.deletions,
                "patch": stats.patch,
            })))
    }
}

/// Convert `text`'s newlines to `ending` (it arrives from the model as `\n`).
fn to_ending(text: &str, ending: &str) -> String {
    let normalized = text.replace("\r\n", "\n");
    if ending == "\n" {
        normalized
    } else {
        normalized.replace('\n', "\r\n")
    }
}

/// The first few lines of `value`, each prefixed with `marker`, as a compact
/// confirmation of what changed (the full patch rides in `structured`).
fn preview(value: &str, marker: char) -> Vec<String> {
    let normalized = value.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();
    let mut out: Vec<String> = lines
        .iter()
        .take(PREVIEW_LINES)
        .map(|line| {
            let body: String = line.chars().take(PREVIEW_LINE_CHARS).collect();
            let clipped = line.chars().count() > PREVIEW_LINE_CHARS;
            format!("{marker}{body}{}", if clipped { "..." } else { "" })
        })
        .collect();
    if lines.len() > PREVIEW_LINES {
        out.push(format!("{marker}..."));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::approving_ctx;
    use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
    use komo_core::domain::context::{SessionContext, ToolContext};
    use std::path::PathBuf;

    fn tool_in(tag: &str) -> (EditTool, PathBuf) {
        let dir = std::env::temp_dir().join(format!("komo_edit_{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        (
            EditTool::new(Arc::new(Workspace::new(vec![dir.clone()]))),
            dir,
        )
    }

    #[tokio::test]
    async fn replaces_a_unique_match_and_reports_the_diff() {
        let (tool, dir) = tool_in("unique");
        std::fs::write(dir.join("a.rs"), "fn main() {\n    old();\n}\n").unwrap();
        let out = tool
            .call(
                json!({ "path": "a.rs", "oldString": "    old();", "newString": "    new();" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("a.rs")).unwrap(),
            "fn main() {\n    new();\n}\n"
        );
        assert!(out.text.contains("1 replacement"), "{}", out.text);
        assert!(out.text.contains("-    old();"), "{}", out.text);
        assert!(out.text.contains("+    new();"), "{}", out.text);
        assert_eq!(out.structured["additions"], 1);
        assert_eq!(out.structured["deletions"], 1);
        // The full patch is machine-readable, for the ledger and the UI.
        assert!(
            out.structured["patch"]
                .as_str()
                .unwrap()
                .contains("-    old();")
        );
    }

    #[tokio::test]
    async fn an_ambiguous_match_is_refused_with_the_count() {
        let (tool, dir) = tool_in("ambiguous");
        std::fs::write(dir.join("a.rs"), "x();\ny();\nx();\n").unwrap();
        let err = tool
            .call(
                json!({ "path": "a.rs", "oldString": "x();", "newString": "z();" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
        assert!(err.to_string().contains("Found 2 matches"), "{err}");
        // Nothing was touched.
        assert_eq!(
            std::fs::read_to_string(dir.join("a.rs")).unwrap(),
            "x();\ny();\nx();\n"
        );
    }

    #[tokio::test]
    async fn replace_all_changes_every_occurrence() {
        let (tool, dir) = tool_in("replaceall");
        std::fs::write(dir.join("a.rs"), "x();\ny();\nx();\n").unwrap();
        let out = tool
            .call(
                json!({ "path": "a.rs", "oldString": "x();", "newString": "z();", "replaceAll": true }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("a.rs")).unwrap(),
            "z();\ny();\nz();\n"
        );
        assert_eq!(out.structured["replacements"], 2);
    }

    #[tokio::test]
    async fn a_missing_match_explains_what_to_do() {
        let (tool, dir) = tool_in("nomatch");
        std::fs::write(dir.join("a.rs"), "actual text\n").unwrap();
        let err = tool
            .call(
                json!({ "path": "a.rs", "oldString": "not there", "newString": "x" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Could not find oldString"),
            "{err}"
        );
        assert!(err.to_string().contains("verbatim"), "{err}");
    }

    #[tokio::test]
    async fn identical_strings_and_an_empty_old_string_are_refused() {
        let (tool, dir) = tool_in("degenerate");
        std::fs::write(dir.join("a.rs"), "x\n").unwrap();
        let ctx = approving_ctx("cli:t");

        let same = tool
            .call(
                json!({ "path": "a.rs", "oldString": "x", "newString": "x" }),
                &ctx,
            )
            .await
            .unwrap_err();
        assert!(same.to_string().contains("identical"), "{same}");

        let empty = tool
            .call(
                json!({ "path": "a.rs", "oldString": "", "newString": "y" }),
                &ctx,
            )
            .await
            .unwrap_err();
        assert!(empty.to_string().contains("must not be empty"), "{empty}");
        assert!(empty.to_string().contains("`write`"), "{empty}");
    }

    #[tokio::test]
    async fn editing_a_missing_file_points_at_write() {
        let (tool, _dir) = tool_in("missing");
        let err = tool
            .call(
                json!({ "path": "nope.rs", "oldString": "a", "newString": "b" }),
                &approving_ctx("cli:t"),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
        assert!(err.to_string().contains("`write`"), "{err}");
    }

    /// A model that read a CRLF file still sends `\n`; the match must succeed and
    /// the file must stay CRLF.
    #[tokio::test]
    async fn crlf_files_match_lf_input_and_stay_crlf() {
        let (tool, dir) = tool_in("crlf");
        std::fs::write(dir.join("a.txt"), "one\r\ntwo\r\nthree\r\n").unwrap();
        tool.call(
            json!({ "path": "a.txt", "oldString": "one\ntwo", "newString": "one\nTWO" }),
            &approving_ctx("cli:t"),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("a.txt")).unwrap(),
            "one\r\nTWO\r\nthree\r\n"
        );
    }

    #[tokio::test]
    async fn a_bom_survives_the_edit() {
        let (tool, dir) = tool_in("bom");
        std::fs::write(dir.join("a.txt"), "\u{feff}old\n").unwrap();
        tool.call(
            json!({ "path": "a.txt", "oldString": "old", "newString": "new" }),
            &approving_ctx("cli:t"),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join("a.txt")).unwrap(),
            "\u{feff}new\n"
        );
    }

    #[tokio::test]
    async fn a_denied_edit_changes_nothing() {
        struct Deny;
        #[async_trait]
        impl Approver for Deny {
            async fn decide(&self, _r: &ApprovalRequest) -> Decision {
                Decision::deny_because("先跑测试再改")
            }
        }
        let (tool, dir) = tool_in("denied");
        std::fs::write(dir.join("a.rs"), "old\n").unwrap();
        let ctx = ToolContext::new(SessionContext::detached("cli:t"), None, Arc::new(Deny));
        let out = tool
            .call(
                json!({ "path": "a.rs", "oldString": "old", "newString": "new" }),
                &ctx,
            )
            .await
            .unwrap();
        assert!(out.text.contains("先跑测试再改"), "{}", out.text);
        assert_eq!(std::fs::read_to_string(dir.join("a.rs")).unwrap(), "old\n");
    }

    /// The stale guard inherited from `file_mutation`: an edit that raced an
    /// external write must not land.
    #[tokio::test]
    async fn an_edit_racing_an_external_write_is_refused() {
        struct ApproveButEdit(PathBuf);
        #[async_trait]
        impl Approver for ApproveButEdit {
            async fn decide(&self, _r: &ApprovalRequest) -> Decision {
                std::fs::write(&self.0, "theirs\n").unwrap();
                Decision::Allow
            }
        }
        let (tool, dir) = tool_in("stale");
        let target = dir.join("a.rs");
        std::fs::write(&target, "old\n").unwrap();
        let ctx = ToolContext::new(
            SessionContext::detached("cli:t"),
            None,
            Arc::new(ApproveButEdit(target.clone())),
        );
        let err = tool
            .call(
                json!({ "path": "a.rs", "oldString": "old", "newString": "new" }),
                &ctx,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("changed after the approval"),
            "{err}"
        );
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "theirs\n");
    }

    #[tokio::test]
    async fn redact_args_drops_both_bodies_but_keeps_the_path() {
        let (tool, _dir) = tool_in("redact");
        let args =
            json!({ "path": "/x/y.rs", "oldString": "secret-old", "newString": "secret-new" })
                .to_string();
        let redacted = tool.redact_args(&args);
        assert!(!redacted.contains("secret-old"));
        assert!(!redacted.contains("secret-new"));
        assert!(redacted.contains("/x/y.rs"));
    }

    #[test]
    fn preview_clips_long_and_many_lines() {
        let many = (0..20)
            .map(|i| format!("line{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let lines = preview(&many, '-');
        assert_eq!(lines.len(), PREVIEW_LINES + 1);
        assert_eq!(lines.last().unwrap(), "-...");

        let long = preview(&"x".repeat(PREVIEW_LINE_CHARS + 50), '+');
        assert!(long[0].ends_with("..."));
    }
}
