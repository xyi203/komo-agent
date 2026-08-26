use std::sync::Arc;

use async_trait::async_trait;
use komo_infra::skills::FsSkillStore;
use komo_services::skill_registry::{LocatedSkill, SkillRegistry, skill_files};
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    approval::{ApprovalRequest, Decision},
    context::ToolContext,
    repository::SkillRepository,
    skill::{SOURCE_LEARNED, Skill},
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

#[derive(Deserialize)]
struct SkillArgs {
    action: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    source: Option<String>,
}

/// Lets the model discover, load, author, and install skills (progressive
/// disclosure): `list` returns the catalog; `view` returns a skill's full
/// instruction body, which the model then follows; `learn` distills a reusable
/// procedure into a **candidate** skill (the on-demand analog of the reflective
/// reviewer's passive extraction — same triage ladder); `install` fetches a
/// skill from a git repo or a raw SKILL.md URL and — once the operator approves
/// — installs it **active** (a human is always in the loop for third-party code).
pub struct SkillTool {
    registry: Arc<SkillRegistry>,
    store: Arc<FsSkillStore>,
}

impl SkillTool {
    pub fn new(registry: Arc<SkillRegistry>, store: Arc<FsSkillStore>) -> Self {
        Self { registry, store }
    }
}

#[async_trait]
impl Tool for SkillTool {
    fn name(&self) -> &'static str {
        "skill"
    }

    fn description(&self) -> &'static str {
        "Discover, load, author, and install skills (reusable instruction \
         playbooks). action=\"list\" returns available skills; action=\"view\" \
         returns a named skill's full instructions, which you should then \
         follow; action=\"learn\" saves a reusable procedure you just worked out \
         as a candidate skill for the operator to review (only learn durable, \
         reusable know-how, not one-off facts); action=\"install\" fetches a \
         skill the user points you at (a git repo or a SKILL.md URL) and \
         installs it after the operator approves."
    }

    /// These calls can park on an approval prompt, so they must outlast one.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(komo_core::domain::tool::APPROVAL_BOUND)
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "view", "learn", "install"],
                    "description": "Whether to list skills, view one, learn a new one, or install one."
                },
                "name": {
                    "type": "string",
                    "description": "Skill name — required for action=view and action=learn. \
                     For learn it doubles as the on-disk directory name: letters, digits, \
                     `-`/`_`/`.` only (a short, class-level slug like `sync-calendar`)."
                },
                "description": {
                    "type": "string",
                    "description": "One-line summary of what the skill does and when to use it \
                     (action=learn). Optional but strongly recommended."
                },
                "instructions": {
                    "type": "string",
                    "description": "The full skill body — the step-by-step reusable procedure \
                     (required for action=learn)."
                },
                "source": {
                    "type": "string",
                    "description": "Where to fetch the skill from (required for action=install): \
                     `owner/repo`, `owner/repo/subpath`, a GitHub URL, any `*.git`/`git@` URL, \
                     or a link straight to a raw SKILL.md."
                }
            },
            "required": ["action"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: SkillArgs = parse_args(&input)?;

        match args.action.as_str() {
            "list" => {
                if self.registry.is_empty() {
                    Ok(ToolOutput::text("(no skills installed)"))
                } else {
                    Ok(ToolOutput::text(self.registry.catalog()))
                }
            }
            "view" => {
                let name = args.name.ok_or_else(|| {
                    ToolError::InvalidInput("`name` is required for action=view".to_string())
                })?;
                match self.registry.get(&name) {
                    // A clear terminal answer, not an error: the model should
                    // move on, not retry other spellings.
                    Some(located) if located.skill.disabled => Ok(ToolOutput::text(format!(
                        "skill `{}` is disabled by the operator and cannot be used.",
                        located.skill.name
                    ))),
                    Some(located) => Ok(render_view(&located)),
                    None => Err(ToolError::InvalidInput(format!(
                        "skill `{name}` not found; use action=list to see available skills"
                    ))),
                }
            }
            "learn" => {
                let name = args.name.ok_or_else(|| {
                    ToolError::InvalidInput("`name` is required for action=learn".to_string())
                })?;
                let instructions = args.instructions.ok_or_else(|| {
                    ToolError::InvalidInput(
                        "`instructions` is required for action=learn".to_string(),
                    )
                })?;
                if instructions.trim().is_empty() {
                    return Err(ToolError::InvalidInput(
                        "`instructions` must not be empty".to_string(),
                    ));
                }
                let skill = Skill {
                    name: name.clone(),
                    description: args.description.unwrap_or_default(),
                    instructions,
                    protected: false,
                    disabled: false,
                    source: SOURCE_LEARNED.to_string(),
                    // Ungated: offer gating is an operator judgment about where a
                    // skill is worth advertising, not something to infer from a turn.
                    platforms: Vec::new(),
                    requires_tools: Vec::new(),
                    // Stamped by the store on write, not carried in.
                    updated_at: None,
                };
                // `save` writes a *candidate* (never an active skill): the same
                // triage ladder as the reviewer, and it refuses a protected
                // active skill or a path-escaping name. A candidate is invisible
                // to the runtime until promoted, so the reply must not imply it's
                // usable this turn.
                self.store.save(&skill).await?;
                Ok(ToolOutput::text(format!(
                    "Learned `{name}` as a candidate skill. Review it with \
                     `komo skills inspect {name}`, then `komo skills promote {name}` \
                     to activate (usable on the agent's next `skill` list once promoted)."
                ))
                .with_title(format!("learned {name}")))
            }
            "install" => {
                let source = args.source.ok_or_else(|| {
                    ToolError::InvalidInput("`source` is required for action=install".to_string())
                })?;
                // Installing third-party code is side-effecting and never
                // unattended: gate it through the approver (session-scoped, so a
                // `/approve session` covers a batch). Denied ⇒ nothing written.
                let request = ApprovalRequest::normal(format!(
                    "Install skill from `{source}` into the active skill store"
                ))
                .with_scope_key("skill:install".to_string());
                if let Decision::Deny { feedback } = ctx.decide(&request).await {
                    return Ok(ToolOutput::text(match feedback {
                        Some(reason) => format!(
                            "Skill install rejected by the user; nothing was installed. \
                             They said: {reason}"
                        ),
                        None => {
                            "Skill install rejected by user; nothing was installed.".to_string()
                        }
                    }));
                }
                let installed = komo_infra::skill_install::install(&self.store, &source).await?;
                let about = if installed.description.is_empty() {
                    String::new()
                } else {
                    format!(" — {}", installed.description)
                };
                Ok(ToolOutput::text(format!(
                    "Installed `{}` ({} file(s)){about}. It's active now: use \
                     `skill` view/list to load it (no restart needed).",
                    installed.name, installed.files
                ))
                .with_structured(json!({ "name": installed.name, "files": installed.files })))
            }
            other => Err(ToolError::InvalidInput(format!(
                "unknown action `{other}` (expected list/view/learn/install)"
            ))),
        }
    }
}

/// How many of a skill's own files are listed in a `view`. A sample, not the
/// inventory — the output says so, so the model asks (`glob`/`read`) rather than
/// assuming a script it needs isn't there.
const FILE_LIMIT: usize = 10;

/// Render `view`: the instruction body, plus **where it lives** and a sample of
/// the files next to it. Without the base directory a SKILL.md that says "run
/// `scripts/foo.py`" is unactionable — the model has no way to know what
/// `scripts/` is relative to.
fn render_view(located: &LocatedSkill) -> ToolOutput {
    let skill = &located.skill;
    let mut text = format!(
        "<skill_content name=\"{}\">\n# Skill: {}\n{}\n\n{}\n",
        skill.name, skill.name, skill.description, skill.instructions
    );
    let files = located
        .dir
        .as_deref()
        .map(|dir| skill_files(dir, FILE_LIMIT))
        .unwrap_or_default();
    if let Some(dir) = located.dir.as_deref() {
        let dir = std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf());
        text.push_str(&format!(
            "\nBase directory for this skill: {}\n\
             Relative paths in this skill (e.g., scripts/, references/) are relative to \
             this base directory.\n",
            dir.display()
        ));
        if !files.is_empty() {
            text.push_str("Note: file list is sampled.\n\n<skill_files>\n");
            for file in &files {
                text.push_str(&format!("<file>{}</file>\n", file.display()));
            }
            text.push_str("</skill_files>\n");
        }
    }
    text.push_str("</skill_content>");

    ToolOutput::text(text)
        .with_title(format!("skill {}", skill.name))
        .with_structured(json!({
            "name": skill.name,
            "directory": located.dir.as_ref().map(|d| d.display().to_string()),
            "files": files.iter().map(|f| f.display().to_string()).collect::<Vec<_>>(),
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::skill::Skill;

    fn registry() -> Arc<SkillRegistry> {
        Arc::new(SkillRegistry::new(vec![Skill {
            name: "greet".to_string(),
            description: "Say hello".to_string(),
            instructions: "Greet the user warmly.".to_string(),
            protected: false,
            disabled: false,
            source: "user".to_string(),
            platforms: Vec::new(),
            requires_tools: Vec::new(),
            updated_at: None,
        }]))
    }

    /// A throwaway on-disk store rooted in a unique temp dir.
    fn store(tag: &str) -> Arc<FsSkillStore> {
        let root = std::env::temp_dir().join(format!("komo_skilltool_{tag}"));
        let _ = std::fs::remove_dir_all(&root);
        Arc::new(FsSkillStore::new(root))
    }

    /// Install is the only action that consults the approver; every other test
    /// runs with this deny-all context, which would fail loudly if one did.
    fn ctx() -> ToolContext {
        crate::test_support::detached_ctx("cli:test")
    }

    fn tool_with(tag: &str) -> (SkillTool, Arc<FsSkillStore>) {
        let store = store(tag);
        (SkillTool::new(registry(), store.clone()), store)
    }

    #[tokio::test]
    async fn lists_and_views_skills() {
        let tool = SkillTool::new(registry(), store("listview"));

        let list = tool
            .call(json!({ "action": "list" }), &ctx())
            .await
            .unwrap();
        assert!(list.text.contains("greet: Say hello"));

        let view = tool
            .call(json!({ "action": "view", "name": "greet" }), &ctx())
            .await
            .unwrap();
        assert!(view.text.contains("Greet the user warmly."));
    }

    #[tokio::test]
    async fn view_disabled_skill_reports_state_without_instructions() {
        let tool = SkillTool::new(
            Arc::new(SkillRegistry::new(vec![Skill {
                name: "paused".to_string(),
                description: "d".to_string(),
                instructions: "secret steps".to_string(),
                protected: false,
                disabled: true,
                source: "user".to_string(),
                platforms: Vec::new(),
                requires_tools: Vec::new(),
                updated_at: None,
            }])),
            store("disabled"),
        );

        let view = tool
            .call(json!({ "action": "view", "name": "paused" }), &ctx())
            .await
            .unwrap();
        assert!(view.text.contains("disabled by the operator"));
        assert!(!view.text.contains("secret steps"));
    }

    /// The point of 08: a multi-file skill's `scripts/` is useless to the model
    /// unless `view` says where it is and what's in it.
    #[tokio::test]
    async fn view_reports_base_directory_and_sampled_files() {
        let root = std::env::temp_dir().join("komo_skillview_files_root");
        let _ = std::fs::remove_dir_all(&root);
        let skill_dir = root.join("packer");
        std::fs::create_dir_all(skill_dir.join("scripts")).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: packer\ndescription: Pack things\n---\nRun scripts/pack.py.",
        )
        .unwrap();
        std::fs::write(skill_dir.join("scripts/pack.py"), "print(1)").unwrap();

        let tool = SkillTool::new(
            Arc::new(SkillRegistry::load_from_dirs(std::slice::from_ref(&root))),
            store("view_files"),
        );
        let out = tool
            .call(json!({ "action": "view", "name": "packer" }), &ctx())
            .await
            .unwrap();

        assert!(out.text.contains("Run scripts/pack.py."));
        assert!(out.text.contains(&format!(
            "Base directory for this skill: {}",
            skill_dir.display()
        )));
        assert!(out.text.contains("file list is sampled"));
        assert!(out.text.contains(&format!(
            "<file>{}</file>",
            skill_dir.join("scripts/pack.py").display()
        )));
        assert_eq!(out.structured["files"].as_array().unwrap().len(), 1);

        let _ = std::fs::remove_dir_all(&root);
    }

    /// No assets ⇒ no `<skill_files>` block (an empty one reads as "this skill
    /// has files" to a model skimming the output), but the base directory still
    /// helps: the skill may create files there.
    #[tokio::test]
    async fn view_omits_the_file_block_for_a_lone_skill_md() {
        let root = std::env::temp_dir().join("komo_skillview_lone_root");
        let _ = std::fs::remove_dir_all(&root);
        let skill_dir = root.join("solo");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: solo\ndescription: d\n---\nbody",
        )
        .unwrap();

        let tool = SkillTool::new(
            Arc::new(SkillRegistry::load_from_dirs(std::slice::from_ref(&root))),
            store("view_lone"),
        );
        let out = tool
            .call(json!({ "action": "view", "name": "solo" }), &ctx())
            .await
            .unwrap();

        assert!(out.text.contains("Base directory for this skill:"));
        assert!(!out.text.contains("<skill_files>"));
        assert!(!out.text.contains("sampled"));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn install_is_refused_when_the_approver_denies() {
        let (tool, store) = tool_with("install_denied");
        // Deny-all context ⇒ short-circuits before any fetch; nothing installed.
        let out = tool
            .call(
                json!({ "action": "install", "source": "owner/repo" }),
                &ctx(),
            )
            .await
            .unwrap();
        assert!(out.text.contains("rejected"));
        assert!(store.list_active().is_empty());
    }

    #[tokio::test]
    async fn install_requires_a_source() {
        let (tool, _store) = tool_with("install_nosource");
        let err = tool
            .call(json!({ "action": "install" }), &ctx())
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
        assert!(err.to_string().contains("source"));
    }

    #[tokio::test]
    async fn view_unknown_skill_errors() {
        let tool = SkillTool::new(registry(), store("unknown"));
        let err = tool
            .call(json!({ "action": "view", "name": "nope" }), &ctx())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn learn_writes_a_candidate() {
        let (tool, store) = tool_with("learn_candidate");
        let reply = tool
            .call(
                json!({
                    "action": "learn",
                    "name": "sync-cal",
                    "description": "Sync the calendar",
                    "instructions": "Step 1. Open the calendar.\nStep 2. Sync."
                }),
                &ctx(),
            )
            .await
            .unwrap()
            .text;
        assert!(reply.contains("candidate"));

        // Lands as a candidate (not active), tagged with `learned` provenance.
        assert!(store.find_active("sync-cal").is_none());
        let cand = store.find_candidate("sync-cal").unwrap();
        assert_eq!(cand.source, komo_core::domain::skill::SOURCE_LEARNED);
        assert_eq!(cand.description, "Sync the calendar");
        assert!(cand.instructions.contains("Step 2. Sync."));
    }

    #[tokio::test]
    async fn learn_requires_name_and_instructions() {
        let (tool, _) = tool_with("learn_missing");
        assert!(
            tool.call(json!({ "action": "learn", "instructions": "x" }), &ctx())
                .await
                .is_err()
        );
        assert!(
            tool.call(json!({ "action": "learn", "name": "x" }), &ctx())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn learn_rejects_path_escaping_name() {
        let (tool, _) = tool_with("learn_badname");
        let err = tool
            .call(
                json!({ "action": "learn", "name": "../escape", "instructions": "body" }),
                &ctx(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid skill name"));
    }

    #[tokio::test]
    async fn learn_refuses_protected_active_skill() {
        let (tool, store) = tool_with("learn_protected");
        // Seed an active, protected skill of the same name.
        store
            .save(&Skill {
                name: "guarded".to_string(),
                description: "d".to_string(),
                instructions: "orig".to_string(),
                protected: false,
                disabled: false,
                source: komo_core::domain::skill::SOURCE_LEARNED.to_string(),
                platforms: Vec::new(),
                requires_tools: Vec::new(),
                updated_at: None,
            })
            .await
            .unwrap();
        store.promote("guarded").unwrap();
        store.set_protected("guarded", true).unwrap();

        let err = tool
            .call(
                json!({ "action": "learn", "name": "guarded", "instructions": "new body" }),
                &ctx(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("protected"));
        assert!(store.find_candidate("guarded").is_none());
    }
}
