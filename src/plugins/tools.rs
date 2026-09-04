//! The built-in tool plugins: komo's own tools, the web pair, and the
//! Home Assistant integration.

use std::sync::Arc;

use async_trait::async_trait;

use komo_tools::apply_patch::ApplyPatchTool;
use komo_tools::ask_user::AskUserTool;
use komo_tools::cron::CronTool;
use komo_tools::edit::EditTool;
use komo_tools::glob::GlobTool;
use komo_tools::grep::GrepTool;
use komo_tools::homeassistant::HomeAssistantTool;
use komo_tools::logs::LogsTool;
use komo_tools::memory::MemoryTool;
use komo_tools::read::ReadTool;
use komo_tools::reminder::ReminderTool;
use komo_tools::session::SessionTool;
use komo_tools::shell::ShellTool;
use komo_tools::skill::SkillTool;
use komo_tools::task::TaskTool;
use komo_tools::time::TimeTool;
use komo_tools::todo::TodoTool;
use komo_tools::wait::WaitTool;
use komo_tools::web_fetch::WebFetchTool;
use komo_tools::web_search::WebSearchTool;
use komo_tools::write::WriteTool;

use super::{Plugin, Scope, ToolCx, ToolRegistry};

/// komo's own tool set. Scopes reproduce the pre-plugin wiring exactly:
/// mutating and stateful tools go to the three agentic runtimes; `time` and
/// `skill` — safe reads the briefing also had — go everywhere.
pub struct CoreToolsPlugin;

#[async_trait]
impl Plugin for CoreToolsPlugin {
    fn name(&self) -> &'static str {
        "core-tools"
    }

    async fn setup_tools(&self, reg: &mut ToolRegistry, cx: &ToolCx<'_>) -> anyhow::Result<()> {
        reg.tool(Scope::ALL, Arc::new(TimeTool));
        reg.tool(
            Scope::ALL,
            Arc::new(SkillTool::new(cx.skills.clone(), cx.skill_store.clone())),
        );

        let ws = &cx.workspace;
        reg.tool(Scope::AGENTIC, Arc::new(ReadTool::new(ws.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(WriteTool::new(ws.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(EditTool::new(ws.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(ApplyPatchTool::new(ws.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(GrepTool::new(ws.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(GlobTool::new(ws.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(ShellTool::new(ws.clone())));
        // komo's own tracing log, so a failed tool call can be diagnosed from
        // the `tool` span in the same conversation that hit it.
        reg.tool(Scope::AGENTIC, Arc::new(LogsTool));
        reg.tool(
            Scope::AGENTIC,
            Arc::new({
                let tool = SessionTool::new(cx.db.clone(), cx.db.clone());
                match &cx.episodic {
                    Some(search) => tool.with_episodic_search(search.clone()),
                    None => tool,
                }
            }),
        );
        reg.tool(Scope::AGENTIC, Arc::new(ReminderTool::new(cx.db.clone())));
        // Scheduled jobs from inside a conversation. Every mutation is gated
        // through the executor's approver — a chat-authored job is
        // model-authored, unlike one added with `komo cron add`.
        reg.tool(
            Scope::AGENTIC,
            Arc::new(CronTool::new(cx.cron_jobs.clone())),
        );
        reg.tool(Scope::AGENTIC, Arc::new(TaskTool::new(cx.kanban.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(TodoTool::new(cx.db.clone())));
        reg.tool(Scope::AGENTIC, Arc::new(AskUserTool::new()));
        // Waiting is not conversation: a routine that checks something, waits two
        // hours and checks again is the reason this is registered everywhere an
        // agent turn runs, unattended ones included.
        reg.tool(Scope::ALL, Arc::new(WaitTool));
        reg.tool(
            Scope::AGENTIC,
            Arc::new(MemoryTool::new(
                cx.memory_repo.clone(),
                cx.memory_query.clone(),
            )),
        );
        Ok(())
    }
}

/// `web_fetch` + `web_search` — safe reads, available everywhere the
/// pre-plugin wiring had them (briefing included).
pub struct WebPlugin;

#[async_trait]
impl Plugin for WebPlugin {
    fn name(&self) -> &'static str {
        "web"
    }

    async fn setup_tools(&self, reg: &mut ToolRegistry, _cx: &ToolCx<'_>) -> anyhow::Result<()> {
        reg.tool(Scope::ALL, Arc::new(WebFetchTool::new()));
        reg.tool(Scope::ALL, Arc::new(WebSearchTool::new()));
        Ok(())
    }
}

/// The Home Assistant tool, mounted only when `[channels]`-independent HA
/// credentials are configured (`HASS_TOKEN`/`HASS_URL`).
pub struct HomeAssistantPlugin;

#[async_trait]
impl Plugin for HomeAssistantPlugin {
    fn name(&self) -> &'static str {
        "homeassistant"
    }

    async fn setup_tools(&self, reg: &mut ToolRegistry, cx: &ToolCx<'_>) -> anyhow::Result<()> {
        if let Some(ha) = &cx.config.runtime.homeassistant_tool {
            reg.tool(
                Scope::ALL,
                Arc::new(HomeAssistantTool::new(
                    ha.base_url.clone(),
                    ha.token.clone(),
                )),
            );
        }
        Ok(())
    }
}
