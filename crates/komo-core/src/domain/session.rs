use serde::{Deserialize, Serialize};

use super::message::Message;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    /// Immutable workspace identity chosen when the session is first created.
    /// Older sessions predate workspaces and therefore belong to the default.
    #[serde(default = "default_workspace")]
    pub workspace: String,
    pub messages: Vec<Message>,
    pub created_at: i64,
    /// Optional operator-set display name (empty = untitled; clients fall back
    /// to a label derived from the id). Set via `SessionRepository::set_title`.
    #[serde(default)]
    pub title: String,
    /// Lifecycle: `"active"` (default), `"archive"`, or `"deleted"`. A soft
    /// status set via `SessionRepository::set_status`; the session list hides
    /// `deleted`. See [`SESSION_STATUS_ACTIVE`] etc.
    #[serde(default = "default_status")]
    pub status: String,
    /// Per-session model override (empty = the gateway's configured model).
    /// Unlike [`workspace`](Self::workspace) this is *not* creation-locked — a
    /// conversation may switch models mid-thread, and the last choice is what
    /// the next turn (and any other client opening the session) uses. Only
    /// honored for the main agent; aux/reviewer/briefing keep their own model.
    #[serde(default)]
    pub model: String,
    /// Per-session reasoning effort (`low` / `medium` / `high`; empty = the
    /// provider default). Which values a provider actually supports is decided
    /// by the LLM adapter — see `infra::llm::reasoning_params`.
    #[serde(default)]
    pub effort: String,
}

/// Default session status when none is stored (older rows, fresh sessions).
pub const SESSION_STATUS_ACTIVE: &str = "active";
pub const SESSION_STATUS_ARCHIVE: &str = "archive";
pub const SESSION_STATUS_DELETED: &str = "deleted";
pub const DEFAULT_WORKSPACE: &str = "__default__";

fn default_status() -> String {
    SESSION_STATUS_ACTIVE.to_string()
}

fn default_workspace() -> String {
    DEFAULT_WORKSPACE.to_string()
}

impl Session {
    pub fn new(id: impl Into<String>) -> Self {
        Self::with_workspace(id, DEFAULT_WORKSPACE)
    }

    pub fn with_workspace(id: impl Into<String>, workspace: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            workspace: workspace.into(),
            messages: Vec::new(),
            created_at: time::OffsetDateTime::now_utc().unix_timestamp(),
            title: String::new(),
            status: default_status(),
            model: String::new(),
            effort: String::new(),
        }
    }

    /// The session's model override, or `None` when it runs on the gateway
    /// default.
    pub fn model_override(&self) -> Option<&str> {
        Some(self.model.trim()).filter(|m| !m.is_empty())
    }

    /// The session's reasoning effort, or `None` for the provider default.
    pub fn effort_override(&self) -> Option<&str> {
        Some(self.effort.trim()).filter(|e| !e.is_empty())
    }

    pub fn user_turns(&self) -> usize {
        self.messages
            .iter()
            .filter(|m| m.role == super::message::Role::User)
            .count()
    }

    /// What the person opened this conversation with, if they have said
    /// anything yet.
    pub fn opening_message(&self) -> Option<&str> {
        self.messages
            .iter()
            .find(|m| m.role == super::message::Role::User)
            .map(|m| m.content.as_str())
    }

    /// How this session should read in a list: the name someone gave it, else
    /// one derived from its opening message. Empty when neither exists and the
    /// client must fall back to something id- or time-based.
    ///
    /// An operator rename always wins — [`title`](Self::title) is only ever
    /// written by a person, and a derived name must never overwrite one.
    pub fn display_title(&self) -> String {
        let named = self.title.trim();
        if !named.is_empty() {
            return named.to_string();
        }
        self.opening_message()
            .and_then(|opening| auto_title(&self.id, opening))
            .unwrap_or_default()
    }
}

/// Session-id prefix for a sub-agent turn spawned by the `delegate` tool. In
/// `domain` because both ends need it and neither owns the other: the tool mints
/// these ids, and the operator-facing session list filters them back out.
pub const SUBAGENT_SESSION_PREFIX: &str = "delegate:";

/// Is this a sub-agent's scratch session rather than a real conversation?
pub fn is_subagent_session(id: &str) -> bool {
    id.starts_with(SUBAGENT_SESSION_PREFIX)
}

/// Session-id prefixes whose turns komo writes to itself — a sweep's prompt is
/// not something a person said. The same two `LearningCoordinator` exempts, for
/// a related reason: neither a lesson nor a name should come from komo's own
/// prose.
const SWEEP_SESSION_PREFIXES: [&str; 2] = ["cron:", "briefing:"];

/// The character budget for a derived title. Generous next to the ~18 CJK
/// characters a sidebar row shows, because that row truncates in CSS and wider
/// surfaces can spend the rest.
pub const AUTO_TITLE_CHARS: usize = 40;

/// A name for a conversation, taken from the first thing the person said in it.
///
/// **Derived, not generated.** No model call, so a conversation is named the
/// instant it starts, naming cannot fail, cost a token, or hand the message to
/// an aux provider — and every session that already exists is named too, with
/// no backfill, because the derivation runs on read. The trade is accuracy:
/// "帮我看一下这个" names nothing. That is why this returns `Option` and the
/// id- and time-based fallbacks stay.
///
/// `None` for a sweep's session: its id (`cron:<job>:<ts>`) already says
/// precisely what it is, and the opening line of a generated prompt would only
/// blur that. `None` for a sub-agent's, which no list shows at all.
pub fn auto_title(session_id: &str, opening_message: &str) -> Option<String> {
    if is_subagent_session(session_id)
        || SWEEP_SESSION_PREFIXES
            .iter()
            .any(|prefix| session_id.starts_with(prefix))
    {
        return None;
    }
    // The first line that carries words. A fence is skipped rather than shown
    // because a message that opens by pasting code would otherwise be named
    // "```rust" — true of the text, useless as a name.
    let line = opening_message
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with("```"))?;
    // One line, single-spaced: a tab or a run of spaces out of a paste renders
    // as a gap a 264px row cannot afford.
    let mut title = line.split_whitespace().collect::<Vec<_>>().join(" ");
    if title.chars().count() > AUTO_TITLE_CHARS {
        title = title.chars().take(AUTO_TITLE_CHARS).collect();
        title.truncate(title.trim_end().len());
        title.push('…');
    }
    Some(title).filter(|t| !t.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::message::Message;

    fn session_saying(id: &str, opening: &str) -> Session {
        let mut session = Session::new(id);
        session.messages.push(Message::user(opening));
        session
    }

    #[test]
    fn a_title_is_the_first_line_a_person_wrote() {
        assert_eq!(
            auto_title("api:0198f0d1", "帮我查一下订单为什么失败").as_deref(),
            Some("帮我查一下订单为什么失败")
        );
    }

    #[test]
    fn leading_blank_lines_and_an_opening_code_fence_are_skipped() {
        // A message that opens by pasting code would otherwise be named after
        // the fence.
        assert_eq!(
            auto_title("api:x", "\n\n```rust\nfn main() {}\n```\n这段为什么不编译").as_deref(),
            Some("fn main() {}")
        );
        assert_eq!(auto_title("api:x", "   \n\t\n").as_deref(), None);
    }

    #[test]
    fn internal_whitespace_collapses_to_single_spaces() {
        assert_eq!(
            auto_title("api:x", "fix   the\tbuild  please").as_deref(),
            Some("fix the build please")
        );
    }

    #[test]
    fn a_long_opening_is_cut_on_a_char_boundary_with_no_dangling_space() {
        let long = "帮".repeat(AUTO_TITLE_CHARS + 10);
        let title = auto_title("api:x", &long).unwrap();
        assert_eq!(title.chars().count(), AUTO_TITLE_CHARS + 1);
        assert!(title.ends_with('…'));

        // The cut must not leave the ellipsis floating after a space.
        let spaced = format!("{} tail", "a".repeat(AUTO_TITLE_CHARS - 1));
        assert_eq!(
            auto_title("api:x", &spaced).unwrap(),
            format!("{}…", "a".repeat(AUTO_TITLE_CHARS - 1))
        );
    }

    #[test]
    fn a_sweep_keeps_its_id_instead() {
        // `cron:alarm:1755600000` says more than the first line of the prompt
        // komo wrote for itself.
        assert_eq!(auto_title("cron:alarm:1755600000", "检查告警并汇报"), None);
        assert_eq!(auto_title("briefing:2026-08-20", "生成今天的简报"), None);
        assert_eq!(auto_title("delegate:0198f0d1", "去查一下这个"), None);
        // Don't over-match: a real conversation may start with those words.
        assert!(auto_title("api:x", "cron: 帮我加个定时任务").is_some());
    }

    #[test]
    fn display_title_prefers_the_name_a_person_gave() {
        let mut session = session_saying("api:x", "随便问问");
        assert_eq!(session.display_title(), "随便问问");
        session.title = "订单排查".to_string();
        assert_eq!(session.display_title(), "订单排查");
    }

    #[test]
    fn display_title_is_empty_when_nothing_can_name_the_session() {
        assert_eq!(Session::new("api:x").display_title(), "");
        assert_eq!(
            session_saying("cron:alarm:1", "检查告警").display_title(),
            ""
        );
    }

    #[test]
    fn only_the_subagent_prefix_is_filtered() {
        // Don't over-match: a user conversation may legitimately mention the word.
        assert!(is_subagent_session("delegate:abc"));
        assert!(!is_subagent_session("api:delegate-notes"));
        assert!(!is_subagent_session("telegram:12345"));
        assert!(!is_subagent_session("cron:nightly:1785228839"));
    }
}
