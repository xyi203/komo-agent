use serde::{Deserialize, Serialize};

use super::context::SessionOrigin;
use super::message::Message;

/// Where a conversation reaches its correspondent: a chat platform and that
/// platform's own id for the peer (`feishu` + `oc_abc`).
///
/// Deliberately **not** part of the session id. A session id is a handle —
/// opaque, stable, and the only thing that identifies a conversation; this is an
/// *address*, and a conversation has one or none. komo used to encode the
/// address into the handle (`feishu:oc_abc`), which meant every consumer
/// re-derived it by splitting a string, "no address" could only be expressed as
/// a missing colon, and a client that chose its own id could claim any channel's
/// scope by typing one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelPeer {
    pub platform: String,
    pub peer_id: String,
}

impl ChannelPeer {
    pub fn new(platform: impl Into<String>, peer_id: impl Into<String>) -> Self {
        Self {
            platform: platform.into(),
            peer_id: peer_id.into(),
        }
    }

    /// The address as one string, for the surfaces that legitimately key on it:
    /// a `MemoryScope::Channel` key, the `home_chat` config value, an operator
    /// display. Not a session id and never parsed back into one.
    pub fn address(&self) -> String {
        format!("{}:{}", self.platform, self.peer_id)
    }

    /// Parse an operator-authored address (`home_chat = "feishu:oc_abc"`).
    pub fn parse(raw: &str) -> Option<Self> {
        let (platform, peer_id) = raw.split_once(':')?;
        (!platform.is_empty() && !peer_id.is_empty()).then(|| Self::new(platform, peer_id))
    }
}

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
    /// The correspondent this conversation talks to, when it has one. `None`
    /// for every local surface (TUI, desktop, web, CLI) and for komo's own
    /// sessions — a sweep and a sub-agent answer to nobody.
    ///
    /// Creation-locked in practice: a channel looks a session up by this and
    /// would not find one whose address had changed.
    #[serde(default)]
    pub channel: Option<ChannelPeer>,
    /// What drives this conversation. Decides how it is titled, whether the
    /// session list shows it, and whether the learning pass may extract from it.
    #[serde(default)]
    pub origin: SessionOrigin,
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
            channel: None,
            origin: SessionOrigin::User,
        }
    }

    /// Bind this session to the correspondent it answers.
    pub fn with_channel(mut self, channel: ChannelPeer) -> Self {
        self.channel = Some(channel);
        self
    }

    /// Declare what drives this session (a sweep, a sub-agent).
    pub fn with_origin(mut self, origin: SessionOrigin) -> Self {
        self.origin = origin;
        self
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
            .and_then(|opening| auto_title(self.origin, opening))
            .unwrap_or_default()
    }
}

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
/// `None` for anything komo wrote to itself — a sweep restates what the agent
/// already knows and a sub-agent's scratch session appears in no list, so the
/// opening line of either is a generated prompt, not a name.
pub fn auto_title(origin: SessionOrigin, opening_message: &str) -> Option<String> {
    if origin != SessionOrigin::User {
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
            auto_title(SessionOrigin::User, "帮我查一下订单为什么失败").as_deref(),
            Some("帮我查一下订单为什么失败")
        );
    }

    #[test]
    fn leading_blank_lines_and_an_opening_code_fence_are_skipped() {
        // A message that opens by pasting code would otherwise be named after
        // the fence.
        assert_eq!(
            auto_title(
                SessionOrigin::User,
                "\n\n```rust\nfn main() {}\n```\n这段为什么不编译"
            )
            .as_deref(),
            Some("fn main() {}")
        );
        assert_eq!(
            auto_title(SessionOrigin::User, "   \n\t\n").as_deref(),
            None
        );
    }

    #[test]
    fn internal_whitespace_collapses_to_single_spaces() {
        assert_eq!(
            auto_title(SessionOrigin::User, "fix   the\tbuild  please").as_deref(),
            Some("fix the build please")
        );
    }

    #[test]
    fn a_long_opening_is_cut_on_a_char_boundary_with_no_dangling_space() {
        let long = "帮".repeat(AUTO_TITLE_CHARS + 10);
        let title = auto_title(SessionOrigin::User, &long).unwrap();
        assert_eq!(title.chars().count(), AUTO_TITLE_CHARS + 1);
        assert!(title.ends_with('…'));

        // The cut must not leave the ellipsis floating after a space.
        let spaced = format!("{} tail", "a".repeat(AUTO_TITLE_CHARS - 1));
        assert_eq!(
            auto_title(SessionOrigin::User, &spaced).unwrap(),
            format!("{}…", "a".repeat(AUTO_TITLE_CHARS - 1))
        );
    }

    #[test]
    fn komo_never_names_a_session_it_wrote_the_prompt_for() {
        for origin in [
            SessionOrigin::Cron,
            SessionOrigin::Briefing,
            SessionOrigin::Delegate,
        ] {
            assert_eq!(auto_title(origin, "检查告警并汇报"), None, "{origin:?}");
        }
        // The gate is the origin, not the words: a real conversation may open
        // with anything, including what a sweep's prompt would say.
        assert!(auto_title(SessionOrigin::User, "cron: 帮我加个定时任务").is_some());
        assert!(auto_title(SessionOrigin::User, "检查告警并汇报").is_some());
    }

    #[test]
    fn a_channel_address_round_trips_and_rejects_a_half_one() {
        let peer = ChannelPeer::new("feishu", "oc_abc");
        assert_eq!(peer.address(), "feishu:oc_abc");
        assert_eq!(ChannelPeer::parse("feishu:oc_abc"), Some(peer));
        // A bare session id is not an address.
        assert_eq!(
            ChannelPeer::parse("0198f0d1-9e3a-7c11-8a2b-1c2d3e4f5a6b"),
            None
        );
        assert_eq!(ChannelPeer::parse("feishu:"), None);
        assert_eq!(ChannelPeer::parse(":oc_abc"), None);
    }

    #[test]
    fn display_title_prefers_the_name_a_person_gave() {
        let mut session = session_saying("s", "随便问问");
        assert_eq!(session.display_title(), "随便问问");
        session.title = "订单排查".to_string();
        assert_eq!(session.display_title(), "订单排查");
    }

    #[test]
    fn display_title_is_empty_when_nothing_can_name_the_session() {
        // Nothing said yet.
        assert_eq!(Session::new("s").display_title(), "");
        // Said plenty, but komo wrote it.
        let mut sweep = session_saying("s", "检查告警");
        sweep.origin = SessionOrigin::Cron;
        assert_eq!(sweep.display_title(), "");
    }

    #[test]
    fn a_sessions_kind_is_a_field_no_reader_has_to_parse() {
        // The word in a conversation is just a word — what used to be a
        // substring test on the id is now a value nobody can spell wrong.
        let talking_about_it = session_saying("s", "帮我看看 delegate 这个工具");
        assert_eq!(talking_about_it.origin, SessionOrigin::User);
        assert_eq!(
            Session::new("s")
                .with_origin(SessionOrigin::Delegate)
                .origin,
            SessionOrigin::Delegate
        );
    }

    #[test]
    fn an_unknown_stored_origin_reads_as_an_ordinary_conversation() {
        // It decides display and learning eligibility, never authorization, so
        // an unreadable row should look like a conversation rather than vanish.
        assert_eq!(SessionOrigin::parse("wat"), SessionOrigin::User);
        for origin in [
            SessionOrigin::User,
            SessionOrigin::Cron,
            SessionOrigin::Briefing,
            SessionOrigin::Delegate,
        ] {
            assert_eq!(SessionOrigin::parse(origin.as_str()), origin);
        }
        // A sub-agent inherits its parent's attendance; only the sweeps are
        // unattended.
        assert!(!SessionOrigin::Delegate.is_unattended());
        assert!(SessionOrigin::Cron.is_unattended());
        assert!(SessionOrigin::Briefing.is_unattended());
        assert!(!SessionOrigin::User.is_unattended());
    }
}
