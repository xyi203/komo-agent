use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use time::format_description::well_known::Rfc3339;

use komo_core::domain::{
    context::ToolContext,
    message::Message,
    repository::{MessageRepository, SessionRepository},
    tool::{Tool, ToolError, ToolOutput, parse_args},
};
use komo_services::session_indexing::SessionSearch;

/// Hits returned per `search` call. Small on purpose: a hit names its index so
/// the model can `show` around whichever ones matter.
const SEARCH_LIMIT: usize = 8;

/// Longest snippet a search hit carries (chars). Enough to judge relevance,
/// short enough that eight hits stay far from the tool-result cap.
const SNIPPET_MAX: usize = 200;

/// Default and maximum messages per `show` page.
const SHOW_DEFAULT: usize = 10;
const SHOW_MAX: usize = 40;

#[derive(Deserialize)]
struct SessionArgs {
    action: String,
    /// `search` only: case-insensitive substring matched against message
    /// content and tool notes.
    #[serde(default)]
    query: String,
    /// `search`/`show`: another session's id. Defaults to this conversation.
    #[serde(default)]
    session: String,
    /// `show` only: 1-based index of the first message to return.
    #[serde(default)]
    offset: usize,
    /// `show` only: how many messages to return (default 10, max 40).
    #[serde(default)]
    limit: usize,
}

/// Introspection over Komo's own stored conversation sessions (this agent's
/// chat-history database, NOT system/tmux/login sessions).
///
/// Beyond counting and listing, `search`/`show` are the model's retrieval path
/// into the parts of a transcript the replay window no longer carries: only a
/// recent window of a long conversation is replayed each turn, and tool notes
/// age out of it even sooner — but the store keeps everything, so "which file
/// did we discuss last week" is answerable instead of gone.
pub struct SessionTool {
    sessions: Arc<dyn SessionRepository>,
    messages: Arc<dyn MessageRepository>,
    /// Hybrid (dense ∪ lexical) search over every transcript. `None` when no
    /// embedding backend is configured, which drops `search` back to the
    /// substring scan over one session — worse, but never silent.
    episodic: Option<Arc<SessionSearch>>,
}

impl SessionTool {
    pub fn new(sessions: Arc<dyn SessionRepository>, messages: Arc<dyn MessageRepository>) -> Self {
        Self {
            sessions,
            messages,
            episodic: None,
        }
    }

    /// Attach the episodic index, making `search` semantic and cross-session.
    pub fn with_episodic_search(mut self, search: Arc<SessionSearch>) -> Self {
        self.episodic = Some(search);
        self
    }

    /// Hybrid search across stored transcripts, with the index caught up first.
    ///
    /// Catch-up is best-effort and its failure is not this call's failure: an
    /// index that is one turn stale still answers the question that was asked,
    /// whereas refusing to search because indexing hiccupped answers nothing.
    async fn episodic_search(
        &self,
        episodic: &SessionSearch,
        query: &str,
        scope: &str,
    ) -> Result<ToolOutput, ToolError> {
        match self.sessions.list().await {
            Ok(sessions) => {
                if let Err(error) = episodic.refresh(&sessions).await {
                    tracing::warn!(%error, "session index catch-up failed (searching anyway)");
                }
            }
            Err(error) => {
                tracing::warn!(%error, "could not list sessions to index (searching anyway)")
            }
        }

        let scope = (!scope.is_empty()).then_some(scope);
        let hits = episodic
            .search(query, scope, SEARCH_LIMIT)
            .await
            .map_err(ToolError::Failed)?;
        let where_ = match scope {
            Some(s) => format!("in {s}"),
            None => "across all stored conversations".to_string(),
        };
        if hits.is_empty() {
            return Ok(ToolOutput::text(format!(
                "no matches for {query:?} {where_}. This searched meaning as well as \
                 wording, so a rephrasing is unlikely to help — say you checked."
            )));
        }
        let mut out = format!("{} match(es) for {query:?} {where_}:\n", hits.len());
        for hit in &hits {
            out.push_str(&format!(
                "\n[{} @ {}] #{}\n{}\n",
                hit.chunk.path,
                hit.chunk.heading_path,
                hit.chunk.ordinal,
                oneline(&hit.chunk.text, SNIPPET_MAX)
            ));
        }
        out.push_str(
            "\nEach hit names its session and the `show` offset of the turn it \
             came from — use action=\"show\" with that session and offset to read \
             it in full before relying on it.",
        );
        Ok(ToolOutput::text(out).with_title(format!("{} match(es)", hits.len())))
    }

    /// The transcript `args` targets: the named session, else this
    /// conversation. Erroring on "no session in context" (rather than
    /// defaulting to something) keeps a detached misuse loud.
    fn target_session(&self, args: &SessionArgs, ctx: &ToolContext) -> Result<String, ToolError> {
        if !args.session.trim().is_empty() {
            return Ok(args.session.trim().to_string());
        }
        Ok(ctx.session.session_id.clone())
    }
}

#[async_trait]
impl Tool for SessionTool {
    fn name(&self) -> &'static str {
        "session"
    }

    fn description(&self) -> &'static str {
        "Search and read Komo's own stored conversations (this agent's chat \
         history, NOT system/tmux/login sessions). Only a recent window of the \
         current conversation is replayed to you each turn — everything older, \
         and every other conversation, still exists here. \
         action=\"search\" finds past turns by meaning as well as wording, \
         across ALL stored conversations by default, and matches a question in \
         one language against a conversation held in another; \
         action=\"show\" reads stored messages verbatim by position; \
         action=\"count\"/\"list\" enumerate sessions. \
         Search whenever the user refers to earlier work, a past decision, or \
         something they told you that you cannot see — including in other \
         conversations. Search first, then `show` the turn to read it in full \
         rather than answering from a snippet. If a search finds nothing, say \
         you checked instead of guessing."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["count", "list", "search", "show"],
                    "description": "count/list = session inventory; search = find messages by keyword; show = read messages by position."
                },
                "query": {
                    "type": "string",
                    "description": "search: what to look for. Matched by meaning and by wording, over message text and tool-activity notes — write it as the question you are actually asking, not as a keyword."
                },
                "session": {
                    "type": "string",
                    "description": "search: a session id from action=list, to narrow to one conversation. Omit to search all of them, which is usually what you want. show: which conversation to read; defaults to this one."
                },
                "offset": {
                    "type": "integer",
                    "description": "show: 1-based index of the first message to return (search hits are labeled with these indexes)."
                },
                "limit": {
                    "type": "integer",
                    "description": "show: messages to return (default 10, max 40)."
                }
            },
            "required": ["action"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: SessionArgs = parse_args(&input)?;

        match args.action.as_str() {
            "count" => {
                let sessions = self.sessions.list().await?;
                Ok(
                    ToolOutput::text(format!("{} stored sessions", sessions.len()))
                        .with_structured(json!({ "count": sessions.len() })),
                )
            }
            "list" => {
                let sessions = self.sessions.list().await?;
                if sessions.is_empty() {
                    return Ok(ToolOutput::text("no stored sessions"));
                }
                let lines: Vec<String> = sessions
                    .iter()
                    .map(|s| {
                        format!(
                            "{} | created {} | {} messages ({} user turns)",
                            s.id,
                            rfc3339(s.created_at),
                            s.messages.len(),
                            s.user_turns()
                        )
                    })
                    .collect();
                Ok(ToolOutput::text(format!(
                    "{} sessions:\n{}",
                    sessions.len(),
                    lines.join("\n")
                ))
                .with_title(format!("{} sessions", sessions.len())))
            }
            "search" => {
                let query = args.query.trim();
                if query.is_empty() {
                    return Err(ToolError::InvalidInput(
                        "search needs a non-empty `query`".into(),
                    ));
                }
                // A named session narrows the search; unnamed searches every
                // stored conversation. That default is the point of the
                // episodic path — "when did we decide X" is a question about
                // *some* past session, and requiring its id up front is
                // requiring the answer as the input.
                let scope = args.session.trim();
                if let Some(episodic) = &self.episodic {
                    match self.episodic_search(episodic, query, scope).await {
                        Ok(out) => return Ok(out),
                        // Degrade, never fail: an embedding backend that is
                        // down must leave the model with the scan it had
                        // before, not with "no matches" — which reads as *the
                        // conversation never happened*.
                        Err(error) => tracing::warn!(
                            %error,
                            "episodic session search failed; falling back to the substring scan"
                        ),
                    }
                }
                let session = self.target_session(&args, ctx)?;
                let messages = self.messages.list_by_session(&session).await?;
                let total = messages.len();
                let needle = query.to_lowercase();
                let hits: Vec<String> = messages
                    .iter()
                    .enumerate()
                    .filter(|(_, m)| {
                        m.content.to_lowercase().contains(&needle)
                            || m.tool_note.to_lowercase().contains(&needle)
                    })
                    .map(|(idx, m)| hit_line(idx + 1, m, &needle))
                    .collect();
                if hits.is_empty() {
                    return Ok(ToolOutput::text(format!(
                        "no matches for {query:?} in {session} ({total} stored messages)"
                    )));
                }
                let shown = hits.len().min(SEARCH_LIMIT);
                let mut out = format!(
                    "{} match(es) for {query:?} in {session} ({total} stored messages, showing {shown}):\n",
                    hits.len()
                );
                out.push_str(&hits[hits.len() - shown..].join("\n"));
                out.push_str("\n\nUse action=\"show\" with an offset near a hit to read the surrounding messages.");
                Ok(ToolOutput::text(out).with_title(format!("{} match(es)", hits.len())))
            }
            "show" => {
                let session = self.target_session(&args, ctx)?;
                let messages = self.messages.list_by_session(&session).await?;
                let total = messages.len();
                if total == 0 {
                    return Ok(ToolOutput::text(format!(
                        "{session} has no stored messages"
                    )));
                }
                let limit = match args.limit {
                    0 => SHOW_DEFAULT,
                    n => n.min(SHOW_MAX),
                };
                // Default to the tail — "the latest N" is the natural page —
                // and clamp an out-of-range offset instead of erroring.
                let start = match args.offset {
                    0 => total.saturating_sub(limit),
                    n => (n - 1).min(total - 1),
                };
                let end = (start + limit).min(total);
                let body: Vec<String> = messages[start..end]
                    .iter()
                    .enumerate()
                    .map(|(i, m)| full_line(start + i + 1, m))
                    .collect();
                Ok(ToolOutput::text(format!(
                    "{session} messages {}-{} of {total}:\n{}",
                    start + 1,
                    end,
                    body.join("\n")
                ))
                .with_title(format!("messages {}-{}", start + 1, end)))
            }
            other => Err(ToolError::InvalidInput(format!(
                "unknown session action `{other}` (expected: count | list | search | show)"
            ))),
        }
    }
}

/// Flatten to one line and bound the length — a turn-chunk can be 2000 chars
/// and eight of them would crowd out the answer they are supposed to support.
fn oneline(text: &str, cap: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= cap {
        return flat;
    }
    flat.chars().take(cap).collect::<String>() + "…"
}

fn rfc3339(unix: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(unix)
        .ok()
        .and_then(|t| t.format(&Rfc3339).ok())
        .unwrap_or_else(|| unix.to_string())
}

fn role_tag(m: &Message) -> &'static str {
    match m.role {
        komo_core::domain::message::Role::User => "user",
        komo_core::domain::message::Role::Assistant => "assistant",
        komo_core::domain::message::Role::System => "system",
        komo_core::domain::message::Role::Tool => "tool",
    }
}

/// One search hit: index, role, time, and the snippet around the first match
/// (from content, else from the tool note, marked as such).
fn hit_line(idx: usize, m: &Message, needle: &str) -> String {
    let (source, text) = if m.content.to_lowercase().contains(needle) {
        ("", m.content.as_str())
    } else {
        ("[tool note] ", m.tool_note.as_str())
    };
    format!(
        "#{idx} [{} @ {}] {source}{}",
        role_tag(m),
        rfc3339(m.timestamp),
        snippet(text, needle)
    )
}

/// A window of `text` centered on the first occurrence of `needle`
/// (case-insensitive), flattened to one line and bounded by [`SNIPPET_MAX`].
fn snippet(text: &str, needle: &str) -> String {
    let lower = text.to_lowercase();
    let at = lower.find(needle).unwrap_or(0);
    // Aim the window so the match sits roughly a third in.
    let from = at.saturating_sub(SNIPPET_MAX / 3);
    let from = ceil_boundary(text, from);
    let taken: String = text[from..].chars().take(SNIPPET_MAX).collect();
    let mut s = String::new();
    if from > 0 {
        s.push('…');
    }
    s.push_str(&taken.replace('\n', " "));
    if from + taken.len() < text.len() {
        s.push('…');
    }
    s
}

/// Smallest char boundary ≥ `at`.
fn ceil_boundary(s: &str, mut at: usize) -> usize {
    while at < s.len() && !s.is_char_boundary(at) {
        at += 1;
    }
    at
}

/// One full message for `show`, tool note labeled separately from the content.
fn full_line(idx: usize, m: &Message) -> String {
    let mut line = format!(
        "#{idx} [{} @ {}]\n{}",
        role_tag(m),
        rfc3339(m.timestamp),
        m.content
    );
    if !m.tool_note.is_empty() {
        line.push_str("\n[tool note] ");
        line.push_str(&m.tool_note);
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::{ApprovalRequest, Approver, Decision};
    use komo_core::domain::context::SessionContext;
    use komo_core::domain::session::Session;

    struct FakeSessions(Vec<Session>);

    #[async_trait]
    impl SessionRepository for FakeSessions {
        async fn find_by_peer(
            &self,
            _channel: &komo_core::domain::session::ChannelPeer,
        ) -> anyhow::Result<Option<Session>> {
            Ok(None)
        }

        async fn find(&self, _id: &str) -> anyhow::Result<Option<Session>> {
            Ok(None)
        }
        async fn find_windowed(&self, _id: &str, _limit: usize) -> anyhow::Result<Option<Session>> {
            Ok(None)
        }
        async fn list(&self) -> anyhow::Result<Vec<Session>> {
            Ok(self.0.clone())
        }
        async fn save(&self, _session: &Session) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_empty_sessions(&self) -> anyhow::Result<usize> {
            Ok(0)
        }
        async fn rotate(&self, _id: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
    }

    /// Per-session transcripts, so tests can prove the tool reads the session
    /// it was asked about (current by default, another when named).
    struct FakeMessages(Vec<(String, Vec<Message>)>);

    #[async_trait]
    impl MessageRepository for FakeMessages {
        async fn list_by_session(&self, session_id: &str) -> anyhow::Result<Vec<Message>> {
            Ok(self
                .0
                .iter()
                .find(|(id, _)| id == session_id)
                .map(|(_, m)| m.clone())
                .unwrap_or_default())
        }
        async fn save(&self, _session_id: &str, _message: &Message) -> anyhow::Result<()> {
            Ok(())
        }
        async fn cancel_last_turn(&self, _session_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn record_interjection(&self, _session_id: &str, _text: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct DenyAll;
    #[async_trait]
    impl Approver for DenyAll {
        async fn decide(&self, _r: &ApprovalRequest) -> Decision {
            Decision::deny()
        }
    }

    /// A `ToolContext` bound to `session`, with a never-consulted approver
    /// (`session` is read-only and requests no approval).
    fn ctx(session: &str) -> ToolContext {
        ToolContext::new(SessionContext::detached(session), None, Arc::new(DenyAll))
    }

    fn transcript() -> Vec<Message> {
        vec![
            Message::user("we should deploy pikachu on friday"),
            Message::assistant("noted, friday it is").with_tool_note("[tools used] cron: add"),
            Message::user("actually make it monday"),
            Message::assistant("switched to monday"),
        ]
    }

    fn tool() -> SessionTool {
        SessionTool::new(
            Arc::new(FakeSessions(vec![])),
            Arc::new(FakeMessages(vec![
                ("chat:1".into(), transcript()),
                ("chat:2".into(), vec![Message::user("other session text")]),
            ])),
        )
    }

    #[tokio::test]
    async fn search_finds_content_and_tool_notes_in_the_current_session() {
        let out = tool()
            .call(
                json!({ "action": "search", "query": "pikachu" }),
                &ctx("chat:1"),
            )
            .await
            .unwrap();
        assert!(
            out.text.contains("#1"),
            "hit carries its index: {}",
            out.text
        );
        assert!(out.text.contains("pikachu"));

        // Tool notes are searchable too — that's what ages out of the replay
        // window fastest.
        let out = tool()
            .call(
                json!({ "action": "search", "query": "cron" }),
                &ctx("chat:1"),
            )
            .await
            .unwrap();
        assert!(out.text.contains("[tool note]"), "{}", out.text);
    }

    #[tokio::test]
    async fn search_scopes_to_the_named_session_when_given() {
        let out = tool()
            .call(
                json!({ "action": "search", "query": "other session", "session": "chat:2" }),
                &ctx("chat:1"),
            )
            .await
            .unwrap();
        assert!(out.text.contains("1 match(es)"), "{}", out.text);
    }

    #[tokio::test]
    async fn search_reports_no_matches_without_erroring() {
        let out = tool()
            .call(
                json!({ "action": "search", "query": "zzz-not-there" }),
                &ctx("chat:1"),
            )
            .await
            .unwrap();
        assert!(out.text.contains("no matches"), "{}", out.text);
    }

    #[tokio::test]
    async fn show_pages_verbatim_messages_with_tool_notes() {
        let out = tool()
            .call(
                json!({ "action": "show", "offset": 1, "limit": 2 }),
                &ctx("chat:1"),
            )
            .await
            .unwrap();
        assert!(out.text.contains("messages 1-2 of 4"), "{}", out.text);
        assert!(out.text.contains("deploy pikachu"));
        assert!(out.text.contains("[tool note] [tools used] cron: add"));
        assert!(!out.text.contains("monday"), "page bound respected");
    }

    #[tokio::test]
    async fn show_defaults_to_the_tail() {
        let out = tool()
            .call(json!({ "action": "show", "limit": 2 }), &ctx("chat:1"))
            .await
            .unwrap();
        assert!(out.text.contains("messages 3-4 of 4"), "{}", out.text);
        assert!(out.text.contains("switched to monday"));
    }

    #[tokio::test]
    async fn snippet_flattens_and_bounds_long_matches() {
        let long = format!("{}pikachu{}", "前".repeat(300), "后\n换行".repeat(100));
        let s = snippet(&long, "pikachu");
        assert!(s.contains("pikachu"));
        assert!(
            s.chars().count() <= SNIPPET_MAX + 2,
            "bounded plus ellipses"
        );
        assert!(!s.contains('\n'), "one line");
    }

    #[tokio::test]
    async fn count_and_list_still_work() {
        let sessions = vec![Session::new("chat:1")];
        let tool = SessionTool::new(
            Arc::new(FakeSessions(sessions)),
            Arc::new(FakeMessages(vec![])),
        );
        let out = tool
            .call(json!({ "action": "count" }), &ctx("chat:1"))
            .await
            .unwrap();
        assert!(out.text.contains("1 stored sessions"));
        let out = tool
            .call(json!({ "action": "list" }), &ctx("chat:1"))
            .await
            .unwrap();
        assert!(out.text.contains("chat:1"));
    }

    #[tokio::test]
    async fn search_requires_a_query() {
        let err = tool()
            .call(json!({ "action": "search" }), &ctx("chat:1"))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)));
    }

    // ── episodic search ────────────────────────────────────────────────────

    /// An index that answers with canned hits, or refuses to.
    struct FakeIndex {
        hits: Vec<komo_core::domain::chunk_index::ChunkHit>,
        fail_search: bool,
    }

    #[async_trait]
    impl komo_core::domain::chunk_index::ChunkIndex for FakeIndex {
        async fn upsert(
            &self,
            _chunks: &[komo_core::domain::chunk_index::IndexedChunk],
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn search(
            &self,
            _q: &[f32],
            _text: &str,
            _limit: usize,
            _min: f32,
        ) -> anyhow::Result<Vec<komo_core::domain::chunk_index::ChunkHit>> {
            if self.fail_search {
                anyhow::bail!("index unavailable");
            }
            Ok(self.hits.clone())
        }
        async fn indexed(
            &self,
        ) -> anyhow::Result<
            std::collections::HashMap<String, komo_core::domain::chunk_index::IndexedFile>,
        > {
            Ok(Default::default())
        }
        async fn delete_paths(&self, _paths: &[String]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn count(&self) -> anyhow::Result<usize> {
            Ok(0)
        }
        async fn reset(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>> {
            Ok(None)
        }
    }

    struct FakeEmbedder(bool);

    #[async_trait]
    impl komo_core::domain::embedding::EmbeddingClient for FakeEmbedder {
        async fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            if self.0 {
                anyhow::bail!("embedding backend down");
            }
            Ok(texts.iter().map(|_| vec![1.0, 0.0]).collect())
        }
        fn model_id(&self) -> &str {
            "fake"
        }
    }

    fn chunk_hit(
        path: &str,
        ordinal: usize,
        text: &str,
    ) -> komo_core::domain::chunk_index::ChunkHit {
        komo_core::domain::chunk_index::ChunkHit {
            chunk: komo_core::domain::chunk_index::IndexedChunk {
                id: komo_core::domain::chunk_index::IndexedChunk::make_id(path, ordinal),
                path: path.into(),
                heading_path: "2026-08-01".into(),
                ordinal,
                text: text.into(),
                mtime: 0,
                embedding: Vec::new(),
                embedding_model: String::new(),
            },
            score: 1.0,
        }
    }

    fn episodic_tool(
        hits: Vec<komo_core::domain::chunk_index::ChunkHit>,
        fail_search: bool,
        fail_embed: bool,
    ) -> SessionTool {
        let search = komo_services::session_indexing::SessionSearch::new(
            Arc::new(FakeIndex { hits, fail_search }),
            Arc::new(FakeEmbedder(fail_embed)),
        );
        tool().with_episodic_search(Arc::new(search))
    }

    /// The point of the episodic path: a question about "some past
    /// conversation" is answerable without already knowing which one.
    #[tokio::test]
    async fn episodic_search_spans_every_session_by_default() {
        let out = episodic_tool(
            vec![
                chunk_hit("feishu:oc_9", 3, "user: 要不要用 rig?\nassistant: 不建议"),
                chunk_hit("cli:old", 7, "user: rig 的问题\nassistant: 侵入 runtime"),
            ],
            false,
            false,
        )
        .call(
            json!({ "action": "search", "query": "为什么不用 rig" }),
            &ctx("chat:1"),
        )
        .await
        .unwrap()
        .text;

        assert!(out.contains("across all stored conversations"), "{out}");
        assert!(out.contains("feishu:oc_9"), "{out}");
        assert!(out.contains("cli:old"), "{out}");
        // A hit has to carry the coordinates `show` takes, or reading it in
        // full means guessing.
        assert!(out.contains("#3"), "{out}");
        assert!(
            out.contains("2026-08-01"),
            "the date locates the turn: {out}"
        );
    }

    #[tokio::test]
    async fn a_named_session_narrows_the_episodic_search() {
        let out = episodic_tool(vec![chunk_hit("cli:old", 7, "user: rig")], false, false)
            .call(
                json!({ "action": "search", "query": "rig", "session": "cli:old" }),
                &ctx("chat:1"),
            )
            .await
            .unwrap()
            .text;
        assert!(out.contains("in cli:old"), "{out}");
    }

    /// Degrading is the contract: an embedding backend that is down must leave
    /// the model with the substring scan, not with "no matches" — which reads
    /// as *the conversation never happened*.
    #[tokio::test]
    async fn a_failing_index_falls_back_to_the_substring_scan() {
        for (fail_search, fail_embed) in [(true, false), (false, true)] {
            let out = episodic_tool(Vec::new(), fail_search, fail_embed)
                .call(
                    json!({ "action": "search", "query": "pikachu" }),
                    &ctx("chat:1"),
                )
                .await
                .unwrap()
                .text;
            assert!(
                out.contains("we should deploy pikachu"),
                "expected the lexical scan to answer; got: {out}"
            );
        }
    }

    /// An honest empty answer, and one that does not invite a reword — the
    /// search already covered meaning, so rephrasing is not the missing step.
    #[tokio::test]
    async fn an_empty_episodic_result_says_it_checked() {
        let out = episodic_tool(Vec::new(), false, false)
            .call(
                json!({ "action": "search", "query": "something never discussed" }),
                &ctx("chat:1"),
            )
            .await
            .unwrap()
            .text;
        assert!(out.contains("no matches"), "{out}");
        assert!(out.contains("say you checked"), "{out}");
    }
}
