//! Replacing a conversation's oldest exchanges with a summary of them
//! (docs/turn-durability.md §4 第三批 3.2).
//!
//! A long-lived session already stopped sending its whole history: the window
//! (`max_history_messages`) replays the newest N messages and the rest are
//! simply not there. That is a silent loss — the model does not know what it
//! stopped seeing, so a question about something decided fifty messages ago is
//! answered from nothing.
//!
//! Compaction turns that loss into a summary. The oldest surface nodes are
//! replaced by one `user/message` that stands where they did, so the model sees
//! *what happened* instead of a gap — and, because a replacement is an append
//! like any other event, **nothing is rewritten**: the shadowed events stay in
//! the log, which is what lets a human transcript still show what the summary
//! covered.
//!
//! Three rules make this safe rather than clever:
//!
//! - **The cut lands where the surface still alternates.** A summary is a user
//!   message, so the node after the range it replaces has to be an assistant
//!   message — two consecutive user messages is exactly what several providers
//!   reject on replay.
//! - **The replacement is validated against the surface it will land on**,
//!   immediately before the append. A replacement citing a node that is no
//!   longer on the surface is not a lost summary: the fold rejects it, and the
//!   session stops being readable at all.
//! - **Every failure means no compaction.** A model error, a timeout, a
//!   surface that will not take the replacement — the window keeps trimming as
//!   it did before, which is the behaviour this improves on, not one it
//!   depends on.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use komo_core::domain::{
    context::SessionOrigin,
    llm::LlmClient,
    message::Message,
    repository::SessionEventRepository,
    session::Session,
    session_event::{
        MessageSource, SessionEventKind, SurfaceContent, SurfacePlacement, SurfaceProjection,
        SurfaceRole, UserMessageEvent,
    },
};

/// How long the summariser may take. It runs after the turn's answer is
/// composed, but still inside the turn's own session slot, so an aux model that
/// has stopped answering must not hold the conversation open indefinitely.
const SUMMARY_TIMEOUT: Duration = Duration::from_secs(60);

/// Newest nodes kept verbatim, as a fraction of the window: half. Summarising
/// everything but the last exchange would make every follow-up question read a
/// paraphrase of what was just said.
const KEEP_DIVISOR: usize = 2;

/// Fewest nodes kept verbatim, whatever the window says. Below an exchange and
/// its answer there is nothing for the next turn to follow on from.
const MIN_KEEP: usize = 4;

/// Longest excerpt of one message shown to the summariser. It is summarising a
/// conversation, not re-reading a pasted file.
const MESSAGE_EXCERPT: usize = 1_500;

/// Longest excerpt handed over in total.
const TOTAL_EXCERPT: usize = 24_000;

/// Longest summary kept. A summary that grows with the conversation defeats the
/// point of replacing it.
const SUMMARY_CAP: usize = 4_000;

const PROMPT: &str = "\
You are compacting the earlier part of a conversation so it can be carried \
forward in less space. What you write replaces those messages: from now on it \
is all the assistant will remember about them.

Write a summary that preserves, in this order of priority:
  1. Decisions made and their reasons — especially ones later work depends on.
  2. Facts about the user, their systems, and their preferences that came up.
  3. Work in progress: what was being done, where it got to, what is left.
  4. Anything the user asked to be remembered or corrected.

Leave out pleasantries, restated questions, and anything already superseded \
later in the excerpt. Do not invent detail that is not there, and do not \
address the user — this is a note to the assistant's future self.

Write it in the language the conversation is in. Prose or short bullets, \
whichever fits; no preamble, no heading, just the summary.";

/// The compactor. Wired on the runtimes that hold long conversations; a runtime
/// without one keeps the plain window.
pub struct Compactor {
    aux: Arc<dyn LlmClient>,
    events: Arc<dyn SessionEventRepository>,
    /// The history window (`max_history_messages`), which is the whole trigger:
    /// a surface longer than this is a surface whose oldest nodes the model has
    /// already stopped seeing. `0` (no window) means nothing is being lost, so
    /// nothing is compacted.
    window: usize,
}

impl Compactor {
    pub fn new(
        aux: Arc<dyn LlmClient>,
        events: Arc<dyn SessionEventRepository>,
        window: usize,
    ) -> Self {
        Self {
            aux,
            events,
            window,
        }
    }

    /// Compact this session if its surface has outgrown the window. Answers
    /// whether a summary was written.
    ///
    /// Called at the end of a turn, from inside that turn's session slot: the
    /// replacement is computed from the surface and appended to it, and no
    /// other turn on this session may be doing the same thing at the same time.
    pub async fn compact_if_long(&self, session_id: &str, turn_id: &str) -> bool {
        let Some(plan) = self.plan(session_id).await else {
            return false;
        };

        // The attempt is durable before the model is asked: a crash in the
        // minute that follows should read as "a compaction was tried here", not
        // as a turn that inexplicably paused.
        self.record(
            session_id,
            vec![SessionEventKind::CompactionStarted {
                turn_id: turn_id.to_string(),
            }],
        )
        .await;

        let Some(summary) = self.summarize(session_id, &plan.excerpt).await else {
            return false;
        };

        // Re-planned against the surface as it is *now*: the model call took
        // time, and a replacement citing a node that has since left the surface
        // is rejected by the fold — which would leave the session unreadable
        // rather than uncompacted.
        let Some(plan) = self.plan(session_id).await else {
            return false;
        };
        if !plan.applies() {
            warn!(
                session_id,
                "the surface moved under a compaction; skipping it"
            );
            return false;
        }

        let shadowed = plan.shadowed.len();
        self.record(
            session_id,
            vec![
                SessionEventKind::UserMessage(UserMessageEvent {
                    turn_id: turn_id.to_string(),
                    content: summary,
                    source: MessageSource::Compaction,
                    surface: plan.placement(),
                }),
                SessionEventKind::CompactionCompleted {
                    turn_id: turn_id.to_string(),
                },
            ],
        )
        .await;
        info!(
            session_id,
            shadowed, "compacted the conversation's older messages into a summary"
        );
        true
    }

    /// What this session's next compaction would replace, or `None` when there
    /// is nothing worth replacing.
    async fn plan(&self, session_id: &str) -> Option<Plan> {
        let projection = match self.events.surface(session_id).await {
            Ok(Some(projection)) => projection,
            Ok(None) => return None,
            Err(error) => {
                warn!(%error, session_id, "could not read the surface to compact it");
                return None;
            }
        };
        Plan::from(&projection, self.window)
    }

    /// Ask the aux model for the summary, capped. `None` on any failure — the
    /// conversation is then compacted on some later turn, or not at all.
    async fn summarize(&self, session_id: &str, excerpt: &str) -> Option<String> {
        // Empty model/effort, like every other aux path: the conversation's own
        // model choice must not leak onto the aux model.
        let session = Session {
            id: format!("compaction-{session_id}"),
            workspace: String::new(),
            messages: vec![Message::user(format!(
                "{PROMPT}\n\nThe conversation so far:\n\n{excerpt}"
            ))],
            created_at: 0,
            title: String::new(),
            status: String::new(),
            model: String::new(),
            effort: String::new(),
            channel: None,
            origin: SessionOrigin::User,
            awaiting: None,
        };
        let answer = match tokio::time::timeout(SUMMARY_TIMEOUT, self.aux.complete(&session)).await
        {
            Ok(Ok(answer)) => answer,
            Ok(Err(error)) => {
                warn!(%error, session_id, "summarising for compaction failed; leaving the history as it is");
                return None;
            }
            Err(_) => {
                warn!(session_id, "summarising for compaction timed out");
                return None;
            }
        };
        let answer = answer.trim();
        if answer.is_empty() {
            warn!(session_id, "the summariser returned nothing");
            return None;
        }
        Some(cap(answer, SUMMARY_CAP))
    }

    /// Append, then make durable. A compaction that is only buffered would read
    /// after a crash as messages that were never summarised — harmless, but the
    /// summary cost a model call, so it is worth the fsync.
    async fn record(&self, session_id: &str, kinds: Vec<SessionEventKind>) {
        if let Err(error) = self.events.append(session_id, kinds).await {
            warn!(%error, session_id, "failed to record a compaction (non-fatal)");
            return;
        }
        if let Err(error) = self.events.durable_flush(session_id).await {
            warn!(%error, session_id, "a compaction is not durable yet (non-fatal)");
        }
    }
}

/// One compaction, worked out against a surface: which nodes it shadows, and
/// what the summariser is shown.
struct Plan {
    /// The surface this was planned against — kept so the replacement can be
    /// validated against it before it is written.
    surface: komo_core::domain::session_event::Surface,
    shadowed: Vec<u64>,
    excerpt: String,
}

impl Plan {
    /// Plan a compaction for a surface longer than `window`, or `None`.
    ///
    /// The cut is the newest node such that everything after it starts with an
    /// assistant message and at least `keep` nodes survive: a summary is a user
    /// message, so what follows it must be the assistant's side.
    fn from(projection: &SurfaceProjection, window: usize) -> Option<Self> {
        // `0` is "replay the whole transcript": nothing falls out of the
        // window, so there is nothing a summary would rescue.
        if window == 0 {
            return None;
        }
        // Only what the model is actually replayed: a `conversation/boundary`
        // already keeps the older stretch out of the window, and a summary that
        // reached across one would put it straight back.
        let nodes = projection.replayed();
        if nodes.len() <= window {
            return None;
        }
        let content: HashMap<u64, &SurfaceContent> = projection
            .content
            .iter()
            .map(|(seq, node)| (*seq, node))
            .collect();

        let keep = (window / KEEP_DIVISOR).max(MIN_KEEP);
        let target = nodes.len().checked_sub(keep)?;
        // Walk back from the target to the nearest cut the surface can take.
        // Every node after the cut is kept, so the first of them decides.
        let end = (0..target).rev().find(|i| {
            matches!(
                content.get(&nodes[i + 1]).map(|node| node.role),
                Some(SurfaceRole::Assistant)
            )
        })?;
        // A summary that replaces one exchange buys nothing and costs a model
        // call.
        if end + 1 < MIN_KEEP {
            return None;
        }
        // The summary has to survive the window it was written for. It stands
        // at the front of the surface, and the window keeps the *last* N
        // messages, so a surface that is still longer than the window
        // afterwards would drop the summary along with everything it covers —
        // the model would be no better off and the model call would be spent.
        // (`nodes.len() - end` is the summary plus everything kept after it.)
        if nodes.len() - end > window {
            return None;
        }

        let shadowed = nodes[..=end].to_vec();
        let excerpt = render(&shadowed, &content);
        Some(Self {
            surface: projection.surface.clone(),
            shadowed,
            excerpt,
        })
    }

    fn placement(&self) -> SurfacePlacement {
        SurfacePlacement::replace(
            self.shadowed[0],
            self.shadowed[self.shadowed.len() - 1],
            self.shadowed.clone(),
        )
    }

    /// Whether the surface will actually take this replacement.
    ///
    /// The fold is the authority and it fails closed: a replacement it rejects
    /// does not lose a summary, it makes every later read of the session an
    /// error. So the same check runs here first, on a copy.
    fn applies(&self) -> bool {
        let mut surface = self.surface.clone();
        // The seq is only what the node becomes; validation is about the range.
        surface.apply(u64::MAX, &self.placement()).is_ok()
    }
}

/// The shadowed nodes as a transcript for the summariser.
fn render(shadowed: &[u64], content: &HashMap<u64, &SurfaceContent>) -> String {
    let mut out = String::new();
    for seq in shadowed {
        let Some(node) = content.get(seq) else {
            continue;
        };
        let who = match node.role {
            SurfaceRole::Assistant => "assistant",
            _ => "user",
        };
        let line = format!("{who}: {}\n", cap(&node.text, MESSAGE_EXCERPT));
        if out.len() + line.len() > TOTAL_EXCERPT {
            // Oldest first, so what falls off the end here is the most recent —
            // which is also the part the kept messages still show verbatim.
            break;
        }
        out.push_str(&line);
        // What the turn *did* is part of what happened, and the note is already
        // capped where the log wrote it.
        if !node.tool_note.is_empty() && out.len() + node.tool_note.len() <= TOTAL_EXCERPT {
            out.push_str(&node.tool_note);
            out.push('\n');
        }
    }
    out
}

/// Truncate on a char boundary, marking the cut.
fn cap(s: &str, limit: usize) -> String {
    if s.chars().count() <= limit {
        return s.to_string();
    }
    let mut out: String = s.chars().take(limit).collect();
    out.push_str(" …");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::session_event::{
        AssistantMessageEvent, SessionEvent, SurfaceProjection,
    };
    use std::sync::Mutex;
    use time::OffsetDateTime;

    fn at(secs: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(secs).unwrap()
    }

    fn said(seq: u64, text: &str) -> SessionEvent {
        SessionEvent::new(
            seq,
            at(100),
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: format!("t{seq}"),
                content: text.into(),
                source: MessageSource::User,
                surface: SurfacePlacement::append(),
            }),
        )
    }

    fn replied(seq: u64, text: &str) -> SessionEvent {
        SessionEvent::new(
            seq,
            at(100),
            SessionEventKind::AssistantMessage(AssistantMessageEvent {
                turn_id: format!("t{}", seq - 1),
                content: text.into(),
                tool_note: String::new(),
                surface: SurfacePlacement::append(),
            }),
        )
    }

    /// `pairs` exchanges, as the surface holds them.
    fn conversation(pairs: u64) -> SurfaceProjection {
        let mut events = Vec::new();
        for i in 0..pairs {
            events.push(said(i * 2, &format!("question {i}")));
            events.push(replied(i * 2 + 1, &format!("answer {i}")));
        }
        SurfaceProjection::fold(&events, 0).unwrap()
    }

    #[test]
    fn a_conversation_inside_the_window_is_not_compacted() {
        // Nothing is being lost yet, so a summary would only cost a model call.
        assert!(Plan::from(&conversation(5), 10).is_none());
        assert!(Plan::from(&conversation(3), 10).is_none());
    }

    #[test]
    fn the_window_never_compacts_when_it_is_disabled() {
        // `0` = replay the whole transcript; nothing falls out of it.
        assert!(Plan::from(&conversation(50), 0).is_none());
    }

    #[test]
    fn the_cut_leaves_the_surface_alternating() {
        // The summary is a user message, so an assistant message has to follow
        // it — several providers reject two user messages in a row.
        let projection = conversation(10); // 20 nodes
        let plan = Plan::from(&projection, 10).expect("a surface twice the window compacts");
        assert!(plan.applies());

        let nodes = projection.surface.nodes();
        let first_kept = nodes[plan.shadowed.len()];
        let content: HashMap<u64, &SurfaceContent> = projection
            .content
            .iter()
            .map(|(seq, node)| (*seq, node))
            .collect();
        assert_eq!(
            content[&first_kept].role,
            SurfaceRole::Assistant,
            "the node the summary is followed by"
        );
        assert!(
            nodes.len() - plan.shadowed.len() >= MIN_KEEP,
            "the newest exchanges stay verbatim"
        );
    }

    #[test]
    fn the_summary_is_shown_what_it_replaces() {
        let projection = conversation(10);
        let plan = Plan::from(&projection, 10).unwrap();
        assert!(plan.excerpt.contains("user: question 0"));
        assert!(plan.excerpt.contains("assistant: answer 0"));
        assert!(
            !plan.excerpt.contains("question 9"),
            "the newest exchange is kept verbatim, not summarised"
        );
    }

    /// The second compaction works on a surface whose oldest node *is* the
    /// first summary — it has to be able to fold that in and replace it too.
    /// The summary stands at the front of the surface and the window keeps the
    /// last N messages, so a compaction that left the surface longer than the
    /// window would drop the summary too — a model call spent on nothing.
    #[test]
    fn a_compaction_that_would_not_fit_the_window_is_not_worth_making() {
        let projection = conversation(10); // 20 nodes
        // A window this small cannot hold `MIN_KEEP` verbatim nodes *and* the
        // summary, so there is no cut worth making.
        assert!(Plan::from(&projection, 4).is_none());

        let plan = Plan::from(&projection, 6).expect("six leaves room for both");
        let kept = projection.surface.nodes().len() - plan.shadowed.len();
        assert!(
            kept + 1 <= 6,
            "the summary plus what stays verbatim fits the window"
        );
    }

    #[test]
    fn a_second_compaction_replaces_the_first_summary_along_with_the_rest() {
        let mut events = Vec::new();
        for i in 0..10 {
            events.push(said(i * 2, &format!("question {i}")));
            events.push(replied(i * 2 + 1, &format!("answer {i}")));
        }
        let projection = SurfaceProjection::fold(&events, 0).unwrap();
        let first = Plan::from(&projection, 10).unwrap();
        assert!(first.applies());

        // The summary lands, and the conversation carries on.
        let mut events = events;
        events.push(SessionEvent::new(
            20,
            at(200),
            SessionEventKind::UserMessage(UserMessageEvent {
                turn_id: "compaction".into(),
                content: "earlier: they asked ten questions".into(),
                source: MessageSource::Compaction,
                surface: first.placement(),
            }),
        ));
        for i in 0..6 {
            events.push(said(21 + i * 2, &format!("later question {i}")));
            events.push(replied(22 + i * 2, &format!("later answer {i}")));
        }

        let projection = SurfaceProjection::fold(&events, 0).unwrap();
        let second =
            Plan::from(&projection, 10).expect("the surface has outgrown the window again");
        assert!(second.applies(), "and the replacement still validates");
        assert_eq!(
            second.shadowed[0], 20,
            "the previous summary is the oldest thing left to replace"
        );
        assert!(
            second.excerpt.contains("earlier: they asked ten questions"),
            "so what it said is carried into the new summary"
        );
    }

    /// An answer arrives, then the surface moves before the replacement is
    /// written. The fold would refuse it and the session would stop being
    /// readable, so the write has to refuse first.
    #[test]
    fn a_replacement_the_surface_will_not_take_is_not_written() {
        let projection = conversation(10);
        let mut plan = Plan::from(&projection, 10).unwrap();
        assert!(plan.applies());
        plan.shadowed.push(9_999);
        assert!(
            !plan.applies(),
            "citing a node the surface does not hold is refused before the log sees it"
        );
    }

    struct FixedLlm(&'static str);

    #[async_trait::async_trait]
    impl LlmClient for FixedLlm {
        async fn complete(&self, _session: &Session) -> anyhow::Result<String> {
            Ok(self.0.to_string())
        }
    }

    /// Whatever else fails, the conversation stays readable and the turn stays
    /// finished: a compaction that cannot happen is a compaction that did not.
    #[tokio::test]
    async fn a_summariser_that_answers_nothing_writes_no_replacement() {
        struct Recording(Mutex<Vec<SessionEventKind>>, SurfaceProjection);

        #[async_trait::async_trait]
        impl SessionEventRepository for Recording {
            async fn session_ids(&self) -> anyhow::Result<Vec<String>> {
                Ok(Vec::new())
            }

            async fn append(
                &self,
                _session_id: &str,
                kinds: Vec<SessionEventKind>,
            ) -> anyhow::Result<Vec<SessionEvent>> {
                self.0.lock().unwrap().extend(kinds);
                Ok(Vec::new())
            }
            async fn durable_flush(&self, _session_id: &str) -> anyhow::Result<()> {
                Ok(())
            }
            async fn events(&self, _session_id: &str) -> anyhow::Result<Vec<SessionEvent>> {
                Ok(Vec::new())
            }
            async fn events_from(
                &self,
                _session_id: &str,
                _seq: u64,
            ) -> anyhow::Result<Vec<SessionEvent>> {
                Ok(Vec::new())
            }
            async fn surface(
                &self,
                _session_id: &str,
            ) -> anyhow::Result<Option<SurfaceProjection>> {
                Ok(Some(self.1.clone()))
            }
            async fn turn_boundary(&self, _session_id: &str) -> anyhow::Result<bool> {
                Ok(false)
            }
            async fn retain(
                &self,
                _session_id: &str,
                _keep_from: u64,
            ) -> anyhow::Result<Option<u64>> {
                Ok(None)
            }
        }

        let events = Arc::new(Recording(Mutex::new(Vec::new()), conversation(10)));
        let compactor = Compactor::new(Arc::new(FixedLlm("   ")), events.clone(), 10);

        assert!(!compactor.compact_if_long("s1", "t1").await);
        let written = events.0.lock().unwrap();
        assert!(
            written
                .iter()
                .all(|kind| matches!(kind, SessionEventKind::CompactionStarted { .. })),
            "the attempt is on the record; the replacement is not"
        );
    }
}
