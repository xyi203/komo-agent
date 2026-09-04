//! What an external event matches (docs/bot-runtime.md §3.7, §5.12–5.14).
//!
//! Pure functions over an [`EventFilter`] or a [`Trigger`] and the thing that
//! arrived, separated from everything that has to happen afterwards — reading
//! standing registrations, claiming one, waking a turn, opening a routine's
//! turn — because "is this about that?" is a question with a definite answer
//! and no I/O, and it is asked by three features: a kanban Task waiting for a
//! reply, a turn parked on `wait { for_event }`, and a routine whose trigger is
//! an event rather than a clock.
//!
//! Matching is on the **address**, never on a name someone typed. `waiting_on:
//! "张三"` is for a human to read; a [`ChannelPeer`] is what an inbound message
//! can actually be compared with, and a task whose `waiting_on` never resolved
//! to one is simply not wakeable.
//!
//! [`Trigger`]: super::cron::Trigger

use std::path::PathBuf;

use super::session::ChannelPeer;
use super::session_event::EventFilter;
use super::wakeup::WakeupRegistration;

/// How much of an event's content its one-line account keeps. The record says
/// *why* something ran; it is not where the payload lives.
pub const EVENT_SUMMARY_CAP: usize = 200;

/// How much of it the triggered turn is handed. Bounded because the content is
/// **external**: a webhook body and a group message are written by whoever
/// wanted the routine to run, and an unbounded one would be an unbounded prompt.
pub const EVENT_DETAIL_CAP: usize = 2_000;

/// How many changed paths one turn is shown before the rest become a count.
const EVENT_DETAIL_PATHS: usize = 20;

/// One thing that arrived, in the terms a filter is written in.
pub enum InboundEvent<'a> {
    /// A chat message, on whichever channel carried it.
    Message {
        peer: &'a ChannelPeer,
        text: &'a str,
    },
    /// A named inbound webhook (`POST /api/hooks/{name}`).
    Webhook { name: &'a str },
}

/// Whether this filter is about the thing that just arrived.
pub fn matches(filter: &EventFilter, event: &InboundEvent<'_>) -> bool {
    match (filter, event) {
        (EventFilter::FromPeer { platform, peer_id }, InboundEvent::Message { peer, .. }) => {
            platform == &peer.platform && peer_id == &peer.peer_id
        }
        (EventFilter::Webhook { name }, InboundEvent::Webhook { name: arrived }) => name == arrived,
        _ => false,
    }
}

/// Something that happened outside komo, in the terms a routine [`Trigger`] is
/// written in (docs/bot-runtime.md §5.12–5.14).
///
/// The three event ingresses — an HTTP hook, a feishu message or reaction, a
/// file that changed — differ in how they arrive and in nothing else: each
/// names routines to run and carries one line saying what happened. One type is
/// what lets the three reach a single `on_event`.
///
/// [`Trigger`]: super::cron::Trigger
#[derive(Debug, Clone)]
pub enum ExternalEvent {
    Webhook {
        name: String,
        body: String,
    },
    Feishu(FeishuEvent),
    /// One debounce window's worth of changed paths — a batch, not a path,
    /// because saving fifty files is one thing happening (§5.14).
    FileChanged {
        paths: Vec<PathBuf>,
    },
}

/// A feishu message or reaction, reduced to what a
/// [`FeishuMatch`](super::cron::FeishuMatch) reads.
///
/// The sender rides along for the record only: **who** set a routine off never
/// decides what it may do — the turn runs on the routine's own grants
/// (docs/bot-runtime.md §8, criterion 6).
#[derive(Debug, Clone, Default)]
pub struct FeishuEvent {
    pub chat: String,
    pub sender: String,
    pub text: String,
    /// The message @s the bot itself.
    pub mention: bool,
    /// Set when this is a reaction rather than a message.
    pub reaction: Option<String>,
}

impl ExternalEvent {
    /// What a standing [`Wakeup::Event`](super::session_event::Wakeup) can be
    /// matched against, when anything can.
    ///
    /// A feishu message answers `None` on purpose: peer waits are fired by the
    /// chat ingress every channel shares, and firing them here too would wake
    /// one commitment twice. A file changing matches no filter shape at all.
    pub fn as_inbound(&self) -> Option<InboundEvent<'_>> {
        match self {
            Self::Webhook { name, .. } => Some(InboundEvent::Webhook { name }),
            Self::Feishu(_) | Self::FileChanged { .. } => None,
        }
    }

    /// One line saying what happened — the content half of a
    /// [`RoutineRun`](super::cron::RoutineRun)'s `event`, and what a woken turn
    /// is handed.
    pub fn summary(&self) -> String {
        match self {
            Self::Webhook { name, body } => match head(body).as_str() {
                "" => format!("webhook `{name}`（空 body）"),
                head => format!("webhook `{name}`: {head}"),
            },
            Self::Feishu(event) => match &event.reaction {
                Some(emoji) => format!("{} 回应了 {emoji}", sender_of(event)),
                None => format!("{}: {}", sender_of(event), head(&event.text)),
            },
            Self::FileChanged { paths } => match paths.split_first() {
                None => "文件变更（无路径）".to_string(),
                Some((first, [])) => format!("文件变更：{}", first.display()),
                Some((first, rest)) => {
                    format!("{} 个文件变更，首个 {}", rest.len() + 1, first.display())
                }
            },
        }
    }
}

impl ExternalEvent {
    /// What the triggered turn is shown: the same event, at length, bounded by
    /// [`EVENT_DETAIL_CAP`]. The caller states the trust boundary around it —
    /// this is content, never instruction.
    pub fn detail(&self) -> String {
        match self {
            Self::Webhook { name, body } => {
                format!("webhook `{name}`\n{}", capped(body))
            }
            Self::Feishu(event) => match &event.reaction {
                Some(emoji) => format!("飞书 {}：{} 回应了 {emoji}", event.chat, sender_of(event)),
                None => format!(
                    "飞书 {}，{}：\n{}",
                    event.chat,
                    sender_of(event),
                    capped(&event.text)
                ),
            },
            Self::FileChanged { paths } => {
                let shown: Vec<String> = paths
                    .iter()
                    .take(EVENT_DETAIL_PATHS)
                    .map(|p| p.display().to_string())
                    .collect();
                let rest = paths.len().saturating_sub(shown.len());
                let mut text = shown.join("\n");
                if rest > 0 {
                    text.push_str(&format!("\n…（共 {} 个）", paths.len()));
                }
                text
            }
        }
    }
}

fn capped(text: &str) -> String {
    let text = text.trim();
    if text.chars().count() <= EVENT_DETAIL_CAP {
        return text.to_string();
    }
    format!(
        "{}\n…（已截断）",
        text.chars().take(EVENT_DETAIL_CAP).collect::<String>()
    )
}

fn sender_of(event: &FeishuEvent) -> &str {
    match event.sender.trim().is_empty() {
        true => "群成员",
        false => event.sender.trim(),
    }
}

/// First line, capped — an event's account has to stay one line in a listing.
fn head(text: &str) -> String {
    let line = text.trim().lines().next().unwrap_or_default().trim();
    if line.chars().count() <= EVENT_SUMMARY_CAP {
        return line.to_string();
    }
    format!(
        "{}…",
        line.chars().take(EVENT_SUMMARY_CAP).collect::<String>()
    )
}

/// Every standing registration this message fires, oldest first.
///
/// All of them, not the first: two commitments waiting on the same person are
/// two standing instructions, and one arriving message answers both.
pub fn matching<'a>(
    registrations: &'a [WakeupRegistration],
    event: &InboundEvent<'_>,
) -> Vec<&'a WakeupRegistration> {
    registrations
        .iter()
        .filter(|r| match &r.wakeup {
            super::session_event::Wakeup::Event { filter } => matches(filter, event),
            _ => false,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::session_event::Wakeup;

    fn from_peer(platform: &str, peer_id: &str) -> Wakeup {
        Wakeup::Event {
            filter: EventFilter::FromPeer {
                platform: platform.into(),
                peer_id: peer_id.into(),
            },
        }
    }

    #[test]
    fn a_peer_filter_matches_that_peer_and_nobody_else() {
        let peer = ChannelPeer::new("feishu", "ou_x");
        let event = InboundEvent::Message {
            peer: &peer,
            text: "在了",
        };
        assert!(matches(
            &EventFilter::FromPeer {
                platform: "feishu".into(),
                peer_id: "ou_x".into()
            },
            &event
        ));
        // Same id on another platform is another person.
        assert!(!matches(
            &EventFilter::FromPeer {
                platform: "telegram".into(),
                peer_id: "ou_x".into()
            },
            &event
        ));
        assert!(!matches(
            &EventFilter::Webhook { name: "ci".into() },
            &event
        ));
    }

    #[test]
    fn every_registration_watching_that_peer_fires() {
        let now = 1_000;
        let rows = vec![
            WakeupRegistration::new("s1", from_peer("feishu", "ou_x"), now),
            WakeupRegistration::new("s2", from_peer("feishu", "ou_y"), now),
            WakeupRegistration::new("s3", from_peer("feishu", "ou_x"), now),
            WakeupRegistration::new("s4", Wakeup::UserReply, now),
        ];
        let peer = ChannelPeer::new("feishu", "ou_x");
        let hits = matching(
            &rows,
            &InboundEvent::Message {
                peer: &peer,
                text: "ok",
            },
        );
        assert_eq!(
            hits.iter().map(|r| &r.session_id).collect::<Vec<_>>(),
            vec!["s1", "s3"]
        );
    }

    /// A turn parked on `wait { for_event: { webhook } }` is woken by the hook
    /// it named and by no other — the ingress cannot tell them apart, so this
    /// is the only thing that does.
    #[test]
    fn a_webhook_filter_matches_its_own_name_only() {
        let arrived = InboundEvent::Webhook { name: "ci-done" };
        assert!(matches(
            &EventFilter::Webhook {
                name: "ci-done".into()
            },
            &arrived
        ));
        assert!(!matches(
            &EventFilter::Webhook {
                name: "deploy".into()
            },
            &arrived
        ));
        assert!(!matches(
            &EventFilter::FromPeer {
                platform: "feishu".into(),
                peer_id: "ci-done".into()
            },
            &arrived
        ));
    }

    /// The half of an event that reaches a person: what happened, in one line.
    #[test]
    fn an_events_account_of_itself_stays_one_line() {
        let hook = ExternalEvent::Webhook {
            name: "ci".into(),
            body: "build 4213 failed\nstack trace follows\n".into(),
        };
        assert_eq!(hook.summary(), "webhook `ci`: build 4213 failed");
        assert!(
            ExternalEvent::Webhook {
                name: "ci".into(),
                body: "  ".into(),
            }
            .summary()
            .contains("空 body")
        );

        let reaction = ExternalEvent::Feishu(FeishuEvent {
            chat: "oc_1".into(),
            sender: "张三".into(),
            reaction: Some("THUMBSUP".into()),
            ..Default::default()
        });
        assert_eq!(reaction.summary(), "张三 回应了 THUMBSUP");

        // A batch says how many and names one, so the record is readable
        // without holding fifty paths.
        let batch = ExternalEvent::FileChanged {
            paths: vec![PathBuf::from("/srv/notes/a.md"), PathBuf::from("/srv/b.md")],
        };
        assert_eq!(batch.summary(), "2 个文件变更，首个 /srv/notes/a.md");
        assert_eq!(
            ExternalEvent::FileChanged {
                paths: vec![PathBuf::from("/srv/notes/a.md")],
            }
            .summary(),
            "文件变更：/srv/notes/a.md"
        );
    }

    /// Only a webhook can wake a standing registration. A feishu message
    /// already fires peer waits through the chat ingress; routing it here as
    /// well would answer one commitment twice.
    #[test]
    fn only_a_webhook_is_offered_to_the_standing_registrations() {
        assert!(
            ExternalEvent::Webhook {
                name: "ci".into(),
                body: String::new(),
            }
            .as_inbound()
            .is_some()
        );
        assert!(
            ExternalEvent::Feishu(FeishuEvent::default())
                .as_inbound()
                .is_none()
        );
        assert!(
            ExternalEvent::FileChanged { paths: vec![] }
                .as_inbound()
                .is_none()
        );
    }
}
