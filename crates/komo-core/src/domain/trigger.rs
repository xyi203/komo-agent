//! What an inbound message matches (docs/bot-runtime.md §3.7, §5.13).
//!
//! One pure function over an [`EventFilter`] and the message that arrived,
//! separated from everything that has to happen afterwards — reading standing
//! registrations, claiming one, waking a turn — because "does this message
//! match?" is a question with a definite answer and no I/O, and it is asked by
//! two features: a kanban Task waiting for a reply, and (§5.13) a routine
//! triggered by a chat message.
//!
//! Matching is on the **address**, never on a name someone typed. `waiting_on:
//! "张三"` is for a human to read; a [`ChannelPeer`] is what an inbound message
//! can actually be compared with, and a task whose `waiting_on` never resolved
//! to one is simply not wakeable.

use super::session::ChannelPeer;
use super::session_event::EventFilter;
use super::wakeup::WakeupRegistration;

/// One thing that arrived, in the terms a filter is written in.
pub struct InboundEvent<'a> {
    pub peer: &'a ChannelPeer,
    pub text: &'a str,
}

/// Whether this filter is about the thing that just arrived.
pub fn matches(filter: &EventFilter, event: &InboundEvent<'_>) -> bool {
    match filter {
        EventFilter::FromPeer { platform, peer_id } => {
            platform == &event.peer.platform && peer_id == &event.peer.peer_id
        }
        // Its ingress is an HTTP route, not a chat message; nothing arriving
        // on a channel can be a webhook firing.
        EventFilter::Webhook { .. } => false,
    }
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
        let event = InboundEvent {
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
            &InboundEvent {
                peer: &peer,
                text: "ok",
            },
        );
        assert_eq!(
            hits.iter().map(|r| &r.session_id).collect::<Vec<_>>(),
            vec!["s1", "s3"]
        );
    }
}
