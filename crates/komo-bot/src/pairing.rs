//! The pairing gate channels consult before handing a message to the agent.
//!
//! Ordering of checks: config `allow_from` (pre-trusted, no db hit) → an
//! approved pairing row → a recently-issued pending code (rate-limited, no new
//! code) → otherwise mint a fresh code (subject to the per-platform pending
//! cap) and tell the sender how to pair. Approval happens out-of-band via
//! `komo pair approve`, which writes the shared SQLite db — the gateway picks
//! it up on the sender's next message, no restart needed.
//!
//! Hardening (after hermes-agent's `pairing.py`): codes are stored only as
//! salted hashes, a sender gets at most one fresh code per
//! [`PAIRING_RATE_LIMIT_SECS`], and at most [`MAX_PENDING_PER_PLATFORM`] senders
//! may await approval per platform.

use std::sync::Arc;

use tracing::{error, info, warn};

use komo_core::domain::pairing::{
    MAX_PENDING_PER_PLATFORM, PAIRING_RATE_LIMIT_SECS, PairingRepository, PairingRequest,
    PairingStatus,
};

/// Outcome of the gate for one inbound message.
pub enum Gate {
    Allowed(Principal),
    /// Not paired: do not run the agent; send `reply` (the pairing prompt)
    /// back on the channel instead.
    Denied {
        reply: String,
    },
}

/// *Who* was admitted — the first half of conversation resolution
/// (docs/bot-runtime.md §3.8).
///
/// The distinction is already in the config and costs nothing to read off it:
/// `allow_from` is where the operator writes down their **own** ids on each
/// platform, and pairing is how they admit **somebody else**. So a pre-trusted
/// sender is the operator, and an approved pairing is a correspondent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    /// The operator themself. Their private chats are all one conversation.
    Operator,
    /// Someone the operator paired with. Gets a conversation of their own.
    Correspondent,
}

pub struct PairingGuard {
    platform: &'static str,
    allow_from: Vec<String>,
    pairings: Arc<dyn PairingRepository>,
}

impl PairingGuard {
    pub fn new(
        platform: &'static str,
        allow_from: Vec<String>,
        pairings: Arc<dyn PairingRepository>,
    ) -> Self {
        Self {
            platform,
            allow_from,
            pairings,
        }
    }

    /// Run the pairing gate and handle a denial in one place: `Some(principal)`
    /// means the sender may proceed and says who they are; `None` means the
    /// message was consumed (an unpaired sender was sent a pairing code, or the
    /// check errored) and the caller should skip it. `send_reply` delivers the
    /// pairing prompt over the channel — the one channel-specific bit (each has
    /// its own sender type). Consolidates the identical gate/log block the
    /// ingress channels repeated.
    pub async fn admit<F, Fut>(
        &self,
        sender_id: &str,
        chat_id: &str,
        send_reply: F,
    ) -> Option<Principal>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        match self.check(sender_id, chat_id).await {
            Ok(Gate::Allowed(principal)) => Some(principal),
            Ok(Gate::Denied { reply }) => {
                info!(platform = self.platform, sender = %sender_id, "sender unpaired; sent pairing code");
                if let Err(error) = send_reply(reply).await {
                    error!(%error, platform = self.platform, chat = %chat_id, "failed to send pairing prompt");
                }
                None
            }
            Err(error) => {
                warn!(%error, platform = self.platform, "pairing check failed; dropping message");
                None
            }
        }
    }

    pub async fn check(&self, sender_id: &str, chat_id: &str) -> anyhow::Result<Gate> {
        if self.allow_from.iter().any(|s| s == sender_id) {
            return Ok(Gate::Allowed(Principal::Operator));
        }
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        match self.pairings.find(self.platform, sender_id).await? {
            Some(p) if p.status == PairingStatus::Approved => {
                Ok(Gate::Allowed(Principal::Correspondent))
            }
            Some(p) if !p.is_expired(now) => {
                if now - p.created_at < PAIRING_RATE_LIMIT_SECS {
                    // A code was issued recently; don't mint another (and we
                    // can't re-send it — only the salted hash is stored).
                    Ok(Gate::Denied {
                        reply: pending_prompt(),
                    })
                } else {
                    // Past the rate-limit window: reissue a fresh code so a
                    // sender who lost theirs can recover (replaces their row).
                    self.mint(sender_id, chat_id, true).await
                }
            }
            // First contact, or the previous code expired: mint a fresh one.
            _ => self.mint(sender_id, chat_id, false).await,
        }
    }

    /// Mint and persist a fresh code, then return the pairing prompt. When not
    /// replacing the sender's own active row, the per-platform pending cap
    /// applies (anti-abuse).
    async fn mint(
        &self,
        sender_id: &str,
        chat_id: &str,
        replacing_active: bool,
    ) -> anyhow::Result<Gate> {
        if !replacing_active
            && self.pairings.count_active_pending(self.platform).await? >= MAX_PENDING_PER_PLATFORM
        {
            return Ok(Gate::Denied {
                reply: cap_prompt(),
            });
        }
        let (request, code) = PairingRequest::mint(self.platform, sender_id, chat_id);
        self.pairings.upsert(&request).await?;
        Ok(Gate::Denied {
            reply: prompt(&code),
        })
    }
}

fn prompt(code: &str) -> String {
    format!(
        "此账号尚未与 komo 配对。配对码: {code}\n\
         请在 komo 所在主机上运行: komo pair approve {code}\n\
         配对完成后再发消息即可对话。"
    )
}

fn pending_prompt() -> String {
    "你的配对请求正在等待管理员批准，请稍候。".to_string()
}

fn cap_prompt() -> String {
    "当前待配对请求过多，请稍后再试。".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::pairing::{ApproveOutcome, PAIRING_CODE_TTL_SECS, verify_code};
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemPairings {
        rows: Mutex<Vec<PairingRequest>>,
    }

    #[async_trait]
    impl PairingRepository for MemPairings {
        async fn upsert(&self, request: &PairingRequest) -> anyhow::Result<()> {
            let mut rows = self.rows.lock().unwrap();
            rows.retain(|r| r.id != request.id);
            rows.push(request.clone());
            Ok(())
        }
        async fn find(
            &self,
            platform: &str,
            sender_id: &str,
        ) -> anyhow::Result<Option<PairingRequest>> {
            let id = format!("{platform}:{sender_id}");
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.id == id)
                .cloned())
        }
        async fn count_active_pending(&self, platform: &str) -> anyhow::Result<usize> {
            let now = time::OffsetDateTime::now_utc().unix_timestamp();
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|r| {
                    r.platform == platform
                        && r.status == PairingStatus::Pending
                        && !r.is_expired(now)
                })
                .count())
        }
        async fn approve_code(&self, code: &str) -> anyhow::Result<ApproveOutcome> {
            let mut rows = self.rows.lock().unwrap();
            for r in rows.iter_mut() {
                if r.status == PairingStatus::Pending && verify_code(&r.salt, &r.code_hash, code) {
                    r.status = PairingStatus::Approved;
                    return Ok(ApproveOutcome::Approved(r.clone()));
                }
            }
            Ok(ApproveOutcome::NotFound)
        }
        async fn list(&self) -> anyhow::Result<Vec<PairingRequest>> {
            Ok(self.rows.lock().unwrap().clone())
        }
        async fn revoke(&self, id: &str) -> anyhow::Result<bool> {
            let mut rows = self.rows.lock().unwrap();
            let before = rows.len();
            rows.retain(|r| r.id != id);
            Ok(rows.len() != before)
        }
    }

    fn guard(allow_from: Vec<String>) -> (PairingGuard, Arc<MemPairings>) {
        let repo = Arc::new(MemPairings::default());
        (
            PairingGuard::new("telegram", allow_from, repo.clone()),
            repo,
        )
    }

    /// Pull the 8-char pairing code out of a prompt reply.
    fn code_in(reply: &str) -> String {
        reply
            .split("komo pair approve ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .expect("reply carries a code")
            .to_string()
    }

    #[tokio::test]
    async fn allow_from_sender_skips_pairing() {
        let (guard, repo) = guard(vec!["42".to_string()]);
        assert!(matches!(
            guard.check("42", "42").await.unwrap(),
            Gate::Allowed(Principal::Operator)
        ));
        assert!(repo.rows.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn admit_allows_trusted_and_sends_no_reply() {
        let (guard, _repo) = guard(vec!["42".to_string()]);
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let sent2 = sent.clone();
        let admitted = guard
            .admit("42", "42", move |reply| {
                let sent2 = sent2.clone();
                async move {
                    sent2.lock().unwrap().push(reply);
                    Ok(())
                }
            })
            .await;
        assert_eq!(
            admitted,
            Some(Principal::Operator),
            "an allow_from sender is the operator, admitted without pairing"
        );
        assert!(sent.lock().unwrap().is_empty(), "no pairing prompt sent");
    }

    #[tokio::test]
    async fn admit_denies_unknown_and_sends_the_pairing_code() {
        let (guard, _repo) = guard(vec![]);
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let sent2 = sent.clone();
        let admitted = guard
            .admit("7", "7", move |reply| {
                let sent2 = sent2.clone();
                async move {
                    sent2.lock().unwrap().push(reply);
                    Ok(())
                }
            })
            .await;
        assert!(admitted.is_none(), "an unpaired sender is not admitted");
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "exactly one pairing prompt sent");
        assert!(sent[0].contains("komo pair approve"), "{}", sent[0]);
    }

    #[tokio::test]
    async fn unknown_sender_gets_pairing_code_and_is_denied() {
        let (guard, repo) = guard(vec![]);
        let Gate::Denied { reply } = guard.check("7", "7").await.unwrap() else {
            panic!("unknown sender must be denied");
        };
        assert!(reply.contains("komo pair approve"), "{reply}");
        // The minted code verifies against the stored (hashed) row.
        let code = code_in(&reply);
        let row = &repo.rows.lock().unwrap()[0];
        assert!(verify_code(&row.salt, &row.code_hash, &code));
    }

    #[tokio::test]
    async fn repeated_messages_within_rate_limit_keep_the_pending_code() {
        let (guard, repo) = guard(vec![]);
        guard.check("7", "7").await.unwrap();
        let first_hash = repo.rows.lock().unwrap()[0].code_hash.clone();
        let Gate::Denied { reply } = guard.check("7", "7").await.unwrap() else {
            panic!("still unpaired");
        };
        // No fresh code minted; the pending row is untouched.
        assert!(!reply.contains("komo pair approve"), "{reply}");
        assert_eq!(repo.rows.lock().unwrap().len(), 1);
        assert_eq!(repo.rows.lock().unwrap()[0].code_hash, first_hash);
    }

    #[tokio::test]
    async fn approved_sender_is_allowed_as_a_correspondent() {
        let (guard, repo) = guard(vec![]);
        let (request, code) = PairingRequest::mint("telegram", "7", "7");
        repo.upsert(&request).await.unwrap();
        assert!(matches!(
            repo.approve_code(&code).await.unwrap(),
            ApproveOutcome::Approved(_)
        ));
        assert!(
            matches!(
                guard.check("7", "7").await.unwrap(),
                Gate::Allowed(Principal::Correspondent)
            ),
            "pairing admits somebody else, never the operator themself"
        );
    }

    #[tokio::test]
    async fn expired_pending_code_is_replaced() {
        let (guard, repo) = guard(vec![]);
        guard.check("7", "7").await.unwrap();
        let first_hash = {
            let mut rows = repo.rows.lock().unwrap();
            rows[0].created_at -= PAIRING_CODE_TTL_SECS + 60;
            rows[0].code_hash.clone()
        };
        guard.check("7", "7").await.unwrap();
        let rows = repo.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_ne!(
            rows[0].code_hash, first_hash,
            "expired code must be reissued"
        );
    }

    #[tokio::test]
    async fn pending_cap_blocks_an_extra_sender() {
        let (guard, _repo) = guard(vec![]);
        for i in 0..MAX_PENDING_PER_PLATFORM {
            let Gate::Denied { reply } = guard.check(&i.to_string(), "c").await.unwrap() else {
                panic!("unpaired sender denied");
            };
            assert!(reply.contains("komo pair approve"), "{reply}");
        }
        // One past the cap: no code, told to try later.
        let Gate::Denied { reply } = guard.check("99", "c").await.unwrap() else {
            panic!("denied");
        };
        assert!(!reply.contains("komo pair approve"), "{reply}");
    }
}
