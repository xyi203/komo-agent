//! Wakeup registrations: `wakeup_records` in `komo.db`, and the
//! [`WakeupRepository`] over them (docs/bot-runtime.md §3.2).
//!
//! One row per standing wait: *when X happens, on session Z, either continue
//! turn Y or start a new one*. **Durable** — a registration that vanishes is a
//! turn that waits forever — so the table is migrated in place, never dropped.
//!
//! The session log stays the authority on what a turn is *doing*; this is the
//! authority on when to come back for it. Kept apart because they are read
//! differently: the log per session on the turn's own path, these every sweep
//! tick across all sessions, by a scheduler that must not open a session
//! artifact per row to find out whether it has anything to do.
//!
//! [`WakeupRepository::take`] is the **claim**: it answers `false` when the row
//! was already gone, so two sweeps racing one registration — or a sweep racing
//! an arriving `/approve` — fire it exactly once.

use anyhow::Context;
use async_trait::async_trait;

use super::db::Db;
use crate::persistence::with_write_retry;
use komo_core::domain::policy::RuleSpec;
use komo_core::domain::session_event::{EventFilter, Wakeup};
use komo_core::domain::wakeup::{WakeupRegistration, WakeupRepository};

/// One standing wakeup. The `Wakeup` variant is flattened: `kind` discriminates
/// and the payload columns below are empty/0 for the variants that do not carry
/// them — the same shape `CronJobRecord` uses for its two action modes, and for
/// the same reason (one table, queried by the sweep as a whole).
#[derive(Debug, toasty::Model)]
pub(crate) struct WakeupRecord {
    #[key]
    id: String,

    #[index]
    session_id: String,

    /// The suspended turn to continue; empty = start a fresh turn.
    turn_id: String,

    /// "at" | "approval" | "user-reply" | "task-done" | "event".
    kind: String,
    /// `Wakeup::At`'s instant; 0 otherwise.
    at: i64,
    /// `Wakeup::Approval`'s call; empty otherwise.
    call_id: String,
    /// `Wakeup::TaskDone`'s task; empty otherwise.
    task_id: String,
    /// `Wakeup::Event`'s filter as JSON; empty otherwise.
    filter: String,

    /// 0 = no deadline (a timer is its own).
    expires_at: i64,
    /// JSON array of `RuleSpec` — what the woken turn may do unattended,
    /// inherited from the turn that suspended. Empty = none.
    grants: String,
    created_at: i64,
}

/// The exact DDL `push_schema` emits for [`WakeupRecord`]. Needed because an
/// existing `komo.db` never re-runs `push_schema`, and this table arrived after
/// the file did — byte-parity is locked by `wakeup_table_ddl_matches_push_schema`.
pub(crate) const WAKEUP_TABLE: &str = "wakeup_records";
pub(crate) const WAKEUP_TABLE_DDL: &[&str] = &[
    "CREATE TABLE \"wakeup_records\" (\"id\" TEXT NOT NULL, \"session_id\" TEXT NOT NULL, \
     \"turn_id\" TEXT NOT NULL, \"kind\" TEXT NOT NULL, \"at\" BIGINT NOT NULL, \
     \"call_id\" TEXT NOT NULL, \"task_id\" TEXT NOT NULL, \"filter\" TEXT NOT NULL, \
     \"expires_at\" BIGINT NOT NULL, \"grants\" TEXT NOT NULL, \"created_at\" BIGINT NOT NULL, \
     PRIMARY KEY (\"id\"))",
    "CREATE INDEX \"index_wakeup_records_by_session_id\" ON \"wakeup_records\" (\"session_id\")",
];

#[async_trait]
impl WakeupRepository for Db {
    async fn save(&self, registration: &WakeupRegistration) -> anyhow::Result<()> {
        let columns = WakeupColumns::from(&registration.wakeup)?;
        let grants = encode_grants(&registration.grants)?;
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            toasty::create!(WakeupRecord {
                id: registration.id.clone(),
                session_id: registration.session_id.clone(),
                turn_id: registration.turn_id.clone().unwrap_or_default(),
                kind: columns.kind.to_string(),
                at: columns.at,
                call_id: columns.call_id.clone(),
                task_id: columns.task_id.clone(),
                filter: columns.filter.clone(),
                expires_at: registration.expires_at.unwrap_or(0),
                grants: grants.clone(),
                created_at: registration.created_at,
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn list(&self) -> anyhow::Result<Vec<WakeupRegistration>> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(WakeupRecord).exec(&mut conn).await?;
        let mut out: Vec<WakeupRegistration> =
            rows.into_iter().map(registration_from_record).collect();
        out.sort_by_key(|r| r.created_at);
        Ok(out)
    }

    async fn take(&self, id: &str) -> anyhow::Result<bool> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            // Read and delete in one transaction: whoever commits second finds
            // the row gone after its conflict is retried, and answers `false`.
            // That is the whole at-most-once guarantee.
            let mut tx = conn.transaction().await?;
            let Ok(record) = WakeupRecord::get_by_id(&mut tx, id).await else {
                return Ok(false);
            };
            record.delete().exec(&mut tx).await?;
            tx.commit().await?;
            Ok(true)
        })
        .await
    }

    async fn take_for_turn(&self, session_id: &str, turn_id: &str) -> anyhow::Result<usize> {
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut tx = conn.transaction().await?;
            let session = session_id.to_string();
            let rows = toasty::query!(WakeupRecord FILTER .session_id == #session)
                .exec(&mut tx)
                .await?;
            let mut dropped = 0;
            for record in rows {
                if record.turn_id != turn_id {
                    continue;
                }
                record.delete().exec(&mut tx).await?;
                dropped += 1;
            }
            tx.commit().await?;
            Ok(dropped)
        })
        .await
    }
}

/// The flattened `Wakeup`.
struct WakeupColumns {
    kind: &'static str,
    at: i64,
    call_id: String,
    task_id: String,
    filter: String,
}

impl WakeupColumns {
    fn from(wakeup: &Wakeup) -> anyhow::Result<Self> {
        let mut columns = Self {
            kind: "",
            at: 0,
            call_id: String::new(),
            task_id: String::new(),
            filter: String::new(),
        };
        match wakeup {
            Wakeup::At { at } => {
                columns.kind = "at";
                columns.at = *at;
            }
            Wakeup::Approval { call_id } => {
                columns.kind = "approval";
                columns.call_id = call_id.clone();
            }
            Wakeup::UserReply => columns.kind = "user-reply",
            Wakeup::TaskDone { task_id } => {
                columns.kind = "task-done";
                columns.task_id = task_id.clone();
            }
            Wakeup::Event { filter } => {
                columns.kind = "event";
                columns.filter =
                    serde_json::to_string(filter).context("encoding an event filter")?;
            }
        }
        Ok(columns)
    }
}

fn encode_grants(grants: &[RuleSpec]) -> anyhow::Result<String> {
    if grants.is_empty() {
        return Ok(String::new());
    }
    serde_json::to_string(grants).context("encoding a wakeup's grants")
}

/// Decode a row. A payload that will not parse degrades to the **expired**
/// reading of its variant rather than to a dropped row: a wait komo can no
/// longer describe still has to come back and say so, or the turn behind it
/// waits forever.
fn registration_from_record(record: WakeupRecord) -> WakeupRegistration {
    let wakeup = match record.kind.as_str() {
        "at" => Wakeup::At { at: record.at },
        "approval" => Wakeup::Approval {
            call_id: record.call_id,
        },
        "task-done" => Wakeup::TaskDone {
            task_id: record.task_id,
        },
        "event" => match serde_json::from_str::<EventFilter>(&record.filter) {
            Ok(filter) => Wakeup::Event { filter },
            Err(error) => {
                tracing::warn!(%error, id = %record.id, "unreadable wakeup filter; treating it as a plain reply wait");
                Wakeup::UserReply
            }
        },
        // Including the literal "user-reply", and anything an older or newer
        // komo wrote: waiting for the user is the reading that expires and
        // reports back, which is the safe end of the range.
        _ => Wakeup::UserReply,
    };
    WakeupRegistration {
        id: record.id,
        session_id: record.session_id,
        turn_id: Some(record.turn_id).filter(|t| !t.is_empty()),
        wakeup,
        expires_at: Some(record.expires_at).filter(|at| *at != 0),
        grants: match record.grants.is_empty() {
            true => Vec::new(),
            false => serde_json::from_str(&record.grants).unwrap_or_default(),
        },
        created_at: record.created_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::policy::RuleSpec;

    /// A `komo.db` in a home of this test's own.
    fn url(name: &str) -> String {
        let home = std::env::temp_dir().join(format!("komo-wk-{name}"));
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        format!("turso:{}", home.join("komo.db").display())
    }

    fn grant() -> RuleSpec {
        RuleSpec {
            category: "shell".into(),
            matcher: "prefix".into(),
            value: "git ".into(),
            access: None,
            channels: None,
            effect: "allow".into(),
            include_dangerous: false,
            unattended: true,
        }
    }

    #[tokio::test]
    async fn every_variant_round_trips_with_what_it_carries() {
        let db = Db::connect(&url("variants")).await.unwrap();
        let wakeups = [
            Wakeup::At { at: 1_700_000_000 },
            Wakeup::Approval {
                call_id: "call-7".into(),
            },
            Wakeup::UserReply,
            Wakeup::TaskDone {
                task_id: "task-3".into(),
            },
            Wakeup::Event {
                filter: EventFilter::Webhook { name: "ci".into() },
            },
        ];
        for (i, wakeup) in wakeups.iter().enumerate() {
            let mut registration = WakeupRegistration::new("s1", wakeup.clone(), 1_000 + i as i64)
                .with_grants(vec![grant()]);
            registration.turn_id = Some(format!("run-{i}"));
            WakeupRepository::save(&db, &registration).await.unwrap();
        }

        let stored = WakeupRepository::list(&db).await.unwrap();
        assert_eq!(stored.len(), wakeups.len(), "oldest first, all of them");
        for (i, (registration, wakeup)) in stored.iter().zip(&wakeups).enumerate() {
            assert_eq!(&registration.wakeup, wakeup, "variant {i}");
            assert_eq!(registration.turn_id.as_deref(), Some(&*format!("run-{i}")));
            assert_eq!(registration.grants.len(), 1, "grants survive the wait");
            assert_eq!(registration.grants[0].value, "git ");
        }
    }

    /// A registration with no turn is a wake that *starts* one — the difference
    /// between "pick up where you left off" and "here is something you were
    /// waiting to hear about".
    #[tokio::test]
    async fn a_registration_without_a_turn_reads_back_without_one() {
        let db = Db::connect(&url("no-turn")).await.unwrap();
        let registration = WakeupRegistration::new("s1", Wakeup::UserReply, 1_000);
        WakeupRepository::save(&db, &registration).await.unwrap();
        let stored = WakeupRepository::list(&db).await.unwrap();
        assert_eq!(stored[0].turn_id, None);
        assert!(stored[0].grants.is_empty());
        assert_eq!(
            stored[0].expires_at, registration.expires_at,
            "and its deadline is the one it was created with"
        );
    }

    /// The claim. Whatever races — two sweeps, or a sweep and an arriving
    /// `/approve` — exactly one of them gets to fire it.
    #[tokio::test]
    async fn taking_a_registration_twice_succeeds_once() {
        let db = Db::connect(&url("claim")).await.unwrap();
        let registration = WakeupRegistration::new("s1", Wakeup::UserReply, 1_000);
        WakeupRepository::save(&db, &registration).await.unwrap();

        assert!(WakeupRepository::take(&db, &registration.id).await.unwrap());
        assert!(
            !WakeupRepository::take(&db, &registration.id).await.unwrap(),
            "the second claim finds it gone"
        );
        assert!(WakeupRepository::list(&db).await.unwrap().is_empty());
        assert!(
            !WakeupRepository::take(&db, "wk-never-existed")
                .await
                .unwrap(),
            "and an id nobody registered is not an error"
        );
    }

    /// A turn that came back takes every wait it was holding with it, whichever
    /// one actually woke it: a turn resumed by an approval must not also be
    /// woken by the timer that was watching the same wait.
    #[tokio::test]
    async fn a_turns_registrations_retire_together() {
        let db = Db::connect(&url("per-turn")).await.unwrap();
        for wakeup in [
            Wakeup::UserReply,
            Wakeup::At { at: 1_700_000_000 },
            Wakeup::Approval {
                call_id: "c1".into(),
            },
        ] {
            let mut registration = WakeupRegistration::new("s1", wakeup, 1_000);
            registration.turn_id = Some("run-1".into());
            WakeupRepository::save(&db, &registration).await.unwrap();
        }
        // Another turn's wait, and another session's, must not be touched.
        let mut other_turn = WakeupRegistration::new("s1", Wakeup::UserReply, 1_000);
        other_turn.turn_id = Some("run-2".into());
        WakeupRepository::save(&db, &other_turn).await.unwrap();
        let mut other_session = WakeupRegistration::new("s2", Wakeup::UserReply, 1_000);
        other_session.turn_id = Some("run-1".into());
        WakeupRepository::save(&db, &other_session).await.unwrap();

        assert_eq!(
            WakeupRepository::take_for_turn(&db, "s1", "run-1")
                .await
                .unwrap(),
            3
        );
        let left = WakeupRepository::list(&db).await.unwrap();
        assert_eq!(left.len(), 2);
        assert!(left.iter().any(|r| r.session_id == "s2"));
        assert!(left.iter().any(|r| r.turn_id.as_deref() == Some("run-2")));
    }
}
