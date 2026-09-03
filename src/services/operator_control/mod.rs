//! Operator control: one module owns how host-operator actions (list/inspect
//! reads, governance and maintenance writes) reach komo's state.
//!
//! Turso's exclusive cross-process lock means a running gateway is the sole
//! owner of the dbs — so every operator action has two transports: routed to
//! the gateway over its loopback api channel, or executed in-process against
//! directly-opened stores. [`OperatorControl`] hides that choice: CLI callers
//! issue one typed [`OperatorQuery`]/[`OperatorCommand`] and never probe the
//! gateway, pick a db, or translate transport payloads themselves.
//!
//! The two adapters may differ only in transport, auth, and connection
//! ownership — the business result comes from the shared projections and
//! transitions in [`actions`], which the gateway's HTTP handlers call too.

pub mod actions;
mod direct;
mod gateway;
pub mod request;

use std::future::Future;
use std::sync::Arc;

pub use request::*;

use crate::domain::run::Run;
use crate::infra::gateway_client::GatewayClient;
use komo_config::RuntimeConfig;
use komo_infra::persistence::db::Db;

use direct::DirectOperatorAdapter;
use gateway::GatewayOperatorAdapter;

/// Where the store lives, for the direct adapter's lazy connection.
#[derive(Debug, Clone)]
pub struct StoreUrls {
    pub db: String,
    /// Note-vault config, when `[wiki]` declares a vault. Carried here so the
    /// direct adapter can open the index itself when no gateway is running —
    /// the same reason the db urls are here.
    pub wiki: Option<komo_config::WikiConfig>,
    /// The governed skill store's root, for the same reason: resolved from the
    /// running config rather than from `FsSkillStore::default_root()`, so a
    /// relocated `KOMO_HOME` reads its own skills and a test reads its temp dir.
    pub skills_root: std::path::PathBuf,
}

impl StoreUrls {
    pub fn from_config(runtime: &RuntimeConfig) -> Self {
        Self {
            db: runtime.db_url.clone(),
            wiki: runtime.wiki.clone(),
            skills_root: runtime.home.join("skills"),
        }
    }
}

enum OperatorBackend {
    Gateway(GatewayOperatorAdapter),
    Direct(DirectOperatorAdapter),
}

/// The operator surface's single entry point. Resolve once per CLI command
/// (`connect` probes the gateway exactly once), then issue any number of
/// queries/commands against the same backend — a batch never re-probes or
/// reconnects per item.
pub struct OperatorControl {
    backend: OperatorBackend,
}

impl OperatorControl {
    /// Probe for a running gateway once: reachable → route over its loopback
    /// api channel; otherwise operate on the stores directly (opened lazily,
    /// only the ones a request actually needs).
    pub async fn connect(urls: StoreUrls) -> anyhow::Result<Self> {
        let backend = match GatewayClient::try_connect().await {
            Some(client) => OperatorBackend::Gateway(GatewayOperatorAdapter::new(client)),
            None => OperatorBackend::Direct(DirectOperatorAdapter::new(urls)),
        };
        Ok(Self { backend })
    }

    /// Whether actions route to a running gateway (status lines only — never
    /// branch behavior on this).
    pub fn via_gateway(&self) -> bool {
        matches!(self.backend, OperatorBackend::Gateway(_))
    }

    /// Run one read-only operator query.
    pub async fn query(&self, query: OperatorQuery) -> anyhow::Result<OperatorQueryResult> {
        match &self.backend {
            OperatorBackend::Gateway(gw) => gw.query(query).await,
            OperatorBackend::Direct(direct) => direct.query(query).await,
        }
    }

    /// Run one state-changing operator command.
    pub async fn command(&self, command: OperatorCommand) -> anyhow::Result<OperatorCommandResult> {
        match &self.backend {
            OperatorBackend::Gateway(gw) => gw.command(command).await,
            OperatorBackend::Direct(direct) => direct.command(command).await,
        }
    }

    /// Resume an interrupted run. `id = None` picks the most recent recoverable
    /// run (same scan on both backends). On the gateway backend the whole
    /// action runs server-side (trusted loopback). On the direct backend the
    /// turn itself must run in the caller's process — interactive approval
    /// needs a human at the terminal — so the caller supplies `local_turn`,
    /// which receives the already-open stores plus the run and the digest
    /// priming input, and returns the reply plus whether it *continued* from
    /// the turn journal (vs re-running fresh from the digest). Eligibility,
    /// the priming digest, and the at-most-once `recoverable` clear all stay
    /// in here.
    pub async fn resume_run<F, Fut>(
        &self,
        id: Option<String>,
        local_turn: F,
    ) -> anyhow::Result<ResumeOutcome>
    where
        F: FnOnce(Arc<Db>, Run, String) -> Fut,
        Fut: Future<Output = anyhow::Result<(String, bool)>>,
    {
        let target_id = match id {
            Some(id) => id,
            None => {
                let OperatorQueryResult::Runs(runs) = self
                    .query(OperatorQuery::Runs {
                        limit: actions::RESUME_SCAN_LIMIT,
                    })
                    .await?
                else {
                    unreachable!("Runs query answers with Runs");
                };
                runs.into_iter()
                    .find(|r| r.recoverable)
                    .map(|r| r.id)
                    .ok_or_else(|| anyhow::anyhow!(actions::NO_RECOVERABLE))?
            }
        };
        match &self.backend {
            OperatorBackend::Gateway(gw) => gw.client().resume(&target_id).await,
            OperatorBackend::Direct(direct) => {
                let db = direct.db().await?.clone();
                match actions::resolve_resume(db.as_ref(), &target_id).await? {
                    actions::ResumeTarget::Missing => {
                        anyhow::bail!("no run with id `{target_id}`")
                    }
                    actions::ResumeTarget::NotRecoverable { status } => {
                        anyhow::bail!(actions::not_recoverable_message(&target_id, &status))
                    }
                    actions::ResumeTarget::Ready { run, steps, input } => {
                        let session_id = run.session_id.clone();
                        let (reply, continued) = local_turn(db.clone(), run, input).await?;
                        // Nothing to clear: the continuation's own
                        // `turn/started{resumed_from}` claimed the turn it
                        // picked up, and a claimed turn is no longer offered.
                        Ok(ResumeOutcome {
                            run_id: target_id,
                            session_id,
                            steps: steps.len(),
                            reply,
                            continued,
                        })
                    }
                }
            }
        }
    }
}

/// Unix seconds — the operator surface's one clock read per request.
pub(crate) fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

#[cfg(test)]
mod tests {
    //! Contract tests over the direct backend. The gateway backend is a thin
    //! mapping onto `GatewayClient` (its transport behaviors — stale
    //! rendezvous fallback, 404 version skew — are tested there); business
    //! results on both paths come from the same `actions` helpers, which these
    //! tests exercise end-to-end through the `OperatorControl` interface.

    use super::*;
    use crate::domain::memory::{Memory, MemoryKind, MemoryRepository, MemoryStatus};
    use crate::domain::run::Run;

    fn temp_urls(tag: &str) -> StoreUrls {
        let dir = std::env::temp_dir().join(format!("komo_opctl_{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        StoreUrls {
            db: format!("turso:{}", dir.join("komo.db").display()),
            // These tests exercise the db-backed operations only.
            wiki: None,
            skills_root: dir.join("skills"),
        }
    }

    fn direct(urls: StoreUrls) -> OperatorControl {
        OperatorControl {
            backend: OperatorBackend::Direct(DirectOperatorAdapter::new(urls)),
        }
    }

    #[tokio::test]
    async fn queries_on_empty_stores_return_empty() {
        let control = direct(temp_urls("empty"));
        let OperatorQueryResult::Runs(runs) = control
            .query(OperatorQuery::Runs { limit: 10 })
            .await
            .unwrap()
        else {
            panic!("Runs answers Runs");
        };
        assert!(runs.is_empty());
        let OperatorQueryResult::Sessions(sessions) =
            control.query(OperatorQuery::Sessions).await.unwrap()
        else {
            panic!("Sessions answers Sessions");
        };
        assert!(sessions.is_empty());
        let OperatorQueryResult::DreamPreview(report) =
            control.query(OperatorQuery::DreamPreview).await.unwrap()
        else {
            panic!("DreamPreview answers DreamPreview");
        };
        assert!(report.is_empty());
    }

    /// The laziness that is left after the merge: one file, opened on the
    /// first request that needs it and not before. A `komo doctor` that only
    /// prints config must not create a database.
    #[tokio::test]
    async fn the_store_opens_on_the_first_request_that_needs_it() {
        let urls = temp_urls("lazy");
        let path = urls.db.strip_prefix("turso:").unwrap().to_string();
        let control = direct(urls);
        assert!(
            !std::path::Path::new(&path).exists(),
            "resolving the backend must not open the store"
        );

        control
            .query(OperatorQuery::Runs { limit: 5 })
            .await
            .unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "and the first query that reads it does"
        );
    }

    #[tokio::test]
    async fn memory_transition_promotes_and_batch_reuses_one_backend() {
        let control = direct(temp_urls("memtrans"));
        // Seed two candidates through the same lazily-opened store.
        let OperatorQueryResult::Memories(initial) =
            control.query(OperatorQuery::Memories).await.unwrap()
        else {
            panic!();
        };
        assert!(initial.is_empty());
        let backend = match &control.backend {
            OperatorBackend::Direct(d) => d,
            _ => unreachable!(),
        };
        let store = backend.db().await.unwrap().clone();
        for content in ["likes tea", "works late"] {
            let mut m = Memory::new(MemoryKind::Preference, content);
            m.status = MemoryStatus::Candidate;
            MemoryRepository::save(store.as_ref(), &m).await.unwrap();
        }
        let OperatorQueryResult::Memories(seeded) =
            control.query(OperatorQuery::Memories).await.unwrap()
        else {
            panic!();
        };
        assert_eq!(seeded.len(), 2);
        // Batch: two transitions on the one resolved backend.
        for m in &seeded {
            let result = control
                .command(OperatorCommand::MemoryTransition {
                    id: m.id.clone(),
                    action: MemoryTransitionAction::Promote,
                })
                .await
                .unwrap();
            assert!(matches!(result, OperatorCommandResult::MemoryTransitioned));
        }
        let OperatorQueryResult::Memories(after) =
            control.query(OperatorQuery::Memories).await.unwrap()
        else {
            panic!();
        };
        assert!(after.iter().all(|m| m.status == MemoryStatus::Active));

        // An unknown id surfaces the same message the gateway path produces.
        let err = control
            .command(OperatorCommand::MemoryTransition {
                id: "nope".into(),
                action: MemoryTransitionAction::Reject,
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no memory with id `nope`"));
    }

    #[tokio::test]
    async fn pair_approve_unknown_code_is_not_found() {
        let control = direct(temp_urls("pair"));
        let OperatorCommandResult::PairApproved(outcome) = control
            .command(OperatorCommand::PairApprove {
                code: "ZZZZZZ".into(),
            })
            .await
            .unwrap()
        else {
            panic!();
        };
        assert!(matches!(outcome, PairApproveOutcome::NotFound));
    }

    #[tokio::test]
    async fn resume_reports_missing_and_not_recoverable() {
        let control = direct(temp_urls("resume"));
        // Nothing recoverable at all.
        let err = control
            .resume_run(None, |_, _, _| async { Ok((String::new(), false)) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no recoverable runs"));
        // An explicit unknown id.
        let err = control
            .resume_run(Some("run-x".into()), |_, _, _| async {
                Ok((String::new(), false))
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no run with id `run-x`"));
        // A finished (non-recoverable) run.
        let backend = match &control.backend {
            OperatorBackend::Direct(d) => d,
            _ => unreachable!(),
        };
        let db = backend.db().await.unwrap().clone();
        let run = Run::start("cli:test", "hello");
        let run_id = run.id.clone();
        // The ledger's rows are a projection of the session log, so this is how
        // a run gets into them.
        let projected = komo_core::domain::run_projection::ProjectedRun {
            run: run.clone(),
            steps: Vec::new(),
            start_seq: 0,
        };
        komo_core::domain::run_projection::RunProjectionStore::commit(
            db.as_ref(),
            &run.session_id,
            &[projected],
            0,
        )
        .await
        .unwrap();
        let err = control
            .resume_run(Some(run_id.clone()), |_, _, _| async {
                Ok((String::new(), false))
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is not recoverable"));

        // Interrupt-reconcile it, then resume dispatches the local turn.
        komo_core::domain::run::RunRepository::reconcile_interrupted(db.as_ref(), now())
            .await
            .unwrap();
        let outcome = control
            .resume_run(Some(run_id.clone()), |_, run, input| async move {
                assert_eq!(run.session_id, "cli:test");
                assert!(input.contains("hello"), "priming digest carries the input");
                Ok(("done".to_string(), false))
            })
            .await
            .unwrap();
        assert_eq!(outcome.run_id, run_id);
        assert_eq!(outcome.reply, "done");
        // At-most-once is the log's now, not a flag this function clears: a
        // real continuation opens with `turn/started{resumed_from}`, and the
        // projection stops offering a claimed turn (`run_projection`, plus the
        // runtime's own resume test). The stub dispatch above continues
        // nothing, so this run stays resumable — which is the honest answer.
    }
}
