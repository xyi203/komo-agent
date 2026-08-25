//! Learning orchestration (docs/episode-learning-framework.md §5.1): one owner
//! for *when* komo learns from what it did, which episodes the extractor sees,
//! and how the watermark and concurrency behave.
//!
//! The unit is an **episode** — one finished [`Run`] and the tool steps it
//! produced — not a session transcript. A transcript holds user and assistant
//! text only: tool results are never persisted as messages, so an extractor
//! reading one cannot tell whether a command ran, what it returned, or whether
//! the turn ended by delivering or by failing. It learns from the agent's
//! account of itself, which is the one source that cannot corroborate it.
//!
//! Two triggers share this one instance (wiring creates it once): the runtime
//! reports each finished run, and the maintenance sweep picks up whatever the
//! interval left behind. The shared instance is what makes the per-session
//! in-flight guard effective across both.
//!
//! **Ordering matters here.** Learning is dispatched *after* `runs.finish`, not
//! from inside the turn: an episode assembled while its run is still open sees a
//! status that has not been decided and steps that are still arriving.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tracing::warn;

use komo_core::domain::{
    episode::AssessedEpisode,
    repository::SessionRepository,
    reviewer::{ReviewOutcome, Reviewer},
    run::{Run, RunRepository},
    session::Session,
};
use komo_services::episode::assemble;

/// Most episodes one learning pass will read. A session that accumulated a
/// backlog (the sweep was off, or the gateway was down for a week) is learned in
/// batches this size rather than in one prompt that would be elided anyway.
const LEARN_BATCH_CAP: usize = 50;

/// How many unlearned runs the sweep pulls per cycle before grouping them by
/// session. Larger than the batch cap so one busy session cannot starve the
/// others of a turn.
const SWEEP_SCAN_CAP: usize = 200;

/// Sessions komo must never learn from: unattended sweep turns (briefings and
/// cron jobs). A sweep restates facts the agent already knows (tasks, memories,
/// device state), so extraction there re-observes the same claims on every run —
/// and each run's session is a "new independent occasion" to the consolidator,
/// which turns repetition into corroboration. The result is a memory library
/// that confirms itself on a timer.
fn exempt_from_learning(session_id: &str) -> bool {
    session_id.starts_with("briefing:") || session_id.starts_with("cron:")
}

/// Why a learning pass is being requested.
pub enum LearningTrigger {
    /// A run just finished. Learn if its session has accumulated a full
    /// interval of unlearned turns, else leave them for the sweep.
    AfterRun { run_id: String },
    /// The maintenance sweep: learn from every session holding unlearned turns,
    /// whatever the interval.
    Scheduled,
}

/// What one coordinator run accomplished, aggregated across sessions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LearningReport {
    pub sessions_learned: usize,
    pub episodes_learned: usize,
    pub memories_written: usize,
    pub skills_written: usize,
    pub tasks_captured: usize,
}

impl LearningReport {
    pub fn is_empty(&self) -> bool {
        self.sessions_learned == 0
            && self.episodes_learned == 0
            && self.memories_written == 0
            && self.skills_written == 0
            && self.tasks_captured == 0
    }

    fn absorb(&mut self, outcome: &ReviewOutcome, episodes: usize) {
        self.sessions_learned += 1;
        self.episodes_learned += episodes;
        self.memories_written += outcome.memories_written.len();
        self.skills_written += outcome.skills_written.len();
        self.tasks_captured += outcome.tasks_captured.len();
    }
}

/// The one learning orchestrator. Both trigger paths must share a single
/// instance — that is what makes the in-flight guard effective when a post-run
/// pass and a sweep reach the same session.
pub struct LearningCoordinator {
    sessions: Arc<dyn SessionRepository>,
    runs: Arc<dyn RunRepository>,
    reviewer: Arc<dyn Reviewer>,
    /// Learn once a session has this many finished turns waiting.
    interval: usize,
    /// Session ids currently being learned from (either trigger).
    in_flight: Mutex<HashSet<String>>,
}

impl LearningCoordinator {
    pub fn new(
        sessions: Arc<dyn SessionRepository>,
        runs: Arc<dyn RunRepository>,
        reviewer: Arc<dyn Reviewer>,
        interval: usize,
    ) -> Self {
        Self {
            sessions,
            runs,
            reviewer,
            interval: interval.max(1),
            in_flight: Mutex::new(HashSet::new()),
        }
    }

    /// Run one learning pass for `trigger`. Callers pass no counts or
    /// watermarks — eligibility is this module's knowledge.
    pub async fn run(&self, trigger: LearningTrigger) -> anyhow::Result<LearningReport> {
        let mut report = LearningReport::default();
        match trigger {
            LearningTrigger::AfterRun { run_id } => {
                let Some(run) = self.runs.get(&run_id).await? else {
                    return Ok(report);
                };
                if exempt_from_learning(&run.session_id) {
                    // Retire it from the backlog rather than leaving it to be
                    // re-examined and re-declined by every future sweep.
                    self.retire(&[run.id]).await;
                    return Ok(report);
                }
                let pending = self
                    .runs
                    .unlearned(Some(&run.session_id), LEARN_BATCH_CAP)
                    .await?;
                if pending.len() < self.interval {
                    return Ok(report);
                }
                self.learn_session(&run.session_id, pending, &mut report)
                    .await?;
            }
            LearningTrigger::Scheduled => {
                let pending = self.runs.unlearned(None, SWEEP_SCAN_CAP).await?;
                for (session_id, runs) in group_by_session(pending) {
                    if exempt_from_learning(&session_id) {
                        self.retire(&run_ids(&runs)).await;
                        continue;
                    }
                    // Isolate per-session failures: one bad pass must not abort
                    // the whole sweep.
                    if let Err(error) = self.learn_session(&session_id, runs, &mut report).await {
                        warn!(%error, session = %session_id, "session learning failed (skipped)");
                    }
                }
            }
        }
        Ok(report)
    }

    /// Learn from one session's pending runs, then retire exactly the batch that
    /// was considered — including the runs deliberately skipped, because
    /// "considered and declined" and "not yet considered" have to be different
    /// states or the sweep re-reads them forever.
    ///
    /// A failed pass retires nothing: the watermark stays where it is and the
    /// next sweep tries again.
    async fn learn_session(
        &self,
        session_id: &str,
        pending: Vec<Run>,
        report: &mut LearningReport,
    ) -> anyhow::Result<()> {
        // At most one pass per session at a time, across both triggers. A second
        // concurrent pass would read the same unretired runs and extract them
        // twice — two independent-looking observations of one occasion, which
        // is exactly what the consolidator counts as corroboration.
        let Some(_guard) = InFlightGuard::claim(&self.in_flight, session_id) else {
            return Ok(());
        };
        let batch: Vec<Run> = pending.into_iter().take(LEARN_BATCH_CAP).collect();
        if batch.is_empty() {
            return Ok(());
        }
        let ids = run_ids(&batch);

        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let mut episodes = Vec::new();
        for run in &batch {
            let Some(view) = assemble(&self.runs, &run.id).await? else {
                continue;
            };
            if !view.learning_eligible() {
                continue;
            }
            episodes.push(AssessedEpisode::deterministic(view, now));
        }
        if episodes.is_empty() {
            // Nothing to extract from, but the batch was still examined.
            self.retire(&ids).await;
            return Ok(());
        }

        // Identity and workspace for the aux call. Windowed to 1 because the
        // extractor reads episodes, not the transcript — loading a long
        // conversation to use two of its metadata fields is what the episode
        // path exists to stop doing.
        let Some(session) = self.sessions.find_windowed(session_id, 1).await? else {
            return Ok(());
        };
        let session = Session {
            messages: Vec::new(),
            ..session
        };

        let count = episodes.len();
        let outcome = self.reviewer.review(&session, &episodes).await?;
        report.absorb(&outcome, count);
        self.retire(&ids).await;
        Ok(())
    }

    /// Advance the watermark past `ids`. Best-effort: a failure only means those
    /// runs are offered again, and the extractor's dedup guards make a re-read
    /// harmless rather than wrong.
    async fn retire(&self, ids: &[String]) {
        if let Err(error) = self.runs.mark_learned(ids).await {
            warn!(%error, "failed to advance the learning watermark");
        }
    }
}

fn run_ids(runs: &[Run]) -> Vec<String> {
    runs.iter().map(|r| r.id.clone()).collect()
}

/// Group runs by session, preserving the oldest-first order within each session
/// and the order in which sessions first appear.
fn group_by_session(runs: Vec<Run>) -> Vec<(String, Vec<Run>)> {
    let mut grouped: Vec<(String, Vec<Run>)> = Vec::new();
    for run in runs {
        match grouped.iter_mut().find(|(id, _)| *id == run.session_id) {
            Some((_, bucket)) => bucket.push(run),
            None => grouped.push((run.session_id.clone(), vec![run])),
        }
    }
    grouped
}

/// RAII claim on a session id in the in-flight set: released on drop, so a
/// panicking or failing pass never wedges the session.
struct InFlightGuard<'a> {
    set: &'a Mutex<HashSet<String>>,
    id: String,
}

impl<'a> InFlightGuard<'a> {
    fn claim(set: &'a Mutex<HashSet<String>>, id: &str) -> Option<Self> {
        set.lock()
            .unwrap()
            .insert(id.to_string())
            .then(|| InFlightGuard {
                set,
                id: id.to_string(),
            })
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.set.lock().unwrap().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::{
        cancel::CANCELLED_ERROR,
        message::Message,
        run::{MemoryUse, RunStatus, RunStep},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A ledger of hand-built runs, recording which ids were retired.
    struct FakeRuns {
        runs: Mutex<Vec<Run>>,
        steps: Vec<RunStep>,
        marked: Mutex<Vec<String>>,
        fail_mark: bool,
    }

    impl FakeRuns {
        fn new(runs: Vec<Run>) -> Arc<Self> {
            Arc::new(Self {
                runs: Mutex::new(runs),
                steps: Vec::new(),
                marked: Mutex::new(Vec::new()),
                fail_mark: false,
            })
        }
    }

    #[async_trait]
    impl RunRepository for FakeRuns {
        async fn get(&self, id: &str) -> anyhow::Result<Option<Run>> {
            Ok(self
                .runs
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.id == id)
                .cloned())
        }
        async fn steps(&self, run_id: &str) -> anyhow::Result<Vec<RunStep>> {
            Ok(self
                .steps
                .iter()
                .filter(|s| s.run_id == run_id)
                .cloned()
                .collect())
        }
        async fn unlearned(
            &self,
            session_id: Option<&str>,
            limit: usize,
        ) -> anyhow::Result<Vec<Run>> {
            Ok(self
                .runs
                .lock()
                .unwrap()
                .iter()
                .filter(|r| !r.learned && !matches!(r.status, RunStatus::Running))
                .filter(|r| session_id.is_none_or(|s| r.session_id == s))
                .take(limit)
                .cloned()
                .collect())
        }
        async fn mark_learned(&self, run_ids: &[String]) -> anyhow::Result<()> {
            if self.fail_mark {
                anyhow::bail!("ledger offline");
            }
            let mut runs = self.runs.lock().unwrap();
            for id in run_ids {
                if let Some(run) = runs.iter_mut().find(|r| r.id == *id) {
                    run.learned = true;
                }
            }
            self.marked.lock().unwrap().extend(run_ids.iter().cloned());
            Ok(())
        }
        async fn start(&self, _run: &Run) -> anyhow::Result<()> {
            Ok(())
        }
        async fn append_step(&self, _step: &RunStep) -> anyhow::Result<()> {
            Ok(())
        }
        async fn finish(&self, _run: &Run) -> anyhow::Result<()> {
            Ok(())
        }
        async fn list(&self, _limit: usize) -> anyhow::Result<Vec<Run>> {
            Ok(Vec::new())
        }
        async fn prune(&self, _cutoff: i64) -> anyhow::Result<usize> {
            Ok(0)
        }
        async fn reconcile_interrupted(&self, _now: i64) -> anyhow::Result<usize> {
            Ok(0)
        }
        async fn mark_resumed(&self, _id: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn steps_by_tool(&self, _t: &str, _l: usize) -> anyhow::Result<Vec<RunStep>> {
            Ok(Vec::new())
        }
        async fn runs_using_memory(
            &self,
            _memory_id: &str,
            _limit: usize,
        ) -> anyhow::Result<Vec<MemoryUse>> {
            Ok(Vec::new())
        }
    }

    struct FakeSessions {
        loads: AtomicUsize,
    }

    impl FakeSessions {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                loads: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl SessionRepository for FakeSessions {
        async fn find(&self, id: &str) -> anyhow::Result<Option<Session>> {
            self.loads.fetch_add(1, Ordering::Relaxed);
            Ok(Some(Session::new(id)))
        }
        async fn find_windowed(&self, id: &str, _limit: usize) -> anyhow::Result<Option<Session>> {
            let mut s = Session::new(id);
            s.messages.push(Message::user("hi"));
            Ok(Some(s))
        }
        async fn list(&self) -> anyhow::Result<Vec<Session>> {
            Ok(Vec::new())
        }
        async fn save(&self, _session: &Session) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_empty_sessions(&self) -> anyhow::Result<usize> {
            Ok(0)
        }
        async fn rotate(&self, _session_id: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }
    }

    /// Records the episodes it was handed, per call.
    struct RecordingReviewer {
        calls: Mutex<Vec<(String, Vec<String>)>>,
        fail: bool,
        gate: Option<Arc<tokio::sync::Notify>>,
        outcome: ReviewOutcome,
    }

    impl Default for RecordingReviewer {
        fn default() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail: false,
                gate: None,
                outcome: ReviewOutcome::default(),
            }
        }
    }

    #[async_trait]
    impl Reviewer for RecordingReviewer {
        async fn review(
            &self,
            session: &Session,
            episodes: &[AssessedEpisode],
        ) -> anyhow::Result<ReviewOutcome> {
            self.calls.lock().unwrap().push((
                session.id.clone(),
                episodes.iter().map(|e| e.view.id().to_string()).collect(),
            ));
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            if self.fail {
                anyhow::bail!("aux model unavailable");
            }
            Ok(self.outcome.clone())
        }
    }

    fn run(id: &str, session: &str, status: RunStatus, error: &str) -> Run {
        let mut r = Run::start(session, "do it");
        r.id = id.into();
        r.status = status;
        r.error = error.into();
        r
    }

    fn done(id: &str, session: &str) -> Run {
        run(id, session, RunStatus::Done, "")
    }

    fn coordinator(
        runs: Arc<FakeRuns>,
        reviewer: Arc<RecordingReviewer>,
        interval: usize,
    ) -> LearningCoordinator {
        LearningCoordinator::new(FakeSessions::new(), runs, reviewer, interval)
    }

    #[tokio::test]
    async fn below_the_interval_nothing_is_learned_and_nothing_is_retired() {
        let runs = FakeRuns::new(vec![done("run-1", "cli:s"), done("run-2", "cli:s")]);
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs.clone(), reviewer.clone(), 10);

        let report = c
            .run(LearningTrigger::AfterRun {
                run_id: "run-2".into(),
            })
            .await
            .unwrap();

        assert!(report.is_empty());
        assert!(reviewer.calls.lock().unwrap().is_empty());
        assert!(
            runs.marked.lock().unwrap().is_empty(),
            "turns still waiting for a full interval must stay in the backlog"
        );
    }

    #[tokio::test]
    async fn a_full_interval_learns_every_pending_episode_oldest_first() {
        let runs = FakeRuns::new(vec![done("run-1", "cli:s"), done("run-2", "cli:s")]);
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs.clone(), reviewer.clone(), 2);

        let report = c
            .run(LearningTrigger::AfterRun {
                run_id: "run-2".into(),
            })
            .await
            .unwrap();

        assert_eq!(report.sessions_learned, 1);
        assert_eq!(report.episodes_learned, 2);
        let calls = reviewer.calls.lock().unwrap();
        assert_eq!(calls[0].0, "cli:s");
        assert_eq!(calls[0].1, vec!["run-1".to_string(), "run-2".to_string()]);
        assert_eq!(
            *runs.marked.lock().unwrap(),
            vec!["run-1".to_string(), "run-2".to_string()]
        );
    }

    /// Golden cases 11 and 12: a cancelled turn is audit, not a lesson — but it
    /// still has to leave the backlog, or every sweep re-examines it forever.
    #[tokio::test]
    async fn cancelled_runs_are_retired_without_being_extracted_from() {
        let runs = FakeRuns::new(vec![
            run("run-1", "cli:s", RunStatus::Failed, CANCELLED_ERROR),
            done("run-2", "cli:s"),
        ]);
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs.clone(), reviewer.clone(), 2);

        c.run(LearningTrigger::AfterRun {
            run_id: "run-2".into(),
        })
        .await
        .unwrap();

        let calls = reviewer.calls.lock().unwrap();
        assert_eq!(
            calls[0].1,
            vec!["run-2".to_string()],
            "the cancelled turn is not handed to the extractor"
        );
        assert_eq!(
            *runs.marked.lock().unwrap(),
            vec!["run-1".to_string(), "run-2".to_string()],
            "both are retired: the cancelled one was considered and declined"
        );
    }

    #[tokio::test]
    async fn a_batch_of_only_cancelled_runs_is_retired_without_calling_the_model() {
        let runs = FakeRuns::new(vec![
            run("run-1", "cli:s", RunStatus::Failed, CANCELLED_ERROR),
            run("run-2", "cli:s", RunStatus::Failed, CANCELLED_ERROR),
        ]);
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs.clone(), reviewer.clone(), 2);

        c.run(LearningTrigger::AfterRun {
            run_id: "run-2".into(),
        })
        .await
        .unwrap();

        assert!(reviewer.calls.lock().unwrap().is_empty());
        assert_eq!(runs.marked.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn sweep_sessions_are_never_learned_from_but_are_retired() {
        let runs = FakeRuns::new(vec![
            done("run-1", "briefing:2026-08-20"),
            done("run-2", "cron:alarm:1755600000"),
            done("run-3", "feishu:oc_1"),
        ]);
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs.clone(), reviewer.clone(), 1);

        let report = c.run(LearningTrigger::Scheduled).await.unwrap();

        assert_eq!(report.sessions_learned, 1);
        let calls = reviewer.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "feishu:oc_1");
        assert_eq!(
            runs.marked.lock().unwrap().len(),
            3,
            "the exempt runs are retired too, or the sweep re-declines them forever"
        );
    }

    #[tokio::test]
    async fn the_after_run_trigger_retires_an_exempt_run_without_scanning() {
        let runs = FakeRuns::new(vec![done("run-1", "cron:alarm:1")]);
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs.clone(), reviewer.clone(), 1);

        let report = c
            .run(LearningTrigger::AfterRun {
                run_id: "run-1".into(),
            })
            .await
            .unwrap();

        assert!(report.is_empty());
        assert!(reviewer.calls.lock().unwrap().is_empty());
        assert_eq!(*runs.marked.lock().unwrap(), vec!["run-1".to_string()]);
    }

    #[tokio::test]
    async fn the_sweep_learns_each_session_separately_whatever_the_interval() {
        let runs = FakeRuns::new(vec![
            done("run-1", "cli:a"),
            done("run-2", "cli:b"),
            done("run-3", "cli:a"),
        ]);
        let reviewer = Arc::new(RecordingReviewer::default());
        // Interval 10 — far above what either session holds; the sweep ignores it.
        let c = coordinator(runs.clone(), reviewer.clone(), 10);

        let report = c.run(LearningTrigger::Scheduled).await.unwrap();

        assert_eq!(report.sessions_learned, 2);
        assert_eq!(report.episodes_learned, 3);
        let calls = reviewer.calls.lock().unwrap();
        assert_eq!(calls[0].0, "cli:a");
        assert_eq!(calls[0].1, vec!["run-1".to_string(), "run-3".to_string()]);
        assert_eq!(calls[1].0, "cli:b");
    }

    #[tokio::test]
    async fn a_failed_pass_retires_nothing_so_the_next_sweep_retries_it() {
        let runs = FakeRuns::new(vec![done("run-1", "cli:s")]);
        let reviewer = Arc::new(RecordingReviewer {
            fail: true,
            ..Default::default()
        });
        let c = coordinator(runs.clone(), reviewer.clone(), 1);

        let _ = c.run(LearningTrigger::Scheduled).await;

        assert!(
            runs.marked.lock().unwrap().is_empty(),
            "a failed extraction must not advance the watermark"
        );
        // And the run is still on offer.
        assert_eq!(runs.unlearned(None, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_failing_session_does_not_abort_the_sweep() {
        // `cli:a` fails; the sweep must still reach `cli:b`. One reviewer that
        // fails on every call would stop both, so failure is per-session here:
        // both fail, and the test asserts both were attempted.
        let runs = FakeRuns::new(vec![done("run-1", "cli:a"), done("run-2", "cli:b")]);
        let reviewer = Arc::new(RecordingReviewer {
            fail: true,
            ..Default::default()
        });
        let c = coordinator(runs.clone(), reviewer.clone(), 1);

        let report = c.run(LearningTrigger::Scheduled).await.unwrap();

        assert!(report.is_empty());
        assert_eq!(
            reviewer.calls.lock().unwrap().len(),
            2,
            "the second session is still attempted after the first fails"
        );
    }

    #[tokio::test]
    async fn concurrent_triggers_learn_a_session_once() {
        let runs = FakeRuns::new(vec![done("run-1", "cli:s")]);
        let gate = Arc::new(tokio::sync::Notify::new());
        let reviewer = Arc::new(RecordingReviewer {
            gate: Some(gate.clone()),
            ..Default::default()
        });
        let c = Arc::new(coordinator(runs.clone(), reviewer.clone(), 1));

        let first = tokio::spawn({
            let c = c.clone();
            async move {
                c.run(LearningTrigger::AfterRun {
                    run_id: "run-1".into(),
                })
                .await
                .unwrap()
            }
        });
        for _ in 0..100 {
            if !reviewer.calls.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let during = c.run(LearningTrigger::Scheduled).await.unwrap();
        assert!(
            during.is_empty(),
            "an in-flight session is skipped: two passes over the same \
             unretired runs would extract each episode twice"
        );

        gate.notify_one();
        assert_eq!(first.await.unwrap().sessions_learned, 1);
        assert_eq!(reviewer.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_watermark_failure_still_reports_what_was_learned() {
        let mut fake = FakeRuns {
            runs: Mutex::new(vec![done("run-1", "cli:s")]),
            steps: Vec::new(),
            marked: Mutex::new(Vec::new()),
            fail_mark: false,
        };
        fake.fail_mark = true;
        let runs = Arc::new(fake);
        let reviewer = Arc::new(RecordingReviewer {
            outcome: ReviewOutcome {
                memories_written: vec!["m1".into()],
                skills_written: Vec::new(),
                tasks_captured: vec!["t1".into()],
            },
            ..Default::default()
        });
        let c = LearningCoordinator::new(FakeSessions::new(), runs, reviewer, 1);

        let report = c.run(LearningTrigger::Scheduled).await.unwrap();

        assert_eq!(report.memories_written, 1);
        assert_eq!(report.tasks_captured, 1);
        // The un-advanced watermark just means a future re-read — allowed.
    }

    #[tokio::test]
    async fn a_run_still_in_flight_is_never_offered() {
        let runs = FakeRuns::new(vec![
            run("run-1", "cli:s", RunStatus::Running, ""),
            done("run-2", "cli:s"),
        ]);
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs.clone(), reviewer.clone(), 1);

        c.run(LearningTrigger::Scheduled).await.unwrap();

        let calls = reviewer.calls.lock().unwrap();
        assert_eq!(calls[0].1, vec!["run-2".to_string()]);
    }

    #[tokio::test]
    async fn an_unknown_run_id_is_ignored() {
        let runs = FakeRuns::new(Vec::new());
        let reviewer = Arc::new(RecordingReviewer::default());
        let c = coordinator(runs, reviewer.clone(), 1);

        let report = c
            .run(LearningTrigger::AfterRun {
                run_id: "run-gone".into(),
            })
            .await
            .unwrap();

        assert!(report.is_empty());
        assert!(reviewer.calls.lock().unwrap().is_empty());
    }
}
