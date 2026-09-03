//! Assembling an [`EpisodeView`] from the run ledger
//! (docs/episode-learning-framework.md §5.2).
//!
//! One rule lives here so its callers do not each re-derive it: **an episode is
//! only ever a finished run**. Reading a run that is still in flight would hand
//! the learning pass a turn whose steps are still arriving and whose status has
//! not been decided — it would learn from half a turn and record the reason as
//! "unknown", which is indistinguishable from a turn that genuinely ended
//! unresolved.
//!
//! The view carries the ledger's own text, redaction and truncation included.
//! There is deliberately no path back to the un-redacted originals: the
//! learning pass is not a reason to widen what a tool's arguments are allowed
//! to expose.

use std::sync::Arc;

use komo_core::domain::{
    episode::EpisodeView,
    run::{RunRepository, RunStatus},
};

/// Load one finished run and its steps.
///
/// `None` means "there is no episode here": the run id is unknown, or the run
/// has not finished. Both are ordinary — a learning trigger can race a run that
/// a crash left `Running` — so neither is an error.
pub async fn assemble(
    runs: &Arc<dyn RunRepository>,
    run_id: &str,
) -> anyhow::Result<Option<EpisodeView>> {
    let Some(run) = runs.get(run_id).await? else {
        return Ok(None);
    };
    if matches!(run.status, RunStatus::Running) {
        return Ok(None);
    }
    // Steps come back ordered by `seq` (the repository's contract) and scoped to
    // this run id, which is what keeps a neighbouring turn's tool calls from
    // being read as part of this one's story.
    let steps = runs.steps(&run.id).await?;
    Ok(Some(EpisodeView { run, steps }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use komo_core::domain::run::{MemoryUse, Run, RunStep};

    /// A ledger holding hand-built runs and steps, keyed by run id.
    struct FakeRuns {
        runs: Vec<Run>,
        steps: Vec<RunStep>,
    }

    impl FakeRuns {
        fn arc(runs: Vec<Run>, steps: Vec<RunStep>) -> Arc<dyn RunRepository> {
            Arc::new(Self { runs, steps })
        }
    }

    #[async_trait]
    impl RunRepository for FakeRuns {
        async fn get(&self, id: &str) -> anyhow::Result<Option<Run>> {
            Ok(self.runs.iter().find(|r| r.id == id).cloned())
        }
        async fn steps(&self, run_id: &str) -> anyhow::Result<Vec<RunStep>> {
            Ok(self
                .steps
                .iter()
                .filter(|s| s.run_id == run_id)
                .cloned()
                .collect())
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
        async fn steps_by_tool(&self, _tool: &str, _limit: usize) -> anyhow::Result<Vec<RunStep>> {
            Ok(Vec::new())
        }
        async fn unlearned(
            &self,
            _session_id: Option<&str>,
            _limit: usize,
        ) -> anyhow::Result<Vec<Run>> {
            Ok(Vec::new())
        }
        async fn mark_learned(&self, _run_ids: &[String]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn set_outcome(&self, _run_id: &str, _outcome: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn previous_in_session(&self, _run_id: &str) -> anyhow::Result<Option<Run>> {
            Ok(None)
        }
        async fn runs_using_memory(
            &self,
            _memory_id: &str,
            _limit: usize,
        ) -> anyhow::Result<Vec<MemoryUse>> {
            Ok(Vec::new())
        }
    }

    fn run(id: &str, status: RunStatus) -> Run {
        let mut r = Run::start("cli:s", "do it");
        r.id = id.to_string();
        r.status = status;
        r
    }

    fn step(run_id: &str, seq: i64) -> RunStep {
        RunStep {
            run_id: run_id.into(),
            seq,
            tool_name: "shell".into(),
            args: "{}".into(),
            result: String::new(),
            error: String::new(),
            ok: true,
            uncertain: false,
            started_at: 0,
            ended_at: 0,
            elapsed_ms: 0,
            structured: serde_json::Value::Null,
            output_paths: Vec::new(),
            approved_by: String::new(),
            approval_waited_ms: 0,
        }
    }

    #[tokio::test]
    async fn assembles_a_finished_run_with_only_its_own_steps() {
        let runs = FakeRuns::arc(
            vec![run("run-a", RunStatus::Done), run("run-b", RunStatus::Done)],
            vec![step("run-a", 1), step("run-b", 1), step("run-a", 2)],
        );

        let episode = assemble(&runs, "run-a").await.unwrap().unwrap();

        assert_eq!(episode.id(), "run-a");
        assert_eq!(episode.steps.len(), 2);
        assert!(
            episode.steps.iter().all(|s| s.run_id == "run-a"),
            "a neighbouring run's steps must never be read as part of this episode"
        );
    }

    #[tokio::test]
    async fn a_run_still_in_flight_is_not_an_episode_yet() {
        let runs = FakeRuns::arc(
            vec![run("run-a", RunStatus::Running)],
            vec![step("run-a", 1)],
        );

        assert!(
            assemble(&runs, "run-a").await.unwrap().is_none(),
            "learning from a turn whose status is undecided would record \
             half a turn as a finished one"
        );
    }

    #[tokio::test]
    async fn a_failed_run_is_a_real_episode() {
        let runs = FakeRuns::arc(vec![run("run-a", RunStatus::Failed)], Vec::new());
        assert!(assemble(&runs, "run-a").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn an_unknown_run_id_is_absence_not_failure() {
        let runs = FakeRuns::arc(Vec::new(), Vec::new());
        assert!(assemble(&runs, "run-gone").await.unwrap().is_none());
    }
}
