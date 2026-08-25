//! `komo run rollback <id>` — undo one turn's file changes.
//!
//! Filesystem only, so unlike every other operator command this does **not**
//! go through `operator_control`: there is no db to be locked out of, and
//! routing it through the gateway would only add a hop. The checkpoint
//! directory is readable whether or not one is running.
//!
//! The rule that makes this safe to run without thinking: a file whose current
//! content is not what the run left is **skipped and named**, never restored.
//! Undoing one turn is the whole promise; quietly undoing a later turn's fix,
//! an editor save, or a `git checkout` along with it would break it.

use std::sync::Arc;

use komo_core::domain::checkpoint::{CheckpointStore, RollbackOutcome};
use komo_services::checkpoint_store::FsCheckpointStore;

pub async fn run(run_id: &str, dry_run: bool) -> anyhow::Result<()> {
    let store: Arc<dyn CheckpointStore> = Arc::new(FsCheckpointStore::new(
        komo_config::komo_home().join("checkpoints"),
    ));

    let changed = store.changed(run_id).await?;
    if changed.is_empty() {
        println!(
            "{run_id} changed no files, or its checkpoints have aged out \
             (kept for 7 days)."
        );
        return Ok(());
    }

    let mut outcomes = Vec::with_capacity(changed.len());
    for file in &changed {
        // "Is it still what the run left?" is asked for every file before any
        // of them is touched in a dry run, and immediately before each restore
        // otherwise — the check is per file, so one skipped file never blocks
        // the rest.
        let current = store.digest_of(&file.path).await.unwrap_or_default();
        if current != file.digest_after {
            outcomes.push((file, RollbackOutcome::ChangedSince));
            continue;
        }
        if dry_run {
            outcomes.push((file, RollbackOutcome::Restored));
            continue;
        }
        let outcome = match store.restore(run_id, &file.path).await {
            Ok(true) => RollbackOutcome::Restored,
            Ok(false) => RollbackOutcome::AlreadyThere,
            Err(error) => RollbackOutcome::Failed(error.to_string()),
        };
        outcomes.push((file, outcome));
    }

    let verb = if dry_run { "would restore" } else { "rollback" };
    println!("{verb} {run_id}:");
    for (file, outcome) in &outcomes {
        let note = match (file.existed_before, outcome) {
            (false, RollbackOutcome::Restored) if dry_run => "would delete (created by this run)",
            (false, RollbackOutcome::Restored) => "deleted (created by this run)",
            _ => outcome.as_str(),
        };
        println!("  {note}  {}", file.path);
        if let RollbackOutcome::Failed(why) = outcome {
            println!("      {why}");
        }
    }

    let skipped = outcomes
        .iter()
        .filter(|(_, o)| matches!(o, RollbackOutcome::ChangedSince))
        .count();
    if skipped > 0 {
        println!(
            "\n{skipped} file(s) changed after this run left them and were not touched. \
             Restoring one would discard that later change; diff it yourself and edit \
             by hand if you still want the old content."
        );
    }
    Ok(())
}
