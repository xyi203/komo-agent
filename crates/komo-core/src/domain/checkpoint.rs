//! Undoing a turn's file changes.
//!
//! Everything else a turn does is recoverable. A memory it wrote is a
//! candidate; a skill it proposed is a candidate; a cron job it created can be
//! removed; a tool call that half-landed is marked `uncertain` so the model
//! checks rather than repeats. The filesystem was the exception: `write`,
//! `edit` and `apply_patch` produce final state, and `apply_patch` says so
//! outright — "no rollback — reports exactly what landed".
//!
//! So the bytes a file held before a run first touched it are kept, and
//! `komo run rollback` puts them back. Not a sandbox, not a snapshot of the
//! workspace: the pre-image of exactly the files that changed, which is the
//! cheap version and the one a personal agent needs far more often than
//! container isolation.
//!
//! **Recorded per run, first touch only.** Rolling back means "as it was before
//! this turn", so a file the turn edited five times restores to what it held
//! before the first edit, not the fourth.
//!
//! **What it holds now is recorded too**, on every touch. Restoring blindly
//! would discard whatever landed after the run — a later turn's fix, an editor
//! save, a `git checkout`. A file that no longer matches what the run left is
//! refused and named, rather than silently overwritten: the whole point is to
//! undo one turn, and undoing more than that is the failure mode.

use std::path::Path;

use async_trait::async_trait;

/// One file a run changed, and the state it can be returned to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedFile {
    /// Absolute path, as the mutating tool resolved it.
    pub path: String,
    /// The file existed before the run touched it. `false` means rolling back
    /// deletes it rather than restoring content.
    pub existed_before: bool,
    /// Digest of what the run left, so a rollback can tell "untouched since"
    /// from "changed by something else". Empty when the run deleted the file.
    pub digest_after: String,
}

/// Keeps pre-images so a run's file changes can be undone.
///
/// Best-effort at every call site, exactly like the run ledger: failing to
/// record a pre-image must never fail the write the user asked for. The cost of
/// that choice is a rollback that reports the file as unrecoverable, which is
/// the honest outcome and strictly better than refusing the edit.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Note that `path` changed during `run_id`.
    ///
    /// `before` is the content it held (`None` = it did not exist), kept only
    /// the first time this run touches this path. `after` is what it holds now
    /// (`None` = the run deleted it), recorded every time.
    async fn record(
        &self,
        run_id: &str,
        path: &Path,
        before: Option<&[u8]>,
        after: Option<&[u8]>,
    ) -> anyhow::Result<()>;

    /// What `run_id` changed, in the order it first touched each file.
    async fn changed(&self, run_id: &str) -> anyhow::Result<Vec<ChangedFile>>;

    /// Restore one file to its pre-run content. Returns whether anything
    /// changed on disk (`false` = it was already in that state).
    async fn restore(&self, run_id: &str, path: &str) -> anyhow::Result<bool>;

    /// Digest of `path`'s current content, in the same form as
    /// [`ChangedFile::digest_after`]; empty when the file is absent. Lives on
    /// the store so callers never re-derive the hashing rule.
    async fn digest_of(&self, path: &str) -> anyhow::Result<String>;
}

/// What a rollback did to one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackOutcome {
    Restored,
    /// Already holds its pre-run content — nothing to do.
    AlreadyThere,
    /// Changed since the run left it, so restoring would discard that change.
    /// Named rather than overwritten, and never silently.
    ChangedSince,
    Failed(String),
}

impl RollbackOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Restored => "restored",
            Self::AlreadyThere => "unchanged",
            Self::ChangedSince => "skipped (modified since)",
            Self::Failed(_) => "failed",
        }
    }
}
