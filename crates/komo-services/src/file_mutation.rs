//! File-mutation primitives shared by `write` (and next, `edit` /
//! `apply_patch`).
//!
//! The one non-obvious guarantee here is **stale protection**. A write goes:
//! read the current bytes → ask the user → write. That middle step can take
//! minutes (a chat approval), and the file can change under it — an editor save,
//! a `git checkout`, another agent turn. Writing anyway silently discards
//! whatever landed in between. So the write re-reads and compares against the
//! snapshot taken before the prompt, and refuses when it moved. Borrowed from
//! opencode v2's `FileMutation.writeIfUnchanged`.
//!
//! Scope is deliberately narrow: this closes the *approval window*, not a
//! cross-turn one. There is no "the model must have read the file first" rule.
//!
//! The second guarantee is **per-path serialization** of the mutations
//! themselves — see [`WRITE_LOCKS`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use komo_core::domain::context::RunContext;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Per-path write locks.
///
/// The snapshot-compare above is only atomic if nothing can slip between the
/// compare and the write. A round's tool calls run **concurrently**
/// (`ToolExecutor::execute_round` spawns them and joins), so two calls mutating
/// the same file each snapshot the same bytes, each find them unchanged, and
/// each write: the first mutation is lost silently and *both* report success to
/// the model. Holding this lock across compare+write turns that into "one wins,
/// the other gets [`StaleContent`]" — a visible error the model can act on.
///
/// Per path, not global: writes to different files stay parallel, and reads
/// never take it at all. Keys are the lexically-normalized absolute paths every
/// mutating tool already resolves to (`Workspace::resolve_contained`), so
/// `a.rs`, `./a.rs` and `sub/../a.rs` share one lock. Aliases the resolver
/// cannot see through (symlinks, case-insensitive filesystems) are out of
/// scope — the snapshot compare is still the backstop there.
///
/// Values are `Weak`, so a path's entry disappears once its last writer is
/// done; the map does not grow with the number of files ever touched.
static WRITE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Acquire `path`'s write lock, holding it until the returned guard drops.
async fn lock_path(path: &Path) -> OwnedMutexGuard<()> {
    let lock = {
        let mut locks = WRITE_LOCKS.lock().unwrap();
        // Reclaim entries whose writers are all gone. Safe to do before the
        // lookup: a live acquisition always holds an `Arc` (the local below, or
        // the guard), so it can never drop the entry someone is waiting on.
        locks.retain(|_, waiters| waiters.strong_count() > 0);
        match locks.get(path).and_then(Weak::upgrade) {
            Some(existing) => existing,
            None => {
                let fresh = Arc::new(AsyncMutex::new(()));
                locks.insert(path.to_path_buf(), Arc::downgrade(&fresh));
                fresh
            }
        }
    };
    lock.lock_owned().await
}

/// The file's state at snapshot time — `None` when it did not exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot(Option<Vec<u8>>);

impl Snapshot {
    pub fn existed(&self) -> bool {
        self.0.is_some()
    }

    /// The snapshot decoded as text, with any UTF-8 BOM stripped — the form an
    /// `edit` matches against (the model never sends a BOM, and
    /// [`write_if_unchanged`] puts it back). `None` when the file was absent or
    /// isn't valid UTF-8.
    pub fn text(&self) -> Option<String> {
        let bytes = self.0.as_deref()?;
        let body = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
        String::from_utf8(body.to_vec()).ok()
    }

    /// The raw bytes, or `None` when the file did not exist — what a checkpoint
    /// keeps, verbatim, since a pre-image has to restore a binary file as
    /// faithfully as a text one.
    pub fn bytes(&self) -> Option<&[u8]> {
        self.0.as_deref()
    }

    /// Whether the snapshot starts with a UTF-8 BOM.
    fn had_bom(&self) -> bool {
        self.0
            .as_deref()
            .is_some_and(|b| b.starts_with(&[0xef, 0xbb, 0xbf]))
    }
}

/// The file changed between the snapshot and the write; nothing was written.
#[derive(Debug)]
pub struct StaleContent {
    pub path: String,
}

impl std::fmt::Display for StaleContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} changed after the approval prompt; nothing was written. \
             Read it again before writing.",
            self.path
        )
    }
}

impl std::error::Error for StaleContent {}

/// Capture the file's current bytes (absent is not an error — a `write` may be
/// creating it).
pub async fn snapshot(path: &Path) -> anyhow::Result<Snapshot> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Snapshot(Some(bytes))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Snapshot(None)),
        Err(e) => Err(anyhow::anyhow!("failed to read {}: {e}", path.display())),
    }
}

/// Write `content` only if the file still matches `expected`.
///
/// A UTF-8 BOM present in the snapshot is re-applied when the new content lacks
/// one: the model never sends a BOM, and silently stripping it would rewrite
/// every line ending of a Windows-authored file's first line in git.
pub async fn write_if_unchanged(
    path: &Path,
    expected: &Snapshot,
    content: &str,
    run: Option<&RunContext>,
) -> anyhow::Result<()> {
    // Held across compare+write: without it two concurrent callers can both
    // pass the compare and both write (see [`WRITE_LOCKS`]).
    let _guard = lock_path(path).await;
    let current = snapshot(path).await?;
    if &current != expected {
        return Err(StaleContent {
            path: path.display().to_string(),
        }
        .into());
    }

    let payload = if expected.had_bom() && !content.starts_with('\u{feff}') {
        format!("\u{feff}{content}")
    } else {
        content.to_string()
    };

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| anyhow::anyhow!("failed to create {}: {e}", parent.display()))?;
    }
    tokio::fs::write(path, payload.as_bytes())
        .await
        .map_err(|e| anyhow::anyhow!("failed to write {}: {e}", path.display()))?;
    // Still under the per-path lock, so the pre-image recorded is exactly the
    // bytes this write replaced — not something an interleaved writer left.
    checkpoint(run, path, expected.bytes(), Some(payload.as_bytes())).await;
    Ok(())
}

/// Record a mutation for `komo run rollback`, if this run is checkpointed.
///
/// Best-effort by design, and deliberately *after* the mutation: a write the
/// user asked for must not fail because a pre-image could not be filed. The
/// cost is a rollback that reports the file as unrecoverable, which is honest —
/// unlike refusing the edit, which is not what anyone asked for.
async fn checkpoint(
    run: Option<&RunContext>,
    path: &Path,
    before: Option<&[u8]>,
    after: Option<&[u8]>,
) {
    let Some(store) = run.and_then(|r| r.checkpoint()) else {
        return;
    };
    let run_id = run.map(|r| r.run_id.as_str()).unwrap_or_default();
    if let Err(error) = store.record(run_id, path, before, after).await {
        tracing::warn!(%error, path = %path.display(), "failed to checkpoint a file change");
    }
}

/// Delete `path`, erroring if it is already gone (the caller named a specific
/// file). Takes the same per-path lock a write does, so a delete can never land
/// between a concurrent write's compare and its write — otherwise that write
/// would recreate the file the model just asked to remove.
pub async fn delete_existing(path: &Path, run: Option<&RunContext>) -> anyhow::Result<()> {
    let _guard = lock_path(path).await;
    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        anyhow::bail!("{} does not exist, so it cannot be deleted", path.display());
    }
    // Read before removing: a deletion's pre-image is the whole file, and once
    // the unlink lands there is nothing left to read.
    let before = snapshot(path).await.ok();
    tokio::fs::remove_file(path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete {}: {e}", path.display()))?;
    checkpoint(run, path, before.as_ref().and_then(|s| s.bytes()), None).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("komo_filemut_{tag}"));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[tokio::test]
    async fn writes_when_the_file_is_unchanged() {
        let p = temp("unchanged");
        std::fs::write(&p, "old").unwrap();
        let snap = snapshot(&p).await.unwrap();
        write_if_unchanged(&p, &snap, "new", None).await.unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "new");
    }

    /// The concurrency guarantee: two calls in one round mutating the same
    /// file both snapshot the same bytes and race to write. Exactly one may
    /// land — the loser must be told its snapshot went stale, not silently
    /// overwrite the winner while reporting success.
    #[tokio::test]
    async fn concurrent_writes_to_one_path_never_both_land() {
        let p = temp("concurrent_same");
        std::fs::write(&p, "original").unwrap();
        let snap = snapshot(&p).await.unwrap();

        let first = {
            let (p, snap) = (p.clone(), snap.clone());
            tokio::spawn(async move { write_if_unchanged(&p, &snap, "from A", None).await })
        };
        let second = {
            let (p, snap) = (p.clone(), snap.clone());
            tokio::spawn(async move { write_if_unchanged(&p, &snap, "from B", None).await })
        };
        let (first, second) = (first.await.unwrap(), second.await.unwrap());

        assert_eq!(
            [first.is_ok(), second.is_ok()]
                .iter()
                .filter(|ok| **ok)
                .count(),
            1,
            "exactly one of the two writes may land"
        );
        let landed = std::fs::read_to_string(&p).unwrap();
        assert!(landed == "from A" || landed == "from B", "got {landed:?}");

        let loser = if first.is_err() { first } else { second };
        assert!(
            loser.unwrap_err().downcast_ref::<StaleContent>().is_some(),
            "the loser must report staleness, not a generic failure"
        );
    }

    /// The lock is per path, not global: unrelated files still write in
    /// parallel, which is the whole point of running a round concurrently.
    #[tokio::test]
    async fn concurrent_writes_to_different_paths_both_land() {
        let (a, b) = (temp("concurrent_a"), temp("concurrent_b"));
        let empty = snapshot(&a).await.unwrap();

        let (ra, rb) = tokio::join!(
            write_if_unchanged(&a, &empty, "a", None),
            write_if_unchanged(&b, &empty, "b", None),
        );
        ra.unwrap();
        rb.unwrap();
        assert_eq!(std::fs::read_to_string(&a).unwrap(), "a");
        assert_eq!(std::fs::read_to_string(&b).unwrap(), "b");
    }

    #[tokio::test]
    async fn deleting_a_missing_file_is_an_error() {
        let p = temp("delete_missing");
        let err = delete_existing(&p, None).await.unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn refuses_when_the_file_moved_under_us() {
        let p = temp("moved");
        std::fs::write(&p, "old").unwrap();
        let snap = snapshot(&p).await.unwrap();
        // Someone else saves the file while the approval prompt is up.
        std::fs::write(&p, "theirs").unwrap();

        let err = write_if_unchanged(&p, &snap, "mine", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("changed after the approval"));
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            "theirs",
            "their write must survive"
        );
    }

    #[tokio::test]
    async fn creating_a_file_expects_it_absent() {
        let p = temp("create");
        let snap = snapshot(&p).await.unwrap();
        assert!(!snap.existed());
        write_if_unchanged(&p, &snap, "fresh", None).await.unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "fresh");
        let _ = std::fs::remove_file(&p);
    }

    /// A file appearing between snapshot and write is stale too — otherwise a
    /// "create" would silently clobber whatever just landed there.
    #[tokio::test]
    async fn a_file_appearing_after_the_snapshot_is_stale() {
        let p = temp("appeared");
        let snap = snapshot(&p).await.unwrap();
        std::fs::write(&p, "someone else got here first").unwrap();
        assert!(write_if_unchanged(&p, &snap, "mine", None).await.is_err());
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            "someone else got here first"
        );
    }

    #[tokio::test]
    async fn a_bom_survives_a_rewrite() {
        let p = temp("bom");
        std::fs::write(&p, "\u{feff}old").unwrap();
        let snap = snapshot(&p).await.unwrap();
        write_if_unchanged(&p, &snap, "new", None).await.unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "\u{feff}new");
    }
}
