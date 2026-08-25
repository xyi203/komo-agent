//! Pre-images on disk, one directory per run.
//!
//! ```text
//! <komo home>/checkpoints/<run id>/
//!     manifest.jsonl   one line per mutation, append-only
//!     <n>.pre          the bytes the n-th distinct path held before the run
//! ```
//!
//! The manifest is append-only for the same reason the transcript is: a run
//! that touches one file five times writes five lines, and the *reading* rule
//! resolves them — first line for a path carries the pre-image to restore, last
//! line carries what the run left. Rewriting a line in place would mean holding
//! a lock across the whole run, to save a few bytes nobody reads.
//!
//! Disposable, and swept on the same terms as `tool-output`: a pre-image is
//! only useful while the change it undoes is still recent, and undoing a
//! week-old turn against a repo that has moved on is not an undo.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use komo_core::domain::checkpoint::{ChangedFile, CheckpointStore};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// How long a run's pre-images stay restorable. Matches `tool_output_store`'s
/// retention — both answer "can I still go back and look at what that turn
/// did", and answering it differently in two places would only be confusing.
pub const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// One recorded mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    path: String,
    /// Index of this path's pre-image file, assigned on its first appearance.
    slot: usize,
    existed_before: bool,
    /// Digest of what the mutation left. Empty when it deleted the file.
    digest_after: String,
}

pub struct FsCheckpointStore {
    root: PathBuf,
    /// Serializes manifest appends per run: a round's tool calls run
    /// concurrently, and two interleaved appends would produce one corrupt
    /// line — or worse, hand the same slot to two different paths.
    writing: Mutex<()>,
}

impl FsCheckpointStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            writing: Mutex::new(()),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn run_dir(&self, run_id: &str) -> PathBuf {
        self.root.join(sanitize(run_id))
    }

    fn read_entries(&self, run_id: &str) -> Vec<Entry> {
        let path = self.run_dir(run_id).join("manifest.jsonl");
        let Ok(body) = std::fs::read_to_string(&path) else {
            return Vec::new();
        };
        body.lines()
            // A truncated final line (killed mid-append) is skipped, not fatal:
            // the rest of the manifest is still a usable record.
            .filter_map(|line| serde_json::from_str::<Entry>(line).ok())
            .collect()
    }

    /// Drop run directories whose manifest is older than [`RETENTION`].
    ///
    /// Called once at construction — the gateway starts, old pre-images go.
    /// Not a cron job for the same reason `tool_output_store`'s sweep is not:
    /// the directory is read only on demand.
    pub fn sweep(&self) {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return;
        };
        let cutoff = SystemTime::now() - RETENTION;
        let mut removed = 0usize;
        for entry in entries.flatten() {
            let dir = entry.path();
            if !dir.is_dir() {
                continue;
            }
            let stale = std::fs::metadata(dir.join("manifest.jsonl"))
                .and_then(|m| m.modified())
                .map(|m| m < cutoff)
                // A run directory with no readable manifest is residue.
                .unwrap_or(true);
            if stale && std::fs::remove_dir_all(&dir).is_ok() {
                removed += 1;
            }
        }
        if removed > 0 {
            debug!(removed, "swept expired checkpoints");
        }
    }
}

/// A run id is a `run-<uuid>`, but the id is the caller's string and this
/// builds a path from it — so anything that could climb out of the root is
/// replaced rather than trusted.
fn sanitize(run_id: &str) -> String {
    run_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[async_trait]
impl CheckpointStore for FsCheckpointStore {
    async fn record(
        &self,
        run_id: &str,
        path: &Path,
        before: Option<&[u8]>,
        after: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let dir = self.run_dir(run_id);
        let path_key = path.display().to_string();
        let digest_after = after.map(digest).unwrap_or_default();

        let _guard = self.writing.lock().unwrap();
        std::fs::create_dir_all(&dir)?;
        let entries = self.read_entries(run_id);
        let slot = match entries.iter().find(|e| e.path == path_key) {
            // Already captured: rolling back means "as it was before this run",
            // so the fourth edit's pre-image is not what anyone wants back.
            Some(existing) => existing.slot,
            None => {
                let slot = entries.iter().map(|e| e.slot + 1).max().unwrap_or(0);
                if let Some(bytes) = before {
                    std::fs::write(dir.join(format!("{slot}.pre")), bytes)?;
                }
                slot
            }
        };
        let entry = Entry {
            path: path_key,
            slot,
            existed_before: entries
                .iter()
                .find(|e| e.slot == slot)
                .map(|e| e.existed_before)
                .unwrap_or(before.is_some()),
            digest_after,
        };
        let line = serde_json::to_string(&entry)?;
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("manifest.jsonl"))?;
        writeln!(file, "{line}")?;
        Ok(())
    }

    async fn changed(&self, run_id: &str) -> anyhow::Result<Vec<ChangedFile>> {
        let entries = self.read_entries(run_id);
        // First appearance sets the order and `existed_before`; the last one
        // for that path says what the run ultimately left there.
        let mut order: Vec<String> = Vec::new();
        let mut latest: HashMap<String, Entry> = HashMap::new();
        for entry in entries {
            if !latest.contains_key(&entry.path) {
                order.push(entry.path.clone());
            }
            let existed_before = latest
                .get(&entry.path)
                .map(|e| e.existed_before)
                .unwrap_or(entry.existed_before);
            latest.insert(
                entry.path.clone(),
                Entry {
                    existed_before,
                    ..entry
                },
            );
        }
        Ok(order
            .into_iter()
            .filter_map(|path| {
                latest.get(&path).map(|e| ChangedFile {
                    path,
                    existed_before: e.existed_before,
                    digest_after: e.digest_after.clone(),
                })
            })
            .collect())
    }

    async fn restore(&self, run_id: &str, path: &str) -> anyhow::Result<bool> {
        let Some(entry) = self
            .read_entries(run_id)
            .into_iter()
            .find(|e| e.path == path)
        else {
            anyhow::bail!("{path} was not changed by {run_id}");
        };
        let target = PathBuf::from(path);
        if !entry.existed_before {
            // The run created it; undoing that is deleting it.
            return Ok(std::fs::remove_file(&target).is_ok());
        }
        let pre = self.run_dir(run_id).join(format!("{}.pre", entry.slot));
        let bytes = std::fs::read(&pre)
            .map_err(|e| anyhow::anyhow!("pre-image for {path} is unreadable: {e}"))?;
        if std::fs::read(&target).is_ok_and(|current| current == bytes) {
            return Ok(false);
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&target, &bytes)?;
        Ok(true)
    }

    async fn digest_of(&self, path: &str) -> anyhow::Result<String> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(digest(&bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(e) => {
                warn!(%e, path, "could not digest a checkpointed file");
                Err(e.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (FsCheckpointStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (FsCheckpointStore::new(dir.path().join("checkpoints")), dir)
    }

    #[tokio::test]
    async fn the_first_touch_is_the_one_kept() {
        let (store, dir) = store();
        let file = dir.path().join("a.txt");
        std::fs::write(&file, b"original").unwrap();

        store
            .record("run-1", &file, Some(b"original"), Some(b"second"))
            .await
            .unwrap();
        store
            .record("run-1", &file, Some(b"second"), Some(b"third"))
            .await
            .unwrap();
        std::fs::write(&file, b"third").unwrap();

        let changed = store.changed("run-1").await.unwrap();
        assert_eq!(changed.len(), 1, "one file, however many edits");
        assert_eq!(changed[0].digest_after, digest(b"third"));

        store
            .restore("run-1", &file.display().to_string())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&file).unwrap(),
            b"original",
            "rollback means before the run, not before the last edit"
        );
    }

    #[tokio::test]
    async fn a_file_the_run_created_is_deleted_on_rollback() {
        let (store, dir) = store();
        let file = dir.path().join("new.txt");
        store
            .record("run-1", &file, None, Some(b"hello"))
            .await
            .unwrap();
        std::fs::write(&file, b"hello").unwrap();

        let changed = store.changed("run-1").await.unwrap();
        assert!(!changed[0].existed_before);

        store
            .restore("run-1", &file.display().to_string())
            .await
            .unwrap();
        assert!(!file.exists(), "undoing a creation is a deletion");
    }

    /// Even when a later edit in the same run rewrote it — the first entry is
    /// what says the run created the file, and the later entries must not
    /// overwrite that with "it existed".
    #[tokio::test]
    async fn creation_survives_a_later_edit_in_the_same_run() {
        let (store, dir) = store();
        let file = dir.path().join("new.txt");
        store
            .record("run-1", &file, None, Some(b"v1"))
            .await
            .unwrap();
        store
            .record("run-1", &file, Some(b"v1"), Some(b"v2"))
            .await
            .unwrap();

        let changed = store.changed("run-1").await.unwrap();
        assert!(
            !changed[0].existed_before,
            "the run still created this file; rollback must delete it"
        );
    }

    #[tokio::test]
    async fn restoring_content_that_is_already_there_reports_no_change() {
        let (store, dir) = store();
        let file = dir.path().join("a.txt");
        std::fs::write(&file, b"same").unwrap();
        store
            .record("run-1", &file, Some(b"same"), Some(b"same"))
            .await
            .unwrap();

        assert!(
            !store
                .restore("run-1", &file.display().to_string())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn two_files_get_separate_pre_images() {
        let (store, dir) = store();
        let a = dir.path().join("a.txt");
        let b = dir.path().join("b.txt");
        store
            .record("run-1", &a, Some(b"A"), Some(b"a2"))
            .await
            .unwrap();
        store
            .record("run-1", &b, Some(b"B"), Some(b"b2"))
            .await
            .unwrap();
        std::fs::write(&a, b"a2").unwrap();
        std::fs::write(&b, b"b2").unwrap();

        store
            .restore("run-1", &a.display().to_string())
            .await
            .unwrap();
        store
            .restore("run-1", &b.display().to_string())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&a).unwrap(), b"A");
        assert_eq!(std::fs::read(&b).unwrap(), b"B");
    }

    #[tokio::test]
    async fn runs_do_not_share_a_directory() {
        let (store, dir) = store();
        let file = dir.path().join("a.txt");
        store
            .record("run-1", &file, Some(b"one"), Some(b"two"))
            .await
            .unwrap();
        store
            .record("run-2", &file, Some(b"two"), Some(b"three"))
            .await
            .unwrap();
        std::fs::write(&file, b"three").unwrap();

        store
            .restore("run-2", &file.display().to_string())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&file).unwrap(),
            b"two",
            "run-2 undoes to run-2's before"
        );
        store
            .restore("run-1", &file.display().to_string())
            .await
            .unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"one");
    }

    #[tokio::test]
    async fn a_run_id_cannot_escape_the_checkpoint_root() {
        let (store, dir) = store();
        let file = dir.path().join("a.txt");
        store
            .record("../../etc", &file, Some(b"x"), Some(b"y"))
            .await
            .unwrap();
        assert!(
            store.root().join("______etc").exists(),
            "path separators in a run id are neutralized, not followed"
        );
    }

    #[tokio::test]
    async fn restoring_a_file_the_run_never_touched_is_an_error() {
        let (store, _dir) = store();
        assert!(store.restore("run-1", "/tmp/never").await.is_err());
    }

    #[tokio::test]
    async fn an_unknown_run_changed_nothing() {
        let (store, _dir) = store();
        assert!(store.changed("run-nope").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_truncated_manifest_line_does_not_lose_the_rest() {
        let (store, dir) = store();
        let file = dir.path().join("a.txt");
        store
            .record("run-1", &file, Some(b"A"), Some(b"a2"))
            .await
            .unwrap();
        // Simulate a kill mid-append.
        let manifest = store.run_dir("run-1").join("manifest.jsonl");
        let mut body = std::fs::read_to_string(&manifest).unwrap();
        body.push_str("{\"path\":\"trunc");
        std::fs::write(&manifest, body).unwrap();

        let changed = store.changed("run-1").await.unwrap();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].path, file.display().to_string());
    }

    #[tokio::test]
    async fn the_sweep_drops_expired_runs_and_keeps_fresh_ones() {
        let (store, dir) = store();
        let file = dir.path().join("a.txt");
        store
            .record("run-old", &file, Some(b"A"), Some(b"a2"))
            .await
            .unwrap();
        store
            .record("run-new", &file, Some(b"a2"), Some(b"a3"))
            .await
            .unwrap();

        // Age the old run's manifest past the retention window.
        let old = store.run_dir("run-old").join("manifest.jsonl");
        let stale = SystemTime::now() - RETENTION - Duration::from_secs(60);
        filetime::set_file_mtime(&old, filetime::FileTime::from_system_time(stale)).unwrap();

        store.sweep();
        assert!(!store.run_dir("run-old").exists());
        assert!(store.run_dir("run-new").exists());
    }
}
