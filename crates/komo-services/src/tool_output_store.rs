//! Over-limit tool output, kept on disk instead of thrown away.
//!
//! The executor caps what a tool result may cost the model's context. That cap
//! used to be a **one-sided** truncation: everything past it was gone for good.
//! But a compiler's error count, a test run's failure summary, a stack trace's
//! innermost frame — the part that answers the question is usually at the *end*.
//!
//! So an over-limit result is written out in full and the model is handed a
//! **head + tail preview** plus the file's path, which it can page through with
//! `read` or search with `grep` (both accept paths under [`root`](Self::root) —
//! see `tools/fs_common::resolve_readable`). Modelled on opencode v2's
//! `tool-output-store.ts`.
//!
//! Files live under `<komo home>/tool-output/<session>/<call>.txt` and are kept
//! for [`RETENTION`]. Nothing else is written beside them: a session directory
//! used to carry an `index.jsonl` naming what produced each file, but it had no
//! reader in the codebase and every field it held (tool, size, path) is already
//! on the run ledger's step for that call. The sweep removes a leftover one.
//!
//! Cleanup is not a cron job: the gateway sweeps once at
//! startup and the store re-sweeps at most hourly while it writes, which is
//! plenty for a directory nobody reads except on demand.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use tracing::{debug, warn};

/// How long a stored output stays readable. Long enough that a model can come
/// back to it later in a session (or the operator can, from `run inspect`),
/// short enough that a chatty week doesn't accumulate.
pub const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Minimum gap between retention sweeps. The sweep is cheap but pointless to
/// repeat per call.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Upper bound on lines in a preview (v2's `MAX_LINES`), split evenly between
/// head and tail. A result can be under the byte budget yet be thousands of
/// one-word lines; a screen of those teaches the model less than the two ends do.
const MAX_PREVIEW_LINES: usize = 2000;

/// Per-session metadata journal: one JSON line per stored output.
/// A metadata journal earlier komo versions wrote beside the outputs. Nothing
/// writes it any more; the sweep deletes one it finds so the directory holds
/// only outputs.
const LEGACY_INDEX_FILE: &str = "index.jsonl";

/// A tool result sized for the model, plus the full-output files it left behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bounded {
    /// What the model sees: the result verbatim, or a head+tail preview.
    pub text: String,
    /// Absolute paths of full outputs written for this call. Empty in the common
    /// case — nothing is written unless the result was over the limit.
    pub output_paths: Vec<PathBuf>,
}

impl Bounded {
    /// The result fit; nothing was written.
    fn passthrough(text: String) -> Self {
        Self {
            text,
            output_paths: Vec::new(),
        }
    }
}

pub struct ToolOutputStore {
    root: PathBuf,
    last_sweep: Mutex<Option<Instant>>,
}

impl ToolOutputStore {
    /// A store rooted at `root` (`<komo home>/tool-output`). The directory is
    /// created lazily, on the first over-limit result — an agent that never
    /// overflows never creates it.
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            last_sweep: Mutex::new(None),
        }
    }

    /// The managed directory. `read` and `grep` treat this as a **read-only**
    /// root outside the workspace, which is what makes a preview's path
    /// actionable; no write tool can reach it (they resolve against the
    /// workspace roots only).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Bound `output` to `cap` bytes for the model.
    ///
    /// Under the cap: returned untouched, no I/O at all. Over it: the full text
    /// is written to `<root>/<session>/<call>.txt` and the model gets a
    /// head+tail preview naming that file. If the write fails for any reason the
    /// result degrades to the previous behavior — a plain truncation — because a
    /// full disk must not turn a working tool call into a failure.
    pub fn bound(&self, session_id: &str, call_id: &str, output: String, cap: usize) -> Bounded {
        if output.len() <= cap {
            return Bounded::passthrough(output);
        }
        let file = format!("{}.txt", sanitize(call_id));
        let dir = self.root.join(sanitize(session_id));
        let path = dir.join(&file);
        match write_full(&path, &output) {
            Ok(()) => {
                self.maybe_sweep();
                Bounded {
                    text: preview(&output, cap, &path),
                    output_paths: vec![path],
                }
            }
            Err(error) => {
                warn!(%error, path = %path.display(), "could not store over-limit tool output; truncating instead");
                Bounded::passthrough(truncated(&output, cap))
            }
        }
    }

    /// Keep `output` in full under `key`, whatever its size, and answer where.
    ///
    /// [`bound`](Self::bound) writes only what overflows a round's budget; a
    /// background task has no round to overflow — its output arrives after the
    /// turn is over, and the wake that reports it carries a summary plus this
    /// path. Same directory, same retention, same read gate, so a
    /// `task/settled`'s `result_ref` is a path the model can already `read` and
    /// `grep`.
    ///
    /// `None` when it could not be written: a task still settles, it just
    /// settles with nothing to point at.
    pub fn store(&self, session_id: &str, key: &str, output: &str) -> Option<PathBuf> {
        let path = self
            .root
            .join(sanitize(session_id))
            .join(format!("{}.txt", sanitize(key)));
        match write_full(&path, output) {
            Ok(()) => {
                self.maybe_sweep();
                Some(path)
            }
            Err(error) => {
                warn!(%error, path = %path.display(), "could not store a background task's output");
                None
            }
        }
    }

    /// Delete stored outputs past [`RETENTION`], and any session directory left
    /// empty. Best-effort: an unreadable entry is skipped, never fatal. Returns
    /// how many files were removed.
    pub fn sweep(&self) -> usize {
        let Ok(sessions) = std::fs::read_dir(&self.root) else {
            return 0; // nothing stored yet
        };
        let mut removed = 0;
        for session in sessions.flatten() {
            let dir = session.path();
            let Ok(files) = std::fs::read_dir(&dir) else {
                continue;
            };
            let mut left = 0;
            for file in files.flatten() {
                // A leftover index from a komo that still wrote one: it is not
                // an output, nothing reads it, and it must not keep an
                // otherwise-empty directory alive.
                if file.file_name().to_str() == Some(LEGACY_INDEX_FILE) {
                    let _ = std::fs::remove_file(file.path());
                    continue;
                }
                if expired(&file.path()) {
                    match std::fs::remove_file(file.path()) {
                        Ok(()) => removed += 1,
                        Err(error) => {
                            debug!(%error, "could not remove expired tool output");
                            left += 1;
                        }
                    }
                } else {
                    left += 1;
                }
            }
            if left == 0 {
                let _ = std::fs::remove_dir(&dir);
            }
        }
        if removed > 0 {
            debug!(removed, "swept expired tool outputs");
        }
        // A sweep from any path counts against the debounce.
        *self.last_sweep.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        removed
    }

    fn maybe_sweep(&self) {
        let mut last = self.last_sweep.lock().unwrap_or_else(|e| e.into_inner());
        if last.is_some_and(|at| at.elapsed() < SWEEP_INTERVAL) {
            return;
        }
        // Claim the slot before sweeping so two concurrent calls don't both run.
        *last = Some(Instant::now());
        drop(last);
        self.sweep();
    }
}

fn write_full(path: &Path, output: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, output)
}

fn expired(path: &Path) -> bool {
    let Ok(modified) = std::fs::metadata(path).and_then(|m| m.modified()) else {
        return false; // can't tell how old it is ⇒ leave it alone
    };
    SystemTime::now()
        .duration_since(modified)
        .is_ok_and(|age| age > RETENTION)
}

/// A session or call id as one path segment: only `[A-Za-z0-9_-]` survives,
/// everything else becomes `-`. Session ids carry a platform prefix
/// (`feishu:oc_x`), and a colon is a path separator's cousin on enough systems to
/// not risk it. Dots go too — that is what keeps `..` and a leading `.` from
/// ever reaching the filesystem, rather than a special case per shape.
///
/// Shared with [`crate::artifact_store`]: both turn the same session id into the
/// same directory name, and two spellings of that would be two directories.
pub(crate) fn sanitize(id: &str) -> String {
    let cleaned: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '-'
            }
        })
        .collect();
    if cleaned.is_empty() {
        return "unknown".to_string();
    }
    cleaned
}

/// The old behavior, kept for the degraded path: cut at a char boundary and say
/// the rest is gone.
fn truncated(output: &str, cap: usize) -> String {
    let mut out = output[..floor_boundary(output, cap)].to_string();
    out.push_str(&format!(
        "\n\n…[truncated: result exceeded the {} KB tool-result limit. Re-run with \
         a narrower query — a filter, a specific id, or a smaller range — to see the rest.]",
        cap / 1024
    ));
    out
}

/// Head + tail of `output`, with the middle replaced by a marker naming the file
/// that holds all of it. Both ends are sampled by whole lines when the text has
/// them, so neither end starts or stops mid-line.
fn preview(output: &str, cap: usize, path: &Path) -> String {
    let marker = format!(
        "\n\n…[output exceeded the {} KB limit, so the middle was elided. \
         The complete {} KB is at {} — page it with `read` (offset/limit) or \
         search it with `grep`.]…\n\n",
        cap / 1024,
        output.len() / 1024,
        path.display()
    );
    // Budget for the two ends. A cap tighter than the marker itself leaves
    // nothing to show but the pointer, which is still the useful half.
    let Some(budget) = cap.checked_sub(marker.len()) else {
        return marker.trim().to_string();
    };
    let half = budget / 2;
    let line_budget = MAX_PREVIEW_LINES / 2;
    format!(
        "{}{marker}{}",
        head(output, half, line_budget),
        tail(output, half, line_budget)
    )
}

/// The longest prefix within `budget` bytes and `lines` lines, ending on a line
/// break where one exists (a minified single-line blob has none — then it is a
/// plain char-boundary cut).
fn head(output: &str, budget: usize, lines: usize) -> &str {
    let limit = floor_boundary(output, budget);
    let mut end = None;
    for (count, (i, _)) in output[..limit].match_indices('\n').enumerate() {
        end = Some(i + 1);
        if count + 1 >= lines {
            break;
        }
    }
    &output[..end.unwrap_or(limit)]
}

/// The mirror of [`head`]: the longest suffix within the same bounds.
fn tail(output: &str, budget: usize, lines: usize) -> &str {
    let start = output.len() - floor_boundary(output, budget.min(output.len()));
    let region = &output[ceil_boundary(output, start)..];
    let breaks: Vec<usize> = region.match_indices('\n').map(|(i, _)| i + 1).collect();
    // Keep at most `lines` lines: start after the break that leaves that many.
    let from = if breaks.len() > lines {
        breaks[breaks.len() - lines]
    } else {
        breaks.first().copied().unwrap_or(0)
    };
    &region[from..]
}

/// Largest index ≤ `at` that is a char boundary (`String` slicing panics
/// otherwise, and a byte budget lands mid-codepoint on any CJK text).
fn floor_boundary(s: &str, at: usize) -> usize {
    let mut at = at.min(s.len());
    while at > 0 && !s.is_char_boundary(at) {
        at -= 1;
    }
    at
}

/// Smallest index ≥ `at` that is a char boundary.
fn ceil_boundary(s: &str, at: usize) -> usize {
    let mut at = at.min(s.len());
    while at < s.len() && !s.is_char_boundary(at) {
        at += 1;
    }
    at
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(tag: &str) -> ToolOutputStore {
        let root = std::env::temp_dir().join(format!("komo_tool_output_{tag}"));
        let _ = std::fs::remove_dir_all(&root);
        ToolOutputStore::new(root)
    }

    #[test]
    fn a_result_under_the_cap_touches_no_disk() {
        let store = store("passthrough");
        let out = store.bound("cli:1", "call-1", "small".to_string(), 1024);
        assert_eq!(out.text, "small");
        assert!(out.output_paths.is_empty());
        assert!(!store.root().exists(), "nothing should be written");
    }

    /// The whole point: the **tail** survives. A compile error's summary line is
    /// the last thing printed, and one-sided truncation ate exactly that.
    #[test]
    fn an_over_limit_result_keeps_both_ends_and_stores_the_whole_thing() {
        let store = store("both_ends");
        let body: String = (0..500).map(|i| format!("line {i}\n")).collect();
        let out = store.bound("cli:1", "call-7", body.clone(), 512);

        assert!(out.text.contains("line 0"), "{}", out.text);
        assert!(out.text.contains("line 499"), "{}", out.text);
        assert!(!out.text.contains("line 250"), "middle should be elided");
        assert!(out.text.contains("`grep`"));

        let path = &out.output_paths[0];
        assert!(out.text.contains(&path.display().to_string()));
        assert_eq!(std::fs::read_to_string(path).unwrap(), body);
        // …and both ends are whole lines, not fragments.
        assert!(out.text.starts_with("line 0\n"));
        assert!(out.text.ends_with("line 499\n"));

        let _ = std::fs::remove_dir_all(store.root());
    }

    #[test]
    fn the_preview_stays_within_the_cap_plus_its_own_marker() {
        let store = store("size");
        let body: String = (0..2000).map(|i| format!("line {i}\n")).collect();
        let cap = 4096;
        let out = store.bound("cli:1", "call-8", body, cap);
        // The marker names a path whose length isn't known up front, so the
        // budget covers the two ends; the preview must not run away past it.
        assert!(out.text.len() <= cap * 2, "preview was {}", out.text.len());
        let _ = std::fs::remove_dir_all(store.root());
    }

    /// A single enormous line (a minified bundle, a JSON blob) has no line
    /// breaks to sample on — the cut must still land on a char boundary.
    #[test]
    fn a_single_multibyte_line_is_cut_on_a_char_boundary() {
        let store = store("multibyte");
        let body = "界".repeat(4000); // 3 bytes each, no newlines
        let out = store.bound("cli:1", "call-9", body, 1024);
        assert!(out.text.starts_with('界'));
        assert!(out.text.ends_with('界'));
        let _ = std::fs::remove_dir_all(store.root());
    }

    #[test]
    fn session_and_call_ids_become_one_safe_path_segment_each() {
        assert_eq!(sanitize("feishu:oc_abc"), "feishu-oc_abc");
        assert_eq!(sanitize("../../etc/passwd"), "------etc-passwd");
        assert_eq!(sanitize(".hidden"), "-hidden");
        assert_eq!(sanitize(""), "unknown");

        let store = store("paths");
        let body = "x".repeat(2048);
        let out = store.bound("../evil", "../worse", body, 64);
        let path = &out.output_paths[0];
        assert!(
            path.starts_with(store.root()),
            "{} escaped {}",
            path.display(),
            store.root().display()
        );
        let _ = std::fs::remove_dir_all(store.root());
    }

    #[test]
    fn the_sweep_removes_only_expired_files() {
        let store = store("sweep");
        let dir = store.root().join("cli-1");
        std::fs::create_dir_all(&dir).unwrap();
        let fresh = dir.join("fresh.txt");
        let stale = dir.join("stale.txt");
        std::fs::write(&fresh, "new").unwrap();
        std::fs::write(&stale, "old").unwrap();
        // Backdate past the retention window.
        let old = SystemTime::now() - RETENTION - Duration::from_secs(60);
        std::fs::File::open(&stale)
            .unwrap()
            .set_modified(old)
            .unwrap();

        assert_eq!(store.sweep(), 1);
        assert!(fresh.exists());
        assert!(!stale.exists());

        let _ = std::fs::remove_dir_all(store.root());
    }

    #[test]
    fn a_leftover_index_file_is_swept_away() {
        // Written by an earlier komo. Nothing reads it, so the sweep removes it
        // outright rather than letting it age out beside real outputs.
        let store = store("legacy_index");
        let dir = store.root().join("cli-1");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(LEGACY_INDEX_FILE), "{}\n").unwrap();
        let fresh = dir.join("fresh.txt");
        std::fs::write(&fresh, "new").unwrap();

        store.sweep();

        assert!(!dir.join(LEGACY_INDEX_FILE).exists());
        assert!(fresh.exists(), "a live output must survive");
        let _ = std::fs::remove_dir_all(store.root());
    }

    #[test]
    fn a_stored_output_writes_only_its_own_file() {
        let store = store("no_index");
        let body = "x".repeat(2048);
        let out = store.bound("cli:1", "run-1-0000", body, 64);
        let dir = out.output_paths[0].parent().unwrap().to_path_buf();
        let names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["run-1-0000.txt".to_string()]);
        let _ = std::fs::remove_dir_all(store.root());
    }

    #[test]
    fn an_emptied_session_directory_is_removed_too() {
        let store = store("empty_dir");
        let dir = store.root().join("cli-1");
        std::fs::create_dir_all(&dir).unwrap();
        // A leftover index must not keep the directory alive once every output
        // has expired.
        std::fs::write(dir.join(LEGACY_INDEX_FILE), "{}\n").unwrap();
        let stale = dir.join("stale.txt");
        std::fs::write(&stale, "old").unwrap();
        std::fs::File::open(&stale)
            .unwrap()
            .set_modified(SystemTime::now() - RETENTION - Duration::from_secs(60))
            .unwrap();

        store.sweep();
        assert!(!dir.exists());

        let _ = std::fs::remove_dir_all(store.root());
    }

    #[test]
    fn the_sweep_is_debounced_between_writes() {
        let store = store("debounce");
        let body = "x".repeat(4096);
        store.bound("cli:1", "a", body.clone(), 128);
        let first = *store.last_sweep.lock().unwrap();
        assert!(first.is_some(), "the first over-limit write sweeps");
        store.bound("cli:1", "b", body, 128);
        assert_eq!(
            *store.last_sweep.lock().unwrap(),
            first,
            "a second write within the interval must not re-sweep"
        );
        let _ = std::fs::remove_dir_all(store.root());
    }
}
