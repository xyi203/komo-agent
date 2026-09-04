//! Where a turn puts what it made.
//!
//! A workspace holds the user's files: a turn that edits them is changing
//! something that was already there and is theirs. What a turn *produces* — the
//! report it was asked for, a scratch script, a file it downloaded — has nowhere
//! to go that is neither someone's repository nor a temp directory the next
//! reboot empties. So it goes here: `<komo home>/artifacts/<session>/`, one
//! directory per session, inside the workspace's **writable** roots
//! (docs/bot-runtime.md §5.16).
//!
//! Two differences from [`crate::tool_output_store`], which this sits beside and
//! borrows its path sanitizing from:
//!
//!   * **Writable, not read-only.** Tool output is komo's record of a call and
//!     the model may only read it back. An artifact is the model's own output,
//!     so `write` / `edit` / `apply_patch` resolve into this root and `shell` may
//!     use it as a working directory.
//!   * **Durable, not swept.** A stored tool result ages out after a week
//!     because it is a byproduct. An artifact is the deliberate residue of a
//!     turn — deleting it on a timer would be deleting the thing the user asked
//!     for.
//!
//! Sessions are not isolated from each other: the whole root is writable, so
//! "the report you wrote yesterday" is readable from today's conversation. The
//! per-session directory is a convention about where to *put* things, not a
//! boundary.
//!
//! Nothing is created up front. The directory for a session appears when
//! something is first written into it (`write` creates missing parents), so a
//! conversation that produces no files leaves no directories behind.

use std::path::{Path, PathBuf};

use crate::tool_output_store::sanitize;

pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    /// A store rooted at `root` (`<komo home>/artifacts`).
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// The managed directory, registered as a writable workspace root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where `session_id`'s artifacts belong. The directory may not exist yet —
    /// that is the point.
    pub fn session_dir(&self, session_id: &str) -> PathBuf {
        self.root.join(sanitize(session_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_directory_is_one_segment_under_the_root() {
        let store = ArtifactStore::new(PathBuf::from("/komo/artifacts"));
        let dir = store.session_dir("feishu:oc_abc");
        assert_eq!(dir, PathBuf::from("/komo/artifacts/feishu-oc_abc"));
    }

    #[test]
    fn a_traversing_session_id_cannot_leave_the_root() {
        let store = ArtifactStore::new(PathBuf::from("/komo/artifacts"));
        let dir = store.session_dir("../../etc");
        assert!(dir.starts_with(store.root()), "{}", dir.display());
    }

    #[test]
    fn nothing_is_created_by_naming_a_directory() {
        let root = std::env::temp_dir().join("komo_artifacts_lazy");
        let _ = std::fs::remove_dir_all(&root);
        let store = ArtifactStore::new(root.clone());
        let _ = store.session_dir("cli-1");
        assert!(!root.exists(), "naming a directory must not create it");
    }
}
