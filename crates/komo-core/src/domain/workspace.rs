use std::path::{Component, Path, PathBuf};

/// Whitelist of directories within which file operations are permitted.
#[derive(Clone)]
pub struct Workspace {
    roots: Vec<PathBuf>,
    readonly_roots: Vec<PathBuf>,
    artifacts_root: Option<PathBuf>,
    unrestricted_reads: bool,
}

impl Workspace {
    /// Create a workspace rooted at the given (absolute) directories.
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self {
            roots,
            readonly_roots: Vec::new(),
            artifacts_root: None,
            unrestricted_reads: false,
        }
    }

    /// A workspace rooted at the current working directory.
    pub fn current_dir() -> std::io::Result<Self> {
        Ok(Self::new(vec![std::env::current_dir()?]))
    }

    /// Add directories that may be **read** but never written — komo's own
    /// managed storage (`~/.komo/tool-output`, where an over-limit tool result is
    /// kept in full). Without this a preview could name a path the model has no
    /// way to open; with it, `read`/`grep` reach that file and nothing else does,
    /// because every mutating tool resolves against the writable roots alone —
    /// the workspace's own plus [`with_artifacts`](Self::with_artifacts).
    pub fn with_readonly(mut self, roots: Vec<PathBuf>) -> Self {
        self.readonly_roots = roots;
        self
    }

    /// Add komo's own artifacts directory (`~/.komo/artifacts`), which a turn may
    /// **write** — it is where what a turn produced belongs, as opposed to the
    /// user's files in [`roots`](Self::roots). Kept apart from those because it is
    /// komo's, not the workspace's: a turn that picks its own root still has it,
    /// and it never anchors a relative path.
    pub fn with_artifacts(mut self, root: PathBuf) -> Self {
        self.artifacts_root = Some(root);
        self
    }

    /// Permit reads from any local path while keeping every mutation confined to
    /// [`roots`](Self::roots). The caller is still responsible for applying the
    /// file-read permission policy before exposing content.
    pub fn with_unrestricted_reads(mut self) -> Self {
        self.unrestricted_reads = true;
        self
    }

    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// The read-only roots, so a derived workspace (a session that picked its own
    /// root) can carry them over.
    pub fn readonly_roots(&self) -> &[PathBuf] {
        &self.readonly_roots
    }

    /// The artifacts root, so a derived workspace carries it over.
    pub fn artifacts_root(&self) -> Option<&Path> {
        self.artifacts_root.as_deref()
    }

    /// Whether reads may reach paths outside the workspace and named read-only
    /// roots. This must be carried into a session-selected workspace.
    pub fn has_unrestricted_reads(&self) -> bool {
        self.unrestricted_reads
    }

    /// Returns true if `path` resolves to a location inside one of the roots.
    ///
    /// Resolution is lexical (collapses `.`/`..` without touching the
    /// filesystem), so it also guards write targets that do not yet exist and
    /// blocks `../` escapes.
    pub fn contains(&self, path: &Path) -> bool {
        let resolved = self.resolve(path);
        self.writable().any(|root| resolved.starts_with(root))
    }

    /// The normalized absolute form of `path`, but only when it lands inside the
    /// workspace — `None` is the refusal. Relative paths anchor to the first
    /// root, so a tool can accept `src/main.rs` as readily as an absolute path.
    pub fn resolve_contained(&self, path: &Path) -> Option<PathBuf> {
        let resolved = self.resolve(path);
        self.writable()
            .any(|root| resolved.starts_with(root))
            .then_some(resolved)
    }

    /// [`resolve_contained`](Self::resolve_contained) widened to the read-only
    /// roots — the resolver a **read** goes through (`read`, `grep`). Writes must
    /// keep using `resolve_contained`, which is what makes the managed roots
    /// read-only in the first place.
    pub fn resolve_readable(&self, path: &Path) -> Option<PathBuf> {
        let resolved = self.resolve(path);
        (self.unrestricted_reads
            || self
                .writable()
                .chain(self.readonly_roots.iter().map(PathBuf::as_path))
                .any(|root| resolved.starts_with(root)))
        .then_some(resolved)
    }

    /// Every root a mutation may land in: the workspace's own, plus the artifacts
    /// directory when one is configured.
    fn writable(&self) -> impl Iterator<Item = &Path> {
        self.roots
            .iter()
            .map(PathBuf::as_path)
            .chain(self.artifacts_root.as_deref())
    }

    fn resolve(&self, path: &Path) -> PathBuf {
        let joined = if path.is_absolute() {
            path.to_path_buf()
        } else {
            // Relative paths are anchored to the first root.
            self.roots.first().cloned().unwrap_or_default().join(path)
        };
        normalize_lexically(&joined)
    }
}

/// Lexically normalize a path: collapse `.` and `..` without filesystem access.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_paths_inside_root_and_blocks_escapes() {
        let ws = Workspace::new(vec![PathBuf::from("/home/user/project")]);

        assert!(ws.contains(Path::new("/home/user/project/src/main.rs")));
        assert!(ws.contains(Path::new("notes.txt"))); // relative → anchored to root
        assert!(ws.contains(Path::new("/home/user/project/a/../b.txt")));

        assert!(!ws.contains(Path::new("/etc/passwd")));
        assert!(!ws.contains(Path::new("/home/user/project/../secret"))); // escape
        assert!(!ws.contains(Path::new("../../etc/passwd")));
        assert!(!ws.contains(Path::new("/home/user/project-evil/x"))); // sibling prefix
    }

    /// A read-only root widens reads only: the mutating resolver must still
    /// refuse it, or komo's managed tool-output store would be writable.
    #[test]
    fn a_readonly_root_is_readable_but_never_writable() {
        let ws = Workspace::new(vec![PathBuf::from("/home/user/project")])
            .with_readonly(vec![PathBuf::from("/home/user/.komo/tool-output")]);
        let stored = Path::new("/home/user/.komo/tool-output/cli-1/call-2.txt");

        assert!(ws.resolve_readable(stored).is_some());
        assert!(ws.resolve_contained(stored).is_none());
        // The workspace itself stays readable, and escapes stay blocked.
        assert!(ws.resolve_readable(Path::new("src/main.rs")).is_some());
        assert!(ws.resolve_readable(Path::new("/etc/passwd")).is_none());
        assert!(
            ws.resolve_readable(Path::new("/home/user/.komo/memory.db"))
                .is_none(),
            "only the named subdirectory, not the whole komo home"
        );
    }

    /// The mirror of the read-only root: komo's artifacts directory is outside
    /// the workspace and still writable, because it is where a turn's own output
    /// belongs.
    #[test]
    fn the_artifacts_root_is_writable_from_outside_the_workspace() {
        let ws = Workspace::new(vec![PathBuf::from("/home/user/project")])
            .with_artifacts(PathBuf::from("/home/user/.komo/artifacts"));
        let report = Path::new("/home/user/.komo/artifacts/sess-1/report.md");

        assert!(ws.resolve_contained(report).is_some());
        assert!(ws.resolve_readable(report).is_some());
        // Confinement is unchanged: the root's parent is not in it, and a
        // traversal out of it is still lexically resolved and refused.
        assert!(
            ws.resolve_contained(Path::new("/home/user/.komo/artifacts/../permissions.json"))
                .is_none()
        );
        assert!(
            ws.resolve_contained(Path::new("/home/user/.komo/komo.db"))
                .is_none()
        );
        // …and a relative path still anchors to the workspace, not to it.
        assert_eq!(
            ws.resolve_contained(Path::new("notes.txt")).unwrap(),
            PathBuf::from("/home/user/project/notes.txt")
        );
    }

    #[test]
    fn unrestricted_reads_do_not_widen_write_roots() {
        let ws =
            Workspace::new(vec![PathBuf::from("/home/user/project")]).with_unrestricted_reads();

        assert!(ws.resolve_readable(Path::new("/etc/passwd")).is_some());
        assert!(ws.resolve_contained(Path::new("/etc/passwd")).is_none());
    }
}
