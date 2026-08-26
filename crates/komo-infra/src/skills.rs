//! Filesystem-backed skill store — the single source of truth for governed
//! skills (roadmap §9).
//!
//! Skills are durable personal data (peers of memory/kanban), so they live as
//! `SKILL.md` files under `~/.komo/skills/<name>/`, not in the disposable
//! `state.db`. Files are editable, shareable, and lock-free: every governance
//! action works while the gateway holds the Turso db lock.
//!
//! Layout under the root:
//! - `<name>/SKILL.md` — an **active** skill, loaded into the runtime
//!   `SkillRegistry` (the root is one of its scan directories).
//! - `.candidates/<name>/SKILL.md` — a reviewer **proposal**, invisible to the
//!   runtime until the operator promotes it (`komo skills promote`). The dot
//!   prefix keeps the registry's directory scan from ever loading it.
//! - `.candidates/<name>/.history/<ts>.md` — prior candidate versions, rolled
//!   on overwrite so a re-extraction never silently destroys the last proposal.
//! - `.history/<name>/<ts>.md` — prior **active** bodies, rolled when a promote
//!   overwrites one. Kept at the root rather than inside the skill directory
//!   because the skill dir is a copied tree: `install_active_dir` clears it, and
//!   `skill view` samples its files into the model's context.
//! - `.archive/<name>/` — an archived skill's whole tree. Archiving is the
//!   heaviest thing the operator can do to an active skill and it is reversible
//!   (`komo skills restore`); nothing in this store deletes an active skill.
//! - `.expired/<name>/` — a candidate dreaming withdrew for want of a verdict.
//!   Kept apart from `.archive/` on purpose: restoring one must return it to
//!   `.candidates/`, never to the active catalog, or a proposal no human ever
//!   approved would go live by way of a restore.
//!
//! The [`SkillRepository`] impl is the automated write path (the reflective
//! reviewer): `save` only ever writes a candidate — it never touches an active
//! file. Operator actions (promote/reject/protect/disable) are inherent
//! methods, used by the CLI directly.

use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tracing::{debug, warn};

use komo_core::domain::{
    repository::SkillRepository,
    skill::{SOURCE_REVIEWER, Skill, valid_skill_name},
};

/// Directory (under the store root) holding reviewer proposals.
const CANDIDATES_DIR: &str = ".candidates";
/// Directory holding rolled prior versions — under a candidate for proposals,
/// under the store root (keyed by skill name) for overwritten active bodies.
const HISTORY_DIR: &str = ".history";
/// Directory (under the store root) holding archived skills.
const ARCHIVE_DIR: &str = ".archive";
/// Directory (under the store root) holding candidates dreaming withdrew.
const EXPIRED_DIR: &str = ".expired";
/// Marker file: the one-time import of legacy `komo.db` skills already ran.
const DB_IMPORT_MARKER: &str = ".imported-from-db";

/// Build the runtime's ordered skill search path. Earlier directories win when
/// two skills share a name. `~/.agents/skills` is the shared, read-only skill
/// collection used by Codex and other local agents; Komo discovers it without
/// taking ownership of its files.
pub fn runtime_skill_dirs(
    configured: &[PathBuf],
    workspace_root: &Path,
    governed_root: &Path,
    user_home: Option<&Path>,
) -> Vec<PathBuf> {
    let mut dirs = configured.to_vec();
    dirs.push(workspace_root.join("skills"));
    dirs.push(workspace_root.join(".claude/skills"));
    dirs.push(governed_root.to_path_buf());
    if let Some(home) = user_home {
        dirs.push(home.join(".agents/skills"));
        dirs.push(home.join(".claude/skills"));
    }
    dirs
}

pub struct FsSkillStore {
    root: PathBuf,
}

impl FsSkillStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// The komo-owned skills home: `~/.komo/skills`.
    pub fn default_root() -> PathBuf {
        komo_config::komo_home().join("skills")
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn candidates_root(&self) -> PathBuf {
        self.root.join(CANDIDATES_DIR)
    }

    pub fn archive_root(&self) -> PathBuf {
        self.root.join(ARCHIVE_DIR)
    }

    pub fn expired_root(&self) -> PathBuf {
        self.root.join(EXPIRED_DIR)
    }

    pub fn active_path(&self, name: &str) -> PathBuf {
        self.root.join(name).join("SKILL.md")
    }

    pub fn candidate_path(&self, name: &str) -> PathBuf {
        self.candidates_root().join(name).join("SKILL.md")
    }

    pub fn archived_path(&self, name: &str) -> PathBuf {
        self.archive_root().join(name).join("SKILL.md")
    }

    pub fn expired_path(&self, name: &str) -> PathBuf {
        self.expired_root().join(name).join("SKILL.md")
    }

    /// Rolled prior versions of a candidate (file names, oldest first) — the
    /// lightweight edit history `skills inspect` shows. Only the reviewer path
    /// rolls history; hand-edited active files are the user's own to version.
    pub fn candidate_history(&self, name: &str) -> Vec<String> {
        history_entries(&self.candidates_root().join(name).join(HISTORY_DIR))
    }

    /// Prior **active** bodies of a skill (file names, oldest first), rolled by
    /// [`promote`](Self::promote) when a proposal overwrote one.
    pub fn active_history(&self, name: &str) -> Vec<String> {
        history_entries(&self.root.join(HISTORY_DIR).join(name))
    }

    /// Active skills (the governed subset the registry loads from this root —
    /// workspace/`~/.claude` skill dirs are governed by their own repos).
    pub fn list_active(&self) -> Vec<Skill> {
        scan_dir(&self.root)
    }

    /// Reviewer proposals awaiting triage.
    pub fn list_candidates(&self) -> Vec<Skill> {
        scan_dir(&self.candidates_root())
    }

    /// Archived skills — retired from the catalog but recoverable.
    pub fn list_archived(&self) -> Vec<Skill> {
        scan_dir(&self.archive_root())
    }

    /// Candidates dreaming withdrew — proposals that lapsed, still recoverable.
    pub fn list_expired(&self) -> Vec<Skill> {
        scan_dir(&self.expired_root())
    }

    pub fn find_archived(&self, name: &str) -> Option<Skill> {
        valid_skill_name(name)
            .then(|| read_skill(&self.archived_path(name)))
            .flatten()
    }

    pub fn find_expired(&self, name: &str) -> Option<Skill> {
        valid_skill_name(name)
            .then(|| read_skill(&self.expired_path(name)))
            .flatten()
    }

    pub fn find_active(&self, name: &str) -> Option<Skill> {
        valid_skill_name(name)
            .then(|| read_skill(&self.active_path(name)))
            .flatten()
    }

    pub fn find_candidate(&self, name: &str) -> Option<Skill> {
        valid_skill_name(name)
            .then(|| read_skill(&self.candidate_path(name)))
            .flatten()
    }

    /// Promote a candidate to active (accepting an update proposal overwrites
    /// the active file). The candidate directory is removed afterwards.
    ///
    /// An overwritten active body is rolled into `.history/<name>/` first. The
    /// automated write path proposes a *whole* body, so promoting a weak
    /// proposal over a hand-written skill would otherwise destroy it with no
    /// copy anywhere — candidates keep history, active files did not.
    pub fn promote(&self, name: &str) -> anyhow::Result<Skill> {
        let Some(skill) = self.find_candidate(name) else {
            anyhow::bail!("no candidate skill named `{name}`");
        };
        self.roll_active_history(name)?;
        self.write_active(&skill)?;
        fs::remove_dir_all(self.candidates_root().join(name))?;
        Ok(skill)
    }

    /// Copy the current active `SKILL.md` into `.history/<name>/<ts>.md`.
    /// No active file ⇒ nothing to preserve, so this is a no-op.
    fn roll_active_history(&self, name: &str) -> anyhow::Result<()> {
        let current = self.active_path(name);
        if !current.is_file() {
            return Ok(());
        }
        let dir = self.root.join(HISTORY_DIR).join(name);
        fs::create_dir_all(&dir)?;
        let ts = time::OffsetDateTime::now_utc().unix_timestamp();
        fs::copy(&current, dir.join(format!("{ts}.md")))?;
        Ok(())
    }

    /// Retire an active skill: move its whole tree to `.archive/<name>/`. The
    /// dot prefix hides it from every scan, so the agent stops seeing it while
    /// the operator keeps the files. Reversible via [`restore`](Self::restore) —
    /// this store never deletes an active skill.
    pub fn archive(&self, name: &str) -> anyhow::Result<Skill> {
        let Some(skill) = self.find_active(name) else {
            anyhow::bail!("no active skill named `{name}` in {}", self.root.display());
        };
        let dest = self.archive_root().join(name);
        if dest.exists() {
            anyhow::bail!(
                "`{name}` is already archived at {} — restore or remove that copy first",
                dest.display()
            );
        }
        fs::create_dir_all(self.archive_root())?;
        fs::rename(self.root.join(name), &dest)?;
        Ok(skill)
    }

    /// Withdraw a lapsed candidate: move `.candidates/<name>/` aside to
    /// `.expired/<name>/`. The dreaming counterpart of [`archive`](Self::archive),
    /// and reversible the same way ([`unexpire`](Self::unexpire)) — a sweep that
    /// runs unattended at 3am must not be able to destroy a proposal.
    ///
    /// Deliberately **not** `.archive/`: restoring from there lands a skill in
    /// the active catalog, which would let a proposal no human ever approved go
    /// live by way of a restore.
    pub fn expire_candidate(&self, name: &str) -> anyhow::Result<Skill> {
        let Some(skill) = self.find_candidate(name) else {
            anyhow::bail!("no candidate skill named `{name}`");
        };
        let dest = self.expired_root().join(name);
        if dest.exists() {
            anyhow::bail!(
                "`{name}` is already expired at {} — restore or remove that copy first",
                dest.display()
            );
        }
        fs::create_dir_all(self.expired_root())?;
        fs::rename(self.candidates_root().join(name), &dest)?;
        Ok(skill)
    }

    /// Bring a withdrawn candidate back for triage — to `.candidates/`, never
    /// to the active catalog: it is still a proposal awaiting a verdict.
    pub fn unexpire(&self, name: &str) -> anyhow::Result<Skill> {
        let Some(skill) = self.find_expired(name) else {
            anyhow::bail!("no expired candidate named `{name}`");
        };
        let dest = self.candidates_root().join(name);
        if dest.exists() {
            anyhow::bail!("a candidate named `{name}` already exists — promote or reject it first");
        }
        fs::create_dir_all(self.candidates_root())?;
        fs::rename(self.expired_root().join(name), &dest)?;
        // Restamp in place, or the restored proposal is still older than the
        // expiry window and the next sweep withdraws it again the same night.
        // Written directly rather than through `write_candidate`, which would
        // roll the identical body into `.history/` and report a revision that
        // never happened.
        fs::write(self.candidate_path(name), render(&skill))?;
        Ok(skill)
    }

    /// Bring an archived skill back into the active catalog. Refuses to clobber
    /// an active skill of the same name — the operator resolves that themselves.
    pub fn restore(&self, name: &str) -> anyhow::Result<Skill> {
        let Some(skill) = self.find_archived(name) else {
            anyhow::bail!("no archived skill named `{name}`");
        };
        let dest = self.root.join(name);
        if dest.exists() {
            anyhow::bail!(
                "an active skill named `{name}` already exists — archive or rename it first"
            );
        }
        fs::rename(self.archive_root().join(name), &dest)?;
        Ok(skill)
    }

    /// Reject (delete) a candidate. Unlike memories there is no usage signal to
    /// earn, so nothing is kept.
    pub fn reject(&self, name: &str) -> anyhow::Result<()> {
        if self.find_candidate(name).is_none() {
            anyhow::bail!("no candidate skill named `{name}`");
        }
        fs::remove_dir_all(self.candidates_root().join(name))?;
        Ok(())
    }

    /// Flip an active skill's `protected` flag (operator-only path).
    pub fn set_protected(&self, name: &str, on: bool) -> anyhow::Result<Skill> {
        self.update_active(name, |s| s.protected = on)
    }

    /// Flip an active skill's `disabled` flag (operator-only path).
    pub fn set_disabled(&self, name: &str, on: bool) -> anyhow::Result<Skill> {
        self.update_active(name, |s| s.disabled = on)
    }

    fn update_active(&self, name: &str, mutate: impl FnOnce(&mut Skill)) -> anyhow::Result<Skill> {
        let Some(mut skill) = self.find_active(name) else {
            anyhow::bail!("no active skill named `{name}` in {}", self.root.display());
        };
        mutate(&mut skill);
        self.write_active(&skill)?;
        Ok(skill)
    }

    fn write_active(&self, skill: &Skill) -> anyhow::Result<()> {
        let path = self.active_path(&skill.name);
        fs::create_dir_all(path.parent().expect("skill path has a parent"))?;
        fs::write(&path, render(skill))?;
        Ok(())
    }

    /// One-time import of skills a pre-filesystem komo accumulated in
    /// `komo.db` (the reviewer used to write there; the runtime never read
    /// it). They land as **candidates** — previously-invisible extractions get
    /// a triage pass instead of silently activating. A marker file makes this
    /// a no-op forever after, even if the db rows outlive it.
    pub fn import_legacy_db(&self, skills: Vec<Skill>) -> anyhow::Result<usize> {
        let marker = self.root.join(DB_IMPORT_MARKER);
        if marker.exists() {
            return Ok(0);
        }
        let mut imported = 0;
        for mut skill in skills {
            if !valid_skill_name(&skill.name) {
                warn!(name = %skill.name, "legacy db skill has an unusable name; skipped");
                continue;
            }
            skill.source = SOURCE_REVIEWER.to_string();
            if self.write_candidate(&skill).is_ok() {
                imported += 1;
            }
        }
        fs::create_dir_all(&self.root)?;
        fs::write(
            &marker,
            "legacy komo.db skills were imported as candidates\n",
        )?;
        Ok(imported)
    }

    /// Install a skill **directory** (its `SKILL.md` plus any supporting files —
    /// scripts, `references/`, etc.) as an **active** skill, copying the whole
    /// tree. This is the install path (operator `komo skills install` + the
    /// approved `skill` tool `install` action), distinct from `save`, which only
    /// renders a single-file candidate. Overwrites an existing active skill of
    /// the same name — unless it's protected, matching the `save` floor.
    /// Returns the parsed skill and the number of files copied.
    pub fn install_active_dir(&self, src_dir: &Path) -> anyhow::Result<(Skill, usize)> {
        let skill = read_skill(&src_dir.join("SKILL.md")).ok_or_else(|| {
            anyhow::anyhow!(
                "no valid SKILL.md (with frontmatter) in {}",
                src_dir.display()
            )
        })?;
        if !valid_skill_name(&skill.name) {
            anyhow::bail!(
                "invalid skill name `{}` (letters, digits, `-`/`_`/`.` only)",
                skill.name
            );
        }
        if self.find_active(&skill.name).is_some_and(|s| s.protected) {
            anyhow::bail!(
                "skill `{}` is protected — refusing to overwrite (operator edits only)",
                skill.name
            );
        }
        let dest = self.root.join(&skill.name);
        if dest.exists() {
            fs::remove_dir_all(&dest)?;
        }
        let files = copy_dir_all(src_dir, &dest)?;
        Ok((skill, files))
    }

    /// Write (or overwrite) a candidate proposal, rolling any existing one into
    /// its `.history/` first.
    fn write_candidate(&self, skill: &Skill) -> anyhow::Result<()> {
        let path = self.candidate_path(&skill.name);
        let dir = path.parent().expect("candidate path has a parent");
        fs::create_dir_all(dir)?;
        if path.exists() {
            let history = dir.join(HISTORY_DIR);
            fs::create_dir_all(&history)?;
            let ts = time::OffsetDateTime::now_utc().unix_timestamp();
            fs::rename(&path, history.join(format!("{ts}.md")))?;
        }
        fs::write(&path, render(skill))?;
        Ok(())
    }
}

/// The automated write path. `find`/`list` expose **active** skills (what the
/// reviewer needs for description fallback and the protected check); `save`
/// writes a **candidate** — automated extraction never takes effect directly
/// (same governance ladder as memory candidates), and a protected active skill
/// refuses even the proposal.
#[async_trait]
impl SkillRepository for FsSkillStore {
    async fn find(&self, name: &str) -> anyhow::Result<Option<Skill>> {
        Ok(self.find_active(name))
    }

    async fn list(&self) -> anyhow::Result<Vec<Skill>> {
        Ok(self.list_active())
    }

    async fn save(&self, skill: &Skill) -> anyhow::Result<()> {
        if !valid_skill_name(&skill.name) {
            anyhow::bail!(
                "invalid skill name `{}` (letters, digits, `-`/`_`/`.` only)",
                skill.name
            );
        }
        if self.find_active(&skill.name).is_some_and(|s| s.protected) {
            anyhow::bail!(
                "skill `{}` is protected — not writing a proposal (operator edits only)",
                skill.name
            );
        }
        self.write_candidate(skill)
    }
}

fn read_skill(path: &Path) -> Option<Skill> {
    let content = fs::read_to_string(path).ok()?;
    Skill::parse(&content)
}

/// Rolled version file names in `dir` (oldest first — the names are unix
/// timestamps, so lexical order is chronological). Missing dir ⇒ no history.
fn history_entries(dir: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Recursively copy `src` into `dst` (created if absent), skipping any nested
/// `.git` directory (defensive — install stages from a subdir, not a clone
/// root, but a vendored skill could still carry one). Returns the number of
/// regular files copied.
fn copy_dir_all(src: &Path, dst: &Path) -> anyhow::Result<usize> {
    fs::create_dir_all(dst)?;
    let mut count = 0;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_name() == ".git" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            count += copy_dir_all(&from, &to)?;
        } else {
            fs::copy(&from, &to)?;
            count += 1;
        }
    }
    Ok(count)
}

/// Scan `dir` for `<name>/SKILL.md` entries (same shape as the runtime
/// registry's scan). Dot-prefixed entries never match: a dot-dir has no
/// `SKILL.md` of its own and dot names are rejected at parse level too.
fn scan_dir(dir: &Path) -> Vec<Skill> {
    let mut skills = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        debug!(?dir, "no skills directory; skipped");
        return skills;
    };
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let manifest = entry.path().join("SKILL.md");
        if !manifest.is_file() {
            continue;
        }
        match read_skill(&manifest) {
            Some(skill) => skills.push(skill),
            None => warn!(?manifest, "SKILL.md missing valid frontmatter; skipped"),
        }
    }
    skills.sort_by(|a, b| a.name.cmp(&b.name));
    skills
}

/// Render a skill back to `SKILL.md`: identity frontmatter, governance keys
/// only when set (hand-written files stay minimal), then the body.
fn render(skill: &Skill) -> String {
    let mut front = format!("---\nname: {}\n", skill.name);
    if !skill.description.is_empty() {
        front.push_str(&format!("description: {}\n", skill.description));
    }
    if skill.source != komo_core::domain::skill::SOURCE_USER {
        front.push_str(&format!("source: {}\n", skill.source));
    }
    if skill.protected {
        front.push_str("protected: true\n");
    }
    if skill.disabled {
        front.push_str("disabled: true\n");
    }
    // Round-tripped so an operator action that rewrites the file (protect,
    // disable, promote) can never silently drop a skill's offer gating.
    if !skill.platforms.is_empty() {
        front.push_str(&format!("platforms: [{}]\n", skill.platforms.join(", ")));
    }
    if !skill.requires_tools.is_empty() {
        front.push_str(&format!(
            "requires_tools: [{}]\n",
            skill.requires_tools.join(", ")
        ));
    }
    front.push_str(&format!(
        "updated_at: {}\n",
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    ));
    format!("{front}---\n\n{}\n", skill.instructions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_dirs_include_user_agents_skills() {
        let configured = vec![PathBuf::from("/configured/skills")];
        let dirs = runtime_skill_dirs(
            &configured,
            Path::new("/workspace"),
            Path::new("/komo/skills"),
            Some(Path::new("/user")),
        );

        assert_eq!(
            dirs,
            vec![
                PathBuf::from("/configured/skills"),
                PathBuf::from("/workspace/skills"),
                PathBuf::from("/workspace/.claude/skills"),
                PathBuf::from("/komo/skills"),
                PathBuf::from("/user/.agents/skills"),
                PathBuf::from("/user/.claude/skills"),
            ]
        );
    }

    fn store(name: &str) -> FsSkillStore {
        let root = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&root);
        FsSkillStore::new(root)
    }

    fn skill(name: &str) -> Skill {
        Skill {
            name: name.to_string(),
            description: format!("does {name}"),
            instructions: format!("How to {name}."),
            protected: false,
            disabled: false,
            source: SOURCE_REVIEWER.to_string(),
            platforms: Vec::new(),
            requires_tools: Vec::new(),
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn save_writes_a_candidate_not_an_active_skill() {
        let store = store("komo_skillstore_candidate");
        store.save(&skill("sync-cal")).await.unwrap();

        assert!(store.find_active("sync-cal").is_none());
        let cand = store.find_candidate("sync-cal").unwrap();
        assert_eq!(cand.source, SOURCE_REVIEWER);
        assert_eq!(store.list_candidates().len(), 1);
        assert!(store.list_active().is_empty());
    }

    #[tokio::test]
    async fn promote_moves_candidate_to_active() {
        let store = store("komo_skillstore_promote");
        store.save(&skill("sync-cal")).await.unwrap();

        store.promote("sync-cal").unwrap();
        assert!(store.find_candidate("sync-cal").is_none());
        let active = store.find_active("sync-cal").unwrap();
        assert_eq!(active.description, "does sync-cal");
        // Round-trips through render/parse.
        assert!(active.instructions.contains("How to sync-cal."));
    }

    #[tokio::test]
    async fn reject_deletes_the_candidate() {
        let store = store("komo_skillstore_reject");
        store.save(&skill("sync-cal")).await.unwrap();
        store.reject("sync-cal").unwrap();
        assert!(store.find_candidate("sync-cal").is_none());
        assert!(store.reject("sync-cal").is_err());
    }

    #[tokio::test]
    async fn candidate_overwrite_rolls_history() {
        let store = store("komo_skillstore_history");
        store.save(&skill("sync-cal")).await.unwrap();
        let mut v2 = skill("sync-cal");
        v2.instructions = "v2 body".to_string();
        store.save(&v2).await.unwrap();

        assert!(
            store
                .find_candidate("sync-cal")
                .unwrap()
                .instructions
                .contains("v2 body")
        );
        let history = store.candidates_root().join("sync-cal").join(HISTORY_DIR);
        assert_eq!(fs::read_dir(history).unwrap().count(), 1);
    }

    /// The loss path this guards: the reviewer proposes a whole body, so
    /// promoting over a hand-written active skill used to destroy it outright.
    #[tokio::test]
    async fn promote_rolls_the_overwritten_active_body_into_history() {
        let store = store("komo_skillstore_active_history");
        let mut v1 = skill("sync-cal");
        v1.instructions = "hand-written body".to_string();
        store.save(&v1).await.unwrap();
        store.promote("sync-cal").unwrap();
        // First promote had nothing to overwrite.
        assert!(store.active_history("sync-cal").is_empty());

        let mut v2 = skill("sync-cal");
        v2.instructions = "reviewer rewrite".to_string();
        store.save(&v2).await.unwrap();
        store.promote("sync-cal").unwrap();

        let history = store.active_history("sync-cal");
        assert_eq!(history.len(), 1);
        let rolled = fs::read_to_string(
            store
                .root()
                .join(HISTORY_DIR)
                .join("sync-cal")
                .join(&history[0]),
        )
        .unwrap();
        assert!(rolled.contains("hand-written body"));
        assert!(
            store
                .find_active("sync-cal")
                .unwrap()
                .instructions
                .contains("reviewer rewrite")
        );
    }

    #[tokio::test]
    async fn archive_hides_the_skill_and_restore_brings_it_back() {
        let store = store("komo_skillstore_archive");
        store.save(&skill("sync-cal")).await.unwrap();
        store.promote("sync-cal").unwrap();
        // A supporting file rides along with the tree.
        fs::write(store.root().join("sync-cal").join("notes.md"), "aside").unwrap();

        store.archive("sync-cal").unwrap();
        assert!(store.find_active("sync-cal").is_none());
        assert!(store.list_active().is_empty());
        assert_eq!(store.list_archived().len(), 1);
        assert!(store.find_archived("sync-cal").is_some());

        store.restore("sync-cal").unwrap();
        assert!(store.find_active("sync-cal").is_some());
        assert!(store.list_archived().is_empty());
        assert!(store.root().join("sync-cal").join("notes.md").is_file());
    }

    #[tokio::test]
    async fn expiring_a_candidate_sets_it_aside_recoverably() {
        let store = store("komo_skillstore_expire");
        store.save(&skill("sync-cal")).await.unwrap();
        fs::write(
            store
                .candidate_path("sync-cal")
                .parent()
                .unwrap()
                .join("notes.md"),
            "aside",
        )
        .unwrap();

        store.expire_candidate("sync-cal").unwrap();
        assert!(store.find_candidate("sync-cal").is_none());
        assert!(store.list_candidates().is_empty());
        assert_eq!(store.list_expired().len(), 1);

        store.unexpire("sync-cal").unwrap();
        assert!(store.find_candidate("sync-cal").is_some());
        assert!(store.list_expired().is_empty());
        assert!(
            store
                .candidate_path("sync-cal")
                .parent()
                .unwrap()
                .join("notes.md")
                .is_file(),
            "the whole proposal tree comes back, not just SKILL.md"
        );
    }

    /// A withdrawn proposal must never re-enter the catalog as an *active*
    /// skill: nobody ever approved it. Restoring returns it to triage.
    #[tokio::test]
    async fn an_expired_candidate_comes_back_as_a_candidate_not_as_active() {
        let store = store("komo_skillstore_expire_governance");
        store.save(&skill("sync-cal")).await.unwrap();
        store.expire_candidate("sync-cal").unwrap();

        // It is not archived, so the active-catalog restore cannot reach it.
        assert!(store.find_archived("sync-cal").is_none());
        assert!(store.restore("sync-cal").is_err());

        store.unexpire("sync-cal").unwrap();
        assert!(store.find_active("sync-cal").is_none());
        assert!(store.find_candidate("sync-cal").is_some());
    }

    /// Restoring restamps the proposal, or the next sweep withdraws it again
    /// the same night and the operator can never get a look at it.
    #[tokio::test]
    async fn unexpire_restarts_the_expiry_clock() {
        use komo_core::domain::skill::{SKILL_CANDIDATE_EXPIRY_DAYS, candidate_expired};

        let store = store("komo_skillstore_expire_clock");
        store.save(&skill("sync-cal")).await.unwrap();
        // Backdate the proposal past the window, as a real lapsed one would be.
        let stale =
            time::OffsetDateTime::now_utc() - time::Duration::days(SKILL_CANDIDATE_EXPIRY_DAYS + 5);
        let path = store.candidate_path("sync-cal");
        let backdated = fs::read_to_string(&path).unwrap().replace(
            &fs::read_to_string(&path)
                .unwrap()
                .lines()
                .find(|l| l.starts_with("updated_at:"))
                .unwrap()
                .to_string(),
            &format!(
                "updated_at: {}",
                stale
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
            ),
        );
        fs::write(&path, backdated).unwrap();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        assert!(candidate_expired(
            &store.find_candidate("sync-cal").unwrap(),
            now
        ));

        store.expire_candidate("sync-cal").unwrap();
        store.unexpire("sync-cal").unwrap();
        assert!(!candidate_expired(
            &store.find_candidate("sync-cal").unwrap(),
            now
        ));
        assert!(
            store.candidate_history("sync-cal").is_empty(),
            "restamping must not fabricate a revision in the proposal history"
        );
    }

    #[tokio::test]
    async fn archive_and_restore_refuse_to_clobber() {
        let store = store("komo_skillstore_archive_clobber");
        store.save(&skill("sync-cal")).await.unwrap();
        store.promote("sync-cal").unwrap();
        store.archive("sync-cal").unwrap();

        assert!(
            store.archive("sync-cal").is_err(),
            "nothing active to archive"
        );
        // A new active skill of the same name blocks the restore rather than
        // silently replacing it.
        store.save(&skill("sync-cal")).await.unwrap();
        store.promote("sync-cal").unwrap();
        let err = store.restore("sync-cal").unwrap_err();
        assert!(err.to_string().contains("already exists"));
        assert!(store.find_archived("sync-cal").is_some());
    }

    #[tokio::test]
    async fn protected_active_skill_refuses_proposals() {
        let store = store("komo_skillstore_protected");
        store.save(&skill("sync-cal")).await.unwrap();
        store.promote("sync-cal").unwrap();
        store.set_protected("sync-cal", true).unwrap();

        let err = store.save(&skill("sync-cal")).await.unwrap_err();
        assert!(err.to_string().contains("protected"));
        assert!(store.find_candidate("sync-cal").is_none());
    }

    #[tokio::test]
    async fn save_rejects_path_escaping_names() {
        let store = store("komo_skillstore_names");
        let mut bad = skill("ok");
        bad.name = "../escape".to_string();
        assert!(store.save(&bad).await.is_err());
    }

    #[tokio::test]
    async fn disable_and_enable_roundtrip() {
        let store = store("komo_skillstore_disable");
        store.save(&skill("sync-cal")).await.unwrap();
        store.promote("sync-cal").unwrap();

        let s = store.set_disabled("sync-cal", true).unwrap();
        assert!(s.disabled);
        assert!(store.find_active("sync-cal").unwrap().disabled);
        let s = store.set_disabled("sync-cal", false).unwrap();
        assert!(!s.disabled);
    }

    #[test]
    fn install_active_dir_copies_the_whole_skill_tree() {
        let store = store("komo_skillstore_install");
        // A multi-file skill: SKILL.md plus a supporting script.
        let src = std::env::temp_dir().join("komo_install_src");
        let _ = fs::remove_dir_all(&src);
        fs::create_dir_all(src.join("scripts")).unwrap();
        fs::write(
            src.join("SKILL.md"),
            "---\nname: mediarr\ndescription: media library\n---\nDo media things.",
        )
        .unwrap();
        fs::write(src.join("scripts").join("run.sh"), "echo hi\n").unwrap();

        let (skill, files) = store.install_active_dir(&src).unwrap();
        assert_eq!(skill.name, "mediarr");
        assert_eq!(files, 2);
        // Active + supporting file both landed in the store.
        assert!(store.find_active("mediarr").is_some());
        assert!(store.root().join("mediarr/scripts/run.sh").is_file());

        let _ = fs::remove_dir_all(&src);
    }

    #[test]
    fn install_refuses_to_overwrite_a_protected_skill() {
        let store = store("komo_skillstore_install_protected");
        let src = std::env::temp_dir().join("komo_install_src_prot");
        let _ = fs::remove_dir_all(&src);
        fs::create_dir_all(&src).unwrap();
        fs::write(
            src.join("SKILL.md"),
            "---\nname: guarded\ndescription: d\n---\nbody",
        )
        .unwrap();

        store.install_active_dir(&src).unwrap();
        store.set_protected("guarded", true).unwrap();
        // A second install of the same name is refused while protected.
        let err = store.install_active_dir(&src).unwrap_err();
        assert!(err.to_string().contains("protected"));

        let _ = fs::remove_dir_all(&src);
    }

    #[test]
    fn install_rejects_a_dir_without_a_valid_manifest() {
        let store = store("komo_skillstore_install_nomanifest");
        let src = std::env::temp_dir().join("komo_install_src_empty");
        let _ = fs::remove_dir_all(&src);
        fs::create_dir_all(&src).unwrap();
        assert!(store.install_active_dir(&src).is_err());
        let _ = fs::remove_dir_all(&src);
    }

    #[test]
    fn legacy_import_lands_candidates_once() {
        let store = store("komo_skillstore_import");
        let n = store.import_legacy_db(vec![skill("old-a"), skill("old-b")]);
        assert_eq!(n.unwrap(), 2);
        assert_eq!(store.list_candidates().len(), 2);
        // Marker makes the second import a no-op.
        let n = store.import_legacy_db(vec![skill("old-c")]);
        assert_eq!(n.unwrap(), 0);
        assert_eq!(store.list_candidates().len(), 2);
    }
}
