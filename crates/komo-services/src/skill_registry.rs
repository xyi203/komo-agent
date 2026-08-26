use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tracing::{debug, warn};

use komo_core::domain::skill::{Skill, SkillOffer};

/// Discovers skills from a set of `<name>/SKILL.md` directories.
///
/// When built via [`load_from_dirs`](Self::load_from_dirs) the registry holds
/// only the directory list and **re-scans on every query**, so a skill
/// installed, promoted, enabled, or disabled on disk is reflected the next time
/// the `skill` tool runs — no gateway restart needed (the filesystem is the
/// source of truth, matching `FsSkillStore` and the `komo skills` CLI). Reads
/// touch only a handful of small files, so live scanning is cheap. A registry
/// built via [`new`](Self::new) instead holds a fixed list and never re-scans
/// (used by tests).
pub struct SkillRegistry {
    /// Directories re-scanned on each query. Empty ⇒ this is a static registry
    /// backed by `static_skills`.
    dirs: Vec<PathBuf>,
    /// Fixed skill list, used only when `dirs` is empty.
    static_skills: Vec<Skill>,
}

/// A resolved skill together with where it lives, so `skill view` can tell the
/// model the base directory its relative paths (`scripts/`, `references/`)
/// resolve against — without that, a SKILL.md saying "run scripts/foo.py" is
/// unactionable.
pub struct LocatedSkill {
    pub skill: Skill,
    /// The skill's own directory (the one holding its `SKILL.md`). `None` only
    /// for a static registry, which never touched disk.
    pub dir: Option<PathBuf>,
}

/// How many directory entries [`skill_files`] will look at before giving up.
/// A skill directory is meant to hold a handful of scripts and references; the
/// budget is only there so a stray checkout under one can't turn a `view` into
/// a filesystem crawl.
const WALK_BUDGET: usize = 1000;

impl SkillRegistry {
    /// A static registry over a fixed skill list — never re-scans disk.
    /// Test-only (exposed to dependent crates' tests by the `test-support`
    /// feature): production builds the live, disk-backed registry via
    /// [`load_from_dirs`](Self::load_from_dirs).
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(skills: Vec<Skill>) -> Self {
        Self {
            dirs: Vec::new(),
            static_skills: skills,
        }
    }

    /// A live registry over multiple directories (e.g. komo's own `skills/`,
    /// the project's `.claude/skills/`, and the user's `~/.agents/skills/`).
    /// Each query re-scans these, so on-disk changes appear without a restart.
    /// The first directory to define a given skill name wins, so workspace-local
    /// skills override globally-shared ones.
    pub fn load_from_dirs(dirs: &[PathBuf]) -> Self {
        Self {
            dirs: dirs.to_vec(),
            static_skills: Vec::new(),
        }
    }

    /// The current skills, sorted by name. Live-scans `dirs` when set (first
    /// directory wins on a name clash), otherwise returns the static list.
    fn snapshot(&self) -> Vec<Skill> {
        self.snapshot_located()
            .into_iter()
            .map(|located| located.skill)
            .collect()
    }

    /// [`snapshot`](Self::snapshot) keeping each skill's directory.
    fn snapshot_located(&self) -> Vec<LocatedSkill> {
        if self.dirs.is_empty() {
            return self
                .static_skills
                .iter()
                .cloned()
                .map(|skill| LocatedSkill { skill, dir: None })
                .collect();
        }
        let mut skills = Vec::new();
        let mut seen = HashSet::new();
        for dir in &self.dirs {
            for located in Self::scan_dir(dir) {
                if seen.insert(located.skill.name.clone()) {
                    skills.push(located);
                }
            }
        }
        skills.sort_by(|a, b| a.skill.name.cmp(&b.skill.name));
        skills
    }

    fn scan_dir(dir: &Path) -> Vec<LocatedSkill> {
        let mut skills = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            debug!(?dir, "no skills directory; skipped");
            return skills;
        };
        for entry in entries.flatten() {
            let manifest = entry.path().join("SKILL.md");
            if !manifest.is_file() {
                continue;
            }
            match std::fs::read_to_string(&manifest) {
                Ok(content) => match Skill::parse(&content) {
                    Some(skill) => {
                        debug!(name = %skill.name, ?dir, "loaded skill");
                        skills.push(LocatedSkill {
                            skill,
                            dir: Some(entry.path()),
                        });
                    }
                    None => warn!(?manifest, "SKILL.md missing valid frontmatter; skipped"),
                },
                Err(e) => warn!(?manifest, %e, "failed to read SKILL.md"),
            }
        }
        skills
    }

    /// A capped `- name: description` catalog for the system prompt: lists up to
    /// `max` skills, noting how many more exist (use the `skill` tool to list all).
    ///
    /// `offer` gates what this always-on surface advertises: a skill whose
    /// `platforms` exclude this OS, or whose `requires_tools` this runtime did
    /// not register, is not worth a prompt line every turn. It is **only** hidden
    /// from this catalog — [`get`](Self::get) and the `skill` tool's `list` stay
    /// unfiltered, so naming it still loads it (explicit ask = explicit consent).
    pub fn catalog_capped(&self, max: usize, offer: &SkillOffer) -> String {
        let snapshot = self.snapshot();
        let enabled: Vec<&Skill> = snapshot
            .iter()
            .filter(|s| !s.disabled && s.offered_by(offer))
            .collect();
        let shown = enabled
            .iter()
            .take(max)
            .map(|s| format!("- {}: {}", s.name, s.description))
            .collect::<Vec<_>>()
            .join("\n");
        if enabled.len() <= max {
            return shown;
        }
        format!(
            "{shown}\n- …and {} more — call the `skill` tool with action=list to see all.",
            enabled.len() - max
        )
    }

    /// Look up by name, including disabled skills — the `skill` tool answers a
    /// `view` on a disabled skill with its state rather than "not found".
    pub fn get(&self, name: &str) -> Option<LocatedSkill> {
        self.snapshot_located()
            .into_iter()
            .find(|located| located.skill.name == name)
    }

    /// No usable (enabled) skills — gates the system-prompt catalog note.
    pub fn is_empty(&self) -> bool {
        !self.snapshot().iter().any(|s| !s.disabled)
    }

    /// A `- name: description` catalog for injection into the system prompt.
    pub fn catalog(&self) -> String {
        self.snapshot()
            .iter()
            .filter(|s| !s.disabled)
            .map(|s| format!("- {}: {}", s.name, s.description))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Up to `limit` asset paths inside a skill directory (`scripts/foo.py`,
/// `references/api.md`, …), recursive, sorted, absolute where the OS allows it,
/// and excluding the `SKILL.md` manifest itself. `.git` is skipped, matching
/// what the installer copies.
///
/// The result is deliberately a **sample**: callers must say so rather than let
/// the model read it as the complete inventory.
pub fn skill_files(dir: &Path, limit: usize) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    let mut visited = 0usize;
    while let Some(current) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            visited += 1;
            if visited > WALK_BUDGET {
                pending.clear();
                break;
            }
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                if entry.file_name() != ".git" {
                    pending.push(path);
                }
            } else if entry.file_name() != "SKILL.md" {
                files.push(std::path::absolute(&path).unwrap_or(path));
            }
        }
    }
    files.sort();
    files.truncate(limit);
    files
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_skills_from_directory() {
        let dir = std::env::temp_dir().join("komo_skill_reg_test");
        let skill_dir = dir.join("greet");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: greet\ndescription: Say hello nicely\n---\nGreet the user warmly.",
        )
        .unwrap();

        let reg = SkillRegistry::load_from_dirs(std::slice::from_ref(&dir));
        assert_eq!(reg.catalog().lines().count(), 1);
        assert_eq!(
            reg.get("greet").unwrap().skill.description,
            "Say hello nicely"
        );
        assert!(reg.catalog().contains("greet: Say hello nicely"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A skill written to disk *after* the registry is built must appear on the
    /// next query without reconstructing the registry — this is the no-restart
    /// hot-reload behavior (the bug that made `skill list` miss a freshly
    /// installed skill while `komo skills list` saw it).
    #[test]
    fn rescans_disk_so_new_skills_appear_without_restart() {
        let dir = std::env::temp_dir().join("komo_skill_hot_reload_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let reg = SkillRegistry::load_from_dirs(std::slice::from_ref(&dir));
        assert!(reg.is_empty());
        assert!(reg.get("late").is_none());

        // Install a skill after construction, as an approved `file` write would.
        let late = dir.join("late");
        std::fs::create_dir_all(&late).unwrap();
        std::fs::write(
            late.join("SKILL.md"),
            "---\nname: late\ndescription: Arrived after startup\n---\nDo the thing.",
        )
        .unwrap();

        // No reconstruction — the same registry now sees it.
        assert!(!reg.is_empty());
        assert_eq!(
            reg.get("late").unwrap().skill.description,
            "Arrived after startup"
        );
        assert!(reg.catalog().contains("late: Arrived after startup"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A registry built from an explicit list (tests) is static — no disk.
    #[test]
    fn static_registry_from_new_does_not_scan_disk() {
        let reg = SkillRegistry::new(vec![Skill {
            name: "fixed".into(),
            description: "d".into(),
            instructions: "b".into(),
            protected: false,
            disabled: false,
            source: "user".into(),
            platforms: Vec::new(),
            requires_tools: Vec::new(),
            updated_at: None,
        }]);
        assert_eq!(reg.get("fixed").unwrap().skill.instructions, "b");
        assert!(reg.catalog().contains("fixed"));
    }

    #[test]
    fn missing_directory_is_empty() {
        let reg = SkillRegistry::load_from_dirs(&[PathBuf::from("/nonexistent/komo/skills")]);
        assert!(reg.is_empty());
    }

    #[test]
    fn disabled_skills_are_hidden_from_the_catalog_but_still_resolvable() {
        let dir = std::env::temp_dir().join("komo_skill_disabled_test");
        for (name, disabled) in [("alive", false), ("paused", true)] {
            let d = dir.join(name);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(
                d.join("SKILL.md"),
                format!("---\nname: {name}\ndescription: d\ndisabled: {disabled}\n---\nbody"),
            )
            .unwrap();
        }

        let reg = SkillRegistry::load_from_dirs(std::slice::from_ref(&dir));
        assert!(reg.catalog().contains("alive"));
        assert!(!reg.catalog().contains("paused"));
        assert!(!reg.is_empty());
        // Still resolvable so the `skill` tool can explain its state.
        assert!(reg.get("paused").unwrap().skill.disabled);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The whole point of offer gating: it trims the always-on prompt catalog
    /// and nothing else. A gated-out skill must still resolve by name, or the
    /// gate has quietly become a load gate.
    #[test]
    fn offer_gating_trims_the_catalog_but_never_the_lookup() {
        let dir = std::env::temp_dir().join("komo_skill_offer_test");
        let _ = std::fs::remove_dir_all(&dir);
        for (name, front) in [
            ("plain", ""),
            ("mac-only", "platforms: [macos]\n"),
            ("needs-ha", "requires_tools: [homeassistant]\n"),
        ] {
            let d = dir.join(name);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(
                d.join("SKILL.md"),
                format!("---\nname: {name}\ndescription: d\n{front}---\nbody"),
            )
            .unwrap();
        }

        let reg = SkillRegistry::load_from_dirs(std::slice::from_ref(&dir));
        let offer = SkillOffer {
            platform: "linux".to_string(),
            tools: HashSet::new(),
        };
        let catalog = reg.catalog_capped(10, &offer);
        assert!(catalog.contains("plain"));
        assert!(!catalog.contains("mac-only"), "{catalog}");
        assert!(!catalog.contains("needs-ha"), "{catalog}");

        // Still loadable by name, and still listed by the unfiltered catalog the
        // `skill` tool's discovery action uses.
        assert!(reg.get("mac-only").is_some());
        assert!(reg.get("needs-ha").is_some());
        assert!(reg.catalog().contains("needs-ha"));

        // Supply the tool and the platform, and both come back.
        let offer = SkillOffer {
            platform: "macos".to_string(),
            tools: HashSet::from(["homeassistant".to_string()]),
        };
        let catalog = reg.catalog_capped(10, &offer);
        assert!(catalog.contains("mac-only"));
        assert!(catalog.contains("needs-ha"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn all_disabled_counts_as_empty() {
        let reg = SkillRegistry::new(vec![Skill {
            name: "paused".into(),
            description: "d".into(),
            instructions: "b".into(),
            protected: false,
            disabled: true,
            source: "user".into(),
            platforms: Vec::new(),
            requires_tools: Vec::new(),
            updated_at: None,
        }]);
        assert!(reg.is_empty());
        assert!(reg.catalog().is_empty());
    }

    #[test]
    fn skill_files_samples_assets_recursively_and_excludes_the_manifest() {
        let dir = std::env::temp_dir().join("komo_skill_files_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("scripts")).unwrap();
        std::fs::create_dir_all(dir.join(".git")).unwrap();
        std::fs::write(dir.join("SKILL.md"), "---\nname: s\n---\nbody").unwrap();
        std::fs::write(dir.join("scripts/foo.py"), "print(1)").unwrap();
        std::fs::write(dir.join(".git/config"), "[core]").unwrap();

        let files = skill_files(&dir, 10);
        assert_eq!(files.len(), 1, "{files:?}");
        assert!(files[0].ends_with("scripts/foo.py"));
        assert!(files[0].is_absolute());

        // The cap is a sample, not a filter: 12 assets ⇒ the first 10 by path.
        for i in 0..12 {
            std::fs::write(dir.join(format!("ref{i:02}.md")), "x").unwrap();
        }
        assert_eq!(skill_files(&dir, 10).len(), 10);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn first_directory_wins_on_name_collision() {
        let base = std::env::temp_dir().join("komo_skill_dirs_test");
        let local = base.join("local");
        let global = base.join("global");
        for (dir, body) in [(&local, "LOCAL version"), (&global, "GLOBAL version")] {
            let d = dir.join("dup");
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(
                d.join("SKILL.md"),
                format!("---\nname: dup\ndescription: d\n---\n{body}"),
            )
            .unwrap();
        }

        let reg = SkillRegistry::load_from_dirs(&[local.clone(), global.clone()]);
        assert_eq!(reg.catalog().lines().count(), 1);
        assert!(reg.get("dup").unwrap().skill.instructions.contains("LOCAL"));

        let _ = std::fs::remove_dir_all(&base);
    }
}
