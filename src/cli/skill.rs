//! Operator governance over the skill store (`~/.komo/skills`) — roadmap §9.
//!
//! All of these are pure file operations on the governed store, so they work
//! whether or not the gateway is running (no db lock involved). The runtime
//! `SkillRegistry` re-scans the skill dirs on every query, so changes that
//! affect the agent's catalog (install / promote / enable / disable) take
//! effect on its next agent `skill` tool list — no gateway restart.

use crate::{
    cli::inspect::local_time,
    services::operator_control::{OperatorControl, OperatorQuery, OperatorQueryResult},
};
use komo_infra::skills::FsSkillStore;
use std::{collections::HashSet, path::PathBuf};

fn store() -> FsSkillStore {
    FsSkillStore::new(FsSkillStore::default_root())
}

fn shared_store() -> Option<FsSkillStore> {
    dirs::home_dir().map(|home| FsSkillStore::new(home.join(".agents/skills")))
}

struct ReadableSkill {
    skill: crate::domain::skill::Skill,
    status: &'static str,
    path: PathBuf,
    history: Vec<String>,
    /// Prior active bodies a promote overwrote (`.history/<name>/`).
    prior_active: Vec<String>,
}

fn find_readable_skill(
    name: &str,
    managed: &FsSkillStore,
    shared: Option<&FsSkillStore>,
) -> Option<ReadableSkill> {
    if let Some(skill) = managed.find_active(name) {
        return Some(ReadableSkill {
            skill,
            status: "active",
            path: managed.active_path(name),
            history: managed.candidate_history(name),
            prior_active: managed.active_history(name),
        });
    }
    if let Some(found) = shared.and_then(|shared| {
        shared.find_active(name).map(|skill| ReadableSkill {
            skill,
            status: "shared (read-only)",
            path: shared.active_path(name),
            history: Vec::new(),
            prior_active: Vec::new(),
        })
    }) {
        return Some(found);
    }
    if let Some(skill) = managed.find_candidate(name) {
        return Some(ReadableSkill {
            skill,
            status: "candidate",
            path: managed.candidate_path(name),
            history: managed.candidate_history(name),
            prior_active: managed.active_history(name),
        });
    }
    managed.find_archived(name).map(|skill| ReadableSkill {
        skill,
        status: "archived",
        path: managed.archived_path(name),
        history: Vec::new(),
        prior_active: managed.active_history(name),
    })
}

/// List Komo-managed skills plus the shared skills already installed for local
/// agents under `~/.agents/skills`. Shared skills are discoverable and
/// inspectable, but governance commands only mutate the managed store.
pub fn list() -> anyhow::Result<()> {
    let managed = store();
    let shared = shared_store();
    let active = managed.list_active();
    let candidates = managed.list_candidates();
    let archived = managed.list_archived();
    let expired = managed.list_expired();
    let managed_names = active
        .iter()
        .map(|skill| skill.name.as_str())
        .collect::<HashSet<_>>();
    let shared_skills = shared
        .as_ref()
        .map(FsSkillStore::list_active)
        .unwrap_or_default()
        .into_iter()
        .filter(|skill| !managed_names.contains(skill.name.as_str()))
        .collect::<Vec<_>>();

    if active.is_empty()
        && candidates.is_empty()
        && shared_skills.is_empty()
        && archived.is_empty()
        && expired.is_empty()
    {
        println!(
            "No skills in {} or ~/.agents/skills.",
            managed.root().display()
        );
        return Ok(());
    }
    if !active.is_empty() {
        println!("managed ({}):", managed.root().display());
        for skill in &active {
            print_skill_line(skill, "  ");
        }
    }
    if !shared_skills.is_empty() {
        let root = shared
            .as_ref()
            .expect("shared skills came from this store")
            .root();
        println!("shared ({}; read-only):", root.display());
        for skill in &shared_skills {
            print_skill_line(skill, "  ");
        }
    }
    if !candidates.is_empty() {
        println!("candidates (`komo skills promote|reject <name>`):");
        for skill in &candidates {
            println!(
                "  {}  [{}]  {}",
                skill.name,
                skill.source,
                oneline(&skill.description, 80)
            );
        }
    }
    if !archived.is_empty() {
        println!("archived (`komo skills restore <name>`):");
        for skill in &archived {
            println!("  {}  {}", skill.name, oneline(&skill.description, 80));
        }
    }
    if !expired.is_empty() {
        println!("withdrawn proposals (`komo skills restore <name>` to reconsider):");
        for skill in &expired {
            println!("  {}  {}", skill.name, oneline(&skill.description, 80));
        }
    }
    Ok(())
}

fn print_skill_line(skill: &crate::domain::skill::Skill, prefix: &str) {
    let lock = if skill.protected { " 🔒" } else { "" };
    let off = if skill.disabled { " [disabled]" } else { "" };
    println!(
        "{prefix}{}{}{}  {}",
        skill.name,
        lock,
        off,
        oneline(&skill.description, 80)
    );
}

fn oneline(value: &str, max: usize) -> String {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.chars().count() <= max {
        return value;
    }
    format!(
        "{}…",
        value
            .chars()
            .take(max.saturating_sub(1))
            .collect::<String>()
    )
}

const RELOAD_HINT: &str = "Takes effect on the agent's next `skill` list (no restart needed).";

/// Install a skill from a git repo (`owner/repo[/subpath]`, a GitHub URL, or any
/// `*.git`/`git@…` URL) or a raw `SKILL.md` URL, straight into the active store.
/// Pure file+network ops — works while the gateway holds the db lock; the live
/// registry means the running agent sees it on its next `skill` list, no restart.
pub async fn install(source: &str) -> anyhow::Result<()> {
    let installed = komo_infra::skill_install::install(&store(), source).await?;
    let files = if installed.files == 1 {
        "1 file".to_string()
    } else {
        format!("{} files", installed.files)
    };
    println!(
        "Installed `{}` ({files}) → {}",
        installed.name,
        installed.path.display()
    );
    if !installed.description.is_empty() {
        println!("  {}", installed.description);
    }
    println!("Active now; the agent picks it up on its next `skill` list (no restart needed).");
    Ok(())
}

/// Accept a reviewer candidate: move it into the active store (overwriting the
/// active skill of the same name, i.e. accepting an update proposal).
pub fn promote(name: &str) -> anyhow::Result<()> {
    let skill = store().promote(name)?;
    println!("Promoted `{}` to active. {RELOAD_HINT}", skill.name);
    Ok(())
}

/// Discard a reviewer candidate.
pub fn reject(name: &str) -> anyhow::Result<()> {
    store().reject(name)?;
    println!("Rejected candidate `{name}` (deleted).");
    Ok(())
}

/// Retire an active skill without deleting it: the whole directory moves to
/// `.archive/`, out of every scan. `disable` is the lighter form (the skill
/// stays in place); archive is for skills you're done with but won't discard.
pub fn archive(name: &str) -> anyhow::Result<()> {
    let managed = store();
    let skill = managed.archive(name)?;
    println!(
        "Archived `{}` → {}. Bring it back with `komo skills restore {}`. {RELOAD_HINT}",
        skill.name,
        managed.archive_root().display(),
        skill.name
    );
    Ok(())
}

/// Bring back a skill that was set aside — an archived one to the active
/// catalog, a candidate dreaming withdrew back to triage.
///
/// One verb for both, dispatching on where the skill actually is: an expired
/// proposal returns as a *candidate*, never as active, because nobody ever
/// approved it.
pub fn restore(name: &str) -> anyhow::Result<()> {
    let managed = store();
    if managed.find_expired(name).is_some() {
        let skill = managed.unexpire(name)?;
        println!(
            "Restored `{}` to candidates — promote it with `komo skills promote {}`.",
            skill.name, skill.name
        );
        return Ok(());
    }
    let skill = managed.restore(name)?;
    println!("Restored `{}` to active. {RELOAD_HINT}", skill.name);
    Ok(())
}

/// Set or clear `protected`: a protected skill is operator-edit-only — the
/// reviewer stops writing even candidate proposals for it.
pub fn protect(name: &str, on: bool) -> anyhow::Result<()> {
    let skill = store().set_protected(name, on)?;
    if skill.protected {
        println!(
            "Protected `{}` — the reviewer will no longer propose changes to it.",
            skill.name
        );
    } else {
        println!("Unprotected `{}`.", skill.name);
    }
    Ok(())
}

/// Enable or disable an active skill without deleting it: disabled skills stay
/// on disk and inspectable but are hidden from the model's catalog.
pub fn set_enabled(name: &str, enabled: bool) -> anyhow::Result<()> {
    let skill = store().set_disabled(name, !enabled)?;
    if skill.disabled {
        println!(
            "Disabled `{}` — kept on disk, hidden from the agent. {RELOAD_HINT}",
            skill.name
        );
    } else {
        println!("Enabled `{}`. {RELOAD_HINT}", skill.name);
    }
    Ok(())
}

/// One skill in full: status, provenance, file path, prior candidate versions,
/// and the instruction body.
pub fn inspect(name: &str) -> anyhow::Result<()> {
    let managed = store();
    let shared = shared_store();
    let found = find_readable_skill(name, &managed, shared.as_ref()).ok_or_else(|| {
        anyhow::anyhow!(
            "no skill named `{name}` in {} or ~/.agents/skills",
            managed.root().display()
        )
    })?;
    let skill = found.skill;

    println!("skill      {}", skill.name);
    let mut state = found.status.to_string();
    if skill.protected {
        state.push_str(" 🔒 protected");
    }
    if skill.disabled {
        state.push_str(" [disabled]");
    }
    println!("status     {state}");
    println!("source     {}", skill.source);
    println!("path       {}", found.path.display());
    if !skill.description.is_empty() {
        println!("describes  {}", skill.description);
    }
    // Offer gating trims the agent's always-on catalog only, so say so — an
    // operator seeing `platforms` here should not think the skill is unusable.
    if !skill.platforms.is_empty() || !skill.requires_tools.is_empty() {
        let mut gates = Vec::new();
        if !skill.platforms.is_empty() {
            gates.push(format!("platforms {}", skill.platforms.join("/")));
        }
        if !skill.requires_tools.is_empty() {
            gates.push(format!("needs tool {}", skill.requires_tools.join(" + ")));
        }
        println!(
            "offered    {} — catalog only; always loadable by name",
            gates.join(", ")
        );
    }
    if !found.history.is_empty() {
        println!(
            "proposals  {} prior candidate version(s): {}",
            found.history.len(),
            found.history.join(", ")
        );
    }
    if !found.prior_active.is_empty() {
        println!(
            "replaced   {} prior active body(ies) kept in {}: {}",
            found.prior_active.len(),
            managed.root().join(".history").join(&skill.name).display(),
            found.prior_active.join(", ")
        );
    }
    println!("audit      `komo skills audit {name}` shows which turns loaded it");
    println!("\n{}", skill.instructions);
    Ok(())
}

/// Which turns loaded this skill — derived from the run ledger (`skill view`
/// steps), so it needs the db, i.e. the operator surface. Without a name,
/// the whole catalog ranked coldest-first.
pub async fn audit(control: &OperatorControl, name: Option<&str>) -> anyhow::Result<()> {
    let Some(name) = name else {
        return usage(control).await;
    };
    let OperatorQueryResult::SkillAudit(invocations) = control
        .query(OperatorQuery::SkillAudit {
            name: name.to_string(),
        })
        .await?
    else {
        unreachable!("SkillAudit query answers with SkillAudit");
    };
    if invocations.is_empty() {
        println!("No recorded loads of `{name}` in the run ledger.");
        return Ok(());
    }
    for i in &invocations {
        let mark = if i.ok { "ok " } else { "ERR" };
        println!(
            "{}  load {}  turn {:<8}  step #{}  {}",
            local_time(i.started_at),
            mark,
            i.verdict.as_str(),
            i.seq,
            i.run_id
        );
    }
    let failures: Vec<&str> = invocations
        .iter()
        .filter(|i| i.verdict == komo_core::domain::episode::OutcomeVerdict::Failure)
        .map(|i| i.run_id.as_str())
        .collect();
    if !failures.is_empty() {
        // The counter-examples, named. A skill that keeps being loaded into
        // turns the user then rejects is the case this report exists for, and
        // burying it in a count would make it invisible.
        println!(
            "\n{} turn(s) that loaded this skill ended in failure: {}",
            failures.len(),
            failures.join(", ")
        );
    }
    println!(
        "\n(`load ok` means the skill was read; `turn` is how that turn ended, \
         which is what says whether it helped.)\n\
         (`komo run inspect <id>` shows the full turn.)"
    );
    Ok(())
}

/// Every active skill, coldest first. Nothing stores a usage counter — this is
/// re-derived from the run ledger on every call, so it only sees as far back as
/// the ledger still reaches (and `state.db` is disposable by design).
async fn usage(control: &OperatorControl) -> anyhow::Result<()> {
    let OperatorQueryResult::SkillUsage(rows) = control.query(OperatorQuery::SkillUsage).await?
    else {
        unreachable!("SkillUsage query answers with SkillUsage");
    };
    if rows.is_empty() {
        println!("No active skills.");
        return Ok(());
    }
    let width = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(4);
    for row in &rows {
        let last = match row.last_at {
            Some(at) => local_time(at),
            None => "never".to_string(),
        };
        let outcomes = if row.failed > 0 {
            format!("  {}✓ {}✗ {}?", row.succeeded, row.failed, row.unknown)
        } else if row.views > 0 {
            format!("  {}✓ {}?", row.succeeded, row.unknown)
        } else {
            String::new()
        };
        println!(
            "  {:width$}  {:>4} view(s){outcomes}  last {}",
            row.name,
            row.views,
            last,
            width = width
        );
    }
    println!(
        "\n(Derived from the run ledger, not a stored counter — `never` means \
         \"not in the ledger's window\", not \"never in its life\".)\n\
         (✓/✗/? are the turns that loaded it, by how they ended. `?` is the \
         honest majority: nothing settled whether the turn worked, and a skill \
         that was loaded but never followed lands there too.)\n\
         (`komo skills audit <name>` shows the individual turns.)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_skill(root: &std::path::Path, name: &str, body: &str) {
        let dir = root.join(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: test\n---\n{body}"),
        )
        .unwrap();
    }

    #[test]
    fn readable_skill_includes_shared_agents_store_with_managed_precedence() {
        let base = std::env::temp_dir().join("komo_cli_shared_skills");
        let _ = fs::remove_dir_all(&base);
        let managed = FsSkillStore::new(base.join("managed"));
        let shared = FsSkillStore::new(base.join("shared"));
        write_skill(shared.root(), "only-shared", "shared body");
        write_skill(shared.root(), "duplicate", "shared version");
        write_skill(shared.root(), "candidate-clash", "shared active version");
        write_skill(managed.root(), "duplicate", "managed version");
        write_skill(
            &managed.root().join(".candidates"),
            "candidate-clash",
            "candidate version",
        );

        let found = find_readable_skill("only-shared", &managed, Some(&shared)).unwrap();
        assert_eq!(found.status, "shared (read-only)");
        assert!(found.skill.instructions.contains("shared body"));

        let found = find_readable_skill("duplicate", &managed, Some(&shared)).unwrap();
        assert_eq!(found.status, "active");
        assert!(found.skill.instructions.contains("managed version"));

        let found = find_readable_skill("candidate-clash", &managed, Some(&shared)).unwrap();
        assert_eq!(found.status, "shared (read-only)");
        assert!(found.skill.instructions.contains("shared active version"));

        let _ = fs::remove_dir_all(&base);
    }
}
