/// A named capability package: lightweight metadata (`name` + `description`)
/// plus a full instruction body loaded on demand (progressive disclosure).
///
/// Governance metadata lives in the same `SKILL.md` frontmatter as the
/// identity fields (skills are files — roadmap §9; the filesystem is the single
/// source of truth):
/// - `protected`: only the operator may change this skill — the reviewer never
///   writes a candidate proposal for it.
/// - `disabled`: kept on disk and inspectable, but hidden from the model's
///   catalog; `skill view` reports it as disabled instead of loading it.
/// - `source`: provenance — `user` (hand-written, the default) or `reviewer`
///   (extracted by the reflective reviewer).
///
/// Two further keys gate where a skill is *offered* — see [`SkillOffer`]:
/// - `platforms`: OS list (`[macos]`, `[linux, macos]`).
/// - `requires_tools`: tool names the skill's procedure depends on.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub instructions: String,
    #[serde(default)]
    pub protected: bool,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default)]
    pub platforms: Vec<String>,
    #[serde(default)]
    pub requires_tools: Vec<String>,
    /// When the store last wrote this file, as unix seconds — read back from the
    /// `updated_at` frontmatter the renderer has always written. `None` for a
    /// hand-written file that carries no such key.
    ///
    /// An observation of the file, not state the caller sets: every write stamps
    /// it afresh. It exists because a skill *candidate* has no other clock —
    /// nothing can load one (dot directories never enter the runtime's scan), so
    /// there is no usage signal to age it by, only how long the proposal has been
    /// sitting there. See [`candidate_expired`].
    #[serde(default)]
    pub updated_at: Option<i64>,
}

/// What this runtime can offer, for **offer-time** skill gating.
///
/// This filters the always-on system-prompt catalog only — the surface where an
/// irrelevant skill costs tokens every single turn. It is deliberately NOT a
/// load gate: `skill view`, the `skill` tool's `list`, and every `komo skills`
/// command ignore it, because asking for a skill by name is explicit consent.
/// A skill gated out of the prompt still works the moment someone names it.
pub struct SkillOffer {
    /// `std::env::consts::OS` — `macos` / `linux` / `windows`.
    pub platform: String,
    /// Tool names this runtime actually registered (post policy-deny drop).
    pub tools: std::collections::HashSet<String>,
}

impl SkillOffer {
    /// The offer for the current process over `tools`.
    pub fn here(tools: impl IntoIterator<Item = String>) -> Self {
        Self {
            platform: std::env::consts::OS.to_string(),
            tools: tools.into_iter().collect(),
        }
    }

    fn platform_matches(&self, declared: &str) -> bool {
        normalize_platform(declared) == normalize_platform(&self.platform)
    }
}

/// `darwin` is what the rest of the world calls Rust's `macos`; accept both so a
/// skill copied from another agent's collection still gates correctly.
fn normalize_platform(value: &str) -> String {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "darwin" => "macos".to_string(),
        "win32" | "windows" => "windows".to_string(),
        _ => value,
    }
}

/// Provenance values for [`Skill::source`].
pub const SOURCE_USER: &str = "user";
pub const SOURCE_REVIEWER: &str = "reviewer";
/// On-demand distillation the operator explicitly asked for (the `learn` action
/// of the `skill` tool), as opposed to the reviewer's passive extraction. Both
/// land as candidates for triage; the provenance only records *why* it exists.
pub const SOURCE_LEARNED: &str = "learned";

fn default_source() -> String {
    SOURCE_USER.to_string()
}

/// A skill name doubles as its directory name on disk, so it must be a plain
/// path segment: non-empty, `[A-Za-z0-9._-]`, and not starting with `.` (dot
/// prefixes are reserved for governance dirs like `.candidates`). This is the
/// floor that keeps an LLM-suggested name from escaping the skills tree.
pub fn valid_skill_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

impl Skill {
    /// Parse a `SKILL.md` document: YAML-ish frontmatter (`name`, `description`,
    /// and the governance keys `protected` / `disabled` / `source`) fenced by
    /// `---`, followed by the instruction body.
    pub fn parse(content: &str) -> Option<Skill> {
        let rest = content.trim_start().strip_prefix("---")?;
        let fence = rest.find("\n---")?;
        let front = &rest[..fence];
        let body = rest[fence + "\n---".len()..]
            .trim_start_matches('-')
            .trim_start_matches(['\n', '\r'])
            .trim()
            .to_string();

        let mut name = None;
        let mut description = None;
        let mut protected = false;
        let mut disabled = false;
        let mut source = default_source();
        let mut platforms = Vec::new();
        let mut requires_tools = Vec::new();
        let mut updated_at = None;
        let lines: Vec<&str> = front.lines().collect();
        let mut cursor = 0;
        while let Some(line) = lines.get(cursor) {
            cursor += 1;
            if let Some(v) = line.strip_prefix("name:") {
                name = Some(unquote(v.trim()));
            } else if let Some(v) = line.strip_prefix("description:") {
                description = Some(unquote(v.trim()));
            } else if let Some(v) = line.strip_prefix("protected:") {
                protected = v.trim() == "true";
            } else if let Some(v) = line.strip_prefix("disabled:") {
                disabled = v.trim() == "true";
            } else if let Some(v) = line.strip_prefix("source:") {
                source = unquote(v.trim());
            } else if let Some(v) = line.strip_prefix("platforms:") {
                platforms = parse_list(v, &lines, &mut cursor);
            } else if let Some(v) = line.strip_prefix("requires_tools:") {
                requires_tools = parse_list(v, &lines, &mut cursor);
            } else if let Some(v) = line.strip_prefix("updated_at:") {
                updated_at = parse_rfc3339(&unquote(v.trim()));
            }
        }

        let name = name?;
        if name.is_empty() {
            return None;
        }
        Some(Skill {
            name,
            description: description.unwrap_or_default(),
            instructions: body,
            protected,
            disabled,
            source,
            platforms,
            requires_tools,
            updated_at,
        })
    }

    /// Whether this skill belongs in `offer`'s always-on catalog.
    ///
    /// Platforms are OR (any declared match wins); required tools are AND (the
    /// procedure needs all of them to be followable). An absent or empty list is
    /// no constraint, so every existing skill keeps behaving exactly as before.
    pub fn offered_by(&self, offer: &SkillOffer) -> bool {
        if !self.platforms.is_empty()
            && !self
                .platforms
                .iter()
                .any(|declared| offer.platform_matches(declared))
        {
            return false;
        }
        self.requires_tools
            .iter()
            .all(|tool| offer.tools.contains(tool.as_str()))
    }
}

/// A frontmatter list value, in either YAML shape: inline (`platforms: [a, b]`)
/// or a following block of `- item` lines, which `cursor` is advanced past.
/// Anything else yields an empty list — no constraint.
fn parse_list(inline: &str, lines: &[&str], cursor: &mut usize) -> Vec<String> {
    let inline = inline.trim();
    let items: Vec<String> = if inline.is_empty() {
        let mut block = Vec::new();
        while let Some(item) = lines
            .get(*cursor)
            .and_then(|line| line.trim_start().strip_prefix("- "))
        {
            block.push(unquote(item.trim()));
            *cursor += 1;
        }
        block
    } else {
        inline
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split(',')
            .map(|item| unquote(item.trim()))
            .collect()
    };
    items.into_iter().filter(|item| !item.is_empty()).collect()
}

fn unquote(s: &str) -> String {
    s.trim_matches(|c| c == '"' || c == '\'').to_string()
}

/// An RFC 3339 frontmatter stamp as unix seconds, or `None` when it does not
/// parse — an unreadable date reads as "no date", which is what keeps a
/// malformed stamp from being taken for the epoch and aging a skill out
/// instantly.
fn parse_rfc3339(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|t| t.timestamp())
}

// ── candidate expiry (the skill half of dreaming) ────────────────────────────

/// How long a skill candidate waits for a human verdict before dreaming
/// withdraws it.
///
/// Thirty days, matching a memory candidate's forget window: these are both
/// proposals that lapse for want of a decision. Withdrawal is reversible — the
/// files move aside, nothing is deleted — and a pattern that still holds will be
/// proposed again by the next review that sees it, which is what makes lapsing
/// the right default rather than keeping every proposal forever.
pub const SKILL_CANDIDATE_EXPIRY_DAYS: i64 = 30;

/// Whether a candidate has sat unanswered long enough to be withdrawn.
///
/// Age is the *only* signal available here, and deliberately so. A candidate
/// cannot be loaded — dot directories never enter the runtime's skill scan — so
/// unlike a memory candidate it accumulates no usage to be judged on. A skill
/// candidate that is never promoted is never used, and the only thing its
/// continued presence measures is how long nobody has triaged it.
///
/// A file with no readable `updated_at` never expires: absent evidence of age is
/// not evidence of staleness, and a hand-written candidate should not vanish
/// because it lacks a key the store happens to write.
pub fn candidate_expired(skill: &Skill, now: i64) -> bool {
    skill
        .updated_at
        .is_some_and(|at| (now - at).max(0) / 86_400 > SKILL_CANDIDATE_EXPIRY_DAYS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_frontmatter_and_body() {
        let doc = "---\nname: summarize-file\ndescription: \"Summarize a file\"\n---\n\nStep 1. Read it.\nStep 2. Summarize.\n";
        let skill = Skill::parse(doc).unwrap();
        assert_eq!(skill.name, "summarize-file");
        assert_eq!(skill.description, "Summarize a file");
        assert!(skill.instructions.starts_with("Step 1."));
        assert!(skill.instructions.contains("Step 2."));
        assert!(!skill.protected);
        assert!(!skill.disabled);
        assert_eq!(skill.source, SOURCE_USER);
    }

    #[test]
    fn parses_governance_keys() {
        let doc = "---\nname: risky\nprotected: true\ndisabled: true\nsource: reviewer\n---\nbody";
        let skill = Skill::parse(doc).unwrap();
        assert!(skill.protected);
        assert!(skill.disabled);
        assert_eq!(skill.source, SOURCE_REVIEWER);
    }

    fn offer(platform: &str, tools: &[&str]) -> SkillOffer {
        SkillOffer {
            platform: platform.to_string(),
            tools: tools.iter().map(|t| t.to_string()).collect(),
        }
    }

    #[test]
    fn parses_offer_lists_in_both_yaml_shapes() {
        let inline = Skill::parse(
            "---\nname: a\nplatforms: [macos, linux]\nrequires_tools: [\"homeassistant\"]\n---\nbody",
        )
        .unwrap();
        assert_eq!(inline.platforms, ["macos", "linux"]);
        assert_eq!(inline.requires_tools, ["homeassistant"]);

        let block = Skill::parse(
            "---\nname: a\nplatforms:\n  - macos\n  - linux\ndescription: after the list\n---\nbody",
        )
        .unwrap();
        assert_eq!(block.platforms, ["macos", "linux"]);
        // The block scan stops at the first non-item line, so later keys survive.
        assert_eq!(block.description, "after the list");
    }

    #[test]
    fn a_skill_declaring_nothing_is_offered_everywhere() {
        let skill = Skill::parse("---\nname: a\n---\nbody").unwrap();
        assert!(skill.offered_by(&offer("macos", &[])));
        assert!(skill.offered_by(&offer("windows", &[])));
    }

    #[test]
    fn platforms_gate_on_any_match_and_accept_the_darwin_alias() {
        let skill = Skill::parse("---\nname: a\nplatforms: [darwin]\n---\nbody").unwrap();
        assert!(skill.offered_by(&offer("macos", &[])));
        assert!(!skill.offered_by(&offer("linux", &[])));
    }

    #[test]
    fn required_tools_must_all_be_present() {
        let skill = Skill::parse("---\nname: a\nrequires_tools: [homeassistant, shell]\n---\nbody")
            .unwrap();
        assert!(skill.offered_by(&offer("macos", &["homeassistant", "shell", "read"])));
        assert!(!skill.offered_by(&offer("macos", &["homeassistant"])));
        assert!(!skill.offered_by(&offer("macos", &[])));
    }

    /// An empty value has no items to gate on, so the skill stays offered —
    /// gating is opt-in and a half-written key must not hide a skill.
    #[test]
    fn an_empty_list_is_no_constraint() {
        let skill = Skill::parse("---\nname: a\nplatforms:\ndescription: d\n---\nbody").unwrap();
        assert!(skill.platforms.is_empty());
        assert!(skill.offered_by(&offer("linux", &[])));
    }

    /// A bare scalar (`platforms: macos`) is a common hand-written slip; read it
    /// as a one-item list rather than as a constraint that matches nothing.
    #[test]
    fn a_scalar_reads_as_a_one_item_list() {
        let skill = Skill::parse("---\nname: a\nplatforms: macos\n---\nbody").unwrap();
        assert_eq!(skill.platforms, ["macos"]);
        assert!(skill.offered_by(&offer("macos", &[])));
        assert!(!skill.offered_by(&offer("linux", &[])));
    }

    #[test]
    fn rejects_document_without_frontmatter() {
        assert!(Skill::parse("no frontmatter here").is_none());
    }

    #[test]
    fn updated_at_is_read_back_from_the_stamp_the_store_writes() {
        let doc = "---\nname: a\nupdated_at: 2026-07-04T14:45:42.136487Z\n---\nbody";
        let skill = Skill::parse(doc).unwrap();
        assert_eq!(skill.updated_at, Some(1783176342));

        // A file the store never wrote, and one whose stamp is unreadable, both
        // read as undated rather than as the epoch.
        assert_eq!(
            Skill::parse("---\nname: a\n---\nb").unwrap().updated_at,
            None
        );
        assert_eq!(
            Skill::parse("---\nname: a\nupdated_at: last tuesday\n---\nb")
                .unwrap()
                .updated_at,
            None
        );
    }

    #[test]
    fn a_candidate_expires_only_once_it_is_older_than_the_window() {
        let now = 10_000 * 86_400;
        let dated = |days_ago: i64| Skill {
            updated_at: Some(now - days_ago * 86_400),
            ..Skill::parse("---\nname: a\n---\nbody").unwrap()
        };
        assert!(!candidate_expired(&dated(SKILL_CANDIDATE_EXPIRY_DAYS), now));
        assert!(candidate_expired(
            &dated(SKILL_CANDIDATE_EXPIRY_DAYS + 1),
            now
        ));
    }

    /// An undated file is not a stale one: without a stamp there is no age to
    /// judge, so it stays until a human rules on it.
    #[test]
    fn an_undated_candidate_never_expires() {
        let skill = Skill::parse("---\nname: a\n---\nbody").unwrap();
        assert!(!candidate_expired(&skill, 10_000 * 86_400));
    }

    #[test]
    fn skill_names_must_be_plain_path_segments() {
        assert!(valid_skill_name("feishu-calendar"));
        assert!(valid_skill_name("v2_sync.beta"));
        assert!(!valid_skill_name(""));
        assert!(!valid_skill_name(".candidates"));
        assert!(!valid_skill_name("../escape"));
        assert!(!valid_skill_name("a/b"));
        assert!(!valid_skill_name("with space"));
    }
}
