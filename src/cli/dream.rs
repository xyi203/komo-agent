//! `komo dream` — operator view over the usage-driven memory "dreaming"
//! consolidation (the OpenClaw-borrowed back-loop).
//!
//! By default this is a **dry run**: it shows which candidate memories would be
//! promoted (corroborated on independent occasions, or explicitly confirmed) or
//! archived (refuted and left unresolved, or old and gone cold), ordered by the
//! dreaming score — like OpenClaw's `rem-harness` / `promote-explain`. The score
//! ranks each bucket; it never decides a verdict.
//! Pass `--apply` to actually run one consolidation cycle
//! (the same `DreamSweep` the gateway runs on `dream_schedule`).
//!
//! Both preview and apply run through the operator surface — whichever
//! transport answers (a running gateway, or the store directly) is not this
//! module's business.

use komo_core::domain::skill::SKILL_CANDIDATE_EXPIRY_DAYS;

use crate::services::operator_control::{
    DreamItem, OperatorCommand, OperatorCommandResult, OperatorControl, OperatorQuery,
    OperatorQueryResult,
};

/// Run a dreaming cycle, or preview one. `apply = false` mutates nothing.
pub async fn run(control: &OperatorControl, apply: bool) -> anyhow::Result<()> {
    let OperatorQueryResult::DreamPreview(report) =
        control.query(OperatorQuery::DreamPreview).await?
    else {
        unreachable!("DreamPreview query answers with DreamPreview");
    };

    if !report.has_actions() {
        println!("{}", no_action_summary(&report));
        return Ok(());
    }

    report_bucket(
        "promote → active (well-recalled candidates)",
        &report.promote,
    );
    report_bucket("archive (refuted, or old and gone cold)", &report.archive);

    if !report.expire_skills.is_empty() {
        println!(
            "\nwithdraw skill proposal (no verdict in {} days): {}",
            SKILL_CANDIDATE_EXPIRY_DAYS,
            report.expire_skills.len()
        );
        for name in &report.expire_skills {
            println!("  {name}");
        }
        println!("  → recoverable with `komo skills restore <name>`");
    }

    let observing = report.observing_count();
    if observing > 0 {
        println!("\nobserving: {observing} candidate(s) still within the evidence or aging window");
    }
    let awaiting = report.skills_awaiting_count();
    if awaiting > 0 {
        println!("awaiting triage: {awaiting} skill proposal(s) still inside the window");
    }

    if !apply {
        println!("\n(dry run — pass --apply to execute this cycle)");
        return Ok(());
    }

    let OperatorCommandResult::DreamApplied {
        promoted,
        archived,
        skills_expired,
    } = control.command(OperatorCommand::DreamApply).await?
    else {
        unreachable!("DreamApply answers with DreamApplied");
    };
    println!(
        "\nApplied: promoted {promoted}, archived {archived}, withdrew {skills_expired} skill proposal(s)."
    );
    Ok(())
}

fn no_action_summary(report: &crate::services::operator_control::DreamReport) -> String {
    if report.candidate_count == 0 && report.skill_candidate_count == 0 {
        "Nothing to dream about — no candidate memories and no skill proposals.".into()
    } else {
        format!(
            "No state changes this cycle — {} memory candidate(s) and {} skill proposal(s) are still being observed.",
            report.candidate_count, report.skill_candidate_count
        )
    }
}

fn report_bucket(label: &str, items: &[DreamItem]) {
    if items.is_empty() {
        return;
    }
    println!("\n{label}: {}", items.len());
    for m in items.iter().take(20) {
        // Support and contradictions first: they are what the verdict reads.
        // Recalls are shown because they explain *archival*, not promotion.
        let belief = if m.belief.is_empty() || m.belief == "current" {
            String::new()
        } else {
            format!(" {}", m.belief)
        };
        println!(
            "  {}  [support={} against={}{} recalls={} score={:.2}]  {}",
            m.id,
            m.support_count,
            m.contradiction_count,
            belief,
            m.recall_count,
            m.score,
            m.content
        );
    }
    if items.len() > 20 {
        println!("  … and {} more", items.len() - 20);
    }
}

#[cfg(test)]
mod tests {
    use crate::services::operator_control::DreamReport;

    use super::no_action_summary;

    #[test]
    fn no_action_summary_distinguishes_no_candidates_from_observation() {
        assert_eq!(
            no_action_summary(&DreamReport::default()),
            "Nothing to dream about — no candidate memories and no skill proposals."
        );
        assert_eq!(
            no_action_summary(&DreamReport {
                candidate_count: 3,
                ..Default::default()
            }),
            "No state changes this cycle — 3 memory candidate(s) and 0 skill proposal(s) are still being observed."
        );
        // Skill proposals alone are enough to have something under observation:
        // a library with no memory candidates is not an idle cycle.
        assert_eq!(
            no_action_summary(&DreamReport {
                skill_candidate_count: 2,
                ..Default::default()
            }),
            "No state changes this cycle — 0 memory candidate(s) and 2 skill proposal(s) are still being observed."
        );
    }
}
