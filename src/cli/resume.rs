//! `komo run resume` — pick an interrupted turn back up from the run ledger.
//!
//! Two tiers. When the run left a turn journal, the turn is *continued*: the
//! exact provider-level state it died with is rebuilt and driven forward, so
//! the tool rounds already paid for are replayed, not re-run. Otherwise —
//! pre-journal runs, a failed journal write — it falls back to one *fresh*
//! turn primed with the original input and a digest of the completed steps
//! (`domain::run::resume_prompt`). Either way the model judges which side
//! effects already took hold; new side effects go through approval as usual.
//!
//! Eligibility, priming, and the at-most-once `recoverable` clear live in
//! [`OperatorControl::resume_run`]. Only the local turn itself is supplied
//! here: with no gateway the run executes in-process with interactive approval
//! at the TTY, built on the very stores the operator backend already opened.

use std::sync::Arc;

use crate::{
    cli::{approver::CliApprover, wiring},
    domain::approval::Approver,
    services::operator_control::OperatorControl,
};
use komo_config::ConfigSnapshot;

/// Resume an interrupted run in its original session. `id = None` picks the
/// most recent recoverable run.
pub async fn run(
    config: &ConfigSnapshot,
    control: &OperatorControl,
    id: Option<String>,
) -> anyhow::Result<()> {
    let outcome = control
        .resume_run(id, |db, run, input| async move {
            // Same construction as the chat TUI's local mode: interactive
            // approval at the TTY.
            let approver: Arc<dyn Approver> = Arc::new(CliApprover::new());
            let runtime = wiring::build(config, db, approver).await?.runtime;
            // Journal-continue first; digest-primed fresh turn as the fallback
            // (same order as the gateway's resume endpoint).
            match runtime.resume_interrupted(&run).await? {
                Some(reply) => Ok((reply, true)),
                None => Ok((runtime.handle_input(&run.session_id, input).await?, false)),
            }
        })
        .await?;
    if outcome.continued {
        println!(
            "Resumed {} (session {}, continued from its turn journal).\n",
            outcome.run_id, outcome.session_id
        );
    } else {
        println!(
            "Resumed {} (session {}, {} completed step(s) handed to the model).\n",
            outcome.run_id, outcome.session_id, outcome.steps
        );
    }
    println!("Agent: {}", outcome.reply);
    Ok(())
}
