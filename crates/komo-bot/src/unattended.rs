//! The inner approvers of the runtimes nobody is watching (cron, briefing) —
//! the rung [`PolicyApprover`](crate::policy_approver::PolicyApprover) escalates
//! to when a `Risk::Normal` action matched no `unattended` rule and no grant of
//! the running job's own.
//!
//! Two of them, because "there is no human here" has two honest answers:
//!
//! - A **routine** is a turn the operator scheduled and will hear back from, so
//!   an ungranted action stops the turn and asks them —
//!   [`UnattendedSuspend`], docs/bot-runtime.md §5.4. The answer may arrive
//!   hours later, in another conversation, after a restart; that is what the
//!   suspension machinery is for.
//! - A **briefing** is a read-only aggregation on a daily slot that degrades to
//!   a tool-less compose the moment its turn fails. Suspending one would park a
//!   wait whose eventual continuation nobody is listening for — the briefing
//!   has already gone out — so it keeps denying: [`UnattendedDeny`].
//!
//! Neither ever lets a [`Risk::Dangerous`] action through. Unattended is where
//! an irreversible action has the least oversight, and a `/approve` typed into
//! the home chat hours later carries none of the context that would make one
//! safe to allow.

use async_trait::async_trait;
use tracing::warn;

use komo_core::domain::approval::{ApprovalRequest, Approver, Decision, Risk};

/// The routine runtime's inner approver: an ungranted action **stops** the turn
/// rather than failing it.
///
/// The gate turns the [`Suspend`](Decision::Suspend) into `turn/suspended` plus
/// a standing wakeup carrying this job's grants, and the sweep that started the
/// turn tells the operator which wait to answer — the wait's id only exists
/// once the registration is written, which is after this has answered.
pub struct UnattendedSuspend;

#[async_trait]
impl Approver for UnattendedSuspend {
    async fn decide(&self, request: &ApprovalRequest) -> Decision {
        if request.risk == Risk::Dangerous {
            warn!(summary = %request.summary,
                "routine: denied (unattended turns never take a dangerous action)");
            return Decision::deny_because(
                "这是无人值守的定时任务，危险操作永远不会被放行——即使操作者事后批准。\
                 请改用不需要审批的做法，或把这一步留给有人在场的会话。",
            );
        }
        Decision::Suspend
    }
}

/// The briefing runtime's inner approver: anything the policy did not grant is
/// refused, with the reason the model can act on.
pub struct UnattendedDeny;

#[async_trait]
impl Approver for UnattendedDeny {
    async fn decide(&self, request: &ApprovalRequest) -> Decision {
        warn!(summary = %request.summary,
            "briefing: denied (unattended; add an `unattended = true` policy rule to grant)");
        Decision::deny_because(
            "这是无人值守的后台任务，没有人能批准这一步。只有配置了 \
             `unattended = true` 的 [policy] 允许规则才会放行；请改用不需要审批的做法。",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_routine_waits_for_an_ordinary_action_instead_of_refusing_it() {
        let decision = UnattendedSuspend
            .decide(&ApprovalRequest::normal("run shell command: git push"))
            .await;
        assert_eq!(decision, Decision::Suspend);
    }

    /// The one thing an unattended turn may never do, however long the operator
    /// takes to answer: a `/approve` hours later has none of the context that
    /// would make an irreversible action safe.
    #[tokio::test]
    async fn a_routine_never_waits_for_a_dangerous_one() {
        let decision = UnattendedSuspend
            .decide(&ApprovalRequest::dangerous(
                "rm -rf /",
                "deletes everything",
            ))
            .await;
        assert!(!decision.is_suspended(), "{decision:?}");
        assert!(!decision.is_allowed());
        assert!(decision.feedback().is_some(), "the model is told why");
    }

    #[tokio::test]
    async fn a_briefing_refuses_rather_than_waiting() {
        for request in [
            ApprovalRequest::normal("install a skill"),
            ApprovalRequest::dangerous("rm -rf /", "deletes everything"),
        ] {
            let decision = UnattendedDeny.decide(&request).await;
            assert!(!decision.is_suspended(), "{decision:?}");
            assert!(!decision.is_allowed());
        }
    }
}
