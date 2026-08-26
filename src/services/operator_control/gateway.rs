//! Gateway adapter: operator actions routed to a running gateway over its
//! loopback api channel (it holds the exclusive Turso lock, so the CLI can't
//! open the dbs itself). Transport, auth, and HTTP-status↔error mapping live
//! in [`GatewayClient`]; this adapter only maps the typed operator requests
//! onto the existing `/api/*` routes.

use crate::infra::gateway_client::{GatewayClient, PairApprove};

use super::request::{
    OperatorCommand, OperatorCommandResult, OperatorQuery, OperatorQueryResult, PairApproveOutcome,
};

pub(super) struct GatewayOperatorAdapter {
    client: GatewayClient,
}

impl GatewayOperatorAdapter {
    pub(super) fn new(client: GatewayClient) -> Self {
        Self { client }
    }

    pub(super) fn client(&self) -> &GatewayClient {
        &self.client
    }

    pub(super) async fn query(&self, query: OperatorQuery) -> anyhow::Result<OperatorQueryResult> {
        Ok(match query {
            OperatorQuery::Reminders => {
                OperatorQueryResult::Reminders(self.client.reminders().await?)
            }
            OperatorQuery::Tasks => OperatorQueryResult::Tasks(self.client.tasks().await?),
            OperatorQuery::Runs { limit } => {
                OperatorQueryResult::Runs(self.client.runs(limit).await?)
            }
            OperatorQuery::Run { id } => OperatorQueryResult::Run(self.client.run(&id).await?),
            OperatorQuery::Sessions => OperatorQueryResult::Sessions(self.client.sessions().await?),
            OperatorQuery::MemorySearch { query, limit } => {
                OperatorQueryResult::MemorySearch(self.client.memory_search(&query, limit).await?)
            }
            OperatorQuery::MemoryUsed { id, limit } => {
                OperatorQueryResult::MemoryUsed(self.client.memory_used(&id, limit).await?)
            }
            OperatorQuery::Memories => OperatorQueryResult::Memories(self.client.memories().await?),
            OperatorQuery::Pairings => OperatorQueryResult::Pairings(self.client.pairings().await?),
            OperatorQuery::DreamPreview => {
                OperatorQueryResult::DreamPreview(self.client.dream_preview().await?)
            }
            OperatorQuery::SkillAudit { name } => {
                OperatorQueryResult::SkillAudit(self.client.skill_audit(&name).await?)
            }
            OperatorQuery::SkillUsage => {
                OperatorQueryResult::SkillUsage(self.client.skill_usage().await?)
            }
            OperatorQuery::HomeOverride => {
                OperatorQueryResult::HomeOverride(self.client.home_override().await?)
            }
            OperatorQuery::CronJobs => {
                OperatorQueryResult::CronJobs(self.client.cron_jobs().await?)
            }
            OperatorQuery::WikiSearch { query, limit } => {
                OperatorQueryResult::WikiHits(self.client.wiki_search(&query, limit).await?)
            }
            OperatorQuery::WikiStatus => {
                OperatorQueryResult::WikiStatus(self.client.wiki_status().await?)
            }
        })
    }

    pub(super) async fn command(
        &self,
        command: OperatorCommand,
    ) -> anyhow::Result<OperatorCommandResult> {
        Ok(match command {
            OperatorCommand::ChunkIndex { rebuild } => {
                OperatorCommandResult::WikiIndexed(self.client.wiki_index(rebuild).await?)
            }
            OperatorCommand::MemoryTransition { id, action } => {
                self.client.memory_transition(&id, action.route()).await?;
                OperatorCommandResult::MemoryTransitioned
            }
            OperatorCommand::PruneRuns { cutoff } => OperatorCommandResult::RunsPruned {
                removed: self.client.prune_runs(cutoff).await?,
            },
            OperatorCommand::CleanSessions => OperatorCommandResult::SessionsCleaned {
                removed: self.client.clean_sessions().await?,
            },
            OperatorCommand::PairApprove { code } => {
                let outcome = match self.client.pair_approve(&code).await? {
                    PairApprove::Approved(id) => PairApproveOutcome::Approved { id },
                    PairApprove::NotFound => PairApproveOutcome::NotFound,
                    PairApprove::Locked(retry_after_secs) => {
                        PairApproveOutcome::Locked { retry_after_secs }
                    }
                };
                OperatorCommandResult::PairApproved(outcome)
            }
            OperatorCommand::PairRevoke { id } => OperatorCommandResult::PairRevoked {
                revoked: self.client.pair_revoke(&id).await?,
            },
            OperatorCommand::DreamApply => {
                let (promoted, archived, skills_expired) = self.client.dream_apply().await?;
                OperatorCommandResult::DreamApplied {
                    promoted,
                    archived,
                    skills_expired,
                }
            }
            OperatorCommand::MemoryBackfill => OperatorCommandResult::MemoryBackfilled {
                embedded: self.client.memory_backfill().await?,
            },
            OperatorCommand::MemoryRepairScopes => OperatorCommandResult::MemoryScopesRepaired {
                repaired: self.client.memory_repair_scopes().await?,
            },
            OperatorCommand::CronAdd { spec } => {
                OperatorCommandResult::CronAdded(Box::new(self.client.cron_add(&spec).await?))
            }
            OperatorCommand::CronRemove { name } => {
                self.client.cron_remove(&name).await?;
                OperatorCommandResult::CronRemoved
            }
            OperatorCommand::CronSetEnabled { name, enabled } => {
                OperatorCommandResult::CronUpdated(Box::new(
                    self.client.cron_set_enabled(&name, enabled).await?,
                ))
            }
            OperatorCommand::CronTrigger { name } => {
                OperatorCommandResult::CronUpdated(Box::new(self.client.cron_trigger(&name).await?))
            }
        })
    }
}
