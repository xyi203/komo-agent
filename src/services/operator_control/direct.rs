//! Direct persistence adapter: operator actions against a directly-opened
//! store, used when no gateway is running (nothing holds the Turso lock).
//!
//! The database opens **lazily, once per command**: a `komo doctor` that only
//! prints config never opens it at all, and a batch of memory transitions
//! reuses the one connection instead of reconnecting per id. It used to be four
//! files and four cells — one merge later (docs/adr/0004) the laziness is all
//! that is left of that.

use komo_infra::persistence::db::Db;
use komo_services::cron_actions;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::OnceCell;

use crate::domain::{
    cron::CronJobRepository,
    home::HomeRepository,
    memory::MemoryRepository,
    pairing::{ApproveOutcome, PairingRepository},
    reminder::ReminderRepository,
    repository::SessionRepository,
    run::RunRepository,
    task::TaskRepository,
};

use super::actions;
use super::request::{
    OperatorCommand, OperatorCommandResult, OperatorQuery, OperatorQueryResult, PairApproveOutcome,
};
use super::{StoreUrls, now};

pub(super) struct DirectOperatorAdapter {
    urls: StoreUrls,
    db: OnceCell<Arc<Db>>,
    /// Opened on first use, like the db: a CLI command that never touches the
    /// vault should not pay for loading its index.
    wiki: OnceCell<Arc<super::actions::WikiOps>>,
}

impl DirectOperatorAdapter {
    pub(super) fn new(urls: StoreUrls) -> Self {
        Self {
            urls,
            db: OnceCell::new(),
            wiki: OnceCell::new(),
        }
    }

    /// The governed skill store. A path holder, not a connection — nothing to
    /// open, so it is built per use rather than cached like the databases.
    pub(super) fn skills(&self) -> komo_infra::skills::FsSkillStore {
        komo_infra::skills::FsSkillStore::new(self.urls.skills_root.clone())
    }

    /// The database — sessions, runs, tasks, memories, cron jobs — opened on
    /// first use.
    pub(super) async fn db(&self) -> anyhow::Result<&Arc<Db>> {
        self.db
            .get_or_try_init(|| async { Ok(Arc::new(Db::connect(&self.urls.db).await?)) })
            .await
    }

    /// Open the note-vault index in this process.
    ///
    /// Only reachable when no gateway is running — if one is, it holds the
    /// index and the gateway adapter answers instead.
    async fn wiki_ops(&self) -> anyhow::Result<&Arc<super::actions::WikiOps>> {
        self.wiki
            .get_or_try_init(|| async {
                let cfg = self
                    .urls
                    .wiki
                    .as_ref()
                    .context("no [wiki] configured in ~/.komo/config.toml")?;
                let index = komo_wiki::build_index(&komo_wiki::WikiSettings {
                    backend: komo_wiki::WikiBackend::parse(&cfg.backend)?,
                    data_dir: cfg.data_dir.clone(),
                    url: cfg.url.clone(),
                    collection: cfg.collection.clone(),
                    api_key: std::env::var("QDRANT_API_KEY").ok(),
                })
                .await?;
                let embedder = komo_infra::embedding::OllamaEmbedder::new(
                    cfg.embedding.url.clone(),
                    cfg.embedding.model.clone(),
                )?;
                Ok(Arc::new(super::actions::WikiOps {
                    // This process is the only indexer when it opens the store
                    // directly (the gateway is down, or there is none), so its
                    // runner is unshared — the gate still holds within it.
                    runner: Arc::new(komo_services::wiki_indexing::WikiIndexRunner::new(
                        index,
                        Arc::new(embedder),
                        cfg.vault.clone(),
                        cfg.embedding.model.clone(),
                    )),
                    backend: cfg.backend.clone(),
                    collection: cfg.collection.clone(),
                    location: if cfg.backend == "server" {
                        cfg.url.clone()
                    } else {
                        cfg.data_dir.join(&cfg.collection).display().to_string()
                    },
                }))
            })
            .await
    }

    pub(super) async fn query(&self, query: OperatorQuery) -> anyhow::Result<OperatorQueryResult> {
        Ok(match query {
            OperatorQuery::Reminders => {
                let mut pending =
                    ReminderRepository::list_pending(self.db().await?.as_ref()).await?;
                pending.sort_by_key(|r| r.run_at);
                OperatorQueryResult::Reminders(pending)
            }
            OperatorQuery::Tasks => OperatorQueryResult::Tasks(
                TaskRepository::list_open(self.db().await?.as_ref()).await?,
            ),
            OperatorQuery::Runs { limit } => OperatorQueryResult::Runs(
                RunRepository::list(self.db().await?.as_ref(), limit).await?,
            ),
            OperatorQuery::Run { id } => {
                let db = self.db().await?;
                let fetched = match RunRepository::get(db.as_ref(), &id).await? {
                    Some(run) => {
                        let steps = RunRepository::steps(db.as_ref(), &run.id).await?;
                        Some((run, steps))
                    }
                    None => None,
                };
                OperatorQueryResult::Run(fetched)
            }
            OperatorQuery::Sessions => OperatorQueryResult::Sessions(actions::session_summaries(
                SessionRepository::list(self.db().await?.as_ref()).await?,
            )),
            OperatorQuery::MemorySearch { query, limit } => {
                // No gateway, so no embedder: the same scoring runs on terms
                // alone. Still bigram/word matching with recall's ranking —
                // strictly better than the substring scan it replaced — but
                // cross-language hits need the gateway's semantic arm.
                let db = self.db().await?;
                let service = komo_services::memory_query::MemoryQueryService::new(db.clone() as _);
                OperatorQueryResult::MemorySearch(
                    actions::search_memories(&service, db.as_ref(), &query, limit).await?,
                )
            }
            OperatorQuery::MemoryUsed { id, limit } => OperatorQueryResult::MemoryUsed(
                RunRepository::runs_using_memory(self.db().await?.as_ref(), &id, limit).await?,
            ),
            OperatorQuery::Memories => OperatorQueryResult::Memories(
                MemoryRepository::list(self.db().await?.as_ref()).await?,
            ),
            OperatorQuery::Pairings => OperatorQueryResult::Pairings(actions::pairing_views(
                PairingRepository::list(self.db().await?.as_ref()).await?,
                now(),
            )),
            OperatorQuery::DreamPreview => {
                let at = now();
                let memories = MemoryRepository::list(self.db().await?.as_ref()).await?;
                let mut report = actions::dream_classify(&memories, at);
                let (expire_skills, skill_candidate_count) =
                    actions::dream_classify_skills(&self.skills().list_candidates(), at);
                report.expire_skills = expire_skills;
                report.skill_candidate_count = skill_candidate_count;
                OperatorQueryResult::DreamPreview(report)
            }
            OperatorQuery::SkillAudit { name } => {
                let steps = RunRepository::steps_by_tool(
                    self.db().await?.as_ref(),
                    "skill",
                    actions::AUDIT_SCAN_LIMIT,
                )
                .await?;
                let runs =
                    RunRepository::list(self.db().await?.as_ref(), actions::AUDIT_SCAN_LIMIT)
                        .await?;
                OperatorQueryResult::SkillAudit(actions::skill_invocations(
                    steps,
                    &name,
                    actions::AUDIT_RESULT_CAP,
                    &actions::run_verdicts(runs),
                ))
            }
            // The skill store is files, not a locked db, so the direct adapter
            // reads it straight rather than routing — same store the gateway's
            // `OperatorActions` holds.
            OperatorQuery::SkillUsage => {
                let steps = RunRepository::steps_by_tool(
                    self.db().await?.as_ref(),
                    "skill",
                    actions::AUDIT_SCAN_LIMIT,
                )
                .await?;
                let names = self
                    .skills()
                    .list_active()
                    .into_iter()
                    .map(|skill| skill.name);
                let runs =
                    RunRepository::list(self.db().await?.as_ref(), actions::AUDIT_SCAN_LIMIT)
                        .await?;
                OperatorQueryResult::SkillUsage(actions::skill_usage(
                    names,
                    steps,
                    &actions::run_verdicts(runs),
                ))
            }
            OperatorQuery::HomeOverride => OperatorQueryResult::HomeOverride(
                HomeRepository::get(self.db().await?.as_ref()).await?,
            ),
            OperatorQuery::WikiSearch { query, limit } => {
                OperatorQueryResult::WikiHits(self.wiki_ops().await?.search(&query, limit).await?)
            }
            OperatorQuery::WikiStatus => {
                OperatorQueryResult::WikiStatus(self.wiki_ops().await?.status().await?)
            }
            OperatorQuery::CronJobs => OperatorQueryResult::CronJobs(
                CronJobRepository::list(self.db().await?.as_ref()).await?,
            ),
        })
    }

    pub(super) async fn command(
        &self,
        command: OperatorCommand,
    ) -> anyhow::Result<OperatorCommandResult> {
        Ok(match command {
            OperatorCommand::ChunkIndex { rebuild } => {
                OperatorCommandResult::WikiIndexed(self.wiki_ops().await?.index(rebuild).await?)
            }
            OperatorCommand::MemoryTransition { id, action } => {
                match actions::apply_memory_transition(
                    self.db().await?.as_ref(),
                    &id,
                    action,
                    now(),
                )
                .await?
                {
                    actions::TransitionOutcome::Applied(_) => {
                        OperatorCommandResult::MemoryTransitioned
                    }
                    actions::TransitionOutcome::NotFound => {
                        anyhow::bail!("no memory with id `{id}`")
                    }
                }
            }
            OperatorCommand::PruneRuns { cutoff } => OperatorCommandResult::RunsPruned {
                removed: RunRepository::prune(self.db().await?.as_ref(), cutoff).await?,
            },
            OperatorCommand::CleanSessions => OperatorCommandResult::SessionsCleaned {
                removed: SessionRepository::delete_empty_sessions(self.db().await?.as_ref())
                    .await?,
            },
            OperatorCommand::PairApprove { code } => {
                let outcome = match PairingRepository::approve_code(
                    self.db().await?.as_ref(),
                    &code,
                )
                .await?
                {
                    ApproveOutcome::Approved(request) => {
                        PairApproveOutcome::Approved { id: request.id }
                    }
                    ApproveOutcome::NotFound => PairApproveOutcome::NotFound,
                    ApproveOutcome::Locked { retry_after_secs } => {
                        PairApproveOutcome::Locked { retry_after_secs }
                    }
                };
                OperatorCommandResult::PairApproved(outcome)
            }
            OperatorCommand::PairRevoke { id } => OperatorCommandResult::PairRevoked {
                revoked: PairingRepository::revoke(self.db().await?.as_ref(), &id).await?,
            },
            OperatorCommand::DreamApply => {
                let summary = komo_agent::daemon::DreamSweep {
                    memories: self.db().await?.clone() as Arc<dyn MemoryRepository>,
                    skills: Arc::new(self.skills()),
                }
                .apply()
                .await?;
                OperatorCommandResult::DreamApplied {
                    promoted: summary.memories_promoted,
                    archived: summary.memories_archived,
                    skills_expired: summary.skill_candidates_expired,
                }
            }
            // Backfill needs an embedder, which is assembled with the rest of
            // the agent at gateway wiring. Rather than build a second one here
            // (a second place for the embedding config to be read, and to drift),
            // this path reports what to do about it.
            OperatorCommand::MemoryBackfill => anyhow::bail!(
                "memory backfill runs in the gateway (it owns the embedding client) — \
                 start it with `komo gateway start`, then re-run this"
            ),
            OperatorCommand::MemoryRepairScopes => OperatorCommandResult::MemoryScopesRepaired {
                repaired: actions::repair_memory_scopes(self.db().await?.as_ref()).await?,
            },
            OperatorCommand::CronAdd { spec } => OperatorCommandResult::CronAdded(Box::new(
                cron_actions::add_cron_job(self.db().await?.as_ref(), spec, now()).await?,
            )),
            OperatorCommand::CronRemove { name } => {
                if !CronJobRepository::delete(self.db().await?.as_ref(), &name).await? {
                    anyhow::bail!(actions::no_cron_job_message(&name));
                }
                OperatorCommandResult::CronRemoved
            }
            OperatorCommand::CronSetEnabled { name, enabled } => {
                match cron_actions::set_cron_enabled(
                    self.db().await?.as_ref(),
                    &name,
                    enabled,
                    now(),
                )
                .await?
                {
                    Some(job) => OperatorCommandResult::CronUpdated(Box::new(job)),
                    None => anyhow::bail!(actions::no_cron_job_message(&name)),
                }
            }
            OperatorCommand::CronTrigger { name } => {
                match cron_actions::trigger_cron_job(self.db().await?.as_ref(), &name, now())
                    .await?
                {
                    Some(job) => OperatorCommandResult::CronUpdated(Box::new(job)),
                    None => anyhow::bail!(actions::no_cron_job_message(&name)),
                }
            }
        })
    }
}
