//! Scheduled jobs: `cron_job_records` in `komo.db`, and the
//! [`CronJobRepository`] over them.
//!
//! Jobs are **durable** — a job silently vanishing means its work silently
//! stops happening — so schema changes here are additive and the table is never
//! dropped to reset. That was a separate file (`cron.db`) until docs/adr/0004
//! made durability a table-level rule; this module keeps the model, the
//! queries, the in-place schema upkeep and the one-time import from the old
//! file, and `Db` owns the connection.

use std::path::Path;

use anyhow::Context;
use async_trait::async_trait;

use super::db::Db;
use crate::persistence::with_write_retry;
use komo_core::domain::cron::{
    CronAction, CronJob, CronJobRepository, parse_catch_up, parse_cron_job_status,
    parse_cron_run_status,
};
use komo_core::domain::policy::RuleSpec;

// Optional i64 fields use 0 as the "unset" sentinel; `args` is a JSON array
// string; `status` is "active"/"paused"/"done"; `last_status` is
// ""/"ok"/"failed" (same conventions as the other stores).
#[derive(Debug, toasty::Model)]
pub(crate) struct CronJobRecord {
    #[key]
    id: String,
    #[index]
    name: String,
    schedule: String,
    /// "command" | "agent" — discriminates the columns below.
    kind: String,
    // Command-mode columns (empty/0 for agent jobs).
    command: String,
    args: String,
    workdir: String,
    timeout_secs: i64,
    // Agent-mode columns (empty for command jobs).
    prompt: String,
    skills: String,
    status: String,
    /// "late" | "skip" — what to do with a missed slot. Additive column.
    catch_up: String,
    next_run_at: i64,
    last_run_at: i64,
    last_status: String,
    last_error: String,
    last_output: String,
    last_run_session: String,
    /// JSON array of `RuleSpec` — the actions this job may take unattended.
    /// Empty string = no grants (every job written before the column existed).
    grants: String,
    created_at: i64,
}

/// Columns added to `cron_job_records` after a `komo.db` was created. Extend
/// this for every new [`CronJobRecord`] column: the table is durable, so it is
/// migrated in place and never dropped to be rebuilt.
const EXPECTED: &[(&str, &str)] = &[
    ("kind", "\"kind\" text NOT NULL DEFAULT 'command'"),
    ("prompt", "\"prompt\" text NOT NULL DEFAULT ''"),
    ("skills", "\"skills\" text NOT NULL DEFAULT ''"),
    ("grants", "\"grants\" text NOT NULL DEFAULT ''"),
    ("status", "\"status\" text NOT NULL DEFAULT 'active'"),
    ("catch_up", "\"catch_up\" text NOT NULL DEFAULT 'late'"),
    ("last_output", "\"last_output\" text NOT NULL DEFAULT ''"),
    (
        "last_run_session",
        "\"last_run_session\" text NOT NULL DEFAULT ''",
    ),
];

/// Bring an existing file's `cron_job_records` up to the current column set,
/// before toasty opens it.
pub(crate) async fn ensure_schema(path: &Path) -> anyhow::Result<()> {
    crate::persistence::ensure_columns(path, "cron_job_records", EXPECTED).await?;
    migrate_enabled_to_status(path).await
}

/// Every job in a legacy `cron.db`, for the one-time merge into `komo.db`.
///
/// The old file gets its own schema upkeep first: a `cron.db` written before
/// `status` existed still has `enabled`, and opening it with the current model
/// would fail on the columns it lacks.
pub(crate) async fn import_from(path: &Path) -> anyhow::Result<Vec<CronJob>> {
    ensure_schema(path).await?;
    let db = toasty::Db::builder()
        .models(toasty::models!(CronJobRecord))
        .connect(&format!("turso:{}", path.display()))
        .await
        .with_context(|| format!("opening {} to merge it in", path.display()))?;
    let mut conn = db.connection().await?;
    let rows = toasty::query!(CronJobRecord).exec(&mut conn).await?;
    rows.into_iter().map(job_from_record).collect()
}

/// One-time migration from the pre-status schema: `enabled` (0/1) becomes the
/// stored `status` ('active'/'paused'), and the old column is dropped so it
/// cannot fork from the new authority (and so inserts, which no longer supply
/// it, don't trip its NOT NULL). Idempotent: a db without `enabled` is a no-op.
/// Runs on a direct turso handle before toasty's pool connects, like
/// `ensure_columns`.
async fn migrate_enabled_to_status(path: &std::path::Path) -> anyhow::Result<()> {
    use anyhow::Context;

    let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
        .build()
        .await
        .with_context(|| format!("opening {} for status migration", path.display()))?;
    let conn = db.connect()?;
    conn.pragma_update("journal_mode", "'mvcc'").await.ok();

    let mut has_enabled = false;
    let mut rows = conn
        .query("PRAGMA table_info(\"cron_job_records\")", ())
        .await
        .context("reading cron_job_records columns")?;
    while let Some(row) = rows.next().await? {
        if let turso::Value::Text(name) = row.get_value(1)?
            && name == "enabled"
        {
            has_enabled = true;
        }
    }
    if !has_enabled {
        return Ok(());
    }
    conn.execute(
        "UPDATE \"cron_job_records\" SET \"status\" = \
         CASE WHEN \"enabled\" = 0 THEN 'paused' ELSE 'active' END",
        (),
    )
    .await
    .context("backfilling status from enabled")?;
    conn.execute(
        "ALTER TABLE \"cron_job_records\" DROP COLUMN \"enabled\"",
        (),
    )
    .await
    .context("dropping the legacy enabled column")?;
    tracing::info!("migrated cron.db: enabled column replaced by status");
    Ok(())
}

#[async_trait]
impl CronJobRepository for Db {
    async fn save(&self, job: &CronJob) -> anyhow::Result<()> {
        let cols = ActionColumns::from_action(&job.action)?;
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            toasty::create!(CronJobRecord {
                id: job.id.clone(),
                name: job.name.clone(),
                schedule: job.schedule.clone(),
                kind: job.action.kind().to_string(),
                command: cols.command.clone(),
                args: cols.args.clone(),
                workdir: cols.workdir.clone(),
                timeout_secs: cols.timeout_secs,
                prompt: cols.prompt.clone(),
                skills: cols.skills.clone(),
                status: job.status.as_str().to_string(),
                catch_up: job.catch_up.as_str().to_string(),
                next_run_at: job.next_run_at,
                last_run_at: job.last_run_at.unwrap_or(0),
                last_status: job
                    .last_status
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
                last_error: job.last_error.clone(),
                last_output: job.last_output.clone(),
                last_run_session: job.last_run_session.clone().unwrap_or_default(),
                created_at: job.created_at,
                grants: encode_grants(&job.grants)?,
            })
            .exec(&mut conn)
            .await?;
            Ok(())
        })
        .await
    }

    async fn list(&self) -> anyhow::Result<Vec<CronJob>> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(CronJobRecord).exec(&mut conn).await?;
        let mut jobs = rows
            .into_iter()
            .map(job_from_record)
            .collect::<anyhow::Result<Vec<_>>>()?;
        jobs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(jobs)
    }

    async fn find_by_name(&self, name: &str) -> anyhow::Result<Option<CronJob>> {
        let mut conn = self.inner.connection().await?;
        let rows = toasty::query!(CronJobRecord).exec(&mut conn).await?;
        for record in rows {
            if record.name == name {
                return Ok(Some(job_from_record(record)?));
            }
        }
        Ok(None)
    }

    async fn update(&self, job: &CronJob) -> anyhow::Result<()> {
        let cols = ActionColumns::from_action(&job.action)?;
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let mut record = CronJobRecord::get_by_id(&mut conn, &job.id).await?;
            record
                .update()
                .name(job.name.clone())
                .schedule(job.schedule.clone())
                .kind(job.action.kind().to_string())
                .command(cols.command.clone())
                .args(cols.args.clone())
                .workdir(cols.workdir.clone())
                .timeout_secs(cols.timeout_secs)
                .prompt(cols.prompt.clone())
                .skills(cols.skills.clone())
                .status(job.status.as_str().to_string())
                .next_run_at(job.next_run_at)
                .last_run_at(job.last_run_at.unwrap_or(0))
                .last_status(
                    job.last_status
                        .as_ref()
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default(),
                )
                .last_error(job.last_error.clone())
                .last_output(job.last_output.clone())
                .last_run_session(job.last_run_session.clone().unwrap_or_default())
                .grants(encode_grants(&job.grants)?)
                .exec(&mut conn)
                .await?;
            Ok(())
        })
        .await
    }

    async fn delete(&self, name: &str) -> anyhow::Result<bool> {
        let Some(job) = self.find_by_name(name).await? else {
            return Ok(false);
        };
        with_write_retry(|| async {
            let mut conn = self.inner.connection().await?;
            let record = CronJobRecord::get_by_id(&mut conn, &job.id).await?;
            record.delete().exec(&mut conn).await?;
            Ok(())
        })
        .await?;
        Ok(true)
    }
}

/// The action fields flattened into record columns; the unused side stays
/// empty/zero. Keeps the enum → columns mapping in one place for save/update.
struct ActionColumns {
    command: String,
    args: String,
    workdir: String,
    timeout_secs: i64,
    prompt: String,
    skills: String,
}

impl ActionColumns {
    fn from_action(action: &CronAction) -> anyhow::Result<Self> {
        Ok(match action {
            CronAction::Command {
                command,
                args,
                workdir,
                timeout_secs,
            } => Self {
                command: command.clone(),
                args: serde_json::to_string(args)?,
                workdir: workdir.clone().unwrap_or_default(),
                timeout_secs: *timeout_secs as i64,
                prompt: String::new(),
                skills: String::new(),
            },
            // An agent job's workspace rides in the `workdir` column: the two
            // are the same question ("where does this job work?"), asked of a
            // process and of a turn, and cron.db is durable — a second column
            // for the same answer is a schema change nobody needs.
            CronAction::Agent {
                prompt,
                skills,
                workspace,
            } => Self {
                command: String::new(),
                args: String::new(),
                workdir: workspace.clone().unwrap_or_default(),
                timeout_secs: 0,
                prompt: prompt.clone(),
                skills: serde_json::to_string(skills)?,
            },
        })
    }
}

/// Grants as the column stores them. An empty list is written as `''` rather
/// than `'[]'` so a job without grants is byte-identical to a pre-column row.
fn encode_grants(grants: &[RuleSpec]) -> anyhow::Result<String> {
    if grants.is_empty() {
        return Ok(String::new());
    }
    Ok(serde_json::to_string(grants)?)
}

fn job_from_record(record: CronJobRecord) -> anyhow::Result<CronJob> {
    let nonzero = |v: i64| (v != 0).then_some(v);
    // Default to command for legacy rows written before `kind` existed.
    let action = if record.kind == "agent" {
        CronAction::Agent {
            prompt: record.prompt,
            skills: serde_json::from_str(&record.skills).unwrap_or_default(),
            workspace: (!record.workdir.is_empty()).then_some(record.workdir),
        }
    } else {
        CronAction::Command {
            command: record.command,
            args: serde_json::from_str(&record.args).unwrap_or_default(),
            workdir: (!record.workdir.is_empty()).then_some(record.workdir),
            timeout_secs: record.timeout_secs.max(0) as u64,
        }
    };
    Ok(CronJob {
        id: record.id,
        name: record.name,
        schedule: record.schedule,
        action,
        status: parse_cron_job_status(&record.status),
        catch_up: parse_catch_up(&record.catch_up),
        next_run_at: record.next_run_at,
        last_run_at: nonzero(record.last_run_at),
        last_status: parse_cron_run_status(&record.last_status),
        last_error: record.last_error,
        last_output: record.last_output,
        last_run_session: (!record.last_run_session.is_empty()).then_some(record.last_run_session),
        created_at: record.created_at,
        // A row written before the column existed reads as empty, which is the
        // same thing as "no grants" — never an error.
        grants: serde_json::from_str(&record.grants).unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::cron::{CronJobStatus, CronRunStatus};

    /// A `komo.db` in a home of this test's own — `Db::connect` scans its
    /// directory for legacy files to merge.
    fn turso_url(name: &str) -> String {
        let home = std::env::temp_dir().join(format!("komo-cron-{name}"));
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        format!("turso:{}", home.join("komo.db").display())
    }

    #[tokio::test]
    async fn job_roundtrip_update_and_delete() {
        let db = Db::connect(&turso_url("komo_cron_repo_test.db"))
            .await
            .unwrap();
        let job = CronJob::new(
            "weekly",
            "0 14 * * 5",
            CronAction::Command {
                command: "/opt/rotate.py".into(),
                args: vec!["--push".into(), "第二个".into()],
                workdir: Some("/opt".into()),
                timeout_secs: 600,
            },
            1234,
        );

        db.save(&job).await.unwrap();
        let listed = db.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "weekly");
        let CronAction::Command {
            command,
            args,
            workdir,
            timeout_secs,
        } = &listed[0].action
        else {
            panic!("command job");
        };
        assert_eq!(command, "/opt/rotate.py");
        assert_eq!(args, &vec!["--push".to_string(), "第二个".to_string()]);
        assert_eq!(workdir.as_deref(), Some("/opt"));
        assert_eq!(*timeout_secs, 600);
        assert_eq!(listed[0].next_run_at, 1234);
        assert_eq!(listed[0].status, CronJobStatus::Active);
        assert!(listed[0].last_status.is_none());
        assert_eq!(listed[0].last_output, "");
        assert_eq!(listed[0].last_run_session, None);

        let mut updated = listed[0].clone();
        updated.status = CronJobStatus::Paused;
        updated.next_run_at = 9999;
        updated.last_run_at = Some(5000);
        updated.last_status = Some(CronRunStatus::Failed);
        updated.last_error = "exit status: 3".into();
        updated.last_output = "boom\n".into();
        updated.last_run_session = Some("cron:weekly:5000".into());
        db.update(&updated).await.unwrap();

        let found = db.find_by_name("weekly").await.unwrap().unwrap();
        assert_eq!(found.status, CronJobStatus::Paused);
        assert_eq!(found.next_run_at, 9999);
        assert_eq!(found.last_run_at, Some(5000));
        assert_eq!(found.last_status, Some(CronRunStatus::Failed));
        assert_eq!(found.last_error, "exit status: 3");
        assert_eq!(found.last_output, "boom\n");
        assert_eq!(found.last_run_session.as_deref(), Some("cron:weekly:5000"));

        assert!(db.delete("weekly").await.unwrap());
        assert!(
            !db.delete("weekly").await.unwrap(),
            "second delete is a no-op"
        );
        assert!(db.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn upgrades_command_only_schema_in_place() {
        let home = std::env::temp_dir().join("komo-cron-addcol");
        std::fs::remove_dir_all(&home).ok();
        std::fs::create_dir_all(&home).expect("test home");
        let path = home.join("cron.db");

        // 1. Seed a turso file with the OLD command-only schema (no
        //    kind/prompt/skills) + one command row, then drop the handle.
        {
            let db = turso::Builder::new_local(path.to_string_lossy().as_ref())
                .build()
                .await
                .unwrap();
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'").await.ok();
            conn.execute(
                "CREATE TABLE \"cron_job_records\" (\
                 \"id\" TEXT NOT NULL, \"name\" TEXT NOT NULL, \"schedule\" TEXT NOT NULL, \
                 \"command\" TEXT NOT NULL, \"args\" TEXT NOT NULL, \"workdir\" TEXT NOT NULL, \
                 \"timeout_secs\" BIGINT NOT NULL, \"enabled\" BIGINT NOT NULL, \
                 \"next_run_at\" BIGINT NOT NULL, \"last_run_at\" BIGINT NOT NULL, \
                 \"last_status\" TEXT NOT NULL, \"last_error\" TEXT NOT NULL, \
                 \"created_at\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO \"cron_job_records\" VALUES \
                 ('id-1', 'legacy', '0 14 * * 5', '/opt/rotate.py', '[\"--push\"]', '', \
                 900, 1, 1000, 0, '', '', 100)",
                (),
            )
            .await
            .unwrap();
            // A disabled legacy row must migrate to `paused`, not `active`.
            conn.execute(
                "INSERT INTO \"cron_job_records\" VALUES \
                 ('id-2', 'parked', '0 3 * * *', '/opt/nightly.sh', '[]', '', \
                 900, 0, 2000, 0, '', '', 100)",
                (),
            )
            .await
            .unwrap();
        }
        std::fs::write(
            crate::persistence::turso_marker_path(&path),
            b"turso-native\n",
        )
        .unwrap();

        // 2. Merge it into a fresh `komo.db`: the old file gains
        //    kind/prompt/skills in place and `enabled` becomes the stored
        //    status *before* it is read, which is the only way a pre-status
        //    file is readable through today's model at all.
        let db = Db::connect(&format!("turso:{}", home.join("komo.db").display()))
            .await
            .unwrap();
        let found = db.find_by_name("legacy").await.unwrap().unwrap();
        let CronAction::Command { command, args, .. } = &found.action else {
            panic!("legacy row must read as a command job");
        };
        assert_eq!(command, "/opt/rotate.py");
        assert_eq!(args, &vec!["--push".to_string()]);
        assert_eq!(found.status, CronJobStatus::Active, "enabled=1 → active");
        assert!(
            found.grants.is_empty(),
            "a row written before the grants column must read as ungranted, not error"
        );
        let parked = db.find_by_name("parked").await.unwrap().unwrap();
        assert_eq!(parked.status, CronJobStatus::Paused, "enabled=0 → paused");

        // 3. The added columns are usable: an agent job saves and reads back.
        db.save(&CronJob::new(
            "brief",
            "0 8 * * *",
            CronAction::Agent {
                prompt: "hi".into(),
                skills: vec!["s".into()],
                workspace: None,
            },
            0,
        ))
        .await
        .unwrap();
        let agent = db.find_by_name("brief").await.unwrap().unwrap();
        assert_eq!(agent.action.kind(), "agent");
    }

    #[tokio::test]
    async fn agent_job_roundtrips() {
        let db = Db::connect(&turso_url("komo_cron_agent_test.db"))
            .await
            .unwrap();
        let job = CronJob::new(
            "brief",
            "0 8 * * *",
            CronAction::Agent {
                prompt: "总结我今天的日程".into(),
                skills: vec!["calendar".into(), "weather".into()],
                workspace: None,
            },
            42,
        );
        db.save(&job).await.unwrap();
        let found = db.find_by_name("brief").await.unwrap().unwrap();
        let CronAction::Agent { prompt, skills, .. } = &found.action else {
            panic!("agent job");
        };
        assert_eq!(prompt, "总结我今天的日程");
        assert_eq!(skills, &vec!["calendar".to_string(), "weather".to_string()]);
        assert_eq!(found.next_run_at, 42);
    }

    /// An agent job's workspace and a command job's workdir share one column,
    /// discriminated by `kind`. Both are read back here, in one store, so a
    /// mapping that crossed the two would surface as a job confined to another
    /// job's directory rather than as a compile error.
    #[tokio::test]
    async fn an_agent_workspace_and_a_command_workdir_share_a_column_without_crossing() {
        let db = Db::connect(&turso_url("komo_cron_workspace_test.db"))
            .await
            .unwrap();
        db.save(&CronJob::new(
            "tidy",
            "0 8 * * *",
            CronAction::Agent {
                prompt: "tidy".into(),
                skills: vec![],
                workspace: Some("/srv/notes".into()),
            },
            42,
        ))
        .await
        .unwrap();
        db.save(&CronJob::new(
            "backup",
            "0 9 * * *",
            CronAction::Command {
                command: "/bin/true".into(),
                args: vec![],
                workdir: Some("/srv/backups".into()),
                timeout_secs: 60,
            },
            42,
        ))
        .await
        .unwrap();

        let agent = db.find_by_name("tidy").await.unwrap().unwrap();
        let CronAction::Agent { workspace, .. } = &agent.action else {
            panic!("agent job");
        };
        assert_eq!(workspace.as_deref(), Some("/srv/notes"));

        let command = db.find_by_name("backup").await.unwrap().unwrap();
        let CronAction::Command { workdir, .. } = &command.action else {
            panic!("command job");
        };
        assert_eq!(workdir.as_deref(), Some("/srv/backups"));
    }

    /// Grants survive save → read → update → read, field for field. An
    /// approval the operator gave once must not quietly widen or narrow because
    /// the job's `last_run_at` was stamped.
    #[tokio::test]
    async fn job_grants_roundtrip_through_save_and_update() {
        let db = Db::connect(&turso_url("komo_cron_grants_test.db"))
            .await
            .unwrap();
        let grant = RuleSpec {
            category: "homeassistant".into(),
            matcher: "exact".into(),
            value: "climate.set_temperature".into(),
            access: None,
            channels: None,
            effect: "allow".into(),
            include_dangerous: false,
            unattended: true,
        };
        let job = CronJob::new(
            "ac-temp",
            "0 22 * * *",
            CronAction::Agent {
                prompt: "设到 26 度".into(),
                skills: vec![],
                workspace: None,
            },
            0,
        )
        .with_grants(vec![grant]);
        db.save(&job).await.unwrap();

        let found = db.find_by_name("ac-temp").await.unwrap().unwrap();
        assert_eq!(found.grants.len(), 1);
        assert_eq!(found.grants[0].value, "climate.set_temperature");
        assert!(found.grants[0].unattended);
        // And it parses into the rule the policy engine will match on.
        let rules = found.granted_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].value, "climate.set_temperature");

        let mut updated = found;
        updated.last_run_at = Some(999);
        db.update(&updated).await.unwrap();
        let again = db.find_by_name("ac-temp").await.unwrap().unwrap();
        assert_eq!(again.grants.len(), 1, "update must not drop grants");
        assert_eq!(again.last_run_at, Some(999));
    }

    /// A job without grants writes the empty column, so it is indistinguishable
    /// from a pre-column row — no `'[]'` noise for an operator reading the db.
    #[tokio::test]
    async fn a_job_without_grants_stores_nothing() {
        assert_eq!(encode_grants(&[]).unwrap(), "");
    }

    /// **Revocation.** Deleting a job takes its permissions with it. This is
    /// the whole advantage over a global `unattended = true` config rule, which
    /// outlives whatever it was written for, so it gets its own test rather
    /// than being left as a property of `delete` nobody checks.
    #[tokio::test]
    async fn removing_a_job_revokes_its_grants() {
        let db = Db::connect(&turso_url("komo_cron_revoke_test.db"))
            .await
            .unwrap();
        let job = CronJob::new(
            "ac-temp",
            "0 22 * * *",
            CronAction::Agent {
                prompt: "设到 26 度".into(),
                skills: vec![],
                workspace: None,
            },
            0,
        )
        .with_grants(vec![RuleSpec {
            category: "homeassistant".into(),
            matcher: "exact".into(),
            value: "climate.set_temperature".into(),
            access: None,
            channels: None,
            effect: "allow".into(),
            include_dangerous: false,
            unattended: true,
        }]);
        db.save(&job).await.unwrap();
        assert!(db.delete("ac-temp").await.unwrap());

        // Nothing anywhere in the store still grants it — no orphaned rule.
        let remaining: Vec<_> = db
            .list()
            .await
            .unwrap()
            .iter()
            .flat_map(|j| j.granted_rules())
            .collect();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn find_by_name_returns_none_for_unknown() {
        let db = Db::connect(&turso_url("komo_cron_find_test.db"))
            .await
            .unwrap();
        assert!(db.find_by_name("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_orders_by_name() {
        let db = Db::connect(&turso_url("komo_cron_order_test.db"))
            .await
            .unwrap();
        for name in ["zeta", "alpha", "mid"] {
            db.save(&CronJob::new_command(name, "* * * * *", "/bin/true", 0))
                .await
                .unwrap();
        }
        let names: Vec<String> = db
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|j| j.name)
            .collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    }
}
