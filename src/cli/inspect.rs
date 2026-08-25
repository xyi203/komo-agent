//! Operator subcommands (`komo cron list`, `komo session list/clean`).
//!
//! These query the database directly and print to stdout — no LLM, no agent
//! runtime. They are the operator's view into what the gateway will act on.

use komo_core::domain::checkpoint::CheckpointStore;

use crate::{
    domain::cron::{CronAction, CronJob, CronJobSpec, CronJobStatus},
    domain::task::TaskStatus,
    services::operator_control::{
        OperatorCommand, OperatorCommandResult, OperatorControl, OperatorQuery, OperatorQueryResult,
    },
};

pub(crate) fn local_time(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| dt.with_timezone(&chrono::Local).to_rfc3339())
        .unwrap_or_else(|| unix.to_string())
}

/// List scheduled jobs (cron.db) and pending reminders — the two things the
/// gateway fires on a clock.
pub async fn cron_list(control: &OperatorControl) -> anyhow::Result<()> {
    let OperatorQueryResult::CronJobs(jobs) = control.query(OperatorQuery::CronJobs).await? else {
        unreachable!("CronJobs query answers with CronJobs");
    };
    let OperatorQueryResult::Reminders(mut pending) =
        control.query(OperatorQuery::Reminders).await?
    else {
        unreachable!("Reminders query answers with Reminders");
    };
    pending.sort_by_key(|r| r.run_at);

    if jobs.is_empty() {
        println!("No scheduled jobs. (`komo cron add <name> <schedule> <command>`)");
    } else {
        println!("jobs:");
        for job in &jobs {
            print_cron_job(job);
        }
    }

    if pending.is_empty() {
        println!("\nNo pending reminders.");
        return Ok(());
    }
    println!("\nreminders:");
    for r in pending {
        if r.is_recurring() {
            println!(
                "  {}  [{}]  next {}  {}",
                r.id,
                r.schedule,
                local_time(r.run_at),
                r.message
            );
        } else {
            println!(
                "  {}  [one-shot]  due {}  {}",
                r.id,
                local_time(r.run_at),
                r.message
            );
        }
    }
    Ok(())
}

/// One job line (+ a detail line for its last run, when it has one).
fn print_cron_job(job: &CronJob) {
    let state = match job.status {
        CronJobStatus::Active => format!("next {}", local_time(job.next_run_at)),
        CronJobStatus::Paused => "paused".to_string(),
        CronJobStatus::Done => "done".to_string(),
    };
    let target = match &job.action {
        CronAction::Command { command, args, .. } => std::iter::once(command.as_str())
            .chain(args.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" "),
        CronAction::Agent {
            prompt,
            skills,
            workspace,
        } => {
            let skills = if skills.is_empty() {
                String::new()
            } else {
                format!(" [skills: {}]", skills.join(", "))
            };
            let workspace = match workspace {
                Some(dir) => format!(" [in {dir}]"),
                None => String::new(),
            };
            format!("agent: {}{skills}{workspace}", oneline(prompt, 80))
        }
    };
    println!(
        "  {}  ({})  [{}]  {}  → {}",
        job.name,
        job.action.kind(),
        job.schedule,
        state,
        target
    );
    // Spelled out, not counted: "2 grants" would make the operator run another
    // command to learn what they approved, and this listing is where they look.
    for rule in job.granted_rules() {
        println!("      grant {}", rule.describe());
    }
    if let (Some(at), Some(status)) = (job.last_run_at, &job.last_status) {
        let mut line = format!("      last run {} {}", local_time(at), status.as_str());
        if !job.last_output.is_empty() {
            let first = job.last_output.lines().next().unwrap_or_default();
            line.push_str(&format!(" — {first}"));
        }
        println!("{line}");
        if let Some(session) = &job.last_run_session {
            println!("      transcript: komo run list (session {session})");
        }
    }
    if !job.last_error.is_empty() {
        println!("      schedule error: {}", job.last_error);
    }
}

/// Create a scheduled job (validated by the shared operator action).
pub async fn cron_add(control: &OperatorControl, spec: CronJobSpec) -> anyhow::Result<()> {
    let OperatorCommandResult::CronAdded(job) =
        control.command(OperatorCommand::CronAdd { spec }).await?
    else {
        unreachable!("CronAdd answers with CronAdded");
    };
    println!(
        "Added {} job `{}` [{}] — first run {}.",
        job.action.kind(),
        job.name,
        job.schedule,
        local_time(job.next_run_at)
    );
    if !control.via_gateway() {
        println!("(no gateway running — it fires once `komo gateway` is up)");
    }
    Ok(())
}

pub async fn cron_remove(control: &OperatorControl, name: &str) -> anyhow::Result<()> {
    let OperatorCommandResult::CronRemoved = control
        .command(OperatorCommand::CronRemove {
            name: name.to_string(),
        })
        .await?
    else {
        unreachable!("CronRemove answers with CronRemoved");
    };
    println!("Removed job `{name}`.");
    Ok(())
}

pub async fn cron_set_enabled(
    control: &OperatorControl,
    name: &str,
    enabled: bool,
) -> anyhow::Result<()> {
    let OperatorCommandResult::CronUpdated(job) = control
        .command(OperatorCommand::CronSetEnabled {
            name: name.to_string(),
            enabled,
        })
        .await?
    else {
        unreachable!("CronSetEnabled answers with CronUpdated");
    };
    if enabled {
        println!(
            "Enabled job `{}` — next run {}.",
            job.name,
            local_time(job.next_run_at)
        );
    } else {
        println!("Disabled job `{}`.", job.name);
    }
    Ok(())
}

/// Make a job due now; the gateway's every-minute sweep picks it up.
pub async fn cron_run(control: &OperatorControl, name: &str) -> anyhow::Result<()> {
    let OperatorCommandResult::CronUpdated(job) = control
        .command(OperatorCommand::CronTrigger {
            name: name.to_string(),
        })
        .await?
    else {
        unreachable!("CronTrigger answers with CronUpdated");
    };
    if control.via_gateway() {
        println!(
            "Job `{}` triggered — it runs on the gateway's next sweep tick (within a minute).",
            job.name
        );
    } else {
        println!(
            "Job `{}` marked due — it runs once a gateway is up (`komo gateway start`).",
            job.name
        );
    }
    Ok(())
}

/// List open tasks grouped by status (inbox first — it needs triage).
pub async fn task_list(control: &OperatorControl) -> anyhow::Result<()> {
    let OperatorQueryResult::Tasks(open) = control.query(OperatorQuery::Tasks).await? else {
        unreachable!("Tasks query answers with Tasks");
    };

    if open.is_empty() {
        println!("No open tasks.");
        return Ok(());
    }
    for status in [TaskStatus::Inbox, TaskStatus::Todo, TaskStatus::Waiting] {
        let group: Vec<_> = open.iter().filter(|t| t.status == status).collect();
        if group.is_empty() {
            continue;
        }
        println!("{}:", status.as_str());
        for t in group {
            let mut line = format!("  {}  {}", t.id, t.title);
            if !t.board.is_empty() {
                line.push_str(&format!("  #{}", t.board));
            }
            if !t.waiting_on.is_empty() {
                line.push_str(&format!("  (waiting on: {})", t.waiting_on));
            }
            if let Some(due) = t.due_at {
                line.push_str(&format!("  due {}", local_time(due)));
            }
            println!("{line}");
        }
    }
    Ok(())
}

/// A step's measured duration, at the precision a reader cares about: whole
/// milliseconds below a second, then one decimal, then whole seconds.
/// Token usage as a compact `  12.3k tok 78%↺` suffix for the run list; empty
/// when the provider reported nothing (0 = unknown, never "free").
///
/// The trailing figure is the prompt-cache hit rate, shown only when the
/// provider reported hits — a run list is where a broken prefix shows up as a
/// column of low numbers, which no per-turn token count would reveal.
fn fmt_tokens(tokens_in: i64, tokens_out: i64, tokens_cached: i64) -> String {
    let total = tokens_in.saturating_add(tokens_out);
    let tokens = match total {
        0 => return String::new(),
        n if n < 1000 => format!("  {n} tok"),
        n => format!("  {:.1}k tok", n as f64 / 1000.0),
    };
    match hit_rate(tokens_in, tokens_cached) {
        Some(rate) => format!("{tokens} {:.0}%↺", rate * 100.0),
        None => tokens,
    }
}

/// The cache hit rate, or `None` when there is nothing to report: no prompt
/// (unknown) or no hits at all, which reads the same as a provider that does
/// not report cache accounting — better silent than a misleading `0%`.
fn hit_rate(tokens_in: i64, tokens_cached: i64) -> Option<f64> {
    (tokens_in > 0 && tokens_cached > 0).then(|| tokens_cached as f64 / tokens_in as f64)
}

fn fmt_elapsed(ms: i64) -> String {
    match ms {
        ms if ms < 1000 => format!("{ms}ms"),
        ms if ms < 10_000 => format!("{:.1}s", ms as f64 / 1000.0),
        ms => format!("{}s", ms / 1000),
    }
}

/// Truncate a string to `n` chars for a single-line summary, collapsing newlines.
fn oneline(s: &str, n: usize) -> String {
    let flat = s.replace('\n', " ");
    if flat.chars().count() <= n {
        flat
    } else {
        let mut out: String = flat.chars().take(n).collect();
        out.push('…');
        out
    }
}

/// List recent runs (most recent first), one per line: id, status, time, plan,
/// and a snippet of the input. The run ledger (roadmap §7).
pub async fn run_list(control: &OperatorControl, limit: usize) -> anyhow::Result<()> {
    let OperatorQueryResult::Runs(runs) = control.query(OperatorQuery::Runs { limit }).await?
    else {
        unreachable!("Runs query answers with Runs");
    };

    if runs.is_empty() {
        println!("No runs recorded.");
        return Ok(());
    }
    for r in runs {
        println!(
            "{}  [{}]{}  {}  {}{}  {}",
            r.id,
            r.status.as_str(),
            if r.recoverable { " ⟲" } else { "" },
            local_time(r.started_at),
            if r.plan.is_empty() { "-" } else { &r.plan },
            fmt_tokens(r.tokens_in, r.tokens_out, r.tokens_cached),
            oneline(&r.input, 60),
        );
    }
    Ok(())
}

/// Show one run in full: its input, plan, outcome, and every tool step in order.
pub async fn run_inspect(control: &OperatorControl, id: &str) -> anyhow::Result<()> {
    let OperatorQueryResult::Run(fetched) = control
        .query(OperatorQuery::Run { id: id.to_string() })
        .await?
    else {
        unreachable!("Run query answers with Run");
    };
    let Some((run, steps)) = fetched else {
        println!("No run with id `{id}`.");
        return Ok(());
    };

    println!("run     {}", run.id);
    println!("session {}", run.session_id);
    println!("status  {}", run.status.as_str());
    if let Some(original) = &run.resumed_from {
        println!("resumed continuation of {original} (from its turn journal)");
    }
    println!("started {}", local_time(run.started_at));
    if let Some(ended) = run.ended_at {
        println!("ended   {}", local_time(ended));
    }
    if !run.plan.is_empty() {
        println!("plan    {}", run.plan);
    }
    // Read straight off the checkpoint directory rather than from a ledger
    // column: the store already knows what this run changed, and a second copy
    // in the ledger could only ever disagree with it.
    let checkpoints = komo_services::checkpoint_store::FsCheckpointStore::new(
        komo_config::komo_home().join("checkpoints"),
    );
    if let Ok(changed) = CheckpointStore::changed(&checkpoints, &run.id).await
        && !changed.is_empty()
    {
        println!(
            "files   {} changed (`komo run rollback {}`)",
            changed.len(),
            run.id
        );
        for file in &changed {
            let mark = if file.existed_before { "M" } else { "A" };
            println!("        {mark} {}", file.path);
        }
    }
    // 0/0 means the provider reported no usage (or the row predates the columns),
    // so say nothing rather than claim a free turn.
    if run.tokens_in != 0 || run.tokens_out != 0 {
        let cached = match hit_rate(run.tokens_in, run.tokens_cached) {
            Some(rate) => format!(
                " ({} cached, {:.0}% hit rate)",
                run.tokens_cached,
                rate * 100.0
            ),
            None => String::new(),
        };
        println!(
            "tokens  {} in{cached} / {} out",
            run.tokens_in, run.tokens_out
        );
    }
    if !run.memories.is_empty() {
        // Which stored memories this answer was built with. Ids, because they
        // are what `komo memory` takes — reading the text here would show what
        // the memory says *now*, not what the turn was given.
        let mut parts = Vec::new();
        if !run.memories.pinned.is_empty() {
            parts.push(format!("pinned {}", run.memories.pinned.join(" ")));
        }
        if !run.memories.recall.is_empty() {
            parts.push(format!("recall {}", run.memories.recall.join(" ")));
        }
        println!("memory  {}", parts.join("  ·  "));
    }
    println!("input   {}", oneline(&run.input, 200));
    if !run.error.is_empty() {
        println!("error   {}", run.error);
    }
    if run.recoverable {
        println!("resume  recoverable — `komo run resume {}`", run.id);
    }
    if !run.final_output.is_empty() {
        println!("output  {}", oneline(&run.final_output, 200));
    }

    if steps.is_empty() {
        println!("\n(no tool steps)");
        return Ok(());
    }
    println!("\nsteps:");
    for s in steps {
        // "??" rather than "ERR": the call never confirmed its result, so
        // whether it landed is unknown — which is exactly what an operator
        // asking "did that go through?" needs told apart from a failure.
        let mark = match (s.ok, s.uncertain) {
            (true, _) => "ok ",
            (false, true) => "?? ",
            (false, false) => "ERR",
        };
        // Steps recorded before `elapsed_ms` existed default to 0 — say nothing
        // rather than claim every one of them took no time.
        let took = if s.elapsed_ms > 0 {
            format!("  {}", fmt_elapsed(s.elapsed_ms))
        } else {
            String::new()
        };
        println!("  #{}  {}  {}{}", s.seq, mark, s.tool_name, took);
        println!("      args   {}", oneline(&s.args, 120));
        if s.ok {
            println!("      result {}", oneline(&s.result, 120));
        } else if s.uncertain {
            println!("      unknown {}", oneline(&s.error, 120));
        } else {
            println!("      error  {}", oneline(&s.error, 120));
        }
        // The tool's machine-readable view, when it has one. Indented JSON: this
        // is the operator surface, and `shell`'s exit code or an `edit`'s line
        // counts are the point of reading a step at all.
        if !s.structured.is_null() {
            let rendered = serde_json::to_string_pretty(&s.structured)
                .unwrap_or_else(|_| s.structured.to_string());
            for (i, line) in rendered.lines().enumerate() {
                let label = if i == 0 { "struct" } else { "      " };
                println!("      {label} {line}");
            }
        }
        // Where the elided middle went, for a result too large to hand the model
        // whole — this is the way back to what the preview cut.
        for path in &s.output_paths {
            println!("      output {path}");
        }
    }
    Ok(())
}

/// Prune the run ledger: delete runs (and their tool steps) started before
/// `cutoff` (unix seconds). The ledger accumulates like messages, so this is the
/// operator's manual trim — `run prune` resolves either `--before` or `--keep`
/// into a cutoff before calling this.
pub async fn run_prune(control: &OperatorControl, cutoff: i64) -> anyhow::Result<()> {
    let OperatorCommandResult::RunsPruned { removed } = control
        .command(OperatorCommand::PruneRuns { cutoff })
        .await?
    else {
        unreachable!("PruneRuns answers with RunsPruned");
    };
    if removed == 0 {
        println!("No runs older than {}; nothing pruned.", local_time(cutoff));
    } else {
        println!(
            "Pruned {removed} run(s) started before {}.",
            local_time(cutoff)
        );
    }
    Ok(())
}

/// Resolve the `--keep N` form to a cutoff timestamp: keep the N most recent
/// runs, returning the `started_at` of the first run to drop (everything older
/// is pruned). `None` = fewer than N+1 runs exist, so there's nothing to prune.
pub async fn run_keep_cutoff(
    control: &OperatorControl,
    keep: usize,
) -> anyhow::Result<Option<i64>> {
    // `Runs` already returns most-recent-first; ask for one more than we keep so
    // the (keep+1)-th run's start time becomes the cutoff.
    let OperatorQueryResult::Runs(runs) = control
        .query(OperatorQuery::Runs { limit: keep + 1 })
        .await?
    else {
        unreachable!("Runs query answers with Runs");
    };
    Ok(runs.get(keep).map(|r| r.started_at))
}

/// List stored sessions with creation time and message counts.
pub async fn session_list(control: &OperatorControl) -> anyhow::Result<()> {
    let OperatorQueryResult::Sessions(sessions) = control.query(OperatorQuery::Sessions).await?
    else {
        unreachable!("Sessions query answers with Sessions");
    };

    if sessions.is_empty() {
        println!("No sessions.");
        return Ok(());
    }
    for s in sessions {
        println!(
            "{}  created {}  {} messages ({} user turns)",
            s.id,
            local_time(s.created_at),
            s.messages,
            s.user_turns
        );
    }
    Ok(())
}

/// Delete every session with zero messages. An operator action — run it by
/// hand or from an external scheduler (launchd/cron), e.g. daily at 4am.
pub async fn session_clean(control: &OperatorControl) -> anyhow::Result<()> {
    let OperatorCommandResult::SessionsCleaned { removed } =
        control.command(OperatorCommand::CleanSessions).await?
    else {
        unreachable!("CleanSessions answers with SessionsCleaned");
    };
    println!("Removed {removed} empty session(s).");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_elapsed_scales_precision_with_magnitude() {
        assert_eq!(fmt_elapsed(0), "0ms");
        assert_eq!(fmt_elapsed(37), "37ms");
        assert_eq!(fmt_elapsed(999), "999ms");
        assert_eq!(fmt_elapsed(1000), "1.0s");
        assert_eq!(fmt_elapsed(2500), "2.5s");
        assert_eq!(fmt_elapsed(9999), "10.0s");
        assert_eq!(fmt_elapsed(12_000), "12s");
    }

    /// The hit rate rides along with the token count when the provider reported
    /// cache hits, and stays out of the way when it didn't.
    #[test]
    fn the_run_list_shows_a_hit_rate_only_when_there_is_one() {
        assert_eq!(fmt_tokens(0, 0, 0), "", "no usage reported says nothing");
        assert_eq!(fmt_tokens(700, 100, 0), "  800 tok");
        assert_eq!(fmt_tokens(9_000, 1_000, 7_200), "  10.0k tok 80%↺");
        // A cold first turn is a real 0%, but it reads identically to a
        // provider that reports no cache accounting at all — so say neither.
        assert_eq!(fmt_tokens(9_000, 1_000, 0), "  10.0k tok");
    }

    #[test]
    fn a_hit_rate_needs_both_a_prompt_and_hits() {
        assert_eq!(hit_rate(1_000, 250), Some(0.25));
        assert_eq!(hit_rate(0, 0), None, "no prompt is unknown, not 0%");
        assert_eq!(hit_rate(1_000, 0), None, "no hits reads as unreported");
    }
}
