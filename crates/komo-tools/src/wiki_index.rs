//! Maintain the note-vault index from inside a conversation.
//!
//! The read half of the vault has always been reachable (`wiki_search`), but
//! keeping it *correct* meant a host command: `komo wiki index --rebuild` when
//! the embedding model changed, and nothing at all for "is my index even
//! current". This closes that, under the same policy ladder as every other
//! side-effecting tool (`Category::Wiki`, targeted by action name).
//!
//! Three actions, deliberately three different risk levels:
//!
//! - `status` — read-only, `Risk::Safe`, deny-only. This is the diagnosis half,
//!   and it costs nothing, so nothing should stand between the model and it.
//! - `refresh` — incremental. Cheap by construction: indexing skips files whose
//!   mtime has not moved, and embedding is the entire cost of a run, so an
//!   unchanged vault costs almost nothing. Runs synchronously.
//! - `rebuild` — `Risk::Dangerous`, and runs in the **background**.
//!
//! Why `rebuild` cannot be synchronous, however generous the timeout: a rebuild
//! `reset()`s the store *before* refilling it, and takes minutes on a real
//! vault. The executor aborts a tool task at `max_duration`, dropping its
//! future — which would leave the store emptied and half-refilled while the tool
//! reported nothing worse than "timed out". So the run is spawned detached (it
//! outlives the aborted call, and the turn), and its outcome is read back with
//! `status`. `komo logs -f` shows progress meanwhile.

use std::sync::Arc;

use async_trait::async_trait;
use komo_core::domain::{
    approval::{ActionRef, ApprovalRequest, Decision},
    context::ToolContext,
    tool::{Tool, ToolError, ToolOutput, parse_args},
};
use komo_services::wiki_indexing::{AlreadyRunning, IndexOutcome, LastRun, WikiIndexRunner};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
struct Args {
    action: String,
}

pub struct WikiIndexTool {
    runner: Arc<WikiIndexRunner>,
}

impl WikiIndexTool {
    pub fn new(runner: Arc<WikiIndexRunner>) -> Self {
        Self { runner }
    }
}

fn now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

/// "3m ago" / "12s ago" — a duration the model can reason about without doing
/// arithmetic on two unix timestamps.
fn ago(seconds: i64) -> String {
    let s = seconds.max(0);
    if s < 90 {
        format!("{s}s ago")
    } else {
        format!("{}m ago", s / 60)
    }
}

fn describe_outcome(outcome: &IndexOutcome) -> String {
    let mut line = format!(
        "{} files seen, {} changed, {} removed; {} chunks embedded, index now holds {}",
        outcome.files_seen,
        outcome.files_changed,
        outcome.files_removed,
        outcome.chunks_written,
        outcome.chunks_total
    );
    // Named, not counted: an unreadable note is the kind of thing that silently
    // stays missing from search results until someone notices it by absence.
    if !outcome.skipped.is_empty() {
        line.push_str(&format!(
            "\nskipped {} file(s): {}",
            outcome.skipped.len(),
            outcome.skipped.join("; ")
        ));
    }
    line
}

fn describe_last(last: &LastRun) -> String {
    let kind = if last.rebuild { "rebuild" } else { "refresh" };
    match &last.result {
        Ok(outcome) => format!(
            "last run: {kind} finished {} — {}",
            ago(now().saturating_sub(last.finished_at)),
            describe_outcome(outcome)
        ),
        Err(error) => format!(
            "last run: {kind} FAILED {} — {error}\n\
             The index may be empty or partial; a failed rebuild leaves it that way \
             until another run succeeds.",
            ago(now().saturating_sub(last.finished_at))
        ),
    }
}

fn busy_message(busy: AlreadyRunning) -> String {
    format!(
        "An index run is already in progress (started {}{}). Nothing was started; \
         check `action=\"status\"` for its outcome once it finishes.",
        ago(now().saturating_sub(busy.since)),
        if busy.rebuild { ", a full rebuild" } else { "" }
    )
}

#[async_trait]
impl Tool for WikiIndexTool {
    fn name(&self) -> &'static str {
        "wiki_index"
    }

    fn description(&self) -> &'static str {
        "Inspect and maintain the search index behind `wiki_search`. \
         action=\"status\" reports what the index holds (files, chunks, the \
         embedding model that wrote it) and what the last maintenance run did — \
         use it whenever `wiki_search` returns nothing for a note the user is \
         sure exists, or before claiming the vault does not contain something. \
         action=\"refresh\" indexes notes changed since the last run; it is cheap \
         and is the right answer when the user just wrote or edited notes. \
         action=\"rebuild\" discards the whole index and builds it again from \
         scratch — needed only when the embedding model changed or the index is \
         corrupt, it takes minutes, and it runs in the background (poll \
         action=\"status\"). Prefer `refresh`; never `rebuild` to fix a search \
         that simply had no good match."
    }

    /// A synchronous `refresh` can park on an approval prompt *and* then embed;
    /// `rebuild` returns immediately, so this bound is `refresh`'s.
    fn max_duration(&self) -> Option<std::time::Duration> {
        Some(komo_core::domain::tool::APPROVAL_BOUND)
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["status", "refresh", "rebuild"],
                    "description": "status = report the index and the last run (read-only); refresh = index changed notes; rebuild = discard and rebuild everything (background, minutes)."
                }
            },
            "required": ["action"]
        })
    }

    async fn call(&self, args: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: Args = parse_args(&args)?;
        match args.action.as_str() {
            "status" => self.status(ctx).await,
            "refresh" => self.refresh(ctx).await,
            "rebuild" => self.rebuild(ctx).await,
            other => Err(ToolError::InvalidInput(format!(
                "unknown action `{other}` (expected status/refresh/rebuild)"
            ))),
        }
    }
}

impl WikiIndexTool {
    /// Read-only, so it consults the approver at `Risk::Safe`: a deny rule can
    /// fence the whole category off, but this never prompts.
    async fn status(&self, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let request = ApprovalRequest::safe("Read the note-vault index's status").with_action(
            ActionRef::Wiki {
                action: "status".to_string(),
            },
        );
        if let Decision::Deny { feedback } = ctx.decide(&request).await {
            return Err(ToolError::Denied(feedback.unwrap_or_else(|| {
                "reading the index status is denied by policy".into()
            })));
        }

        let index = self.runner.index();
        let indexed = index
            .indexed()
            .await
            .map_err(|e| ToolError::Failed(e.context("reading the index failed")))?;
        let chunks = index
            .count()
            .await
            .map_err(|e| ToolError::Failed(e.context("counting the index failed")))?;
        let spec = index.vector_spec().await.unwrap_or(None);
        let configured_model = self.runner.embedding_model();

        let mut lines = vec![
            format!("vault    {}", self.runner.vault().display()),
            format!("model    {configured_model}"),
            format!("indexed  {} files, {chunks} chunks", indexed.len()),
        ];
        match &spec {
            Some((dims, wrote)) if !wrote.is_empty() => {
                lines.push(format!("vectors  {dims}-dim, written by `{wrote}`"));
                // Vectors from two models are not comparable and the index fixes
                // its width at creation, so this is *the* index anomaly — and the
                // only fix is a rebuild.
                if wrote != configured_model {
                    lines.push(format!(
                        "\n! STALE: the index was built with `{wrote}` but config now says \
                         `{configured_model}`. Searches are matching against the wrong vector \
                         space. This needs action=\"rebuild\"."
                    ));
                }
            }
            Some((dims, _)) => lines.push(format!("vectors  {dims}-dim")),
            None => lines.push(
                "vectors  (none — the index is empty; action=\"refresh\" fills it)".to_string(),
            ),
        }

        let run = self.runner.snapshot();
        match run.running_since {
            Some(since) => lines.push(format!(
                "running  yes — {} started {}",
                if run.running_rebuild {
                    "a full rebuild"
                } else {
                    "a refresh"
                },
                ago(now().saturating_sub(since))
            )),
            None => lines.push("running  no".to_string()),
        }
        if let Some(last) = &run.last {
            lines.push(describe_last(last));
        }

        Ok(ToolOutput::text(lines.join("\n")).with_structured(json!({
            "files": indexed.len(),
            "chunks": chunks,
            "configured_model": configured_model,
            "indexed_by": spec.as_ref().map(|(_, m)| m.clone()),
            "stale": spec
                .as_ref()
                .is_some_and(|(_, m)| !m.is_empty() && m != configured_model),
            "running": run.running_since.is_some(),
        })))
    }

    /// Incremental, and synchronous because it is cheap: unchanged files are
    /// never read or embedded, so this is near-free on a vault nobody touched.
    async fn refresh(&self, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let request = ApprovalRequest::normal("Index note-vault changes (incremental)")
            .with_scope_key("wiki:refresh".to_string())
            .with_action(ActionRef::Wiki {
                action: "refresh".to_string(),
            });
        if let Decision::Deny { feedback } = ctx.decide(&request).await {
            return Err(ToolError::Denied(
                feedback.unwrap_or_else(|| "indexing the vault was not approved".into()),
            ));
        }

        let outcome = match self.runner.run(false, now()).await {
            Err(busy) => return Ok(ToolOutput::text(busy_message(busy))),
            Ok(Err(error)) => {
                return Err(ToolError::Failed(
                    error.context("indexing the vault failed"),
                ));
            }
            Ok(Ok(outcome)) => outcome,
        };

        let text = if outcome.chunks_written == 0 && outcome.files_removed == 0 {
            format!(
                "Index already current — nothing changed ({} files, {} chunks).",
                outcome.files_seen, outcome.chunks_total
            )
        } else {
            format!("Refreshed. {}", describe_outcome(&outcome))
        };
        Ok(ToolOutput::text(text).with_structured(json!({
            "files_changed": outcome.files_changed,
            "files_removed": outcome.files_removed,
            "chunks_written": outcome.chunks_written,
            "chunks_total": outcome.chunks_total,
        })))
    }

    /// Destructive and minutes long: the store is reset before it is refilled,
    /// so the vault is unsearchable until the run finishes. Dangerous, and
    /// detached so no timeout can abandon it half-done.
    async fn rebuild(&self, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let request = ApprovalRequest::dangerous(
            "Rebuild the note-vault index from scratch",
            format!(
                "The existing index for {} is discarded immediately, then rebuilt by \
                 re-embedding every note. This takes minutes, and `wiki_search` finds \
                 nothing until it finishes. If it fails part-way the index stays empty or \
                 partial — searches then quietly return no results rather than erroring. \
                 Only worth it when the embedding model changed or the index is corrupt; \
                 an ordinary update is action=\"refresh\".",
                self.runner.vault().display()
            ),
        )
        .with_action(ActionRef::Wiki {
            action: "rebuild".to_string(),
        });
        if let Decision::Deny { feedback } = ctx.decide(&request).await {
            return Err(ToolError::Denied(
                feedback.unwrap_or_else(|| "rebuilding the index was not approved".into()),
            ));
        }

        // Claim **before** spawning, so a conflict is reported to the model here
        // rather than discovered inside a detached task with nobody listening.
        // Reporting "started" for a run that was refused is the one lie this
        // design must not tell.
        let claim = match self.runner.claim(true, now()) {
            Ok(claim) => claim,
            Err(busy) => return Ok(ToolOutput::text(busy_message(busy))),
        };

        // Detached on purpose: the executor aborts *this* call at
        // `max_duration`, and a rebuild outlives that. The spawned task carries
        // the claim, which frees the slot on drop even if the task is killed.
        let runner = self.runner.clone();
        tokio::spawn(async move {
            // Outcome and failure are both logged by the runner and recorded for
            // `status` — there is deliberately nothing to do with the result here.
            let _ = runner.run_claimed(claim).await;
        });

        Ok(ToolOutput::text(format!(
            "Rebuild started in the background for {}. It takes minutes and `wiki_search` \
             will find nothing until it finishes. Check `action=\"status\"` for the result; \
             progress is in the gateway log (`komo logs -f`). Do not start another run \
             meanwhile — it would be refused.",
            self.runner.vault().display()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::{Approver, Risk};
    use komo_core::domain::chunk_index::{ChunkHit, ChunkIndex, IndexedChunk, IndexedFile};
    use komo_core::domain::context::SessionContext;
    use komo_core::domain::embedding::EmbeddingClient;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// An index that answers `status` from fixed values and refuses to be
    /// written — every test here is about what the tool decides, not indexing.
    struct FakeIndex {
        chunks: usize,
        spec: Option<(usize, String)>,
        reset_calls: Mutex<usize>,
    }

    #[async_trait]
    impl ChunkIndex for FakeIndex {
        async fn upsert(&self, _chunks: &[IndexedChunk]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_paths(&self, _paths: &[String]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn indexed(&self) -> anyhow::Result<HashMap<String, IndexedFile>> {
            Ok(HashMap::new())
        }
        async fn search(
            &self,
            _v: &[f32],
            _q: &str,
            _limit: usize,
            _floor: f32,
        ) -> anyhow::Result<Vec<ChunkHit>> {
            Ok(Vec::new())
        }
        async fn count(&self) -> anyhow::Result<usize> {
            Ok(self.chunks)
        }
        async fn reset(&self) -> anyhow::Result<()> {
            *self.reset_calls.lock().unwrap() += 1;
            Ok(())
        }
        async fn vector_spec(&self) -> anyhow::Result<Option<(usize, String)>> {
            Ok(self.spec.clone())
        }
    }

    struct FakeEmbedder;
    #[async_trait]
    impl EmbeddingClient for FakeEmbedder {
        async fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
            Ok(texts.iter().map(|_| vec![0.1, 0.2]).collect())
        }
        fn model_id(&self) -> &str {
            "configured-model"
        }
    }

    /// Records the requests it saw and answers with a fixed verdict.
    struct Recorder {
        allow: bool,
        seen: Mutex<Vec<(Risk, Option<String>)>>,
    }

    #[async_trait]
    impl Approver for Recorder {
        async fn decide(&self, request: &ApprovalRequest) -> Decision {
            let target = request.action.as_ref().map(|a| match a {
                ActionRef::Wiki { action } => action.clone(),
                _ => "other".to_string(),
            });
            self.seen.lock().unwrap().push((request.risk, target));
            self.allow.into()
        }
    }

    fn tool_with(
        chunks: usize,
        spec: Option<(usize, String)>,
        allow: bool,
    ) -> (WikiIndexTool, Arc<Recorder>) {
        let runner = Arc::new(WikiIndexRunner::new(
            Arc::new(FakeIndex {
                chunks,
                spec,
                reset_calls: Mutex::new(0),
            }),
            Arc::new(FakeEmbedder),
            // A path that does not exist, so any real run fails fast — the tests
            // that care about running assert on the gate, not on indexing.
            std::path::PathBuf::from("/nonexistent-komo-vault"),
            "configured-model".to_string(),
        ));
        (
            WikiIndexTool::new(runner),
            Arc::new(Recorder {
                allow,
                seen: Mutex::new(Vec::new()),
            }),
        )
    }

    async fn run(
        t: &WikiIndexTool,
        action: &str,
        rec: &Arc<Recorder>,
    ) -> Result<ToolOutput, ToolError> {
        let ctx = ToolContext::new(SessionContext::detached("cli:test"), None, rec.clone());
        t.call(json!({"action": action}), &ctx).await
    }

    /// `status` is the diagnosis half, so it asks at `Risk::Safe` — the level
    /// the policy layer treats as deny-only, never escalating it to a prompt.
    /// The risk it asks *at* is the invariant; whether a given approver says yes
    /// is that approver's business.
    #[tokio::test]
    async fn status_asks_at_safe_risk() {
        let (t, rec) = tool_with(42, Some((1024, "configured-model".into())), true);
        run(&t, "status", &rec).await.unwrap();
        let seen = rec.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, Risk::Safe);
        assert_eq!(seen[0].1.as_deref(), Some("status"));
    }

    /// The index anomaly that matters: built by one model, config now names
    /// another. Vectors from two models are not comparable, so this has to be
    /// stated loudly and has to point at the only fix.
    #[tokio::test]
    async fn status_flags_a_model_mismatch_as_stale() {
        let (t, rec) = tool_with(500, Some((768, "old-model".into())), true);
        let out = run(&t, "status", &rec).await.unwrap();
        assert!(out.text.contains("STALE"), "{}", out.text);
        assert!(out.text.contains("old-model"), "{}", out.text);
        assert!(out.text.contains("configured-model"), "{}", out.text);
        assert!(out.text.contains("rebuild"), "{}", out.text);
        assert_eq!(out.structured["stale"], json!(true));
    }

    #[tokio::test]
    async fn status_of_a_matching_index_is_not_stale() {
        let (t, rec) = tool_with(500, Some((768, "configured-model".into())), true);
        let out = run(&t, "status", &rec).await.unwrap();
        assert!(!out.text.contains("STALE"), "{}", out.text);
        assert_eq!(out.structured["stale"], json!(false));
    }

    #[tokio::test]
    async fn status_reports_an_empty_index() {
        let (t, rec) = tool_with(0, None, true);
        let out = run(&t, "status", &rec).await.unwrap();
        assert!(out.text.contains("empty"), "{}", out.text);
    }

    /// `rebuild` is the destructive one, so it must reach the approver as
    /// `Dangerous` — that is what keeps a saved grant from ever covering it.
    #[tokio::test]
    async fn rebuild_asks_as_dangerous_and_refresh_as_normal() {
        let (t, rec) = tool_with(1, None, false);
        let _ = run(&t, "rebuild", &rec).await;
        let _ = run(&t, "refresh", &rec).await;
        let seen = rec.seen.lock().unwrap();
        assert_eq!(seen[0], (Risk::Dangerous, Some("rebuild".to_string())));
        assert_eq!(seen[1], (Risk::Normal, Some("refresh".to_string())));
    }

    /// A denial must stop the run, not merely report one. For `rebuild` this is
    /// the difference between "nothing happened" and "the index was dropped".
    #[tokio::test]
    async fn a_denied_rebuild_starts_nothing() {
        let (t, rec) = tool_with(1, None, false);
        let err = run(&t, "rebuild", &rec).await.unwrap_err();
        assert!(matches!(err, ToolError::Denied(_)), "{err:?}");
        assert!(
            t.runner.snapshot().running_since.is_none(),
            "a denied rebuild must not have claimed a run"
        );
    }

    /// The claim is taken synchronously, so a second `rebuild` while one is in
    /// flight is refused with an answer rather than silently queued.
    #[tokio::test]
    async fn a_second_rebuild_is_refused_while_one_runs() {
        let (t, rec) = tool_with(1, None, true);
        // Hold the slot exactly as an in-flight run would.
        let _claim = t.runner.claim(true, now()).unwrap();
        let out = run(&t, "rebuild", &rec).await.unwrap();
        assert!(out.text.contains("already in progress"), "{}", out.text);
        assert!(out.text.contains("Nothing was started"), "{}", out.text);
    }

    #[tokio::test]
    async fn unknown_action_is_rejected() {
        let (t, rec) = tool_with(1, None, true);
        let err = run(&t, "reindex", &rec).await.unwrap_err();
        assert!(matches!(err, ToolError::InvalidInput(_)), "{err:?}");
    }
}
