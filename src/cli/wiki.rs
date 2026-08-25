//! `komo wiki index|search|status` — build and inspect the note-vault index.
//!
//! Routed through `operator_control` like the memory commands, and for the same
//! reason: a running gateway holds the index open, and the embedded backend
//! takes an exclusive lock on it. Going direct failed with a WAL lock error
//! whenever the gateway was up — which is most of the time.
//!
//! With no gateway running, the direct adapter opens the index in this process
//! instead. Both paths run the same `WikiOps`, so neither caller knows which
//! one answered.

use crate::services::operator_control::{
    OperatorCommand, OperatorCommandResult, OperatorControl, OperatorQuery, OperatorQueryResult,
};

/// Index the vault. Incremental by mtime unless `rebuild`.
///
/// A full rebuild takes minutes. Through the gateway there is no progress in the
/// terminal — the gateway logs it (`komo logs -f`), because the operator
/// protocol is request/response and streaming one command's progress would not
/// pay for the machinery.
pub async fn index(control: &OperatorControl, rebuild: bool) -> anyhow::Result<()> {
    if control.via_gateway() {
        println!("indexing via the running gateway — progress: komo logs -f");
    }
    if rebuild {
        println!("(rebuilding from scratch)");
    }
    let outcome = match control
        .command(OperatorCommand::ChunkIndex { rebuild })
        .await?
    {
        OperatorCommandResult::WikiIndexed(outcome) => outcome,
        _ => unreachable!("ChunkIndex answers with WikiIndexed"),
    };

    println!(
        "files    {} ({} changed, {} removed, {} unchanged)",
        outcome.files_seen,
        outcome.files_changed,
        outcome.files_removed,
        outcome.files_seen.saturating_sub(outcome.files_changed),
    );
    for skipped in &outcome.skipped {
        eprintln!("  skip {skipped}");
    }
    if outcome.chunks_written == 0 && outcome.files_removed == 0 {
        println!(
            "nothing to do — index is current ({} chunks)",
            outcome.chunks_total
        );
    } else {
        println!(
            "embedded {} chunks; index now holds {}",
            outcome.chunks_written, outcome.chunks_total
        );
    }
    Ok(())
}

/// Query the index exactly as the `wiki_search` tool does — same embedding,
/// same floor, same per-file cap — so this predicts what a turn would get back.
pub async fn search(control: &OperatorControl, query: &str, limit: usize) -> anyhow::Result<()> {
    let hits = match control
        .query(OperatorQuery::WikiSearch {
            query: query.to_string(),
            limit,
        })
        .await?
    {
        OperatorQueryResult::WikiHits(hits) => hits,
        _ => unreachable!("WikiSearch answers with WikiHits"),
    };

    if hits.is_empty() {
        println!("no matches");
        return Ok(());
    }
    for hit in hits {
        let preview: String = hit.text.chars().take(160).collect();
        println!(
            "\n── {} ({:.3})\n   {}\n   {}",
            hit.path,
            hit.score,
            hit.heading_path,
            preview.replace('\n', " ")
        );
    }
    Ok(())
}

pub async fn status(control: &OperatorControl) -> anyhow::Result<()> {
    let status = match control.query(OperatorQuery::WikiStatus).await? {
        OperatorQueryResult::WikiStatus(status) => status,
        _ => unreachable!("WikiStatus answers with WikiStatus"),
    };

    println!("vault      {}", status.vault);
    println!("backend    {}", status.backend);
    println!("location   {}", status.location);
    println!("collection {}", status.collection);
    println!("model      {}", status.model);
    println!(
        "indexed    {} files, {} chunks",
        status.files, status.chunks
    );
    match (status.dims, status.indexed_by.as_deref()) {
        (Some(dims), Some(indexed_by)) => {
            println!("vectors    {dims}-dim, written by `{indexed_by}`");
            // Vectors from two models are not comparable, and the index fixes
            // its width at creation — so this is not a warning to sit on.
            if indexed_by != status.model {
                println!(
                    "\n! index was built with `{indexed_by}` but config says `{}`.\n  \
                     Run `komo wiki index --rebuild`.",
                    status.model
                );
            }
        }
        (Some(dims), None) => println!("vectors    {dims}-dim"),
        _ => println!("vectors    (empty — run `komo wiki index`)"),
    }
    Ok(())
}
