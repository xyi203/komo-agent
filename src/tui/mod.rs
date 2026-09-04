//! Full-screen chat TUI — `komo chat`'s interface (a terminal is required;
//! scripted access goes through the gateway's api channel instead). A ratatui
//! front end over two backends: a running gateway via
//! [`GatewayClient::chat_streaming`] (trusted loopback, approvals auto-granted
//! server-side), or the in-process [`AgentRuntime`] against the local db.
//!
//! The first frame paints before any backend exists: connecting (gateway
//! probe, or db opens plus full wiring) runs on a background task ([`Boot`])
//! while the event loop is already live. Drafting works immediately; one
//! submission queues (`in_flight` already enforces one turn at a time) and
//! dispatches the moment the backend lands.
//!
//! Layout: scrollable transcript · status line (spinner while a turn runs) ·
//! bordered input box. Enter sends; Shift/Alt-Enter or Ctrl-J insert a newline,
//! and the box grows with the draft. Pastes follow grok build (see
//! `tui/paste.rs`): a big one folds to a `[Pasted: N lines]` chip that edits and
//! deletes as one object while the draft keeps the text verbatim, and a burst of
//! keystrokes from a terminal without bracketed paste is coalesced back into one
//! paste instead of submitting at its first newline. As the agent works, each tool call renders as a live
//! activity line (`⚙ shell …` → `✓`/`✗` with a result preview) fed by the
//! turn's [`TurnEvent`] stream — from the in-process executor's event sink in
//! local mode, or parsed from the gateway's SSE stream in remote mode; the
//! running tool also shows in the status line. In local mode, a side-effecting
//! tool's approval request arrives over a channel ([`TuiApprover`]) and renders
//! as a modal — `y`/`s`/`n` — with the same semantics as `cli/approver.rs`'s
//! stdin prompt (still used by `komo run resume`).
//!
//! Logs: `main.rs::init_tracing` routes tracing to `~/.komo/logs/chat-tui.log`
//! when it detects the TUI will run — stderr writes would tear the alternate
//! screen. `ratatui::init` installs a panic hook that restores the terminal.

mod app;
mod approver;
mod markdown;
mod paste;
mod ui;

use komo_agent::interaction::{CancelState, CancelTicket, WaitParts, is_user_reply, record_wake};
use komo_agent::runtime::AgentRuntime;
use komo_core::domain::awaiting::Awaiting;
use komo_core::domain::cancel::{CANCELLED_REPLY, is_cancelled};
use komo_core::domain::session_event::WakeupCause;
use komo_core::domain::wakeup::is_suspended;
use komo_infra::persistence::db::Db;
use komo_services::tool_execution::{SessionContext, SessionOrigin, with_session};
use std::{io, path::PathBuf, sync::Arc};

use crossterm::{
    event::{
        DisableBracketedPaste, EnableBracketedPaste, Event, EventStream, KeyEventKind,
        KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
    },
    execute,
    terminal::supports_keyboard_enhancement,
};
use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::{
    cli::wiring,
    domain::{
        approval::Approver,
        events::{ToolEventSink, TurnEvent},
        gateway::ReplySink,
        message::{Message, Role as MessageRole},
        repository::{MessageRepository, SessionRepository},
        session::Session,
    },
    infra::gateway_client::{GatewayClient, folder_workspace_id, folder_workspace_path},
};
use komo_config::ConfigSnapshot;

use app::{Action, App, Role};
use approver::{ApprovalPrompt, TuiApprover};

/// How a turn reaches the agent. Cloneable (all `Arc`) so each turn can run on
/// its own task while the event loop keeps handling keys.
#[derive(Clone)]
enum Backend {
    /// A running gateway over loopback HTTP (trusted; server-side approvals).
    Remote {
        gateway: Arc<GatewayClient>,
        /// Opaque, server-validated id for the directory from which this TUI
        /// was launched. The gateway applies it only when creating a session.
        workspace: String,
    },
    /// In-process agent against the local db (approvals via the TUI modal).
    Local {
        runtime: Arc<AgentRuntime>,
        db: Arc<Db>,
    },
}

impl Backend {
    /// Run one turn. In local mode, `ctx` carries an interactive session
    /// context (its sink feeds mid-turn messages — `ask_user` questions —
    /// into the transcript); remote turns are handled server-side.
    async fn turn(
        &self,
        session_id: &str,
        input: String,
        ctx: Option<SessionContext>,
        events: mpsc::UnboundedSender<TurnEvent>,
    ) -> anyhow::Result<String> {
        match self {
            // Remote turns run server-side; stream their tool events over SSE
            // and forward each onto the loop's event channel.
            Backend::Remote { gateway, workspace } => {
                gateway
                    .chat_streaming_in_workspace(session_id, &input, workspace, |ev| {
                        let _ = events.send(ev);
                    })
                    .await
            }
            // Local turns emit tool events through the sink on `ctx` (set by the
            // caller); the `events` sender is unused on this arm.
            Backend::Local { runtime, .. } => match ctx {
                Some(ctx) => with_session(ctx, runtime.handle_input(session_id, input)).await,
                None => runtime.handle_input(session_id, input).await,
            },
        }
    }
}

/// A [`ToolEventSink`] that forwards each live [`TurnEvent`] (a tool starting or
/// finishing) into the TUI event loop's channel for the activity feed. Used by
/// the local in-process turn; the remote turn feeds the same channel by parsing
/// the gateway's SSE stream (see [`Backend::turn`]).
struct TuiEventSink {
    tx: mpsc::UnboundedSender<TurnEvent>,
}

impl ToolEventSink for TuiEventSink {
    fn emit(&self, event: TurnEvent) {
        // Best-effort: if the loop is gone the send just drops.
        let _ = self.tx.send(event);
    }
}

/// A [`ReplySink`] that feeds mid-turn agent messages (the `ask_user`
/// question) into the TUI event loop's channel for transcript rendering.
struct ChannelSink {
    tx: mpsc::UnboundedSender<String>,
}

#[async_trait::async_trait]
impl ReplySink for ChannelSink {
    async fn send(&self, text: &str) -> anyhow::Result<()> {
        self.tx
            .send(text.to_string())
            .map_err(|_| anyhow::anyhow!("TUI sink closed"))
    }
}

/// Everything the event loop needs that is not ready at first paint. Produced
/// by the background boot task while the first frame is already on screen —
/// connecting (gateway probe, or three db opens plus full wiring) is the whole
/// startup cost, and nothing in it needs the terminal, so the paint must not
/// wait for it.
struct Boot {
    backend: Backend,
    /// The session to drive: the fresh id, or the resolved one on resume.
    session: String,
    /// The resumed transcript; empty for a fresh session.
    history: Vec<Message>,
    /// The session's own workspace on resume; the startup directory otherwise.
    workspace: PathBuf,
    /// The wait a turn of this session is stopped in, on resume. A fresh
    /// session has none.
    awaiting: Option<Awaiting>,
}

type BootTask = tokio::task::JoinHandle<anyhow::Result<Boot>>;

/// Start the TUI on a fresh session: paint immediately, and connect in the
/// background — a running gateway holds the db lock, so route turns to it;
/// otherwise run in-process.
pub async fn run(config: ConfigSnapshot) -> anyhow::Result<()> {
    let workspace = startup_workspace()?;
    let (approval_tx, approval_rx) = mpsc::unbounded_channel();
    let session = new_session_id();
    let boot: BootTask = tokio::spawn({
        let (workspace, session) = (workspace.clone(), session.clone());
        let approval_tx = approval_tx.clone();
        async move {
            let connected = connect(&config, &workspace, approval_tx).await?;
            // The session row must exist before the first turn lands on it;
            // rotation via `/new` is gated until this task completes, so the
            // id ensured here is the id the first turn uses.
            if let Backend::Local { db, .. } = &connected.backend {
                ensure_session(db, &session, &workspace).await?;
            }
            Ok(Boot {
                backend: connected.backend,
                session,
                history: Vec::new(),
                workspace,
                awaiting: None,
            })
        }
    });
    drive(boot, (approval_tx, approval_rx), session, false, workspace).await
}

/// Continue an existing session (`komo resume <id>` on a TTY). Errors if the
/// session doesn't exist — resume never creates one. A bare API id is accepted
/// as a convenience for the UUID shown by clients.
pub async fn resume(config: ConfigSnapshot, id: &str) -> anyhow::Result<()> {
    let fallback_workspace = startup_workspace()?;
    let (approval_tx, approval_rx) = mpsc::unbounded_channel();
    let id = id.to_string();
    let boot: BootTask = tokio::spawn({
        let fallback = fallback_workspace.clone();
        let id = id.clone();
        let approval_tx = approval_tx.clone();
        async move {
            let connected = connect(&config, &fallback, approval_tx).await?;
            let session = resolve_resume_id(&connected.backend, &id).await?;
            let workspace = resume_workspace(&connected.backend, &session, fallback).await?;
            let history = resume_messages(&connected.backend, &session).await?;
            let awaiting = resume_awaiting(&connected.backend, &session).await?;
            Ok(Boot {
                backend: connected.backend,
                session,
                history,
                workspace,
                awaiting,
            })
        }
    });
    // The raw argument stands in as the session id until the boot task
    // resolves it; a queued draft dispatches only after the resolved id is
    // installed.
    drive(
        boot,
        (approval_tx, approval_rx),
        id,
        true,
        fallback_workspace,
    )
    .await
}

/// Confirm the id names a session that exists. A session id is a UUID and
/// nothing else now, so there is no second form to try.
///
/// A non-UUID id is refused **here**, not at the first message. Sessions from an
/// older komo (`feishu:oc_x`, `api:<uuid>`) are still listed and still readable,
/// but the gateway will not run a turn on one — so opening a chat window that
/// hydrates fine and then rejects everything typed into it is the worse of the
/// two failures.
async fn resolve_resume_id(backend: &Backend, id: &str) -> anyhow::Result<String> {
    if uuid::Uuid::parse_str(id).is_err() {
        anyhow::bail!(
            "`{id}` is a session from an older komo and can no longer be continued \
             (its transcript is still in ~/.komo/sessions/); start a new one with `komo chat`"
        );
    }
    let resolved = match backend {
        Backend::Remote { gateway, .. } => gateway
            .sessions()
            .await?
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.id.clone()),
        Backend::Local { db, .. } => SessionRepository::find(&**db, id)
            .await?
            .map(|session| session.id),
    };
    resolved.ok_or_else(|| anyhow::anyhow!("no session with id `{id}` (see `komo session list`)"))
}

async fn resume_messages(backend: &Backend, id: &str) -> anyhow::Result<Vec<Message>> {
    match backend {
        Backend::Remote { gateway, .. } => gateway.session_messages(id).await,
        Backend::Local { db, .. } => MessageRepository::list_by_session(&**db, id).await,
    }
}

/// The wait this session is stopped in, if any — the session projection's
/// `awaiting`, folded from the log at the last turn boundary.
///
/// Read once, on resume: a turn suspended in *this* UI is a turn this UI is
/// still holding, so the only wait it can learn about here is one another
/// ingress parked.
async fn resume_awaiting(backend: &Backend, id: &str) -> anyhow::Result<Option<Awaiting>> {
    match backend {
        Backend::Remote { gateway, .. } => Ok(gateway
            .sessions()
            .await?
            .into_iter()
            .find(|s| s.id == id)
            .and_then(|s| s.awaiting)),
        Backend::Local { db, .. } => Ok(SessionRepository::find(&**db, id)
            .await?
            .and_then(|session| session.awaiting)),
    }
}

/// Local sessions persist an opaque folder id. Honor it on resume so reopening
/// a conversation from a different terminal directory cannot redirect its
/// filesystem tools. Older sessions and unavailable folders retain the startup
/// directory as their backward-compatible default.
async fn resume_workspace(
    backend: &Backend,
    id: &str,
    fallback: PathBuf,
) -> anyhow::Result<PathBuf> {
    let Backend::Local { db, .. } = backend else {
        return Ok(fallback);
    };
    Ok(SessionRepository::find(&**db, id)
        .await?
        .and_then(|session| folder_workspace_path(&session.workspace))
        .unwrap_or(fallback))
}

struct Connected {
    backend: Backend,
}

/// Connect to whatever backend is available. `approval_tx` feeds the event
/// loop's approval modal; the loop keeps its own parked sender so the channel
/// stays open in remote mode, where this one is simply dropped.
async fn connect(
    config: &ConfigSnapshot,
    workspace: &PathBuf,
    approval_tx: mpsc::UnboundedSender<ApprovalPrompt>,
) -> anyhow::Result<Connected> {
    if let Some(gw) = GatewayClient::try_connect().await {
        return Ok(Connected {
            backend: Backend::Remote {
                gateway: Arc::new(gw),
                workspace: folder_workspace_id(workspace)?,
            },
        });
    }
    // No gateway is running (we'd have taken the remote path above), so opening
    // the database here can't collide with its exclusive lock. Jobs the user
    // schedules from this session fire once a gateway comes up.
    let db = Arc::new(Db::connect(&config.runtime.db_url).await?);
    let approver: Arc<dyn Approver> = Arc::new(TuiApprover::new(approval_tx));
    let wired = wiring::build(config, db.clone(), approver).await?;
    Ok(Connected {
        backend: Backend::Local {
            runtime: Arc::new(wired.runtime),
            db,
        },
    })
}

/// Set up the terminal, run the event loop, and always restore — including on
/// an error path (the panic path is covered by `ratatui::init`'s hook).
async fn drive(
    boot: BootTask,
    approvals: (
        mpsc::UnboundedSender<ApprovalPrompt>,
        mpsc::UnboundedReceiver<ApprovalPrompt>,
    ),
    session: String,
    resuming: bool,
    workspace: PathBuf,
) -> anyhow::Result<()> {
    let mut terminal = ratatui::init();
    // Bracketed paste turns a multi-line clipboard dump into one `Event::Paste`
    // instead of a stream of Enters (which used to send a message per line).
    // The kitty keyboard flags are what let a terminal report Shift/Alt-Enter as
    // a modified Enter; where they are unsupported, Ctrl-J still inserts a
    // newline.
    let _ = execute!(io::stdout(), EnableBracketedPaste);
    let enhanced = matches!(supports_keyboard_enhancement(), Ok(true))
        && execute!(
            io::stdout(),
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        )
        .is_ok();
    let result = event_loop(&mut terminal, boot, approvals, session, resuming, workspace).await;
    if enhanced {
        let _ = execute!(io::stdout(), PopKeyboardEnhancementFlags);
    }
    let _ = execute!(io::stdout(), DisableBracketedPaste);
    ratatui::restore();
    // Print only after leaving the alternate screen, so the command survives in
    // the user's normal terminal scrollback and is immediately copyable. A quit
    // before the backend connected has no session to point at (fresh sessions
    // are only created once the boot task lands), so the hint is skipped.
    if let Ok(Some(session_id)) = &result {
        println!("komo resume {session_id}");
    }
    result.map(|_| ())
}

async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    boot: BootTask,
    approvals: (
        mpsc::UnboundedSender<ApprovalPrompt>,
        mpsc::UnboundedReceiver<ApprovalPrompt>,
    ),
    session: String,
    resuming: bool,
    workspace: PathBuf,
) -> anyhow::Result<Option<String>> {
    // Keep the sender alive for the whole loop (remote mode has no other
    // holder) so `approval_rx.recv()` pends instead of returning None forever.
    let (_approval_tx, mut approval_rx) = approvals;
    // Mid-turn agent messages (the `ask_user` question) from the local turn's
    // sink; the sender is also parked so the arm pends in remote mode.
    let (sink_tx, mut sink_rx) = mpsc::unbounded_channel::<String>();
    // Live tool-call events for the activity feed (both backends). The sender is
    // cloned per turn; this original stays alive so the arm never closes.
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<TurnEvent>();
    // Cancellation slots for local turns, the same registry the api channel uses
    // — reused rather than reimplemented so "stop" means one thing whichever
    // surface asked. Remote turns are cancelled server-side over HTTP, where
    // their slot already lives.
    let cancels = Arc::new(CancelState::new());

    // The backend arrives from the boot task while the UI is already live.
    // Until then the user can draft freely and queue one submission (the same
    // one-turn-at-a-time discipline `in_flight` already enforces); everything
    // that needs the backend — dispatch, `/new`, resume history — waits.
    let mut boot = Some(boot);
    let mut backend: Option<Backend> = None;
    let mut pending: Option<String> = None;
    let mut workspace = workspace;

    let mut app = App::new(session);
    app.connecting = true;
    app.push(
        Role::Info,
        if resuming {
            format!("Komo v0.1 — resuming `{}`…", app.session_id)
        } else {
            format!(
                "Komo v0.1 — session `{}`\nworkspace: `{}`",
                app.session_id,
                workspace.display(),
            )
        },
    );

    let (turn_tx, mut turn_rx) = mpsc::unbounded_channel::<TurnEnd>();
    // Terminal input arrives over a channel rather than being awaited inline: a
    // paste that a terminal delivers as keystrokes has to be *collected* (see
    // `paste::extend_for_paste`), which needs the receiver free while the batch
    // is assembled.
    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Event>();
    tokio::spawn(async move {
        let mut events = EventStream::new();
        while let Some(Ok(event)) = events.next().await {
            if input_tx.send(event).is_err() {
                break; // the loop is gone
            }
        }
    });
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(120));

    'main: loop {
        terminal.draw(|frame| ui::render(frame, &app))?;

        let mut batch: Vec<Event> = Vec::new();
        tokio::select! {
            // Biased so ordering is deterministic: keys stay responsive, and
            // queued tool events always drain before the final reply — so the
            // ✓ activity lines never render below the agent's answer.
            biased;
            maybe_event = input_rx.recv() => {
                // Collected below, once the receiver is free again.
                let Some(event) = maybe_event else { break 'main };
                batch.push(event);
            }
            // The backend landing (fires once). Install it, render what it
            // brought — resume history, the mode line — and dispatch the
            // draft queued while it was starting. A boot failure ends the
            // loop: the terminal is restored on the way out and the error
            // prints where the pre-paint failure used to.
            booted = async { boot.as_mut().expect("guarded by is_some").await }, if boot.is_some() => {
                boot = None;
                let ready = booted.map_err(anyhow::Error::from).and_then(|r| r)?;
                app.connecting = false;
                app.session_id = ready.session;
                workspace = ready.workspace;
                app.awaiting = ready.awaiting;
                for message in ready.history {
                    let role = match message.role {
                        MessageRole::User => Role::You,
                        MessageRole::Assistant => Role::Agent,
                        // Persisted system/tool entries are history, not live
                        // tool activity (which has special spinner/status
                        // rendering in the TUI).
                        MessageRole::System | MessageRole::Tool => Role::Info,
                    };
                    app.push(role, message.content);
                }
                let mode = match &ready.backend {
                    Backend::Remote { .. } => "connected to the running gateway (trusted)",
                    Backend::Local { .. } => "in-process (no gateway)",
                };
                app.push(
                    Role::Info,
                    if resuming {
                        format!(
                            "{mode}, resumed session `{}`\nworkspace: `{}`",
                            app.session_id,
                            workspace.display(),
                        )
                    } else {
                        mode.to_string()
                    },
                );
                backend = Some(ready.backend);
                if let Some(text) = pending.take() {
                    spawn_turn(
                        backend.as_ref().expect("installed above"),
                        &cancels, &app.session_id, text,
                        &turn_tx, &event_tx, &sink_tx,
                    );
                }
            }
            // Live tool-call events (both backends): render each as an activity
            // line, updating the same line in place when it finishes. Kept above
            // `turn_rx` (under `biased`) so a turn's events drain before its reply.
            Some(event) = event_rx.recv() => {
                match event {
                    TurnEvent::ToolStarted { seq, name, args, .. } => {
                        app.tool_started(seq, name, args);
                    }
                    TurnEvent::ToolFinished { seq, name, ok, summary, .. } => {
                        app.tool_finished(seq, name, ok, summary);
                    }
                    // The model's answer as it is generated. Grows a live entry
                    // so a long round reads as progress instead of a hang.
                    TurnEvent::AssistantDelta { text } => {
                        app.stream_delta(&text);
                    }
                    // Reasoning is progress, not answer: counted for the status
                    // line, never rendered into the transcript.
                    TurnEvent::ReasoningDelta { text } => {
                        app.note_reasoning(&text);
                    }
                    // Mid-turn narration: the agent saying what it is about to
                    // do, and the authoritative version of whatever just
                    // streamed. Rendered as agent speech (which it is) ahead of
                    // the tool lines it explains; the turn's answer still
                    // arrives separately via `turn_rx`.
                    TurnEvent::AssistantText { text } => {
                        app.finish_stream(text);
                    }
                }
            }
            Some(result) = turn_rx.recv() => {
                app.finish_turn();
                // No tool is running once the turn is done.
                app.active_tool = None;
                // A turn that stopped on a question is not over: the input is
                // unlocked as its answer, and nothing is rendered — the
                // question itself already came through the sink.
                app.awaiting_answer = matches!(result, TurnEnd::Waiting);
                match result {
                    // The final round streamed this same text, so settle that
                    // live entry on the reply rather than appending a duplicate.
                    TurnEnd::Reply(reply) => app.finish_stream(reply),
                    TurnEnd::Waiting => {}
                    TurnEnd::Failed(error) => app.push(Role::Error, error),
                }
            }
            // Mid-turn agent message (local mode): the `ask_user` question, or
            // an approval prompt. Rendered as it arrives; whether the turn is
            // now waiting is answered by how it *ends* (above), not by this.
            Some(text) = sink_rx.recv() => {
                app.push(Role::Agent, text);
            }
            // Show one approval at a time; further prompts wait in the channel
            // until the current modal is answered.
            Some(prompt) = approval_rx.recv(), if app.modal.is_none() => {
                app.modal = Some(prompt);
            }
            _ = tick.tick() => {
                if app.in_flight || app.connecting {
                    app.spinner = app.spinner.wrapping_add(1);
                }
            }
        }

        // Terminal input. The batch is assembled here, outside the select, so a
        // paste that arrived as a burst of keystrokes can be collected and folded
        // back into one `Event::Paste` before anything is interpreted — otherwise
        // its first newline would submit the message.
        if batch.is_empty() {
            continue;
        }
        paste::drain_ready(&mut batch, &mut input_rx);
        if paste::should_extend(&batch) {
            paste::extend_for_paste(&mut batch, &mut input_rx).await;
        }
        for event in paste::coalesce_rapid_keys(batch) {
            if let Event::Paste(text) = &event {
                app.on_paste(text);
                continue;
            }
            let Event::Key(key) = event else { continue };
            // kitty-protocol terminals also send Release/Repeat.
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match app.on_key(key) {
                Some(Action::Quit) => break 'main,
                // `shown` folds pasted blocks to their chip label; `text` is
                // the full draft the agent receives.
                Some(Action::Submit { text, shown }) => {
                    app.push(Role::You, shown);
                    app.start_turn();
                    // Fresh tool feed for the new turn (seqs restart).
                    app.begin_tools();
                    match &backend {
                        Some(backend) => spawn_turn(
                            backend,
                            &cancels,
                            &app.session_id,
                            text,
                            &turn_tx,
                            &event_tx,
                            &sink_tx,
                        ),
                        // Still booting: hold the message (`in_flight` blocks a
                        // second one); the boot arm dispatches it on arrival.
                        None => pending = Some(text),
                    }
                }
                Some(Action::NewSession) => {
                    // The boot task ensures the fresh session's row under the
                    // id it was given; rotating before it lands would leave
                    // the first turn on a row nobody created.
                    let Some(backend) = &backend else {
                        app.push(Role::Info, "正在启动，稍候再 /new。".to_string());
                        continue;
                    };
                    // Turns are keyed by session id, so an in-flight turn
                    // for the old id can finish and render harmlessly. A
                    // question left standing stays answerable on its own
                    // session — `/new` ends the conversation, not the turn.
                    app.awaiting_answer = false;
                    app.session_id = new_session_id();
                    if let Backend::Local { db, .. } = backend {
                        ensure_session(db, &app.session_id, &workspace).await?;
                    }
                    app.push(
                        Role::Info,
                        format!("Started new session `{}`", app.session_id),
                    );
                }
                Some(Action::Answer { text, shown }) => {
                    let answered = match &backend {
                        Some(backend) => {
                            answer_question(
                                backend,
                                &cancels,
                                &app.session_id,
                                text,
                                &turn_tx,
                                &event_tx,
                                &sink_tx,
                            )
                            .await
                        }
                        None => false,
                    };
                    if answered {
                        app.push(Role::You, shown);
                        app.start_turn();
                        app.begin_tools();
                    } else {
                        app.push(Role::Info, "问题已失效（已被回答或已过期）。".to_string());
                    }
                }
                Some(Action::Interrupt) => {
                    // A turn suspended on a question is not running, so the
                    // cancel signal has nothing to interrupt: answering it with
                    // the stop text is what ends it. Same order the api
                    // channel's `cancel_turn` uses.
                    // Still booting: the only thing running is the queued
                    // draft, so Esc takes that back.
                    let Some(backend) = &backend else {
                        let message = if pending.take().is_some() {
                            app.finish_turn();
                            "已取消排队的消息。"
                        } else {
                            "没有正在运行的回合可中断。"
                        };
                        app.push(Role::Info, message.to_string());
                        continue;
                    };
                    let stopped = match backend {
                        Backend::Remote { gateway, .. } => {
                            gateway.cancel_turn(&app.session_id).await.unwrap_or(false)
                        }
                        Backend::Local { .. } => {
                            let answered = app.awaiting_answer
                                && answer_question(
                                    backend,
                                    &cancels,
                                    &app.session_id,
                                    CANCELLED_REPLY.to_string(),
                                    &turn_tx,
                                    &event_tx,
                                    &sink_tx,
                                )
                                .await;
                            app.awaiting_answer = false;
                            cancels.cancel(&app.session_id) || answered
                        }
                    };
                    // Said out loud either way: "nothing happened" and "stopping,
                    // give it a moment" look identical otherwise, and the turn
                    // ends at its *next* await, not instantly.
                    app.push(
                        Role::Info,
                        if stopped {
                            "正在中断…（工具调用可能要跑完当前这一步）".to_string()
                        } else {
                            "没有正在运行的回合可中断。".to_string()
                        },
                    );
                }
                Some(Action::Answered(_)) | None => {}
            }
        }
    }
    // No backend means no session was ever created or resumed — there is
    // nothing for the resume hint to point at.
    Ok(backend.is_some().then_some(app.session_id))
}

/// How a turn ended, as the loop has to render it.
enum TurnEnd {
    Reply(String),
    /// It stopped to wait for the user's answer (`ask_user`). Neither an answer
    /// nor a failure: the input unlocks, and what is typed continues this same
    /// turn.
    Waiting,
    Failed(String),
}

/// The interactive context a locally driven turn runs under: its sink carries
/// mid-turn messages (the `ask_user` question, an approval prompt) into the
/// transcript, and its cancel signal is what Esc flips.
fn local_ctx(
    session_id: &str,
    ticket: Option<&CancelTicket>,
    event_tx: &mpsc::UnboundedSender<TurnEvent>,
    sink_tx: &mpsc::UnboundedSender<String>,
) -> SessionContext {
    SessionContext {
        session_id: session_id.to_string(),
        workspace_root: None,
        sink: Arc::new(ChannelSink {
            tx: sink_tx.clone(),
        }),
        interactive: true,
        auto_approve: false,
        event_sink: Some(Arc::new(TuiEventSink {
            tx: event_tx.clone(),
        })),
        // Esc stops the turn: the loop flips this signal and the agent loop
        // gives up at its next await.
        cancel: ticket.map(|ticket| ticket.signal()),
        interject: None,
        channel: None,
        origin: SessionOrigin::User,
    }
}

/// Dispatch one turn onto its own task so the loop keeps handling keys. The
/// result lands on `turn_tx`, live tool events on `event_tx`, and — local mode
/// only — mid-turn agent messages on `sink_tx`.
fn spawn_turn(
    backend: &Backend,
    cancels: &Arc<CancelState>,
    session_id: &str,
    text: String,
    turn_tx: &mpsc::UnboundedSender<TurnEnd>,
    event_tx: &mpsc::UnboundedSender<TurnEvent>,
    sink_tx: &mpsc::UnboundedSender<String>,
) {
    let session_id = session_id.to_string();
    let turn_tx = turn_tx.clone();
    let events = event_tx.clone();
    // One registration per local turn, retired when the turn's task ends —
    // taken only where the context is, so an Esc still reports "nothing to
    // stop" on a backend that has no interruptible turn.
    let local = matches!(backend, Backend::Local { .. });
    let ticket = local.then(|| cancels.register(&session_id));
    let ctx = local.then(|| local_ctx(&session_id, ticket.as_ref(), event_tx, sink_tx));
    let backend = backend.clone();
    tokio::spawn(async move {
        let result = classify_end(backend.turn(&session_id, text, ctx, events).await);
        // Retire the slot so a later Esc can't hit a finished turn.
        drop(ticket);
        let _ = turn_tx.send(result);
    });
}

/// Answer the question a locally suspended turn is waiting on, and continue it.
///
/// The TUI drives its own turns, so it continues them itself — but the log
/// writes go through [`record_wake`], the one writer of `wakeup/fired`.
/// Without a gateway there is no sweep either, so this is the only wake a local
/// session can get: a question can be answered here and now, while a `wait 2h`
/// comes back whenever a gateway next runs.
///
/// Answers whether anything was actually waiting.
async fn answer_question(
    backend: &Backend,
    cancels: &Arc<CancelState>,
    session_id: &str,
    text: String,
    turn_tx: &mpsc::UnboundedSender<TurnEnd>,
    event_tx: &mpsc::UnboundedSender<TurnEvent>,
    sink_tx: &mpsc::UnboundedSender<String>,
) -> bool {
    let Backend::Local { runtime, db } = backend else {
        return false;
    };
    let waits = WaitParts {
        runs: db.clone(),
        events: db.clone(),
        wakeups: db.clone(),
    };
    let Ok(registrations) = waits.wakeups.list().await else {
        return false;
    };
    let Some(registration) = registrations
        .into_iter()
        .find(|r| r.session_id == session_id && r.turn_id.is_some() && is_user_reply(&r.wakeup))
    else {
        return false;
    };
    let turn_id = registration.turn_id.clone().expect("filtered above");
    // Claim it before writing anything: a gateway started meanwhile could be
    // reaching for the same wait with an expiry.
    if !matches!(waits.wakeups.take(&registration.id).await, Ok(true)) {
        return false;
    }
    let cause = match text.trim().is_empty() {
        true => WakeupCause::MovedOn,
        false => WakeupCause::Reply,
    };
    if let Err(error) = record_wake(&waits, &registration, &turn_id, cause, &text).await {
        tracing::warn!(%error, "failed to record the answer to a question");
        return false;
    }
    let Ok(Some(run)) = waits.runs.get(&turn_id).await else {
        return false;
    };

    let runtime = runtime.clone();
    let session_id = session_id.to_string();
    let turn_tx = turn_tx.clone();
    let ticket = cancels.register(&session_id);
    let ctx = local_ctx(&session_id, Some(&ticket), event_tx, sink_tx);
    tokio::spawn(async move {
        let outcome = with_session(ctx, runtime.resume_interrupted(&run)).await;
        drop(ticket);
        let result = match outcome {
            Ok(Some(reply)) => TurnEnd::Reply(reply),
            // Not continuable: its transcript already ends in a reply, or the
            // log has nothing for it.
            Ok(None) => TurnEnd::Failed("那一轮已经结束了，无法继续。".to_string()),
            Err(error) => classify_end(Err(error)),
        };
        let _ = turn_tx.send(result);
    });
    true
}

/// Read a turn's outcome. Classified **before** the error is stringified:
/// `is_cancelled` / `is_suspended` downcast, and `{e:#}` would leave a
/// deliberate stop — or a turn that is merely waiting — looking like a failure.
fn classify_end(outcome: anyhow::Result<String>) -> TurnEnd {
    match outcome {
        Ok(reply) => TurnEnd::Reply(reply),
        // The remote arm never lands here — the gateway already answers a
        // cancelled turn with this same text.
        Err(error) if is_cancelled(&error) => TurnEnd::Reply(CANCELLED_REPLY.to_string()),
        Err(error) if is_suspended(&error) => TurnEnd::Waiting,
        Err(error) => TurnEnd::Failed(format!("{error:#}")),
    }
}

async fn ensure_session(db: &Db, session_id: &str, workspace: &PathBuf) -> anyhow::Result<()> {
    if SessionRepository::find(db, session_id).await?.is_none() {
        let workspace_id = folder_workspace_id(workspace)?;
        SessionRepository::save(db, &Session::with_workspace(session_id, workspace_id)).await?;
    }
    Ok(())
}

fn new_session_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Snapshot the TUI's startup folder once. A session's workspace is immutable,
/// so later `cd`s in child shells must not redirect a conversation's tools.
fn startup_workspace() -> anyhow::Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    cwd.canonicalize().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::new_session_id;

    #[test]
    fn a_new_session_id_is_a_bare_uuid() {
        // The id the TUI prints in its `komo resume` hint is the id the gateway
        // stores and the id the header carries — one form, nothing to strip.
        let id = new_session_id();
        assert!(uuid::Uuid::parse_str(&id).is_ok(), "{id}");
    }
}
