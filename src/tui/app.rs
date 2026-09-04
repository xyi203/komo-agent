//! TUI state + key handling, kept free of terminal I/O so it is unit-testable.
//! The event loop (`mod.rs`) feeds key events in and interprets the returned
//! [`Action`]s; rendering (`ui.rs`) reads the state.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use komo_core::domain::awaiting::Awaiting;

use super::approver::{Answer, ApprovalPrompt};
use super::paste;

/// A pasted block the composer folds to a one-line label. Chips never overlap and
/// stay ordered, so the composer can treat one as a single atomic glyph — grok
/// build's `KIND_PASTE` element, without the general element machinery.
///
/// The range is carried twice on purpose: `range` is in **chars** (the unit
/// `cursor` uses), `bytes` in **bytes**, which is what lets the renderer *slice
/// past* a folded block instead of walking it. Without that, every frame would
/// still traverse a megabyte paste character by character — exactly the cost the
/// chip exists to avoid.
#[derive(Debug, Clone)]
pub struct PasteChip {
    pub range: std::ops::Range<usize>,
    pub bytes: std::ops::Range<usize>,
    pub label: String,
}

/// Who a transcript entry belongs to (drives the prefix + styling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    You,
    Agent,
    /// System notices (session started, …).
    Info,
    Error,
    /// A tool-call activity line (fed by the turn's live [`TurnEvent`] stream).
    /// Its glyph/color comes from [`Entry::tool_ok`], not a fixed prefix.
    ///
    /// [`TurnEvent`]: komo_core::domain::events::TurnEvent
    Tool,
}

pub struct Entry {
    pub role: Role,
    pub text: String,
    /// For [`Role::Tool`] entries: `None` while the call is running, `Some(true)`
    /// on success, `Some(false)` on failure. `None` (unused) for every other role.
    pub tool_ok: Option<bool>,
}

/// What the event loop should do in response to a key, beyond the state
/// mutation already applied.
#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    /// Run a turn with this input. `text` is the full draft (what the agent
    /// gets); `shown` is the same draft with pasted blocks folded to their chip
    /// labels — what belongs in the transcript.
    Submit {
        text: String,
        shown: String,
    },
    /// Start a fresh session (`/new` / `/clear`).
    NewSession,
    /// The user answered the approval modal.
    Answered(Answer),
    /// The user answered a mid-turn `ask_user` question (local mode): resolve
    /// it into the suspended turn instead of starting a new one. Same
    /// `text`/`shown` split as [`Action::Submit`].
    Answer {
        text: String,
        shown: String,
    },
    /// Stop the turn in flight (Esc). Only produced while one *is* in flight —
    /// see [`App::on_key`].
    Interrupt,
    Quit,
}

pub struct App {
    pub session_id: String,
    pub entries: Vec<Entry>,
    pub input: String,
    /// Cursor as a char index into `input`.
    pub cursor: usize,
    /// Folded paste blocks inside `input`, ordered and non-overlapping. Display
    /// state only: `input` always holds the full text.
    pub chips: Vec<PasteChip>,
    /// Scroll offset in wrapped lines from the bottom; 0 = follow the tail.
    pub scroll_from_bottom: u16,
    pub in_flight: bool,
    /// The backend is still booting in the background (the UI paints before
    /// connect finishes). Drafting works; a submission is queued by the event
    /// loop and dispatched when the backend lands.
    pub connecting: bool,
    /// Monotonic start time for the active turn. Kept in the UI state so the
    /// status row can show useful progress without depending on wall-clock
    /// time (or being affected by a system clock adjustment).
    turn_started_at: Option<Instant>,
    /// A mid-turn `ask_user` question is pending: the next submit is its
    /// answer (allowed through even though a turn is in flight).
    pub awaiting_answer: bool,
    /// The wait a *suspended* turn of this session is stopped in, read from the
    /// session projection when a conversation is resumed. Unlike
    /// [`awaiting_answer`](Self::awaiting_answer) this outlives the process that
    /// asked: the turn is parked in the log, not in a channel this UI holds.
    pub awaiting: Option<Awaiting>,
    pub spinner: usize,
    pub modal: Option<ApprovalPrompt>,
    /// Set while the modal is collecting a *reason* for a denial (the user
    /// pressed `n`): a one-line buffer whose content is handed to the agent so
    /// it can correct the call instead of retrying it. `None` = the modal is in
    /// its normal key-per-answer mode.
    pub modal_reason: Option<String>,
    /// Maps a running tool's turn sequence → its transcript entry index, so a
    /// `ToolFinished` can update the same line in place. Reset each turn (seqs
    /// restart per turn); `-1` (un-ledgered) calls are not tracked here.
    tool_index: HashMap<i64, usize>,
    /// Name of the tool currently running, shown in the status line. `None`
    /// when nothing is mid-call.
    pub active_tool: Option<String>,
    /// Index of the agent entry currently being streamed into, if any. Deltas
    /// append to it; the authoritative end-of-round text replaces it
    /// ([`finish_stream`](Self::finish_stream)), which is what keeps a streamed
    /// answer from being rendered twice.
    streaming: Option<usize>,
    /// Characters of reasoning the model has streamed this round. Shown in the
    /// status line so a long think reads as progress rather than a hang — the
    /// reasoning text itself is deliberately not rendered into the transcript
    /// (it is not part of the answer and is never persisted).
    pub reasoning_chars: usize,
}

impl App {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            entries: Vec::new(),
            input: String::new(),
            cursor: 0,
            chips: Vec::new(),
            scroll_from_bottom: 0,
            in_flight: false,
            connecting: false,
            turn_started_at: None,
            awaiting_answer: false,
            awaiting: None,
            spinner: 0,
            modal: None,
            modal_reason: None,
            tool_index: HashMap::new(),
            active_tool: None,
            streaming: None,
            reasoning_chars: 0,
        }
    }

    pub fn push(&mut self, role: Role, text: impl Into<String>) {
        self.entries.push(Entry {
            role,
            text: text.into(),
            tool_ok: None,
        });
        // New content: snap back to following the tail.
        self.scroll_from_bottom = 0;
    }

    /// Reset the per-turn tool state (call when a new turn is submitted): a new
    /// turn's sequence counter restarts, so stale seq→index mappings must go.
    pub fn begin_tools(&mut self) {
        self.tool_index.clear();
        self.active_tool = None;
        self.streaming = None;
        self.reasoning_chars = 0;
    }

    /// Append a streamed chunk of the agent's answer, starting a live entry if
    /// this is the round's first chunk.
    ///
    /// Empty chunks are ignored rather than creating an entry: providers do emit
    /// zero-length deltas, and one would otherwise leave a blank agent bubble in
    /// the transcript.
    pub fn stream_delta(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        match self.streaming {
            Some(at) => self.entries[at].text.push_str(text),
            None => {
                self.push(Role::Agent, text);
                self.streaming = Some(self.entries.len() - 1);
            }
        }
        self.scroll_from_bottom = 0;
    }

    /// Settle a streamed round on its authoritative text.
    ///
    /// The stream and the final text can legitimately differ — the runtime
    /// substitutes a fallback for an empty reply, and prefixes a note when a
    /// turn stopped early — so the finished text *replaces* what streamed rather
    /// than being appended to it. With nothing streamed (an older gateway, or a
    /// round with no visible output) this is just a push.
    pub fn finish_stream(&mut self, text: impl Into<String>) {
        let text = text.into();
        match self.streaming.take() {
            Some(at) => {
                if text.trim().is_empty() {
                    // Nothing authoritative to show: keep what streamed.
                    return;
                }
                self.entries[at].text = text;
            }
            None => self.push(Role::Agent, text),
        }
        self.reasoning_chars = 0;
        self.scroll_from_bottom = 0;
    }

    /// Note streamed reasoning. Counted for the status line, not rendered: it is
    /// work in progress, not the answer.
    pub fn note_reasoning(&mut self, text: &str) {
        self.reasoning_chars += text.chars().count();
    }

    /// Mark a turn as running and start its elapsed-time counter.
    pub fn start_turn(&mut self) {
        self.in_flight = true;
        self.turn_started_at = Some(Instant::now());
        // Saying something else *is* the answer to a pending wait (the gateway
        // resolves it as moved-on), so the badge goes with the message.
        self.awaiting = None;
    }

    /// Mark a turn as complete and clear its elapsed-time counter.
    pub fn finish_turn(&mut self) {
        self.in_flight = false;
        self.turn_started_at = None;
    }

    /// Elapsed time for the current turn, if it was started through the UI.
    pub fn turn_elapsed(&self) -> Option<Duration> {
        self.turn_started_at.map(|started| started.elapsed())
    }

    /// A tool call started: append a running activity line and remember it so
    /// [`tool_finished`](Self::tool_finished) can update it in place.
    pub fn tool_started(&mut self, seq: i64, name: String, args: String) {
        let args = preview(&args, 100);
        let text = if args.is_empty() {
            name.clone()
        } else {
            format!("{name}  {args}")
        };
        self.entries.push(Entry {
            role: Role::Tool,
            text,
            tool_ok: None,
        });
        if seq >= 0 {
            self.tool_index.insert(seq, self.entries.len() - 1);
        }
        self.active_tool = Some(name);
        self.scroll_from_bottom = 0;
    }

    /// A tool call finished: mark its line ✓/✗ with a result preview, updating
    /// the running line in place when the seq is known, else appending one.
    pub fn tool_finished(&mut self, seq: i64, name: String, ok: bool, summary: String) {
        let summary = preview(&summary, 120);
        let text = if summary.is_empty() {
            name.clone()
        } else {
            format!("{name}  {summary}")
        };
        match self
            .tool_index
            .remove(&seq)
            .filter(|&i| i < self.entries.len())
        {
            Some(i) => {
                self.entries[i].text = text;
                self.entries[i].tool_ok = Some(ok);
            }
            None => self.entries.push(Entry {
                role: Role::Tool,
                text,
                tool_ok: Some(ok),
            }),
        }
        // Only the tracked run is "active"; a later start may have replaced it,
        // but on finish we clear the indicator (the next start re-sets it).
        self.active_tool = None;
        self.scroll_from_bottom = 0;
    }

    /// Close the approval modal, deliver `answer` to the waiting approver, and
    /// report it to the event loop.
    fn resolve_modal(&mut self, answer: Answer) -> Option<Action> {
        self.modal_reason = None;
        if let Some(mut prompt) = self.modal.take()
            && let Some(reply) = prompt.reply.take()
        {
            let _ = reply.send(answer.clone());
        }
        Some(Action::Answered(answer))
    }

    /// Handle one key press. Mutates the state and returns the action (if any)
    /// the event loop must carry out.
    pub fn on_key(&mut self, key: KeyEvent) -> Option<Action> {
        // The approval modal captures the keyboard while shown.
        if self.modal.is_some() {
            // Sub-mode: `n` opened a one-line "why?" prompt. Enter sends the
            // reason with the denial (empty = a plain denial); Esc bails out.
            if let Some(reason) = self.modal_reason.as_mut() {
                match key.code {
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return Some(Action::Quit);
                    }
                    KeyCode::Enter => {
                        let text = reason.trim().to_string();
                        return self
                            .resolve_modal(Answer::Deny((!text.is_empty()).then_some(text)));
                    }
                    KeyCode::Esc => return self.resolve_modal(Answer::Deny(None)),
                    KeyCode::Backspace => {
                        reason.pop();
                    }
                    KeyCode::Char(c) => reason.push(c),
                    _ => {}
                }
                return None;
            }

            let answer = match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => Some(Answer::Once),
                KeyCode::Char('s') | KeyCode::Char('S') => Some(Answer::Session),
                // Offered only when the modal showed the rule it would save.
                KeyCode::Char('a') | KeyCode::Char('A')
                    if self.modal.as_ref().is_some_and(|m| m.always_rule.is_some()) =>
                {
                    Some(Answer::Always)
                }
                // `n` asks for a reason first (one extra keystroke); Esc is the
                // immediate, explanation-free denial.
                KeyCode::Char('n') | KeyCode::Char('N') => {
                    self.modal_reason = Some(String::new());
                    return None;
                }
                KeyCode::Esc => Some(Answer::Deny(None)),
                // Ctrl-C still quits even under a modal (the dropped reply
                // reads as a denial on the approver side).
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Some(Action::Quit);
                }
                _ => None,
            };
            if let Some(answer) = answer {
                return self.resolve_modal(answer);
            }
            return None;
        }

        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::Quit)
            }
            // Esc interrupts the turn in flight, and does nothing at all when
            // idle. Deliberately *not* "clear the input" when idle: the whole
            // point of a stop key is that it can be hit without thinking, and a
            // key that sometimes discards the draft instead is worse than one
            // extra keystroke. (Under the approval modal Esc still means "deny"
            // — that branch returned above; a turn parked on a prompt is not
            // going anywhere until the prompt is answered anyway.)
            KeyCode::Esc if self.in_flight => Some(Action::Interrupt),
            // Ctrl-D quits only on an empty input, shell-style.
            KeyCode::Char('d')
                if key.modifiers.contains(KeyModifiers::CONTROL) && self.input.is_empty() =>
            {
                Some(Action::Quit)
            }
            // Newline instead of send. Shift/Alt-Enter needs a terminal that
            // reports the modifier (kitty keyboard protocol, pushed in `drive`);
            // Ctrl-J is the fallback everywhere else.
            KeyCode::Enter
                if key
                    .modifiers
                    .intersects(KeyModifiers::SHIFT | KeyModifiers::ALT) =>
            {
                self.insert_char('\n');
                None
            }
            KeyCode::Char('j') if key.modifiers == KeyModifiers::CONTROL => {
                self.insert_char('\n');
                None
            }
            KeyCode::Enter => {
                let text = self.input.trim().to_string();
                if text.is_empty() {
                    return None;
                }
                if text == "/new" || text == "/clear" {
                    self.clear_input();
                    return Some(Action::NewSession);
                }
                // The transcript shows the draft as it looked — pasted blocks
                // stay folded to their chip label. The agent gets `text`, which
                // is always the full content.
                let shown = self.folded_input();
                if self.in_flight {
                    // A pending clarify question lets the input through as its
                    // answer — the suspended turn continues with it.
                    if self.awaiting_answer {
                        self.clear_input();
                        self.awaiting_answer = false;
                        return Some(Action::Answer { text, shown });
                    }
                    // One turn at a time; keep the draft so nothing is lost.
                    return None;
                }
                self.clear_input();
                Some(Action::Submit { text, shown })
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.insert_char(c);
                None
            }
            // A chip deletes whole: the cursor is never inside a pasted block,
            // so one Backspace behind it removes the paste rather than shaving a
            // character off content the user cannot see.
            KeyCode::Backspace => {
                match self.chip_ending_at(self.cursor) {
                    Some(i) => self.delete_chars(self.chips[i].range.clone()),
                    None if self.cursor > 0 => self.delete_chars(self.cursor - 1..self.cursor),
                    None => {}
                }
                None
            }
            KeyCode::Delete => {
                match self.chip_starting_at(self.cursor) {
                    Some(i) => self.delete_chars(self.chips[i].range.clone()),
                    None if self.cursor < self.input.chars().count() => {
                        self.delete_chars(self.cursor..self.cursor + 1)
                    }
                    None => {}
                }
                None
            }
            // Chips are atomic to the cursor too: stepping over one lands on the
            // far side instead of inside the hidden text.
            KeyCode::Left => {
                self.cursor = match self.chip_ending_at(self.cursor) {
                    Some(i) => self.chips[i].range.start,
                    None => self.cursor.saturating_sub(1),
                };
                None
            }
            KeyCode::Right => {
                self.cursor = match self.chip_starting_at(self.cursor) {
                    Some(i) => self.chips[i].range.end,
                    None => (self.cursor + 1).min(self.input.chars().count()),
                };
                None
            }
            KeyCode::Home => {
                self.cursor = 0;
                None
            }
            KeyCode::End => {
                self.cursor = self.input.chars().count();
                None
            }
            KeyCode::Up => {
                self.scroll_from_bottom = self.scroll_from_bottom.saturating_add(1);
                None
            }
            KeyCode::Down => {
                self.scroll_from_bottom = self.scroll_from_bottom.saturating_sub(1);
                None
            }
            KeyCode::PageUp => {
                self.scroll_from_bottom = self.scroll_from_bottom.saturating_add(10);
                None
            }
            KeyCode::PageDown => {
                self.scroll_from_bottom = self.scroll_from_bottom.saturating_sub(10);
                None
            }
            _ => None,
        }
    }

    /// Byte offset of the char cursor (input is UTF-8; CJK chars are multibyte).
    fn byte_cursor(&self) -> usize {
        self.byte_at(self.cursor)
    }

    fn clear_input(&mut self) {
        self.input.clear();
        self.cursor = 0;
        self.chips.clear();
    }

    fn insert_char(&mut self, c: char) {
        let at = self.byte_cursor();
        self.input.insert(at, c);
        self.shift_chips(self.cursor, 1, c.len_utf8() as isize);
        self.cursor += 1;
    }

    /// Bracketed paste (or a burst of keystrokes a terminal without bracketed
    /// paste turned into one — see `paste::coalesce_rapid_keys`): the clipboard
    /// text lands in the draft verbatim, newlines included, so a multi-line paste
    /// can never fire a send per line.
    ///
    /// A paste past the chip threshold is *displayed* as a one-line label. The
    /// draft still holds every character, so submitting needs no expansion — the
    /// fold is purely how it renders, which is also what keeps a megabyte paste
    /// from being re-wrapped on every frame.
    pub fn on_paste(&mut self, text: &str) {
        // The approval modal owns the keyboard; a stray paste must not leak into
        // the draft behind it.
        if self.modal.is_some() {
            return;
        }
        let text = paste::normalize_cr(text);
        if text.is_empty() {
            return;
        }

        // Repaste-to-expand, from grok build: "paste didn't do what I want?
        // paste again." Pasting a chip's exact content with the cursor on it
        // unfolds that chip instead of inserting a second copy.
        if let Some(i) = self
            .chip_ending_at(self.cursor)
            .or_else(|| self.chip_starting_at(self.cursor))
            && self.chip_text(i) == text
        {
            let chip = self.chips.remove(i);
            self.cursor = chip.range.end;
            return;
        }

        let at = self.byte_cursor();
        self.input.insert_str(at, &text);
        let len = text.chars().count();
        self.shift_chips(self.cursor, len as isize, text.len() as isize);
        if paste::is_chip_worthy(&text) {
            let chip = PasteChip {
                range: self.cursor..self.cursor + len,
                bytes: at..at + text.len(),
                label: paste::chip_label(&text),
            };
            let index = self
                .chips
                .partition_point(|c| c.range.start < chip.range.start);
            self.chips.insert(index, chip);
        }
        self.cursor += len;
    }

    /// The draft as it is shown: every chip range replaced by its label. Used for
    /// the transcript entry when the draft is sent (the agent gets `input`).
    fn folded_input(&self) -> String {
        if self.chips.is_empty() {
            return self.input.trim().to_string();
        }
        let mut out = String::new();
        let mut at = 0usize;
        for chip in &self.chips {
            out.extend(self.input.chars().skip(at).take(chip.range.start - at));
            out.push_str(&chip.label);
            at = chip.range.end;
        }
        out.extend(self.input.chars().skip(at));
        out.trim().to_string()
    }

    /// Index of the chip that ends exactly at `at` (the cursor sits behind it).
    fn chip_ending_at(&self, at: usize) -> Option<usize> {
        self.chips.iter().position(|c| c.range.end == at)
    }

    /// Index of the chip that starts exactly at `at` (the cursor sits in front).
    fn chip_starting_at(&self, at: usize) -> Option<usize> {
        self.chips.iter().position(|c| c.range.start == at)
    }

    fn chip_text(&self, i: usize) -> &str {
        &self.input[self.chips[i].bytes.clone()]
    }

    /// Move every chip starting at or after `at` (a char index) by `delta` chars
    /// / `delta_bytes` bytes — the draft changed length in front of them.
    fn shift_chips(&mut self, at: usize, delta: isize, delta_bytes: isize) {
        for chip in &mut self.chips {
            if chip.range.start >= at {
                chip.range.start = chip.range.start.saturating_add_signed(delta);
                chip.range.end = chip.range.end.saturating_add_signed(delta);
                chip.bytes.start = chip.bytes.start.saturating_add_signed(delta_bytes);
                chip.bytes.end = chip.bytes.end.saturating_add_signed(delta_bytes);
            }
        }
    }

    /// Delete a char range from the draft, dropping any chip it covers and
    /// pulling the later chips back. The cursor lands where the text was.
    fn delete_chars(&mut self, range: std::ops::Range<usize>) {
        let (start, end) = (self.byte_at(range.start), self.byte_at(range.end));
        self.input.replace_range(start..end, "");
        self.chips
            .retain(|c| !(c.range.start >= range.start && c.range.end <= range.end));
        self.shift_chips(
            range.end,
            -((range.end - range.start) as isize),
            -((end - start) as isize),
        );
        self.cursor = range.start;
    }

    /// Byte offset of char index `at` (the end of the string when past it).
    fn byte_at(&self, at: usize) -> usize {
        self.input
            .char_indices()
            .nth(at)
            .map(|(i, _)| i)
            .unwrap_or(self.input.len())
    }
}

/// Collapse a (possibly multi-line, possibly long) tool arg/result into a
/// single tidy line for the activity feed: newlines/tabs → spaces, runs of
/// whitespace squeezed, then truncated to `max` display chars with an ellipsis.
fn preview(s: &str, max: usize) -> String {
    let mut out = String::with_capacity(s.len().min(max + 1));
    let mut last_space = false;
    for c in s.chars() {
        let c = if c.is_whitespace() { ' ' } else { c };
        if c == ' ' {
            if last_space || out.is_empty() {
                continue;
            }
            last_space = true;
        } else {
            last_space = false;
        }
        out.push(c);
    }
    let trimmed = out.trim_end();
    if trimmed.chars().count() > max {
        let kept: String = trimmed.chars().take(max).collect();
        format!("{kept}…")
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    /// The failure this reconciliation exists to prevent: the final round
    /// streams its answer *and* delivers it again as the reply, so a naive
    /// implementation renders it twice.
    #[test]
    fn a_streamed_answer_is_not_rendered_twice() {
        let mut app = App::new("s".into());
        app.begin_tools();
        for chunk in ["Hel", "lo ", "there"] {
            app.stream_delta(chunk);
        }
        assert_eq!(app.entries.len(), 1, "one growing entry, not one per chunk");
        assert_eq!(app.entries[0].text, "Hello there");

        // The reply arrives carrying the same text.
        app.finish_stream("Hello there");
        assert_eq!(app.entries.len(), 1, "the reply settles the live entry");
        assert_eq!(app.entries[0].text, "Hello there");
    }

    /// The stream and the final text legitimately differ — the runtime prefixes
    /// a note when a turn stopped early — so the authoritative text wins.
    #[test]
    fn the_authoritative_text_replaces_what_streamed() {
        let mut app = App::new("s".into());
        app.stream_delta("partial thought");
        app.finish_stream("(Reached the tool-call limit.) partial thought");
        assert_eq!(app.entries.len(), 1);
        assert_eq!(
            app.entries[0].text,
            "(Reached the tool-call limit.) partial thought"
        );
    }

    /// With no deltas (an older gateway, or a round that only called tools) the
    /// reply still has to land.
    #[test]
    fn a_reply_with_nothing_streamed_is_pushed() {
        let mut app = App::new("s".into());
        app.finish_stream("the answer");
        assert_eq!(app.entries.len(), 1);
        assert_eq!(app.entries[0].text, "the answer");
        assert!(matches!(app.entries[0].role, Role::Agent));
    }

    /// Each round gets its own entry: narration streamed before a tool call must
    /// not be overwritten by the next round's stream.
    #[test]
    fn a_new_round_streams_into_a_new_entry() {
        let mut app = App::new("s".into());
        app.stream_delta("checking the config");
        app.finish_stream("checking the config");
        app.stream_delta("found it");
        app.finish_stream("found it");
        assert_eq!(
            app.entries
                .iter()
                .map(|e| e.text.as_str())
                .collect::<Vec<_>>(),
            vec!["checking the config", "found it"]
        );
    }

    #[test]
    fn empty_deltas_never_create_a_blank_bubble() {
        let mut app = App::new("s".into());
        app.stream_delta("");
        assert!(app.entries.is_empty(), "providers do emit empty deltas");
    }

    /// An empty reply must not blank out text the user already watched arrive.
    #[test]
    fn an_empty_reply_keeps_what_streamed() {
        let mut app = App::new("s".into());
        app.stream_delta("real content");
        app.finish_stream("   ");
        assert_eq!(app.entries[0].text, "real content");
    }

    #[test]
    fn reasoning_is_counted_but_never_rendered() {
        let mut app = App::new("s".into());
        app.note_reasoning("думаю");
        app.note_reasoning("字");
        assert!(app.entries.is_empty(), "reasoning is progress, not answer");
        assert_eq!(app.reasoning_chars, 6, "counted in chars, not bytes");
        // A finished round resets the counter for the next one.
        app.finish_stream("done");
        assert_eq!(app.reasoning_chars, 0);
    }

    fn type_str(app: &mut App, s: &str) {
        for c in s.chars() {
            app.on_key(key(KeyCode::Char(c)));
        }
    }

    /// Type into the modal's denial-reason line (same keys, different target).
    fn type_reason(app: &mut App, s: &str) {
        assert!(app.modal_reason.is_some(), "not in reason-entry mode");
        type_str(app, s);
    }

    #[test]
    fn shift_enter_and_ctrl_j_insert_a_newline_instead_of_sending() {
        let mut app = App::new("s".into());
        type_str(&mut app, "one");
        assert_eq!(
            app.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT)),
            None,
            "Shift-Enter must not submit"
        );
        type_str(&mut app, "two");
        assert_eq!(app.on_key(ctrl('j')), None, "Ctrl-J must not submit");
        type_str(&mut app, "three");
        assert_eq!(app.input, "one\ntwo\nthree");
        assert_eq!(app.cursor, app.input.chars().count());
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Action::Submit {
                text: "one\ntwo\nthree".into(),
                shown: "one\ntwo\nthree".into(),
            }),
            "a bare Enter still sends the whole draft"
        );
    }

    #[test]
    fn paste_keeps_newlines_and_never_submits() {
        let mut app = App::new("s".into());
        type_str(&mut app, "x");
        app.on_paste("first\r\nsecond\rthird");
        assert_eq!(app.input, "xfirst\nsecond\nthird");
        assert_eq!(app.cursor, app.input.chars().count());
        assert!(app.chips.is_empty(), "3 lines stays inline");
        // Nothing was submitted: the draft is still there for the user to edit.
        assert!(!app.in_flight);
    }

    #[test]
    fn a_big_paste_folds_to_a_chip_but_sends_in_full() {
        let mut app = App::new("s".into());
        type_str(&mut app, "look: ");
        let pasted = "one\ntwo\nthree\nfour";
        app.on_paste(pasted);

        assert_eq!(
            app.input,
            format!("look: {pasted}"),
            "draft keeps every char"
        );
        assert_eq!(app.chips.len(), 1);
        assert_eq!(app.chips[0].label, "[Pasted: 4 lines]");
        assert_eq!(app.chips[0].range, 6..6 + pasted.chars().count());
        assert_eq!(app.cursor, app.input.chars().count());

        let action = app.on_key(key(KeyCode::Enter));
        assert_eq!(
            action,
            Some(Action::Submit {
                text: format!("look: {pasted}"),
                shown: "look: [Pasted: 4 lines]".into(),
            }),
            "the agent gets the full text; the transcript shows the chip"
        );
        assert!(app.chips.is_empty(), "chips die with the draft");
    }

    #[test]
    fn backspace_removes_a_whole_chip() {
        let mut app = App::new("s".into());
        app.on_paste("one\ntwo\nthree\nfour");
        type_str(&mut app, "!");
        assert_eq!(app.chips.len(), 1);

        // One Backspace takes the typed char, the next takes the entire paste —
        // never a single character out of text the user cannot see.
        app.on_key(key(KeyCode::Backspace));
        assert_eq!(app.chips.len(), 1, "still folded");
        app.on_key(key(KeyCode::Backspace));
        assert!(
            app.input.is_empty(),
            "the paste went whole: {:?}",
            app.input
        );
        assert!(app.chips.is_empty());
        assert_eq!(app.cursor, 0);
    }

    #[test]
    fn arrows_step_over_a_chip_instead_of_into_it() {
        let mut app = App::new("s".into());
        let pasted = "one\ntwo\nthree\nfour";
        app.on_paste(pasted);
        type_str(&mut app, "ab");
        let end = app.input.chars().count();

        app.on_key(key(KeyCode::Left));
        assert_eq!(app.cursor, end - 1, "inside plain text, one char at a time");
        app.on_key(key(KeyCode::Left));
        assert_eq!(app.cursor, pasted.chars().count(), "now just past the chip");
        app.on_key(key(KeyCode::Left));
        assert_eq!(app.cursor, 0, "one step clears the whole chip");
        app.on_key(key(KeyCode::Right));
        assert_eq!(app.cursor, pasted.chars().count(), "and back over it");
    }

    #[test]
    fn repasting_the_same_content_expands_the_chip() {
        let mut app = App::new("s".into());
        let pasted = "one\ntwo\nthree\nfour";
        app.on_paste(pasted);
        assert_eq!(app.chips.len(), 1);

        // grok build's "paste didn't do what I want? paste again" — the second
        // paste unfolds the block instead of inserting a second copy.
        app.on_paste(pasted);
        assert_eq!(app.input, pasted, "content is unchanged, not doubled");
        assert!(app.chips.is_empty(), "now shown inline");
        assert_eq!(app.cursor, pasted.chars().count());
    }

    #[test]
    fn chip_offsets_survive_editing_in_front_of_it() {
        let mut app = App::new("s".into());
        let pasted = "one\ntwo\nthree\nfour";
        app.on_paste(pasted);
        app.cursor = 0;
        // A multibyte char in front of the chip must move both its char range and
        // its byte range, or the renderer slices the wrong bytes.
        type_str(&mut app, "中");
        assert_eq!(app.chips[0].range, 1..1 + pasted.chars().count());
        assert_eq!(app.chips[0].bytes, 3.."中".len() + pasted.len());
        assert_eq!(&app.input[app.chips[0].bytes.clone()], pasted);
    }

    #[test]
    fn tool_line_updates_in_place_on_finish() {
        let mut app = App::new("s".into());
        app.begin_tools();
        app.tool_started(1, "shell".into(), "ls -la /very/long/path".into());
        assert_eq!(app.entries.len(), 1);
        assert_eq!(app.entries[0].role, Role::Tool);
        assert_eq!(app.entries[0].tool_ok, None);
        assert_eq!(app.active_tool.as_deref(), Some("shell"));

        // Finishing the same seq updates the existing line, not a new one.
        app.tool_finished(1, "shell".into(), true, "total 42\nfoo".into());
        assert_eq!(app.entries.len(), 1, "updated in place, no new entry");
        assert_eq!(app.entries[0].tool_ok, Some(true));
        assert!(app.entries[0].text.contains("total 42"));
        assert!(
            app.entries[0].text.contains("foo"),
            "newline collapsed inline"
        );
        assert_eq!(app.active_tool, None);

        // A new turn resets tracking so a reused seq starts a fresh line.
        app.begin_tools();
        app.tool_started(1, "web_fetch".into(), String::new());
        app.tool_finished(1, "web_fetch".into(), false, "404".into());
        assert_eq!(app.entries.len(), 2);
        assert_eq!(app.entries[1].tool_ok, Some(false));
    }

    #[test]
    fn preview_collapses_whitespace_and_truncates() {
        assert_eq!(preview("a\n\n  b\tc", 100), "a b c");
        let long = "x".repeat(200);
        let p = preview(&long, 10);
        assert_eq!(p.chars().count(), 11); // 10 + ellipsis
        assert!(p.ends_with('…'));
    }

    #[test]
    fn typing_and_multibyte_editing_keep_utf8_boundaries() {
        let mut app = App::new("s".into());
        type_str(&mut app, "你好a");
        assert_eq!(app.input, "你好a");
        // Backspace removes whole chars, not bytes.
        app.on_key(key(KeyCode::Backspace));
        app.on_key(key(KeyCode::Backspace));
        assert_eq!(app.input, "你");
        // Insert mid-string via Left.
        type_str(&mut app, "们");
        app.on_key(key(KeyCode::Left));
        type_str(&mut app, "x");
        assert_eq!(app.input, "你x们");
    }

    #[test]
    fn enter_submits_and_clears_but_not_while_in_flight() {
        let mut app = App::new("s".into());
        type_str(&mut app, "hello");
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Action::Submit {
                text: "hello".into(),
                shown: "hello".into(),
            })
        );
        assert!(app.input.is_empty());

        app.in_flight = true;
        type_str(&mut app, "queued?");
        assert_eq!(app.on_key(key(KeyCode::Enter)), None, "one turn at a time");
        assert_eq!(app.input, "queued?", "draft preserved");
    }

    /// Esc is the stop key, and only while there is something to stop.
    #[test]
    fn esc_interrupts_only_while_a_turn_is_in_flight() {
        let mut app = App::new("s".into());
        assert_eq!(
            app.on_key(key(KeyCode::Esc)),
            None,
            "idle Esc must do nothing at all"
        );

        app.start_turn();
        assert_eq!(app.on_key(key(KeyCode::Esc)), Some(Action::Interrupt));
    }

    /// Idle Esc must not double as "clear the input": a stop key that sometimes
    /// discards the draft instead is worse than one extra keystroke.
    #[test]
    fn idle_esc_leaves_the_draft_alone() {
        let mut app = App::new("s".into());
        type_str(&mut app, "半句话");
        assert_eq!(app.on_key(key(KeyCode::Esc)), None);
        assert_eq!(app.input, "半句话");
    }

    /// A turn parked on an `ask_user` question is still in flight, so Esc has to
    /// reach it — that is the case where the cancel signal alone is not enough
    /// (the loop is not at an await), and the event loop resolves the question.
    #[test]
    fn esc_interrupts_a_turn_waiting_on_a_question() {
        let mut app = App::new("s".into());
        app.in_flight = true;
        app.awaiting_answer = true;
        assert_eq!(app.on_key(key(KeyCode::Esc)), Some(Action::Interrupt));
    }

    /// Precedence: a modal is only ever shown *during* a turn, so both Esc
    /// meanings are live at once. The modal wins — denying is the way out of it,
    /// and the turn it belongs to cannot advance until the prompt is answered
    /// anyway, so nothing is lost by not interrupting on the first press.
    #[test]
    fn esc_under_the_modal_denies_rather_than_interrupting() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let mut app = App::new("s".into());
        app.in_flight = true;
        app.modal = Some(ApprovalPrompt {
            summary: "rm -rf build".into(),
            detail: None,
            dangerous: true,
            always_rule: None,
            reply: Some(tx),
        });
        assert_eq!(
            app.on_key(key(KeyCode::Esc)),
            Some(Action::Answered(Answer::Deny(None)))
        );
    }

    #[test]
    fn turn_lifecycle_tracks_running_state_and_elapsed_time() {
        let mut app = App::new("s".into());
        assert_eq!(app.turn_elapsed(), None);

        app.start_turn();
        assert!(app.in_flight);
        assert!(app.turn_elapsed().is_some());

        app.finish_turn();
        assert!(!app.in_flight);
        assert_eq!(app.turn_elapsed(), None);
    }

    #[test]
    fn pending_clarify_lets_a_mid_turn_submit_through_as_answer() {
        let mut app = App::new("s".into());
        app.in_flight = true;
        app.awaiting_answer = true;
        type_str(&mut app, "蓝色");
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Action::Answer {
                text: "蓝色".into(),
                shown: "蓝色".into(),
            })
        );
        assert!(app.input.is_empty());
        assert!(!app.awaiting_answer, "one answer per question");
        // The next mid-turn submit is back to being blocked.
        type_str(&mut app, "more");
        assert_eq!(app.on_key(key(KeyCode::Enter)), None);
    }

    #[test]
    fn slash_new_is_a_new_session_even_mid_turn() {
        let mut app = App::new("s".into());
        app.in_flight = true;
        type_str(&mut app, "/new");
        assert_eq!(app.on_key(key(KeyCode::Enter)), Some(Action::NewSession));
    }

    #[test]
    fn modal_captures_keys_and_replies() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut app = App::new("s".into());
        app.modal = Some(ApprovalPrompt {
            summary: "run shell".into(),
            detail: None,
            dangerous: false,
            always_rule: None,
            reply: Some(tx),
        });
        // Ordinary typing is captured by the modal.
        assert_eq!(app.on_key(key(KeyCode::Char('x'))), None);
        assert!(app.input.is_empty());
        // Answering resolves the oneshot and closes the modal.
        assert_eq!(
            app.on_key(key(KeyCode::Char('y'))),
            Some(Action::Answered(Answer::Once))
        );
        assert!(app.modal.is_none());
        assert_eq!(rx.blocking_recv(), Ok(Answer::Once));
    }

    /// `n` opens a one-line reason prompt whose text reaches the approver, so a
    /// refusal can tell the agent what to do instead.
    #[test]
    fn denying_with_n_collects_a_reason() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut app = App::new("s".into());
        app.modal = Some(ApprovalPrompt {
            summary: "rm -rf build".into(),
            detail: None,
            dangerous: true,
            always_rule: None,
            reply: Some(tx),
        });

        // `n` does not answer yet — it switches the modal to reason entry.
        assert_eq!(app.on_key(key(KeyCode::Char('n'))), None);
        assert_eq!(app.modal_reason.as_deref(), Some(""));
        assert!(app.modal.is_some(), "modal stays open while typing");

        for c in "用 trash".chars() {
            assert_eq!(app.on_key(key(KeyCode::Char(c))), None);
        }
        // The reason buffer is separate from the composer draft.
        assert!(app.input.is_empty());
        assert_eq!(app.modal_reason.as_deref(), Some("用 trash"));

        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Action::Answered(Answer::Deny(Some("用 trash".into()))))
        );
        assert!(app.modal.is_none());
        assert!(app.modal_reason.is_none());
        assert_eq!(
            rx.blocking_recv(),
            Ok(Answer::Deny(Some("用 trash".into())))
        );
    }

    #[test]
    fn esc_denies_immediately_without_a_reason() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut app = App::new("s".into());
        app.modal = Some(ApprovalPrompt {
            summary: "rm -rf build".into(),
            detail: None,
            dangerous: true,
            always_rule: None,
            reply: Some(tx),
        });
        assert_eq!(
            app.on_key(key(KeyCode::Esc)),
            Some(Action::Answered(Answer::Deny(None)))
        );
        assert_eq!(rx.blocking_recv(), Ok(Answer::Deny(None)));
    }

    /// Esc out of reason entry is still a denial — just an unexplained one.
    #[test]
    fn esc_during_reason_entry_denies_without_the_partial_text() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut app = App::new("s".into());
        app.modal = Some(ApprovalPrompt {
            summary: "rm -rf build".into(),
            detail: None,
            dangerous: true,
            always_rule: None,
            reply: Some(tx),
        });
        app.on_key(key(KeyCode::Char('n')));
        type_reason(&mut app, "half-typed");
        assert_eq!(
            app.on_key(key(KeyCode::Esc)),
            Some(Action::Answered(Answer::Deny(None)))
        );
        assert_eq!(rx.blocking_recv(), Ok(Answer::Deny(None)));
    }

    /// An empty reason is the same as a plain denial.
    #[test]
    fn enter_with_a_blank_reason_is_a_plain_denial() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let mut app = App::new("s".into());
        app.modal = Some(ApprovalPrompt {
            summary: "write".into(),
            detail: None,
            dangerous: false,
            always_rule: None,
            reply: Some(tx),
        });
        app.on_key(key(KeyCode::Char('n')));
        type_reason(&mut app, "  ");
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Action::Answered(Answer::Deny(None)))
        );
    }

    #[test]
    fn ctrl_c_quits_everywhere_ctrl_d_only_on_empty_input() {
        let mut app = App::new("s".into());
        assert_eq!(app.on_key(ctrl('d')), Some(Action::Quit));
        type_str(&mut app, "draft");
        assert_eq!(app.on_key(ctrl('d')), None, "Ctrl-D with a draft is inert");
        assert_eq!(app.on_key(ctrl('c')), Some(Action::Quit));
    }
}
