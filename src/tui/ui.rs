//! Rendering for the chat TUI. Pure functions over the [`App`] state; the
//! wrapping helpers are width-aware (CJK chars are double-width) and
//! unit-tested — ratatui's own `Paragraph::wrap` can't report how many lines
//! it produced, which the bottom-anchored scroll needs.

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
};
use unicode_width::UnicodeWidthChar;

use super::app::{App, PasteChip, Role};
use super::markdown::markdown_lines_cached;

const SPINNER: [&str; 4] = ["⠇", "⠋", "⠙", "⠸"];

/// Visible rows the composer may grow to before it scrolls internally.
const INPUT_MAX_ROWS: u16 = 8;

pub fn render(frame: &mut Frame, app: &App) {
    // Grok Build keeps persistent identity/progress chrome, but deliberately
    // sheds optional rows in short terminals. Follow that principle here: the
    // session header makes a long chat easier to orient in, without starving
    // the transcript in a small split pane.
    let area = frame.area();
    let compact = area.height <= 8;
    let input_height = input_height(app, area, if compact { 0 } else { 1 });
    let (header_area, transcript_area, status_area, input_area) = if compact {
        let [transcript, status, input] = Layout::vertical([
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(input_height),
        ])
        .areas(area);
        (None, transcript, status, input)
    } else {
        let [header, transcript, status, input] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(input_height),
        ])
        .areas(area);
        (Some(header), transcript, status, input)
    };

    if let Some(area) = header_area {
        render_header(frame, app, area);
    }
    render_transcript(frame, app, transcript_area);
    render_status(frame, app, status_area);
    render_input(frame, app, input_area);
    if let Some(prompt) = &app.modal {
        render_modal(frame, prompt, app.modal_reason.as_deref(), frame.area());
    }
}

/// A quiet identity row keeps the session visible without repeating its full
/// UUID below every activity update. The compact form leaves the conversation
/// as the visual focus while still making screenshots/debug reports useful.
fn render_header(frame: &mut Frame, app: &App, area: Rect) {
    let [brand_area, session_area] =
        Layout::horizontal([Constraint::Min(1), Constraint::Length(18)]).areas(area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " KOMO",
                Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
            Span::styled("  chat", Style::new().fg(Color::DarkGray)),
        ])),
        brand_area,
    );
    let short_id: String = app
        .session_id
        .chars()
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!("session · {short_id} "),
            Style::new().fg(Color::DarkGray),
        )))
        .alignment(ratatui::layout::Alignment::Right),
        session_area,
    );
}

fn render_transcript(frame: &mut Frame, app: &App, area: Rect) {
    let width = area.width.max(1) as usize;
    let mut lines: Vec<Line> = Vec::new();
    for entry in &app.entries {
        // Agent replies render as markdown; everything else is plain text
        // behind a colored role prefix.
        if entry.role == Role::Agent {
            for logical in markdown_lines_cached(&entry.text) {
                lines.extend(wrap_spans(logical.spans, width));
            }
            lines.push(Line::default());
            continue;
        }
        // Tool lines carry their glyph/color in `tool_ok` (running vs ✓/✗) and
        // render the body dimmed so they read as secondary activity.
        let (prefix, head_style, body_style) = match entry.role {
            Role::You => (
                "❯ ",
                Style::new().fg(Color::Cyan),
                Style::new().fg(Color::Cyan),
            ),
            Role::Agent => ("", Style::new(), Style::new()),
            Role::Info => (
                "· ",
                Style::new().fg(Color::DarkGray),
                Style::new().fg(Color::DarkGray),
            ),
            Role::Error => (
                "✗ ",
                Style::new().fg(Color::Red),
                Style::new().fg(Color::Red),
            ),
            Role::Tool => {
                let (glyph, color) = match entry.tool_ok {
                    None => ("⚙ ", Color::Cyan),
                    Some(true) => ("✓ ", Color::Green),
                    Some(false) => ("✗ ", Color::Red),
                };
                (
                    glyph,
                    Style::new().fg(color),
                    Style::new().fg(Color::DarkGray),
                )
            }
        };
        let prefix_width = display_width(prefix);
        for (i, wrapped) in wrap_text(&entry.text, width.saturating_sub(prefix_width))
            .into_iter()
            .enumerate()
        {
            let head = if i == 0 {
                prefix.to_string()
            } else {
                " ".repeat(prefix_width)
            };
            lines.push(Line::from(vec![
                Span::styled(head, head_style.add_modifier(Modifier::BOLD)),
                Span::styled(wrapped, body_style),
            ]));
        }
        // Tool calls often arrive in bursts. Keep them as a compact activity
        // group; conversational messages retain their breathing room.
        if entry.role != Role::Tool {
            lines.push(Line::default());
        }
    }

    // Bottom-anchored scroll: 0 = follow the tail; scrolling up moves the
    // window back through the wrapped lines, clamped at the top.
    let height = area.height as usize;
    let max_offset = lines.len().saturating_sub(height);
    let offset = (app.scroll_from_bottom as usize).min(max_offset);
    let start = max_offset - offset;
    let visible: Vec<Line> = lines.into_iter().skip(start).take(height).collect();
    frame.render_widget(Paragraph::new(visible), area);
}

fn render_status(frame: &mut Frame, app: &App, area: Rect) {
    let status = if app.connecting {
        // The UI paints before the backend finishes booting; say so instead of
        // claiming 就绪, and tell the user their draft is not lost.
        Line::from(vec![
            Span::styled(
                format!(" {} 正在启动后端… ", SPINNER[app.spinner % SPINNER.len()]),
                Style::new().fg(Color::Yellow),
            ),
            Span::styled(
                if app.in_flight {
                    "消息已排队，就绪后自动发送 · Esc 取消"
                } else {
                    "可以先输入，Enter 会排队发送"
                },
                Style::new().fg(Color::DarkGray),
            ),
        ])
    } else if app.in_flight && app.awaiting_answer {
        Line::from(vec![
            Span::styled(
                " ❓ 等待你的回答 — 直接输入并回车 ",
                Style::new().fg(Color::Cyan),
            ),
            Span::styled(
                "等待输入期间，当前 turn 保持挂起 · Esc 中断",
                Style::new().fg(Color::DarkGray),
            ),
        ])
    } else if app.in_flight && app.active_tool.is_some() {
        let tool = app.active_tool.as_deref().unwrap_or_default();
        Line::from(vec![
            Span::styled(
                format!(
                    " {} {tool} 运行中 · {} ",
                    SPINNER[app.spinner % SPINNER.len()],
                    elapsed_label(app)
                ),
                Style::new().fg(Color::Cyan),
            ),
            Span::styled("· Esc 中断 ", Style::new().fg(Color::DarkGray)),
        ])
    } else if app.in_flight {
        // A reasoning model can spend most of a round thinking before any
        // visible text exists; showing how much it has produced turns that
        // silence into progress.
        let thinking = if app.reasoning_chars > 0 {
            format!(" 正在思考 {} 字", app.reasoning_chars)
        } else {
            " 正在思考".to_string()
        };
        Line::from(vec![
            Span::styled(
                format!(
                    " {}{thinking} · {} ",
                    SPINNER[app.spinner % SPINNER.len()],
                    elapsed_label(app)
                ),
                Style::new().fg(Color::Yellow),
            ),
            Span::styled("· Esc 中断 ", Style::new().fg(Color::DarkGray)),
        ])
    } else if let Some(awaiting) = &app.awaiting {
        // A turn of this conversation is parked somewhere else — waiting on an
        // approval answered from a chat, on a timer, on a background job. It is
        // not this UI's turn, so nothing else here would ever mention it.
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        Line::from(vec![
            Span::styled(
                format!(" ⏸ {} ", awaiting.label(now)),
                Style::new().fg(Color::Yellow),
            ),
            Span::styled(
                "这条对话有一个 turn 停在等待 · 直接发消息会取代它",
                Style::new().fg(Color::DarkGray),
            ),
        ])
    } else {
        Line::from(Span::styled(
            " ● 就绪  · Enter 发送 · Shift-Enter/Ctrl-J 换行 · /new 新会话 · ↑↓ 滚动 · Ctrl-C 退出",
            Style::new().fg(Color::DarkGray),
        ))
    };
    frame.render_widget(Paragraph::new(status), area);
}

fn elapsed_label(app: &App) -> String {
    let seconds = app.turn_elapsed().map_or(0, |elapsed| elapsed.as_secs());
    if seconds >= 60 {
        format!("{}m{}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

/// How tall the composer box should be, borders included: it grows with the
/// draft (Shift-Enter and pastes make it multi-line) but never eats the header,
/// the status row, or the transcript's last line.
fn input_height(app: &App, area: Rect, header_rows: u16) -> u16 {
    let inner_width = area.width.saturating_sub(2).max(1) as usize;
    let rows = wrap_input(&app.input, &app.chips, inner_width, app.cursor)
        .0
        .len() as u16;
    let ceiling = area.height.saturating_sub(header_rows + 2).max(3);
    (rows.clamp(1, INPUT_MAX_ROWS) + 2).min(ceiling)
}

fn render_input(frame: &mut Frame, app: &App, area: Rect) {
    let title = if app.in_flight {
        " message · draft preserved "
    } else {
        " message · Enter 发送 · Shift-Enter 换行 "
    };
    let block = Block::new()
        .borders(Borders::ALL)
        .border_style(Style::new().fg(if app.awaiting_answer {
            Color::Cyan
        } else {
            Color::DarkGray
        }))
        .title(title);
    let inner = block.inner(area);
    let inner_width = inner.width.max(1) as usize;
    let height = inner.height.max(1) as usize;

    // Soft-wrapped rows; scroll down just far enough to keep the cursor row in
    // the box (a long paste shows its tail, which is where editing happens).
    let (rows, (cursor_row, cursor_col)) =
        wrap_input(&app.input, &app.chips, inner_width, app.cursor);
    let first = cursor_row.saturating_sub(height - 1);
    let visible: Vec<Line> = rows
        .into_iter()
        .skip(first)
        .take(height)
        .map(|row| {
            Line::from(
                row.into_iter()
                    .map(|seg| {
                        // A chip reads as one object rather than editable text,
                        // so it is styled as a unit.
                        if seg.chip {
                            Span::styled(
                                seg.text,
                                Style::new().fg(Color::Magenta).add_modifier(Modifier::BOLD),
                            )
                        } else {
                            Span::raw(seg.text)
                        }
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    frame.render_widget(Paragraph::new(visible).block(block), area);
    let x = inner.x + cursor_col.min(inner_width.saturating_sub(1)) as u16;
    let y = inner.y + (cursor_row - first) as u16;
    frame.set_cursor_position((x, y));
}

/// A run of same-kind text inside one wrapped composer row: either ordinary
/// draft text or a folded paste chip, which is styled differently.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Seg {
    pub text: String,
    pub chip: bool,
}

/// Soft-wrap the draft to `width` columns and locate `cursor` (a char index)
/// within the wrapped rows. Wrapping and cursor placement must happen in one
/// pass — otherwise the cursor lands on a different row than the char it sits
/// in front of. Same CJK width rules as [`wrap_text`].
///
/// A chip renders as its label and is never split, and the loop *slices past* the
/// folded text (`chip.bytes`) instead of walking it — so the per-frame cost is
/// the size of what's on screen, not the size of what was pasted.
fn wrap_input(
    text: &str,
    chips: &[PasteChip],
    width: usize,
    cursor: usize,
) -> (Vec<Vec<Seg>>, (usize, usize)) {
    let width = width.max(1);
    let mut rows: Vec<Vec<Seg>> = Vec::new();
    let mut line: Vec<Seg> = Vec::new();
    let mut cols = 0usize;
    let mut at: Option<(usize, usize)> = None;
    let mut i = 0usize; // char index
    let mut b = 0usize; // byte index
    let mut next_chip = 0usize;

    loop {
        // A chip starting here: emit the whole label, then jump the hidden text.
        if let Some(chip) = chips.get(next_chip).filter(|c| c.range.start == i) {
            let w = display_width(&chip.label);
            if cols + w > width && !line.is_empty() {
                rows.push(std::mem::take(&mut line));
                cols = 0;
            }
            if at.is_none() && cursor == i {
                at = Some((rows.len(), cols));
            }
            line.push(Seg {
                text: chip.label.clone(),
                chip: true,
            });
            cols += w;
            i = chip.range.end;
            b = chip.bytes.end;
            next_chip += 1;
            // The cursor is never inside a chip; if it somehow is, it belongs on
            // the far side rather than in text nobody can see.
            if at.is_none() && cursor <= i {
                at = Some((rows.len(), cols));
            }
            continue;
        }

        let Some(c) = text[b..].chars().next() else {
            break;
        };
        if c == '\n' {
            if at.is_none() && cursor == i {
                at = Some((rows.len(), cols));
            }
            rows.push(std::mem::take(&mut line));
            cols = 0;
        } else {
            let w = c.width().unwrap_or(0);
            if cols + w > width && !line.is_empty() {
                rows.push(std::mem::take(&mut line));
                cols = 0;
            }
            if at.is_none() && cursor == i {
                at = Some((rows.len(), cols));
            }
            match line.last_mut() {
                Some(seg) if !seg.chip => seg.text.push(c),
                _ => line.push(Seg {
                    text: c.to_string(),
                    chip: false,
                }),
            }
            cols += w;
        }
        i += 1;
        b += c.len_utf8();
    }

    // Cursor at (or past) the end: a full last row means it belongs on a fresh one.
    let at = at.unwrap_or_else(|| {
        if cols >= width {
            rows.push(std::mem::take(&mut line));
            cols = 0;
        }
        (rows.len(), cols)
    });
    rows.push(line);
    (rows, at)
}

/// Draw the approval modal. `reason` is `Some` while the user is typing a
/// denial reason (see `App::modal_reason`), which replaces the key legend with a
/// one-line editor.
fn render_modal(
    frame: &mut Frame,
    prompt: &super::approver::ApprovalPrompt,
    reason: Option<&str>,
    screen: Rect,
) {
    let (title, border) = if prompt.dangerous {
        (" 🛑 需要审批(危险操作) ", Style::new().fg(Color::Red))
    } else {
        (" ⚠ 需要审批 ", Style::new().fg(Color::Yellow))
    };
    let width = screen.width.saturating_sub(8).clamp(20, 80);
    let inner_width = width.saturating_sub(2) as usize;

    let mut lines: Vec<Line> = wrap_text(&prompt.summary, inner_width)
        .into_iter()
        .map(Line::from)
        .collect();
    if let Some(detail) = &prompt.detail {
        lines.push(Line::default());
        for l in wrap_text(detail, inner_width) {
            lines.push(Line::from(Span::styled(
                l,
                Style::new().fg(Color::DarkGray),
            )));
        }
    }
    lines.push(Line::default());
    match reason {
        // Collecting a denial reason: show the editor and what it's for.
        Some(text) => {
            lines.push(Line::from(Span::styled(
                "拒绝理由（可留空）— 会转达给 agent：",
                Style::new().fg(Color::DarkGray),
            )));
            for l in wrap_text(&format!("❯ {text}▏"), inner_width) {
                lines.push(Line::from(Span::styled(l, Style::new().fg(Color::Cyan))));
            }
            lines.push(Line::from(Span::styled(
                "[Enter] 确认拒绝   [Esc] 不给理由直接拒绝",
                Style::new().add_modifier(Modifier::BOLD),
            )));
        }
        None => {
            // The saved rule is spelled out before the key that saves it: the
            // operator has to see how wide the grant is to judge it.
            if let Some(rule) = &prompt.always_rule {
                for l in wrap_text(&format!("[a] 以后都允许，保存规则：{rule}"), inner_width)
                {
                    lines.push(Line::from(Span::styled(
                        l,
                        Style::new().fg(Color::DarkGray),
                    )));
                }
            }
            let keys = match prompt.always_rule {
                Some(_) => {
                    "[y] 允许一次   [s] 本会话内同类操作   [a] 以后都允许   [n] 拒绝并说明   [Esc] 拒绝"
                }
                None => "[y] 允许一次   [s] 本会话内同类操作   [n] 拒绝并说明   [Esc] 拒绝",
            };
            lines.push(Line::from(Span::styled(
                keys,
                Style::new().add_modifier(Modifier::BOLD),
            )));
        }
    }

    let height = (lines.len() as u16 + 2).min(screen.height.saturating_sub(2));
    let rect = centered(screen, width, height);
    frame.render_widget(Clear, rect);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::new()
                .borders(Borders::ALL)
                .border_style(border)
                .title(title),
        ),
        rect,
    );
}

fn centered(screen: Rect, width: u16, height: u16) -> Rect {
    let x = screen.x + screen.width.saturating_sub(width) / 2;
    let y = screen.y + screen.height.saturating_sub(height) / 2;
    Rect::new(x, y, width.min(screen.width), height.min(screen.height))
}

/// Display width of a string (CJK double-width aware).
pub(super) fn display_width(s: &str) -> usize {
    s.chars().map(|c| c.width().unwrap_or(0)).sum()
}

/// Hard-wrap `text` (which may contain newlines) to `width` display columns.
/// Splits on char boundaries with CJK width awareness — no word-break
/// cleverness, but never overflows and never loses content. An empty input
/// still yields one empty line so the entry occupies a row.
pub(super) fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut out = Vec::new();
    for logical in text.split('\n') {
        let mut line = String::new();
        let mut cols = 0usize;
        for c in logical.chars() {
            let w = c.width().unwrap_or(0);
            if cols + w > width && !line.is_empty() {
                out.push(std::mem::take(&mut line));
                cols = 0;
            }
            line.push(c);
            cols += w;
        }
        out.push(line);
    }
    out
}

/// Hard-wrap a logical line of styled spans to `width` display columns,
/// splitting spans at char boundaries — the styled counterpart of
/// [`wrap_text`], with the same CJK width rules. An empty input still yields
/// one empty line so the entry occupies a row.
fn wrap_spans(spans: Vec<Span<'static>>, width: usize) -> Vec<Line<'static>> {
    let width = width.max(1);
    let mut out = Vec::new();
    let mut current: Vec<Span> = Vec::new();
    let mut cols = 0usize;
    for span in spans {
        let mut buf = String::new();
        for c in span.content.chars() {
            let w = c.width().unwrap_or(0);
            if cols + w > width && cols > 0 {
                if !buf.is_empty() {
                    current.push(Span::styled(std::mem::take(&mut buf), span.style));
                }
                out.push(Line::from(std::mem::take(&mut current)));
                cols = 0;
            }
            buf.push(c);
            cols += w;
        }
        if !buf.is_empty() {
            current.push(Span::styled(buf, span.style));
        }
    }
    out.push(Line::from(current));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_respects_cjk_double_width() {
        // 4 CJK chars = 8 columns; at width 4 that is 2 chars per line.
        let lines = wrap_text("你好世界", 4);
        assert_eq!(lines, vec!["你好", "世界"]);
    }

    #[test]
    fn wrap_preserves_newlines_and_empty_lines() {
        assert_eq!(wrap_text("a\n\nb", 10), vec!["a", "", "b"]);
    }

    #[test]
    fn wrap_never_loses_content() {
        let text = "abcdefghij";
        let rejoined: String = wrap_text(text, 3).concat();
        assert_eq!(rejoined, text);
    }

    #[test]
    fn wrap_spans_keeps_styles_across_the_break() {
        let spans = vec![
            Span::styled("你好", Style::new().fg(Color::Cyan)),
            Span::styled("世界", Style::new().fg(Color::Red)),
        ];
        // 8 columns of CJK at width 4 = 2 chars per line, one span each.
        let lines = wrap_spans(spans, 4);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].spans[0].content, "你好");
        assert_eq!(lines[0].spans[0].style.fg, Some(Color::Cyan));
        assert_eq!(lines[1].spans[0].content, "世界");
        assert_eq!(lines[1].spans[0].style.fg, Some(Color::Red));
    }

    #[test]
    fn wrap_spans_splits_one_span_without_losing_content() {
        let lines = wrap_spans(vec![Span::raw("abcdefghij")], 3);
        let rejoined: String = lines
            .iter()
            .flat_map(|l| l.spans.iter())
            .map(|s| s.content.as_ref())
            .collect();
        assert_eq!(rejoined, "abcdefghij");
        assert_eq!(lines.len(), 4);
    }

    /// The rows' plain text, for assertions that don't care about chip styling.
    fn row_texts(rows: &[Vec<Seg>]) -> Vec<String> {
        rows.iter()
            .map(|row| row.iter().map(|s| s.text.as_str()).collect())
            .collect()
    }

    #[test]
    fn wrap_input_places_cursor_after_an_explicit_newline() {
        let (rows, at) = wrap_input("ab\ncd", &[], 10, 3);
        assert_eq!(row_texts(&rows), vec!["ab", "cd"]);
        assert_eq!(at, (1, 0), "cursor sits at the start of the second line");
    }

    #[test]
    fn wrap_input_moves_the_cursor_to_a_fresh_row_when_the_last_is_full() {
        let (rows, at) = wrap_input("abc", &[], 3, 3);
        assert_eq!(row_texts(&rows), vec!["abc", ""]);
        assert_eq!(at, (1, 0));
    }

    #[test]
    fn wrap_input_keeps_the_cursor_inside_a_soft_wrapped_row() {
        // Width 3, cursor before 'd' — 'd' opens the second row.
        let (rows, at) = wrap_input("abcde", &[], 3, 3);
        assert_eq!(row_texts(&rows), vec!["abc", "de"]);
        assert_eq!(at, (1, 0));
    }

    #[test]
    fn a_chip_renders_as_its_label_and_never_splits() {
        // "see: " + a folded 4-line paste. The label is wider than the remaining
        // columns, so it moves to its own row whole.
        let pasted = "one\ntwo\nthree\nfour";
        let text = format!("see: {pasted}");
        let chips = [PasteChip {
            range: 5..5 + pasted.chars().count(),
            bytes: 5..5 + pasted.len(),
            label: "[Pasted: 4 lines]".into(),
        }];
        let (rows, at) = wrap_input(&text, &chips, 12, text.chars().count());
        assert_eq!(row_texts(&rows), vec!["see: ", "[Pasted: 4 lines]"]);
        assert!(rows[1][0].chip, "the label is styled as a chip");
        assert_eq!(at, (1, 17), "cursor sits just past the label");
    }

    #[test]
    fn wrapping_does_not_walk_a_folded_paste() {
        // A megabyte paste must cost what its label costs, not what its content
        // does — this is the whole point of folding it.
        let pasted = "x".repeat(1_000_000);
        let chips = [PasteChip {
            range: 0..pasted.chars().count(),
            bytes: 0..pasted.len(),
            label: "[Pasted: 1.0 MB]".into(),
        }];
        let started = std::time::Instant::now();
        let (rows, at) = wrap_input(&pasted, &chips, 40, pasted.chars().count());
        assert_eq!(row_texts(&rows), vec!["[Pasted: 1.0 MB]"]);
        assert_eq!(at, (0, 16));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(5),
            "wrapping a folded paste must not scale with its size: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn input_box_grows_with_the_draft() {
        let area = Rect::new(0, 0, 20, 24);
        let mut app = App::new("s".into());
        assert_eq!(input_height(&app, area, 1), 3, "one row + borders");
        app.input = "a\nb\nc".into();
        app.cursor = app.input.chars().count();
        assert_eq!(input_height(&app, area, 1), 5);
        app.input = "x\n".repeat(40);
        app.cursor = app.input.chars().count();
        assert_eq!(
            input_height(&app, area, 1),
            INPUT_MAX_ROWS + 2,
            "capped, then scrolls internally"
        );
    }

    /// Headless render smoke: a full frame (transcript + status + input, then
    /// with an approval modal) draws without panicking and shows the content.
    #[test]
    fn renders_frame_and_modal_on_test_backend() {
        use ratatui::{Terminal, backend::TestBackend};

        let mut app = App::new("sess-1".into());
        app.push(Role::Info, "Komo v0.1 — session sess-1");
        app.push(Role::You, "hello 你好");
        app.push(Role::Agent, "hi there");
        app.input = "draft".into();
        app.cursor = 5;

        let mut terminal = Terminal::new(TestBackend::new(60, 16)).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let content = format!("{:?}", terminal.backend().buffer());
        assert!(
            content.contains("KOMO"),
            "header rendered on a normal terminal"
        );
        assert!(content.contains("hello"), "user entry rendered");
        assert!(content.contains("hi there"), "agent entry rendered");
        assert!(content.contains("draft"), "input draft rendered");

        // Modal overlays and captures the frame.
        app.modal = Some(super::super::approver::ApprovalPrompt {
            summary: "run shell command: rm -rf /tmp/x".into(),
            detail: Some("matched dangerous pattern".into()),
            dangerous: true,
            always_rule: None,
            reply: None,
        });
        app.in_flight = true; // spinner path renders too
        terminal.draw(|f| render(f, &app)).unwrap();
        let content = format!("{:?}", terminal.backend().buffer());
        assert!(content.contains("rm -rf"), "modal summary rendered");
        assert!(content.contains("拒绝"), "modal key hints rendered");
    }

    /// Full render path with a folded paste in the composer: the label shows, the
    /// pasted content does not, and the box stays one row tall.
    #[test]
    fn renders_a_folded_paste_chip_in_the_composer() {
        use ratatui::{Terminal, backend::TestBackend};

        let mut app = App::new("sess-1".into());
        app.on_paste("alpha\nbeta\ngamma\ndelta");
        assert_eq!(app.chips.len(), 1, "precondition: the paste folded");

        let mut terminal = Terminal::new(TestBackend::new(60, 16)).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let content = format!("{:?}", terminal.backend().buffer());
        assert!(content.contains("[Pasted: 4 lines]"), "chip label rendered");
        assert!(
            !content.contains("gamma"),
            "the folded content stays off screen"
        );
    }

    /// While the backend boots, the status line says so instead of claiming
    /// 就绪 — and once a message is queued, it says the draft will be sent.
    #[test]
    fn status_line_shows_backend_boot_and_queued_draft() {
        use ratatui::{Terminal, backend::TestBackend};

        let mut app = App::new("sess-1".into());
        app.connecting = true;

        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let content = format!("{:?}", terminal.backend().buffer());
        assert!(content.contains("正在启动后端"), "boot status rendered");
        assert!(
            !content.contains("就绪"),
            "idle status suppressed while booting"
        );

        app.in_flight = true; // a submission was queued
        terminal.draw(|f| render(f, &app)).unwrap();
        let content = format!("{:?}", terminal.backend().buffer());
        assert!(content.contains("消息已排队"), "queued-draft hint rendered");
    }

    /// Headless render smoke for the tool activity feed: a running line shows
    /// the ⚙ glyph and the "运行中…" status; a finished line shows ✓ and the
    /// result preview.
    #[test]
    fn renders_tool_activity_line() {
        use ratatui::{Terminal, backend::TestBackend};

        let mut app = App::new("sess-1".into());
        app.in_flight = true;
        app.tool_started(0, "shell".into(), "ls /tmp".into());

        let mut terminal = Terminal::new(TestBackend::new(60, 12)).unwrap();
        terminal.draw(|f| render(f, &app)).unwrap();
        let content = format!("{:?}", terminal.backend().buffer());
        assert!(content.contains('⚙'), "running glyph rendered");
        assert!(content.contains("shell"), "tool name rendered");
        assert!(content.contains("运行中"), "status shows the running tool");

        app.tool_finished(0, "shell".into(), true, "3 entries".into());
        app.in_flight = false;
        terminal.draw(|f| render(f, &app)).unwrap();
        let content = format!("{:?}", terminal.backend().buffer());
        assert!(content.contains('✓'), "success glyph rendered");
        assert!(content.contains("3 entries"), "result preview rendered");
    }
}
