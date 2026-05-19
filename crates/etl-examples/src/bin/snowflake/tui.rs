use std::{
    collections::VecDeque,
    io::Stdout,
    sync::{Arc, Mutex},
};

use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, LineGauge, Paragraph, Row, Table, Wrap},
};

use crate::{
    commands,
    state::{DashboardState, GlobalPhase, LogLevel, TableInfo},
};

pub struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

pub fn setup_terminal(
) -> Result<(Terminal<CrosstermBackend<Stdout>>, TerminalGuard), Box<dyn std::error::Error>> {
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, crossterm::terminal::EnterAlternateScreen)?;
    Ok((Terminal::new(CrosstermBackend::new(stdout))?, TerminalGuard))
}

pub fn restore_terminal() {
    let _ = crossterm::terminal::disable_raw_mode();
    let _ = crossterm::execute!(std::io::stdout(), crossterm::terminal::LeaveAlternateScreen);
}

pub fn render(
    frame: &mut ratatui::Frame,
    state: &DashboardState,
    log_buffer: &Arc<Mutex<VecDeque<String>>>,
) {
    if state.show_help {
        render_help_overlay(frame);
        return;
    }

    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),      // global stats + gauge
            Constraint::Percentage(45), // table + detail
            Constraint::Min(6),         // logs (takes remaining space)
            Constraint::Length(1),      // command bar
        ])
        .split(frame.area());

    render_global_stats(frame, main_chunks[0], state);
    render_middle_panel(frame, main_chunks[1], state);
    render_log_panel(frame, main_chunks[2], log_buffer, state.log_scroll, state.log_level_filter);
    render_command_bar(frame, main_chunks[3], state);
}

fn render_global_stats(frame: &mut ratatui::Frame, area: Rect, state: &DashboardState) {
    let block = Block::default().title(" Snowflake CDC Pipeline ").borders(Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let elapsed_secs = state.elapsed.as_secs();
    let elapsed_str = format!(
        "{:02}:{:02}:{:02}",
        elapsed_secs / 3600,
        (elapsed_secs % 3600) / 60,
        elapsed_secs % 60
    );

    let phase_color = match state.phase {
        GlobalPhase::TableCopy => Color::Green,
        GlobalPhase::CdcStreaming => Color::Cyan,
        GlobalPhase::Stopped => Color::Red,
    };

    let running_indicator = if state.pipeline_running {
        Span::styled(" RUNNING ", Style::default().fg(Color::Black).bg(Color::Green))
    } else {
        Span::styled(" STOPPED ", Style::default().fg(Color::Black).bg(Color::Red))
    };

    // Row 1: phase, elapsed, tables
    let header = Line::from(vec![
        running_indicator,
        Span::raw("  Phase: "),
        Span::styled(state.phase.label(), Style::default().fg(phase_color)),
        Span::raw(format!("  Elapsed: {elapsed_str}  ")),
        Span::raw(format!("Tables: {}", state.table_count)),
    ]);

    // Row 2: copy/CDC stats as text
    let stats_line = if state.phase == GlobalPhase::CdcStreaming {
        let copy_part = if let Some(copy_elapsed) = state.copy_elapsed {
            format!(
                "Copy: {} rows in {:.1}s",
                format_num(state.total_copy_rows),
                copy_elapsed.as_secs_f64()
            )
        } else {
            format!("Copy: {} rows", format_num(state.total_copy_rows))
        };
        Line::from(vec![
            Span::raw(format!(" {copy_part}")),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("CDC: {} events", format_num(state.total_cdc_events)),
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.0} r/s", state.throughput_current)),
            if state.api_errors > 0 {
                Span::styled(
                    format!("  Errors: {}", state.api_errors),
                    Style::default().fg(Color::Red),
                )
            } else {
                Span::raw("")
            },
        ])
    } else {
        let total = state.estimated_rows.max(1);
        let pct = (state.total_rows_synced as f64 / total as f64 * 100.0).min(100.0);
        Line::from(vec![
            Span::raw(format!(
                " Copy: {} / {} ({:.0}%)",
                format_num(state.total_rows_synced),
                format_num(total),
                pct
            )),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{:.0} r/s (avg: {:.0})",
                state.throughput_current, state.throughput_avg
            )),
            if state.api_errors > 0 {
                Span::styled(
                    format!("  Errors: {}", state.api_errors),
                    Style::default().fg(Color::Red),
                )
            } else {
                Span::raw("")
            },
        ])
    };

    // Row 3: progress gauge
    let total = state.estimated_rows.max(1);
    let ratio = if state.phase == GlobalPhase::TableCopy {
        (state.total_rows_synced as f64 / total as f64).clamp(0.0, 1.0)
    } else {
        1.0
    };

    let gauge_color = match state.phase {
        GlobalPhase::TableCopy => Color::Green,
        GlobalPhase::CdcStreaming => Color::Cyan,
        GlobalPhase::Stopped => Color::DarkGray,
    };

    let sub = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1), Constraint::Length(1)])
        .split(inner);

    frame.render_widget(Paragraph::new(vec![header]), sub[0]);
    frame.render_widget(Paragraph::new(vec![stats_line]), sub[1]);

    let gauge_label = if state.phase == GlobalPhase::CdcStreaming {
        if let Some((ref msg, _)) = state.status_message {
            msg.clone()
        } else {
            "streaming".to_owned()
        }
    } else {
        format!(" {:.0}%", ratio * 100.0)
    };

    frame.render_widget(
        LineGauge::default()
            .filled_style(Style::default().fg(gauge_color))
            .ratio(ratio)
            .label(gauge_label)
            .filled_symbol(symbols::line::THICK.horizontal)
            .unfilled_symbol(symbols::line::NORMAL.horizontal),
        sub[2],
    );
}

fn render_middle_panel(frame: &mut ratatui::Frame, area: Rect, state: &DashboardState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(75), Constraint::Percentage(25)])
        .split(area);

    render_table_list(frame, chunks[0], state);
    render_table_detail(frame, chunks[1], state);
}

fn render_table_list(frame: &mut ratatui::Frame, area: Rect, state: &DashboardState) {
    if state.tables.is_empty() {
        let block = Block::default().title(" Tables (j/k) ").borders(Borders::ALL);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        frame.render_widget(Paragraph::new("  Waiting for tables..."), inner);
        return;
    }

    let header = Row::new(vec![
        Cell::from(" Name"),
        Cell::from("Phase"),
        Cell::from("Rows"),
        Cell::from("r/s"),
        Cell::from(" "),
    ])
    .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
    .bottom_margin(0);

    let rows: Vec<Row> = state
        .tables
        .iter()
        .enumerate()
        .map(|(i, table)| {
            let selected = i == state.selected_table;
            let base_style = if selected {
                Style::default().bg(Color::DarkGray).fg(Color::White).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };

            let status_icon = if table.is_errored() {
                Cell::from(Span::styled("✗", Style::default().fg(Color::Red)))
            } else if table.is_ready() {
                Cell::from(Span::styled("✓", Style::default().fg(Color::Green)))
            } else if table.is_copying() {
                Cell::from(Span::styled("↻", Style::default().fg(Color::Yellow)))
            } else {
                Cell::from("·")
            };

            Row::new(vec![
                Cell::from(format!(
                    "{}{}",
                    if selected { "▸" } else { " " },
                    truncate_str(&table.destination_name, 24)
                )),
                Cell::from(Span::styled(
                    table.phase_label(),
                    Style::default().fg(phase_color(table.phase_label())),
                )),
                Cell::from(format_compact(table.rows_synced)),
                Cell::from(format!("{:.0}", table.throughput)),
                status_icon,
            ])
            .style(base_style)
        })
        .collect();

    let widths = [
        Constraint::Min(20),
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(8),
        Constraint::Length(2),
    ];

    let table_widget = Table::new(rows, widths)
        .header(header)
        .block(Block::default().title(" Tables (j/k) ").borders(Borders::ALL));

    frame.render_widget(table_widget, area);
}

fn render_table_detail(frame: &mut ratatui::Frame, area: Rect, state: &DashboardState) {
    let title = match state.selected_table_info() {
        Some(t) => format!(" {} ", truncate_str(&t.destination_name, 18)),
        None => " Details ".to_owned(),
    };

    let block = Block::default().title(title).borders(Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some(table) = state.selected_table_info() else {
        frame.render_widget(Paragraph::new(" Select a table"), inner);
        return;
    };

    let mut lines = vec![];

    // Phase with visual indicator
    let pc = phase_color(table.phase_label());
    let indicator = phase_indicator(table);
    lines.push(Line::from(vec![
        Span::styled(table.phase_label(), Style::default().fg(pc).add_modifier(Modifier::BOLD)),
        Span::raw(" "),
        indicator,
    ]));

    // Phase duration
    if let Some(entered) = table.phase_entered_at {
        let dur = entered.elapsed();
        lines.push(Line::from(Span::styled(
            format!("{:.0}s in phase", dur.as_secs_f64()),
            Style::default().fg(Color::DarkGray),
        )));
    }

    lines.push(Line::from(""));

    // Offsets
    lines.push(Line::from(vec![
        Span::styled("Local: ", Style::default().fg(Color::DarkGray)),
        Span::raw(table.local_offset.as_deref().unwrap_or("—")),
    ]));
    lines.push(Line::from(vec![
        Span::styled("SF:    ", Style::default().fg(Color::DarkGray)),
        Span::raw(table.snowflake_offset.as_deref().unwrap_or("—")),
    ]));

    lines.push(Line::from(""));

    // Global stats
    lines.push(Line::from(vec![
        Span::styled("Copy:  ", Style::default().fg(Color::DarkGray)),
        Span::raw(format_num(state.total_copy_rows)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("CDC:   ", Style::default().fg(Color::DarkGray)),
        Span::raw(format_num(state.total_cdc_events)),
    ]));

    lines.push(Line::from(""));

    // Table OID
    lines.push(Line::from(Span::styled(
        format!("OID: {}", table.table_id),
        Style::default().fg(Color::DarkGray),
    )));

    // Error details
    if let Some(ref reason) = table.error_reason {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Error:",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )));
        for error_line in reason.lines() {
            lines.push(Line::from(Span::styled(error_line, Style::default().fg(Color::Red))));
        }
        if let Some(ref solution) = table.error_solution {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("Fix:", Style::default().fg(Color::Yellow))));
            for sol_line in solution.lines() {
                lines.push(Line::from(Span::styled(sol_line, Style::default().fg(Color::Yellow))));
            }
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("F2 to reset", Style::default().fg(Color::Magenta))));
    }

    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn render_log_panel(
    frame: &mut ratatui::Frame,
    area: Rect,
    log_buffer: &Arc<Mutex<VecDeque<String>>>,
    scroll_offset: usize,
    log_level: LogLevel,
) {
    let title = format!(" Logs (J/K scroll) [{}] ", log_level.label());
    let block = Block::default().title(title).borders(Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let logs: Vec<String> = {
        let buf = log_buffer.lock().unwrap();
        buf.iter()
            .filter(|line| match log_level {
                LogLevel::All => true,
                LogLevel::WarnAndAbove => line.contains("[WARN]") || line.contains("[ERROR]"),
                LogLevel::ErrorOnly => line.contains("[ERROR]"),
            })
            .cloned()
            .collect()
    };

    let visible_height = inner.height as usize;
    let total = logs.len();
    let start = if total > visible_height {
        let max_scroll = total - visible_height;
        let from_bottom = scroll_offset.min(max_scroll);
        max_scroll - from_bottom
    } else {
        0
    };

    let visible: Vec<Line> = logs
        .iter()
        .skip(start)
        .take(visible_height)
        .map(|line| {
            let color = if line.contains("[ERROR]") {
                Color::Red
            } else if line.contains("[WARN]") {
                Color::Yellow
            } else {
                Color::DarkGray
            };
            Line::from(Span::styled(line.clone(), Style::default().fg(color)))
        })
        .collect();

    frame.render_widget(Paragraph::new(visible), inner);
}

fn render_command_bar(frame: &mut ratatui::Frame, area: Rect, state: &DashboardState) {
    let commands = [
        ("F1", "Help"),
        ("F2", "Reset"),
        ("F3", "Restart"),
        ("F4", "LogLvl"),
        ("F5", "Sync"),
        ("F10", "Quit"),
    ];

    let mut spans = Vec::new();
    for (key, label) in &commands {
        spans.push(Span::styled(
            format!(" {key} "),
            Style::default().fg(Color::Black).bg(Color::Cyan),
        ));
        spans.push(Span::raw(format!("{label} ")));
    }

    if !state.pipeline_running {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            " Pipeline stopped — F3 to restart ",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ));
    }

    frame.render_widget(Paragraph::new(vec![Line::from(spans)]), area);
}

fn render_help_overlay(frame: &mut ratatui::Frame) {
    let area = frame.area();
    let width = 72.min(area.width.saturating_sub(4));
    let height = 38.min(area.height.saturating_sub(2));
    let x = (area.width.saturating_sub(width)) / 2;
    let y = (area.height.saturating_sub(height)) / 2;

    let popup = Rect::new(x, y, width, height);

    frame.render_widget(Clear, popup);
    let block = Block::default()
        .title(" Help (press any key to close) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let text: Vec<Line> =
        commands::HELP_TEXT.lines().map(|l| Line::from(Span::raw(l.to_owned()))).collect();

    frame.render_widget(Paragraph::new(text).wrap(Wrap { trim: false }), inner);
}

fn phase_color(label: &str) -> Color {
    match label {
        "init" => Color::DarkGray,
        "copying" | "data_sync" => Color::Green,
        "copied" | "finished_copy" => Color::Blue,
        "sync_wait" | "catchup" | "sync_done" => Color::Blue,
        "ready" => Color::Cyan,
        "errored" => Color::Red,
        _ => Color::White,
    }
}

fn phase_indicator(table: &TableInfo) -> Span<'static> {
    if table.is_copying() {
        Span::styled("● copying", Style::default().fg(Color::Green))
    } else if table.is_ready() {
        Span::styled("● CDC", Style::default().fg(Color::Cyan))
    } else if table.is_errored() {
        Span::styled("● error", Style::default().fg(Color::Red))
    } else {
        Span::styled("●", Style::default().fg(Color::DarkGray))
    }
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max { s.to_owned() } else { format!("{}…", &s[..max - 1]) }
}

pub fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn format_compact(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
