use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use etl::{
    state::TableReplicationPhase,
    store::{PostgresStore, StateStore},
    types::TableId,
};
use etl_destinations::snowflake::{OffsetToken, SnowflakeDestination};

/// Per-table information displayed in the table list and detail panel.
#[derive(Clone)]
#[allow(dead_code)]
pub struct TableInfo {
    pub table_id: TableId,
    pub destination_name: String,
    pub phase: TableReplicationPhase,
    pub rows_synced: u64,
    pub throughput: f64,
    pub local_offset: Option<String>,
    pub snowflake_offset: Option<String>,
    pub last_known_lsn: Option<String>,
    pub error_reason: Option<String>,
    pub error_solution: Option<String>,
    pub phase_entered_at: Option<Instant>,
}

impl TableInfo {
    pub fn phase_label(&self) -> &'static str {
        match &self.phase {
            TableReplicationPhase::Init => "init",
            TableReplicationPhase::DataSync => "copying",
            TableReplicationPhase::FinishedCopy => "copied",
            TableReplicationPhase::SyncWait { .. } => "sync_wait",
            TableReplicationPhase::Catchup { .. } => "catchup",
            TableReplicationPhase::SyncDone { .. } => "sync_done",
            TableReplicationPhase::Ready => "ready",
            TableReplicationPhase::Errored { .. } => "errored",
        }
    }

    pub fn is_errored(&self) -> bool {
        matches!(&self.phase, TableReplicationPhase::Errored { .. })
    }

    pub fn is_copying(&self) -> bool {
        matches!(&self.phase, TableReplicationPhase::DataSync | TableReplicationPhase::Init)
    }

    pub fn is_ready(&self) -> bool {
        matches!(&self.phase, TableReplicationPhase::Ready)
    }
}

/// Global dashboard state shared between the monitor task and the TUI render
/// loop.
#[allow(dead_code)]
pub struct DashboardState {
    pub tables: Vec<TableInfo>,
    pub selected_table: usize,

    pub total_rows_synced: u64,
    pub total_copy_rows: u64,
    pub total_cdc_events: u64,
    pub estimated_rows: u64,
    pub table_count: u64,

    pub elapsed: Duration,
    pub phase: GlobalPhase,
    pub copy_elapsed: Option<Duration>,

    pub throughput_current: f64,
    pub throughput_avg: f64,
    pub throughput_min: f64,
    pub throughput_max: f64,

    pub api_calls: u64,
    pub api_errors: u64,
    pub channel_recoveries: u64,

    pub log_scroll: usize,
    pub show_help: bool,
    pub status_message: Option<(String, Instant)>,
    pub log_level_filter: LogLevel,

    pub pipeline_running: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum GlobalPhase {
    TableCopy,
    CdcStreaming,
    Stopped,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    All,
    WarnAndAbove,
    ErrorOnly,
}

impl LogLevel {
    pub fn next(self) -> Self {
        match self {
            Self::All => Self::WarnAndAbove,
            Self::WarnAndAbove => Self::ErrorOnly,
            Self::ErrorOnly => Self::All,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::WarnAndAbove => "Warn+",
            Self::ErrorOnly => "Error",
        }
    }
}

impl GlobalPhase {
    pub fn label(&self) -> &'static str {
        match self {
            Self::TableCopy => "Table Copy",
            Self::CdcStreaming => "CDC Streaming",
            Self::Stopped => "Stopped",
        }
    }
}

#[allow(dead_code)]
impl DashboardState {
    pub fn new(table_count: u64, estimated_rows: u64) -> Self {
        Self {
            tables: Vec::new(),
            selected_table: 0,
            total_rows_synced: 0,
            total_copy_rows: 0,
            total_cdc_events: 0,
            estimated_rows,
            table_count,
            elapsed: Duration::ZERO,
            phase: GlobalPhase::TableCopy,
            copy_elapsed: None,
            throughput_current: 0.0,
            throughput_avg: 0.0,
            throughput_min: 0.0,
            throughput_max: 0.0,
            api_calls: 0,
            api_errors: 0,
            channel_recoveries: 0,
            log_scroll: 0,
            show_help: false,
            status_message: None,
            log_level_filter: LogLevel::All,
            pipeline_running: true,
        }
    }

    pub fn selected_table_info(&self) -> Option<&TableInfo> {
        self.tables.get(self.selected_table)
    }

    pub fn selected_table_id(&self) -> Option<TableId> {
        self.tables.get(self.selected_table).map(|t| t.table_id)
    }

    pub fn select_next(&mut self) {
        if !self.tables.is_empty() {
            self.selected_table = (self.selected_table + 1).min(self.tables.len() - 1);
        }
    }

    pub fn select_prev(&mut self) {
        self.selected_table = self.selected_table.saturating_sub(1);
    }

    pub fn set_status(&mut self, msg: String) {
        self.status_message = Some((msg, Instant::now()));
    }

    pub fn clear_stale_status(&mut self) {
        if let Some((_, at)) = &self.status_message {
            if at.elapsed() > Duration::from_secs(10) {
                self.status_message = None;
            }
        }
    }
}

pub fn parse_prometheus_metric(text: &str, metric_name: &str) -> Option<f64> {
    let prefix = format!("{metric_name} ");
    text.lines()
        .find(|line| line.starts_with(&prefix))
        .and_then(|line| line[prefix.len()..].trim().parse::<f64>().ok())
}

pub fn parse_prometheus_metric_sum(text: &str, metric_name: &str) -> Option<f64> {
    let mut total = 0.0;
    let mut found = false;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(metric_name) {
            if rest.starts_with(' ') || rest.starts_with('{') {
                if let Some(val) =
                    rest.split_whitespace().last().and_then(|v| v.parse::<f64>().ok())
                {
                    total += val;
                    found = true;
                }
            }
        }
    }
    found.then_some(total)
}

pub fn parse_prometheus_metric_with_label(
    text: &str,
    metric_name: &str,
    label_key: &str,
    label_value: &str,
) -> Option<f64> {
    let needle = format!("{label_key}=\"{label_value}\"");
    let mut total = 0.0;
    let mut found = false;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(metric_name) {
            if rest.starts_with('{') && rest.contains(&needle) {
                if let Some(val) =
                    rest.split_whitespace().last().and_then(|v| v.parse::<f64>().ok())
                {
                    total += val;
                    found = true;
                }
            }
        }
    }
    found.then_some(total)
}

/// Update dashboard state from Prometheus metrics and the state store.
#[allow(clippy::too_many_arguments)]
pub async fn refresh_dashboard(
    dashboard: &Arc<Mutex<DashboardState>>,
    store: &PostgresStore,
    metrics_text: &str,
    start_time: Instant,
    samples: &mut Vec<f64>,
    last_rows: &mut u64,
    last_tick: &mut Instant,
    table_names: &BTreeMap<TableId, String>,
    phase_times: &mut BTreeMap<TableId, (String, Instant)>,
) {
    let elapsed = start_time.elapsed();

    let rows_synced =
        parse_prometheus_metric(metrics_text, "etl_snowflake_batch_size_sum").unwrap_or(0.0) as u64;
    let copy_rows = parse_prometheus_metric_with_label(
        metrics_text,
        "etl_events_processed_total",
        "action",
        "table_copy",
    )
    .unwrap_or(0.0) as u64;
    let cdc_events = parse_prometheus_metric_with_label(
        metrics_text,
        "etl_events_processed_total",
        "action",
        "table_streaming",
    )
    .unwrap_or(0.0) as u64;
    let api_errors = parse_prometheus_metric_sum(metrics_text, "etl_snowflake_insert_errors_total")
        .unwrap_or(0.0) as u64;
    let channel_recoveries =
        parse_prometheus_metric_sum(metrics_text, "etl_snowflake_channel_recoveries_total")
            .unwrap_or(0.0) as u64;

    let tick_elapsed = last_tick.elapsed().as_secs_f64();
    let delta = rows_synced.saturating_sub(*last_rows);
    let throughput_current = if tick_elapsed > 0.0 { delta as f64 / tick_elapsed } else { 0.0 };
    *last_rows = rows_synced;
    *last_tick = Instant::now();

    if throughput_current > 0.0 {
        samples.push(throughput_current);
    }
    let throughput_avg =
        if samples.is_empty() { 0.0 } else { samples.iter().sum::<f64>() / samples.len() as f64 };
    let throughput_min = samples.iter().copied().fold(f64::MAX, f64::min);
    let throughput_max = samples.iter().copied().fold(0.0f64, f64::max);

    let states = store.get_table_replication_states().await.ok();

    let mut table_infos: Vec<TableInfo> = Vec::new();
    let mut all_ready = true;
    let mut any_table = false;

    // Preserve per-table state from previous refresh cycle
    let prev_table_state: BTreeMap<TableId, (Option<String>, Option<String>)> = {
        let dash = dashboard.lock().unwrap();
        dash.tables
            .iter()
            .map(|t| (t.table_id, (t.last_known_lsn.clone(), t.snowflake_offset.clone())))
            .collect()
    };

    if let Some(ref states) = states {
        for (id, phase) in states.iter() {
            any_table = true;
            let dest_name = table_names.get(id).cloned().unwrap_or_else(|| format!("{id}"));

            let phase_label = phase_label_str(phase);
            let prev = phase_times.get(id).map(|(l, _)| l.as_str());
            if prev != Some(phase_label) {
                phase_times.insert(*id, (phase_label.to_owned(), Instant::now()));
            }
            let phase_entered = phase_times.get(id).map(|(_, t)| *t);

            let (error_reason, error_solution) =
                if let TableReplicationPhase::Errored { reason, solution, .. } = phase {
                    all_ready = false;
                    (Some(reason.clone()), solution.clone())
                } else {
                    if !matches!(phase, TableReplicationPhase::Ready) {
                        all_ready = false;
                    }
                    (None, None)
                };

            let current_lsn = extract_lsn(phase);
            let (prev_lsn, prev_sf_offset) =
                prev_table_state.get(id).cloned().unwrap_or((None, None));
            let last_known_lsn = current_lsn.clone().or(prev_lsn);

            table_infos.push(TableInfo {
                table_id: *id,
                destination_name: dest_name,
                phase: phase.clone(),
                rows_synced: 0,
                throughput: 0.0,
                local_offset: current_lsn.or_else(|| offset_description(phase)),
                snowflake_offset: prev_sf_offset,
                last_known_lsn,
                error_reason,
                error_solution,
                phase_entered_at: phase_entered,
            });
        }
    }

    let global_phase =
        if !any_table || !all_ready { GlobalPhase::TableCopy } else { GlobalPhase::CdcStreaming };

    let mut dash = dashboard.lock().unwrap();
    let prev_selected = dash.selected_table;
    dash.elapsed = elapsed;
    dash.total_rows_synced = rows_synced;
    dash.total_copy_rows = copy_rows;
    dash.total_cdc_events = cdc_events;
    dash.throughput_current = throughput_current;
    dash.throughput_avg = throughput_avg;
    dash.throughput_min = if throughput_min == f64::MAX { 0.0 } else { throughput_min };
    dash.throughput_max = throughput_max;
    dash.api_errors = api_errors;
    dash.channel_recoveries = channel_recoveries;
    dash.tables = table_infos;
    dash.selected_table = prev_selected.min(dash.tables.len().saturating_sub(1));

    let was_copying = dash.phase == GlobalPhase::TableCopy;
    if was_copying && global_phase == GlobalPhase::CdcStreaming {
        dash.copy_elapsed = Some(elapsed);
    }
    if dash.pipeline_running {
        dash.phase = global_phase;
    }

    dash.clear_stale_status();
}

fn phase_label_str(phase: &TableReplicationPhase) -> &'static str {
    match phase {
        TableReplicationPhase::Init => "init",
        TableReplicationPhase::DataSync => "data_sync",
        TableReplicationPhase::FinishedCopy => "finished_copy",
        TableReplicationPhase::SyncWait { .. } => "sync_wait",
        TableReplicationPhase::Catchup { .. } => "catchup",
        TableReplicationPhase::SyncDone { .. } => "sync_done",
        TableReplicationPhase::Ready => "ready",
        TableReplicationPhase::Errored { .. } => "errored",
    }
}

fn extract_lsn(phase: &TableReplicationPhase) -> Option<String> {
    match phase {
        TableReplicationPhase::SyncWait { lsn }
        | TableReplicationPhase::Catchup { lsn }
        | TableReplicationPhase::SyncDone { lsn } => Some(format!("{lsn}")),
        _ => None,
    }
}

fn offset_description(phase: &TableReplicationPhase) -> Option<String> {
    match phase {
        TableReplicationPhase::Init => Some("not started".to_owned()),
        TableReplicationPhase::DataSync => Some("copying...".to_owned()),
        TableReplicationPhase::FinishedCopy => Some("copy done, syncing".to_owned()),
        TableReplicationPhase::Ready => Some("streaming".to_owned()),
        _ => None,
    }
}

/// Build a map from TableId to destination table name by querying the store.
pub async fn build_table_name_map(store: &PostgresStore) -> BTreeMap<TableId, String> {
    let mut map = BTreeMap::new();
    let states = store.get_table_replication_states().await.ok();
    if let Some(states) = states {
        for id in states.keys() {
            if let Ok(Some(meta)) = store.get_destination_table_metadata(*id).await {
                map.insert(*id, meta.destination_table_id);
            }
        }
    }
    map
}

/// Fetch Snowflake committed offsets for all tables.
pub async fn fetch_snowflake_offsets(
    dashboard: &Arc<Mutex<DashboardState>>,
    destination: &SnowflakeDestination<PostgresStore>,
) {
    let table_ids: Vec<TableId> = {
        let dash = dashboard.lock().unwrap();
        dash.tables.iter().map(|t| t.table_id).collect()
    };

    let mut offsets: BTreeMap<TableId, Option<OffsetToken>> = BTreeMap::new();
    for id in &table_ids {
        match destination.committed_offset(*id).await {
            Ok(offset) => {
                offsets.insert(*id, offset);
            }
            Err(_) => {
                offsets.insert(*id, None);
            }
        }
    }

    let mut dash = dashboard.lock().unwrap();
    for table in &mut dash.tables {
        if let Some(offset) = offsets.get(&table.table_id) {
            table.snowflake_offset = offset.as_ref().map(|o| format!("{o}"));
        }
    }
}
