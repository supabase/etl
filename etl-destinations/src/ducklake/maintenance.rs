use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Duration;

use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use metrics::{counter, histogram};
use parking_lot::Mutex;
use pg_escape::quote_literal;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{info, warn};

use crate::ducklake::client::{
    DuckDbBlockingOperationKind, DuckLakeConnectionManager, build_warm_ducklake_pool,
    format_query_error_detail, run_duckdb_blocking,
};
use crate::ducklake::core::{DuckLakeTableBatchKind, flush_table_inlined_data};
use crate::ducklake::metrics::{
    ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS, ETL_DUCKLAKE_MAINTENANCE_SKIPPED_TOTAL,
    MAINTENANCE_OPERATION_LABEL, MAINTENANCE_OUTCOME_LABEL, MAINTENANCE_REASON_LABEL,
    MAINTENANCE_TASK_LABEL,
};
use crate::ducklake::{ATTACH_DATA_INLINING_ROW_LIMIT, DuckLakeTableName, LAKE_CATALOG};

/// Dedicated pool size for background DuckLake maintenance work.
const MAINTENANCE_POOL_SIZE: u32 = 1;
/// Poll interval for checking per-table inline flush thresholds.
const MAINTENANCE_FLUSH_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// How long a table can stay idle before pending inlined data is materialized.
const MAINTENANCE_IDLE_FLUSH_THRESHOLD: Duration = Duration::from_secs(60 * 5);
/// Pending bytes threshold that triggers a background inline flush.
const MAINTENANCE_PENDING_BYTES_THRESHOLD: u64 = 5_000_000; // 5MB
/// Pending inserted rows threshold that triggers a background inline flush.
const MAINTENANCE_PENDING_ROWS_THRESHOLD: u64 = ATTACH_DATA_INLINING_ROW_LIMIT - 100;
/// Minimum idle window before targeted table maintenance runs, to not have maintenances ran too frequently.
const MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD: Duration = Duration::from_secs(90);
/// Minimum delay between targeted maintenance runs for the same table.
const MAINTENANCE_TABLE_COMPACTION_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Global checkpoint interval used to keep catalog maintenance moving.
const MAINTENANCE_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(15 * 60);
/// Timeout for sending a notification to the maintenance worker.
pub(super) const NOTIFICATION_SEND_TIMEOUT: Duration = Duration::from_secs(5);

const MAINTENANCE_TASK_FLUSH: &str = "flush";
const MAINTENANCE_TASK_TARGETED_MAINTENANCE: &str = "targeted_maintenance";
const MAINTENANCE_TASK_CHECKPOINT: &str = "checkpoint";

#[cfg(test)]
static FAIL_CHECKPOINT_ONCE_FOR_TESTS: AtomicBool = AtomicBool::new(false);

/// Concrete DuckLake maintenance operations emitted in metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceOperation {
    FlushInlinedData,
    RewriteDataFiles,
    MergeAdjacentFiles,
    Checkpoint,
}

impl MaintenanceOperation {
    /// Returns the stable metric label value for this operation.
    fn as_str(self) -> &'static str {
        match self {
            Self::FlushInlinedData => "flush_inlined_data",
            Self::RewriteDataFiles => "rewrite_data_files",
            Self::MergeAdjacentFiles => "merge_adjacent_files",
            Self::Checkpoint => "checkpoint",
        }
    }
}

/// Primary reasons that schedule one background maintenance decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceReason {
    PendingBytesThreshold,
    PendingInsertedRowsThreshold,
    IdleFlushThreshold,
    DirtyIdleWindow,
    CheckpointInterval,
}

impl MaintenanceReason {
    /// Returns the stable metric label value for this reason.
    fn as_str(self) -> &'static str {
        match self {
            Self::PendingBytesThreshold => "pending_bytes_threshold",
            Self::PendingInsertedRowsThreshold => "pending_inserted_rows_threshold",
            Self::IdleFlushThreshold => "idle_flush_threshold",
            Self::DirtyIdleWindow => "dirty_idle_window",
            Self::CheckpointInterval => "checkpoint_interval",
        }
    }
}

/// Outcome for one maintenance operation attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceOutcome {
    Applied,
    Noop,
    SkippedBusy,
    Failed,
}

impl MaintenanceOutcome {
    /// Returns the stable metric label value for this outcome.
    fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Noop => "noop",
            Self::SkippedBusy => "skipped_busy",
            Self::Failed => "failed",
        }
    }

    /// Returns whether the maintenance cycle completed successfully.
    fn is_completed(self) -> bool {
        matches!(self, Self::Applied | Self::Noop)
    }
}

impl From<u64> for MaintenanceOutcome {
    fn from(value: u64) -> Self {
        if value > 0 { Self::Applied } else { Self::Noop }
    }
}

/// Per-table write activity sent to the background maintenance worker.
#[derive(Clone, Debug, Default)]
pub(super) struct TableMaintenanceNotification {
    pub(super) table_name: DuckLakeTableName,
    pub(super) approx_bytes: u64,
    pub(super) inserted_rows: u64,
}

/// Coalesced maintenance state for one DuckLake table.
#[derive(Debug)]
struct TableMaintenanceState {
    pending_bytes: u64,
    pending_inserted_rows: u64,
    dirty_since_compaction: bool,
    last_write_at: Instant,
    last_compaction_at: Option<Instant>,
}

impl TableMaintenanceState {
    /// Aggregates one write notification into the existing table state.
    fn record(&mut self, notification: &TableMaintenanceNotification, now: Instant) {
        self.pending_bytes = self.pending_bytes.saturating_add(notification.approx_bytes);
        self.pending_inserted_rows = self
            .pending_inserted_rows
            .saturating_add(notification.inserted_rows);
        self.dirty_since_compaction = true;
        self.last_write_at = now;
    }

    /// Returns whether the table still has pending inline work.
    fn has_pending_flush_work(&self) -> bool {
        self.pending_bytes > 0 || self.pending_inserted_rows > 0
    }

    /// Returns the primary reason that pending inlined work should be flushed.
    fn flush_reason(&self, now: Instant) -> Option<MaintenanceReason> {
        if self.pending_bytes >= MAINTENANCE_PENDING_BYTES_THRESHOLD {
            return Some(MaintenanceReason::PendingBytesThreshold);
        }

        if self.pending_inserted_rows >= MAINTENANCE_PENDING_ROWS_THRESHOLD {
            return Some(MaintenanceReason::PendingInsertedRowsThreshold);
        }

        let idle = now.saturating_duration_since(self.last_write_at);
        if self.has_pending_flush_work() && idle >= MAINTENANCE_IDLE_FLUSH_THRESHOLD {
            return Some(MaintenanceReason::IdleFlushThreshold);
        }

        None
    }

    /// Clears pending flush counters after a successful flush/materialization.
    fn clear_pending_flush(&mut self) {
        self.pending_bytes = 0;
        self.pending_inserted_rows = 0;
    }

    /// Returns the reason targeted maintenance should run for this table.
    fn compact_reason(&self, now: Instant) -> Option<MaintenanceReason> {
        if !self.dirty_since_compaction {
            return None;
        }

        let idle = now.saturating_duration_since(self.last_write_at);
        let enough_idle = idle >= MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD;
        let enough_gap = match self.last_compaction_at {
            Some(last) => {
                now.saturating_duration_since(last) >= MAINTENANCE_TABLE_COMPACTION_INTERVAL
            }
            None => true,
        };

        if enough_idle && enough_gap {
            Some(MaintenanceReason::DirtyIdleWindow)
        } else {
            None
        }
    }
}

/// Shared state for the background DuckLake maintenance worker.
pub(super) struct DuckLakeMaintenanceWorker {
    pub(super) notification_tx: mpsc::Sender<TableMaintenanceNotification>,
    pub(super) shutdown_tx: watch::Sender<()>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

/// Records one DuckLake background maintenance operation duration sample.
fn record_ducklake_maintenance_duration(
    task: &'static str,
    operation: MaintenanceOperation,
    reason: MaintenanceReason,
    outcome: MaintenanceOutcome,
    duration_seconds: f64,
) {
    debug_assert_ne!(outcome, MaintenanceOutcome::SkippedBusy);
    histogram!(
        ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS,
        MAINTENANCE_TASK_LABEL => task,
        MAINTENANCE_OPERATION_LABEL => operation.as_str(),
        MAINTENANCE_REASON_LABEL => reason.as_str(),
        MAINTENANCE_OUTCOME_LABEL => outcome.as_str(),
    )
    .record(duration_seconds);
}

/// Records one DuckLake background maintenance operation skip.
fn record_ducklake_maintenance_skipped(
    task: &'static str,
    operation: MaintenanceOperation,
    reason: MaintenanceReason,
) {
    counter!(
        ETL_DUCKLAKE_MAINTENANCE_SKIPPED_TOTAL,
        MAINTENANCE_TASK_LABEL => task,
        MAINTENANCE_OPERATION_LABEL => operation.as_str(),
        MAINTENANCE_REASON_LABEL => reason.as_str(),
    )
    .increment(1);
}

/// Records both targeted-maintenance operations as skipped because the table is busy.
fn record_skipped_targeted_maintenance(reason: MaintenanceReason) {
    for operation in [
        MaintenanceOperation::RewriteDataFiles,
        MaintenanceOperation::MergeAdjacentFiles,
    ] {
        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            operation,
            reason,
        );
    }
}

/// Returns the targeted-maintenance cycle outcome from successful per-operation outcomes.
fn targeted_maintenance_outcome(
    rewrite_outcome: MaintenanceOutcome,
    merge_outcome: MaintenanceOutcome,
) -> MaintenanceOutcome {
    debug_assert!(rewrite_outcome.is_completed());
    debug_assert!(merge_outcome.is_completed());

    if rewrite_outcome == MaintenanceOutcome::Applied
        || merge_outcome == MaintenanceOutcome::Applied
    {
        MaintenanceOutcome::Applied
    } else {
        MaintenanceOutcome::Noop
    }
}

/// Builds the dedicated maintenance pool and spawns the periodic DuckLake worker.
pub(super) async fn spawn_ducklake_maintenance_worker(
    manager: DuckLakeConnectionManager,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
) -> EtlResult<DuckLakeMaintenanceWorker> {
    let pool =
        Arc::new(build_warm_ducklake_pool(manager, MAINTENANCE_POOL_SIZE, "maintenance").await?);
    let blocking_slots = Arc::new(Semaphore::new(MAINTENANCE_POOL_SIZE as usize));
    let (notification_tx, notification_rx) = mpsc::channel(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let handle = tokio::spawn(run_ducklake_maintenance_worker(
        pool,
        blocking_slots,
        table_write_slots,
        notification_rx,
        shutdown_rx,
    ));

    Ok(DuckLakeMaintenanceWorker {
        notification_tx,
        shutdown_tx,
        handle: Mutex::new(handle.into()),
    })
}

/// Coalesces write notifications and runs background DuckLake maintenance.
async fn run_ducklake_maintenance_worker(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    mut notification_rx: mpsc::Receiver<TableMaintenanceNotification>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut flush_interval = tokio::time::interval(MAINTENANCE_FLUSH_POLL_INTERVAL);
    flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut checkpoint_interval = tokio::time::interval_at(
        Instant::now() + MAINTENANCE_CHECKPOINT_INTERVAL,
        MAINTENANCE_CHECKPOINT_INTERVAL,
    );
    checkpoint_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut table_states: HashMap<DuckLakeTableName, TableMaintenanceState> = HashMap::new();

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                info!("ducklake maintenance worker shutting down");
                break;
            }
            maybe_notification = notification_rx.recv() => {
                let Some(notification) = maybe_notification else {
                    info!("ducklake maintenance worker channel closed");
                    break;
                };

                let now = Instant::now();
                table_states
                    .entry(notification.table_name.clone())
                    .and_modify(|state| state.record(&notification, now))
                    .or_insert_with(|| {
                        let mut state = TableMaintenanceState {
                            pending_bytes: 0,
                            pending_inserted_rows: 0,
                            dirty_since_compaction: true,
                            last_write_at: now,
                            last_compaction_at: None,
                        };
                        state.record(&notification, now);
                        state
                    });
            }
            _ = flush_interval.tick() => {
                let mut now = Instant::now();

                for (table_name, table_state) in &mut table_states {
                    // If it needs to be flushed
                    if let Some(reason) = table_state.flush_reason(now) {
                        match flush_table_inlined_data_in_background(
                            Arc::clone(&pool),
                            Arc::clone(&blocking_slots),
                            Arc::clone(&table_write_slots),
                            table_name.clone(),
                            reason,
                        )
                        .await
                        {
                            Ok(outcome) => {
                                if outcome.is_completed() {
                                    table_state.clear_pending_flush();
                                }
                            }
                            Err(error) => {
                                warn!(
                                    table = %table_name,
                                    reason = reason.as_str(),
                                    error = ?error,
                                    "ducklake background flush failed"
                                );
                            }
                        }
                    }
                    now = Instant::now();
                    // If it needs to be compacted
                    if let Some(reason) = table_state.compact_reason(now) {
                        match run_targeted_table_maintenance(
                            Arc::clone(&pool),
                            Arc::clone(&blocking_slots),
                            Arc::clone(&table_write_slots),
                            table_name.clone(),
                            reason,
                        )
                        .await
                        {
                            Ok(outcome) => {
                                if outcome.is_completed() {
                                    table_state.dirty_since_compaction = false;
                                    table_state.last_compaction_at = Some(Instant::now());
                                }
                            }
                            Err(error) => {
                                warn!(
                                    table = %table_name,
                                    reason = reason.as_str(),
                                    error = ?error,
                                    "ducklake targeted maintenance failed"
                                );
                            }
                        }
                    }
                }

                table_states.retain(|_, state| {
                    state.has_pending_flush_work() || state.dirty_since_compaction
                });
            }
            _ = checkpoint_interval.tick() => {
                if table_states.is_empty() {
                    continue;
                }

                if let Err(error) = run_background_checkpoint(
                    Arc::clone(&pool),
                    Arc::clone(&blocking_slots),
                    MaintenanceReason::CheckpointInterval,
                )
                .await
                {
                    warn!(
                        reason = MaintenanceReason::CheckpointInterval.as_str(),
                        error = ?error,
                        "ducklake background checkpoint failed"
                    );
                }
            }
        }
    }
}

/// Returns the table-local semaphore shared by writes and background maintenance.
pub(super) fn table_write_slot(
    table_write_slots: &Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: &str,
) -> Arc<Semaphore> {
    let mut slots = table_write_slots.lock();
    slots
        .entry(table_name.to_string())
        .or_insert_with(|| Arc::new(Semaphore::new(1)))
        .clone()
}

/// Tries to acquire the table-local semaphore without blocking the maintenance worker.
fn try_acquire_table_write_slot(
    table_write_slots: &Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: &str,
) -> Option<OwnedSemaphorePermit> {
    table_write_slot(table_write_slots, table_name)
        .try_acquire_owned()
        .ok()
}

/// Materializes one table's pending inlined rows on the maintenance pool.
async fn flush_table_inlined_data_in_background(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: DuckLakeTableName,
    reason: MaintenanceReason,
) -> EtlResult<MaintenanceOutcome> {
    let Some(table_write_permit) = try_acquire_table_write_slot(&table_write_slots, &table_name)
    else {
        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            reason,
        );
        return Ok(MaintenanceOutcome::SkippedBusy);
    };

    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        move |conn| {
            let _table_write_permit = table_write_permit;
            flush_table_inlined_data_in_background_blocking(conn, &table_name, reason)
        },
    )
    .await
}

/// Runs targeted rewrite and merge maintenance for one table.
async fn run_targeted_table_maintenance(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: DuckLakeTableName,
    reason: MaintenanceReason,
) -> EtlResult<MaintenanceOutcome> {
    let Some(table_write_permit) = try_acquire_table_write_slot(&table_write_slots, &table_name)
    else {
        record_skipped_targeted_maintenance(reason);
        return Ok(MaintenanceOutcome::SkippedBusy);
    };

    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        move |conn| {
            let _table_write_permit = table_write_permit;
            run_targeted_table_maintenance_blocking(conn, &table_name, reason)
        },
    )
    .await
}

/// Runs a coarse-grained checkpoint to keep catalog maintenance moving.
async fn run_background_checkpoint(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    reason: MaintenanceReason,
) -> EtlResult<()> {
    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        move |conn| run_background_checkpoint_blocking(conn, reason),
    )
    .await
}

/// Materializes one table's pending inlined rows and records the maintenance outcome.
fn flush_table_inlined_data_in_background_blocking(
    conn: &duckdb::Connection,
    table_name: &str,
    reason: MaintenanceReason,
) -> EtlResult<MaintenanceOutcome> {
    let flush_started = Instant::now();
    let rows_flushed = flush_table_inlined_data(conn, table_name, DuckLakeTableBatchKind::Mutation)
        .inspect_err(|_error| {
            record_ducklake_maintenance_duration(
                MAINTENANCE_TASK_FLUSH,
                MaintenanceOperation::FlushInlinedData,
                reason,
                MaintenanceOutcome::Failed,
                flush_started.elapsed().as_secs_f64(),
            );
        })?;
    let outcome = MaintenanceOutcome::from(rows_flushed);
    record_ducklake_maintenance_duration(
        MAINTENANCE_TASK_FLUSH,
        MaintenanceOperation::FlushInlinedData,
        reason,
        outcome,
        flush_started.elapsed().as_secs_f64(),
    );
    Ok(outcome)
}

/// Runs targeted table maintenance and records per-operation outcomes.
fn run_targeted_table_maintenance_blocking(
    conn: &duckdb::Connection,
    table_name: &str,
    reason: MaintenanceReason,
) -> EtlResult<MaintenanceOutcome> {
    let rewrite_started = Instant::now();
    let rewritten_files = rewrite_table_data_files(conn, table_name).inspect_err(|_error| {
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            reason,
            MaintenanceOutcome::Failed,
            rewrite_started.elapsed().as_secs_f64(),
        );
    })?;
    let rewrite_outcome = MaintenanceOutcome::from(rewritten_files);
    record_ducklake_maintenance_duration(
        MAINTENANCE_TASK_TARGETED_MAINTENANCE,
        MaintenanceOperation::RewriteDataFiles,
        reason,
        rewrite_outcome,
        rewrite_started.elapsed().as_secs_f64(),
    );

    let merge_started = Instant::now();
    let merged_files = merge_adjacent_table_files(conn, table_name).inspect_err(|_error| {
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            reason,
            MaintenanceOutcome::Failed,
            merge_started.elapsed().as_secs_f64(),
        );
    })?;
    let merge_outcome = MaintenanceOutcome::from(merged_files);
    record_ducklake_maintenance_duration(
        MAINTENANCE_TASK_TARGETED_MAINTENANCE,
        MaintenanceOperation::MergeAdjacentFiles,
        reason,
        merge_outcome,
        merge_started.elapsed().as_secs_f64(),
    );

    info!(
        table = %table_name,
        rewritten_files,
        merged_files,
        "ducklake targeted maintenance completed"
    );

    Ok(targeted_maintenance_outcome(rewrite_outcome, merge_outcome))
}

/// Runs a coarse-grained checkpoint and records the maintenance outcome.
fn run_background_checkpoint_blocking(
    conn: &duckdb::Connection,
    reason: MaintenanceReason,
) -> EtlResult<()> {
    let checkpoint_started = Instant::now();
    #[cfg(test)]
    if FAIL_CHECKPOINT_ONCE_FOR_TESTS.swap(false, AtomicOrdering::Relaxed) {
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            reason,
            MaintenanceOutcome::Failed,
            checkpoint_started.elapsed().as_secs_f64(),
        );
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake checkpoint failed"
        ));
    }

    conn.execute_batch("CHECKPOINT").map_err(|error| {
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            reason,
            MaintenanceOutcome::Failed,
            checkpoint_started.elapsed().as_secs_f64(),
        );
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake checkpoint failed",
            source: error
        )
    })?;
    info!("ducklake background checkpoint completed");
    record_ducklake_maintenance_duration(
        MAINTENANCE_TASK_CHECKPOINT,
        MaintenanceOperation::Checkpoint,
        reason,
        MaintenanceOutcome::Applied,
        checkpoint_started.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// Rewrites one table's delete-heavy files and returns created file count.
fn rewrite_table_data_files(conn: &duckdb::Connection, table_name: &str) -> EtlResult<u64> {
    let sql = format!(
        r#"SELECT COALESCE(SUM(files_created), 0)
         FROM ducklake_rewrite_data_files({}, {});"#,
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
    );
    let files_created: i64 = conn
        .query_row(&sql, [], |row| row.get(0))
        .map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake rewrite data files failed",
                format_query_error_detail(&sql, &error),
                source: error
            )
        })?;

    Ok(files_created.max(0) as u64)
}

/// Merges one table's adjacent files and returns created file count.
fn merge_adjacent_table_files(conn: &duckdb::Connection, table_name: &str) -> EtlResult<u64> {
    let sql = format!(
        r#"SELECT COALESCE(SUM(files_created), 0)
         FROM ducklake_merge_adjacent_files({}, {});"#,
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
    );
    let files_created: i64 = conn
        .query_row(&sql, [], |row| row.get(0))
        .map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake merge adjacent files failed",
                format_query_error_detail(&sql, &error),
                source: error
            )
        })?;

    Ok(files_created.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    use etl_telemetry::metrics::init_metrics_handle;

    use crate::ducklake::metrics::register_metrics;

    fn maintenance_duration_count(
        rendered: &str,
        task: &str,
        operation: MaintenanceOperation,
        reason: MaintenanceReason,
        outcome: MaintenanceOutcome,
    ) -> f64 {
        let task_label = format!(r#"{MAINTENANCE_TASK_LABEL}="{task}""#);
        let operation_label = format!(r#"{MAINTENANCE_OPERATION_LABEL}="{}""#, operation.as_str());
        let reason_label = format!(r#"{MAINTENANCE_REASON_LABEL}="{}""#, reason.as_str());
        let outcome_label = format!(r#"{MAINTENANCE_OUTCOME_LABEL}="{}""#, outcome.as_str());

        rendered
            .lines()
            .find_map(|line| {
                if line.starts_with(&format!(
                    "{ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS}_count"
                )) && line.contains(&task_label)
                    && line.contains(&operation_label)
                    && line.contains(&reason_label)
                    && line.contains(&outcome_label)
                {
                    line.split_whitespace().last()?.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
    }

    fn maintenance_skipped_counter_value(
        rendered: &str,
        task: &str,
        operation: MaintenanceOperation,
        reason: MaintenanceReason,
    ) -> f64 {
        let task_label = format!(r#"{MAINTENANCE_TASK_LABEL}="{task}""#);
        let operation_label = format!(r#"{MAINTENANCE_OPERATION_LABEL}="{}""#, operation.as_str());
        let reason_label = format!(r#"{MAINTENANCE_REASON_LABEL}="{}""#, reason.as_str());

        rendered
            .lines()
            .find_map(|line| {
                if line.starts_with(ETL_DUCKLAKE_MAINTENANCE_SKIPPED_TOTAL)
                    && line.contains(&task_label)
                    && line.contains(&operation_label)
                    && line.contains(&reason_label)
                {
                    line.split_whitespace().last()?.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
    }

    #[tokio::test]
    async fn test_maintenance_duration_histogram_counts_are_exported_with_labels() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let flush_applied_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
            MaintenanceOutcome::Applied,
        );
        let flush_noop_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::IdleFlushThreshold,
            MaintenanceOutcome::Noop,
        );
        let rewrite_applied_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Applied,
        );
        let merge_noop_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Noop,
        );
        let checkpoint_failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            MaintenanceReason::CheckpointInterval,
            MaintenanceOutcome::Failed,
        );

        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
            MaintenanceOutcome::Applied,
            0.25,
        );
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::IdleFlushThreshold,
            MaintenanceOutcome::Noop,
            0.15,
        );
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Applied,
            1.0,
        );
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Noop,
            0.4,
        );
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            MaintenanceReason::CheckpointInterval,
            MaintenanceOutcome::Failed,
            0.05,
        );

        let rendered_after = handle.render();
        let flush_applied_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
            MaintenanceOutcome::Applied,
        );
        let flush_noop_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::IdleFlushThreshold,
            MaintenanceOutcome::Noop,
        );
        let rewrite_applied_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Applied,
        );
        let merge_noop_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Noop,
        );
        let checkpoint_failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            MaintenanceReason::CheckpointInterval,
            MaintenanceOutcome::Failed,
        );

        assert!(
            flush_applied_after > flush_applied_before,
            "flush applied duration count did not increase"
        );
        assert!(
            flush_noop_after > flush_noop_before,
            "flush noop duration count did not increase"
        );
        assert!(
            rewrite_applied_after > rewrite_applied_before,
            "rewrite applied duration count did not increase"
        );
        assert!(
            merge_noop_after > merge_noop_before,
            "merge noop duration count did not increase"
        );
        assert!(
            checkpoint_failed_after > checkpoint_failed_before,
            "checkpoint failed duration count did not increase"
        );
    }

    #[tokio::test]
    async fn test_targeted_maintenance_busy_emits_two_skip_counter_events_and_no_durations() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let rewrite_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
        );
        let merge_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
        );
        let rewrite_duration_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::SkippedBusy,
        );
        let merge_duration_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::SkippedBusy,
        );

        record_skipped_targeted_maintenance(MaintenanceReason::DirtyIdleWindow);

        let rendered_after = handle.render();
        let rewrite_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
        );
        let merge_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
        );
        let rewrite_duration_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::SkippedBusy,
        );
        let merge_duration_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::SkippedBusy,
        );

        assert!(
            rewrite_after > rewrite_before,
            "rewrite skipped counter did not increase"
        );
        assert!(
            merge_after > merge_before,
            "merge skipped counter did not increase"
        );
        assert_eq!(
            rewrite_duration_after, rewrite_duration_before,
            "rewrite skip should not emit a duration sample"
        );
        assert_eq!(
            merge_duration_after, merge_duration_before,
            "merge skip should not emit a duration sample"
        );
    }

    #[test]
    fn test_table_maintenance_state_prefers_pending_bytes_flush_reason() {
        let now = Instant::now();
        let state = TableMaintenanceState {
            pending_bytes: MAINTENANCE_PENDING_BYTES_THRESHOLD,
            pending_inserted_rows: MAINTENANCE_PENDING_ROWS_THRESHOLD,
            dirty_since_compaction: true,
            last_write_at: now - MAINTENANCE_IDLE_FLUSH_THRESHOLD,
            last_compaction_at: None,
        };

        assert_eq!(
            state.flush_reason(now),
            Some(MaintenanceReason::PendingBytesThreshold)
        );
    }

    #[test]
    fn test_table_maintenance_state_uses_pending_inserted_rows_flush_reason() {
        let now = Instant::now();
        let state = TableMaintenanceState {
            pending_bytes: MAINTENANCE_PENDING_BYTES_THRESHOLD - 1,
            pending_inserted_rows: MAINTENANCE_PENDING_ROWS_THRESHOLD,
            dirty_since_compaction: true,
            last_write_at: now,
            last_compaction_at: None,
        };

        assert_eq!(
            state.flush_reason(now),
            Some(MaintenanceReason::PendingInsertedRowsThreshold)
        );
    }

    #[test]
    fn test_table_maintenance_state_uses_idle_flush_reason() {
        let now = Instant::now();
        let state = TableMaintenanceState {
            pending_bytes: 128,
            pending_inserted_rows: 1,
            dirty_since_compaction: true,
            last_write_at: now,
            last_compaction_at: None,
        };

        assert_eq!(state.flush_reason(now + Duration::from_secs(5)), None);
        assert_eq!(
            state.flush_reason(now + MAINTENANCE_IDLE_FLUSH_THRESHOLD),
            Some(MaintenanceReason::IdleFlushThreshold)
        );
    }

    #[test]
    fn test_table_maintenance_state_compacts_only_when_idle_and_dirty() {
        let now = Instant::now();
        let state = TableMaintenanceState {
            pending_bytes: 0,
            pending_inserted_rows: 0,
            dirty_since_compaction: true,
            last_write_at: now,
            last_compaction_at: Some(now),
        };

        assert!(
            state
                .compact_reason(now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD)
                .is_none(),
            "compaction should wait for the minimum compaction interval"
        );
        assert_eq!(
            state.compact_reason(
                now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD
                    + MAINTENANCE_TABLE_COMPACTION_INTERVAL,
            ),
            Some(MaintenanceReason::DirtyIdleWindow)
        );
    }

    #[test]
    fn test_maintenance_outcome_from_rows_flushed_marks_applied_and_noop() {
        assert_eq!(MaintenanceOutcome::from(0), MaintenanceOutcome::Noop);
        assert_eq!(MaintenanceOutcome::from(3), MaintenanceOutcome::Applied);
    }

    #[test]
    fn test_maintenance_outcome_from_files_created_marks_applied_and_noop() {
        assert_eq!(MaintenanceOutcome::from(0), MaintenanceOutcome::Noop);
        assert_eq!(MaintenanceOutcome::from(2), MaintenanceOutcome::Applied);
    }

    #[test]
    fn test_targeted_maintenance_outcome_is_applied_when_any_operation_applies() {
        assert_eq!(
            targeted_maintenance_outcome(MaintenanceOutcome::Noop, MaintenanceOutcome::Noop),
            MaintenanceOutcome::Noop
        );
        assert_eq!(
            targeted_maintenance_outcome(MaintenanceOutcome::Applied, MaintenanceOutcome::Noop),
            MaintenanceOutcome::Applied
        );
        assert_eq!(
            targeted_maintenance_outcome(MaintenanceOutcome::Noop, MaintenanceOutcome::Applied),
            MaintenanceOutcome::Applied
        );
    }

    #[tokio::test]
    async fn test_flush_failure_records_failed_metric() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let conn = duckdb::Connection::open_in_memory().expect("failed to open in-memory duckdb");

        let rendered_before = handle.render();
        let failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
            MaintenanceOutcome::Failed,
        );

        let error = flush_table_inlined_data_in_background_blocking(
            &conn,
            "public_users",
            MaintenanceReason::PendingBytesThreshold,
        )
        .expect_err("flush should fail without ducklake functions");

        assert!(
            matches!(error.kind(), ErrorKind::DestinationQueryFailed),
            "unexpected error kind: {:?}",
            error.kind()
        );

        let rendered_after = handle.render();
        let failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
            MaintenanceOutcome::Failed,
        );

        assert!(
            failed_after > failed_before,
            "flush failed duration count did not increase"
        );
    }

    #[tokio::test]
    async fn test_rewrite_failure_records_duration_and_prevents_merge_duration() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let conn = duckdb::Connection::open_in_memory().expect("failed to open in-memory duckdb");

        let rendered_before = handle.render();
        let rewrite_failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Failed,
        );
        let merge_failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Failed,
        );

        let error = run_targeted_table_maintenance_blocking(
            &conn,
            "public_users",
            MaintenanceReason::DirtyIdleWindow,
        )
        .expect_err("targeted maintenance should fail without ducklake functions");

        assert!(
            matches!(error.kind(), ErrorKind::DestinationQueryFailed),
            "unexpected error kind: {:?}",
            error.kind()
        );

        let rendered_after = handle.render();
        let rewrite_failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Failed,
        );
        let merge_failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::DirtyIdleWindow,
            MaintenanceOutcome::Failed,
        );

        assert!(
            rewrite_failed_after > rewrite_failed_before,
            "rewrite failed duration count did not increase"
        );
        assert_eq!(
            merge_failed_after, merge_failed_before,
            "merge failed duration should not increase when rewrite fails first"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_success_records_applied_duration() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let conn = duckdb::Connection::open_in_memory().expect("failed to open in-memory duckdb");

        let rendered_before = handle.render();
        let applied_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            MaintenanceReason::CheckpointInterval,
            MaintenanceOutcome::Applied,
        );

        run_background_checkpoint_blocking(&conn, MaintenanceReason::CheckpointInterval)
            .expect("checkpoint should succeed");

        let rendered_after = handle.render();
        let applied_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            MaintenanceReason::CheckpointInterval,
            MaintenanceOutcome::Applied,
        );

        assert!(
            applied_after > applied_before,
            "checkpoint applied duration count did not increase"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_failure_records_failed_duration() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let conn = duckdb::Connection::open_in_memory().expect("failed to open in-memory duckdb");
        FAIL_CHECKPOINT_ONCE_FOR_TESTS.store(true, AtomicOrdering::Relaxed);

        let rendered_before = handle.render();
        let failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            MaintenanceReason::CheckpointInterval,
            MaintenanceOutcome::Failed,
        );

        let result =
            run_background_checkpoint_blocking(&conn, MaintenanceReason::CheckpointInterval);

        let rendered_after = handle.render();
        let failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_CHECKPOINT,
            MaintenanceOperation::Checkpoint,
            MaintenanceReason::CheckpointInterval,
            MaintenanceOutcome::Failed,
        );

        assert!(
            result.is_err(),
            "checkpoint should fail when the test hook is armed"
        );
        assert!(
            failed_after > failed_before,
            "checkpoint failed duration count did not increase"
        );
    }

    #[tokio::test]
    async fn test_flush_busy_emits_skip_counter_only() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let skipped_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
        );
        let duration_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
            MaintenanceOutcome::SkippedBusy,
        );

        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
        );

        let rendered_after = handle.render();
        let skipped_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
        );
        let duration_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingBytesThreshold,
            MaintenanceOutcome::SkippedBusy,
        );

        assert!(
            skipped_after > skipped_before,
            "flush skipped counter did not increase"
        );
        assert_eq!(
            duration_after, duration_before,
            "flush skip should not emit a duration sample"
        );
    }
}
