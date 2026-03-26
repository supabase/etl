use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use metrics::counter;
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
use crate::ducklake::metrics::{ETL_DUCKLAKE_MAINTENANCE_FAILURES_TOTAL, MAINTENANCE_TASK_LABEL};
use crate::ducklake::{ATTACH_DATA_INLINING_ROW_LIMIT, DuckLakeTableName, LAKE_CATALOG};

/// Dedicated pool size for background DuckLake maintenance work.
const MAINTENANCE_POOL_SIZE: u32 = 1;
/// Poll interval for checking per-table inline flush thresholds.
const MAINTENANCE_FLUSH_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// How long a table can stay idle before pending inlined data is materialized.
const MAINTENANCE_IDLE_FLUSH_THRESHOLD: Duration = Duration::from_secs(30);
/// Pending bytes threshold that triggers a background inline flush.
const MAINTENANCE_PENDING_BYTES_THRESHOLD: u64 = 5_000_000; // 5MB
/// Pending inserted rows threshold that triggers a background inline flush.
const MAINTENANCE_PENDING_ROWS_THRESHOLD: u64 = ATTACH_DATA_INLINING_ROW_LIMIT - 100;
/// Pending deleted rows threshold that triggers a background inline flush.
const MAINTENANCE_PENDING_DELETES_THRESHOLD: u64 = 500;
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

/// Per-table write activity sent to the background maintenance worker.
#[derive(Clone, Debug, Default)]
pub(super) struct TableMaintenanceNotification {
    pub(super) table_name: DuckLakeTableName,
    pub(super) approx_bytes: u64,
    pub(super) inserted_rows: u64,
    pub(super) deleted_rows: u64,
}

/// Coalesced maintenance state for one DuckLake table.
#[derive(Debug)]
struct TableMaintenanceState {
    pending_bytes: u64,
    pending_inserted_rows: u64,
    pending_deleted_rows: u64,
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
        self.pending_deleted_rows = self
            .pending_deleted_rows
            .saturating_add(notification.deleted_rows);
        self.dirty_since_compaction = true;
        self.last_write_at = now;
    }

    /// Returns whether pending inlined work should be flushed now.
    fn should_flush(&self, now: Instant) -> bool {
        let idle = now.saturating_duration_since(self.last_write_at);
        self.pending_bytes >= MAINTENANCE_PENDING_BYTES_THRESHOLD
            || self.pending_inserted_rows >= MAINTENANCE_PENDING_ROWS_THRESHOLD
            || self.pending_deleted_rows >= MAINTENANCE_PENDING_DELETES_THRESHOLD
            // If the table is inactive for a while then flush now, it's a good strategy to not be in conflicts with current transactions.
            || ((self.pending_bytes > 0
                || self.pending_inserted_rows > 0
                || self.pending_deleted_rows > 0)
                && idle >= MAINTENANCE_IDLE_FLUSH_THRESHOLD)
    }

    /// Clears pending flush counters after a successful flush/materialization.
    fn clear_pending_flush(&mut self) {
        self.pending_bytes = 0;
        self.pending_inserted_rows = 0;
        self.pending_deleted_rows = 0;
    }

    /// Returns whether targeted maintenance should run for this table.
    fn should_compact(&self, now: Instant) -> bool {
        if !self.dirty_since_compaction {
            return false;
        }

        let idle = now.saturating_duration_since(self.last_write_at);
        let enough_idle = idle >= MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD;
        let enough_gap = match self.last_compaction_at {
            Some(last) => {
                now.saturating_duration_since(last) >= MAINTENANCE_TABLE_COMPACTION_INTERVAL
            }
            None => true,
        };

        enough_idle && enough_gap
    }
}

/// Shared state for the background DuckLake maintenance worker.
pub(super) struct DuckLakeMaintenanceWorker {
    pub(super) notification_tx: mpsc::Sender<TableMaintenanceNotification>,
    pub(super) shutdown_tx: watch::Sender<()>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

/// Records one failed DuckLake background maintenance run.
fn record_ducklake_maintenance_failure(task: &'static str) {
    counter!(
        ETL_DUCKLAKE_MAINTENANCE_FAILURES_TOTAL,
        MAINTENANCE_TASK_LABEL => task,
    )
    .increment(1);
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
                            pending_deleted_rows: 0,
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
                    if table_state.should_flush(now) {
                        match flush_table_inlined_data_in_background(
                            Arc::clone(&pool),
                            Arc::clone(&blocking_slots),
                            Arc::clone(&table_write_slots),
                            table_name.clone(),
                        )
                        .await
                        {
                            Ok(flushed) => {
                                if flushed {
                                    table_state.clear_pending_flush();
                                }
                            }
                            Err(error) => {
                                record_ducklake_maintenance_failure(MAINTENANCE_TASK_FLUSH);
                                warn!(
                                    table = %table_name,
                                    error = ?error,
                                    "ducklake background flush failed"
                                );
                            }
                        }
                    }
                    now = Instant::now();
                    if table_state.should_compact(now) {
                        match run_targeted_table_maintenance(
                            Arc::clone(&pool),
                            Arc::clone(&blocking_slots),
                            Arc::clone(&table_write_slots),
                            table_name.clone(),
                        )
                        .await
                        {
                            Ok(compacted) => {
                                if compacted {
                                    table_state.dirty_since_compaction = false;
                                    table_state.last_compaction_at = Some(Instant::now());
                                }
                            }
                            Err(error) => {
                                record_ducklake_maintenance_failure(
                                    MAINTENANCE_TASK_TARGETED_MAINTENANCE,
                                );
                                warn!(
                                    table = %table_name,
                                    error = ?error,
                                    "ducklake targeted maintenance failed"
                                );
                            }
                        }
                    }
                }

                table_states.retain(|_, state| {
                    state.pending_bytes > 0
                        || state.pending_inserted_rows > 0
                        || state.pending_deleted_rows > 0
                        || state.dirty_since_compaction
                });
            }
            _ = checkpoint_interval.tick() => {
                if table_states.is_empty() {
                    continue;
                }

                if let Err(error) = run_background_checkpoint(
                    Arc::clone(&pool),
                    Arc::clone(&blocking_slots),
                )
                .await
                {
                    record_ducklake_maintenance_failure(MAINTENANCE_TASK_CHECKPOINT);
                    warn!(error = ?error, "ducklake background checkpoint failed");
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
) -> EtlResult<bool> {
    let Some(table_write_permit) = try_acquire_table_write_slot(&table_write_slots, &table_name)
    else {
        return Ok(false);
    };

    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        move |conn| {
            let _table_write_permit = table_write_permit;
            flush_table_inlined_data(conn, &table_name, DuckLakeTableBatchKind::Mutation)?;
            Ok(())
        },
    )
    .await?;

    Ok(true)
}

/// Runs targeted rewrite and merge maintenance for one table.
async fn run_targeted_table_maintenance(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: DuckLakeTableName,
) -> EtlResult<bool> {
    let Some(table_write_permit) = try_acquire_table_write_slot(&table_write_slots, &table_name)
    else {
        return Ok(false);
    };

    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        move |conn| {
            let _table_write_permit = table_write_permit;
            let rewritten_files = rewrite_table_data_files(conn, &table_name)?;
            let merged_files = merge_adjacent_table_files(conn, &table_name)?;
            info!(
                table = %table_name,
                rewritten_files,
                merged_files,
                "ducklake targeted maintenance completed"
            );
            Ok(())
        },
    )
    .await?;

    Ok(true)
}

/// Runs a coarse-grained checkpoint to keep catalog maintenance moving.
async fn run_background_checkpoint(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
) -> EtlResult<()> {
    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        |conn| {
            conn.execute_batch("CHECKPOINT").map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake checkpoint failed",
                    source: error
                )
            })?;
            info!("ducklake background checkpoint completed");
            Ok(())
        },
    )
    .await
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

    fn maintenance_failure_counter_value(rendered: &str, task: &str) -> f64 {
        let task_label = format!(r#"{MAINTENANCE_TASK_LABEL}="{task}""#);

        rendered
            .lines()
            .find_map(|line| {
                if line.starts_with(ETL_DUCKLAKE_MAINTENANCE_FAILURES_TOTAL)
                    && line.contains(&task_label)
                {
                    line.split_whitespace().last()?.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
    }

    #[tokio::test]
    async fn test_maintenance_failure_metrics_are_exported_by_task() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let flush_before =
            maintenance_failure_counter_value(&rendered_before, MAINTENANCE_TASK_FLUSH);
        let targeted_before = maintenance_failure_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
        );
        let checkpoint_before =
            maintenance_failure_counter_value(&rendered_before, MAINTENANCE_TASK_CHECKPOINT);

        record_ducklake_maintenance_failure(MAINTENANCE_TASK_FLUSH);
        record_ducklake_maintenance_failure(MAINTENANCE_TASK_TARGETED_MAINTENANCE);
        record_ducklake_maintenance_failure(MAINTENANCE_TASK_CHECKPOINT);

        let rendered_after = handle.render();
        let flush_after =
            maintenance_failure_counter_value(&rendered_after, MAINTENANCE_TASK_FLUSH);
        let targeted_after = maintenance_failure_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
        );
        let checkpoint_after =
            maintenance_failure_counter_value(&rendered_after, MAINTENANCE_TASK_CHECKPOINT);

        assert!(
            flush_after > flush_before,
            "flush failure counter did not increase"
        );
        assert!(
            targeted_after > targeted_before,
            "targeted maintenance failure counter did not increase"
        );
        assert!(
            checkpoint_after > checkpoint_before,
            "checkpoint failure counter did not increase"
        );
    }

    #[test]
    fn test_table_maintenance_state_flushes_on_row_threshold() {
        let now = Instant::now();
        let mut state = TableMaintenanceState {
            pending_bytes: 0,
            pending_inserted_rows: 0,
            pending_deleted_rows: 0,
            dirty_since_compaction: false,
            last_write_at: now,
            last_compaction_at: None,
        };
        let notification = TableMaintenanceNotification {
            table_name: "public_users".to_string(),
            approx_bytes: 256,
            inserted_rows: MAINTENANCE_PENDING_ROWS_THRESHOLD,
            deleted_rows: 0,
        };

        state.record(&notification, now);

        assert!(state.should_flush(now));
    }

    #[test]
    fn test_table_maintenance_state_flushes_after_idle_window() {
        let now = Instant::now();
        let state = TableMaintenanceState {
            pending_bytes: 128,
            pending_inserted_rows: 1,
            pending_deleted_rows: 0,
            dirty_since_compaction: true,
            last_write_at: now,
            last_compaction_at: None,
        };

        assert!(!state.should_flush(now + Duration::from_secs(5)));
        assert!(state.should_flush(now + MAINTENANCE_IDLE_FLUSH_THRESHOLD));
    }

    #[test]
    fn test_table_maintenance_state_compacts_only_when_idle_and_dirty() {
        let now = Instant::now();
        let state = TableMaintenanceState {
            pending_bytes: 0,
            pending_inserted_rows: 0,
            pending_deleted_rows: 0,
            dirty_since_compaction: true,
            last_write_at: now,
            last_compaction_at: Some(now),
        };

        assert!(
            !state.should_compact(now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD),
            "compaction should wait for the minimum compaction interval"
        );
        assert!(state.should_compact(
            now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD
                + MAINTENANCE_TABLE_COMPACTION_INTERVAL,
        ));
    }
}
