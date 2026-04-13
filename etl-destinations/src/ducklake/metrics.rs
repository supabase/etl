use std::collections::HashSet;
use std::sync::{Arc, Once, OnceLock};

use chrono::Utc;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use metrics::{Unit, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use parking_lot::Mutex;
use pg_escape::{quote_identifier, quote_literal};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, MissedTickBehavior};
use tracing::{info, warn};

use crate::ducklake::client::{
    DuckDbBlockingOperationKind, DuckLakeConnectionManager, build_warm_ducklake_pool,
    format_query_error_detail, run_duckdb_blocking,
};
use crate::ducklake::maintenance::{
    TableMaintenanceNotification, TableMetricsSample, send_maintenance_notification,
};
use crate::ducklake::{DuckLakeTableName, LAKE_CATALOG};

static REGISTER_METRICS: Once = Once::new();

/// Files smaller than this are usually too small to be efficient in object storage.
pub(super) const SMALL_FILE_SIZE_BYTES: i64 = 5_000_000; // 5MB
/// Dedicated pool size for background DuckLake metrics sampling.
const METRICS_POOL_SIZE: u32 = 1;
/// Frequency of background DuckLake metrics sampling.
const METRICS_POLL_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) const ETL_DUCKLAKE_POOL_SIZE: &str = "etl_ducklake_pool_size";
pub(crate) const ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS: &str =
    "etl_ducklake_blocking_slot_wait_seconds";
pub(crate) const ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS: &str =
    "etl_ducklake_pool_checkout_wait_seconds";
pub(crate) const ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS: &str =
    "etl_ducklake_blocking_operation_duration_seconds";
pub(crate) const ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS: &str =
    "etl_ducklake_batch_commit_duration_seconds";
pub(crate) const ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS: &str =
    "etl_ducklake_batch_prepared_mutations";
pub(crate) const ETL_DUCKLAKE_UPSERT_ROWS: &str = "etl_ducklake_upsert_rows";
pub(crate) const ETL_DUCKLAKE_DELETE_PREDICATES: &str = "etl_ducklake_delete_predicates";
pub(crate) const ETL_DUCKLAKE_INLINE_FLUSH_ROWS: &str = "etl_ducklake_inline_flush_rows";
pub(crate) const ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS: &str =
    "etl_ducklake_inline_flush_duration_seconds";
pub(crate) const ETL_DUCKLAKE_RETRIES_TOTAL: &str = "etl_ducklake_retries_total";
pub(crate) const ETL_DUCKLAKE_FAILED_BATCHES_TOTAL: &str = "etl_ducklake_failed_batches_total";
pub(crate) const ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL: &str = "etl_ducklake_replayed_batches_total";
pub(crate) const ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS: &str =
    "etl_ducklake_maintenance_duration_seconds";
pub(crate) const ETL_DUCKLAKE_MAINTENANCE_SKIPPED_TOTAL: &str =
    "etl_ducklake_maintenance_skipped_total";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILES: &str =
    "etl_ducklake_table_active_data_files";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DATA_BYTES: &str =
    "etl_ducklake_table_active_data_bytes";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILE_AVG_SIZE_BYTES: &str =
    "etl_ducklake_table_active_data_file_avg_size_bytes";
pub(crate) const ETL_DUCKLAKE_TABLE_SMALL_FILE_RATIO: &str = "etl_ducklake_table_small_file_ratio";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_FILES: &str =
    "etl_ducklake_table_active_delete_files";
pub(crate) const ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_BYTES: &str =
    "etl_ducklake_table_active_delete_bytes";
pub(crate) const ETL_DUCKLAKE_TABLE_DELETED_ROW_RATIO: &str =
    "etl_ducklake_table_deleted_row_ratio";
pub(crate) const ETL_DUCKLAKE_ACTIVE_DATA_FILES_TOTAL: &str =
    "etl_ducklake_active_data_files_total";
pub(crate) const ETL_DUCKLAKE_SNAPSHOTS_TOTAL: &str = "etl_ducklake_snapshots_total";
pub(crate) const ETL_DUCKLAKE_OLDEST_SNAPSHOT_AGE_SECONDS: &str =
    "etl_ducklake_oldest_snapshot_age_seconds";
pub(crate) const ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_TOTAL: &str =
    "etl_ducklake_files_scheduled_for_deletion_total";
pub(crate) const ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_BYTES: &str =
    "etl_ducklake_files_scheduled_for_deletion_bytes";
pub(crate) const ETL_DUCKLAKE_OLDEST_SCHEDULED_DELETION_AGE_SECONDS: &str =
    "etl_ducklake_oldest_scheduled_deletion_age_seconds";

pub(crate) const BATCH_KIND_LABEL: &str = "batch_kind";
pub(crate) const SUB_BATCH_KIND_LABEL: &str = "sub_batch_kind";
pub(crate) const PREPARED_ROWS_KIND_LABEL: &str = "prepared_rows_kind";
pub(crate) const DELETE_ORIGIN_LABEL: &str = "delete_origin";
pub(crate) const RETRY_SCOPE_LABEL: &str = "retry_scope";
pub(crate) const RESULT_LABEL: &str = "result";
pub(crate) const MAINTENANCE_TASK_LABEL: &str = "task";
pub(crate) const MAINTENANCE_OPERATION_LABEL: &str = "operation";
pub(crate) const MAINTENANCE_REASON_LABEL: &str = "reason";
pub(crate) const MAINTENANCE_OUTCOME_LABEL: &str = "outcome";

/// Shared state for the background DuckLake metrics sampler.
pub(super) struct DuckLakeMetricsSampler {
    pub(super) shutdown_tx: watch::Sender<()>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

/// Aggregated storage health sampled for one DuckLake table.
#[derive(Clone, Debug)]
pub(super) struct DuckLakeTableStorageMetrics {
    pub(super) active_data_files: i64,
    pub(super) active_data_bytes: i64,
    pub(super) small_data_files: i64,
    pub(super) active_data_rows: i64,
    pub(super) active_delete_files: i64,
    pub(super) active_delete_bytes: i64,
    pub(super) deleted_rows: i64,
}

impl DuckLakeTableStorageMetrics {
    /// Returns the average active data-file size in bytes.
    pub(super) fn average_data_file_size_bytes(&self) -> f64 {
        if self.active_data_files > 0 {
            self.active_data_bytes.max(0) as f64 / self.active_data_files as f64
        } else {
            0.0
        }
    }

    /// Returns the ratio of active data files smaller than the shared 5 MiB floor.
    pub(super) fn small_file_ratio(&self) -> f64 {
        if self.active_data_files > 0 {
            self.small_data_files.max(0) as f64 / self.active_data_files as f64
        } else {
            0.0
        }
    }

    /// Returns the ratio of deleted rows to active data-file rows.
    pub(super) fn deleted_row_ratio(&self) -> f64 {
        if self.active_data_rows > 0 {
            self.deleted_rows.max(0) as f64 / self.active_data_rows as f64
        } else {
            0.0
        }
    }
}

/// Global maintenance backlog sampled from the DuckLake catalog.
pub(super) struct DuckLakeCatalogMaintenanceMetrics {
    pub(super) active_data_files_total: i64,
    pub(super) snapshots_total: i64,
    pub(super) oldest_snapshot_age_seconds: i64,
    pub(super) files_scheduled_for_deletion_total: i64,
    pub(super) files_scheduled_for_deletion_bytes: i64,
    pub(super) oldest_scheduled_deletion_age_seconds: i64,
}

/// Registers DuckLake metrics once per process.
pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(
            ETL_DUCKLAKE_POOL_SIZE,
            Unit::Count,
            "Configured size of the warm DuckLake connection pool."
        );

        describe_histogram!(
            ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS,
            Unit::Seconds,
            "Time spent waiting for a DuckLake blocking slot."
        );
        describe_histogram!(
            ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS,
            Unit::Seconds,
            "Time spent waiting to check out a DuckLake connection from the pool."
        );
        describe_histogram!(
            ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS,
            Unit::Seconds,
            "Time spent executing a DuckLake blocking operation after checkout."
        );
        describe_histogram!(
            ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
            Unit::Seconds,
            "End-to-end duration of a committed DuckLake atomic batch, labeled by batch_kind and sub_batch_kind."
        );
        describe_histogram!(
            ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
            Unit::Count,
            "Prepared mutation statements per committed DuckLake atomic batch, labeled by batch_kind and sub_batch_kind."
        );
        describe_histogram!(
            ETL_DUCKLAKE_UPSERT_ROWS,
            Unit::Count,
            "Rows included in one DuckLake upsert statement, labeled by batch_kind and prepared_rows_kind."
        );
        describe_histogram!(
            ETL_DUCKLAKE_DELETE_PREDICATES,
            Unit::Count,
            "Predicates included in one DuckLake delete statement, labeled by batch_kind and delete_origin."
        );
        describe_histogram!(
            ETL_DUCKLAKE_INLINE_FLUSH_ROWS,
            Unit::Count,
            "Rows materialized by ducklake_flush_inlined_data, labeled by batch_kind and result."
        );
        describe_histogram!(
            ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of ducklake_flush_inlined_data, labeled by batch_kind and result."
        );
        describe_counter!(
            ETL_DUCKLAKE_RETRIES_TOTAL,
            Unit::Count,
            "Retry attempts for DuckLake write paths, labeled by batch_kind and retry_scope."
        );
        describe_counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            Unit::Count,
            "DuckLake batch executions that still failed after retries, labeled by batch_kind and retry_scope."
        );
        describe_counter!(
            ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
            Unit::Count,
            "DuckLake batches skipped because an applied marker already existed, labeled by batch_kind."
        );
        describe_histogram!(
            ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS,
            Unit::Seconds,
            "Duration of DuckLake background maintenance operations, labeled by task, operation, reason, and outcome. Use the histogram count as the event count for non-skipped outcomes."
        );
        describe_counter!(
            ETL_DUCKLAKE_MAINTENANCE_SKIPPED_TOTAL,
            Unit::Count,
            "DuckLake background maintenance operations skipped because execution was deferred, labeled by task, operation, and reason."
        );

        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILES,
            Unit::Count,
            "Sampled active data-file count for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DATA_BYTES,
            Unit::Bytes,
            "Sampled active data bytes for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILE_AVG_SIZE_BYTES,
            Unit::Bytes,
            "Sampled average active data-file size for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_SMALL_FILE_RATIO,
            "Sampled ratio of active data files smaller than 5 MiB for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_FILES,
            Unit::Count,
            "Sampled active delete-file count for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_BYTES,
            Unit::Bytes,
            "Sampled active delete-file bytes for one DuckLake table from the background metrics task."
        );
        describe_histogram!(
            ETL_DUCKLAKE_TABLE_DELETED_ROW_RATIO,
            "Sampled ratio of active deleted rows to active data-file rows for one DuckLake table from the background metrics task."
        );

        describe_gauge!(
            ETL_DUCKLAKE_ACTIVE_DATA_FILES_TOTAL,
            Unit::Count,
            "Current total number of active data files in the DuckLake catalog."
        );
        describe_gauge!(
            ETL_DUCKLAKE_SNAPSHOTS_TOTAL,
            Unit::Count,
            "Current number of snapshots in the DuckLake catalog."
        );
        describe_gauge!(
            ETL_DUCKLAKE_OLDEST_SNAPSHOT_AGE_SECONDS,
            Unit::Seconds,
            "Age in seconds of the oldest snapshot in the DuckLake catalog."
        );
        describe_gauge!(
            ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_TOTAL,
            Unit::Count,
            "Current number of files scheduled for deletion."
        );
        describe_gauge!(
            ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_BYTES,
            Unit::Bytes,
            "Current total bytes of files scheduled for deletion."
        );
        describe_gauge!(
            ETL_DUCKLAKE_OLDEST_SCHEDULED_DELETION_AGE_SECONDS,
            Unit::Seconds,
            "Age in seconds of the oldest file waiting in files_scheduled_for_deletion."
        );
    });
}

/// Builds the dedicated metrics pool and spawns the periodic DuckLake sampler task.
pub(super) async fn spawn_ducklake_metrics_sampler(
    manager: DuckLakeConnectionManager,
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
    maintenance_notification_tx: mpsc::Sender<TableMaintenanceNotification>,
) -> EtlResult<DuckLakeMetricsSampler> {
    let pool = Arc::new(build_warm_ducklake_pool(manager, METRICS_POOL_SIZE, "metrics").await?);
    let blocking_slots = Arc::new(Semaphore::new(METRICS_POOL_SIZE as usize));
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let handle = tokio::spawn(run_ducklake_metrics_sampler(
        pool,
        blocking_slots,
        created_tables,
        maintenance_notification_tx,
        shutdown_rx,
    ));

    Ok(DuckLakeMetricsSampler {
        shutdown_tx,
        handle: Mutex::new(handle.into()),
    })
}

/// Periodically samples DuckLake metadata on a dedicated connection pool.
async fn run_ducklake_metrics_sampler(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
    maintenance_notification_tx: mpsc::Sender<TableMaintenanceNotification>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut interval = tokio::time::interval(METRICS_POLL_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                info!("ducklake metrics sampler shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(error) =
                    record_catalog_maintenance_metrics(Arc::clone(&pool), Arc::clone(&blocking_slots)).await
                {
                    warn!(error = ?error, "ducklake catalog maintenance metrics collection failed");
                }

                let table_names = {
                    let cache = created_tables.lock();
                    cache.iter().cloned().collect::<Vec<_>>()
                };

                for table_name in table_names {
                    if shutdown_rx.has_changed().unwrap_or(false) {
                        info!("ducklake metrics sampler stopping after shutdown signal");
                        return;
                    }

                    if let Err(error) = record_table_storage_metrics(
                        Arc::clone(&pool),
                        Arc::clone(&blocking_slots),
                        table_name.clone(),
                        &maintenance_notification_tx,
                    )
                    .await
                    {
                        warn!(
                            table = %table_name,
                            error = ?error,
                            "ducklake table storage metrics collection failed"
                        );
                    }
                }
            }
        }
    }
}

/// Samples storage health for one table without affecting the write path.
async fn record_table_storage_metrics(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_name: DuckLakeTableName,
    maintenance_notification_tx: &mpsc::Sender<TableMaintenanceNotification>,
) -> EtlResult<()> {
    let metrics = query_table_storage_metrics(pool, blocking_slots, table_name.clone()).await?;
    let active_data_files = metrics.active_data_files.max(0) as f64;
    let active_data_bytes = metrics.active_data_bytes.max(0) as f64;
    let active_delete_files = metrics.active_delete_files.max(0) as f64;
    let active_delete_bytes = metrics.active_delete_bytes.max(0) as f64;
    let small_file_ratio = metrics.small_file_ratio();
    let average_data_file_size_bytes = metrics.average_data_file_size_bytes();
    let deleted_row_ratio = metrics.deleted_row_ratio();

    histogram!(ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILES).record(active_data_files);
    histogram!(ETL_DUCKLAKE_TABLE_ACTIVE_DATA_BYTES).record(active_data_bytes);
    histogram!(ETL_DUCKLAKE_TABLE_ACTIVE_DATA_FILE_AVG_SIZE_BYTES)
        .record(average_data_file_size_bytes);
    histogram!(ETL_DUCKLAKE_TABLE_SMALL_FILE_RATIO).record(small_file_ratio);
    histogram!(ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_FILES).record(active_delete_files);
    histogram!(ETL_DUCKLAKE_TABLE_ACTIVE_DELETE_BYTES).record(active_delete_bytes);
    histogram!(ETL_DUCKLAKE_TABLE_DELETED_ROW_RATIO).record(deleted_row_ratio);
    send_maintenance_notification(
        maintenance_notification_tx,
        TableMaintenanceNotification::TableMetricsSample(TableMetricsSample {
            table_name,
            sampled_at: Instant::now(),
            metrics,
        }),
    )
    .await;

    Ok(())
}

/// Samples global snapshot and deletion backlog gauges.
async fn record_catalog_maintenance_metrics(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
) -> EtlResult<()> {
    let metrics = query_catalog_maintenance_metrics(pool, blocking_slots).await?;
    gauge!(ETL_DUCKLAKE_ACTIVE_DATA_FILES_TOTAL).set(metrics.active_data_files_total.max(0) as f64);
    gauge!(ETL_DUCKLAKE_SNAPSHOTS_TOTAL).set(metrics.snapshots_total.max(0) as f64);
    gauge!(ETL_DUCKLAKE_OLDEST_SNAPSHOT_AGE_SECONDS)
        .set(metrics.oldest_snapshot_age_seconds.max(0) as f64);
    gauge!(ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_TOTAL)
        .set(metrics.files_scheduled_for_deletion_total.max(0) as f64);
    gauge!(ETL_DUCKLAKE_FILES_SCHEDULED_FOR_DELETION_BYTES)
        .set(metrics.files_scheduled_for_deletion_bytes.max(0) as f64);
    gauge!(ETL_DUCKLAKE_OLDEST_SCHEDULED_DELETION_AGE_SECONDS)
        .set(metrics.oldest_scheduled_deletion_age_seconds.max(0) as f64);

    Ok(())
}

/// Returns table-level storage health from the DuckLake metadata tables.
async fn query_table_storage_metrics(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_name: DuckLakeTableName,
) -> EtlResult<DuckLakeTableStorageMetrics> {
    let table_name_for_query = table_name.clone();
    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Metrics,
        move |conn| query_table_storage_metrics_blocking(conn, &table_name_for_query),
    )
    .await
}

/// Returns table-level storage health from the DuckLake metadata tables.
pub(super) fn query_table_storage_metrics_blocking(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<DuckLakeTableStorageMetrics> {
    let metadata_namespace = ducklake_metadata_namespace(conn)?;
    let sql = format!(
        r#"WITH target_table AS (
             SELECT table_id
             FROM {metadata_namespace}.ducklake_table
             WHERE end_snapshot IS NULL AND table_name = {}
             LIMIT 1
         ),
         data_stats AS (
             SELECT
                 COUNT(*) AS active_data_files,
                 COALESCE(SUM(file_size_bytes), 0) AS active_data_bytes,
                 COALESCE(SUM(CASE WHEN file_size_bytes < {} THEN 1 ELSE 0 END), 0) AS small_data_files,
                 COALESCE(SUM(record_count), 0) AS active_data_rows
             FROM {metadata_namespace}.ducklake_data_file
             WHERE end_snapshot IS NULL AND table_id = (SELECT table_id FROM target_table)
         ),
         delete_stats AS (
             SELECT
                 COUNT(*) AS active_delete_files,
                 COALESCE(SUM(file_size_bytes), 0) AS active_delete_bytes,
                 COALESCE(SUM(delete_count), 0) AS deleted_rows
             FROM {metadata_namespace}.ducklake_delete_file
             WHERE end_snapshot IS NULL AND table_id = (SELECT table_id FROM target_table)
         )
         SELECT
             active_data_files,
             active_data_bytes,
             small_data_files,
             active_data_rows,
             active_delete_files,
             active_delete_bytes,
             deleted_rows
         FROM data_stats CROSS JOIN delete_stats;"#,
        quote_literal(table_name),
        SMALL_FILE_SIZE_BYTES,
    );

    conn.query_row(&sql, [], |row| {
        Ok(DuckLakeTableStorageMetrics {
            active_data_files: row.get(0)?,
            active_data_bytes: row.get(1)?,
            small_data_files: row.get(2)?,
            active_data_rows: row.get(3)?,
            active_delete_files: row.get(4)?,
            active_delete_bytes: row.get(5)?,
            deleted_rows: row.get(6)?,
        })
    })
    .map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake table storage metrics query failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })
}

/// Returns global maintenance backlog metrics from the DuckLake metadata tables.
async fn query_catalog_maintenance_metrics(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
) -> EtlResult<DuckLakeCatalogMaintenanceMetrics> {
    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Metrics,
        query_catalog_maintenance_metrics_blocking,
    )
    .await
}

/// Returns global maintenance backlog metrics from the DuckLake metadata tables.
pub(super) fn query_catalog_maintenance_metrics_blocking(
    conn: &duckdb::Connection,
) -> EtlResult<DuckLakeCatalogMaintenanceMetrics> {
    let metadata_namespace = ducklake_metadata_namespace(conn)?;
    let sql = format!(
        r#"WITH active_data_file_stats AS (
             SELECT COUNT(*) AS active_data_files_total
             FROM {metadata_namespace}.ducklake_data_file
             WHERE end_snapshot IS NULL
         ),
         snapshot_stats AS (
             SELECT
                 COUNT(*) AS snapshots_total,
                 MIN(epoch_ms(snapshot_time)) AS oldest_snapshot_epoch_ms
             FROM {metadata_namespace}.ducklake_snapshot
         ),
         scheduled_deletion_stats AS (
             SELECT
                 COUNT(*) AS files_scheduled_for_deletion_total,
                 COALESCE(SUM(data_files.file_size_bytes), 0) AS files_scheduled_for_deletion_bytes,
                 MIN(epoch_ms(scheduled.schedule_start)) AS oldest_scheduled_deletion_epoch_ms
             FROM {metadata_namespace}.ducklake_files_scheduled_for_deletion AS scheduled
             LEFT JOIN {metadata_namespace}.ducklake_data_file AS data_files USING (data_file_id)
         )
         SELECT
             active_data_files_total,
             snapshots_total,
             oldest_snapshot_epoch_ms,
             files_scheduled_for_deletion_total,
             files_scheduled_for_deletion_bytes,
             oldest_scheduled_deletion_epoch_ms
         FROM active_data_file_stats
         CROSS JOIN snapshot_stats
         CROSS JOIN scheduled_deletion_stats;"#
    );
    let now_epoch_ms = Utc::now().timestamp_millis();

    conn.query_row(&sql, [], |row| {
        let oldest_snapshot_epoch_ms = row.get::<_, Option<i64>>(2)?;
        let oldest_scheduled_deletion_epoch_ms = row.get::<_, Option<i64>>(5)?;
        Ok(DuckLakeCatalogMaintenanceMetrics {
            active_data_files_total: row.get(0)?,
            snapshots_total: row.get(1)?,
            oldest_snapshot_age_seconds: epoch_age_seconds(now_epoch_ms, oldest_snapshot_epoch_ms),
            files_scheduled_for_deletion_total: row.get(3)?,
            files_scheduled_for_deletion_bytes: row.get(4)?,
            oldest_scheduled_deletion_age_seconds: epoch_age_seconds(
                now_epoch_ms,
                oldest_scheduled_deletion_epoch_ms,
            ),
        })
    })
    .map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake catalog maintenance metrics query failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })
}

fn epoch_age_seconds(now_epoch_ms: i64, oldest_epoch_ms: Option<i64>) -> i64 {
    oldest_epoch_ms
        .map(|epoch_ms| now_epoch_ms.saturating_sub(epoch_ms) / 1_000)
        .unwrap_or(0)
}

/// Returns the fully-qualified hidden DuckLake metadata namespace.
fn ducklake_metadata_namespace(conn: &duckdb::Connection) -> EtlResult<String> {
    static METADATA_NAMESPACE: OnceLock<String> = OnceLock::new();

    if let Some(namespace) = METADATA_NAMESPACE.get() {
        return Ok(namespace.clone());
    }

    let namespace = resolve_ducklake_metadata_namespace(conn)?;
    let _ = METADATA_NAMESPACE.set(namespace.clone());
    Ok(namespace)
}

/// Resolves the schema that DuckLake uses inside its hidden metadata catalog.
fn resolve_ducklake_metadata_namespace(conn: &duckdb::Connection) -> EtlResult<String> {
    let metadata_catalog = format!("__ducklake_metadata_{LAKE_CATALOG}");
    let sql = format!(
        r#"SELECT table_schema
           FROM information_schema.tables
           WHERE table_catalog = {}
             AND table_name = 'ducklake_snapshot'
           ORDER BY CASE
               WHEN table_schema = 'main' THEN 0
               WHEN table_schema = 'ducklake' THEN 1
               ELSE 2
           END,
           table_schema
           LIMIT 1;"#,
        quote_literal(&metadata_catalog),
    );
    let table_schema = conn
        .query_row(&sql, [], |row| row.get::<_, String>(0))
        .map_err(|e| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake metadata namespace query failed",
                format_query_error_detail(&sql, &e),
                source: e
            )
        })?;

    Ok(format!(
        "{}.{}",
        quote_identifier(&metadata_catalog),
        quote_identifier(&table_schema),
    ))
}
