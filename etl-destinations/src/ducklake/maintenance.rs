use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::Duration,
};

use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use metrics::{counter, gauge, histogram};
use parking_lot::Mutex;
use pg_escape::quote_literal;
use tokio::{
    sync::{OwnedSemaphorePermit, RwLock, Semaphore, mpsc, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};
use tracing::{debug, info, warn};

use crate::ducklake::{
    DuckLakeTableName, LAKE_CATALOG,
    client::{
        DuckDbBlockingOperationKind, DuckLakeConnectionManager, LazyDuckLakePool,
        format_query_error_detail, run_duckdb_blocking,
    },
    inline_size::{DuckLakePendingInlineDataSizes, DuckLakePendingInlineSizeSampler},
    metrics::{
        DuckLakeTableStorageMetrics, ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
        ETL_DUCKLAKE_INLINE_FLUSH_ROWS, ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS,
        ETL_DUCKLAKE_MAINTENANCE_IN_PROGRESS, ETL_DUCKLAKE_MAINTENANCE_SKIPPED_TOTAL,
        ETL_DUCKLAKE_MAINTENANCE_STARTED_TOTAL, MAINTENANCE_OPERATION_LABEL,
        MAINTENANCE_OUTCOME_LABEL, MAINTENANCE_REASON_LABEL, MAINTENANCE_TASK_LABEL, RESULT_LABEL,
    },
};

/// Dedicated pool size for background DuckLake maintenance work.
const MAINTENANCE_POOL_SIZE: u32 = 1;
/// Poll interval for checking per-table inline flush thresholds.
const MAINTENANCE_FLUSH_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Fixed cadence for expiring old DuckLake snapshots.
const MAINTENANCE_EXPIRE_SNAPSHOTS_INTERVAL: Duration = Duration::from_secs(5 * 60 * 60);
/// Fixed cadence for cleaning up old DuckLake files.
const MAINTENANCE_CLEANUP_OLD_FILES_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
/// Pending inlined bytes threshold that triggers a background inline flush. We
/// multiply using `PARQUET_COMPRESSION_RATIO_ESTIMATE` to make sure we won't
/// get too small data files
const MAINTENANCE_PENDING_INLINED_DATA_BYTES_THRESHOLD: u64 = 10_000_000;
/// Minimum idle window before targeted table maintenance runs, to not have
/// maintenances ran too frequently.
const MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD: Duration = Duration::from_secs(90);
/// Minimum delay between targeted maintenance runs for the same table.
const MAINTENANCE_TABLE_COMPACTION_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Keeps the legacy targeted rewrite path compiled without scheduling it.
const ENABLE_TARGETED_TABLE_MAINTENANCE: bool = false;
/// Minimum active delete-file count before idle rewrite is worth attempting.
const MAINTENANCE_IDLE_REWRITE_DELETE_FILES_THRESHOLD: i64 = 32;
/// Deleted-row ratio that makes idle rewrite worthwhile.
const MAINTENANCE_IDLE_REWRITE_DELETED_ROW_RATIO_THRESHOLD: f64 = 0.10;
/// Active delete-file count that warrants emergency rewrite.
const MAINTENANCE_EMERGENCY_REWRITE_DELETE_FILES_THRESHOLD: i64 = 128;
/// Deleted-row ratio that warrants emergency rewrite.
const MAINTENANCE_EMERGENCY_REWRITE_DELETED_ROW_RATIO_THRESHOLD: f64 = 0.25;
/// Timeout for sending a notification to the maintenance worker.
pub(super) const NOTIFICATION_SEND_TIMEOUT: Duration = Duration::from_secs(5);

const MAINTENANCE_TASK_FLUSH: &str = "flush";
const MAINTENANCE_TASK_CATALOG_MAINTENANCE: &str = "catalog_maintenance";
const MAINTENANCE_TASK_TARGETED_MAINTENANCE: &str = "targeted_maintenance";

#[cfg(test)]
static FAIL_REWRITE_SINGLE_OUTPUT_FILE_ONCE_FOR_TESTS: AtomicBool = AtomicBool::new(false);

/// Concrete DuckLake maintenance operations emitted in metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceOperation {
    FlushInlinedData,
    ExpireSnapshots,
    CleanupOldFiles,
    RewriteDataFiles,
}

impl MaintenanceOperation {
    /// Returns the stable metric label value for this operation.
    fn as_str(self) -> &'static str {
        match self {
            Self::FlushInlinedData => "flush_inlined_data",
            Self::ExpireSnapshots => "expire_snapshots",
            Self::CleanupOldFiles => "cleanup_old_files",
            Self::RewriteDataFiles => "rewrite_data_files",
        }
    }
}

/// Primary reasons that schedule one background maintenance decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum MaintenanceReason {
    PendingInlinedDataBytesThreshold,
    SnapshotRetentionThreshold,
    CleanupIntervalElapsed,
    IdleRewriteMetricsThreshold,
    EmergencyRewriteMetricsThreshold,
}

impl MaintenanceReason {
    /// Returns the stable metric label value for this reason.
    fn as_str(self) -> &'static str {
        match self {
            Self::PendingInlinedDataBytesThreshold => "pending_inlined_data_bytes_threshold",
            Self::SnapshotRetentionThreshold => "snapshot_retention_threshold",
            Self::CleanupIntervalElapsed => "cleanup_interval_elapsed",
            Self::IdleRewriteMetricsThreshold => "idle_rewrite_metrics_threshold",
            Self::EmergencyRewriteMetricsThreshold => "emergency_rewrite_metrics_threshold",
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
pub(super) struct TableWriteActivity {
    pub(super) table_name: DuckLakeTableName,
}

/// Table-health metrics sent from the background sampler to maintenance.
#[derive(Clone, Debug)]
pub(super) struct TableMetricsSample {
    pub(super) table_name: DuckLakeTableName,
    pub(super) sampled_at: Instant,
    pub(super) metrics: DuckLakeTableStorageMetrics,
}

/// Notifications consumed by the background DuckLake maintenance worker.
#[derive(Clone, Debug)]
pub(super) enum TableMaintenanceNotification {
    WriteActivity(TableWriteActivity),
    TableMetricsSample(TableMetricsSample),
    FlushCompleted(TableFlushCompletion),
}

impl TableMaintenanceNotification {
    /// Returns the table name carried by this notification.
    fn table_name(&self) -> &str {
        match self {
            Self::WriteActivity(activity) => &activity.table_name,
            Self::TableMetricsSample(sample) => &sample.table_name,
            Self::FlushCompleted(completion) => &completion.table_name,
        }
    }
}

/// Completion notification for one requested inline flush.
#[derive(Clone, Debug)]
pub(super) struct TableFlushCompletion {
    pub(super) table_name: DuckLakeTableName,
    pub(super) completed_at: Instant,
}

/// Per-table inline flushes that should run at the next safe ingest pause.
#[derive(Debug, Default)]
pub(super) struct PendingInlineFlushRequests {
    requests: Mutex<HashMap<DuckLakeTableName, MaintenanceReason>>,
}

impl PendingInlineFlushRequests {
    /// Records or updates one requested inline flush.
    fn request(&self, table_name: DuckLakeTableName, reason: MaintenanceReason) {
        self.requests.lock().insert(table_name, reason);
    }

    /// Drains the currently requested inline flushes.
    fn take_all(&self) -> Vec<(DuckLakeTableName, MaintenanceReason)> {
        self.requests.lock().drain().collect()
    }

    /// Restores requested inline flushes after a skipped or failed attempt.
    fn restore(&self, requests: impl IntoIterator<Item = (DuckLakeTableName, MaintenanceReason)>) {
        self.requests.lock().extend(requests);
    }
}

/// Trigger scope for one targeted maintenance selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TargetedMaintenanceScope {
    /// When the table has been idle long enough
    Idle,
    /// When ducklake state is not healthy (files fragmented, ...)
    Emergency,
}

/// Selected targeted-maintenance operations for one table.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TargetedMaintenancePlan {
    rewrite_reason: Option<MaintenanceReason>,
}

impl TargetedMaintenancePlan {
    /// Returns whether this plan selected any maintenance work.
    fn has_work(self) -> bool {
        self.rewrite_reason.is_some()
    }
}

/// Static configuration for periodic catalog-level maintenance.
#[derive(Debug, Clone)]
struct CatalogMaintenanceConfig {
    expire_snapshots_older_than: Arc<str>,
    expire_snapshots_interval: Duration,
    cleanup_old_files_interval: Duration,
}

impl CatalogMaintenanceConfig {
    /// Builds the fixed catalog-maintenance configuration.
    fn new(expire_snapshots_older_than: Arc<str>) -> Self {
        Self {
            expire_snapshots_older_than,
            expire_snapshots_interval: MAINTENANCE_EXPIRE_SNAPSHOTS_INTERVAL,
            cleanup_old_files_interval: MAINTENANCE_CLEANUP_OLD_FILES_INTERVAL,
        }
    }
}

/// Periodic catalog-maintenance state tracked by the background worker.
#[derive(Debug)]
struct CatalogMaintenanceState {
    last_expire_snapshots_completed_at: Option<Instant>,
    last_cleanup_old_files_completed_at: Option<Instant>,
}

impl CatalogMaintenanceState {
    /// Builds catalog-maintenance state seeded to "just ran now".
    ///
    /// Fresh destinations should not immediately run snapshot expiration or
    /// old-file cleanup on startup before the configured cadence elapses.
    fn new(now: Instant) -> Self {
        Self {
            last_expire_snapshots_completed_at: Some(now),
            last_cleanup_old_files_completed_at: Some(now),
        }
    }

    /// Returns whether snapshot expiration is due now.
    fn expire_snapshots_due(&self, now: Instant, interval: Duration) -> bool {
        match self.last_expire_snapshots_completed_at {
            Some(last_completed_at) => now.saturating_duration_since(last_completed_at) >= interval,
            None => true,
        }
    }

    /// Returns whether old-file cleanup is due now.
    fn cleanup_old_files_due(&self, now: Instant, interval: Duration) -> bool {
        match self.last_cleanup_old_files_completed_at {
            Some(last_completed_at) => now.saturating_duration_since(last_completed_at) >= interval,
            None => true,
        }
    }

    /// Records one successful snapshot-expiration run.
    fn complete_expire_snapshots(&mut self, now: Instant) {
        self.last_expire_snapshots_completed_at = Some(now);
    }

    /// Records one successful cleanup-old-files run.
    fn complete_cleanup_old_files(&mut self, now: Instant) {
        self.last_cleanup_old_files_completed_at = Some(now);
    }
}

/// Coalesced maintenance state for one DuckLake table.
#[derive(Debug, Default)]
struct TableMaintenanceState {
    dirty_since_compaction: bool,
    last_write_at: Option<Instant>,
    latest_pending_inline_data_sizes: Option<DuckLakePendingInlineDataSizes>,
    latest_pending_inline_data_sampled_at: Option<Instant>,
    last_targeted_maintenance_at: Option<Instant>,
    latest_storage_metrics: Option<DuckLakeTableStorageMetrics>,
    latest_storage_metrics_sampled_at: Option<Instant>,
    last_emergency_assessment_at: Option<Instant>,
}

impl TableMaintenanceState {
    /// Aggregates one write notification into the existing table state.
    fn record_write_activity(&mut self, now: Instant) {
        self.dirty_since_compaction = true;
        self.last_write_at = Some(now);
    }

    /// Records one sampled inlined-data snapshot for this table.
    fn record_pending_inline_data_sizes(
        &mut self,
        sampled_at: Instant,
        sizes: DuckLakePendingInlineDataSizes,
    ) {
        self.latest_pending_inline_data_sizes = Some(sizes);
        self.latest_pending_inline_data_sampled_at = Some(sampled_at);
    }

    /// Records one sampled metrics snapshot for this table.
    fn record_metrics_sample(&mut self, sample: TableMetricsSample) {
        self.latest_storage_metrics = Some(sample.metrics);
        self.latest_storage_metrics_sampled_at = Some(sample.sampled_at);
    }

    /// Returns whether the current dirty period still needs an inline-size
    /// sample.
    fn needs_pending_inline_data_sizes_sample(&self) -> bool {
        self.dirty_since_compaction
            && self.last_write_at.is_some()
            && self.current_pending_inline_data_sizes().is_none()
    }

    /// Returns the latest inlined-data sample covering the current dirty
    /// period.
    fn current_pending_inline_data_sizes(&self) -> Option<DuckLakePendingInlineDataSizes> {
        let sizes = self.latest_pending_inline_data_sizes?;
        let sampled_at = self.latest_pending_inline_data_sampled_at?;

        if let Some(last_write_at) = self.last_write_at
            && sampled_at < last_write_at
        {
            return None;
        }

        Some(sizes)
    }

    /// Returns the primary reason that pending inlined work should be flushed.
    fn flush_reason(&self) -> Option<MaintenanceReason> {
        if let Some(sizes) = self.current_pending_inline_data_sizes() {
            return (sizes.inlined_bytes >= MAINTENANCE_PENDING_INLINED_DATA_BYTES_THRESHOLD)
                .then_some(MaintenanceReason::PendingInlinedDataBytesThreshold);
        }

        None
    }

    /// Clears pending flush counters after a successful flush/materialization.
    fn clear_pending_flush(&mut self, now: Instant) {
        self.latest_pending_inline_data_sizes = Some(DuckLakePendingInlineDataSizes::default());
        self.latest_pending_inline_data_sampled_at = Some(now);
    }

    /// Returns the latest metrics sample covering the current dirty period.
    fn current_storage_metrics(&self) -> Option<(&DuckLakeTableStorageMetrics, Instant)> {
        let metrics = self.latest_storage_metrics.as_ref()?;
        let sampled_at = self.latest_storage_metrics_sampled_at?;

        if let Some(last_write_at) = self.last_write_at
            && sampled_at < last_write_at
        {
            return None;
        }

        Some((metrics, sampled_at))
    }

    /// Returns the plan for idle targeted maintenance, if it is due.
    fn idle_targeted_maintenance_plan(&self, now: Instant) -> Option<TargetedMaintenancePlan> {
        if !self.dirty_since_compaction {
            return None;
        }

        let last_write_at = self.last_write_at?;
        let (metrics, _) = self.current_storage_metrics()?;
        let idle = now.saturating_duration_since(last_write_at);
        let enough_idle = idle >= MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD;
        let enough_gap = match self.last_targeted_maintenance_at {
            Some(last) => {
                now.saturating_duration_since(last) >= MAINTENANCE_TABLE_COMPACTION_INTERVAL
            }
            None => true,
        };

        if enough_idle && enough_gap {
            Some(targeted_maintenance_plan(metrics, TargetedMaintenanceScope::Idle))
        } else {
            None
        }
    }

    /// Returns the plan for emergency targeted maintenance from a fresh sample.
    fn emergency_targeted_maintenance_plan(
        &self,
        now: Instant,
    ) -> Option<(TargetedMaintenancePlan, Instant)> {
        if !self.dirty_since_compaction {
            return None;
        }

        let (metrics, sampled_at) = self.current_storage_metrics()?;
        let enough_gap = match self.last_targeted_maintenance_at {
            Some(last) => {
                now.saturating_duration_since(last) >= MAINTENANCE_TABLE_COMPACTION_INTERVAL
            }
            None => true,
        };
        let unseen_sample = match self.last_emergency_assessment_at {
            Some(last_assessment_at) => sampled_at > last_assessment_at,
            None => true,
        };

        if enough_gap && unseen_sample {
            Some((
                targeted_maintenance_plan(metrics, TargetedMaintenanceScope::Emergency),
                sampled_at,
            ))
        } else {
            None
        }
    }

    /// Marks one completed idle maintenance assessment.
    fn complete_idle_targeted_maintenance(&mut self, now: Instant) {
        self.dirty_since_compaction = false;
        self.last_targeted_maintenance_at = Some(now);
    }

    /// Marks one completed targeted maintenance run.
    fn complete_targeted_maintenance(&mut self, now: Instant) {
        self.complete_idle_targeted_maintenance(now);
    }
}

/// Shared state for the background DuckLake maintenance worker.
pub(super) struct DuckLakeMaintenanceWorker {
    pub(super) notification_tx: mpsc::Sender<TableMaintenanceNotification>,
    pub(super) shutdown_tx: watch::Sender<()>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

/// Records one DuckLake background maintenance operation start.
fn record_ducklake_maintenance_started(
    task: &'static str,
    operation: MaintenanceOperation,
    reason: MaintenanceReason,
) {
    counter!(
        ETL_DUCKLAKE_MAINTENANCE_STARTED_TOTAL,
        MAINTENANCE_TASK_LABEL => task,
        MAINTENANCE_OPERATION_LABEL => operation.as_str(),
        MAINTENANCE_REASON_LABEL => reason.as_str(),
    )
    .increment(1);
}

/// Increments one DuckLake background maintenance operation in-progress sample.
fn increment_ducklake_maintenance_in_progress(
    task: &'static str,
    operation: MaintenanceOperation,
    reason: MaintenanceReason,
) {
    gauge!(
        ETL_DUCKLAKE_MAINTENANCE_IN_PROGRESS,
        MAINTENANCE_TASK_LABEL => task,
        MAINTENANCE_OPERATION_LABEL => operation.as_str(),
        MAINTENANCE_REASON_LABEL => reason.as_str(),
    )
    .increment(1.0);
}

/// Decrements one DuckLake background maintenance operation in-progress sample.
fn decrement_ducklake_maintenance_in_progress(
    task: &'static str,
    operation: MaintenanceOperation,
    reason: MaintenanceReason,
) {
    gauge!(
        ETL_DUCKLAKE_MAINTENANCE_IN_PROGRESS,
        MAINTENANCE_TASK_LABEL => task,
        MAINTENANCE_OPERATION_LABEL => operation.as_str(),
        MAINTENANCE_REASON_LABEL => reason.as_str(),
    )
    .decrement(1.0);
}

/// Keeps the maintenance in-progress gauge balanced for one operation attempt.
#[must_use = "the returned guard tracks an in-progress maintenance metric until dropped"]
struct DuckLakeMaintenanceInProgressGuard {
    task: &'static str,
    operation: MaintenanceOperation,
    reason: MaintenanceReason,
}

impl DuckLakeMaintenanceInProgressGuard {
    /// Starts one maintenance attempt and tracks it until the guard is dropped.
    fn start(
        task: &'static str,
        operation: MaintenanceOperation,
        reason: MaintenanceReason,
    ) -> Self {
        record_ducklake_maintenance_started(task, operation, reason);
        increment_ducklake_maintenance_in_progress(task, operation, reason);
        Self { task, operation, reason }
    }
}

impl Drop for DuckLakeMaintenanceInProgressGuard {
    fn drop(&mut self) {
        decrement_ducklake_maintenance_in_progress(self.task, self.operation, self.reason);
    }
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

/// Records all selected targeted-maintenance operations as skipped because the
/// table is busy.
fn record_skipped_targeted_maintenance(plan: TargetedMaintenancePlan) {
    if let Some(reason) = plan.rewrite_reason {
        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            reason,
        );
    }
}

/// Records skipped catalog-maintenance operations when the worker cannot enter
/// the exclusive safe point yet.
fn record_skipped_catalog_maintenance(expire_snapshots_due: bool, cleanup_old_files_due: bool) {
    if expire_snapshots_due {
        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::ExpireSnapshots,
            MaintenanceReason::SnapshotRetentionThreshold,
        );
    }
    if cleanup_old_files_due {
        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::CleanupOldFiles,
            MaintenanceReason::CleanupIntervalElapsed,
        );
    }
}

/// Returns whether this maintenance failure matches a known DuckLake compaction
/// bug.
fn is_known_ducklake_compaction_single_output_file_error(error: &EtlError) -> bool {
    error.detail().is_some_and(|detail| {
        detail.contains("INTERNAL Error: DuckLakeCompaction - expected a single output file")
    })
}

/// Returns the failing maintenance operation and reason for one known
/// compaction bug.
fn known_ducklake_compaction_error_context(
    plan: TargetedMaintenancePlan,
    error: &EtlError,
) -> Option<(MaintenanceOperation, MaintenanceReason)> {
    if !is_known_ducklake_compaction_single_output_file_error(error) {
        return None;
    }

    match error.description() {
        Some("DuckLake rewrite data files failed") => {
            plan.rewrite_reason.map(|reason| (MaintenanceOperation::RewriteDataFiles, reason))
        }
        _ => None,
    }
}

/// Logs and suppresses one known DuckLake compaction internal error.
///
/// Connection recycling is handled generically in [`run_duckdb_blocking`],
/// which marks any failing DuckDB connection as broken before it returns the
/// error to this layer. This helper stays intentionally narrow: it only decides
/// which maintenance-only failures are safe to downgrade after the pool has
/// already recycled the invalidated connection.
fn suppress_known_ducklake_compaction_error(
    table_name: &str,
    plan: TargetedMaintenancePlan,
    error: &EtlError,
) -> bool {
    let Some((operation, reason)) = known_ducklake_compaction_error_context(plan, error) else {
        return false;
    };

    warn!(
        table = %table_name,
        operation = operation.as_str(),
        reason = reason.as_str(),
        error = ?error,
        "ducklake targeted maintenance skipped after known duckdb internal error"
    );
    true
}

/// Returns the targeted-maintenance plan implied by one metrics sample.
fn targeted_maintenance_plan(
    metrics: &DuckLakeTableStorageMetrics,
    scope: TargetedMaintenanceScope,
) -> TargetedMaintenancePlan {
    let mut plan = TargetedMaintenancePlan::default();
    let active_delete_files = metrics.active_delete_files.max(0);
    let deleted_row_ratio = metrics.deleted_row_ratio();

    match scope {
        TargetedMaintenanceScope::Idle => {
            if active_delete_files >= MAINTENANCE_IDLE_REWRITE_DELETE_FILES_THRESHOLD
                && deleted_row_ratio >= MAINTENANCE_IDLE_REWRITE_DELETED_ROW_RATIO_THRESHOLD
            {
                plan.rewrite_reason = Some(MaintenanceReason::IdleRewriteMetricsThreshold);
            }
        }
        TargetedMaintenanceScope::Emergency => {
            if active_delete_files >= MAINTENANCE_EMERGENCY_REWRITE_DELETE_FILES_THRESHOLD
                || (active_delete_files >= MAINTENANCE_IDLE_REWRITE_DELETE_FILES_THRESHOLD
                    && deleted_row_ratio
                        >= MAINTENANCE_EMERGENCY_REWRITE_DELETED_ROW_RATIO_THRESHOLD)
            {
                plan.rewrite_reason = Some(MaintenanceReason::EmergencyRewriteMetricsThreshold);
            }
        }
    }

    plan
}

/// Sends one maintenance notification without blocking the caller indefinitely.
pub(super) async fn send_maintenance_notification(
    notification_tx: &mpsc::Sender<TableMaintenanceNotification>,
    notification: TableMaintenanceNotification,
) {
    if let Err(error) = notification_tx.send_timeout(notification, NOTIFICATION_SEND_TIMEOUT).await
    {
        match error {
            mpsc::error::SendTimeoutError::Timeout(notification) => {
                warn!(
                    table = %notification.table_name(),
                    "ducklake maintenance notification timed out"
                );
            }
            mpsc::error::SendTimeoutError::Closed(notification) => {
                warn!(
                    table = %notification.table_name(),
                    "ducklake maintenance notification dropped"
                );
            }
        }
    }
}

/// Tries to enqueue one maintenance notification without awaiting channel
/// capacity.
fn try_send_maintenance_notification(
    notification_tx: &mpsc::Sender<TableMaintenanceNotification>,
    notification: TableMaintenanceNotification,
) {
    if let Err(error) = notification_tx.try_send(notification) {
        match error {
            mpsc::error::TrySendError::Full(notification) => {
                warn!(
                    table = %notification.table_name(),
                    "ducklake maintenance notification dropped because channel is full"
                );
            }
            mpsc::error::TrySendError::Closed(notification) => {
                warn!(
                    table = %notification.table_name(),
                    "ducklake maintenance notification dropped"
                );
            }
        }
    }
}

/// Starts warming the maintenance pool and spawns the periodic DuckLake worker.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_ducklake_maintenance_worker(
    manager: DuckLakeConnectionManager,
    checkpoint_gate: Arc<RwLock<()>>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    inline_flush_requested: Arc<AtomicBool>,
    pending_inline_flush_requests: Arc<PendingInlineFlushRequests>,
    pending_inline_size_sampler: Option<DuckLakePendingInlineSizeSampler>,
    expire_snapshots_older_than: Arc<str>,
) -> EtlResult<DuckLakeMaintenanceWorker> {
    let mut pool = LazyDuckLakePool::new(manager, MAINTENANCE_POOL_SIZE, "maintenance");
    pool.warm_in_background();
    let (notification_tx, notification_rx) = mpsc::channel(1024);
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let handle = tokio::spawn(run_ducklake_maintenance_worker(
        pool,
        checkpoint_gate,
        table_write_slots,
        inline_flush_requested,
        pending_inline_flush_requests,
        pending_inline_size_sampler,
        CatalogMaintenanceConfig::new(expire_snapshots_older_than),
        notification_rx,
        shutdown_rx,
    ));

    Ok(DuckLakeMaintenanceWorker {
        notification_tx,
        shutdown_tx,
        handle: Mutex::new(handle.into()),
    })
}

/// Coalesces notifications and runs background DuckLake maintenance.
#[allow(clippy::too_many_arguments)]
async fn run_ducklake_maintenance_worker(
    mut pool: LazyDuckLakePool,
    checkpoint_gate: Arc<RwLock<()>>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    inline_flush_requested: Arc<AtomicBool>,
    pending_inline_flush_requests: Arc<PendingInlineFlushRequests>,
    pending_inline_size_sampler: Option<DuckLakePendingInlineSizeSampler>,
    catalog_maintenance_config: CatalogMaintenanceConfig,
    mut notification_rx: mpsc::Receiver<TableMaintenanceNotification>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let blocking_slots = pool.blocking_slots();
    let started_at = Instant::now();
    let mut flush_interval = tokio::time::interval_at(
        started_at + MAINTENANCE_FLUSH_POLL_INTERVAL,
        MAINTENANCE_FLUSH_POLL_INTERVAL,
    );
    flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut table_states: HashMap<DuckLakeTableName, TableMaintenanceState> = HashMap::new();
    let mut catalog_maintenance_state = CatalogMaintenanceState::new(started_at);

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

                apply_maintenance_notification(&mut table_states, notification);
            }
            _ = flush_interval.tick() => {
                for (table_name, table_state) in &mut table_states {
                    if let Some(pending_inline_size_sampler) = &pending_inline_size_sampler
                        && table_state.needs_pending_inline_data_sizes_sample()
                    {
                        match pending_inline_size_sampler.sample_table(table_name).await {
                            Ok(sizes) => {
                                table_state.record_pending_inline_data_sizes(Instant::now(), sizes);
                            }
                            Err(error) => {
                                tracing::error!(
                                    table = %table_name,
                                    error = ?error,
                                    "ducklake inline-size sampler query failed"
                                );
                            }
                        }
                    }

                    // If it needs to be flushed
                    if let Some(reason) = table_state.flush_reason() {
                        pending_inline_flush_requests.request(table_name.clone(), reason);
                        inline_flush_requested.store(true, AtomicOrdering::Release);
                    }
                    let now = Instant::now();

                    if !ENABLE_TARGETED_TABLE_MAINTENANCE {
                        continue;
                    }

                    if let Some((plan, sampled_at)) = table_state
                        .emergency_targeted_maintenance_plan(now)
                    {
                        table_state.last_emergency_assessment_at = Some(sampled_at);
                        if plan.has_work() {
                            let pool = match pool.get_or_init_pool().await {
                                Ok(pool) => pool,
                                Err(error) => {
                                    warn!(error = ?error, "ducklake maintenance pool initialization failed");
                                    continue;
                                }
                            };
                            match run_targeted_table_maintenance(
                                pool,
                                Arc::clone(&checkpoint_gate),
                                Arc::clone(&blocking_slots),
                                Arc::clone(&table_write_slots),
                                table_name.clone(),
                                plan,
                            )
                            .await
                            {
                                Ok(outcome) => {
                                    if outcome.is_completed() {
                                        table_state.complete_targeted_maintenance(Instant::now());
                                    }
                                }
                                Err(error) => {
                                    warn!(
                                        table = %table_name,
                                        error = ?error,
                                        "ducklake targeted maintenance failed"
                                    );
                                }
                            }
                            continue;
                        }
                    }

                    if let Some(plan) = table_state.idle_targeted_maintenance_plan(now) {
                        if !plan.has_work() {
                            table_state.complete_idle_targeted_maintenance(Instant::now());
                            continue;
                        }

                        let pool = match pool.get_or_init_pool().await {
                            Ok(pool) => pool,
                            Err(error) => {
                                warn!(error = ?error, "ducklake maintenance pool initialization failed");
                                continue;
                            }
                        };
                        match run_targeted_table_maintenance(
                            pool,
                            Arc::clone(&checkpoint_gate),
                            Arc::clone(&blocking_slots),
                            Arc::clone(&table_write_slots),
                            table_name.clone(),
                            plan,
                        )
                        .await
                        {
                            Ok(outcome) => {
                                if outcome.is_completed() {
                                    table_state.complete_targeted_maintenance(Instant::now());
                                }
                            }
                            Err(error) => {
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
                    state.dirty_since_compaction || state.latest_storage_metrics.is_some()
                });

                maybe_run_catalog_maintenance(
                    &mut pool,
                    Arc::clone(&checkpoint_gate),
                    Arc::clone(&blocking_slots),
                    &catalog_maintenance_config,
                    &mut catalog_maintenance_state,
                )
                .await;
            }
        }
    }
}

/// Applies one maintenance notification to the table-state cache.
fn apply_maintenance_notification(
    table_states: &mut HashMap<DuckLakeTableName, TableMaintenanceState>,
    notification: TableMaintenanceNotification,
) {
    let now = Instant::now();
    match notification {
        TableMaintenanceNotification::WriteActivity(activity) => {
            table_states
                .entry(activity.table_name)
                .and_modify(|state| state.record_write_activity(now))
                .or_insert_with(|| {
                    let mut state = TableMaintenanceState::default();
                    state.record_write_activity(now);
                    state
                });
        }
        TableMaintenanceNotification::TableMetricsSample(sample) => {
            table_states
                .entry(sample.table_name.clone())
                .and_modify(|state| state.record_metrics_sample(sample.clone()))
                .or_insert_with(|| {
                    let mut state = TableMaintenanceState::default();
                    state.record_metrics_sample(sample);
                    state
                });
        }
        TableMaintenanceNotification::FlushCompleted(completion) => {
            let Some(state) = table_states.get_mut(&completion.table_name) else {
                return;
            };
            state.clear_pending_flush(completion.completed_at);
        }
    }
}

/// Returns the table-local semaphore shared by writes and background
/// maintenance.
pub(super) fn table_write_slot(
    table_write_slots: &Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: &str,
) -> Arc<Semaphore> {
    let mut slots = table_write_slots.lock();
    let slot = slots.entry(table_name.to_owned()).or_insert_with(|| Arc::new(Semaphore::new(1)));
    Arc::clone(slot)
}

/// Tries to acquire the table-local semaphore without blocking the maintenance
/// worker.
fn try_acquire_table_write_slot(
    table_write_slots: &Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: &str,
) -> Option<OwnedSemaphorePermit> {
    table_write_slot(table_write_slots, table_name).try_acquire_owned().ok()
}

/// Runs targeted rewrite and merge maintenance for one table.
async fn run_targeted_table_maintenance(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    checkpoint_gate: Arc<RwLock<()>>,
    blocking_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: DuckLakeTableName,
    plan: TargetedMaintenancePlan,
) -> EtlResult<MaintenanceOutcome> {
    let Some(table_write_permit) = try_acquire_table_write_slot(&table_write_slots, &table_name)
    else {
        record_skipped_targeted_maintenance(plan);
        return Ok(MaintenanceOutcome::SkippedBusy);
    };
    let table_name_for_query = table_name.clone();
    let plan_for_query = plan;
    let _checkpoint_guard = checkpoint_gate.read_owned().await;

    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        move |conn| {
            let _table_write_permit = table_write_permit;
            run_targeted_table_maintenance_blocking(conn, &table_name_for_query, plan_for_query)
        },
    )
    .await
    .or_else(|error| {
        if suppress_known_ducklake_compaction_error(&table_name, plan, &error) {
            Ok(MaintenanceOutcome::Noop)
        } else {
            Err(error)
        }
    })
}

/// Runs catalog-level DuckLake maintenance when its fixed cadence is due.
async fn maybe_run_catalog_maintenance(
    pool: &mut LazyDuckLakePool,
    checkpoint_gate: Arc<RwLock<()>>,
    blocking_slots: Arc<Semaphore>,
    config: &CatalogMaintenanceConfig,
    state: &mut CatalogMaintenanceState,
) {
    let now = Instant::now();
    let expire_snapshots_due = state.expire_snapshots_due(now, config.expire_snapshots_interval);
    let cleanup_old_files_due = state.cleanup_old_files_due(now, config.cleanup_old_files_interval);

    if !expire_snapshots_due && !cleanup_old_files_due {
        return;
    }

    let Ok(_checkpoint_guard) = checkpoint_gate.try_write_owned() else {
        record_skipped_catalog_maintenance(expire_snapshots_due, cleanup_old_files_due);
        return;
    };

    let pool = match pool.get_or_init_pool().await {
        Ok(pool) => pool,
        Err(error) => {
            warn!(error = ?error, "ducklake maintenance pool initialization failed");
            return;
        }
    };

    match run_catalog_maintenance(
        pool,
        Arc::clone(&blocking_slots),
        Arc::clone(&config.expire_snapshots_older_than),
        expire_snapshots_due,
        cleanup_old_files_due,
    )
    .await
    {
        Ok((expired_snapshots, cleaned_up_files)) => {
            let completed_at = Instant::now();
            if expire_snapshots_due {
                state.complete_expire_snapshots(completed_at);
            }
            if cleanup_old_files_due {
                state.complete_cleanup_old_files(completed_at);
            }
            info!(
                expire_snapshots_older_than = %config.expire_snapshots_older_than,
                expire_snapshots_due,
                cleanup_old_files_due,
                expired_snapshots,
                cleaned_up_files,
                "ducklake catalog maintenance completed"
            );
        }
        Err(error) => {
            warn!(
                expire_snapshots_older_than = %config.expire_snapshots_older_than,
                error = ?error,
                "ducklake catalog maintenance failed"
            );
        }
    }
}

/// Runs catalog-level maintenance inside one DuckDB blocking operation.
async fn run_catalog_maintenance(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    expire_snapshots_older_than: Arc<str>,
    expire_snapshots_due: bool,
    cleanup_old_files_due: bool,
) -> EtlResult<(u64, u64)> {
    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Maintenance,
        move |conn| {
            run_catalog_maintenance_blocking(
                conn,
                expire_snapshots_older_than.as_ref(),
                expire_snapshots_due,
                cleanup_old_files_due,
            )
        },
    )
    .await
}

/// Runs requested inline flushes before foreground ingestion begins.
pub(super) async fn maybe_run_requested_inline_flush(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    checkpoint_gate: Arc<RwLock<()>>,
    blocking_slots: Arc<Semaphore>,
    inline_flush_requested: &AtomicBool,
    pending_inline_flush_requests: &PendingInlineFlushRequests,
    notification_tx: Option<mpsc::Sender<TableMaintenanceNotification>>,
) -> EtlResult<()> {
    if !inline_flush_requested.swap(false, AtomicOrdering::AcqRel) {
        return Ok(());
    }

    let requested_flushes = pending_inline_flush_requests.take_all();
    if requested_flushes.is_empty() {
        return Ok(());
    }

    let Ok(_checkpoint_guard) = checkpoint_gate.try_write_owned() else {
        for (_, reason) in &requested_flushes {
            record_ducklake_maintenance_skipped(
                MAINTENANCE_TASK_FLUSH,
                MaintenanceOperation::FlushInlinedData,
                *reason,
            );
        }
        pending_inline_flush_requests.restore(requested_flushes);
        inline_flush_requested.store(true, AtomicOrdering::Release);
        return Ok(());
    };

    let mut requested_flushes = requested_flushes.into_iter();
    while let Some((table_name, reason)) = requested_flushes.next() {
        let table_name_for_query = table_name.clone();
        let outcome = run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Maintenance,
            move |conn| {
                flush_table_inlined_data_in_background_blocking(conn, &table_name_for_query, reason)
            },
        )
        .await;

        match outcome {
            Ok(outcome) => {
                if outcome.is_completed()
                    && let Some(notification_tx) = notification_tx.as_ref()
                {
                    try_send_maintenance_notification(
                        notification_tx,
                        TableMaintenanceNotification::FlushCompleted(TableFlushCompletion {
                            table_name,
                            completed_at: Instant::now(),
                        }),
                    );
                }
            }
            Err(error) => {
                pending_inline_flush_requests.request(table_name, reason);
                pending_inline_flush_requests.restore(requested_flushes);
                inline_flush_requested.store(true, AtomicOrdering::Release);
                return Err(error);
            }
        }
    }

    Ok(())
}

/// Materializes one table's pending inlined rows and records the maintenance
/// outcome.
fn flush_table_inlined_data_in_background_blocking(
    conn: &duckdb::Connection,
    table_name: &str,
    reason: MaintenanceReason,
) -> EtlResult<MaintenanceOutcome> {
    let flush_started = Instant::now();
    let _in_progress_guard = DuckLakeMaintenanceInProgressGuard::start(
        MAINTENANCE_TASK_FLUSH,
        MaintenanceOperation::FlushInlinedData,
        reason,
    );
    let rows_flushed = flush_table_inlined_data(conn, table_name).inspect_err(|_error| {
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
    plan: TargetedMaintenancePlan,
) -> EtlResult<MaintenanceOutcome> {
    let mut rewritten_files = 0u64;
    let mut rewrite_outcome = None;

    if let Some(reason) = plan.rewrite_reason {
        let rewrite_started = Instant::now();
        let _in_progress_guard = DuckLakeMaintenanceInProgressGuard::start(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            reason,
        );
        rewritten_files = rewrite_table_data_files(conn, table_name).inspect_err(|_error| {
            record_ducklake_maintenance_duration(
                MAINTENANCE_TASK_TARGETED_MAINTENANCE,
                MaintenanceOperation::RewriteDataFiles,
                reason,
                MaintenanceOutcome::Failed,
                rewrite_started.elapsed().as_secs_f64(),
            );
        })?;
        let outcome = MaintenanceOutcome::from(rewritten_files);
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            reason,
            outcome,
            rewrite_started.elapsed().as_secs_f64(),
        );
        rewrite_outcome = Some(outcome);
    }

    info!(
        table = %table_name,
        rewrite_selected = plan.rewrite_reason.is_some(),
        rewritten_files,
        "ducklake targeted maintenance completed"
    );

    Ok(rewrite_outcome.unwrap_or(MaintenanceOutcome::Noop))
}

/// Builds the DuckLake snapshot-expiration call for one retention window.
fn expire_snapshots_sql(expire_snapshots_older_than: &str) -> String {
    format!(
        "CALL ducklake_expire_snapshots({}, older_than => CAST(now() AS TIMESTAMP) - CAST({} AS \
         INTERVAL));",
        quote_literal(LAKE_CATALOG),
        quote_literal(expire_snapshots_older_than),
    )
}

/// Builds the DuckLake old-file cleanup call for one retention window.
fn cleanup_old_files_sql(expire_snapshots_older_than: &str) -> String {
    format!(
        "CALL ducklake_cleanup_old_files({}, older_than => CAST(now() AS TIMESTAMP) - CAST({} AS \
         INTERVAL));",
        quote_literal(LAKE_CATALOG),
        quote_literal(expire_snapshots_older_than),
    )
}

/// Counts the rows returned by one DuckLake maintenance call.
fn count_ducklake_maintenance_rows(
    conn: &duckdb::Connection,
    sql: &str,
    description: &'static str,
) -> EtlResult<u64> {
    let mut statement = conn.prepare(sql).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql, &source),
            source: source
        )
    })?;
    let mut rows = statement.query([]).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql, &source),
            source: source
        )
    })?;
    let mut count = 0u64;

    while let Some(_row) = rows.next().map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql, &source),
            source: source
        )
    })? {
        count = count.saturating_add(1);
    }

    Ok(count)
}

/// Runs DuckLake catalog maintenance and records per-operation outcomes.
fn run_catalog_maintenance_blocking(
    conn: &duckdb::Connection,
    expire_snapshots_older_than: &str,
    expire_snapshots_due: bool,
    cleanup_old_files_due: bool,
) -> EtlResult<(u64, u64)> {
    let mut expired_snapshots = 0u64;
    let mut cleaned_up_files = 0u64;

    if expire_snapshots_due {
        let expire_reason = MaintenanceReason::SnapshotRetentionThreshold;
        let expire_started = Instant::now();
        let _expire_guard = DuckLakeMaintenanceInProgressGuard::start(
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::ExpireSnapshots,
            expire_reason,
        );
        let expire_sql = expire_snapshots_sql(expire_snapshots_older_than);
        expired_snapshots =
            count_ducklake_maintenance_rows(conn, &expire_sql, "DuckLake expire snapshots failed")
                .inspect_err(|_error| {
                    record_ducklake_maintenance_duration(
                        MAINTENANCE_TASK_CATALOG_MAINTENANCE,
                        MaintenanceOperation::ExpireSnapshots,
                        expire_reason,
                        MaintenanceOutcome::Failed,
                        expire_started.elapsed().as_secs_f64(),
                    );
                })?;
        let expire_outcome = MaintenanceOutcome::from(expired_snapshots);
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::ExpireSnapshots,
            expire_reason,
            expire_outcome,
            expire_started.elapsed().as_secs_f64(),
        );
    }

    if cleanup_old_files_due {
        let cleanup_reason = MaintenanceReason::CleanupIntervalElapsed;
        let cleanup_started = Instant::now();
        let _cleanup_guard = DuckLakeMaintenanceInProgressGuard::start(
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::CleanupOldFiles,
            cleanup_reason,
        );
        let cleanup_sql = cleanup_old_files_sql(expire_snapshots_older_than);
        cleaned_up_files = count_ducklake_maintenance_rows(
            conn,
            &cleanup_sql,
            "DuckLake cleanup old files failed",
        )
        .inspect_err(|_error| {
            record_ducklake_maintenance_duration(
                MAINTENANCE_TASK_CATALOG_MAINTENANCE,
                MaintenanceOperation::CleanupOldFiles,
                cleanup_reason,
                MaintenanceOutcome::Failed,
                cleanup_started.elapsed().as_secs_f64(),
            );
        })?;
        let cleanup_outcome = MaintenanceOutcome::from(cleaned_up_files);
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::CleanupOldFiles,
            cleanup_reason,
            cleanup_outcome,
            cleanup_started.elapsed().as_secs_f64(),
        );
    }

    Ok((expired_snapshots, cleaned_up_files))
}

/// Flushes inlined user data for one table after the write transaction commits.
pub(super) fn flush_table_inlined_data(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<u64> {
    let flush_started = Instant::now();
    let sql = format!(
        r#"SELECT COALESCE(SUM(rows_flushed), 0)
         FROM ducklake_flush_inlined_data({}, table_name => {});"#,
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
    );
    let rows_flushed: i64 = conn.query_row(&sql, [], |row| row.get(0)).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake inlined data flush failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })?;
    let rows_flushed = rows_flushed.max(0) as u64;
    let flush_result = if rows_flushed > 0 { "flushed" } else { "noop" };
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_ROWS,
        RESULT_LABEL => flush_result,
    )
    .record(rows_flushed as f64);
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
        RESULT_LABEL => flush_result,
    )
    .record(flush_started.elapsed().as_secs_f64());

    if rows_flushed > 0 {
        debug!(
            table = %table_name,
            rows_flushed,
            "ducklake inlined data flushed"
        );
    } else {
        debug!(
            table = %table_name,
            "ducklake inlined data already flushed"
        );
    }
    Ok(rows_flushed)
}

/// Rewrites one table's delete-heavy files and returns created file count.
fn rewrite_table_data_files(conn: &duckdb::Connection, table_name: &str) -> EtlResult<u64> {
    let sql = format!(
        r#"SELECT COALESCE(SUM(files_created), 0)
         FROM ducklake_rewrite_data_files({}, {});"#,
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
    );
    #[cfg(test)]
    if FAIL_REWRITE_SINGLE_OUTPUT_FILE_ONCE_FOR_TESTS.swap(false, AtomicOrdering::Relaxed) {
        let source = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("INTERNAL Error: DuckLakeCompaction - expected a single output file".to_owned()),
        );
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake rewrite data files failed",
            format_query_error_detail(&sql, &source),
            source: source
        ));
    }

    let files_created: i64 = conn.query_row(&sql, [], |row| row.get(0)).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake rewrite data files failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;

    Ok(files_created.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use etl_telemetry::metrics::init_metrics_handle;

    use super::*;
    #[cfg(feature = "test-utils")]
    use crate::ducklake::client::build_warm_ducklake_pool;
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
                if line.starts_with(&format!("{ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS}_count"))
                    && line.contains(&task_label)
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

    fn maintenance_started_counter_value(
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
                if line.starts_with(ETL_DUCKLAKE_MAINTENANCE_STARTED_TOTAL)
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

    fn maintenance_in_progress_gauge_value(
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
                if line.starts_with(ETL_DUCKLAKE_MAINTENANCE_IN_PROGRESS)
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

    fn table_metrics_sample(
        sampled_at: Instant,
        metrics: DuckLakeTableStorageMetrics,
    ) -> TableMetricsSample {
        TableMetricsSample { table_name: "public_users".to_owned(), sampled_at, metrics }
    }

    fn storage_metrics(
        active_data_files: i64,
        active_data_bytes: i64,
        small_data_files: i64,
        active_data_rows: i64,
        active_delete_files: i64,
        active_delete_bytes: i64,
        deleted_rows: i64,
    ) -> DuckLakeTableStorageMetrics {
        DuckLakeTableStorageMetrics {
            active_data_files,
            active_data_bytes,
            small_data_files,
            active_data_rows,
            active_delete_files,
            active_delete_bytes,
            deleted_rows,
        }
    }

    #[tokio::test]
    async fn maintenance_duration_histogram_counts_are_exported_with_labels() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let flush_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
            MaintenanceOutcome::Applied,
        );
        let rewrite_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Applied,
        );

        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
            MaintenanceOutcome::Applied,
            0.25,
        );
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Applied,
            1.0,
        );

        let rendered_after = handle.render();
        let flush_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
            MaintenanceOutcome::Applied,
        );
        let rewrite_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Applied,
        );

        assert!(flush_after > flush_before, "flush duration count did not increase");
        assert!(rewrite_after > rewrite_before, "rewrite duration count did not increase");
    }

    #[tokio::test]
    async fn maintenance_started_counter_and_in_progress_gauge_are_exported_with_labels() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let started_before = maintenance_started_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );
        let in_progress_before = maintenance_in_progress_gauge_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );

        let started_during;
        let in_progress_during;
        {
            let _in_progress_guard = DuckLakeMaintenanceInProgressGuard::start(
                MAINTENANCE_TASK_TARGETED_MAINTENANCE,
                MaintenanceOperation::RewriteDataFiles,
                MaintenanceReason::EmergencyRewriteMetricsThreshold,
            );

            let rendered_during = handle.render();
            started_during = maintenance_started_counter_value(
                &rendered_during,
                MAINTENANCE_TASK_TARGETED_MAINTENANCE,
                MaintenanceOperation::RewriteDataFiles,
                MaintenanceReason::EmergencyRewriteMetricsThreshold,
            );
            in_progress_during = maintenance_in_progress_gauge_value(
                &rendered_during,
                MAINTENANCE_TASK_TARGETED_MAINTENANCE,
                MaintenanceOperation::RewriteDataFiles,
                MaintenanceReason::EmergencyRewriteMetricsThreshold,
            );
        }

        let rendered_after = handle.render();
        let started_after = maintenance_started_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );
        let in_progress_after = maintenance_in_progress_gauge_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );

        assert!(started_during > started_before, "maintenance started counter did not increase");
        assert!(
            in_progress_during > in_progress_before,
            "maintenance in-progress gauge did not increase"
        );
        assert_eq!(started_after, started_during);
        assert_eq!(in_progress_after, in_progress_before);
    }

    #[tokio::test]
    async fn targeted_maintenance_busy_emits_skip_counter_for_rewrite() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let skipped_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );

        record_skipped_targeted_maintenance(TargetedMaintenancePlan {
            rewrite_reason: Some(MaintenanceReason::EmergencyRewriteMetricsThreshold),
        });

        let rendered_after = handle.render();
        let skipped_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );

        assert!(skipped_after > skipped_before, "rewrite skip count did not increase");
    }

    #[tokio::test]
    async fn catalog_maintenance_busy_emits_skip_counter_for_both_operations() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let expire_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::ExpireSnapshots,
            MaintenanceReason::SnapshotRetentionThreshold,
        );
        let cleanup_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::CleanupOldFiles,
            MaintenanceReason::CleanupIntervalElapsed,
        );

        record_skipped_catalog_maintenance(true, true);

        let rendered_after = handle.render();
        let expire_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::ExpireSnapshots,
            MaintenanceReason::SnapshotRetentionThreshold,
        );
        let cleanup_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::CleanupOldFiles,
            MaintenanceReason::CleanupIntervalElapsed,
        );

        assert!(expire_after > expire_before, "expire snapshots skip count did not increase");
        assert!(cleanup_after > cleanup_before, "cleanup old files skip count did not increase");
    }

    #[test]
    fn table_maintenance_state_without_sample_or_row_threshold_does_not_flush() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(now);

        assert_eq!(state.flush_reason(), None);
    }

    #[test]
    fn table_maintenance_state_uses_sampled_inline_data_bytes_flush_reason() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(now);
        state.record_pending_inline_data_sizes(
            now + Duration::from_secs(1),
            DuckLakePendingInlineDataSizes {
                inlined_bytes: MAINTENANCE_PENDING_INLINED_DATA_BYTES_THRESHOLD,
            },
        );

        assert_eq!(state.flush_reason(), Some(MaintenanceReason::PendingInlinedDataBytesThreshold));
    }

    #[test]
    fn table_maintenance_state_ignores_below_threshold_sampled_inline_data() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(now);
        state.record_pending_inline_data_sizes(
            now + Duration::from_secs(1),
            DuckLakePendingInlineDataSizes {
                inlined_bytes: MAINTENANCE_PENDING_INLINED_DATA_BYTES_THRESHOLD - 1,
            },
        );

        assert_eq!(state.flush_reason(), None);
    }

    #[test]
    fn table_maintenance_state_selects_idle_rewrite_from_delete_pressure() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(now);
        state.record_metrics_sample(table_metrics_sample(
            now + Duration::from_secs(1),
            storage_metrics(10, 20_000_000, 2, 1000, 32, 10_000, 150),
        ));

        assert_eq!(
            state.idle_targeted_maintenance_plan(
                now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD
                    + MAINTENANCE_TABLE_COMPACTION_INTERVAL,
            ),
            Some(TargetedMaintenancePlan {
                rewrite_reason: Some(MaintenanceReason::IdleRewriteMetricsThreshold),
            })
        );
    }

    #[test]
    fn table_maintenance_state_selects_emergency_rewrite_from_delete_pressure() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(now);
        state.record_metrics_sample(table_metrics_sample(
            now + Duration::from_secs(1),
            storage_metrics(10, 20_000_000, 2, 1000, 128, 10_000, 150),
        ));

        assert_eq!(
            state.emergency_targeted_maintenance_plan(now + MAINTENANCE_TABLE_COMPACTION_INTERVAL),
            Some((
                TargetedMaintenancePlan {
                    rewrite_reason: Some(MaintenanceReason::EmergencyRewriteMetricsThreshold),
                },
                now + Duration::from_secs(1),
            ))
        );
    }

    #[test]
    fn maintenance_outcome_from_rows_flushed_marks_applied_and_noop() {
        assert_eq!(MaintenanceOutcome::from(0), MaintenanceOutcome::Noop);
        assert_eq!(MaintenanceOutcome::from(3), MaintenanceOutcome::Applied);
    }

    #[test]
    fn catalog_maintenance_state_tracks_independent_due_intervals() {
        let now = Instant::now();
        let state = CatalogMaintenanceState::new(now);

        assert!(!state.expire_snapshots_due(now, MAINTENANCE_EXPIRE_SNAPSHOTS_INTERVAL));
        assert!(!state.cleanup_old_files_due(now, MAINTENANCE_CLEANUP_OLD_FILES_INTERVAL));

        assert!(!state.expire_snapshots_due(
            now + MAINTENANCE_EXPIRE_SNAPSHOTS_INTERVAL - Duration::from_secs(1),
            MAINTENANCE_EXPIRE_SNAPSHOTS_INTERVAL
        ));
        assert!(state.expire_snapshots_due(
            now + MAINTENANCE_EXPIRE_SNAPSHOTS_INTERVAL,
            MAINTENANCE_EXPIRE_SNAPSHOTS_INTERVAL
        ));
        assert!(!state.cleanup_old_files_due(
            now + MAINTENANCE_CLEANUP_OLD_FILES_INTERVAL - Duration::from_secs(1),
            MAINTENANCE_CLEANUP_OLD_FILES_INTERVAL
        ));
        assert!(state.cleanup_old_files_due(
            now + MAINTENANCE_CLEANUP_OLD_FILES_INTERVAL,
            MAINTENANCE_CLEANUP_OLD_FILES_INTERVAL
        ));
    }

    #[test]
    fn catalog_maintenance_sql_builders_use_interval_casts() {
        let expire_sql = expire_snapshots_sql("2 days");
        let cleanup_sql = cleanup_old_files_sql("2 days");

        assert!(expire_sql.contains("ducklake_expire_snapshots"));
        assert!(cleanup_sql.contains("ducklake_cleanup_old_files"));
        assert!(expire_sql.contains("CAST('2 days' AS INTERVAL)"));
        assert!(cleanup_sql.contains("CAST('2 days' AS INTERVAL)"));
    }

    #[tokio::test]
    async fn flush_failure_records_failed_metric() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let conn = duckdb::Connection::open_in_memory().expect("failed to open in-memory duckdb");

        let rendered_before = handle.render();
        let failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
            MaintenanceOutcome::Failed,
        );

        let error = flush_table_inlined_data_in_background_blocking(
            &conn,
            "public_users",
            MaintenanceReason::PendingInlinedDataBytesThreshold,
        )
        .expect_err("flush should fail without ducklake functions");

        assert!(matches!(error.kind(), ErrorKind::DestinationQueryFailed));

        let rendered_after = handle.render();
        let failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
            MaintenanceOutcome::Failed,
        );

        assert!(failed_after > failed_before, "flush failed duration count did not increase");
    }

    #[tokio::test]
    async fn rewrite_failure_records_failed_duration() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let conn = duckdb::Connection::open_in_memory().expect("failed to open in-memory duckdb");

        let rendered_before = handle.render();
        let failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Failed,
        );

        let error = run_targeted_table_maintenance_blocking(
            &conn,
            "public_users",
            TargetedMaintenancePlan {
                rewrite_reason: Some(MaintenanceReason::IdleRewriteMetricsThreshold),
            },
        )
        .expect_err("targeted maintenance should fail without ducklake functions");

        assert!(matches!(error.kind(), ErrorKind::DestinationQueryFailed));

        let rendered_after = handle.render();
        let failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Failed,
        );

        assert!(failed_after > failed_before, "rewrite failed duration count did not increase");
    }

    #[tokio::test]
    async fn catalog_expire_failure_records_failed_duration() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let conn = duckdb::Connection::open_in_memory().expect("failed to open in-memory duckdb");

        let rendered_before = handle.render();
        let failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::ExpireSnapshots,
            MaintenanceReason::SnapshotRetentionThreshold,
            MaintenanceOutcome::Failed,
        );

        let error = run_catalog_maintenance_blocking(&conn, "7 days", true, true)
            .expect_err("catalog maintenance should fail without ducklake functions");

        assert!(matches!(error.kind(), ErrorKind::DestinationQueryFailed));

        let rendered_after = handle.render();
        let failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::ExpireSnapshots,
            MaintenanceReason::SnapshotRetentionThreshold,
            MaintenanceOutcome::Failed,
        );
        let cleanup_failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_CATALOG_MAINTENANCE,
            MaintenanceOperation::CleanupOldFiles,
            MaintenanceReason::CleanupIntervalElapsed,
            MaintenanceOutcome::Failed,
        );

        assert!(failed_after > failed_before, "expire snapshots failed duration did not increase");
        assert_eq!(cleanup_failed_after, 0.0, "cleanup should not run after expiration fails");
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn known_rewrite_single_output_file_error_is_suppressed_and_recycles_connection() {
        let open_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let manager = DuckLakeConnectionManager {
            setup_plan: Arc::new(crate::ducklake::config::DuckLakeSetupPlan::default()),
            disable_extension_autoload: cfg!(target_os = "linux"),
            interrupt_registry: Arc::new(crate::ducklake::client::DuckLakeInterruptRegistry::default()),
            open_count: Arc::clone(&open_count),
        };
        let pool = Arc::new(
            build_warm_ducklake_pool(manager, 1, "test")
                .await
                .expect("failed to build maintenance test pool"),
        );
        let checkpoint_gate = Arc::new(RwLock::new(()));
        let blocking_slots = Arc::new(Semaphore::new(1));
        let table_write_slots = Arc::new(Mutex::new(HashMap::new()));
        FAIL_REWRITE_SINGLE_OUTPUT_FILE_ONCE_FOR_TESTS.store(true, AtomicOrdering::Relaxed);

        let outcome = run_targeted_table_maintenance(
            Arc::clone(&pool),
            Arc::clone(&checkpoint_gate),
            Arc::clone(&blocking_slots),
            Arc::clone(&table_write_slots),
            "public_users".to_owned(),
            TargetedMaintenancePlan {
                rewrite_reason: Some(MaintenanceReason::IdleRewriteMetricsThreshold),
            },
        )
        .await
        .expect("known DuckLake compaction bug should be suppressed");

        assert_eq!(outcome, MaintenanceOutcome::Noop);

        let verification = run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Maintenance,
            |conn| {
                conn.query_row("SELECT 1", [], |row| row.get::<_, i64>(0)).map_err(|source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake connection recycling verification query failed",
                        source: source
                    )
                })
            },
        )
        .await
        .expect("expected follow-up query to succeed on recycled connection");

        assert_eq!(verification, 1);
        assert!(open_count.load(AtomicOrdering::Relaxed) > 1);
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn spawn_ducklake_maintenance_worker_warms_pool_in_background() {
        let open_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = spawn_ducklake_maintenance_worker(
            DuckLakeConnectionManager {
                setup_plan: Arc::new(crate::ducklake::config::DuckLakeSetupPlan::default()),
                disable_extension_autoload: cfg!(target_os = "linux"),
                interrupt_registry: Arc::new(crate::ducklake::client::DuckLakeInterruptRegistry::default()),
                open_count: Arc::clone(&open_count),
            },
            Arc::new(RwLock::new(())),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicBool::new(false)),
            Arc::new(PendingInlineFlushRequests::default()),
            None,
            Arc::<str>::from("7 days"),
        )
        .expect("failed to spawn maintenance worker");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while open_count.load(AtomicOrdering::Relaxed) < MAINTENANCE_POOL_SIZE as usize {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        worker.shutdown_tx.send(()).expect("maintenance worker shutdown channel should stay open");
        let handle = worker.handle.lock().take().expect("maintenance worker handle should exist");
        handle.await.expect("maintenance worker should shut down cleanly");
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn requested_inline_flush_stays_pending_when_mutation_guard_is_active() {
        let pool = Arc::new(
            build_warm_ducklake_pool(
                DuckLakeConnectionManager {
                    setup_plan: Arc::new(crate::ducklake::config::DuckLakeSetupPlan::default()),
                    disable_extension_autoload: cfg!(target_os = "linux"),
                    interrupt_registry: Arc::new(crate::ducklake::client::DuckLakeInterruptRegistry::default()),
                    open_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                },
                1,
                "test",
            )
            .await
            .expect("failed to build inline flush test pool"),
        );
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let checkpoint_gate = Arc::new(RwLock::new(()));
        let mutation_guard = Arc::clone(&checkpoint_gate).read_owned().await;
        let inline_flush_requested = Arc::new(AtomicBool::new(true));
        let pending_inline_flush_requests = PendingInlineFlushRequests::default();
        pending_inline_flush_requests.request(
            "public_users".to_owned(),
            MaintenanceReason::PendingInlinedDataBytesThreshold,
        );

        let rendered_before = handle.render();
        let skipped_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
        );

        maybe_run_requested_inline_flush(
            Arc::clone(&pool),
            Arc::clone(&checkpoint_gate),
            Arc::new(Semaphore::new(1)),
            inline_flush_requested.as_ref(),
            &pending_inline_flush_requests,
            None,
        )
        .await
        .expect("inline flush should be skipped while mutation work is active");

        let rendered_after = handle.render();
        let skipped_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
        );

        assert!(skipped_after > skipped_before);
        assert!(inline_flush_requested.load(AtomicOrdering::Acquire));
        assert_eq!(
            pending_inline_flush_requests.requests.lock().get("public_users"),
            Some(&MaintenanceReason::PendingInlinedDataBytesThreshold)
        );

        drop(mutation_guard);
    }

    #[tokio::test]
    async fn flush_busy_emits_skip_counter_only() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let rendered_before = handle.render();
        let skipped_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
        );
        let duration_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
            MaintenanceOutcome::SkippedBusy,
        );

        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
        );

        let rendered_after = handle.render();
        let skipped_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
        );
        let duration_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_FLUSH,
            MaintenanceOperation::FlushInlinedData,
            MaintenanceReason::PendingInlinedDataBytesThreshold,
            MaintenanceOutcome::SkippedBusy,
        );

        assert!(skipped_after > skipped_before, "flush skipped counter did not increase");
        assert_eq!(duration_after, duration_before, "flush skip should not emit a duration sample");
    }
}
