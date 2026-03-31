use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::Duration;

use etl::error::{ErrorKind, EtlError, EtlResult};
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
use crate::ducklake::core::{DuckLakeInlineFlushKind, flush_table_inlined_data};
use crate::ducklake::metrics::{
    DuckLakeTableStorageMetrics, ETL_DUCKLAKE_MAINTENANCE_DURATION_SECONDS,
    ETL_DUCKLAKE_MAINTENANCE_SKIPPED_TOTAL, MAINTENANCE_OPERATION_LABEL, MAINTENANCE_OUTCOME_LABEL,
    MAINTENANCE_REASON_LABEL, MAINTENANCE_TASK_LABEL, SMALL_FILE_SIZE_BYTES,
};
use crate::ducklake::{DuckLakeTableName, LAKE_CATALOG};

/// Dedicated pool size for background DuckLake maintenance work.
const MAINTENANCE_POOL_SIZE: u32 = 1;
/// Poll interval for checking per-table inline flush thresholds.
const MAINTENANCE_FLUSH_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Estimated ratio from raw row payload to compressed parquet bytes.
const PARQUET_COMPRESSION_RATIO_ESTIMATE: u64 = 4;
/// Pending bytes threshold that triggers a background inline flush.
const MAINTENANCE_PENDING_BYTES_THRESHOLD: u64 =
    SMALL_FILE_SIZE_BYTES as u64 * PARQUET_COMPRESSION_RATIO_ESTIMATE;
/// Optional pending inserted-rows threshold that triggers a background inline flush.
const MAINTENANCE_PENDING_ROWS_THRESHOLD: Option<u64> = None;
/// Minimum idle window before targeted table maintenance runs, to not have maintenances ran too frequently.
const MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD: Duration = Duration::from_secs(90);
/// Minimum delay between targeted maintenance runs for the same table.
const MAINTENANCE_TABLE_COMPACTION_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Minimum active delete-file count before idle rewrite is worth attempting.
const MAINTENANCE_IDLE_REWRITE_DELETE_FILES_THRESHOLD: i64 = 32;
/// Deleted-row ratio that makes idle rewrite worthwhile.
const MAINTENANCE_IDLE_REWRITE_DELETED_ROW_RATIO_THRESHOLD: f64 = 0.10;
/// Active delete-file count that warrants emergency rewrite.
const MAINTENANCE_EMERGENCY_REWRITE_DELETE_FILES_THRESHOLD: i64 = 128;
/// Deleted-row ratio that warrants emergency rewrite.
const MAINTENANCE_EMERGENCY_REWRITE_DELETED_ROW_RATIO_THRESHOLD: f64 = 0.25;
/// Small-file ratio threshold that makes merge worth attempting.
const MAINTENANCE_MERGE_SMALL_FILE_RATIO_THRESHOLD: f64 = 0.5;
/// Minimum small data-file count before merge is worth attempting.
const MAINTENANCE_MIN_SMALL_DATA_FILES_FOR_MERGE: i64 = 2;
/// Global checkpoint interval used to keep catalog maintenance moving.
const MAINTENANCE_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(15 * 60);
/// Minimum active data-file count before emergency merge is worth attempting.
const MAINTENANCE_MIN_ACTIVE_DATA_FILES: i64 = 8;
/// Timeout for sending a notification to the maintenance worker.
pub(super) const NOTIFICATION_SEND_TIMEOUT: Duration = Duration::from_secs(5);

const MAINTENANCE_TASK_FLUSH: &str = "flush";
const MAINTENANCE_TASK_TARGETED_MAINTENANCE: &str = "targeted_maintenance";
const MAINTENANCE_TASK_CHECKPOINT: &str = "checkpoint";

#[cfg(test)]
static FAIL_CHECKPOINT_ONCE_FOR_TESTS: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FAIL_REWRITE_SINGLE_OUTPUT_FILE_ONCE_FOR_TESTS: AtomicBool = AtomicBool::new(false);

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
    IdleRewriteMetricsThreshold,
    EmergencyRewriteMetricsThreshold,
    IdleMergeMetricsThreshold,
    EmergencyMergeMetricsThreshold,
    CheckpointInterval,
}

impl MaintenanceReason {
    /// Returns the stable metric label value for this reason.
    fn as_str(self) -> &'static str {
        match self {
            Self::PendingBytesThreshold => "pending_bytes_threshold",
            Self::PendingInsertedRowsThreshold => "pending_inserted_rows_threshold",
            Self::IdleRewriteMetricsThreshold => "idle_rewrite_metrics_threshold",
            Self::EmergencyRewriteMetricsThreshold => "emergency_rewrite_metrics_threshold",
            Self::IdleMergeMetricsThreshold => "idle_merge_metrics_threshold",
            Self::EmergencyMergeMetricsThreshold => "emergency_merge_metrics_threshold",
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
pub(super) struct TableWriteActivity {
    pub(super) table_name: DuckLakeTableName,
    pub(super) approx_bytes: u64,
    pub(super) inserted_rows: u64,
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
}

impl TableMaintenanceNotification {
    /// Returns the table name carried by this notification.
    fn table_name(&self) -> &str {
        match self {
            Self::WriteActivity(activity) => &activity.table_name,
            Self::TableMetricsSample(sample) => &sample.table_name,
        }
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
    merge_reason: Option<MaintenanceReason>,
}

impl TargetedMaintenancePlan {
    /// Returns whether this plan selected any maintenance work.
    fn has_work(self) -> bool {
        self.rewrite_reason.is_some() || self.merge_reason.is_some()
    }
}

/// Coalesced maintenance state for one DuckLake table.
#[derive(Debug, Default)]
struct TableMaintenanceState {
    pending_bytes: u64,
    pending_inserted_rows: u64,
    dirty_since_compaction: bool,
    last_write_at: Option<Instant>,
    last_targeted_maintenance_at: Option<Instant>,
    latest_storage_metrics: Option<DuckLakeTableStorageMetrics>,
    latest_storage_metrics_sampled_at: Option<Instant>,
    last_emergency_assessment_at: Option<Instant>,
}

impl TableMaintenanceState {
    /// Aggregates one write notification into the existing table state.
    fn record_write_activity(&mut self, notification: &TableWriteActivity, now: Instant) {
        self.pending_bytes = self.pending_bytes.saturating_add(notification.approx_bytes);
        self.pending_inserted_rows = self
            .pending_inserted_rows
            .saturating_add(notification.inserted_rows);
        self.dirty_since_compaction = true;
        self.last_write_at = Some(now);
    }

    /// Records one sampled metrics snapshot for this table.
    fn record_metrics_sample(&mut self, sample: TableMetricsSample) {
        self.latest_storage_metrics = Some(sample.metrics);
        self.latest_storage_metrics_sampled_at = Some(sample.sampled_at);
    }

    /// Returns whether the table still has pending inline work.
    fn has_pending_flush_work(&self) -> bool {
        self.pending_bytes > 0 || self.pending_inserted_rows > 0
    }

    /// Returns the primary reason that pending inlined work should be flushed.
    fn flush_reason(&self, now: Instant) -> Option<MaintenanceReason> {
        self.flush_reason_with_pending_rows_threshold(now, MAINTENANCE_PENDING_ROWS_THRESHOLD)
    }

    /// Returns the primary reason that pending inlined work should be flushed.
    fn flush_reason_with_pending_rows_threshold(
        &self,
        _now: Instant,
        pending_rows_threshold: Option<u64>,
    ) -> Option<MaintenanceReason> {
        if self.pending_bytes >= MAINTENANCE_PENDING_BYTES_THRESHOLD {
            return Some(MaintenanceReason::PendingBytesThreshold);
        }

        if let Some(pending_rows_threshold) = pending_rows_threshold
            && self.pending_inserted_rows >= pending_rows_threshold
        {
            return Some(MaintenanceReason::PendingInsertedRowsThreshold);
        }

        None
    }

    /// Clears pending flush counters after a successful flush/materialization.
    fn clear_pending_flush(&mut self) {
        self.pending_bytes = 0;
        self.pending_inserted_rows = 0;
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
            Some(targeted_maintenance_plan(
                metrics,
                TargetedMaintenanceScope::Idle,
            ))
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

/// Records all selected targeted-maintenance operations as skipped because the table is busy.
fn record_skipped_targeted_maintenance(plan: TargetedMaintenancePlan) {
    if let Some(reason) = plan.rewrite_reason {
        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            reason,
        );
    }

    if let Some(reason) = plan.merge_reason {
        record_ducklake_maintenance_skipped(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            reason,
        );
    }
}

/// Returns the targeted-maintenance cycle outcome from successful per-operation outcomes.
fn targeted_maintenance_outcome(
    rewrite_outcome: Option<MaintenanceOutcome>,
    merge_outcome: Option<MaintenanceOutcome>,
) -> MaintenanceOutcome {
    debug_assert!(rewrite_outcome.is_none_or(MaintenanceOutcome::is_completed));
    debug_assert!(merge_outcome.is_none_or(MaintenanceOutcome::is_completed));

    if rewrite_outcome == Some(MaintenanceOutcome::Applied)
        || merge_outcome == Some(MaintenanceOutcome::Applied)
    {
        MaintenanceOutcome::Applied
    } else {
        MaintenanceOutcome::Noop
    }
}

/// Returns whether this maintenance failure matches a known DuckLake compaction bug.
fn is_known_ducklake_compaction_single_output_file_error(error: &EtlError) -> bool {
    error.detail().is_some_and(|detail| {
        detail.contains("INTERNAL Error: DuckLakeCompaction - expected a single output file")
    })
}

/// Returns the failing maintenance operation and reason for one known compaction bug.
fn known_ducklake_compaction_error_context(
    plan: TargetedMaintenancePlan,
    error: &EtlError,
) -> Option<(MaintenanceOperation, MaintenanceReason)> {
    if !is_known_ducklake_compaction_single_output_file_error(error) {
        return None;
    }

    match error.description() {
        Some("DuckLake rewrite data files failed") => plan
            .rewrite_reason
            .map(|reason| (MaintenanceOperation::RewriteDataFiles, reason)),
        Some("DuckLake merge adjacent files failed") => plan
            .merge_reason
            .map(|reason| (MaintenanceOperation::MergeAdjacentFiles, reason)),
        _ => None,
    }
}

/// Logs and suppresses one known DuckLake compaction internal error.
///
/// Connection recycling is handled generically in [`run_duckdb_blocking`], which
/// marks any failing DuckDB connection as broken before it returns the error to
/// this layer. This helper stays intentionally narrow: it only decides which
/// maintenance-only failures are safe to downgrade after the pool has already
/// recycled the invalidated connection.
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
    let active_data_files = metrics.active_data_files.max(0);
    let small_data_files = metrics.small_data_files.max(0);
    let deleted_row_ratio = metrics.deleted_row_ratio();
    let small_file_ratio = metrics.small_file_ratio();
    let average_data_file_size_bytes = metrics.average_data_file_size_bytes();

    match scope {
        TargetedMaintenanceScope::Idle => {
            if active_delete_files >= MAINTENANCE_IDLE_REWRITE_DELETE_FILES_THRESHOLD
                && deleted_row_ratio >= MAINTENANCE_IDLE_REWRITE_DELETED_ROW_RATIO_THRESHOLD
            {
                plan.rewrite_reason = Some(MaintenanceReason::IdleRewriteMetricsThreshold);
            }

            if small_data_files >= MAINTENANCE_MIN_SMALL_DATA_FILES_FOR_MERGE
                && small_file_ratio > MAINTENANCE_MERGE_SMALL_FILE_RATIO_THRESHOLD
                && average_data_file_size_bytes < SMALL_FILE_SIZE_BYTES as f64
            {
                plan.merge_reason = Some(MaintenanceReason::IdleMergeMetricsThreshold);
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

            if active_data_files > MAINTENANCE_MIN_ACTIVE_DATA_FILES
                && small_data_files >= MAINTENANCE_MIN_SMALL_DATA_FILES_FOR_MERGE
                && small_file_ratio > MAINTENANCE_MERGE_SMALL_FILE_RATIO_THRESHOLD
                && average_data_file_size_bytes < SMALL_FILE_SIZE_BYTES as f64
            {
                plan.merge_reason = Some(MaintenanceReason::EmergencyMergeMetricsThreshold);
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
    if let Err(error) = notification_tx
        .send_timeout(notification, NOTIFICATION_SEND_TIMEOUT)
        .await
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

/// Coalesces notifications and runs background DuckLake maintenance.
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
                    .entry(notification.table_name().to_string())
                    .and_modify(|state| match &notification {
                        TableMaintenanceNotification::WriteActivity(activity) => {
                            state.record_write_activity(activity, now);
                        }
                        TableMaintenanceNotification::TableMetricsSample(sample) => {
                            state.record_metrics_sample(sample.clone());
                        }
                    })
                    .or_insert_with(|| {
                        let mut state = TableMaintenanceState::default();
                        match notification {
                            TableMaintenanceNotification::WriteActivity(activity) => {
                                state.record_write_activity(&activity, now);
                            }
                            TableMaintenanceNotification::TableMetricsSample(sample) => {
                                state.record_metrics_sample(sample);
                            }
                        }
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

                    if let Some((plan, sampled_at)) = table_state
                        .emergency_targeted_maintenance_plan(now)
                    {
                        table_state.last_emergency_assessment_at = Some(sampled_at);
                        if plan.has_work() {
                            match run_targeted_table_maintenance(
                                Arc::clone(&pool),
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

                        match run_targeted_table_maintenance(
                            Arc::clone(&pool),
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
                    state.has_pending_flush_work()
                        || state.dirty_since_compaction
                        || state.latest_storage_metrics.is_some()
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
    plan: TargetedMaintenancePlan,
) -> EtlResult<MaintenanceOutcome> {
    let Some(table_write_permit) = try_acquire_table_write_slot(&table_write_slots, &table_name)
    else {
        record_skipped_targeted_maintenance(plan);
        return Ok(MaintenanceOutcome::SkippedBusy);
    };
    let table_name_for_query = table_name.clone();
    let plan_for_query = plan;

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
    let rows_flushed =
        flush_table_inlined_data(conn, table_name, DuckLakeInlineFlushKind::Mutation).inspect_err(
            |_error| {
                record_ducklake_maintenance_duration(
                    MAINTENANCE_TASK_FLUSH,
                    MaintenanceOperation::FlushInlinedData,
                    reason,
                    MaintenanceOutcome::Failed,
                    flush_started.elapsed().as_secs_f64(),
                );
            },
        )?;
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
    let mut merged_files = 0u64;
    let mut rewrite_outcome = None;
    let mut merge_outcome = None;

    if let Some(reason) = plan.rewrite_reason {
        let rewrite_started = Instant::now();
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

    if let Some(reason) = plan.merge_reason {
        let merge_started = Instant::now();
        merged_files = merge_adjacent_table_files(conn, table_name).inspect_err(|_error| {
            record_ducklake_maintenance_duration(
                MAINTENANCE_TASK_TARGETED_MAINTENANCE,
                MaintenanceOperation::MergeAdjacentFiles,
                reason,
                MaintenanceOutcome::Failed,
                merge_started.elapsed().as_secs_f64(),
            );
        })?;
        let outcome = MaintenanceOutcome::from(merged_files);
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            reason,
            outcome,
            merge_started.elapsed().as_secs_f64(),
        );
        merge_outcome = Some(outcome);
    }

    info!(
        table = %table_name,
        rewrite_selected = plan.rewrite_reason.is_some(),
        merge_selected = plan.merge_reason.is_some(),
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
    #[cfg(test)]
    if FAIL_REWRITE_SINGLE_OUTPUT_FILE_ONCE_FOR_TESTS.swap(false, AtomicOrdering::Relaxed) {
        let source = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("INTERNAL Error: DuckLakeCompaction - expected a single output file".to_string()),
        );
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake rewrite data files failed",
            format_query_error_detail(&sql, &source),
            source: source
        ));
    }

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
         FROM ducklake_merge_adjacent_files({}, {}, max_file_size => {});"#,
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
        SMALL_FILE_SIZE_BYTES,
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

    use std::sync::{Arc, LazyLock};

    use etl_telemetry::metrics::init_metrics_handle;

    use crate::ducklake::client::{
        DuckDbBlockingOperationKind, DuckLakeConnectionManager, build_warm_ducklake_pool,
        run_duckdb_blocking,
    };
    use crate::ducklake::metrics::register_metrics;

    static CHECKPOINT_TEST_GUARD: LazyLock<Arc<Semaphore>> =
        LazyLock::new(|| Arc::new(Semaphore::new(1)));

    async fn acquire_checkpoint_test_guard() -> OwnedSemaphorePermit {
        Arc::clone(&CHECKPOINT_TEST_GUARD)
            .acquire_owned()
            .await
            .expect("checkpoint test semaphore should stay open")
    }

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

    fn write_activity(approx_bytes: u64, inserted_rows: u64) -> TableWriteActivity {
        TableWriteActivity {
            table_name: "public_users".to_string(),
            approx_bytes,
            inserted_rows,
        }
    }

    fn table_metrics_sample(
        sampled_at: Instant,
        metrics: DuckLakeTableStorageMetrics,
    ) -> TableMetricsSample {
        TableMetricsSample {
            table_name: "public_users".to_string(),
            sampled_at,
            metrics,
        }
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
            MaintenanceReason::PendingInsertedRowsThreshold,
            MaintenanceOutcome::Noop,
        );
        let rewrite_applied_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Applied,
        );
        let merge_noop_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::IdleMergeMetricsThreshold,
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
            MaintenanceReason::PendingInsertedRowsThreshold,
            MaintenanceOutcome::Noop,
            0.15,
        );
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Applied,
            1.0,
        );
        record_ducklake_maintenance_duration(
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::IdleMergeMetricsThreshold,
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
            MaintenanceReason::PendingInsertedRowsThreshold,
            MaintenanceOutcome::Noop,
        );
        let rewrite_applied_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Applied,
        );
        let merge_noop_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::IdleMergeMetricsThreshold,
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
    async fn test_targeted_maintenance_busy_emits_skip_counter_only_for_selected_operations() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let plan = TargetedMaintenancePlan {
            rewrite_reason: Some(MaintenanceReason::EmergencyRewriteMetricsThreshold),
            merge_reason: None,
        };

        let rendered_before = handle.render();
        let rewrite_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );
        let merge_before = maintenance_skipped_counter_value(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::EmergencyMergeMetricsThreshold,
        );
        let rewrite_duration_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
            MaintenanceOutcome::SkippedBusy,
        );
        let merge_duration_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::EmergencyMergeMetricsThreshold,
            MaintenanceOutcome::SkippedBusy,
        );

        record_skipped_targeted_maintenance(plan);

        let rendered_after = handle.render();
        let rewrite_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
        );
        let merge_after = maintenance_skipped_counter_value(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::EmergencyMergeMetricsThreshold,
        );
        let rewrite_duration_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::EmergencyRewriteMetricsThreshold,
            MaintenanceOutcome::SkippedBusy,
        );
        let merge_duration_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::EmergencyMergeMetricsThreshold,
            MaintenanceOutcome::SkippedBusy,
        );

        assert!(
            rewrite_after > rewrite_before,
            "rewrite skipped counter did not increase"
        );
        assert!(
            (merge_after - merge_before).abs() < f64::EPSILON,
            "merge skipped counter should stay unchanged when merge was not selected"
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
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(&write_activity(MAINTENANCE_PENDING_BYTES_THRESHOLD, 1), now);

        assert_eq!(
            state.flush_reason_with_pending_rows_threshold(now, Some(1)),
            Some(MaintenanceReason::PendingBytesThreshold)
        );
    }

    #[test]
    fn test_table_maintenance_state_disables_pending_inserted_rows_flush_reason_by_default() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(
            &write_activity(MAINTENANCE_PENDING_BYTES_THRESHOLD - 1, 10_000),
            now,
        );

        assert_eq!(state.flush_reason(now), None);
    }

    #[test]
    fn test_table_maintenance_state_uses_pending_inserted_rows_flush_reason_when_enabled() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(
            &write_activity(MAINTENANCE_PENDING_BYTES_THRESHOLD - 1, 10_000),
            now,
        );

        assert_eq!(
            state.flush_reason_with_pending_rows_threshold(now, Some(10_000)),
            Some(MaintenanceReason::PendingInsertedRowsThreshold)
        );
    }

    #[test]
    fn test_table_maintenance_state_does_not_use_idle_flush_reason() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(&write_activity(128, 1), now);

        assert_eq!(state.flush_reason(now + Duration::from_secs(60 * 10)), None);
    }

    #[test]
    fn test_table_maintenance_state_records_metrics_samples() {
        let now = Instant::now();
        let metrics = storage_metrics(10, 20_000_000, 6, 1000, 4, 1024, 25);
        let mut state = TableMaintenanceState::default();
        state.record_metrics_sample(table_metrics_sample(now, metrics.clone()));
        let (recorded_metrics, sampled_at) = state
            .current_storage_metrics()
            .expect("metrics sample should be visible");

        assert_eq!(sampled_at, now);
        assert_eq!(recorded_metrics.small_data_files, metrics.small_data_files);
    }

    #[test]
    fn test_table_maintenance_state_waits_for_fresh_sample_after_write() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        let stale_metrics = storage_metrics(8, 16_000_000, 6, 1000, 0, 0, 0);
        state.record_metrics_sample(table_metrics_sample(now, stale_metrics));
        state.record_write_activity(&write_activity(1024, 1), now + Duration::from_secs(1));

        assert!(
            state
                .idle_targeted_maintenance_plan(
                    now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD
                        + MAINTENANCE_TABLE_COMPACTION_INTERVAL,
                )
                .is_none(),
            "maintenance should wait for a sample that includes the latest write"
        );
    }

    #[test]
    fn test_table_maintenance_state_selects_idle_merge_from_small_file_ratio() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(&write_activity(1024, 1), now);
        state.record_metrics_sample(table_metrics_sample(
            now + Duration::from_secs(1),
            storage_metrics(10, 20_000_000, 6, 1000, 0, 0, 0),
        ));

        assert_eq!(
            state.idle_targeted_maintenance_plan(
                now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD
                    + MAINTENANCE_TABLE_COMPACTION_INTERVAL,
            ),
            Some(TargetedMaintenancePlan {
                rewrite_reason: None,
                merge_reason: Some(MaintenanceReason::IdleMergeMetricsThreshold),
            })
        );
    }

    #[test]
    fn test_table_maintenance_state_does_not_select_merge_when_ratio_is_half() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(&write_activity(1024, 1), now);
        state.record_metrics_sample(table_metrics_sample(
            now + Duration::from_secs(1),
            storage_metrics(10, 20_000_000, 5, 1000, 0, 0, 0),
        ));

        assert_eq!(
            state.idle_targeted_maintenance_plan(
                now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD
                    + MAINTENANCE_TABLE_COMPACTION_INTERVAL,
            ),
            Some(TargetedMaintenancePlan::default())
        );
    }

    #[test]
    fn test_table_maintenance_state_does_not_select_merge_when_average_size_is_healthy() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(&write_activity(1024, 1), now);
        state.record_metrics_sample(table_metrics_sample(
            now + Duration::from_secs(1),
            storage_metrics(10, 80_000_000, 6, 1000, 0, 0, 0),
        ));

        assert_eq!(
            state.idle_targeted_maintenance_plan(
                now + MAINTENANCE_TABLE_COMPACTION_IDLE_THRESHOLD
                    + MAINTENANCE_TABLE_COMPACTION_INTERVAL,
            ),
            Some(TargetedMaintenancePlan::default())
        );
    }

    #[test]
    fn test_targeted_maintenance_plan_does_not_select_merge_with_one_small_file() {
        let metrics = storage_metrics(1, 2_343_233, 1, 1000, 0, 0, 0);

        assert_eq!(
            targeted_maintenance_plan(&metrics, TargetedMaintenanceScope::Idle),
            TargetedMaintenancePlan::default()
        );
        assert_eq!(
            targeted_maintenance_plan(&metrics, TargetedMaintenanceScope::Emergency),
            TargetedMaintenancePlan::default()
        );
    }

    #[test]
    fn test_table_maintenance_state_selects_idle_rewrite_from_delete_pressure() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(&write_activity(1024, 1), now);
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
                merge_reason: None,
            })
        );
    }

    #[test]
    fn test_targeted_maintenance_plan_does_not_select_emergency_rewrite_from_ratio_alone() {
        let metrics = storage_metrics(10, 80_000_000, 0, 1000, 1, 65_101, 936);

        assert_eq!(
            targeted_maintenance_plan(&metrics, TargetedMaintenanceScope::Emergency),
            TargetedMaintenancePlan::default()
        );
    }

    #[test]
    fn test_table_maintenance_state_selects_emergency_rewrite_from_delete_pressure() {
        let now = Instant::now();
        let mut state = TableMaintenanceState::default();
        state.record_write_activity(&write_activity(1024, 1), now);
        state.record_metrics_sample(table_metrics_sample(
            now + Duration::from_secs(1),
            storage_metrics(10, 20_000_000, 2, 1000, 128, 10_000, 150),
        ));

        assert_eq!(
            state.emergency_targeted_maintenance_plan(now + MAINTENANCE_TABLE_COMPACTION_INTERVAL,),
            Some((
                TargetedMaintenancePlan {
                    rewrite_reason: Some(MaintenanceReason::EmergencyRewriteMetricsThreshold,),
                    merge_reason: None,
                },
                now + Duration::from_secs(1),
            ))
        );
    }

    #[test]
    fn test_targeted_maintenance_plan_selects_emergency_merge_when_table_is_fragmented() {
        let metrics = storage_metrics(10, 20_000_000, 6, 1000, 0, 0, 0);

        assert_eq!(
            targeted_maintenance_plan(&metrics, TargetedMaintenanceScope::Emergency),
            TargetedMaintenancePlan {
                rewrite_reason: None,
                merge_reason: Some(MaintenanceReason::EmergencyMergeMetricsThreshold),
            }
        );
    }

    #[test]
    fn test_targeted_maintenance_plan_does_not_select_work_for_sample_shaped_metrics() {
        let metrics = storage_metrics(1, 2_343_233, 1, 1000, 1, 65_101, 936);

        assert_eq!(
            targeted_maintenance_plan(&metrics, TargetedMaintenanceScope::Emergency),
            TargetedMaintenancePlan::default()
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
            targeted_maintenance_outcome(
                Some(MaintenanceOutcome::Noop),
                Some(MaintenanceOutcome::Noop),
            ),
            MaintenanceOutcome::Noop
        );
        assert_eq!(
            targeted_maintenance_outcome(
                Some(MaintenanceOutcome::Applied),
                Some(MaintenanceOutcome::Noop),
            ),
            MaintenanceOutcome::Applied
        );
        assert_eq!(
            targeted_maintenance_outcome(
                Some(MaintenanceOutcome::Noop),
                Some(MaintenanceOutcome::Applied),
            ),
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
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Failed,
        );
        let merge_failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::IdleMergeMetricsThreshold,
            MaintenanceOutcome::Failed,
        );

        let error = run_targeted_table_maintenance_blocking(
            &conn,
            "public_users",
            TargetedMaintenancePlan {
                rewrite_reason: Some(MaintenanceReason::IdleRewriteMetricsThreshold),
                merge_reason: Some(MaintenanceReason::IdleMergeMetricsThreshold),
            },
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
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Failed,
        );
        let merge_failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::MergeAdjacentFiles,
            MaintenanceReason::IdleMergeMetricsThreshold,
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

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn test_known_rewrite_single_output_file_error_is_suppressed_and_recycles_connection() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();
        let open_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let manager = DuckLakeConnectionManager {
            setup_sql: std::sync::Arc::new(String::new()),
            disable_extension_autoload: cfg!(target_os = "linux"),
            open_count: std::sync::Arc::clone(&open_count),
        };
        let pool = std::sync::Arc::new(
            build_warm_ducklake_pool(manager, 1, "test")
                .await
                .expect("failed to build maintenance test pool"),
        );
        let blocking_slots = std::sync::Arc::new(Semaphore::new(1));
        let table_write_slots = std::sync::Arc::new(Mutex::new(HashMap::new()));
        FAIL_REWRITE_SINGLE_OUTPUT_FILE_ONCE_FOR_TESTS.store(true, AtomicOrdering::Relaxed);

        let rendered_before = handle.render();
        let rewrite_failed_before = maintenance_duration_count(
            &rendered_before,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Failed,
        );

        let outcome = run_targeted_table_maintenance(
            std::sync::Arc::clone(&pool),
            std::sync::Arc::clone(&blocking_slots),
            std::sync::Arc::clone(&table_write_slots),
            "public_users".to_string(),
            TargetedMaintenancePlan {
                rewrite_reason: Some(MaintenanceReason::IdleRewriteMetricsThreshold),
                merge_reason: None,
            },
        )
        .await
        .expect("known DuckLake compaction bug should be suppressed");

        assert_eq!(outcome, MaintenanceOutcome::Noop);

        let verification = run_duckdb_blocking(
            std::sync::Arc::clone(&pool),
            std::sync::Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Maintenance,
            |conn| {
                conn.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
                    .map_err(|source| {
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
        assert!(
            open_count.load(AtomicOrdering::Relaxed) > 1,
            "expected broken duckdb connection to be recreated after internal error"
        );

        let rendered_after = handle.render();
        let rewrite_failed_after = maintenance_duration_count(
            &rendered_after,
            MAINTENANCE_TASK_TARGETED_MAINTENANCE,
            MaintenanceOperation::RewriteDataFiles,
            MaintenanceReason::IdleRewriteMetricsThreshold,
            MaintenanceOutcome::Failed,
        );

        assert!(
            rewrite_failed_after > rewrite_failed_before,
            "rewrite failed duration count did not increase"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_success_records_applied_duration() {
        let _checkpoint_test_guard = acquire_checkpoint_test_guard().await;
        FAIL_CHECKPOINT_ONCE_FOR_TESTS.store(false, AtomicOrdering::Relaxed);

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
        let _checkpoint_test_guard = acquire_checkpoint_test_guard().await;
        FAIL_CHECKPOINT_ONCE_FOR_TESTS.store(false, AtomicOrdering::Relaxed);

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

        FAIL_CHECKPOINT_ONCE_FOR_TESTS.store(false, AtomicOrdering::Relaxed);
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
