use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use etl_config::shared::BatchConfig;
use futures::{Stream, StreamExt};
use metrics::{counter, gauge, histogram};
use tokio::{
    pin,
    sync::{Mutex, oneshot, watch},
    task::{JoinHandle, JoinSet},
};
use tracing::info;

#[cfg(feature = "failpoints")]
use crate::failpoints::{START_TABLE_SYNC_DURING_DATA_SYNC_FP, etl_fail_point};
use crate::{
    data::TableRow,
    destination::{Destination, WriteTableRowsResult},
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::{
        ACTION_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS, ETL_TABLE_COPY_ACTIVE_WORKERS,
        ETL_TABLE_COPY_PARTITION_DURATION_SECONDS, ETL_TABLE_COPY_PARTITION_ROWS,
        ETL_TABLE_COPY_PARTITION_ROWS_IMBALANCE, ETL_TABLE_COPY_PARTITION_TIME_IMBALANCE,
        ETL_TABLE_COPY_PARTITIONS_COMPLETED_CURRENT, ETL_TABLE_COPY_PARTITIONS_PLANNED,
        ETL_TABLE_COPY_PARTITIONS_TOTAL, ETL_TABLE_COPY_ROWS_CURRENT, ETL_TABLE_COPY_ROWS_TOTAL,
        OUTCOME_LABEL, TABLE_ID_LABEL, WORKER_TYPE_LABEL,
    },
    postgres::{
        TableCopyStream,
        client::{
            CtidPartition, PgChildReplicationTransaction, PgReplicationTransaction,
            PostgresConnectionUpdate,
        },
    },
    runtime::{
        BatchBudgetController, MemoryMonitor,
        concurrency::{
            ShutdownResult, ShutdownRx, TryBatchBackpressureStream,
            table_sync_worker_copy_stream_id,
        },
    },
    schema::{ReplicatedTableSchema, TableId},
};

/// Target number of CTID ranges per worker when copy is parallel.
const CTID_PARTITIONS_PER_COPY_WORKER: u16 = 4;
/// Target estimated rows per CTID range for large tables.
const CTID_COPY_ROWS_PER_PARTITION: i64 = 250_000;
/// Maximum CTID ranges planned for one tracked table.
const MAX_CTID_COPY_PARTITIONS: u16 = 1024;
/// Interval for publishing in-flight table copy progress.
const TABLE_COPY_PROGRESS_REPORT_INTERVAL: Duration = Duration::from_millis(500);

/// Result of a table copy operation.
#[derive(Debug)]
pub(crate) enum TableCopyResult {
    /// All rows copied successfully.
    Completed { total_rows: u64, total_duration_secs: f64 },
    /// Copy was interrupted by a shutdown signal.
    Shutdown,
}

/// A physical ctid range copied by one worker.
#[derive(Debug)]
struct CopyPartition {
    /// The table to issue COPY against.
    source_table_id: TableId,
    /// The table used to resolve publication row filters.
    filter_table_id: TableId,
    /// The physical ctid range for `source_table_id`.
    ctid_partition: CtidPartition,
}

/// Planned work for one tracked table copy.
#[derive(Debug)]
struct PlannedCopyPartitions {
    /// CTID work items workers will pull from.
    copy_partitions: Vec<CopyPartition>,
    /// Target partition count used before empty physical tables were skipped.
    target_partitions: u16,
}

/// Cumulative metrics shared by all workers copying one tracked table.
#[derive(Clone, Debug)]
struct TableCopyMetrics {
    /// Stable label for the tracked table.
    table_id_label: Arc<str>,
    /// Rows flushed by all workers.
    copied_rows: Arc<AtomicU64>,
    /// Rows already emitted to the cumulative counter.
    reported_rows: Arc<AtomicU64>,
    /// Partitions finished by all workers.
    completed_partitions: Arc<AtomicU64>,
}

impl TableCopyMetrics {
    /// Creates metrics for one table copy.
    fn new(table_id: TableId, planned_partitions: usize, active_workers: usize) -> Self {
        let metrics = Self {
            table_id_label: Arc::from(table_id.to_string()),
            copied_rows: Arc::new(AtomicU64::new(0)),
            reported_rows: Arc::new(AtomicU64::new(0)),
            completed_partitions: Arc::new(AtomicU64::new(0)),
        };

        gauge!(
            ETL_TABLE_COPY_ROWS_CURRENT,
            TABLE_ID_LABEL => metrics.table_id_label.to_string(),
        )
        .set(0.0);
        gauge!(
            ETL_TABLE_COPY_PARTITIONS_PLANNED,
            TABLE_ID_LABEL => metrics.table_id_label.to_string(),
        )
        .set(planned_partitions as f64);
        gauge!(
            ETL_TABLE_COPY_PARTITIONS_COMPLETED_CURRENT,
            TABLE_ID_LABEL => metrics.table_id_label.to_string(),
        )
        .set(0.0);
        gauge!(
            ETL_TABLE_COPY_ACTIVE_WORKERS,
            TABLE_ID_LABEL => metrics.table_id_label.to_string(),
        )
        .set(active_workers as f64);

        metrics
    }

    /// Records rows flushed by one destination batch.
    fn record_copied_rows(&self, rows: u64) {
        self.copied_rows.fetch_add(rows, Ordering::Relaxed);
    }

    /// Publishes the latest cumulative table-copy progress.
    fn publish_progress(&self) {
        let copied_rows = self.copied_rows.load(Ordering::Relaxed);
        let reported_rows = self.reported_rows.swap(copied_rows, Ordering::Relaxed);

        if copied_rows > reported_rows {
            counter!(
                ETL_TABLE_COPY_ROWS_TOTAL,
                TABLE_ID_LABEL => self.table_id_label.to_string(),
            )
            .increment(copied_rows - reported_rows);
        }

        gauge!(
            ETL_TABLE_COPY_ROWS_CURRENT,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
        )
        .set(copied_rows as f64);
        gauge!(
            ETL_TABLE_COPY_PARTITIONS_COMPLETED_CURRENT,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
        )
        .set(self.completed_partitions.load(Ordering::Relaxed) as f64);
    }

    /// Records one successfully copied ctid partition.
    fn record_completed_partition(&self, rows: u64, duration_secs: f64) {
        self.completed_partitions.fetch_add(1, Ordering::Relaxed);

        counter!(
            ETL_TABLE_COPY_PARTITIONS_TOTAL,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
            OUTCOME_LABEL => "completed",
        )
        .increment(1);
        histogram!(
            ETL_TABLE_COPY_PARTITION_ROWS,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
        )
        .record(rows as f64);
        histogram!(
            ETL_TABLE_COPY_PARTITION_DURATION_SECONDS,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
        )
        .record(duration_secs);
    }

    /// Records one partition that observed shutdown before completing.
    fn record_shutdown_partition(&self) {
        counter!(
            ETL_TABLE_COPY_PARTITIONS_TOTAL,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
            OUTCOME_LABEL => "shutdown",
        )
        .increment(1);
    }

    /// Records final load imbalance for completed copy partitions.
    fn record_imbalance(&self, partition_durations: &[f64], partition_row_counts: &[u64]) {
        let time_lif = calculate_skew_metrics(partition_durations);
        let rows_lif = calculate_skew_metrics(
            &partition_row_counts.iter().map(|rows| *rows as f64).collect::<Vec<_>>(),
        );

        histogram!(
            ETL_TABLE_COPY_PARTITION_TIME_IMBALANCE,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
        )
        .record(time_lif);
        histogram!(
            ETL_TABLE_COPY_PARTITION_ROWS_IMBALANCE,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
        )
        .record(rows_lif);
    }

    /// Marks the table copy as no longer actively using worker connections.
    fn finish(&self) {
        self.publish_progress();
        gauge!(
            ETL_TABLE_COPY_ACTIVE_WORKERS,
            TABLE_ID_LABEL => self.table_id_label.to_string(),
        )
        .set(0.0);
    }
}

/// Ensures terminal table-copy metrics are published on all return paths.
struct ActiveTableCopyMetrics {
    /// Metrics to finalize when the guard is dropped.
    metrics: TableCopyMetrics,
}

impl ActiveTableCopyMetrics {
    /// Creates an active metrics guard.
    fn new(metrics: TableCopyMetrics) -> Self {
        Self { metrics }
    }
}

impl Drop for ActiveTableCopyMetrics {
    fn drop(&mut self) {
        self.metrics.finish();
    }
}

/// Handle for the table-copy metrics reporter task.
struct TableCopyMetricsReporter {
    /// Signal used to stop the reporter after copy completion.
    stop_tx: oneshot::Sender<()>,
    /// Join handle for the reporter task.
    handle: JoinHandle<()>,
}

/// Rows and timings copied by a completed worker.
#[derive(Debug)]
struct CompletedCopyWorker {
    /// Rows copied by this worker.
    total_rows: u64,
    /// Durations of completed partitions.
    partition_durations: Vec<f64>,
    /// Row counts of completed partitions.
    partition_row_counts: Vec<u64>,
}

/// Outcome of one worker connection.
#[derive(Debug)]
enum CopyWorkerOutcome {
    /// The worker drained all available work and committed its transaction.
    Completed(CompletedCopyWorker),
    /// The worker observed shutdown before committing its transaction.
    Shutdown,
}

/// Calculates Load Imbalance Factor (LIF) for a set of values.
///
/// LIF = max_value / mean_value.
///
/// A value of 1.0 indicates perfect balance. Higher values indicate more
/// imbalance. Returns 0.0 if the input is empty or mean is zero.
fn calculate_skew_metrics(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;

    if mean == 0.0 {
        return 0.0;
    }

    let max_value = values
        .iter()
        .copied()
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);

    max_value / mean
}

/// Returns `numerator / denominator`, rounded up.
fn div_ceil_u128(numerator: u128, denominator: u128) -> u128 {
    debug_assert!(denominator > 0);

    let quotient = numerator / denominator;
    let remainder = numerator % denominator;

    quotient + if remainder > 0 { 1 } else { 0 }
}

/// Returns how many ctid ranges to target for a table copy.
fn target_ctid_partition_count(
    max_copy_connections: u16,
    total_estimated_rows: Option<i64>,
) -> u16 {
    if max_copy_connections == 1 {
        return 1;
    }

    let worker_target = max_copy_connections.saturating_mul(CTID_PARTITIONS_PER_COPY_WORKER).max(1);
    let row_target = total_estimated_rows.filter(|estimated_rows| *estimated_rows > 0).map_or(
        1,
        |estimated_rows| {
            div_ceil_u128(estimated_rows as u128, CTID_COPY_ROWS_PER_PARTITION as u128)
                .min(u128::from(u16::MAX))
                .max(1) as u16
        },
    );

    worker_target.max(row_target).clamp(1, MAX_CTID_COPY_PARTITIONS)
}

/// Allocates ctid partition count to one physical table by planning weight.
fn partitions_for_table_weight(
    partition_weight: i64,
    total_weight: i64,
    block_count: i64,
    target_partitions: u16,
) -> u16 {
    debug_assert!(partition_weight > 0);
    debug_assert!(total_weight > 0);
    debug_assert!(block_count > 0);

    let partition_weight = partition_weight as u128;
    let total_weight = total_weight as u128;
    let block_count = block_count as u64;
    let target_partitions = u128::from(target_partitions);
    let weighted_partitions = div_ceil_u128(partition_weight * target_partitions, total_weight);

    weighted_partitions.min(u128::from(block_count)).min(u128::from(u16::MAX)).max(1) as u16
}

/// Chooses the planning weight for one physical table.
fn table_copy_partition_weight(
    estimated_rows: i64,
    table_blocks: i64,
    use_estimated_rows: bool,
) -> i64 {
    if use_estimated_rows { estimated_rows } else { table_blocks }
}

/// Returns true when the table copy should stop for shutdown.
fn is_shutdown_requested(shutdown_rx: &ShutdownRx) -> bool {
    shutdown_rx.has_changed().unwrap_or(true)
}

/// Spawns a periodic reporter for progressive table-copy metrics.
fn spawn_table_copy_metrics_reporter(
    metrics: TableCopyMetrics,
    mut shutdown_rx: ShutdownRx,
) -> TableCopyMetricsReporter {
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(TABLE_COPY_PROGRESS_REPORT_INTERVAL);

        loop {
            tokio::select! {
                biased;

                _ = &mut stop_rx => {
                    metrics.publish_progress();
                    break;
                }
                _ = shutdown_rx.changed() => {
                    metrics.publish_progress();
                    break;
                }
                _ = interval.tick() => {
                    metrics.publish_progress();
                }
            }
        }
    });

    TableCopyMetricsReporter { stop_tx, handle }
}

/// Stops the periodic reporter and emits one final progress sample.
async fn stop_table_copy_metrics_reporter(
    metrics_reporter: &mut Option<TableCopyMetricsReporter>,
    metrics: &TableCopyMetrics,
) {
    if let Some(metrics_reporter) = metrics_reporter.take() {
        let _ = metrics_reporter.stop_tx.send(());
        let _ = metrics_reporter.handle.await;
    }

    metrics.publish_progress();
}

/// Copies a table through ctid work items, using worker child connections.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn table_copy<D: Destination + Clone + Send + 'static>(
    replication_transaction: &PgReplicationTransaction<'_>,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<&str>,
    max_copy_connections: u16,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult> {
    worker_table_copy(
        replication_transaction,
        table_id,
        replicated_table_schema,
        publication_name,
        max_copy_connections.max(1),
        batch_config,
        shutdown_rx,
        destination,
        memory_monitor,
        batch_budget,
    )
    .await
}

/// Copies a table by assigning physical ctid ranges to child-connection
/// workers.
#[expect(clippy::too_many_arguments)]
async fn worker_table_copy<D: Destination + Clone + Send + 'static>(
    replication_transaction: &PgReplicationTransaction<'_>,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<&str>,
    max_copy_connections: u16,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult> {
    let start_time = Instant::now();
    let planned_copy_partitions =
        plan_copy_partitions(replication_transaction, table_id, max_copy_connections).await?;
    let target_partitions = planned_copy_partitions.target_partitions;
    let copy_partitions = planned_copy_partitions.copy_partitions;
    let worker_count = usize::from(max_copy_connections).min(copy_partitions.len());
    let table_copy_metrics = TableCopyMetrics::new(table_id, copy_partitions.len(), worker_count);
    let _active_table_copy_metrics = ActiveTableCopyMetrics::new(table_copy_metrics.clone());

    if copy_partitions.is_empty() {
        info!(table_id = table_id.0, "table is empty, skipping table copy");

        return Ok(TableCopyResult::Completed { total_rows: 0, total_duration_secs: 0.0 });
    }

    info!(
        table_id = table_id.0,
        max_copy_connections,
        target_partitions,
        planned_partitions = copy_partitions.len(),
        active_workers = worker_count,
        "starting table copy"
    );

    let snapshot_id = Arc::<str>::from(replication_transaction.export_snapshot().await?);
    let work_queue = Arc::new(Mutex::new(VecDeque::from(copy_partitions)));
    let publication_name = publication_name.map(Arc::<str>::from);
    let mut join_set = JoinSet::new();
    let mut metrics_reporter =
        Some(spawn_table_copy_metrics_reporter(table_copy_metrics.clone(), shutdown_rx.clone()));

    for worker_index in 0..worker_count {
        let child_replication_client = match replication_transaction.fork_child().await {
            Ok(child_replication_client) => child_replication_client,
            Err(error) => {
                abort_and_drain_copy_workers(&mut join_set).await;
                stop_table_copy_metrics_reporter(&mut metrics_reporter, &table_copy_metrics).await;

                return Err(error);
            }
        };

        let snapshot_id = Arc::clone(&snapshot_id);
        let work_queue = Arc::clone(&work_queue);
        let replicated_table_schema = replicated_table_schema.clone();
        let publication_name = publication_name.clone();
        let batch_config = batch_config.clone();
        let shutdown_rx = shutdown_rx.clone();
        let destination = destination.clone();
        let memory_monitor = memory_monitor.clone();
        let batch_budget = batch_budget.clone();
        let table_copy_metrics = table_copy_metrics.clone();

        join_set.spawn(async move {
            copy_worker(
                worker_index,
                child_replication_client,
                snapshot_id,
                work_queue,
                table_id,
                replicated_table_schema,
                publication_name,
                batch_config,
                shutdown_rx,
                destination,
                memory_monitor,
                batch_budget,
                table_copy_metrics,
            )
            .await
        });
    }

    let mut total_rows = 0;
    let mut partition_durations = Vec::new();
    let mut partition_row_counts = Vec::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(CopyWorkerOutcome::Completed(worker_result))) => {
                total_rows += worker_result.total_rows;
                partition_durations.extend(worker_result.partition_durations);
                partition_row_counts.extend(worker_result.partition_row_counts);
            }
            Ok(Ok(CopyWorkerOutcome::Shutdown)) => {
                info!(
                    table_id = table_id.0,
                    "shutting down table copy after worker received shutdown"
                );
                drain_copy_workers(&mut join_set).await?;
                stop_table_copy_metrics_reporter(&mut metrics_reporter, &table_copy_metrics).await;

                return Ok(TableCopyResult::Shutdown);
            }
            Ok(Err(error)) => {
                abort_and_drain_copy_workers(&mut join_set).await;
                stop_table_copy_metrics_reporter(&mut metrics_reporter, &table_copy_metrics).await;

                return Err(error);
            }
            Err(join_error) => {
                abort_and_drain_copy_workers(&mut join_set).await;
                stop_table_copy_metrics_reporter(&mut metrics_reporter, &table_copy_metrics).await;

                return Err(etl_error!(
                    ErrorKind::TableSyncWorkerPanic,
                    "One or more table copy workers panicked",
                    source: join_error
                ));
            }
        }
    }

    stop_table_copy_metrics_reporter(&mut metrics_reporter, &table_copy_metrics).await;
    table_copy_metrics.record_imbalance(&partition_durations, &partition_row_counts);

    let total_duration_secs = start_time.elapsed().as_secs_f64();

    info!(table_id = table_id.0, total_rows, total_duration_secs, "completed table copy");

    Ok(TableCopyResult::Completed { total_rows, total_duration_secs })
}

/// Plans ctid work items for every physical table that backs `table_id`.
async fn plan_copy_partitions(
    replication_transaction: &PgReplicationTransaction<'_>,
    table_id: TableId,
    max_copy_connections: u16,
) -> EtlResult<PlannedCopyPartitions> {
    let source_tables = if replication_transaction.is_partitioned_table(table_id).await? {
        replication_transaction.get_leaf_partitions(table_id).await?
    } else {
        vec![table_id]
    };

    let mut table_estimates = Vec::with_capacity(source_tables.len());

    for source_table_id in source_tables {
        let estimate =
            replication_transaction.get_table_copy_planning_estimate(source_table_id).await?;

        if estimate.table_blocks == 0 {
            continue;
        }

        table_estimates.push((source_table_id, estimate));
    }

    if table_estimates.is_empty() {
        return Ok(PlannedCopyPartitions { copy_partitions: vec![], target_partitions: 0 });
    }

    let use_estimated_rows =
        table_estimates.iter().all(|(_, estimate)| estimate.estimated_rows > 0);
    let total_estimated_rows = use_estimated_rows
        .then(|| table_estimates.iter().map(|(_, estimate)| estimate.estimated_rows).sum());
    let target_partitions = target_ctid_partition_count(max_copy_connections, total_estimated_rows);
    let total_weight: i64 = table_estimates
        .iter()
        .map(|(_, estimate)| {
            table_copy_partition_weight(
                estimate.estimated_rows,
                estimate.table_blocks,
                use_estimated_rows,
            )
        })
        .sum();

    let mut copy_partitions = Vec::new();
    for (source_table_id, estimate) in table_estimates {
        let partition_weight = table_copy_partition_weight(
            estimate.estimated_rows,
            estimate.table_blocks,
            use_estimated_rows,
        );
        let table_partition_count = partitions_for_table_weight(
            partition_weight,
            total_weight,
            estimate.table_blocks,
            target_partitions,
        );
        let ctid_partitions = replication_transaction
            .plan_ctid_partitions(source_table_id, table_partition_count)
            .await?;

        copy_partitions.extend(ctid_partitions.into_iter().map(|ctid_partition| CopyPartition {
            source_table_id,
            filter_table_id: table_id,
            ctid_partition,
        }));
    }

    Ok(PlannedCopyPartitions { copy_partitions, target_partitions })
}

/// Runs one child connection until there is no more copy work to claim.
#[expect(clippy::too_many_arguments)]
async fn copy_worker<D>(
    worker_index: usize,
    mut child_replication_client: crate::postgres::client::ChildPgReplicationClient,
    snapshot_id: Arc<str>,
    work_queue: Arc<Mutex<VecDeque<CopyPartition>>>,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<Arc<str>>,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
    table_copy_metrics: TableCopyMetrics,
) -> EtlResult<CopyWorkerOutcome>
where
    D: Destination + Clone + Send + 'static,
{
    let child_replication_transaction =
        child_replication_client.begin_transaction(&snapshot_id).await?;
    let mut total_rows = 0;
    let mut partition_durations = Vec::new();
    let mut partition_row_counts = Vec::new();

    loop {
        if is_shutdown_requested(&shutdown_rx) {
            info!(table_id = table_id.0, worker_index, "table copy worker received shutdown");

            return Ok(CopyWorkerOutcome::Shutdown);
        }

        let copy_partition = work_queue.lock().await.pop_front();
        let Some(copy_partition) = copy_partition else {
            child_replication_transaction.commit().await?;

            return Ok(CopyWorkerOutcome::Completed(CompletedCopyWorker {
                total_rows,
                partition_durations,
                partition_row_counts,
            }));
        };

        match copy_partition_rows(
            &child_replication_transaction,
            table_id,
            replicated_table_schema.clone(),
            publication_name.clone(),
            copy_partition,
            batch_config.clone(),
            shutdown_rx.clone(),
            destination.clone(),
            memory_monitor.clone(),
            batch_budget.clone(),
            table_copy_metrics.clone(),
        )
        .await?
        {
            TableCopyResult::Completed { total_rows: partition_rows, total_duration_secs } => {
                total_rows += partition_rows;
                partition_durations.push(total_duration_secs);
                partition_row_counts.push(partition_rows);
            }
            TableCopyResult::Shutdown => return Ok(CopyWorkerOutcome::Shutdown),
        }
    }
}

/// Copies a single physical ctid range into the destination.
#[expect(clippy::too_many_arguments)]
async fn copy_partition_rows<D>(
    child_replication_transaction: &PgChildReplicationTransaction<'_>,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<Arc<str>>,
    partition: CopyPartition,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
    table_copy_metrics: TableCopyMetrics,
) -> EtlResult<TableCopyResult>
where
    D: Destination + Clone + Send + 'static,
{
    if is_shutdown_requested(&shutdown_rx) {
        table_copy_metrics.record_shutdown_partition();

        return Ok(TableCopyResult::Shutdown);
    }

    let start_time = Instant::now();
    let replicated_column_schemas =
        replicated_table_schema.column_schemas().cloned().collect::<Vec<_>>();

    info!(
        table_id = table_id.0,
        source_table_id = partition.source_table_id.0,
        "starting ctid partition copy"
    );

    let copy_stream = child_replication_transaction
        .get_table_copy_stream_with_ctid_partition_and_filter_table(
            partition.source_table_id,
            partition.filter_table_id,
            &replicated_column_schemas,
            publication_name.as_deref(),
            &partition.ctid_partition,
        )
        .await?;

    let table_copy_stream = TableCopyStream::wrap(copy_stream, replicated_column_schemas.iter());
    let connection_updates_rx = child_replication_transaction.connection_updates_rx();
    let _table_copy_stream_guard = batch_budget.register_stream_load(1);
    let cached_batch_budget = batch_budget.cached();
    let stream_id = table_sync_worker_copy_stream_id(table_id);
    let table_copy_stream = TryBatchBackpressureStream::wrap(
        table_copy_stream,
        stream_id,
        batch_config,
        memory_monitor.subscribe(),
        cached_batch_budget,
    );
    pin!(table_copy_stream);

    let total_rows = match copy_table_rows_from_stream(
        table_copy_stream.as_mut(),
        shutdown_rx,
        connection_updates_rx,
        replicated_table_schema,
        destination,
        table_copy_metrics.clone(),
    )
    .await?
    {
        ShutdownResult::Ok(total_rows) => total_rows,
        ShutdownResult::Shutdown(_) => {
            table_copy_metrics.record_shutdown_partition();

            return Ok(TableCopyResult::Shutdown);
        }
    };

    let total_duration_secs = start_time.elapsed().as_secs_f64();
    table_copy_metrics.record_completed_partition(total_rows, total_duration_secs);

    info!(
        table_id = table_id.0,
        source_table_id = partition.source_table_id.0,
        total_rows,
        total_duration_secs,
        "completed ctid partition copy"
    );

    Ok(TableCopyResult::Completed { total_rows, total_duration_secs })
}

/// Copies rows from a batched table-copy stream into the destination with
/// prioritized shutdown handling.
async fn copy_table_rows_from_stream<D, S>(
    mut table_copy_stream: Pin<&mut S>,
    mut shutdown_rx: ShutdownRx,
    mut connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    replicated_table_schema: ReplicatedTableSchema,
    destination: D,
    table_copy_metrics: TableCopyMetrics,
) -> EtlResult<ShutdownResult<u64, u64>>
where
    D: Destination + Clone + Send + 'static,
    S: Stream<Item = EtlResult<Vec<TableRow>>>,
{
    let mut total_rows = 0;

    loop {
        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                return Ok(ShutdownResult::Shutdown(total_rows));
            }

            changed = connection_updates_rx.changed() => {
                if changed.is_err() {
                    return Err(etl_error!(
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL connection updates ended during table copy"
                    ));
                }

                let update = connection_updates_rx.borrow_and_update().clone();
                match update {
                    PostgresConnectionUpdate::Running => {}
                    PostgresConnectionUpdate::Terminated => {
                        return Err(etl_error!(
                            ErrorKind::SourceConnectionFailed,
                            "PostgreSQL connection terminated during table copy"
                        ));
                    }
                    PostgresConnectionUpdate::Errored { error } => {
                        return Err(etl_error!(
                            ErrorKind::SourceConnectionFailed,
                            "PostgreSQL connection errored during table copy",
                            error.to_string()
                        ));
                    }
                }
            }

            maybe_batch = table_copy_stream.next() => {
                let Some(table_rows) = maybe_batch else {
                    return Ok(ShutdownResult::Ok(total_rows));
                };

                let table_rows = table_rows?;
                let batch_size = table_rows.len() as u64;

                let before_sending = Instant::now();
                let (flush_result, pending_flush_result) = WriteTableRowsResult::new(());

                destination
                    .write_table_rows(&replicated_table_schema, table_rows, flush_result)
                    .await?;
                pending_flush_result.await.into_result()?;

                total_rows += batch_size;
                table_copy_metrics.record_copied_rows(batch_size);

                let send_duration_seconds = before_sending.elapsed().as_secs_f64();
                histogram!(
                    ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
                    WORKER_TYPE_LABEL => "table_sync",
                    ACTION_LABEL => "table_copy",
                )
                .record(send_duration_seconds);

                #[cfg(feature = "failpoints")]
                etl_fail_point(START_TABLE_SYNC_DURING_DATA_SYNC_FP)?;
            }
        }
    }
}

/// Drains remaining workers without aborting them.
async fn drain_copy_workers(join_set: &mut JoinSet<EtlResult<CopyWorkerOutcome>>) -> EtlResult<()> {
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(error),
            Err(join_error) => {
                return Err(etl_error!(
                    ErrorKind::TableSyncWorkerPanic,
                    "One or more table copy workers panicked",
                    source: join_error
                ));
            }
        }
    }

    Ok(())
}

/// Aborts and drains all remaining copy workers.
async fn abort_and_drain_copy_workers(join_set: &mut JoinSet<EtlResult<CopyWorkerOutcome>>) {
    join_set.abort_all();

    while join_set.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CTID_COPY_PARTITIONS, partitions_for_table_weight, target_ctid_partition_count,
    };

    #[test]
    fn target_ctid_partition_count_matches_workers() {
        assert_eq!(target_ctid_partition_count(1, Some(10_000_000)), 1);
        assert_eq!(target_ctid_partition_count(8, None), 32);
        assert_eq!(target_ctid_partition_count(8, Some(2_000_000)), 32);
        assert_eq!(target_ctid_partition_count(8, Some(20_000_000)), 80);
    }

    #[test]
    fn target_ctid_partition_count_caps_large_values() {
        assert_eq!(target_ctid_partition_count(u16::MAX, Some(i64::MAX)), MAX_CTID_COPY_PARTITIONS);
    }

    #[test]
    fn partitions_for_table_weight_allocates_by_weight() {
        assert_eq!(partitions_for_table_weight(250, 1000, 250, 128), 32);
        assert_eq!(partitions_for_table_weight(750, 1000, 750, 128), 96);
    }

    #[test]
    fn partitions_for_table_weight_represents_small_tables() {
        assert_eq!(partitions_for_table_weight(1, 10_000, 1, 128), 1);
        assert_eq!(partitions_for_table_weight(4, 10_000, 4, 128), 1);
    }

    #[test]
    fn partitions_for_table_weight_does_not_exceed_table_blocks() {
        assert_eq!(partitions_for_table_weight(1000, 1000, 3, 128), 3);
    }
}
