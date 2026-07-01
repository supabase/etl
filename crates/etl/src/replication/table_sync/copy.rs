use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use etl_config::shared::BatchConfig;
use futures::{Stream, StreamExt};
use metrics::{counter, gauge, histogram};
use tokio::{
    pin,
    sync::{Mutex, watch},
    task::{JoinHandle, JoinSet},
    time::MissedTickBehavior,
};
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, warn};

#[cfg(feature = "failpoints")]
use crate::failpoints::{START_TABLE_SYNC_DURING_DATA_SYNC_FP, etl_fail_point};
use crate::{
    data::TableRow,
    destination::{Destination, WriteTableRowsResult},
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::{
        ACTION_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS, ETL_TABLE_COPY_DURATION_SECONDS,
        ETL_TABLE_COPY_EFFECTIVE_PARTITIONS, ETL_TABLE_COPY_END_TO_END_LAG_BYTES,
        ETL_TABLE_COPY_PARTITION_BLOCKS, ETL_TABLE_COPY_PARTITION_DURATION_SECONDS,
        ETL_TABLE_COPY_PARTITION_ROWS, ETL_TABLE_COPY_PARTITIONS_TOTAL,
        ETL_TABLE_COPY_PLANNED_PARTITIONS, ETL_TABLE_COPY_ROWS_TOTAL, WORKER_TYPE_LABEL,
    },
    postgres::{
        OutOfBandSourcePool, TableCopyStream,
        client::{
            ChildPgReplicationClient, CtidPartition, PgChildReplicationTransaction,
            PgReplicationTransaction, PostgresConnectionUpdate,
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

/// Rows and timings copied by a completed worker.
#[derive(Debug)]
struct CompletedCopyWorker {
    /// Rows copied by this worker.
    total_rows: u64,
}

/// Outcome of one worker connection.
#[derive(Debug)]
enum CopyWorkerOutcome {
    /// The worker drained all available work and committed its transaction.
    Completed(CompletedCopyWorker),
    /// The worker observed shutdown before committing its transaction.
    Shutdown,
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
    // If we have just one copy connection, we just load everything as one
    // partition.
    if max_copy_connections == 1 {
        return 1;
    }

    // We try to estimate the number of partitions given the worker count.
    let worker_target = max_copy_connections.saturating_mul(CTID_PARTITIONS_PER_COPY_WORKER).max(1);

    // We try to estimate the number of partitions also using the total number of
    // estimated rows for the table(s).
    let row_target = total_estimated_rows.filter(|estimated_rows| *estimated_rows > 0).map_or(
        1,
        |estimated_rows| {
            div_ceil_u128(estimated_rows as u128, CTID_COPY_ROWS_PER_PARTITION as u128)
                .min(u128::from(u16::MAX))
                .max(1) as u16
        },
    );

    // We take the biggest number of partitions between the two, while remaining
    // within sane limits.
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

    // Use a wide intermediate so very large row or block estimates cannot
    // overflow while computing the proportional share.
    let partition_weight = partition_weight as u128;
    let total_weight = total_weight as u128;
    let block_count = block_count as u64;
    let target_partitions = u128::from(target_partitions);
    let weighted_partitions = div_ceil_u128(partition_weight * target_partitions, total_weight);

    // CTID ranges split heap blocks, so more partitions than blocks would only
    // create empty work items.
    weighted_partitions.min(u128::from(block_count)).min(u128::from(u16::MAX)).max(1) as u16
}

/// Returns true when the table copy should stop for shutdown.
fn is_shutdown_requested(shutdown_rx: &ShutdownRx) -> bool {
    shutdown_rx.has_changed().unwrap_or(true)
}

/// Spawns a periodic reporter for table-copy replication lag.
fn spawn_table_copy_lag_reporter(
    table_id: TableId,
    consistent_point: PgLsn,
    out_of_band_source_pool: OutOfBandSourcePool,
    replication_lag_refresh_interval: Duration,
    mut shutdown_rx: ShutdownRx,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut replication_lag_interval = tokio::time::interval(replication_lag_refresh_interval);
        replication_lag_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                _ = shutdown_rx.changed() => {
                    break;
                }

                _ = replication_lag_interval.tick() => {
                    emit_table_copy_replication_lag_metrics(
                        table_id,
                        consistent_point,
                        &out_of_band_source_pool,
                    ).await;
                }
            }
        }
    })
}

/// Emits end-to-end lag metrics for a table sync while initial copy runs.
async fn emit_table_copy_replication_lag_metrics(
    table_id: TableId,
    consistent_point: PgLsn,
    out_of_band_source_pool: &OutOfBandSourcePool,
) {
    match out_of_band_source_pool.get_current_wal_lsn().await {
        Ok(source_current_lsn) => {
            let source_current_lsn = u64::from(source_current_lsn);
            let consistent_point = u64::from(consistent_point);
            let table_copy_lag_bytes = source_current_lsn.saturating_sub(consistent_point);

            gauge!(ETL_TABLE_COPY_END_TO_END_LAG_BYTES).set(table_copy_lag_bytes as f64);
        }
        Err(error) => {
            warn!(
                table_id = table_id.0,
                error = %error,
                "table copy replication lag reporter failed to poll source database"
            );
        }
    }
}

/// Stops the periodic replication lag reporter task.
async fn stop_table_copy_lag_reporter(lag_reporter: &mut Option<JoinHandle<()>>) {
    if let Some(mut handle) = lag_reporter.take() {
        handle.abort();

        if let Err(error) = (&mut handle).await
            && !error.is_cancelled()
        {
            warn!(
                error = %error,
                "table copy lag reporter failed before completing"
            );
        }
    }
}

/// Copies a table through ctid work items, using worker child connections.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn table_copy<D: Destination + Clone + Send + 'static>(
    replication_transaction: &PgReplicationTransaction<'_>,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<&str>,
    max_copy_connections: u16,
    consistent_point: PgLsn,
    out_of_band_source_pool: OutOfBandSourcePool,
    replication_lag_refresh_interval: Duration,
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
        consistent_point,
        out_of_band_source_pool,
        replication_lag_refresh_interval,
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
    consistent_point: PgLsn,
    out_of_band_source_pool: OutOfBandSourcePool,
    replication_lag_refresh_interval: Duration,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult> {
    let start_time = Instant::now();
    let copy_partitions =
        plan_copy_partitions(replication_transaction, table_id, max_copy_connections).await?;
    let worker_count = usize::from(max_copy_connections).min(copy_partitions.len());

    if copy_partitions.is_empty() {
        info!(table_id = table_id.0, "table is empty, skipping table copy");

        counter!(ETL_TABLE_COPY_ROWS_TOTAL).increment(0);
        histogram!(ETL_TABLE_COPY_DURATION_SECONDS).record(0.0);

        return Ok(TableCopyResult::Completed { total_rows: 0, total_duration_secs: 0.0 });
    }

    info!(
        table_id = table_id.0,
        max_copy_connections,
        planned_partitions = copy_partitions.len(),
        active_workers = worker_count,
        "starting table copy"
    );

    // Every child copy connection imports the same exported snapshot, so all
    // CTID ranges for this table copy see a consistent source view.
    let snapshot_id = replication_transaction.export_snapshot().await?;
    let work_queue = Arc::new(Mutex::new(VecDeque::from(copy_partitions)));
    let publication_name = publication_name.map(str::to_owned);
    let mut join_set = JoinSet::new();
    let mut lag_reporter = Some(spawn_table_copy_lag_reporter(
        table_id,
        consistent_point,
        out_of_band_source_pool,
        replication_lag_refresh_interval,
        shutdown_rx.clone(),
    ));

    for worker_index in 0..worker_count {
        let child_replication_client = match replication_transaction.fork_child().await {
            Ok(child_replication_client) => child_replication_client,
            Err(error) => {
                stop_table_copy_lag_reporter(&mut lag_reporter).await;

                return Err(error);
            }
        };

        // Keep these values owned by each worker. They are small, and cloning
        // them avoids extra shared ownership machinery in the hot path.
        let snapshot_id = snapshot_id.clone();
        let work_queue = Arc::clone(&work_queue);
        let replicated_table_schema = replicated_table_schema.clone();
        let publication_name = publication_name.clone();
        let batch_config = batch_config.clone();
        let shutdown_rx = shutdown_rx.clone();
        let destination = destination.clone();
        let memory_monitor = memory_monitor.clone();
        let batch_budget = batch_budget.clone();

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
            )
            .await
        });
    }

    let mut total_rows = 0;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(CopyWorkerOutcome::Completed(worker_result))) => {
                total_rows += worker_result.total_rows;
            }
            Ok(Ok(CopyWorkerOutcome::Shutdown)) => {
                info!(
                    table_id = table_id.0,
                    "shutting down table copy after worker received shutdown"
                );

                stop_table_copy_lag_reporter(&mut lag_reporter).await;

                return Ok(TableCopyResult::Shutdown);
            }
            Ok(Err(error)) => {
                stop_table_copy_lag_reporter(&mut lag_reporter).await;

                return Err(error);
            }
            Err(join_error) if join_error.is_cancelled() => {
                debug!(error = %join_error, "table copy worker task was cancelled");

                stop_table_copy_lag_reporter(&mut lag_reporter).await;

                return Ok(TableCopyResult::Shutdown);
            }
            Err(join_error) => {
                stop_table_copy_lag_reporter(&mut lag_reporter).await;

                return Err(etl_error!(
                    ErrorKind::TableCopyWorkerPanic,
                    "Table copy worker panicked",
                    source: join_error
                ));
            }
        }
    }

    stop_table_copy_lag_reporter(&mut lag_reporter).await;

    let total_duration_secs = start_time.elapsed().as_secs_f64();
    counter!(ETL_TABLE_COPY_ROWS_TOTAL).increment(total_rows);
    histogram!(ETL_TABLE_COPY_DURATION_SECONDS).record(total_duration_secs);

    info!(table_id = table_id.0, total_rows, total_duration_secs, "completed table copy");

    Ok(TableCopyResult::Completed { total_rows, total_duration_secs })
}

/// Plans ctid work items for every physical table that backs `table_id`.
async fn plan_copy_partitions(
    replication_transaction: &PgReplicationTransaction<'_>,
    table_id: TableId,
    max_copy_connections: u16,
) -> EtlResult<Vec<CopyPartition>> {
    let source_tables = if replication_transaction.is_partitioned_table(table_id).await? {
        replication_transaction.get_leaf_partitions(table_id).await?
    } else {
        vec![table_id]
    };

    let mut table_estimates = Vec::with_capacity(source_tables.len());

    for source_table_id in source_tables {
        let estimate =
            replication_transaction.get_table_copy_planning_estimate(source_table_id).await?;

        // If a table has no blocks, we don't want to consider it for the copy.
        if estimate.is_empty() {
            continue;
        }

        table_estimates.push((source_table_id, estimate));
    }

    // If there are no estimates, we don't need to copy any partitions.
    if table_estimates.is_empty() {
        histogram!(ETL_TABLE_COPY_PLANNED_PARTITIONS).record(0.0);
        histogram!(ETL_TABLE_COPY_EFFECTIVE_PARTITIONS).record(0.0);

        return Ok(vec![]);
    }

    // Row estimates are useful for allocating work across leaf tables, but only
    // if every physical table has one. Otherwise, fall back to heap blocks so a
    // non-empty table with stale stats is not weighted as zero.
    let use_estimated_rows =
        table_estimates.iter().all(|(_, estimate)| estimate.estimated_rows() > 0);

    let total_estimated_rows = use_estimated_rows
        .then(|| table_estimates.iter().map(|(_, estimate)| estimate.estimated_rows()).sum());

    let target_partitions = target_ctid_partition_count(max_copy_connections, total_estimated_rows);

    let total_weight: i64 = table_estimates
        .iter()
        .map(|(_, estimate)| estimate.partition_weight(use_estimated_rows))
        .sum();

    let mut copy_partitions = Vec::with_capacity(usize::from(target_partitions));
    for (source_table_id, estimate) in table_estimates {
        let partition_weight = estimate.partition_weight(use_estimated_rows);
        let table_partition_count = partitions_for_table_weight(
            partition_weight,
            total_weight,
            estimate.table_blocks(),
            target_partitions,
        );
        let ctid_partitions = estimate.plan_ctid_partitions(table_partition_count)?;

        for planned_partition in ctid_partitions {
            let (ctid_partition, planned_blocks) = planned_partition.into_parts();
            histogram!(ETL_TABLE_COPY_PARTITION_BLOCKS).record(planned_blocks as f64);

            // Partitioned parents have no physical CTIDs, so COPY runs against
            // the leaf table while publication row filters are resolved from
            // the tracked table.
            copy_partitions.push(CopyPartition {
                source_table_id,
                filter_table_id: table_id,
                ctid_partition,
            });
        }
    }

    histogram!(ETL_TABLE_COPY_PLANNED_PARTITIONS).record(f64::from(target_partitions));
    histogram!(ETL_TABLE_COPY_EFFECTIVE_PARTITIONS).record(copy_partitions.len() as f64);

    Ok(copy_partitions)
}

/// Runs one child connection until there is no more copy work to claim.
#[expect(clippy::too_many_arguments)]
async fn copy_worker<D>(
    worker_index: usize,
    mut child_replication_client: ChildPgReplicationClient,
    snapshot_id: String,
    work_queue: Arc<Mutex<VecDeque<CopyPartition>>>,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<String>,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<CopyWorkerOutcome>
where
    D: Destination + Clone + Send + 'static,
{
    let child_replication_transaction =
        child_replication_client.begin_transaction(&snapshot_id).await?;
    let mut total_rows = 0;

    loop {
        if is_shutdown_requested(&shutdown_rx) {
            info!(table_id = table_id.0, worker_index, "table copy worker received shutdown");

            return Ok(CopyWorkerOutcome::Shutdown);
        }

        let copy_partition = work_queue.lock().await.pop_front();
        let Some(copy_partition) = copy_partition else {
            // The queue is fully populated before workers start; an empty queue
            // means all CTID work has been claimed.
            child_replication_transaction.commit().await?;

            return Ok(CopyWorkerOutcome::Completed(CompletedCopyWorker { total_rows }));
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
        )
        .await?
        {
            TableCopyResult::Completed { total_rows: partition_rows, .. } => {
                total_rows += partition_rows;
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
    publication_name: Option<String>,
    partition: CopyPartition,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult>
where
    D: Destination + Clone + Send + 'static,
{
    if is_shutdown_requested(&shutdown_rx) {
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
        .get_table_copy_stream_with_ctid_partition(
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
    )
    .await?
    {
        ShutdownResult::Ok(total_rows) => total_rows,
        ShutdownResult::Shutdown(_) => {
            return Ok(TableCopyResult::Shutdown);
        }
    };

    let total_duration_secs = start_time.elapsed().as_secs_f64();
    counter!(ETL_TABLE_COPY_PARTITIONS_TOTAL).increment(1);
    histogram!(ETL_TABLE_COPY_PARTITION_ROWS).record(total_rows as f64);
    histogram!(ETL_TABLE_COPY_PARTITION_DURATION_SECONDS).record(total_duration_secs);

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
