use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use etl_config::shared::BatchConfig;
use etl_postgres::types::TableId;
use futures::{Stream, StreamExt};
use metrics::{counter, histogram};
use tokio::pin;
use tokio::sync::Semaphore;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::concurrency::batch_budget::BatchBudgetController;
use crate::concurrency::memory_monitor::MemoryMonitor;
use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::concurrency::stream::TryBatchBackpressureStream;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
#[cfg(feature = "failpoints")]
use crate::failpoints::{START_TABLE_SYNC_DURING_DATA_SYNC_FP, etl_fail_point};
use crate::metrics::{
    ACTION_LABEL, DESTINATION_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
    ETL_EVENTS_PROCESSED_TOTAL, ETL_PARALLEL_TABLE_COPY_ROWS_IMBALANCE,
    ETL_PARALLEL_TABLE_COPY_TIME_IMBALANCE, ETL_TABLE_COPY_ROWS, PARTITIONING_LABEL,
    PIPELINE_ID_LABEL, WORKER_TYPE_LABEL,
};
use crate::replication::client::{
    CtidPartition, PgReplicationChildTransaction, PgReplicationTransaction,
    PostgresConnectionUpdate,
};
use crate::replication::stream::TableCopyStream;
use crate::types::{PipelineId, ReplicatedTableSchema, TableRow};

/// Calculates Load Imbalance Factor (LIF) for a set of values.
///
/// LIF = max_value / mean_value
///
/// A value of 1.0 indicates perfect balance (all partitions equal).
/// Higher values indicate more imbalance (e.g., 2.0 means the slowest partition
/// took twice the average time).
///
/// Returns 0.0 if the input is empty or mean is zero.
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

/// Result of a table copy operation.
#[derive(Debug)]
pub enum TableCopyResult {
    /// All rows copied successfully.
    Completed {
        total_rows: u64,
        total_duration_secs: f64,
    },
    /// Copy was interrupted by a shutdown signal.
    Shutdown,
}

/// Copies rows from a batched table-copy stream into the destination with prioritized shutdown handling.
async fn copy_table_rows_from_stream<D, S>(
    mut table_copy_stream: Pin<&mut S>,
    mut shutdown_rx: ShutdownRx,
    mut connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    replicated_table_schema: ReplicatedTableSchema,
    pipeline_id: PipelineId,
    partitioning: &'static str,
    destination: D,
) -> EtlResult<ShutdownResult<u64, u64>>
where
    D: Destination + Clone + Send + 'static,
    S: Stream<Item = EtlResult<Vec<TableRow>>>,
{
    let mut total_rows: u64 = 0;

    loop {
        tokio::select! {
            biased;

            // PRIORITY 1: Handle shutdown signals.
            // Shutdown takes precedence so table copy workers stop promptly when requested.
            _ = shutdown_rx.changed() => {
                return Ok(ShutdownResult::Shutdown(total_rows));
            }

            // PRIORITY 2: Handle PostgreSQL connection lifecycle updates.
            // Connection termination or errors stop copy immediately and surface a source error.
            changed = connection_updates_rx.changed() => {
                if changed.is_err() {
                    return Err(etl_error!(
                        ErrorKind::SourceConnectionFailed,
                        "postgresql connection updates ended during table copy"
                    ));
                }

                let update = connection_updates_rx.borrow_and_update().clone();
                match update {
                    PostgresConnectionUpdate::Running => {}
                    PostgresConnectionUpdate::Terminated => {
                        return Err(etl_error!(
                            ErrorKind::SourceConnectionFailed,
                            "postgresql connection terminated during table copy"
                        ));
                    }
                    PostgresConnectionUpdate::Errored { error } => {
                        return Err(etl_error!(
                            ErrorKind::SourceConnectionFailed,
                            "postgresql connection errored during table copy",
                            error.to_string()
                        ));
                    }
                }
            }

            // PRIORITY 3: Consume batched COPY rows.
            // This is the normal copy path and runs when no higher-priority branch is pending.
            maybe_batch = table_copy_stream.next() => {
                let Some(table_rows) = maybe_batch else {
                    return Ok(ShutdownResult::Ok(total_rows));
                };

                let table_rows = table_rows?;
                let batch_size = table_rows.len() as u64;
                total_rows += batch_size;

                let before_sending = Instant::now();

                destination
                    .write_table_rows(&replicated_table_schema, table_rows)
                    .await?;

                counter!(
                    ETL_EVENTS_PROCESSED_TOTAL,
                    WORKER_TYPE_LABEL => "table_sync",
                    ACTION_LABEL => "table_copy",
                    PIPELINE_ID_LABEL => pipeline_id.to_string(),
                    DESTINATION_LABEL => D::name(),
                )
                .increment(batch_size);

                let send_duration_seconds = before_sending.elapsed().as_secs_f64();
                histogram!(
                    ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
                    WORKER_TYPE_LABEL => "table_sync",
                    ACTION_LABEL => "table_copy",
                    PIPELINE_ID_LABEL => pipeline_id.to_string(),
                    DESTINATION_LABEL => D::name(),
                    PARTITIONING_LABEL => partitioning,
                )
                .record(send_duration_seconds);

                #[cfg(feature = "failpoints")]
                etl_fail_point(START_TABLE_SYNC_DURING_DATA_SYNC_FP)?;
            }
        }
    }
}

/// Describes which slice of a table a single parallel copy task should read.
///
/// For non-partitioned tables, the table is divided into ctid ranges by page estimation.
/// For partitioned tables, each leaf partition becomes its own copy unit.
#[derive(Debug)]
enum CopyPartition {
    /// A ctid range within a single (non-partitioned) table.
    CtidRange(CtidPartition),
    /// A leaf partition of a partitioned table, copied in its entirety.
    LeafPartition { leaf_table_id: TableId },
}

/// Copies a table using the appropriate strategy based on the number of connections.
///
/// When `max_copy_connections` is 1, performs a serial copy using the slot transaction's
/// consistent snapshot. When greater than 1, performs a parallel copy using ctid-based
/// partitioning (for regular tables) or per-leaf-partition parallelism (for partitioned
/// tables) across multiple child connections that share the same exported snapshot.
#[expect(clippy::too_many_arguments)]
pub async fn table_copy<D: Destination + Clone + Send + 'static>(
    transaction: &PgReplicationTransaction,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<&str>,
    max_copy_connections: u16,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult> {
    if max_copy_connections > 1 {
        parallel_table_copy(
            transaction,
            table_id,
            replicated_table_schema,
            publication_name,
            max_copy_connections,
            batch_config,
            shutdown_rx,
            pipeline_id,
            destination,
            memory_monitor,
            batch_budget,
        )
        .await
    } else {
        serial_table_copy(
            transaction,
            table_id,
            replicated_table_schema,
            publication_name,
            batch_config,
            shutdown_rx,
            pipeline_id,
            destination,
            memory_monitor,
            batch_budget,
        )
        .await
    }
}

/// Copies a table serially using a single COPY stream from the slot transaction.
#[expect(clippy::too_many_arguments)]
async fn serial_table_copy<D: Destination + Clone + Send + 'static>(
    transaction: &PgReplicationTransaction,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<&str>,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult> {
    let start_time = Instant::now();

    let replicated_column_schemas = replicated_table_schema
        .column_schemas()
        .cloned()
        .collect::<Vec<_>>();
    let table_copy_stream = transaction
        .get_table_copy_stream(table_id, &replicated_column_schemas, publication_name)
        .await?;
    let table_copy_stream = TableCopyStream::wrap(
        table_copy_stream,
        replicated_column_schemas.iter(),
        pipeline_id,
    );
    let connection_updates_rx = transaction.get_cloned_client().connection_updates_rx();
    let _table_copy_stream_guard = batch_budget.register_stream_load(1);
    let cached_batch_budget = batch_budget.cached();
    let table_copy_stream = TryBatchBackpressureStream::wrap(
        table_copy_stream,
        batch_config,
        memory_monitor.subscribe(),
        cached_batch_budget,
    );
    pin!(table_copy_stream);

    info!(table_id = table_id.0, "starting serial table copy");

    let total_rows = match copy_table_rows_from_stream(
        table_copy_stream.as_mut(),
        shutdown_rx,
        connection_updates_rx,
        replicated_table_schema,
        pipeline_id,
        "false",
        destination,
    )
    .await?
    {
        ShutdownResult::Ok(total_rows) => total_rows,
        ShutdownResult::Shutdown(total_rows) => {
            info!(
                table_id = table_id.0,
                total_rows, "shutting down serial table copy"
            );

            return Ok(TableCopyResult::Shutdown);
        }
    };

    histogram!(
        ETL_TABLE_COPY_ROWS,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
        PARTITIONING_LABEL => "false",
    )
    .record(total_rows as f64);

    let total_duration_secs = start_time.elapsed().as_secs_f64();

    info!(
        table_id = table_id.0,
        total_rows, total_duration_secs, "completed serial table copy"
    );

    Ok(TableCopyResult::Completed {
        total_rows,
        total_duration_secs,
    })
}

/// Copies a table in parallel across multiple child connections.
///
/// For non-partitioned tables, uses ctid-based partitioning by page estimation.
/// For partitioned tables, copies each leaf partition as a separate unit.
/// A semaphore limits the number of concurrent copy tasks to `max_copy_connections`.
#[expect(clippy::too_many_arguments)]
async fn parallel_table_copy<D: Destination + Clone + Send + 'static>(
    transaction: &PgReplicationTransaction,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<&str>,
    max_copy_connections: u16,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult> {
    let start_time = Instant::now();

    info!(
        table_id = table_id.0,
        max_copy_connections, "starting parallel table copy"
    );

    // Determine copy partitions: ctid ranges for regular tables, leaf partitions for
    // partitioned tables. Ctid-based partitioning cannot be used with partitioned tables
    // because each child partition has its own ctid space, which causes duplicate rows.
    let is_partitioned = transaction.is_partitioned_table(table_id).await?;
    let copy_partitions: Vec<CopyPartition> = if is_partitioned {
        let leave_table_ids = transaction.get_leaf_partitions(table_id).await?;

        info!(
            table_id = table_id.0,
            num_leaf_partitions = leave_table_ids.len(),
            "using per-leaf-partition parallelism for partitioned table"
        );

        leave_table_ids
            .into_iter()
            .map(|leaf_table_id| CopyPartition::LeafPartition { leaf_table_id })
            .collect()
    } else {
        let ctid_partitions = transaction
            .plan_ctid_partitions(table_id, max_copy_connections)
            .await?;

        info!(
            table_id = table_id.0,
            num_partitions = ctid_partitions.len(),
            "using ctid-based parallelism"
        );

        ctid_partitions
            .into_iter()
            .map(CopyPartition::CtidRange)
            .collect()
    };

    if copy_partitions.is_empty() {
        info!(
            table_id = table_id.0,
            "table is empty, skipping parallel copy"
        );

        return Ok(TableCopyResult::Completed {
            total_rows: 0,
            total_duration_secs: 0.0,
        });
    }

    // Export the snapshot from the main connection's transaction so child connections can
    // import it via SET TRANSACTION SNAPSHOT. The main transaction must stay open for the
    // entire duration of the parallel copy to keep the snapshot valid.
    let snapshot_id = transaction.export_snapshot().await?;

    let semaphore = Arc::new(Semaphore::new(max_copy_connections as usize));
    let mut join_set = JoinSet::new();
    let publication_name = publication_name.map(Arc::<str>::from);

    for partition in copy_partitions {
        // Acquire a concurrency slot to make sure we are always using at most `max_copy_connections`.
        let permit = semaphore.clone().acquire_owned().await.map_err(|err| {
            etl_error!(
                ErrorKind::InvalidState,
                "Could not acquire semaphore while copying a table in parallel",
                err.to_string()
            )
        })?;

        let replication_client = transaction.get_cloned_client();
        let snapshot_id = snapshot_id.clone();
        let replicated_table_schema = replicated_table_schema.clone();
        let publication_name = publication_name.clone();
        let batch_config = batch_config.clone();
        let shutdown_rx = shutdown_rx.clone();
        let destination = destination.clone();
        let memory_monitor = memory_monitor.clone();
        let batch_budget = batch_budget.clone();

        join_set.spawn(async move {
            let child_replication_client = replication_client.fork_child().await?;
            let child_transaction =
                PgReplicationChildTransaction::new(child_replication_client, &snapshot_id).await?;

            let result = copy_partition(
                child_transaction,
                table_id,
                replicated_table_schema,
                publication_name,
                partition,
                batch_config,
                shutdown_rx,
                pipeline_id,
                destination,
                memory_monitor,
                batch_budget,
            )
            .await;

            drop(permit);

            result
        });
    }

    let mut total_rows: u64 = 0;
    let mut partition_durations = Vec::new();
    let mut partition_row_counts = Vec::new();

    // TODO: we might want to edit retries within the same partition if the copy fails. However, we
    //  need to investigate the frequency.
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(result)) => {
                // If all the copy was successful, we collect how many rows were copied but if at least
                // one received the shutdown result, we shut everything down causing all progress to be
                // lost.
                match result {
                    TableCopyResult::Completed {
                        total_rows: partition_total_rows,
                        total_duration_secs: partition_total_duration_secs,
                    } => {
                        total_rows += partition_total_rows;
                        partition_row_counts.push(partition_total_rows);
                        partition_durations.push(partition_total_duration_secs);
                    }
                    TableCopyResult::Shutdown => {
                        info!(
                            "shutting down parallel table copy since one or more partitions were interrupted by shutdown"
                        );

                        return Ok(TableCopyResult::Shutdown);
                    }
                }
            }
            Ok(Err(err)) => {
                error!(
                    table_id = table_id.0,
                    error = %err,
                    "one or more parallel copy partitions failed"
                );

                return Err(err);
            }
            Err(join_err) => {
                error!(
                    table_id = table_id.0,
                    error = %join_err,
                    "one or more parallel copy partitions panicked"
                );

                return Err(etl_error!(
                    ErrorKind::TableSyncWorkerPanic,
                    "One or more parallel copy partition tasks panicked, aborting all",
                    join_err.to_string()
                ));
            }
        }
    }

    let total_duration_secs = start_time.elapsed().as_secs_f64();

    // Calculate partition skew metrics
    let time_lif = calculate_skew_metrics(&partition_durations);
    let rows_lif = calculate_skew_metrics(
        &partition_row_counts
            .iter()
            .map(|&r| r as f64)
            .collect::<Vec<_>>(),
    );

    // Record imbalance metrics.
    histogram!(
        ETL_PARALLEL_TABLE_COPY_TIME_IMBALANCE,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
    )
    .record(time_lif);

    histogram!(
        ETL_PARALLEL_TABLE_COPY_ROWS_IMBALANCE,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
    )
    .record(rows_lif);

    info!(
        table_id = table_id.0,
        total_rows,
        total_duration_secs,
        time_load_imbalance_factor = time_lif,
        rows_load_imbalance_factor = rows_lif,
        "completed parallel table copy"
    );

    Ok(TableCopyResult::Completed {
        total_rows,
        total_duration_secs,
    })
}

/// Copies a single partition from the source table to the destination.
///
/// The child transaction is already pinned to the exported snapshot. Depending on the
/// [`CopyPartition`] variant, streams rows from either a ctid range or an entire leaf
/// partition. All rows are written under the parent `table_id`.
#[expect(clippy::too_many_arguments)]
async fn copy_partition<D>(
    child_transaction: PgReplicationChildTransaction,
    table_id: TableId,
    replicated_table_schema: ReplicatedTableSchema,
    publication_name: Option<Arc<str>>,
    partition: CopyPartition,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableCopyResult>
where
    D: Destination + Clone + Send + 'static,
{
    let start_time = Instant::now();
    let replicated_column_schemas = replicated_table_schema
        .column_schemas()
        .cloned()
        .collect::<Vec<_>>();

    match &partition {
        CopyPartition::CtidRange(ctid) => match ctid {
            CtidPartition::OpenStart { end_tid } => {
                info!(
                    table_id = table_id.0,
                    end_tid = %end_tid,
                    "starting ctid partition copy (open start)"
                );
            }
            CtidPartition::Closed { start_tid, end_tid } => {
                info!(
                    table_id = table_id.0,
                    start_tid = %start_tid,
                    end_tid = %end_tid,
                    "starting ctid partition copy (closed)"
                );
            }
            CtidPartition::OpenEnd { start_tid } => {
                info!(
                    table_id = table_id.0,
                    start_tid = %start_tid,
                    "starting ctid partition copy (open end)"
                );
            }
        },
        CopyPartition::LeafPartition { leaf_table_id } => {
            info!(
                table_id = table_id.0,
                leaf_table_id = leaf_table_id.0,
                "starting leaf partition copy"
            );
        }
    }

    let copy_stream = match &partition {
        CopyPartition::CtidRange(ctid) => {
            child_transaction
                .get_table_copy_stream_with_ctid_partition(
                    table_id,
                    &replicated_column_schemas,
                    publication_name.as_ref().map(Arc::as_ref),
                    ctid,
                )
                .await?
        }
        CopyPartition::LeafPartition { leaf_table_id } => {
            child_transaction
                .get_table_copy_stream(
                    *leaf_table_id,
                    &replicated_column_schemas,
                    publication_name.as_ref().map(Arc::as_ref),
                )
                .await?
        }
    };

    let table_copy_stream =
        TableCopyStream::wrap(copy_stream, replicated_column_schemas.iter(), pipeline_id);
    let connection_updates_rx = child_transaction
        .get_cloned_client()
        .connection_updates_rx();
    let _table_copy_stream_guard = batch_budget.register_stream_load(1);
    let cached_batch_budget = batch_budget.cached();
    let table_copy_stream = TryBatchBackpressureStream::wrap(
        table_copy_stream,
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
        pipeline_id,
        "true",
        destination,
    )
    .await?
    {
        ShutdownResult::Ok(total_rows) => total_rows,
        ShutdownResult::Shutdown(total_rows) => {
            info!(
                table_id = table_id.0,
                total_rows, "partition copy interrupted by shutdown, skipping rollback"
            );

            return Ok(TableCopyResult::Shutdown);
        }
    };

    // We commit the transaction for the same reason as the rollback, to let Postgres immediately free
    // up things related to in-progress transactions.
    child_transaction.commit().await?;

    histogram!(
        ETL_TABLE_COPY_ROWS,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
        PARTITIONING_LABEL => "true",
    )
    .record(total_rows as f64);

    let total_duration_secs = start_time.elapsed().as_secs_f64();

    match &partition {
        CopyPartition::CtidRange(ctid) => match ctid {
            CtidPartition::OpenStart { end_tid } => {
                info!(
                    table_id = table_id.0,
                    total_rows,
                    total_duration_secs,
                    end_tid = %end_tid,
                    "completed ctid partition copy (open start)"
                );
            }
            CtidPartition::Closed { start_tid, end_tid } => {
                info!(
                    table_id = table_id.0,
                    total_rows,
                    total_duration_secs,
                    start_tid = %start_tid,
                    end_tid = %end_tid,
                    "completed ctid partition copy (closed)"
                );
            }
            CtidPartition::OpenEnd { start_tid } => {
                info!(
                    table_id = table_id.0,
                    total_rows,
                    total_duration_secs,
                    start_tid = %start_tid,
                    "completed ctid partition copy (open end)"
                );
            }
        },
        CopyPartition::LeafPartition { leaf_table_id } => {
            info!(
                table_id = table_id.0,
                leaf_table_id = leaf_table_id.0,
                total_rows,
                total_duration_secs,
                "completed leaf partition copy"
            );
        }
    }

    Ok(TableCopyResult::Completed {
        total_rows,
        total_duration_secs,
    })
}
