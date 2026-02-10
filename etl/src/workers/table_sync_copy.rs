use std::time::Instant;

use etl_config::shared::BatchConfig;
use etl_postgres::types::{ColumnSchema, TableId, TableSchema};
use futures::StreamExt;
use metrics::{counter, histogram};
use tokio::pin;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::concurrency::stream::TimeoutBatchStream;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
#[cfg(feature = "failpoints")]
use crate::failpoints::{START_TABLE_SYNC_DURING_DATA_SYNC, etl_fail_point};
use crate::metrics::{
    ACTION_LABEL, DESTINATION_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
    ETL_EVENTS_PROCESSED_TOTAL, ETL_TABLE_COPY_ROWS, PARTITIONING_LABEL, PIPELINE_ID_LABEL,
    WORKER_TYPE_LABEL,
};
use crate::replication::client::{
    CtidPartition, PgReplicationChildTransaction, PgReplicationTransaction,
};
use crate::replication::stream::TableCopyStream;
use crate::types::PipelineId;

/// Result of a table copy operation.
#[derive(Debug)]
pub enum TableCopyResult {
    /// All rows copied successfully.
    Completed { total_rows: u64 },
    /// Copy was interrupted by a shutdown signal.
    Shutdown,
}

/// Copies a table using the appropriate strategy based on the number of connections.
///
/// When `max_copy_connections` is 1, performs a serial copy using the slot transaction's
/// consistent snapshot. When greater than 1, performs a parallel copy using ctid-based
/// partitioning across multiple child connections that share the same exported snapshot.
#[expect(clippy::too_many_arguments)]
pub async fn table_copy<D: Destination + Clone + Send + 'static>(
    transaction: &PgReplicationTransaction,
    table_id: TableId,
    table_schema: &TableSchema,
    publication_name: Option<&str>,
    max_copy_connections: u16,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
) -> EtlResult<TableCopyResult> {
    if max_copy_connections > 1 {
        parallel_table_copy(
            transaction,
            table_id,
            table_schema,
            publication_name,
            max_copy_connections,
            batch_config,
            shutdown_rx,
            pipeline_id,
            destination,
        )
        .await
    } else {
        serial_table_copy(
            transaction,
            table_id,
            table_schema,
            publication_name,
            batch_config,
            shutdown_rx,
            pipeline_id,
            destination,
        )
        .await
    }
}

/// Copies a table serially using a single COPY stream from the slot transaction.
#[expect(clippy::too_many_arguments)]
async fn serial_table_copy<D: Destination + Clone + Send + 'static>(
    transaction: &PgReplicationTransaction,
    table_id: TableId,
    table_schema: &TableSchema,
    publication_name: Option<&str>,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
) -> EtlResult<TableCopyResult> {
    let table_copy_stream = transaction
        .get_table_copy_stream(table_id, &table_schema.column_schemas, publication_name)
        .await?;
    let table_copy_stream =
        TableCopyStream::wrap(table_copy_stream, &table_schema.column_schemas, pipeline_id);
    let table_copy_stream = TimeoutBatchStream::wrap(table_copy_stream, batch_config, shutdown_rx);
    pin!(table_copy_stream);

    info!(table_id = table_id.0, "starting serial table copy");

    let mut total_rows: u64 = 0;

    while let Some(result) = table_copy_stream.next().await {
        match result {
            ShutdownResult::Ok(table_rows) => {
                let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
                let batch_size = table_rows.len() as u64;
                total_rows += batch_size;

                let before_sending = Instant::now();

                destination.write_table_rows(table_id, table_rows).await?;

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
                    PARTITIONING_LABEL => "false",
                )
                .record(send_duration_seconds);

                #[cfg(feature = "failpoints")]
                etl_fail_point(START_TABLE_SYNC_DURING_DATA_SYNC)?;
            }
            ShutdownResult::Shutdown(_) => {
                info!(
                    table_id = table_id.0,
                    total_rows, "shutting down serial table copy"
                );
                return Ok(TableCopyResult::Shutdown);
            }
        }
    }

    histogram!(
        ETL_TABLE_COPY_ROWS,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
        PARTITIONING_LABEL => "false",
    )
    .record(total_rows as f64);

    info!(
        table_id = table_id.0,
        total_rows, "completed serial table copy"
    );

    Ok(TableCopyResult::Completed { total_rows })
}

/// Copies a table in parallel using ctid-based partitioning across multiple connections.
///
/// Exports the main connection's snapshot, plans partitions using NTILE, then spawns
/// one child connection per partition to copy concurrently. All children read from the
/// same consistent snapshot.
#[expect(clippy::too_many_arguments)]
async fn parallel_table_copy<D: Destination + Clone + Send + 'static>(
    transaction: &PgReplicationTransaction,
    table_id: TableId,
    table_schema: &TableSchema,
    publication_name: Option<&str>,
    max_copy_connections: u16,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
) -> EtlResult<TableCopyResult> {
    info!(
        table_id = table_id.0,
        max_copy_connections, "starting parallel table copy"
    );

    // Plan ctid partitions on the main connection, which is already pinned to the
    // exported snapshot via SET TRANSACTION SNAPSHOT in create_slot_with_transaction.
    let partitions = transaction
        .plan_ctid_partitions(table_id, publication_name, max_copy_connections)
        .await?;

    // If there are no partitions, we don't copy anything.
    if partitions.is_empty() {
        info!(
            table_id = table_id.0,
            "table is empty, skipping parallel copy"
        );
        return Ok(TableCopyResult::Completed { total_rows: 0 });
    }

    info!(
        table_id = table_id.0,
        num_partitions = partitions.len(),
        "planned ctid partitions for parallel copy"
    );

    // Export the snapshot from the main connection's transaction so child connections can
    // import it via SET TRANSACTION SNAPSHOT. The main transaction must stay open for the
    // entire duration of the parallel copy to keep the snapshot valid.
    let snapshot_id = transaction.export_snapshot().await?;

    info!(
        table_id = table_id.0,
        "exported snapshot for child connections"
    );

    let mut join_set = JoinSet::new();

    for partition in partitions {
        // We create the child connection and then transaction, so that we can consistently perform
        // operations.
        let child_replication_client = transaction.fork_child().await?;
        let child_transaction =
            PgReplicationChildTransaction::new(child_replication_client, &snapshot_id).await?;

        let column_schemas = table_schema.column_schemas.clone();
        let publication_name = publication_name.map(|s| s.to_string());
        let batch_config = batch_config.clone();
        let shutdown_rx = shutdown_rx.clone();
        let destination = destination.clone();

        let copy_partition_fut = copy_partition(
            child_transaction,
            table_id,
            column_schemas,
            publication_name,
            partition,
            batch_config,
            shutdown_rx,
            pipeline_id,
            destination,
        );
        join_set.spawn(copy_partition_fut);
    }

    let mut total_rows: u64 = 0;

    // TODO: we might want to edit retries within the same partition if the copy fails. However, we
    //  need to investigate the frequency.
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(rows)) => {
                total_rows += rows;
            }
            Ok(Err(err)) => {
                error!(
                    table_id = table_id.0,
                    error = %err,
                    "one or more parallel copy partitions failed"
                );
                join_set.abort_all();

                return Err(err);
            }
            Err(join_err) => {
                error!(
                    table_id = table_id.0,
                    error = %join_err,
                    "one or more parallel copy partitions panicked"
                );
                join_set.abort_all();

                return Err(etl_error!(
                    ErrorKind::TableSyncWorkerPanic,
                    "One or more parallel copy partition tasks panicked, aborting all",
                    join_err.to_string()
                ));
            }
        }
    }

    // After all tasks complete, check if shutdown was requested.
    if shutdown_rx.has_changed().unwrap_or(false) {
        info!(
            table_id = table_id.0,
            total_rows, "shutting down parallel table copy"
        );
        return Ok(TableCopyResult::Shutdown);
    }

    info!(
        table_id = table_id.0,
        total_rows, "completed parallel table copy"
    );

    Ok(TableCopyResult::Completed { total_rows })
}

/// Copies a single ctid partition from the source table to the destination.
///
/// The child transaction is already pinned to the exported snapshot. Streams rows
/// within the partition's ctid range and feeds batches to the destination.
#[expect(clippy::too_many_arguments)]
async fn copy_partition<D>(
    child_transaction: PgReplicationChildTransaction,
    table_id: TableId,
    column_schemas: Vec<ColumnSchema>,
    publication_name: Option<String>,
    partition: CtidPartition,
    batch_config: BatchConfig,
    shutdown_rx: ShutdownRx,
    pipeline_id: PipelineId,
    destination: D,
) -> EtlResult<u64>
where
    D: Destination + Clone + Send + 'static,
{
    info!(
        table_id = table_id.0,
        start_tid = %partition.start_tid,
        end_tid = %partition.end_tid,
        "starting partition copy"
    );

    let copy_stream = child_transaction
        .get_table_copy_stream_with_ctid_partition(
            table_id,
            &column_schemas,
            publication_name.as_deref(),
            &partition,
        )
        .await?;

    let table_copy_stream = TableCopyStream::wrap(copy_stream, &column_schemas, pipeline_id);
    let table_copy_stream = TimeoutBatchStream::wrap(table_copy_stream, batch_config, shutdown_rx);
    pin!(table_copy_stream);

    let mut rows_copied: u64 = 0;

    while let Some(result) = table_copy_stream.next().await {
        match result {
            ShutdownResult::Ok(table_rows) => {
                let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
                let batch_size = table_rows.len() as u64;
                rows_copied += batch_size;

                let before_sending = Instant::now();

                destination.write_table_rows(table_id, table_rows).await?;

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
                    PARTITIONING_LABEL => "true",
                )
                .record(send_duration_seconds);

                #[cfg(feature = "failpoints")]
                etl_fail_point(START_TABLE_SYNC_DURING_DATA_SYNC)?;
            }
            ShutdownResult::Shutdown(_) => {
                info!(
                    table_id = table_id.0,
                    rows_copied, "partition copy interrupted by shutdown"
                );
                break;
            }
        }
    }

    child_transaction.commit().await?;

    histogram!(
        ETL_TABLE_COPY_ROWS,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
        PARTITIONING_LABEL => "true",
    )
    .record(rows_copied as f64);

    info!(
        table_id = table_id.0,
        rows_copied,
        start_tid = %partition.start_tid,
        end_tid = %partition.end_tid,
        "completed partition copy"
    );

    Ok(rows_copied)
}
