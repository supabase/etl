use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::get_slot_name;
use etl_postgres::replication::worker::WorkerType;
use etl_postgres::schema::TableId;
use futures::StreamExt;
use metrics::{counter, gauge};
use std::sync::Arc;
use std::time::Instant;
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{error, info, warn};

use crate::bail;
use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::concurrency::signal::SignalTx;
use crate::concurrency::stream::BatchStream;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
#[cfg(feature = "failpoints")]
use crate::failpoints::{
    START_TABLE_SYNC__BEFORE_DATA_SYNC_SLOT_CREATION, START_TABLE_SYNC__DURING_DATA_SYNC,
    etl_fail_point,
};
use crate::metrics::{
    ETL_BATCH_SEND_MILLISECONDS_TOTAL, ETL_BATCH_SIZE, ETL_TABLE_SYNC_ROWS_COPIED_TOTAL,
};
use crate::replication::client::PgReplicationClient;
use crate::replication::stream::TableCopyStream;
use crate::state::table::RetryPolicy;
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::table_sync::TableSyncWorkerState;

/// Result type for table synchronization operations.
///
/// [`TableSyncResult`] indicates the outcome of a table sync operation,
/// providing context for how the table sync worker should proceed with the table.
#[derive(Debug)]
pub enum TableSyncResult {
    /// Synchronization was stopped due to shutdown or external signal.
    SyncStopped,
    /// Synchronization was not required (table already synchronized).
    SyncNotRequired,
    /// Synchronization completed successfully with the starting LSN for replication.
    SyncCompleted {
        /// LSN position where continuous replication should begin for this table.
        start_lsn: PgLsn,
    },
}

/// Starts table synchronization for a specific table.
///
/// This function performs the initial data copy for a table from the source
/// Postgres database to the destination. It handles the complete sync process
/// including data copying, state management, and coordination with the apply worker.
#[expect(clippy::too_many_arguments)]
pub async fn start_table_sync<S, D>(
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    table_id: TableId,
    table_sync_worker_state: TableSyncWorkerState,
    store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    force_syncing_tables_tx: SignalTx,
) -> EtlResult<TableSyncResult>
where
    S: StateStore + SchemaStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    info!("starting table sync for table {}", table_id);

    // We are safe to keep the lock only for this section, since we know that the state will be changed by the
    // apply worker only if `SyncWait` is set, which is not the case if we arrive here, so we are
    // good to reduce the length of the critical section.
    let phase_type = {
        let inner = table_sync_worker_state.lock().await;
        let phase_type = inner.replication_phase().as_type();

        // In case the work for this table has been already done, we don't want to continue and we
        // successfully return.
        if matches!(
            phase_type,
            TableReplicationPhaseType::SyncDone | TableReplicationPhaseType::Ready
        ) {
            info!(
                "table {} sync not required, already in phase '{:?}'",
                table_id, phase_type
            );

            return Ok(TableSyncResult::SyncNotRequired);
        }

        // In case the phase is different from the standard phases in which a table sync worker can perform
        // table syncing, we want to return an error.
        if !matches!(
            phase_type,
            TableReplicationPhaseType::Init
                | TableReplicationPhaseType::DataSync
                | TableReplicationPhaseType::FinishedCopy
        ) {
            warn!(
                "invalid replication phase '{:?}' for table {}, cannot perform table sync",
                phase_type, table_id
            );

            bail!(
                ErrorKind::InvalidState,
                "Invalid replication phase",
                format!(
                    "Invalid replication phase '{:?}': expected 'Init', 'DataSync', or 'FinishedCopy'",
                    phase_type
                )
            );
        }

        phase_type
    };

    let slot_name = get_slot_name(pipeline_id, WorkerType::TableSync { table_id })?;

    // There are three phases in which the table can be in:
    // - `Init` -> this means that the table sync was never done, so we just perform it.
    // - `DataSync` -> this means that there was a failure during data sync, and we have to restart
    //  copying all the table data and delete the slot.
    // - `FinishedCopy` -> this means that the table was successfully copied, but we didn't manage to
    //  complete the table sync function, so we just want to continue the cdc stream from the slot's
    // confirmed_flush_lsn value.
    //
    // In case the phase is any other phase, we will return an error.
    let start_lsn = match phase_type {
        TableReplicationPhaseType::Init | TableReplicationPhaseType::DataSync => {
            if phase_type == TableReplicationPhaseType::DataSync {
                // If we are in `DataSync` it means we failed during table copying, so we want to delete the
                // existing slot before continuing.
                if let Err(err) = replication_client.delete_slot(&slot_name).await {
                    // If the slot is not found, we are safe to continue, for any other error, we bail.
                    if err.kind() != ErrorKind::ReplicationSlotNotFound {
                        return Err(err);
                    }
                }

                // We must truncate the destination table before starting a copy to avoid data inconsistencies.
                // Example scenario:
                // 1. The source table has a single row (id = 1) that is copied to the destination during the initial copy.
                // 2. Before the table’s phase is set to `FinishedCopy`, the process crashes.
                // 3. While down, the source deletes row id = 1 and inserts row id = 2.
                // 4. When restarted, the process sees the table in the ` DataSync ` state, deletes the slot, and copies again.
                // 5. This time, only row id = 2 is copied, but row id = 1 still exists in the destination.
                // Result: the destination has two rows (id = 1 and id = 2) instead of only one (id = 2).
                // Fix: Always truncate the destination table before starting a copy.
                destination.truncate_table(table_id).await?;
            }

            // We are ready to start copying table data, and we update the state accordingly.
            info!("starting data copy for table {}", table_id);
            {
                let mut inner = table_sync_worker_state.lock().await;
                inner
                    .set_and_store(TableReplicationPhase::DataSync, &store)
                    .await?;
            }

            // Fail point to test when the table sync fails before copying data.
            #[cfg(feature = "failpoints")]
            etl_fail_point(START_TABLE_SYNC__BEFORE_DATA_SYNC_SLOT_CREATION)?;

            // We create the slot with a transaction, since we need to have a consistent snapshot of the database
            // before copying the schema and tables.
            //
            // If a slot already exists at this point, we could delete it and try to recover, but it means
            // that the state was somehow reset without the slot being deleted, and we want to surface this.
            let (transaction, slot) = replication_client
                .create_slot_with_transaction(&slot_name)
                .await?;

            // We copy the table schema and write it both to the state store and destination.
            //
            // Note that we write the schema in both places:
            // - State store -> we write here because the table schema is used across table copying and cdc
            //  for correct decoding, thus we rely on our own state store to preserve this information.
            // - Destination -> we write here because some consumers might want to have the schema of incoming
            //  data.
            info!("fetching table schema for table {}", table_id);
            let table_schema = transaction
                .get_table_schema(table_id, Some(&config.publication_name))
                .await?;

            if !table_schema.has_primary_keys() {
                store
                    .update_table_replication_state(
                        table_id,
                        TableReplicationPhase::Errored {
                            reason: "The table has no primary keys".to_string(),
                            solution: Some(format!(
                                "You should set at least one primary key on the table {table_id}"
                            )),
                            retry_policy: RetryPolicy::ManualRetry,
                        },
                    )
                    .await?;

                bail!(
                    ErrorKind::SourceSchemaError,
                    "Missing primary key",
                    format!("table {} has no primary key", table_schema.name)
                );
            }

            // We store the table schema in the schema store to be able to retrieve it even when the
            // pipeline is restarted, since it's outside the lifecycle of the pipeline.
            store.store_table_schema(table_schema.clone()).await?;

            // We create the copy table stream.
            let table_copy_stream = transaction
                .get_table_copy_stream(table_id, &table_schema.column_schemas)
                .await?;
            let table_copy_stream =
                TableCopyStream::wrap(table_copy_stream, &table_schema.column_schemas);
            let table_copy_stream =
                BatchStream::wrap(table_copy_stream, config.batch.clone(), shutdown_rx.clone());
            pin!(table_copy_stream);

            info!("starting table copy stream for table {}", table_id);
            // We start consuming the table stream. If any error occurs, we will bail the entire copy since
            // we want to be fully consistent.
            let mut rows_copied = 0;
            while let Some(result) = table_copy_stream.next().await {
                match result {
                    ShutdownResult::Ok(table_rows) => {
                        let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
                        rows_copied += table_rows.len();
                        let before_sending = Instant::now();

                        destination.write_table_rows(table_id, table_rows).await?;

                        counter!(ETL_TABLE_SYNC_ROWS_COPIED_TOTAL).increment(rows_copied as u64);
                        gauge!(ETL_BATCH_SIZE).set(rows_copied as f64);
                        let time_taken_to_send = before_sending.elapsed().as_millis();
                        gauge!(ETL_BATCH_SEND_MILLISECONDS_TOTAL).set(time_taken_to_send as f64);
                        // Fail point to test when the table sync fails after copying one batch.
                        #[cfg(feature = "failpoints")]
                        etl_fail_point(START_TABLE_SYNC__DURING_DATA_SYNC)?;
                    }
                    ShutdownResult::Shutdown(_) => {
                        // If we received a shutdown in the middle of a table copy, we bail knowing
                        // that the system can automatically recover if a table copy has failed in
                        // the middle of processing.
                        info!(
                            "shutting down table sync worker for table {} during table copy",
                            table_id
                        );

                        return Ok(TableSyncResult::SyncStopped);
                    }
                }
            }

            // We commit the transaction before starting the apply loop, otherwise it will fail
            // since no transactions can be running while replication is started.
            transaction.commit().await?;

            info!(
                "completed table copy for table {} ({} rows copied)",
                table_id, rows_copied
            );

            // We mark that we finished the copy of the table schema and data.
            {
                let mut inner = table_sync_worker_state.lock().await;
                inner
                    .set_and_store(TableReplicationPhase::FinishedCopy, &store)
                    .await?;
            }

            slot.consistent_point
        }
        TableReplicationPhaseType::FinishedCopy => {
            let slot = replication_client.get_slot(&slot_name).await?;
            info!(
                "resuming table sync for table {} from lsn {}",
                table_id, slot.confirmed_flush_lsn
            );

            slot.confirmed_flush_lsn
        }
        _ => unreachable!("phase type already validated above"),
    };

    // We mark this worker as `SyncWait` (in memory only) to signal the apply worker that we are
    // ready to start catchup.
    {
        let mut inner = table_sync_worker_state.lock().await;
        inner
            .set_and_store(TableReplicationPhase::SyncWait, &store)
            .await?;

        // We notify the main apply worker to force syncing tables. In this way, the `Catchup` phase
        // will be started even if no events are flowing in the main apply loop.
        if force_syncing_tables_tx.send(()).is_err() {
            error!(
                "error while forcing syncing tables during '{:?}' phase of the table sync worker, the apply worker was likely shutdown",
                TableReplicationPhaseType::SyncWait
            );
        }
    }

    // We also wait to be signaled to catchup with the main apply worker up to a specific lsn.
    let result = table_sync_worker_state
        .wait_for_phase_type(TableReplicationPhaseType::Catchup, shutdown_rx.clone())
        .await;

    // If we are told to shut down while waiting for a phase change, we will signal this to
    // the caller.
    if result.should_shutdown() {
        info!(
            "shutting down table sync worker for table {} while waiting for catchup",
            table_id
        );

        return Ok(TableSyncResult::SyncStopped);
    }

    info!("table sync for table {} completed", table_id);

    Ok(TableSyncResult::SyncCompleted { start_lsn })
}
