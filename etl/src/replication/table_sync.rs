use config::shared::PipelineConfig;
use fail::fail_point;
use futures::StreamExt;
use postgres::schema::TableId;
use std::sync::Arc;
use thiserror::Error;
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{error, info, warn};

use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::concurrency::signal::SignalTx;
use crate::concurrency::stream::BatchStream;
use crate::destination::base::{Destination, DestinationError};
use crate::failpoints::START_TABLE_SYNC_AFTER_DATA_SYNC;
use crate::pipeline::PipelineId;
use crate::replication::client::{PgReplicationClient, PgReplicationError};
use crate::replication::slot::{SlotError, get_slot_name};
use crate::replication::stream::{TableCopyStream, TableCopyStreamError};
use crate::schema::cache::SchemaCache;
use crate::state::store::base::{StateStore, StateStoreError};
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::workers::base::WorkerType;
use crate::workers::table_sync::{TableSyncWorkerState, TableSyncWorkerStateError};

#[derive(Debug, Error)]
pub enum TableSyncError {
    #[error("Invalid replication phase '{0}': expected Init, DataSync, or FinishedCopy")]
    InvalidPhase(TableReplicationPhaseType),

    #[error("Invalid replication slot name: {0}")]
    InvalidSlotName(#[from] SlotError),

    #[error("PostgreSQL replication operation failed: {0}")]
    PgReplication(#[from] PgReplicationError),

    #[error("An error occurred while interacting with the table sync worker state: {0}")]
    TableSyncWorkerState(#[from] TableSyncWorkerStateError),

    #[error("An error occurred while writing to the destination: {0}")]
    Destination(#[from] DestinationError),

    #[error("An error happened in the state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error happened in the table copy stream")]
    TableCopyStream(#[from] TableCopyStreamError),
}

#[derive(Debug)]
pub enum TableSyncResult {
    SyncStopped,
    SyncNotRequired,
    SyncCompleted { start_lsn: PgLsn },
}

#[allow(clippy::too_many_arguments)]
pub async fn start_table_sync<S, D>(
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    table_id: TableId,
    table_sync_worker_state: TableSyncWorkerState,
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    force_syncing_tables_tx: SignalTx,
) -> Result<TableSyncResult, TableSyncError>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    info!("starting table sync for table {}", table_id);

    // We are safe to keep the lock only for this section, since we know that the state will be changed by the
    // apply worker only if `SyncWait` is set, which is not the case if we arrive here, so we are
    // good to reduce the length of the critical section.
    let phase_type = {
        let inner = table_sync_worker_state.get_inner().lock().await;
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

            return Err(TableSyncError::InvalidPhase(phase_type));
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
            // If we are in `DataSync` it means we failed during table copying, so we want to delete the
            // existing slot before continuing.
            if phase_type == TableReplicationPhaseType::DataSync {
                // TODO: After we delete the slot we will have to truncate the table in the destination,
                // otherwise there can be an inconsistent copy of the data. E.g. consider this scenario:
                // A table had a single row with id 1 and this was copied to the destination during initial
                // table copy. Before the table's phase was set to FinishedCopy, the process crashed.
                // While the process was down, row with id 1 in the source was deleted and another row with
                // id 2 was inserted. The process comes back up to find the table's state in DataSync,
                // deletes the slot and makes a copy again. This time it copies the row with id 2. Now
                // the destinations contains two rows (with id 1 and 2) instead of only one (with id 2).
                // The simplest fix here would be to unconditionally send a truncate to the destination
                // before starting a table copy.
                if let Err(err) = replication_client.delete_slot(&slot_name).await {
                    // If the slot is not found, we are safe to continue, for any other error, we bail.
                    if !matches!(err, PgReplicationError::SlotNotFound(_)) {
                        return Err(err.into());
                    }
                }
            }

            // We are ready to start copying table data, and we update the state accordingly.
            info!("starting data copy for table {}", table_id);
            {
                let mut inner = table_sync_worker_state.get_inner().lock().await;
                inner
                    .set_phase_with(TableReplicationPhase::DataSync, state_store.clone())
                    .await?;
            }

            // Fail point to test when the table sync fails.
            fail_point!(START_TABLE_SYNC_AFTER_DATA_SYNC);

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
            schema_cache.add_table_schema(table_schema.clone()).await;
            destination.write_table_schema(table_schema.clone()).await?;

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
                        destination.write_table_rows(table_id, table_rows).await?;
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
                let mut inner = table_sync_worker_state.get_inner().lock().await;
                inner
                    .set_phase_with(TableReplicationPhase::FinishedCopy, state_store.clone())
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
        let mut inner = table_sync_worker_state.get_inner().lock().await;
        inner
            .set_phase_with(TableReplicationPhase::SyncWait, state_store)
            .await?;

        // We notify the main apply worker to force syncing tables. In this way, the `Catchup` phase
        // will be started even if no events are flowing in the main apply loop.
        let _ = force_syncing_tables_tx.send(());
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
