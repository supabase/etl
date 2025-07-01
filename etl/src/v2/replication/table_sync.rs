use crate::v2::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::v2::concurrency::stream::BatchStream;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::PgReplicationClient;
use crate::v2::replication::slot::get_slot_name;
use crate::v2::replication::stream::TableCopyStream;
use crate::v2::schema::cache::SchemaCache;
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::base::WorkerType;
use crate::v2::workers::table_sync::TableSyncWorkerState;
use crate::{
    error::{Error, Result},
    v2::state::store::base::StateStore,
};
use futures::StreamExt;
use postgres::schema::TableId;
use std::sync::Arc;
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::info;

#[derive(Debug)]
pub enum TableSyncResult {
    SyncStopped,
    SyncNotRequired,
    SyncCompleted { start_lsn: PgLsn },
}

#[allow(clippy::too_many_arguments)]
pub async fn start_table_sync<S, D>(
    identity: PipelineIdentity,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    table_id: TableId,
    table_sync_worker_state: TableSyncWorkerState,
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
) -> Result<TableSyncResult>
where
    S: StateStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    let inner = table_sync_worker_state.get_inner().read().await;
    let phase_type = inner.replication_phase().as_type();

    // In case the work for this table has been already done, we don't want to continue and we
    // successfully return.
    if matches!(
        phase_type,
        TableReplicationPhaseType::SyncDone | TableReplicationPhaseType::Ready
    ) {
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
        return Err(Error::other(
            "table_sync",
            format!(
                "Invalid replication phase '{}': expected Init, DataSync, or FinishedCopy",
                phase_type
            ),
        ));
    }

    // We are safe to unlock the state here, since we know that the state will be changed by the
    // apply worker only if `SyncWait` is set, which is not the case if we arrive here, so we are
    // good to reduce the length of the critical section.
    drop(inner);

    let slot_name = get_slot_name(identity.id(), WorkerType::TableSync { table_id })?;

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
                    // Convert PgReplicationError to our centralized Error type
                    // For slot not found, we continue; for other errors, we propagate
                    let error_message = err.to_string();
                    if !error_message.contains("slot") || !error_message.contains("not found") {
                        return Err(Error::other("postgres_replication", error_message));
                    }
                }
            }

            // We are ready to start copying table data, and we update the state accordingly.
            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner
                    .set_phase_with(TableReplicationPhase::DataSync, state_store.clone())
                    .await?;
            }

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
            let table_schema = transaction
                .get_table_schema(table_id, Some(identity.publication_name()))
                .await?;
            schema_cache.add_table_schema(table_schema.clone()).await;
            destination.write_table_schema(table_schema.clone()).await?;

            // We create the copy table stream.
            let table_copy_stream = transaction
                .get_table_copy_stream(table_id, &table_schema.column_schemas)
                .await?;
            let table_copy_stream =
                TableCopyStream::wrap(table_id, table_copy_stream, &table_schema.column_schemas);
            let table_copy_stream =
                BatchStream::wrap(table_copy_stream, config.batch.clone(), shutdown_rx.clone());
            pin!(table_copy_stream);

            // We start consuming the table stream. If any error occurs, we will bail the entire copy since
            // we want to be fully consistent.
            while let Some(result) = table_copy_stream.next().await {
                match result {
                    ShutdownResult::Ok(table_rows) => {
                        let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
                        destination.write_table_rows(table_id, table_rows).await?;
                    }
                    ShutdownResult::Shutdown(_) => {
                        // If we received a shutdown in the middle of a table copy, we bail knowing
                        // that the system can automatically recover if a table copy has failed in
                        // the middle of processing.
                        info!(
                            "Shutting down table sync worker for table {} during table copy",
                            table_id
                        );
                        return Ok(TableSyncResult::SyncStopped);
                    }
                }
            }

            // We commit the transaction before starting the apply loop, otherwise it will fail
            // since no transactions can be running while replication is started.
            transaction.commit().await?;

            // We mark that we finished the copy of the table schema and data.
            {
                let mut inner = table_sync_worker_state.get_inner().write().await;
                inner
                    .set_phase_with(TableReplicationPhase::FinishedCopy, state_store.clone())
                    .await?;
            }

            slot.consistent_point
        }
        TableReplicationPhaseType::FinishedCopy => {
            let slot = replication_client.get_slot(&slot_name).await?;
            slot.confirmed_flush_lsn
        }
        _ => unreachable!("Phase type already validated above"),
    };

    // We mark this worker as `SyncWait` (in memory only) to signal the apply worker that we are
    // ready to start catchup.
    {
        let mut inner = table_sync_worker_state.get_inner().write().await;
        inner
            .set_phase_with(TableReplicationPhase::SyncWait, state_store)
            .await?;
    }

    // We also wait to be signaled to catchup with the main apply worker up to a specific lsn.
    let result = table_sync_worker_state
        .wait_for_phase_type(TableReplicationPhaseType::Catchup, shutdown_rx.clone())
        .await;

    // If we are told to shut down while waiting for a phase change, we will signal this to
    // the caller.
    if result.should_shutdown() {
        info!(
            "Shutting down table sync worker for table {} while waiting for catchup",
            table_id
        );
        return Ok(TableSyncResult::SyncStopped);
    }

    Ok(TableSyncResult::SyncCompleted { start_lsn })
}
