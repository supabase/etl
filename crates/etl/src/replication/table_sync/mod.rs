use std::{sync::Arc, time::Duration};

mod copy;

pub(crate) use copy::{TableCopyResult, table_copy};
use etl_config::shared::PipelineConfig;
use etl_postgres::slots::EtlReplicationSlot;
#[cfg(feature = "failpoints")]
use fail::fail_point;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, warn};

#[cfg(feature = "failpoints")]
use crate::failpoints::{
    START_TABLE_SYNC_AFTER_FINISHED_COPY_FP, START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP,
    etl_fail_point,
};
use crate::{
    bail,
    destination::{
        DestinationTableMetadata, DestinationWriteStatus, DropTableForCopyResult,
        PipelineDestination, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    pipeline::PipelineId,
    postgres::{OutOfBandSourcePool, client::PgReplicationClient},
    replication::{
        state::{TableState, TableStateType},
        table_cache::SharedTableCache,
    },
    runtime::{
        BatchBudgetController, MemoryMonitor, TableSyncWorkerState,
        concurrency::{ShutdownResult, ShutdownRx},
    },
    schema::{ReplicatedTableSchema, ReplicationMask, SchemaError, TableId},
    store::{PipelineStore, SchemaStore, StateStore},
};

/// Result type for table synchronization operations.
///
/// [`TableSyncResult`] indicates the outcome of a table sync operation,
/// providing context for how the table sync worker should proceed with the
/// table.
#[derive(Debug)]
pub(crate) enum TableSyncResult {
    /// Synchronization was stopped due to shutdown or external signal.
    Stopped,
    /// Synchronization was not required (table already synchronized).
    NotRequired,
    /// Synchronization completed successfully with the starting LSN for
    /// replication.
    Completed {
        /// LSN position where continuous replication should begin for this
        /// table.
        start_lsn: PgLsn,
    },
}

/// Returns existing destination metadata and schema if they exist.
///
/// A [`ReplicatedTableSchema`] could be there when starting a table copy
/// because it was either interrupted or the state was reset.
async fn get_existing_replicated_table_schema<S>(
    store: &S,
    table_id: TableId,
) -> EtlResult<Option<(DestinationTableMetadata, ReplicatedTableSchema)>>
where
    S: StateStore + SchemaStore + Send + 'static,
{
    let Some(current_metadata) = store.get_destination_table_metadata(table_id).await? else {
        return Ok(None);
    };

    let Some(table_schema) =
        store.get_table_schema(&table_id, current_metadata.snapshot_id).await?
    else {
        bail!(
            ErrorKind::InvalidState,
            "Destination table metadata found, but no corresponding table schema exists"
        );
    };

    let existing_replicated_table_schema =
        ReplicatedTableSchema::from_mask(table_schema, current_metadata.replication_mask.clone());

    Ok(Some((current_metadata, existing_replicated_table_schema)))
}

/// Starts table synchronization for a specific table.
///
/// This function performs the initial data copy for a table from the source
/// Postgres database to the destination. It handles the complete sync process
/// including data copying, state management, and coordination with the apply
/// worker.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn start_table_sync<S, D>(
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    replication_client: &mut PgReplicationClient,
    table_id: TableId,
    table_sync_worker_state: TableSyncWorkerState,
    store: S,
    destination: D,
    shared_table_cache: &SharedTableCache,
    out_of_band_source_pool: OutOfBandSourcePool,
    mut shutdown_rx: ShutdownRx,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableSyncResult>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    info!(table_id = table_id.0, "starting initial table sync");

    // We are safe to keep the lock only for this section, since we know that the
    // state will be changed by the apply worker only if `SyncWait` is set,
    // which is not the case if we arrive here, so we are good to reduce the
    // length of the critical section.
    let state_type = {
        let inner = table_sync_worker_state.lock().await;
        let state_type = inner.table_state().as_type();

        // In case the work for this table has been already done, we don't want to
        // continue and we successfully return.
        if matches!(
            state_type,
            TableStateType::SyncDone | TableStateType::Ready | TableStateType::Errored
        ) {
            warn!(
                table_id = table_id.0,
                table_state_type = %state_type,
                "initial table sync not required"
            );

            return Ok(TableSyncResult::NotRequired);
        }

        // In case the state is different from the standard states in which a table sync
        // worker can perform table syncing, we want to return an error.
        if !matches!(
            state_type,
            TableStateType::Init | TableStateType::DataSync | TableStateType::FinishedCopy
        ) {
            warn!(
                table_id = table_id.0,
                table_state_type = %state_type,
                "invalid table state for table sync"
            );

            bail!(
                ErrorKind::InvalidState,
                "Invalid table state",
                format!(
                    "Invalid table state '{:?}': expected 'Init', 'DataSync', or 'FinishedCopy'",
                    state_type
                )
            );
        }

        state_type
    };

    let slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into()?;

    // There are three states from which table sync can start:
    // - `Init` -> the table sync was never done or the table was reset, so we
    //   perform it.
    // - `DataSync` -> a previous copy did not complete, so we restart the copy from
    //   a clean snapshot.
    // - `FinishedCopy` -> the copy completed, but the table sync worker did not
    //   finish the ownership handoff. This is a narrow crash window, so we
    //   intentionally restart the copy instead of trying to resume catchup.
    //   Resuming soundly would require persisting the exact runtime relation
    //   decoding state, including identity masks, and making handoff safe when no
    //   further `RELATION` message arrives.
    //
    // In case the state is any other state, we will return an error.
    let start_lsn = match state_type {
        TableStateType::Init | TableStateType::DataSync | TableStateType::FinishedCopy => {
            // We must drop the destination table before starting a copy to avoid data
            // inconsistencies when there is a previous table.
            //
            // Example scenario:
            // 1. The source table has a single row (id = 1) that is copied to the
            //    destination during the initial copy.
            // 2. Before the table's state is set to `FinishedCopy`, the process crashes.
            // 3. While down, the source deletes row id = 1 and inserts row id = 2.
            // 4. When restarted, the process sees the table in the `DataSync` state,
            //    deletes the slot, and copies again.
            // 5. This time, only row id = 2 is copied, but row id = 1 still exists in the
            //    destination.
            //
            // Result: the destination has two rows (id = 1 and id = 2) instead of only one
            // (id = 2).
            //
            // Fix: Always drop the destination table before starting a copy.
            if let Some((current_metadata, current_replication_table_schema)) =
                get_existing_replicated_table_schema(&store, table_id).await?
            {
                warn!(
                    table_id = table_id.0,
                    destination_table_id = %current_metadata.destination_table_id,
                    snapshot_id = %current_metadata.snapshot_id,
                    "dropping pre-existing destination table before table copy"
                );

                let (drop_result, pending_drop_result) = DropTableForCopyResult::new(());
                destination
                    .drop_table_for_copy(&current_replication_table_schema, drop_result)
                    .await?;
                let ShutdownResult::Ok(completed_drop_result) =
                    pending_drop_result.with_shutdown(&mut shutdown_rx).await
                else {
                    return Ok(TableSyncResult::Stopped);
                };
                completed_drop_result.into_result()?;
            }

            // We try to delete the slot if it already exists, since we might be starting a
            // table copy after a previous one was reset or didn't complete
            // successfully.
            replication_client.delete_slot_if_exists(&slot_name).await?;

            // We prepare durable and in-memory table-copy state only after the
            // destination drop succeeds. The shared cache removal is idempotent: a
            // first copy has no cached state yet, while an in-process retry can still
            // hold the previous ready runtime schema. The fresh `0/0` copy schema
            // below is the only state allowed to repopulate the cache.
            store.prepare_table_state_for_copy(table_id).await?;
            shared_table_cache.remove_table(table_id).await;
            debug!(table_id = table_id.0, "prepared table state before copy");

            // We are ready to start copying table data, and we update the state
            // accordingly.
            {
                let mut inner = table_sync_worker_state.lock().await;
                inner.set_and_store(TableState::DataSync, &store).await?;
            }

            // Fail point to test when the table sync fails before copying data.
            #[cfg(feature = "failpoints")]
            etl_fail_point(START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP)?;

            // We create the slot with a transaction, since we need to have a consistent
            // snapshot of the database before copying the schema and tables.
            //
            // If a slot already exists at this point, we could delete it and try to
            // recover, but it means that the state was somehow reset without
            // the slot being deleted, and we want to surface this.
            let (replication_transaction, slot) =
                replication_client.create_slot_with_transaction(&slot_name).await?;

            // We copy the table schema and write it both to the state store and
            // destination.
            //
            // Note that we write the schema in both places:
            // - State store -> we write here because the table schema is used across table
            //   copying and cdc
            //  for correct decoding, thus we rely on our own state store to preserve this
            // information.
            // - Destination -> we write here because some consumers might want to have the
            //   schema of incoming
            //  data.
            info!(%table_id, "fetching table schema");
            let (table_schema, identity) =
                replication_transaction.get_table_schema_with_identity(table_id).await?;

            // We store the table schema in the schema store to be able to retrieve it even
            // when the pipeline is restarted, since it's outside the lifecycle
            // of the pipeline.
            let table_schema = store.store_table_schema(table_schema).await?;

            // Get the names of columns being replicated based on the publication's column
            // filter. This must be done in the same transaction as
            // `get_table_schema` for consistency.
            let replicated_column_names = replication_transaction
                .get_replicated_column_names(table_id, &table_schema, &config.publication_name)
                .await?;

            // Build and store the per-table protocol state for use during CDC.
            // We use `try_build` here because the schema was just loaded and should match
            // the publication's column filter. Any mismatch indicates a schema
            // inconsistency.
            let replication_mask =
                ReplicationMask::try_build(&table_schema, &replicated_column_names).map_err(
                    |err: SchemaError| {
                        etl_error!(
                            ErrorKind::InvalidState,
                            "Schema mismatch during table sync",
                            format!("{}", err)
                        )
                    },
                )?;
            let identity_mask = identity.build_identity_mask(&table_schema, &replication_mask)?;

            // Create the replicated table schema with the exact runtime identity
            // metadata so catchup and apply can continue decoding row events
            // without waiting for a fresh relation message after restarts.
            let replicated_table_schema =
                ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);

            let mut total_table_copy_rows = 0_u64;
            let mut total_table_copy_duration_secs = 0.0;
            let mut table_copy_barrier_required = false;

            // We check if the table should be copied, or we can skip it.
            if config.table_sync_copy.should_copy_table(table_id.into_inner()) {
                let result = table_copy(
                    &replication_transaction,
                    table_id,
                    replicated_table_schema.clone(),
                    Some(&config.publication_name),
                    config.max_copy_connections_per_table,
                    slot.consistent_point,
                    out_of_band_source_pool.clone(),
                    Duration::from_millis(config.replication_lag_refresh_interval_ms),
                    config.batch.clone(),
                    shutdown_rx.clone(),
                    destination.clone(),
                    memory_monitor.clone(),
                    batch_budget.clone(),
                )
                .await?;

                match result {
                    TableCopyResult::Completed {
                        total_rows,
                        total_duration_secs,
                        barrier_required,
                    } => {
                        total_table_copy_rows = total_rows;
                        total_table_copy_duration_secs = total_duration_secs;
                        table_copy_barrier_required = barrier_required;
                    }
                    TableCopyResult::Shutdown => {
                        info!(
                            table_id = table_id.0,
                            "table copy interrupted by shutdown, terminating table sync"
                        );

                        return Ok(TableSyncResult::Stopped);
                    }
                }
            } else {
                info!(table_id = table_id.0, "skipping table copy");
            }

            // We commit the transaction before starting the apply loop, otherwise it will
            // fail since no transactions can be running while replication is
            // started.
            replication_transaction.commit().await?;

            // If no table rows were written, call the method nonetheless to kickstart
            // table creation. Additionally, if any copy write was only accepted (and not
            // durably committed), the empty write is also the terminal table-wide
            // durability barrier.
            if total_table_copy_rows == 0 || table_copy_barrier_required {
                let (flush_result, pending_flush_result) = WriteTableRowsResult::new(());
                destination
                    .write_table_rows(&replicated_table_schema, vec![], flush_result)
                    .await?;
                let ShutdownResult::Ok(completed_flush_result) =
                    pending_flush_result.with_shutdown(&mut shutdown_rx).await
                else {
                    return Ok(TableSyncResult::Stopped);
                };

                match completed_flush_result.into_result()? {
                    DestinationWriteStatus::Durable => {}
                    DestinationWriteStatus::Accepted => bail!(
                        ErrorKind::DestinationError,
                        "Table copy durability barrier did not confirm durability"
                    ),
                }
            }

            info!(
                table_id = table_id.0,
                total_table_copy_rows, total_table_copy_duration_secs, "completed table copy"
            );

            // We mark that we finished the copy of the table schema and data.
            {
                let mut inner = table_sync_worker_state.lock().await;
                inner.set_and_store(TableState::FinishedCopy, &store).await?;
            }

            // After we finished copying, we mark this table as ready in the cache, so
            // that we can start streaming and decoding immediately.
            //
            // This is needed, since it could be that the apply loop for the `Catchup` state
            // might be idle and progress only via keepalives and in that case no `Relation`
            // message will be received, so we want the apply worker to already be able to
            // start decoding.
            shared_table_cache.note_ready(table_id, replicated_table_schema.clone()).await;

            #[cfg(feature = "failpoints")]
            fail_point!(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP);

            slot.consistent_point
        }
        _ => unreachable!("state type already validated above"),
    };

    // We mark this worker as `SyncWait` (in memory only) to signal the apply worker
    // that we are ready to start catchup. We pass the snapshot LSN so the apply
    // worker can use max(snapshot_lsn, current_lsn) when setting the Catchup
    // LSN.
    {
        let mut inner = table_sync_worker_state.lock().await;
        inner.set_and_store(TableState::SyncWait { lsn: start_lsn }, &store).await?;
    }

    // We also wait to be signaled to catch up with the main apply worker up to a
    // specific lsn.
    let result = table_sync_worker_state
        .wait_for_state_type(&[TableStateType::Catchup], shutdown_rx.clone())
        .await;

    // If we are told to shut down while waiting for a state change, we will signal
    // this to the caller.
    if result.should_shutdown() {
        info!(table_id = table_id.0, "shutting down table sync while waiting for catchup");

        return Ok(TableSyncResult::Stopped);
    }

    info!(table_id = table_id.0, "table sync completed, starting streaming");

    Ok(TableSyncResult::Completed { start_lsn })
}
