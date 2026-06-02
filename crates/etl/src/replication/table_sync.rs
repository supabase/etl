use std::sync::Arc;

use etl_config::shared::PipelineConfig;
use etl_postgres::{
    replication::slots::EtlReplicationSlot,
    types::{ReplicatedTableSchema, ReplicationMask, SchemaError, TableId, TableSchema},
};
use metrics::histogram;
use tokio_postgres::types::PgLsn;
use tracing::{info, warn};

#[cfg(feature = "failpoints")]
use crate::failpoints::{START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP, etl_fail_point};
use crate::{
    bail,
    concurrency::{BatchBudgetController, MemoryMonitor, ShutdownRx},
    conversions::arrow::table_rows_to_arrow_batch,
    destination::{
        PipelineDestination,
        async_result::{DropTableForCopyResult, WriteSnapshotBatchResult},
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    metrics::{ETL_TABLE_COPY_DURATION_SECONDS, PARTITIONING_LABEL},
    replication::{SharedTableCache, client::PgReplicationClient},
    state::{TableState, TableStateType},
    store::{PipelineStore, schema::SchemaStore, state::StateStore},
    types::PipelineId,
    workers::{TableCopyResult, TableSyncWorkerState, table_copy},
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

/// Builds the Arrow batch schema for a replicated table schema.
fn arrow_table_schema(replicated_table_schema: &ReplicatedTableSchema) -> Arc<TableSchema> {
    Arc::new(TableSchema::with_snapshot_id(
        replicated_table_schema.id(),
        replicated_table_schema.name().clone(),
        replicated_table_schema.column_schemas().cloned().collect(),
        replicated_table_schema.inner().snapshot_id,
    ))
}

/// Returns the existing replicated schema if the destination has table
/// metadata.
async fn get_existing_replicated_table_schema<S>(
    store: &S,
    table_id: TableId,
) -> EtlResult<Option<ReplicatedTableSchema>>
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

    Ok(Some(ReplicatedTableSchema::from_mask(table_schema, current_metadata.replication_mask)))
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
    shutdown_rx: ShutdownRx,
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
    let phase_type = {
        let inner = table_sync_worker_state.lock().await;
        let phase_type = inner.table_state().as_type();

        // In case the work for this table has been already done, we don't want to
        // continue and we successfully return.
        if matches!(
            phase_type,
            TableStateType::SyncDone | TableStateType::Ready | TableStateType::Errored
        ) {
            warn!(table_id = table_id.0, %phase_type, "initial table sync not required");

            return Ok(TableSyncResult::NotRequired);
        }

        // In case the phase is different from the standard phases in which a table sync
        // worker can perform table syncing, we want to return an error.
        if !matches!(
            phase_type,
            TableStateType::Init | TableStateType::DataSync | TableStateType::FinishedCopy
        ) {
            warn!(table_id = table_id.0, %phase_type, "invalid replication phase for table sync");

            bail!(
                ErrorKind::InvalidState,
                "Invalid replication phase",
                format!(
                    "Invalid replication phase '{:?}': expected 'Init', 'DataSync', or \
                     'FinishedCopy'",
                    phase_type
                )
            );
        }

        phase_type
    };

    let slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into()?;

    // There are three phases from which table sync can start:
    // - `Init` -> the table sync was never done or the state was reset, so we
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
    // In case the phase is any other phase, we will return an error.
    let start_lsn = match phase_type {
        TableStateType::Init | TableStateType::DataSync | TableStateType::FinishedCopy => {
            // We must drop destination state before starting a copy to avoid
            // data inconsistencies when there is a previous table.
            //
            // Example scenario:
            // 1. The source table has a single row (id = 1) that is copied to the
            //    destination during the initial copy.
            // 2. Before the table's phase is set to `FinishedCopy`, the process crashes.
            // 3. While down, the source deletes row id = 1 and inserts row id = 2.
            // 4. When restarted, the process sees the table in the `DataSync` state,
            //    deletes the slot, and copies again.
            // 5. This time, only row id = 2 is copied, but row id = 1 still exists in the
            //    destination.
            //
            // Result: the destination has two rows (id = 1 and id = 2) instead
            // of only one (id = 2).
            //
            // Fix: drop the destination table before starting a fresh copy when
            // we have stored destination metadata for the table. If cleanup
            // fails, the copy must not continue.
            if let Some(existing_schema) =
                get_existing_replicated_table_schema(&store, table_id).await?
            {
                let (drop_result, pending_drop_result) = DropTableForCopyResult::new(());
                destination.drop_table_for_copy(&existing_schema, drop_result).await?;
                pending_drop_result.await.into_result()?;
            }

            // We try to delete the slot if it already exists, since we might be
            // starting a table copy after a previous one was reset or did not
            // complete successfully.
            replication_client.delete_slot_if_exists(&slot_name).await?;

            // We clear durable and in-memory table-copy state only after
            // external cleanup succeeds. The shared cache removal is
            // idempotent: a first copy has no cached state yet, while an
            // in-process retry can still hold the previous ready runtime
            // schema. The fresh copy schema below is the only state allowed to
            // repopulate the cache.
            store.clear_table_copy_state(table_id).await?;
            shared_table_cache.remove_table(table_id).await;

            // We are ready to start copying table data, and we update the state
            // accordingly.
            info!(table_id = table_id.0, "starting data copy");
            {
                let mut inner = table_sync_worker_state.lock().await;
                inner.set_and_store(TableState::DataSync, &store).await?;
            }

            // Fail point to test when the table sync fails before copying data.
            #[cfg(feature = "failpoints")]
            etl_fail_point(START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP)?;

            // We create the slot with a transaction, since we need to have a consistent
            // snapshot of the database before copying the schema and tables.
            // The transaction uses USE_SNAPSHOT so all subsequent reads on this
            // connection see a consistent point. The transaction must stay open until
            // the copy is done; for parallel copy, pg_export_snapshot() is called within it
            // so child connections can share the same snapshot.
            //
            // If a slot already exists at this point, we could delete it and try to
            // recover, but it means that the state was somehow reset without
            // the slot being deleted, and we want to surface this.
            let (transaction, slot) =
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
            info!(table_id = table_id.0, "fetching table schema");
            let (table_schema, identity) =
                transaction.get_table_schema_with_identity(table_id).await?;

            if !table_schema.has_primary_keys() {
                bail!(
                    ErrorKind::SourceSchemaError,
                    "Primary key not found",
                    format!("Table '{}' has no primary key", table_schema.name)
                );
            }

            // We store the table schema in the schema store to be able to retrieve it even
            // when the pipeline is restarted, since it's outside the lifecycle
            // of the pipeline.
            let table_schema = store.store_table_schema(table_schema).await?;

            let replicated_column_names = transaction
                .get_replicated_column_names(table_id, &table_schema, &config.publication_name)
                .await?;
            let replication_mask =
                ReplicationMask::try_build(&table_schema, &replicated_column_names).map_err(
                    |err: SchemaError| {
                        etl_error!(
                            ErrorKind::InvalidState,
                            "Schema mismatch during table sync",
                            source: err
                        )
                    },
                )?;
            let identity_mask = identity.build_identity_mask(&table_schema, &replication_mask)?;
            let replicated_table_schema =
                ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);
            let arrow_table_schema = arrow_table_schema(&replicated_table_schema);

            let mut total_table_copy_rows = 0;
            let mut total_table_copy_duration_secs = 0.0;

            // We check if the table should be copied, or we can skip it.
            if config.table_sync_copy.should_copy_table(table_id.into_inner()) {
                let result = table_copy(
                    &transaction,
                    table_id,
                    Arc::clone(&arrow_table_schema),
                    replicated_table_schema.clone(),
                    Some(&config.publication_name),
                    config.max_copy_connections_per_table,
                    config.batch.clone(),
                    shutdown_rx.clone(),
                    destination.clone(),
                    memory_monitor.clone(),
                    batch_budget.clone(),
                )
                .await?;

                match result {
                    TableCopyResult::Completed { total_rows, total_duration_secs } => {
                        total_table_copy_rows = total_rows as usize;
                        total_table_copy_duration_secs = total_duration_secs;
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
            transaction.commit().await?;

            // If no table rows were written, we call the method nonetheless with no rows,
            // to kickstart table creation.
            if total_table_copy_rows == 0 {
                let snapshot_batch =
                    table_rows_to_arrow_batch(Arc::clone(&arrow_table_schema), &[])?
                        .with_replicated_table_schema(replicated_table_schema.clone());
                let (flush_result, pending_flush_result) = WriteSnapshotBatchResult::new(());
                destination.write_snapshot_batch(snapshot_batch, flush_result).await?;
                pending_flush_result.await.into_result()?;
                info!(table_id = table_id.0, "writing empty table rows for empty table");
            }

            // Record the table copy duration.
            let with_partitioning = config.max_copy_connections_per_table > 1;
            histogram!(
                ETL_TABLE_COPY_DURATION_SECONDS,
                PARTITIONING_LABEL => with_partitioning.to_string(),
            )
            .record(total_table_copy_duration_secs);

            info!(
                table_id = table_id.0,
                total_table_copy_rows, total_table_copy_duration_secs, "completed table copy"
            );

            // We mark that we finished the copy of the table schema and data.
            {
                let mut inner = table_sync_worker_state.lock().await;
                inner.set_and_store(TableState::FinishedCopy, &store).await?;
            }

            shared_table_cache.note_ready(table_id, replicated_table_schema).await;

            slot.consistent_point
        }
        _ => unreachable!("phase type already validated above"),
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

    // If we are told to shut down while waiting for a phase change, we will signal
    // this to the caller.
    if result.should_shutdown() {
        info!(table_id = table_id.0, "shutting down table sync while waiting for catchup");

        return Ok(TableSyncResult::Stopped);
    }

    info!(table_id = table_id.0, "table sync completed, starting streaming");

    Ok(TableSyncResult::Completed { start_lsn })
}
