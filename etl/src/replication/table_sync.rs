use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::EtlReplicationSlot;
use etl_postgres::types::{ReplicatedTableSchema, ReplicationMask, SchemaError, TableId};
use metrics::histogram;
use std::sync::Arc;
use tokio_postgres::types::PgLsn;
use tracing::{info, warn};

use crate::{bail, etl_error};
use crate::concurrency::batch_budget::BatchBudgetController;
use crate::concurrency::memory_monitor::MemoryMonitor;
use crate::concurrency::shutdown::ShutdownRx;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlResult};
#[cfg(feature = "failpoints")]
use crate::failpoints::{
    START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP, etl_fail_point,
};
use crate::metrics::{ETL_TABLE_COPY_DURATION_SECONDS, PARTITIONING_LABEL, PIPELINE_ID_LABEL};
use crate::replication::client::PgReplicationClient;
use crate::replication::masks::ReplicationMasks;
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::table_sync::TableSyncWorkerState;
use crate::workers::table_sync_copy::{TableCopyResult, table_copy};

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
    replication_masks: &ReplicationMasks,
    shutdown_rx: ShutdownRx,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
) -> EtlResult<TableSyncResult>
where
    S: StateStore + SchemaStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
{
    info!(table_id = table_id.0, "starting initial table sync");

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
            TableReplicationPhaseType::SyncDone
                | TableReplicationPhaseType::Ready
                | TableReplicationPhaseType::Errored
        ) {
            warn!(table_id = table_id.0, %phase_type, "initial table sync not required");

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
            warn!(table_id = table_id.0, %phase_type, "invalid replication phase for table sync");

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

    let slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into()?;

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
            // When we are in these states, it could be for the following reasons:
            // - `Init` -> we can be in this state because we just started replicating the table or the state
            //  was reset. In this case we don't want to make assumptions about the previous state, so we
            //  just try to delete the slot and truncate the table.
            // - `DataSync` -> we can be in this state because we failed during data sync, meaning that table
            //  copy failed. In this case, we want to delete the slot and truncate the table.
            //
            // We try to delete the slot also during `Init` because we support state rollback and a
            // slot might be there from the previous run.
            replication_client.delete_slot_if_exists(&slot_name).await?;

            // We must truncate the destination table before starting a copy to avoid data inconsistencies.
            //
            // Example scenario:
            // 1. The source table has a single row (id = 1) that is copied to the destination during the initial copy.
            // 2. Before the table’s phase is set to `FinishedCopy`, the process crashes.
            // 3. While down, the source deletes row id = 1 and inserts row id = 2.
            // 4. When restarted, the process sees the table in the ` DataSync ` state, deletes the slot, and copies again.
            // 5. This time, only row id = 2 is copied, but row id = 1 still exists in the destination.
            // Result: the destination has two rows (id = 1 and id = 2) instead of only one (id = 2).
            // Fix: Always truncate the destination table before starting a copy.
            //
            // Try to load the previously stored destination table metadata, which contains
            // both the snapshot_id and replication_mask. If available, we can load the
            // corresponding table schema and truncate the destination table before starting a copy.
            // If the metadata is not present, we can safely assume that no data is there in the
            // table; thus a truncate won't be issued.
            if let Some(current_metadata) = store.get_destination_table_metadata(table_id).await? {
                if let Some(table_schema) = store
                    .get_table_schema(&table_id, current_metadata.snapshot_id)
                    .await?
                {
                    let replicated_table_schema = ReplicatedTableSchema::from_mask(
                        table_schema,
                        current_metadata.replication_mask,
                    );
                    destination.truncate_table(&replicated_table_schema).await?;
                    
                    info!(%table_id, "truncated destination table before starting copy");
                } else {
                    bail!(
                        ErrorKind::InvalidState,
                        "Destination table metadata found, but not corresponding table schema exists"
                    );
                }
            }

            // We are ready to start copying table data, and we update the state accordingly.
            info!(table_id = table_id.0, "starting data copy");
            {
                let mut inner = table_sync_worker_state.lock().await;
                inner
                    .set_and_store(TableReplicationPhase::DataSync, &store)
                    .await?;
            }

            // Fail point to test when the table sync fails before copying data.
            #[cfg(feature = "failpoints")]
            etl_fail_point(START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP)?;

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
            info!(%table_id, "fetching table schema");
            let table_schema = transaction.get_table_schema(table_id).await?;

            if !table_schema.has_primary_keys() {
                bail!(
                    ErrorKind::SourceSchemaError,
                    "Primary key not found",
                    format!("Table '{}' has no primary key", table_schema.name)
                );
            }

            // We store the table schema in the schema store to be able to retrieve it even when the
            // pipeline is restarted, since it's outside the lifecycle of the pipeline.
            let table_schema = store.store_table_schema(table_schema).await?;

            // Get the names of columns being replicated based on the publication's column filter.
            // This must be done in the same transaction as `get_table_schema` for consistency.
            let replicated_column_names = transaction
                .get_replicated_column_names(table_id, &table_schema, &config.publication_name)
                .await?;

            // Build and store the replication mask for use during CDC.
            // We use `try_build` here because the schema was just loaded and should match
            // the publication's column filter. Any mismatch indicates a schema inconsistency.
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
            replication_masks
                .set(table_id, replication_mask.clone())
                .await;

            // Create the replicated table schema with the replication mask.
            let replicated_table_schema =
                ReplicatedTableSchema::from_mask(table_schema, replication_mask);

            let mut total_table_copy_rows = 0;
            let mut total_table_copy_duration_secs = 0.0;

            // We check if the table should be copied, or we can skip it.
            if config
                .table_sync_copy
                .should_copy_table(table_id.into_inner())
            {
                let result = table_copy(
                    &transaction,
                    table_id,
                    replicated_table_schema.clone(),
                    Some(&config.publication_name),
                    config.max_copy_connections_per_table,
                    config.batch.clone(),
                    shutdown_rx.clone(),
                    pipeline_id,
                    destination.clone(),
                    memory_monitor.clone(),
                    batch_budget.clone(),
                )
                .await?;

                match result {
                    TableCopyResult::Completed {
                        total_rows,
                        total_duration_secs,
                    } => {
                        total_table_copy_rows = total_rows as usize;
                        total_table_copy_duration_secs = total_duration_secs;
                    }
                    TableCopyResult::Shutdown => {
                        info!(
                            table_id = table_id.0,
                            "table copy interrupted by shutdown, terminating table sync"
                        );
                        return Ok(TableSyncResult::SyncStopped);
                    }
                }
            }

            // We commit the transaction before starting the apply loop, otherwise it will fail
            // since no transactions can be running while replication is started.
            transaction.commit().await?;

            // If no table rows were written, we call the method nonetheless with no rows, to kickstart
            // table creation.
            if total_table_copy_rows == 0 {
                destination
                    .write_table_rows(&replicated_table_schema, vec![])
                    .await?;
                info!(
                    table_id = table_id.0,
                    "writing empty table rows for empty table"
                );
            }

            // Record the table copy duration.
            let with_partitioning = config.max_copy_connections_per_table > 1;
            histogram!(
                ETL_TABLE_COPY_DURATION_SECONDS,
                PIPELINE_ID_LABEL => pipeline_id.to_string(),
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
                inner
                    .set_and_store(TableReplicationPhase::FinishedCopy, &store)
                    .await?;
            }

            slot.consistent_point
        }
        TableReplicationPhaseType::FinishedCopy => {
            let slot = replication_client.get_slot(&slot_name).await?;
            info!(table_id = table_id.0, confirmed_flush_lsn = %slot.confirmed_flush_lsn, "resuming table sync");

            slot.confirmed_flush_lsn
        }
        _ => unreachable!("phase type already validated above"),
    };

    // We mark this worker as `SyncWait` (in memory only) to signal the apply worker that we are
    // ready to start catchup. We pass the snapshot LSN so the apply worker can use
    // max(snapshot_lsn, current_lsn) when setting the Catchup LSN.
    {
        let mut inner = table_sync_worker_state.lock().await;
        inner
            .set_and_store(TableReplicationPhase::SyncWait { lsn: start_lsn }, &store)
            .await?;
    }

    // We also wait to be signaled to catch up with the main apply worker up to a specific lsn.
    let result = table_sync_worker_state
        .wait_for_phase_type(&[TableReplicationPhaseType::Catchup], shutdown_rx.clone())
        .await;

    // If we are told to shut down while waiting for a phase change, we will signal this to
    // the caller.
    if result.should_shutdown() {
        info!(
            table_id = table_id.0,
            "shutting down table sync while waiting for catchup"
        );

        return Ok(TableSyncResult::SyncStopped);
    }

    info!(
        table_id = table_id.0,
        "table sync completed, starting streaming"
    );

    Ok(TableSyncResult::SyncCompleted { start_lsn })
}
