use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::EtlReplicationSlot;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, error, info};

use crate::bail;
use crate::concurrency::shutdown::ShutdownRx;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::replication::apply::{ApplyLoop, ApplyWorkerContext, WorkerContext};
use crate::replication::client::{GetOrCreateSlotResult, PgReplicationClient};
use crate::replication::masks::ReplicationMasks;
use crate::state::table::TableReplicationPhaseType;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::pool::TableSyncWorkerPool;

/// Handle for monitoring and controlling the apply worker.
///
/// [`ApplyWorkerHandle`] provides control over the apply worker that processes
/// replication stream events and coordinates with table sync workers. The handle
/// enables waiting for worker completion and checking final results.
#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<EtlResult<()>>>,
}

impl ApplyWorkerHandle {
    /// Waits for the apply worker to complete execution.
    ///
    /// This method blocks until the apply worker finishes processing, either
    /// due to successful completion, shutdown signal, or error. It properly
    /// handles panics that might occur within the worker task.
    pub async fn wait(mut self) -> EtlResult<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await.map_err(|err| {
            if err.is_cancelled() {
                etl_error!(
                    ErrorKind::ApplyWorkerCancelled,
                    "Apply worker was cancelled",
                    err
                )
            } else {
                etl_error!(ErrorKind::ApplyWorkerPanic, "Apply worker panicked", err)
            }
        })??;

        Ok(())
    }
}

/// Worker that applies replication stream events to destinations.
///
/// [`ApplyWorker`] is the core worker responsible for processing Postgres logical
/// replication events and applying them to the configured destination. It coordinates
/// with table sync workers during initial synchronization and handles the continuous
/// replication stream during normal operation.
///
/// The worker manages transaction boundaries, coordinates table synchronization,
/// and ensures data consistency throughout the replication process.
#[derive(Debug)]
pub struct ApplyWorker<S, D> {
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    pool: Arc<TableSyncWorkerPool>,
    store: S,
    destination: D,
    replication_masks: ReplicationMasks,
    shutdown_rx: ShutdownRx,
    table_sync_worker_permits: Arc<Semaphore>,
}

impl<S, D> ApplyWorker<S, D> {
    /// Creates a new apply worker with the given configuration and dependencies.
    ///
    /// The worker will use the provided replication client to read the Postgres
    /// replication stream and coordinate with the table sync worker pool for
    /// initial synchronization operations.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: Arc<TableSyncWorkerPool>,
        store: S,
        destination: D,
        replication_masks: ReplicationMasks,
        shutdown_rx: ShutdownRx,
        table_sync_worker_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            replication_client,
            pool,
            store,
            destination,
            replication_masks,
            shutdown_rx,
            table_sync_worker_permits,
        }
    }
}

impl<S, D> ApplyWorker<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Spawns the apply worker and returns a handle for monitoring.
    ///
    /// This method initializes the apply worker by determining the starting LSN,
    /// creating coordination signals, and launching the main apply loop. The worker
    /// runs asynchronously and can be monitored through the returned handle.
    pub async fn spawn(self) -> EtlResult<ApplyWorkerHandle> {
        info!("starting apply worker");

        let apply_worker_span = tracing::info_span!(
            "apply_worker",
            pipeline_id = self.pipeline_id,
            publication_name = self.config.publication_name
        );
        let apply_worker = async move {
            let start_lsn =
                get_start_lsn(self.pipeline_id, &self.replication_client, &self.store).await?;

            let worker_context = WorkerContext::Apply(ApplyWorkerContext {
                pipeline_id: self.pipeline_id,
                config: self.config.clone(),
                pool: self.pool,
                store: self.store.clone(),
                destination: self.destination.clone(),
                replication_masks: self.replication_masks.clone(),
                shutdown_rx: self.shutdown_rx.clone(),
                table_sync_worker_permits: self.table_sync_worker_permits,
            });

            let result = ApplyLoop::start(
                self.pipeline_id,
                start_lsn,
                self.config,
                self.replication_client,
                self.store,
                self.destination,
                self.replication_masks,
                worker_context,
                self.shutdown_rx,
            )
            .await;

            match result {
                Ok(_) => {
                    info!("apply worker completed successfully");
                    Ok(())
                }
                Err(err) => {
                    // We log the error here, this way it's logged even if the worker is not awaited.
                    error!(error = %err, "apply worker failed");
                    Err(err)
                }
            }
        }
        .instrument(apply_worker_span.or_current());

        let handle = tokio::spawn(apply_worker);

        Ok(ApplyWorkerHandle {
            handle: Some(handle),
        })
    }
}

/// Determines the LSN position from which the apply worker should start reading the replication stream.
///
/// This function implements critical replication consistency logic by managing the apply worker's
/// replication slot. The slot serves as a persistent marker in Postgres's WAL (Write-Ahead Log)
/// that tracks the apply worker's progress and prevents WAL deletion of unreplicated data.
///
/// When creating a new slot, this function validates that all tables are in the Init state.
/// If any table is not in Init state when creating a new slot, it indicates that data was
/// synchronized based on a different apply worker lineage, which would break replication
/// correctness.
async fn get_start_lsn<S: StateStore>(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    store: &S,
) -> EtlResult<PgLsn> {
    let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into()?;

    // We try to get or create the slot. Both operations will return an LSN that we can use to start
    // streaming events.
    let slot = replication_client.get_or_create_slot(&slot_name).await?;

    // When creating a new apply worker slot, all tables must be in the `Init` state. If any table
    // is not in Init state, it means the table was synchronized based on another apply worker
    // lineage (different slot) which will break correctness.
    if let GetOrCreateSlotResult::CreateSlot(_) = &slot
        && let Err(err) = validate_tables_in_init_state(store).await
    {
        // Delete the slot before failing, otherwise the system will restart and skip validation
        // since the slot will already exist.
        replication_client.delete_slot(&slot_name).await?;

        return Err(err);
    }

    // We return the LSN from which we will start streaming events.
    Ok(slot.get_start_lsn())
}

/// Validates that all tables are in the Init state.
///
/// This validation is required when creating a new apply worker slot to ensure replication
/// correctness. If any table has progressed beyond Init state, it indicates the table was
/// synchronized based on a different apply worker lineage.
async fn validate_tables_in_init_state<S: StateStore>(store: &S) -> EtlResult<()> {
    let table_states = store.get_table_replication_states().await?;

    let non_init_tables: Vec<_> = table_states
        .iter()
        .filter(|(_, phase)| phase.as_type() != TableReplicationPhaseType::Init)
        .map(|(table_id, phase)| (*table_id, phase.as_type()))
        .collect();

    if non_init_tables.is_empty() {
        return Ok(());
    }

    let table_details: Vec<String> = non_init_tables
        .iter()
        .map(|(id, phase)| format!("table {id} in state {phase}"))
        .collect();

    bail!(
        ErrorKind::InvalidState,
        "Cannot create apply worker slot when tables are not in Init state",
        format!(
            "Creating a new apply worker replication slot requires all tables to be in Init state, \
            but found {} table(s) in non-Init states: {}. This indicates that tables were \
            synchronized based on a different apply worker lineage. To fix this, either restore \
            the original apply worker slot or reset all tables to Init state.",
            non_init_tables.len(),
            table_details.join(", ")
        )
    );
}
