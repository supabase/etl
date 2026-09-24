//! Core pipeline orchestration and execution.
//!
//! Contains the main [`Pipeline`] struct that coordinates Postgres logical
//! replication with destination systems. Manages worker lifecycles, shutdown
//! coordination, and error handling.

use std::{collections::HashSet, sync::Arc, time::Duration};

use etl_config::shared::validate_table_error_retry_delay_ms;
use etl_postgres::slots::EtlReplicationSlot;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::{debug, info, warn};

use crate::{
    bail,
    config::PipelineConfig,
    destination::PipelineDestination,
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::register_metrics,
    postgres::{OutOfBandSourcePool, client::PgReplicationClient, migrations},
    replication::state::TableState,
    runtime::{ApplyWorker, ApplyWorkerHandle, MemoryMonitor, TableSyncWorkerPool},
    schema::TableId,
    store::PipelineStore,
    task::abort_and_join,
};

/// Unique identifier for an ETL pipeline instance.
///
/// [`PipelineId`] provides a simple numeric identifier to distinguish between
/// multiple pipeline instances running concurrently. This ID is used for
/// logging, monitoring, and coordinating shutdown operations across pipeline
/// components.
pub type PipelineId = u64;

/// Task ownership transferred together so a failed or cancelled wait drops
/// every remaining pipeline-owned handle.
#[derive(Debug)]
struct PipelineTasks {
    /// Handle for the running apply worker.
    apply_worker: ApplyWorkerHandle,
    /// Pool that owns all table sync worker tasks.
    pool: Arc<TableSyncWorkerPool>,
    /// Sampler owned independently of the readings shared with workers.
    memory_monitor_task: AbortOnDropHandle<()>,
}

/// Internal state tracking for pipeline lifecycle.
///
/// Tracks whether the pipeline has been started and maintains handles to
/// running workers. The pipeline can only be in one of these states at a time.
#[derive(Debug)]
enum PipelineState {
    /// Pipeline has been created but not yet started.
    NotStarted,
    /// Pipeline has started; its tasks may have moved to the completion waiter.
    Started {
        /// Resources transferred to the first completion waiter.
        tasks: Mutex<Option<PipelineTasks>>,
    },
}

/// Core ETL pipeline that orchestrates Postgres logical replication.
///
/// A [`Pipeline`] represents a complete ETL workflow connecting a Postgres
/// publication to a destination. It manages source preparation, initial table
/// copies, streaming replication, worker coordination, and graceful shutdown.
///
/// The pipeline operates in two main phases:
/// 1. **Initial table synchronization** - Copies existing data from source
///    tables
/// 2. **Continuous replication** - Streams ongoing changes from the replication
///    log
///
/// Multiple table sync workers run in parallel during the initial stage, while
/// a single apply worker processes replication streams for tables that were
/// already copied.
///
/// Dropping the pipeline aborts its workers and background monitors.
/// Use [`Pipeline::shutdown_and_wait`] to drain writes and finish destination
/// cleanup before returning.
#[derive(Debug)]
pub struct Pipeline<S, D> {
    config: Arc<PipelineConfig>,
    store: S,
    destination: D,
    state: PipelineState,
    shutdown_token: CancellationToken,
}

impl<S, D> Pipeline<S, D>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    /// Creates a new pipeline with the given configuration.
    ///
    /// The pipeline is initially in the not-started state and must be
    /// explicitly started using [`Pipeline::start`]. The store tracks persisted
    /// replication checkpoints, table schemas, destination table metadata, and
    /// table lifecycle state, while the destination receives replicated data.
    /// The pipeline ID is extracted from the configuration, ensuring
    /// consistency between pipeline identity and configuration settings.
    pub fn new(config: PipelineConfig, store: S, destination: D) -> Self {
        // Register metrics here during pipeline creation to avoid burdening the
        // users of etl crate to explicitly calling it. Since this method is
        // safe to call multiple times, it is ok even if there are multiple
        // pipelines created.
        register_metrics();

        Self {
            config: Arc::new(config),
            store,
            destination,
            state: PipelineState::NotStarted,
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Returns the unique identifier for this pipeline.
    pub fn id(&self) -> PipelineId {
        self.config.id
    }

    /// Starts the pipeline and begins replication processing.
    ///
    /// This method initializes the connection to Postgres, prepares every
    /// store cache, creates the worker pool for table
    /// synchronization, and starts the apply worker for processing replication
    /// stream events.
    ///
    /// An unsupported retry delay returns [`ErrorKind::ConfigError`] before
    /// any startup work. After this method succeeds, subsequent calls return
    /// [`ErrorKind::InvalidState`] without performing any startup work.
    pub async fn start(&mut self) -> EtlResult<()> {
        if !matches!(&self.state, PipelineState::NotStarted) {
            bail!(ErrorKind::InvalidState, "Pipeline has already been started");
        }

        validate_table_error_retry_delay_ms(self.config.table_error_retry_delay_ms).map_err(
            |err| etl_error!(ErrorKind::ConfigError, "Invalid table error retry delay", source: err),
        )?;

        info!(
            publication_name = %self.config.publication_name,
            pipeline_id = %self.config.id,
            "starting pipeline"
        );

        // Source migrations install schema helper functions and DDL event
        // triggers used by all stores. Creating the `ddl_command_end` event
        // trigger requires superuser, so this is gated: a de-elevated role can
        // disable it and have an admin install the source objects out-of-band.
        if self.config.run_source_migrations {
            migrations::run_source_migrations(&self.config.pg_connection).await?;
        } else {
            warn!(
                "skipping source migrations (run_source_migrations = false); the source schema \
                 helpers and ddl event trigger must be installed out-of-band"
            );
        }

        // We create the first connection to Postgres.
        let replication_client =
            PgReplicationClient::connect(self.config.pg_connection.clone()).await?;

        // Warm every store cache before initialization reads, destinations, or
        // workers.
        self.store.load_cache().await?;

        // Reconcile the cached table states with current publication
        // membership.
        self.initialize_table_states(&replication_client).await?;

        // We then let destinations perform their startup sequence if any.
        self.destination.startup().await?;

        // We create the table sync workers pool to manage all table sync
        // workers in a central place.
        let pool = Arc::new(TableSyncWorkerPool::new());

        // We create the permits semaphore which is used to control how many
        // table sync workers can be running at the same time.
        let table_sync_worker_permits =
            Arc::new(Semaphore::new(self.config.max_table_sync_workers as usize));

        // We create a shared lazy pool for low-frequency, out-of-band source
        // database queries that should not use the replication connection.
        let out_of_band_source_pool = OutOfBandSourcePool::new(
            &self.config.pg_connection,
            Duration::from_millis(self.config.table_sync_monitor_refresh_interval_ms),
        );

        // Start memory monitoring only after fallible startup work completes.
        // From this point onward, the monitor is owned by the started pipeline
        // and Pipeline::wait joins its refresh task after shutdown.
        let (memory_monitor, memory_monitor_task) = MemoryMonitor::spawn(
            self.config.memory_backpressure.clone(),
            self.config.memory_refresh_interval_ms,
        );

        // We create and start the apply worker.
        let apply_worker = ApplyWorker::new(
            self.config.id,
            Arc::clone(&self.config),
            Arc::clone(&pool),
            self.store.clone(),
            self.destination.clone(),
            out_of_band_source_pool,
            self.shutdown_token.clone(),
            table_sync_worker_permits,
            memory_monitor,
        )
        .spawn();

        self.state = PipelineState::Started {
            tasks: Mutex::new(Some(PipelineTasks { apply_worker, pool, memory_monitor_task })),
        };

        Ok(())
    }

    /// Waits for the pipeline to complete all processing and terminate.
    ///
    /// If the pipeline was never started, this returns immediately. After
    /// startup, only one waiter can take ownership of its tasks. This method
    /// borrows the pipeline so its owner can request shutdown while waiting.
    ///
    /// Joins apply and table sync workers, shuts down the destination, then
    /// stops memory sampling. Requested aborts are silently accepted; panics,
    /// task errors, and unexpected cancellations return immediately. On failure
    /// or cancellation of this future, remaining owned handles request abort
    /// on drop without awaiting further cleanup. Destination-owned resources,
    /// including ownership cycles, may still require explicit teardown.
    pub async fn wait(&self) -> EtlResult<()> {
        let PipelineState::Started { tasks } = &self.state else {
            warn!("pipeline was not started, skipping wait");
            return Ok(());
        };
        let Some(PipelineTasks { apply_worker, pool, memory_monitor_task }) =
            tasks.lock().await.take()
        else {
            bail!(ErrorKind::InvalidState, "Pipeline wait has already been called");
        };
        // Cancellation must reach workers even when an error or a dropped
        // waiter skips the successful teardown sequence.
        let _shutdown_guard = self.shutdown_token.clone().drop_guard();

        // The apply worker may spawn table sync workers until it exits.
        debug!("waiting for apply worker to complete");
        apply_worker.wait().await?;
        self.shutdown();

        debug!("waiting for table sync workers to complete");
        pool.wait_all().await?;

        debug!("waiting for destination shutdown to complete");
        self.destination.shutdown().await?;

        // Consumers need fresh readings until workers and destination drain.
        debug!("waiting for memory monitor to complete");
        abort_and_join(memory_monitor_task).await?;

        Ok(())
    }

    /// Initiates graceful shutdown of the pipeline.
    ///
    /// Cancels the shared shutdown token. Apply loops stop new intake and
    /// drain pending destination writes; initial sync workers interrupt their
    /// copy waits and stop. The request is retained even if no workers have
    /// started yet. Repeated requests are harmless.
    ///
    /// This method returns immediately without waiting for workers to stop.
    ///
    /// Use [`Pipeline::wait`] after calling this method to wait for complete
    /// shutdown.
    pub fn shutdown(&self) {
        info!("initiating pipeline shutdown");

        self.shutdown_token.cancel();

        info!("shutdown signal sent to all workers");
    }

    /// Initiates shutdown and waits for complete pipeline termination.
    ///
    /// This convenience method combines [`Pipeline::shutdown`] and
    /// [`Pipeline::wait`] to provide a single call that both initiates
    /// shutdown and waits for completion. Returns any errors encountered
    /// during the shutdown process. If startup failed or was cancelled before
    /// workers were spawned, still shuts down the constructed destination.
    pub async fn shutdown_and_wait(self) -> EtlResult<()> {
        self.shutdown();
        if matches!(self.state, PipelineState::NotStarted) {
            return self.destination.shutdown().await;
        }
        self.wait().await
    }

    /// Initializes table states for tables in the publication and purges state
    /// for tables removed from it.
    ///
    /// Ensures each table currently in the Postgres publication has a
    /// corresponding table state; tables without existing states are
    /// initialized to [`TableState::Init`].
    ///
    /// Also detects tables for which we have stored state but are no longer
    /// part of the publication, deletes their stored state (replication state,
    /// destination table metadata, table schemas, and durable table-sync
    /// progress), and performs best-effort cleanup of their table sync
    /// replication slots without touching the actual destination tables.
    async fn initialize_table_states(
        &self,
        replication_client: &PgReplicationClient,
    ) -> EtlResult<()> {
        // We need to make sure that the publication exists.
        if !replication_client.publication_exists(&self.config.publication_name).await? {
            bail!(
                ErrorKind::ConfigError,
                "Missing publication",
                format!(
                    "The publication '{}' does not exist in the database",
                    self.config.publication_name
                )
            );
        }

        let publication_table_ids =
            replication_client.get_publication_table_ids(&self.config.publication_name).await?;

        info!(
            publication_name = %self.config.publication_name,
            table_count = publication_table_ids.len(),
            "publication tables loaded"
        );

        let table_states = self.store.get_table_states().await?;

        // Initialize states for newly added tables in the publication.
        for table_id in &publication_table_ids {
            if !table_states.contains_key(table_id) {
                self.store.update_table_state(*table_id, TableState::Init).await?;
            }
        }

        // Detect and purge tables that have been removed from the publication.
        //
        // The purging doesn't delete any data in the destination, it just
        // removes internal state for that table.
        let publication_set: HashSet<TableId> = publication_table_ids.iter().copied().collect();
        for &table_id in table_states.keys() {
            if !publication_set.contains(&table_id) {
                info!(
                    table_id = table_id.0,
                    "table removed from publication, purging stored state and slot"
                );

                // We delete all table state before removing the slot, so that
                // we don't incur in the case where we have a slot tied to an
                // invalid state.
                self.store.delete_table_state(table_id).await?;

                // We try to delete the replication slot.
                let slot_name: String =
                    EtlReplicationSlot::for_table_sync_worker(self.config.id, table_id)
                        .try_into()?;
                replication_client.delete_slot_if_exists(&slot_name).await?;

                info!(
                    table_id = table_id.0,
                    slot_name,
                    "purged stored state and table sync slot for table removed from publication"
                );
            }
        }

        Ok(())
    }
}
