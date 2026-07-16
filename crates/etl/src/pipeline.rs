//! Core pipeline orchestration and execution.
//!
//! Contains the main [`Pipeline`] struct that coordinates Postgres logical
//! replication with destination systems. Manages worker lifecycles, shutdown
//! coordination, and error handling.

use std::{sync::Arc, time::Duration};

use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

pub use crate::runtime::concurrency::ShutdownTx;
use crate::{
    bail,
    config::PipelineConfig,
    destination::PipelineDestination,
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::register_metrics,
    postgres::{OutOfBandSourcePool, client::PgReplicationClient, migrations},
    replication::{SharedTableCache, state::TableState},
    runtime::{
        ApplyWorker, ApplyWorkerHandle, MemoryMonitor, TableSyncWorkerPool,
        concurrency::create_shutdown_channel, prepare_apply_start_lsn,
    },
    store::PipelineStore,
};

/// Unique identifier for an ETL pipeline instance.
///
/// [`PipelineId`] provides a simple numeric identifier to distinguish between
/// multiple pipeline instances running concurrently. This ID is used for
/// logging, monitoring, and coordinating shutdown operations across pipeline
/// components.
pub type PipelineId = u64;

/// Internal state tracking for pipeline lifecycle.
///
/// Tracks whether the pipeline has been started and maintains handles to
/// running workers. The pipeline can only be in one of these states at a time.
#[derive(Debug)]
enum PipelineState {
    /// Pipeline has been created but not yet started.
    NotStarted,
    /// Pipeline is running with active workers.
    Started {
        /// Handle for the running apply worker.
        apply_worker: ApplyWorkerHandle,
        /// Pool that owns all table sync worker tasks.
        pool: Arc<TableSyncWorkerPool>,
        /// Background memory monitor used by workers.
        memory_monitor: MemoryMonitor,
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
#[derive(Debug)]
pub struct Pipeline<S, D> {
    config: Arc<PipelineConfig>,
    store: S,
    destination: D,
    state: PipelineState,
    shutdown_tx: ShutdownTx,
}

impl<S, D> Pipeline<S, D>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    /// Creates a new pipeline with the given configuration.
    ///
    /// The pipeline is initially in the not-started state and must be
    /// explicitly started using [`Pipeline::start`]. The store is
    /// used for tracking replication progress, table schemas, destination
    /// table metadata, and table lifecycle state, while the destination
    /// receives replicated data.
    /// The pipeline ID is extracted from the configuration, ensuring
    /// consistency between pipeline identity and configuration settings.
    pub fn new(config: PipelineConfig, store: S, destination: D) -> Self {
        // Register metrics here during pipeline creation to avoid burdening the
        // users of etl crate to explicitly calling it. Since this method is safe to
        // call multiple times, it is ok even if there are multiple pipelines created.
        register_metrics();

        // We create a watch channel of unit types since this is just used to notify all
        // subscribers that shutdown is needed.
        //
        // Here we are not taking the `shutdown_rx` since we will just extract it from
        // the `shutdown_tx` via the `subscribe` method. This is done to make
        // the code cleaner.
        let (shutdown_tx, _) = create_shutdown_channel();

        Self {
            config: Arc::new(config),
            store,
            destination,
            state: PipelineState::NotStarted,
            shutdown_tx,
        }
    }

    /// Returns the unique identifier for this pipeline.
    pub fn id(&self) -> PipelineId {
        self.config.id
    }

    /// Returns a handle for sending shutdown signals to this pipeline.
    ///
    /// Multiple components can hold shutdown handles to coordinate graceful
    /// termination. When shutdown is signaled, all workers will complete
    /// their current operations and terminate cleanly.
    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.shutdown_tx.clone()
    }

    /// Starts the pipeline and begins replication processing.
    ///
    /// This method initializes the connection to Postgres, loads destination
    /// table metadata and schemas, creates the worker pool for table
    /// synchronization, and starts the apply worker for processing replication
    /// stream events.
    pub async fn start(&mut self) -> EtlResult<()> {
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
                 helpers and DDL event trigger must be installed out-of-band"
            );
        }

        // We always start memory monitoring for running workers to keep total memory
        // snapshots available.
        let memory_monitor = MemoryMonitor::new(
            self.shutdown_tx.subscribe(),
            self.config.memory_backpressure.clone(),
            self.config.memory_refresh_interval_ms,
        );

        // We create the first connection to Postgres.
        let replication_client =
            PgReplicationClient::connect(self.config.pg_connection.clone()).await?;

        // We load the destination table metadata and schemas from the store to have
        // them cached for quick access.
        //
        // It's really important to load the metadata and schemas before starting the
        // apply worker since downstream code relies on the assumption that they
        // are loaded in the cache.
        self.store.load_destination_tables_metadata().await?;
        self.store.load_table_schemas().await?;

        // Establish the apply-slot lineage before reading publication membership.
        // Changes committed after this point are retained in WAL, so startup only
        // needs to add current members and must never infer removals from a later
        // catalog snapshot.
        let (_, apply_slot_created) = prepare_apply_start_lsn(
            self.config.id,
            &replication_client,
            &self.store,
            &self.config.invalidated_slot_behavior,
        )
        .await?;

        self.initialize_table_states(&replication_client, apply_slot_created).await?;

        // We then let destinations perform their startup sequence if any.
        self.destination.startup().await?;

        // We create the table sync workers pool to manage all table sync workers in a
        // central place.
        let pool = Arc::new(TableSyncWorkerPool::new());

        // We create the permits semaphore which is used to control how many table sync
        // workers can be running at the same time.
        let table_sync_worker_permits =
            Arc::new(Semaphore::new(self.config.max_table_sync_workers as usize));

        // We create the shared per-table protocol cache used by both the apply worker
        // and table sync workers. It tracks the latest schema snapshot and
        // replication mask for each table.
        let shared_table_cache = SharedTableCache::new();

        // We create a shared lazy pool for low-frequency, out-of-band source
        // database queries that should not use the replication connection.
        let out_of_band_source_pool = OutOfBandSourcePool::new(
            &self.config.pg_connection,
            Duration::from_millis(self.config.replication_lag_refresh_interval_ms),
        );

        // We create and start the apply worker.
        let apply_worker = ApplyWorker::new(
            self.config.id,
            Arc::clone(&self.config),
            Arc::clone(&pool),
            self.store.clone(),
            self.destination.clone(),
            shared_table_cache,
            out_of_band_source_pool,
            self.shutdown_tx.subscribe(),
            table_sync_worker_permits,
            memory_monitor.clone(),
        )
        .spawn()?;

        self.state = PipelineState::Started { apply_worker, pool, memory_monitor };

        Ok(())
    }

    /// Waits for the pipeline to complete all processing and terminate.
    ///
    /// This method blocks until both the apply worker and all table sync
    /// workers have finished their work. If the pipeline was never started,
    /// this returns immediately. If any workers encounter errors, those
    /// errors are collected and returned.
    ///
    /// The wait process ensures proper shutdown ordering:
    /// 1. Apply worker completes first (may spawn additional table sync
    ///    workers)
    /// 2. All table sync workers complete
    /// 3. Any errors from workers are aggregated and returned
    /// 4. Background pipeline tasks complete after shutdown
    pub async fn wait(self) -> EtlResult<()> {
        let PipelineState::Started { apply_worker, pool, memory_monitor } = self.state else {
            warn!("pipeline was not started, skipping wait");

            return Ok(());
        };

        let mut errors = vec![];

        // We first wait for the apply worker to finish, since that must be done before
        // waiting for the table sync workers to finish, otherwise if we wait
        // for sync workers first, we might be having the apply worker that
        // spawns new sync workers after we waited for the current
        // ones to finish.
        debug!("waiting for apply worker to complete");
        let apply_worker_result = apply_worker.wait().await;
        if let Err(err) = apply_worker_result {
            errors.push(err);

            // If we fail to send the shutdown signal, we are not going to capture the error
            // since it means that no table sync workers are running, which is
            // fine.
            let _ = self.shutdown_tx.shutdown();
        }

        // We wait for all table sync workers to finish.
        debug!("waiting for table sync workers to complete");
        let table_sync_workers_result = pool.wait_all().await;
        if let Err(err) = table_sync_workers_result {
            errors.push(err);
        }

        // Once all workers completed, we notify the destination of shutting down.
        debug!("waiting for destination shutdown to complete");
        if let Err(err) = self.destination.shutdown().await {
            warn!("destination shutdown failed, collecting errors");

            errors.push(err);
        }

        // As last thing, we want the memory refresh to be done, so that we can cleanly
        // terminate the process.
        debug!("waiting for memory monitor to complete");
        if let Err(err) = memory_monitor.wait_for_refresh_task().await {
            if err.is_cancelled() {
                warn!(error = %err, "memory monitor task was cancelled");
            } else {
                errors.push(etl_error!(
                    ErrorKind::InvalidState,
                    "Memory monitor task panicked",
                    source: err
                ));
            }
        }

        if !errors.is_empty() {
            return Err(errors.into());
        }

        Ok(())
    }

    /// Initiates graceful shutdown of the pipeline.
    ///
    /// Sends shutdown signals to all workers, instructing them to complete
    /// their current operations and terminate. This method returns
    /// immediately after sending the signals and does not wait for workers
    /// to actually stop.
    ///
    /// Use [`Pipeline::wait`] after calling this method to wait for complete
    /// shutdown.
    pub fn shutdown(&self) {
        info!("initiating pipeline shutdown");

        if let Err(err) = self.shutdown_tx.shutdown() {
            error!(error = %err, "failed to send shutdown signal");
            return;
        }

        info!("shutdown signal sent to all workers");
    }

    /// Initiates shutdown and waits for complete pipeline termination.
    ///
    /// This convenience method combines [`Pipeline::shutdown`] and
    /// [`Pipeline::wait`] to provide a single call that both initiates
    /// shutdown and waits for completion. Returns any errors encountered
    /// during the shutdown process.
    pub async fn shutdown_and_wait(self) -> EtlResult<()> {
        self.shutdown();
        self.wait().await
    }

    /// Initializes missing table states from the current publication.
    ///
    /// Ensures each table currently in the Postgres publication has a
    /// corresponding table state; tables without existing states are
    /// initialized to [`TableState::Init`].
    ///
    /// In reactive mode this additive reconciliation runs on every startup.
    /// Disabled mode captures membership only when a new apply slot establishes
    /// a fresh replication lineage. Removals are never inferred from a startup
    /// catalog snapshot because only ordered WAL messages provide the
    /// durability boundary needed to delete ETL-owned state safely.
    async fn initialize_table_states(
        &self,
        replication_client: &PgReplicationClient,
        apply_slot_created: bool,
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

        if !apply_slot_created && !self.config.publication_changes_mode.is_reactive() {
            return Ok(());
        }

        let publication_table_ids =
            replication_client.get_publication_table_ids(&self.config.publication_name).await?;

        info!(
            publication_name = %self.config.publication_name,
            table_count = publication_table_ids.len(),
            "publication tables loaded"
        );

        // We load the current table states.
        self.store.load_table_states().await?;
        let table_states = self.store.get_table_states().await?;

        // Initialize states for newly added tables in the publication.
        for table_id in &publication_table_ids {
            if !table_states.contains_key(table_id) {
                self.store.update_table_state(*table_id, TableState::Init).await?;
            }
        }

        Ok(())
    }
}
