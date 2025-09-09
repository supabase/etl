//! Core pipeline orchestration and execution.
//!
//! Contains the main [`Pipeline`] struct that coordinates Postgres logical replication
//! with destination systems. Manages worker lifecycles, shutdown coordination, and error handling.

use crate::bail;
use crate::concurrency::shutdown::{ShutdownTx, create_shutdown_channel};
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlResult};
use crate::metrics::register_metrics;
use crate::replication::client::PgReplicationClient;
use crate::state::table::TableReplicationPhase;
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::apply::{ApplyWorker, ApplyWorkerHandle};
use crate::workers::base::{Worker, WorkerHandle};
use crate::workers::pool::TableSyncWorkerPool;
use etl_config::shared::PipelineConfig;
use etl_postgres::types::TableId;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

/// Internal state tracking for pipeline lifecycle.
///
/// Tracks whether the pipeline has been started and maintains handles to running workers.
/// The pipeline can only be in one of these states at a time.
#[derive(Debug)]
enum PipelineState {
    /// Pipeline has been created but not yet started.
    NotStarted,
    /// Pipeline is running with active workers.
    Started {
        // TODO: investigate whether we could benefit from a central launcher that deals at a high-level
        //  with workers management, which should not be done in the pipeline.
        apply_worker: ApplyWorkerHandle,
        pool: TableSyncWorkerPool,
    },
}

/// Core ETL pipeline that orchestrates Postgres logical replication.
///
/// A [`Pipeline`] represents a complete ETL workflow connecting a Postgres publication
/// to a destination through configurable transformations. It manages the replication
/// stream, coordinates worker processes, and handles failures gracefully.
///
/// The pipeline operates in two main phases:
/// 1. **Initial table synchronization** - Copies existing data from source tables
/// 2. **Continuous replication** - Streams ongoing changes from the replication log
///
/// Multiple table sync workers run in parallel during the initial phase, while a single
/// apply worker processes the replication stream of table that were already copied.
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
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Creates a new pipeline with the given configuration.
    ///
    /// The pipeline is initially in the not-started state and must be
    /// explicitly started using [`Pipeline::start`]. The state store is used for tracking
    /// replication progress, table schemas, and table mappings, while the destination receives replicated data.
    /// The pipeline ID is extracted from the configuration, ensuring consistency between
    /// pipeline identity and configuration settings.
    pub fn new(config: PipelineConfig, state_store: S, destination: D) -> Self {
        // Register metrics here during pipeline creation to avoid burdening the
        // users of etl crate to explicity calling it. Since this method is safe to
        // call mutltiple time, it is ok even if there are multiple pipelines created.
        register_metrics();
        // We create a watch channel of unit types since this is just used to notify all subscribers
        // that shutdown is needed.
        //
        // Here we are not taking the `shutdown_rx` since we will just extract it from the `shutdown_tx`
        // via the `subscribe` method. This is done to make the code cleaner.
        let (shutdown_tx, _) = create_shutdown_channel();

        Self {
            config: Arc::new(config),
            store: state_store,
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
    /// Multiple components can hold shutdown handles to coordinate graceful termination.
    /// When shutdown is signaled, all workers will complete their current operations
    /// and terminate cleanly.
    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.shutdown_tx.clone()
    }

    /// Starts the pipeline and begins replication processing.
    ///
    /// This method initializes the connection to Postgres, sets up table mappings and schemas,
    /// creates the worker pool for table synchronization, and starts the apply worker for
    /// processing replication stream events.
    pub async fn start(&mut self) -> EtlResult<()> {
        info!(
            "starting pipeline for publication '{}' with id {}",
            self.config.publication_name, self.config.id
        );

        // We create the first connection to Postgres.
        let replication_client =
            PgReplicationClient::connect(self.config.pg_connection.clone()).await?;

        // We load the table mappings and schemas from the store to have them cached for quick
        // access.
        //
        // It's really important to load the mappings and schemas before starting the apply worker
        // since downstream code relies on the assumption that the mappings and schemas are loaded
        // in the cache.
        self.store.load_table_mappings().await?;
        self.store.load_table_schemas().await?;

        // We load the table states by checking the table ids of a publication and loading/creating
        // the table replication states based on the current state.
        self.initialize_table_states(&replication_client).await?;

        // We create the table sync workers pool to manage all table sync workers in a central place.
        let pool = TableSyncWorkerPool::new();

        // We create the permits semaphore which is used to control how many table sync workers can
        // be running at the same time.
        let table_sync_worker_permits =
            Arc::new(Semaphore::new(self.config.max_table_sync_workers as usize));

        // We create and start the apply worker (temporarily leaving out retries_orchestrator)
        // TODO: Remove retries_orchestrator from ApplyWorker constructor
        let apply_worker = ApplyWorker::new(
            self.config.id,
            self.config.clone(),
            replication_client,
            pool.clone(),
            self.store.clone(),
            self.destination.clone(),
            self.shutdown_tx.subscribe(),
            table_sync_worker_permits,
        )
        .start()
        .await?;

        self.state = PipelineState::Started { apply_worker, pool };

        Ok(())
    }

    /// Waits for the pipeline to complete all processing and terminate.
    ///
    /// This method blocks until both the apply worker and all table sync workers have
    /// finished their work. If the pipeline was never started, this returns immediately.
    /// If any workers encounter errors, those errors are collected and returned.
    ///
    /// The wait process ensures proper shutdown ordering:
    /// 1. Apply worker completes first (may spawn additional table sync workers)
    /// 2. All table sync workers complete
    /// 3. Any errors from workers are aggregated and returned
    pub async fn wait(self) -> EtlResult<()> {
        let PipelineState::Started { apply_worker, pool } = self.state else {
            info!("pipeline was not started, nothing to wait for");

            return Ok(());
        };

        info!("waiting for apply worker to complete");

        let mut errors = vec![];

        // We first wait for the apply worker to finish, since that must be done before waiting for
        // the table sync workers to finish, otherwise if we wait for sync workers first, we might
        // be having the apply worker that spawns new sync workers after we waited for the current
        // ones to finish.
        let apply_worker_result = apply_worker.wait().await;
        if let Err(err) = apply_worker_result {
            errors.push(err);

            // TODO: in the future we might build a system based on the `ReactiveFuture` that
            //  automatically sends a shutdown signal to table sync workers on apply worker failure.
            // If there was an error in the apply worker, we want to shut down all table sync
            // workers, since without an apply worker they are lost.
            //
            // If we fail to send the shutdown signal, we are not going to capture the error since
            // it means that no table sync workers are running, which is fine.
            let _ = self.shutdown_tx.shutdown();

            info!("apply worker completed with an error, shutting down table sync workers");
        }

        info!("waiting for table sync workers to complete");

        // We wait for all table sync workers to finish.
        let table_sync_workers_result = pool.wait_all().await;
        if let Err(err) = table_sync_workers_result {
            // We naively use the `kinds` as number of errors.
            let errors_number = err.kinds().len();

            errors.push(err);

            info!("{} table sync workers failed with an error", errors_number);
        }

        if !errors.is_empty() {
            return Err(errors.into());
        }

        Ok(())
    }

    /// Initiates graceful shutdown of the pipeline.
    ///
    /// Sends shutdown signals to all workers, instructing them to complete their current
    /// operations and terminate. This method returns immediately after sending the signals
    /// and does not wait for workers to actually stop.
    ///
    /// Use [`Pipeline::wait`] after calling this method to wait for complete shutdown.
    pub fn shutdown(&self) {
        info!("trying to shut down the pipeline");

        if let Err(err) = self.shutdown_tx.shutdown() {
            error!("failed to send shutdown signal to the pipeline: {}", err);
            return;
        }

        info!("shut down signal successfully sent to all workers");
    }

    /// Initiates shutdown and waits for complete pipeline termination.
    ///
    /// This convenience method combines [`Pipeline::shutdown`] and [`Pipeline::wait`]
    /// to provide a single call that both initiates shutdown and waits for completion.
    /// Returns any errors encountered during the shutdown process.
    pub async fn shutdown_and_wait(self) -> EtlResult<()> {
        self.shutdown();
        self.wait().await
    }

    /// Initializes table replication states for tables in the publication and
    /// purges state for tables removed from it.
    ///
    /// Ensures each table currently in the Postgres publication has a
    /// corresponding replication state; tables without existing states are
    /// initialized to [`TableReplicationPhase::Init`].
    ///
    /// Also detects tables for which we have stored state but are no longer
    /// part of the publication, and deletes their stored state (replication
    /// state, table mappings, and table schemas) without touching the actual
    /// destination tables.
    async fn initialize_table_states(
        &self,
        replication_client: &PgReplicationClient,
    ) -> EtlResult<()> {
        // We need to make sure that the publication exists.
        if !replication_client
            .publication_exists(&self.config.publication_name)
            .await?
        {
            error!(
                "publication '{}' does not exist in the database",
                self.config.publication_name
            );

            bail!(
                ErrorKind::ConfigError,
                "Missing publication",
                format!(
                    "The publication '{}' does not exist in the database",
                    self.config.publication_name
                )
            );
        }

        let publication_table_ids = replication_client
            .get_publication_table_ids(&self.config.publication_name)
            .await?;

        info!(
            "the publication '{}' contains {} tables",
            self.config.publication_name,
            publication_table_ids.len()
        );

        self.store.load_table_replication_states().await?;
        let table_replication_states = self.store.get_table_replication_states().await?;

        // Initialize states for newly added tables in the publication
        for table_id in &publication_table_ids {
            if !table_replication_states.contains_key(table_id) {
                self.store
                    .update_table_replication_state(*table_id, TableReplicationPhase::Init)
                    .await?;
            }
        }

        // Detect and purge tables that have been removed from the publication.
        //
        // We must not delete the destination table, only the internal state.
        let publication_set: HashSet<TableId> = publication_table_ids.iter().copied().collect();
        for (table_id, _) in table_replication_states {
            if !publication_set.contains(&table_id) {
                info!(
                    "table {} removed from publication, purging stored state",
                    table_id
                );

                self.store.cleanup_table_state(table_id).await?;
            }
        }

        Ok(())
    }
}
