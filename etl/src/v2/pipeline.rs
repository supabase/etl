use config::shared::PipelineConfig;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Semaphore, watch};
use tracing::{error, info};

use crate::v2::concurrency::shutdown::{ShutdownTx, create_shutdown_channel};
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::schema::cache::SchemaCache;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::TableReplicationPhase;
use crate::v2::workers::apply::{ApplyWorker, ApplyWorkerError, ApplyWorkerHandle};
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerWaitError, WorkerWaitErrors};
use crate::v2::workers::pool::TableSyncWorkerPool;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(
        "Both the apply worker and table sync workers failed. Apply worker error: {0}, Table sync workers errors: {1}"
    )]
    BothWorkerTypesFailed(WorkerWaitError, WorkerWaitErrors),

    #[error("The apply worker failed: {0}")]
    ApplyWorkerFailed(#[from] WorkerWaitError),

    #[error("Table sync workers failed: {0}")]
    TableSyncWorkersFailed(WorkerWaitErrors),

    #[error("PostgreSQL replication operation failed: {0}")]
    PgReplicationClient(#[from] PgReplicationError),

    #[error("Apply worker failed to start in the pipeline: {0}")]
    ApplyWorkerFailedOnStart(#[from] ApplyWorkerError),

    #[error("An error happened in the state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error occurred in the destination: {0}")]
    Destination(#[from] DestinationError),

    #[error(
        "An error occurred while shutting down the pipeline, likely because no workers are running: {0}"
    )]
    ShutdownFailed(#[from] watch::error::SendError<()>),

    #[error("The publication '{0}' does not exist in the database")]
    MissingPublication(String),
}

#[derive(Debug)]
enum PipelineWorkers {
    NotStarted,
    Started {
        // TODO: investigate whether we could benefit from a central launcher that deals at a high-level
        //  with workers management, which should not be done in the pipeline.
        apply_worker: ApplyWorkerHandle,
        pool: TableSyncWorkerPool,
    },
}

pub type PipelineId = u64;

#[derive(Debug)]
pub struct Pipeline<S, D> {
    id: PipelineId,
    config: Arc<PipelineConfig>,
    state_store: S,
    destination: D,
    workers: PipelineWorkers,
    shutdown_tx: ShutdownTx,
}

impl<S, D> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    pub fn new(id: PipelineId, config: PipelineConfig, state_store: S, destination: D) -> Self {
        // We create a watch channel of unit types since this is just used to notify all subscribers
        // that shutdown is needed.
        //
        // Here we are not taking the `shutdown_rx` since we will just extract it from the `shutdown_tx`
        // via the `subscribe` method. This is done to make the code cleaner.
        let (shutdown_tx, _) = create_shutdown_channel();

        Self {
            id,
            config: Arc::new(config),
            state_store,
            destination,
            workers: PipelineWorkers::NotStarted,
            shutdown_tx,
        }
    }

    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.shutdown_tx.clone()
    }

    pub async fn start(&mut self) -> Result<(), PipelineError> {
        info!(
            "Starting pipeline for publication '{}' with id {}",
            self.config.publication_name, self.id
        );

        // We create the schema cache, which will be shared also with the destination.
        let schema_cache = SchemaCache::default();

        // We inject pipeline specific dependencies within the destination.
        self.destination.inject(schema_cache.clone()).await?;

        // We prepare the schema cache with table schemas loaded, in case there is the need.
        self.prepare_schema_cache(&schema_cache).await?;

        // We create the first connection to Postgres.
        let replication_client =
            PgReplicationClient::connect(self.config.pg_connection.clone()).await?;

        // We synchronize the relation subscription states with the publication, to make sure we
        // always know which tables to work with. Maybe in the future we also want to react in real
        // time to new relation ids being sent over by the cdc event stream.
        self.initialize_table_states(&replication_client).await?;

        // We create the table sync workers pool to manage all table sync workers in a central place.
        let pool = TableSyncWorkerPool::new();

        let table_sync_worker_permits =
            Arc::new(Semaphore::new(self.config.max_table_sync_workers as usize));

        // We create and start the apply worker.
        let apply_worker = ApplyWorker::new(
            self.id,
            self.config.clone(),
            replication_client,
            pool.clone(),
            schema_cache,
            self.state_store.clone(),
            self.destination.clone(),
            self.shutdown_tx.subscribe(),
            table_sync_worker_permits,
        )
        .start()
        .await?;

        self.workers = PipelineWorkers::Started { apply_worker, pool };

        Ok(())
    }

    async fn prepare_schema_cache(&self, schema_cache: &SchemaCache) -> Result<(), PipelineError> {
        // We initialize the schema cache, which is local to a pipeline, and we try to load existing
        // schemas that were previously stored at the destination (if any).
        let table_schemas = self.destination.load_table_schemas().await?;
        schema_cache.add_table_schemas(table_schemas).await;

        Ok(())
    }

    async fn initialize_table_states(
        &self,
        replication_client: &PgReplicationClient,
    ) -> Result<(), PipelineError> {
        info!("Initializing table states");

        // We need to make sure that the publication exists.
        if !replication_client
            .publication_exists(&self.config.publication_name)
            .await?
        {
            error!(
                "Publication '{}' does not exist in the database",
                self.config.publication_name
            );
            return Err(PipelineError::MissingPublication(
                self.config.publication_name.clone(),
            ));
        }

        let table_ids = replication_client
            .get_publication_table_ids(&self.config.publication_name)
            .await?;
        info!(
            "Got table ids from publication: {}",
            table_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        self.state_store.load_table_replication_states().await?;
        let states = self.state_store.get_table_replication_states().await?;
        for table_id in table_ids {
            if !states.contains_key(&table_id) {
                self.state_store
                    .update_table_replication_state(table_id, TableReplicationPhase::Init)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn wait(self) -> Result<(), PipelineError> {
        let PipelineWorkers::Started { apply_worker, pool } = self.workers else {
            info!("Pipeline was not started, nothing to wait for");

            return Ok(());
        };

        info!("Waiting for apply worker to complete");

        // We first wait for the apply worker to finish, since that must be done before waiting for
        // the table sync workers to finish, otherwise if we wait for sync workers first, we might
        // be having the apply worker that spawns new sync workers after we waited for the current
        // ones to finish.
        let apply_worker_result = apply_worker.wait().await;
        if apply_worker_result.is_err() {
            // TODO: in the future we might build a system based on the `ReactiveFuture` that
            //  automatically sends a shutdown signal to table sync workers on apply worker failure.
            // If there was an error in the apply worker, we want to shut down all table sync
            // workers, since without an apply worker they are lost.
            if let Err(err) = self.shutdown_tx.shutdown() {
                info!(
                    "Shut down signal could not be delivered, likely because no workers are running: {:?}",
                    err
                );
            }

            info!("Apply worker completed with an error, shutting down table sync workers");
        } else {
            info!("Apply worker completed successfully");
        }

        info!("Waiting for table sync workers to complete");

        let table_sync_workers_result = pool.wait_all().await;
        if table_sync_workers_result.is_err() {
            info!("One or more table sync workers failed with an error");
        } else {
            info!("All table sync workers completed successfully");
        }

        match (apply_worker_result, table_sync_workers_result) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(err), Ok(_)) => Err(PipelineError::ApplyWorkerFailed(err)),
            (Ok(_), Err(err)) => Err(PipelineError::TableSyncWorkersFailed(err)),
            (Err(err), Err(errs)) => Err(PipelineError::BothWorkerTypesFailed(err, errs)),
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn shutdown(&self) -> Result<(), PipelineError> {
        info!("Trying to shut down the pipeline");
        self.shutdown_tx.shutdown()?;
        info!("Shut down signal successfully sent to all workers");

        Ok(())
    }

    pub async fn shutdown_and_wait(self) -> Result<(), PipelineError> {
        self.shutdown()?;
        self.wait().await
    }
}
