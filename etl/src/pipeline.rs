use etl_config::shared::PipelineConfig;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::bail;
use crate::concurrency::shutdown::{ShutdownTx, create_shutdown_channel};
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::replication::client::PgReplicationClient;
use crate::state::table::TableReplicationPhase;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::apply::{ApplyWorker, ApplyWorkerHandle};
use crate::workers::base::{Worker, WorkerHandle};
use crate::workers::pool::TableSyncWorkerPool;

#[derive(Debug)]
enum PipelineState {
    NotStarted,
    Started {
        // TODO: investigate whether we could benefit from a central launcher that deals at a high-level
        //  with workers management, which should not be done in the pipeline.
        apply_worker: ApplyWorkerHandle,
        pool: TableSyncWorkerPool,
    },
}

#[derive(Debug)]
pub struct Pipeline<S, D> {
    id: PipelineId,
    config: Arc<PipelineConfig>,
    store: S,
    destination: D,
    state: PipelineState,
    shutdown_tx: ShutdownTx,
}

impl<S, D> Pipeline<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
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
            store: state_store,
            destination,
            state: PipelineState::NotStarted,
            shutdown_tx,
        }
    }

    pub fn id(&self) -> PipelineId {
        self.id
    }

    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.shutdown_tx.clone()
    }

    pub async fn start(&mut self) -> EtlResult<()> {
        info!(
            "starting pipeline for publication '{}' with id {}",
            self.config.publication_name, self.id
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
            self.id,
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

    pub fn shutdown(&self) {
        info!("trying to shut down the pipeline");

        if let Err(err) = self.shutdown_tx.shutdown() {
            error!("failed to send shutdown signal to the pipeline: {}", err);
            return;
        }

        info!("shut down signal successfully sent to all workers");
    }

    pub async fn shutdown_and_wait(self) -> EtlResult<()> {
        self.shutdown();
        self.wait().await
    }

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

        let table_ids = replication_client
            .get_publication_table_ids(&self.config.publication_name)
            .await?;

        self.store.load_table_replication_states().await?;
        let states = self.store.get_table_replication_states().await?;
        for table_id in table_ids {
            if !states.contains_key(&table_id) {
                self.store
                    .update_table_replication_state(table_id, TableReplicationPhase::Init)
                    .await?;
            }
        }

        Ok(())
    }
}
