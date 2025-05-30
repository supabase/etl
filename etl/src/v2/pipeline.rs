use crate::v2::destination::base::Destination;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::state::store::base::PipelineStateStore;
use crate::v2::workers::apply::{ApplyWorker, ApplyWorkerHandle};
use crate::v2::workers::base::{Worker, WorkerHandle};
use crate::v2::workers::pool::TableSyncWorkerPool;
use postgres::tokio::options::PgDatabaseOptions;
use rustls::pki_types::CertificateDer;
use thiserror::Error;
use tokio_postgres::config::SslMode;
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("An error occurred in a worker")]
    WorkerError,

    #[error("An error occurred in the replication client: {0}")]
    PgReplicationClient(#[from] PgReplicationError),
}

#[derive(Debug)]
enum PipelineWorkers {
    NotStarted,
    Started {
        // TODO: investigate whether we could benefit from a central launcher that deals at a high-level
        //  with workers management, which should not be done in the pipeline.
        apply_worker: ApplyWorkerHandle,
        table_sync_workers: TableSyncWorkerPool,
    },
}

#[derive(Debug)]
pub struct Pipeline<S, D> {
    _id: u64,
    publication_name: String,
    options: PgDatabaseOptions,
    trusted_root_certs: Vec<CertificateDer<'static>>,
    state_store: S,
    destination: D,
    workers: PipelineWorkers,
}

impl<S, D> Pipeline<S, D>
where
    S: PipelineStateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    pub async fn new(
        _id: u64,
        publication_name: String,
        options: PgDatabaseOptions,
        trusted_root_certs: Vec<CertificateDer<'static>>,
        state_store: S,
        destination: D,
    ) -> Self {
        Self {
            _id,
            publication_name,
            options,
            trusted_root_certs,
            state_store,
            destination,
            workers: PipelineWorkers::NotStarted,
        }
    }

    pub async fn start(&mut self) -> Result<(), PipelineError> {
        info!(
            "Starting pipeline for publication {}",
            self.publication_name
        );

        // We synchronize the relation subscription states with the publication, to make sure we
        // always know which tables to work with. Maybe in the future we also want to react in real
        // time to new relation ids being sent over by the cdc event stream.
        self.sync_relation_subscription_states().await;

        // We create the table sync workers pool to manage all table sync workers in a central place.
        let pool = TableSyncWorkerPool::new();

        // We create the main replication client that will be used by the apply worker.
        let replication_client = match self.options.ssl_mode {
            SslMode::Disable => PgReplicationClient::connect_no_tls(self.options.clone()).await?,
            _ => {
                PgReplicationClient::connect_tls(
                    self.options.clone(),
                    self.trusted_root_certs.clone(),
                )
                .await?
            }
        };

        // We create and start the apply worker.
        let apply_worker = ApplyWorker::new(
            replication_client,
            pool.clone(),
            self.state_store.clone(),
            self.destination.clone(),
        )
        .start()
        .await
        .ok_or_else(|| {
            error!("Failed to start apply worker");
            PipelineError::WorkerError
        })?;

        self.workers = PipelineWorkers::Started {
            apply_worker,
            table_sync_workers: pool,
        };

        Ok(())
    }

    async fn sync_relation_subscription_states(&self) {
        info!("Synchronizing relation subscription states");
        // TODO: in this function we want to:
        //  1. Load all tables for the publication
        //  2. For each table, we check if it already exists in the store
        //  3. If the table is not there, add it with `Init` state
        //  4. If it's there do not do anything
    }

    pub async fn wait(self) {
        let PipelineWorkers::Started {
            apply_worker,
            table_sync_workers,
        } = self.workers
        else {
            info!("Pipeline was not started, nothing to wait for");
            return;
        };

        // TODO: handle failure of errors on wait.
        info!("Waiting for pipeline workers to complete");
        // We first wait for the apply worker to finish, since that must be done before waiting for
        // the table sync workers to finish, otherwise if we wait for sync workers first, we might
        // be having the apply worker that spawns new sync workers after we waited for the current
        // ones to finish.
        apply_worker
            .wait()
            .await
            .expect("Failed to wait for apply worker");
        info!("Apply worker completed");

        let mut table_sync_workers = table_sync_workers.write().await;
        table_sync_workers.wait_all().await;
        info!("All table sync workers completed");
    }
}
