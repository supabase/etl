//! Core pipeline orchestration and execution.

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
use crate::workers::heartbeat::{HeartbeatWorker, HeartbeatWorkerHandle};
use crate::workers::pool::TableSyncWorkerPool;
use etl_config::shared::PipelineConfig;
use etl_postgres::types::TableId;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

#[derive(Debug)]
enum PipelineState {
    NotStarted,
    Started {
        apply_worker: ApplyWorkerHandle,
        pool: TableSyncWorkerPool,
        heartbeat_worker: Option<HeartbeatWorkerHandle>,
    },
}

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
    pub fn new(config: PipelineConfig, state_store: S, destination: D) -> Self {
        register_metrics();
        let (shutdown_tx, _) = create_shutdown_channel();
        Self {
            config: Arc::new(config),
            store: state_store,
            destination,
            state: PipelineState::NotStarted,
            shutdown_tx,
        }
    }

    pub fn id(&self) -> PipelineId {
        self.config.id
    }

    pub fn shutdown_tx(&self) -> ShutdownTx {
        self.shutdown_tx.clone()
    }

    pub async fn start(&mut self) -> EtlResult<()> {
        info!(
            publication_name = %self.config.publication_name,
            pipeline_id = %self.config.id,
            replica_mode = %self.config.is_replica_mode(),
            "starting pipeline"
        );

        let replication_client =
            PgReplicationClient::connect(self.config.pg_connection.clone()).await?;

        self.store.load_table_mappings().await?;
        self.store.load_table_schemas().await?;
        self.initialize_table_states(&replication_client).await?;

        let pool = TableSyncWorkerPool::new();
        let table_sync_worker_permits =
            Arc::new(Semaphore::new(self.config.max_table_sync_workers as usize));

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

        let heartbeat_worker = if let Some(primary_config) = &self.config.primary_connection {
            info!(pipeline_id = %self.config.id, "replica mode enabled, starting heartbeat worker");
            let heartbeat_config = self.config.heartbeat_config();
            let worker = HeartbeatWorker::new(
                self.config.id,
                primary_config.clone(),
                heartbeat_config,
                self.shutdown_tx.subscribe(),
            );
            Some(worker.start())
        } else {
            None
        };

        self.state = PipelineState::Started {
            apply_worker,
            pool,
            heartbeat_worker,
        };

        Ok(())
    }

    pub async fn wait(self) -> EtlResult<()> {
        let PipelineState::Started { apply_worker, pool, heartbeat_worker } = self.state else {
            info!("pipeline was not started, skipping wait");
            return Ok(());
        };

        info!("waiting for apply worker to complete");
        let mut errors = vec![];

        let apply_worker_result = apply_worker.wait().await;
        if let Err(err) = apply_worker_result {
            errors.push(err);
            let _ = self.shutdown_tx.shutdown();
            info!("apply worker failed, shutting down table sync workers");
        }

        info!("waiting for table sync workers to complete");
        let table_sync_workers_result = pool.wait_all().await;
        if let Err(err) = table_sync_workers_result {
            let errors_number = err.kinds().len();
            errors.push(err);
            info!(error_count = errors_number, "table sync workers failed");
        }

        if let Some(heartbeat_handle) = heartbeat_worker {
            info!("waiting for heartbeat worker to complete");
            if let Err(err) = heartbeat_handle.wait().await {
                error!(error = %err, "heartbeat worker failed");
            }
        }

        info!("shutting down destination");
        if let Err(err) = self.destination.shutdown().await {
            errors.push(err);
        }

        if !errors.is_empty() {
            return Err(errors.into());
        }

        Ok(())
    }

    pub fn shutdown(&self) {
        info!("initiating pipeline shutdown");
        if let Err(err) = self.shutdown_tx.shutdown() {
            error!(error = %err, "failed to send shutdown signal");
            return;
        }
        info!("shutdown signal sent to all workers");
    }

    pub async fn shutdown_and_wait(self) -> EtlResult<()> {
        self.shutdown();
        self.wait().await
    }

    async fn initialize_table_states(
        &self,
        replication_client: &PgReplicationClient,
    ) -> EtlResult<()> {
        if !replication_client
            .publication_exists(&self.config.publication_name)
            .await?
        {
            error!(publication_name = %self.config.publication_name, "publication does not exist");
            bail!(
                ErrorKind::ConfigError,
                "Missing publication",
                format!("The publication '{}' does not exist", self.config.publication_name)
            );
        }

        let publication_table_ids = replication_client
            .get_publication_table_ids(&self.config.publication_name)
            .await?;

        info!(
            publication_name = %self.config.publication_name,
            table_count = publication_table_ids.len(),
            "publication tables loaded"
        );

        let publish_via_partition_root = replication_client
            .get_publish_via_partition_root(&self.config.publication_name)
            .await?;

        if !publish_via_partition_root {
            let has_partitioned_tables = replication_client
                .has_partitioned_tables(&publication_table_ids)
                .await?;

            if has_partitioned_tables {
                error!(publication_name = %self.config.publication_name, "publication has publish_via_partition_root=false but contains partitioned tables");
                bail!(
                    ErrorKind::ConfigError,
                    "Invalid publication configuration for partitioned tables",
                    format!(
                        "The publication '{}' contains partitioned tables but has publish_via_partition_root=false.",
                        self.config.publication_name
                    )
                );
            }
        }

        self.store.load_table_replication_states().await?;
        let table_replication_states = self.store.get_table_replication_states().await?;

        for table_id in &publication_table_ids {
            if !table_replication_states.contains_key(table_id) {
                self.store
                    .update_table_replication_state(*table_id, TableReplicationPhase::Init)
                    .await?;
            }
        }

        let publication_set: HashSet<TableId> = publication_table_ids.iter().copied().collect();
        for (table_id, _) in table_replication_states {
            if !publication_set.contains(&table_id) {
                info!(%table_id, "table removed from publication, purging stored state");
                self.store.cleanup_table_state(table_id).await?;
            }
        }

        Ok(())
    }
}
