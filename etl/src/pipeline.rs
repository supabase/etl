use crate::concurrency::shutdown::ShutdownTx;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::metrics::register_metrics;
use crate::replication::client::PgReplicationClient;
use crate::replication::source::ReplicationSource;
use crate::replication::stream::ReplicationStream;
use crate::schema_store::SchemaStore;
use crate::state::state_event::StateEventBuilder;
use crate::state::table::TableReplicationPhaseType;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::apply::{ApplyWorker, ApplyWorkerHandle};
use crate::workers::base::{Worker, WorkerHandle};
use crate::workers::heartbeat::{HeartbeatWorker, HeartbeatWorkerHandle};
use crate::workers::pool::TableSyncWorkerPool;
use etl_config::shared::PipelineConfig;
use etl_postgres::types::TableId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{Instrument, debug, error, info, instrument, warn};

const SHUTDOWN_DEFAULT: bool = false;

enum PipelineState {
    Created,
    Started {
        // TODO: If we decide to keep the workers in the pipeline we should think about
        //  a better abstraction that decouples the lifecycle of a pipeline with the lifecycle
        //  of the workers it contains. This is currently needed only for the wait functionality,
        //  with workers management, which should not be done in the pipeline.
        apply_worker: ApplyWorkerHandle,
        pool: TableSyncWorkerPool,
        heartbeat_worker: Option<HeartbeatWorkerHandle>,
    },
}

/// Core orchestrator for managing a complete ETL pipeline.
///
/// The pipeline coordinates the flow of data from a PostgreSQL source through the ETL
/// process to a destination. It manages three main phases:
/// 1. **Schema Discovery**: Connects to PostgreSQL and discovers table schemas.
/// 2. **Initial Sync**: Copies existing data from tables using the table sync workers.
/// 3. **Streaming**: Continuously replicates changes using logical replication.
///
/// Multiple table sync workers run in parallel during the initial phase, while a single
/// apply worker processes the replication stream of table that were already copied.
///
/// When configured for read replica mode (with `primary_connection` set), the pipeline
/// also starts a heartbeat worker that emits periodic messages to the primary database
/// to keep the replication slot active.
#[derive(Debug)]
pub struct Pipeline<S, D> {
    config: Arc<PipelineConfig>,
    state: PipelineState,
    store: S,
    destination: D,
    shutdown_tx: ShutdownTx,
}

impl<S, D> std::fmt::Debug for PipelineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineState::Created => write!(f, "Created"),
            PipelineState::Started { .. } => write!(f, "Started"),
        }
    }
}

impl<S, D> Pipeline<S, D>
where
    S: StateStore,
    D: Destination,
{
    /// Creates a new pipeline with the given configuration.
    ///
    /// The pipeline is created in the `Created` state. Call [`Pipeline::start`] to begin
    /// replication.
    pub fn new(config: PipelineConfig, store: S, destination: D) -> Self {
        register_metrics();
        let (shutdown_tx, _shutdown_rx) = watch::channel(SHUTDOWN_DEFAULT);
        Pipeline {
            config: Arc::new(config),
            state: PipelineState::Created,
            store,
            destination,
            shutdown_tx,
        }
    }

    /// Starts the pipeline and begins replication.
    ///
    /// This method initializes the connection to Postgres, sets up table mappings and schemas,
    /// creates the worker pool for table synchronization, and starts the apply worker for
    /// processing replication stream events.
    ///
    /// When configured for read replica mode (with `primary_connection` set), this method
    /// also starts a heartbeat worker that maintains replication slot activity on the primary.
    pub async fn start(&mut self) -> EtlResult<()> {
        info!(
            publication_name = %self.config.publication_name,
            pipeline_id = %self.config.id,
            replica_mode = %self.config.is_replica_mode(),
            "starting pipeline"
        );

        let client = PgReplicationClient::connect((*self.config.pg_connection).clone()).await?;

        let source = ReplicationSource::new(
            client,
            &self.config.publication_name,
            &self.config.id.to_string(),
        );

        let table_schemas = source.discover_tables().await?;
        let table_map: HashMap<TableId, String> = table_schemas
            .iter()
            .map(|table| (table.id, table.name.full_name.clone()))
            .collect();

        let schema_store = Arc::new(SchemaStore::new(table_schemas.clone()));

        let (replication_stream, table_states) = source
            .start_replication(&mut self.store, &table_map)
            .await?;

        let mut pool = TableSyncWorkerPool::new(
            &self.config,
            self.store.clone(),
            schema_store.clone(),
            self.destination.clone(),
            self.shutdown_tx.subscribe(),
        );

        let apply_worker = ApplyWorker::new(
            &self.config,
            replication_stream,
            table_states,
            &mut pool,
            self.store.clone(),
            schema_store,
            self.destination.clone(),
            self.shutdown_tx.subscribe(),
        )
        .start()
        .await?;

        // Start heartbeat worker if configured for read replica mode.
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

    /// Waits for the pipeline to complete all work.
    ///
    /// This method blocks until all workers have finished processing and returns any errors
    /// that occurred during execution. Use this after calling [`Pipeline::shutdown`] to ensure
    /// clean termination.
    ///
    /// The wait process ensures proper shutdown ordering:
    /// 1. Apply worker completes first (may spawn additional table sync workers).
    /// 2. All table sync workers complete.
    /// 3. Heartbeat worker completes (if running).
    /// 4. Any errors from workers are aggregated and returned.
    pub async fn wait(self) -> EtlResult<()> {
        let PipelineState::Started {
            apply_worker,
            pool,
            heartbeat_worker,
        } = self.state
        else {
            info!("pipeline was not started, skipping wait");

            return Ok(());
        };

        info!("waiting for apply worker to complete");
        if let Err(err) = apply_worker.wait().await {
            error!(error = %err, "apply worker failed");
        }

        info!("waiting for table sync workers to complete");
        let errors = pool.wait().await;
        let errors_number = errors.len();
        if !errors.is_empty() {
            let error_messages: Vec<String> = errors
                .into_iter()
                .map(|e| {
                    error!(error = %e, "table sync worker error");
                    e.to_string()
                })
                .collect();
            info!(error_count = errors_number, "table sync workers failed");
        }

        // Wait for heartbeat worker if it was running.
        if let Some(heartbeat_handle) = heartbeat_worker {
            info!("waiting for heartbeat worker to complete");
            if let Err(err) = heartbeat_handle.wait().await {
                error!(error = %err, "heartbeat worker failed");
            }
        }

        // Once all workers completed, we notify the destination of shutting down.
        info!("shutting down destination");
        if let Err(err) = self.destination.shutdown().await {
            error!(error = %err, "failed to shutdown destination");
        }

        Ok(())
    }

    /// Initiates a graceful shutdown of the pipeline.
    ///
    /// This signals all workers to stop processing and begin their shutdown procedures.
    /// Call [`Pipeline::wait`] after this to wait for the shutdown to complete.
    pub fn shutdown(&self) {
        info!("sending shutdown signal to pipeline workers");
        let _ = self.shutdown_tx.send(true);
    }

    /// Signals a graceful shutdown and then waits for all workers to complete.
    ///
    /// This is a convenience method that combines [`Pipeline::shutdown`] and [`Pipeline::wait`].
    pub async fn shutdown_and_wait(self) -> EtlResult<()> {
        self.shutdown();
        self.wait().await
    }
}
