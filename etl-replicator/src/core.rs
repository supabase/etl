use crate::migrations::migrate_state_store;
use etl::destination::Destination;
use etl::destination::memory::MemoryDestination;
use etl::pipeline::Pipeline;
use etl::store::both::postgres::PostgresStore;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::PipelineId;
use etl_config::shared::{
    BatchConfig, DestinationConfig, PgConnectionConfig, PipelineConfig, ReplicatorConfig,
};
use etl_destinations::bigquery::{BigQueryDestination, install_crypto_provider_for_bigquery};
use secrecy::ExposeSecret;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{debug, info, warn};

/// Starts the replicator service with the provided configuration.
///
/// Initializes the state store, creates the appropriate destination based on
/// configuration, and starts the pipeline. Handles both memory and BigQuery
/// destinations with proper initialization and error handling.
pub async fn start_replicator_with_config(
    replicator_config: ReplicatorConfig,
) -> anyhow::Result<()> {
    info!("starting replicator service");

    log_config(&replicator_config);

    // We initialize the state store, which for the replicator is not configurable.
    let state_store = init_store(
        replicator_config.pipeline.id,
        replicator_config.pipeline.pg_connection.clone(),
    )
    .await?;

    // For each destination, we start the pipeline. This is more verbose due to static dispatch, but
    // we prefer more performance at the cost of ergonomics.
    match &replicator_config.destination {
        DestinationConfig::Memory => {
            let destination = MemoryDestination::new();

            let pipeline = Pipeline::new(replicator_config.pipeline, state_store, destination);
            start_pipeline(pipeline).await?;
        }
        DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
            max_staleness_mins,
            max_concurrent_streams,
        } => {
            install_crypto_provider_for_bigquery();

            let destination = BigQueryDestination::new_with_key(
                project_id.clone(),
                dataset_id.clone(),
                service_account_key.expose_secret(),
                *max_staleness_mins,
                *max_concurrent_streams,
                state_store.clone(),
            )
            .await?;

            let pipeline = Pipeline::new(replicator_config.pipeline, state_store, destination);
            start_pipeline(pipeline).await?;
        }
    }

    info!("replicator service completed");

    Ok(())
}

fn log_config(config: &ReplicatorConfig) {
    log_destination_config(&config.destination);
    log_pipeline_config(&config.pipeline);
}

fn log_destination_config(config: &DestinationConfig) {
    match config {
        DestinationConfig::Memory => {
            debug!("using memory destination config");
        }
        DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key: _,
            max_staleness_mins,
            max_concurrent_streams,
        } => {
            debug!(
                project_id,
                dataset_id,
                max_staleness_mins,
                max_concurrent_streams,
                "using bigquery destination config"
            )
        }
    }
}

fn log_pipeline_config(config: &PipelineConfig) {
    debug!(
        pipeline_id = config.id,
        publication_name = config.publication_name,
        table_error_retry_delay_ms = config.table_error_retry_delay_ms,
        max_table_sync_workers = config.max_table_sync_workers,
        "pipeline config"
    );
    log_pg_connection_config(&config.pg_connection);
    log_batch_config(&config.batch);
}

fn log_pg_connection_config(config: &PgConnectionConfig) {
    debug!(
        host = config.host,
        port = config.port,
        dbname = config.name,
        username = config.username,
        tls_enabled = config.tls.enabled,
        "source postgres connection config",
    );
}

fn log_batch_config(config: &BatchConfig) {
    debug!(
        max_size = config.max_size,
        max_fill_ms = config.max_fill_ms,
        "batch config"
    );
}

/// Initializes the state store with migrations.
///
/// Runs necessary database migrations on the state store and creates a
/// [`PostgresStore`] instance for the given pipeline and connection configuration.
async fn init_store(
    pipeline_id: PipelineId,
    pg_connection_config: PgConnectionConfig,
) -> anyhow::Result<impl StateStore + SchemaStore + CleanupStore + Clone> {
    migrate_state_store(&pg_connection_config).await?;

    Ok(PostgresStore::new(pipeline_id, pg_connection_config))
}

/// Starts a pipeline and handles graceful shutdown signals.
///
/// Launches the pipeline, sets up signal handlers for SIGTERM and SIGINT,
/// and ensures proper cleanup on shutdown. The pipeline will attempt to
/// finish processing current batches before terminating.
#[tracing::instrument(skip(pipeline), fields(pipeline_id = pipeline.id()))]
async fn start_pipeline<S, D>(mut pipeline: Pipeline<S, D>) -> anyhow::Result<()>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    // Start the pipeline.
    pipeline.start().await?;

    // Spawn a task to listen for shutdown signals and trigger shutdown.
    let shutdown_tx = pipeline.shutdown_tx();
    let shutdown_handle = tokio::spawn(async move {
        // Listen for SIGTERM, sent by Kubernetes before SIGKILL during pod termination.
        //
        // If the process is killed before shutdown completes, the pipeline may become corrupted,
        // depending on the state store and destination implementations.
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT (Ctrl+C) received, shutting down pipeline");
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received, shutting down pipeline");
            }
        }

        if let Err(e) = shutdown_tx.shutdown() {
            warn!("failed to send shutdown signal: {:?}", e);
            return;
        }

        info!("pipeline shutdown successfully")
    });

    // Wait for the pipeline to finish (either normally or via shutdown).
    let result = pipeline.wait().await;

    // Ensure the shutdown task is finished before returning.
    // If the pipeline finished before Ctrl+C, we want to abort the shutdown task.
    // If Ctrl+C was pressed, the shutdown task will have already triggered shutdown.
    // We don't care about the result of the shutdown_handle, but we should abort it if it's still running.
    shutdown_handle.abort();
    let _ = shutdown_handle.await;

    // Propagate any pipeline error as anyhow error.
    result?;

    Ok(())
}
