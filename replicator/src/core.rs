use crate::config::load_replicator_config;
use crate::migrations::migrate_state_store;
use config::shared::{
    BatchConfig, DestinationConfig, PgConnectionConfig, PipelineConfig, ReplicatorConfig,
    RetryConfig,
};
use etl::destination::bigquery::BigQueryDestination;
use etl::destination::memory::MemoryDestination;
use etl::encryption::bigquery::install_crypto_provider_once;
use etl::pipeline::Pipeline;
use etl::state::store::base::StateStore;
use etl::state::store::postgres::PostgresStateStore;
use etl::{destination::base::Destination, pipeline::PipelineId};
use secrecy::ExposeSecret;
use std::fmt;
use tracing::{info, warn};

pub async fn start_replicator() -> anyhow::Result<()> {
    info!("starting replicator service");
    let replicator_config = load_replicator_config()?;

    log_config(&replicator_config);

    // We initialize the state store, which for the replicator is not configurable.
    let state_store = init_state_store(
        replicator_config.pipeline.id,
        replicator_config.pipeline.pg_connection.clone(),
    )
    .await?;

    // For each destination, we start the pipeline. This is more verbose due to static dispatch, but
    // we prefer more performance at the cost of ergonomics.
    match &replicator_config.destination {
        DestinationConfig::Memory => {
            let destination = MemoryDestination::new();

            let pipeline = Pipeline::new(
                replicator_config.pipeline.id,
                replicator_config.pipeline,
                state_store,
                destination,
            );
            start_pipeline(pipeline).await?;
        }
        DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
            max_staleness_mins,
        } => {
            install_crypto_provider_once();

            let destination = BigQueryDestination::new_with_key(
                project_id.clone(),
                dataset_id.clone(),
                service_account_key.expose_secret(),
                *max_staleness_mins,
            )
            .await?;

            let pipeline = Pipeline::new(
                replicator_config.pipeline.id,
                replicator_config.pipeline,
                state_store,
                destination,
            );
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
            info!("memory config");
        }
        DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key: _,
            max_staleness_mins,
        } => {
            info!(
                project_id,
                dataset_id, max_staleness_mins, "bigquery config"
            )
        }
    }
}

fn log_pipeline_config(config: &PipelineConfig) {
    info!(
        pipeline_id = config.id,
        publication_name = config.publication_name,
        max_table_sync_workers = config.max_table_sync_workers,
        "pipeline config"
    );
    log_pg_connection_config(&config.pg_connection);
    log_batch_config(&config.batch);
    log_apply_worker_init_retry(&config.apply_worker_init_retry);
}

fn log_pg_connection_config(config: &PgConnectionConfig) {
    info!(
        host = config.host,
        port = config.port,
        dbname = config.name,
        username = config.username,
        tls_enabled = config.tls.enabled,
        "source postgres connection config",
    );
}

fn log_batch_config(config: &BatchConfig) {
    info!(
        max_size = config.max_size,
        max_fill_ms = config.max_fill_ms,
        "batch config"
    );
}

fn log_apply_worker_init_retry(config: &RetryConfig) {
    info!(
        max_attempts = config.max_attempts,
        initial_delay_ms = config.initial_delay_ms,
        max_delay_ms = config.max_delay_ms,
        backoff_factor = config.backoff_factor,
        "apply worker init retry config"
    )
}

async fn init_state_store(
    pipeline_id: PipelineId,
    pg_connection_config: PgConnectionConfig,
) -> anyhow::Result<impl StateStore + Clone> {
    migrate_state_store(&pg_connection_config).await?;
    Ok(PostgresStateStore::new(pipeline_id, pg_connection_config))
}

#[tracing::instrument(skip(pipeline), fields(pipeline_id = pipeline.id()))]
async fn start_pipeline<S, D>(mut pipeline: Pipeline<S, D>) -> anyhow::Result<()>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + fmt::Debug + 'static,
{
    // Start the pipeline.
    pipeline.start().await?;

    // Spawn a task to listen for shutdown signals and trigger shutdown.
    let shutdown_tx = pipeline.shutdown_tx();
    let shutdown_handle = tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};

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
