use crate::config::load_replicator_config;
use crate::migrations::migrate_state_store;
use config::shared::{
    BatchConfig, DestinationConfig, PgConnectionConfig, PipelineConfig, ReplicatorConfig,
    RetryConfig,
};
use etl::v2::destination::bigquery::BigQueryDestination;
use etl::v2::destination::memory::MemoryDestination;
use etl::v2::encryption::bigquery::install_crypto_provider_once;
use etl::v2::pipeline::Pipeline;
use etl::v2::state::store::base::StateStore;
use etl::v2::state::store::postgres::PostgresStateStore;
use etl::v2::{destination::base::Destination, pipeline::PipelineId};
use secrecy::ExposeSecret;
use std::fmt;
use tracing::{error, info, instrument, warn};

pub async fn start_replicator() -> anyhow::Result<()> {
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

    Ok(())
}

fn log_config(config: &ReplicatorConfig) {
    log_destination_config(&config.destination);
    log_pipeline_config(&config.pipeline);
}

fn log_destination_config(config: &DestinationConfig) {
    match config {
        DestinationConfig::Memory => {
            info!("Memory config");
        }
        DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key: _,
            max_staleness_mins,
        } => {
            info!(
                project_id,
                dataset_id, max_staleness_mins, "BigQuery config"
            )
        }
    }
}

fn log_pipeline_config(config: &PipelineConfig) {
    info!(
        pipeline_id = config.id,
        publication_name = config.publication_name,
        max_table_sync_workers = config.max_table_sync_workers,
        "Pipeline config"
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
        "Source Postgres connection config",
    );
}

fn log_batch_config(config: &BatchConfig) {
    info!(
        max_size = config.max_size,
        max_fill_ms = config.max_fill_ms,
        "Batch config"
    );
}

fn log_apply_worker_init_retry(config: &RetryConfig) {
    info!(
        max_attempts = config.max_attempts,
        initial_delay_ms = config.initial_delay_ms,
        max_delay_ms = config.max_delay_ms,
        backoff_factor = config.backoff_factor,
        "Apply worker init retry config"
    )
}

async fn init_state_store(
    pipeline_id: PipelineId,
    pg_connection_config: PgConnectionConfig,
) -> anyhow::Result<impl StateStore + Clone> {
    migrate_state_store(&pg_connection_config).await?;
    Ok(PostgresStateStore::new(pipeline_id, pg_connection_config))
}

#[instrument(skip(pipeline))]
async fn start_pipeline<S, D>(mut pipeline: Pipeline<S, D>) -> anyhow::Result<()>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + fmt::Debug + 'static,
{
    // Start the pipeline.
    pipeline.start().await?;

    // Spawn a task to listen for Ctrl+C and trigger shutdown.
    let shutdown_tx = pipeline.shutdown_tx();
    let shutdown_handle = tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl+C: {:?}", e);
            return;
        }

        info!("Ctrl+C received, shutting down pipeline...");
        if let Err(e) = shutdown_tx.shutdown() {
            warn!("Failed to send shutdown signal: {:?}", e);
        }
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
