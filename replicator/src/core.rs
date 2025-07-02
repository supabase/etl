use crate::config::load_replicator_config;
use crate::migrations::migrate_state_store;
use config::shared::{DestinationConfig, ReplicatorConfig};
use etl::v2::destination::base::Destination;
use etl::v2::destination::bigquery::BigQueryDestination;
use etl::v2::destination::memory::MemoryDestination;
use etl::v2::encryption::bigquery::install_crypto_provider_once;
use etl::v2::pipeline::Pipeline;
use etl::v2::state::store::base::StateStore;
use etl::v2::state::store::postgres::PostgresStateStore;
use secrecy::ExposeSecret;
use std::fmt;
use tracing::{error, info, warn};

pub async fn start_replicator() -> anyhow::Result<()> {
    let replicator_config = load_replicator_config()?;

    // We initialize the state store, which for the replicator is not configurable.
    let state_store = init_state_store(&replicator_config).await?;

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

async fn init_state_store(config: &ReplicatorConfig) -> anyhow::Result<impl StateStore + Clone> {
    migrate_state_store(config.pipeline.pg_connection.clone()).await?;
    Ok(PostgresStateStore::new(
        config.pipeline.id,
        config.pipeline.pg_connection.clone(),
    ))
}

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
