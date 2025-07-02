use config::shared::{DestinationConfig, ReplicatorConfig};
use etl::v2::config::batch::BatchConfig;
use etl::v2::config::pipeline::PipelineConfig;
use etl::v2::config::retry::RetryConfig;
use etl::v2::destination::base::Destination;
use etl::v2::destination::bigquery::BigQueryDestination;
use etl::v2::destination::memory::MemoryDestination;
use etl::v2::encryption::bigquery::install_crypto_provider_once;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use etl::v2::state::store::postgres::PostgresStateStore;
use etl::SslMode;
use postgres::tokio::config::{PgConnectionConfig, PgTlsConfig};
use secrecy::ExposeSecret;
use std::fmt;
use std::io::BufReader;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::config::load_replicator_config;
use crate::migrations::migrate_state_store;

// Macro to statically dispatch pipeline creation and starting
macro_rules! start_pipeline_dispatch {
    ($identity:expr, $pipeline_config:expr, $state_store:expr, $destination:expr) => {{
        let pipeline = Pipeline::new($identity, $pipeline_config, $state_store, $destination);
        start_pipeline(pipeline).await
    }};
}

pub async fn start_replicator() -> anyhow::Result<()> {
    let replicator_config = load_replicator_config()?;

    // We set up the certificates and SSL mode.
    let mut trusted_root_certs = vec![];
    let ssl_mode = if replicator_config.source.tls.enabled {
        let mut root_certs_reader =
            BufReader::new(replicator_config.source.tls.trusted_root_certs.as_bytes());
        for cert in rustls_pemfile::certs(&mut root_certs_reader) {
            let cert = cert?;
            trusted_root_certs.push(cert);
        }
        SslMode::VerifyFull
    } else {
        SslMode::Disable
    };

    // We create the configuration that is used by the pipeline, which is separate from the configs
    // found in the 'config' crate.
    let pipeline_config = PipelineConfig {
        pg_connection: PgConnectionConfig {
            host: replicator_config.source.host.clone(),
            port: replicator_config.source.port,
            name: replicator_config.source.name.clone(),
            username: replicator_config.source.username.clone(),
            password: replicator_config.source.password.clone().map(Into::into),
            tls_config: PgTlsConfig {
                ssl_mode,
                trusted_root_certs: trusted_root_certs.clone(),
            },
        },
        batch: BatchConfig {
            max_size: replicator_config.pipeline.batch.max_size,
            max_fill: Duration::from_millis(replicator_config.pipeline.batch.max_fill_ms),
        },
        apply_worker_initialization_retry: RetryConfig {
            max_attempts: replicator_config
                .pipeline
                .apply_worker_init_retry
                .max_attempts,
            initial_delay: Duration::from_millis(
                replicator_config
                    .pipeline
                    .apply_worker_init_retry
                    .initial_delay_ms,
            ),
            max_delay: Duration::from_millis(
                replicator_config
                    .pipeline
                    .apply_worker_init_retry
                    .max_delay_ms,
            ),
            backoff_factor: replicator_config
                .pipeline
                .apply_worker_init_retry
                .backoff_factor,
        },
    };

    let identity = PipelineIdentity::new(
        replicator_config.pipeline.id,
        &replicator_config.pipeline.publication_name,
    );

    // We initialize the state store, which for the replicator is not configurable.
    let state_store = init_state_store(&replicator_config).await?;

    // For each destination, we start the pipeline. This is more verbose due to static dispatch, but
    // we prefer more performance at the cost of ergonomics.
    match &replicator_config.destination {
        DestinationConfig::Memory => {
            let destination = MemoryDestination::new();

            start_pipeline_dispatch!(identity, pipeline_config, state_store, destination)?;
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

            start_pipeline_dispatch!(identity, pipeline_config, state_store, destination)?;
        }
    }

    Ok(())
}

async fn init_state_store(config: &ReplicatorConfig) -> anyhow::Result<PostgresStateStore> {
    migrate_state_store(config.source.clone()).await?;

    Ok(PostgresStateStore::new(
        config.pipeline.id,
        config.source.clone(),
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
