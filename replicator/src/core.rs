use config::shared::{DestinationConfig, ReplicatorConfig};
use etl::v2::config::batch::BatchConfig;
use etl::v2::config::pipeline::PipelineConfig;
use etl::v2::config::retry::RetryConfig;
use etl::v2::destination::base::Destination;
use etl::v2::destination::memory::MemoryDestination;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use etl::v2::state::store::postgres::PostgresStateStore;
use etl::SslMode;
use postgres::tokio::config::PgConnectionConfig;
use std::fmt;
use std::io::BufReader;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, info, warn};

use crate::config::load_replicator_config;
use crate::migrations::migrate_state_store;

#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("The destination {0} is currently unsupported")]
    UnsupportedDestination(String),
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

    // We initialize the state store and destination.
    let state_store = init_state_store(&replicator_config).await?;
    let destination = init_destination(&replicator_config).await?;

    // We create the identity of this pipeline.
    let identity = PipelineIdentity::new(
        replicator_config.pipeline.id,
        &replicator_config.pipeline.publication_name,
    );

    // We prepare the configuration of the pipeline.
    // TODO: improve v2 pipeline config to make conversions nicer.
    let pipeline_config = PipelineConfig {
        pg_connection: PgConnectionConfig {
            host: replicator_config.source.host,
            port: replicator_config.source.port,
            name: replicator_config.source.name,
            username: replicator_config.source.username,
            password: replicator_config.source.password.map(Into::into),
            ssl_mode,
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

    let pipeline = Pipeline::new(
        identity,
        pipeline_config,
        trusted_root_certs,
        state_store,
        destination,
    );
    start_pipeline(pipeline).await?;

    Ok(())
}

async fn init_state_store(config: &ReplicatorConfig) -> anyhow::Result<impl StateStore + Clone> {
    migrate_state_store(config.source.clone()).await?;
    Ok(PostgresStateStore::new(
        config.pipeline.id,
        config.source.clone(),
    ))
}

async fn init_destination(
    config: &ReplicatorConfig,
) -> anyhow::Result<impl Destination + Clone + Send + Sync + fmt::Debug + 'static> {
    match config.destination {
        DestinationConfig::Memory => Ok(MemoryDestination::new()),
        _ => {
            Err(ReplicatorError::UnsupportedDestination(format!("{:?}", config.destination)).into())
        }
    }
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
