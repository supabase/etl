use std::collections::HashMap;

use etl::destination::memory::MemoryDestination;
use etl::pipeline::Pipeline;
use etl::store::both::postgres::PostgresStore;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::PipelineId;
use etl::{config::IcebergConfig, destination::Destination};
use etl_config::Environment;
use etl_config::shared::{
    BatchConfig, DestinationConfig, PgConnectionConfig, PipelineConfig, ReplicatorConfig,
};
use etl_destinations::iceberg::{
    DestinationNamespace, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY,
};
use etl_destinations::{
    bigquery::BigQueryDestination,
    iceberg::{IcebergClient, IcebergDestination},
};
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
    // Migrations are run by the pipeline during startup.
    let state_store = init_store(
        replicator_config.pipeline.id,
        replicator_config.pipeline.pg_connection.clone(),
    );

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
        DestinationConfig::Iceberg {
            config:
                IcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                },
        } => {
            let env = Environment::load()?;
            let client = IcebergClient::new_with_supabase_catalog(
                project_ref,
                env.get_supabase_domain(),
                catalog_token.expose_secret().to_string(),
                warehouse_name.clone(),
                s3_access_key_id.expose_secret().to_string(),
                s3_secret_access_key.expose_secret().to_string(),
                s3_region.clone(),
            )
            .await?;
            let namespace = match namespace {
                Some(ns) => DestinationNamespace::Single(ns.to_string()),
                None => DestinationNamespace::OnePerSchema,
            };
            let destination = IcebergDestination::new(client, namespace, state_store.clone());

            let pipeline = Pipeline::new(replicator_config.pipeline, state_store, destination);
            start_pipeline(pipeline).await?;
        }
        DestinationConfig::Iceberg {
            config:
                IcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                },
        } => {
            let client = IcebergClient::new_with_rest_catalog(
                catalog_uri.clone(),
                warehouse_name.clone(),
                create_props(
                    s3_access_key_id.expose_secret().to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
                    s3_endpoint.clone(),
                ),
            )
            .await?;
            let namespace = match namespace {
                Some(ns) => DestinationNamespace::Single(ns.to_string()),
                None => DestinationNamespace::OnePerSchema,
            };
            let destination = IcebergDestination::new(client, namespace, state_store.clone());

            let pipeline = Pipeline::new(replicator_config.pipeline, state_store, destination);
            start_pipeline(pipeline).await?;
        }
    }

    info!("replicator service completed");

    Ok(())
}

pub fn create_props(
    s3_access_key_id: String,
    s3_secret_access_key: String,
    s3_endpoint: String,
) -> HashMap<String, String> {
    let mut props: HashMap<String, String> = HashMap::new();

    props.insert(S3_ACCESS_KEY_ID.to_string(), s3_access_key_id);
    props.insert(S3_SECRET_ACCESS_KEY.to_string(), s3_secret_access_key);
    props.insert(S3_ENDPOINT.to_string(), s3_endpoint);

    props
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
        DestinationConfig::Iceberg {
            config:
                IcebergConfig::Supabase {
                    namespace,
                    project_ref,
                    catalog_token: _,
                    warehouse_name,
                    s3_access_key_id: _,
                    s3_secret_access_key: _,
                    s3_region,
                },
        } => {
            debug!(
                namespace,
                project_ref, warehouse_name, s3_region, "using supabase iceberg destination config"
            )
        }
        DestinationConfig::Iceberg {
            config:
                IcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id: _,
                    s3_secret_access_key: _,
                    s3_endpoint,
                },
        } => {
            debug!(
                catalog_uri,
                warehouse_name,
                namespace,
                s3_endpoint,
                "using generic rest iceberg destination config"
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

/// Initializes the state store.
///
/// Creates a [`PostgresStore`] instance for the given pipeline and connection configuration.
/// Migrations are handled by the pipeline during startup.
fn init_store(
    pipeline_id: PipelineId,
    pg_connection_config: PgConnectionConfig,
) -> impl StateStore + SchemaStore + CleanupStore + Clone {
    PostgresStore::new(pipeline_id, pg_connection_config)
}

/// Starts a pipeline and handles graceful shutdown signals.
///
/// Launches the pipeline, sets up signal handlers for SIGTERM and SIGINT,
/// and ensures proper cleanup on shutdown. The pipeline will attempt to
/// finish processing current batches before terminating.
#[tracing::instrument(skip(pipeline))]
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
                info!("sigint (ctrl+c) received, shutting down pipeline");
            }
            _ = sigterm.recv() => {
                info!("sigterm received, shutting down pipeline");
            }
        }

        if let Err(e) = shutdown_tx.shutdown() {
            warn!(error = ?e, "failed to send shutdown signal");
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
