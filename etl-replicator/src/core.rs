use std::collections::HashMap;

use crate::error::{ReplicatorError, ReplicatorResult};
use crate::error_notification::ErrorNotificationClient;
use crate::error_reporting::ErrorReportingStateStore;
use crate::metrics;
use crate::sentry::set_destination_tag;
use etl::pipeline::Pipeline;
use etl::store::both::postgres::PostgresStore;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::PipelineId;
use etl::{config::IcebergConfig, destination::Destination};
use etl_config::shared::{DestinationConfig, PgConnectionConfig, ReplicatorConfig};
use etl_config::{Environment, parse_ducklake_url};
use etl_destinations::iceberg::{
    DestinationNamespace, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY,
};
use etl_destinations::{
    bigquery::BigQueryDestination,
    ducklake::{DuckLakeDestination, S3Config as DucklakeS3Config},
    iceberg::{IcebergClient, IcebergDestination},
};
use secrecy::ExposeSecret;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info, warn};

/// Starts the replicator service with the provided configuration.
///
/// Initializes the state store, creates the appropriate destination based on
/// configuration, and starts the pipeline.
pub async fn start_replicator_with_config(
    replicator_config: ReplicatorConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<()> {
    let pipeline_id = replicator_config.pipeline.id;

    // We initialize the state store, which for the replicator is not configurable.
    let state_store = init_store(
        pipeline_id,
        replicator_config.pipeline.pg_connection.clone(),
        notification_client,
    )
    .await?;

    // For each destination, we start the pipeline. This is more verbose due to static dispatch, but
    // we prefer more performance at the cost of ergonomics.
    match &replicator_config.destination {
        DestinationConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
            max_staleness_mins,
            connection_pool_size,
        } => {
            set_destination_scope::<BigQueryDestination<ErrorReportingStateStore<PostgresStore>>>();

            let destination = BigQueryDestination::new_with_key(
                project_id.clone(),
                dataset_id.clone(),
                service_account_key.expose_secret(),
                *max_staleness_mins,
                *connection_pool_size,
                pipeline_id,
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
            set_destination_scope::<IcebergDestination<ErrorReportingStateStore<PostgresStore>>>();

            let env = Environment::load().map_err(ReplicatorError::config)?;
            let client = IcebergClient::new_with_supabase_catalog(
                project_ref,
                env.get_supabase_domain(),
                catalog_token.expose_secret().to_string(),
                warehouse_name.clone(),
                s3_access_key_id.expose_secret().to_string(),
                s3_secret_access_key.expose_secret().to_string(),
                s3_region.clone(),
            )
            .await
            .map_err(ReplicatorError::config)?;
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
            set_destination_scope::<IcebergDestination<ErrorReportingStateStore<PostgresStore>>>();

            let client = IcebergClient::new_with_rest_catalog(
                catalog_uri.clone(),
                warehouse_name.clone(),
                create_props(
                    s3_access_key_id.expose_secret().to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
                    s3_endpoint.clone(),
                ),
            )
            .await
            .map_err(ReplicatorError::config)?;
            let namespace = match namespace {
                Some(ns) => DestinationNamespace::Single(ns.to_string()),
                None => DestinationNamespace::OnePerSchema,
            };
            let destination = IcebergDestination::new(client, namespace, state_store.clone());

            let pipeline = Pipeline::new(replicator_config.pipeline, state_store, destination);
            start_pipeline(pipeline).await?;
        }
        DestinationConfig::Ducklake {
            catalog_url,
            data_path,
            pool_size,
            s3_access_key_id,
            s3_secret_access_key,
            s3_region,
            s3_endpoint,
            s3_url_style,
            s3_use_ssl,
            metadata_schema,
        } => {
            set_destination_scope::<DuckLakeDestination<PostgresStore>>();

            let s3_config = match (s3_access_key_id, s3_secret_access_key) {
                (Some(access_key_id), Some(secret_access_key)) => Some(DucklakeS3Config {
                    access_key_id: access_key_id.expose_secret().to_string(),
                    secret_access_key: secret_access_key.expose_secret().to_string(),
                    region: s3_region.clone().unwrap_or_else(|| "us-east-1".to_string()),
                    endpoint: s3_endpoint.clone(),
                    url_style: s3_url_style.clone().unwrap_or_else(|| "path".to_string()),
                    use_ssl: s3_use_ssl.unwrap_or(false),
                }),
                (None, None) => None,
                _ => {
                    return Err(ReplicatorError::config(std::io::Error::other(
                        "ducklake s3 credentials must include both access key id and secret access key",
                    )));
                }
            };

            let destination = DuckLakeDestination::new(
                parse_ducklake_url(catalog_url).map_err(ReplicatorError::config)?,
                parse_ducklake_url(data_path).map_err(ReplicatorError::config)?,
                *pool_size,
                s3_config,
                metadata_schema.clone(),
                state_store.clone(),
            )
            .await?;

            let pipeline = Pipeline::new(replicator_config.pipeline, state_store, destination);
            start_pipeline(pipeline).await?;
        }
    }

    Ok(())
}

/// Sets the destination tag on the current error-reporting scope.
fn set_destination_scope<D: Destination>() {
    set_destination_tag(D::name());
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

/// Initializes the state store.
///
/// Creates a [`PostgresStore`] instance for the given pipeline and connection
/// configuration. The pipeline itself owns state-store migration startup.
async fn init_store(
    pipeline_id: PipelineId,
    pg_connection_config: PgConnectionConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<impl StateStore + SchemaStore + CleanupStore + Clone> {
    info!("initializing postgres state store");

    Ok(ErrorReportingStateStore::new(
        PostgresStore::new(pipeline_id, pg_connection_config).await?,
        notification_client,
    ))
}

/// Starts a pipeline and handles graceful shutdown signals.
///
/// Launches the pipeline, sets up signal handlers for SIGTERM and SIGINT,
/// and ensures proper cleanup on shutdown. The pipeline will attempt to
/// finish processing current batches before terminating.
#[tracing::instrument(skip(pipeline))]
async fn start_pipeline<S, D>(mut pipeline: Pipeline<S, D>) -> ReplicatorResult<()>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    // Start the pipeline.
    pipeline.start().await?;

    // We spawn metrics collection after the pipeline was started, so that if we crash before starting
    // we don't keep emitting metrics that make it look as if the system is running.
    metrics::spawn_metrics_tasks(pipeline.id());

    // Spawn a task to listen for shutdown signals and trigger shutdown.
    let shutdown_tx = pipeline.shutdown_tx();
    let shutdown_handle = tokio::spawn(async move {
        // Listen for SIGTERM, sent by Kubernetes before SIGKILL during pod termination.
        //
        // If the process is killed before shutdown completes, the pipeline may become corrupted,
        // depending on the state store and destination implementations.
        let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
            error!("failed to register sigterm handler, shutting down pipeline");

            if let Err(err) = shutdown_tx.shutdown() {
                warn!(error = %err, "failed to send shutdown signal");
            }

            return;
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("sigint (ctrl+c) received, shutting down pipeline");
            }
            _ = sigterm.recv() => {
                info!("sigterm received, shutting down pipeline");
            }
        }

        if let Err(err) = shutdown_tx.shutdown() {
            warn!(error = %err, "failed to send shutdown signal");
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

    // Propagate any pipeline error.
    result?;

    Ok(())
}
