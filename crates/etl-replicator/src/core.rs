//! Replicator service orchestration.

use std::net::Ipv4Addr;

use etl::{pipeline::PipelineId, store::PostgresStore};
use etl_config::shared::{PgConnectionConfig, ReplicatorConfig, ReplicatorHealthConfig};
use tokio::{net::TcpListener, task::JoinHandle};
use tracing::{debug, error};

use crate::{
    error::ReplicatorResult,
    error_notification::ErrorNotificationClient,
    error_reporting::ErrorReportingStateStore,
    health::{self, ReplicatorHealth},
};

mod destinations;
#[cfg(feature = "any-destination")]
mod pipeline;

#[cfg(all(
    feature = "any-destination",
    not(any(
        feature = "bigquery",
        feature = "clickhouse",
        feature = "ducklake",
        feature = "iceberg",
        feature = "snowflake"
    ))
))]
compile_error!("`any-destination` is internal; enable a concrete destination feature instead.");

/// Store type used by the replicator runtime.
type ReplicatorStore = ErrorReportingStateStore<PostgresStore>;

/// Replicator lifecycle updated by the runner and observed by health probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "any-destination"), allow(dead_code))]
pub(crate) enum ReplicatorState {
    /// Store, destination, or pipeline initialization is still in progress.
    Initializing,
    /// Pipeline workers have started.
    Running,
    /// Shutdown was requested or pipeline execution has finished.
    Stopping,
}

/// Binds the optional probe listener and spawns its server task.
///
/// Kubernetes uses `/livez` for startup too: the listener starts before
/// pipeline initialization, so waiting for a replication slot does not consume
/// a fixed startup deadline. `/readyz` remains unavailable until work is
/// observed.
async fn spawn_health_server(
    health_config: Option<ReplicatorHealthConfig>,
    replicator_health: ReplicatorHealth,
) -> ReplicatorResult<Option<JoinHandle<()>>> {
    let Some(health_config) = health_config else {
        return Ok(None);
    };
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, health_config.port)).await?;
    debug!(
        configured_port = health_config.port,
        stall_timeout_ms = health_config.stall_timeout_ms,
        "health server listener bound"
    );
    let router = health::router(replicator_health);

    Ok(Some(tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            error!(error = %error, "health server failed");
        }
    })))
}

/// Initializes the store.
///
/// Creates a [`PostgresStore`] instance for the given pipeline and connection
/// configuration. The pipeline itself owns source migration startup.
async fn init_replicator_store(
    pipeline_id: PipelineId,
    store_pg_connection_config: PgConnectionConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<ReplicatorStore> {
    debug!(pipeline_id, "initializing replicator store");

    Ok(ErrorReportingStateStore::new(
        PostgresStore::new(pipeline_id, store_pg_connection_config).await?,
        notification_client,
    ))
}

/// Starts the replicator service with the provided configuration.
///
/// Initializes the store, creates the appropriate destination based on
/// configuration, and starts the pipeline.
pub(crate) async fn start_replicator_with_config(
    replicator_config: ReplicatorConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<()> {
    let replicator_health = ReplicatorHealth::new(replicator_config.health.unwrap_or_default());
    let health_server_task =
        spawn_health_server(replicator_config.health, replicator_health.clone()).await?;

    let replicator_result = async {
        let pipeline_id = replicator_config.pipeline.id;

        // We initialize the store, using the optional store connection when the
        // replication connection points at a read replica.
        let store_pg_connection_config = replicator_config.pipeline.store_pg_connection().clone();
        let replicator_store =
            init_replicator_store(pipeline_id, store_pg_connection_config, notification_client)
                .await?;

        destinations::start(replicator_config, replicator_store, replicator_health).await
    }
    .await;

    // Stop probes on both completion and initialization failure.
    if let Some(health_server_task) = health_server_task {
        health_server_task.abort();
    }

    replicator_result
}
