//! Replicator service orchestration.

use std::net::Ipv4Addr;

use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    pipeline::PipelineId,
    store::PostgresStore,
    task::abort_and_join,
};
use etl_config::shared::{PgConnectionConfig, ReplicatorConfig, ReplicatorHealthConfig};
use tokio::net::TcpListener;
use tokio_util::task::AbortOnDropHandle;
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
mod shutdown;

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
) -> ReplicatorResult<Option<AbortOnDropHandle<EtlResult<()>>>> {
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

    Ok(Some(AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router).await.map_err(|error| {
            error!(error = %error, "health server failed");
            etl_error!(ErrorKind::InvalidState, "Health server failed", source: error)
        })
    }))))
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
    // Register before any asynchronous initialization so startup remains
    // cancellable.
    let mut shutdown_signal = shutdown::ShutdownSignal::new()?;
    let replicator_health = ReplicatorHealth::new(replicator_config.health.unwrap_or_default());
    let health_server_task =
        spawn_health_server(replicator_config.health, replicator_health.clone()).await?;

    let replicator_result = async {
        let pipeline_id = replicator_config.pipeline.id;

        // We initialize the store, using the optional store connection when the
        // replication connection points at a read replica.
        let store_pg_connection_config = replicator_config.pipeline.store_pg_connection().clone();
        let replicator_store = tokio::select! {
            biased;

            _ = shutdown_signal.wait() => return Ok(()),

            result = init_replicator_store(pipeline_id, store_pg_connection_config, notification_client) => result?,
        };

        destinations::start(
            replicator_config,
            replicator_store,
            &mut shutdown_signal,
            &replicator_health,
        )
        .await
    }
    .await;

    replicator_health.set_replicator_state(ReplicatorState::Stopping);

    // Probes are disposable, but joining still reports server failures and panics.
    let health_result = if let Some(health_server_task) = health_server_task {
        abort_and_join(health_server_task).await.and_then(|result| result.unwrap_or(Ok(())))
    } else {
        Ok(())
    };

    match (replicator_result, health_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Err(error), Err(health_error)) => Err(EtlError::from(vec![
            etl_error!(ErrorKind::InvalidState, "Replicator failed", source: error),
            health_error,
        ])
        .into()),
    }
}
