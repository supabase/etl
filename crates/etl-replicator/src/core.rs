//! Replicator service orchestration.

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    pipeline::PipelineId,
    store::PostgresStore,
    task::abort_and_join_result,
};
use etl_config::shared::{PipelineConfig, ReplicatorConfig, ReplicatorHealthConfig};
use etl_telemetry::listener::bind_listener;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error};

use crate::{
    core::shutdown::with_shutdown,
    error::ReplicatorResult,
    error_reporting::ErrorReportingStateStore,
    health::{self, ReplicatorHealth},
    management_api::ManagementApiClient,
    source_repair::SourceRepair,
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
        feature = "snowflake"
    ))
))]
compile_error!("`any-destination` is internal; enable a concrete destination feature instead.");

/// Store type used by the replicator runtime.
type ReplicatorStore = ErrorReportingStateStore<PostgresStore>;

/// Replicator lifecycle updated by the runner and observed by health probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(any(feature = "any-destination", test)), expect(dead_code))]
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
fn spawn_health_server(
    health_config: Option<ReplicatorHealthConfig>,
    replicator_health: ReplicatorHealth,
) -> ReplicatorResult<Option<AbortOnDropHandle<EtlResult<()>>>> {
    let Some(health_config) = health_config else {
        return Ok(None);
    };
    let listener = bind_listener(health_config.port.get())?;

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
    config: &PipelineConfig,
    notification_client: Option<ManagementApiClient>,
    source_repair: Option<&SourceRepair<'_>>,
) -> ReplicatorResult<ReplicatorStore> {
    let pipeline_id = PipelineId::from(&config.id);
    debug!(%pipeline_id, "initializing replicator store");

    let store_config = config.store_pg_connection();
    let store = PostgresStore::new(pipeline_id, store_config.clone()).await;
    // A separate metadata primary can fail while the replication source is
    // healthy.
    if let Err(error) = &store
        && let Some(repair) = source_repair
        && store_config.host == config.pg_connection.host
        && store_config.hostaddr == config.pg_connection.hostaddr
        && store_config.port == config.pg_connection.port
    {
        repair.startup(error).await;
    }
    Ok(ErrorReportingStateStore::new(store?, notification_client))
}

/// Starts the replicator service with the provided configuration.
///
/// Initializes the store, creates the appropriate destination based on
/// configuration, and starts the pipeline.
pub(crate) async fn start_replicator_with_config(
    replicator_config: ReplicatorConfig,
    notification_client: Option<ManagementApiClient>,
    source_repair: Option<&SourceRepair<'_>>,
) -> ReplicatorResult<()> {
    // Register before any asynchronous initialization so startup remains
    // cancellable.
    let mut shutdown_signal = shutdown::ShutdownSignal::new()?;
    let replicator_health = ReplicatorHealth::new(replicator_config.health.unwrap_or_default());
    let health_server_task =
        spawn_health_server(replicator_config.health, replicator_health.clone())?;

    let replicator_result: ReplicatorResult<()> = async {
        // We initialize the store, using the optional store connection when the
        // replication connection points at a read replica.
        let Some(result) = with_shutdown!(
            init_replicator_store(&replicator_config.pipeline, notification_client, source_repair),
            shutdown_signal.wait(),
        ) else {
            return Ok(());
        };

        let replicator_store = result?;

        destinations::start(
            replicator_config,
            replicator_store.clone(),
            &mut shutdown_signal,
            &replicator_health,
            source_repair,
        )
        .await?;

        replicator_store.shutdown().await?;

        Ok(())
    }
    .await;

    replicator_health.set_replicator_state(ReplicatorState::Stopping);

    replicator_result?;

    // On failure the handle drops; successful teardown also observes the
    // server result.
    if let Some(health_server_task) = health_server_task {
        abort_and_join_result(health_server_task).await?;
    }

    Ok(())
}
