//! Replicator service orchestration.

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

use etl::{store::both::postgres::PostgresStore, types::PipelineId};
use etl_config::shared::{PgConnectionConfig, ReplicatorConfig};
use tracing::info;

use crate::{
    error::ReplicatorResult, error_notification::ErrorNotificationClient,
    error_reporting::ErrorReportingStateStore,
};

/// Store type used by the replicator runtime.
type ReplicatorStore = ErrorReportingStateStore<PostgresStore>;

/// Starts the replicator service with the provided configuration.
///
/// Initializes the store, creates the appropriate destination based on
/// configuration, and starts the pipeline.
pub(crate) async fn start_replicator_with_config(
    replicator_config: ReplicatorConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<()> {
    let pipeline_id = replicator_config.pipeline.id;

    // We initialize the store, using the optional store connection when the
    // replication connection points at a read replica.
    let store_pg_connection_config = replicator_config.pipeline.store_pg_connection().clone();
    let store = init_store(pipeline_id, store_pg_connection_config, notification_client).await?;

    destinations::start(replicator_config, store).await
}

/// Initializes the store.
///
/// Creates a [`PostgresStore`] instance for the given pipeline and connection
/// configuration. The pipeline itself owns source migration startup.
async fn init_store(
    pipeline_id: PipelineId,
    store_pg_connection_config: PgConnectionConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<ReplicatorStore> {
    info!("initializing postgres store");

    Ok(ErrorReportingStateStore::new(
        PostgresStore::new(pipeline_id, store_pg_connection_config).await?,
        notification_client,
    ))
}
