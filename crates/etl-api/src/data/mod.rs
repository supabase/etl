use std::sync::LazyLock;

use etl_config::shared::{PgConnectionConfig, PgConnectionOptions};
use etl_postgres::replication::connect_to_source_database;
use sqlx::PgPool;

pub mod destinations;
pub mod destinations_pipelines;
pub mod images;
pub mod pipelines;
pub mod publications;
pub mod replicators;
pub mod sources;
pub mod tables;
pub mod tenants;
pub mod tenants_sources;
pub mod utils;

/// Minimum number of connections for the source Postgres connection pool.
const MIN_POOL_CONNECTIONS: u32 = 1;
/// Maximum number of connections for the source Postgres connection pool.
const MAX_POOL_CONNECTIONS: u32 = 1;
/// Application name for ETL API source database connections.
const APP_NAME_API: &str = "supabase_etl_api";

/// Connection options for API source database queries.
///
/// Uses strict timeouts to keep API requests responsive under contention.
static API_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_API).lock_timeout(5_000).build());

/// Connects to the source database with the specified configuration and default
/// connection pool size.
///
/// Uses state management options with moderate timeouts suitable for
/// administrative queries like listing tables and reading publications. If
/// configured, the source connection uses `hostaddr` as the TCP target and
/// preserves `host` as the canonical database hostname in stored API state.
pub async fn connect_to_source_database_from_api(
    config: &PgConnectionConfig,
) -> Result<PgPool, sqlx::Error> {
    connect_to_source_database(
        config,
        MIN_POOL_CONNECTIONS,
        MAX_POOL_CONNECTIONS,
        Some(&API_OPTIONS),
    )
    .await
}
