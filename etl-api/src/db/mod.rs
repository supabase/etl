use etl_config::shared::PgConnectionConfig;
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

/// Connects to the source database with the specified configuration and default connection pool size.
pub async fn connect_to_source_database_with_defaults(
    config: &PgConnectionConfig,
) -> Result<PgPool, sqlx::Error> {
    connect_to_source_database(config, MIN_POOL_CONNECTIONS, MAX_POOL_CONNECTIONS).await
}
