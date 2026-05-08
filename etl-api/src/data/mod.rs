use etl_config::shared::{ETL_API_OPTIONS, PgConnectionConfig};
use etl_postgres::tokio::{PgSourceClient, PgSourceError};

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

/// Connects to the source database with API session options.
///
/// Uses API options with moderate timeouts suitable for
/// administrative queries like listing tables and reading publications. If
/// configured, the source connection uses `hostaddr` as the TCP target and
/// preserves `host` as the canonical database hostname in stored API state.
pub async fn connect_to_source_database_from_api(
    config: &PgConnectionConfig,
) -> Result<PgSourceClient, PgSourceError> {
    PgSourceClient::connect_with_options(config, Some(&ETL_API_OPTIONS)).await
}
