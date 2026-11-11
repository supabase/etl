//! One-shot source upgrades without constructing a replication pipeline.

use etl::migrations::run_migrations;
use etl_config::{Config, load_config, shared::PgConnectionConfig};
use serde::Deserialize;
use tokio::runtime::Builder;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Minimal configuration for migrations, independent of destinations.
#[derive(Deserialize)]
struct MigrationConfig {
    /// Connection to the source database to upgrade.
    pg_connection: PgConnectionConfig,
    /// Optional metadata database; defaults to the source connection.
    store_pg_connection: Option<PgConnectionConfig>,
}

impl Config for MigrationConfig {
    const LIST_PARSE_KEYS: &'static [&'static str] = &[];
}

/// Applies the image's embedded migrations once and exits.
pub(crate) fn run() -> ReplicatorResult<()> {
    let config = load_config::<MigrationConfig>().map_err(ReplicatorError::config)?;

    Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run_migrations(&config.pg_connection, config.store_pg_connection.as_ref()))?;

    Ok(())
}
