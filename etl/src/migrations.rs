//! Database migrations required by ETL.

use sqlx::{Connection, Executor, PgConnection, migrate::Migrator, postgres::PgConnectOptions};
use tracing::debug;

use crate::{
    config::{ETL_MIGRATION_OPTIONS, IntoConnectOptions, PgConnectionConfig},
    error::{ErrorKind, EtlResult},
    etl_error,
};

/// Creates a PostgreSQL connection prepared for ETL migrations.
async fn create_migration_connection(
    connection_config: &PgConnectionConfig,
) -> Result<PgConnection, sqlx::Error> {
    let options: PgConnectOptions = connection_config.with_db(Some(&ETL_MIGRATION_OPTIONS));

    let mut conn = PgConnection::connect_with(&options).await?;

    // Suppress routine DDL notices so startup logs stay focused on phase-level
    // events.
    conn.execute("set client_min_messages = warning;").await?;

    // Create the `etl` schema if it doesn't exist.
    conn.execute("create schema if not exists etl;").await?;

    // Set the `search_path` to `etl` so that the `_sqlx_migrations`
    // metadata table is created inside that schema instead of the public
    // schema.
    conn.execute("set search_path = 'etl';").await?;

    Ok(conn)
}

/// Returns the migrator for source-side replication helpers.
fn source_migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations/source");
    // Source and Postgres store migrations intentionally share
    // `etl._sqlx_migrations`. Each migrator must ignore versions owned by the
    // other set while still validating checksums for its own versions.
    migrator.set_ignore_missing(true);
    migrator
}

/// Returns the migrator for [`crate::store::PostgresStore`] tables.
fn postgres_store_migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations/postgres_store");
    // See [`source_migrator`] for why split migrators use `ignore_missing`.
    migrator.set_ignore_missing(true);
    migrator
}

/// Runs one ETL migration set against the source database.
async fn run_migration_set(
    connection_config: &PgConnectionConfig,
    migrator: Migrator,
    label: &'static str,
) -> Result<(), sqlx::Error> {
    let mut conn = create_migration_connection(connection_config).await?;

    debug!(migration_set = label, "applying ETL migrations");
    migrator.run_direct(&mut conn).await?;
    debug!(migration_set = label, "ETL migrations successfully applied");

    Ok(())
}

/// Runs source-side migrations required by every ETL pipeline.
///
/// These migrations install the `etl` schema, schema snapshot helper
/// functions, and the DDL event trigger used by replication.
///
/// [`crate::pipeline::Pipeline::start`] runs these migrations automatically.
/// This function is public for applications that want to preflight or
/// pre-apply the source-side setup.
pub async fn run_source_migrations(source_config: &PgConnectionConfig) -> EtlResult<()> {
    run_migration_set(source_config, source_migrator(), "source").await.map_err(|err| {
        etl_error!(ErrorKind::SourceError, "Failed to run ETL source migrations", err)
    })
}

/// Runs migrations required only by [`crate::store::PostgresStore`].
pub(crate) async fn run_postgres_store_migrations(
    source_config: &PgConnectionConfig,
) -> EtlResult<()> {
    run_migration_set(source_config, postgres_store_migrator(), "postgres_store").await.map_err(
        |err| etl_error!(ErrorKind::SourceError, "Failed to run Postgres store migrations", err),
    )
}
