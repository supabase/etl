//! Database migrations required by ETL.

use std::sync::LazyLock;

use sqlx::{Connection, Executor, PgConnection, migrate::Migrator, postgres::PgConnectOptions};
use tracing::debug;

use crate::{
    config::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions},
    error::{ErrorKind, EtlResult},
    etl_error,
    postgres::connect_error::migration_connect_error,
};

/// Application name for ETL migration connections.
const APP_NAME_REPLICATOR_MIGRATIONS: &str = "supabase_etl_replicator_migrations";

/// Connection options for ETL migration queries.
///
/// Uses an extended statement timeout for DDL while keeping bounded lock and
/// idle-in-transaction timeouts from the common Postgres defaults.
static MIGRATION_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_REPLICATOR_MIGRATIONS).build());

/// Advisory lock key that serializes creation of the `etl` schema.
///
/// `create schema if not exists` is not safe to run concurrently: two sessions
/// can both find the schema missing and both insert it into `pg_namespace`, and
/// the second insert fails with a unique violation. ETL holds this
/// transaction-level advisory lock around the statement, so pipelines that
/// start together against a fresh database bootstrap it once. Applications that
/// create the schema themselves while a pipeline may be starting should hold
/// the same lock. Advisory locks are scoped to a database, so the fixed key
/// cannot clash across databases.
pub const ETL_SCHEMA_LOCK_ID: i64 = 0x6574_6c5f_7363_6865;

/// Connects before running any SQL so network failures have a precise origin.
async fn connect(
    connection_config: &PgConnectionConfig,
    fallback: ErrorKind,
    description: &'static str,
) -> EtlResult<PgConnection> {
    let options: PgConnectOptions = connection_config.with_db(Some(&MIGRATION_OPTIONS));
    PgConnection::connect_with(&options)
        .await
        .map_err(|err| migration_connect_error(&options, err, fallback, description))
}

/// Prepares a connection for ETL migrations.
async fn prepare_connection(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    // Suppress routine DDL notices so startup logs stay focused on phase-level
    // events.
    conn.execute("set client_min_messages = warning;").await?;

    // Create the `etl` schema if it doesn't exist. The advisory lock is
    // transaction-level: a concurrent session waits for the commit and then
    // finds the schema in place.
    let mut tx = conn.begin().await?;
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(ETL_SCHEMA_LOCK_ID)
        .execute(&mut *tx)
        .await?;
    tx.execute("create schema if not exists etl;").await?;
    tx.commit().await?;

    // Set the `search_path` to `etl` so that the `_sqlx_migrations` metadata
    // table is created inside that schema instead of the public schema.
    conn.execute("set search_path = 'etl';").await?;

    Ok(())
}

/// Returns whether the source database is currently a physical standby.
async fn source_database_in_recovery(connection_config: &PgConnectionConfig) -> EtlResult<bool> {
    const FAILURE: &str = "Failed to inspect source database recovery state";
    let mut conn = connect(connection_config, ErrorKind::SourceConnectionFailed, FAILURE).await?;
    sqlx::query_scalar("select pg_is_in_recovery()")
        .fetch_one(&mut conn)
        .await
        .map_err(|err| etl_error!(ErrorKind::SourceConnectionFailed, FAILURE, source: err))
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
    failure: &'static str,
) -> EtlResult<()> {
    let mut conn = connect(connection_config, ErrorKind::SourceError, failure).await?;
    let migration_error = |err| etl_error!(ErrorKind::SourceError, failure, source: err);
    prepare_connection(&mut conn).await.map_err(migration_error)?;

    debug!(migration_set = label, "applying etl migrations");
    migrator
        .run_direct(None, &mut conn, false)
        .await
        .map_err(sqlx::Error::from)
        .map_err(migration_error)?;
    debug!(migration_set = label, "etl migrations successfully applied");

    Ok(())
}

/// Runs source-side migrations required by every ETL pipeline.
///
/// These migrations install the `etl` schema, schema snapshot helper functions,
/// and the DDL event trigger used by replication. When the configured source is
/// a physical standby, this function skips migration execution because standby
/// connections are read-only. In that setup, source-side migrations must be
/// applied on the primary and then replayed to the standby before the pipeline
/// starts.
///
/// [`crate::pipeline::Pipeline::start`] runs these migrations automatically.
/// This function is public for applications that want to preflight or pre-apply
/// the source-side setup.
pub async fn run_source_migrations(source_config: &PgConnectionConfig) -> EtlResult<()> {
    let in_recovery = source_database_in_recovery(source_config).await?;

    if in_recovery {
        debug!("skipping etl source migrations on standby source database");

        return Ok(());
    }

    run_migration_set(
        source_config,
        source_migrator(),
        "source",
        "Failed to run ETL source migrations",
    )
    .await
}

/// Runs migrations required only by [`crate::store::PostgresStore`].
pub(crate) async fn run_postgres_store_migrations(
    source_config: &PgConnectionConfig,
) -> EtlResult<()> {
    run_migration_set(
        source_config,
        postgres_store_migrator(),
        "postgres_store",
        "Failed to run Postgres store migrations",
    )
    .await
}

/// Applies store and source migrations without starting replication.
///
/// Uses the same migration sets and ordering as replicator startup.
/// An optional separate store connection follows the normal pipeline setting.
/// This is intended for stopped pipelines; callers must keep replication
/// stopped until the upgrade completes. Successful completion does not create
/// replication slots, read pipeline state, or access a destination.
pub async fn run_migrations(
    source_config: &PgConnectionConfig,
    store_config: Option<&PgConnectionConfig>,
) -> EtlResult<()> {
    let in_recovery = source_database_in_recovery(source_config).await?;

    if in_recovery {
        return Err(etl_error!(
            ErrorKind::SourceError,
            "Cannot run explicit migrations against a standby source; configure the writable \
             primary"
        ));
    }

    run_postgres_store_migrations(store_config.unwrap_or(source_config)).await?;
    run_source_migrations(source_config).await
}
