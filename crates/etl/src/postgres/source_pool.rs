//! Shared source database pool for out-of-band ETL queries.

use std::{str::FromStr, sync::LazyLock, time::Duration};

use sqlx::{
    PgPool,
    postgres::{PgPoolOptions, types::Oid as SqlxTableId},
};
use tokio_postgres::types::PgLsn;

use crate::{
    bail,
    config::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions},
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::TableId,
};

/// Maximum number of connections in the out-of-band pool.
const MAX_POOL_CONNECTIONS: u32 = 1;
/// Minimum duration after which idle out-of-band connections are closed.
const MIN_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Extra idle time kept beyond the configured lag refresh interval.
const IDLE_TIMEOUT_REFRESH_PADDING: Duration = Duration::from_secs(30);
/// Application name for ETL out-of-band source database connections.
const APP_NAME_REPLICATOR_OUT_OF_BAND: &str = "supabase_etl_replicator_out_of_band";

/// Publication membership observed at a source WAL position.
#[derive(Debug, Clone)]
pub(crate) struct PublicationTableSnapshot {
    /// Table identifiers visible in the publication catalog snapshot.
    pub(crate) table_ids: Vec<TableId>,
    /// Source WAL position that follows the catalog snapshot's visible commits.
    pub(crate) barrier_lsn: PgLsn,
}

/// Connection options for out-of-band source database queries.
///
/// Uses the common bounded-query Postgres defaults because lag sampling and
/// publication membership queries should be quick and should not block source
/// database work.
static OUT_OF_BAND_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_REPLICATOR_OUT_OF_BAND).build());

/// Shared lazy pool for out-of-band source database queries.
#[derive(Debug, Clone)]
pub(crate) struct OutOfBandSourcePool {
    pool: PgPool,
}

impl OutOfBandSourcePool {
    /// Creates a new lazy out-of-band source pool.
    pub(crate) fn new(
        connection_config: &PgConnectionConfig,
        replication_lag_refresh_interval: Duration,
    ) -> Self {
        let connect_options = connection_config.with_db(Some(&OUT_OF_BAND_OPTIONS));
        let idle_timeout = replication_lag_refresh_interval
            .saturating_add(IDLE_TIMEOUT_REFRESH_PADDING)
            .max(MIN_IDLE_TIMEOUT);
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .max_connections(MAX_POOL_CONNECTIONS)
            .idle_timeout(Some(idle_timeout))
            .connect_lazy_with(connect_options);

        Self { pool }
    }

    /// Returns the underlying SQLx pool.
    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Queries the source database's current WAL LSN.
    pub(crate) async fn get_current_wal_lsn(&self) -> EtlResult<PgLsn> {
        let current_wal_lsn: String = sqlx::query_scalar("select pg_current_wal_lsn()::text")
            .fetch_one(self.pool())
            .await
            .map_err(|error| {
                etl_error!(
                    ErrorKind::SourceConnectionFailed,
                    "Source current LSN query failed",
                    source: error
                )
            })?;

        PgLsn::from_str(&current_wal_lsn).map_err(|_| {
            etl_error!(
                ErrorKind::InvalidState,
                "Invalid source current LSN returned by Postgres",
                current_wal_lsn
            )
        })
    }

    /// Queries publication membership and a source WAL barrier in one
    /// statement.
    ///
    /// Primaries use the current WAL insert position. Standbys use the replay
    /// position because logical decoding there cannot advance beyond replayed
    /// WAL.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::ConfigError`] if the publication contains no
    /// tables.
    pub(crate) async fn get_publication_table_snapshot(
        &self,
        publication_name: &str,
    ) -> EtlResult<PublicationTableSnapshot> {
        let rows: Vec<(Option<SqlxTableId>, String)> = sqlx::query_as(
            r#"
            with publication_tables as materialized (
                select distinct gpt.relid::oid as table_id
                from pg_get_publication_tables($1) gpt
            )
            select
                publication_tables.table_id,
                case
                    when pg_is_in_recovery() then pg_last_wal_replay_lsn()
                    else pg_current_wal_lsn()
                end::text as barrier_lsn
            from (select 1) singleton
            left join publication_tables on true
            order by publication_tables.table_id
            "#,
        )
        .bind(publication_name)
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            etl_error!(
                ErrorKind::SourceQueryFailed,
                "Publication table snapshot query failed",
                source: error
            )
        })?;

        let barrier_lsn = rows
            .first()
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Publication table snapshot query returned no rows"
                )
            })?
            .1
            .parse::<PgLsn>()
            .map_err(|_| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Invalid publication barrier LSN returned by Postgres",
                    rows.first().map(|row| row.1.clone()).unwrap_or_default()
                )
            })?;
        let table_ids = rows
            .into_iter()
            .filter_map(|(table_id, _)| table_id.map(|table_id| TableId::new(table_id.0)))
            .collect::<Vec<_>>();

        if table_ids.is_empty() {
            bail!(
                ErrorKind::ConfigError,
                "Publication has no tables",
                format!(
                    "Publication '{}' does not contain any tables. Ensure the publication is \
                     configured with tables using FOR TABLE, FOR ALL TABLES, or FOR TABLES IN \
                     SCHEMA.",
                    publication_name
                )
            );
        }

        Ok(PublicationTableSnapshot { table_ids, barrier_lsn })
    }
}
