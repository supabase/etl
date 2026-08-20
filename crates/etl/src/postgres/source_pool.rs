//! Shared source database pool for out-of-band ETL queries.

use std::{str::FromStr, sync::LazyLock, time::Duration};

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio_postgres::types::PgLsn;

use crate::{
    config::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions},
    error::{ErrorKind, EtlResult},
    etl_error,
    postgres::client::SlotState,
};

/// Maximum number of connections in the out-of-band pool.
const MAX_POOL_CONNECTIONS: u32 = 1;
/// Minimum duration after which idle out-of-band connections are closed.
const MIN_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Extra idle time kept beyond the configured lag refresh interval.
const IDLE_TIMEOUT_REFRESH_PADDING: Duration = Duration::from_secs(30);
/// Application name for ETL out-of-band source database connections.
const APP_NAME_REPLICATOR_OUT_OF_BAND: &str = "supabase_etl_replicator_out_of_band";

/// Connection options for out-of-band source database queries.
///
/// Uses the common bounded-query Postgres defaults because lag sampling queries
/// should be quick and should not block source database work.
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
        table_sync_monitor_refresh_interval: Duration,
    ) -> Self {
        let connect_options = connection_config.with_db(Some(&OUT_OF_BAND_OPTIONS));
        let idle_timeout = table_sync_monitor_refresh_interval
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

    /// Queries the current state of the replication slot named `slot_name`.
    ///
    /// Returns [`SlotState::Invalidated`] when the slot's `wal_status` is
    /// `lost`, which happens once PostgreSQL has removed WAL segments the slot
    /// still needed. Returns [`SlotState::NotInvalidated`] when the slot exists
    /// and is not known to be invalidated, including when its `wal_status` is
    /// `NULL`.
    ///
    /// Returns an error if the slot does not exist.
    pub(crate) async fn get_slot_state(&self, slot_name: &str) -> EtlResult<SlotState> {
        let row: Option<Option<String>> =
            sqlx::query_scalar("select wal_status from pg_replication_slots where slot_name = $1")
                .bind(slot_name)
                .fetch_optional(self.pool())
                .await
                .map_err(|error| {
                    etl_error!(
                        ErrorKind::SourceConnectionFailed,
                        "Replication slot state query failed",
                        source: error
                    )
                })?;

        let Some(wal_status) = row else {
            return Err(etl_error!(
                ErrorKind::ReplicationSlotNotFound,
                "Replication slot not found",
                format!("Replication slot '{slot_name}' not found in database")
            ));
        };

        // A NULL status means PostgreSQL cannot determine WAL availability from the
        // slot's restart LSN, for example because the slot has not reserved WAL yet.
        Ok(match wal_status.as_deref() {
            Some("lost") => SlotState::Invalidated,
            Some(_) | None => SlotState::NotInvalidated,
        })
    }
}
