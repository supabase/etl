//! Shared source database pool for out-of-band ETL queries.

use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::config::{ETL_OUT_OF_BAND_OPTIONS, IntoConnectOptions, PgConnectionConfig};

/// Maximum number of connections in the out-of-band pool.
const MAX_POOL_CONNECTIONS: u32 = 1;
/// Minimum duration after which idle out-of-band connections are closed.
const MIN_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Extra idle time kept beyond the configured lag refresh interval.
const IDLE_TIMEOUT_REFRESH_PADDING: Duration = Duration::from_secs(30);

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
        let connect_options = connection_config.with_db(Some(&ETL_OUT_OF_BAND_OPTIONS));
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
}
