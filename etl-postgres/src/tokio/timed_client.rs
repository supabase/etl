use std::{
    fmt,
    ops::{Deref, DerefMut},
    time::{Duration, Instant},
};

use etl_config::shared::{PgConnectionConfig, PgConnectionOptions};
use tracing::debug;

use crate::tokio::{PgSourceClient, PgSourceError};

/// Default idle timeout for cached source database connections.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// A lazily connected source database client with idle expiry.
///
/// The cached connection is reused until it is closed by the server/client or
/// remains idle beyond the configured timeout. The next checkout recreates the
/// connection.
pub struct TimedPgSourceClient {
    /// Source database connection configuration.
    config: PgConnectionConfig,
    /// Session options used when opening a source database connection.
    options: Option<PgConnectionOptions>,
    /// Maximum idle time before the cached connection is dropped.
    idle_timeout: Duration,
    /// Cached source database client.
    client: Option<PgSourceClient>,
    /// Last time a checked-out client lease was dropped.
    last_used_at: Option<Instant>,
}

/// A checked-out cached source database client.
///
/// Dropping the lease updates the parent client's last-used timestamp, so idle
/// expiry is measured from the end of each database operation scope.
pub struct TimedPgSourceClientLease<'a> {
    /// Checked-out source database client.
    client: &'a mut PgSourceClient,
    /// Parent last-used timestamp updated when the lease is dropped.
    last_used_at: &'a mut Option<Instant>,
}

impl fmt::Debug for TimedPgSourceClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimedPgSourceClient")
            .field("idle_timeout", &self.idle_timeout)
            .field("has_client", &self.client.is_some())
            .field("last_used_at", &self.last_used_at)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for TimedPgSourceClientLease<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimedPgSourceClientLease").finish_non_exhaustive()
    }
}

impl Drop for TimedPgSourceClientLease<'_> {
    fn drop(&mut self) {
        *self.last_used_at = Some(Instant::now());
    }
}

impl Deref for TimedPgSourceClientLease<'_> {
    type Target = PgSourceClient;

    fn deref(&self) -> &Self::Target {
        self.client
    }
}

impl DerefMut for TimedPgSourceClientLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client
    }
}

impl TimedPgSourceClient {
    /// Creates a lazily connected client with the default idle timeout.
    pub fn new(config: PgConnectionConfig, options: Option<&PgConnectionOptions>) -> Self {
        Self::with_idle_timeout(config, options, DEFAULT_IDLE_TIMEOUT)
    }

    /// Creates a lazily connected client with a custom idle timeout.
    pub fn with_idle_timeout(
        config: PgConnectionConfig,
        options: Option<&PgConnectionOptions>,
        idle_timeout: Duration,
    ) -> Self {
        Self { config, options: options.cloned(), idle_timeout, client: None, last_used_at: None }
    }

    /// Checks out the cached client, reconnecting if it is missing, closed, or
    /// idle.
    pub async fn checkout(&mut self) -> Result<TimedPgSourceClientLease<'_>, PgSourceError> {
        if self.should_reconnect() {
            self.reconnect().await?;
        }

        let client = self.client.as_mut().expect("source client is connected after checkout");
        Ok(TimedPgSourceClientLease { client, last_used_at: &mut self.last_used_at })
    }

    /// Returns whether the cached client should be recreated.
    fn should_reconnect(&self) -> bool {
        let Some(client) = &self.client else {
            return true;
        };

        if client.is_closed() {
            return true;
        }

        self.last_used_at.is_some_and(|last_used_at| last_used_at.elapsed() >= self.idle_timeout)
    }

    /// Drops the cached client and opens a fresh connection.
    async fn reconnect(&mut self) -> Result<(), PgSourceError> {
        if self.client.is_some() {
            debug!("reconnecting cached source postgres client");
        }

        self.client = None;
        self.last_used_at = None;
        self.client =
            Some(PgSourceClient::connect_with_options(&self.config, self.options.as_ref()).await?);

        Ok(())
    }
}
