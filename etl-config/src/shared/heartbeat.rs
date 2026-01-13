//! Heartbeat configuration for read replica replication slot support.
//!
//! When replicating from a read replica, periodic heartbeats must be emitted to the
//! primary database to ensure WAL flows through the replication chain and keeps the
//! replication slot active.

use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Configuration for heartbeat emission to the primary database.
///
/// When replicating from a read replica (PostgreSQL 15+), the replication slot on the
/// primary may become inactive if no WAL activity occurs. This configuration controls
/// periodic heartbeat messages that generate WAL activity to keep the slot active.
///
/// # Example
///
/// ```ignore
/// let config = HeartbeatConfig {
///     interval_secs: 30,
///     initial_backoff_secs: 1,
///     max_backoff_secs: 60,
///     jitter_percent: 20,
/// };
/// ```
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct HeartbeatConfig {
    /// Interval in seconds between heartbeat emissions.
    ///
    /// Default: 30 seconds. Should be less than the `wal_sender_timeout` on the primary
    /// to prevent the replication connection from being terminated.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,

    /// Initial backoff duration in seconds after a connection failure.
    ///
    /// Default: 1 second. The backoff doubles after each failure up to `max_backoff_secs`.
    #[serde(default = "default_initial_backoff_secs")]
    pub initial_backoff_secs: u64,

    /// Maximum backoff duration in seconds between reconnection attempts.
    ///
    /// Default: 60 seconds. Caps the exponential backoff to prevent excessive delays.
    #[serde(default = "default_max_backoff_secs")]
    pub max_backoff_secs: u64,

    /// Percentage of jitter to add/subtract from backoff duration.
    ///
    /// Default: 20%. Adds randomness to prevent thundering herd when multiple
    /// pipelines reconnect simultaneously. Value of 20 means ±20% jitter.
    #[serde(default = "default_jitter_percent")]
    pub jitter_percent: u8,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_interval_secs(),
            initial_backoff_secs: default_initial_backoff_secs(),
            max_backoff_secs: default_max_backoff_secs(),
            jitter_percent: default_jitter_percent(),
        }
    }
}

fn default_interval_secs() -> u64 {
    30
}

fn default_initial_backoff_secs() -> u64 {
    1
}

fn default_max_backoff_secs() -> u64 {
    60
}

fn default_jitter_percent() -> u8 {
    20
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval_secs, 30);
        assert_eq!(config.initial_backoff_secs, 1);
        assert_eq!(config.max_backoff_secs, 60);
        assert_eq!(config.jitter_percent, 20);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = HeartbeatConfig {
            interval_secs: 45,
            initial_backoff_secs: 2,
            max_backoff_secs: 120,
            jitter_percent: 15,
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: HeartbeatConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.interval_secs, decoded.interval_secs);
        assert_eq!(config.initial_backoff_secs, decoded.initial_backoff_secs);
        assert_eq!(config.max_backoff_secs, decoded.max_backoff_secs);
        assert_eq!(config.jitter_percent, decoded.jitter_percent);
    }
}
