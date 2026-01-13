//! Heartbeat configuration for read replica replication slot support.
//!
//! When replicating from a read replica, the replication slot exists on the primary
//! but the ETL pipeline connects to the replica. Without activity on the primary,
//! the slot can become invalid. The heartbeat worker periodically emits messages
//! to the primary to keep the slot active.

use serde::{Deserialize, Serialize};

/// Default interval between heartbeat emissions in seconds.
const fn default_interval_secs() -> u64 {
    30
}

/// Default initial backoff delay for reconnection attempts in seconds.
const fn default_initial_backoff_secs() -> u64 {
    1
}

/// Default maximum backoff delay for reconnection attempts in seconds.
const fn default_max_backoff_secs() -> u64 {
    60
}

/// Default jitter percentage for backoff randomization.
const fn default_jitter_percent() -> u8 {
    25
}

/// Configuration for the heartbeat worker that keeps replication slots active.
///
/// When using read replica mode, the heartbeat worker connects to the primary
/// database and periodically calls `pg_logical_emit_message()` to generate WAL
/// activity. This prevents the replication slot from being invalidated due to
/// inactivity.
///
/// # Backoff Strategy
///
/// On connection failures, the worker uses exponential backoff with jitter:
/// - Starts at `initial_backoff_secs`
/// - Doubles on each failure up to `max_backoff_secs`
/// - Adds random jitter (±`jitter_percent`%) to prevent thundering herd
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// Interval in seconds between heartbeat emissions.
    ///
    /// Should be less than the `wal_sender_timeout` on the primary (default 60s)
    /// to ensure the slot remains active. Default: 30 seconds.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,

    /// Initial delay in seconds before retrying after a connection failure.
    ///
    /// Default: 1 second.
    #[serde(default = "default_initial_backoff_secs")]
    pub initial_backoff_secs: u64,

    /// Maximum delay in seconds between reconnection attempts.
    ///
    /// The backoff delay will not exceed this value. Default: 60 seconds.
    #[serde(default = "default_max_backoff_secs")]
    pub max_backoff_secs: u64,

    /// Percentage of jitter to add to backoff delays (0-100).
    ///
    /// Jitter helps prevent multiple workers from reconnecting simultaneously.
    /// A value of 25 means ±25% randomization. Default: 25%.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval_secs, 30);
        assert_eq!(config.initial_backoff_secs, 1);
        assert_eq!(config.max_backoff_secs, 60);
        assert_eq!(config.jitter_percent, 25);
    }

    #[test]
    fn test_serde_defaults() {
        let json = r#"{}"#;
        let config: HeartbeatConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.interval_secs, 30);
        assert_eq!(config.initial_backoff_secs, 1);
        assert_eq!(config.max_backoff_secs, 60);
        assert_eq!(config.jitter_percent, 25);
    }

    #[test]
    fn test_serde_custom_values() {
        let json = r#"{
            "interval_secs": 15,
            "initial_backoff_secs": 2,
            "max_backoff_secs": 120,
            "jitter_percent": 10
        }"#;
        let config: HeartbeatConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.interval_secs, 15);
        assert_eq!(config.initial_backoff_secs, 2);
        assert_eq!(config.max_backoff_secs, 120);
        assert_eq!(config.jitter_percent, 10);
    }
}
