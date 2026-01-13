//! Heartbeat configuration for read replica support.
//!
//! When running ETL pipelines against read replicas, the replication slot on the replica
//! can become invalidated if the primary database recycles WAL segments before the replica
//! consumes them. The heartbeat mechanism prevents this by periodically emitting messages
//! to the primary database, ensuring WAL segments are retained.

use serde::{Deserialize, Serialize};

/// Configuration for heartbeat emission to prevent replication slot invalidation.
///
/// Heartbeats are only relevant when connecting to a read replica. They work by
/// periodically calling `pg_logical_emit_message()` on the primary database,
/// which generates WAL activity and prevents the primary from recycling WAL
/// segments that the replica's replication slot still needs.
///
/// # Example
///
/// ```rust
/// use etl_config::shared::HeartbeatConfig;
///
/// // Use defaults (30s interval, 1-60s backoff, 25% jitter)
/// let config = HeartbeatConfig::default();
///
/// // Or customize
/// let config = HeartbeatConfig {
///     interval_secs: 15,
///     initial_backoff_secs: 2,
///     max_backoff_secs: 120,
///     jitter_percent: 20,
/// };
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// Interval in seconds between heartbeat emissions.
    ///
    /// This should be less than the WAL retention period on the primary to ensure
    /// replication slots are not invalidated. Default is 30 seconds.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,

    /// Initial backoff delay in seconds after a connection failure.
    ///
    /// When the heartbeat connection to the primary fails, the worker will wait
    /// this duration before the first retry. Subsequent retries use exponential
    /// backoff up to `max_backoff_secs`. Default is 1 second.
    #[serde(default = "default_initial_backoff_secs")]
    pub initial_backoff_secs: u64,

    /// Maximum backoff delay in seconds between retry attempts.
    ///
    /// The exponential backoff will not exceed this value. Default is 60 seconds.
    #[serde(default = "default_max_backoff_secs")]
    pub max_backoff_secs: u64,

    /// Percentage of jitter to add to backoff delays (0-100).
    ///
    /// Jitter helps prevent thundering herd problems when multiple pipelines
    /// reconnect simultaneously. A value of 25 means the actual delay will be
    /// the calculated backoff ± 25%. Default is 25.
    #[serde(default = "default_jitter_percent")]
    pub jitter_percent: u8,
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
    25
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
    fn test_default_values() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval_secs, 30);
        assert_eq!(config.initial_backoff_secs, 1);
        assert_eq!(config.max_backoff_secs, 60);
        assert_eq!(config.jitter_percent, 25);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let config = HeartbeatConfig {
            interval_secs: 15,
            initial_backoff_secs: 2,
            max_backoff_secs: 120,
            jitter_percent: 20,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HeartbeatConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.interval_secs, deserialized.interval_secs);
        assert_eq!(config.initial_backoff_secs, deserialized.initial_backoff_secs);
        assert_eq!(config.max_backoff_secs, deserialized.max_backoff_secs);
        assert_eq!(config.jitter_percent, deserialized.jitter_percent);
    }

    #[test]
    fn test_deserialization_with_defaults() {
        // Only specify interval_secs, others should use defaults
        let json = r#"{"interval_secs": 45}"#;
        let config: HeartbeatConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.interval_secs, 45);
        assert_eq!(config.initial_backoff_secs, 1);
        assert_eq!(config.max_backoff_secs, 60);
        assert_eq!(config.jitter_percent, 25);
    }
}
