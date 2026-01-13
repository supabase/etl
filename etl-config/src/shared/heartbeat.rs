//! Configuration for the heartbeat worker.
//!
//! The heartbeat worker maintains replication slot activity when replicating from
//! read replicas by periodically emitting WAL messages to the primary database.

use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Configuration for the heartbeat worker.
///
/// The heartbeat worker periodically emits `pg_logical_emit_message()` calls to
/// the primary database to keep the replication slot active when replicating
/// from a read replica.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct HeartbeatConfig {
    /// Interval in milliseconds between heartbeat emissions.
    ///
    /// Default: 30000 (30 seconds).
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,

    /// Minimum backoff in milliseconds after a failed heartbeat attempt.
    ///
    /// Default: 1000 (1 second).
    #[serde(default = "default_min_backoff_ms")]
    pub min_backoff_ms: u64,

    /// Maximum backoff in milliseconds after repeated failures.
    ///
    /// Default: 60000 (60 seconds).
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,

    /// Jitter percentage (0-100) to add randomness to backoff timing.
    ///
    /// Default: 25.
    #[serde(default = "default_jitter_percent")]
    pub jitter_percent: u8,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_ms: default_interval_ms(),
            min_backoff_ms: default_min_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            jitter_percent: default_jitter_percent(),
        }
    }
}

fn default_interval_ms() -> u64 {
    30_000
}

fn default_min_backoff_ms() -> u64 {
    1_000
}

fn default_max_backoff_ms() -> u64 {
    60_000
}

fn default_jitter_percent() -> u8 {
    25
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval_ms, 30_000);
        assert_eq!(config.min_backoff_ms, 1_000);
        assert_eq!(config.max_backoff_ms, 60_000);
        assert_eq!(config.jitter_percent, 25);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let config = HeartbeatConfig {
            interval_ms: 15_000,
            min_backoff_ms: 500,
            max_backoff_ms: 30_000,
            jitter_percent: 10,
        };
        let json = serde_json::to_string(&config).unwrap();
        let decoded: HeartbeatConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.interval_ms, decoded.interval_ms);
        assert_eq!(config.min_backoff_ms, decoded.min_backoff_ms);
        assert_eq!(config.max_backoff_ms, decoded.max_backoff_ms);
        assert_eq!(config.jitter_percent, decoded.jitter_percent);
    }
}
