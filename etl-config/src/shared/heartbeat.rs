//! Heartbeat configuration for read replica replication slot maintenance.

use crate::shared::ValidationError;
use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Configuration for the heartbeat worker that maintains replication slot activity.
///
/// When replicating from a read replica, the replication slot on the primary can
/// become inactive during idle periods. The heartbeat worker periodically emits
/// WAL messages to keep the slot active.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct HeartbeatConfig {
    /// Interval in milliseconds between heartbeat emissions.
    ///
    /// Default: 30000 (30 seconds)
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,

    /// Minimum backoff duration in milliseconds after a failed heartbeat attempt.
    ///
    /// Default: 1000 (1 second)
    #[serde(default = "default_min_backoff_ms")]
    pub min_backoff_ms: u64,

    /// Maximum backoff duration in milliseconds after repeated failures.
    ///
    /// Default: 60000 (60 seconds)
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,

    /// Jitter percentage to apply to backoff duration (0-100).
    ///
    /// Helps prevent thundering herd when multiple workers reconnect.
    /// Default: 25
    #[serde(default = "default_jitter_percent")]
    pub jitter_percent: u8,
}

impl HeartbeatConfig {
    /// Default heartbeat interval: 30 seconds.
    pub const DEFAULT_INTERVAL_MS: u64 = 30_000;

    /// Default minimum backoff: 1 second.
    pub const DEFAULT_MIN_BACKOFF_MS: u64 = 1_000;

    /// Default maximum backoff: 60 seconds.
    pub const DEFAULT_MAX_BACKOFF_MS: u64 = 60_000;

    /// Default jitter percentage: 25%.
    pub const DEFAULT_JITTER_PERCENT: u8 = 25;

    /// Validates the heartbeat configuration.
    ///
    /// Ensures jitter_percent is <= 100 and min_backoff_ms <= max_backoff_ms.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.jitter_percent > 100 {
            return Err(ValidationError::InvalidFieldValue {
                field: "jitter_percent".to_string(),
                constraint: "must be <= 100".to_string(),
            });
        }

        if self.min_backoff_ms > self.max_backoff_ms {
            return Err(ValidationError::InvalidFieldValue {
                field: "min_backoff_ms".to_string(),
                constraint: "must be <= max_backoff_ms".to_string(),
            });
        }

        Ok(())
    }
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_ms: Self::DEFAULT_INTERVAL_MS,
            min_backoff_ms: Self::DEFAULT_MIN_BACKOFF_MS,
            max_backoff_ms: Self::DEFAULT_MAX_BACKOFF_MS,
            jitter_percent: Self::DEFAULT_JITTER_PERCENT,
        }
    }
}

fn default_interval_ms() -> u64 {
    HeartbeatConfig::DEFAULT_INTERVAL_MS
}

fn default_min_backoff_ms() -> u64 {
    HeartbeatConfig::DEFAULT_MIN_BACKOFF_MS
}

fn default_max_backoff_ms() -> u64 {
    HeartbeatConfig::DEFAULT_MAX_BACKOFF_MS
}

fn default_jitter_percent() -> u8 {
    HeartbeatConfig::DEFAULT_JITTER_PERCENT
}

/// Connection options optimized for heartbeat connections.
///
/// Uses shorter timeouts since heartbeat connections are lightweight
/// health checks that should fail fast.
pub const ETL_HEARTBEAT_OPTIONS: &str = concat!(
    "application_name=etl_heartbeat",
    " statement_timeout=5000",
    " lock_timeout=5000",
    " idle_in_transaction_session_timeout=30000",
);

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
    fn test_heartbeat_options() {
        assert!(ETL_HEARTBEAT_OPTIONS.contains("application_name=etl_heartbeat"));
        assert!(ETL_HEARTBEAT_OPTIONS.contains("statement_timeout=5000"));
    }

    #[test]
    fn test_validate_valid_config() {
        let config = HeartbeatConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_jitter_too_high() {
        let config = HeartbeatConfig {
            jitter_percent: 101,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_min_greater_than_max() {
        let config = HeartbeatConfig {
            min_backoff_ms: 10_000,
            max_backoff_ms: 1_000,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
