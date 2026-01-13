//! Configuration for the heartbeat worker.
//!
//! The heartbeat worker maintains replication slot activity when replicating from
//! read replicas by periodically emitting WAL messages to the primary database.

use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::ValidationError;

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

impl HeartbeatConfig {
    /// Validates heartbeat configuration settings.
    ///
    /// Checks that jitter_percent is within 0-100 range and that
    /// min_backoff_ms does not exceed max_backoff_ms.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.jitter_percent > 100 {
            return Err(ValidationError::InvalidFieldValue {
                field: "jitter_percent".to_string(),
                constraint: "must be between 0 and 100".to_string(),
            });
        }

        if self.min_backoff_ms > self.max_backoff_ms {
            return Err(ValidationError::InvalidFieldValue {
                field: "min_backoff_ms".to_string(),
                constraint: format!(
                    "must be less than or equal to max_backoff_ms ({})",
                    self.max_backoff_ms
                ),
            });
        }

        Ok(())
    }
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

    #[test]
    fn test_validate_valid_config() {
        let config = HeartbeatConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_jitter_percent_too_high() {
        let config = HeartbeatConfig {
            jitter_percent: 150,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        if let Err(ValidationError::InvalidFieldValue { field, .. }) = result {
            assert_eq!(field, "jitter_percent");
        }
    }

    #[test]
    fn test_validate_min_greater_than_max_backoff() {
        let config = HeartbeatConfig {
            min_backoff_ms: 10_000,
            max_backoff_ms: 5_000,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        if let Err(ValidationError::InvalidFieldValue { field, .. }) = result {
            assert_eq!(field, "min_backoff_ms");
        }
    }

    #[test]
    fn test_validate_boundary_jitter_percent() {
        // 100 should be valid
        let config = HeartbeatConfig {
            jitter_percent: 100,
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        // 0 should be valid
        let config = HeartbeatConfig {
            jitter_percent: 0,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_equal_backoff_values() {
        // min == max should be valid
        let config = HeartbeatConfig {
            min_backoff_ms: 5_000,
            max_backoff_ms: 5_000,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}
