//! Heartbeat configuration for read replica replication support.
//!
//! When streaming from a read replica, periodic heartbeat messages must be sent
//! to the primary database via `pg_logical_emit_message()` to prevent WAL accumulation.

use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Configuration for heartbeat operations when using replica mode.
///
/// Heartbeats are periodic messages sent to the primary database to advance
/// WAL checkpoints and prevent WAL accumulation during idle periods.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct HeartbeatConfig {
    /// Seconds between heartbeat emissions.
    ///
    /// Default: 30 seconds. Must be between 1 and 300 seconds.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,

    /// Initial reconnection backoff in seconds.
    ///
    /// Default: 1 second. Must be greater than 0.
    #[serde(default = "default_initial_backoff_secs")]
    pub initial_backoff_secs: u64,

    /// Maximum reconnection backoff in seconds.
    ///
    /// Default: 60 seconds. Must be >= initial_backoff_secs.
    #[serde(default = "default_max_backoff_secs")]
    pub max_backoff_secs: u64,

    /// Random jitter percentage for backoff (0-50).
    ///
    /// Default: 25%. Prevents thundering herd on reconnection.
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

impl HeartbeatConfig {
    /// Validates heartbeat configuration settings.
    ///
    /// Ensures interval and backoff values are within acceptable ranges.
    pub fn validate(&self) -> Result<(), HeartbeatConfigError> {
        if self.interval_secs == 0 || self.interval_secs > 300 {
            return Err(HeartbeatConfigError::InvalidInterval {
                value: self.interval_secs,
            });
        }

        if self.initial_backoff_secs == 0 {
            return Err(HeartbeatConfigError::InvalidInitialBackoff);
        }

        if self.max_backoff_secs < self.initial_backoff_secs {
            return Err(HeartbeatConfigError::MaxBackoffTooSmall {
                max: self.max_backoff_secs,
                initial: self.initial_backoff_secs,
            });
        }

        if self.jitter_percent > 50 {
            return Err(HeartbeatConfigError::InvalidJitterPercent {
                value: self.jitter_percent,
            });
        }

        Ok(())
    }
}

/// Errors that can occur during heartbeat configuration validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum HeartbeatConfigError {
    /// Heartbeat interval must be between 1 and 300 seconds.
    #[error("heartbeat interval must be between 1 and 300 seconds, got {value}")]
    InvalidInterval { value: u64 },

    /// Initial backoff must be greater than 0.
    #[error("initial backoff must be greater than 0")]
    InvalidInitialBackoff,

    /// Maximum backoff must be >= initial backoff.
    #[error("max_backoff_secs ({max}) must be >= initial_backoff_secs ({initial})")]
    MaxBackoffTooSmall { max: u64, initial: u64 },

    /// Jitter percent must be between 0 and 50.
    #[error("jitter_percent must be between 0 and 50, got {value}")]
    InvalidJitterPercent { value: u8 },
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
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_invalid_interval() {
        let config = HeartbeatConfig {
            interval_secs: 0,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(HeartbeatConfigError::InvalidInterval { .. })
        ));

        let config = HeartbeatConfig {
            interval_secs: 301,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(HeartbeatConfigError::InvalidInterval { .. })
        ));
    }

    #[test]
    fn test_validation_invalid_backoff() {
        let config = HeartbeatConfig {
            initial_backoff_secs: 0,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(HeartbeatConfigError::InvalidInitialBackoff)
        ));

        let config = HeartbeatConfig {
            initial_backoff_secs: 10,
            max_backoff_secs: 5,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(HeartbeatConfigError::MaxBackoffTooSmall { .. })
        ));
    }

    #[test]
    fn test_validation_invalid_jitter() {
        let config = HeartbeatConfig {
            jitter_percent: 51,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(HeartbeatConfigError::InvalidJitterPercent { .. })
        ));
    }

    #[test]
    fn test_serialization() {
        let config = HeartbeatConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let decoded: HeartbeatConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.interval_secs, decoded.interval_secs);
        assert_eq!(config.initial_backoff_secs, decoded.initial_backoff_secs);
        assert_eq!(config.max_backoff_secs, decoded.max_backoff_secs);
        assert_eq!(config.jitter_percent, decoded.jitter_percent);
    }
}
