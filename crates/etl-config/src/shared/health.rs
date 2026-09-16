//! Optional HTTP activity probes for the standalone replicator.

use serde::{Deserialize, Serialize};

use crate::shared::{Validate, ValidationError};

/// Replicator probe listener and inactivity policy.
///
/// Omit the replicator's `health` block to disable its listener. Slot
/// acquisition is exempt from inactivity checks. These probes observe activity,
/// not durable destination progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicatorHealthConfig {
    /// HTTP port for `/livez` and `/readyz` on all IPv4 interfaces.
    pub port: u16,
    /// Minimum inactivity allowance in milliseconds.
    ///
    /// Apply loops allow at least PostgreSQL's `wal_sender_timeout` (or the
    /// fallback when disabled or unavailable) so quiet sources can exchange
    /// keepalives before being marked stalled.
    pub stall_timeout_ms: u64,
}

impl Default for ReplicatorHealthConfig {
    fn default() -> Self {
        Self { port: 9001, stall_timeout_ms: 600_000 }
    }
}

impl Validate for ReplicatorHealthConfig {
    fn validate(&self) -> Result<(), ValidationError> {
        for (field, is_zero) in [
            ("health.port", self.port == 0),
            ("health.stall_timeout_ms", self.stall_timeout_ms == 0),
        ] {
            if is_zero {
                return Err(ValidationError::InvalidFieldValue {
                    field: field.to_owned(),
                    constraint: "must be greater than zero".to_owned(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::shared::{ReplicatorHealthConfig, Validate};

    #[test]
    fn defaults_and_overrides() {
        let config: ReplicatorHealthConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config, ReplicatorHealthConfig::default());
        config.validate().unwrap();
        let config: ReplicatorHealthConfig =
            serde_json::from_str(r#"{"stall_timeout_ms":120000}"#).unwrap();
        assert_eq!(config.port, 9001);
        assert_eq!(config.stall_timeout_ms, 120_000);
    }

    #[test]
    fn rejects_zero_values() {
        for config in [
            ReplicatorHealthConfig { port: 0, ..Default::default() },
            ReplicatorHealthConfig { stall_timeout_ms: 0, ..Default::default() },
        ] {
            assert!(config.validate().is_err());
        }
    }
}
