//! Optional HTTP activity probes for the standalone replicator.

use serde::{Deserialize, Serialize};

use crate::shared::{Validate, ValidationError};

/// Default HTTP port for replicator activity probes.
const DEFAULT_HEALTH_PORT: u16 = 9001;
/// Default minimum inactivity allowance, in milliseconds.
///
/// Five minutes tolerates slow batches before probes report inactivity;
/// Kubernetes applies its own failure thresholds before marking unready or
/// restarting.
const DEFAULT_STALL_TIMEOUT_MILLISECONDS: u64 = 5 * 60 * 1000;

/// Replicator probe listener and inactivity policy.
///
/// Omit the replicator's `health` block to disable its listener. Slot
/// acquisition is exempt from inactivity checks. These probes observe activity,
/// not durable destination progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicatorHealthConfig {
    /// HTTP port for `/livez` and `/readyz` on all IPv4 and IPv6 interfaces.
    /// Falls back to IPv4 when IPv6 sockets are unsupported.
    pub port: u16,
    /// Minimum inactivity allowance in milliseconds, defaulting to five
    /// minutes.
    ///
    /// Apply loops allow at least PostgreSQL's `wal_sender_timeout` (or the
    /// fallback when disabled or unavailable) so quiet sources can exchange
    /// keepalives before being marked stalled.
    pub stall_timeout_ms: u64,
}

impl Default for ReplicatorHealthConfig {
    fn default() -> Self {
        Self { port: DEFAULT_HEALTH_PORT, stall_timeout_ms: DEFAULT_STALL_TIMEOUT_MILLISECONDS }
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

    /// Partial listener configuration retains the defaults for omitted fields.
    #[test]
    fn defaults_and_overrides() {
        let config: ReplicatorHealthConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config, ReplicatorHealthConfig { port: 9001, stall_timeout_ms: 300_000 });
        config.validate().unwrap();
        let config: ReplicatorHealthConfig =
            serde_json::from_str(r#"{"stall_timeout_ms":120000}"#).unwrap();
        assert_eq!(config.port, 9001);
        assert_eq!(config.stall_timeout_ms, 120_000);
    }

    /// A listener requires a nonzero port and inactivity allowance.
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
