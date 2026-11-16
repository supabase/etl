//! Optional HTTP activity probes for the standalone replicator.

use std::num::{NonZeroU16, NonZeroU64};

use serde::{Deserialize, Serialize};

/// Default HTTP port for replicator activity probes.
const DEFAULT_HEALTH_PORT: NonZeroU16 = NonZeroU16::new(9001).unwrap();
/// Default minimum inactivity allowance, in milliseconds.
///
/// Five minutes tolerates slow batches before probes report inactivity;
/// Kubernetes applies its own failure thresholds before marking unready or
/// restarting.
const DEFAULT_STALL_TIMEOUT_MILLISECONDS: NonZeroU64 = NonZeroU64::new(5 * 60 * 1000).unwrap();

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
    pub port: NonZeroU16,
    /// Minimum inactivity allowance in milliseconds, defaulting to five
    /// minutes.
    ///
    /// Apply loops allow at least PostgreSQL's `wal_sender_timeout` (or the
    /// fallback when disabled or unavailable) so quiet sources can exchange
    /// keepalives before being marked stalled.
    pub stall_timeout_ms: NonZeroU64,
}

impl Default for ReplicatorHealthConfig {
    fn default() -> Self {
        Self { port: DEFAULT_HEALTH_PORT, stall_timeout_ms: DEFAULT_STALL_TIMEOUT_MILLISECONDS }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU64};

    use crate::shared::ReplicatorHealthConfig;

    /// Partial listener configuration retains the defaults for omitted fields.
    #[test]
    fn defaults_and_overrides() {
        let config: ReplicatorHealthConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            config,
            ReplicatorHealthConfig {
                port: NonZeroU16::new(9001).unwrap(),
                stall_timeout_ms: NonZeroU64::new(300_000).unwrap(),
            }
        );

        let config: ReplicatorHealthConfig =
            serde_json::from_str(r#"{"stall_timeout_ms":120000}"#).unwrap();
        assert_eq!(config.port.get(), 9001);
        assert_eq!(config.stall_timeout_ms.get(), 120_000);
    }

    /// A listener requires a nonzero port and inactivity allowance, which the
    /// field types enforce while deserializing.
    #[test]
    fn rejects_zero_values() {
        for json in [r#"{"port":0}"#, r#"{"stall_timeout_ms":0}"#] {
            serde_json::from_str::<ReplicatorHealthConfig>(json).unwrap_err();
        }
    }
}
