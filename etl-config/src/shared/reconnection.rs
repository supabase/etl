use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for automatic reconnection behavior.
///
/// Controls how the pipeline handles connection interruptions and
/// attempts to reconnect to the source database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReconnectionConfig {
    /// Whether automatic reconnection is enabled.
    ///
    /// When disabled, connection errors will cause immediate pipeline failure.
    /// Default: true
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Initial delay before the first reconnection attempt.
    ///
    /// Specified in milliseconds for serialization compatibility.
    /// Default: 1000ms (1 second)
    #[serde(
        default = "default_initial_retry_delay_ms",
        rename = "initial_retry_delay_ms"
    )]
    pub initial_retry_delay_ms: u64,

    /// Maximum delay between reconnection attempts.
    ///
    /// The backoff algorithm will not exceed this delay.
    /// Specified in milliseconds for serialization compatibility.
    /// Default: 60000ms (60 seconds)
    #[serde(default = "default_max_retry_delay_ms", rename = "max_retry_delay_ms")]
    pub max_retry_delay_ms: u64,

    /// Multiplier for exponential backoff between attempts.
    ///
    /// After each failed attempt, the delay is multiplied by this value.
    /// Must be >= 1.0.
    /// Default: 2.0
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Maximum total duration to attempt reconnection.
    ///
    /// If reconnection cannot be established within this duration,
    /// the pipeline will fail with an error.
    /// Specified in milliseconds for serialization compatibility.
    /// Default: 300000ms (5 minutes)
    #[serde(
        default = "default_max_retry_duration_ms",
        rename = "max_retry_duration_ms"
    )]
    pub max_retry_duration_ms: u64,
}

fn default_enabled() -> bool {
    true
}

fn default_initial_retry_delay_ms() -> u64 {
    1000
}

fn default_max_retry_delay_ms() -> u64 {
    60000
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_max_retry_duration_ms() -> u64 {
    300000
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            initial_retry_delay_ms: default_initial_retry_delay_ms(),
            max_retry_delay_ms: default_max_retry_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            max_retry_duration_ms: default_max_retry_duration_ms(),
        }
    }
}

impl ReconnectionConfig {
    /// Returns the initial retry delay as a Duration.
    pub fn initial_retry_delay(&self) -> Duration {
        Duration::from_millis(self.initial_retry_delay_ms)
    }

    /// Returns the maximum retry delay as a Duration.
    pub fn max_retry_delay(&self) -> Duration {
        Duration::from_millis(self.max_retry_delay_ms)
    }

    /// Returns the maximum retry duration as a Duration.
    pub fn max_retry_duration(&self) -> Duration {
        Duration::from_millis(self.max_retry_duration_ms)
    }
}
