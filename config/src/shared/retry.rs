use serde::{Deserialize, Serialize};

/// Retry policy configuration for operations such as worker initialization.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts before giving up.
    pub max_attempts: u32,

    /// Initial delay, in milliseconds, before the first retry.
    pub initial_delay_ms: u64,

    /// Maximum delay between retries.
    pub max_delay_ms: u64,

    /// Exponential backoff multiplier applied to the delay after each attempt.
    pub backoff_factor: f32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay_ms: 500,
            max_delay_ms: 10_000,
            backoff_factor: 2.0,
        }
    }
}
