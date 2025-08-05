use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Retry policy configuration for operations such as worker initialization.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct RetryConfig {
    /// Maximum number of retry attempts before giving up.
    #[cfg_attr(feature = "utoipa", schema(example = 5))]
    pub max_attempts: u32,
    /// Initial delay, in milliseconds, before the first retry.
    #[cfg_attr(feature = "utoipa", schema(example = 500))]
    pub initial_delay_ms: u64,
    /// Maximum delay between retries.
    #[cfg_attr(feature = "utoipa", schema(example = 10000))]
    pub max_delay_ms: u64,
    /// Exponential backoff multiplier applied to the delay after each attempt.
    #[cfg_attr(feature = "utoipa", schema(example = 2.0))]
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
