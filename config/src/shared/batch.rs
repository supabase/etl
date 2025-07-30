use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Batch processing configuration for pipelines.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct BatchConfig {
    /// Maximum number of items in a batch for table copy and event streaming.
    #[cfg_attr(feature = "utoipa", schema(example = 1000))]
    pub max_size: usize,
    /// Maximum time, in milliseconds, to wait for a batch to fill before processing.
    #[cfg_attr(feature = "utoipa", schema(example = 1000))]
    pub max_fill_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 1000,
            max_fill_ms: 1000,
        }
    }
}
