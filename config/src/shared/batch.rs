use serde::{Deserialize, Serialize};

/// Batch processing configuration for pipelines.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchConfig {
    /// Maximum number of items in a batch for table copy and event streaming.
    pub max_size: usize,
    /// Maximum time, in milliseconds, to wait for a batch to fill before processing.
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
