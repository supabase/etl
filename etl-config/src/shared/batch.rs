use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::ValidationError;

/// Batch processing configuration for pipelines.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct BatchConfig {
    /// Maximum number of items in a batch for table copy and event streaming.
    #[serde(default = "default_batch_max_size")]
    #[cfg_attr(feature = "utoipa", schema(example = 10000))]
    pub max_size: usize,
    /// Maximum time, in milliseconds, to wait for a batch to fill before processing.
    #[serde(default = "default_batch_max_fill_ms")]
    #[cfg_attr(feature = "utoipa", schema(example = 0))]
    pub max_fill_ms: u64,
}

impl BatchConfig {
    /// Default maximum batch size for table copy and event streaming.
    pub const DEFAULT_MAX_SIZE: usize = 10000;

    /// Default maximum fill time in milliseconds.
    pub const DEFAULT_MAX_FILL_MS: u64 = 0;

    /// Validates batch configuration settings.
    ///
    /// Ensures max_size is non-zero.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.max_size == 0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "batch.max_size".to_string(),
                constraint: "must be greater than 0".to_string(),
            });
        }

        Ok(())
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: default_batch_max_size(),
            max_fill_ms: default_batch_max_fill_ms(),
        }
    }
}

fn default_batch_max_size() -> usize {
    BatchConfig::DEFAULT_MAX_SIZE
}

fn default_batch_max_fill_ms() -> u64 {
    BatchConfig::DEFAULT_MAX_FILL_MS
}
