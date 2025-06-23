use serde::{Deserialize, Serialize};

/// Configuration for a pipeline's batching and worker retry behavior.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PipelineConfig {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of replication slots and state
    /// store.
    pub id: u64,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,
    /// Retry configuration for initializing apply workers.
    #[serde(default)]
    pub apply_worker_init_retry: RetryConfig,
}

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
