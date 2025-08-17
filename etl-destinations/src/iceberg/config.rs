//! Configuration structures for the Iceberg destination.

use serde::{Deserialize, Serialize};

/// Writer configuration for Iceberg operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfig {
    /// Batch size for writing (default: 1000)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum batch size in bytes (default: 30MB, optimized for S3)
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,

    /// Maximum time to wait before committing (default: 10s)
    #[serde(default = "default_max_commit_time_ms")]
    pub max_commit_time_ms: u64,

    /// Enable metrics collection
    #[serde(default = "default_enable_metrics")]
    pub enable_metrics: bool,
}


/// Default implementations for configuration fields.
impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            max_batch_size_bytes: default_max_batch_size_bytes(),
            max_commit_time_ms: default_max_commit_time_ms(),
            enable_metrics: default_enable_metrics(),
        }
    }
}


// Default value functions for serde
fn default_batch_size() -> usize {
    1000
}

fn default_max_batch_size_bytes() -> usize {
    30 * 1024 * 1024 // 30MB - optimized for S3 Tables API
}

fn default_max_commit_time_ms() -> u64 {
    10_000 // 10 seconds max latency
}

fn default_enable_metrics() -> bool {
    true
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_writer_config() {
        let config = WriterConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_batch_size_bytes, 30 * 1024 * 1024); // 30MB
        assert_eq!(config.max_commit_time_ms, 10_000); // 10s
        assert!(config.enable_metrics);
    }
}
