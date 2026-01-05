use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::configs::{log::LogLevel, store::Store};

const DEFAULT_BATCH_MAX_SIZE: usize = 100000;
const DEFAULT_BATCH_MAX_FILL_MS: u64 = 10000;
const DEFAULT_TABLE_ERROR_RETRY_DELAY_MS: u64 = 10000;
const DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_MAX_TABLE_SYNC_WORKERS: u16 = 4;

const fn default_table_error_retry_max_attempts() -> u32 {
    DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS
}

/// Batch processing configuration for pipelines.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct ApiBatchConfig {
    /// Maximum number of items in a batch for table copy and event streaming.
    #[schema(example = 1000)]
    pub max_size: Option<usize>,
    /// Maximum time, in milliseconds, to wait for a batch to fill before processing.
    #[schema(example = 1000)]
    pub max_fill_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FullApiPipelineConfig {
    #[schema(example = "my_publication")]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub publication_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<ApiBatchConfig>,
    #[schema(example = 1000)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_error_retry_delay_ms: Option<u64>,
    #[schema(example = 5)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_error_retry_max_attempts: Option<u32>,
    #[schema(example = 4)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_table_sync_workers: Option<u16>,
    pub log_level: Option<LogLevel>,
}

impl From<StoredPipelineConfig> for FullApiPipelineConfig {
    fn from(value: StoredPipelineConfig) -> Self {
        Self {
            publication_name: value.publication_name,
            batch: Some(ApiBatchConfig {
                max_size: Some(value.batch.max_size),
                max_fill_ms: Some(value.batch.max_fill_ms),
            }),
            table_error_retry_delay_ms: Some(value.table_error_retry_delay_ms),
            table_error_retry_max_attempts: Some(value.table_error_retry_max_attempts),
            max_table_sync_workers: Some(value.max_table_sync_workers),
            log_level: value.log_level,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PartialApiPipelineConfig {
    #[schema(example = "my_publication")]
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::utils::trim_option_string"
    )]
    pub publication_name: Option<String>,
    #[schema(example = r#"{"max_size": 1000000, "max_fill_ms": 10000}"#)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<ApiBatchConfig>,
    #[schema(example = 1000)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_error_retry_delay_ms: Option<u64>,
    #[schema(example = 5)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_error_retry_max_attempts: Option<u32>,
    #[schema(example = 4)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_table_sync_workers: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<LogLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPipelineConfig {
    pub publication_name: String,
    pub batch: BatchConfig,
    pub table_error_retry_delay_ms: u64,
    #[serde(default = "default_table_error_retry_max_attempts")]
    pub table_error_retry_max_attempts: u32,
    pub max_table_sync_workers: u16,
    pub log_level: Option<LogLevel>,
}

impl StoredPipelineConfig {
    pub fn into_etl_config(
        self,
        pipeline_id: u64,
        pg_connection_config: PgConnectionConfig,
    ) -> PipelineConfig {
        PipelineConfig {
            id: pipeline_id,
            publication_name: self.publication_name,
            pg_connection: pg_connection_config,
            batch: self.batch,
            table_error_retry_delay_ms: self.table_error_retry_delay_ms,
            table_error_retry_max_attempts: self.table_error_retry_max_attempts,
            max_table_sync_workers: self.max_table_sync_workers,
            reconnection: Default::default(),
        }
    }

    pub fn merge(&mut self, partial: PartialApiPipelineConfig) {
        if let Some(value) = partial.publication_name {
            self.publication_name = value;
        }

        if let Some(value) = partial.batch
            && let (Some(max_size), Some(max_fill_ms)) = (value.max_size, value.max_fill_ms)
        {
            self.batch = BatchConfig {
                max_size,
                max_fill_ms,
            };
        }

        if let Some(value) = partial.table_error_retry_delay_ms {
            self.table_error_retry_delay_ms = value;
        }

        if let Some(value) = partial.table_error_retry_max_attempts {
            self.table_error_retry_max_attempts = value;
        }

        if let Some(value) = partial.max_table_sync_workers {
            self.max_table_sync_workers = value;
        }

        self.log_level = partial.log_level
    }
}

impl Store for StoredPipelineConfig {}

impl From<FullApiPipelineConfig> for StoredPipelineConfig {
    fn from(value: FullApiPipelineConfig) -> Self {
        let batch = value
            .batch
            .map(|b| BatchConfig {
                max_size: b.max_size.unwrap_or(DEFAULT_BATCH_MAX_SIZE),
                max_fill_ms: b.max_fill_ms.unwrap_or(DEFAULT_BATCH_MAX_FILL_MS),
            })
            .unwrap_or(BatchConfig {
                max_size: DEFAULT_BATCH_MAX_SIZE,
                max_fill_ms: DEFAULT_BATCH_MAX_FILL_MS,
            });

        Self {
            publication_name: value.publication_name,
            batch,
            table_error_retry_delay_ms: value
                .table_error_retry_delay_ms
                .unwrap_or(DEFAULT_TABLE_ERROR_RETRY_DELAY_MS),
            table_error_retry_max_attempts: value
                .table_error_retry_max_attempts
                .unwrap_or(DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS),
            max_table_sync_workers: value
                .max_table_sync_workers
                .unwrap_or(DEFAULT_MAX_TABLE_SYNC_WORKERS),
            log_level: value.log_level,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_config::shared::BatchConfig;

    #[test]
    fn test_stored_pipeline_config_serialization() {
        let config = StoredPipelineConfig {
            publication_name: "test_publication".to_string(),
            batch: BatchConfig {
                max_size: 1000,
                max_fill_ms: 5000,
            },
            table_error_retry_delay_ms: 2000,
            table_error_retry_max_attempts: 7,
            max_table_sync_workers: 4,
            log_level: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StoredPipelineConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.publication_name, deserialized.publication_name);
        assert_eq!(config.batch.max_size, deserialized.batch.max_size);
        assert_eq!(
            config.table_error_retry_delay_ms,
            deserialized.table_error_retry_delay_ms
        );
        assert_eq!(
            config.table_error_retry_max_attempts,
            deserialized.table_error_retry_max_attempts
        );
        assert_eq!(
            config.max_table_sync_workers,
            deserialized.max_table_sync_workers
        );
    }

    #[test]
    fn test_full_api_pipeline_config_conversion() {
        let full_config = FullApiPipelineConfig {
            publication_name: "test_publication".to_string(),
            batch: None,
            table_error_retry_delay_ms: None,
            table_error_retry_max_attempts: None,
            max_table_sync_workers: None,
            log_level: Some(LogLevel::Debug),
        };

        let stored: StoredPipelineConfig = full_config.clone().into();
        let back_to_full: FullApiPipelineConfig = stored.into();

        assert_eq!(full_config.publication_name, back_to_full.publication_name);
    }

    #[test]
    fn test_full_api_pipeline_config_defaults() {
        let full_config = FullApiPipelineConfig {
            publication_name: "test_publication".to_string(),
            batch: None,
            table_error_retry_delay_ms: None,
            table_error_retry_max_attempts: None,
            max_table_sync_workers: None,
            log_level: None,
        };

        let stored: StoredPipelineConfig = full_config.into();

        assert_eq!(stored.batch.max_size, DEFAULT_BATCH_MAX_SIZE);
        assert_eq!(stored.batch.max_fill_ms, DEFAULT_BATCH_MAX_FILL_MS);
        assert_eq!(
            stored.table_error_retry_delay_ms,
            DEFAULT_TABLE_ERROR_RETRY_DELAY_MS
        );
        assert_eq!(
            stored.table_error_retry_max_attempts,
            DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS
        );
        assert_eq!(
            stored.max_table_sync_workers,
            DEFAULT_MAX_TABLE_SYNC_WORKERS
        );
    }

    #[test]
    fn test_partial_api_pipeline_config_merge() {
        let mut stored = StoredPipelineConfig {
            publication_name: "old_publication".to_string(),
            batch: BatchConfig {
                max_size: 500,
                max_fill_ms: 2000,
            },
            table_error_retry_delay_ms: 1000,
            table_error_retry_max_attempts: 3,
            max_table_sync_workers: 2,
            log_level: None,
        };

        let partial = PartialApiPipelineConfig {
            publication_name: Some("new_publication".to_string()),
            batch: Some(ApiBatchConfig {
                max_size: Some(1000),
                max_fill_ms: Some(8000),
            }),
            table_error_retry_delay_ms: Some(5000),
            table_error_retry_max_attempts: Some(9),
            max_table_sync_workers: None,
            log_level: None,
        };

        stored.merge(partial);

        assert_eq!(stored.publication_name, "new_publication");
        assert_eq!(stored.batch.max_size, 1000);
        assert_eq!(stored.batch.max_fill_ms, 8000);
        assert_eq!(stored.table_error_retry_delay_ms, 5000);
        assert_eq!(stored.table_error_retry_max_attempts, 9);
        assert_eq!(stored.max_table_sync_workers, 2);
    }
}
