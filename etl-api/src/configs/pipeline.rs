use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const DEFAULT_BATCH_MAX_SIZE: usize = 1000000;
const DEFAULT_BATCH_MAX_FILL_MS: u64 = 10000;
const DEFAULT_TABLE_ERROR_RETRY_DELAY_MS: u64 = 10000;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FullApiPipelineConfig {
    #[schema(example = "my_publication")]
    pub publication_name: String,
    pub batch: Option<BatchConfig>,
    #[schema(example = 1000)]
    pub table_error_retry_delay_ms: Option<u64>,
    #[schema(example = 4)]
    pub max_table_sync_workers: Option<u16>,
}

impl From<StoredPipelineConfig> for FullApiPipelineConfig {
    fn from(value: StoredPipelineConfig) -> Self {
        Self {
            publication_name: value.publication_name,
            batch: Some(value.batch),
            table_error_retry_delay_ms: Some(value.table_error_retry_delay_ms),
            max_table_sync_workers: Some(value.max_table_sync_workers),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PartialApiPipelineConfig {
    #[schema(example = "my_publication")]
    pub publication_name: Option<String>,
    #[schema(example = r#"{"max_size": 1000000, "max_fill_ms": 10000}"#)]
    pub batch: Option<BatchConfig>,
    #[schema(example = 1000)]
    pub table_error_retry_delay_ms: Option<u64>,
    #[schema(example = 4)]
    pub max_table_sync_workers: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPipelineConfig {
    pub publication_name: String,
    pub batch: BatchConfig,
    pub table_error_retry_delay_ms: u64,
    pub max_table_sync_workers: u16,
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
            max_table_sync_workers: self.max_table_sync_workers,
        }
    }

    pub fn merge(&mut self, partial: PartialApiPipelineConfig) {
        if let Some(value) = partial.publication_name {
            self.publication_name = value;
        }

        if let Some(value) = partial.batch {
            self.batch = value;
        }

        if let Some(value) = partial.table_error_retry_delay_ms {
            self.table_error_retry_delay_ms = value;
        }

        if let Some(value) = partial.max_table_sync_workers {
            self.max_table_sync_workers = value;
        }
    }
}

impl From<FullApiPipelineConfig> for StoredPipelineConfig {
    fn from(value: FullApiPipelineConfig) -> Self {
        Self {
            publication_name: value.publication_name,
            batch: value.batch.unwrap_or(BatchConfig {
                max_size: DEFAULT_BATCH_MAX_SIZE,
                max_fill_ms: DEFAULT_BATCH_MAX_FILL_MS,
            }),
            table_error_retry_delay_ms: value
                .table_error_retry_delay_ms
                .unwrap_or(DEFAULT_TABLE_ERROR_RETRY_DELAY_MS),
            max_table_sync_workers: value.max_table_sync_workers.unwrap_or_default(),
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
            max_table_sync_workers: 4,
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
            max_table_sync_workers: None,
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
            max_table_sync_workers: None,
        };

        let stored: StoredPipelineConfig = full_config.into();

        assert_eq!(stored.batch.max_size, DEFAULT_BATCH_MAX_SIZE);
        assert_eq!(stored.batch.max_fill_ms, DEFAULT_BATCH_MAX_FILL_MS);
        assert_eq!(
            stored.table_error_retry_delay_ms,
            DEFAULT_TABLE_ERROR_RETRY_DELAY_MS
        );
        assert_eq!(stored.max_table_sync_workers, 0);
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
            max_table_sync_workers: 2,
        };

        let partial = PartialApiPipelineConfig {
            publication_name: Some("new_publication".to_string()),
            batch: Some(BatchConfig {
                max_size: 2000,
                max_fill_ms: 8000,
            }),
            table_error_retry_delay_ms: Some(5000),
            max_table_sync_workers: None,
        };

        stored.merge(partial);

        assert_eq!(stored.publication_name, "new_publication");
        assert_eq!(stored.batch.max_size, 2000);
        assert_eq!(stored.batch.max_fill_ms, 8000);
        assert_eq!(stored.table_error_retry_delay_ms, 5000);
        assert_eq!(stored.max_table_sync_workers, 2);
    }
}
