use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

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
    }
}

impl From<FullApiPipelineConfig> for StoredPipelineConfig {
    fn from(value: FullApiPipelineConfig) -> Self {
        Self {
            publication_name: value.publication_name,
            batch: value.batch.unwrap_or(BatchConfig {
                max_size: 1000000,
                max_fill_ms: 10000,
            }),
            table_error_retry_delay_ms: value.table_error_retry_delay_ms.unwrap_or(10000),
            max_table_sync_workers: value.max_table_sync_workers.unwrap_or_default(),
        }
    }
}
