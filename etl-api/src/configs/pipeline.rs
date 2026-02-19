use etl_config::shared::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::configs::{log::LogLevel, store::Store};

const fn default_table_error_retry_max_attempts() -> u32 {
    PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS
}

const fn default_table_error_retry_delay_ms() -> u64 {
    PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS
}

const fn default_max_table_sync_workers() -> u16 {
    PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS
}

const fn default_max_copy_connections_per_table() -> u16 {
    PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE
}

const fn default_memory_refresh_interval_ms() -> u64 {
    PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS
}

/// Batch processing configuration for pipelines.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct ApiBatchConfig {
    /// Maximum time, in milliseconds, to wait before flushing a partially filled batch.
    ///
    /// This is the latency bound in streams: after the first item is buffered, the batch is
    /// flushed when this timeout elapses even if size-based targets are not reached.
    #[schema(example = 1000)]
    pub max_fill_ms: Option<u64>,
    /// Ratio of process memory reserved for incoming stream batch bytes.
    ///
    /// The effective byte budget is divided by active streams at runtime, and flush happens on
    /// the first trigger between this byte budget and [`Self::max_fill_ms`].
    #[schema(example = 0.2)]
    pub memory_budget_ratio: Option<f32>,
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
    #[schema(example = 2)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_copy_connections_per_table: Option<u16>,
    #[schema(example = 100)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_refresh_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_backpressure: Option<MemoryBackpressureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_sync_copy: Option<TableSyncCopyConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invalidated_slot_behavior: Option<InvalidatedSlotBehavior>,
    pub log_level: Option<LogLevel>,
}

impl From<StoredPipelineConfig> for FullApiPipelineConfig {
    fn from(value: StoredPipelineConfig) -> Self {
        Self {
            publication_name: value.publication_name,
            batch: Some(ApiBatchConfig {
                max_fill_ms: Some(value.batch.max_fill_ms),
                memory_budget_ratio: Some(value.batch.memory_budget_ratio),
            }),
            table_error_retry_delay_ms: Some(value.table_error_retry_delay_ms),
            table_error_retry_max_attempts: Some(value.table_error_retry_max_attempts),
            max_table_sync_workers: Some(value.max_table_sync_workers),
            max_copy_connections_per_table: Some(value.max_copy_connections_per_table),
            memory_refresh_interval_ms: Some(value.memory_refresh_interval_ms),
            memory_backpressure: value.memory_backpressure,
            table_sync_copy: Some(value.table_sync_copy),
            invalidated_slot_behavior: Some(value.invalidated_slot_behavior),
            log_level: value.log_level,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPipelineConfig {
    pub publication_name: String,
    #[serde(default)]
    pub batch: BatchConfig,
    #[serde(default = "default_table_error_retry_delay_ms")]
    pub table_error_retry_delay_ms: u64,
    #[serde(default = "default_table_error_retry_max_attempts")]
    pub table_error_retry_max_attempts: u32,
    #[serde(default = "default_max_table_sync_workers")]
    pub max_table_sync_workers: u16,
    #[serde(default = "default_max_copy_connections_per_table")]
    pub max_copy_connections_per_table: u16,
    #[serde(default = "default_memory_refresh_interval_ms")]
    pub memory_refresh_interval_ms: u64,
    #[serde(default)]
    pub memory_backpressure: Option<MemoryBackpressureConfig>,
    #[serde(default)]
    pub table_sync_copy: TableSyncCopyConfig,
    #[serde(default)]
    pub invalidated_slot_behavior: InvalidatedSlotBehavior,
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
            memory_refresh_interval_ms: self.memory_refresh_interval_ms,
            memory_backpressure: self.memory_backpressure,
            table_sync_copy: self.table_sync_copy,
            invalidated_slot_behavior: self.invalidated_slot_behavior,
            max_copy_connections_per_table: self.max_copy_connections_per_table,
        }
    }
}

impl Store for StoredPipelineConfig {}

impl From<FullApiPipelineConfig> for StoredPipelineConfig {
    fn from(value: FullApiPipelineConfig) -> Self {
        let batch = value
            .batch
            .map(|b| BatchConfig {
                max_fill_ms: b.max_fill_ms.unwrap_or(BatchConfig::DEFAULT_MAX_FILL_MS),
                memory_budget_ratio: b
                    .memory_budget_ratio
                    .unwrap_or(BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO),
            })
            .unwrap_or(BatchConfig {
                max_fill_ms: BatchConfig::DEFAULT_MAX_FILL_MS,
                memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
            });

        Self {
            publication_name: value.publication_name,
            batch,
            table_error_retry_delay_ms: value
                .table_error_retry_delay_ms
                .unwrap_or(PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS),
            table_error_retry_max_attempts: value
                .table_error_retry_max_attempts
                .unwrap_or(PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS),
            max_table_sync_workers: value
                .max_table_sync_workers
                .unwrap_or(PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS),
            max_copy_connections_per_table: value
                .max_copy_connections_per_table
                .unwrap_or(PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE),
            memory_refresh_interval_ms: value
                .memory_refresh_interval_ms
                .unwrap_or(PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS),
            memory_backpressure: value.memory_backpressure,
            table_sync_copy: value.table_sync_copy.unwrap_or_default(),
            invalidated_slot_behavior: value.invalidated_slot_behavior.unwrap_or_default(),
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
                max_fill_ms: 5000,
                memory_budget_ratio: 0.2,
            },
            table_error_retry_delay_ms: 2000,
            table_error_retry_max_attempts: 7,
            max_table_sync_workers: 4,
            max_copy_connections_per_table: 8,
            memory_refresh_interval_ms: 100,
            memory_backpressure: Some(MemoryBackpressureConfig {
                activate_threshold: 0.8,
                resume_threshold: 0.7,
            }),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            log_level: None,
            invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StoredPipelineConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.publication_name, deserialized.publication_name);
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
        assert_eq!(
            config.max_copy_connections_per_table,
            deserialized.max_copy_connections_per_table
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
            max_copy_connections_per_table: None,
            memory_refresh_interval_ms: None,
            memory_backpressure: None,
            table_sync_copy: None,
            invalidated_slot_behavior: None,
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
            max_copy_connections_per_table: None,
            memory_refresh_interval_ms: None,
            memory_backpressure: None,
            table_sync_copy: None,
            invalidated_slot_behavior: None,
            log_level: None,
        };

        let stored: StoredPipelineConfig = full_config.into();

        assert_eq!(stored.batch.max_fill_ms, BatchConfig::DEFAULT_MAX_FILL_MS);
        assert_eq!(
            stored.table_error_retry_delay_ms,
            PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS
        );
        assert_eq!(
            stored.table_error_retry_max_attempts,
            PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS
        );
        assert_eq!(
            stored.max_table_sync_workers,
            PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS
        );
        assert_eq!(
            stored.max_copy_connections_per_table,
            PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE
        );
        assert_eq!(
            stored.memory_refresh_interval_ms,
            PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS
        );
        assert_eq!(stored.memory_backpressure, None);
        assert_eq!(
            stored.invalidated_slot_behavior,
            InvalidatedSlotBehavior::Error
        );
    }
}
