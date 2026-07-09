use etl_config::shared::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig,
};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::configs::{log::LogLevel, store::Store, update::UpdateField};

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

const fn default_replication_lag_refresh_interval_ms() -> u64 {
    PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS
}

fn default_memory_backpressure() -> Option<MemoryBackpressureConfig> {
    Some(MemoryBackpressureConfig::default())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct ReplicatorResourcesConfig {
    /// CPU request for the replicator container, in millicores.
    ///
    /// When unset, the replicator uses the default request from the ETL API
    /// service configuration.
    #[schema(example = 500)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_request_millicores: Option<i32>,
    /// Memory request for the replicator container, in MiB.
    ///
    /// When unset, the replicator uses the default request from the ETL API
    /// service configuration.
    #[schema(example = 2000)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_request_mib: Option<i32>,
    /// CPU limit for the replicator container, in millicores.
    ///
    /// When unset, the ETL API uses the final CPU request as the CPU limit.
    #[schema(example = 1000)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_limit_millicores: Option<i32>,
    /// Memory limit for the replicator container, in MiB.
    ///
    /// When unset, the ETL API uses the final memory request as the memory
    /// limit.
    #[schema(example = 2400)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit_mib: Option<i32>,
}

impl ReplicatorResourcesConfig {
    /// Validates that configured resource overrides are positive.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(cpu_request_millicores) = self.cpu_request_millicores
            && cpu_request_millicores <= 0
        {
            return Err("Replicator cpu request must be greater than 0".to_owned());
        }

        if let Some(memory_request_mib) = self.memory_request_mib
            && memory_request_mib <= 0
        {
            return Err("Replicator memory request must be greater than 0".to_owned());
        }

        if let Some(cpu_limit_millicores) = self.cpu_limit_millicores
            && cpu_limit_millicores <= 0
        {
            return Err("Replicator cpu limit must be greater than 0".to_owned());
        }

        if let Some(memory_limit_mib) = self.memory_limit_mib
            && memory_limit_mib <= 0
        {
            return Err("Replicator memory limit must be greater than 0".to_owned());
        }

        Ok(())
    }
}

/// DuckLake maintenance controller settings for one pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct DuckLakeMaintenanceConfig {
    /// Minimum time between maintenance runs, in seconds.
    #[schema(example = 3600)]
    #[serde(default = "default_ducklake_maintenance_min_interval_seconds")]
    pub min_interval_seconds: u64,
    /// Maximum time replication may be paused for one maintenance run, in
    /// seconds.
    #[schema(example = 2700)]
    #[serde(default = "default_ducklake_maintenance_max_pause_seconds")]
    pub max_pause_seconds: u64,
    /// Minimum inlined bytes required before inline flush runs.
    #[schema(example = 10000000)]
    #[serde(default = "default_ducklake_maintenance_min_inlined_bytes")]
    pub min_inlined_bytes: u64,
    /// Maximum number of adjacent files compacted by one merge operation.
    #[schema(example = 40)]
    #[serde(default = "default_ducklake_maintenance_max_compacted_files")]
    pub max_compacted_files: u32,
    /// Maximum number of tables processed by each operation in one run.
    #[schema(example = 8)]
    #[serde(default = "default_ducklake_maintenance_max_tables_per_run")]
    pub max_tables_per_run: u32,
    /// DuckLake target file size used for compaction.
    #[schema(example = "500MB")]
    #[serde(default = "default_ducklake_maintenance_target_file_size")]
    pub target_file_size: String,
    /// Deleted-row fraction that triggers data file rewrite.
    #[schema(example = 0.5)]
    #[serde(default = "default_ducklake_maintenance_delete_threshold")]
    pub delete_threshold: f64,
    /// Minimum active data files required before data file rewrite runs.
    #[schema(example = 40)]
    #[serde(default = "default_ducklake_maintenance_min_active_data_files")]
    pub min_active_data_files: i64,
    /// CPU request for maintenance jobs, in millicores.
    #[schema(example = 1000)]
    #[serde(default = "default_ducklake_maintenance_cpu_request_millicores")]
    pub cpu_request_millicores: u32,
    /// Memory request for maintenance jobs, in MiB.
    #[schema(example = 1024)]
    #[serde(default = "default_ducklake_maintenance_memory_request_mib")]
    pub memory_request_mib: u32,
    /// Maximum runtime for one maintenance job, in seconds.
    #[schema(example = 1800)]
    #[serde(default = "default_ducklake_maintenance_active_deadline_seconds")]
    pub active_deadline_seconds: i64,
}

impl Default for DuckLakeMaintenanceConfig {
    fn default() -> Self {
        Self {
            min_interval_seconds: default_ducklake_maintenance_min_interval_seconds(),
            max_pause_seconds: default_ducklake_maintenance_max_pause_seconds(),
            min_inlined_bytes: default_ducklake_maintenance_min_inlined_bytes(),
            max_compacted_files: default_ducklake_maintenance_max_compacted_files(),
            max_tables_per_run: default_ducklake_maintenance_max_tables_per_run(),
            target_file_size: default_ducklake_maintenance_target_file_size(),
            delete_threshold: default_ducklake_maintenance_delete_threshold(),
            min_active_data_files: default_ducklake_maintenance_min_active_data_files(),
            cpu_request_millicores: default_ducklake_maintenance_cpu_request_millicores(),
            memory_request_mib: default_ducklake_maintenance_memory_request_mib(),
            active_deadline_seconds: default_ducklake_maintenance_active_deadline_seconds(),
        }
    }
}

impl DuckLakeMaintenanceConfig {
    /// Validates maintenance controller settings.
    pub fn validate(&self) -> Result<(), String> {
        if self.min_interval_seconds == 0 {
            return Err("ducklake maintenance min interval must be greater than 0".to_owned());
        }
        if self.max_pause_seconds == 0 {
            return Err("ducklake maintenance max pause must be greater than 0".to_owned());
        }
        if self.max_compacted_files == 0 {
            return Err(
                "ducklake maintenance max compacted files must be greater than 0".to_owned()
            );
        }
        if self.max_tables_per_run == 0 {
            return Err("ducklake maintenance max tables per run must be greater than 0".to_owned());
        }
        if !(0.0..=1.0).contains(&self.delete_threshold) {
            return Err("ducklake maintenance delete threshold must be between 0 and 1".to_owned());
        }
        if self.min_active_data_files < 0 {
            return Err("ducklake maintenance min active data files must be greater than or \
                        equal to 0"
                .to_owned());
        }
        if self.cpu_request_millicores == 0 {
            return Err("ducklake maintenance cpu request must be greater than 0".to_owned());
        }
        if self.memory_request_mib == 0 {
            return Err("ducklake maintenance memory request must be greater than 0".to_owned());
        }
        if self.active_deadline_seconds <= 0 {
            return Err("ducklake maintenance active deadline must be greater than 0".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiPipelineConfig {
    #[schema(example = "my_publication")]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub publication_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<BatchConfig>,
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
    #[schema(example = 10000)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_lag_refresh_interval_ms: Option<u64>,
    #[serde(default = "default_memory_backpressure", skip_serializing_if = "Option::is_none")]
    pub memory_backpressure: Option<MemoryBackpressureConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_sync_copy: Option<TableSyncCopyConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invalidated_slot_behavior: Option<InvalidatedSlotBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicator_resources: Option<ReplicatorResourcesConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ducklake_maintenance: Option<DuckLakeMaintenanceConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_level: Option<LogLevel>,
}

/// Errors returned while merging pipeline update configuration.
#[derive(Debug, Error)]
pub enum PipelineConfigUpdateError {
    /// A required field was explicitly cleared.
    #[error("Field `{field}` cannot be cleared")]
    RequiredFieldCleared { field: &'static str },
}

/// Patch-style pipeline configuration used by update endpoints.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateApiPipelineConfig {
    #[schema(example = "my_publication", value_type = Option<String>)]
    #[serde(
        default,
        skip_serializing_if = "UpdateField::is_preserve",
        deserialize_with = "deserialize_update_trimmed_string"
    )]
    pub publication_name: UpdateField<String>,
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub batch: UpdateField<BatchConfig>,
    #[schema(example = 1000)]
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub table_error_retry_delay_ms: UpdateField<u64>,
    #[schema(example = 5)]
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub table_error_retry_max_attempts: UpdateField<u32>,
    #[schema(example = 4)]
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub max_table_sync_workers: UpdateField<u16>,
    #[schema(example = 2)]
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub max_copy_connections_per_table: UpdateField<u16>,
    #[schema(example = 100)]
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub memory_refresh_interval_ms: UpdateField<u64>,
    #[schema(example = 10000)]
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub replication_lag_refresh_interval_ms: UpdateField<u64>,
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub memory_backpressure: UpdateField<MemoryBackpressureConfig>,
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub table_sync_copy: UpdateField<TableSyncCopyConfig>,
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub invalidated_slot_behavior: UpdateField<InvalidatedSlotBehavior>,
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub replicator_resources: UpdateField<ReplicatorResourcesConfig>,
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub ducklake_maintenance: UpdateField<DuckLakeMaintenanceConfig>,
    #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
    pub log_level: UpdateField<LogLevel>,
}

impl UpdateApiPipelineConfig {
    /// Builds a replacement update from an API pipeline configuration.
    ///
    /// Optional fields that are absent in the API config are cleared.
    pub fn from_api_config(value: ApiPipelineConfig) -> Self {
        Self {
            publication_name: UpdateField::Set(value.publication_name),
            batch: UpdateField::from_option(value.batch),
            table_error_retry_delay_ms: UpdateField::from_option(value.table_error_retry_delay_ms),
            table_error_retry_max_attempts: UpdateField::from_option(
                value.table_error_retry_max_attempts,
            ),
            max_table_sync_workers: UpdateField::from_option(value.max_table_sync_workers),
            max_copy_connections_per_table: UpdateField::from_option(
                value.max_copy_connections_per_table,
            ),
            memory_refresh_interval_ms: UpdateField::from_option(value.memory_refresh_interval_ms),
            replication_lag_refresh_interval_ms: UpdateField::from_option(
                value.replication_lag_refresh_interval_ms,
            ),
            memory_backpressure: UpdateField::from_option(value.memory_backpressure),
            table_sync_copy: UpdateField::from_option(value.table_sync_copy),
            invalidated_slot_behavior: UpdateField::from_option(value.invalidated_slot_behavior),
            replicator_resources: UpdateField::from_option(value.replicator_resources),
            ducklake_maintenance: UpdateField::from_option(value.ducklake_maintenance),
            log_level: UpdateField::from_option(value.log_level),
        }
    }

    /// Validates API-only pipeline configuration fields.
    pub fn validate(&self) -> Result<(), String> {
        if let UpdateField::Set(replicator_resources) = &self.replicator_resources {
            replicator_resources.validate()?;
        }

        if let UpdateField::Set(ducklake_maintenance) = &self.ducklake_maintenance {
            ducklake_maintenance.validate()?;
        }

        Ok(())
    }

    /// Merges this update into a stored pipeline configuration.
    pub(crate) fn merge_into_stored(
        self,
        stored: StoredPipelineConfig,
    ) -> Result<StoredPipelineConfig, PipelineConfigUpdateError> {
        Ok(StoredPipelineConfig {
            publication_name: apply_required_field(
                self.publication_name,
                stored.publication_name,
                "publication_name",
            )?,
            batch: self.batch.apply_to_value(stored.batch, BatchConfig::default),
            table_error_retry_delay_ms: self.table_error_retry_delay_ms.apply_to_value(
                stored.table_error_retry_delay_ms,
                default_table_error_retry_delay_ms,
            ),
            table_error_retry_max_attempts: self.table_error_retry_max_attempts.apply_to_value(
                stored.table_error_retry_max_attempts,
                default_table_error_retry_max_attempts,
            ),
            max_table_sync_workers: self
                .max_table_sync_workers
                .apply_to_value(stored.max_table_sync_workers, default_max_table_sync_workers),
            max_copy_connections_per_table: self.max_copy_connections_per_table.apply_to_value(
                stored.max_copy_connections_per_table,
                default_max_copy_connections_per_table,
            ),
            memory_refresh_interval_ms: self.memory_refresh_interval_ms.apply_to_value(
                stored.memory_refresh_interval_ms,
                default_memory_refresh_interval_ms,
            ),
            replication_lag_refresh_interval_ms: self
                .replication_lag_refresh_interval_ms
                .apply_to_value(
                    stored.replication_lag_refresh_interval_ms,
                    default_replication_lag_refresh_interval_ms,
                ),
            memory_backpressure: self
                .memory_backpressure
                .apply_to_option(stored.memory_backpressure),
            table_sync_copy: self
                .table_sync_copy
                .apply_to_value(stored.table_sync_copy, TableSyncCopyConfig::default),
            invalidated_slot_behavior: self
                .invalidated_slot_behavior
                .apply_to_value(stored.invalidated_slot_behavior, InvalidatedSlotBehavior::default),
            replicator_resources: self
                .replicator_resources
                .apply_to_option(stored.replicator_resources),
            ducklake_maintenance: self
                .ducklake_maintenance
                .apply_to_option(stored.ducklake_maintenance),
            log_level: self.log_level.apply_to_option(stored.log_level),
        })
    }

    /// Restores fields that were preserved by this update from raw storage.
    pub(crate) fn restore_preserved_fields(
        &self,
        stored_config: &serde_json::Value,
        updated_config: &mut serde_json::Value,
    ) {
        self.publication_name.restore_preserved_value(
            stored_config,
            updated_config,
            "publication_name",
        );
        self.batch.restore_preserved_value(stored_config, updated_config, "batch");
        self.table_error_retry_delay_ms.restore_preserved_value(
            stored_config,
            updated_config,
            "table_error_retry_delay_ms",
        );
        self.table_error_retry_max_attempts.restore_preserved_value(
            stored_config,
            updated_config,
            "table_error_retry_max_attempts",
        );
        self.max_table_sync_workers.restore_preserved_value(
            stored_config,
            updated_config,
            "max_table_sync_workers",
        );
        self.max_copy_connections_per_table.restore_preserved_value(
            stored_config,
            updated_config,
            "max_copy_connections_per_table",
        );
        self.memory_refresh_interval_ms.restore_preserved_value(
            stored_config,
            updated_config,
            "memory_refresh_interval_ms",
        );
        self.replication_lag_refresh_interval_ms.restore_preserved_value(
            stored_config,
            updated_config,
            "replication_lag_refresh_interval_ms",
        );
        self.memory_backpressure.restore_preserved_value(
            stored_config,
            updated_config,
            "memory_backpressure",
        );
        self.table_sync_copy.restore_preserved_value(
            stored_config,
            updated_config,
            "table_sync_copy",
        );
        self.invalidated_slot_behavior.restore_preserved_value(
            stored_config,
            updated_config,
            "invalidated_slot_behavior",
        );
        self.replicator_resources.restore_preserved_value(
            stored_config,
            updated_config,
            "replicator_resources",
        );
        self.ducklake_maintenance.restore_preserved_value(
            stored_config,
            updated_config,
            "ducklake_maintenance",
        );
        self.log_level.restore_preserved_value(stored_config, updated_config, "log_level");
    }
}

impl ApiPipelineConfig {
    /// Validates API-only pipeline configuration fields.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(replicator_resources) = &self.replicator_resources {
            replicator_resources.validate()?;
        }

        if let Some(ducklake_maintenance) = &self.ducklake_maintenance {
            ducklake_maintenance.validate()?;
        }

        Ok(())
    }
}

impl From<StoredPipelineConfig> for ApiPipelineConfig {
    fn from(value: StoredPipelineConfig) -> Self {
        Self {
            publication_name: value.publication_name,
            batch: Some(value.batch),
            table_error_retry_delay_ms: Some(value.table_error_retry_delay_ms),
            table_error_retry_max_attempts: Some(value.table_error_retry_max_attempts),
            max_table_sync_workers: Some(value.max_table_sync_workers),
            max_copy_connections_per_table: Some(value.max_copy_connections_per_table),
            memory_refresh_interval_ms: Some(value.memory_refresh_interval_ms),
            replication_lag_refresh_interval_ms: Some(value.replication_lag_refresh_interval_ms),
            memory_backpressure: value.memory_backpressure,
            table_sync_copy: Some(value.table_sync_copy),
            invalidated_slot_behavior: Some(value.invalidated_slot_behavior),
            replicator_resources: value.replicator_resources,
            ducklake_maintenance: value.ducklake_maintenance,
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
    #[serde(default = "default_replication_lag_refresh_interval_ms")]
    pub replication_lag_refresh_interval_ms: u64,
    #[serde(default = "default_memory_backpressure")]
    pub memory_backpressure: Option<MemoryBackpressureConfig>,
    #[serde(default)]
    pub table_sync_copy: TableSyncCopyConfig,
    #[serde(default)]
    pub invalidated_slot_behavior: InvalidatedSlotBehavior,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicator_resources: Option<ReplicatorResourcesConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ducklake_maintenance: Option<DuckLakeMaintenanceConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
            store_pg_connection: None,
            batch: self.batch,
            table_error_retry_delay_ms: self.table_error_retry_delay_ms,
            table_error_retry_max_attempts: self.table_error_retry_max_attempts,
            max_table_sync_workers: self.max_table_sync_workers,
            memory_refresh_interval_ms: self.memory_refresh_interval_ms,
            replication_lag_refresh_interval_ms: self.replication_lag_refresh_interval_ms,
            memory_backpressure: self.memory_backpressure,
            table_sync_copy: self.table_sync_copy,
            invalidated_slot_behavior: self.invalidated_slot_behavior,
            max_copy_connections_per_table: self.max_copy_connections_per_table,
            // The API-managed pipelines run with an elevated role, so keep the
            // existing behavior of installing source migrations on start.
            run_source_migrations: true,
        }
    }
}

impl Store for StoredPipelineConfig {}

impl From<ApiPipelineConfig> for StoredPipelineConfig {
    fn from(value: ApiPipelineConfig) -> Self {
        let batch = value.batch.unwrap_or_default();

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
            replication_lag_refresh_interval_ms: value
                .replication_lag_refresh_interval_ms
                .unwrap_or(PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS),
            memory_backpressure: value.memory_backpressure,
            table_sync_copy: value.table_sync_copy.unwrap_or_default(),
            invalidated_slot_behavior: value.invalidated_slot_behavior.unwrap_or_default(),
            replicator_resources: value.replicator_resources,
            ducklake_maintenance: value.ducklake_maintenance,
            log_level: value.log_level,
        }
    }
}

fn apply_required_field<T>(
    update: UpdateField<T>,
    stored: T,
    field: &'static str,
) -> Result<T, PipelineConfigUpdateError> {
    match update {
        UpdateField::Preserve => Ok(stored),
        UpdateField::Clear => Err(PipelineConfigUpdateError::RequiredFieldCleared { field }),
        UpdateField::Set(value) => Ok(value),
    }
}

fn deserialize_update_trimmed_string<'de, D>(
    deserializer: D,
) -> Result<UpdateField<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
        .map(|value| UpdateField::from_option(value.map(|value| value.trim().to_owned())))
}

fn default_ducklake_maintenance_min_interval_seconds() -> u64 {
    3600
}

fn default_ducklake_maintenance_max_pause_seconds() -> u64 {
    2700
}

fn default_ducklake_maintenance_min_inlined_bytes() -> u64 {
    10_000_000
}

fn default_ducklake_maintenance_max_compacted_files() -> u32 {
    40
}

fn default_ducklake_maintenance_max_tables_per_run() -> u32 {
    8
}

fn default_ducklake_maintenance_target_file_size() -> String {
    "500MB".to_owned()
}

fn default_ducklake_maintenance_delete_threshold() -> f64 {
    0.5
}

fn default_ducklake_maintenance_min_active_data_files() -> i64 {
    40
}

fn default_ducklake_maintenance_cpu_request_millicores() -> u32 {
    1000
}

fn default_ducklake_maintenance_memory_request_mib() -> u32 {
    1024
}

fn default_ducklake_maintenance_active_deadline_seconds() -> i64 {
    1800
}

#[cfg(test)]
mod tests {
    use etl_config::shared::BatchConfig;

    use super::*;

    #[test]
    fn stored_pipeline_config_serialization() {
        let config = StoredPipelineConfig {
            publication_name: "test_publication".to_owned(),
            batch: BatchConfig {
                max_fill_ms: 5000,
                memory_budget_ratio: 0.2,
                max_bytes: 8 * 1024 * 1024,
            },
            table_error_retry_delay_ms: 2000,
            table_error_retry_max_attempts: 7,
            max_table_sync_workers: 4,
            max_copy_connections_per_table: 8,
            memory_refresh_interval_ms: 100,
            replication_lag_refresh_interval_ms: 10_000,
            memory_backpressure: Some(MemoryBackpressureConfig {
                activate_threshold: 0.8,
                resume_threshold: 0.7,
            }),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            replicator_resources: Some(ReplicatorResourcesConfig {
                cpu_request_millicores: Some(500),
                memory_request_mib: Some(2000),
                ..ReplicatorResourcesConfig::default()
            }),
            ducklake_maintenance: None,
            log_level: None,
            invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StoredPipelineConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.publication_name, deserialized.publication_name);
        assert_eq!(config.table_error_retry_delay_ms, deserialized.table_error_retry_delay_ms);
        assert_eq!(
            config.table_error_retry_max_attempts,
            deserialized.table_error_retry_max_attempts
        );
        assert_eq!(config.max_table_sync_workers, deserialized.max_table_sync_workers);
        assert_eq!(
            config.max_copy_connections_per_table,
            deserialized.max_copy_connections_per_table
        );
    }

    #[test]
    fn create_api_pipeline_config_conversion() {
        let create_config = ApiPipelineConfig {
            publication_name: "test_publication".to_owned(),
            batch: None,
            table_error_retry_delay_ms: None,
            table_error_retry_max_attempts: None,
            max_table_sync_workers: None,
            max_copy_connections_per_table: None,
            memory_refresh_interval_ms: None,
            replication_lag_refresh_interval_ms: None,
            memory_backpressure: None,
            table_sync_copy: None,
            invalidated_slot_behavior: None,
            replicator_resources: Some(ReplicatorResourcesConfig {
                cpu_request_millicores: Some(500),
                memory_request_mib: Some(2000),
                ..ReplicatorResourcesConfig::default()
            }),
            ducklake_maintenance: None,
            log_level: Some(LogLevel::Debug),
        };

        let stored: StoredPipelineConfig = create_config.clone().into();
        let back_to_create: ApiPipelineConfig = stored.into();

        assert_eq!(create_config.publication_name, back_to_create.publication_name);
    }

    #[test]
    fn create_api_pipeline_config_defaults() {
        let create_config = ApiPipelineConfig {
            publication_name: "test_publication".to_owned(),
            batch: None,
            table_error_retry_delay_ms: None,
            table_error_retry_max_attempts: None,
            max_table_sync_workers: None,
            max_copy_connections_per_table: None,
            memory_refresh_interval_ms: None,
            replication_lag_refresh_interval_ms: None,
            memory_backpressure: None,
            table_sync_copy: None,
            invalidated_slot_behavior: None,
            replicator_resources: None,
            ducklake_maintenance: None,
            log_level: None,
        };

        let stored: StoredPipelineConfig = create_config.into();

        assert_eq!(stored.batch.max_fill_ms, BatchConfig::DEFAULT_MAX_FILL_MS);
        assert_eq!(
            stored.table_error_retry_delay_ms,
            PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS
        );
        assert_eq!(
            stored.table_error_retry_max_attempts,
            PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS
        );
        assert_eq!(stored.max_table_sync_workers, PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS);
        assert_eq!(
            stored.max_copy_connections_per_table,
            PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE
        );
        assert_eq!(
            stored.memory_refresh_interval_ms,
            PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS
        );
        assert_eq!(
            stored.replication_lag_refresh_interval_ms,
            PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS
        );
        assert_eq!(stored.memory_backpressure, None);
        assert_eq!(stored.invalidated_slot_behavior, InvalidatedSlotBehavior::Error);
    }

    #[test]
    fn update_api_pipeline_config_merges_preserve_set_and_clear_fields() {
        let stored = StoredPipelineConfig {
            publication_name: "publication".to_owned(),
            batch: BatchConfig {
                max_fill_ms: 5000,
                memory_budget_ratio: 0.2,
                max_bytes: 8 * 1024 * 1024,
            },
            table_error_retry_delay_ms: 1234,
            table_error_retry_max_attempts: 7,
            max_table_sync_workers: 4,
            max_copy_connections_per_table: 8,
            memory_refresh_interval_ms: 100,
            replication_lag_refresh_interval_ms: 10_000,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
            replicator_resources: Some(ReplicatorResourcesConfig::default()),
            ducklake_maintenance: Some(DuckLakeMaintenanceConfig::default()),
            log_level: Some(LogLevel::Info),
        };
        let update = UpdateApiPipelineConfig {
            publication_name: UpdateField::Set("updated_publication".to_owned()),
            batch: UpdateField::Clear,
            log_level: UpdateField::Clear,
            ..UpdateApiPipelineConfig::default()
        };

        let updated = update.merge_into_stored(stored).unwrap();

        assert_eq!(updated.publication_name, "updated_publication");
        assert_eq!(updated.batch.max_fill_ms, BatchConfig::DEFAULT_MAX_FILL_MS);
        assert_eq!(updated.table_error_retry_delay_ms, 1234);
        assert!(updated.log_level.is_none());
    }

    #[test]
    fn update_api_pipeline_config_rejects_cleared_publication_name() {
        let stored: StoredPipelineConfig = ApiPipelineConfig {
            publication_name: "publication".to_owned(),
            batch: None,
            table_error_retry_delay_ms: None,
            table_error_retry_max_attempts: None,
            max_table_sync_workers: None,
            max_copy_connections_per_table: None,
            memory_refresh_interval_ms: None,
            replication_lag_refresh_interval_ms: None,
            memory_backpressure: None,
            table_sync_copy: None,
            invalidated_slot_behavior: None,
            replicator_resources: None,
            ducklake_maintenance: None,
            log_level: None,
        }
        .into();
        let update = UpdateApiPipelineConfig {
            publication_name: UpdateField::Clear,
            ..UpdateApiPipelineConfig::default()
        };

        let error = update.merge_into_stored(stored).unwrap_err();

        assert!(matches!(
            error,
            PipelineConfigUpdateError::RequiredFieldCleared { field: "publication_name" }
        ));
    }

    #[test]
    fn update_api_pipeline_config_resets_cleared_defaulted_field() {
        let stored: StoredPipelineConfig = ApiPipelineConfig {
            publication_name: "publication".to_owned(),
            batch: Some(BatchConfig {
                max_fill_ms: 5000,
                memory_budget_ratio: 0.2,
                max_bytes: 8 * 1024 * 1024,
            }),
            table_error_retry_delay_ms: None,
            table_error_retry_max_attempts: None,
            max_table_sync_workers: None,
            max_copy_connections_per_table: None,
            memory_refresh_interval_ms: None,
            replication_lag_refresh_interval_ms: None,
            memory_backpressure: None,
            table_sync_copy: None,
            invalidated_slot_behavior: None,
            replicator_resources: None,
            ducklake_maintenance: None,
            log_level: None,
        }
        .into();
        let update = UpdateApiPipelineConfig {
            batch: UpdateField::Clear,
            ..UpdateApiPipelineConfig::default()
        };

        let updated = update.merge_into_stored(stored).unwrap();

        assert_eq!(updated.batch.max_fill_ms, BatchConfig::DEFAULT_MAX_FILL_MS);
        assert_eq!(updated.batch.memory_budget_ratio, BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO);
        assert_eq!(updated.batch.max_bytes, BatchConfig::DEFAULT_MAX_BYTES);
    }

    #[test]
    fn stored_pipeline_config_deserializes_without_replicator_resources() {
        let config = StoredPipelineConfig {
            publication_name: "test_publication".to_owned(),
            batch: BatchConfig {
                max_fill_ms: 5000,
                memory_budget_ratio: 0.2,
                max_bytes: 8 * 1024 * 1024,
            },
            table_error_retry_delay_ms: 2000,
            table_error_retry_max_attempts: 7,
            max_table_sync_workers: 4,
            max_copy_connections_per_table: 8,
            memory_refresh_interval_ms: 100,
            replication_lag_refresh_interval_ms: 10_000,
            memory_backpressure: Some(MemoryBackpressureConfig {
                activate_threshold: 0.8,
                resume_threshold: 0.7,
            }),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            replicator_resources: Some(ReplicatorResourcesConfig {
                cpu_request_millicores: Some(500),
                memory_request_mib: Some(2000),
                ..ReplicatorResourcesConfig::default()
            }),
            ducklake_maintenance: None,
            log_level: None,
            invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
        };
        let mut json = serde_json::to_value(config).unwrap();
        json.as_object_mut().unwrap().remove("replicator_resources");

        let deserialized: StoredPipelineConfig = serde_json::from_value(json).unwrap();

        assert_eq!(deserialized.replicator_resources, None);
    }
}
