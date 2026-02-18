use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::{
    PgConnectionConfig, PgConnectionConfigWithoutSecrets, ValidationError, batch::BatchConfig,
};

/// Behavior when the main replication slot is found to be invalidated.
///
/// A replication slot can become invalidated when it falls too far behind the current
/// WAL position (e.g., when `max_slot_wal_keep_size` is exceeded) or when PostgreSQL
/// explicitly invalidates it. This enum controls how the pipeline responds to such situations.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, Default)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum InvalidatedSlotBehavior {
    /// Prevents pipeline startup when the slot is invalidated.
    ///
    /// The pipeline will fail with an error indicating that the slot needs to be
    /// manually addressed before replication can continue. This is the safest option
    /// as it requires explicit operator intervention.
    #[default]
    Error,
    /// Automatically recreates the slot and restarts replication from scratch.
    ///
    /// When an invalidated slot is detected, the pipeline will:
    /// 1. Reset all table replication states to `Init`
    /// 2. Delete all existing replication slots for the pipeline
    /// 3. Create a new replication slot
    /// 4. Run table sync for all tables, respecting [`TableSyncCopyConfig`] rules
    ///
    /// This option allows the pipeline to restart replication and automatically recover.
    Recreate,
}

/// Controls which tables are eligible for initial table copy and streaming.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
#[derive(Default)]
pub enum TableSyncCopyConfig {
    /// Performs the initial copy for all tables.
    #[default]
    IncludeAllTables,
    /// Skips the initial copy for all tables.
    SkipAllTables,
    /// Performs the initial copy for the specified table ids.
    IncludeTables {
        /// Table ids of the table for which copy should be performed.
        table_ids: Vec<u32>,
    },
    /// Skips the initial copy for the specified table ids.
    SkipTables {
        /// Table ids of the table for which copy should be skipped.
        table_ids: Vec<u32>,
    },
}

impl TableSyncCopyConfig {
    /// Returns `true` if the table should be copied during initial sync, `false` otherwise.
    pub fn should_copy_table(&self, table_id: u32) -> bool {
        match self {
            TableSyncCopyConfig::IncludeAllTables => true,
            TableSyncCopyConfig::SkipAllTables => false,
            TableSyncCopyConfig::IncludeTables { table_ids } => table_ids.contains(&table_id),
            TableSyncCopyConfig::SkipTables { table_ids } => !table_ids.contains(&table_id),
        }
    }
}

/// Configuration for an ETL pipeline.
///
/// Contains all settings required to run a replication pipeline including
/// source database connection, batching parameters, and worker limits.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct PipelineConfig {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of replication slots and state
    /// store.
    pub id: u64,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
    /// The connection configuration for the Postgres instance to which the pipeline connects for
    /// replication.
    pub pg_connection: PgConnectionConfig,
    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another for timed worker retries.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_delay_ms")]
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic timed retry attempts before failing the worker.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_max_attempts")]
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    #[serde(default = "default_max_table_sync_workers")]
    pub max_table_sync_workers: u16,
    /// Maximum parallel connections per table during initial copy.
    /// When 1, the existing serial copy path is used.
    /// When >1 (default), ctid-based partitioning splits the table across N connections.
    #[serde(default = "default_max_copy_connections_per_table")]
    pub max_copy_connections_per_table: u16,
    /// Memory usage ratio above which backpressure is activated.
    ///
    /// The value must be in the `(0.0, 1.0]` interval and greater than
    /// [`Self::memory_backpressure_resume_percentage`].
    #[serde(default = "default_memory_backpressure_activate_percentage")]
    pub memory_backpressure_activate_percentage: f32,
    /// Memory usage ratio below which backpressure is released.
    ///
    /// The value must be in the `[0.0, 1.0)` interval and lower than
    /// [`Self::memory_backpressure_activate_percentage`].
    #[serde(default = "default_memory_backpressure_resume_percentage")]
    pub memory_backpressure_resume_percentage: f32,
    /// Selection rules for tables participating in replication.
    #[serde(default)]
    pub table_sync_copy: TableSyncCopyConfig,
    /// Behavior when the main replication slot is found to be invalidated.
    #[serde(default)]
    pub invalidated_slot_behavior: InvalidatedSlotBehavior,
}

impl PipelineConfig {
    /// Default retry delay in milliseconds between table error retries.
    pub const DEFAULT_TABLE_ERROR_RETRY_DELAY_MS: u64 = 10000;

    /// Default maximum number of retry attempts for table errors.
    pub const DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS: u32 = 5;

    /// Default maximum number of concurrent table sync workers.
    pub const DEFAULT_MAX_TABLE_SYNC_WORKERS: u16 = 4;

    /// Default maximum parallel connections per table during initial copy.
    pub const DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE: u16 = 2;
    /// Default memory usage ratio to activate backpressure.
    pub const DEFAULT_MEMORY_BACKPRESSURE_ACTIVATE_THRESHOLD: f32 = 0.85;
    /// Default memory usage ratio to release backpressure.
    pub const DEFAULT_MEMORY_BACKPRESSURE_RESUME_THRESHOLD: f32 = 0.75;

    /// Validates pipeline configuration settings.
    ///
    /// Checks batch configuration and ensures worker counts and retry attempts are non-zero.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.batch.validate()?;

        if self.max_table_sync_workers == 0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "max_table_sync_workers".to_string(),
                constraint: "must be greater than 0".to_string(),
            });
        }

        if self.table_error_retry_max_attempts == 0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "table_error_retry_max_attempts".to_string(),
                constraint: "must be greater than 0".to_string(),
            });
        }

        if self.max_copy_connections_per_table == 0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "max_copy_connections_per_table".to_string(),
                constraint: "must be greater than 0".to_string(),
            });
        }

        if !(0.0..=1.0).contains(&self.memory_backpressure_activate_percentage)
            || self.memory_backpressure_activate_percentage == 0.0
        {
            return Err(ValidationError::InvalidFieldValue {
                field: "memory_backpressure_activate_percentage".to_string(),
                constraint: "must be in the (0.0, 1.0] interval".to_string(),
            });
        }

        if !(0.0..=1.0).contains(&self.memory_backpressure_resume_percentage)
            || self.memory_backpressure_resume_percentage == 1.0
        {
            return Err(ValidationError::InvalidFieldValue {
                field: "memory_backpressure_resume_percentage".to_string(),
                constraint: "must be in the [0.0, 1.0) interval".to_string(),
            });
        }

        if self.memory_backpressure_resume_percentage
            >= self.memory_backpressure_activate_percentage
        {
            return Err(ValidationError::InvalidFieldValue {
                field: "memory_backpressure_resume_percentage".to_string(),
                constraint: "must be lower than memory_backpressure_activate_percentage"
                    .to_string(),
            });
        }

        Ok(())
    }
}

fn default_table_error_retry_delay_ms() -> u64 {
    PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS
}

fn default_table_error_retry_max_attempts() -> u32 {
    PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS
}

fn default_max_table_sync_workers() -> u16 {
    PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS
}

fn default_max_copy_connections_per_table() -> u16 {
    PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE
}

fn default_memory_backpressure_activate_percentage() -> f32 {
    PipelineConfig::DEFAULT_MEMORY_BACKPRESSURE_ACTIVATE_THRESHOLD
}

fn default_memory_backpressure_resume_percentage() -> f32 {
    PipelineConfig::DEFAULT_MEMORY_BACKPRESSURE_RESUME_THRESHOLD
}

/// Same as [`PipelineConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineConfigWithoutSecrets {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of replication slots and state
    /// store.
    pub id: u64,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
    /// The connection configuration for the Postgres instance to which the pipeline connects for
    /// replication.
    pub pg_connection: PgConnectionConfigWithoutSecrets,
    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another for timed worker retries.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_delay_ms")]
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic timed retry attempts before failing the worker.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_max_attempts")]
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    #[serde(default = "default_max_table_sync_workers")]
    pub max_table_sync_workers: u16,
    /// Maximum parallel connections per table during initial copy.
    /// When 1, the existing serial copy path is used.
    /// When >1 (default), ctid-based partitioning splits the table across N connections.
    #[serde(default = "default_max_copy_connections_per_table")]
    pub max_copy_connections_per_table: u16,
    /// Memory usage ratio above which backpressure is activated.
    #[serde(default = "default_memory_backpressure_activate_percentage")]
    pub memory_backpressure_activate_percentage: f32,
    /// Memory usage ratio below which backpressure is released.
    #[serde(default = "default_memory_backpressure_resume_percentage")]
    pub memory_backpressure_resume_percentage: f32,
    /// Selection rules for tables participating in replication.
    #[serde(default)]
    pub table_sync_copy: TableSyncCopyConfig,
    /// Behavior when the main replication slot is found to be invalidated.
    #[serde(default)]
    pub invalidated_slot_behavior: InvalidatedSlotBehavior,
}

impl From<PipelineConfig> for PipelineConfigWithoutSecrets {
    fn from(value: PipelineConfig) -> Self {
        PipelineConfigWithoutSecrets {
            id: value.id,
            publication_name: value.publication_name,
            pg_connection: value.pg_connection.into(),
            batch: value.batch,
            table_error_retry_delay_ms: value.table_error_retry_delay_ms,
            table_error_retry_max_attempts: value.table_error_retry_max_attempts,
            max_table_sync_workers: value.max_table_sync_workers,
            max_copy_connections_per_table: value.max_copy_connections_per_table,
            memory_backpressure_activate_percentage: value.memory_backpressure_activate_percentage,
            memory_backpressure_resume_percentage: value.memory_backpressure_resume_percentage,
            table_sync_copy: value.table_sync_copy,
            invalidated_slot_behavior: value.invalidated_slot_behavior,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_sync_copy_serialization_skip_all() {
        let selection = TableSyncCopyConfig::SkipAllTables;
        let json = serde_json::to_string(&selection).unwrap();
        let decoded: TableSyncCopyConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(selection, decoded);
    }

    #[test]
    fn test_table_sync_copy_serialization_include_tables() {
        let selection = TableSyncCopyConfig::IncludeTables {
            table_ids: vec![1, 2, 3],
        };
        let json = serde_json::to_string(&selection).unwrap();
        let decoded: TableSyncCopyConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(selection, decoded);
    }

    #[test]
    fn test_table_sync_copy_serialization_exclude_tables() {
        let selection = TableSyncCopyConfig::SkipTables {
            table_ids: vec![4, 5],
        };
        let json = serde_json::to_string(&selection).unwrap();
        let decoded: TableSyncCopyConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(selection, decoded);
    }
}
