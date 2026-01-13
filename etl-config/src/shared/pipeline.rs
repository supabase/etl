use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::{
    HeartbeatConfig, PgConnectionConfig, PgConnectionConfigWithoutSecrets, ValidationError,
    batch::BatchConfig,
};

/// Selection rules for tables participating in replication.
///
/// Controls which tables are eligible for initial table copy and streaming.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum TableSyncCopyConfig {
    /// Performs the initial copy for all tables.
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

impl Default for TableSyncCopyConfig {
    fn default() -> Self {
        Self::IncludeAllTables
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
    /// Optional connection to the primary database when `pg_connection` points to a read replica.
    ///
    /// When set, enables heartbeat emission to prevent replication slot invalidation on the replica.
    /// The heartbeat worker will connect to this primary database and periodically emit WAL messages
    /// to prevent the primary from recycling WAL segments needed by the replica's replication slot.
    #[serde(default)]
    pub primary_connection: Option<PgConnectionConfig>,
    /// Heartbeat configuration for read replica support.
    ///
    /// Only used when `primary_connection` is set. Controls the frequency and retry behavior
    /// of heartbeat emissions to the primary database.
    #[serde(default)]
    pub heartbeat: Option<HeartbeatConfig>,
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another when a table error occurs.
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic retry attempts before requiring manual intervention.
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    pub max_table_sync_workers: u16,
    /// Selection rules for tables participating in replication.
    pub table_sync_copy: TableSyncCopyConfig,
}

impl PipelineConfig {
    /// Validates pipeline configuration settings.
    ///
    /// Checks connection settings and ensures worker count is non-zero.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pg_connection.tls.validate()?;

        // Validate primary connection TLS if configured
        if let Some(ref primary) = self.primary_connection {
            primary.tls.validate()?;
        }

        if self.max_table_sync_workers == 0 {
            return Err(ValidationError::MaxTableSyncWorkersZero);
        }

        if self.table_error_retry_max_attempts == 0 {
            return Err(ValidationError::TableErrorRetryMaxAttemptsZero);
        }

        Ok(())
    }

    /// Returns `true` if the pipeline is configured for read replica mode.
    ///
    /// Read replica mode is enabled when a `primary_connection` is configured,
    /// which enables heartbeat emission to prevent replication slot invalidation.
    pub fn is_replica_mode(&self) -> bool {
        self.primary_connection.is_some()
    }

    /// Returns the heartbeat configuration, using defaults if not explicitly set.
    ///
    /// Only meaningful when `is_replica_mode()` returns `true`.
    pub fn heartbeat_config(&self) -> HeartbeatConfig {
        self.heartbeat.clone().unwrap_or_default()
    }
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
    /// Optional connection to the primary database when `pg_connection` points to a read replica.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_connection: Option<PgConnectionConfigWithoutSecrets>,
    /// Heartbeat configuration for read replica support.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<HeartbeatConfig>,
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another when a table error occurs.
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic retry attempts before requiring manual intervention.
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    pub max_table_sync_workers: u16,
    /// Selection rules for tables participating in replication.
    pub table_sync_copy: TableSyncCopyConfig,
}

impl From<PipelineConfig> for PipelineConfigWithoutSecrets {
    fn from(value: PipelineConfig) -> Self {
        PipelineConfigWithoutSecrets {
            id: value.id,
            publication_name: value.publication_name,
            pg_connection: value.pg_connection.into(),
            primary_connection: value.primary_connection.map(Into::into),
            heartbeat: value.heartbeat,
            batch: value.batch,
            table_error_retry_delay_ms: value.table_error_retry_delay_ms,
            table_error_retry_max_attempts: value.table_error_retry_max_attempts,
            max_table_sync_workers: value.max_table_sync_workers,
            table_sync_copy: value.table_sync_copy,
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
