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
    ///
    /// In standard mode, this connects directly to the primary database.
    /// In replica mode, this connects to the read replica for WAL streaming.
    pub pg_connection: PgConnectionConfig,
    /// Optional connection to the primary database for read replica mode.
    ///
    /// When set, enables replica mode where:
    /// - `pg_connection` connects to the read replica for WAL streaming
    /// - `primary_connection` connects to the primary for heartbeat emissions
    ///
    /// The heartbeat worker uses this connection to call `pg_logical_emit_message()`
    /// which generates WAL activity and prevents replication slot invalidation.
    #[serde(default)]
    pub primary_connection: Option<PgConnectionConfig>,
    /// Heartbeat configuration for read replica mode.
    ///
    /// Controls how frequently heartbeats are emitted and reconnection behavior.
    /// Only used when `primary_connection` is set (replica mode).
    /// If not specified, defaults are used when in replica mode.
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
    /// Returns `true` if the pipeline is configured for read replica mode.
    ///
    /// Replica mode is enabled when `primary_connection` is set, indicating that
    /// the pipeline should stream from a replica while sending heartbeats to the primary.
    pub fn is_replica_mode(&self) -> bool {
        self.primary_connection.is_some()
    }

    /// Returns the heartbeat configuration, using defaults if not explicitly set.
    ///
    /// This method is useful when starting the heartbeat worker, as it provides
    /// sensible defaults even when the user hasn't explicitly configured heartbeat settings.
    pub fn heartbeat_config(&self) -> HeartbeatConfig {
        self.heartbeat.clone().unwrap_or_default()
    }

    /// Validates pipeline configuration settings.
    ///
    /// Checks connection settings and ensures worker count is non-zero.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pg_connection.tls.validate()?;

        // Validate primary connection TLS if in replica mode
        if let Some(primary) = &self.primary_connection {
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
    /// Optional connection to the primary database for read replica mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_connection: Option<PgConnectionConfigWithoutSecrets>,
    /// Heartbeat configuration for read replica mode.
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

    #[test]
    fn test_is_replica_mode_false_by_default() {
        let json = r#"{
            "id": 1,
            "publication_name": "test",
            "pg_connection": {
                "host": "localhost",
                "port": 5432,
                "name": "testdb",
                "username": "postgres",
                "tls": { "enabled": false, "trusted_root_certs": "" }
            },
            "batch": { "max_size": 100, "max_fill_ms": 1000 },
            "table_error_retry_delay_ms": 1000,
            "table_error_retry_max_attempts": 3,
            "max_table_sync_workers": 4,
            "table_sync_copy": { "type": "include_all_tables" }
        }"#;
        let config: PipelineConfig = serde_json::from_str(json).unwrap();
        assert!(!config.is_replica_mode());
    }

    #[test]
    fn test_heartbeat_config_defaults() {
        let json = r#"{
            "id": 1,
            "publication_name": "test",
            "pg_connection": {
                "host": "localhost",
                "port": 5432,
                "name": "testdb",
                "username": "postgres",
                "tls": { "enabled": false, "trusted_root_certs": "" }
            },
            "batch": { "max_size": 100, "max_fill_ms": 1000 },
            "table_error_retry_delay_ms": 1000,
            "table_error_retry_max_attempts": 3,
            "max_table_sync_workers": 4,
            "table_sync_copy": { "type": "include_all_tables" }
        }"#;
        let config: PipelineConfig = serde_json::from_str(json).unwrap();
        let heartbeat = config.heartbeat_config();
        assert_eq!(heartbeat.interval_secs, 30);
        assert_eq!(heartbeat.initial_backoff_secs, 1);
        assert_eq!(heartbeat.max_backoff_secs, 60);
        assert_eq!(heartbeat.jitter_percent, 25);
    }
}
