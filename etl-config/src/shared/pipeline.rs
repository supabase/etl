use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::{
    HeartbeatConfig, PgConnectionConfig, PgConnectionConfigWithoutSecrets, ValidationError,
    batch::BatchConfig,
};

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum TableSyncCopyConfig {
    IncludeAllTables,
    SkipAllTables,
    IncludeTables { table_ids: Vec<u32> },
    SkipTables { table_ids: Vec<u32> },
}

impl TableSyncCopyConfig {
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

#[derive(Clone, Debug, Deserialize)]
pub struct PipelineConfig {
    pub id: u64,
    pub publication_name: String,
    pub pg_connection: PgConnectionConfig,
    /// Optional connection to the primary database for read replica mode.
    #[serde(default)]
    pub primary_connection: Option<PgConnectionConfig>,
    /// Heartbeat configuration for read replica mode.
    #[serde(default)]
    pub heartbeat: Option<HeartbeatConfig>,
    pub batch: BatchConfig,
    pub table_error_retry_delay_ms: u64,
    pub table_error_retry_max_attempts: u32,
    pub max_table_sync_workers: u16,
    pub table_sync_copy: TableSyncCopyConfig,
}

impl PipelineConfig {
    /// Returns `true` if the pipeline is configured for read replica mode.
    pub fn is_replica_mode(&self) -> bool {
        self.primary_connection.is_some()
    }

    /// Returns the heartbeat configuration, using defaults if not explicitly set.
    pub fn heartbeat_config(&self) -> HeartbeatConfig {
        self.heartbeat.clone().unwrap_or_default()
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pg_connection.tls.validate()?;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineConfigWithoutSecrets {
    pub id: u64,
    pub publication_name: String,
    pub pg_connection: PgConnectionConfigWithoutSecrets,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_connection: Option<PgConnectionConfigWithoutSecrets>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<HeartbeatConfig>,
    pub batch: BatchConfig,
    pub table_error_retry_delay_ms: u64,
    pub table_error_retry_max_attempts: u32,
    pub max_table_sync_workers: u16,
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
        let selection = TableSyncCopyConfig::IncludeTables { table_ids: vec![1, 2, 3] };
        let json = serde_json::to_string(&selection).unwrap();
        let decoded: TableSyncCopyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(selection, decoded);
    }
}
