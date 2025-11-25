use serde::{Deserialize, Serialize};

use crate::shared::{
    PgConnectionConfig, PgConnectionConfigWithoutSecrets, ValidationError, batch::BatchConfig,
};

/// Controls how and when schemas are created in destinations.
///
/// The creation mode determines whether schema creation is attempted repeatedly
/// or only once during the pipeline lifecycle.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaCreationMode {
    /// Attempt to create schemas only during the initial setup.
    CreateOnce,
    /// Attempt to create schemas every time data is written, recreating them if missing.
    CreateIfMissing,
}

impl Default for SchemaCreationMode {
    fn default() -> Self {
        Self::CreateIfMissing
    }
}

const fn default_schema_creation_mode() -> SchemaCreationMode {
    SchemaCreationMode::CreateIfMissing
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
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another when a table error occurs.
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic retry attempts before requiring manual intervention.
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    pub max_table_sync_workers: u16,
    /// Strategy controlling how destination schemas are created.
    ///
    /// Defaults to [`SchemaCreationMode::CreateIfMissing`] to ensure destinations recover
    /// from out-of-band schema drops.
    #[serde(default = "default_schema_creation_mode")]
    pub schema_creation_mode: SchemaCreationMode,
}

impl PipelineConfig {
    /// Validates pipeline configuration settings.
    ///
    /// Checks connection settings and ensures worker count is non-zero.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pg_connection.tls.validate()?;

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
    /// Batch processing configuration.
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another when a table error occurs.
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic retry attempts before requiring manual intervention.
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    pub max_table_sync_workers: u16,
    /// Strategy controlling how destination schemas are created.
    ///
    /// Defaults to [`SchemaCreationMode::CreateIfMissing`] to ensure destinations recover
    /// from out-of-band schema drops.
    #[serde(default = "default_schema_creation_mode")]
    pub schema_creation_mode: SchemaCreationMode,
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
            schema_creation_mode: value.schema_creation_mode,
        }
    }
}
