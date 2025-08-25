use serde::{Deserialize, Serialize};

use crate::shared::{PgConnectionConfig, ValidationError, batch::BatchConfig};

/// Configuration for an ETL pipeline.
///
/// Contains all settings required to run a replication pipeline including
/// source database connection, batching parameters, and worker limits.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    /// Number of ms between one retry and another when a table error occurs.
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of table sync workers that can run at a time
    pub max_table_sync_workers: u16,
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

        Ok(())
    }
}
