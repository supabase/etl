use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::{PgConnectionConfig, ValidationError, batch::BatchConfig, retry::RetryConfig};

/// Configuration for a pipeline's batching and worker retry behavior.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct PipelineConfig {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of replication slots and state
    /// store.
    #[cfg_attr(feature = "utoipa", schema(example = 123))]
    pub id: u64,

    /// Name of the Postgres publication to use for logical replication.
    #[cfg_attr(feature = "utoipa", schema(example = "my_publication"))]
    pub publication_name: String,

    /// The connection configuration for the Postgres instance to which the pipeline connects for
    /// replication.
    pub pg_connection: PgConnectionConfig,

    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,

    /// Retry configuration for initializing apply workers.
    #[serde(default)]
    pub apply_worker_init_retry: RetryConfig,

    /// Maximum number of table sync workers that can run at a time
    #[cfg_attr(feature = "utoipa", schema(example = 4))]
    pub max_table_sync_workers: u16,
}

impl PipelineConfig {
    /// Validates the [`PipelineConfig`].
    ///
    /// This method checks that the [`PipelineConfig::pg_connection`] and [`PipelineConfig::max_table_sync_workers`] are valid.
    ///
    /// Returns [`ValidationError::MaxTableSyncWorkersZero`] if [`PipelineConfig::max_table_sync_workers`] is zero.
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.pg_connection.tls.validate()?;

        if self.max_table_sync_workers == 0 {
            return Err(ValidationError::MaxTableSyncWorkersZero);
        }

        Ok(())
    }
}
