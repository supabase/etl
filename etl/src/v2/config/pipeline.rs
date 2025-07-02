use config::shared::PgConnectionConfig;

use crate::v2::config::batch::BatchConfig;
use crate::v2::config::retry::RetryConfig;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// The connection configuration for the Postgres instance to which the pipeline connects for
    /// replication.
    pub pg_connection: PgConnectionConfig,
    /// The batching configuration for the streams used by the workers for table copies and events
    /// streaming.
    pub batch: BatchConfig,
    /// The retry configuration for apply worker initialization.
    pub apply_worker_initialization_retry: RetryConfig,
}
