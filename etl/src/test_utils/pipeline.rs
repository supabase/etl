use crate::destination::Destination;
use crate::pipeline::Pipeline;
use crate::store::state::StateStore;
use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, TableSyncCopyConfig};

/// Creates a test-specific replication slot name.
///
/// Prefixes the slot name with "test_" to distinguish test slots from production slots.
pub fn test_slot_name(slot_name: &str) -> String {
    format!("test_{}", slot_name)
}

/// Builder pattern for constructing test pipelines.
///
/// Provides sensible defaults for testing while allowing customization of specific fields.
///
/// # Examples
///
/// ```ignore
/// // Create a pipeline with default settings.
/// let pipeline = PipelineBuilder::new(pg_config, id, pub_name, store, dest)
///     .build();
///
/// // Create a pipeline with custom batch settings.
/// let pipeline = PipelineBuilder::new(pg_config, id, pub_name, store, dest)
///     .with_batch(BatchConfig { max_size: 100, max_fill_ms: 500 })
///     .build();
/// ```
pub struct PipelineBuilder<S, D>
where
    S: StateStore,
    D: Destination,
{
    pg_connection_config: PgConnectionConfig,
    pipeline_id: u64,
    publication_name: String,
    store: S,
    destination: D,
    batch: Option<BatchConfig>,
    max_table_sync_workers: Option<u16>,
    table_error_retry_delay_ms: Option<u64>,
    table_error_retry_max_attempts: Option<u32>,
    table_sync_copy: Option<TableSyncCopyConfig>,
}

impl<S, D> PipelineBuilder<S, D>
where
    S: StateStore,
    D: Destination,
{
    /// Creates a new pipeline builder with required parameters.
    pub fn new(
        pg_connection_config: PgConnectionConfig,
        pipeline_id: u64,
        publication_name: String,
        store: S,
        destination: D,
    ) -> Self {
        Self {
            pg_connection_config,
            pipeline_id,
            publication_name,
            store,
            destination,
            batch: None,
            max_table_sync_workers: None,
            table_error_retry_delay_ms: None,
            table_error_retry_max_attempts: None,
            table_sync_copy: None,
        }
    }

    /// Sets custom batch configuration.
    pub fn with_batch(mut self, batch: BatchConfig) -> Self {
        self.batch = Some(batch);
        self
    }

    /// Sets the maximum number of concurrent table sync workers.
    pub fn with_max_table_sync_workers(mut self, max_workers: u16) -> Self {
        self.max_table_sync_workers = Some(max_workers);
        self
    }

    /// Sets the delay between table error retries.
    pub fn with_table_error_retry_delay_ms(mut self, delay_ms: u64) -> Self {
        self.table_error_retry_delay_ms = Some(delay_ms);
        self
    }

    /// Sets the maximum number of table error retry attempts.
    pub fn with_table_error_retry_max_attempts(mut self, max_attempts: u32) -> Self {
        self.table_error_retry_max_attempts = Some(max_attempts);
        self
    }

    /// Sets the table sync copy configuration.
    pub fn with_table_sync_copy(mut self, table_sync_copy: TableSyncCopyConfig) -> Self {
        self.table_sync_copy = Some(table_sync_copy);
        self
    }

    /// Builds the pipeline with the configured settings.
    ///
    /// Uses default values for any unset optional parameters.
    pub fn build(self) -> Pipeline<S, D> {
        let config = PipelineConfig {
            id: self.pipeline_id,
            publication_name: self.publication_name,
            pg_connection: self.pg_connection_config,
            primary_connection: None,
            heartbeat: None,
            batch: self.batch.unwrap_or(BatchConfig {
                max_size: 1,
                max_fill_ms: 1000,
            }),
            max_table_sync_workers: self.max_table_sync_workers.unwrap_or(4),
            table_error_retry_delay_ms: self.table_error_retry_delay_ms.unwrap_or(1000),
            table_error_retry_max_attempts: self.table_error_retry_max_attempts.unwrap_or(3),
            table_sync_copy: self.table_sync_copy.unwrap_or_default(),
        };

        Pipeline::new(config, self.store, self.destination)
    }
}
