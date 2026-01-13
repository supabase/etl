use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, TableSyncCopyConfig};
use uuid::Uuid;

use crate::destination::Destination;
use crate::pipeline::Pipeline;
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;

pub fn test_slot_name(slot_name: &str) -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("test_{slot_name}_{uuid}")
}

pub struct PipelineBuilder<S, D> {
    pg_connection_config: PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    batch: Option<BatchConfig>,
    table_error_retry_delay_ms: u64,
    table_error_retry_max_attempts: u32,
    max_table_sync_workers: u16,
    table_sync_copy: Option<TableSyncCopyConfig>,
}

impl<S, D> PipelineBuilder<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    pub fn new(
        pg_connection_config: PgConnectionConfig,
        pipeline_id: PipelineId,
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
            table_error_retry_delay_ms: 1000,
            table_error_retry_max_attempts: 2,
            max_table_sync_workers: 1,
            table_sync_copy: None,
        }
    }

    pub fn with_batch_config(mut self, batch: BatchConfig) -> Self {
        self.batch = Some(batch);
        self
    }

    pub fn with_table_sync_copy_config(mut self, table_sync_copy: TableSyncCopyConfig) -> Self {
        self.table_sync_copy = Some(table_sync_copy);
        self
    }

    pub fn with_retry_config(mut self, delay_ms: u64, max_attempts: u32) -> Self {
        self.table_error_retry_delay_ms = delay_ms;
        self.table_error_retry_max_attempts = max_attempts;
        self
    }

    pub fn with_max_table_sync_workers(mut self, workers: u16) -> Self {
        self.max_table_sync_workers = workers;
        self
    }

    pub fn build(self) -> Pipeline<S, D> {
        let config = PipelineConfig {
            id: self.pipeline_id,
            publication_name: self.publication_name,
            pg_connection: self.pg_connection_config,
            primary_connection: None,
            heartbeat: None,
            batch: self.batch.unwrap_or(BatchConfig { max_size: 1, max_fill_ms: 1000 }),
            table_error_retry_delay_ms: self.table_error_retry_delay_ms,
            table_error_retry_max_attempts: self.table_error_retry_max_attempts,
            max_table_sync_workers: self.max_table_sync_workers,
            table_sync_copy: self.table_sync_copy.unwrap_or_default(),
        };
        Pipeline::new(config, self.store, self.destination)
    }
}

pub fn create_pipeline<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    PipelineBuilder::new(pg_connection_config.clone(), pipeline_id, publication_name, store, destination).build()
}

pub fn create_pipeline_with_batch_config<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    batch: BatchConfig,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    PipelineBuilder::new(pg_connection_config.clone(), pipeline_id, publication_name, store, destination)
        .with_batch_config(batch)
        .with_retry_config(1000, 5)
        .build()
}

pub fn create_pipeline_with_table_sync_copy_config<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    table_sync_copy: TableSyncCopyConfig,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + CleanupStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    PipelineBuilder::new(pg_connection_config.clone(), pipeline_id, publication_name, store, destination)
        .with_table_sync_copy_config(table_sync_copy)
        .build()
}
