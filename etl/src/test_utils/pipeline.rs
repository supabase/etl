use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig};
use uuid::Uuid;

use crate::destination::Destination;
use crate::pipeline::Pipeline;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;

/// Generates a test-specific replication slot name with a random component.
///
/// This function prefixes the provided slot name with "test_" to avoid conflicts
/// with other replication slots and other tests running in parallel.
pub fn test_slot_name(slot_name: &str) -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("test_{slot_name}_{uuid}")
}

pub fn create_pipeline<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let config = PipelineConfig {
        id: pipeline_id,
        publication_name,
        pg_connection: pg_connection_config.clone(),
        batch: BatchConfig {
            max_size: 1,
            max_fill_ms: 1000,
        },
        table_error_retry_delay_ms: 1000,
        max_table_sync_workers: 1,
    };

    Pipeline::new(pipeline_id, config, store, destination)
}

pub fn create_pipeline_with<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    store: S,
    destination: D,
    batch_config: Option<BatchConfig>,
) -> Pipeline<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let batch = batch_config.unwrap_or(BatchConfig {
        max_size: 1,
        max_fill_ms: 1000,
    });

    let config = PipelineConfig {
        id: pipeline_id,
        publication_name,
        pg_connection: pg_connection_config.clone(),
        batch,
        table_error_retry_delay_ms: 1000,
        max_table_sync_workers: 1,
    };

    Pipeline::new(pipeline_id, config, store, destination)
}
