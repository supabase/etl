use config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, RetryConfig};
use etl::v2::destination::base::Destination;
use etl::v2::pipeline::{Pipeline, PipelineId};
use etl::v2::state::store::base::StateStore;

pub fn create_pipeline<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    state_store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let config = PipelineConfig {
        id: pipeline_id,
        pg_connection: pg_connection_config.clone(),
        batch: BatchConfig {
            max_size: 1,
            max_fill_ms: 1000,
        },
        apply_worker_init_retry: RetryConfig {
            max_attempts: 2,
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_factor: 2.0,
        },
        publication_name,
    };

    Pipeline::new(pipeline_id, config, state_store, destination)
}

pub fn create_pipeline_with<S, D>(
    pg_connection_config: &PgConnectionConfig,
    pipeline_id: PipelineId,
    publication_name: String,
    state_store: S,
    destination: D,
    batch_config: Option<BatchConfig>,
) -> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let batch = batch_config.unwrap_or(BatchConfig {
        max_size: 1,
        max_fill_ms: 1000,
    });

    let config = PipelineConfig {
        id: pipeline_id,
        pg_connection: pg_connection_config.clone(),
        batch,
        apply_worker_init_retry: RetryConfig {
            max_attempts: 2,
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_factor: 2.0,
        },
        publication_name,
    };

    Pipeline::new(pipeline_id, config, state_store, destination)
}
