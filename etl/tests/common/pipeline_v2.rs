use config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, RetryConfig};
use etl::v2::destination::base::Destination;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use rand::random;

pub fn create_pipeline_identity(publication_name: &str) -> PipelineIdentity {
    let pipeline_id = random();
    PipelineIdentity::new(pipeline_id, publication_name)
}

pub fn create_pipeline<S, D>(
    identity: &PipelineIdentity,
    pg_connection_config: &PgConnectionConfig,
    state_store: S,
    destination: D,
) -> Pipeline<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let config = PipelineConfig {
        id: identity.id(),
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
        publication_name: identity.publication_name().to_string(),
    };

    Pipeline::new(identity.clone(), config, state_store, destination)
}
