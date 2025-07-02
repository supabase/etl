use config::shared::PgConnectionConfig;
use etl::v2::config::batch::BatchConfig;
use etl::v2::config::pipeline::PipelineConfig;
use etl::v2::config::retry::RetryConfig;
use etl::v2::destination::base::Destination;
use etl::v2::pipeline::{Pipeline, PipelineIdentity};
use etl::v2::state::store::base::StateStore;
use rand::random;
use std::time::Duration;

pub fn create_pipeline_identity(publication_name: &str) -> PipelineIdentity {
    let pipeline_id = random();
    PipelineIdentity::new(pipeline_id, publication_name)
}

pub fn spawn_pg_pipeline<S, D>(
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
        pg_connection: pg_connection_config.clone(),
        batch: BatchConfig {
            max_size: 1,
            max_fill: Duration::from_secs(1),
        },
        apply_worker_initialization_retry: RetryConfig {
            max_attempts: 2,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(5),
            backoff_factor: 2.0,
        },
    };

    Pipeline::new(identity.clone(), config, state_store, destination)
}
