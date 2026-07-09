#[cfg(feature = "bigquery")]
mod bigquery;
mod destination;
#[cfg(feature = "ducklake")]
mod ducklake;
#[cfg(feature = "iceberg")]
mod iceberg;
mod pipeline;
#[cfg(feature = "snowflake")]
mod snowflake;

use etl_api::{configs::pipeline::CreateApiPipelineConfig, validation::ValidationContext};
use etl_config::{Environment, shared::BatchConfig};
use etl_postgres::sqlx::test_utils::create_pg_database;

use crate::support::database::get_test_db_config;

#[cfg(any(feature = "bigquery", feature = "iceberg", feature = "snowflake"))]
pub(super) fn create_validation_context() -> ValidationContext {
    let environment = Environment::load().expect("Failed to load environment");
    ValidationContext::builder(environment).build()
}

pub(super) async fn create_validation_context_with_source()
-> (ValidationContext, sqlx::PgPool, etl_config::shared::PgConnectionConfig) {
    let config = get_test_db_config();
    let pool = create_pg_database(&config).await;
    let environment = Environment::load().expect("Failed to load environment");
    let ctx = ValidationContext::builder(environment).source_pool(pool.clone()).build();

    (ctx, pool, config)
}

pub(super) fn create_pipeline_config(publication_name: &str) -> CreateApiPipelineConfig {
    CreateApiPipelineConfig {
        publication_name: publication_name.to_owned(),
        batch: Some(BatchConfig {
            max_fill_ms: BatchConfig::DEFAULT_MAX_FILL_MS,
            memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
            max_bytes: BatchConfig::DEFAULT_MAX_BYTES,
        }),
        log_level: None,
        table_error_retry_delay_ms: None,
        table_error_retry_max_attempts: None,
        max_table_sync_workers: Some(2),
        memory_refresh_interval_ms: Some(100),
        replication_lag_refresh_interval_ms: None,
        max_copy_connections_per_table: None,
        memory_backpressure: None,
        table_sync_copy: None,
        invalidated_slot_behavior: None,
        replicator_resources: None,
        ducklake_maintenance: None,
    }
}
