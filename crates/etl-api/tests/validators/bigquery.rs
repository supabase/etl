use etl_api::{
    configs::destination::FullApiDestinationConfig,
    validation::{FailureType, ValidationContext, validate_destination, validate_pipeline},
};
use etl_config::{Environment, SerializableSecretString};
use etl_destinations::bigquery::test_utils::{
    setup_bigquery_database, setup_bigquery_database_without_dataset,
    skip_if_missing_bigquery_env_vars,
};
use etl_postgres::sqlx::test_utils::drop_pg_database;

use super::{
    create_pipeline_config, create_validation_context, create_validation_context_with_source,
};

fn create_bigquery_config(
    project_id: &str,
    dataset_id: &str,
    sa_key: &str,
) -> FullApiDestinationConfig {
    FullApiDestinationConfig::BigQuery {
        project_id: project_id.to_owned(),
        dataset_id: dataset_id.to_owned(),
        service_account_key: SerializableSecretString::from(sa_key.to_owned()),
        max_staleness_mins: None,
        connection_pool_size: None,
    }
}

#[tokio::test]
async fn validate_bigquery_connection_success() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let db = setup_bigquery_database().await;

    let ctx = create_validation_context();
    let config = create_bigquery_config(db.project_id(), db.dataset_id(), &db.sa_key());
    let failures = validate_destination(&ctx, &config, None).await.unwrap();

    assert!(failures.is_empty(), "Expected no failures: {failures:?}");
}

#[tokio::test]
async fn validate_bigquery_dataset_not_found() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let db = setup_bigquery_database_without_dataset().await;

    let ctx = create_validation_context();
    let config = create_bigquery_config(db.project_id(), db.dataset_id(), &db.sa_key());
    let failures = validate_destination(&ctx, &config, None).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "BigQuery Dataset Not Found");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
async fn validate_bigquery_invalid_credentials() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let ctx = create_validation_context();
    let config = create_bigquery_config("fake-project", "fake-dataset", "{}");
    let failures = validate_destination(&ctx, &config, None).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "BigQuery Authentication Failed");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
async fn validate_pipeline_includes_source_validation() {
    let (ctx, _pool, config) = create_validation_context_with_source().await;
    let environment = Environment::load().expect("Failed to load environment");
    let ctx = ValidationContext::builder(environment)
        .source_pool(ctx.source_pool.expect("source pool should be present for validation"))
        .trusted_username(Some("different_user".to_owned()))
        .build();

    let failures = validate_pipeline(&ctx, &create_pipeline_config("test_pub")).await.unwrap();

    let source_failure = failures.iter().find(|failure| failure.name == "Invalid source username");
    assert!(source_failure.is_some(), "Expected source validation failure");

    drop_pg_database(&config).await;
}
