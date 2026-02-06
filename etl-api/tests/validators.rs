mod support;

use etl_api::configs::destination::{FullApiDestinationConfig, FullApiIcebergConfig};
use etl_api::configs::pipeline::FullApiPipelineConfig;
use etl_api::validation::{
    FailureType, ValidationContext, validate_destination, validate_pipeline,
};
use etl_config::{Environment, SerializableSecretString};
use etl_destinations::bigquery::test_utils::{
    setup_bigquery_database, setup_bigquery_database_without_dataset,
};
use etl_destinations::iceberg::test_utils::{
    LAKEKEEPER_URL, LakekeeperClient, MINIO_PASSWORD, MINIO_URL, MINIO_USERNAME,
};
use etl_postgres::sqlx::test_utils::{create_pg_database, drop_pg_database};
use sqlx::Executor;
use support::database::get_test_db_config;

fn create_validation_context() -> ValidationContext {
    let environment = Environment::load().expect("Failed to load environment");
    ValidationContext::builder(environment).build()
}

async fn create_validation_context_with_source() -> (
    ValidationContext,
    sqlx::PgPool,
    etl_config::shared::PgConnectionConfig,
) {
    let config = get_test_db_config();
    let pool = create_pg_database(&config).await;
    let environment = Environment::load().expect("Failed to load environment");
    let ctx = ValidationContext::builder(environment)
        .source_pool(pool.clone())
        .build();

    (ctx, pool, config)
}

fn create_iceberg_config(warehouse_name: &str) -> FullApiDestinationConfig {
    FullApiDestinationConfig::Iceberg {
        config: FullApiIcebergConfig::Rest {
            catalog_uri: format!("{LAKEKEEPER_URL}/catalog"),
            warehouse_name: warehouse_name.to_string(),
            s3_access_key_id: SerializableSecretString::from(MINIO_USERNAME.to_string()),
            s3_secret_access_key: SerializableSecretString::from(MINIO_PASSWORD.to_string()),
            s3_endpoint: MINIO_URL.to_string(),
            namespace: Some("test".to_string()),
        },
    }
}

fn create_bigquery_config(
    project_id: &str,
    dataset_id: &str,
    sa_key: &str,
) -> FullApiDestinationConfig {
    FullApiDestinationConfig::BigQuery {
        project_id: project_id.to_string(),
        dataset_id: dataset_id.to_string(),
        service_account_key: SerializableSecretString::from(sa_key.to_string()),
        max_staleness_mins: None,
        connection_pool_size: None,
    }
}

fn create_pipeline_config(publication_name: &str) -> FullApiPipelineConfig {
    FullApiPipelineConfig {
        publication_name: publication_name.to_string(),
        max_table_sync_workers: Some(2),
        batch: None,
        log_level: None,
        table_error_retry_delay_ms: None,
        table_error_retry_max_attempts: None,
        table_sync_copy: None,
        invalidated_slot_behavior: None,
    }
}

#[tokio::test]
async fn validate_iceberg_connection_success() {
    let lakekeeper = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper
        .create_warehouse()
        .await
        .expect("Failed to create warehouse");

    let ctx = create_validation_context();
    let config = create_iceberg_config(&warehouse_name);
    let failures = validate_destination(&ctx, &config).await.unwrap();

    let _ = lakekeeper.drop_warehouse(warehouse_id).await;

    assert!(failures.is_empty(), "Expected no failures: {failures:?}");
}

#[tokio::test]
async fn validate_iceberg_connection_failure() {
    let ctx = create_validation_context();
    let config = create_iceberg_config("nonexistent-warehouse");
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Iceberg Connection Failed");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
async fn validate_bigquery_connection_success() {
    let db = setup_bigquery_database().await;

    let ctx = create_validation_context();
    let config = create_bigquery_config(db.project_id(), db.dataset_id(), &db.sa_key());
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(failures.is_empty(), "Expected no failures: {failures:?}");
    // db is dropped here, automatically cleaning up the dataset
}

#[tokio::test]
async fn validate_bigquery_dataset_not_found() {
    let db = setup_bigquery_database_without_dataset().await;

    let ctx = create_validation_context();
    // Dataset was not created, so validation should fail
    let config = create_bigquery_config(db.project_id(), db.dataset_id(), &db.sa_key());
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "BigQuery Dataset Not Found");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
async fn validate_bigquery_invalid_credentials() {
    let ctx = create_validation_context();
    let config = create_bigquery_config("fake-project", "fake-dataset", "{}");
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "BigQuery Authentication Failed");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
async fn validate_memory_destination_always_passes() {
    let ctx = create_validation_context();
    let config = FullApiDestinationConfig::Memory;
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(failures.is_empty());
}

#[tokio::test]
async fn validate_pipeline_wal_level_success() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table test_table (id serial primary key)")
        .await
        .unwrap();
    pool.execute("create publication test_pub for table test_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("test_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let wal_failure = failures.iter().find(|f| f.name == "Invalid WAL Level");
    assert!(
        wal_failure.is_none(),
        "WAL level should be logical in test DB"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_publication_not_found() {
    let (ctx, _pool, config) = create_validation_context_with_source().await;

    let pipeline_config = create_pipeline_config("nonexistent_publication");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let pub_failure = failures.iter().find(|f| f.name == "Publication Not Found");
    assert!(
        pub_failure.is_some(),
        "Should fail for nonexistent publication"
    );
    assert_eq!(pub_failure.unwrap().failure_type, FailureType::Critical);

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_publication_empty() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create publication empty_pub").await.unwrap();

    let pipeline_config = create_pipeline_config("empty_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let empty_failure = failures.iter().find(|f| f.name == "Publication Empty");
    assert!(empty_failure.is_some(), "Should fail for empty publication");
    assert_eq!(empty_failure.unwrap().failure_type, FailureType::Critical);

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_tables_without_primary_keys() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table no_pk_table (id int, name text)")
        .await
        .unwrap();
    pool.execute("create publication pk_test_pub for table no_pk_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("pk_test_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let pk_failure = failures
        .iter()
        .find(|f| f.name == "Tables Missing Primary Keys");
    assert!(
        pk_failure.is_some(),
        "Should warn for table without primary key"
    );
    assert_eq!(pk_failure.unwrap().failure_type, FailureType::Warning);
    assert!(
        pk_failure.unwrap().reason.contains("no_pk_table"),
        "Failure reason should mention the table name"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_tables_with_primary_keys_passes() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table pk_table (id serial primary key, name text)")
        .await
        .unwrap();
    pool.execute("create publication pk_pass_pub for table pk_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("pk_pass_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let pk_failure = failures
        .iter()
        .find(|f| f.name == "Tables Missing Primary Keys");
    assert!(
        pk_failure.is_none(),
        "Should pass for table with primary key"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_generated_columns() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute(
        "create table gen_col_table (
            id serial primary key,
            first_name text,
            last_name text,
            full_name text generated always as (first_name || ' ' || last_name) stored
        )",
    )
    .await
    .unwrap();

    pool.execute("create publication gen_col_pub for table gen_col_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("gen_col_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let gen_failure = failures
        .iter()
        .find(|f| f.name == "Tables With Generated Columns");
    assert!(
        gen_failure.is_some(),
        "Should warn about table with generated columns"
    );
    assert_eq!(gen_failure.unwrap().failure_type, FailureType::Warning);
    assert!(
        gen_failure.unwrap().reason.contains("gen_col_table"),
        "Failure reason should mention the table name"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_no_generated_columns_passes() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table normal_table (id serial primary key, name text)")
        .await
        .unwrap();
    pool.execute("create publication normal_pub for table normal_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("normal_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let gen_failure = failures
        .iter()
        .find(|f| f.name == "Tables With Generated Columns");
    assert!(
        gen_failure.is_none(),
        "Should not warn for table without generated columns"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_all_checks_pass() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table good_table (id serial primary key, data text)")
        .await
        .unwrap();
    pool.execute("create publication good_pub for table good_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("good_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    assert!(
        failures.is_empty(),
        "Expected no failures for properly configured pipeline, got: {failures:?}"
    );

    drop_pg_database(&config).await;
}
