use etl_api::validation::{FailureType, ValidationContext, validate_pipeline, validate_source};
use etl_config::Environment;
use etl_postgres::sqlx::test_utils::drop_pg_database;
use sqlx::Executor;

use super::{create_pipeline_config, create_validation_context_with_source};
use crate::support::database::get_test_db_config;

#[tokio::test]
async fn validate_pipeline_wal_level_success() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table test_table (id serial primary key)").await.unwrap();
    pool.execute("create publication test_pub for table test_table").await.unwrap();

    let pipeline_config = create_pipeline_config("test_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let wal_failure = failures.iter().find(|f| f.name == "Invalid WAL Level");
    assert!(wal_failure.is_none(), "WAL level should be logical in test DB");

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_source_with_trusted_username_mismatch() {
    let config = get_test_db_config();
    let pool = etl_postgres::sqlx::test_utils::create_pg_database(&config).await;
    let environment = Environment::load().expect("Failed to load environment");
    let ctx = ValidationContext::builder(environment)
        .source_pool(pool)
        .trusted_username(Some("different_user".to_owned()))
        .build();

    let failures = validate_source(&ctx).await.unwrap();

    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].name, "Invalid source username");
    assert_eq!(failures[0].failure_type, FailureType::Critical);

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_publication_not_found() {
    let (ctx, _pool, config) = create_validation_context_with_source().await;

    let pipeline_config = create_pipeline_config("nonexistent_publication");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let pub_failure = failures.iter().find(|f| f.name == "Publication Not Found");
    assert!(pub_failure.is_some(), "Should fail for nonexistent publication");
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

    pool.execute("create table no_pk_table (id int, name text)").await.unwrap();
    pool.execute("create publication pk_test_pub for table no_pk_table").await.unwrap();

    let pipeline_config = create_pipeline_config("pk_test_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let pk_failure = failures.iter().find(|f| f.name == "Tables Missing Primary Keys");
    assert!(pk_failure.is_some(), "Should warn for table without primary key");
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

    pool.execute("create table pk_table (id serial primary key, name text)").await.unwrap();
    pool.execute("create publication pk_pass_pub for table pk_table").await.unwrap();

    let pipeline_config = create_pipeline_config("pk_pass_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let pk_failure = failures.iter().find(|f| f.name == "Tables Missing Primary Keys");
    assert!(pk_failure.is_none(), "Should pass for table with primary key");

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

    pool.execute("create publication gen_col_pub for table gen_col_table").await.unwrap();

    let pipeline_config = create_pipeline_config("gen_col_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let gen_failure = failures.iter().find(|f| f.name == "Tables With Generated Columns");
    assert!(gen_failure.is_some(), "Should warn about table with generated columns");
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

    pool.execute("create table normal_table (id serial primary key, name text)").await.unwrap();
    pool.execute("create publication normal_pub for table normal_table").await.unwrap();

    let pipeline_config = create_pipeline_config("normal_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    let gen_failure = failures.iter().find(|f| f.name == "Tables With Generated Columns");
    assert!(gen_failure.is_none(), "Should not warn for table without generated columns");

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_pipeline_all_checks_pass() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table good_table (id serial primary key, data text)").await.unwrap();
    pool.execute("create publication good_pub for table good_table").await.unwrap();

    let pipeline_config = create_pipeline_config("good_pub");
    let failures = validate_pipeline(&ctx, &pipeline_config).await.unwrap();

    assert!(
        failures.is_empty(),
        "Expected no failures for properly configured pipeline, got: {failures:?}"
    );

    drop_pg_database(&config).await;
}
