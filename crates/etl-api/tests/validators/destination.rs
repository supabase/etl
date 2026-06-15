use etl_api::{
    configs::destination::FullApiDestinationConfig,
    validation::{FailureType, validate_destination},
};
use etl_config::SerializableSecretString;
use etl_postgres::sqlx::test_utils::drop_pg_database;
use sqlx::Executor;

use super::{create_pipeline_config, create_validation_context_with_source};

fn create_bigquery_config() -> FullApiDestinationConfig {
    FullApiDestinationConfig::BigQuery {
        project_id: "project-id".to_owned(),
        dataset_id: "dataset-id".to_owned(),
        service_account_key: SerializableSecretString::from("service-account-key".to_owned()),
        max_staleness_mins: None,
        connection_pool_size: Some(1),
    }
}

#[tokio::test]
async fn validate_destination_warns_for_unsupported_replica_identity() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute(
        "create table alt_identity_table (
            id serial primary key,
            email text not null,
            name text
        )",
    )
    .await
    .unwrap();
    pool.execute("create unique index alt_identity_table_email_idx on alt_identity_table (email)")
        .await
        .unwrap();
    pool.execute(
        "alter table alt_identity_table replica identity using index alt_identity_table_email_idx",
    )
    .await
    .unwrap();
    pool.execute("create publication alt_identity_pub for table alt_identity_table").await.unwrap();

    let pipeline_config = create_pipeline_config("alt_identity_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let replica_identity_failure = failures
        .iter()
        .find(|failure| failure.name == "Unsupported Replica Identity")
        .expect("Should warn for alternative replica identity");
    assert_eq!(replica_identity_failure.failure_type, FailureType::Warning);
    assert!(
        replica_identity_failure.reason.contains("alt_identity_table (alternative_key)"),
        "Failure reason should mention the table and identity type"
    );
    assert!(
        replica_identity_failure.reason.contains("REPLICA IDENTITY DEFAULT"),
        "Failure reason should suggest primary-key identity"
    );
    assert!(
        replica_identity_failure.reason.contains("REPLICA IDENTITY FULL"),
        "Failure reason should suggest full identity"
    );
    assert!(
        replica_identity_failure.reason.contains("UPDATE or DELETE"),
        "Failure reason should explain the operation risk"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_accepts_primary_key_replica_identity_index() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute(
        "create table primary_key_identity_table (
            id serial primary key,
            name text
        )",
    )
    .await
    .unwrap();
    pool.execute(
        "alter table primary_key_identity_table replica identity using index \
         primary_key_identity_table_pkey",
    )
    .await
    .unwrap();
    pool.execute(
        "create publication primary_key_identity_pub for table primary_key_identity_table",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("primary_key_identity_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let replica_identity_failure =
        failures.iter().find(|failure| failure.name == "Unsupported Replica Identity");
    assert!(
        replica_identity_failure.is_none(),
        "Should accept USING INDEX when it points at the primary-key index"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_accepts_identity_index_matching_primary_key_columns() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute(
        "create table primary_key_columns_identity_table (
            account_id integer not null,
            id integer not null,
            name text,
            primary key (account_id, id)
        )",
    )
    .await
    .unwrap();
    pool.execute(
        "create unique index primary_key_columns_identity_idx
         on primary_key_columns_identity_table (id, account_id)",
    )
    .await
    .unwrap();
    pool.execute(
        "alter table primary_key_columns_identity_table replica identity using index \
         primary_key_columns_identity_idx",
    )
    .await
    .unwrap();
    pool.execute(
        "create publication primary_key_columns_identity_pub
         for table primary_key_columns_identity_table",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("primary_key_columns_identity_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let replica_identity_failure =
        failures.iter().find(|failure| failure.name == "Unsupported Replica Identity");
    assert!(
        replica_identity_failure.is_none(),
        "Should accept USING INDEX when it covers the same columns as the primary key"
    );

    drop_pg_database(&config).await;
}
