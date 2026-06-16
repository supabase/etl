use etl_api::{
    configs::destination::FullApiDestinationConfig,
    validation::{FailureType, validate_destination},
};
use etl_config::{SerializableSecretString, shared::ClickHouseEngine};
use etl_postgres::{sqlx::test_utils::drop_pg_database, version::POSTGRES_15};
use sqlx::Executor;
use url::Url;

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

fn create_clickhouse_config(engine: ClickHouseEngine) -> FullApiDestinationConfig {
    FullApiDestinationConfig::ClickHouse {
        url: Url::parse("http://clickhouse.example.com:8123").unwrap(),
        user: "etl".to_owned(),
        password: None,
        database: "default".to_owned(),
        engine,
    }
}

async fn create_nested_partitioned_table_with_primary_key(pool: &sqlx::PgPool) {
    pool.execute(
        "create table nested_partitioned_events (
            id bigint not null,
            partition_year integer not null,
            partition_month integer not null,
            data text,
            primary key (id, partition_year, partition_month)
        ) partition by range (partition_year)",
    )
    .await
    .unwrap();
    pool.execute(
        "create table nested_partitioned_events_2026
         partition of nested_partitioned_events
         for values from (2026) to (2027)
         partition by range (partition_month)",
    )
    .await
    .unwrap();
    pool.execute(
        "create table nested_partitioned_events_2026_01
         partition of nested_partitioned_events_2026
         for values from (1) to (2)",
    )
    .await
    .unwrap();
}

async fn create_nested_partitioned_table_without_primary_key(pool: &sqlx::PgPool) {
    pool.execute(
        "create table nested_partitioned_events_no_pk (
            id bigint not null,
            partition_year integer not null,
            partition_month integer not null,
            data text
        ) partition by range (partition_year)",
    )
    .await
    .unwrap();
    pool.execute(
        "create table nested_partitioned_events_no_pk_2026
         partition of nested_partitioned_events_no_pk
         for values from (2026) to (2027)
         partition by range (partition_month)",
    )
    .await
    .unwrap();
    pool.execute(
        "create table nested_partitioned_events_no_pk_2026_01
         partition of nested_partitioned_events_no_pk_2026
         for values from (1) to (2)",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn validate_destination_fails_when_bigquery_source_table_has_no_primary_key() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table no_pk_bigquery_table (id int, name text)").await.unwrap();
    pool.execute("create publication no_pk_bigquery_pub for table no_pk_bigquery_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("no_pk_bigquery_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let primary_key_failure = failures
        .iter()
        .find(|failure| failure.name == "Source Primary Keys Required")
        .expect("Should fail when BigQuery source tables do not have primary keys");
    assert_eq!(primary_key_failure.failure_type, FailureType::Critical);
    assert!(
        primary_key_failure.reason.contains("no_pk_bigquery_table"),
        "Failure reason should mention the table name"
    );
    assert!(
        primary_key_failure.reason.contains("initial loads"),
        "Failure reason should explain why BigQuery requires primary keys"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_fails_when_clickhouse_replacing_merge_tree_table_has_no_primary_key()
{
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table no_pk_clickhouse_table (id int, name text)").await.unwrap();
    pool.execute("create publication no_pk_clickhouse_pub for table no_pk_clickhouse_table")
        .await
        .unwrap();

    let pipeline_config = create_pipeline_config("no_pk_clickhouse_pub");
    let failures = validate_destination(
        &ctx,
        &create_clickhouse_config(ClickHouseEngine::ReplacingMergeTree),
        Some(&pipeline_config),
    )
    .await
    .unwrap();

    let primary_key_failure =
        failures.iter().find(|failure| failure.name == "Source Primary Keys Required").expect(
            "Should fail when ClickHouse ReplacingMergeTree source tables have no primary keys",
        );
    assert_eq!(primary_key_failure.failure_type, FailureType::Critical);
    assert!(
        primary_key_failure.reason.contains("no_pk_clickhouse_table"),
        "Failure reason should mention the table name"
    );
    assert!(
        primary_key_failure.reason.contains("ORDER BY"),
        "Failure reason should explain why ReplacingMergeTree requires primary keys"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_allows_clickhouse_merge_tree_table_without_primary_key() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute("create table no_pk_clickhouse_merge_tree_table (id int, name text)")
        .await
        .unwrap();
    pool.execute(
        "create publication no_pk_clickhouse_merge_tree_pub for table \
         no_pk_clickhouse_merge_tree_table",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("no_pk_clickhouse_merge_tree_pub");
    let failures = validate_destination(
        &ctx,
        &create_clickhouse_config(ClickHouseEngine::MergeTree),
        Some(&pipeline_config),
    )
    .await
    .unwrap();

    let primary_key_failure =
        failures.iter().find(|failure| failure.name == "Source Primary Keys Required");
    assert!(
        primary_key_failure.is_none(),
        "ClickHouse MergeTree should allow source tables without primary keys"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_fails_when_bigquery_column_list_omits_primary_key_column() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    let server_version_num: i32 =
        sqlx::query_scalar("select current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await
            .unwrap();
    if server_version_num < POSTGRES_15 {
        drop_pg_database(&config).await;
        return;
    }

    pool.execute(
        "create table omitted_pk_column_bigquery_table (
            id integer not null,
            account_id integer not null,
            name text,
            primary key (id, account_id)
        )",
    )
    .await
    .unwrap();
    pool.execute(
        "create publication omitted_pk_column_bigquery_pub
         for table omitted_pk_column_bigquery_table (id, name)
         with (publish = 'insert')",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("omitted_pk_column_bigquery_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let primary_key_failure = failures
        .iter()
        .find(|failure| failure.name == "Source Primary Key Columns Required")
        .expect("Should fail when BigQuery source primary-key columns are omitted");
    assert_eq!(primary_key_failure.failure_type, FailureType::Critical);
    assert!(
        primary_key_failure.reason.contains("omitted_pk_column_bigquery_table (account_id)"),
        "Failure reason should mention the table and omitted primary-key column"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_fails_when_clickhouse_merge_tree_omits_primary_key_column() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    let server_version_num: i32 =
        sqlx::query_scalar("select current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await
            .unwrap();
    if server_version_num < POSTGRES_15 {
        drop_pg_database(&config).await;
        return;
    }

    pool.execute(
        "create table omitted_pk_column_clickhouse_table (
            id integer not null,
            account_id integer not null,
            name text,
            primary key (id, account_id)
        )",
    )
    .await
    .unwrap();
    pool.execute(
        "create publication omitted_pk_column_clickhouse_pub
         for table omitted_pk_column_clickhouse_table (id, name)
         with (publish = 'insert')",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("omitted_pk_column_clickhouse_pub");
    let failures = validate_destination(
        &ctx,
        &create_clickhouse_config(ClickHouseEngine::MergeTree),
        Some(&pipeline_config),
    )
    .await
    .unwrap();

    let primary_key_failure = failures
        .iter()
        .find(|failure| failure.name == "Source Primary Key Columns Required")
        .expect("Should fail when ClickHouse source primary-key columns are omitted");
    assert_eq!(primary_key_failure.failure_type, FailureType::Critical);
    assert!(
        primary_key_failure.reason.contains("omitted_pk_column_clickhouse_table (account_id)"),
        "Failure reason should mention the table and omitted primary-key column"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_accepts_nested_partition_leaf_with_parent_primary_key() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    create_nested_partitioned_table_with_primary_key(&pool).await;
    pool.execute(
        "create publication nested_partitioned_leaf_pub
         for table nested_partitioned_events_2026_01",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("nested_partitioned_leaf_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let primary_key_failure =
        failures.iter().find(|failure| failure.name == "Source Primary Keys Required");
    assert!(
        primary_key_failure.is_none(),
        "Primary-key validation should use partition parent key metadata"
    );

    let replica_identity_failure =
        failures.iter().find(|failure| failure.name == "Unsupported Replica Identity");
    assert!(
        replica_identity_failure.is_none(),
        "Replica-identity validation should classify default partition identity as primary-key \
         identity when the partition inherits the parent key"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_fails_for_nested_partition_leaf_without_parent_primary_key() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    create_nested_partitioned_table_without_primary_key(&pool).await;
    pool.execute(
        "create publication nested_partitioned_leaf_no_pk_pub
         for table nested_partitioned_events_no_pk_2026_01",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("nested_partitioned_leaf_no_pk_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let primary_key_failure = failures
        .iter()
        .find(|failure| failure.name == "Source Primary Keys Required")
        .expect("Should fail for nested partition leaves without parent primary keys");
    assert_eq!(primary_key_failure.failure_type, FailureType::Critical);
    assert!(
        primary_key_failure.reason.contains("nested_partitioned_events_no_pk_2026_01"),
        "Failure reason should mention the published leaf partition"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_fails_for_blocking_unsupported_replica_identity() {
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
        .expect("Should fail for alternative replica identity");
    assert_eq!(replica_identity_failure.failure_type, FailureType::Critical);
    assert!(
        replica_identity_failure.reason.contains("alt_identity_table (alternative_key)"),
        "Failure reason should mention the table and identity type"
    );
    assert!(
        replica_identity_failure.reason.contains("cannot safely replicate UPDATE or DELETE"),
        "Failure reason should explain this blocks pipeline start"
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
    assert!(
        replica_identity_failure.reason.contains("large TOAST-backed columns"),
        "Failure reason should recommend full replica identity for TOAST-backed columns"
    );

    drop_pg_database(&config).await;
}

#[tokio::test]
async fn validate_destination_warns_for_insert_only_unsupported_replica_identity() {
    let (ctx, pool, config) = create_validation_context_with_source().await;

    pool.execute(
        "create table insert_only_alt_identity_table (
            id serial primary key,
            email text not null,
            name text
        )",
    )
    .await
    .unwrap();
    pool.execute(
        "create unique index insert_only_alt_identity_table_email_idx
         on insert_only_alt_identity_table (email)",
    )
    .await
    .unwrap();
    pool.execute(
        "alter table insert_only_alt_identity_table replica identity using index \
         insert_only_alt_identity_table_email_idx",
    )
    .await
    .unwrap();
    pool.execute(
        "create publication insert_only_alt_identity_pub
         for table insert_only_alt_identity_table
         with (publish = 'insert')",
    )
    .await
    .unwrap();

    let pipeline_config = create_pipeline_config("insert_only_alt_identity_pub");
    let failures = validate_destination(&ctx, &create_bigquery_config(), Some(&pipeline_config))
        .await
        .unwrap();

    let replica_identity_failure = failures
        .iter()
        .find(|failure| failure.name == "Unsupported Replica Identity")
        .expect("Should warn for insert-only alternative replica identity");
    assert_eq!(replica_identity_failure.failure_type, FailureType::Warning);
    assert!(
        replica_identity_failure
            .reason
            .contains("insert_only_alt_identity_table (alternative_key)"),
        "Failure reason should mention the table and identity type"
    );
    assert!(
        replica_identity_failure.reason.contains("only replicates INSERT"),
        "Failure reason should explain this does not block pipeline start"
    );
    assert!(
        replica_identity_failure.reason.contains("UPDATE or DELETE"),
        "Failure reason should explain the mutation risk"
    );
    assert!(
        replica_identity_failure.reason.contains("large TOAST-backed columns"),
        "Failure reason should recommend full replica identity for TOAST-backed columns"
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
