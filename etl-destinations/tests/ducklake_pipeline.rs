#![cfg(feature = "ducklake")]

//! End-to-end integration tests for DuckLake using a real ETL [`Pipeline`].
//!
//! These tests use a local DuckDB-backed DuckLake catalog and verify the final
//! table contents by querying DuckLake directly through DuckDB.

use duckdb::Connection;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::spawn_source_database;
use etl::test_utils::notifying_store::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{EventType, PipelineId};
use etl_destinations::ducklake::{DuckLakeDestination, table_name_to_ducklake_table_name};
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::{quote_identifier, quote_literal};
use rand::random;
use std::path::{Path, PathBuf};
use url::Url;

use crate::support::ducklake::{ducklake_load_sql, open_verification_connection};

mod support;

/// Creates a persistent temp directory named after the test and prints its path.
/// Returns the directory path kept on disk after the test completes.
fn make_test_dir(test_name: &str) -> PathBuf {
    let dir = tempfile::Builder::new()
        .prefix(&format!("etl_ducklake_{test_name}_"))
        .tempdir()
        .expect("failed to create temp dir")
        .keep();

    println!(
        "[{test_name}] catalog : {}",
        dir.join("catalog.ducklake").display()
    );
    println!("[{test_name}] data    : {}", dir.join("data").display());

    dir
}

fn path_to_file_url(path: &Path) -> Url {
    Url::from_file_path(path).expect("failed to convert path to file url")
}

fn make_lake_urls(test_name: &str) -> (Url, Url) {
    let dir = make_test_dir(test_name);
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).expect("failed to create DuckLake data dir");

    (path_to_file_url(&catalog), path_to_file_url(&data))
}

fn open_lake_conn(catalog: &Url, data: &Url) -> Connection {
    let conn = open_verification_connection();
    conn.execute_batch(&format!(
        "{} \
         ATTACH {} AS {} (DATA_PATH {});",
        ducklake_load_sql(),
        quote_literal(&format!("ducklake:{}", catalog.as_str())),
        quote_identifier("lake"),
        quote_literal(data.as_str())
    ))
    .expect("failed to attach DuckLake catalog");

    conn
}

/// Forces DuckLake to checkpoint catalog metadata before cross-connection verification.
///
/// These end-to-end tests shut the destination down and then attach a fresh
/// DuckDB connection to verify the final lake state. Without an explicit
/// checkpoint here, that new connection can observe stale catalog metadata for
/// a short period even though the pipeline shutdown has already completed,
/// which makes the assertions flaky in CI. Running `CHECKPOINT` makes the final
/// durable state visible to the verification connection deterministically.
fn checkpoint_lake(catalog: &Url, data: &Url) {
    let conn = open_lake_conn(catalog, data);
    conn.execute_batch("CHECKPOINT")
        .expect("failed to checkpoint DuckLake catalog");
}

fn query_user_rows(conn: &Connection, table_name: &str) -> Vec<(i64, String, i32)> {
    let sql = format!(
        "SELECT id, name, age FROM {}.{} ORDER BY id",
        quote_identifier("lake"),
        quote_identifier(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare users query");
    let mut rows = statement.query([]).expect("failed to run users query");
    let mut result = Vec::new();

    while let Some(row) = rows.next().expect("failed to read users query row") {
        result.push((
            row.get(0).expect("failed to read users id"),
            row.get(1).expect("failed to read users name"),
            row.get(2).expect("failed to read users age"),
        ));
    }

    result
}

fn query_order_rows(conn: &Connection, table_name: &str) -> Vec<(i64, String)> {
    let sql = format!(
        "SELECT id, description FROM {}.{} ORDER BY id",
        quote_identifier("lake"),
        quote_identifier(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare orders query");
    let mut rows = statement.query([]).expect("failed to run orders query");
    let mut result = Vec::new();

    while let Some(row) = rows.next().expect("failed to read orders query row") {
        result.push((
            row.get(0).expect("failed to read orders id"),
            row.get(1).expect("failed to read orders description"),
        ));
    }

    result
}

async fn build_destination(
    catalog_url: &Url,
    data_url: &Url,
    store: NotifyingStore,
) -> TestDestinationWrapper<DuckLakeDestination<NotifyingStore>> {
    let raw_destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .expect("failed to create DuckLake destination");

    TestDestinationWrapper::wrap(raw_destination)
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let (catalog_url, data_url) = make_lake_urls("table_copy_and_streaming_with_restart");

    let users_table_name = table_name_to_ducklake_table_name(&database_schema.users_schema().name)
        .expect("failed to build DuckLake users table name");
    let orders_table_name =
        table_name_to_ducklake_table_name(&database_schema.orders_schema().name)
            .expect("failed to build DuckLake orders table name");

    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();

    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_ready = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;
    orders_ready.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_user_rows(&conn, &users_table_name),
        vec![(1, "user_1".to_string(), 1), (2, "user_2".to_string(), 2),]
    );
    assert_eq!(
        query_order_rows(&conn, &orders_table_name),
        vec![
            (1, "description_1".to_string()),
            (2, "description_2".to_string()),
        ]
    );
    drop(conn);

    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_user_rows(&conn, &users_table_name),
        vec![
            (1, "user_1".to_string(), 1),
            (2, "user_2".to_string(), 2),
            (3, "user_3".to_string(), 3),
            (4, "user_4".to_string(), 4),
        ]
    );
    assert_eq!(
        query_order_rows(&conn, &orders_table_name),
        vec![
            (1, "description_1".to_string()),
            (2, "description_2".to_string()),
            (3, "description_3".to_string()),
            (4, "description_4".to_string()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_insert_update_delete() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let (catalog_url, data_url) = make_lake_urls("table_insert_update_delete");

    let users_table_name = table_name_to_ducklake_table_name(&database_schema.users_schema().name)
        .expect("failed to build DuckLake users table name");

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let users_ready = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    users_ready.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_user_rows(&conn, &users_table_name),
        vec![(1, "user_1".to_string(), 1)]
    );
    drop(conn);

    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_10", &10i32],
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_user_rows(&conn, &users_table_name),
        vec![(1, "user_10".to_string(), 10)]
    );
    drop(conn);

    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["id"],
            &["1"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert!(query_user_rows(&conn, &users_table_name).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_streaming_with_truncate() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let (catalog_url, data_url) = make_lake_urls("cdc_streaming_with_truncate");

    let users_table_name = table_name_to_ducklake_table_name(&database_schema.users_schema().name)
        .expect("failed to build DuckLake users table name");
    let orders_table_name =
        table_name_to_ducklake_table_name(&database_schema.orders_schema().name)
            .expect("failed to build DuckLake orders table name");

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_ready = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;
    orders_ready.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    event_notify.notified().await;
    destination.clear_events().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Truncate, 2)])
        .await;

    database
        .truncate_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    database
        .truncate_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();

    event_notify.notified().await;
    destination.clear_events().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_user_rows(&conn, &users_table_name),
        vec![(3, "user_3".to_string(), 3), (4, "user_4".to_string(), 4),]
    );
    assert_eq!(
        query_order_rows(&conn, &orders_table_name),
        vec![
            (3, "description_3".to_string()),
            (4, "description_4".to_string()),
        ]
    );
}
