//! End-to-end integration tests for DuckLake using a real ETL [`Pipeline`].
//!
//! These tests use a PostgreSQL-backed DuckLake catalog and verify the final
//! table contents by querying DuckLake directly through DuckDB.

use duckdb::Connection;
use etl::{
    state::table::TableReplicationPhaseType,
    store::state::StateStore,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_mock_data, setup_test_database_schema},
    },
    types::{EventType, PipelineId},
};
use etl_destinations::ducklake::{DuckLakeDestination, table_name_to_ducklake_table_name};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::{quote_identifier, quote_literal};
use rand::random;
use url::Url;

use crate::support::ducklake::{
    catalog_attach_target, create_test_lake, ducklake_load_sql, open_verification_connection,
};

/// Expected row shape after adding columns during replication.
#[derive(Debug, Eq, PartialEq)]
struct SchemaAddRow {
    id: i64,
    name: String,
    age: i32,
    email: Option<String>,
    score: Option<i32>,
}

/// Expected row shape after adding, dropping, and renaming columns.
#[derive(Debug, Eq, PartialEq)]
struct SchemaMultiRow {
    id: i64,
    full_name: String,
    status: Option<String>,
    email: Option<String>,
}

/// Expected row shape after schema evolution followed by mutations.
#[derive(Debug, Eq, PartialEq)]
struct SchemaMutationRow {
    id: i64,
    full_name: String,
    visits: i32,
    email: Option<String>,
}

/// Opens a DuckLake verification connection using blocking DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn open_lake_conn(catalog: &Url, data: &Url) -> Connection {
    let conn = open_verification_connection();
    let catalog_attach_target = catalog_attach_target(catalog);
    conn.execute_batch(&format!(
        "{} attach {} as {} (data_path {});",
        ducklake_load_sql(),
        quote_literal(&format!("ducklake:{catalog_attach_target}")),
        quote_identifier("lake"),
        quote_literal(data.as_str())
    ))
    .expect("failed to attach DuckLake catalog");

    conn
}

/// Forces DuckLake to checkpoint catalog metadata before cross-connection
/// verification.
///
/// These end-to-end tests shut the destination down and then attach a fresh
/// DuckDB connection to verify the final lake state. Without an explicit
/// checkpoint here, that new connection can observe stale catalog metadata for
/// a short period even though the pipeline shutdown has already completed,
/// which makes the assertions flaky in CI. Running `checkpoint` makes the final
/// durable state visible to the verification connection deterministically.
///
/// Production async code must wrap equivalent blocking DuckDB work in
/// `run_duckdb_blocking`.
fn checkpoint_lake(catalog: &Url, data: &Url) {
    let conn = open_lake_conn(catalog, data);
    conn.execute_batch("checkpoint").expect("failed to checkpoint DuckLake catalog");
}

/// Queries replicated user rows using blocking DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_user_rows(conn: &Connection, table_name: &str) -> Vec<(i64, String, i32)> {
    let sql = format!(
        "select id, name, age from {}.{} order by id",
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

/// Queries replicated order rows using blocking DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_order_rows(conn: &Connection, table_name: &str) -> Vec<(i64, String)> {
    let sql = format!(
        "select id, description from {}.{} order by id",
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

/// Queries DuckLake table columns using blocking DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_table_columns(conn: &Connection, table_name: &str) -> Vec<String> {
    let sql = format!(
        "select column_name from information_schema.columns where table_catalog = {} and \
         table_schema = {} and table_name = {} order by ordinal_position",
        quote_literal("lake"),
        quote_literal("main"),
        quote_literal(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare columns query");
    let mut rows = statement.query([]).expect("failed to run columns query");
    let mut result = Vec::new();

    while let Some(row) = rows.next().expect("failed to read columns query row") {
        result.push(row.get(0).expect("failed to read column name"));
    }

    result
}

/// Queries rows after an add-column schema change using blocking DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_schema_add_rows(conn: &Connection, table_name: &str) -> Vec<SchemaAddRow> {
    let sql = format!(
        "select id, name, age, email, score from {}.{} order by id",
        quote_identifier("lake"),
        quote_identifier(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare schema add query");
    let mut rows = statement.query([]).expect("failed to run schema add query");
    let mut result = Vec::new();

    while let Some(row) = rows.next().expect("failed to read schema add row") {
        result.push(SchemaAddRow {
            id: row.get(0).expect("failed to read id"),
            name: row.get(1).expect("failed to read name"),
            age: row.get(2).expect("failed to read age"),
            email: row.get(3).expect("failed to read email"),
            score: row.get(4).expect("failed to read score"),
        });
    }

    result
}

/// Queries rows after add/drop/rename schema changes using blocking DuckDB
/// APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_schema_multi_rows(conn: &Connection, table_name: &str) -> Vec<SchemaMultiRow> {
    let sql = format!(
        "select id, full_name, status, email from {}.{} order by id",
        quote_identifier("lake"),
        quote_identifier(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare schema multi query");
    let mut rows = statement.query([]).expect("failed to run schema multi query");
    let mut result = Vec::new();

    while let Some(row) = rows.next().expect("failed to read schema multi row") {
        result.push(SchemaMultiRow {
            id: row.get(0).expect("failed to read id"),
            full_name: row.get(1).expect("failed to read full_name"),
            status: row.get(2).expect("failed to read status"),
            email: row.get(3).expect("failed to read email"),
        });
    }

    result
}

/// Queries rows after schema evolution and mutations using blocking DuckDB
/// APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_schema_mutation_rows(conn: &Connection, table_name: &str) -> Vec<SchemaMutationRow> {
    let sql = format!(
        "select id, full_name, visits, email from {}.{} order by id",
        quote_identifier("lake"),
        quote_identifier(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare schema mutation query");
    let mut rows = statement.query([]).expect("failed to run schema mutation query");
    let mut result = Vec::new();

    while let Some(row) = rows.next().expect("failed to read schema mutation row") {
        result.push(SchemaMutationRow {
            id: row.get(0).expect("failed to read id"),
            full_name: row.get(1).expect("failed to read full_name"),
            visits: row.get(2).expect("failed to read visits"),
            email: row.get(3).expect("failed to read email"),
        });
    }

    result
}

async fn build_destination(
    catalog_url: &Url,
    data_url: &Url,
    store: NotifyingStore,
) -> TestDestinationWrapper<DuckLakeDestination<NotifyingStore>> {
    let raw_destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        None,
        None,
        store,
    )
    .await
    .expect("failed to create DuckLake destination");

    TestDestinationWrapper::wrap(raw_destination)
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let lake = create_test_lake("table_copy_and_streaming_with_restart").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();

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
        vec![(1, "user_1".to_owned(), 1), (2, "user_2".to_owned(), 2),]
    );
    assert_eq!(
        query_order_rows(&conn, &orders_table_name),
        vec![(1, "description_1".to_owned()), (2, "description_2".to_owned()),]
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

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

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
            (1, "user_1".to_owned(), 1),
            (2, "user_2".to_owned(), 2),
            (3, "user_3".to_owned(), 3),
            (4, "user_4".to_owned(), 4),
        ]
    );
    assert_eq!(
        query_order_rows(&conn, &orders_table_name),
        vec![
            (1, "description_1".to_owned()),
            (2, "description_2".to_owned()),
            (3, "description_3".to_owned()),
            (4, "description_4".to_owned()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_reset_drops_destination_table_before_recopy() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let lake = create_test_lake("table_copy_reset_drops_destination_table_before_recopy").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();

    let users_schema = database_schema.users_schema();
    let users_table_name = table_name_to_ducklake_table_name(&users_schema.name)
        .expect("failed to build DuckLake users table name");
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();

    database
        .insert_values(users_schema.name.clone(), &["name", "age"], &[&"before_1", &1])
        .await
        .unwrap();
    database
        .insert_values(users_schema.name.clone(), &["name", "age"], &[&"before_2", &2])
        .await
        .unwrap();

    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready =
        store.notify_on_table_state_type(users_schema.id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_user_rows(&conn, &users_table_name),
        vec![(1, "before_1".to_owned(), 1), (2, "before_2".to_owned(), 2)]
    );
    drop(conn);
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    database
        .run_sql(&format!("delete from {} where true", users_schema.name.as_quoted_identifier()))
        .await
        .unwrap();
    database
        .insert_values(users_schema.name.clone(), &["name", "age"], &[&"after", &3])
        .await
        .unwrap();
    store.reset_table_state(users_schema.id).await.unwrap();

    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready =
        store.notify_on_table_state_type(users_schema.id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert!(destination.was_table_dropped_for_copy(users_schema.id).await);
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(query_user_rows(&conn, &users_table_name), vec![(3, "after".to_owned(), 3)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_without_restart() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let lake = create_test_lake("table_copy_and_streaming_without_restart").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();

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

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

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
            (1, "user_1".to_owned(), 1),
            (2, "user_2".to_owned(), 2),
            (3, "user_3".to_owned(), 3),
            (4, "user_4".to_owned(), 4),
        ]
    );
    assert_eq!(
        query_order_rows(&conn, &orders_table_name),
        vec![
            (1, "description_1".to_owned()),
            (2, "description_2".to_owned()),
            (3, "description_3".to_owned()),
            (4, "description_4".to_owned()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_insert_update_delete() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let lake = create_test_lake("table_insert_update_delete").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();

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

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

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
    assert_eq!(query_user_rows(&conn, &users_table_name), vec![(1, "user_1".to_owned(), 1)]);
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

    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

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
    assert_eq!(query_user_rows(&conn, &users_table_name), vec![(1, "user_10".to_owned(), 10)]);
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

    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;

    database
        .delete_values(database_schema.users_schema().name.clone(), &["id"], &["1"], "")
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
    let lake = create_test_lake("cdc_streaming_with_truncate").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();

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

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

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

    let event_notify = destination.wait_for_events_count(vec![(EventType::Truncate, 2)]).await;

    database.truncate_table(database_schema.users_schema().name.clone()).await.unwrap();
    database.truncate_table(database_schema.orders_schema().name.clone()).await.unwrap();

    event_notify.notified().await;
    destination.clear_events().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

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
        vec![(3, "user_3".to_owned(), 3), (4, "user_4".to_owned(), 4),]
    );
    assert_eq!(
        query_order_rows(&conn, &orders_table_name),
        vec![(3, "description_3".to_owned()), (4, "description_4".to_owned()),]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_add_column() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("ducklake_schema_add_col");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await
        .expect("failed to create source table");
    let publication_name = "test_pub_ducklake_schema_add";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("failed to create publication");
    database
        .run_sql(&format!(
            "insert into {} (name, age) values ('Alice', 25)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Alice");

    let lake = create_test_lake("schema_change_add_column").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();
    let ducklake_table_name = table_name_to_ducklake_table_name(&table_name)
        .expect("failed to build DuckLake table name");
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );
    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let initial_metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after table creation");
    let initial_snapshot_id = initial_metadata.snapshot_id;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[
                TableModification::AddColumn { name: "email", data_type: "text" },
                TableModification::AddColumn {
                    name: "score",
                    data_type: "integer not null default 0",
                },
            ],
        )
        .await
        .expect("failed to alter source table");
    database
        .run_sql(&format!(
            "insert into {} (name, age, email, score) values ('Bob', 30, 'bob@example.com', 7)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Bob");

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_table_columns(&conn, &ducklake_table_name),
        vec!["id", "name", "age", "email", "score"]
    );
    assert_eq!(
        query_schema_add_rows(&conn, &ducklake_table_name),
        vec![
            SchemaAddRow { id: 1, name: "Alice".to_owned(), age: 25, email: None, score: None },
            SchemaAddRow {
                id: 2,
                name: "Bob".to_owned(),
                age: 30,
                email: Some("bob@example.com".to_owned()),
                score: Some(7),
            },
        ]
    );

    let final_metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after schema change");
    assert!(final_metadata.snapshot_id > initial_snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_is_visible_to_already_open_connection() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("ducklake_schema_open_conn");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await
        .expect("failed to create source table");
    let publication_name = "test_pub_ducklake_schema_open_conn";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("failed to create publication");
    database
        .run_sql(&format!(
            "insert into {} (name, age) values ('Alice', 25)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Alice");

    let lake = create_test_lake("schema_change_is_visible_to_already_open_connection").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();
    let ducklake_table_name = table_name_to_ducklake_table_name(&table_name)
        .expect("failed to build DuckLake table name");
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );
    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    checkpoint_lake(&catalog_url, &data_url);
    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(query_table_columns(&conn, &ducklake_table_name), vec!["id", "name", "age"]);

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[
                TableModification::AddColumn { name: "email", data_type: "text" },
                TableModification::AddColumn {
                    name: "score",
                    data_type: "integer not null default 0",
                },
            ],
        )
        .await
        .expect("failed to alter source table");
    database
        .run_sql(&format!(
            "insert into {} (name, age, email, score) values ('Bob', 30, 'bob@example.com', 7)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Bob");

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    assert_eq!(
        query_table_columns(&conn, &ducklake_table_name),
        vec!["id", "name", "age", "email", "score"]
    );
    assert_eq!(
        query_schema_add_rows(&conn, &ducklake_table_name),
        vec![
            SchemaAddRow { id: 1, name: "Alice".to_owned(), age: 25, email: None, score: None },
            SchemaAddRow {
                id: 2,
                name: "Bob".to_owned(),
                age: 30,
                email: Some("bob@example.com".to_owned()),
                score: Some(7),
            },
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_add_drop_rename() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("ducklake_schema_multi");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null"), ("status", "text")],
        )
        .await
        .expect("failed to create source table");
    let publication_name = "test_pub_ducklake_schema_multi";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("failed to create publication");
    database
        .run_sql(&format!(
            "insert into {} (name, age, status) values ('Alice', 25, 'active')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Alice");

    let lake = create_test_lake("schema_change_add_drop_rename").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();
    let ducklake_table_name = table_name_to_ducklake_table_name(&table_name)
        .expect("failed to build DuckLake table name");
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );
    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let initial_snapshot_id = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after table creation")
        .snapshot_id;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "name", new_name: "full_name" }],
        )
        .await
        .expect("failed to rename source column");
    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .expect("failed to drop source column");
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text" }],
        )
        .await
        .expect("failed to add source column");
    database
        .run_sql(&format!(
            "insert into {} (full_name, status, email) values ('Bob', 'pending', \
             'bob@example.com')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Bob");

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_table_columns(&conn, &ducklake_table_name),
        vec!["id", "full_name", "status", "email"]
    );
    assert_eq!(
        query_schema_multi_rows(&conn, &ducklake_table_name),
        vec![
            SchemaMultiRow {
                id: 1,
                full_name: "Alice".to_owned(),
                status: Some("active".to_owned()),
                email: None,
            },
            SchemaMultiRow {
                id: 2,
                full_name: "Bob".to_owned(),
                status: Some("pending".to_owned()),
                email: Some("bob@example.com".to_owned()),
            },
        ]
    );

    let final_snapshot_id = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after schema change")
        .snapshot_id;
    assert!(final_snapshot_id > initial_snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_then_update_and_delete() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("ducklake_schema_mutation");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("visits", "integer not null")],
        )
        .await
        .expect("failed to create source table");
    let publication_name = "test_pub_ducklake_schema_mutation";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("failed to create publication");
    database
        .run_sql(&format!(
            "insert into {} (name, visits) values ('Alice', 1)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Alice");

    let lake = create_test_lake("schema_change_then_update_and_delete").await;
    let catalog_url = lake.catalog_url.clone();
    let data_url = lake.data_url.clone();
    let ducklake_table_name = table_name_to_ducklake_table_name(&table_name)
        .expect("failed to build DuckLake table name");
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = build_destination(&catalog_url, &data_url, store.clone()).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );
    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![
            (EventType::Relation, 1),
            (EventType::Insert, 1),
            (EventType::Update, 1),
            (EventType::Delete, 1),
        ])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "name", new_name: "full_name" }],
        )
        .await
        .expect("failed to rename source column");
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text" }],
        )
        .await
        .expect("failed to add source column");
    database
        .run_sql(&format!(
            "insert into {} (full_name, visits, email) values ('Bob', 1, 'bob@example.com')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert Bob");
    database
        .run_sql(&format!(
            "update {} set full_name = 'Alice Smith', visits = 2, email = 'alice@example.com' \
             where id = 1",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to update Alice");
    database
        .run_sql(&format!(
            "delete from {} where full_name = 'Bob'",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to delete Bob");

    event_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_table_columns(&conn, &ducklake_table_name),
        vec!["id", "full_name", "visits", "email"]
    );
    assert_eq!(
        query_schema_mutation_rows(&conn, &ducklake_table_name),
        vec![SchemaMutationRow {
            id: 1,
            full_name: "Alice Smith".to_owned(),
            visits: 2,
            email: Some("alice@example.com".to_owned()),
        }]
    );
}
