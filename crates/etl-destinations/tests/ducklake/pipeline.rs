//! End-to-end integration tests for DuckLake using a real ETL [`Pipeline`].
//!
//! These tests use a PostgreSQL-backed DuckLake catalog and verify the final
//! table contents by querying DuckLake directly through DuckDB.

use duckdb::Connection;
use etl::{
    event::EventType,
    pipeline::PipelineId,
    store::{StateStore, TableStateType},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_mock_data, setup_test_database_schema},
    },
};
use etl_destinations::ducklake::{
    DuckLakeDestination, DuckLakeTableName, table_name_to_ducklake_table_name,
};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::{quote_identifier, quote_literal};
use rand::random;
use rust_decimal::Decimal;
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

/// Expected row shape after simulator-style generated-column rotation.
#[derive(Debug, Eq, PartialEq)]
struct SimulatorDdlRotationRow {
    id: i64,
    text_col: String,
    ddl_col_0_0: Option<String>,
}

/// Expected row shape after simulator-style generated-column type changes.
#[derive(Debug, Eq, PartialEq)]
struct SimulatorDdlTypeRow {
    id: i64,
    text_col: String,
    ddl_col_0_0: Option<String>,
    ddl_col_1_0: Option<i32>,
    ddl_col_2_0: Option<bool>,
    ddl_col_3_0_is_present: bool,
    ddl_col_4_0: Option<Decimal>,
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

fn qualified_lake_table_name(table_name: &DuckLakeTableName) -> String {
    format!(
        "{}.{}.{}",
        quote_identifier("lake"),
        quote_identifier(table_name.schema()),
        quote_identifier(table_name.table())
    )
}

/// Counts one applied-marker kind for a destination table.
fn count_applied_batches(
    conn: &Connection,
    table_name: &DuckLakeTableName,
    batch_kind: &str,
) -> i64 {
    let sql = format!(
        "select count(*) from {}.{} where table_name = {} and batch_kind = {}",
        quote_identifier("lake"),
        quote_identifier("__etl_applied_table_batches"),
        quote_literal(&table_name.id()),
        quote_literal(batch_kind),
    );
    conn.query_row(&sql, [], |row| row.get(0)).expect("failed to count applied table batches")
}

/// Queries replicated user rows using blocking DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_user_rows(conn: &Connection, table_name: &DuckLakeTableName) -> Vec<(i64, String, i32)> {
    let sql =
        format!("select id, name, age from {} order by id", qualified_lake_table_name(table_name));
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
fn query_order_rows(conn: &Connection, table_name: &DuckLakeTableName) -> Vec<(i64, String)> {
    let sql = format!(
        "select id, description from {} order by id",
        qualified_lake_table_name(table_name)
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
fn query_table_columns(conn: &Connection, table_name: &DuckLakeTableName) -> Vec<String> {
    let sql = format!(
        "select column_name from information_schema.columns where table_catalog = {} and \
         table_schema = {} and table_name = {} order by ordinal_position",
        quote_literal("lake"),
        quote_literal(table_name.schema()),
        quote_literal(table_name.table())
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
fn query_schema_add_rows(conn: &Connection, table_name: &DuckLakeTableName) -> Vec<SchemaAddRow> {
    let sql = format!(
        "select id, name, age, email, score from {} order by id",
        qualified_lake_table_name(table_name)
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
fn query_schema_multi_rows(
    conn: &Connection,
    table_name: &DuckLakeTableName,
) -> Vec<SchemaMultiRow> {
    let sql = format!(
        "select id, full_name, status, email from {} order by id",
        qualified_lake_table_name(table_name)
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
fn query_schema_mutation_rows(
    conn: &Connection,
    table_name: &DuckLakeTableName,
) -> Vec<SchemaMutationRow> {
    let sql = format!(
        "select id, full_name, visits, email from {} order by id",
        qualified_lake_table_name(table_name)
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

/// Queries rows after simulator-style generated-column rotation using blocking
/// DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_simulator_ddl_rotation_rows(
    conn: &Connection,
    table_name: &DuckLakeTableName,
) -> Vec<SimulatorDdlRotationRow> {
    let sql = format!(
        "select id, text_col, ddl_col_0_0 from {} order by id",
        qualified_lake_table_name(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare simulator DDL rotation query");
    let mut rows = statement.query([]).expect("failed to run simulator DDL rotation query");
    let mut result = Vec::new();

    while let Some(row) = rows.next().expect("failed to read simulator DDL rotation row") {
        result.push(SimulatorDdlRotationRow {
            id: row.get(0).expect("failed to read id"),
            text_col: row.get(1).expect("failed to read text_col"),
            ddl_col_0_0: row.get(2).expect("failed to read ddl_col_0_0"),
        });
    }

    result
}

/// Queries the final row after simulator-style generated-column type changes
/// using blocking DuckDB APIs.
///
/// Production async code must wrap equivalent DuckDB work in
/// `run_duckdb_blocking`.
fn query_simulator_ddl_type_row(
    conn: &Connection,
    table_name: &DuckLakeTableName,
) -> SimulatorDdlTypeRow {
    let sql = format!(
        "select id, text_col, ddl_col_0_0, ddl_col_1_0, ddl_col_2_0, ddl_col_3_0 is not null, \
         ddl_col_4_0 from {} where id = 5",
        qualified_lake_table_name(table_name)
    );
    let mut statement = conn.prepare(&sql).expect("failed to prepare simulator DDL type query");
    let row = statement.query_row([], |row| {
        Ok(SimulatorDdlTypeRow {
            id: row.get(0)?,
            text_col: row.get(1)?,
            ddl_col_0_0: row.get(2)?,
            ddl_col_1_0: row.get(3)?,
            ddl_col_2_0: row.get(4)?,
            ddl_col_3_0_is_present: row.get(5)?,
            ddl_col_4_0: row.get(6)?,
        })
    });

    row.expect("failed to read simulator DDL type row")
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
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_ready = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
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
    assert_eq!(count_applied_batches(&conn, &users_table_name, "copy_complete"), 1);
    assert_eq!(count_applied_batches(&conn, &orders_table_name, "copy_complete"), 1);
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
        store.notify_on_table_state_type(users_schema.id, TableStateType::Ready).await;

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
        store.notify_on_table_state_type(users_schema.id, TableStateType::Ready).await;

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
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_ready = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
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
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
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
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_ready = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
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
    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

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

    let final_metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after schema change");
    assert!(final_metadata.snapshot_id > initial_snapshot_id);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_schema_add_rows(&conn, &ducklake_table_name),
        vec![
            SchemaAddRow { id: 1, name: "Alice".to_owned(), age: 25, email: None, score: Some(0) },
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
    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let initial_snapshot_id = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after table creation")
        .snapshot_id;

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

    let final_snapshot_id = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after schema change")
        .snapshot_id;
    assert!(final_snapshot_id > initial_snapshot_id);

    assert_eq!(
        query_table_columns(&conn, &ducklake_table_name),
        vec!["id", "name", "age", "email", "score"]
    );
    assert_eq!(
        query_schema_add_rows(&conn, &ducklake_table_name),
        vec![
            SchemaAddRow { id: 1, name: "Alice".to_owned(), age: 25, email: None, score: Some(0) },
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
    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

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
    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

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

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_matches_simulator_generated_column_rotation() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("ducklake_schema_simulator_rotation");
    let table_id = database
        .create_table(table_name.clone(), true, &[("text_col", "text not null")])
        .await
        .expect("failed to create source table");
    let publication_name = "test_pub_ducklake_schema_simulator_rotation";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("failed to create publication");

    let lake = create_test_lake("schema_change_matches_simulator_generated_column_rotation").await;
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
    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "ddl_col_0_0", data_type: "text" }],
        )
        .await
        .expect("failed to add first generated source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_0) values ('after_add', 'slot0_v0')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after generated column add");
    event_notify.notified().await;

    destination.clear_events().await;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "ddl_col_0_0", new_name: "ddl_col_0_1" }],
        )
        .await
        .expect("failed to rename generated source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_1) values ('after_rename', 'slot0_v1')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after generated column rename");
    event_notify.notified().await;

    destination.clear_events().await;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "ddl_col_0_1" }])
        .await
        .expect("failed to drop generated source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col) values ('after_drop')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after generated column drop");
    event_notify.notified().await;

    destination.clear_events().await;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "ddl_col_0_0", data_type: "text" }],
        )
        .await
        .expect("failed to re-add generated source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_0) values ('after_readd', 'slot0_readded')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after generated column re-add");
    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_table_columns(&conn, &ducklake_table_name),
        vec!["id", "text_col", "ddl_col_0_0"]
    );
    assert_eq!(
        query_simulator_ddl_rotation_rows(&conn, &ducklake_table_name),
        vec![
            SimulatorDdlRotationRow { id: 1, text_col: "after_add".to_owned(), ddl_col_0_0: None },
            SimulatorDdlRotationRow {
                id: 2,
                text_col: "after_rename".to_owned(),
                ddl_col_0_0: None,
            },
            SimulatorDdlRotationRow { id: 3, text_col: "after_drop".to_owned(), ddl_col_0_0: None },
            SimulatorDdlRotationRow {
                id: 4,
                text_col: "after_readd".to_owned(),
                ddl_col_0_0: Some("slot0_readded".to_owned()),
            },
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_matches_simulator_generated_column_types() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("ducklake_schema_simulator_types");
    let table_id = database
        .create_table(table_name.clone(), true, &[("text_col", "text not null")])
        .await
        .expect("failed to create source table");
    let publication_name = "test_pub_ducklake_schema_simulator_types";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("failed to create publication");

    let lake = create_test_lake("schema_change_matches_simulator_generated_column_types").await;
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
    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "ddl_col_0_0", data_type: "text" }],
        )
        .await
        .expect("failed to add generated text source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_0) values ('after_text', 'text_0')",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after text generated column add");
    event_notify.notified().await;

    destination.clear_events().await;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "ddl_col_1_0", data_type: "integer" }],
        )
        .await
        .expect("failed to add generated integer source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_0, ddl_col_1_0) values ('after_integer', \
             'text_1', 11)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after integer generated column add");
    event_notify.notified().await;

    destination.clear_events().await;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "ddl_col_2_0", data_type: "boolean" }],
        )
        .await
        .expect("failed to add generated boolean source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_0, ddl_col_1_0, ddl_col_2_0) values \
             ('after_boolean', 'text_2', 22, true)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after boolean generated column add");
    event_notify.notified().await;

    destination.clear_events().await;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "ddl_col_3_0", data_type: "timestamptz" }],
        )
        .await
        .expect("failed to add generated timestamptz source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_0, ddl_col_1_0, ddl_col_2_0, ddl_col_3_0) values \
             ('after_timestamptz', 'text_3', 33, false, '2026-01-02 03:04:05+00'::timestamptz)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after timestamptz generated column add");
    event_notify.notified().await;

    destination.clear_events().await;
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "ddl_col_4_0", data_type: "numeric(10, 2)" }],
        )
        .await
        .expect("failed to add generated numeric source column");
    database
        .run_sql(&format!(
            "insert into {} (text_col, ddl_col_0_0, ddl_col_1_0, ddl_col_2_0, ddl_col_3_0, \
             ddl_col_4_0) values ('after_numeric', 'text_4', 44, true, '2026-01-02 \
             03:04:05+00'::timestamptz, 12345.67)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("failed to insert row after numeric generated column add");
    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(
        query_table_columns(&conn, &ducklake_table_name),
        vec![
            "id",
            "text_col",
            "ddl_col_0_0",
            "ddl_col_1_0",
            "ddl_col_2_0",
            "ddl_col_3_0",
            "ddl_col_4_0",
        ]
    );
    assert_eq!(
        query_simulator_ddl_type_row(&conn, &ducklake_table_name),
        SimulatorDdlTypeRow {
            id: 5,
            text_col: "after_numeric".to_owned(),
            ddl_col_0_0: Some("text_4".to_owned()),
            ddl_col_1_0: Some(44),
            ddl_col_2_0: Some(true),
            ddl_col_3_0_is_present: true,
            ddl_col_4_0: Some(Decimal::new(1234567, 2)),
        }
    );
}
