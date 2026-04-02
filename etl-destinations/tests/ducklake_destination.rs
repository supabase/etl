#![cfg(feature = "ducklake")]

//! Integration tests for the DuckLake destination.
//!
//! These tests use a local DuckDB-backed DuckLake catalog (a `.ducklake`
//! metadata file + a local Parquet data directory). No PostgreSQL or cloud
//! storage account is required.
//!
//! Run with `-- --nocapture` to see the paths printed for each test:
//!
//! ```
//! cargo test -p etl-destinations --features ducklake --test ducklake_destination -- --nocapture
//! ```
//!
//! After the tests finish you can query the lake directly with the DuckDB CLI:
//!
//! ```
//! duckdb :memory: -c "
//!   LOAD '/absolute/path/to/ducklake.duckdb_extension';
//!   ATTACH 'ducklake:file:///path/printed/catalog.ducklake' AS lake
//!     (DATA_PATH 'file:///path/printed/data');
//!   SELECT * FROM lake.public_users;
//! "
//! ```

use chrono::NaiveDate;
use duckdb::Connection;
use etl::destination::Destination;
use etl::error::ErrorKind;
use etl::store::both::memory::MemoryStore;
use etl::store::schema::SchemaStore;
use etl::types::{
    Cell, ColumnSchema, Event, TableId, TableName, TableRow, TableSchema, Type as PgType,
};
use etl_destinations::ducklake::{DuckLakeDestination, table_name_to_ducklake_table_name};
#[cfg(feature = "test-utils")]
use etl_destinations::ducklake::{
    arm_fail_after_atomic_batch_commit_once_for_tests,
    arm_fail_after_copy_batch_commit_once_for_tests, reset_ducklake_test_hooks,
};
use pg_escape::{quote_identifier, quote_literal};
use std::f64::consts::PI;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
use std::time::Duration;
#[cfg(feature = "test-utils")]
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::support::ducklake::{ducklake_load_sql, open_verification_connection};

mod support;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Creates a persistent temp directory named after the test and prints its path.
/// Returns the directory path (kept on disk after the test completes).
fn make_test_dir(test_name: &str) -> PathBuf {
    let dir = tempfile::Builder::new()
        .prefix(&format!("etl_ducklake_{test_name}_"))
        .tempdir()
        .expect("failed to create temp dir")
        .keep(); // `into_path` prevents auto-cleanup on drop

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

#[cfg(feature = "test-utils")]
static DUCKLAKE_TEST_HOOKS_GUARD: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[cfg(feature = "test-utils")]
async fn acquire_ducklake_test_hook_guard() -> OwnedSemaphorePermit {
    Arc::clone(&DUCKLAKE_TEST_HOOKS_GUARD)
        .acquire_owned()
        .await
        .expect("ducklake test hook semaphore should stay open")
}

fn make_schema(table_id: u32, schema: &str, table: &str) -> TableSchema {
    TableSchema::new(
        TableId::new(table_id),
        TableName::new(schema.to_string(), table.to_string()),
        vec![
            ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
        ],
    )
}

fn make_rich_schema(table_id: u32) -> TableSchema {
    TableSchema::new(
        TableId::new(table_id),
        TableName::new("public".to_string(), "rich".to_string()),
        vec![
            ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
            ColumnSchema::new("label".to_string(), PgType::VARCHAR, -1, true, false),
            ColumnSchema::new("score".to_string(), PgType::FLOAT8, -1, true, false),
            ColumnSchema::new("active".to_string(), PgType::BOOL, -1, true, false),
            ColumnSchema::new("birthday".to_string(), PgType::DATE, -1, true, false),
        ],
    )
}

/// Opens a verification connection to the same DuckLake catalog and returns it.
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

fn lake_table_exists(conn: &Connection, table_name: &str) -> bool {
    conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_catalog = {} AND table_schema = {} AND table_name = {}",
            quote_literal("lake"),
            quote_literal("main"),
            quote_literal(table_name)
        ),
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .unwrap_or(false)
}

async fn open_lake_conn_when_tables_visible(
    catalog: &Url,
    data: &Url,
    table_names: &[&str],
) -> Connection {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let conn = open_lake_conn(catalog, data);
        if table_names
            .iter()
            .all(|table_name| lake_table_exists(&conn, table_name))
        {
            return conn;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for DuckLake tables to become visible: {table_names:?}"
        );
        drop(conn);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn count_rows(conn: &Connection, table_name: &str) -> i64 {
    conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM {}.{}",
            quote_identifier("lake"),
            quote_identifier(table_name)
        ),
        [],
        |r| r.get(0),
    )
    .expect("count query failed")
}

fn count_applied_batches(conn: &Connection, table_name: &str, batch_kind: &str) -> i64 {
    conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM {}.{} WHERE table_name = {} AND batch_kind = {}",
            quote_identifier("lake"),
            quote_identifier("__etl_applied_table_batches"),
            quote_literal(table_name),
            quote_literal(batch_kind)
        ),
        [],
        |r| r.get(0),
    )
    .expect("batch marker count query failed")
}

fn count_table_files(data: &Path, table_name: &str) -> usize {
    let table_dir = data.join("main").join(table_name);
    match std::fs::read_dir(&table_dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_file())
            .count(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!("table file count query failed: {error}"),
    }
}

fn flush_inlined_rows(conn: &Connection, table_name: &str) -> i64 {
    conn.query_row(
        &format!(
            "SELECT COALESCE(SUM(rows_flushed), 0) \
             FROM ducklake_flush_inlined_data({}, table_name => {})",
            quote_literal("lake"),
            quote_literal(table_name),
        ),
        [],
        |r| r.get(0),
    )
    .expect("inlined data flush query failed")
}

async fn wait_for_condition<F>(timeout: Duration, mut condition: F)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition() {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Forces DuckLake to checkpoint catalog metadata before cross-connection verification.
///
/// Some tests shut the destination down and then open a brand-new DuckDB
/// connection to verify the resulting lake state. Without an explicit
/// checkpoint here, that fresh connection can temporarily observe stale catalog
/// metadata even after shutdown has finished, which makes the assertions flaky.
/// Running `CHECKPOINT` makes the final durable state visible deterministically.
fn checkpoint_lake(catalog: &Url, data: &Url) {
    let conn = open_lake_conn(catalog, data);
    conn.execute_batch("CHECKPOINT")
        .expect("failed to checkpoint DuckLake catalog");
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// `write_table_rows` inserts rows that can be queried back through the DuckLake
/// catalog using a separate DuckDB connection.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_basic() {
    let dir = make_test_dir("write_table_rows_basic");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(1);
    let schema = make_schema(1, "public", "users");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();
    destination
        .write_table_rows(
            table_id,
            vec![
                TableRow::new(vec![Cell::I32(1), Cell::String("Alice".to_string())]),
                TableRow::new(vec![Cell::I32(2), Cell::String("Bob".to_string())]),
                TableRow::new(vec![Cell::I32(3), Cell::Null]),
            ],
        )
        .await
        .expect("write_table_rows failed");

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 3);

    let (id, name): (i32, Option<String>) = conn
        .query_row(
            &format!(
                "SELECT id, name FROM {}.{} WHERE id = 1",
                quote_identifier("lake"),
                quote_identifier(&table_name)
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(name.as_deref(), Some("Alice"));
}

/// Small copy batches should remain inlined after the caller returns.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_small_batch_stays_inlined_after_return() {
    let dir = make_test_dir("write_table_rows_small_batch_stays_inlined_after_return");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(16);
    let schema = make_schema(16, "public", "copy_flush");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("first".to_string()),
            ])],
        )
        .await
        .unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(count_applied_batches(&conn, &table_name, "copy"), 1);
    assert_eq!(
        count_table_files(&data, &table_name),
        0,
        "small copy batch should remain inlined until background maintenance"
    );
}

/// `pool_size = 0` should fail fast with a configuration error.
#[tokio::test(flavor = "multi_thread")]
async fn test_ducklake_rejects_zero_pool_size() {
    let dir = make_test_dir("ducklake_rejects_zero_pool_size");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let err = DuckLakeDestination::new(
        path_to_file_url(&catalog),
        path_to_file_url(&data),
        0,
        None,
        None,
        MemoryStore::new(),
    )
    .await
    .err()
    .expect("pool_size = 0 should fail");

    assert_eq!(err.kind(), ErrorKind::ConfigError);
}

/// Repeated writes should reuse the warm pooled DuckDB connection.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_reuses_warm_pooled_connection() {
    let _test_hook_guard = acquire_ducklake_test_hook_guard().await;
    reset_ducklake_test_hooks();

    let dir = make_test_dir("write_table_rows_reuses_warm_pooled_connection");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(21);
    let schema = make_schema(21, "public", "pooled_copy");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    assert_eq!(destination.connection_open_count_for_tests(), 1);

    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("first".to_string()),
            ])],
        )
        .await
        .unwrap();
    assert_eq!(destination.connection_open_count_for_tests(), 1);

    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(2),
                Cell::String("second".to_string()),
            ])],
        )
        .await
        .unwrap();
    assert_eq!(destination.connection_open_count_for_tests(), 1);

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 2);
}

/// A failed write attempt should discard the pooled DuckDB connection and replace it.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_replaces_broken_pooled_connection_after_retry() {
    let _test_hook_guard = acquire_ducklake_test_hook_guard().await;
    reset_ducklake_test_hooks();

    let dir = make_test_dir("write_table_rows_replaces_broken_pooled_connection_after_retry");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(22);
    let schema = make_schema(22, "public", "pooled_retry");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    assert_eq!(destination.connection_open_count_for_tests(), 1);

    arm_fail_after_copy_batch_commit_once_for_tests(&table_name);
    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("first".to_string()),
            ])],
        )
        .await
        .unwrap();
    assert_eq!(destination.connection_open_count_for_tests(), 2);

    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(2),
                Cell::String("second".to_string()),
            ])],
        )
        .await
        .unwrap();
    assert_eq!(destination.connection_open_count_for_tests(), 2);

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 2);

    reset_ducklake_test_hooks();
}

/// A post-commit retry should not duplicate table-copy rows.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_retry_after_post_commit_failure_is_idempotent() {
    let _test_hook_guard = acquire_ducklake_test_hook_guard().await;
    reset_ducklake_test_hooks();

    let dir = make_test_dir("write_table_rows_retry_after_post_commit_failure_is_idempotent");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(13);
    let schema = make_schema(13, "public", "copy_retry");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    arm_fail_after_copy_batch_commit_once_for_tests(&table_name);
    destination
        .write_table_rows(
            table_id,
            vec![
                TableRow::new(vec![Cell::I32(1), Cell::String("alpha".to_string())]),
                TableRow::new(vec![Cell::I32(2), Cell::String("beta".to_string())]),
            ],
        )
        .await
        .unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 2);
    assert_eq!(count_applied_batches(&conn, &table_name, "copy"), 1);

    reset_ducklake_test_hooks();
}

/// Concurrent same-table copy batches should serialize cleanly and remain exact.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_same_table_copy_batches_complete() {
    let dir = make_test_dir("concurrent_same_table_copy_batches_complete");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(14);
    let schema = make_schema(14, "public", "parallel_copy");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = Arc::new(
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap(),
    );

    // Create the replay marker table ahead of the concurrent scenario so this
    // test isolates same-table serialization behavior rather than first-use
    // marker-table initialization.
    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(-1),
                Cell::String("seed".to_string()),
            ])],
        )
        .await
        .unwrap();
    destination.truncate_table(table_id).await.unwrap();

    let first_batch: Vec<TableRow> = (0..50)
        .map(|id| TableRow::new(vec![Cell::I32(id), Cell::String(format!("first-{id}"))]))
        .collect();
    let second_batch: Vec<TableRow> = (50..100)
        .map(|id| TableRow::new(vec![Cell::I32(id), Cell::String(format!("second-{id}"))]))
        .collect();

    let task_a = {
        let destination = Arc::clone(&destination);
        tokio::spawn(async move { destination.write_table_rows(table_id, first_batch).await })
    };
    let task_b = {
        let destination = Arc::clone(&destination);
        tokio::spawn(async move { destination.write_table_rows(table_id, second_batch).await })
    };

    task_a.await.unwrap().unwrap();
    task_b.await.unwrap().unwrap();

    destination.shutdown().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 100);
    assert_eq!(count_applied_batches(&conn, &table_name, "copy"), 2);
}

/// `write_table_rows` with an empty slice still creates the table schema.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_empty_creates_table() {
    let dir = make_test_dir("write_table_rows_empty");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(2);
    let schema = make_schema(2, "public", "events");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();
    destination
        .write_table_rows(table_id, vec![])
        .await
        .expect("empty write failed");

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(
        count_rows(&conn, &table_name),
        0,
        "table should exist but be empty"
    );
}

/// `truncate_table` deletes all rows while leaving the table schema intact.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_clears_rows() {
    let dir = make_test_dir("truncate_clears_rows");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(3);
    let schema = make_schema(3, "public", "logs");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();
    destination
        .write_table_rows(
            table_id,
            vec![
                TableRow::new(vec![Cell::I32(1), Cell::String("first".to_string())]),
                TableRow::new(vec![Cell::I32(2), Cell::String("second".to_string())]),
            ],
        )
        .await
        .unwrap();

    destination
        .truncate_table(table_id)
        .await
        .expect("truncate failed");

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(
        count_rows(&conn, &table_name),
        0,
        "table should be empty after truncate"
    );

    // Confirm the schema is still intact.
    let col_count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_catalog = {} AND table_schema = {} AND table_name = {}",
                quote_literal("lake"),
                quote_literal("main"),
                quote_literal(&table_name),
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        col_count, 2,
        "table should still have 2 columns after truncate"
    );
}

/// Truncation should clear copy markers so the same rows can be copied again.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_clears_copy_markers_for_recopy() {
    let dir = make_test_dir("truncate_clears_copy_markers_for_recopy");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(15);
    let schema = make_schema(15, "public", "recopy_logs");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let rows = vec![
        TableRow::new(vec![Cell::I32(1), Cell::String("first".to_string())]),
        TableRow::new(vec![Cell::I32(2), Cell::String("second".to_string())]),
    ];

    destination
        .write_table_rows(table_id, rows.clone())
        .await
        .unwrap();
    destination.truncate_table(table_id).await.unwrap();
    destination.write_table_rows(table_id, rows).await.unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 2);
    assert_eq!(count_applied_batches(&conn, &table_name, "copy"), 1);
}

/// `write_events` applies inserts, updates, and deletes to the current table state.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events() {
    use etl::types::{DeleteEvent, InsertEvent, PgLsn, UpdateEvent};

    let dir = make_test_dir("write_events");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(4);
    let schema = make_schema(4, "public", "products");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let lsn = PgLsn::from(100u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 0,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Widget".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 1,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Gadget".to_string())]),
                old_table_row: None,
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 2,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("Spare".to_string())]),
            }),
            Event::Delete(DeleteEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 3,
                table_id,
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(2), Cell::String("Spare".to_string())]),
                )),
            }),
        ])
        .await
        .expect("write_events failed");

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);

    let (id, name): (i32, String) = conn
        .query_row(
            &format!(
                "SELECT id, name FROM {}.{}",
                quote_identifier("lake"),
                quote_identifier(&table_name)
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("state query failed");
    assert_eq!(id, 1);
    assert_eq!(name, "Gadget");
}

/// Small CDC batches should remain inlined after the caller returns.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_small_batch_stays_inlined_after_return() {
    use etl::types::{InsertEvent, PgLsn};

    let dir = make_test_dir("write_events_small_batch_stays_inlined_after_return");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(17);
    let schema = make_schema(17, "public", "cdc_flush");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let lsn = PgLsn::from(700u64);
    destination
        .write_events(vec![Event::Insert(InsertEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            tx_ordinal: 0,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("created".to_string())]),
        })])
        .await
        .unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(count_applied_batches(&conn, &table_name, "mutation"), 1);
    assert_eq!(
        count_table_files(&data, &table_name),
        0,
        "small CDC batch should remain inlined until background maintenance"
    );
}

/// `write_events` keeps update events with old rows on the current-state path.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_with_old_row_update() {
    use etl::types::{InsertEvent, PgLsn, UpdateEvent};

    let dir = make_test_dir("write_events_with_old_row_update");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(8);
    let schema = make_schema(8, "public", "inventory");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let lsn = PgLsn::from(200u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 0,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Widget".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 1,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Gadget".to_string())]),
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(1), Cell::String("Widget".to_string())]),
                )),
            }),
        ])
        .await
        .expect("write_events with old row update failed");

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);

    let (id, name): (i32, String) = conn
        .query_row(
            &format!(
                "SELECT id, name FROM {}.{}",
                quote_identifier("lake"),
                quote_identifier(&table_name)
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("state query failed");
    assert_eq!(id, 1);
    assert_eq!(name, "Gadget");
}

/// Replaying the same CDC batch should be a no-op after the marker row is committed.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_replay_is_idempotent() {
    use etl::types::{DeleteEvent, InsertEvent, PgLsn, UpdateEvent};

    let dir = make_test_dir("write_events_replay_is_idempotent");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(9);
    let schema = make_schema(9, "public", "orders");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let lsn = PgLsn::from(300u64);
    let batch = vec![
        Event::Insert(InsertEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            tx_ordinal: 0,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("draft".to_string())]),
        }),
        Event::Update(UpdateEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            tx_ordinal: 1,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("paid".to_string())]),
            old_table_row: Some((
                false,
                TableRow::new(vec![Cell::I32(1), Cell::String("draft".to_string())]),
            )),
        }),
        Event::Insert(InsertEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            tx_ordinal: 2,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(2), Cell::String("temp".to_string())]),
        }),
        Event::Delete(DeleteEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            tx_ordinal: 3,
            table_id,
            old_table_row: Some((
                false,
                TableRow::new(vec![Cell::I32(2), Cell::String("temp".to_string())]),
            )),
        }),
    ];

    destination.write_events(batch.clone()).await.unwrap();
    destination.write_events(batch).await.unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(count_applied_batches(&conn, &table_name, "mutation"), 1);

    let (id, name): (i32, String) = conn
        .query_row(
            &format!(
                "SELECT id, name FROM {}.{}",
                quote_identifier("lake"),
                quote_identifier(&table_name)
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("state query failed");
    assert_eq!(id, 1);
    assert_eq!(name, "paid");
}

/// Marker-table rows should stay in the DuckLake catalog instead of creating Parquet files.
#[tokio::test(flavor = "multi_thread")]
async fn test_applied_batches_table_uses_data_inlining() {
    use etl::types::{InsertEvent, PgLsn};

    let dir = make_test_dir("applied_batches_table_uses_data_inlining");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(13);
    let schema = make_schema(13, "public", "audit");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let lsn = PgLsn::from(600u64);
    destination
        .write_events(vec![Event::Insert(InsertEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            tx_ordinal: 0,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("created".to_string())]),
        })])
        .await
        .unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(count_applied_batches(&conn, &table_name, "mutation"), 1);
    assert_eq!(count_table_files(&data, "__etl_applied_table_batches"), 0);
}

/// Shutdown should materialize copy rows that were still inlined.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_flushes_inlined_copy_rows() {
    let dir = make_test_dir("shutdown_flushes_inlined_copy_rows");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(34);
    let schema = make_schema(34, "public", "shutdown_copy_flush");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("pending copy row".to_string()),
            ])],
        )
        .await
        .unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(
        count_table_files(&data, &table_name),
        0,
        "small copy batch should stay inlined before shutdown"
    );
    drop(conn);

    destination.shutdown().await.unwrap();

    wait_for_condition(Duration::from_secs(10), || {
        count_table_files(&data, &table_name) >= 1
    })
    .await;

    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert!(
        count_table_files(&data, &table_name) >= 1,
        "shutdown should materialize inlined copy rows"
    );
    assert_eq!(
        flush_inlined_rows(&conn, &table_name),
        0,
        "shutdown should leave no inlined copy rows pending"
    );
}

/// Shutdown should flush inlined CDC rows for all known tables.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_flushes_inlined_cdc_rows_for_all_known_tables() {
    use etl::types::{InsertEvent, PgLsn};

    let dir = make_test_dir("shutdown_flushes_inlined_cdc_rows_for_all_known_tables");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id_a = TableId::new(35);
    let table_id_b = TableId::new(36);
    let schema_a = make_schema(35, "public", "shutdown_cdc_alpha");
    let schema_b = make_schema(36, "public", "shutdown_cdc_beta");
    let table_name_a = table_name_to_ducklake_table_name(&schema_a.name).unwrap();
    let table_name_b = table_name_to_ducklake_table_name(&schema_b.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema_a).await.unwrap();
    store.store_table_schema(schema_b).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let lsn = PgLsn::from(900u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 0,
                table_id: table_id_a,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("alpha".to_string())]),
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 1,
                table_id: table_id_b,
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("beta".to_string())]),
            }),
        ])
        .await
        .unwrap();

    let conn = open_lake_conn_when_tables_visible(
        &catalog_url,
        &data_url,
        &[&table_name_a, &table_name_b],
    )
    .await;
    assert_eq!(count_rows(&conn, &table_name_a), 1);
    assert_eq!(count_rows(&conn, &table_name_b), 1);
    assert_eq!(
        count_table_files(&data, &table_name_a),
        0,
        "small CDC batch should stay inlined before shutdown for alpha"
    );
    assert_eq!(
        count_table_files(&data, &table_name_b),
        0,
        "small CDC batch should stay inlined before shutdown for beta"
    );
    drop(conn);

    destination.shutdown().await.unwrap();

    wait_for_condition(Duration::from_secs(10), || {
        count_table_files(&data, &table_name_a) >= 1 && count_table_files(&data, &table_name_b) >= 1
    })
    .await;

    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn_when_tables_visible(
        &catalog_url,
        &data_url,
        &[&table_name_a, &table_name_b],
    )
    .await;
    assert_eq!(count_rows(&conn, &table_name_a), 1);
    assert_eq!(count_rows(&conn, &table_name_b), 1);
    assert_eq!(
        flush_inlined_rows(&conn, &table_name_a),
        0,
        "shutdown should leave no inlined CDC rows pending for alpha"
    );
    assert_eq!(
        flush_inlined_rows(&conn, &table_name_b),
        0,
        "shutdown should leave no inlined CDC rows pending for beta"
    );
}

/// Mixed table batches remain correct when multiple tables are written in one flush.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_mixed_multi_table_batches() {
    use etl::types::{DeleteEvent, InsertEvent, PgLsn, UpdateEvent};

    let dir = make_test_dir("write_events_mixed_multi_table_batches");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id_a = TableId::new(10);
    let table_id_b = TableId::new(11);
    let schema_a = make_schema(10, "public", "alpha_events");
    let schema_b = make_schema(11, "public", "beta_events");
    let table_name_a = table_name_to_ducklake_table_name(&schema_a.name).unwrap();
    let table_name_b = table_name_to_ducklake_table_name(&schema_b.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema_a).await.unwrap();
    store.store_table_schema(schema_b).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    let lsn = PgLsn::from(400u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 0,
                table_id: table_id_a,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("a-one".to_string())]),
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 1,
                table_id: table_id_b,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("b-one".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 2,
                table_id: table_id_a,
                table_row: TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("a-one-updated".to_string()),
                ]),
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(1), Cell::String("a-one".to_string())]),
                )),
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 3,
                table_id: table_id_b,
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("b-two".to_string())]),
            }),
            Event::Delete(DeleteEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 4,
                table_id: table_id_b,
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(1), Cell::String("b-one".to_string())]),
                )),
            }),
        ])
        .await
        .unwrap();

    destination.shutdown().await.unwrap();
    drop(destination);
    checkpoint_lake(&catalog_url, &data_url);

    let conn = open_lake_conn_when_tables_visible(
        &catalog_url,
        &data_url,
        &[&table_name_a, &table_name_b],
    )
    .await;
    assert_eq!(count_rows(&conn, &table_name_a), 1);
    assert_eq!(count_rows(&conn, &table_name_b), 1);
    assert_eq!(count_applied_batches(&conn, &table_name_a, "mutation"), 1);
    assert_eq!(count_applied_batches(&conn, &table_name_b, "mutation"), 1);

    let name_a: String = conn
        .query_row(
            &format!(
                "SELECT name FROM {}.{} WHERE id = 1",
                quote_identifier("lake"),
                quote_identifier(&table_name_a)
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    let name_b: String = conn
        .query_row(
            &format!(
                "SELECT name FROM {}.{} WHERE id = 2",
                quote_identifier("lake"),
                quote_identifier(&table_name_b)
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();

    assert_eq!(name_a, "a-one-updated");
    assert_eq!(name_b, "b-two");
}

/// A post-commit retry should detect the batch marker and avoid double-applying rows.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_retry_after_post_commit_failure_is_idempotent() {
    use etl::types::{DeleteEvent, InsertEvent, PgLsn, UpdateEvent};

    let _test_hook_guard = acquire_ducklake_test_hook_guard().await;
    reset_ducklake_test_hooks();

    let dir = make_test_dir("write_events_retry_after_post_commit_failure_is_idempotent");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(12);
    let schema = make_schema(12, "public", "payments");
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();

    arm_fail_after_atomic_batch_commit_once_for_tests(&table_name);

    let lsn = PgLsn::from(500u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 0,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("queued".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 1,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("posted".to_string())]),
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(1), Cell::String("queued".to_string())]),
                )),
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 2,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("tmp".to_string())]),
            }),
            Event::Delete(DeleteEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 3,
                table_id,
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(2), Cell::String("tmp".to_string())]),
                )),
            }),
        ])
        .await
        .unwrap();

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(count_applied_batches(&conn, &table_name, "mutation"), 1);

    let name: String = conn
        .query_row(
            &format!(
                "SELECT name FROM {}.{} WHERE id = 1",
                quote_identifier("lake"),
                quote_identifier(&table_name)
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name, "posted");

    reset_ducklake_test_hooks();
}

/// Concurrent async callers should serialize cleanly behind one DuckDB slot.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_writes_with_single_slot_complete() {
    let dir = make_test_dir("concurrent_writes_single_slot");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id_a = TableId::new(6);
    let table_id_b = TableId::new(7);
    let schema_a = make_schema(6, "public", "alpha");
    let schema_b = make_schema(7, "public", "beta");
    let table_name_a = table_name_to_ducklake_table_name(&schema_a.name).unwrap();
    let table_name_b = table_name_to_ducklake_table_name(&schema_b.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema_a).await.unwrap();
    store.store_table_schema(schema_b).await.unwrap();

    let destination = Arc::new(
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap(),
    );

    let rows_a: Vec<TableRow> = (0..50)
        .map(|i| TableRow::new(vec![Cell::I32(i), Cell::String(format!("alpha-{i}"))]))
        .collect();
    let rows_b: Vec<TableRow> = (0..50)
        .map(|i| TableRow::new(vec![Cell::I32(i), Cell::String(format!("beta-{i}"))]))
        .collect();

    let task_a = {
        let destination = Arc::clone(&destination);
        tokio::spawn(async move { destination.write_table_rows(table_id_a, rows_a).await })
    };
    let task_b = {
        let destination = Arc::clone(&destination);
        tokio::spawn(async move { destination.write_table_rows(table_id_b, rows_b).await })
    };

    task_a.await.unwrap().unwrap();
    task_b.await.unwrap().unwrap();

    let conn = open_lake_conn_when_tables_visible(
        &catalog_url,
        &data_url,
        &[&table_name_a, &table_name_b],
    )
    .await;
    assert_eq!(count_rows(&conn, &table_name_a), 50);
    assert_eq!(count_rows(&conn, &table_name_b), 50);
}

/// Verifies that common Postgres types survive the write → read cycle.
#[tokio::test(flavor = "multi_thread")]
async fn test_type_mapping_round_trip() {
    let dir = make_test_dir("type_mapping");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(5);
    let schema = make_rich_schema(5);
    let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
            .await
            .unwrap();
    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(42),
                Cell::String("hello".to_string()),
                Cell::F64(PI),
                Cell::Bool(true),
                Cell::Date(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            ])],
        )
        .await
        .expect("write failed");

    let conn = open_lake_conn_when_tables_visible(&catalog_url, &data_url, &[&table_name]).await;
    let row: (i32, String, f64, bool, String) = conn
        .query_row(
            &format!(
                "SELECT id, label, score, active, CAST(birthday AS VARCHAR) \
                 FROM {}.{}",
                quote_identifier("lake"),
                quote_identifier(&table_name)
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .expect("query failed");

    assert_eq!(row.0, 42);
    assert_eq!(row.1, "hello");
    assert!((row.2 - PI).abs() < 1e-9);
    assert!(row.3);
    assert_eq!(row.4, "2024-06-15");
}
