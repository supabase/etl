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
use duckdb::{Config, Connection};
use etl::destination::Destination;
use etl::store::both::memory::MemoryStore;
use etl::store::schema::SchemaStore;
use etl::types::{
    Cell, ColumnSchema, Event, TableId, TableName, TableRow, TableSchema, Type as PgType,
};
use etl_destinations::ducklake::{
    DuckDbLogConfig, DuckLakeDestination, table_name_to_ducklake_table_name,
};
#[cfg(feature = "test-utils")]
use etl_destinations::ducklake::{
    arm_fail_after_atomic_batch_commit_once_for_tests,
    arm_fail_after_copy_batch_commit_once_for_tests, reset_ducklake_test_hooks,
};
use pg_escape::{quote_identifier, quote_literal};
use std::f64::consts::PI;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

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

fn open_verification_connection() -> Connection {
    if cfg!(target_os = "linux") {
        return Connection::open_in_memory_with_flags(
            Config::default()
                .enable_autoload_extension(false)
                .expect("failed to disable DuckDB extension autoload"),
        )
        .expect("failed to open in-memory DuckDB");
    }

    Connection::open_in_memory().expect("failed to open in-memory DuckDB")
}

fn ducklake_load_sql() -> String {
    if cfg!(target_os = "linux") {
        let platform_dir = match std::env::consts::ARCH {
            "x86_64" => "linux_amd64",
            "aarch64" => "linux_arm64",
            arch => panic!("unsupported linux architecture for DuckDB test extensions: {arch}"),
        };
        let env_override = std::env::var_os("ETL_DUCKDB_EXTENSION_ROOT").map(PathBuf::from);
        let candidate_roots = env_override
            .into_iter()
            .chain([
                PathBuf::from("/app/duckdb_extensions"),
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/duckdb/extensions"),
            ])
            .collect::<Vec<_>>();

        for root in candidate_roots {
            let extension_dir = root.join("1.4.4").join(platform_dir);
            let extension_path = extension_dir.join("ducklake.duckdb_extension");
            let json_extension_path = extension_dir.join("json.duckdb_extension");
            let parquet_extension_path = extension_dir.join("parquet.duckdb_extension");

            if extension_path.is_file()
                && json_extension_path.is_file()
                && parquet_extension_path.is_file()
            {
                return format!(
                    "LOAD {}; LOAD {}; LOAD {};",
                    quote_literal(&extension_path.display().to_string()),
                    quote_literal(&json_extension_path.display().to_string()),
                    quote_literal(&parquet_extension_path.display().to_string())
                );
            }
        }
    }

    "INSTALL ducklake; LOAD ducklake; INSTALL json; LOAD json; INSTALL parquet; LOAD parquet;"
        .to_string()
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

fn count_table_files(conn: &Connection, table_name: &str) -> i64 {
    conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM ducklake_list_files({}, {})",
            quote_literal("lake"),
            quote_literal(table_name)
        ),
        [],
        |r| r.get(0),
    )
    .expect("table file count query failed")
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
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

    let conn = open_lake_conn(&catalog_url, &data_url);
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

/// A post-commit retry should not duplicate table-copy rows.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_retry_after_post_commit_failure_is_idempotent() {
    reset_ducklake_test_hooks();

    let dir = make_test_dir("write_table_rows_retry_after_post_commit_failure_is_idempotent");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(13);
    let schema = make_schema(13, "public", "copy_retry");
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
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

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(count_rows(&conn, &table_name), 2);
    assert_eq!(count_applied_batches(&conn, &table_name, "copy"), 1);

    reset_ducklake_test_hooks();
}

/// Concurrent same-table copy batches should remain exact across a post-commit retry.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_same_table_copy_batches_retry_after_post_commit_failure() {
    reset_ducklake_test_hooks();

    let dir = make_test_dir("concurrent_same_table_copy_batches_retry_after_post_commit_failure");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(14);
    let schema = make_schema(14, "public", "parallel_copy");
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = Arc::new(
        DuckLakeDestination::new(
            catalog_url.clone(),
            data_url.clone(),
            2,
            None,
            None,
            None,
            store,
        )
        .await
        .unwrap(),
    );

    let first_batch: Vec<TableRow> = (0..50)
        .map(|id| TableRow::new(vec![Cell::I32(id), Cell::String(format!("first-{id}"))]))
        .collect();
    let second_batch: Vec<TableRow> = (50..100)
        .map(|id| TableRow::new(vec![Cell::I32(id), Cell::String(format!("second-{id}"))]))
        .collect();

    arm_fail_after_copy_batch_commit_once_for_tests(&table_name);

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

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(count_rows(&conn, &table_name), 100);
    assert_eq!(count_applied_batches(&conn, &table_name, "copy"), 2);

    reset_ducklake_test_hooks();
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
    .await
    .unwrap();
    destination
        .write_table_rows(table_id, vec![])
        .await
        .expect("empty write failed");

    let conn = open_lake_conn(&catalog_url, &data_url);
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
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

    let conn = open_lake_conn(&catalog_url, &data_url);
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
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

    let conn = open_lake_conn(&catalog_url, &data_url);
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
    .await
    .unwrap();

    let lsn = PgLsn::from(100u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Widget".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Gadget".to_string())]),
                old_table_row: None,
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("Spare".to_string())]),
            }),
            Event::Delete(DeleteEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(2), Cell::String("Spare".to_string())]),
                )),
            }),
        ])
        .await
        .expect("write_events failed");

    let conn = open_lake_conn(&catalog_url, &data_url);
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
    .await
    .unwrap();

    let lsn = PgLsn::from(200u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Widget".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
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

    let conn = open_lake_conn(&catalog_url, &data_url);
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
    .await
    .unwrap();

    let lsn = PgLsn::from(300u64);
    let batch = vec![
        Event::Insert(InsertEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("draft".to_string())]),
        }),
        Event::Update(UpdateEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
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
            table_id,
            table_row: TableRow::new(vec![Cell::I32(2), Cell::String("temp".to_string())]),
        }),
        Event::Delete(DeleteEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            table_id,
            old_table_row: Some((
                false,
                TableRow::new(vec![Cell::I32(2), Cell::String("temp".to_string())]),
            )),
        }),
    ];

    destination.write_events(batch.clone()).await.unwrap();
    destination.write_events(batch).await.unwrap();

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(count_applied_batches(&conn, &table_name, "mutation"), 4);

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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
    .await
    .unwrap();

    let lsn = PgLsn::from(600u64);
    destination
        .write_events(vec![Event::Insert(InsertEvent {
            start_lsn: lsn,
            commit_lsn: lsn,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("created".to_string())]),
        })])
        .await
        .unwrap();

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(count_applied_batches(&conn, &table_name, "mutation"), 1);
    assert_eq!(count_table_files(&conn, "__etl_applied_table_batches"), 0);
    assert!(
        count_table_files(&conn, &table_name) >= 1,
        "user data table should still materialize Parquet files"
    );
}

/// Shutdown should dump file-backed DuckDB logs when logging is enabled.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_dumps_duckdb_logs() {
    let dir = make_test_dir("shutdown_dumps_duckdb_logs");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    let duckdb_log_storage = dir.join("duckdb_logs");
    let duckdb_log_dump = dir.join("duckdb_logs_dump.csv");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(14);
    let schema = make_schema(14, "public", "loggable");

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url,
        data_url,
        1,
        None,
        None,
        Some(DuckDbLogConfig {
            storage_path: duckdb_log_storage.display().to_string(),
            dump_path: duckdb_log_dump.display().to_string(),
        }),
        store,
    )
    .await
    .unwrap();

    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("first row".to_string()),
            ])],
        )
        .await
        .unwrap();

    destination.shutdown().await.unwrap();

    let dump_contents = std::fs::read_to_string(&duckdb_log_dump).unwrap();
    assert!(
        !dump_contents.is_empty(),
        "duckdb log dump should not be empty"
    );
    assert!(
        dump_contents.lines().count() >= 1,
        "duckdb log dump should contain at least a header row"
    );
    assert!(
        duckdb_log_storage.exists(),
        "duckdb log storage path should exist when logging is enabled"
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
    let table_name_a = table_name_to_ducklake_table_name(&schema_a.name);
    let table_name_b = table_name_to_ducklake_table_name(&schema_b.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema_a).await.unwrap();
    store.store_table_schema(schema_b).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
    .await
    .unwrap();

    let lsn = PgLsn::from(400u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id: table_id_a,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("a-one".to_string())]),
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id: table_id_b,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("b-one".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
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
                table_id: table_id_b,
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("b-two".to_string())]),
            }),
            Event::Delete(DeleteEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id: table_id_b,
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(1), Cell::String("b-one".to_string())]),
                )),
            }),
        ])
        .await
        .unwrap();

    drop(destination);

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(count_rows(&conn, &table_name_a), 1);
    assert_eq!(count_rows(&conn, &table_name_b), 1);
    assert_eq!(count_applied_batches(&conn, &table_name_a, "mutation"), 2);
    assert_eq!(count_applied_batches(&conn, &table_name_b, "mutation"), 2);

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

    reset_ducklake_test_hooks();

    let dir = make_test_dir("write_events_retry_after_post_commit_failure_is_idempotent");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_url = path_to_file_url(&catalog);
    let data_url = path_to_file_url(&data);

    let table_id = TableId::new(12);
    let schema = make_schema(12, "public", "payments");
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
    .await
    .unwrap();

    arm_fail_after_atomic_batch_commit_once_for_tests(&table_name);

    let lsn = PgLsn::from(500u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                table_row: TableRow::new(vec![Cell::I32(1), Cell::String("queued".to_string())]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
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
                table_id,
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("tmp".to_string())]),
            }),
            Event::Delete(DeleteEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(2), Cell::String("tmp".to_string())]),
                )),
            }),
        ])
        .await
        .unwrap();

    let conn = open_lake_conn(&catalog_url, &data_url);
    assert_eq!(count_rows(&conn, &table_name), 1);
    assert_eq!(count_applied_batches(&conn, &table_name, "mutation"), 4);

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
    let table_name_a = table_name_to_ducklake_table_name(&schema_a.name);
    let table_name_b = table_name_to_ducklake_table_name(&schema_b.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema_a).await.unwrap();
    store.store_table_schema(schema_b).await.unwrap();

    let destination = Arc::new(
        DuckLakeDestination::new(
            catalog_url.clone(),
            data_url.clone(),
            1,
            None,
            None,
            None,
            store,
        )
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

    let conn = open_lake_conn(&catalog_url, &data_url);
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
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(
        catalog_url.clone(),
        data_url.clone(),
        1,
        None,
        None,
        None,
        store,
    )
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

    let conn = open_lake_conn(&catalog_url, &data_url);
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
