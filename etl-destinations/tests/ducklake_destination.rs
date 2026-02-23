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
//!   INSTALL ducklake; LOAD ducklake;
//!   ATTACH 'ducklake:/path/printed/catalog.ducklake' AS lake
//!     (DATA_PATH '/path/printed/data/');
//!   SELECT * FROM lake.public_users;
//! "
//! ```

use chrono::NaiveDate;
use duckdb::Connection;
use etl::destination::Destination;
use etl::store::both::memory::MemoryStore;
use etl::store::schema::SchemaStore;
use etl::types::{
    Cell, ColumnSchema, Event, TableId, TableName, TableRow, TableSchema, Type as PgType,
};
use etl_destinations::ducklake::{DuckLakeDestination, table_name_to_ducklake_table_name};
use std::path::PathBuf;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Creates a persistent temp directory named after the test and prints its path.
/// Returns the directory path (kept on disk after the test completes).
fn make_test_dir(test_name: &str) -> PathBuf {
    let dir = tempfile::Builder::new()
        .prefix(&format!("etl_ducklake_{test_name}_"))
        .tempdir()
        .expect("failed to create temp dir")
        .into_path(); // `into_path` prevents auto-cleanup on drop

    println!("[{test_name}] catalog : {}", dir.join("catalog.ducklake").display());
    println!("[{test_name}] data    : {}", dir.join("data").display());
    dir
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
fn open_lake_conn(catalog: &str, data: &str) -> Connection {
    let conn = Connection::open_in_memory().expect("failed to open in-memory DuckDB");
    conn.execute_batch(&format!(
        "INSTALL ducklake; LOAD ducklake; \
         ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data}');"
    ))
    .expect("failed to attach DuckLake catalog");
    conn
}

fn count_rows(conn: &Connection, table_name: &str) -> i64 {
    conn.query_row(
        &format!("SELECT COUNT(*) FROM lake.\"{table_name}\""),
        [],
        |r| r.get(0),
    )
    .expect("count query failed")
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

    let catalog_str = catalog.to_str().unwrap();
    let data_str = data.to_str().unwrap();

    let table_id = TableId::new(1);
    let schema = make_schema(1, "public", "users");
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(catalog_str, data_str, 1, store).unwrap();
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

    let conn = open_lake_conn(catalog_str, data_str);
    assert_eq!(count_rows(&conn, &table_name), 3);

    let (id, name): (i32, Option<String>) = conn
        .query_row(
            &format!("SELECT id, name FROM lake.\"{table_name}\" WHERE id = 1"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(name.as_deref(), Some("Alice"));
}

/// `write_table_rows` with an empty slice still creates the table schema.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_empty_creates_table() {
    let dir = make_test_dir("write_table_rows_empty");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_str = catalog.to_str().unwrap();
    let data_str = data.to_str().unwrap();

    let table_id = TableId::new(2);
    let schema = make_schema(2, "public", "events");
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(catalog_str, data_str, 1, store).unwrap();
    destination
        .write_table_rows(table_id, vec![])
        .await
        .expect("empty write failed");

    let conn = open_lake_conn(catalog_str, data_str);
    assert_eq!(count_rows(&conn, &table_name), 0, "table should exist but be empty");
}

/// `truncate_table` deletes all rows while leaving the table schema intact.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_clears_rows() {
    let dir = make_test_dir("truncate_clears_rows");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_str = catalog.to_str().unwrap();
    let data_str = data.to_str().unwrap();

    let table_id = TableId::new(3);
    let schema = make_schema(3, "public", "logs");
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(catalog_str, data_str, 1, store).unwrap();
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

    destination.truncate_table(table_id).await.expect("truncate failed");

    let conn = open_lake_conn(catalog_str, data_str);
    assert_eq!(count_rows(&conn, &table_name), 0, "table should be empty after truncate");

    // Confirm the schema is still intact.
    let col_count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_name = '{table_name}' AND table_schema = 'lake'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(col_count, 2, "table should still have 2 columns after truncate");
}

/// `write_events` appends Insert, Update, and Delete rows to the lake.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events() {
    use etl::types::{DeleteEvent, InsertEvent, PgLsn, UpdateEvent};

    let dir = make_test_dir("write_events");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_str = catalog.to_str().unwrap();
    let data_str = data.to_str().unwrap();

    let table_id = TableId::new(4);
    let schema = make_schema(4, "public", "products");
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(catalog_str, data_str, 1, store).unwrap();

    let lsn = PgLsn::from(100u64);
    destination
        .write_events(vec![
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                table_row: TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("Widget".to_string()),
                ]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                table_row: TableRow::new(vec![
                    Cell::I32(2),
                    Cell::String("Gadget".to_string()),
                ]),
                old_table_row: None,
            }),
            Event::Delete(DeleteEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                table_id,
                old_table_row: Some((
                    false,
                    TableRow::new(vec![Cell::I32(3), Cell::String("Doohickey".to_string())]),
                )),
            }),
        ])
        .await
        .expect("write_events failed");

    let conn = open_lake_conn(catalog_str, data_str);
    assert_eq!(count_rows(&conn, &table_name), 3);
}

/// Verifies that common Postgres types survive the write → read cycle.
#[tokio::test(flavor = "multi_thread")]
async fn test_type_mapping_round_trip() {
    let dir = make_test_dir("type_mapping");
    let catalog = dir.join("catalog.ducklake");
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();

    let catalog_str = catalog.to_str().unwrap();
    let data_str = data.to_str().unwrap();

    let table_id = TableId::new(5);
    let schema = make_rich_schema(5);
    let table_name = table_name_to_ducklake_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckLakeDestination::new(catalog_str, data_str, 1, store).unwrap();
    destination
        .write_table_rows(
            table_id,
            vec![TableRow::new(vec![
                Cell::I32(42),
                Cell::String("hello".to_string()),
                Cell::F64(3.14),
                Cell::Bool(true),
                Cell::Date(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            ])],
        )
        .await
        .expect("write failed");

    let conn = open_lake_conn(catalog_str, data_str);
    let row: (i32, String, f64, bool, String) = conn
        .query_row(
            &format!(
                "SELECT id, label, score, active, CAST(birthday AS VARCHAR) \
                 FROM lake.\"{table_name}\""
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .expect("query failed");

    assert_eq!(row.0, 42);
    assert_eq!(row.1, "hello");
    assert!((row.2 - 3.14).abs() < 1e-9);
    assert!(row.3);
    assert_eq!(row.4, "2024-06-15");
}
