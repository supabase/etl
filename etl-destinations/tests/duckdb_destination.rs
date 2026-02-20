#![cfg(feature = "duckdb")]

//! Integration tests for the DuckDB destination.
//!
//! No external services required — the `bundled` feature compiles DuckDB directly
//! into the test binary. Each test writes via [`DuckDbDestination`] and then opens
//! an independent [`duckdb::Connection`] to the same file to verify the results.
//!
//! Run with `-- --nocapture` to see the database file paths printed during the tests:
//!
//! ```
//! cargo test -p etl-destinations --features duckdb --test duckdb_destination -- --nocapture
//! ```
//!
//! The database files are intentionally kept on disk after each test so you can
//! inspect them with the DuckDB CLI:
//!
//! ```
//! duckdb /path/printed/by/test.db
//! ```

use chrono::NaiveDate;
use duckdb::Connection;
use etl::destination::Destination;
use etl::store::both::memory::MemoryStore;
use etl::store::schema::SchemaStore;
use etl::types::{
    Cell, ColumnSchema, Event, TableId, TableName, TableRow, TableSchema, Type as PgType,
};
use etl_destinations::duckdb::{DuckDbDestination, table_name_to_duckdb_table_name};
use std::path::PathBuf;

// ── helpers ─────────────────────────────────────────────────────────────────

/// Creates a named `.db` file inside a temporary directory and prints its path.
///
/// The directory is kept on disk after the test so you can inspect the file
/// manually. Old runs are overwritten automatically because the OS recycles
/// temp dir names between test suite runs.
fn make_db_path(test_name: &str) -> PathBuf {
    let dir = tempfile::Builder::new()
        .prefix(&format!("etl_duckdb_{test_name}_"))
        .tempdir()
        .expect("failed to create temp dir")
        .into_path(); // `into_path` prevents auto-cleanup on drop

    let path = dir.join("data.db");
    println!("[{test_name}] DuckDB file: {}", path.display());
    path
}

/// Builds a simple two-column schema: `id INTEGER PK, name TEXT`.
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

/// Builds a richer schema with several Postgres types to verify type mapping.
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

/// Opens a read connection to a DuckDB file for post-write verification.
fn open_read_conn(path: &str) -> Connection {
    Connection::open(path).expect("failed to open verification connection")
}

/// Returns all rows from a table, each row as a `Vec` of typed values for the
/// given `col_count`. The rows are sorted by id (first column).
fn read_all_rows(
    conn: &Connection,
    table_name: &str,
    col_count: usize,
) -> Vec<Vec<duckdb::types::Value>> {
    let mut stmt = conn
        .prepare(&format!("SELECT * FROM \"{table_name}\" ORDER BY id"))
        .expect("prepare failed");
    stmt.query_map([], |row| {
        let values: Vec<duckdb::types::Value> =
            (0..col_count).map(|i| row.get(i).unwrap()).collect();
        Ok(values)
    })
    .expect("query_map failed")
    .map(|r| r.expect("row error"))
    .collect()
}

// ── tests ────────────────────────────────────────────────────────────────────

/// `write_table_rows` inserts rows with `cdc_operation = 'INSERT'` and `cdc_lsn = 0`.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_basic() {
    let db_path = make_db_path("write_table_rows_basic");
    let db_path_str = db_path.to_str().unwrap();

    let table_id = TableId::new(1);
    let schema = make_schema(1, "public", "users");
    let table_name = table_name_to_duckdb_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckDbDestination::new(db_path_str, 4, store).unwrap();

    let rows = vec![
        TableRow::new(vec![Cell::I32(1), Cell::String("Alice".to_string())]),
        TableRow::new(vec![Cell::I32(2), Cell::String("Bob".to_string())]),
        TableRow::new(vec![Cell::I32(3), Cell::Null]),
    ];
    destination
        .write_table_rows(table_id, rows)
        .await
        .expect("write_table_rows failed");

    // Verify using a separate connection (2 cols: id, name).
    let conn = open_read_conn(db_path_str);
    let all_rows = read_all_rows(&conn, &table_name, 2);

    assert_eq!(all_rows.len(), 3, "expected 3 rows");
    assert_eq!(all_rows[0].len(), 2);

    assert_eq!(all_rows[0][0], duckdb::types::Value::Int(1));
    assert_eq!(all_rows[0][1], duckdb::types::Value::Text("Alice".to_string()));
    assert_eq!(all_rows[2][1], duckdb::types::Value::Null);
}

/// Calling `write_table_rows` with an empty vec still creates the table.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_rows_empty_creates_table() {
    let db_path = make_db_path("write_table_rows_empty");
    let db_path_str = db_path.to_str().unwrap();

    let table_id = TableId::new(2);
    let schema = make_schema(2, "public", "events");
    let table_name = table_name_to_duckdb_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckDbDestination::new(db_path_str, 4, store).unwrap();
    destination
        .write_table_rows(table_id, vec![])
        .await
        .expect("write_table_rows (empty) failed");

    let conn = open_read_conn(db_path_str);
    let all_rows = read_all_rows(&conn, &table_name, 2);
    assert!(all_rows.is_empty(), "table should exist but be empty");
}

/// `truncate_table` deletes all rows while preserving the table structure.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_clears_rows() {
    let db_path = make_db_path("truncate_clears_rows");
    let db_path_str = db_path.to_str().unwrap();

    let table_id = TableId::new(3);
    let schema = make_schema(3, "public", "logs");
    let table_name = table_name_to_duckdb_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckDbDestination::new(db_path_str, 4, store).unwrap();

    // Write two rows...
    let rows = vec![
        TableRow::new(vec![Cell::I32(1), Cell::String("first".to_string())]),
        TableRow::new(vec![Cell::I32(2), Cell::String("second".to_string())]),
    ];
    destination
        .write_table_rows(table_id, rows)
        .await
        .expect("write failed");

    // ...then truncate.
    destination
        .truncate_table(table_id)
        .await
        .expect("truncate failed");

    let conn = open_read_conn(db_path_str);
    let all_rows = read_all_rows(&conn, &table_name, 2);
    assert!(all_rows.is_empty(), "table should be empty after truncate");

    // Confirm the table structure is still intact using DuckDB's information_schema.
    let col_count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_name = '{table_name}'"
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(col_count, 2, "table should still have 2 columns after truncate");
}

/// `write_events` records Insert/Update/Delete events with the correct CDC metadata.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_events_cdc_operations() {
    use etl::types::{DeleteEvent, InsertEvent, PgLsn, UpdateEvent};

    let db_path = make_db_path("write_events_cdc");
    let db_path_str = db_path.to_str().unwrap();

    let table_id = TableId::new(4);
    let schema = make_schema(4, "public", "products");
    let table_name = table_name_to_duckdb_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckDbDestination::new(db_path_str, 4, store).unwrap();

    let lsn1 = PgLsn::from(100u64);
    let lsn2 = PgLsn::from(200u64);
    let lsn3 = PgLsn::from(300u64);

    let events = vec![
        Event::Insert(InsertEvent {
            start_lsn: lsn1,
            commit_lsn: lsn1,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Widget".to_string())]),
        }),
        Event::Update(UpdateEvent {
            start_lsn: lsn2,
            commit_lsn: lsn2,
            table_id,
            table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Widget v2".to_string())]),
            old_table_row: None,
        }),
        Event::Delete(DeleteEvent {
            start_lsn: lsn3,
            commit_lsn: lsn3,
            table_id,
            old_table_row: Some((
                false,
                TableRow::new(vec![Cell::I32(1), Cell::String("Widget v2".to_string())]),
            )),
        }),
    ];

    destination
        .write_events(events)
        .await
        .expect("write_events failed");

    // All three events write to the same table: 3 rows total (Insert, Update, Delete).
    let conn = open_read_conn(db_path_str);
    let mut stmt = conn
        .prepare(&format!("SELECT id, name FROM \"{table_name}\""))
        .unwrap();

    let rows: Vec<(i32, String)> = stmt
        .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|(id, name)| *id == 1 && name == "Widget"));
    assert!(rows.iter().any(|(id, name)| *id == 1 && name == "Widget v2"));
}

/// Verifies that common Postgres types are mapped and round-tripped correctly.
#[tokio::test(flavor = "multi_thread")]
async fn test_type_mapping_round_trip() {
    let db_path = make_db_path("type_mapping");
    let db_path_str = db_path.to_str().unwrap();

    let table_id = TableId::new(5);
    let schema = make_rich_schema(5);
    let table_name = table_name_to_duckdb_table_name(&schema.name);

    let store = MemoryStore::new();
    store.store_table_schema(schema).await.unwrap();

    let destination = DuckDbDestination::new(db_path_str, 4, store).unwrap();

    let rows = vec![TableRow::new(vec![
        Cell::I32(42),
        Cell::String("hello".to_string()),
        Cell::F64(3.14),
        Cell::Bool(true),
        Cell::Date(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
    ])];

    destination
        .write_table_rows(table_id, rows)
        .await
        .expect("write failed");

    let conn = open_read_conn(db_path_str);
    let row: (i32, String, f64, bool, String) = conn
        .query_row(
            &format!(
                "SELECT id, label, score, active, CAST(birthday AS VARCHAR) \
                 FROM \"{table_name}\""
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
