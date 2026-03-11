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
use etl_destinations::ducklake::{DuckLakeDestination, table_name_to_ducklake_table_name};
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

    let destination =
        DuckLakeDestination::new(catalog_url.clone(), data_url.clone(), 1, None, None, store)
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
