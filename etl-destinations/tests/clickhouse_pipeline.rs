#![cfg(all(feature = "clickhouse", feature = "test-utils"))]

use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notifying_store::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::types::PipelineId;
use etl_destinations::clickhouse::test_utils::{ClickHouseTestDatabase, setup_clickhouse_database};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use std::sync::Once;
use std::time::Duration;
use tokio::time::sleep;

use crate::support::clickhouse::{AllTypesRow, BoundaryValuesRow};

mod support;

/// Ensures the rustls crypto provider is only installed once across all tests.
static INIT_CRYPTO: Once = Once::new();

fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

/// SELECT query that fetches all verified columns from the ClickHouse table.
///
/// `uuid_col` is projected via `toString()` because the ClickHouse UUID RowBinary
/// wire format does not directly map to a Rust `String`; `toString()` gives us the
/// canonical `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` string form.
///
/// All other columns are read with their native ClickHouse types:
/// - `Date`          -> u16  (days since 1970-01-01)
/// - `DateTime64(6)` -> i64  (microseconds since epoch)
/// - `Array(Nullable(T))` -> `Vec<Option<T>>`
const ALL_TYPES_SELECT: &str = concat!(
    "SELECT ",
    "id, smallint_col, integer_col, bigint_col, real_col, double_col, ",
    "numeric_col, boolean_col, text_col, varchar_col, ",
    "date_col, timestamp_col, timestamptz_col, time_col, interval_col, ",
    "jsonb_col, json_col, integer_array_col, text_array_col, ",
    "bytea_col, inet_col, cidr_col, macaddr_col, ",
    "toString(uuid_col) AS uuid_col, ",
    "cdc_operation ",
    "FROM \"test_all__types__encoding\" ",
    "ORDER BY id",
);

/// A row read back from the ClickHouse `update_flow` test table.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
struct UpdateFlowRow {
    id: i64,
    value: String,
    cdc_operation: String,
    cdc_lsn: i64,
}

/// SELECT query used to verify the `update_flow` streaming test.
const UPDATE_FLOW_SELECT: &str = concat!(
    "SELECT id, value, cdc_operation, cdc_lsn ",
    "FROM \"test_update__flow\" ",
    "ORDER BY id, cdc_lsn",
);

/// SELECT query used to verify the `delete_flow` streaming test.
const DELETE_FLOW_SELECT: &str = concat!(
    "SELECT id, value, cdc_operation, cdc_lsn ",
    "FROM \"test_delete__flow\" ",
    "ORDER BY id, cdc_lsn",
);

/// SELECT query used to verify the `restart_flow` test.
const RESTART_FLOW_SELECT: &str = concat!(
    "SELECT id, value, cdc_operation, cdc_lsn ",
    "FROM \"test_restart__flow\" ",
    "ORDER BY id, cdc_lsn",
);

/// Days from 1970-01-01 to 2024-01-15 (used to verify the `date_col` round-trip).
///
/// Python: `(date(2024, 1, 15) - date(1970, 1, 1)).days` = 19737
const DATE_2024_01_15_DAYS: u16 = 19737;

/// Microseconds from epoch for `2024-01-15 12:00:00 UTC`.
///
/// Python: `int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000)`
/// = 1705320000000000
const TS_2024_01_15_12_00_US: i64 = 1_705_320_000_000_000;

/// Waits until ClickHouse returns at least `expected_rows` from `UPDATE_FLOW_SELECT`.
async fn wait_for_update_flow_rows(
    ch_db: &ClickHouseTestDatabase,
    expected_rows: usize,
) -> Vec<UpdateFlowRow> {
    let mut rows: Vec<UpdateFlowRow> = Vec::with_capacity(expected_rows);
    for _ in 0..50 {
        rows = ch_db.query(UPDATE_FLOW_SELECT).await;
        if rows.len() >= expected_rows {
            return rows;
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!(
        "timed out waiting for clickhouse update_flow rows: got {} of {}",
        rows.len(),
        expected_rows,
    );
}

/// Waits until ClickHouse returns at least `expected_rows` from `DELETE_FLOW_SELECT`.
async fn wait_for_delete_flow_rows(
    ch_db: &ClickHouseTestDatabase,
    expected_rows: usize,
) -> Vec<UpdateFlowRow> {
    let mut rows: Vec<UpdateFlowRow> = Vec::with_capacity(expected_rows);
    for _ in 0..50 {
        rows = ch_db.query(DELETE_FLOW_SELECT).await;
        if rows.len() >= expected_rows {
            return rows;
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!(
        "timed out waiting for clickhouse delete_flow rows: got {} of {}",
        rows.len(),
        expected_rows,
    );
}

/// Waits until ClickHouse returns at least `expected_rows` from `RESTART_FLOW_SELECT`.
async fn wait_for_restart_flow_rows(
    ch_db: &ClickHouseTestDatabase,
    expected_rows: usize,
) -> Vec<UpdateFlowRow> {
    let mut rows: Vec<UpdateFlowRow> = Vec::with_capacity(expected_rows);
    for _ in 0..50 {
        rows = ch_db.query(RESTART_FLOW_SELECT).await;
        if rows.len() >= expected_rows {
            return rows;
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!(
        "timed out waiting for clickhouse restart_flow rows: got {} of {}",
        rows.len(),
        expected_rows,
    );
}

/// Tests that all Postgres column types (including nullable arrays) round-trip
/// correctly through the ClickHouse RowBinary encoding.
///
/// # GIVEN
///
/// A Postgres table covering every supported column type -- scalars (integers,
/// floats, numeric, boolean, text, varchar, date, timestamp, timestamptz, time,
/// interval, jsonb, json, bytea, inet, cidr, macaddr, uuid) and nullable array
/// columns (`integer[]`, `text[]`). Two rows are inserted before the pipeline
/// starts:
///
/// 1. Positive/typical values with **empty** arrays.
/// 2. Boundary values (min-ints, negative floats) with **non-empty** arrays.
///
/// # WHEN
///
/// The pipeline runs initial table copy from Postgres to ClickHouse.
///
/// # THEN
///
/// Every column round-trips correctly:
/// - Scalars match their inserted values exactly (floats within epsilon).
/// - Empty arrays remain empty; non-empty arrays preserve elements.
/// - Both rows have `cdc_operation = "INSERT"`.
///
/// # Regression
///
/// Row 2's non-empty arrays specifically catch the nullable-array encoding bug
/// where `nullable_flags[i] = true` for array columns caused `rb_encode_nullable`
/// to prepend an extra null-indicator byte. ClickHouse read that byte as
/// `varint(0)` (empty array) and then parsed the actual element bytes as
/// subsequent column data, failing with "Cannot read all data" at row 2.
#[tokio::test(flavor = "multi_thread")]
async fn all_types_table_copy() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: Postgres source with all supported column types ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("all_types_encoding");

    let table_id = database
        .create_table(
            table_name.clone(),
            true, // add serial primary key
            &[
                // Scalar types
                ("smallint_col", "smallint not null"),
                ("integer_col", "integer not null"),
                ("bigint_col", "bigint not null"),
                ("real_col", "real not null"),
                ("double_col", "double precision not null"),
                ("numeric_col", "numeric(10,2) not null"),
                ("boolean_col", "boolean not null"),
                ("text_col", "text not null"),
                ("varchar_col", "varchar(100) not null"),
                ("date_col", "date not null"),
                ("timestamp_col", "timestamp not null"),
                ("timestamptz_col", "timestamptz not null"),
                ("time_col", "time not null"),
                ("interval_col", "interval not null"),
                ("jsonb_col", "jsonb not null"),
                ("json_col", "json not null"),
                // Nullable array columns (key for the regression test).
                // These are intentionally nullable so that nullable_flags[i] would
                // have been set to `true` before the fix, triggering the bug.
                ("integer_array_col", "integer[]"),
                ("text_array_col", "text[]"),
                // Other types
                ("bytea_col", "bytea not null"),
                ("inet_col", "inet not null"),
                ("cidr_col", "cidr not null"),
                ("macaddr_col", "macaddr not null"),
                ("uuid_col", "uuid not null"),
            ],
        )
        .await
        .expect("Failed to create test table");

    let publication_name = "test_pub_clickhouse";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    // Row 1: empty arrays.  With the old encoding bug, this accidentally
    //        produced valid RowBinary because `0x00` (null-indicator) ==
    //        varint(0) == empty array.
    database
        .run_sql(&format!(
            r#"INSERT INTO {table} (
                smallint_col, integer_col, bigint_col,
                real_col, double_col, numeric_col, boolean_col,
                text_col, varchar_col,
                date_col, timestamp_col, timestamptz_col,
                time_col, interval_col, jsonb_col, json_col,
                integer_array_col, text_array_col,
                bytea_col, inet_col, cidr_col, macaddr_col, uuid_col
            ) VALUES (
                42, 1000, 9999999,
                1.5, 2.5, 12345.67, true,
                'hello text', 'hello varchar',
                '2024-01-15', '2024-01-15 12:00:00', '2024-01-15 12:00:00+00',
                '14:30:00', '1 day',
                '{{"key":"value"}}', '{{"simple":42}}',
                ARRAY[]::integer[], ARRAY[]::text[],
                '\xdeadbeef',
                '192.168.1.1', '192.168.0.0/16', 'aa:bb:cc:dd:ee:ff',
                'f47ac10b-58cc-4372-a567-0e02b2c3d479'
            )"#,
            table = table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert row 1");

    // Row 2: NON-EMPTY arrays.  With the old encoding bug, this row caused
    //        ClickHouse to fail with "Cannot read all data" because the extra
    //        null-indicator byte caused the entire RowBinary stream to be
    //        mis-aligned after the array column.
    database
        .run_sql(&format!(
            r#"INSERT INTO {table} (
                smallint_col, integer_col, bigint_col,
                real_col, double_col, numeric_col, boolean_col,
                text_col, varchar_col,
                date_col, timestamp_col, timestamptz_col,
                time_col, interval_col, jsonb_col, json_col,
                integer_array_col, text_array_col,
                bytea_col, inet_col, cidr_col, macaddr_col, uuid_col
            ) VALUES (
                -32768, -2147483648, -9223372036854775808,
                -1.5, -2.5, -99999.99, false,
                'world text', 'world varchar',
                '2024-01-15', '2024-01-15 12:00:00', '2024-01-15 12:00:00+00',
                '00:00:01', '30 days 23 hours',
                '{{"arr":[1,2,3]}}', '{{"n":0}}',
                ARRAY[1, 2, 3]::integer[], ARRAY['alpha', 'beta']::text[],
                '\xcafebabe',
                '10.0.0.1', '10.0.0.0/8', 'ff:ee:dd:cc:bb:aa',
                'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
            )"#,
            table = table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert row 2");

    // --- WHEN: pipeline copies data to ClickHouse ---
    let ch_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = ch_db.build_destination(pipeline_id, store.clone());

    let table_ready = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination,
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: every column round-trips correctly ---
    let rows: Vec<AllTypesRow> = ch_db.query(ALL_TYPES_SELECT).await;

    assert_eq!(rows.len(), 2, "expected 2 rows in ClickHouse");

    // Row 1: positive/typical values, empty arrays.
    let r1 = &rows[0];
    assert_eq!(r1.id, 1);
    assert_eq!(r1.smallint_col, 42);
    assert_eq!(r1.integer_col, 1000);
    assert_eq!(r1.bigint_col, 9_999_999);
    assert!((r1.real_col - 1.5_f32).abs() < 1e-3, "real_col mismatch");
    assert!(
        (r1.double_col - 2.5_f64).abs() < 1e-6,
        "double_col mismatch"
    );
    assert_eq!(r1.numeric_col, "12345.67");
    assert!(r1.boolean_col);
    assert_eq!(r1.text_col, "hello text");
    assert_eq!(r1.varchar_col, "hello varchar");
    assert_eq!(r1.date_col, DATE_2024_01_15_DAYS, "date round-trip failed");
    assert_eq!(
        r1.timestamp_col, TS_2024_01_15_12_00_US,
        "timestamp round-trip failed"
    );
    assert_eq!(
        r1.timestamptz_col, TS_2024_01_15_12_00_US,
        "timestamptz round-trip failed"
    );
    assert_eq!(r1.time_col, "14:30:00");
    assert_eq!(r1.bytea_col, "deadbeef");
    assert_eq!(r1.inet_col, "192.168.1.1");
    assert_eq!(r1.cidr_col, "192.168.0.0/16");
    assert_eq!(r1.macaddr_col, "aa:bb:cc:dd:ee:ff");
    assert_eq!(
        r1.uuid_col.to_lowercase(),
        "f47ac10b-58cc-4372-a567-0e02b2c3d479"
    );
    assert_eq!(r1.cdc_operation, "INSERT");
    // Empty arrays -- the regression case that accidentally worked before the fix.
    assert_eq!(
        r1.integer_array_col,
        Vec::<Option<i32>>::new(),
        "row 1 integer_array_col should be empty"
    );
    assert_eq!(
        r1.text_array_col,
        Vec::<Option<String>>::new(),
        "row 1 text_array_col should be empty"
    );

    // Row 2: boundary values, non-empty arrays (the regression case).
    let r2 = &rows[1];
    assert_eq!(r2.id, 2);
    assert_eq!(r2.smallint_col, -32768);
    assert_eq!(r2.integer_col, -2_147_483_648);
    assert_eq!(r2.bigint_col, i64::MIN);
    assert!(!r2.boolean_col);
    assert_eq!(r2.numeric_col, "-99999.99");
    assert_eq!(r2.bytea_col, "cafebabe");
    assert_eq!(
        r2.uuid_col.to_lowercase(),
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    );
    assert_eq!(r2.cdc_operation, "INSERT");
    // Non-empty arrays -- the regression case that triggered the bug before the fix.
    assert_eq!(
        r2.integer_array_col,
        vec![Some(1), Some(2), Some(3)],
        "row 2 integer_array_col mismatch -- nullable-array encoding bug likely present"
    );
    assert_eq!(
        r2.text_array_col,
        vec![Some("alpha".to_string()), Some("beta".to_string())],
        "row 2 text_array_col mismatch -- nullable-array encoding bug likely present"
    );
}

/// Tests that UPDATE events are streamed to ClickHouse after the initial table copy.
///
/// # GIVEN
///
/// A Postgres table with a single row (`id=1, value='before'`).
///
/// # WHEN
///
/// The pipeline copies the row, then an `UPDATE ... SET value = 'after'` is
/// issued against Postgres.
///
/// # THEN
///
/// ClickHouse contains two rows (append-only CDC):
/// - The original `INSERT` from table copy with `cdc_lsn = 0`.
/// - The streamed `UPDATE` with the new value and a positive LSN.
#[tokio::test(flavor = "multi_thread")]
async fn updates_are_streamed_to_clickhouse() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: Postgres source with one row ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("update_flow");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create update_flow test table");

    let publication_name = "test_pub_clickhouse_updates";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create update_flow publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('before')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert initial update_flow row");

    // --- WHEN: pipeline copies data, then an UPDATE is streamed ---
    let ch_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = ch_db.build_destination(pipeline_id, store.clone());

    let table_ready = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination,
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    database
        .run_sql(&format!(
            "UPDATE {} SET value = 'after' WHERE id = 1",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to update update_flow row");

    let rows = wait_for_update_flow_rows(&ch_db, 2).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: one INSERT from table copy, one UPDATE from streaming ---
    assert_eq!(rows.len(), 2, "expected copied row plus streamed update");

    let insert_row = &rows[0];
    assert_eq!(insert_row.id, 1);
    assert_eq!(insert_row.value, "before");
    assert_eq!(insert_row.cdc_operation, "INSERT");
    assert_eq!(insert_row.cdc_lsn, 0);

    let update_row = &rows[1];
    assert_eq!(update_row.id, 1);
    assert_eq!(update_row.value, "after");
    assert_eq!(update_row.cdc_operation, "UPDATE");
    assert!(
        update_row.cdc_lsn > insert_row.cdc_lsn,
        "streamed update should have a positive LSN"
    );
}

const BOUNDARY_VALUES_SELECT: &str = concat!(
    "SELECT id, nullable_text, nullable_int, ",
    "int_array_col, text_array_col, ",
    "cdc_operation ",
    "FROM \"test_boundary__values\" ",
    "ORDER BY id",
);

/// Tests that edge-case values survive the Postgres -> ClickHouse pipeline
/// without data loss or corruption.
///
/// # GIVEN
///
/// A Postgres table with nullable scalar columns and nullable array columns,
/// populated with four rows that exercise encoding boundary conditions:
///
/// 1. **All NULLs** -- nullable scalars are NULL, arrays are empty.
/// 2. **NULL elements inside arrays** -- `{1, NULL, 3}`, `{'a', NULL, 'c'}`.
/// 3. **Empty strings** -- a present-but-empty text value next to a NULL integer,
///    plus single-element arrays (varint length = 1).
/// 4. **Multi-byte UTF-8** -- emoji and CJK characters, verifying that the
///    RowBinary varint encodes byte length (not character count) correctly.
///
/// # WHEN
///
/// The pipeline runs initial table copy from Postgres to ClickHouse.
///
/// # THEN
///
/// Every row in ClickHouse exactly matches what was inserted into Postgres:
/// - SQL NULLs remain NULL (not empty string, not zero).
/// - Empty strings remain empty strings (not NULL).
/// - Array elements preserve their position, including interior NULLs.
/// - Multi-byte text round-trips byte-for-byte.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_values_table_copy() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: Postgres source with boundary-value rows ---

    let database = spawn_source_database().await;
    let table_name = test_table_name("boundary_values");

    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("nullable_text", "text"),      // nullable
                ("nullable_int", "integer"),    // nullable
                ("int_array_col", "integer[]"), // Array(Nullable(Int32))
                ("text_array_col", "text[]"),   // Array(Nullable(String))
            ],
        )
        .await
        .expect("Failed to create boundary_values table");

    let publication_name = "test_pub_ch_boundary";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    // Row 1: all nullable columns are NULL, arrays are empty.
    database
        .run_sql(&format!(
            "INSERT INTO {} (nullable_text, nullable_int, int_array_col, text_array_col) \
             VALUES (NULL, NULL, ARRAY[]::integer[], ARRAY[]::text[])",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert row 1 (all NULLs)");

    // Row 2: arrays with interior NULL elements -- the element at index 1 is NULL
    // while surrounding elements are present.
    database
        .run_sql(&format!(
            "INSERT INTO {} (nullable_text, nullable_int, int_array_col, text_array_col) \
             VALUES ('present', 42, ARRAY[1, NULL, 3]::integer[], ARRAY['a', NULL, 'c']::text[])",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert row 2 (NULL array elements)");

    // Row 3: empty string (not NULL) for text, NULL for integer, and
    // single-element arrays (varint length byte = 0x01).
    database
        .run_sql(&format!(
            "INSERT INTO {} (nullable_text, nullable_int, int_array_col, text_array_col) \
             VALUES ('', NULL, ARRAY[99]::integer[], ARRAY['only']::text[])",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert row 3 (empty string + single-element arrays)");

    // Row 4: multi-byte UTF-8 -- emoji (4 bytes per char) and CJK (3 bytes per char).
    // The RowBinary varint encodes byte length, not character count.
    database
        .run_sql(&format!(
            "INSERT INTO {} (nullable_text, nullable_int, int_array_col, text_array_col) \
             VALUES ('hello 🌍🚀', 0, ARRAY[1, 2], ARRAY['日本語', '中文'])",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert row 4 (multi-byte UTF-8)");

    // --- WHEN: pipeline copies data to ClickHouse ---
    let ch_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = ch_db.build_destination(pipeline_id, store.clone());

    let table_ready = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination,
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: ClickHouse data matches Postgres exactly ---
    let rows: Vec<BoundaryValuesRow> = ch_db.query(BOUNDARY_VALUES_SELECT).await;
    assert_eq!(rows.len(), 4, "expected 4 rows in ClickHouse");

    // Row 1: NULL scalars stay NULL, empty arrays stay empty.
    let r = &rows[0];
    assert_eq!(
        r.nullable_text, None,
        "NULL text must not become empty string"
    );
    assert_eq!(r.nullable_int, None, "NULL int must not become zero");
    assert!(r.int_array_col.is_empty());
    assert!(r.text_array_col.is_empty());

    // Row 2: interior NULLs preserved in position.
    let r = &rows[1];
    assert_eq!(r.nullable_text.as_deref(), Some("present"));
    assert_eq!(r.nullable_int, Some(42));
    assert_eq!(
        r.int_array_col,
        vec![Some(1), None, Some(3)],
        "interior NULL in integer array must be preserved"
    );
    assert_eq!(
        r.text_array_col,
        vec![Some("a".to_string()), None, Some("c".to_string())],
        "interior NULL in text array must be preserved"
    );

    // Row 3: empty string is distinct from NULL.
    let r = &rows[2];
    assert_eq!(
        r.nullable_text.as_deref(),
        Some(""),
        "empty string must round-trip as empty string, not NULL"
    );
    assert_eq!(r.nullable_int, None);
    assert_eq!(r.int_array_col, vec![Some(99)], "single-element array");
    assert_eq!(
        r.text_array_col,
        vec![Some("only".to_string())],
        "single-element array"
    );

    // Row 4: multi-byte UTF-8 preserved byte-for-byte.
    let r = &rows[3];
    assert_eq!(
        r.nullable_text.as_deref(),
        Some("hello 🌍🚀"),
        "multi-byte UTF-8 must round-trip exactly"
    );
    assert_eq!(r.nullable_int, Some(0), "zero must not become NULL");
    assert_eq!(
        r.text_array_col,
        vec![Some("日本語".to_string()), Some("中文".to_string())],
        "multi-byte UTF-8 in arrays must round-trip exactly"
    );
}

/// Tests that DELETE events are streamed to ClickHouse after the initial table copy.
///
/// # GIVEN
///
/// A Postgres table with `REPLICA IDENTITY FULL` (so Postgres sends all column
/// values in DELETE events, not just the primary key), populated with two rows:
///
/// 1. `id=1, value='keep_me'` -- will remain untouched.
/// 2. `id=2, value='delete_me'` -- will be deleted after table copy.
///
/// # WHEN
///
/// The pipeline copies both rows, then a `DELETE ... WHERE id = 2` is issued
/// against Postgres.
///
/// # THEN
///
/// ClickHouse contains three rows (append-only CDC):
/// - Two `INSERT` rows from the initial table copy (`cdc_lsn = 0`).
/// - One `DELETE` row for `id=2` with the old row data preserved and a positive LSN.
/// - The `id=1` row has no corresponding `DELETE`.
#[tokio::test(flavor = "multi_thread")]
async fn deletes_are_streamed_to_clickhouse() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: Postgres source with two rows, REPLICA IDENTITY FULL ---

    let database = spawn_source_database().await;
    let table_name = test_table_name("delete_flow");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create delete_flow test table");

    database
        .run_sql(&format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to set replica identity full");

    let publication_name = "test_pub_clickhouse_deletes";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create delete_flow publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('keep_me'), ('delete_me')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert delete_flow rows");

    // --- WHEN: pipeline copies data, then a DELETE is streamed ---

    let ch_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = ch_db.build_destination(pipeline_id, store.clone());

    let table_ready = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination,
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    database
        .run_sql(&format!(
            "DELETE FROM {} WHERE id = 2",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to delete delete_flow row");

    let rows = wait_for_delete_flow_rows(&ch_db, 3).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: two INSERTs from table copy, one DELETE from streaming ---

    assert_eq!(
        rows.len(),
        3,
        "expected 2 copied rows plus 1 streamed delete"
    );

    // Row 1: copied, untouched.
    let r = &rows[0];
    assert_eq!(r.id, 1);
    assert_eq!(r.value, "keep_me");
    assert_eq!(r.cdc_operation, "INSERT");
    assert_eq!(r.cdc_lsn, 0);

    // Row 2: copied, then deleted.
    let r = &rows[1];
    assert_eq!(r.id, 2);
    assert_eq!(r.value, "delete_me");
    assert_eq!(r.cdc_operation, "INSERT");
    assert_eq!(r.cdc_lsn, 0);

    // Row 3: the streamed DELETE for id=2, preserving old row data.
    let r = &rows[2];
    assert_eq!(r.id, 2, "delete must target the correct row");
    assert_eq!(
        r.value, "delete_me",
        "old row data must be preserved in DELETE"
    );
    assert_eq!(r.cdc_operation, "DELETE");
    assert!(r.cdc_lsn > 0, "streamed delete should have a positive LSN");
}

/// Tests that a pipeline restart resumes CDC streaming without re-running
/// the initial table copy.
///
/// # GIVEN
///
/// A Postgres table with one row (`id=1, value='before_restart'`), copied
/// to ClickHouse by a first pipeline run that then shuts down cleanly.
///
/// # WHEN
///
/// A new `ClickHouseDestination` and `Pipeline` are built with the same
/// store and pipeline_id (simulating process restart), the pipeline is
/// started, and a second row (`id=2, value='after_restart'`) is inserted
/// into Postgres.
///
/// # THEN
///
/// ClickHouse contains exactly two rows:
/// - `id=1` from the initial table copy (`cdc_lsn = 0`).
/// - `id=2` from CDC streaming in the second run (`cdc_lsn > 0`).
/// No duplicate `id=1` row exists -- table copy must not re-run.
#[tokio::test(flavor = "multi_thread")]
async fn pipeline_restart_resumes_streaming() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: first pipeline run copies one row ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("restart_flow");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create restart_flow test table");

    let publication_name = "test_pub_clickhouse_restart";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create restart_flow publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('before_restart')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert initial restart_flow row");

    let ch_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = ch_db.build_destination(pipeline_id, store.clone());

    let table_ready = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination,
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // Verify first run produced exactly one row.
    let rows: Vec<UpdateFlowRow> = ch_db.query(RESTART_FLOW_SELECT).await;
    assert_eq!(rows.len(), 1, "first run should copy exactly one row");
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].value, "before_restart");

    // --- WHEN: rebuild destination and pipeline, then stream a new insert ---
    let destination = ch_db.build_destination(pipeline_id, store.clone());

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination,
    );

    pipeline.start().await.unwrap();

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('after_restart')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert post-restart row");

    let rows = wait_for_restart_flow_rows(&ch_db, 2).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: exactly two rows, no duplicate from re-running table copy ---
    assert_eq!(
        rows.len(),
        2,
        "expected original copied row plus one streamed insert, no duplicates"
    );

    let r = &rows[0];
    assert_eq!(r.id, 1);
    assert_eq!(r.value, "before_restart");
    assert_eq!(r.cdc_operation, "INSERT");
    assert_eq!(r.cdc_lsn, 0, "first row should be from table copy");

    let r = &rows[1];
    assert_eq!(r.id, 2);
    assert_eq!(r.value, "after_restart");
    assert_eq!(r.cdc_operation, "INSERT");
    assert!(
        r.cdc_lsn > 0,
        "second row should be from CDC streaming after restart"
    );
}
