#![cfg(all(feature = "clickhouse", feature = "test-utils"))]

use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notifying_store::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::types::PipelineId;
use etl_destinations::clickhouse::test_utils::{
    setup_clickhouse_database, skip_if_missing_clickhouse_env_vars,
};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use std::sync::Once;

use crate::support::clickhouse::AllTypesRow;

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

/// ClickHouse table name for `test.all_types_encoding`.
///
/// Derived from `table_name_to_clickhouse_table_name("test", "all_types_encoding")`:
/// - "test"                → "test"  (no underscores)
/// - "all_types_encoding"  → "all__types__encoding"  (underscores escaped to __)
const ALL_TYPES_CH_TABLE: &str = "test_all__types__encoding";

/// SELECT query that fetches all verified columns from the ClickHouse table.
///
/// `uuid_col` is projected via `toString()` because the ClickHouse UUID RowBinary
/// wire format does not directly map to a Rust `String`; `toString()` gives us the
/// canonical `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` string form.
///
/// All other columns are read with their native ClickHouse types:
/// - `Date`          → u16  (days since 1970-01-01)
/// - `DateTime64(6)` → i64  (microseconds since epoch)
/// - `Array(Nullable(T))` → `Vec<Option<T>>`
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

/// Days from 1970-01-01 to 2024-01-15 (used to verify the `date_col` round-trip).
///
/// Python: `(date(2024, 1, 15) - date(1970, 1, 1)).days` = 19737
const DATE_2024_01_15_DAYS: u16 = 19737;

/// Microseconds from epoch for `2024-01-15 12:00:00 UTC`.
///
/// Python: `int(datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000)`
/// = 1705320000000000
const TS_2024_01_15_12_00_US: i64 = 1_705_320_000_000_000;

/// Tests that all Postgres column types (including nullable arrays) round-trip
/// correctly through the ClickHouse RowBinary encoding.
///
/// # Regression test
///
/// This test specifically catches the nullable-array encoding bug where
/// `nullable_flags[i] = true` for array columns caused `rb_encode_nullable` to
/// prepend an extra null-indicator byte. ClickHouse read that byte as `varint(0)`
/// (empty array) and then parsed the actual element bytes as subsequent column
/// data, ultimately failing with "Cannot read all data" at row 2.
///
/// The fix: array columns always use `nullable_flags[i] = false` because the DDL
/// emits `Array(Nullable(T))` without an outer `Nullable` wrapper.
///
/// Row 1 has **empty** arrays (accidentally passed with the old code because
/// `0x00` null-indicator == `varint(0)` = empty array).
/// Row 2 has **non-empty** arrays (fails with the old code, passes with the fix).
#[tokio::test(flavor = "multi_thread")]
async fn all_types_table_copy() {
    if skip_if_missing_clickhouse_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    // ── Postgres source ───────────────────────────────────────────────────────
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

    // Insert rows BEFORE starting the pipeline — they will be captured by the
    // initial table-copy phase (write_table_rows path).
    //
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

    // ── ClickHouse destination ────────────────────────────────────────────────
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

    // ── Verify ClickHouse data ────────────────────────────────────────────────
    let rows: Vec<AllTypesRow> = ch_db.query(ALL_TYPES_SELECT).await;

    assert_eq!(rows.len(), 2, "expected 2 rows in ClickHouse");

    // ── Row 1 assertions ─────────────────────────────────────────────────────
    let r1 = &rows[0];
    assert_eq!(r1.id, 1);
    assert_eq!(r1.smallint_col, 42);
    assert_eq!(r1.integer_col, 1000);
    assert_eq!(r1.bigint_col, 9_999_999);
    assert!((r1.real_col - 1.5_f32).abs() < 1e-3, "real_col mismatch");
    assert!((r1.double_col - 2.5_f64).abs() < 1e-6, "double_col mismatch");
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
    // Empty arrays — the regression case that accidentally worked before the fix.
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

    // ── Row 2 assertions ─────────────────────────────────────────────────────
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
    // Non-empty arrays — the regression case that triggered the bug before the fix.
    assert_eq!(
        r2.integer_array_col,
        vec![Some(1), Some(2), Some(3)],
        "row 2 integer_array_col mismatch — nullable-array encoding bug likely present"
    );
    assert_eq!(
        r2.text_array_col,
        vec![Some("alpha".to_string()), Some("beta".to_string())],
        "row 2 text_array_col mismatch — nullable-array encoding bug likely present"
    );
}
