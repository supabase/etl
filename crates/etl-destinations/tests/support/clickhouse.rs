#![allow(dead_code)]

use std::sync::Once;

use etl_config::shared::ClickHouseEngine;

/// Installs the rustls default crypto provider once per process. Subsequent
/// `install_default()` calls return `Err` (already installed); we discard
/// that error so multiple test files can call this safely.
pub(crate) fn install_crypto_provider() {
    static INIT_CRYPTO: Once = Once::new();
    INIT_CRYPTO.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// A row read back from the ClickHouse `all_types_encoding` test table.
///
/// Column-to-type mapping:
/// - `Date32`        -> `i32`  (signed days from 1970-01-01 in RowBinary)
/// - `DateTime64(6)` -> `i64`  (microseconds since epoch in RowBinary)
/// - `UUID`          -> `String` (via `toString()` in the SELECT query)
/// - `Array(Nullable(T))` -> `Vec<Option<T>>`
///
/// Fields must match the SELECT column list in the test query exactly.
#[derive(clickhouse::Row, serde::Deserialize, Debug, Clone)]
pub(crate) struct AllTypesRow {
    pub id: i64,
    pub smallint_col: i16,
    pub integer_col: i32,
    pub bigint_col: i64,
    pub real_col: f32,
    pub double_col: f64,
    pub numeric_col: String,
    pub boolean_col: bool,
    pub text_col: String,
    pub varchar_col: String,
    pub date_col: i32,        // Date32 -> signed days from 1970-01-01
    pub timestamp_col: i64,   // DateTime64(6) -> microseconds
    pub timestamptz_col: i64, // DateTime64(6,'UTC') -> microseconds
    pub time_col: String,
    pub interval_col: String,
    pub jsonb_col: String,
    pub json_col: String,
    pub integer_array_col: Vec<Option<i32>>,
    pub text_array_col: Vec<Option<String>>,
    pub bytea_col: String, // hex-encoded
    pub inet_col: String,
    pub cidr_col: String,
    pub macaddr_col: String,
    pub uuid_col: String, // via toString() in SELECT
}

/// A row read back from the ClickHouse `boundary_values` test table.
///
/// Covers edge cases that the `all_types` test does not: nullable scalars,
/// NULL array elements, empty strings, and multi-byte UTF-8.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
pub(crate) struct BoundaryValuesRow {
    pub id: i64,
    pub nullable_text: Option<String>,
    pub nullable_int: Option<i32>,
    pub int_array_col: Vec<Option<i32>>,
    pub text_array_col: Vec<Option<String>>,
}

/// A row read back from a ClickHouse table with a single `Date32` column,
/// used to verify Postgres `date` round-tripping for values outside the Unix
/// epoch (pre-1970 and far-future). The `date_col` is the signed day offset
/// from 1970-01-01.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
pub(crate) struct DateBoundariesRow {
    pub id: i64,
    pub date_col: i32,
}

/// Builds an engine-aware "current state" SELECT for a replicated table.
///
/// The projection is supplied by the caller (so per-column SQL like
/// `toString(uuid_col) AS uuid_col` keeps working). The helper handles the
/// engine-specific dedup + tombstone filter and applies the caller's
/// `ORDER BY` for deterministic test reads.
///
/// MergeTree path: take the latest event per PK with `LIMIT 1 BY`, then drop
/// any whose latest event is a DELETE. The drop-DELETE filter must come
/// AFTER the dedup, otherwise a deleted PK whose latest event is a DELETE
/// would surface its prior INSERT instead of being absent.
///
/// ReplacingMergeTree path: `FINAL` + `_etl_deleted = 0`.
pub(crate) fn current_state_query(
    engine: ClickHouseEngine,
    table: &str,
    projection: &str,
    pk_cols: &[&str],
    order_by: &str,
) -> String {
    match engine {
        ClickHouseEngine::MergeTree => format!(
            "SELECT {projection} FROM (SELECT * FROM \"{table}\" ORDER BY cdc_lsn DESC LIMIT 1 BY \
             ({pks})) AS current WHERE cdc_operation != 'DELETE' ORDER BY {order_by}",
            pks = pk_cols.join(", ")
        ),
        ClickHouseEngine::ReplacingMergeTree => format!(
            "SELECT {projection} FROM \"{table}\" FINAL WHERE _etl_deleted = 0 ORDER BY {order_by}"
        ),
    }
}

/// SQL to force a `ReplacingMergeTree` table to drop tombstoned rows. The
/// `SETTINGS` clause enables the (still experimental, as of CH 25.x) merges-
/// with-cleanup feature for this query only, without requiring it to be
/// enabled server-wide.
pub(crate) fn optimize_final_cleanup_sql(table: &str) -> String {
    format!(
        "OPTIMIZE TABLE \"{table}\" FINAL CLEANUP SETTINGS \
         allow_experimental_replacing_merge_tree_with_cleanup = 1"
    )
}

/// SQL to read the `engine` column from `system.tables` for a table.
pub(crate) fn table_engine_query(table: &str) -> String {
    format!(
        "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = '{table}'"
    )
}
