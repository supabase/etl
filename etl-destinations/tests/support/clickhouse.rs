#![allow(dead_code)]
#![cfg(all(feature = "clickhouse", feature = "test-utils"))]

/// A row read back from the ClickHouse `all_types_encoding` test table.
///
/// Column-to-type mapping:
/// - `Date`          -> `u16`  (days since 1970-01-01 in RowBinary)
/// - `DateTime64(6)` -> `i64`  (microseconds since epoch in RowBinary)
/// - `UUID`          -> `String` (via `toString()` in the SELECT query)
/// - `Array(Nullable(T))` -> `Vec<Option<T>>`
///
/// Fields must match the SELECT column list in the test query exactly.
#[derive(clickhouse::Row, serde::Deserialize, Debug, Clone)]
pub struct AllTypesRow {
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
    pub date_col: u16,        // Date -> days since epoch
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
    pub cdc_operation: String,
}

/// A row read back from the ClickHouse `boundary_values` test table.
///
/// Covers edge cases that the `all_types` test does not: nullable scalars,
/// NULL array elements, empty strings, and multi-byte UTF-8.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
pub struct BoundaryValuesRow {
    pub id: i64,
    pub nullable_text: Option<String>,
    pub nullable_int: Option<i32>,
    pub int_array_col: Vec<Option<i32>>,
    pub text_array_col: Vec<Option<String>>,
    pub cdc_operation: String,
}
