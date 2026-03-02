#![allow(dead_code)]
#![cfg(all(feature = "clickhouse", feature = "test-utils"))]

/// A row read back from the ClickHouse `all_types_encoding` test table.
///
/// Column-to-type mapping:
/// - `Date`          → `u16`  (days since 1970-01-01 in RowBinary)
/// - `DateTime64(6)` → `i64`  (microseconds since epoch in RowBinary)
/// - `UUID`          → `String` (via `toString()` in the SELECT query)
/// - `Array(Nullable(T))` → `Vec<Option<T>>`
///
/// Fields must match the SELECT column list in the test query exactly.
#[derive(clickhouse::Row, serde::Deserialize, Debug, Clone)]
pub struct AllTypesRow {
    pub id: i32,
    pub smallint_col: i16,
    pub integer_col: i32,
    pub bigint_col: i64,
    pub real_col: f32,
    pub double_col: f64,
    pub numeric_col: String,
    pub boolean_col: bool,
    pub text_col: String,
    pub varchar_col: String,
    pub date_col: u16,        // Date → days since epoch
    pub timestamp_col: i64,   // DateTime64(6) → microseconds
    pub timestamptz_col: i64, // DateTime64(6,'UTC') → microseconds
    pub time_col: String,
    pub interval_col: String,
    pub jsonb_col: String,
    pub json_col: String,
    pub integer_array_col: Vec<Option<i32>>,
    pub text_array_col: Vec<Option<String>>,
    pub bytea_col: String,    // hex-encoded
    pub inet_col: String,
    pub cidr_col: String,
    pub macaddr_col: String,
    pub uuid_col: String,     // via toString() in SELECT
    pub cdc_operation: String,
}

impl PartialEq for AllTypesRow {
    fn eq(&self, other: &Self) -> bool {
        fn f32_eq(a: f32, b: f32) -> bool {
            (a - b).abs() < 1e-3
        }
        fn f64_eq(a: f64, b: f64) -> bool {
            (a - b).abs() < 1e-6
        }

        self.id == other.id
            && self.smallint_col == other.smallint_col
            && self.integer_col == other.integer_col
            && self.bigint_col == other.bigint_col
            && f32_eq(self.real_col, other.real_col)
            && f64_eq(self.double_col, other.double_col)
            && self.numeric_col == other.numeric_col
            && self.boolean_col == other.boolean_col
            && self.text_col == other.text_col
            && self.varchar_col == other.varchar_col
            && self.date_col == other.date_col
            && self.timestamp_col == other.timestamp_col
            && self.timestamptz_col == other.timestamptz_col
            && self.time_col == other.time_col
            && self.jsonb_col == other.jsonb_col
            && self.json_col == other.json_col
            && self.integer_array_col == other.integer_array_col
            && self.text_array_col == other.text_array_col
            && self.bytea_col == other.bytea_col
            && self.inet_col == other.inet_col
            && self.cidr_col == other.cidr_col
            && self.macaddr_col == other.macaddr_col
            && self.uuid_col.to_lowercase() == other.uuid_col.to_lowercase()
            && self.cdc_operation == other.cdc_operation
    }
}

impl Eq for AllTypesRow {}
