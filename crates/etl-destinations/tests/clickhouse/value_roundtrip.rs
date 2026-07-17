//! Value roundtrip properties for the ClickHouse destination.
//!
//! Each property generates typed [`Cell`] values, writes them through the
//! production destination path (schema DDL, `cell_to_clickhouse_value`,
//! RowBinary encoding, HTTP insert), reads them back from ClickHouse, and
//! asserts the stored value equals the written one. ClickHouse itself is the
//! oracle, so these properties catch silent value corruption in the write
//! path, not just encoder panics.
//!
//! Every property runs new random cases until a wall-clock budget elapses,
//! using the shared runner in `etl::test_utils::property`. See that module
//! for the `PROPERTY_TEST_BUDGET_SECS` budget knob and the
//! `PROPERTY_TEST_SEED` failure replay knob.
//!
//! The generated envelope mirrors what the Postgres codec can produce (no NUL
//! bytes in text, microsecond temporal precision) and stays inside the ranges
//! the destination accepts, e.g. ClickHouse `Date32`'s
//! `1900-01-01..=2299-12-31`. Out-of-range values are covered separately by
//! the loud-rejection property.

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    data::{ArrayCell, Cell, PgNumeric, TableRow},
    error::EtlResult,
    schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type},
    store::{MemoryStore, SchemaStore},
    test_utils::property::run_property,
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::{
    ClickHouseDestination,
    test_utils::{ClickHouseTestDatabase, setup_clickhouse_database},
};
use etl_telemetry::tracing::init_test_tracing;
use proptest::{option, prelude::*};
use uuid::Uuid;

use crate::support::crypto::install_crypto_provider;

/// One ClickHouse table receiving generated rows through the production
/// destination write path.
///
/// The table always has a non-nullable `id` primary-key column so every case
/// can read back exactly the row it wrote.
struct PropertyTable {
    database: ClickHouseTestDatabase,
    destination: ClickHouseDestination<MemoryStore>,
    replicated_table_schema: ReplicatedTableSchema,
    clickhouse_table: String,
    next_id: AtomicI64,
}

impl PropertyTable {
    /// Creates an isolated database and a destination for one property table.
    ///
    /// `table` must not contain underscores so the ClickHouse table name stays
    /// the predictable `test_<table>`. `value_columns` are `(name, type,
    /// nullable)` triples appended after the `id` column.
    async fn create(table: &str, value_columns: &[(&str, Type, bool)]) -> Self {
        init_test_tracing();
        install_crypto_provider();
        assert!(!table.contains('_'), "table name would change the ClickHouse name mapping");

        let database = setup_clickhouse_database().await;

        let mut columns =
            vec![ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1)];
        for (index, (name, typ, nullable)) in value_columns.iter().enumerate() {
            let ordinal = i32::try_from(index + 2).expect("column ordinal fits in i32");
            columns.push(ColumnSchema::new(
                (*name).to_owned(),
                typ.clone(),
                -1,
                ordinal,
                *nullable,
            ));
        }
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("test".to_owned(), table.to_owned()),
            columns,
        );

        let store = MemoryStore::new();
        store.store_table_schema(table_schema.clone()).await.unwrap();
        let destination =
            database.build_destination_with_engine(store, ClickHouseEngine::MergeTree).await;

        Self {
            database,
            destination,
            replicated_table_schema: ReplicatedTableSchema::all(Arc::new(table_schema)),
            clickhouse_table: format!("test_{table}"),
            next_id: AtomicI64::new(1),
        }
    }

    /// Writes one row of `values` through the production path, returning the
    /// generated `id`.
    async fn write(&self, values: Vec<Cell>) -> EtlResult<i64> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut cells = vec![Cell::I64(id)];
        cells.extend(values);
        self.destination
            .write_table_rows(&self.replicated_table_schema, vec![TableRow::new(cells)])
            .await?;
        Ok(id)
    }

    /// Reads the row written under `id` back, selecting `projection`.
    async fn read<T>(&self, projection: &str, id: i64) -> T
    where
        T: for<'a> clickhouse::Row<Value<'a> = T> + serde::de::DeserializeOwned + 'static,
    {
        let sql = format!("select {projection} from {} where id = {id}", self.clickhouse_table);
        let mut rows = self.database.query::<T>(&sql).await;
        assert_eq!(rows.len(), 1, "expected exactly one row for id {id}");
        rows.remove(0)
    }
}

/// Runs an async future to completion from inside a synchronous proptest
/// closure.
///
/// Properties execute on a multi-threaded Tokio runtime worker, so blocking in
/// place is safe and keeps the HTTP client driven.
fn block_on<F: Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

/// Maps a generated optional value into a nullable cell.
fn opt_cell<T>(value: Option<T>, into_cell: impl Fn(T) -> Cell) -> Cell {
    value.map_or(Cell::Null, into_cell)
}

/// Converts a write error into a property failure.
fn write_failed(err: etl::error::EtlError) -> TestCaseError {
    TestCaseError::fail(format!("destination write failed: {err}"))
}

/// Bit-level float equality that treats every NaN as equal to every NaN.
///
/// RowBinary carries raw IEEE-754 bytes in both directions, so non-NaN values
/// (including signed zeros) must survive bit-exactly; NaN payloads are only
/// required to stay NaN.
fn f64_matches(expected: f64, parsed: f64) -> bool {
    if expected.is_nan() { parsed.is_nan() } else { expected.to_bits() == parsed.to_bits() }
}

/// See [`f64_matches`].
fn f32_matches(expected: f32, parsed: f32) -> bool {
    if expected.is_nan() { parsed.is_nan() } else { expected.to_bits() == parsed.to_bits() }
}

/// Optional-aware [`f64_matches`].
fn opt_f64_matches(expected: &Option<f64>, parsed: &Option<f64>) -> bool {
    match (expected, parsed) {
        (None, None) => true,
        (Some(expected), Some(parsed)) => f64_matches(*expected, *parsed),
        _ => false,
    }
}

/// Strings that are valid Postgres `text` values (no NUL bytes).
fn pg_text() -> impl Strategy<Value = String> {
    any::<String>().prop_filter("postgres text cannot contain NUL", |s| !s.contains('\0'))
}

/// Dates inside ClickHouse `Date32`'s supported range.
fn ch_date() -> impl Strategy<Value = NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let min = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
    let max = NaiveDate::from_ymd_opt(2299, 12, 31).unwrap();
    let min_days = min.signed_duration_since(epoch).num_days();
    let max_days = max.signed_duration_since(epoch).num_days();

    (min_days..=max_days).prop_map(move |days| epoch + chrono::Duration::days(days))
}

/// Times with microsecond precision, matching what the Postgres codec emits.
fn pg_time() -> impl Strategy<Value = NaiveTime> {
    (0i64..86_400_000_000).prop_map(|micros| {
        let seconds = u32::try_from(micros / 1_000_000).expect("seconds fit in u32");
        let nanos = u32::try_from(micros % 1_000_000).expect("micros fit in u32") * 1_000;
        NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos).expect("valid time")
    })
}

/// Timestamps inside ClickHouse `DateTime64(6)`'s supported range.
fn ch_timestamp() -> impl Strategy<Value = NaiveDateTime> {
    (ch_date(), pg_time()).prop_map(|(date, time)| NaiveDateTime::new(date, time))
}

/// Timezone-aware timestamps inside ClickHouse `DateTime64(6)`'s range.
fn ch_timestamptz() -> impl Strategy<Value = DateTime<Utc>> {
    ch_timestamp().prop_map(|naive| DateTime::from_naive_utc_and_offset(naive, Utc))
}

/// Reconstructs the date a `Date32` day offset stores.
fn date_from_days(days: i32) -> NaiveDate {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    epoch + chrono::Duration::days(i64::from(days))
}

/// Lowercase hex expected for a `bytea` value, computed independently of the
/// production encoder.
fn expected_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Valid Postgres numeric values built from generated digit strings.
fn pg_numeric() -> impl Strategy<Value = PgNumeric> {
    let digits = |max: usize| proptest::collection::vec(0u8..=9, 1..=max);

    (any::<bool>(), digits(38), option::of(digits(20)), option::of(-25i32..=25)).prop_map(
        |(negative, int_digits, frac_digits, exponent)| {
            let mut literal = String::new();
            if negative {
                literal.push('-');
            }
            for digit in int_digits {
                literal.push(char::from(b'0' + digit));
            }
            if let Some(frac_digits) = frac_digits {
                literal.push('.');
                for digit in frac_digits {
                    literal.push(char::from(b'0' + digit));
                }
            }
            if let Some(exponent) = exponent {
                literal.push_str(&format!("e{exponent}"));
            }
            literal.parse().expect("generated numeric literal is valid")
        },
    )
}

/// JSON documents with finite numbers, matching what `jsonb` can store.
fn json_value() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::from),
        any::<i64>().prop_map(serde_json::Value::from),
        any::<f64>()
            .prop_filter("json numbers are finite", |f| f.is_finite())
            .prop_map(serde_json::Value::from),
        pg_text().prop_map(serde_json::Value::from),
    ];
    leaf.prop_recursive(3, 24, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..=6).prop_map(serde_json::Value::from),
            proptest::collection::btree_map(pg_text(), inner, 0..=6)
                .prop_map(|map| serde_json::Value::Object(map.into_iter().collect())),
        ]
    })
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct IntegersRow {
    v16: Option<i16>,
    w16: i16,
    v32: Option<i32>,
    w32: i32,
    v64: Option<i64>,
    w64: i64,
    void: Option<u32>,
    woid: u32,
    vb: Option<bool>,
    wb: bool,
}

#[tokio::test(flavor = "multi_thread")]
async fn integer_and_bool_values_roundtrip_through_destination() {
    let table = PropertyTable::create(
        "propints",
        &[
            ("v16", Type::INT2, true),
            ("w16", Type::INT2, false),
            ("v32", Type::INT4, true),
            ("w32", Type::INT4, false),
            ("v64", Type::INT8, true),
            ("w64", Type::INT8, false),
            ("void", Type::OID, true),
            ("woid", Type::OID, false),
            ("vb", Type::BOOL, true),
            ("wb", Type::BOOL, false),
        ],
    )
    .await;

    let strategy = (
        (option::of(any::<i16>()), any::<i16>(), option::of(any::<i32>()), any::<i32>()),
        (option::of(any::<i64>()), any::<i64>(), option::of(any::<u32>()), any::<u32>()),
        (option::of(any::<bool>()), any::<bool>()),
    );
    run_property("clickhouse integer roundtrip", &strategy, |values| {
        let ((v16, w16, v32, w32), (v64, w64, void, woid), (vb, wb)) = values;
        let row: IntegersRow = block_on(async {
            let id = table
                .write(vec![
                    opt_cell(*v16, Cell::I16),
                    Cell::I16(*w16),
                    opt_cell(*v32, Cell::I32),
                    Cell::I32(*w32),
                    opt_cell(*v64, Cell::I64),
                    Cell::I64(*w64),
                    opt_cell(*void, Cell::U32),
                    Cell::U32(*woid),
                    opt_cell(*vb, Cell::Bool),
                    Cell::Bool(*wb),
                ])
                .await?;
            Ok(table.read("v16, w16, v32, w32, v64, w64, void, woid, vb, wb", id).await)
        })
        .map_err(write_failed)?;

        prop_assert_eq!(&row.v16, v16);
        prop_assert_eq!(row.w16, *w16);
        prop_assert_eq!(&row.v32, v32);
        prop_assert_eq!(row.w32, *w32);
        prop_assert_eq!(&row.v64, v64);
        prop_assert_eq!(row.w64, *w64);
        prop_assert_eq!(&row.void, void);
        prop_assert_eq!(row.woid, *woid);
        prop_assert_eq!(&row.vb, vb);
        prop_assert_eq!(row.wb, *wb);
        Ok(())
    });
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct FloatsRow {
    v4: Option<f32>,
    w4: f32,
    v8: Option<f64>,
    w8: f64,
}

#[tokio::test(flavor = "multi_thread")]
async fn float_values_roundtrip_through_destination() {
    let table = PropertyTable::create(
        "propfloats",
        &[
            ("v4", Type::FLOAT4, true),
            ("w4", Type::FLOAT4, false),
            ("v8", Type::FLOAT8, true),
            ("w8", Type::FLOAT8, false),
        ],
    )
    .await;

    let f32_bits = any::<u32>().prop_map(f32::from_bits);
    let f64_bits = any::<u64>().prop_map(f64::from_bits);
    let strategy = (option::of(f32_bits.clone()), f32_bits, option::of(f64_bits.clone()), f64_bits);
    run_property("clickhouse float roundtrip", &strategy, |(v4, w4, v8, w8)| {
        let row: FloatsRow = block_on(async {
            let id = table
                .write(vec![
                    opt_cell(*v4, Cell::F32),
                    Cell::F32(*w4),
                    opt_cell(*v8, Cell::F64),
                    Cell::F64(*w8),
                ])
                .await?;
            Ok(table.read("v4, w4, v8, w8", id).await)
        })
        .map_err(write_failed)?;

        let v4_matches = match (v4, &row.v4) {
            (None, None) => true,
            (Some(expected), Some(parsed)) => f32_matches(*expected, *parsed),
            _ => false,
        };
        prop_assert!(v4_matches, "float4 {v4:?} stored as {:?}", row.v4);
        prop_assert!(f32_matches(*w4, row.w4), "float4 {w4:?} stored as {:?}", row.w4);
        prop_assert!(opt_f64_matches(v8, &row.v8), "float8 {v8:?} stored as {:?}", row.v8);
        prop_assert!(f64_matches(*w8, row.w8), "float8 {w8:?} stored as {:?}", row.w8);
        Ok(())
    });
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TextRow {
    vt: Option<String>,
    wt: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn text_values_roundtrip_through_destination() {
    let table =
        PropertyTable::create("proptext", &[("vt", Type::TEXT, true), ("wt", Type::TEXT, false)])
            .await;

    let strategy = (option::of(pg_text()), pg_text());
    run_property("clickhouse text roundtrip", &strategy, |(vt, wt)| {
        let row: TextRow = block_on(async {
            let id = table
                .write(vec![opt_cell(vt.clone(), Cell::String), Cell::String(wt.clone())])
                .await?;
            Ok(table.read("vt, wt", id).await)
        })
        .map_err(write_failed)?;

        prop_assert_eq!(&row.vt, vt);
        prop_assert_eq!(&row.wt, wt);
        Ok(())
    });
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct StringMappedRow {
    vn: Option<String>,
    wn: String,
    vj: Option<String>,
    wj: String,
    vtime: Option<String>,
    wtime: String,
    vbytes: Option<String>,
    wbytes: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn string_mapped_values_roundtrip_through_destination() {
    let table = PropertyTable::create(
        "propstrmapped",
        &[
            ("vn", Type::NUMERIC, true),
            ("wn", Type::NUMERIC, false),
            ("vj", Type::JSONB, true),
            ("wj", Type::JSONB, false),
            ("vtime", Type::TIME, true),
            ("wtime", Type::TIME, false),
            ("vbytes", Type::BYTEA, true),
            ("wbytes", Type::BYTEA, false),
        ],
    )
    .await;

    let bytes = proptest::collection::vec(any::<u8>(), 0..=64);
    let strategy = (
        (option::of(pg_numeric()), pg_numeric()),
        (option::of(json_value()), json_value()),
        (option::of(pg_time()), pg_time()),
        (option::of(bytes.clone()), bytes),
    );
    run_property(
        "clickhouse string-mapped roundtrip",
        &strategy,
        |((vn, wn), (vj, wj), (vtime, wtime), (vbytes, wbytes))| {
            let row: StringMappedRow = block_on(async {
                let id = table
                    .write(vec![
                        opt_cell(vn.clone(), Cell::Numeric),
                        Cell::Numeric(wn.clone()),
                        opt_cell(vj.clone(), Cell::Json),
                        Cell::Json(wj.clone()),
                        opt_cell(*vtime, Cell::Time),
                        Cell::Time(*wtime),
                        opt_cell(vbytes.clone(), Cell::Bytes),
                        Cell::Bytes(wbytes.clone()),
                    ])
                    .await?;
                Ok(table.read("vn, wn, vj, wj, vtime, wtime, vbytes, wbytes", id).await)
            })
            .map_err(write_failed)?;

            prop_assert_eq!(&row.vn, &vn.as_ref().map(ToString::to_string));
            prop_assert_eq!(&row.wn, &wn.to_string());
            prop_assert_eq!(&row.vj, &vj.as_ref().map(ToString::to_string));
            prop_assert_eq!(&row.wj, &wj.to_string());
            prop_assert_eq!(&row.vtime, &vtime.map(|t| t.to_string()));
            prop_assert_eq!(&row.wtime, &wtime.to_string());
            prop_assert_eq!(&row.vbytes, &vbytes.as_deref().map(expected_hex));
            prop_assert_eq!(&row.wbytes, &expected_hex(wbytes));
            Ok(())
        },
    );
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TemporalsRow {
    vd: Option<i32>,
    wd: i32,
    vts: Option<i64>,
    wts: i64,
    vtstz: Option<i64>,
    wtstz: i64,
}

#[tokio::test(flavor = "multi_thread")]
async fn temporal_values_roundtrip_through_destination() {
    let table = PropertyTable::create(
        "proptemporal",
        &[
            ("vd", Type::DATE, true),
            ("wd", Type::DATE, false),
            ("vts", Type::TIMESTAMP, true),
            ("wts", Type::TIMESTAMP, false),
            ("vtstz", Type::TIMESTAMPTZ, true),
            ("wtstz", Type::TIMESTAMPTZ, false),
        ],
    )
    .await;

    let strategy = (
        (option::of(ch_date()), ch_date()),
        (option::of(ch_timestamp()), ch_timestamp()),
        (option::of(ch_timestamptz()), ch_timestamptz()),
    );
    run_property(
        "clickhouse temporal roundtrip",
        &strategy,
        |((vd, wd), (vts, wts), (vtstz, wtstz))| {
            let row: TemporalsRow = block_on(async {
                let id = table
                    .write(vec![
                        opt_cell(*vd, Cell::Date),
                        Cell::Date(*wd),
                        opt_cell(*vts, Cell::Timestamp),
                        Cell::Timestamp(*wts),
                        opt_cell(*vtstz, Cell::TimestampTz),
                        Cell::TimestampTz(*wtstz),
                    ])
                    .await?;
                Ok(table.read("vd, wd, vts, wts, vtstz, wtstz", id).await)
            })
            .map_err(write_failed)?;

            prop_assert_eq!(row.vd.map(date_from_days), *vd);
            prop_assert_eq!(date_from_days(row.wd), *wd);

            let stored_ts = row.vts.map(|micros| {
                DateTime::from_timestamp_micros(micros).expect("valid micros").naive_utc()
            });
            prop_assert_eq!(stored_ts, *vts);
            let stored_ts =
                DateTime::from_timestamp_micros(row.wts).expect("valid micros").naive_utc();
            prop_assert_eq!(stored_ts, *wts);

            let stored_tstz = row
                .vtstz
                .map(|micros| DateTime::from_timestamp_micros(micros).expect("valid micros"));
            prop_assert_eq!(stored_tstz, *vtstz);
            let stored_tstz = DateTime::from_timestamp_micros(row.wtstz).expect("valid micros");
            prop_assert_eq!(stored_tstz, *wtstz);
            Ok(())
        },
    );
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct UuidsRow {
    vu: Option<String>,
    wu: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn uuid_values_roundtrip_through_destination() {
    let table =
        PropertyTable::create("propuuid", &[("vu", Type::UUID, true), ("wu", Type::UUID, false)])
            .await;

    let uuid = any::<u128>().prop_map(Uuid::from_u128);
    let strategy = (option::of(uuid.clone()), uuid);
    run_property("clickhouse uuid roundtrip", &strategy, |(vu, wu)| {
        let row: UuidsRow = block_on(async {
            let id = table.write(vec![opt_cell(*vu, Cell::Uuid), Cell::Uuid(*wu)]).await?;
            Ok(table.read("toString(vu) as vu, toString(wu) as wu", id).await)
        })
        .map_err(write_failed)?;

        prop_assert_eq!(&row.vu, &vu.map(|u| u.to_string()));
        prop_assert_eq!(&row.wu, &wu.to_string());
        Ok(())
    });
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct ArraysRow {
    ai: Vec<Option<i64>>,
    at: Vec<Option<String>>,
    af: Vec<Option<f64>>,
    ab: Vec<Option<String>>,
    ad: Vec<Option<i32>>,
}

#[tokio::test(flavor = "multi_thread")]
async fn array_values_roundtrip_through_destination() {
    let table = PropertyTable::create(
        "proparrays",
        &[
            ("ai", Type::INT8_ARRAY, true),
            ("at", Type::TEXT_ARRAY, true),
            ("af", Type::FLOAT8_ARRAY, true),
            ("ab", Type::BYTEA_ARRAY, true),
            ("ad", Type::DATE_ARRAY, true),
        ],
    )
    .await;

    // The int8[] lengths cross the LEB128 single-byte boundary at 128 so
    // multi-byte varint array lengths are exercised; the other arrays stay
    // short to keep per-case cost down. Every element is Nullable on the
    // ClickHouse side, so None elements exercise the per-element null byte.
    let bytes = proptest::collection::vec(any::<u8>(), 0..=16);
    let strategy = (
        proptest::collection::vec(option::of(any::<i64>()), 0..=160),
        proptest::collection::vec(option::of(pg_text()), 0..=8),
        proptest::collection::vec(option::of(any::<u64>().prop_map(f64::from_bits)), 0..=8),
        proptest::collection::vec(option::of(bytes), 0..=8),
        proptest::collection::vec(option::of(ch_date()), 0..=8),
    );
    run_property("clickhouse array roundtrip", &strategy, |(ai, at, af, ab, ad)| {
        let row: ArraysRow = block_on(async {
            let id = table
                .write(vec![
                    Cell::Array(ArrayCell::I64(ai.clone())),
                    Cell::Array(ArrayCell::String(at.clone())),
                    Cell::Array(ArrayCell::F64(af.clone())),
                    Cell::Array(ArrayCell::Bytes(ab.clone())),
                    Cell::Array(ArrayCell::Date(ad.clone())),
                ])
                .await?;
            Ok(table.read("ai, at, af, ab, ad", id).await)
        })
        .map_err(write_failed)?;

        prop_assert_eq!(&row.ai, ai);
        prop_assert_eq!(&row.at, at);
        prop_assert_eq!(row.af.len(), af.len());
        for (expected, stored) in af.iter().zip(&row.af) {
            prop_assert!(
                opt_f64_matches(expected, stored),
                "float8[] {:?} stored as {:?}",
                expected,
                stored
            );
        }
        let expected_ab: Vec<Option<String>> =
            ab.iter().map(|element| element.as_deref().map(expected_hex)).collect();
        prop_assert_eq!(&row.ab, &expected_ab);
        let stored_ad: Vec<Option<NaiveDate>> =
            row.ad.iter().map(|element| element.map(date_from_days)).collect();
        prop_assert_eq!(&stored_ad, ad);
        Ok(())
    });
}

/// Dates legal in Postgres but outside ClickHouse `Date32`'s
/// `1900-01-01..=2299-12-31` range.
fn out_of_range_date() -> impl Strategy<Value = NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = |date: NaiveDate| date.signed_duration_since(epoch).num_days();

    let low_min = days(NaiveDate::from_ymd_opt(1, 1, 1).unwrap());
    let low_max = days(NaiveDate::from_ymd_opt(1899, 12, 31).unwrap());
    let high_min = days(NaiveDate::from_ymd_opt(2300, 1, 1).unwrap());
    let high_max = days(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap());

    prop_oneof![low_min..=low_max, high_min..=high_max]
        .prop_map(move |days| epoch + chrono::Duration::days(days))
}

#[tokio::test(flavor = "multi_thread")]
async fn out_of_range_dates_are_rejected_loudly() {
    let table = PropertyTable::create("propdatereject", &[("vd", Type::DATE, true)]).await;

    run_property("clickhouse date rejection", &out_of_range_date(), |date| {
        let result = block_on(table.write(vec![Cell::Date(*date)]));
        prop_assert!(result.is_err(), "date {} is outside Date32 but the write succeeded", date);
        Ok(())
    });
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TimestampRejectRow {
    vts: Option<i64>,
    vtstz: Option<i64>,
}

/// Out-of-range writes must never silently change values: either the write
/// fails loudly or the stored value reads back equal to what was written.
///
/// Unlike dates, timestamps outside `DateTime64(6)`'s documented
/// `1900..=2299` range have no local range check. Empirically ClickHouse
/// accepts the raw microsecond ticks and reads them back bit-exact, so the
/// values survive storage unchanged; this property pins that behavior and
/// fails if either side ever starts mutating such values silently.
#[tokio::test(flavor = "multi_thread")]
async fn out_of_range_timestamps_are_rejected_or_roundtrip() {
    let table = PropertyTable::create(
        "proptsreject",
        &[("vts", Type::TIMESTAMP, true), ("vtstz", Type::TIMESTAMPTZ, true)],
    )
    .await;

    let out_of_range_timestamp =
        || (out_of_range_date(), pg_time()).prop_map(|(date, time)| NaiveDateTime::new(date, time));
    let strategy = (
        out_of_range_timestamp(),
        out_of_range_timestamp().prop_map(|naive| DateTime::from_naive_utc_and_offset(naive, Utc)),
    );
    run_property("clickhouse timestamp rejection", &strategy, |(ts, tstz)| {
        // Loud rejection is a valid outcome.
        let Ok(id) = block_on(table.write(vec![Cell::Timestamp(*ts), Cell::TimestampTz(*tstz)]))
        else {
            return Ok(());
        };

        let row: TimestampRejectRow = block_on(table.read("vts, vtstz", id));
        let stored_ts = row.vts.map(|micros| {
            DateTime::from_timestamp_micros(micros).expect("valid micros").naive_utc()
        });
        prop_assert_eq!(
            stored_ts,
            Some(*ts),
            "timestamp {} was accepted but stored differently",
            ts
        );
        let stored_tstz =
            row.vtstz.map(|micros| DateTime::from_timestamp_micros(micros).expect("valid micros"));
        prop_assert_eq!(
            stored_tstz,
            Some(*tstz),
            "timestamptz {} was accepted but stored differently",
            tstz
        );
        Ok(())
    });
}
