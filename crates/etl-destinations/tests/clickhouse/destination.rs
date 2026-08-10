//! Direct integration tests for the ClickHouse destination.
//!
//! Each property generates typed [`Cell`] values, writes them through the
//! production destination path (schema DDL, `cell_to_clickhouse_value`,
//! RowBinary encoding, HTTP insert), reads them back from ClickHouse, and
//! asserts the stored value equals the written one. ClickHouse itself is the
//! storage oracle. Expected values are computed independently of the
//! production encoder where practical (hex for `bytea`, clock components for
//! `time`, `Date32` day offsets and raw microsecond ticks for temporals).
//! For `numeric` and `jsonb` the format pin shares the production
//! `to_string` path, so those properties additionally parse the stored text
//! back and compare values, failing on any rendering that changes
//! information.
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

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use etl::{
    data::{ArrayCell, Cell, PgNumeric, TableRow},
    destination::{DestinationTableMetadata, DestinationTableSchemaStatus},
    error::{ErrorKind, EtlError, EtlResult},
    event::{Event, RelationEvent},
    schema::{
        ColumnSchema, PgLsn, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId,
        TableName, TableSchema, Type,
    },
    store::{MemoryStore, SchemaStore, StateStore},
    test_utils::{
        notifying_store::NotifyingStore,
        property::{
            any_f32, any_f64, block_on, f32_matches, f64_matches, opt_f32_matches, opt_f64_matches,
            pg_text, pg_time, run_property,
        },
    },
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::{
    ClickHouseClientConfig, ClickHouseDestination,
    client::ClickHouseClient,
    test_utils::{
        ClickHouseTestDatabase, get_clickhouse_password, get_clickhouse_url, get_clickhouse_user,
        setup_clickhouse_database,
    },
};
use etl_telemetry::tracing::init_test_tracing;
use proptest::{option, prelude::*};
use url::Url;
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

/// Maps a generated optional value into a nullable cell.
fn opt_cell<T>(value: Option<T>, into_cell: impl Fn(T) -> Cell) -> Cell {
    value.map_or(Cell::Null, into_cell)
}

/// Converts a write error into a property failure.
fn write_failed(err: EtlError) -> TestCaseError {
    TestCaseError::fail(format!("destination write failed: {err}"))
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

/// Renders the text expected for a `time` value, computed from clock
/// components independently of the chrono `Display` path the production
/// encoder uses.
///
/// Mirrors the stored format's fraction rules: no fractional digits for
/// whole seconds, three digits for whole milliseconds, six otherwise.
fn expected_time_string(time: &NaiveTime) -> String {
    let micros = time.nanosecond() / 1_000;
    let base = format!("{:02}:{:02}:{:02}", time.hour(), time.minute(), time.second());
    if micros == 0 {
        base
    } else if micros.is_multiple_of(1_000) {
        format!("{base}.{:03}", micros / 1_000)
    } else {
        format!("{base}.{micros:06}")
    }
}

/// Asserts stored numeric text reparses to the exact value that was written.
///
/// The format pin shares the production `to_string` path, so it cannot see
/// rendering bugs; reparsing closes the loop ([`PgNumeric`] equality is
/// structural, and its `FromStr` is validated against Postgres by the codec
/// properties in `etl`).
fn assert_numeric_reparses(stored: &str, written: &PgNumeric) -> Result<(), TestCaseError> {
    let reparsed: PgNumeric = stored.parse().map_err(|err| {
        TestCaseError::fail(format!("stored numeric {stored:?} does not reparse: {err}"))
    })?;
    prop_assert_eq!(&reparsed, written, "stored numeric {} reparses to a different value", stored);
    Ok(())
}

/// Asserts stored json text parses back to the document that was written.
///
/// `serde_json`'s parser is an independent inverse of the production
/// rendering, so a `to_string` that changes a value fails here even though
/// the format pin cannot see it.
fn assert_json_parses_back(stored: &str, written: &serde_json::Value) -> Result<(), TestCaseError> {
    let parsed: serde_json::Value = serde_json::from_str(stored)
        .map_err(|err| TestCaseError::fail(format!("stored json does not parse: {err}")))?;
    prop_assert_eq!(&parsed, written, "stored json {} parses to a different document", stored);
    Ok(())
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

    let strategy = (option::of(any_f32()), any_f32(), option::of(any_f64()), any_f64());
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

        prop_assert!(opt_f32_matches(*v4, row.v4), "float4 {v4:?} stored as {:?}", row.v4);
        prop_assert!(f32_matches(*w4, row.w4), "float4 {w4:?} stored as {:?}", row.w4);
        prop_assert!(opt_f64_matches(*v8, row.v8), "float8 {v8:?} stored as {:?}", row.v8);
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
            prop_assert_eq!(&row.vtime, &vtime.as_ref().map(expected_time_string));
            prop_assert_eq!(&row.wtime, &expected_time_string(wtime));
            prop_assert_eq!(&row.vbytes, &vbytes.as_deref().map(expected_hex));
            prop_assert_eq!(&row.wbytes, &expected_hex(wbytes));

            // Value oracles for the columns whose format pins share
            // production code: the stored text must parse back to the value
            // that was written.
            if let (Some(stored), Some(written)) = (&row.vn, vn) {
                assert_numeric_reparses(stored, written)?;
            }
            assert_numeric_reparses(&row.wn, wn)?;
            if let (Some(stored), Some(written)) = (&row.vj, vj) {
                assert_json_parses_back(stored, written)?;
            }
            assert_json_parses_back(&row.wj, wj)?;
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
        proptest::collection::vec(option::of(any_f64()), 0..=8),
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
                opt_f64_matches(*expected, *stored),
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

/// # GIVEN
/// A ClickHouseClient pointed at the running test ClickHouse instance.
///
/// # WHEN
/// `validate_connectivity()` is called.
///
/// # THEN
/// It returns Ok(()).
#[tokio::test(flavor = "multi_thread")]
async fn validate_connectivity_succeeds_against_running_clickhouse() {
    let client = ClickHouseClient::new(
        get_clickhouse_url(),
        get_clickhouse_user(),
        get_clickhouse_password(),
        "default",
        ClickHouseClientConfig::default(),
    );
    assert!(client.validate_connectivity().await.is_ok());
}

/// # GIVEN
/// A ClickHouseClient pointed at a URL where nothing is listening.
///
/// # WHEN
/// `validate_connectivity()` is called.
///
/// # THEN
/// It returns Err.
#[tokio::test(flavor = "multi_thread")]
async fn validate_connectivity_fails_against_unreachable_clickhouse() {
    let client = ClickHouseClient::new(
        Url::parse("http://localhost:1").unwrap(),
        "nobody",
        None::<String>,
        "default",
        ClickHouseClientConfig::default(),
    );
    assert!(client.validate_connectivity().await.is_err());
}

/// Creates a synthetic composite snapshot ID for tests.
fn test_snapshot_id(commit_lsn: u64, message_lsn: u64) -> SnapshotId {
    SnapshotId::new(PgLsn::from(commit_lsn), PgLsn::from(message_lsn))
}

/// Stores one schema version whose `status` column has the supplied default.
async fn store_status_default_schema(
    store: &NotifyingStore,
    table_id: TableId,
    table_name: &TableName,
    snapshot_id: SnapshotId,
    default_expression: Option<&str>,
) -> ReplicatedTableSchema {
    let schema = store
        .store_table_schema(TableSchema::with_snapshot_id(
            table_id,
            table_name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, true)
                    .with_default_expression_option(default_expression.map(ToOwned::to_owned)),
            ],
            snapshot_id,
        ))
        .await
        .unwrap();

    ReplicatedTableSchema::all(schema)
}

/// Returns ClickHouse's stored `DEFAULT` expression for `column_name`.
async fn clickhouse_column_default_expression(
    database: &ClickHouseTestDatabase,
    table_name: &str,
    column_name: &str,
) -> Option<String> {
    database
        .db_client()
        .query(
            "select default_expression from system.columns where database = currentDatabase() and \
             table = ? and name = ? and default_kind = 'DEFAULT'",
        )
        .bind(table_name)
        .bind(column_name)
        .fetch_optional::<String>()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn existing_column_default_changes_drop_before_setting_supported_replacement() {
    init_test_tracing();
    install_crypto_provider();

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let table_id = TableId::new(4245);
    let table_name = TableName::new("public".to_owned(), "default_changes".to_owned());
    let initial_schema = store_status_default_schema(
        &store,
        table_id,
        &table_name,
        test_snapshot_id(100, 100),
        Some("lower('unsupported')"),
    )
    .await;
    let supported_schema = store_status_default_schema(
        &store,
        table_id,
        &table_name,
        test_snapshot_id(200, 200),
        Some("'queued'::text"),
    )
    .await;
    let unsupported_schema = store_status_default_schema(
        &store,
        table_id,
        &table_name,
        test_snapshot_id(300, 300),
        Some("lower('unsupported')"),
    )
    .await;
    let supported_again_schema = store_status_default_schema(
        &store,
        table_id,
        &table_name,
        test_snapshot_id(400, 400),
        Some("'done'::text"),
    )
    .await;
    let dropped_schema = store_status_default_schema(
        &store,
        table_id,
        &table_name,
        test_snapshot_id(500, 500),
        None,
    )
    .await;
    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
        .await;

    destination.write_table_rows(&initial_schema, vec![]).await.unwrap();
    let destination_table_name = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .unwrap()
        .destination_table_id;
    assert_eq!(
        clickhouse_column_default_expression(&clickhouse_db, &destination_table_name, "status")
            .await,
        None
    );

    for (schema, expected_default) in [
        (supported_schema, Some("'queued'")),
        (unsupported_schema, None),
        (supported_again_schema, Some("'done'")),
        (dropped_schema, None),
    ] {
        destination
            .write_events(vec![Event::Relation(RelationEvent { replicated_table_schema: schema })])
            .await
            .unwrap();
        assert_eq!(
            clickhouse_column_default_expression(&clickhouse_db, &destination_table_name, "status")
                .await
                .as_deref(),
            expected_default,
        );
    }
}

/// Retained row shape for interrupted publication-mask recovery.
#[derive(clickhouse::Row, serde::Deserialize, Debug, PartialEq, Eq)]
struct RecoveryMaskRow {
    id: i64,
    name: Option<String>,
}

/// Tests that interrupted schema-change recovery rejects a stale schema
/// snapshot instead of replaying DDL against it.
///
/// # GIVEN
///
/// Destination metadata in `Applying` state targeting snapshot 200 with
/// previous snapshot 100 (an interrupted schema change).
///
/// # WHEN
///
/// The recovery path runs with a schema carrying snapshot 100 -- a stale
/// replay arriving before the interrupted change's relation event.
///
/// # THEN
///
/// The write fails with `ErrorKind::DestinationSchemaRewind` instead of
/// diffing against the stale schema and wrongly marking the interrupted
/// change as applied.
#[tokio::test(flavor = "multi_thread")]
async fn schema_change_recovery_rejects_stale_snapshot_merge_tree() {
    init_test_tracing();
    install_crypto_provider();

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();

    let table_id = TableId::new(4242);
    let table_schema = Arc::new(TableSchema::with_snapshot_id(
        table_id,
        TableName::new("public".to_owned(), "stale_recovery".to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ],
        test_snapshot_id(100_u64, 100_u64),
    ));
    let replication_mask = ReplicationMask::all(&table_schema);
    let stale_schema =
        ReplicatedTableSchema::from_mask(Arc::clone(&table_schema), replication_mask.clone());

    // Interrupted schema change: metadata targets snapshot 200, previous 100.
    let metadata = DestinationTableMetadata::new_applied(
        "public_stale_recovery".to_owned(),
        test_snapshot_id(100_u64, 100_u64),
        replication_mask.clone(),
    )
    .with_schema_change(
        test_snapshot_id(200_u64, 200_u64),
        replication_mask,
        DestinationTableSchemaStatus::Applying,
    );
    store.store_destination_table_metadata(table_id, metadata).await.unwrap();

    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
        .await;

    let err = destination
        .write_events(vec![Event::Relation(RelationEvent {
            replicated_table_schema: stale_schema,
        })])
        .await
        .expect_err("recovery with a stale schema snapshot should be rejected");
    assert_eq!(err.kind(), ErrorKind::DestinationSchemaRewind);
}

/// Interrupted recovery rejects an equal snapshot with a different publication
/// mask instead of applying DDL for schema state other than the recorded
/// target.
#[tokio::test(flavor = "multi_thread")]
async fn schema_change_recovery_rejects_mismatched_mask_merge_tree() {
    init_test_tracing();
    install_crypto_provider();

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let table_id = TableId::new(4245);
    let table_schema = Arc::new(TableSchema::with_snapshot_id(
        table_id,
        TableName::new("public".to_owned(), "mask_recovery".to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ],
        test_snapshot_id(200_u64, 200_u64),
    ));
    let target_mask = ReplicationMask::all(&table_schema);
    let arriving_schema =
        ReplicatedTableSchema::from_mask(table_schema, ReplicationMask::from_bytes(vec![1, 0]));
    let metadata = DestinationTableMetadata::new_applied(
        "public_mask_recovery".to_owned(),
        test_snapshot_id(100_u64, 100_u64),
        target_mask.clone(),
    )
    .with_schema_change(
        test_snapshot_id(200_u64, 200_u64),
        target_mask,
        DestinationTableSchemaStatus::Applying,
    );
    store.store_destination_table_metadata(table_id, metadata).await.unwrap();

    let destination =
        clickhouse_db.build_destination_with_engine(store, ClickHouseEngine::MergeTree).await;
    let err = destination
        .write_events(vec![Event::Relation(RelationEvent {
            replicated_table_schema: arriving_schema,
        })])
        .await
        .expect_err("Recovery with a mismatched replication mask should be rejected");

    assert_eq!(err.kind(), ErrorKind::DestinationSchemaRewind);
}

/// Tests that interrupted schema-change recovery replays the diff and marks
/// the change applied when the arriving schema matches the recovery target.
///
/// # GIVEN
///
/// A destination table physically created at snapshot 100 (id, name) whose
/// metadata was then flipped to `Applying` targeting snapshot 200 (id, name,
/// email) with previous snapshot 100 -- the state a crash leaves behind after
/// `handle_relation_event` recorded the change but before the DDL completed.
///
/// # WHEN
///
/// A relation event arrives carrying the target snapshot 200 and its exact
/// replication mask.
///
/// # THEN
///
/// Recovery replays the interrupted diff (adds `email`), transitions the
/// metadata to `Applied` at snapshot 200, and the relation succeeds without a
/// synthetic DML event sequence key.
#[tokio::test(flavor = "multi_thread")]
async fn schema_change_recovery_replays_interrupted_diff_merge_tree() {
    init_test_tracing();
    install_crypto_provider();

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();

    let table_id = TableId::new(4243);
    let table_name = TableName::new("public".to_owned(), "recovery_replay".to_owned());
    let old_columns = vec![
        ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
        ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
    ];
    // Recovery loads the previous snapshot from the schema store, so the old
    // schema must be stored, not just passed to the write call.
    let old_table_schema = store
        .store_table_schema(TableSchema::with_snapshot_id(
            table_id,
            table_name.clone(),
            old_columns.clone(),
            test_snapshot_id(100_u64, 100_u64),
        ))
        .await
        .unwrap();
    let old_mask = ReplicationMask::all(&old_table_schema);
    let old_schema = ReplicatedTableSchema::from_mask(old_table_schema, old_mask.clone());

    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
        .await;

    // Create the physical table and `Applied` metadata at snapshot 100.
    destination.write_table_rows(&old_schema, vec![]).await.unwrap();

    let mut new_columns = old_columns;
    new_columns.push(ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true));
    let new_table_schema = Arc::new(TableSchema::with_snapshot_id(
        table_id,
        table_name,
        new_columns,
        test_snapshot_id(200_u64, 200_u64),
    ));
    let new_mask = ReplicationMask::all(&new_table_schema);
    let new_schema = ReplicatedTableSchema::from_mask(new_table_schema, new_mask.clone());

    // Simulate a crash after the change was recorded as `Applying` but before
    // the DDL completed.
    let applied_metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after table creation");
    let clickhouse_table_name = applied_metadata.destination_table_id.clone();
    let interrupted_metadata = DestinationTableMetadata::new_applied(
        clickhouse_table_name.clone(),
        test_snapshot_id(100_u64, 100_u64),
        old_mask,
    )
    .with_schema_change(
        test_snapshot_id(200_u64, 200_u64),
        new_mask,
        DestinationTableSchemaStatus::Applying,
    );
    store.store_destination_table_metadata(table_id, interrupted_metadata).await.unwrap();

    // A restarted destination (empty table cache, so metadata is consulted)
    // receiving the target snapshot must replay the interrupted diff.
    let restarted_destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
        .await;
    restarted_destination
        .write_events(vec![Event::Relation(RelationEvent { replicated_table_schema: new_schema })])
        .await
        .unwrap();

    let columns = clickhouse_db.column_names(&clickhouse_table_name).await;
    assert_eq!(columns, vec!["id", "name", "email"], "recovery must add the interrupted column");

    let recovered_metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should be applied after recovery");
    assert_eq!(
        recovered_metadata.snapshot_id,
        test_snapshot_id(200_u64, 200_u64),
        "recovery must mark the target snapshot applied"
    );
}

/// Tests that recovery removes a column excluded by an interrupted
/// publication-mask change.
#[tokio::test(flavor = "multi_thread")]
async fn schema_change_recovery_replays_interrupted_mask_contraction_merge_tree() {
    init_test_tracing();
    install_crypto_provider();

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();

    let table_id = TableId::new(4244);
    let table_name = TableName::new("public".to_owned(), "recovery_mask_contraction".to_owned());
    let columns = vec![
        ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
        ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ColumnSchema::new("hidden".to_owned(), Type::TEXT, -1, 3, true),
    ];
    let old_table_schema = store
        .store_table_schema(TableSchema::with_snapshot_id(
            table_id,
            table_name.clone(),
            columns.clone(),
            test_snapshot_id(100_u64, 100_u64),
        ))
        .await
        .unwrap();
    let old_mask = ReplicationMask::all(&old_table_schema);
    let old_schema = ReplicatedTableSchema::from_mask(old_table_schema, old_mask.clone());

    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
        .await;
    destination
        .write_table_rows(
            &old_schema,
            vec![TableRow::new(vec![
                Cell::I64(1),
                Cell::String("Alice".to_owned()),
                Cell::String("private".to_owned()),
            ])],
        )
        .await
        .unwrap();

    let target_table_schema = Arc::new(TableSchema::with_snapshot_id(
        table_id,
        table_name,
        columns,
        test_snapshot_id(200_u64, 200_u64),
    ));
    let target_mask = ReplicationMask::from_bytes(vec![1, 1, 0]);
    let target_schema =
        ReplicatedTableSchema::from_mask(Arc::clone(&target_table_schema), target_mask.clone());

    let applied_metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after table creation");
    let clickhouse_table_name = applied_metadata.destination_table_id.clone();
    let interrupted_metadata = DestinationTableMetadata::new_applied(
        clickhouse_table_name.clone(),
        test_snapshot_id(100_u64, 100_u64),
        old_mask,
    )
    .with_schema_change(
        target_table_schema.snapshot_id,
        target_mask.clone(),
        DestinationTableSchemaStatus::Applying,
    );
    store.store_destination_table_metadata(table_id, interrupted_metadata).await.unwrap();

    let restarted_destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
        .await;
    restarted_destination
        .write_events(vec![Event::Relation(RelationEvent {
            replicated_table_schema: target_schema,
        })])
        .await
        .unwrap();

    assert_eq!(clickhouse_db.column_names(&clickhouse_table_name).await, vec!["id", "name"]);
    let recovered_metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should be applied after recovery");
    assert_eq!(recovered_metadata.snapshot_id, target_table_schema.snapshot_id);
    assert_eq!(recovered_metadata.replication_mask, target_mask);

    let rows: Vec<RecoveryMaskRow> = clickhouse_db
        .query(&format!("SELECT id, name FROM \"{clickhouse_table_name}\" ORDER BY id"))
        .await;
    assert_eq!(rows, vec![RecoveryMaskRow { id: 1, name: Some("Alice".to_owned()) }]);
}
