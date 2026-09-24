//! Direct integration tests for the ClickHouse destination.
//!
//! Each property generates typed [`Cell`] values, writes them through the
//! production destination path (schema DDL, `cell_to_clickhouse_value`,
//! RowBinary encoding, HTTP insert), reads them back from ClickHouse, and
//! asserts the stored value equals the written one. ClickHouse itself is the
//! storage oracle. Expected values are computed independently of the production
//! encoder where practical (hex for `bytea`, clock components for `time`,
//! `Date32` day offsets and raw microsecond ticks for temporals). For `numeric`
//! and `jsonb` the format pin shares the production `to_string` path, so those
//! properties additionally parse the stored text back and compare values,
//! failing on any rendering that changes information.
//!
//! Every property runs new random cases until a wall-clock budget elapses,
//! using the shared runner in `etl::test_utils::property`. See that module for
//! the `PROPERTY_TEST_BUDGET_SECS` budget knob and the `PROPERTY_TEST_SEED`
//! failure replay knob.
//!
//! The generated envelope mirrors what the Postgres codec can produce (no NUL
//! bytes in text, microsecond temporal precision) and stays inside the ranges
//! the destination accepts, e.g. ClickHouse `Date32`'s
//! `1900-01-01..=2299-12-31`. Out-of-range values are covered separately by the
//! loud-rejection property.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use etl::{
    data::{
        ArrayCell, Cell, Date, OldTableRow, PgNumeric, PgTime, TableRow, Timestamp, UpdatedTableRow,
    },
    destination::{
        Destination, DestinationTableMetadata, DestinationWriteStatus, DropTableForCopyResult,
        TableCopyBatchId, WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlError, EtlResult},
    event::{Event, InsertEvent, RelationEvent, TruncateEvent, UpdateEvent},
    schema::{
        ColumnSchema, PgLsn, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId,
        TableName, TableSchema, Type,
    },
    store::{MemoryStore, SchemaStore, StateStore},
    test_utils::{
        destination::{
            drop_table_for_copy as drop_table_for_copy_via_trait,
            write_events as write_events_via_trait,
        },
        notifying_store::NotifyingStore,
        property::{
            any_f32, any_f64, block_on, f32_matches, f64_matches, opt_f32_matches, opt_f64_matches,
            pg_text, pg_time, run_property,
        },
    },
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::{
    ClickHouseClientConfig, ClickHouseDestination, ClickHouseInserterConfig,
    arm_fail_drop_table_for_copy_once_for_tests,
    client::{ClickHouseClient, arm_pause_before_insert_statement_for_tests},
    test_utils::{
        ClickHouseTestDatabase, get_clickhouse_password, get_clickhouse_url, get_clickhouse_user,
        setup_clickhouse_database,
    },
};
use etl_telemetry::tracing::init_test_tracing;
use parking_lot::Mutex;
use proptest::{option, prelude::*};
use tokio::sync::oneshot;
use url::Url;
use uuid::Uuid;

use crate::support::{clickhouse::current_state_query, crypto::install_crypto_provider};

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

/// Renders the text expected for a `time` value, computed from clock components
/// independently of the chrono `Display` path the production encoder uses.
///
/// Mirrors the stored format's fraction rules: no fractional digits for whole
/// seconds, three digits for whole milliseconds, six otherwise.
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
/// `serde_json`'s parser is an independent inverse of the production rendering,
/// so a `to_string` that changes a value fails here even though the format pin
/// cannot see it.
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
                        opt_cell(*vtime, |value| Cell::Time(PgTime::Value(value))),
                        Cell::Time(PgTime::Value(*wtime)),
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

            // Value oracles for the columns whose format pins share production
            // code: the stored text must parse back to the value that was
            // written.
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
                        opt_cell(*vd, |value| Cell::Date(Date::Value(value))),
                        Cell::Date(Date::Value(*wd)),
                        opt_cell(*vts, |value| Cell::Timestamp(Timestamp::Value(value))),
                        Cell::Timestamp(Timestamp::Value(*wts)),
                        opt_cell(*vtstz, |value| Cell::TimestampTz(Timestamp::Value(value))),
                        Cell::TimestampTz(Timestamp::Value(*wtstz)),
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
            ("ai", Type::INT8_ARRAY, false),
            ("at", Type::TEXT_ARRAY, false),
            ("af", Type::FLOAT8_ARRAY, false),
            ("ab", Type::BYTEA_ARRAY, false),
            ("ad", Type::DATE_ARRAY, false),
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
                    Cell::Array(ArrayCell::Date(
                        ad.iter().map(|value| value.map(Date::Value)).collect(),
                    )),
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

#[tokio::test(flavor = "multi_thread")]
async fn nullable_array_columns_fail_only_for_top_level_null_values() {
    let table = PropertyTable::create("nullablearray", &[("values", Type::INT8_ARRAY, true)]).await;

    table.write(vec![Cell::Array(ArrayCell::I64(vec![]))]).await.unwrap();
    let error = table.write(vec![Cell::Null]).await.unwrap_err();

    assert_eq!(error.kind(), ErrorKind::ConversionError);
    assert_eq!(error.description(), Some("NULL value for non-nullable ClickHouse column"));
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

#[derive(clickhouse::Row, serde::Deserialize)]
struct TimestampRejectRow {
    vts: Option<i64>,
    vtstz: Option<i64>,
}

/// Out-of-range writes must never silently change values: either the write
/// fails loudly or the stored value reads back equal to what was written.
///
/// Unlike dates, timestamps outside `DateTime64(6)`'s documented `1900..=2299`
/// range have no local range check. Empirically ClickHouse accepts the raw
/// microsecond ticks and reads them back bit-exact, so the values survive
/// storage unchanged; this property pins that behavior and fails if either side
/// ever starts mutating such values silently.
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
        let Ok(id) = block_on(table.write(vec![
            Cell::Timestamp(Timestamp::Value(*ts)),
            Cell::TimestampTz(Timestamp::Value(*tstz)),
        ])) else {
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

/// Stores the source schema shared by upgrade and replay scenarios.
async fn store_id_value_schema(store: &MemoryStore, table: &str) -> ReplicatedTableSchema {
    let schema = store
        .store_table_schema(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), table.to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 2, false),
            ],
        ))
        .await
        .unwrap();
    ReplicatedTableSchema::all(schema)
}

/// Legacy MergeTree writes fail without repair or data loss, then resume after
/// the documented manual upgrade while preserving existing metadata.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_merge_tree_resumes_only_after_manual_ordinal_upgrade() {
    // GIVEN: a legacy MergeTree table with retained rows and metadata.
    init_test_tracing();
    install_crypto_provider();
    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();
    let schema = store_id_value_schema(&store, "upgrade").await;
    let metadata = DestinationTableMetadata::new_applied(
        "retained_rows".to_owned(),
        schema.inner().snapshot_id,
        schema.replication_mask().clone(),
    );
    store.store_destination_table_metadata(schema.id(), metadata.clone()).await.unwrap();

    // Use the previous release's DDL, not the current schema generator.
    database
        .db_client()
        .query(
            "create table retained_rows (id Int64, value String, cdc_operation String, cdc_lsn \
             UInt64) engine = MergeTree() order by tuple()",
        )
        .execute()
        .await
        .unwrap();
    database
        .db_client()
        .query("insert into retained_rows values (1, 'retained', 'INSERT', 10)")
        .execute()
        .await
        .unwrap();

    // WHEN: a new destination attempts to write without upgrading the table.
    let destination =
        database.build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree).await;
    let error = destination
        .write_table_rows(
            &schema,
            vec![TableRow::new(vec![Cell::I64(2), Cell::String("rejected".to_owned())])],
        )
        .await
        .unwrap_err();

    // THEN: the write fails without changing rows, columns, or metadata.
    assert_eq!(error.kind(), ErrorKind::CorruptedTableSchema);
    drop(destination);

    assert_eq!(
        database.query::<(i64, String)>("select id, value from retained_rows").await,
        vec![(1, "retained".to_owned())]
    );
    assert_eq!(
        database
            .query::<String>(
                "select name from system.columns where database = currentDatabase() and table = \
                 'retained_rows' order by position",
            )
            .await,
        vec!["id", "value", "cdc_operation", "cdc_lsn"]
    );
    assert_eq!(
        store.get_destination_table_metadata(schema.id()).await.unwrap(),
        Some(metadata.clone())
    );

    // WHEN: the ordinal column is added and the destination restarts.
    database
        .db_client()
        .query("alter table retained_rows add column cdc_tx_ordinal UInt64 default 0 after cdc_lsn")
        .execute()
        .await
        .unwrap();
    let restarted =
        database.build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree).await;
    restarted
        .write_events(vec![Event::Insert(InsertEvent {
            commit_lsn: PgLsn::from(100),
            tx_ordinal: 7,
            replicated_table_schema: schema.clone(),
            table_row: TableRow::new(vec![Cell::I64(2), Cell::String("resumed".to_owned())]),
        })])
        .await
        .unwrap();
    drop(restarted);

    // THEN: old rows keep ordinal zero and new rows use their event ordinal.
    assert_eq!(
        database
            .query::<(i64, String, u64, u64)>(
                "select id, value, cdc_lsn, cdc_tx_ordinal from retained_rows order by id",
            )
            .await,
        vec![(1, "retained".to_owned(), 10, 0), (2, "resumed".to_owned(), 100, 7)]
    );
    assert_eq!(store.get_destination_table_metadata(schema.id()).await.unwrap(), Some(metadata));
}

/// A live schema change on a legacy MergeTree table is rejected before any
/// ALTER runs or metadata advances, mirroring the DML and recovery paths.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_merge_tree_rejects_schema_change_before_altering() {
    // GIVEN: a legacy MergeTree table with retained rows and applied metadata.
    init_test_tracing();
    install_crypto_provider();
    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();
    let schema = store_id_value_schema(&store, "upgrade").await;
    let metadata = DestinationTableMetadata::new_applied(
        "retained_rows".to_owned(),
        schema.inner().snapshot_id,
        schema.replication_mask().clone(),
    );
    store.store_destination_table_metadata(schema.id(), metadata.clone()).await.unwrap();

    // Use the previous release's DDL, not the current schema generator.
    database
        .db_client()
        .query(
            "create table retained_rows (id Int64, value String, cdc_operation String, cdc_lsn \
             UInt64) engine = MergeTree() order by tuple()",
        )
        .execute()
        .await
        .unwrap();
    database
        .db_client()
        .query("insert into retained_rows values (1, 'retained', 'INSERT', 10)")
        .execute()
        .await
        .unwrap();

    // WHEN: a newer source snapshot adds a column before any row is written.
    let new_schema = store
        .store_table_schema(TableSchema::with_snapshot_id(
            schema.id(),
            schema.name().clone(),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("note".to_owned(), Type::TEXT, -1, 3, true),
            ],
            test_snapshot_id(1, 1),
        ))
        .await
        .unwrap();
    let destination =
        database.build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree).await;
    let error = destination
        .write_events(vec![Event::Relation(RelationEvent {
            replicated_table_schema: ReplicatedTableSchema::all(new_schema),
        })])
        .await
        .unwrap_err();

    // THEN: the upgrade error surfaces with rows, columns, and metadata
    // untouched.
    assert_eq!(error.kind(), ErrorKind::CorruptedTableSchema);
    assert_eq!(
        error.description(),
        Some("ClickHouse MergeTree table requires a transaction ordinal upgrade")
    );
    drop(destination);

    assert_eq!(
        database.query::<(i64, String)>("select id, value from retained_rows").await,
        vec![(1, "retained".to_owned())]
    );
    assert_eq!(
        database
            .query::<String>(
                "select name from system.columns where database = currentDatabase() and table = \
                 'retained_rows' order by position",
            )
            .await,
        vec!["id", "value", "cdc_operation", "cdc_lsn"]
    );
    assert_eq!(store.get_destination_table_metadata(schema.id()).await.unwrap(), Some(metadata));
}

/// The previous ReplacingMergeTree layout and current-state view remain usable
/// without ALTER or recopy when a new destination loads retained metadata.
#[tokio::test(flavor = "multi_thread")]
async fn existing_replacing_merge_tree_resumes_without_layout_changes() {
    // GIVEN: an existing ReplacingMergeTree table, view, and retained metadata.
    init_test_tracing();
    install_crypto_provider();
    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();
    let schema = store_id_value_schema(&store, "upgrade").await;
    let metadata = DestinationTableMetadata::new_applied(
        "retained_rows".to_owned(),
        schema.inner().snapshot_id,
        schema.replication_mask().clone(),
    );
    store.store_destination_table_metadata(schema.id(), metadata.clone()).await.unwrap();

    database
        .db_client()
        .query(
            "create table retained_rows (id Int64, value String, _etl_version UInt128, \
             _etl_deleted UInt8) engine = ReplacingMergeTree(_etl_version, _etl_deleted) order by \
             id",
        )
        .execute()
        .await
        .unwrap();
    database
        .db_client()
        .query(
            "create view retained_rows__current as select id, value from retained_rows final \
             where _etl_deleted = 0",
        )
        .execute()
        .await
        .unwrap();
    database
        .db_client()
        .query("insert into retained_rows values (1, 'old', 0, 0), (2, 'retained', 0, 0)")
        .execute()
        .await
        .unwrap();

    // WHEN: a new destination writes an update without changing the layout.
    let destination = database
        .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
        .await;
    destination
        .write_events(vec![Event::Update(UpdateEvent {
            commit_lsn: PgLsn::from(100),
            tx_ordinal: 7,
            replicated_table_schema: schema.clone(),
            updated_table_row: UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I64(1),
                Cell::String("updated".to_owned()),
            ])),
            old_table_row: None,
        })])
        .await
        .unwrap();
    drop(destination);

    // THEN: the view shows the update and retained row, and metadata is
    // unchanged.
    assert_eq!(
        database
            .query::<(i64, String)>("select id, value from retained_rows__current order by id")
            .await,
        vec![(1, "updated".to_owned()), (2, "retained".to_owned())]
    );
    assert_eq!(store.get_destination_table_metadata(schema.id()).await.unwrap(), Some(metadata));
}

/// Builds a replayable key change with an ordinal within the same transaction.
fn replay_key_change(
    schema: &ReplicatedTableSchema,
    old_id: i64,
    new_id: i64,
    value: &str,
    tx_ordinal: u64,
) -> Event {
    Event::Update(UpdateEvent {
        commit_lsn: PgLsn::from(100),
        tx_ordinal,
        replicated_table_schema: schema.clone(),
        updated_table_row: UpdatedTableRow::Full(TableRow::new(vec![
            Cell::I64(new_id),
            Cell::String(value.to_owned()),
        ])),
        old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I64(old_id)]))),
    })
}

/// A partial INSERT failure can persist the old-key tombstone before rejecting
/// its replacement. Restart and at-least-once replay must converge, including
/// when a later event in the same transaction reuses the original key.
async fn partial_key_change_restart_replay_inner(engine: ClickHouseEngine) {
    // GIVEN: copied rows, single-row inserts, and a rejecting constraint.
    init_test_tracing();
    install_crypto_provider();
    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();
    let schema = store_id_value_schema(&store, "replay").await;
    let config = ClickHouseInserterConfig { engine, max_bytes_per_insert: 1 };
    let destination = database.build_destination_with_config(store.clone(), config).await;
    destination
        .write_table_rows(
            &schema,
            vec![
                TableRow::new(vec![Cell::I64(1), Cell::String("original".to_owned())]),
                TableRow::new(vec![Cell::I64(9), Cell::String("unaffected".to_owned())]),
            ],
        )
        .await
        .unwrap();
    let current_query = current_state_query(engine, "public_replay", "id, value", &["id"], "id");

    // One row per INSERT makes the server accept the tombstone before the
    // constraint rejects the replacement, without timing or network races.
    database
        .db_client()
        .query("alter table public_replay add constraint reject_two check id != 2")
        .execute()
        .await
        .unwrap();

    // WHEN: a key change fails after writing its old-key tombstone.
    let error = destination
        .write_events(vec![replay_key_change(&schema, 1, 2, "moved", 7)])
        .await
        .unwrap_err();

    // THEN: the failure is retryable and only the unaffected row remains.
    assert_eq!(error.kind(), ErrorKind::DestinationAtomicBatchRetryable);
    drop(destination);

    assert_eq!(
        database.query::<(i64, String)>(&current_query).await,
        vec![(9, "unaffected".to_owned())]
    );
    let tombstone_query = match engine {
        ClickHouseEngine::MergeTree => {
            "select id from public_replay where cdc_operation = 'DELETE'"
        }
        ClickHouseEngine::ReplacingMergeTree => {
            "select id from public_replay where _etl_deleted = 1"
        }
    };
    assert_eq!(database.query::<i64>(tombstone_query).await, vec![1]);

    // WHEN: the constraint is removed and the key change replays on restart.
    database
        .db_client()
        .query("alter table public_replay drop constraint reject_two")
        .execute()
        .await
        .unwrap();
    let restarted = database.build_destination_with_config(store, config).await;
    restarted.write_events(vec![replay_key_change(&schema, 1, 2, "moved", 7)]).await.unwrap();

    // THEN: replay restores the moved row and preserves the unaffected row.
    assert_eq!(
        database.query::<(i64, String)>(&current_query).await,
        vec![(2, "moved".to_owned()), (9, "unaffected".to_owned())]
    );

    // WHEN: a later event reuses the original key before stale replay.
    restarted.write_events(vec![replay_key_change(&schema, 2, 1, "reused", 8)]).await.unwrap();
    restarted.write_events(vec![replay_key_change(&schema, 1, 2, "moved", 7)]).await.unwrap();
    drop(restarted);

    // THEN: the reused key survives and the stale replacement stays absent.
    assert_eq!(
        database.query::<(i64, String)>(&current_query).await,
        vec![(1, "reused".to_owned()), (9, "unaffected".to_owned())]
    );
}

/// MergeTree current state converges despite duplicate replayed log entries.
#[tokio::test(flavor = "multi_thread")]
async fn partial_key_change_restart_replay_merge_tree() {
    partial_key_change_restart_replay_inner(ClickHouseEngine::MergeTree).await;
}

/// ReplacingMergeTree versions preserve later key reuse across stale replay.
#[tokio::test(flavor = "multi_thread")]
async fn partial_key_change_restart_replay_replacing_merge_tree() {
    partial_key_change_restart_replay_inner(ClickHouseEngine::ReplacingMergeTree).await;
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
    let metadata = store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert!(metadata.is_applied());
    let destination_table_name = metadata.table_id().to_owned();
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

/// A source default containing backslashes reaches ClickHouse with the same
/// characters PostgreSQL stores.
///
/// PostgreSQL renders `default 'C:\temp'` as the literal `'C:\temp'::text`,
/// where the backslash is an ordinary character. ClickHouse reads `\t` inside a
/// string literal as a tab, so forwarding the PostgreSQL literal verbatim
/// silently changes the default. The test makes ClickHouse materialise the
/// stored default by inserting a row without the column, then compares the
/// value with what PostgreSQL holds.
#[tokio::test(flavor = "multi_thread")]
async fn column_default_with_backslashes_keeps_source_value() {
    init_test_tracing();
    install_crypto_provider();

    // GIVEN: text and json defaults with backslashes, one trailing.
    let clickhouse_db = setup_clickhouse_database().await;
    let store = MemoryStore::new();
    let table_id = TableId::new(4246);
    let table_name = TableName::new("public".to_owned(), "escaped".to_owned());
    let schema = store
        .store_table_schema(TableSchema::new(
            table_id,
            table_name,
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("path".to_owned(), Type::TEXT, -1, 2, false)
                    .with_default_expression(r"'C:\temp'::text".to_owned()),
                ColumnSchema::new("trailing".to_owned(), Type::TEXT, -1, 3, false)
                    .with_default_expression(r"'abc\'::text".to_owned()),
                ColumnSchema::new("payload".to_owned(), Type::JSONB, -1, 4, false)
                    .with_default_expression(r#"'{"p":"C:\\dir"}'::jsonb"#.to_owned()),
            ],
        ))
        .await
        .unwrap();
    let schema = ReplicatedTableSchema::all(schema);
    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
        .await;

    // WHEN: the table is created and ClickHouse fills omitted columns.
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    clickhouse_db
        .db_client()
        .query(
            "insert into \"public_escaped\" (id, cdc_operation, cdc_lsn, cdc_tx_ordinal) values \
             (1, 'INSERT', 0, 0)",
        )
        .execute()
        .await
        .unwrap();

    // THEN: every default holds exactly the characters PostgreSQL stores.
    assert_eq!(
        clickhouse_db
            .query::<(String, String, String)>(
                "select path, trailing, payload from \"public_escaped\""
            )
            .await,
        vec![(r"C:\temp".to_owned(), r"abc\".to_owned(), r#"{"p":"C:\\dir"}"#.to_owned())]
    );
}

/// Builds a replicated `public.<table>` schema with one integer primary key.
fn id_only_schema(table_id: u32, table: &str) -> ReplicatedTableSchema {
    ReplicatedTableSchema::all(Arc::new(TableSchema::new(
        TableId::new(table_id),
        TableName::new("public".to_owned(), table.to_owned()),
        vec![ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1)],
    )))
}

/// A source table whose destination name equals another table's current view
/// is rejected under ReplacingMergeTree.
///
/// Destination names double underscores, so `public.foo_current` encodes to
/// `public_foo__current`, the name of the current view over `public.foo`. If
/// that table exists first, ClickHouse's `CREATE VIEW IF NOT EXISTS` for
/// `public.foo` silently keeps the table, so `public.foo` never gets its view.
#[tokio::test(flavor = "multi_thread")]
async fn table_named_like_current_view_is_rejected_under_replacing_merge_tree() {
    init_test_tracing();
    install_crypto_provider();

    // GIVEN: a ReplacingMergeTree destination.
    let clickhouse_db = setup_clickhouse_database().await;
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::ReplacingMergeTree)
        .await;

    // WHEN: `public.foo_current` is created, then `public.foo`.
    let error =
        destination.write_table_rows(&id_only_schema(1, "foo_current"), vec![]).await.unwrap_err();
    destination.write_table_rows(&id_only_schema(2, "foo"), vec![]).await.unwrap();

    // THEN: the colliding table is rejected and `public.foo` owns the view
    // name.
    assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
    assert_eq!(
        clickhouse_db
            .query::<String>("select engine from system.tables where name = 'public_foo__current'")
            .await,
        vec!["View".to_owned()]
    );
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
/// The recovery path runs with a schema carrying snapshot 100 -- a stale replay
/// arriving before the interrupted change's relation event.
///
/// # THEN
///
/// The write fails with `ErrorKind::DestinationSchemaRewind` instead of diffing
/// against the stale schema and wrongly marking the interrupted change as
/// applied.
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
    .with_schema_change(test_snapshot_id(200_u64, 200_u64), replication_mask)
    .unwrap();
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
    .with_schema_change(test_snapshot_id(200_u64, 200_u64), target_mask)
    .unwrap();
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

/// Tests that interrupted schema-change recovery replays the diff and marks the
/// change applied when the arriving schema matches the recovery target.
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
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after table creation");
    assert!(applied_metadata.is_applied());
    let clickhouse_table_name = applied_metadata.table_id().to_owned();
    let interrupted_metadata = DestinationTableMetadata::new_applied(
        clickhouse_table_name.clone(),
        test_snapshot_id(100_u64, 100_u64),
        old_mask,
    )
    .with_schema_change(test_snapshot_id(200_u64, 200_u64), new_mask)
    .unwrap();
    store.store_destination_table_metadata(table_id, interrupted_metadata).await.unwrap();

    // A restarted destination has an empty process-local cache and must replay
    // the interrupted diff from durable metadata.
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
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should be applied after recovery");
    assert!(recovered_metadata.is_applied());
    assert_eq!(
        recovered_metadata.snapshot_id(),
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
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should exist after table creation");
    assert!(applied_metadata.is_applied());
    let clickhouse_table_name = applied_metadata.table_id().to_owned();
    let interrupted_metadata = DestinationTableMetadata::new_applied(
        clickhouse_table_name.clone(),
        test_snapshot_id(100_u64, 100_u64),
        old_mask,
    )
    .with_schema_change(target_table_schema.snapshot_id, target_mask.clone())
    .unwrap();
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
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata should be applied after recovery");
    assert!(recovered_metadata.is_applied());
    assert_eq!(recovered_metadata.snapshot_id(), target_table_schema.snapshot_id);
    assert_eq!(recovered_metadata.replication_mask(), &target_mask);

    let rows: Vec<RecoveryMaskRow> = clickhouse_db
        .query(&format!("SELECT id, name FROM \"{clickhouse_table_name}\" ORDER BY id"))
        .await;
    assert_eq!(rows, vec![RecoveryMaskRow { id: 1, name: Some("Alice".to_owned()) }]);
}

/// Builds an id/value schema with an explicit table ID for tests that need
/// several independent tables.
///
/// `table` must not contain underscores so the ClickHouse table name stays
/// the predictable `public_<table>`.
fn lifecycle_schema_with_id(table: &str, table_id: u32) -> ReplicatedTableSchema {
    assert!(!table.contains('_'), "table name would change the ClickHouse name mapping");
    let table_schema = Arc::new(TableSchema::new(
        TableId::new(table_id),
        TableName::new("public".to_owned(), table.to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 2, false),
        ],
    ));

    ReplicatedTableSchema::all(table_schema)
}

/// Builds the id/value schema used by the dispatch lifecycle tests.
fn lifecycle_schema(table: &str) -> ReplicatedTableSchema {
    lifecycle_schema_with_id(table, 7100)
}

/// Builds one streaming insert event for the lifecycle schema.
fn lifecycle_insert(schema: &ReplicatedTableSchema, id: i64, value: &str) -> Event {
    Event::Insert(InsertEvent {
        commit_lsn: PgLsn::from(1000),
        tx_ordinal: 0,
        replicated_table_schema: schema.clone(),
        table_row: TableRow::new(vec![Cell::I64(id), Cell::String(value.to_owned())]),
    })
}

/// Records how long the `write_events` trait dispatch itself takes,
/// independent of when its asynchronous result completes.
struct DispatchTimingProbe<D> {
    inner: D,
    write_events_dispatch: Mutex<Option<Duration>>,
}

impl<D> Destination for DispatchTimingProbe<D>
where
    D: Destination + Send + Sync,
{
    fn name() -> &'static str {
        D::name()
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.inner.drop_table_for_copy(replicated_table_schema, async_result).await
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        self.inner
            .write_table_rows(replicated_table_schema, batch_id, table_rows, async_result)
            .await
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        let started = Instant::now();
        let result = self.inner.write_events(events, durability, async_result).await;
        *self.write_events_dispatch.lock() = Some(started.elapsed());
        result
    }
}

/// Yields to the scheduler until `condition` holds.
///
/// Cooperative replacement for wall-clock waits; panics when the condition
/// is not reached within a bounded yield budget so a regression fails
/// instead of hanging.
async fn yield_until(condition: impl Fn() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition not reached within the yield budget");
}

/// Gives the scheduler bounded opportunity to run other tasks.
///
/// Used before asserting that a task is still blocked; cooperative yields
/// let spawned work reach its parked state without wall-clock sleeps.
async fn yield_rounds() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

/// The trait dispatch returns while an in-flight insert is still pending,
/// and the asynchronous result reports `Durable` only after the insert
/// lands.
#[tokio::test(flavor = "multi_thread")]
async fn write_events_dispatch_returns_while_insert_is_pending() {
    // GIVEN: a destination table and a pause armed before the batch's first
    // INSERT statement.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("deferred");
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    let (reached, release) = arm_pause_before_insert_statement_for_tests(0);

    // WHEN: one insert event batch dispatched through the trait parks at
    // the armed statement.
    let probe = Arc::new(DispatchTimingProbe {
        inner: destination,
        write_events_dispatch: Mutex::new(None),
    });
    let write_handle = tokio::spawn({
        let probe = Arc::clone(&probe);
        let schema = schema.clone();
        async move {
            write_events_via_trait(
                probe.as_ref(),
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 1, "deferred")],
            )
            .await
        }
    });
    reached.await.unwrap();
    yield_until(|| probe.write_events_dispatch.lock().is_some()).await;

    // THEN: dispatch returned while the parked write had sent nothing and
    // its result was pending; releasing the pause completes it durably.
    assert!(!write_handle.is_finished());
    assert_eq!(
        clickhouse_db.query::<i64>("select id from \"public_deferred\"").await,
        Vec::<i64>::new()
    );
    release.send(()).unwrap();
    assert_eq!(write_handle.await.unwrap().unwrap(), DestinationWriteStatus::Durable);
    assert_eq!(clickhouse_db.query::<i64>("select id from \"public_deferred\"").await, vec![1]);
}

/// A destructive table reset drains the admitted write before dropping the
/// table, so the parked insert lands and the drop waits for it.
#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_waits_for_admitted_write() {
    // GIVEN: a destination table and a write parked at its first INSERT
    // statement.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("resetrace");
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    let (reached, release) = arm_pause_before_insert_statement_for_tests(0);
    let write_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 1, "landed")],
            )
            .await
        }
    });
    reached.await.unwrap();

    // WHEN: a table reset starts while the write is parked, and the write
    // is released afterwards.
    let reset_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = schema.clone();
        async move { drop_table_for_copy_via_trait(&destination, &schema).await }
    });
    yield_rounds().await;
    // The reset must be draining the parked task rather than finishing.
    assert!(!reset_handle.is_finished());
    release.send(()).unwrap();
    let write_status = write_handle.await.unwrap();
    let drop_result = reset_handle.await.unwrap();

    // THEN: the released write completed durably instead of racing the
    // dropped table, and the reset then removed the table.
    assert_eq!(write_status.unwrap(), DestinationWriteStatus::Durable);
    drop_result.unwrap();
    assert_eq!(
        clickhouse_db
            .query::<String>(
                "select name from system.tables where database = currentDatabase() and name = \
                 'public_resetrace'",
            )
            .await,
        Vec::<String>::new()
    );
}

/// A reset after source tables swap names by rename drops the destination
/// table recorded in metadata, not the table matching the new source name.
#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_after_rename_drops_recorded_table() {
    // GIVEN: `orders` and `ordersnew` were replicated, then swapped names:
    // `orders` became `ordersold` and `ordersnew` became `orders`.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let public = |table: &str| TableName::new("public".to_owned(), table.to_owned());
    let archived_id = TableId::new(4247);
    let replacement_id = TableId::new(4248);
    let archived = store_status_default_schema(
        &store,
        archived_id,
        &public("orders"),
        test_snapshot_id(100, 100),
        None,
    )
    .await;
    let replacement = store_status_default_schema(
        &store,
        replacement_id,
        &public("ordersnew"),
        test_snapshot_id(100, 101),
        None,
    )
    .await;
    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
        .await;
    destination
        .write_table_rows(
            &archived,
            vec![TableRow::new(vec![Cell::I64(1), Cell::String("kept".to_owned())])],
        )
        .await
        .unwrap();
    destination.write_table_rows(&replacement, vec![]).await.unwrap();
    let archived_renamed = store_status_default_schema(
        &store,
        archived_id,
        &public("ordersold"),
        test_snapshot_id(200, 200),
        None,
    )
    .await;
    let replacement_renamed = store_status_default_schema(
        &store,
        replacement_id,
        &public("orders"),
        test_snapshot_id(200, 201),
        None,
    )
    .await;
    destination
        .write_events(vec![
            Event::Relation(RelationEvent { replicated_table_schema: archived_renamed }),
            Event::Relation(RelationEvent { replicated_table_schema: replacement_renamed.clone() }),
        ])
        .await
        .unwrap();

    // WHEN: the replacement table, now named `orders`, is reset for a fresh
    // copy.
    drop_table_for_copy_via_trait(&destination, &replacement_renamed).await.unwrap();

    // THEN: only the replacement's own table and view are gone; the archived
    // table, which still writes to `public_orders`, keeps its rows.
    assert_eq!(
        clickhouse_db
            .query::<String>(
                "select name from system.tables where database = currentDatabase() order by name",
            )
            .await,
        vec!["public_orders".to_owned(), "public_orders__current".to_owned()]
    );
    assert_eq!(
        clickhouse_db.query::<(i64, Option<String>)>("select id, status from public_orders").await,
        vec![(1, Some("kept".to_owned()))]
    );
}

/// Shutdown aborts an admitted write and the pending result reports the
/// aborted task as an error instead of a silent success.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_aborts_admitted_write_without_silent_success() {
    // GIVEN: a destination table and a write parked at its first INSERT
    // statement.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("aborted");
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    let (reached, _release) = arm_pause_before_insert_statement_for_tests(0);
    let write_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 1, "aborted")],
            )
            .await
        }
    });
    reached.await.unwrap();

    // WHEN: shutdown runs while the write is parked.
    Destination::shutdown(&destination).await.unwrap();

    // THEN: the aborted write surfaced as an error rather than a fabricated
    // success, and nothing reached the table.
    let error = write_handle.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DestinationError);
    assert_eq!(
        clickhouse_db.query::<i64>("select id from \"public_aborted\"").await,
        Vec::<i64>::new()
    );
}

/// An aborted write releases its table fence. The fence guards travel inside
/// the spawned future, so abort must drop them even though the task never
/// reaches its explicit release; otherwise every later batch for that table
/// would wait forever on the same destination.
async fn aborted_write_releases_fence_for_later_writes_inner(engine: ClickHouseEngine) {
    // GIVEN: a destination table and a write parked at its first INSERT
    // statement while holding the table's fence.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("refenced");
    let destination = clickhouse_db.build_destination_with_engine(MemoryStore::new(), engine).await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    let (reached, _release) = arm_pause_before_insert_statement_for_tests(0);
    let write_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 1, "aborted")],
            )
            .await
        }
    });
    reached.await.unwrap();

    // WHEN: shutdown aborts the parked write and the same table is written
    // again through the same destination.
    Destination::shutdown(&destination).await.unwrap();
    write_handle.await.unwrap().unwrap_err();
    // A leaked fence would park this call forever; the timeout turns that
    // into a failure instead of a hung test.
    let status = tokio::time::timeout(
        Duration::from_secs(30),
        write_events_via_trait(
            &destination,
            WriteEventsDurability::MayDefer,
            vec![lifecycle_insert(&schema, 2, "admitted")],
        ),
    )
    .await
    .unwrap()
    .unwrap();

    // THEN: the later write was admitted past the fence and landed alone.
    assert_eq!(status, DestinationWriteStatus::Durable);
    assert_eq!(clickhouse_db.query::<i64>("select id from \"public_refenced\"").await, vec![2]);
}

/// MergeTree releases an aborted write's fence for later writes.
#[tokio::test(flavor = "multi_thread")]
async fn aborted_write_releases_fence_for_later_writes_merge_tree() {
    aborted_write_releases_fence_for_later_writes_inner(ClickHouseEngine::MergeTree).await;
}

/// ReplacingMergeTree releases an aborted write's fence for later writes.
#[tokio::test(flavor = "multi_thread")]
async fn aborted_write_releases_fence_for_later_writes_replacing_merge_tree() {
    aborted_write_releases_fence_for_later_writes_inner(ClickHouseEngine::ReplacingMergeTree).await;
}

/// An insert rejected by the server reaches the caller through the async
/// result channel, and the destination keeps admitting later work.
#[tokio::test(flavor = "multi_thread")]
async fn write_events_reports_insert_failure_through_async_result() {
    // GIVEN: a destination table with a constraint that rejects one key.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("rejected");
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    clickhouse_db
        .db_client()
        .query("alter table \"public_rejected\" add constraint reject_two check id != 2")
        .execute()
        .await
        .unwrap();

    // WHEN: a rejected insert and then an accepted insert are dispatched.
    let error = write_events_via_trait(
        &destination,
        WriteEventsDurability::MayDefer,
        vec![lifecycle_insert(&schema, 2, "rejected")],
    )
    .await
    .unwrap_err();
    let status = write_events_via_trait(
        &destination,
        WriteEventsDurability::MayDefer,
        vec![lifecycle_insert(&schema, 1, "accepted")],
    )
    .await
    .unwrap();

    // THEN: the failure carried the typed insert error and later admission
    // still succeeded.
    assert_eq!(error.kind(), ErrorKind::DestinationAtomicBatchRetryable);
    assert_eq!(status, DestinationWriteStatus::Durable);
    assert_eq!(clickhouse_db.query::<i64>("select id from \"public_rejected\"").await, vec![1]);
}

/// A write aborted between INSERT statements replays to a converged current
/// state after a destination restart.
async fn aborted_write_replays_to_converged_state_inner(engine: ClickHouseEngine) {
    // GIVEN: single-row INSERT statements and a pause armed before the
    // batch's second statement.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("replayed");
    let store = MemoryStore::new();
    let config = ClickHouseInserterConfig { engine, max_bytes_per_insert: 1 };
    let destination = clickhouse_db.build_destination_with_config(store.clone(), config).await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    let batch = || {
        vec![
            lifecycle_insert(&schema, 1, "one"),
            lifecycle_insert(&schema, 2, "two"),
            lifecycle_insert(&schema, 3, "three"),
        ]
    };
    let (reached, _release) = arm_pause_before_insert_statement_for_tests(1);

    // WHEN: shutdown aborts the admitted batch exactly between its first and
    // second statements, and a restarted destination replays the identical
    // batch.
    let write_handle = tokio::spawn({
        let destination = destination.clone();
        let events = batch();
        async move {
            write_events_via_trait(&destination, WriteEventsDurability::MayDefer, events).await
        }
    });
    reached.await.unwrap();
    Destination::shutdown(&destination).await.unwrap();
    let error = write_handle.await.unwrap().unwrap_err();
    let surviving_rows =
        clickhouse_db.query::<i64>("select id from \"public_replayed\" order by id").await;
    let restarted = clickhouse_db.build_destination_with_config(store, config).await;
    let status =
        write_events_via_trait(&restarted, WriteEventsDurability::MayDefer, batch()).await.unwrap();

    // THEN: the abort surfaced as an error, exactly the first statement
    // survived it, and the replay converged on the batch contents.
    assert_eq!(error.kind(), ErrorKind::DestinationError);
    assert_eq!(surviving_rows, vec![1]);
    assert_eq!(status, DestinationWriteStatus::Durable);
    let query = current_state_query(engine, "public_replayed", "id, value", &["id"], "id");
    assert_eq!(
        clickhouse_db.query::<(i64, String)>(&query).await,
        vec![(1, "one".to_owned()), (2, "two".to_owned()), (3, "three".to_owned())]
    );
}

/// MergeTree event logs converge across an aborted-batch replay.
#[tokio::test(flavor = "multi_thread")]
async fn aborted_write_replays_to_converged_state_merge_tree() {
    aborted_write_replays_to_converged_state_inner(ClickHouseEngine::MergeTree).await;
}

/// ReplacingMergeTree versions converge across an aborted-batch replay.
#[tokio::test(flavor = "multi_thread")]
async fn aborted_write_replays_to_converged_state_replacing_merge_tree() {
    aborted_write_replays_to_converged_state_inner(ClickHouseEngine::ReplacingMergeTree).await;
}

/// A write dispatched during a table reset is admitted only after the reset
/// completes and publishes its result.
#[tokio::test(flavor = "multi_thread")]
async fn write_admission_waits_for_table_reset() {
    // GIVEN: a write parked at the gated table's first INSERT statement and
    // an untouched bystander table.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let gated_schema = lifecycle_schema_with_id("gated", 7100);
    let bystander_schema = lifecycle_schema_with_id("bystander", 7200);
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&gated_schema, vec![]).await.unwrap();
    destination.write_table_rows(&bystander_schema, vec![]).await.unwrap();
    let (reached, release) = arm_pause_before_insert_statement_for_tests(0);
    let gated_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = gated_schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 1, "landed")],
            )
            .await
        }
    });
    reached.await.unwrap();

    // WHEN: a reset starts draining the parked write and a bystander write
    // is dispatched while the reset holds the task registry.
    let reset_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = gated_schema.clone();
        async move { drop_table_for_copy_via_trait(&destination, &schema).await }
    });
    yield_rounds().await;
    assert!(!reset_handle.is_finished());
    let bystander_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = bystander_schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 7, "after")],
            )
            .await
        }
    });
    yield_rounds().await;
    // Admission is closed while the reset holds the registry, so the
    // bystander write must still be blocked even though its own table is
    // unaffected.
    assert!(!bystander_handle.is_finished());
    release.send(()).unwrap();

    // THEN: every operation completed after the release, and the bystander
    // row landed in its own table.
    assert_eq!(gated_handle.await.unwrap().unwrap(), DestinationWriteStatus::Durable);
    reset_handle.await.unwrap().unwrap();
    assert_eq!(bystander_handle.await.unwrap().unwrap(), DestinationWriteStatus::Durable);
    assert_eq!(
        clickhouse_db.query::<(i64, String)>("select id, value from \"public_bystander\"").await,
        vec![(7, "after".to_owned())]
    );
}

/// Concurrently admitted writes complete independently, and a reset drains
/// every in-flight task, not only the reset table's.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_complete_independently_and_reset_drains_both() {
    // GIVEN: writes to two tables, each parked at its first INSERT
    // statement by one of two armed pauses.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let left_schema = lifecycle_schema_with_id("left", 7100);
    let right_schema = lifecycle_schema_with_id("right", 7200);
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&left_schema, vec![]).await.unwrap();
    destination.write_table_rows(&right_schema, vec![]).await.unwrap();
    let (first_reached, first_release) = arm_pause_before_insert_statement_for_tests(0);
    let (second_reached, second_release) = arm_pause_before_insert_statement_for_tests(0);
    let left_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = left_schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 3, "left")],
            )
            .await
        }
    });
    let right_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = right_schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 7, "right")],
            )
            .await
        }
    });
    first_reached.await.unwrap();
    second_reached.await.unwrap();

    // WHEN: a reset of the left table starts while both writes are parked,
    // and both writes are released afterwards.
    let reset_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = left_schema.clone();
        async move { drop_table_for_copy_via_trait(&destination, &schema).await }
    });
    yield_rounds().await;
    // The reset must be draining both parked tasks rather than finishing.
    assert!(!reset_handle.is_finished());
    first_release.send(()).unwrap();
    second_release.send(()).unwrap();

    // THEN: each write delivered its own durable result, the reset removed
    // the left table only after both tasks finished, and the surviving row
    // landed in its own table.
    assert_eq!(left_handle.await.unwrap().unwrap(), DestinationWriteStatus::Durable);
    assert_eq!(right_handle.await.unwrap().unwrap(), DestinationWriteStatus::Durable);
    reset_handle.await.unwrap().unwrap();
    assert_eq!(
        clickhouse_db.query::<(i64, String)>("select id, value from \"public_right\"").await,
        vec![(7, "right".to_owned())]
    );
    assert_eq!(
        clickhouse_db
            .query::<String>(
                "select name from system.tables where database = currentDatabase() and name = \
                 'public_left'",
            )
            .await,
        Vec::<String>::new()
    );
}

/// Builds one streaming truncate event for the lifecycle schema.
fn lifecycle_truncate(schema: &ReplicatedTableSchema) -> Event {
    Event::Truncate(TruncateEvent {
        commit_lsn: PgLsn::from(2000),
        tx_ordinal: 0,
        options: 0,
        truncated_tables: vec![schema.clone()],
    })
}

/// A retried apply attempt replays the abandoned batch and a later truncate
/// against the same destination while the abandoned insert is still in
/// flight. The replay's dispatch must wait at the table's fence until the
/// abandoned insert is acknowledged. Until then it cannot reach its own
/// INSERT, let alone the TRUNCATE. Without the fence, the late insert would
/// restore rows the truncate removed.
#[tokio::test(flavor = "multi_thread")]
async fn replayed_truncate_waits_for_abandoned_insert_on_same_table() {
    // GIVEN: a destination table, one pause for the abandoned insert, and
    // one pause for the replay's insert so its progress is observable.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("replay");
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();
    let (abandoned_reached, abandoned_release) = arm_pause_before_insert_statement_for_tests(0);
    let (mut replay_reached, replay_release) = arm_pause_before_insert_statement_for_tests(0);

    // The first attempt's write is admitted and parks before its INSERT. The
    // apply loop that issued it has already given up on the result.
    let abandoned_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 1, "restored")],
            )
            .await
        }
    });
    abandoned_reached.await.unwrap();

    // WHEN: the retried attempt replays the same insert followed by a
    // truncate of the table through the same destination.
    let replay_handle = tokio::spawn({
        let destination = destination.clone();
        let schema = schema.clone();
        async move {
            write_events_via_trait(
                &destination,
                WriteEventsDurability::MayDefer,
                vec![lifecycle_insert(&schema, 1, "restored"), lifecycle_truncate(&schema)],
            )
            .await
        }
    });

    // THEN: the replay waits at the fence behind the abandoned insert, so it
    // does not reach its own INSERT statement. An unfenced replay reaches it
    // within a few polls, so holding across the whole yield budget makes a
    // regression fail rather than race.
    for _ in 0..10_000 {
        assert!(matches!(replay_reached.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        tokio::task::yield_now().await;
    }
    assert!(!replay_handle.is_finished());

    // Releasing the abandoned insert lets it land first. Only then does the
    // replay reach its INSERT, and its TRUNCATE then removes both copies.
    abandoned_release.send(()).unwrap();
    assert_eq!(abandoned_handle.await.unwrap().unwrap(), DestinationWriteStatus::Durable);
    replay_reached.await.unwrap();
    assert_eq!(clickhouse_db.query::<i64>("select id from \"public_replay\"").await, vec![1]);
    replay_release.send(()).unwrap();
    assert_eq!(replay_handle.await.unwrap().unwrap(), DestinationWriteStatus::Durable);
    assert_eq!(
        clickhouse_db.query::<i64>("select id from \"public_replay\"").await,
        Vec::<i64>::new()
    );
}

/// RequireDurable writes, including the empty durability barrier, report
/// Durable rather than Accepted.
#[tokio::test(flavor = "multi_thread")]
async fn require_durable_writes_report_durable() {
    // GIVEN: a created destination table.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let schema = lifecycle_schema("barrier");
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&schema, vec![]).await.unwrap();

    // WHEN: a nonempty RequireDurable write and an empty durability barrier
    // are dispatched.
    let write_status = write_events_via_trait(
        &destination,
        WriteEventsDurability::RequireDurable,
        vec![lifecycle_insert(&schema, 1, "kept")],
    )
    .await
    .unwrap();
    let barrier_status =
        write_events_via_trait(&destination, WriteEventsDurability::RequireDurable, vec![])
            .await
            .unwrap();

    // THEN: both report Durable, which the apply loop requires for
    // RequireDurable calls, and the write landed.
    assert_eq!(write_status, DestinationWriteStatus::Durable);
    assert_eq!(barrier_status, DestinationWriteStatus::Durable);
    assert_eq!(clickhouse_db.query::<i64>("select id from \"public_barrier\"").await, vec![1]);
}

/// A table reset drains cleanly after a failed write, and admission stays
/// usable afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn table_reset_succeeds_after_failed_write() {
    // GIVEN: a write already rejected by a server constraint.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let failing_schema = lifecycle_schema_with_id("failing", 7100);
    let bystander_schema = lifecycle_schema_with_id("bystander", 7200);
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&failing_schema, vec![]).await.unwrap();
    destination.write_table_rows(&bystander_schema, vec![]).await.unwrap();
    clickhouse_db
        .db_client()
        .query("alter table \"public_failing\" add constraint reject_two check id != 2")
        .execute()
        .await
        .unwrap();
    let error = write_events_via_trait(
        &destination,
        WriteEventsDurability::MayDefer,
        vec![lifecycle_insert(&failing_schema, 2, "rejected")],
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DestinationAtomicBatchRetryable);

    // WHEN: the failed table is reset and a bystander write follows.
    let reset_result = drop_table_for_copy_via_trait(&destination, &failing_schema).await;
    let bystander_status = write_events_via_trait(
        &destination,
        WriteEventsDurability::MayDefer,
        vec![lifecycle_insert(&bystander_schema, 7, "after")],
    )
    .await;

    // THEN: the reset drained the failed task without resurfacing its error
    // and later admission completed durably.
    reset_result.unwrap();
    assert_eq!(bystander_status.unwrap(), DestinationWriteStatus::Durable);
    assert_eq!(
        clickhouse_db.query::<(i64, String)>("select id, value from \"public_bystander\"").await,
        vec![(7, "after".to_owned())]
    );
}

/// A failed table reset publishes its error through the async result,
/// releases the registry for later admission, and can be retried.
#[tokio::test(flavor = "multi_thread")]
async fn failed_table_reset_publishes_error_and_reopens_admission() {
    // GIVEN: two created tables and a one-shot injected reset failure.
    init_test_tracing();
    install_crypto_provider();
    let clickhouse_db = setup_clickhouse_database().await;
    let failing_schema = lifecycle_schema_with_id("resetfail", 7100);
    let bystander_schema = lifecycle_schema_with_id("bystander", 7200);
    let destination = clickhouse_db
        .build_destination_with_engine(MemoryStore::new(), ClickHouseEngine::MergeTree)
        .await;
    destination.write_table_rows(&failing_schema, vec![]).await.unwrap();
    destination.write_table_rows(&bystander_schema, vec![]).await.unwrap();
    arm_fail_drop_table_for_copy_once_for_tests();

    // WHEN: the reset fails, a bystander write follows, and the reset is
    // retried.
    let reset_error =
        drop_table_for_copy_via_trait(&destination, &failing_schema).await.unwrap_err();
    let bystander_status = write_events_via_trait(
        &destination,
        WriteEventsDurability::MayDefer,
        vec![lifecycle_insert(&bystander_schema, 7, "after")],
    )
    .await
    .unwrap();
    let retried_reset = drop_table_for_copy_via_trait(&destination, &failing_schema).await;

    // THEN: the injected failure travelled the async result, admission
    // stayed usable afterwards, and the one-shot failure did not stick to
    // the retried reset.
    assert_eq!(reset_error.kind(), ErrorKind::DestinationError);
    assert_eq!(bystander_status, DestinationWriteStatus::Durable);
    retried_reset.unwrap();
    assert_eq!(
        clickhouse_db.query::<(i64, String)>("select id, value from \"public_bystander\"").await,
        vec![(7, "after".to_owned())]
    );
    assert_eq!(
        clickhouse_db
            .query::<String>(
                "select name from system.tables where database = currentDatabase() and name = \
                 'public_resetfail'",
            )
            .await,
        Vec::<String>::new()
    );
}
