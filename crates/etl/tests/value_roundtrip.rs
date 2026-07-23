//! Differential roundtrip properties for the Postgres text codec.
//!
//! Each property generates typed values, has a real Postgres render them in
//! the text format that replication tuples and COPY carry, parses that text
//! with the production codec, and asserts the parsed cell equals the value
//! Postgres stores. Postgres itself is the oracle, so these properties catch
//! silent value corruption, not just parser panics.
//!
//! Every property runs new random cases until a wall-clock budget elapses,
//! using the shared runner in `etl::test_utils::property`. See that module
//! for the `PROPERTY_TEST_BUDGET_SECS` budget knob and the
//! `PROPERTY_TEST_SEED` failure replay knob.
//!
//! Known codec gaps stay outside the generated envelope and are documented on
//! the strategies that would otherwise reach them: temporal `infinity`
//! values, `BC` dates, years above 9999, and the `24:00:00` time are all
//! legal in Postgres but are rejected by the codec today. Multidimensional
//! arrays are also rejected; their property pins reject-not-corrupt.

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    data::{ArrayCell, Cell},
    schema::ColumnSchema,
    test_utils::{
        database::spawn_source_database,
        property::{
            any_f32, any_f64, block_on, f32_matches, f64_matches, opt_f64_matches, pg_text,
            pg_time, run_property,
        },
        replication_stream::{parse_copy_row, parse_text_cell},
    },
};
use etl_postgres::time::PgTimeTz;
use etl_telemetry::tracing::init_test_tracing;
use futures::StreamExt;
use proptest::{option, prelude::*};
use tokio_postgres::{
    Client, Statement,
    types::{Kind, ToSql, Type},
};
use uuid::Uuid;

/// Fetches the single text column the statement returns.
fn query_text(
    client: &Client,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<String, TestCaseError> {
    query_texts(client, statement, params).map(|mut texts| texts.remove(0))
}

/// Fetches all text columns of the single row the statement returns.
fn query_texts(
    client: &Client,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<String>, TestCaseError> {
    block_on(client.query_one(statement, params))
        .map(|row| (0..row.len()).map(|index| row.get(index)).collect())
        .map_err(|err| TestCaseError::fail(format!("postgres query failed: {err}")))
}

/// Parses one rendered text value and asserts it equals the expected cell.
fn assert_parses_to(typ: &Type, rendered: &str, expected: &Cell) -> Result<(), TestCaseError> {
    let cell = parse_text_cell(typ, rendered)
        .map_err(|err| TestCaseError::fail(format!("codec rejected {rendered:?}: {err}")))?;

    prop_assert_eq!(&cell, expected);
    Ok(())
}

/// Nullable-element arrays of Postgres `text` values.
fn pg_text_array() -> impl Strategy<Value = Vec<Option<String>>> {
    proptest::collection::vec(option::of(pg_text()), 0..=8)
}

#[tokio::test(flavor = "multi_thread")]
async fn text_array_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::text[])::text").await.unwrap();

    run_property("text[] text roundtrip", &pg_text_array(), |values| {
        let rendered = query_text(client, &render, &[values])?;
        assert_parses_to(
            &Type::TEXT_ARRAY,
            &rendered,
            &Cell::Array(ArrayCell::String(values.clone())),
        )
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn int8_array_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::int8[])::text").await.unwrap();

    let strategy = proptest::collection::vec(option::of(any::<i64>()), 0..=8);
    run_property("int8[] text roundtrip", &strategy, |values| {
        let rendered = query_text(client, &render, &[values])?;
        assert_parses_to(&Type::INT8_ARRAY, &rendered, &Cell::Array(ArrayCell::I64(values.clone())))
    });
}

/// Formats an int8 array literal with an explicit lower bound, e.g.
/// `[0:1]={7,8}`.
fn shifted_bounds_literal(lower: i16, values: &[Option<i64>]) -> String {
    let upper = i32::from(lower) + values.len() as i32 - 1;
    let elements =
        values.iter().map(|v| v.map_or("NULL".to_owned(), |v| v.to_string())).collect::<Vec<_>>();

    format!("[{lower}:{upper}]={{{}}}", elements.join(","))
}

#[tokio::test(flavor = "multi_thread")]
async fn shifted_lower_bound_arrays_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select (($1::text)::int8[])::text").await.unwrap();

    // Postgres renders arrays whose lower bound is not 1 with an explicit
    // dimensions prefix, e.g. `[0:1]={7,8}`. `ArrayCell` represents these
    // arrays as ordered elements without subscript bounds, so the codec
    // intentionally discards the prefix while preserving the elements.
    let strategy = (-8i16..=8, proptest::collection::vec(option::of(any::<i64>()), 1..=8));
    run_property("shifted lower bound int8[] text roundtrip", &strategy, |(lower, values)| {
        let literal = shifted_bounds_literal(*lower, values);
        let rendered = query_text(client, &render, &[&literal])?;
        assert_parses_to(&Type::INT8_ARRAY, &rendered, &Cell::Array(ArrayCell::I64(values.clone())))
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn multidimensional_arrays_are_rejected_by_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select (array[$1::text[], $2::text[]])::text").await.unwrap();

    // `ArrayCell` is one-dimensional, so the codec cannot represent a
    // multidimensional value. It must reject the rendered text loudly instead
    // of flattening or corrupting elements; this property pins
    // reject-not-corrupt until real support exists.
    let strategy = (1usize..=4).prop_flat_map(|cols| {
        (
            proptest::collection::vec(option::of(pg_text()), cols..=cols),
            proptest::collection::vec(option::of(pg_text()), cols..=cols),
        )
    });
    run_property("multidimensional text[] rejection", &strategy, |(first, second)| {
        let rendered = query_text(client, &render, &[first, second])?;
        let result = parse_text_cell(&Type::TEXT_ARRAY, &rendered);
        prop_assert!(
            result.is_err(),
            "codec accepted multidimensional array {:?} as {:?}",
            rendered,
            result
        );
        Ok(())
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn scalar_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client
        .prepare("select ($1::int2)::text, ($2::int4)::text, ($3::oid)::text, ($4::uuid)::text")
        .await
        .unwrap();

    // `bool` is deliberately absent: the `bool::text` cast renders
    // `true`/`false` through a dedicated cast function, while replication and
    // COPY carry the `boolout` output function's `t`/`f`. The COPY property
    // covers booleans on the representative path.
    let strategy =
        (any::<i16>(), any::<i32>(), any::<u32>(), any::<u128>().prop_map(Uuid::from_u128));
    run_property("scalar text roundtrip", &strategy, |(i16v, i32v, oid, uuid)| {
        let rendered = query_texts(client, &render, &[i16v, i32v, oid, uuid])?;
        assert_parses_to(&Type::INT2, &rendered[0], &Cell::I16(*i16v))?;
        assert_parses_to(&Type::INT4, &rendered[1], &Cell::I32(*i32v))?;
        assert_parses_to(&Type::OID, &rendered[2], &Cell::U32(*oid))?;
        assert_parses_to(&Type::UUID, &rendered[3], &Cell::Uuid(*uuid))
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn float_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::float8)::text, ($2::float4)::text").await.unwrap();

    let strategy = (any_f64(), any_f32());
    run_property("float text roundtrip", &strategy, |(f64v, f32v)| {
        let rendered = query_texts(client, &render, &[f64v, f32v])?;

        let Cell::F64(parsed64) = parse_text_cell(&Type::FLOAT8, &rendered[0]).map_err(|err| {
            TestCaseError::fail(format!("codec rejected {:?}: {err}", rendered[0]))
        })?
        else {
            return Err(TestCaseError::fail("expected f64 cell"));
        };
        prop_assert!(f64_matches(*f64v, parsed64), "float8 {f64v:?} parsed as {parsed64:?}");

        let Cell::F32(parsed32) = parse_text_cell(&Type::FLOAT4, &rendered[1]).map_err(|err| {
            TestCaseError::fail(format!("codec rejected {:?}: {err}", rendered[1]))
        })?
        else {
            return Err(TestCaseError::fail("expected f32 cell"));
        };
        prop_assert!(f32_matches(*f32v, parsed32), "float4 {f32v:?} parsed as {parsed32:?}");
        Ok(())
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn float8_array_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::float8[])::text").await.unwrap();

    let strategy = proptest::collection::vec(option::of(any_f64()), 0..=8);
    run_property("float8[] text roundtrip", &strategy, |values| {
        let rendered = query_text(client, &render, &[values])?;
        let Cell::Array(ArrayCell::F64(parsed)) = parse_text_cell(&Type::FLOAT8_ARRAY, &rendered)
            .map_err(|err| {
            TestCaseError::fail(format!("codec rejected {rendered:?}: {err}"))
        })?
        else {
            return Err(TestCaseError::fail("expected f64 array cell"));
        };

        prop_assert_eq!(parsed.len(), values.len());
        for (expected, parsed) in values.iter().zip(&parsed) {
            prop_assert!(
                opt_f64_matches(*expected, *parsed),
                "float8[] {expected:?} parsed as {parsed:?}"
            );
        }
        Ok(())
    });
}

/// Decimal literal strings covering integers, fractions, scientific notation,
/// and the numeric specials.
///
/// Exponents are tiered: most cases stay small so typical magnitudes
/// dominate, while the two boundary tiers reach the extremes the codec and
/// Postgres share, `1e131071` on the weight side and `1e-16383` on the
/// scale side. With up to 45 integer and 45 fractional digits, exponents in
/// `[-16_338, 131_027]` always stay inside Postgres's accepted range
/// (verified empirically: one step past either bound overflows the numeric
/// format), so Postgres stays a render oracle and never rejects a generated
/// literal.
fn numeric_literal() -> impl Strategy<Value = String> {
    let digits = |max: usize| proptest::collection::vec(0u8..=9, 1..=max);

    let exponent = prop_oneof![
        8 => -60i32..=60,
        1 => -16_338i32..=-16_200,
        1 => 130_900i32..=131_027,
    ];

    let finite = (any::<bool>(), digits(45), option::of(digits(45)), option::of(exponent))
        .prop_map(|(negative, int_digits, frac_digits, exponent)| {
            let mut literal = String::new();
            if negative {
                literal.push('-');
            }
            for digit in int_digits {
                literal.push((b'0' + digit) as char);
            }
            if let Some(frac_digits) = frac_digits {
                literal.push('.');
                for digit in frac_digits {
                    literal.push((b'0' + digit) as char);
                }
            }
            if let Some(exponent) = exponent {
                literal.push('e');
                literal.push_str(&exponent.to_string());
            }

            literal
        });

    prop_oneof![
        45 => finite,
        1 => Just("NaN".to_owned()),
        1 => Just("Infinity".to_owned()),
        1 => Just("-Infinity".to_owned()),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn numeric_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::text::numeric)::text").await.unwrap();

    run_property("numeric text roundtrip", &numeric_literal(), |literal| {
        let rendered = query_text(client, &render, &[literal])?;
        let cell = parse_text_cell(&Type::NUMERIC, &rendered)
            .map_err(|err| TestCaseError::fail(format!("codec rejected {rendered:?}: {err}")))?;
        let Cell::Numeric(numeric) = &cell else {
            return Err(TestCaseError::fail(format!("expected numeric cell, got {cell:?}")));
        };

        // The parsed value must serialize back to exactly the canonical text
        // Postgres produced, and re-parsing that text must be stable.
        prop_assert_eq!(&numeric.to_string(), &rendered);
        let reparsed = parse_text_cell(&Type::NUMERIC, &numeric.to_string()).map_err(|err| {
            TestCaseError::fail(format!("codec rejected its own output {numeric}: {err}"))
        })?;
        prop_assert_eq!(&reparsed, &cell);
        Ok(())
    });
}

/// Dates over the four-digit ISO year range.
///
/// Postgres also legally emits `BC`-suffixed years and years above 9999
/// (dates up to year 5874897, timestamps up to 294276), plus the special
/// `infinity`/`-infinity` values, but the codec rejects all of them today.
/// They stay outside the generated envelope until the codec handles them; see
/// the findings recorded with this harness.
fn pg_date() -> impl Strategy<Value = NaiveDate> {
    let min = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().num_days_from_ce();
    let max = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap().num_days_from_ce();

    (min..=max).prop_map(|days| NaiveDate::from_num_days_from_ce_opt(days).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn date_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::date)::text").await.unwrap();

    run_property("date text roundtrip", &pg_date(), |date| {
        let rendered = query_text(client, &render, &[date])?;
        assert_parses_to(&Type::DATE, &rendered, &Cell::Date(*date))
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn time_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::time)::text").await.unwrap();

    run_property("time text roundtrip", &pg_time(), |time| {
        let rendered = query_text(client, &render, &[time])?;
        assert_parses_to(&Type::TIME, &rendered, &Cell::Time(*time))
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamp_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::timestamp)::text").await.unwrap();

    let strategy = (pg_date(), pg_time()).prop_map(|(date, time)| NaiveDateTime::new(date, time));
    run_property("timestamp text roundtrip", &strategy, |timestamp| {
        let rendered = query_text(client, &render, &[timestamp])?;
        assert_parses_to(&Type::TIMESTAMP, &rendered, &Cell::Timestamp(*timestamp))
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamptz_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::timestamptz)::text").await.unwrap();

    let strategy = (pg_date(), pg_time()).prop_map(|(date, time)| {
        DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::new(date, time), Utc)
    });
    run_property("timestamptz text roundtrip", &strategy, |timestamp| {
        let rendered = query_text(client, &render, &[timestamp])?;
        assert_parses_to(&Type::TIMESTAMPTZ, &rendered, &Cell::TimestampTz(*timestamp))
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn timestamptz_array_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::timestamptz[])::text").await.unwrap();

    // Rendered timestamptz array elements contain spaces, so they exercise
    // the quoted-element path of the array parser.
    let element = (pg_date(), pg_time()).prop_map(|(date, time)| {
        DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::new(date, time), Utc)
    });
    let strategy = proptest::collection::vec(option::of(element), 0..=8);
    run_property("timestamptz[] text roundtrip", &strategy, |values| {
        let rendered = query_text(client, &render, &[values])?;
        assert_parses_to(
            &Type::TIMESTAMPTZ_ARRAY,
            &rendered,
            &Cell::Array(ArrayCell::TimestampTz(values.clone())),
        )
    });
}

/// Formats a Postgres `time with time zone` input literal.
///
/// Offsets cover the full Postgres range including a seconds component.
fn timetz_literal(time: &NaiveTime, offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let abs = offset_seconds.unsigned_abs();

    format!(
        "{}{}{:02}:{:02}:{:02}",
        time.format("%H:%M:%S%.6f"),
        sign,
        abs / 3600,
        abs % 3600 / 60,
        abs % 60
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn timetz_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::text::timetz)::text").await.unwrap();

    let strategy = (pg_time(), -57_599i32..=57_599);
    run_property("timetz text roundtrip", &strategy, |(time, offset_seconds)| {
        let literal = timetz_literal(time, *offset_seconds);
        let rendered = query_text(client, &render, &[&literal])?;
        let expected = PgTimeTz::new(*time, FixedOffset::east_opt(*offset_seconds).unwrap());
        assert_parses_to(&Type::TIMETZ, &rendered, &Cell::TimeTz(expected))
    });
}

/// JSON documents whose text form is stable through jsonb normalization:
/// no floats (jsonb canonicalizes numeric text) and no NUL escapes (jsonb
/// rejects them).
fn jsonb_value() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::from),
        any::<i64>().prop_map(serde_json::Value::from),
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

#[tokio::test(flavor = "multi_thread")]
async fn jsonb_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::jsonb)::text").await.unwrap();

    run_property("jsonb text roundtrip", &jsonb_value(), |value| {
        let rendered = query_text(client, &render, &[value])?;
        assert_parses_to(&Type::JSONB, &rendered, &Cell::Json(value.clone()))
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn bytea_array_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::bytea[])::text").await.unwrap();

    // Rendered bytea array elements are quoted hex strings with doubled
    // backslashes, so they exercise the escape path of the array parser
    // feeding into the hex parser.
    let strategy = proptest::collection::vec(
        option::of(proptest::collection::vec(any::<u8>(), 0..=16)),
        0..=8,
    );
    run_property("bytea[] text roundtrip", &strategy, |values| {
        let rendered = query_text(client, &render, &[values])?;
        assert_parses_to(
            &Type::BYTEA_ARRAY,
            &rendered,
            &Cell::Array(ArrayCell::Bytes(values.clone())),
        )
    });
}

/// Enum labels valid for `create type ... as enum`: 1 to 63 bytes, no NUL.
fn enum_label() -> impl Strategy<Value = String> {
    any::<String>().prop_filter("enum labels are 1-63 bytes without NUL", |s| {
        !s.is_empty() && s.len() <= 63 && !s.contains('\0')
    })
}

/// Builds the enum and enum-array [`Type`]s the replication path would
/// construct from the catalog for a custom enum.
fn enum_types(labels: Vec<String>, oid: u32, array_oid: u32) -> (Type, Type) {
    let element = Type::new("prop_enum".to_owned(), oid, Kind::Enum(labels), "test".to_owned());
    let array = Type::new(
        "_prop_enum".to_owned(),
        array_oid,
        Kind::Array(element.clone()),
        "test".to_owned(),
    );

    (element, array)
}

#[tokio::test(flavor = "multi_thread")]
async fn enum_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Labels index into the set; the scalar pick and the array picks choose
    // labels by index so they always name existing enum values.
    let strategy = (
        proptest::collection::btree_set(enum_label(), 1..=6),
        any::<prop::sample::Index>(),
        proptest::collection::vec(option::of(any::<prop::sample::Index>()), 0..=6),
    );
    run_property("enum text roundtrip", &strategy, |(labels, pick, array_picks)| {
        let labels: Vec<String> = labels.iter().cloned().collect();
        let scalar = labels[pick.index(labels.len())].clone();
        let array: Vec<Option<String>> = array_picks
            .iter()
            .map(|pick| pick.as_ref().map(|pick| labels[pick.index(labels.len())].clone()))
            .collect();

        let quoted_labels =
            labels.iter().map(|label| pg_escape::quote_literal(label)).collect::<Vec<_>>();
        let (oid, array_oid, rendered_scalar, rendered_array) = block_on(async {
            client.execute("drop type if exists test.prop_enum cascade", &[]).await?;
            client
                .execute(
                    &format!("create type test.prop_enum as enum ({})", quoted_labels.join(", ")),
                    &[],
                )
                .await?;

            let row = client
                .query_one(
                    "select t.oid, t.typarray from pg_type t join pg_namespace n on n.oid = \
                     t.typnamespace where t.typname = 'prop_enum' and n.nspname = 'test'",
                    &[],
                )
                .await?;
            let oid: u32 = row.get(0);
            let array_oid: u32 = row.get(1);

            let row = client
                .query_one(
                    "select ($1::text)::test.prop_enum::text, ($2::text[])::test.prop_enum[]::text",
                    &[&scalar, &array],
                )
                .await?;

            Ok::<_, tokio_postgres::Error>((
                oid,
                array_oid,
                row.get::<_, String>(0),
                row.get::<_, String>(1),
            ))
        })
        .map_err(|err| TestCaseError::fail(format!("postgres enum setup failed: {err}")))?;

        let (enum_type, enum_array_type) = enum_types(labels, oid, array_oid);
        assert_parses_to(&enum_type, &rendered_scalar, &Cell::String(scalar))?;
        assert_parses_to(&enum_array_type, &rendered_array, &Cell::Array(ArrayCell::String(array)))
    });
}

/// Reads the single COPY text row the table currently holds.
async fn copy_single_row(client: &Client, query: &str) -> Result<Vec<u8>, tokio_postgres::Error> {
    let stream = client.copy_out(query).await?;
    let mut bytes = Vec::new();
    futures::pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }

    Ok(bytes)
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_rows_roundtrip_through_copy_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    client
        .execute(
            "create table test.copy_roundtrip (a text, b text[], c int8, d float8, e timestamptz, \
             f bytea, g bool)",
            &[],
        )
        .await
        .unwrap();
    let insert = client
        .prepare("insert into test.copy_roundtrip values ($1, $2, $3, $4, $5, $6, $7)")
        .await
        .unwrap();

    let column_schemas = vec![
        ColumnSchema::new("a".to_owned(), Type::TEXT, -1, 1, true),
        ColumnSchema::new("b".to_owned(), Type::TEXT_ARRAY, -1, 2, true),
        ColumnSchema::new("c".to_owned(), Type::INT8, -1, 3, true),
        ColumnSchema::new("d".to_owned(), Type::FLOAT8, -1, 4, true),
        ColumnSchema::new("e".to_owned(), Type::TIMESTAMPTZ, -1, 5, true),
        ColumnSchema::new("f".to_owned(), Type::BYTEA, -1, 6, true),
        ColumnSchema::new("g".to_owned(), Type::BOOL, -1, 7, true),
    ];

    // NaN is excluded because expected-value comparison uses Cell equality;
    // the float property covers NaN with bit-level comparison.
    let float = any_f64().prop_filter("NaN breaks Cell equality", |f| !f.is_nan());
    let timestamptz = (pg_date(), pg_time()).prop_map(|(date, time)| {
        DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::new(date, time), Utc)
    });
    let strategy = (
        option::of(pg_text()),
        option::of(pg_text_array()),
        option::of(any::<i64>()),
        option::of(float),
        option::of(timestamptz),
        option::of(proptest::collection::vec(any::<u8>(), 0..=16)),
        option::of(any::<bool>()),
    );
    run_property("copy row roundtrip", &strategy, |(text, array, int, float, tstz, bytes, b)| {
        let row_bytes = block_on(async {
            client.execute("delete from test.copy_roundtrip", &[]).await?;
            client.execute(&insert, &[text, array, int, float, tstz, bytes, b]).await?;
            copy_single_row(client, "copy test.copy_roundtrip to stdout").await
        })
        .map_err(|err| TestCaseError::fail(format!("postgres copy failed: {err}")))?;

        let row = parse_copy_row(&row_bytes, &column_schemas).map_err(|err| {
            TestCaseError::fail(format!("codec rejected copy row {row_bytes:?}: {err}"))
        })?;

        let expected = vec![
            text.clone().map_or(Cell::Null, Cell::String),
            array.clone().map_or(Cell::Null, |values| Cell::Array(ArrayCell::String(values))),
            int.map_or(Cell::Null, Cell::I64),
            float.map_or(Cell::Null, Cell::F64),
            tstz.map_or(Cell::Null, Cell::TimestampTz),
            bytes.clone().map_or(Cell::Null, Cell::Bytes),
            b.map_or(Cell::Null, Cell::Bool),
        ];
        prop_assert_eq!(row.values(), expected.as_slice());
        Ok(())
    });
}
