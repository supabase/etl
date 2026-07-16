//! Differential roundtrip properties for the Postgres text codec.
//!
//! Each property generates typed values, has a real Postgres render them in
//! the text format that replication tuples and COPY carry, parses that text
//! with the production codec, and asserts the parsed cell equals the value
//! Postgres stores. Postgres itself is the oracle, so these properties catch
//! silent value corruption, not just parser panics.
//!
//! Every property runs new random cases until its wall-clock budget elapses.
//! The budget comes from `ROUNDTRIP_PROPERTY_BUDGET_SECS` (default 3), so the
//! regular suite pays a few seconds per property while CI chaos jobs or local
//! deep runs can raise it to minutes.

use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    data::{ArrayCell, Cell},
    schema::ColumnSchema,
    test_utils::{
        database::spawn_source_database,
        replication_stream::{parse_copy_row, parse_text_cell},
    },
};
use etl_telemetry::tracing::init_test_tracing;
use futures::StreamExt;
use proptest::{option, prelude::*, test_runner::TestRunner as PropTestRunner};
use tokio_postgres::{
    Client, Statement,
    types::{ToSql, Type},
};

/// Number of generated cases between deadline checks.
const CASES_PER_CHUNK: u32 = 64;

/// Returns the wall-clock budget for one property.
fn property_budget() -> Duration {
    let secs = std::env::var("ROUNDTRIP_PROPERTY_BUDGET_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);

    Duration::from_secs(secs)
}

/// Runs `check` on freshly generated values until the property budget elapses.
///
/// Each chunk uses a new random seed. On failure the value is shrunk by
/// proptest and reported through a panic, so the minimal failing input shows
/// up in the test output and can be turned into a regression test.
fn run_property<S>(name: &str, strategy: &S, check: impl Fn(&S::Value) -> Result<(), TestCaseError>)
where
    S: Strategy,
    S::Value: std::fmt::Debug,
{
    let deadline = Instant::now() + property_budget();
    let mut total_cases = 0u64;

    loop {
        let mut runner = PropTestRunner::new(proptest::test_runner::Config {
            cases: CASES_PER_CHUNK,
            failure_persistence: None,
            ..Default::default()
        });

        match runner.run(strategy, |value| check(&value)) {
            Ok(()) => total_cases += u64::from(CASES_PER_CHUNK),
            Err(err) => {
                panic!("property '{name}' failed after ~{total_cases} passing cases:\n{err}")
            }
        }

        if Instant::now() >= deadline {
            break;
        }
    }

    println!("property '{name}': {total_cases} cases passed");
}

/// Runs an async future to completion from inside a synchronous proptest
/// closure.
///
/// Properties execute on a multi-threaded Tokio runtime worker, so blocking in
/// place is safe and keeps the database connection driven.
fn block_on<F: Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

/// Fetches the single text column the statement returns.
fn query_text(
    client: &Client,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<String, TestCaseError> {
    block_on(client.query_one(statement, params))
        .map(|row| row.get(0))
        .map_err(|err| TestCaseError::fail(format!("postgres query failed: {err}")))
}

/// Strings that are valid Postgres `text` values (no NUL bytes).
fn pg_text() -> impl Strategy<Value = String> {
    any::<String>().prop_filter("postgres text cannot contain NUL", |s| !s.contains('\0'))
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
        let cell = parse_text_cell(&Type::TEXT_ARRAY, &rendered)
            .map_err(|err| TestCaseError::fail(format!("codec rejected {rendered:?}: {err}")))?;

        prop_assert_eq!(&cell, &Cell::Array(ArrayCell::String(values.clone())));
        Ok(())
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
        let cell = parse_text_cell(&Type::INT8_ARRAY, &rendered)
            .map_err(|err| TestCaseError::fail(format!("codec rejected {rendered:?}: {err}")))?;

        prop_assert_eq!(&cell, &Cell::Array(ArrayCell::I64(values.clone())));
        Ok(())
    });
}

/// Decimal literal strings covering integers, fractions, scientific notation,
/// and the numeric specials.
fn numeric_literal() -> impl Strategy<Value = String> {
    let digits = |max: usize| proptest::collection::vec(0u8..=9, 1..=max);

    let finite = (any::<bool>(), digits(45), option::of(digits(45)), option::of(-60i32..=60))
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

/// Dates over the full four-digit ISO year range.
fn pg_date() -> impl Strategy<Value = NaiveDate> {
    let min = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().num_days_from_ce();
    let max = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap().num_days_from_ce();

    (min..=max).prop_map(|days| NaiveDate::from_num_days_from_ce_opt(days).unwrap())
}

/// Microsecond-precision times of day, matching Postgres's time resolution.
fn pg_time() -> impl Strategy<Value = NaiveTime> {
    (0u32..86_400, 0u32..1_000_000).prop_map(|(seconds, micros)| {
        NaiveTime::from_num_seconds_from_midnight_opt(seconds, micros * 1_000).unwrap()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn date_values_roundtrip_through_text_codec() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    let render = client.prepare("select ($1::date)::text").await.unwrap();

    run_property("date text roundtrip", &pg_date(), |date| {
        let rendered = query_text(client, &render, &[date])?;
        let cell = parse_text_cell(&Type::DATE, &rendered)
            .map_err(|err| TestCaseError::fail(format!("codec rejected {rendered:?}: {err}")))?;

        prop_assert_eq!(&cell, &Cell::Date(*date));
        Ok(())
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
        let cell = parse_text_cell(&Type::TIMESTAMPTZ, &rendered)
            .map_err(|err| TestCaseError::fail(format!("codec rejected {rendered:?}: {err}")))?;

        prop_assert_eq!(&cell, &Cell::TimestampTz(*timestamp));
        Ok(())
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
        .execute("create table test.copy_roundtrip (a text, b text[], c int8)", &[])
        .await
        .unwrap();
    let insert =
        client.prepare("insert into test.copy_roundtrip values ($1, $2, $3)").await.unwrap();

    let column_schemas = vec![
        ColumnSchema::new("a".to_owned(), Type::TEXT, -1, 1, true),
        ColumnSchema::new("b".to_owned(), Type::TEXT_ARRAY, -1, 2, true),
        ColumnSchema::new("c".to_owned(), Type::INT8, -1, 3, true),
    ];

    let strategy = (option::of(pg_text()), option::of(pg_text_array()), option::of(any::<i64>()));
    run_property("copy row roundtrip", &strategy, |(text, array, int)| {
        let row_bytes = block_on(async {
            client.execute("delete from test.copy_roundtrip", &[]).await?;
            client.execute(&insert, &[text, array, int]).await?;
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
        ];
        prop_assert_eq!(row.values(), expected.as_slice());
        Ok(())
    });
}
