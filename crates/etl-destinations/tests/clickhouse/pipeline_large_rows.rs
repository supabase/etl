//! Tests that exercise the Postgres -> ClickHouse pipeline with very large
//! single-row JSONB payloads.
//!
//! Each row is a two-field JSON object: `{"x":"aaa...","y":"aaa..."}`. The
//! field values are built server-side with PostgreSQL's `repeat('a', N)`,
//! and on read-back the row is summarised in ClickHouse into a small
//! `(id, len, a_count, head, tail)` tuple -- the payload bytes never leave
//! the database. That keeps both the SQL we send and test-process memory
//! tiny no matter how large the row is.

use etl::{
    state::table::TableReplicationPhaseType,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
    types::{EventType, PipelineId, TableName},
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::test_utils::setup_clickhouse_database;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::clickhouse::{current_state_query, install_crypto_provider};

const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;

/// ClickHouse-side table name after `try_stringify_table_name` escaping of
/// `test.large_rows`.
const LARGE_ROW_TABLE: &str = "test_large__rows";

const PUBLICATION_NAME: &str = "test_pub_ch_large_rows";

/// Byte count of the JSON object framing as ClickHouse receives it.
///
/// Postgres `jsonb_out` emits a canonical form with spaces after `:` and `,`
/// (e.g. `{"x": "", "y": ""}` = 18 bytes), but ETL parses jsonb through
/// `serde_json` (see `crates/etl/src/conversions/text.rs`) and re-serialises
/// in compact form (`{"x":"","y":""}` = 15 bytes). The destination only ever
/// sees the compact representation, so byte-exact assertions key off 15.
const PAYLOAD_OVERHEAD: usize = 15;

/// First 16 bytes of the payload as ClickHouse sees it: 6 framing bytes
/// (`{"x":"`) followed by 10 `'a'`s from the first field value.
const EXPECTED_HEAD: &str = r#"{"x":"aaaaaaaaaa"#;

/// Last 16 bytes of the payload: 14 `'a'`s from the second field value
/// followed by the 2-byte closer `"}`.
const EXPECTED_TAIL: &str = r#"aaaaaaaaaaaaaa"}"#;

/// ClickHouse-side projection summarising each row server-side. The
/// `length` / `countSubstrings` aggregates and the boundary `substring`s are
/// all bounded, so the response stays tiny regardless of payload size.
const SUMMARY_PROJECTION: &str = concat!(
    "id, ",
    "toUInt64(length(payload)) AS len, ",
    "toUInt64(countSubstrings(payload, 'a')) AS a_count, ",
    "substring(payload, 1, 16) AS head, ",
    "substring(payload, length(payload) - 15, 16) AS tail",
);

/// One row read back from ClickHouse, summarised entirely server-side so the
/// payload never crosses the wire to the test process.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
struct LargeRowSummary {
    id: i64,
    len: u64,
    a_count: u64,
    head: String,
    tail: String,
}

/// Builds the INSERT for one row of `{"x":"<N 'a's>","y":"<N 'a's>"}`. The
/// large value is constructed server-side via `repeat('a', N)` so the SQL we
/// send stays small.
///
/// Produces SQL of the shape:
///
/// ```sql
/// INSERT INTO "test"."large_rows" (payload) VALUES (
///   ('{"x":"' || repeat('a', N) || '","y":"' || repeat('a', N) || '"}')::jsonb
/// )
/// ```
fn insert_two_field_payload_sql(table_name: &TableName, field_chars: usize) -> String {
    format!(
        r#"INSERT INTO {table} (payload) VALUES (('{{"x":"' || repeat('a', {n}) || '","y":"' || repeat('a', {n}) || '"}}')::jsonb)"#,
        table = table_name.as_quoted_identifier(),
        n = field_chars,
    )
}

/// Drives one large-row test for the given engine and per-field byte count.
///
/// # GIVEN
///
/// A Postgres table `(id bigserial pk, payload jsonb not null)`. Row 1 is
/// inserted before the pipeline starts (initial-copy path) and row 2 is
/// inserted after the pipeline is ready (streaming INSERT-event path).
///
/// # THEN
///
/// Both rows arrive in ClickHouse with `length(payload) ==
/// PAYLOAD_OVERHEAD + 2 * field_chars`, the expected count of `'a'`
/// characters (`2 * field_chars`; neither key contains an `'a'`), and
/// matching head / tail boundary slices.
async fn large_row_inner(engine: ClickHouseEngine, field_chars: usize) {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: Postgres source with one pre-pipeline large JSONB row ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("large_rows");

    let table_id = database
        .create_table(table_name.clone(), true, &[("payload", "jsonb not null")])
        .await
        .expect("Failed to create large_rows table");

    database
        .create_publication(PUBLICATION_NAME, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create large_rows publication");

    // Row 1 exercises the initial table-copy path.
    database
        .run_sql(&insert_two_field_payload_sql(&table_name, field_chars))
        .await
        .expect("Failed to insert large JSONB row");

    // --- WHEN: pipeline copies row 1 and row 2 streams as an INSERT event ---
    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db.build_destination_with_engine(store.clone(), engine).await,
    );

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        PUBLICATION_NAME.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    // Row 2 exercises the streaming INSERT-event path.
    database
        .run_sql(&insert_two_field_payload_sql(&table_name, field_chars))
        .await
        .expect("Failed to insert large JSONB row");

    event_notify.notified().await;

    let query = current_state_query(engine, LARGE_ROW_TABLE, SUMMARY_PROJECTION, &["id"], "id");
    let rows: Vec<LargeRowSummary> = clickhouse_db.query(&query).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: both rows arrived intact and byte-exact ---
    assert_eq!(rows.len(), 2, "expected 2 current-state rows (copy + streaming)");

    let expected_len = (PAYLOAD_OVERHEAD + 2 * field_chars) as u64;
    let expected_a_count = (2 * field_chars) as u64;

    for row in &rows {
        assert_eq!(
            row.len, expected_len,
            "row id={} payload length {} != expected {expected_len}",
            row.id, row.len,
        );
        assert_eq!(
            row.a_count, expected_a_count,
            "row id={} count of 'a' bytes {} != expected {expected_a_count}",
            row.id, row.a_count,
        );
        assert_eq!(row.head, EXPECTED_HEAD, "row id={} payload head mismatch", row.id);
        assert_eq!(row.tail, EXPECTED_TAIL, "row id={} payload tail mismatch", row.id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn large_row_1mib_merge_tree() {
    large_row_inner(ClickHouseEngine::MergeTree, 512 * KIB).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_row_1mib_replacing_merge_tree() {
    large_row_inner(ClickHouseEngine::ReplacingMergeTree, 512 * KIB).await;
}

/// 16 MiB exceeds `BatchConfig::DEFAULT_MAX_BYTES` (8 MiB), so the batch
/// stream must flush a single oversized row on its own.
#[tokio::test(flavor = "multi_thread")]
async fn large_row_16mib_merge_tree() {
    large_row_inner(ClickHouseEngine::MergeTree, 8 * MIB).await;
}

/// 64 MiB exceeds `ClickHouseInserterConfig::DEFAULT_MAX_BYTES_PER_INSERT`
/// (64 MiB), validating the one-row-per-INSERT path in `insert_rows`.
#[tokio::test(flavor = "multi_thread")]
async fn large_row_64mib_merge_tree() {
    large_row_inner(ClickHouseEngine::MergeTree, 32 * MIB).await;
}

/// 256 MiB stress test with two ~128 MiB fields. Each row allocates ~256 MiB
/// peak in Postgres, ETL, and ClickHouse roughly concurrently.
///
/// `134_217_716` is the largest per-field byte count that still fits within
/// Postgres's JSONB object element-size ceiling (`2^28 - 1 = 268_435_455`
/// bytes; see `convertJsonbObject` in `jsonb_util.c`). Empirically,
/// `134_217_717` rejects and `134_217_716` accepts, so the materialised row
/// lands ~9 bytes shy of a clean 256 MiB.
#[tokio::test(flavor = "multi_thread")]
async fn large_row_256mib_merge_tree() {
    large_row_inner(ClickHouseEngine::MergeTree, 134_217_716).await;
}
