//! MergeTree-only integration tests. These verify event-log semantics
//! (`cdc_operation` + `cdc_lsn`) that exist only on the MT engine; the
//! parameterized spine in `pipeline.rs` covers current-state behavior on
//! both engines.

use etl::{
    state::table::TableReplicationPhaseType,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
    types::{EventType, PipelineId},
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::test_utils::setup_clickhouse_database;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::clickhouse::install_crypto_provider;

/// MT event-log row: includes CDC metadata. All three operations in this
/// test target the same source row, so `id` is asserted on alongside the
/// CDC columns.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
struct EventLogRow {
    id: i64,
    value: String,
    cdc_operation: String,
    cdc_lsn: u64,
}

const TX_ORDER_SELECT: &str = concat!(
    "SELECT id, value, cdc_operation, cdc_lsn ",
    "FROM \"test_tx__order\" ",
    "ORDER BY id, cdc_lsn",
);

/// MT-only: verifies that updates from separately committed transactions
/// arrive with strictly increasing `cdc_lsn` matching Postgres commit order.
///
/// RMT collapses the event log under `FINAL`, so this ordering check has no
/// analog on the RMT side.
#[tokio::test(flavor = "multi_thread")]
async fn sequential_transactions_preserve_commit_order_merge_tree() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: one row, two database connections ---
    let mut database_1 = spawn_source_database().await;
    let mut database_2 = database_1.duplicate().await;
    let table_name = test_table_name("tx_order");

    let table_id = database_1
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create tx_order test table");

    let publication_name = "test_pub_clickhouse_tx_order";
    database_1
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create tx_order publication");

    database_1
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('original')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert initial tx_order row");

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db
            .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
            .await,
    );

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database_1.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 2)]).await;

    // --- WHEN: two transactions commit sequentially on separate connections ---
    let tx_a = database_1.begin_transaction().await;
    tx_a.run_sql(&format!(
        "UPDATE {} SET value = 'update_a' WHERE id = 1",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to execute update_a");
    tx_a.commit_transaction().await;

    let tx_b = database_2.begin_transaction().await;
    tx_b.run_sql(&format!(
        "UPDATE {} SET value = 'update_b' WHERE id = 1",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to execute update_b");
    tx_b.commit_transaction().await;

    event_notify.notified().await;

    let rows: Vec<EventLogRow> = clickhouse_db.query(TX_ORDER_SELECT).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: three rows on id=1 with strictly increasing LSNs ---
    assert_eq!(rows.len(), 3, "expected INSERT + two UPDATEs");

    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].value, "original");
    assert_eq!(rows[0].cdc_operation, "INSERT");
    assert_eq!(rows[0].cdc_lsn, 0);

    assert_eq!(rows[1].id, 1);
    assert_eq!(rows[1].value, "update_a");
    assert_eq!(rows[1].cdc_operation, "UPDATE");
    assert!(rows[1].cdc_lsn > 0);

    assert_eq!(rows[2].id, 1);
    assert_eq!(rows[2].value, "update_b");
    assert_eq!(rows[2].cdc_operation, "UPDATE");
    assert!(rows[2].cdc_lsn > rows[1].cdc_lsn, "update_b must have a higher LSN than update_a");
}
