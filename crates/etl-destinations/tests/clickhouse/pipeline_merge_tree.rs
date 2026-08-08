//! MergeTree-only integration tests. These verify event-log semantics
//! (`cdc_operation` + `cdc_lsn` + `cdc_tx_ordinal`) on the MergeTree engine;
//! the parameterized spine in `pipeline.rs` covers current-state behavior on
//! both engines.

use etl::{
    event::EventType,
    pipeline::PipelineId,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::EventCondition,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::test_utils::setup_clickhouse_database;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::{clickhouse::current_state_query, crypto::install_crypto_provider};

/// MergeTree event-log row with source CDC ordering metadata.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
struct EventLogRow {
    id: i64,
    value: String,
    cdc_operation: String,
    cdc_lsn: u64,
    cdc_tx_ordinal: u64,
}

const TX_ORDER_SELECT: &str = concat!(
    "select id, value, cdc_operation, cdc_lsn, cdc_tx_ordinal ",
    "from \"test_tx__order\" ",
    "order by id, cdc_lsn, cdc_tx_ordinal",
);

/// MergeTree-only: verifies that updates from separately committed transactions
/// arrive with strictly increasing `cdc_lsn` matching Postgres commit order.
///
/// ReplacingMergeTree collapses the event log under `FINAL`, so this ordering
/// check has no analog on the ReplacingMergeTree side.
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

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    let mut pipeline = create_pipeline(
        &database_1.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_sync_complete_notify.notified().await;

    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Update, table_id, 2)])
        .await;

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

    events_notify.notified().await;

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

/// Current user row projected from MergeTree event history.
#[derive(clickhouse::Row, serde::Deserialize, Debug)]
struct CurrentRow {
    id: i64,
    value: String,
}

/// MergeTree preserves same-transaction order when primary-key changes move a
/// row away from a key and then back to it.
#[tokio::test(flavor = "multi_thread")]
async fn same_transaction_primary_key_change_preserves_order_merge_tree() {
    init_test_tracing();
    install_crypto_provider();

    let mut database = spawn_source_database().await;
    let table_name = test_table_name("same_tx_pk_change");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create same_tx_pk_change test table");

    let publication_name = "test_pub_clickhouse_same_tx_pk_change";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create same_tx_pk_change publication");
    database
        .run_sql(&format!(
            "insert into {} (value) values ('original')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert initial same_tx_pk_change row");

    let clickhouse_db = setup_clickhouse_database().await;
    clickhouse_db
        .db_client()
        .query(
            "create table \"test_same__tx__pk__change\" (
                \"id\" Int64,
                \"value\" String,
                \"cdc_operation\" String,
                \"cdc_lsn\" UInt64
            ) engine = MergeTree() order by tuple()",
        )
        .execute()
        .await
        .expect("Failed to create legacy MergeTree table");
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db
            .build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree)
            .await,
    );
    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    let mut pipeline = create_pipeline(
        &database.config,
        random::<PipelineId>(),
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_sync_complete_notify.notified().await;

    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Update, table_id, 3)])
        .await;
    let tx = database.begin_transaction().await;
    tx.run_sql(&format!(
        "update {} set value = 'intermediate' where id = 1",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to update the value");
    tx.run_sql(&format!(
        "update {} set id = 2, value = 'moved' where id = 1",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to update the primary key");
    tx.run_sql(&format!(
        "update {} set id = 1, value = 'final' where id = 2",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to reuse the original primary key");
    tx.commit_transaction().await;

    events_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let event_rows: Vec<EventLogRow> = clickhouse_db
        .query(
            "select id, value, cdc_operation, cdc_lsn, cdc_tx_ordinal from \
             \"test_same__tx__pk__change\" order by cdc_lsn, cdc_tx_ordinal, id, cdc_operation",
        )
        .await;
    let current_rows: Vec<CurrentRow> = clickhouse_db
        .query(&current_state_query(
            ClickHouseEngine::MergeTree,
            "test_same__tx__pk__change",
            "id, value",
            &["id"],
            "id",
        ))
        .await;

    assert_eq!(event_rows.len(), 6);
    assert_eq!(event_rows[0].cdc_operation, "INSERT");
    assert_eq!(event_rows[0].cdc_lsn, 0);
    assert_eq!(event_rows[0].cdc_tx_ordinal, 0);

    let streaming_lsn = event_rows[1].cdc_lsn;
    assert!(event_rows[1..].iter().all(|row| row.cdc_lsn == streaming_lsn));

    assert_eq!(event_rows[1].id, 1);
    assert_eq!(event_rows[1].value, "intermediate");
    assert_eq!(event_rows[1].cdc_operation, "UPDATE");
    assert!(event_rows[1].cdc_tx_ordinal < event_rows[2].cdc_tx_ordinal);

    assert_eq!(event_rows[2].id, 1);
    assert_eq!(event_rows[2].cdc_operation, "DELETE");
    assert_eq!(event_rows[2].cdc_tx_ordinal, event_rows[3].cdc_tx_ordinal);

    assert_eq!(event_rows[3].id, 2);
    assert_eq!(event_rows[3].value, "moved");
    assert_eq!(event_rows[3].cdc_operation, "UPDATE");
    assert!(event_rows[3].cdc_tx_ordinal < event_rows[4].cdc_tx_ordinal);

    assert_eq!(event_rows[4].id, 1);
    assert_eq!(event_rows[4].value, "final");
    assert_eq!(event_rows[4].cdc_operation, "UPDATE");
    assert_eq!(event_rows[4].cdc_tx_ordinal, event_rows[5].cdc_tx_ordinal);

    assert_eq!(event_rows[5].id, 2);
    assert_eq!(event_rows[5].cdc_operation, "DELETE");

    assert_eq!(current_rows.len(), 1);
    assert_eq!(current_rows[0].id, 1);
    assert_eq!(current_rows[0].value, "final");
}
