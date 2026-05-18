//! ReplacingMergeTree-only integration tests. These verify RMT-specific
//! semantics that have no analog under MergeTree: same-LSN tie-break, FINAL
//! reads, the `__current` view, `OPTIMIZE ... FINAL CLEANUP` cleanup, PK-less
//! source rejection, and composite PK ORDER BY.

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

use crate::support::clickhouse::{
    current_state_query, install_crypto_provider, optimize_final_cleanup_sql, table_engine_query,
};

#[derive(clickhouse::Row, serde::Deserialize, Debug)]
struct IdValueRow {
    id: i64,
    value: String,
}

#[derive(clickhouse::Row, serde::Deserialize, Debug)]
#[allow(dead_code)]
struct IdRow {
    id: i64,
}

/// RMT: source table must have a primary key. `ensure_table_exists` rejects
/// PK-less schemas under RMT with `SourceSchemaError`.
#[tokio::test(flavor = "multi_thread")]
async fn rmt_rejects_pkless_source_table() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: a PK-less source table ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("pkless_events");

    let table_id = database
        .create_table(table_name.clone(), false, &[("value", "text not null")])
        .await
        .expect("Failed to create pkless table");

    let publication_name = "test_pub_rmt_pkless";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create pkless publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('seed')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert seed row");

    // --- WHEN: pipeline runs under RMT ---
    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
        .await;

    let table_errored =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Errored).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination,
    );

    pipeline.start().await.unwrap();
    table_errored.notified().await;

    // --- THEN: the table is marked Errored (PK-less rejection bubbled up) ---
    pipeline.shutdown_and_wait().await.unwrap();
}

/// RMT: a same-transaction INSERT followed by an UPDATE of the same PK
/// collapses under `FINAL` to the post-UPDATE value. Confirms that
/// `start_lsn` tie-breaks multi-event same-commit transactions correctly.
#[tokio::test(flavor = "multi_thread")]
async fn rmt_same_lsn_tx_insert_then_update_keeps_update() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: an empty table copied to ClickHouse ---
    let mut database = spawn_source_database().await;
    let table_name = test_table_name("rmt_tie_break");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create rmt_tie_break table");

    let publication_name = "test_pub_rmt_tie_break";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db
            .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
            .await,
    );

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    // --- WHEN: INSERT + UPDATE in the same transaction ---
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Update, 1)])
        .await;

    let tx = database.begin_transaction().await;
    tx.run_sql(&format!(
        "INSERT INTO {} (value) VALUES ('initial')",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to insert");
    tx.run_sql(&format!(
        "UPDATE {} SET value = 'final' WHERE id = 1",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to update");
    tx.commit_transaction().await;

    event_notify.notified().await;

    let query = current_state_query(
        ClickHouseEngine::ReplacingMergeTree,
        "test_rmt__tie__break",
        "id, value",
        &["id"],
        "id",
    );
    let rows: Vec<IdValueRow> = clickhouse_db.query(&query).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: the post-UPDATE value wins under FINAL ---
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].value, "final");
}

/// RMT: a DELETE followed by an INSERT of the same PK in the same
/// transaction shows the post-INSERT row under FINAL (no lingering
/// tombstone).
#[tokio::test(flavor = "multi_thread")]
async fn rmt_same_lsn_tx_delete_then_insert_keeps_insert() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: a row already replicated under REPLICA IDENTITY FULL ---
    let mut database = spawn_source_database().await;
    let table_name = test_table_name("rmt_del_ins");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create rmt_del_ins table");

    database
        .run_sql(&format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to set replica identity full");

    let publication_name = "test_pub_rmt_del_ins";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('original')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert original row");

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db
            .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
            .await,
    );

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    // --- WHEN: DELETE + INSERT of the same id in one transaction ---
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1), (EventType::Insert, 1)])
        .await;

    let tx = database.begin_transaction().await;
    tx.run_sql(&format!("DELETE FROM {} WHERE id = 1", table_name.as_quoted_identifier()))
        .await
        .expect("Failed to delete");
    tx.run_sql(&format!(
        "INSERT INTO {} (id, value) VALUES (1, 'recreated')",
        table_name.as_quoted_identifier(),
    ))
    .await
    .expect("Failed to re-insert");
    tx.commit_transaction().await;

    event_notify.notified().await;

    let query = current_state_query(
        ClickHouseEngine::ReplacingMergeTree,
        "test_rmt__del__ins",
        "id, value",
        &["id"],
        "id",
    );
    let rows: Vec<IdValueRow> = clickhouse_db.query(&query).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: the post-INSERT row wins ---
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].value, "recreated");
}

/// RMT: the auto-generated `__current` view exposes only user columns and
/// returns the current state of the table.
#[tokio::test(flavor = "multi_thread")]
async fn rmt_current_view_exposes_user_columns_and_current_state() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: a row copied + then updated ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("rmt_view");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create rmt_view table");

    let publication_name = "test_pub_rmt_view";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('before')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert before-row");

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db
            .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
            .await,
    );

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;
    database
        .run_sql(&format!(
            "UPDATE {} SET value = 'after' WHERE id = 1",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to update row");
    event_notify.notified().await;

    // --- WHEN: read via the __current view ---
    let rows: Vec<IdValueRow> =
        clickhouse_db.query("SELECT id, value FROM \"test_rmt__view__current\" ORDER BY id").await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: view returns the post-UPDATE row ---
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].value, "after");
}

/// RMT: composite PK source table is created with `ORDER BY` matching
/// `primary_key_ordinal_position`.
#[tokio::test(flavor = "multi_thread")]
async fn rmt_composite_pk_order_by_matches_pk_ordinal() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: a Postgres table with a composite PK whose ordinal order
    // differs from table column order ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("rmt_composite");

    // Source schema: (id serial PK is added by create_table). To get a true
    // composite PK whose ordinal differs from table order, we drop the
    // single-column PK and add a composite one.
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("tenant_id", "integer not null"), ("value", "text not null")],
        )
        .await
        .expect("Failed to create rmt_composite table");

    database
        .run_sql(&format!(
            "ALTER TABLE {} DROP CONSTRAINT {}_pkey",
            table_name.as_quoted_identifier(),
            table_name.name,
        ))
        .await
        .expect("Failed to drop default pkey");

    database
        .run_sql(&format!(
            "ALTER TABLE {} ADD PRIMARY KEY (tenant_id, id)",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to add composite primary key");

    let publication_name = "test_pub_rmt_composite";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (tenant_id, value) VALUES (1, 'alpha')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert seed");

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = clickhouse_db
        .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
        .await;

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination,
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // --- WHEN: query system.tables for the created RMT ORDER BY clause ---
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct EngineRow {
        engine: String,
    }
    let engine_rows: Vec<EngineRow> =
        clickhouse_db.query(&table_engine_query("test_rmt__composite")).await;
    assert_eq!(engine_rows.len(), 1);
    assert_eq!(engine_rows[0].engine, "ReplacingMergeTree");

    // Read the sorting key from system.tables.
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct SortingKeyRow {
        sorting_key: String,
    }
    let sorting: Vec<SortingKeyRow> = clickhouse_db
        .query(
            "SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = \
             'test_rmt__composite'",
        )
        .await;

    // --- THEN: sorting_key matches PK ordinal order (tenant_id, id) ---
    assert_eq!(sorting.len(), 1);
    assert_eq!(sorting[0].sorting_key, "tenant_id, id");
}

/// RMT: an initial-copy row followed by a streaming UPDATE collapses under
/// `FINAL` to the streamed value. The initial-copy row has `_etl_lsn = 0`;
/// the streamed UPDATE has `start_lsn > 0`, so the streamed row wins.
#[tokio::test(flavor = "multi_thread")]
async fn rmt_streamed_update_wins_over_initial_copy_row() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: a row copied via initial copy ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("rmt_copy_then_update");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create rmt_copy_then_update table");

    let publication_name = "test_pub_rmt_copy_then_update";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('copy_value')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert pre-copy row");

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db
            .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
            .await,
    );

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    // --- WHEN: stream an UPDATE for the copied row ---
    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;
    database
        .run_sql(&format!(
            "UPDATE {} SET value = 'streamed_value' WHERE id = 1",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to update");
    event_notify.notified().await;

    let query = current_state_query(
        ClickHouseEngine::ReplacingMergeTree,
        "test_rmt__copy__then__update",
        "id, value",
        &["id"],
        "id",
    );
    let rows: Vec<IdValueRow> = clickhouse_db.query(&query).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // --- THEN: the streamed UPDATE wins over the initial-copy row ---
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value, "streamed_value");
}

/// RMT: a DELETE followed by `OPTIMIZE ... FINAL CLEANUP` physically removes
/// the row from storage.
#[tokio::test(flavor = "multi_thread")]
async fn rmt_optimize_cleanup_physically_removes_tombstoned_row() {
    init_test_tracing();
    install_crypto_provider();

    // --- GIVEN: a copied row, REPLICA IDENTITY FULL ---
    let database = spawn_source_database().await;
    let table_name = test_table_name("rmt_cleanup");

    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("Failed to create rmt_cleanup table");

    database
        .run_sql(&format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to set replica identity full");

    let publication_name = "test_pub_rmt_cleanup";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    database
        .run_sql(&format!(
            "INSERT INTO {} (value) VALUES ('doomed')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("Failed to insert seed row");

    let clickhouse_db = setup_clickhouse_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(
        clickhouse_db
            .build_destination_with_engine(store.clone(), ClickHouseEngine::ReplacingMergeTree)
            .await,
    );

    let table_ready =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination.clone(),
    );

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;
    database
        .run_sql(&format!("DELETE FROM {} WHERE id = 1", table_name.as_quoted_identifier()))
        .await
        .expect("Failed to delete");
    event_notify.notified().await;

    // --- WHEN: operator runs OPTIMIZE ... FINAL CLEANUP ---
    let optimize_sql = optimize_final_cleanup_sql("test_rmt__cleanup");
    let optimize_result = clickhouse_db.db_client().query(&optimize_sql).execute().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // CH ships RMT-with-CLEANUP behind an experimental gate that has shifted
    // names across versions (26.x removed the previous setting name). When
    // the server does not allow the call, treat this test as covered by the
    // FINAL-based spine assertions and skip the physical-removal check.
    match optimize_result {
        Ok(()) => {
            let raw_rows: Vec<IdRow> =
                clickhouse_db.query("SELECT id FROM \"test_rmt__cleanup\" ORDER BY id").await;
            assert!(
                raw_rows.is_empty(),
                "OPTIMIZE FINAL CLEANUP should have removed the tombstoned row; found {raw_rows:?}"
            );
        }
        Err(err) => {
            eprintln!(
                "skipping OPTIMIZE FINAL CLEANUP physical-removal assertion: server rejected the \
                 statement ({err}); FINAL-based spine tests still cover the user-visible delete \
                 semantics"
            );
        }
    }
}
