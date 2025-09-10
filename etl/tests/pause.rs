#![cfg(feature = "test-utils")]

use etl::destination::memory::MemoryDestination;
use etl::replication::client::PgReplicationClient;
use etl::test_utils::database::spawn_source_database;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_users_data, setup_test_database_schema};
use etl::types::{EventType, PipelineId};
use etl_postgres::replication::slots::get_slot_name;
use etl_postgres::replication::worker::WorkerType;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn pause_stops_replication_and_resume_catches_up() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Start and wait for initial sync to complete.
    let sync_done = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            etl::state::table::TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    sync_done.notified().await;

    // Produce one row and wait to ensure streaming is active.
    let first_insert_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;
    first_insert_notify.notified().await;

    // Capture current flush LSN from the apply worker slot.
    let client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();
    let slot_name = get_slot_name(pipeline_id, WorkerType::Apply).unwrap();
    let before_pause_lsn = client
        .get_slot(&slot_name)
        .await
        .unwrap()
        .confirmed_flush_lsn;

    // Pause the pipeline and insert more rows.
    pipeline.pause_tx().pause().unwrap();

    // Count only INSERT events seen so far to avoid over-counting BEGIN/RELATION/COMMIT.
    let before_inserts = destination
        .get_events()
        .await
        .into_iter()
        .filter(|e| matches!(e, etl::types::Event::Insert(_)))
        .count() as u64;

    let rows_to_insert = 5;
    insert_users_data(
        &mut database,
        &database_schema.users_schema().name,
        2..=(rows_to_insert + 1),
    )
    .await;

    // Wait a bit to ensure that if replication were active, events would arrive and LSN would advance.
    sleep(Duration::from_secs(2)).await;

    // Assert destination did not receive the new events while paused.
    let after_inserts = destination
        .get_events()
        .await
        .into_iter()
        .filter(|e| matches!(e, etl::types::Event::Insert(_)))
        .count() as u64;
    assert_eq!(after_inserts, before_inserts);

    // Assert confirmed_flush_lsn did not advance while paused.
    let during_pause_lsn = client
        .get_slot(&slot_name)
        .await
        .unwrap()
        .confirmed_flush_lsn;
    assert_eq!(during_pause_lsn, before_pause_lsn);

    // Resume and wait for the pending events to be applied.
    let expected_total = before_inserts + rows_to_insert as u64;
    let catch_up_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, expected_total)])
        .await;
    pipeline.pause_tx().resume().unwrap();
    catch_up_notify.notified().await;

    // Confirm the slot advanced after resume.
    let after_resume_lsn = client
        .get_slot(&slot_name)
        .await
        .unwrap()
        .confirmed_flush_lsn;
    assert!(after_resume_lsn > before_pause_lsn);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_toggle_multiple_times() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Start and wait for initial sync to complete.
    let sync_done = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            etl::state::table::TableReplicationPhaseType::SyncDone,
        )
        .await;
    pipeline.start().await.unwrap();
    sync_done.notified().await;

    // Produce a first row and await delivery as baseline.
    let first_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;
    first_notify.notified().await;

    // Helper to count only inserts.
    let count_inserts = |events: Vec<etl::types::Event>| -> u64 {
        events
            .into_iter()
            .filter(|e| matches!(e, etl::types::Event::Insert(_)))
            .count() as u64
    };

    // Toggle 1: pause, insert, ensure no delivery, resume and catch up.
    pipeline.pause_tx().pause().unwrap();
    let before = count_inserts(destination.get_events().await);
    insert_users_data(&mut database, &database_schema.users_schema().name, 2..=3).await;
    sleep(Duration::from_secs(1)).await;
    let during = count_inserts(destination.get_events().await);
    assert_eq!(during, before);
    let after_resume_total = before + 2;
    let resume_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, after_resume_total)])
        .await;
    pipeline.pause_tx().resume().unwrap();
    resume_notify.notified().await;

    // Toggle 2: pause again, insert more, verify blocked, then resume and catch up.
    pipeline.pause_tx().pause().unwrap();
    let before2 = count_inserts(destination.get_events().await);
    insert_users_data(&mut database, &database_schema.users_schema().name, 4..=5).await;
    sleep(Duration::from_secs(1)).await;
    let during2 = count_inserts(destination.get_events().await);
    assert_eq!(during2, before2);
    let after_resume_total2 = before2 + 2;
    let resume_notify2 = destination
        .wait_for_events_count(vec![(EventType::Insert, after_resume_total2)])
        .await;
    pipeline.pause_tx().resume().unwrap();
    resume_notify2.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn start_paused_blocks_initial_copy_until_resume() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    // Seed source table with rows to be copied during initial sync.
    let rows_to_copy = 3u64;
    insert_users_data(
        &mut database,
        &database_schema.users_schema().name,
        1..=(rows_to_copy as usize),
    )
    .await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Pause before starting the pipeline, so initial copy should not progress.
    pipeline.pause_tx().pause().unwrap();

    // Start the pipeline.
    pipeline.start().await.unwrap();

    // Give some time; initial copy must not write any rows while paused.
    sleep(Duration::from_secs(1)).await;
    let table_rows = destination.get_table_rows().await;
    let total_rows_copied: usize = table_rows.values().map(|v| v.len()).sum();
    assert_eq!(
        total_rows_copied as u64, 0,
        "no rows should be copied while paused"
    );

    // Resume and wait for sync completion, then assert rows were copied.
    let sync_done = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            etl::state::table::TableReplicationPhaseType::SyncDone,
        )
        .await;
    pipeline.pause_tx().resume().unwrap();
    sync_done.notified().await;

    let table_rows = destination.get_table_rows().await;
    let total_rows_copied: usize = table_rows.values().map(|v| v.len()).sum();
    assert_eq!(total_rows_copied as u64, rows_to_copy);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn start_paused_blocks_streaming_until_resume() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Pause before starting.
    pipeline.pause_tx().pause().unwrap();

    // Start pipeline; initial copy has nothing to copy.
    pipeline.start().await.unwrap();

    // Insert events while paused, verify none are delivered.
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=3).await;
    sleep(Duration::from_secs(1)).await;
    let inserts_during_pause = destination
        .get_events()
        .await
        .into_iter()
        .filter(|e| matches!(e, etl::types::Event::Insert(_)))
        .count();
    assert_eq!(inserts_during_pause, 0);

    // Resume and wait for delivery of the 3 inserts.
    let notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 3)])
        .await;
    pipeline.pause_tx().resume().unwrap();
    notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
}
