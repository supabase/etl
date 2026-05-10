#![cfg(feature = "test-utils")]

use std::time::Duration;

use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::spawn_source_database;
use etl::test_utils::delayed_confirm_destination::DelayedConfirmDestination;
use etl::test_utils::notifying_store::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_schema::{
    TableSelection, insert_users_data, setup_test_database_schema,
};
use etl::types::{EventType, PipelineId};
use etl_postgres::replication::slots::EtlReplicationSlot;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::types::PgLsn;

/// Helper: query the confirmed_flush_lsn for a given replication slot.
async fn get_confirmed_flush_lsn(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Option<PgLsn> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .expect("Failed to query pg_replication_slots");

    row.map(|r| r.get::<_, PgLsn>(0))
}

/// When the destination doesn't confirm, the Postgres replication slot's
/// confirmed_flush_lsn does NOT advance to the commit LSN.
#[tokio::test(flavor = "multi_thread")]
async fn destination_controlled_flush_delays_slot_advancement() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let delayed_dest = DelayedConfirmDestination::new(store.clone());

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
        .try_into()
        .unwrap();

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        delayed_dest.clone(),
    );

    let table_ready_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();
    table_ready_notify.notified().await;

    // Insert 1 row and wait for the insert event.
    let users_table_name = database_schema.users_schema().name.clone();
    let events_notify = delayed_dest
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    insert_users_data(&mut database, &users_table_name, 1..=1).await;
    events_notify.notified().await;

    // Wait briefly for the commit to be processed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Do NOT confirm — the slot should not have advanced to the commit LSN.
    let commit_lsns = delayed_dest.get_received_commit_lsns().await;
    assert!(
        !commit_lsns.is_empty(),
        "should have tracked at least one commit LSN"
    );
    let max_commit_lsn = commit_lsns.iter().copied().max().unwrap();

    // Shutdown WITHOUT confirming.
    pipeline.shutdown_and_wait().await.unwrap();
    database.wait_for_slot_inactive(&apply_slot_name).await;

    // The slot should NOT have advanced to the commit LSN.
    let slot_lsn =
        get_confirmed_flush_lsn(database.client.as_ref().unwrap(), &apply_slot_name)
            .await
            .expect("slot should still exist after shutdown");

    assert!(
        slot_lsn < max_commit_lsn,
        "slot confirmed_flush_lsn ({slot_lsn:?}) should not have advanced to \
         the commit LSN ({max_commit_lsn:?}) since destination never confirmed"
    );
}

/// Confirm the batch, shutdown, verify the slot advances to the commit LSN.
#[tokio::test(flavor = "multi_thread")]
async fn destination_controlled_flush_confirm_advances_slot() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let delayed_dest = DelayedConfirmDestination::new(store.clone());

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
        .try_into()
        .unwrap();

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        delayed_dest.clone(),
    );

    let table_ready_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();
    table_ready_notify.notified().await;

    // Insert 1 row and wait for the insert event.
    let users_table_name = database_schema.users_schema().name.clone();
    let events_notify = delayed_dest
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    insert_users_data(&mut database, &users_table_name, 1..=1).await;
    events_notify.notified().await;

    // Wait briefly for the commit to be processed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Confirm the batch.
    delayed_dest.confirm_all().await;

    // Wait for the apply loop to process confirmation and send status update.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Get the commit LSN before shutdown.
    let commit_lsns = delayed_dest.get_received_commit_lsns().await;
    assert!(
        !commit_lsns.is_empty(),
        "should have tracked at least one commit LSN"
    );
    let max_commit_lsn = commit_lsns.iter().copied().max().unwrap();

    // Shutdown pipeline.
    pipeline.shutdown_and_wait().await.unwrap();
    database.wait_for_slot_inactive(&apply_slot_name).await;

    // The slot should have advanced to at least the commit LSN since we confirmed.
    let slot_lsn =
        get_confirmed_flush_lsn(database.client.as_ref().unwrap(), &apply_slot_name)
            .await
            .expect("slot should still exist after shutdown");

    assert!(
        slot_lsn >= max_commit_lsn,
        "after confirming, the slot LSN ({slot_lsn:?}) should be at least \
         the commit LSN ({max_commit_lsn:?})"
    );
}

/// Crash-resume test: kill pipeline without confirming, restart, verify data is replayed.
#[tokio::test(flavor = "multi_thread")]
async fn destination_controlled_flush_crash_resume_no_data_loss() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let delayed_dest = DelayedConfirmDestination::new(store.clone());

    let pipeline_id: PipelineId = random();

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        delayed_dest.clone(),
    );

    let table_ready_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();
    table_ready_notify.notified().await;

    // Insert 1 row and wait for the insert event.
    let users_table_name = database_schema.users_schema().name.clone();
    let events_notify = delayed_dest
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    insert_users_data(&mut database, &users_table_name, 1..=1).await;
    events_notify.notified().await;

    // DO NOT confirm — simulate a crash where the batch was not durably written.
    pipeline.shutdown_and_wait().await.unwrap();

    let apply_slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
        .try_into()
        .unwrap();
    database.wait_for_slot_inactive(&apply_slot_name).await;

    // Create a NEW pipeline with a FRESH DelayedConfirmDestination (same pipeline_id).
    let delayed_dest_2 = DelayedConfirmDestination::new(store.clone());

    let mut pipeline_2 = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        delayed_dest_2.clone(),
    );

    let table_ready_notify_2 = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // Wait for the insert event to be replayed on the new destination.
    let events_notify_2 = delayed_dest_2
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    pipeline_2.start().await.unwrap();
    table_ready_notify_2.notified().await;
    events_notify_2.notified().await;

    // Confirm on the new destination so shutdown can complete cleanly.
    delayed_dest_2.confirm_all().await;
    pipeline_2.shutdown_and_wait().await.unwrap();

    // Verify the new destination received the insert event (data was replayed, not lost).
    let events = delayed_dest_2.get_events().await;
    let insert_count = events
        .iter()
        .filter(|e| matches!(e, etl::types::Event::Insert(_)))
        .count();
    assert!(
        insert_count >= 1,
        "the new destination should have received at least 1 insert event \
         (replayed from unconfirmed slot position), got {insert_count}"
    );
}

/// Multiple transactions with selective confirmation: confirm only the first,
/// verify the slot advances to the first commit but not the second.
#[tokio::test(flavor = "multi_thread")]
async fn destination_controlled_flush_selective_confirmation() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let delayed_dest = DelayedConfirmDestination::new(store.clone());

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
        .try_into()
        .unwrap();

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        delayed_dest.clone(),
    );

    let table_ready_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();
    table_ready_notify.notified().await;

    let users_table_name = database_schema.users_schema().name.clone();

    // --- Transaction 1: Insert 1 row ---
    let events_notify_1 = delayed_dest
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    insert_users_data(&mut database, &users_table_name, 1..=1).await;
    events_notify_1.notified().await;

    // Wait briefly for commit to be tracked.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Record commit LSN from transaction 1.
    let commit_lsns_after_tx1 = delayed_dest.get_received_commit_lsns().await;
    assert!(
        !commit_lsns_after_tx1.is_empty(),
        "should have tracked at least one commit LSN after transaction 1"
    );
    let first_commit_lsn = *commit_lsns_after_tx1.last().unwrap();

    // Confirm transaction 1.
    delayed_dest.confirm_all().await;

    // Wait for apply loop to process the confirmation.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Transaction 2: Insert 1 more row ---
    let events_notify_2 = delayed_dest
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    insert_users_data(&mut database, &users_table_name, 2..=2).await;
    events_notify_2.notified().await;

    // Wait briefly for commit to be tracked.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // DO NOT confirm transaction 2.
    let commit_lsns_after_tx2 = delayed_dest.get_received_commit_lsns().await;
    assert!(
        commit_lsns_after_tx2.len() >= 2,
        "should have tracked at least 2 commit LSNs, got {}",
        commit_lsns_after_tx2.len()
    );
    let second_commit_lsn = *commit_lsns_after_tx2.last().unwrap();

    // Shutdown pipeline (transaction 2 is unconfirmed).
    pipeline.shutdown_and_wait().await.unwrap();
    database.wait_for_slot_inactive(&apply_slot_name).await;

    // Query slot's confirmed_flush_lsn.
    let slot_lsn =
        get_confirmed_flush_lsn(database.client.as_ref().unwrap(), &apply_slot_name)
            .await
            .expect("slot should still exist after shutdown");

    // The slot should have advanced past the first commit LSN (which was confirmed).
    assert!(
        slot_lsn >= first_commit_lsn,
        "slot LSN ({slot_lsn:?}) should be at least the first commit LSN \
         ({first_commit_lsn:?}) since it was confirmed"
    );

    // The slot should NOT have advanced to the second commit LSN (which was not confirmed).
    assert!(
        slot_lsn < second_commit_lsn,
        "slot LSN ({slot_lsn:?}) should be less than the second commit LSN \
         ({second_commit_lsn:?}) since it was not confirmed"
    );
}
