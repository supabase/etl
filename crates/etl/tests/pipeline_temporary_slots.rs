//! Temporary-slot lifecycle and recovery boundaries.

use etl::{
    config::ReplicationSlotPersistence,
    error::ErrorKind,
    event::EventType,
    pipeline::PipelineId,
    postgres::client::PgReplicationClient,
    store::TableStateType,
    test_utils::{
        database::{WalsenderTermination, spawn_source_database, terminate_active_walsender},
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, wait_for_pipeline_error},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, build_expected_users_inserts, insert_users_data,
            setup_test_database_schema,
        },
    },
};
use etl_postgres::{slots::EtlReplicationSlot, tokio::test_utils::PgDatabase};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::Client;

use crate::support::read_replica::wait_until;

/// Returns slot persistence, or [`None`] after the slot is removed.
async fn slot_is_temporary(database: &PgDatabase<Client>, slot_name: &str) -> Option<bool> {
    database
        .client
        .as_ref()
        .unwrap()
        .query_opt("select temporary from pg_replication_slots where slot_name = $1", &[&slot_name])
        .await
        .unwrap()
        .map(|row| row.get(0))
}

/// Copy and streaming use temporary slots, and a fresh pipeline can reuse the
/// same id after shutdown while copying the current source data again.
#[tokio::test(flavor = "multi_thread")]
async fn temporary_pipeline_copies_streams_and_restarts_with_fresh_state() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users = schema.users_schema();
    insert_users_data(&mut database, &users.name, 1..=1).await;
    let pipeline_id: PipelineId = random();
    let apply_slot: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let copy_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, users.id).try_into().unwrap();

    for copied_rows in [1, 3] {
        let store = NotifyingStore::new();
        let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
        let mut pipeline = PipelineBuilder::new(
            database.config.clone(),
            pipeline_id,
            schema.publication_name(),
            store.clone(),
            destination.clone(),
        )
        .with_replication_slot_persistence(ReplicationSlotPersistence::Temporary)
        .build();
        let sync_done = store.notify_on_table_state_type(users.id, TableStateType::SyncDone).await;
        let ready = store.notify_on_table_state_type(users.id, TableStateType::Ready).await;
        let held_copy = destination.hold_next(FaultyOp::WriteTableRows).await;

        pipeline.start().await.unwrap();
        held_copy.wait_reached().await;
        assert_eq!(slot_is_temporary(&database, &apply_slot).await, Some(true));
        assert_eq!(slot_is_temporary(&database, &copy_slot).await, Some(true));

        let catchup = destination
            .wait_for_all_events(vec![EventCondition::TableCount(
                EventType::Insert,
                users.id,
                u64::try_from(copied_rows + 1).unwrap(),
            )])
            .await;
        held_copy.release_ok();
        insert_users_data(&mut database, &users.name, copied_rows + 1..=copied_rows + 1).await;
        catchup.notified().await;
        sync_done.notified().await;

        let live_insert = destination
            .wait_for_all_events(vec![EventCondition::TableCount(
                EventType::Insert,
                users.id,
                u64::try_from(copied_rows + 2).unwrap(),
            )])
            .await;
        insert_users_data(&mut database, &users.name, copied_rows + 2..=copied_rows + 2).await;
        ready.notified().await;
        live_insert.notified().await;
        pipeline.shutdown_and_wait().await.unwrap();

        wait_until("temporary pipeline slot cleanup", || async {
            let row = database
                .client
                .as_ref()
                .unwrap()
                .query_one(
                    "select count(*) from pg_replication_slots where slot_name in ($1, $2)",
                    &[&apply_slot, &copy_slot],
                )
                .await?;
            Ok(row.get::<_, i64>(0) == 0)
        })
        .await;

        let rows = destination.get_table_rows().await;
        assert_eq!(rows.get(&users.id).unwrap().len(), copied_rows);
        let events = destination.get_events().await;
        let events = group_events_by_type_and_table_id(&events);
        let inserts = events.get(&(EventType::Insert, users.id)).unwrap();
        let expected = if copied_rows == 1 {
            vec![("user_2", 2), ("user_3", 3)]
        } else {
            vec![("user_4", 4), ("user_5", 5)]
        };
        assert_events_equal(
            inserts,
            &build_expected_users_inserts(
                i64::try_from(copied_rows + 1).unwrap(),
                &users,
                expected,
            ),
        );
    }
}

/// Selecting temporary persistence must not silently reuse a permanent slot.
#[tokio::test(flavor = "multi_thread")]
async fn temporary_pipeline_rejects_an_existing_apply_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let pipeline_id: PipelineId = random();
    let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let mut client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    client.create_slot(&slot_name, &Default::default()).await.unwrap();
    let store = NotifyingStore::new();
    let destination = MemoryDestination::new(store.clone());
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        schema.publication_name(),
        store,
        destination,
    )
    .with_replication_slot_persistence(ReplicationSlotPersistence::Temporary)
    .build();

    pipeline.start().await.unwrap();
    let error = wait_for_pipeline_error(&pipeline).await;
    assert_eq!(error.kind(), ErrorKind::ConfigError);
    assert_eq!(slot_is_temporary(&database, &slot_name).await, Some(false));
}

/// Losing the apply session stops the pipeline instead of pairing a new slot
/// with table state from the previous slot's WAL history.
#[tokio::test(flavor = "multi_thread")]
async fn temporary_pipeline_rejects_stale_state_after_connection_loss() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users = schema.users_schema();
    let pipeline_id: PipelineId = random();
    let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let store = NotifyingStore::new();
    let destination = MemoryDestination::new(store.clone());
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        schema.publication_name(),
        store.clone(),
        destination,
    )
    .with_replication_slot_persistence(ReplicationSlotPersistence::Temporary)
    .build();
    let sync_done = store.notify_on_table_state_type(users.id, TableStateType::SyncDone).await;

    pipeline.start().await.unwrap();
    sync_done.notified().await;
    let terminated =
        terminate_active_walsender(database.client.as_ref().unwrap(), &slot_name).await.unwrap();
    assert!(matches!(terminated, WalsenderTermination::Terminated { .. }));
    let error = wait_for_pipeline_error(&pipeline).await;
    assert_eq!(error.kind(), ErrorKind::InvalidState);
    assert_eq!(slot_is_temporary(&database, &slot_name).await, None);
}
