//! Temporary-slot lifecycle and recovery boundaries.

use std::time::{Duration, Instant};

use etl::{
    config::{ReplicationSlotConfig, ReplicationSlotPersistence},
    data::{Cell, TableRow},
    error::ErrorKind,
    event::EventType,
    pipeline::PipelineId,
    postgres::client::PgReplicationClient,
    store::{StateStore, TableState, TableStateType, WorkerType},
    test_utils::{
        database::{WalsenderTermination, spawn_source_database, terminate_active_walsender},
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, wait_for_pipeline_error},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, assert_table_rows_equal_ignoring_size,
            build_expected_users_inserts, insert_users_data, setup_test_database_schema,
        },
    },
};
use etl_postgres::{slots::EtlReplicationSlot, tokio::test_utils::PgDatabase};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::{Client, types::PgLsn};

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
    assert_eq!(error.kind(), ErrorKind::ReplicationSlotInvalidated);
    assert!(matches!(
        store.get_table_states().await.get(&users.id).unwrap().as_type(),
        TableStateType::SyncDone | TableStateType::Ready
    ));
    wait_until("temporary apply slot cleanup", || async {
        Ok(slot_is_temporary(&database, &slot_name).await.is_none())
    })
    .await;
}

/// Restarts with retained state after either a clean stop or slot loss during
/// copy, checking that a deleted source row is removed by a fresh copy.
async fn assert_temporary_restart_resyncs(lose_slot_during_copy: bool) {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users = schema.users_schema();
    insert_users_data(&mut database, &users.name, 1..=3).await;
    let pipeline_id: PipelineId = random();
    let apply_slot: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let copy_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, users.id).try_into().unwrap();
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

    if lose_slot_during_copy {
        let held_copy = destination.hold_next(FaultyOp::WriteTableRows).await;
        pipeline.start().await.unwrap();
        held_copy.wait_reached().await;
        let terminated = terminate_active_walsender(database.client.as_ref().unwrap(), &apply_slot)
            .await
            .unwrap();
        assert!(matches!(terminated, WalsenderTermination::Terminated { .. }));
        let error = wait_for_pipeline_error(&pipeline).await;
        assert_eq!(error.kind(), ErrorKind::ReplicationSlotInvalidated);
        assert_eq!(
            store.get_table_states().await.get(&users.id).unwrap().as_type(),
            TableStateType::DataSync
        );

        // wait() returned on the apply error without joining table-sync
        // workers. Observe their session cleanup before releasing the old
        // response or starting another run against the same store.
        wait_until("temporary copy slot cleanup", || async {
            Ok(slot_is_temporary(&database, &copy_slot).await.is_none())
        })
        .await;
        held_copy.release_ok();
        insert_users_data(&mut database, &users.name, 4..=4).await;
    } else {
        let sync_done = store.notify_on_table_sync_complete(users.id).await;
        let ready = store.notify_on_table_state_type(users.id, TableStateType::Ready).await;
        pipeline.start().await.unwrap();
        sync_done.notified().await;
        let delivered = destination
            .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, users.id, 4)])
            .await;
        insert_users_data(&mut database, &users.name, 4..=4).await;
        ready.notified().await;
        delivered.notified().await;
        pipeline.shutdown_and_wait().await.unwrap();
    }

    wait_until("temporary pipeline slot cleanup", || async {
        Ok(slot_is_temporary(&database, &apply_slot).await.is_none()
            && slot_is_temporary(&database, &copy_slot).await.is_none())
    })
    .await;
    assert!(!destination.was_table_dropped_for_copy(users.id).await);
    database.delete_values(users.name.clone(), &["name"], &["'user_1'"], "and").await.unwrap();
    insert_users_data(&mut database, &users.name, 5..=5).await;

    // A checkpoint from the old slot must not survive startup resync.
    let stale_checkpoint = PgLsn::from(u64::MAX);
    store.upsert_replication_checkpoint(WorkerType::Apply, stale_checkpoint).await.unwrap();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_replication_slot_persistence(ReplicationSlotPersistence::Temporary)
    .build();
    let sync_done = store.notify_on_table_sync_complete(users.id).await;
    pipeline.start().await.unwrap();
    sync_done.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    assert!(destination.was_table_dropped_for_copy(users.id).await);
    let mut rows = destination.get_table_rows().await.remove(&users.id).unwrap();
    rows.sort_by_key(|row| match row.values()[0] {
        Cell::I64(id) => id,
        _ => unreachable!("Users primary keys are int8"),
    });
    let expected = (2..=5)
        .map(|id| {
            TableRow::new(vec![
                Cell::I64(i64::from(id)),
                Cell::String(format!("user_{id}")),
                Cell::I32(id),
            ])
        })
        .collect::<Vec<_>>();
    assert_table_rows_equal_ignoring_size(&rows, &expected);
    assert_ne!(
        store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(),
        Some(stale_checkpoint)
    );
}

/// A clean restart reuses the store and destination while rebuilding data.
#[tokio::test(flavor = "multi_thread")]
async fn temporary_pipeline_restarts_with_existing_state() {
    assert_temporary_restart_resyncs(false).await;
}

/// Slot loss during copy stops the run, and a later start rebuilds its tables.
#[tokio::test(flavor = "multi_thread")]
async fn temporary_pipeline_recovers_after_slot_loss_during_copy() {
    assert_temporary_restart_resyncs(true).await;
}

/// Another session's temporary slot uses bounded retries without changing
/// stored state, and a later start succeeds once that session releases it.
#[tokio::test(flavor = "multi_thread")]
async fn temporary_pipeline_waits_for_an_existing_temporary_slot() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users = schema.users_schema();
    insert_users_data(&mut database, &users.name, 1..=1).await;
    let pipeline_id: PipelineId = random();
    let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let store = NotifyingStore::new();
    let old_state = TableState::SyncDone { lsn: PgLsn::from(1), table_decoding_state: None };
    store.update_table_state(users.id, old_state.clone()).await.unwrap();
    let old_checkpoint = PgLsn::from(u64::MAX);
    store.upsert_replication_checkpoint(WorkerType::Apply, old_checkpoint).await.unwrap();
    let config = ReplicationSlotConfig {
        persistence: ReplicationSlotPersistence::Temporary,
        ..Default::default()
    };
    let mut owner = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    owner.create_slot(&slot_name, &config).await.unwrap();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_replication_slot_persistence(ReplicationSlotPersistence::Temporary)
    .with_retry_config(1000, 1)
    .build();
    let started = Instant::now();
    pipeline.start().await.unwrap();
    let error = wait_for_pipeline_error(&pipeline).await;
    assert_eq!(error.kind(), ErrorKind::ReplicationSlotInUse);
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(store.get_table_states().await.get(&users.id), Some(&old_state));
    assert_eq!(
        store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(),
        Some(old_checkpoint)
    );
    assert_eq!(slot_is_temporary(&database, &slot_name).await, Some(true));

    drop(owner);
    wait_until("previous temporary slot cleanup", || async {
        Ok(slot_is_temporary(&database, &slot_name).await.is_none())
    })
    .await;
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_replication_slot_persistence(ReplicationSlotPersistence::Temporary)
    .with_retry_config(1000, 5)
    .build();
    let sync_done = store.notify_on_table_sync_complete(users.id).await;
    pipeline.start().await.unwrap();
    sync_done.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    assert_eq!(destination.get_table_rows().await.get(&users.id).unwrap().len(), 1);
    assert_ne!(
        store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(),
        Some(old_checkpoint)
    );
}
