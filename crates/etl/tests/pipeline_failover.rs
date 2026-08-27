use etl::{
    event::EventType,
    pipeline::PipelineId,
    store::TableStateType,
    test_utils::{
        database::{
            local_pg_read_replica_connection_config, spawn_source_database,
            wait_for_replication_slot_flush_lsn,
        },
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::PipelineBuilder,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, build_expected_users_inserts, insert_users_data,
            setup_test_database_schema,
        },
    },
};
use etl_config::shared::{PgConnectionConfig, TableSyncCopyConfig};
use etl_postgres::{
    below_version,
    slots::EtlReplicationSlot,
    tokio::test_utils::{PgDatabase, try_connect_to_pg_database},
    version::POSTGRES_17,
};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::Client;

use crate::support::read_replica::{wait_for_read_replica_replay, wait_until};

/// Returns whether the test database supports failover-enabled logical slots.
fn supports_replication_slot_failover(database: &PgDatabase<Client>) -> bool {
    if below_version!(database.server_version(), POSTGRES_17) {
        eprintln!("Skipping test: PostgreSQL 17+ required for failover replication slots");
        false
    } else {
        true
    }
}

/// Returns the failover setting for a replication slot, or [`None`] when the
/// slot does not exist.
async fn replication_slot_failover(database: &PgDatabase<Client>, slot_name: &str) -> Option<bool> {
    database
        .client
        .as_ref()
        .unwrap()
        .query_opt("select failover from pg_replication_slots where slot_name = $1", &[&slot_name])
        .await
        .unwrap()
        .map(|row| row.get(0))
}

/// Returns whether a synchronized failover slot is promotion-ready.
async fn synchronized_failover_slot_is_ready(
    replica_config: &PgConnectionConfig,
    slot_name: &str,
) -> Result<bool, tokio_postgres::Error> {
    let Ok((client, _)) = try_connect_to_pg_database(replica_config).await else {
        return Ok(false);
    };
    let row = client
        .query_opt(
            "select failover, synced, temporary, invalidation_reason from pg_replication_slots \
             where slot_name = $1",
            &[&slot_name],
        )
        .await?;

    let Some(row) = row else {
        return Ok(false);
    };

    let failover: bool = row.get(0);
    let synced: bool = row.get(1);
    let temporary: bool = row.get(2);
    let invalidation_reason: Option<String> = row.get(3);

    Ok(failover && synced && !temporary && invalidation_reason.is_none())
}

/// Waits until the read replica has a promotion-ready copy of `slot_name`.
async fn wait_for_synchronized_failover_slot(
    database: &PgDatabase<Client>,
    replica_config: &PgConnectionConfig,
    slot_name: &str,
) {
    // Advance the logical slot through a fresh running-xacts snapshot before
    // asking the standby to build its synchronized slot. This keeps the
    // primary slot's catalog horizon from falling behind the standby's.
    let synchronization_lsn = database.log_standby_snapshot().await.unwrap();
    wait_for_replication_slot_flush_lsn(
        database.client.as_ref().unwrap(),
        slot_name,
        synchronization_lsn,
    )
    .await;
    wait_for_read_replica_replay(replica_config, synchronization_lsn).await;

    // Wake the slot-sync worker so the test does not depend on its adaptive
    // idle backoff, which can otherwise add up to one minute.
    let (replica_client, _) = try_connect_to_pg_database(replica_config).await.unwrap();
    let config_reloaded: bool =
        replica_client.query_one("select pg_reload_conf()", &[]).await.unwrap().get(0);
    assert!(config_reloaded);

    wait_until("failover slot synchronization", || {
        synchronized_failover_slot_is_ready(replica_config, slot_name)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_creates_failover_apply_and_table_sync_slots() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    if !supports_replication_slot_failover(&database) {
        return;
    }

    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;
    insert_users_data(&mut database, &users_schema.name, 1..=1).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_replication_slot_failover(true)
    .build();

    let sync_done_notify =
        store.notify_on_table_state_type(table_id, TableStateType::SyncDone).await;
    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let held_copy = destination.hold_next(FaultyOp::WriteTableRows).await;

    pipeline.start().await.unwrap();

    // Hold the copy response while both replication slots exist, avoiding a
    // race with table-sync cleanup.
    held_copy.wait_reached().await;

    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let table_sync_slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into().unwrap();
    assert_eq!(replication_slot_failover(&database, &apply_slot_name).await, Some(true));
    assert_eq!(replication_slot_failover(&database, &table_sync_slot_name).await, Some(true));

    let catchup_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;

    held_copy.release_ok();
    insert_users_data(&mut database, &users_schema.name, 2..=2).await;

    catchup_notify.notified().await;
    sync_done_notify.notified().await;

    let insert_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 3)])
        .await;

    insert_users_data(&mut database, &users_schema.name, 3..=3).await;

    ready_notify.notified().await;
    insert_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert_eq!(replication_slot_failover(&database, &table_sync_slot_name).await, None);

    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).map_or(0, Vec::len), 1);

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    let expected_events =
        build_expected_users_inserts(2, &users_schema, vec![("user_2", 2), ("user_3", 3)]);
    assert_events_equal(insert_events, &expected_events);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_enables_failover_on_an_existing_apply_slot() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    if !supports_replication_slot_failover(&database) {
        return;
    }

    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;
    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();

    // Model a pipeline whose apply slot predates the failover setting.
    let replication_client =
        etl::postgres::client::PgReplicationClient::connect(database.config.clone()).await.unwrap();
    replication_client.create_slot(&apply_slot_name, false).await.unwrap();
    assert_eq!(replication_slot_failover(&database, &apply_slot_name).await, Some(false));

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_table_sync_copy_config(TableSyncCopyConfig::SkipAllTables)
    .with_replication_slot_failover(true)
    .build();

    let sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    sync_complete_notify.notified().await;

    let insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    insert_users_data(&mut database, &users_schema.name, 1..=1).await;

    ready_notify.notified().await;
    insert_notify.notified().await;

    assert_eq!(replication_slot_failover(&database, &apply_slot_name).await, Some(true));

    pipeline.shutdown_and_wait().await.unwrap();

    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).map_or(0, Vec::len), 0);
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    let expected_events = build_expected_users_inserts(1, &users_schema, vec![("user_1", 1)]);
    assert_events_equal(insert_events, &expected_events);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_failover_slot_is_synced_to_read_replica() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    if !supports_replication_slot_failover(&database) {
        return;
    }

    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_table_sync_copy_config(TableSyncCopyConfig::SkipAllTables)
    .with_replication_slot_failover(true)
    .build();

    let sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    sync_complete_notify.notified().await;

    let insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    insert_users_data(&mut database, &users_schema.name, 1..=1).await;

    ready_notify.notified().await;
    insert_notify.notified().await;

    let replica_config = local_pg_read_replica_connection_config(&database.config);
    wait_for_synchronized_failover_slot(&database, &replica_config, &apply_slot_name).await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert!(synchronized_failover_slot_is_ready(&replica_config, &apply_slot_name).await.unwrap());
}
