use std::time::Duration;

use etl::{
    data::Cell,
    error::ErrorKind,
    event::{Event, EventType},
    pipeline::PipelineId,
    schema::TableId,
    store::{StateStore, TableRetryPolicy, TableState, TableStateType},
    test_utils::{
        database::{replication_slot_state, spawn_source_database, wait_for_new_walsender},
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::{FaultAction, FaultyOp},
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_users_data, setup_test_database_schema},
    },
};
use etl_postgres::slots::EtlReplicationSlot;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::types::PgLsn;

/// Returns the commit LSNs of recorded insert events for the table, in order.
fn table_insert_commit_lsns(events: &[Event], table_id: TableId) -> Vec<PgLsn> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                Some(insert.commit_lsn)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_shutdown_error_is_returned_by_shutdown_and_wait() {
    init_test_tracing();

    // GIVEN: a healthy pipeline whose destination fails on shutdown
    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    destination
        .inject_fault(
            FaultyOp::Shutdown,
            FaultAction::reject(ErrorKind::DestinationQueryFailed, "injected shutdown failure"),
        )
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    // WHEN: the pipeline shuts down
    let result = pipeline.shutdown_and_wait().await;

    // THEN: the injected shutdown error surfaces and shutdown was invoked
    let err = result.unwrap_err();
    assert!(err.kinds().contains(&ErrorKind::DestinationQueryFailed));
    assert!(destination.shutdown_called().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_retry_reselects_relation_snapshots_after_ambiguous_write() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();
    users_ready_notify.notified().await;

    // The inner destination applies the transaction, but the apply worker sees
    // a timed-retriable failure and reconnects from its durable start LSN.
    destination
        .inject_fault(
            FaultyOp::WriteEvents,
            FaultAction::fail_after_write(
                ErrorKind::DestinationTimeout,
                "injected ambiguous streaming failure",
            ),
        )
        .await;

    let (_, active_pid) =
        replication_slot_state(database.client.as_ref().unwrap(), &apply_slot_name).await;
    let old_pid = active_pid.expect("apply walsender should be active");

    let replayed_events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 2),
            EventCondition::TableCount(EventType::Insert, table_id, 2),
        ])
        .await;

    // Both schema versions occur in one transaction. On retry, the first
    // Relation must resolve the pre-DDL schema instead of reusing the
    // post-DDL runtime schema cached by the failed attempt.
    let transaction = database.begin_transaction().await;
    transaction
        .insert_values(users_schema.name.clone(), &["name", "age"], &[&"before", &1])
        .await
        .unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} drop column age",
            users_schema.name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.insert_values(users_schema.name.clone(), &["name"], &[&"after"]).await.unwrap();
    transaction.commit_transaction().await;

    wait_for_new_walsender(database.client.as_ref().unwrap(), &apply_slot_name, old_pid).await;
    replayed_events_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation_schemas = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) if relation.replicated_table_schema.id() == table_id => {
                Some(&relation.replicated_table_schema)
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(relation_schemas.len(), 2);
    assert_eq!(
        relation_schemas[0].column_schemas().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "name", "age"]
    );
    assert_eq!(
        relation_schemas[1].column_schemas().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert!(relation_schemas[0].inner().snapshot_id < relation_schemas[1].inner().snapshot_id);

    let insert_values = events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                Some(insert.table_row.values().to_vec())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        insert_values,
        vec![
            vec![Cell::I64(1), Cell::String("before".to_owned()), Cell::I32(1)],
            vec![Cell::I64(2), Cell::String("after".to_owned())],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_rejection_keeps_table_restartable_until_retry() {
    init_test_tracing();

    // GIVEN: a pipeline that has copied a table to Ready
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let initial_rows = 2;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=initial_rows).await;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    // WHEN: a resync starts and the destination rejects the drop
    destination
        .inject_fault(
            FaultyOp::DropTableForCopy,
            FaultAction::reject(ErrorKind::DestinationConnectionFailed, "injected drop rejection"),
        )
        .await;

    let users_errored_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored_notify.notified().await;

    // THEN: the table errors with a timed retry and nothing was torn down
    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::TimedRetry { .. }, .. }
    ));
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert!(store.get_latest_table_schemas().await.contains_key(&table_id));

    // The drop never ran on the inner destination, so its rows are untouched.
    assert_eq!(memory_destination.table_rows().await.get(&table_id).unwrap().len(), initial_rows);

    // THEN: the timed retry drops and recopies the table cleanly
    let users_ready_again_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    users_ready_again_notify.notified().await;

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).unwrap().len(), initial_rows);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_failure_after_write_keeps_table_restartable_until_retry() {
    init_test_tracing();

    // GIVEN: a pipeline that has copied a table to Ready
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let initial_rows = 2;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=initial_rows).await;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    // WHEN: a resync starts and the drop fails after being applied
    destination
        .inject_fault(
            FaultyOp::DropTableForCopy,
            FaultAction::fail_after_write(
                ErrorKind::DestinationConnectionFailed,
                "injected drop failure after write",
            ),
        )
        .await;

    let users_errored_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored_notify.notified().await;

    // THEN: the table errors with a timed retry and ETL state was not cleared
    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::TimedRetry { .. }, .. }
    ));
    assert!(store.get_latest_table_schemas().await.contains_key(&table_id));

    // The inner destination applied the drop; the apply loop saw a failure.
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert!(!memory_destination.table_rows().await.contains_key(&table_id));

    // THEN: the timed retry replays the drop and recopies the table cleanly
    let users_ready_again_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    users_ready_again_notify.notified().await;

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).unwrap().len(), initial_rows);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_pending_write_events_before_destination_shutdown() {
    init_test_tracing();

    // GIVEN: a streaming pipeline whose next write_events response is held
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    let hold = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    hold.wait_reached().await;

    // WHEN: the pipeline shuts down while the write response is withheld
    let mut shutdown_task = tokio::spawn(pipeline.shutdown_and_wait());

    // THEN: shutdown waits on the pending response instead of proceeding
    if let Ok(result) = tokio::time::timeout(Duration::from_secs(2), &mut shutdown_task).await {
        panic!("shutdown completed while the write response was withheld: {result:?}");
    }
    assert!(!destination.shutdown_called().await);

    // WHEN: the held response is released
    hold.release_ok();

    // THEN: shutdown completes and the write was acknowledged before it
    shutdown_task.await.unwrap().unwrap();
    assert!(destination.shutdown_called().await);

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    assert_eq!(grouped_events.get(&(EventType::Insert, table_id)).map_or(0, Vec::len), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_disconnect_with_write_held_until_after_reconnect_replays_without_loss() {
    init_test_tracing();

    // GIVEN: a streaming pipeline whose next write_events response is held
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    let hold = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    hold.wait_reached().await;

    // WHEN: the apply connection dies while the write response is withheld
    let client = database.client.as_ref().unwrap();
    let (flush_lsn_at_kill, active_pid) = replication_slot_state(client, &apply_slot_name).await;
    let old_pid = active_pid.expect("apply walsender should be active");

    client.query_one("select pg_terminate_backend($1)", &[&old_pid]).await.unwrap();

    wait_for_new_walsender(client, &apply_slot_name, old_pid).await;

    // WHEN: the held response is released only after the reconnect
    let replay_recorded_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;

    hold.release_ok();

    // THEN: the insert replays because the acknowledgement never reached the
    // old apply loop
    replay_recorded_notify.notified().await;

    let commit_lsns = table_insert_commit_lsns(&destination.get_events().await, table_id);
    assert_eq!(commit_lsns.len(), 2);
    assert_eq!(commit_lsns[0], commit_lsns[1]);
    let first_commit_lsn = commit_lsns[0];

    // THEN: the persisted checkpoint never advanced past the unacknowledged write
    assert!(flush_lsn_at_kill < first_commit_lsn);

    // THEN: streaming continues without loss after the replay
    let second_insert_notify = destination
        .notify_on_events(move |events| {
            table_insert_commit_lsns(events, table_id)
                .last()
                .is_some_and(|last| *last > first_commit_lsn)
        })
        .await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 2..=2).await;

    second_insert_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let final_lsns = table_insert_commit_lsns(&destination.get_events().await, table_id);
    assert_eq!(final_lsns.iter().filter(|lsn| **lsn == first_commit_lsn).count(), 2);
    assert_eq!(final_lsns.iter().filter(|lsn| **lsn > first_commit_lsn).count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_disconnect_with_write_released_before_reconnect_recovers_without_loss() {
    init_test_tracing();

    // GIVEN: a streaming pipeline whose next write_events response is held
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    let hold = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    hold.wait_reached().await;

    // WHEN: the apply connection dies and the response releases before reconnect
    let client = database.client.as_ref().unwrap();
    let (_, active_pid) = replication_slot_state(client, &apply_slot_name).await;
    let old_pid = active_pid.expect("apply walsender should be active");

    let first_insert_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    client.query_one("select pg_terminate_backend($1)", &[&old_pid]).await.unwrap();

    hold.release_ok();

    first_insert_notify.notified().await;
    let first_commit_lsn = *table_insert_commit_lsns(&destination.get_events().await, table_id)
        .first()
        .expect("released insert should be recorded");

    wait_for_new_walsender(client, &apply_slot_name, old_pid).await;

    // THEN: streaming continues without loss, replaying the insert at most once
    let second_insert_notify = destination
        .notify_on_events(move |events| {
            table_insert_commit_lsns(events, table_id)
                .last()
                .is_some_and(|last| *last > first_commit_lsn)
        })
        .await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 2..=2).await;

    second_insert_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let final_lsns = table_insert_commit_lsns(&destination.get_events().await, table_id);
    let first_count = final_lsns.iter().filter(|lsn| **lsn == first_commit_lsn).count();
    assert!(
        (1..=2).contains(&first_count),
        "insert must survive with at most one replay, got {first_count} copies"
    );
    assert_eq!(final_lsns.iter().filter(|lsn| **lsn > first_commit_lsn).count(), 1);
}
