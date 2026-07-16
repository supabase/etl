use std::time::Duration;

use etl::{
    error::ErrorKind,
    event::EventType,
    pipeline::PipelineId,
    store::{StateStore, TableRetryPolicy, TableState, TableStateType},
    test_utils::{
        database::spawn_source_database,
        event::group_events_by_type_and_table_id,
        faults::{FaultAction, FaultyOp},
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_users_data, setup_test_database_schema},
    },
};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

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

    let users_ready = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;

    // WHEN: the pipeline shuts down
    let result = pipeline.shutdown_and_wait().await;

    // THEN: the injected shutdown error surfaces and shutdown was invoked
    let err = result.unwrap_err();
    assert!(err.kinds().contains(&ErrorKind::DestinationQueryFailed));
    assert!(destination.shutdown_called().await);
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

    let users_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;

    // WHEN: a resync starts and the destination rejects the drop
    destination
        .inject_fault(
            FaultyOp::DropTableForCopy,
            FaultAction::reject(ErrorKind::DestinationConnectionFailed, "injected drop rejection"),
        )
        .await;

    let users_errored = store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored.notified().await;

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
    let users_ready_again = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    users_ready_again.notified().await;

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

    let users_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;

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

    let users_errored = store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored.notified().await;

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
    let users_ready_again = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    users_ready_again.notified().await;

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

    let users_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_ready.notified().await;

    let hold = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    hold.wait_reached().await;

    // WHEN: the pipeline shuts down while the write response is withheld
    let mut shutdown_task = tokio::spawn(pipeline.shutdown_and_wait());

    // THEN: shutdown waits on the pending response instead of proceeding
    let still_draining =
        tokio::time::timeout(Duration::from_secs(2), &mut shutdown_task).await.is_err();
    assert!(still_draining);
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
