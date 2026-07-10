use etl::{
    error::ErrorKind,
    pipeline::PipelineId,
    store::{StateStore, TableRetryPolicy, TableState, TableStateType},
    test_utils::{
        database::spawn_source_database,
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

    // --- GIVEN: a healthy pipeline whose destination fails on shutdown ---
    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    destination
        .inject_fault(
            FaultyOp::Shutdown,
            FaultAction::fail_dispatch(
                ErrorKind::DestinationQueryFailed,
                "injected shutdown failure",
            ),
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

    // --- WHEN: the pipeline shuts down ---
    let result = pipeline.shutdown_and_wait().await;

    // --- THEN: the injected destination shutdown error is propagated and shutdown
    // was still invoked ---
    let err = result.unwrap_err();
    assert!(err.kinds().contains(&ErrorKind::DestinationQueryFailed));
    assert!(destination.shutdown_called().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_dispatch_failure_keeps_table_restartable_until_retry() {
    init_test_tracing();

    // --- GIVEN: a pipeline that has copied a table to Ready ---
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

    // --- WHEN: a resync starts and the destination refuses the drop at dispatch
    // ---
    destination
        .inject_fault(
            FaultyOp::DropTableForCopy,
            FaultAction::fail_dispatch(
                ErrorKind::DestinationConnectionFailed,
                "injected drop dispatch failure",
            ),
        )
        .await;

    let users_errored = store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored.notified().await;

    // --- THEN: the table errors with a timed retry and nothing was torn down ---
    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::TimedRetry { .. }, .. }
    ));
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert!(store.get_latest_table_schemas().await.contains_key(&table_id));

    // The drop was refused before the inner destination ran, so its rows are
    // untouched.
    assert_eq!(memory_destination.table_rows().await.get(&table_id).unwrap().len(), initial_rows);

    // --- THEN: the timed retry drops and recopies the table cleanly ---
    let users_ready_again = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    users_ready_again.notified().await;

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).unwrap().len(), initial_rows);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_result_failure_keeps_table_restartable_until_retry() {
    init_test_tracing();

    // --- GIVEN: a pipeline that has copied a table to Ready ---
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

    // --- WHEN: a resync starts and the drop result reports failure after the inner
    // destination applied it ---
    destination
        .inject_fault(
            FaultyOp::DropTableForCopy,
            FaultAction::fail_result(
                ErrorKind::DestinationConnectionFailed,
                "injected drop result failure",
            ),
        )
        .await;

    let users_errored = store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored.notified().await;

    // --- THEN: the table errors with a timed retry and ETL state was not cleared
    // ---
    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::TimedRetry { .. }, .. }
    ));
    assert!(store.get_latest_table_schemas().await.contains_key(&table_id));

    // Lost-response semantics: the inner destination applied the drop, but the
    // apply loop observed a failure, so the wrapper acknowledged nothing.
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert!(!memory_destination.table_rows().await.contains_key(&table_id));

    // --- THEN: the timed retry replays the drop and recopies the table cleanly ---
    let users_ready_again = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    users_ready_again.notified().await;

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).unwrap().len(), initial_rows);

    pipeline.shutdown_and_wait().await.unwrap();
}
