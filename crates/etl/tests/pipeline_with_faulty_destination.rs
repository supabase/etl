use etl::{
    error::ErrorKind,
    pipeline::PipelineId,
    store::TableStateType,
    test_utils::{
        database::spawn_source_database,
        faults::{FaultAction, FaultyOp},
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, setup_test_database_schema},
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
