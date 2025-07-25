use etl::destination::memory::MemoryDestination;
use etl::error::ErrorKind;
use etl::failpoints::{
    START_TABLE_SYNC_AFTER_DATA_SYNC_ERROR, START_TABLE_SYNC_AFTER_DATA_SYNC_PANIC,
};
use etl::pipeline::PipelineId;
use etl::state::store::notify::NotifyingStateStore;
use etl::state::table::TableReplicationPhaseType;
use fail::FailScenario;
use rand::random;
use telemetry::init_test_tracing;

use etl::test_utils::database::spawn_database;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};

// TODO: add more tests with fault injection.

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_handles_table_sync_worker_panic_during_data_sync() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_DATA_SYNC_PANIC, "panic").unwrap();

    init_test_tracing();

    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let state_store = NotifyingStateStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    // We register the interest in waiting for both table syncs to have started.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We stop and inspect errors.
    let err = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(err.kinds().len(), 2);
    assert_eq!(err.kinds()[0], ErrorKind::TableSyncWorkerPanic);
    assert_eq!(err.kinds()[1], ErrorKind::TableSyncWorkerPanic);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_handles_table_sync_worker_error_during_data_sync() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_DATA_SYNC_ERROR, "return").unwrap();

    init_test_tracing();

    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let state_store = NotifyingStateStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    // We register the interest in waiting for both table syncs to have started.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We stop and inspect errors.
    let err = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(err.kinds().len(), 2);
    assert_eq!(err.kinds()[0], ErrorKind::Unknown);
    assert_eq!(err.kinds()[1], ErrorKind::Unknown);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_is_consistent_after_data_sync_threw_an_error() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_DATA_SYNC_ERROR, "return").unwrap();

    init_test_tracing();

    let mut database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=rows_inserted,
        false,
    )
    .await;

    let state_store = NotifyingStateStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table sync phases.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    let err = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(err.kinds().len(), 2);

    // We disable the failpoint.
    fail::remove(START_TABLE_SYNC_AFTER_DATA_SYNC_ERROR);

    // Restart pipeline with normal state store to verify recovery.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    // Wait for schema reception and table sync completion.
    let schemas_notify = destination.wait_for_n_schemas(2).await;

    // Register notifications for table sync phases.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    schemas_notify.notified().await;
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    let orders_table_rows = table_rows.get(&database_schema.orders_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);

    // Verify table schemas were correctly stored.
    let table_schemas = destination.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert_eq!(table_schemas[0], database_schema.orders_schema());
    assert_eq!(table_schemas[1], database_schema.users_schema());
}
