use etl::destination::memory::MemoryDestination;
use etl::failpoints::START_TABLE_SYNC_AFTER_DATA_SYNC;
use etl::pipeline::{PipelineError, PipelineId};
use etl::state::store::notify::NotifyingStateStore;
use etl::state::table::TableReplicationPhaseType;
use etl::workers::base::WorkerWaitError;
use fail::FailScenario;
use rand::random;
use telemetry::init_test_tracing;

use etl::test_utils::database::spawn_database;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, setup_test_database_schema};

/*
Tests that we want to add:
TBD
 */

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_handles_table_sync_worker_panic_during_data_sync() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_DATA_SYNC, "panic").unwrap();

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
    match pipeline.shutdown_and_wait().await.err().unwrap() {
        PipelineError::OneOrMoreWorkersFailed(err) => {
            assert!(matches!(
                err.0.as_slice(),
                [
                    WorkerWaitError::WorkerPanicked(_),
                    WorkerWaitError::WorkerPanicked(_)
                ]
            ));
        }
        other => panic!("Expected TableSyncWorkersFailed error, but got: {other:?}"),
    }
}

// TODO: inject the failure via fail-rs.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn pipeline_handles_table_sync_worker_error() {
    let _scenario = FailScenario::setup();

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

    // Register notifications for when table sync is started.
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
    match pipeline.shutdown_and_wait().await.err().unwrap() {
        PipelineError::OneOrMoreWorkersFailed(err) => {
            assert!(matches!(
                err.0.as_slice(),
                [
                    WorkerWaitError::WorkerPanicked(_),
                    WorkerWaitError::WorkerPanicked(_)
                ]
            ));
        }
        other => panic!("Expected TableSyncWorkersFailed error, but got: {other:?}"),
    }
}

// TODO: inject the failure via fail-rs.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn table_schema_copy_retries_after_data_sync_failure() {
    let _scenario = FailScenario::setup();

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

    // This result could be an error or not based on if we manage to shut down before the error is
    // thrown. This is a shortcoming of this fault injection implementation, we have plans to fix
    // this in future PRs.
    // TODO: assert error once better failure injection is implemented.
    let _ = pipeline.shutdown_and_wait().await;

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
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    schemas_notify.notified().await;
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify table replication states.
    let table_replication_states = state_store.get_table_replication_states().await;
    assert_eq!(table_replication_states.len(), 2);
    assert_eq!(
        table_replication_states
            .get(&database_schema.users_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::FinishedCopy
    );
    assert_eq!(
        table_replication_states
            .get(&database_schema.orders_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::FinishedCopy
    );

    // Verify table schemas were correctly stored.
    let table_schemas = destination.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert_eq!(table_schemas[0], database_schema.orders_schema());
    assert_eq!(table_schemas[1], database_schema.users_schema());
}
