use etl::destination::memory::MemoryDestination;
use etl::pipeline::{PipelineError, PipelineId};
use etl::state::store::notify::NotifyingStateStore;
use etl::state::table::TableReplicationPhaseType;
use etl::workers::base::{WorkerWaitError};
use postgres::schema::ColumnSchema;
use postgres::tokio::test_utils::TableModification;
use rand::random;
use telemetry::init_test_tracing;
use tokio_postgres::types::Type;

use etl::test_utils::pipeline::{create_pipeline};
use etl::test_utils::state_store::{FaultConfig, FaultInjectingStateStore, FaultType};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{
    TableSelection, setup_test_database_schema,
};
use etl::test_utils::database::spawn_database;

// TODO: find a way to inject errors in a way that is predictable.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn pipeline_handles_table_sync_worker_panic() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let fault_config = FaultConfig {
        store_table_replication_state: Some(FaultType::Panic),
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(NotifyingStateStore::new(), fault_config);
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
        .get_inner()
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .get_inner()
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

// TODO: find a way to inject errors in a way that is predictable.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn pipeline_handles_table_sync_worker_error() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let fault_config = FaultConfig {
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(NotifyingStateStore::new(), fault_config);
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
        .get_inner()
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .get_inner()
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

// TODO: find a way to inject errors in a way that is predictable.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn table_schema_copy_retries_after_data_sync_failure() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let state_store = NotifyingStateStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Configure state store to fail during data sync.
    let fault_config = FaultConfig {
        ..Default::default()
    };
    let failing_state_store = FaultInjectingStateStore::wrap(state_store.clone(), fault_config);

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        failing_state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table sync phases.
    let users_state_notify = failing_state_store
        .get_inner()
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = failing_state_store
        .get_inner()
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

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_copy_retries_after_finished_copy_failure() {
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

    // We wait for two table schemas to be received.
    let schemas_notify = destination.wait_for_n_schemas(2).await;
    // We wait for both table states to be in sync done.
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

    // We check that the states are correctly set.
    let table_replication_states = state_store.get_table_replication_states().await;
    assert_eq!(table_replication_states.len(), 2);
    assert_eq!(
        table_replication_states
            .get(&database_schema.users_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::SyncDone
    );
    assert_eq!(
        table_replication_states
            .get(&database_schema.orders_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::SyncDone
    );

    // We check that the table schemas have been stored.
    let table_schemas = destination.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert_eq!(table_schemas[0], database_schema.orders_schema());
    assert_eq!(table_schemas[1], database_schema.users_schema());

    // We assume now that the schema of a table changes before sync done is performed.
    database
        .alter_table(
            database_schema.orders_schema().name.clone(),
            &[TableModification::AddColumn {
                name: "date",
                data_type: "integer",
            }],
        )
        .await
        .unwrap();
    let mut extended_orders_table_schema = database_schema.orders_schema().clone();
    extended_orders_table_schema
        .column_schemas
        .push(ColumnSchema {
            name: "date".to_string(),
            typ: Type::INT4,
            modifier: -1,
            nullable: true,
            primary: false,
        });

    // We recreate a pipeline, assuming the other one was stopped, using the same state and destination.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    pipeline.shutdown_and_wait().await.unwrap();

    // We check that the table schemas haven't changed.
    let table_schemas = destination.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert_eq!(table_schemas[0], database_schema.orders_schema());
    assert_eq!(table_schemas[1], database_schema.users_schema());
}