use etl::conversions::table_row::TableRow;
use etl::conversions::Cell;
use etl::v2::conversions::event::{Event, EventType, InsertEvent};
use etl::v2::pipeline::{PipelineError, PipelineId};
use etl::v2::state::table::TableReplicationPhaseType;
use etl::v2::workers::base::WorkerWaitError;
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres::tokio::test_utils::{id_column_schema, PgDatabase, TableModification};
use rand::random;
use std::ops::RangeInclusive;
use telemetry::init_test_tracing;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, GenericClient};

use crate::common::database::{spawn_database, test_table_name};
use crate::common::destination_v2::TestDestination;
use crate::common::event::{group_events_by_type, group_events_by_type_and_table_id};
use crate::common::pipeline_v2::create_pipeline;
use crate::common::state_store::{
    FaultConfig, FaultInjectingStateStore, FaultType, TestStateStore,
};

#[derive(Debug, Clone, Copy)]
enum TableSelection {
    Both,
    #[allow(dead_code)]
    UsersOnly,
    OrdersOnly,
}

#[derive(Debug)]
struct TestDatabaseSchema {
    users_table_schema: Option<TableSchema>,
    orders_table_schema: Option<TableSchema>,
    publication_name: String,
}

impl TestDatabaseSchema {
    /// Returns a clone of the users table schema, panicking if it doesn't exist.
    fn users_schema(&self) -> TableSchema {
        self.users_table_schema
            .clone()
            .expect("Users table schema not found")
    }

    /// Returns a clone of the orders table schema, panicking if it doesn't exist.
    fn orders_schema(&self) -> TableSchema {
        self.orders_table_schema
            .clone()
            .expect("Orders table schema not found")
    }
}

async fn setup_test_database_schema<G: GenericClient>(
    database: &PgDatabase<G>,
    selection: TableSelection,
) -> TestDatabaseSchema {
    let mut tables_to_publish = Vec::new();
    let mut users_table_schema = None;
    let mut orders_table_schema = None;

    if matches!(selection, TableSelection::Both | TableSelection::UsersOnly) {
        let users_table_name = test_table_name("users");
        let users_table_id = database
            .create_table(
                users_table_name.clone(),
                &[("name", "text not null"), ("age", "integer not null")],
            )
            .await
            .expect("Failed to create users table");

        tables_to_publish.push(users_table_name.clone());

        users_table_schema = Some(TableSchema::new(
            users_table_id,
            users_table_name,
            vec![
                id_column_schema(),
                ColumnSchema {
                    name: "name".to_string(),
                    typ: Type::TEXT,
                    modifier: -1,
                    nullable: false,
                    primary: false,
                },
                ColumnSchema {
                    name: "age".to_string(),
                    typ: Type::INT4,
                    modifier: -1,
                    nullable: false,
                    primary: false,
                },
            ],
        ));
    }

    if matches!(selection, TableSelection::Both | TableSelection::OrdersOnly) {
        let orders_table_name = test_table_name("orders");
        let orders_table_id = database
            .create_table(
                orders_table_name.clone(),
                &[("description", "text not null")],
            )
            .await
            .expect("Failed to create orders table");

        tables_to_publish.push(orders_table_name.clone());

        orders_table_schema = Some(TableSchema::new(
            orders_table_id,
            orders_table_name,
            vec![
                id_column_schema(),
                ColumnSchema {
                    name: "description".to_string(),
                    typ: Type::TEXT,
                    modifier: -1,
                    nullable: false,
                    primary: false,
                },
            ],
        ));
    }

    // Create publication for selected tables
    let publication_name = "test_pub";
    database
        .create_publication(publication_name, &tables_to_publish)
        .await
        .expect("Failed to create publication");

    TestDatabaseSchema {
        users_table_schema,
        orders_table_schema,
        publication_name: publication_name.to_owned(),
    }
}

async fn insert_mock_data(
    database: &mut PgDatabase<Client>,
    users_table_name: &TableName,
    orders_table_name: &TableName,
    range: RangeInclusive<usize>,
    use_transaction: bool,
) {
    if use_transaction {
        let mut transaction = database.begin_transaction().await;

        // Insert users with deterministic data.
        for i in range.clone() {
            transaction
                .insert_values(
                    users_table_name.clone(),
                    &["name", "age"],
                    &[&format!("user_{i}"), &(i as i32)],
                )
                .await
                .expect("Failed to insert users");
        }

        // Insert orders with deterministic data.
        for i in range {
            transaction
                .insert_values(
                    orders_table_name.clone(),
                    &["description"],
                    &[&format!("description_{i}")],
                )
                .await
                .expect("Failed to insert orders");
        }

        // Commit the transaction.
        transaction.commit_transaction().await;
    } else {
        // Insert users with deterministic data.
        for i in range.clone() {
            database
                .insert_values(
                    users_table_name.clone(),
                    &["name", "age"],
                    &[&format!("user_{i}"), &(i as i32)],
                )
                .await
                .expect("Failed to insert users");
        }

        // Insert orders with deterministic data.
        for i in range {
            database
                .insert_values(
                    orders_table_name.clone(),
                    &["description"],
                    &[&format!("description_{i}")],
                )
                .await
                .expect("Failed to insert orders");
        }
    }
}

async fn get_users_age_sum_from_rows(destination: &TestDestination, table_id: TableId) -> i32 {
    let mut actual_sum = 0;

    let tables_rows = destination.get_table_rows().await;
    let table_rows = tables_rows.get(&table_id).unwrap();
    for table_row in table_rows {
        if let Cell::I32(age) = &table_row.values[2] {
            actual_sum += age;
        }
    }

    actual_sum
}

fn get_n_integers_sum(n: usize) -> i32 {
    ((n * (n + 1)) / 2) as i32
}

fn build_expected_users_inserts(
    mut starting_id: i64,
    users_table_id: TableId,
    expected_rows: Vec<(&str, i32)>,
) -> Vec<Event> {
    let mut events = Vec::new();

    for (name, age) in expected_rows {
        events.push(Event::Insert(InsertEvent {
            table_id: users_table_id,
            row: TableRow {
                values: vec![
                    Cell::I64(starting_id),
                    Cell::String(name.to_owned()),
                    Cell::I32(age),
                ],
            },
        }));

        starting_id += 1;
    }

    events
}

fn build_expected_orders_inserts(
    mut starting_id: i64,
    orders_table_id: TableId,
    expected_rows: Vec<&str>,
) -> Vec<Event> {
    let mut events = Vec::new();

    for name in expected_rows {
        events.push(Event::Insert(InsertEvent {
            table_id: orders_table_id,
            row: TableRow {
                values: vec![Cell::I64(starting_id), Cell::String(name.to_owned())],
            },
        }));

        starting_id += 1;
    }

    events
}

// TODO: find a way to inject errors in a way that is predictable.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_with_table_sync_worker_panic() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let fault_config = FaultConfig {
        store_table_replication_state: Some(FaultType::Panic),
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(TestStateStore::new(), fault_config);
    let destination = TestDestination::new();

    let pipeline_id: PipelineId = random();
    // We start the pipeline from scratch.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // We register the interest in waiting for both table syncs to have started.
    let users_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We stop and inspect errors.
    match pipeline.shutdown_and_wait().await.err().unwrap() {
        PipelineError::TableSyncWorkersFailed(err) => {
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
async fn test_pipeline_with_table_sync_worker_error() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let fault_config = FaultConfig {
        ..Default::default()
    };
    let state_store = FaultInjectingStateStore::wrap(TestStateStore::new(), fault_config);
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for when table sync is started.
    let users_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = state_store
        .get_inner()
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We stop and inspect errors.
    match pipeline.shutdown_and_wait().await.err().unwrap() {
        PipelineError::TableSyncWorkersFailed(err) => {
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
async fn test_table_schema_copy_with_data_sync_retry() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // Configure state store to fail during data sync.
    let fault_config = FaultConfig {
        ..Default::default()
    };
    let failing_state_store = FaultInjectingStateStore::wrap(state_store.clone(), fault_config);
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        failing_state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table sync phases.
    let users_state_notify = failing_state_store
        .get_inner()
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::DataSync,
        )
        .await;
    let orders_state_notify = failing_state_store
        .get_inner()
        .notify_on_replication_phase(
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
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // Wait for schema reception and table sync completion.
    let schemas_notify = destination.wait_for_n_schemas(2).await;

    // Register notifications for table sync phases.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
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
async fn test_table_schema_copy_with_finished_copy_retry() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // We wait for two table schemas to be received.
    let schemas_notify = destination.wait_for_n_schemas(2).await;
    // We wait for both table states to be in finished done (sync wait is only memory and not
    // available on the store).
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
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
        TableReplicationPhaseType::FinishedCopy
    );
    assert_eq!(
        table_replication_states
            .get(&database_schema.orders_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::FinishedCopy
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
        database_schema.publication_name.clone(),
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

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_survives_restarts() {
    init_test_tracing();
    let mut database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // We wait for two table schemas to be received.
    let schemas_notify = destination.wait_for_n_schemas(2).await;
    // We wait for both table states to be in finished done (sync wait is only memory and not
    // available on the store).
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
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
        TableReplicationPhaseType::FinishedCopy
    );
    assert_eq!(
        table_replication_states
            .get(&database_schema.orders_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::FinishedCopy
    );

    // We check that the table schemas have been stored.
    let table_schemas = destination.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert_eq!(table_schemas[0], database_schema.orders_schema());
    assert_eq!(table_schemas[1], database_schema.users_schema());

    // We recreate a pipeline, assuming the other one was stopped, using the same state and destination.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We wait for two inserts to be processed, one for `users` and one for `orders`.
    let insert_events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    // Insert a single row for each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        // 1 element.
        0..=0,
        true,
    )
    .await;

    insert_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We check that both inserts were received, and we know that we can receive them only when the table
    // schemas are available.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .unwrap();
    let orders_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .unwrap();

    assert_eq!(users_inserts.len(), 1);
    assert_eq!(orders_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy() {
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

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    let orders_table_rows = table_rows.get(&database_schema.orders_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);

    // Verify age sum calculation.
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(&destination, database_schema.users_schema().id).await;
    assert_eq!(age_sum, expected_age_sum);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_and_sync() {
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

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for initial table copy completion.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Register notifications for sync completion.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    // Insert additional data to test streaming.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        (rows_inserted + 1)..=(rows_inserted + 2),
        true,
    )
    .await;

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Register notifications for ready state.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // We wait for all the inserts to be received.
    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 8)])
        .await;

    // Insert more data to test apply worker processing.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        (rows_inserted + 3)..=(rows_inserted + 4),
        true,
    )
    .await;

    users_state_notify.notified().await;
    orders_state_notify.notified().await;
    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify initial table copy data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    let orders_table_rows = table_rows.get(&database_schema.orders_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);

    // Verify age sum calculation.
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(&destination, database_schema.users_schema().id).await;
    assert_eq!(age_sum, expected_age_sum);

    // Get all the events that were produced to the destination and assert them individually by table
    // since the only thing we are guaranteed is that the order of operations is preserved within the
    // same table but not across tables given the asynchronous nature of the pipeline (e.g., we could
    // start streaming earlier on a table for data which was inserted after another table which was
    // modified before this one)
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .unwrap();
    let orders_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .unwrap();

    // Build expected events for verification
    let expected_users_inserts = build_expected_users_inserts(
        11,
        database_schema.users_schema().id,
        vec![
            ("user_11", 11),
            ("user_12", 12),
            ("user_13", 13),
            ("user_14", 14),
        ],
    );
    let expected_orders_inserts = build_expected_orders_inserts(
        11,
        database_schema.orders_schema().id,
        vec![
            "description_11",
            "description_12",
            "description_13",
            "description_14",
        ],
    );
    assert_eq!(*users_inserts, expected_users_inserts);
    assert_eq!(*orders_inserts, expected_orders_inserts);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_and_sync_with_changed_schema_in_table_sync_worker() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::OrdersOnly).await;

    // Insert data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"description_1"],
        )
        .await
        .unwrap();

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for initial table copy completion.
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    orders_state_notify.notified().await;

    // Register notification for the skipped state.
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Skipped,
        )
        .await;

    // Change the schema of orders by adding a new column.
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

    // Insert new data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description", "date"],
            &[&"description_with_date", &(10i32)],
        )
        .await
        .unwrap();

    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We assert that the schema is the initial one.
    let table_schemas = destination.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert_eq!(table_schemas[0], database_schema.orders_schema());

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type(&events);

    // We assert that only one `Commit` message was received, since the apply worker doesn't filter
    // transaction control operations by table id, so those should always go out for each apply +
    // table sync worker. And since we are skipping a table on the table schema change, we only expect
    // the first `Commit` to be sent by the apply worker.
    assert_eq!(grouped_events.get(&EventType::Commit).unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_and_sync_with_changed_schema_in_apply_worker() {
    init_test_tracing();
    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::OrdersOnly).await;

    // Insert data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"description_1"],
        )
        .await
        .unwrap();

    let state_store = TestStateStore::new();
    let destination = TestDestination::new();

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for initial table copy completion.
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    orders_state_notify.notified().await;

    // Register notification for the sync done state.
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    // Insert new data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"description_2"],
        )
        .await
        .unwrap();

    orders_state_notify.notified().await;

    // Register notification for the ready state.
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // Insert new data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"description_3"],
        )
        .await
        .unwrap();

    orders_state_notify.notified().await;

    // Register notification for the skipped state.
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Skipped,
        )
        .await;

    // Change the schema of orders by adding a new column.
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

    // Insert new data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description", "date"],
            &[&"description_with_date", &(10i32)],
        )
        .await
        .unwrap();

    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We assert that the schema is the initial one.
    let table_schemas = destination.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert_eq!(table_schemas[0], database_schema.orders_schema());

    // We check that we got the insert events after the first data of the table has been copied.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let orders_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .unwrap();

    let expected_orders_inserts = build_expected_orders_inserts(
        2,
        database_schema.orders_schema().id,
        vec!["description_2", "description_3"],
    );
    assert_eq!(*orders_inserts, expected_orders_inserts);
}
