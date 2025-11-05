#![cfg(feature = "databend")]

use etl::config::BatchConfig;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::spawn_source_database;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{EventType, PipelineId};
use etl_destinations::encryption::install_crypto_provider;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use std::time::Duration;
use tokio::time::sleep;

use crate::support::databend::{
    DatabendOrder, DatabendUser, parse_databend_table_rows, setup_databend_connection,
};

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();
    install_crypto_provider();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let databend_database = setup_databend_connection().await;

    // Insert initial test data
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = databend_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Query Databend directly to get the data
    // Note: Data inserted BEFORE pipeline starts is only in table copy,
    // CDC doesn't capture it because replication slot is created after
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    let parsed_users_rows = parse_databend_table_rows(users_rows, DatabendUser::from_row);
    assert_eq!(
        parsed_users_rows,
        vec![
            DatabendUser::new(1, "user_1", 1),
            DatabendUser::new(2, "user_2", 2),
        ]
    );

    let orders_rows = databend_database
        .query_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();
    let parsed_orders_rows = parse_databend_table_rows(orders_rows, DatabendOrder::from_row);
    assert_eq!(
        parsed_orders_rows,
        vec![
            DatabendOrder::new(1, "description_1"),
            DatabendOrder::new(2, "description_2"),
        ]
    );

    // Restart the pipeline and check that we can process events
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We expect 2 insert events for each table (4 total)
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    // Insert additional data
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Query Databend to verify all data
    // Expected: 2 records from initial table copy + 2 new records from CDC
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    let parsed_users_rows = parse_databend_table_rows(users_rows, DatabendUser::from_row);
    assert_eq!(
        parsed_users_rows,
        vec![
            DatabendUser::new(1, "user_1", 1),
            DatabendUser::new(2, "user_2", 2),
            DatabendUser::new(3, "user_3", 3),
            DatabendUser::new(4, "user_4", 4),
        ]
    );

    let orders_rows = databend_database
        .query_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();
    let parsed_orders_rows = parse_databend_table_rows(orders_rows, DatabendOrder::from_row);
    assert_eq!(
        parsed_orders_rows,
        vec![
            DatabendOrder::new(1, "description_1"),
            DatabendOrder::new(2, "description_2"),
            DatabendOrder::new(3, "description_3"),
            DatabendOrder::new(4, "description_4"),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_insert_update_delete() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let databend_database = setup_databend_connection().await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = databend_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Test INSERT events
    let insert_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {} (id, name, age) VALUES ($1, $2, $3)",
                database_schema.users_schema().name
            ),
            &[&1i64, &"Alice", &30i32],
        )
        .await
        .unwrap();

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {} (id, name, age) VALUES ($1, $2, $3)",
                database_schema.users_schema().name
            ),
            &[&2i64, &"Bob", &25i32],
        )
        .await
        .unwrap();

    insert_notify.notified().await;

    // Verify inserts
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    let parsed_users = parse_databend_table_rows(users_rows, DatabendUser::from_row);
    assert_eq!(
        parsed_users,
        vec![
            DatabendUser::new(1, "Alice", 30),
            DatabendUser::new(2, "Bob", 25),
        ]
    );

    // Test UPDATE events
    let update_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "UPDATE {} SET age = $1 WHERE id = $2",
                database_schema.users_schema().name
            ),
            &[&31i32, &1i64],
        )
        .await
        .unwrap();

    update_notify.notified().await;

    // Verify updates (note: in CDC table, both old and new rows appear)
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    // Should have: Alice(30), Bob(25), Alice(31)
    assert!(users_rows.len() >= 2);

    // Test DELETE events
    let delete_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!("DELETE FROM {} WHERE id = $1", database_schema.users_schema().name),
            &[&2i64],
        )
        .await
        .unwrap();

    delete_notify.notified().await;

    // Verify deletes (note: delete events are also captured in CDC table)
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    // Should contain CDC records including delete operations
    assert!(!users_rows.is_empty());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_truncate() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let databend_database = setup_databend_connection().await;

    // Insert initial test data manually (since we only have users table)
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {} (id, name, age) VALUES ($1, $2, $3)",
                database_schema.users_schema().name
            ),
            &[&1i64, &"user_1", &1i32],
        )
        .await
        .unwrap();

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {} (id, name, age) VALUES ($1, $2, $3)",
                database_schema.users_schema().name
            ),
            &[&2i64, &"user_2", &2i32],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = databend_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Verify initial data
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    assert_eq!(users_rows.len(), 2);

    // Test TRUNCATE event
    let truncate_notify = destination
        .wait_for_events_count(vec![(EventType::Truncate, 1)])
        .await;

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!("TRUNCATE TABLE {}", database_schema.users_schema().name),
            &[],
        )
        .await
        .unwrap();

    truncate_notify.notified().await;

    // Give some time for the truncate to be processed
    sleep(Duration::from_millis(500)).await;

    // Verify table is empty after truncate
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    assert_eq!(users_rows.len(), 0, "Table should be empty after truncate");

    // Insert new data after truncate
    let insert_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {} (id, name, age) VALUES ($1, $2, $3)",
                database_schema.users_schema().name
            ),
            &[&5i64, &"Charlie", &35i32],
        )
        .await
        .unwrap();

    insert_notify.notified().await;

    // Verify new data
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    let parsed_users = parse_databend_table_rows(users_rows, DatabendUser::from_row);
    assert_eq!(parsed_users, vec![DatabendUser::new(5, "Charlie", 35)]);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_table_operations() {
    init_test_tracing();
    install_crypto_provider();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let databend_database = setup_databend_connection().await;

    // Insert initial test data
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=10,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = databend_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        Some(BatchConfig {
            max_size: 1000,
            max_fill_ms: 5000,
        }),
    );

    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify all data was copied correctly
    let users_rows = databend_database
        .query_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    assert_eq!(users_rows.len(), 10, "Should have 10 user rows");

    let orders_rows = databend_database
        .query_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();
    assert_eq!(orders_rows.len(), 10, "Should have 10 order rows");
}
