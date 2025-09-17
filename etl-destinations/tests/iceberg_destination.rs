#![cfg(feature = "iceberg")]

use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::spawn_source_database;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{Cell, PipelineId, TableRow};
use etl_destinations::iceberg::{IcebergClient, IcebergDestination};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::iceberg::{LAKEKEEPER_URL, create_props, get_catalog_url, read_all_rows};
use crate::support::lakekeeper::LakekeeperClient;

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    // Insert initial test data.
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

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client =
        IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name, create_props());

    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    let raw_destination =
        IcebergDestination::new(client.clone(), namespace.to_string(), store.clone());
    // let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = format!(
        "{}_{}",
        database_schema.users_schema().name.schema,
        database_schema.users_schema().name.name
    );
    let orders_table = format!(
        "{}_{}",
        database_schema.orders_schema().name.schema,
        database_schema.orders_schema().name.name
    );

    let mut actual_users = read_all_rows(&client, namespace.to_string(), users_table.clone()).await;

    let expected_users = vec![
        TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("user_1".to_string()),
                Cell::I32(1),
            ],
        },
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("user_2".to_string()),
                Cell::I32(2),
            ],
        },
    ];

    // Sort deterministically by the debug representation as a simple stable key for tests.
    actual_users.sort_by(|a, b| format!("{:?}", a.values[0]).cmp(&format!("{:?}", b.values[0])));
    assert_eq!(actual_users, expected_users);

    let mut actual_orders =
        read_all_rows(&client, namespace.to_string(), orders_table.clone()).await;

    let expected_orders = vec![
        TableRow {
            values: vec![Cell::I64(1), Cell::String("description_1".to_string())],
        },
        TableRow {
            values: vec![Cell::I64(2), Cell::String("description_2".to_string())],
        },
    ];

    // Sort deterministically by the debug representation as a simple stable key for tests.
    actual_orders.sort_by(|a, b| format!("{:?}", a.values[0]).cmp(&format!("{:?}", b.values[0])));
    assert_eq!(actual_orders, expected_orders);

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client.drop_table(namespace, users_table).await.unwrap();
    client.drop_table(namespace, orders_table).await.unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}
