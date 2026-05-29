use etl::{
    state::TableStateType,
    test_utils::{
        database::spawn_source_database,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, TestDatabaseSchema, assert_table_rows_equal_ignoring_size,
            insert_mock_data, setup_test_database_schema,
        },
    },
    types::{Cell, EventType, PipelineId, TableRow},
};
use etl_destinations::iceberg::{
    DestinationNamespace, IcebergClient, IcebergDestination, table_name_to_iceberg_table_name,
    test_utils::{LAKEKEEPER_URL, LakekeeperClient, create_minio_props, get_catalog_url},
};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::iceberg::read_all_rows;

#[tokio::test(flavor = "multi_thread")]
async fn table_copy() {
    run_table_copy_test(DestinationNamespace::Single("test_namespace".to_owned())).await;
    run_table_copy_test(DestinationNamespace::OnePerSchema).await;
}

async fn run_table_copy_test(destination_namespace: DestinationNamespace) {
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
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let namespace = match destination_namespace {
        DestinationNamespace::Single(ref ns) => {
            client.create_namespace_if_missing(ns).await.unwrap();
            ns.clone()
        }
        DestinationNamespace::OnePerSchema => TestDatabaseSchema::schema().to_owned(),
    };

    let single_destination_namespace = destination_namespace.is_single();
    let raw_destination =
        IcebergDestination::new(client.clone(), destination_namespace, store.clone());
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
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = table_name_to_iceberg_table_name(
        &database_schema.users_schema().name,
        single_destination_namespace,
    )
    .unwrap();
    let orders_table = table_name_to_iceberg_table_name(
        &database_schema.orders_schema().name,
        single_destination_namespace,
    )
    .unwrap();

    let mut actual_users = read_all_rows(&client, namespace.clone(), users_table.clone()).await;

    let expected_users = vec![
        TableRow::new(vec![Cell::I64(1), Cell::String("user_1".to_owned()), Cell::I32(1)]),
        TableRow::new(vec![Cell::I64(2), Cell::String("user_2".to_owned()), Cell::I32(2)]),
    ];

    // Sort deterministically by the debug representation as a simple stable key for
    // tests.
    actual_users
        .sort_by(|a, b| format!("{:?}", a.values()[0]).cmp(&format!("{:?}", b.values()[0])));
    assert_table_rows_equal_ignoring_size(&actual_users, &expected_users);

    let mut actual_orders = read_all_rows(&client, namespace.clone(), orders_table.clone()).await;

    let expected_orders = vec![
        TableRow::new(vec![Cell::I64(1), Cell::String("description_1".to_owned())]),
        TableRow::new(vec![Cell::I64(2), Cell::String("description_2".to_owned())]),
    ];

    // Sort deterministically by the debug representation as a simple stable key for
    // tests.
    actual_orders
        .sort_by(|a, b| format!("{:?}", a.values()[0]).cmp(&format!("{:?}", b.values()[0])));
    assert_table_rows_equal_ignoring_size(&actual_orders, &expected_orders);

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the
    // warehouse level. This feature is planned for future releases. We'll start
    // to use it when it becomes available. The cleanup is not in a Drop impl
    // because each test has a different number of objects to clean up.
    client.drop_table_if_exists(&namespace, users_table).await.unwrap();
    client.drop_table_if_exists(&namespace, orders_table).await.unwrap();
    client.drop_namespace(&namespace).await.unwrap();
    lakekeeper_client.drop_warehouse(warehouse_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_streaming() {
    run_cdc_streaming_test(DestinationNamespace::Single("test_namespace".to_owned())).await;
    run_cdc_streaming_test(DestinationNamespace::OnePerSchema).await;
}

async fn run_cdc_streaming_test(destination_namespace: DestinationNamespace) {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let namespace = match destination_namespace {
        DestinationNamespace::Single(ref ns) => {
            client.create_namespace_if_missing(ns).await.unwrap();
            ns.clone()
        }
        DestinationNamespace::OnePerSchema => TestDatabaseSchema::schema().to_owned(),
    };

    let single_destination_namespace = destination_namespace.is_single();
    let raw_destination =
        IcebergDestination::new(client.clone(), destination_namespace, store.clone());
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch (no initial data). We'll stream CDC events only.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion (Ready for both tables).
    let users_state_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    // Wait until initial sync is done before producing CDC events.
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // === CDC INSERT EVENTS ===
    // We'll expect 2 inserts per table -> 4 insert events total.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

    // Insert rows AFTER Ready so they are captured as CDC events.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    // Wait for all CDC insert events to be written to Iceberg.
    event_notify.notified().await;
    destination.clear_events().await;

    // === CDC UPDATE EVENTS ===
    // We'll expect 2 updates per table -> 4 update events total.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 4)]).await;

    // Update users
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"updated_name", &42i32],
        )
        .await
        .unwrap();

    // Update orders
    database
        .update_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"updated_description"],
        )
        .await
        .unwrap();

    // Wait for all CDC update events to be written to Iceberg.
    event_notify.notified().await;
    destination.clear_events().await;

    // === CDC DELETE EVENTS ===
    // We'll expect 1 delete per table -> 2 delete events total.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 2)]).await;

    // Delete user with id 1
    database
        .delete_values(database_schema.users_schema().name.clone(), &["id"], &["1"], "")
        .await
        .unwrap();

    // Delete order with id 2.
    database
        .delete_values(database_schema.orders_schema().name.clone(), &["id"], &["2"], "")
        .await
        .unwrap();

    // Wait for all CDC delete events to be written to Iceberg.
    event_notify.notified().await;

    // Build base table names.
    let users_table = table_name_to_iceberg_table_name(
        &database_schema.users_schema().name,
        single_destination_namespace,
    )
    .unwrap();
    let orders_table = table_name_to_iceberg_table_name(
        &database_schema.orders_schema().name,
        single_destination_namespace,
    )
    .unwrap();

    // Stop the pipeline to finalize writes.
    pipeline.shutdown_and_wait().await.unwrap();

    let mut actual_users = read_all_rows(&client, namespace.clone(), users_table.clone()).await;
    actual_users
        .sort_by(|a, b| format!("{:?}", a.values()[0]).cmp(&format!("{:?}", b.values()[0])));

    let expected_users = vec![TableRow::new(vec![
        Cell::I64(2),
        Cell::String("updated_name".to_owned()),
        Cell::I32(42),
    ])];

    assert_table_rows_equal_ignoring_size(&actual_users, &expected_users);

    let mut actual_orders = read_all_rows(&client, namespace.clone(), orders_table.clone()).await;

    actual_orders
        .sort_by(|a, b| format!("{:?}", a.values()[0]).cmp(&format!("{:?}", b.values()[0])));

    let expected_orders =
        vec![TableRow::new(vec![Cell::I64(1), Cell::String("updated_description".to_owned())])];

    assert_table_rows_equal_ignoring_size(&actual_orders, &expected_orders);

    // Cleanup: drop tables, namespace, and warehouse.
    client.drop_table_if_exists(&namespace, users_table).await.unwrap();
    client.drop_table_if_exists(&namespace, orders_table).await.unwrap();
    client.drop_namespace(&namespace).await.unwrap();
    lakekeeper_client.drop_warehouse(warehouse_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_streaming_with_truncate() {
    run_cdc_streaming_with_truncate_test(DestinationNamespace::Single("test_namespace".to_owned()))
        .await;
    run_cdc_streaming_with_truncate_test(DestinationNamespace::OnePerSchema).await;
}

async fn run_cdc_streaming_with_truncate_test(destination_namespace: DestinationNamespace) {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let namespace = match destination_namespace {
        DestinationNamespace::Single(ref ns) => {
            client.create_namespace_if_missing(ns).await.unwrap();
            ns.clone()
        }
        DestinationNamespace::OnePerSchema => TestDatabaseSchema::schema().to_owned(),
    };

    let single_destination_namespace = destination_namespace.is_single();
    let raw_destination =
        IcebergDestination::new(client.clone(), destination_namespace, store.clone());
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch (no initial data). We'll stream CDC events only.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion (Ready for both tables).
    let users_state_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    // Wait until initial sync is done before producing CDC events.
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We'll expect 2 inserts per table -> 4 insert events total.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

    // Insert 2 rows per each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    // Wait for all expected insert events to be processed.
    event_notify.notified().await;
    destination.clear_events().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Truncate, 2)]).await;

    // Truncate both tables in the source; destination should drop and recreate
    // the Iceberg tables.
    database.truncate_table(database_schema.users_schema().name.clone()).await.unwrap();
    database.truncate_table(database_schema.orders_schema().name.clone()).await.unwrap();

    // Wait for all expected truncate events to be processed.
    event_notify.notified().await;
    destination.clear_events().await;

    // Build base table names.
    let users_table = table_name_to_iceberg_table_name(
        &database_schema.users_schema().name,
        single_destination_namespace,
    )
    .unwrap();
    let orders_table = table_name_to_iceberg_table_name(
        &database_schema.orders_schema().name,
        single_destination_namespace,
    )
    .unwrap();

    let actual_users = read_all_rows(&client, namespace.clone(), users_table.clone()).await;
    let actual_orders = read_all_rows(&client, namespace.clone(), orders_table.clone()).await;

    assert!(actual_users.is_empty());
    assert!(actual_orders.is_empty());

    // We'll expect 2 inserts per table -> 4 insert events total.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

    // Insert 2 extra rows per each table after truncation.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    // Wait for all expected insert and truncate events to be processed.
    event_notify.notified().await;
    destination.clear_events().await;

    // After truncate, pre-truncate rows should be gone. Only post-truncate rows
    // remain.
    let mut actual_users = read_all_rows(&client, namespace.clone(), users_table.clone()).await;
    actual_users
        .sort_by(|a, b| format!("{:?}", a.values()[0]).cmp(&format!("{:?}", b.values()[0])));

    let expected_users = vec![
        TableRow::new(vec![Cell::I64(3), Cell::String("user_3".to_owned()), Cell::I32(3)]),
        TableRow::new(vec![Cell::I64(4), Cell::String("user_4".to_owned()), Cell::I32(4)]),
    ];
    assert_table_rows_equal_ignoring_size(&actual_users, &expected_users);

    let mut actual_orders = read_all_rows(&client, namespace.clone(), orders_table.clone()).await;
    actual_orders
        .sort_by(|a, b| format!("{:?}", a.values()[0]).cmp(&format!("{:?}", b.values()[0])));

    let expected_orders = vec![
        TableRow::new(vec![Cell::I64(3), Cell::String("description_3".to_owned())]),
        TableRow::new(vec![Cell::I64(4), Cell::String("description_4".to_owned())]),
    ];
    assert_table_rows_equal_ignoring_size(&actual_orders, &expected_orders);

    // Stop the pipeline to finalize writes.
    pipeline.shutdown_and_wait().await.unwrap();

    // Cleanup: drop tables, namespace, and warehouse.
    client.drop_table_if_exists(&namespace, users_table).await.unwrap();
    client.drop_table_if_exists(&namespace, orders_table).await.unwrap();
    client.drop_namespace(&namespace).await.unwrap();
    lakekeeper_client.drop_warehouse(warehouse_id).await.unwrap();
}
