#![cfg(all(feature = "iceberg", feature = "test-utils"))]

use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::spawn_source_database;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{
    TableSelection, TestDatabaseSchema, insert_mock_data, setup_test_database_schema,
};
use etl::types::{Cell, EventType, PipelineId, TableRow};
use etl_destinations::iceberg::test_utils::LakekeeperClient;
use etl_destinations::iceberg::{
    DestinationNamespace, IcebergClient, IcebergDestination, IcebergOperationType,
    table_name_to_iceberg_table_name,
};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::iceberg::read_all_rows;
use etl_destinations::iceberg::test_utils::{LAKEKEEPER_URL, create_minio_props, get_catalog_url};

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn table_copy() {
    run_table_copy_test(DestinationNamespace::Single("test_namespace".to_string())).await;
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
            ns.to_string()
        }
        DestinationNamespace::OnePerSchema => TestDatabaseSchema::schema().to_string(),
    };

    let single_destination_namespace = destination_namespace.is_single();
    let raw_destination =
        IcebergDestination::new(client.clone(), destination_namespace, store.clone());
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
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let users_table = table_name_to_iceberg_table_name(
        &database_schema.users_schema().name,
        single_destination_namespace,
    );
    let orders_table = table_name_to_iceberg_table_name(
        &database_schema.orders_schema().name,
        single_destination_namespace,
    );

    let mut actual_users = read_all_rows(&client, namespace.to_string(), users_table.clone()).await;

    let expected_users = vec![
        TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("user_1".to_string()),
                Cell::I32(1),
                IcebergOperationType::Insert.into(),
                Cell::String("0000000000000000/0000000000000000".to_string()),
            ],
        },
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("user_2".to_string()),
                Cell::I32(2),
                IcebergOperationType::Insert.into(),
                Cell::String("0000000000000000/0000000000000000".to_string()),
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
            values: vec![
                Cell::I64(1),
                Cell::String("description_1".to_string()),
                IcebergOperationType::Insert.into(),
                Cell::String("0000000000000000/0000000000000000".to_string()),
            ],
        },
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("description_2".to_string()),
                IcebergOperationType::Insert.into(),
                Cell::String("0000000000000000/0000000000000000".to_string()),
            ],
        },
    ];

    // Sort deterministically by the debug representation as a simple stable key for tests.
    actual_orders.sort_by(|a, b| format!("{:?}", a.values[0]).cmp(&format!("{:?}", b.values[0])));
    assert_eq!(actual_orders, expected_orders);

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client
        .drop_table_if_exists(&namespace, users_table)
        .await
        .unwrap();
    client
        .drop_table_if_exists(&namespace, orders_table)
        .await
        .unwrap();
    client.drop_namespace(&namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_streaming() {
    run_cdc_streaming_test(DestinationNamespace::Single("test_namespace".to_string())).await;
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
            ns.to_string()
        }
        DestinationNamespace::OnePerSchema => TestDatabaseSchema::schema().to_string(),
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
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    // Wait until initial sync is done before producing CDC events.
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // === CDC INSERT EVENTS ===
    // We'll expect 2 inserts per table -> 4 insert events total.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

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
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 4)])
        .await;

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
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 2)])
        .await;

    // Delete user with id 1
    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["id"],
            &["1"],
            "",
        )
        .await
        .unwrap();

    // Delete order with id 2
    database
        .delete_values(
            database_schema.orders_schema().name.clone(),
            &["id"],
            &["2"],
            "",
        )
        .await
        .unwrap();

    // Wait for all CDC delete events to be written to Iceberg.
    event_notify.notified().await;

    // base table names
    let users_table = table_name_to_iceberg_table_name(
        &database_schema.users_schema().name,
        single_destination_namespace,
    );
    let orders_table = table_name_to_iceberg_table_name(
        &database_schema.orders_schema().name,
        single_destination_namespace,
    );

    let mut actual_users = read_all_rows(&client, namespace.to_string(), users_table.clone()).await;

    // Sort deterministically by the sequence number for stable assertions
    actual_users.sort_by(|a, b| {
        let a_key = format!("{:?}", a.values[4]);
        let b_key = format!("{:?}", b.values[4]);
        a_key.cmp(&b_key)
    });

    // Drop the last column (non-deterministic sequence number) before comparison.
    for row in &mut actual_users {
        let _ = row.values.pop();
    }

    // Expected CDC rows: 2 inserts (UPSERT), 2 updates (UPSERT), 1 delete (DELETE)
    // Note: order here is messed up due to limitations in how read_all_rows can't sort, so we sort manually
    // by id and cdc operation columns
    let expected_users = vec![
        // Initial insert of user 1
        TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("user_1".to_string()),
                Cell::I32(1),
                IcebergOperationType::Insert.into(),
            ],
        },
        // Initial insert of user 2
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("user_2".to_string()),
                Cell::I32(2),
                IcebergOperationType::Insert.into(),
            ],
        },
        // Update of user 1
        TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("updated_name".to_string()),
                Cell::I32(42),
                IcebergOperationType::Update.into(),
            ],
        },
        // Update of user 2
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("updated_name".to_string()),
                Cell::I32(42),
                IcebergOperationType::Update.into(),
            ],
        },
        // Delete of user with id 1
        TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("".to_string()),
                Cell::I32(0),
                IcebergOperationType::Delete.into(),
            ],
        },
    ];

    assert_eq!(actual_users, expected_users);

    let mut actual_orders =
        read_all_rows(&client, namespace.to_string(), orders_table.clone()).await;

    // Sort deterministically by the primary key (id) and sequence number for stable assertions
    actual_orders.sort_by(|a, b| {
        let a_key = format!("{:?}", a.values[3]);
        let b_key = format!("{:?}", b.values[3]);
        a_key.cmp(&b_key)
    });

    // Drop the last column (non-deterministic sequence number) before comparison.
    for row in &mut actual_orders {
        let _ = row.values.pop();
    }

    let expected_orders = vec![
        // Initial insert of order 1
        TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("description_1".to_string()),
                IcebergOperationType::Insert.into(),
            ],
        },
        // Initial insert of order 2
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("description_2".to_string()),
                IcebergOperationType::Insert.into(),
            ],
        },
        // Update of order 1
        TableRow {
            values: vec![
                Cell::I64(1),
                Cell::String("updated_description".to_string()),
                IcebergOperationType::Update.into(),
            ],
        },
        // Update of order 2
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("updated_description".to_string()),
                IcebergOperationType::Update.into(),
            ],
        },
        // Delete of order 2
        TableRow {
            values: vec![
                Cell::I64(2),
                Cell::String("".to_string()),
                IcebergOperationType::Delete.into(),
            ],
        },
    ];

    assert_eq!(actual_orders, expected_orders);

    // Stop the pipeline to finalize writes.
    pipeline.shutdown_and_wait().await.unwrap();

    // Cleanup: drop CDC tables, namespace, and warehouse.
    client
        .drop_table_if_exists(&namespace, users_table)
        .await
        .unwrap();
    client
        .drop_table_if_exists(&namespace, orders_table)
        .await
        .unwrap();
    client.drop_namespace(&namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_streaming_with_truncate() {
    run_cdc_streaming_with_truncate_test(DestinationNamespace::Single(
        "test_namespace".to_string(),
    ))
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
            ns.to_string()
        }
        DestinationNamespace::OnePerSchema => TestDatabaseSchema::schema().to_string(),
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
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    // Wait until initial sync is done before producing CDC events.
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // We'll expect 2 inserts per table -> 4 insert events total.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    // Insert 2 rows per each table (captured as CDC UPSERT events).
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

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Truncate, 2)])
        .await;

    // Truncate both tables in the source; destination should drop and recreate base + CDC tables.
    database
        .truncate_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    database
        .truncate_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();

    // Wait for all expected truncate events to be processed.
    event_notify.notified().await;
    destination.clear_events().await;

    // base table names
    let users_table = table_name_to_iceberg_table_name(
        &database_schema.users_schema().name,
        single_destination_namespace,
    );
    let orders_table = table_name_to_iceberg_table_name(
        &database_schema.orders_schema().name,
        single_destination_namespace,
    );

    let actual_users = read_all_rows(&client, namespace.to_string(), users_table.clone()).await;
    let actual_orders = read_all_rows(&client, namespace.to_string(), users_table.clone()).await;

    assert!(actual_users.is_empty());
    assert!(actual_orders.is_empty());

    // We'll expect 2 inserts per table -> 4 insert events total.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

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

    // After truncate, pre-truncate CDC rows should be gone (tables were dropped). Only post-truncate rows remain.
    let mut actual_users = read_all_rows(&client, namespace.to_string(), users_table.clone()).await;
    for row in &mut actual_users {
        let _ = row.values.pop(); // drop sequence_number
    }
    actual_users.sort_by(|a, b| format!("{:?}", a.values[0]).cmp(&format!("{:?}", b.values[0])));

    let expected_users = vec![
        TableRow {
            values: vec![
                Cell::I64(3),
                Cell::String("user_3".to_string()),
                Cell::I32(3),
                IcebergOperationType::Insert.into(),
            ],
        },
        TableRow {
            values: vec![
                Cell::I64(4),
                Cell::String("user_4".to_string()),
                Cell::I32(4),
                IcebergOperationType::Insert.into(),
            ],
        },
    ];
    assert_eq!(actual_users, expected_users);

    let mut actual_orders =
        read_all_rows(&client, namespace.to_string(), orders_table.clone()).await;
    for row in &mut actual_orders {
        let _ = row.values.pop(); // drop sequence_number
    }
    actual_orders.sort_by(|a, b| format!("{:?}", a.values[0]).cmp(&format!("{:?}", b.values[0])));

    let expected_orders = vec![
        TableRow {
            values: vec![
                Cell::I64(3),
                Cell::String("description_3".to_string()),
                IcebergOperationType::Insert.into(),
            ],
        },
        TableRow {
            values: vec![
                Cell::I64(4),
                Cell::String("description_4".to_string()),
                IcebergOperationType::Insert.into(),
            ],
        },
    ];
    assert_eq!(actual_orders, expected_orders);

    // Stop the pipeline to finalize writes.
    pipeline.shutdown_and_wait().await.unwrap();

    // Cleanup: drop CDC tables, namespace, and warehouse.
    client
        .drop_table_if_exists(&namespace, users_table)
        .await
        .unwrap();
    client
        .drop_table_if_exists(&namespace, orders_table)
        .await
        .unwrap();
    client.drop_namespace(&namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}
