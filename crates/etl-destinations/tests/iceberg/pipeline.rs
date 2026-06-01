use etl::{
    state::{
        TableStateType,
        destination_table_metadata::{DestinationTableMetadata, DestinationTableSchemaStatus},
    },
    store::{both::memory::MemoryStore, schema::SchemaStore, state::StateStore},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        notifying_store::NotifyingStore,
        pipeline::{create_pipeline, create_pipeline_with_batch_config},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, TestDatabaseSchema, assert_table_rows_equal_ignoring_size,
            insert_mock_data, setup_test_database_schema,
        },
    },
    types::{
        Cell, ColumnSchema, Event, EventType, InsertEvent, PgLsn, PipelineId, RelationEvent,
        ReplicatedTableSchema, SnapshotId, TableId, TableRow, TableSchema, Type,
    },
};
use etl_config::shared::BatchConfig;
use etl_destinations::iceberg::{
    DestinationNamespace, IcebergClient, IcebergDestination, table_name_to_iceberg_table_name,
    test_utils::{LAKEKEEPER_URL, LakekeeperClient, create_minio_props, get_catalog_url},
};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::iceberg::read_all_rows;

fn sort_rows_by_id(rows: &mut [TableRow]) {
    rows.sort_by_key(|row| match row.values().first() {
        Some(Cell::I64(id)) => *id,
        value => panic!("expected first cell to be I64 id, got {value:?}"),
    });
}

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

    sort_rows_by_id(&mut actual_users);
    assert_table_rows_equal_ignoring_size(&actual_users, &expected_users);

    let mut actual_orders = read_all_rows(&client, namespace.clone(), orders_table.clone()).await;

    let expected_orders = vec![
        TableRow::new(vec![Cell::I64(1), Cell::String("description_1".to_owned())]),
        TableRow::new(vec![Cell::I64(2), Cell::String("description_2".to_owned())]),
    ];

    sort_rows_by_id(&mut actual_orders);
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
async fn cdc_streaming_coalesces_insert_then_delete_in_one_batch() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

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

    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    let raw_destination = IcebergDestination::new(
        client.clone(),
        DestinationNamespace::Single(namespace.to_owned()),
        store.clone(),
    );
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let mut pipeline = create_pipeline_with_batch_config(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        BatchConfig { max_fill_ms: 2_000, memory_budget_ratio: 0.2, max_bytes: 8 * 1024 * 1024 },
    );

    let users_state_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Delete, 1)])
        .await;
    let users_table_name = database_schema.users_schema().name;

    database
        .insert_values(
            users_table_name.clone(),
            &["id", "name", "age"],
            &[&10_i64, &"transient", &10_i32],
        )
        .await
        .unwrap();
    database.delete_values(users_table_name.clone(), &["id"], &["10"], "").await.unwrap();

    event_notify.notified().await;

    let users_table =
        table_name_to_iceberg_table_name(&users_table_name, true).expect("valid table name");

    pipeline.shutdown_and_wait().await.unwrap();

    let actual_users = read_all_rows(&client, namespace.to_owned(), users_table.clone()).await;
    assert!(actual_users.is_empty());

    client.drop_table_if_exists(namespace, users_table).await.unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client.drop_warehouse(warehouse_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_streaming_applies_schema_changes_end_to_end() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("iceberg_schema_multi_ops");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null"), ("status", "text")],
        )
        .await
        .unwrap();
    let publication_name = format!("test_pub_schema_change_{}", random::<u64>());
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

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

    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    let raw_destination = IcebergDestination::new(
        client.clone(),
        DestinationNamespace::Single(namespace.to_owned()),
        store.clone(),
    );
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();
    table_ready.notified().await;

    let initial_state = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("destination table metadata should exist");
    let initial_snapshot_id = initial_state.snapshot_id;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;
    database
        .insert_values(
            table_name.clone(),
            &["name", "age", "status"],
            &[&"Alice", &25_i32, &"active"],
        )
        .await
        .unwrap();
    event_notify.notified().await;
    destination.clear_events().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "name", new_name: "full_name" }],
        )
        .await
        .unwrap();
    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "status" }])
        .await
        .unwrap();
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text" }],
        )
        .await
        .unwrap();
    database
        .insert_values(
            table_name.clone(),
            &["full_name", "age", "email"],
            &[&"Bob", &30_i32, &"bob@example.com"],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let final_state = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("destination table metadata should exist");
    assert!(final_state.snapshot_id > initial_snapshot_id);

    let iceberg_table = table_name_to_iceberg_table_name(&table_name, true).unwrap();
    let table = client.load_table(namespace.to_owned(), iceberg_table.clone()).await.unwrap();
    let schema = table.metadata().current_schema();
    let field_names: Vec<_> =
        schema.as_struct().fields().iter().map(|field| field.name.as_str()).collect();
    assert_eq!(field_names, vec!["id", "full_name", "age", "email"]);
    assert!(schema.field_by_name("name").is_none());
    assert!(schema.field_by_name("status").is_none());

    let mut actual_rows = read_all_rows(&client, namespace.to_owned(), iceberg_table.clone()).await;
    sort_rows_by_id(&mut actual_rows);
    let expected_rows = vec![
        TableRow::new(vec![
            Cell::I64(1),
            Cell::String("Alice".to_owned()),
            Cell::I32(25),
            Cell::Null,
        ]),
        TableRow::new(vec![
            Cell::I64(2),
            Cell::String("Bob".to_owned()),
            Cell::I32(30),
            Cell::String("bob@example.com".to_owned()),
        ]),
    ];
    assert_table_rows_equal_ignoring_size(&actual_rows, &expected_rows);

    client.drop_table_if_exists(namespace, iceberg_table).await.unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client.drop_warehouse(warehouse_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn cdc_streaming_recovers_applying_schema_change() {
    init_test_tracing();

    let table_id = TableId::new(41);
    let table_name = test_table_name("iceberg_recover_schema");
    let old_schema = TableSchema::with_snapshot_id(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
        ],
        SnapshotId::from(41_u64),
    );
    let new_schema = TableSchema::with_snapshot_id(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, None, true),
        ],
        SnapshotId::from(42_u64),
    );
    let old_replicated_table_schema = ReplicatedTableSchema::all(old_schema.into());
    let new_replicated_table_schema = ReplicatedTableSchema::all(new_schema.into());
    let table_name = old_replicated_table_schema.name().clone();
    let iceberg_table_name = table_name_to_iceberg_table_name(&table_name, true).unwrap();

    let store = MemoryStore::new();
    store.store_table_schema(old_replicated_table_schema.inner().clone()).await.unwrap();
    store.store_table_schema(new_replicated_table_schema.inner().clone()).await.unwrap();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let namespace = "test_namespace";
    let destination = IcebergDestination::new(
        client.clone(),
        DestinationNamespace::Single(namespace.to_owned()),
        store.clone(),
    );

    destination
        .write_table_rows_for_tests(
            &old_replicated_table_schema,
            vec![TableRow::new(vec![Cell::I64(1), Cell::String("Alice".to_owned())])],
        )
        .await
        .unwrap();

    let applying_metadata = DestinationTableMetadata::new_applied(
        iceberg_table_name.clone(),
        old_replicated_table_schema.inner().snapshot_id,
        old_replicated_table_schema.replication_mask().clone(),
    )
    .with_schema_change(
        new_replicated_table_schema.inner().snapshot_id,
        new_replicated_table_schema.replication_mask().clone(),
        DestinationTableSchemaStatus::Applying,
    );
    store.store_destination_table_metadata(table_id, applying_metadata).await.unwrap();

    let lsn = PgLsn::from(42_u64);
    destination
        .write_events_for_tests(vec![
            Event::Relation(RelationEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 0,
                replicated_table_schema: new_replicated_table_schema.clone(),
            }),
            Event::Insert(InsertEvent {
                start_lsn: lsn,
                commit_lsn: lsn,
                tx_ordinal: 1,
                replicated_table_schema: new_replicated_table_schema.clone(),
                table_row: TableRow::new(vec![
                    Cell::I64(2),
                    Cell::String("Bob".to_owned()),
                    Cell::String("bob@example.com".to_owned()),
                ]),
            }),
        ])
        .await
        .expect("write_events should recover applying schema metadata");

    let metadata =
        store.get_destination_table_metadata(table_id).await.unwrap().expect("metadata exists");
    assert!(metadata.is_applied());
    assert_eq!(metadata.snapshot_id, new_replicated_table_schema.inner().snapshot_id);

    let mut actual_rows =
        read_all_rows(&client, namespace.to_owned(), iceberg_table_name.clone()).await;
    sort_rows_by_id(&mut actual_rows);
    let expected_rows = vec![
        TableRow::new(vec![Cell::I64(1), Cell::String("Alice".to_owned()), Cell::Null]),
        TableRow::new(vec![
            Cell::I64(2),
            Cell::String("Bob".to_owned()),
            Cell::String("bob@example.com".to_owned()),
        ]),
    ];
    assert_table_rows_equal_ignoring_size(&actual_rows, &expected_rows);

    client.drop_table_if_exists(namespace, iceberg_table_name).await.unwrap();
    client.drop_namespace(namespace).await.unwrap();
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
    sort_rows_by_id(&mut actual_users);

    let expected_users = vec![TableRow::new(vec![
        Cell::I64(2),
        Cell::String("updated_name".to_owned()),
        Cell::I32(42),
    ])];

    assert_table_rows_equal_ignoring_size(&actual_users, &expected_users);

    let mut actual_orders = read_all_rows(&client, namespace.clone(), orders_table.clone()).await;

    sort_rows_by_id(&mut actual_orders);

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

    pipeline.shutdown_and_wait().await.unwrap();

    // After truncate, pre-truncate rows should be gone. Only post-truncate rows
    // remain.
    let mut actual_users = read_all_rows(&client, namespace.clone(), users_table.clone()).await;
    sort_rows_by_id(&mut actual_users);

    let expected_users = vec![
        TableRow::new(vec![Cell::I64(3), Cell::String("user_3".to_owned()), Cell::I32(3)]),
        TableRow::new(vec![Cell::I64(4), Cell::String("user_4".to_owned()), Cell::I32(4)]),
    ];
    assert_table_rows_equal_ignoring_size(&actual_users, &expected_users);

    let mut actual_orders = read_all_rows(&client, namespace.clone(), orders_table.clone()).await;
    sort_rows_by_id(&mut actual_orders);

    let expected_orders = vec![
        TableRow::new(vec![Cell::I64(3), Cell::String("description_3".to_owned())]),
        TableRow::new(vec![Cell::I64(4), Cell::String("description_4".to_owned())]),
    ];
    assert_table_rows_equal_ignoring_size(&actual_orders, &expected_orders);

    // Cleanup: drop tables, namespace, and warehouse.
    client.drop_table_if_exists(&namespace, users_table).await.unwrap();
    client.drop_table_if_exists(&namespace, orders_table).await.unwrap();
    client.drop_namespace(&namespace).await.unwrap();
    lakekeeper_client.drop_warehouse(warehouse_id).await.unwrap();
}
