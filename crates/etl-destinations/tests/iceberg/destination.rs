use etl::{
    state::destination_table_metadata::{DestinationTableMetadata, DestinationTableSchemaStatus},
    store::{both::memory::MemoryStore, schema::SchemaStore, state::StateStore},
    test_utils::{database::test_table_name, test_schema::assert_table_rows_equal_ignoring_size},
    types::{
        Cell, ColumnSchema, Event, InsertEvent, PgLsn, RelationEvent, ReplicatedTableSchema,
        SnapshotId, TableId, TableRow, TableSchema, Type,
    },
};
use etl_destinations::iceberg::{
    DestinationNamespace, IcebergClient, IcebergDestination, table_name_to_iceberg_table_name,
    test_utils::{LAKEKEEPER_URL, LakekeeperClient, create_minio_props, get_catalog_url},
};
use etl_telemetry::tracing::init_test_tracing;

use crate::support::iceberg::read_all_rows;

fn sort_rows_by_id(rows: &mut [TableRow]) {
    rows.sort_by_key(|row| match row.values().first() {
        Some(Cell::I64(id)) => *id,
        value => panic!("expected first cell to be I64 id, got {value:?}"),
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn recovers_applying_schema_change() {
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
async fn recovers_after_schema_commit_before_metadata_applied() {
    init_test_tracing();

    let table_id = TableId::new(43);
    let table_name = test_table_name("iceberg_recover_committed_schema");
    let old_schema = TableSchema::with_snapshot_id(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
        ],
        SnapshotId::from(43_u64),
    );
    let new_schema = TableSchema::with_snapshot_id(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, None, true),
        ],
        SnapshotId::from(44_u64),
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

    let old_column_schemas: Vec<_> =
        old_replicated_table_schema.column_schemas().cloned().collect();
    let new_column_schemas: Vec<_> =
        new_replicated_table_schema.column_schemas().cloned().collect();
    let diff = old_replicated_table_schema.diff(&new_replicated_table_schema);
    client
        .evolve_table_schema(
            namespace,
            iceberg_table_name.clone(),
            &old_column_schemas,
            &new_column_schemas,
            &diff,
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

    let lsn = PgLsn::from(44_u64);
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
        .expect("write_events should recover after the Iceberg schema commit");

    let metadata =
        store.get_destination_table_metadata(table_id).await.unwrap().expect("metadata exists");
    assert!(metadata.is_applied());
    assert_eq!(metadata.snapshot_id, new_replicated_table_schema.inner().snapshot_id);

    let table = client.load_table(namespace.to_owned(), iceberg_table_name.clone()).await.unwrap();
    let schema = table.metadata().current_schema();
    let field_names: Vec<_> =
        schema.as_struct().fields().iter().map(|field| field.name.as_str()).collect();
    assert_eq!(field_names, vec!["id", "name", "email"]);

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
