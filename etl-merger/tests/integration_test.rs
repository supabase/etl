//! Integration tests for the etl-merger crate.
//!
//! These tests verify the merger correctly:
//! 1. Creates an in-memory index from existing mirror table data
//! 2. Merges CDC events from changelog tables into mirror tables
//! 3. Resumes from checkpoint after restart, only merging unmerged events

#![cfg(all(feature = "iceberg", feature = "test-utils"))]

use etl::types::{Cell, ColumnSchema, TableRow, Type};
use etl_destinations::iceberg::test_utils::{
    create_minio_props, get_catalog_url, LakekeeperClient, LAKEKEEPER_URL,
};
use etl_destinations::iceberg::IcebergClient;
use etl_merger::{
    Merger, MergerConfig, CDC_OPERATION_COLUMN_NAME, SEQUENCE_NUMBER_COLUMN_NAME,
};
use etl_telemetry::tracing::init_test_tracing;

mod support;

use support::iceberg::read_all_rows;

/// Creates a changelog table schema with the given data columns plus CDC columns.
fn create_changelog_schema(data_columns: Vec<ColumnSchema>) -> Vec<ColumnSchema> {
    let mut schema = data_columns;
    // CDC columns are always at the end
    schema.push(ColumnSchema::new(
        CDC_OPERATION_COLUMN_NAME.to_string(),
        Type::TEXT,
        -1,
        false,
        false,
    ));
    schema.push(ColumnSchema::new(
        SEQUENCE_NUMBER_COLUMN_NAME.to_string(),
        Type::TEXT,
        -1,
        false,
        false,
    ));
    schema
}

/// Creates a mirror table schema (data columns only, no CDC columns).
fn create_mirror_schema() -> Vec<ColumnSchema> {
    vec![
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ColumnSchema::new("value".to_string(), Type::INT4, -1, true, false),
    ]
}

/// Creates a changelog entry row with CDC columns.
fn create_changelog_row(id: i32, name: &str, value: i32, operation: &str, seq_num: &str) -> TableRow {
    TableRow::new(vec![
        Cell::I32(id),
        Cell::String(name.to_string()),
        Cell::I32(value),
        Cell::String(operation.to_string()),
        Cell::String(seq_num.to_string()),
    ])
}

/// Test that the merger correctly creates an in-memory index from existing mirror table data.
#[tokio::test(flavor = "multi_thread")]
async fn test_builds_index_from_existing_mirror_table() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Create namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table with CDC columns
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Create mirror table and pre-populate with some data (simulating existing data)
    let mirror_schema = create_mirror_schema();
    client
        .create_table_if_missing(mirror_namespace, mirror_table.to_string(), &mirror_schema)
        .await
        .unwrap();

    // Insert existing data into mirror table
    let existing_rows = vec![
        TableRow::new(vec![
            Cell::I32(1),
            Cell::String("Alice".to_string()),
            Cell::I32(100),
        ]),
        TableRow::new(vec![
            Cell::I32(2),
            Cell::String("Bob".to_string()),
            Cell::I32(200),
        ]),
        TableRow::new(vec![
            Cell::I32(3),
            Cell::String("Carol".to_string()),
            Cell::I32(300),
        ]),
    ];
    client
        .insert_rows(
            mirror_namespace.to_string(),
            mirror_table.to_string(),
            existing_rows,
        )
        .await
        .unwrap();

    // Create merger config
    let config = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100);

    // Create merger
    let mut merger = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema,
        vec![0], // PK is first column (id)
        config,
    )
    .await
    .unwrap();

    // Build index from existing mirror table
    let index_size = merger.build_index_from_mirror(&client).await.unwrap();

    // Verify index was built with 3 entries
    assert_eq!(index_size, 3);
    assert_eq!(merger.index_len(), 3);

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Test that the merger correctly merges INSERT events from a changelog table
/// into a mirror table and maintains the index.
#[tokio::test(flavor = "multi_thread")]
async fn test_merges_inserts_and_maintains_index() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Create namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table with CDC columns
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Insert CDC INSERT events into changelog table in separate batches
    let batch1_rows = vec![
        create_changelog_row(1, "Alice", 100, "INSERT", "0000000000000001/0000000000000001"),
        create_changelog_row(2, "Bob", 200, "INSERT", "0000000000000002/0000000000000002"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            batch1_rows,
        )
        .await
        .unwrap();

    let batch2_rows = vec![
        create_changelog_row(3, "Carol", 300, "INSERT", "0000000000000003/0000000000000003"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            batch2_rows,
        )
        .await
        .unwrap();

    // Create merger config
    let config = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(2);

    // Create merger (this will also create the mirror table)
    let mirror_schema = create_mirror_schema();
    let mut merger = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema,
        vec![0], // PK is first column (id)
        config,
    )
    .await
    .unwrap();

    // Run merge until caught up
    let summary = merger.merge_until_caught_up().await.unwrap();

    // Verify merge summary
    assert_eq!(summary.events_processed, 3);
    assert!(summary.batches_processed >= 1);

    // Verify index has 3 entries
    assert_eq!(merger.index_len(), 3);

    // Read mirror table and verify contents
    let mirror_rows = read_all_rows(&client, mirror_namespace.to_string(), mirror_table.to_string()).await;

    // We expect 3 rows
    assert_eq!(mirror_rows.len(), 3);

    // Find the rows by id
    let row1 = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(1));
    let row2 = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(2));
    let row3 = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(3));

    // Verify all rows exist with correct values
    assert!(row1.is_some());
    assert_eq!(row1.unwrap().values[1], Cell::String("Alice".to_string()));

    assert!(row2.is_some());
    assert_eq!(row2.unwrap().values[1], Cell::String("Bob".to_string()));

    assert!(row3.is_some());
    assert_eq!(row3.unwrap().values[1], Cell::String("Carol".to_string()));

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Test that after a merger restart with a checkpoint, only unmerged events
/// are processed from the changelog table.
#[tokio::test(flavor = "multi_thread")]
async fn test_resumes_from_checkpoint_after_restart() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Create namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table with CDC columns
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Insert first batch of CDC events (INSERT only)
    let batch1_rows = vec![
        create_changelog_row(1, "Alice", 100, "INSERT", "0000000000000001/0000000000000001"),
        create_changelog_row(2, "Bob", 200, "INSERT", "0000000000000002/0000000000000002"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            batch1_rows,
        )
        .await
        .unwrap();

    // Create first merger instance and run merge
    let config1 = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100);

    let mirror_schema = create_mirror_schema();
    let mut merger1 = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema.clone(),
        vec![0],
        config1,
    )
    .await
    .unwrap();

    let summary1 = merger1.merge_until_caught_up().await.unwrap();
    assert_eq!(summary1.events_processed, 2);

    // Get the checkpoint from the first merger
    let checkpoint = merger1.progress().last_sequence_number.clone();
    assert!(checkpoint.is_some());
    let checkpoint = checkpoint.unwrap();

    // Verify mirror table has 2 rows
    let mirror_rows_before = read_all_rows(
        &client,
        mirror_namespace.to_string(),
        mirror_table.to_string(),
    )
    .await;
    assert_eq!(mirror_rows_before.len(), 2);

    // Now add more INSERT events to the changelog (simulating new CDC events after restart)
    let batch2_rows = vec![
        create_changelog_row(3, "Carol", 300, "INSERT", "0000000000000003/0000000000000003"),
        create_changelog_row(4, "Dave", 400, "INSERT", "0000000000000004/0000000000000004"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            batch2_rows,
        )
        .await
        .unwrap();

    // Simulate restart: Create a new merger instance with the checkpoint
    let config2 = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100)
        .with_checkpoint(Some(checkpoint));

    let mut merger2 = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema,
        vec![0],
        config2,
    )
    .await
    .unwrap();

    // Build index from existing mirror table (as would happen on restart)
    let index_size = merger2.build_index_from_mirror(&client).await.unwrap();
    assert_eq!(index_size, 2); // 2 rows from previous merge

    // Run merge - should only process the new events (batch2)
    let summary2 = merger2.merge_until_caught_up().await.unwrap();

    // Should have processed only 2 new events (not 4 total)
    assert_eq!(summary2.events_processed, 2);

    // Verify index now has 4 entries (id=1, id=2, id=3, id=4)
    assert_eq!(merger2.index_len(), 4);

    // Read mirror table and verify final contents
    let mirror_rows = read_all_rows(&client, mirror_namespace.to_string(), mirror_table.to_string()).await;
    assert_eq!(mirror_rows.len(), 4);

    // Find the rows by id
    let row1 = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(1));
    let row2 = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(2));
    let row3 = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(3));
    let row4 = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(4));

    // Verify all rows exist with correct values
    assert!(row1.is_some());
    assert_eq!(row1.unwrap().values[1], Cell::String("Alice".to_string()));

    assert!(row2.is_some());
    assert_eq!(row2.unwrap().values[1], Cell::String("Bob".to_string()));

    assert!(row3.is_some());
    assert_eq!(row3.unwrap().values[1], Cell::String("Carol".to_string()));

    assert!(row4.is_some());
    assert_eq!(row4.unwrap().values[1], Cell::String("Dave".to_string()));

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Test that the merger handles an empty changelog table correctly.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_changelog_produces_empty_mirror() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Create namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table with CDC columns (but no data)
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Create merger config
    let config = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100);

    // Create merger
    let mirror_schema = create_mirror_schema();
    let mut merger = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema,
        vec![0],
        config,
    )
    .await
    .unwrap();

    // Run merge - should process 0 events
    let summary = merger.merge_until_caught_up().await.unwrap();

    assert_eq!(summary.events_processed, 0);
    assert_eq!(summary.batches_processed, 0);
    assert_eq!(merger.index_len(), 0);

    // Mirror table should exist but be empty
    let mirror_rows = read_all_rows(&client, mirror_namespace.to_string(), mirror_table.to_string()).await;
    assert_eq!(mirror_rows.len(), 0);

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Test that the merger correctly handles inserts only (no updates or deletes).
#[tokio::test(flavor = "multi_thread")]
async fn test_inserts_only() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Create namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Insert only INSERT events
    let changelog_rows = vec![
        create_changelog_row(1, "Alice", 100, "INSERT", "0000000000000001/0000000000000001"),
        create_changelog_row(2, "Bob", 200, "INSERT", "0000000000000002/0000000000000002"),
        create_changelog_row(3, "Carol", 300, "INSERT", "0000000000000003/0000000000000003"),
        create_changelog_row(4, "Dave", 400, "INSERT", "0000000000000004/0000000000000004"),
        create_changelog_row(5, "Eve", 500, "INSERT", "0000000000000005/0000000000000005"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            changelog_rows,
        )
        .await
        .unwrap();

    // Create and run merger
    let config = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100);

    let mirror_schema = create_mirror_schema();
    let mut merger = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema,
        vec![0],
        config,
    )
    .await
    .unwrap();

    let summary = merger.merge_until_caught_up().await.unwrap();

    // Verify all 5 inserts were processed
    assert_eq!(summary.events_processed, 5);
    assert_eq!(merger.index_len(), 5);

    // Verify mirror table has all 5 rows
    let mirror_rows = read_all_rows(&client, mirror_namespace.to_string(), mirror_table.to_string()).await;
    assert_eq!(mirror_rows.len(), 5);

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Test that UPDATE operations correctly use deletion vectors and insert new rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_operation() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Setup namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Insert initial rows
    let initial_rows = vec![
        create_changelog_row(1, "Alice", 100, "INSERT", "0000000000000001/0000000000000001"),
        create_changelog_row(2, "Bob", 200, "INSERT", "0000000000000002/0000000000000002"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            initial_rows,
        )
        .await
        .unwrap();

    // Create and run merger for initial inserts
    let config = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100);
    let mirror_schema = create_mirror_schema();
    let mut merger = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema.clone(),
        vec![0],
        config,
    )
    .await
    .unwrap();

    let summary1 = merger.merge_until_caught_up().await.unwrap();
    assert_eq!(summary1.events_processed, 2);

    // Now insert an UPDATE event
    let update_rows = vec![
        create_changelog_row(1, "Alice Updated", 150, "UPDATE", "0000000000000003/0000000000000003"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            update_rows,
        )
        .await
        .unwrap();

    // Run merge again
    let summary2 = merger.merge_until_caught_up().await.unwrap();
    assert_eq!(summary2.events_processed, 1);

    // Verify mirror table shows updated value
    let mirror_rows = read_all_rows(&client, mirror_namespace.to_string(), mirror_table.to_string()).await;

    // Should have 2 visible rows (Bob unchanged, Alice updated)
    assert_eq!(mirror_rows.len(), 2);

    let alice = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(1)).unwrap();
    assert_eq!(alice.values[1], Cell::String("Alice Updated".to_string()));
    assert_eq!(alice.values[2], Cell::I32(150));

    let bob = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(2)).unwrap();
    assert_eq!(bob.values[1], Cell::String("Bob".to_string()));
    assert_eq!(bob.values[2], Cell::I32(200));

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Test that DELETE operations correctly use deletion vectors.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_operation() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Setup namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Insert initial rows
    let initial_rows = vec![
        create_changelog_row(1, "Alice", 100, "INSERT", "0000000000000001/0000000000000001"),
        create_changelog_row(2, "Bob", 200, "INSERT", "0000000000000002/0000000000000002"),
        create_changelog_row(3, "Carol", 300, "INSERT", "0000000000000003/0000000000000003"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            initial_rows,
        )
        .await
        .unwrap();

    // Create and run merger for initial inserts
    let config = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100);
    let mirror_schema = create_mirror_schema();
    let mut merger = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema.clone(),
        vec![0],
        config,
    )
    .await
    .unwrap();

    merger.merge_until_caught_up().await.unwrap();

    // Now insert a DELETE event
    let delete_rows = vec![
        create_changelog_row(2, "Bob", 200, "DELETE", "0000000000000004/0000000000000004"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            delete_rows,
        )
        .await
        .unwrap();

    // Run merge again
    let summary = merger.merge_until_caught_up().await.unwrap();
    assert_eq!(summary.events_processed, 1);

    // Verify mirror table shows only Alice and Carol
    let mirror_rows = read_all_rows(&client, mirror_namespace.to_string(), mirror_table.to_string()).await;
    assert_eq!(mirror_rows.len(), 2);

    assert!(mirror_rows.iter().any(|r| r.values[0] == Cell::I32(1))); // Alice
    assert!(mirror_rows.iter().any(|r| r.values[0] == Cell::I32(3))); // Carol
    assert!(!mirror_rows.iter().any(|r| r.values[0] == Cell::I32(2))); // Bob deleted

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Test mixed INSERT, UPDATE, and DELETE operations.
#[tokio::test(flavor = "multi_thread")]
async fn test_mixed_operations() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name,
        create_minio_props(),
    )
    .await
    .unwrap();

    let changelog_namespace = "changelog_ns";
    let mirror_namespace = "mirror_ns";
    let changelog_table = "users_changelog";
    let mirror_table = "users";

    // Setup namespaces
    client
        .create_namespace_if_missing(changelog_namespace)
        .await
        .unwrap();
    client
        .create_namespace_if_missing(mirror_namespace)
        .await
        .unwrap();

    // Create changelog table
    let changelog_schema = create_changelog_schema(create_mirror_schema());
    client
        .create_table_if_missing(changelog_namespace, changelog_table.to_string(), &changelog_schema)
        .await
        .unwrap();

    // Insert initial rows
    let initial_rows = vec![
        create_changelog_row(1, "Alice", 100, "INSERT", "0000000000000001/0000000000000001"),
        create_changelog_row(2, "Bob", 200, "INSERT", "0000000000000002/0000000000000002"),
        create_changelog_row(3, "Carol", 300, "INSERT", "0000000000000003/0000000000000003"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            initial_rows,
        )
        .await
        .unwrap();

    // Create and run merger for initial inserts
    let config = MergerConfig::new(changelog_namespace.to_string(), mirror_namespace.to_string())
        .with_batch_size(100);
    let mirror_schema = create_mirror_schema();
    let mut merger = Merger::new(
        client.clone(),
        changelog_table,
        mirror_table,
        mirror_schema.clone(),
        vec![0],
        config,
    )
    .await
    .unwrap();

    merger.merge_until_caught_up().await.unwrap();

    // Now insert mixed operations:
    // - UPDATE id=1 (Alice -> Alice Modified)
    // - DELETE id=2 (Bob)
    // - INSERT id=4 (Dave)
    let mixed_rows = vec![
        create_changelog_row(1, "Alice Modified", 150, "UPDATE", "0000000000000004/0000000000000004"),
        create_changelog_row(2, "Bob", 200, "DELETE", "0000000000000005/0000000000000005"),
        create_changelog_row(4, "Dave", 400, "INSERT", "0000000000000006/0000000000000006"),
    ];
    client
        .insert_rows(
            changelog_namespace.to_string(),
            changelog_table.to_string(),
            mixed_rows,
        )
        .await
        .unwrap();

    // Run merge again
    let summary = merger.merge_until_caught_up().await.unwrap();
    assert_eq!(summary.events_processed, 3);

    // Verify final state:
    // id=1 "Alice Modified" 150
    // id=3 "Carol" 300
    // id=4 "Dave" 400
    let mirror_rows = read_all_rows(&client, mirror_namespace.to_string(), mirror_table.to_string()).await;
    assert_eq!(mirror_rows.len(), 3);

    let alice = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(1)).unwrap();
    assert_eq!(alice.values[1], Cell::String("Alice Modified".to_string()));
    assert_eq!(alice.values[2], Cell::I32(150));

    let carol = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(3)).unwrap();
    assert_eq!(carol.values[1], Cell::String("Carol".to_string()));
    assert_eq!(carol.values[2], Cell::I32(300));

    let dave = mirror_rows.iter().find(|r| r.values[0] == Cell::I32(4)).unwrap();
    assert_eq!(dave.values[1], Cell::String("Dave".to_string()));
    assert_eq!(dave.values[2], Cell::I32(400));

    // Bob should not be present
    assert!(!mirror_rows.iter().any(|r| r.values[0] == Cell::I32(2)));

    // Cleanup
    client
        .drop_table_if_exists(mirror_namespace, mirror_table.to_string())
        .await
        .unwrap();
    client
        .drop_table_if_exists(changelog_namespace, changelog_table.to_string())
        .await
        .unwrap();
    client.drop_namespace(mirror_namespace).await.unwrap();
    client.drop_namespace(changelog_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}
