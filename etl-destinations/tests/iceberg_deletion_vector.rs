#![cfg(all(feature = "iceberg", feature = "test-utils"))]

use etl::types::{Cell, ColumnSchema, TableRow, Type};
use etl_destinations::iceberg::test_utils::{
    LAKEKEEPER_URL, LakekeeperClient, create_minio_props, get_catalog_url,
};
use etl_destinations::iceberg::{
    DeletionVector, IcebergClient, commit_deletion_vectors, create_deletion_vector_data_file,
    get_data_file_paths, write_deletion_vector,
};
use etl_telemetry::tracing::init_test_tracing;
use iceberg::CatalogBuilder;

mod support;
use crate::support::iceberg::read_all_rows;

/// Tests that deletion vectors can be written to Puffin files and DataFile entries
/// can be created correctly using the proxy structs approach.
///
/// This test:
/// 1. Creates a table and inserts rows
/// 2. Creates a deletion vector Puffin file with proper Iceberg format
///    (magic bytes + RoaringTreemap + CRC32C)
/// 3. Creates a DataFile entry for the deletion vector using proxy structs
/// 4. Verifies the infrastructure works correctly
/// 5. Commit the deletion vectors
#[tokio::test]
async fn deletion_vector_write_infrastructure() {
    init_test_tracing();

    // Setup: Create warehouse and client
    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name.clone(),
        create_minio_props(),
    )
    .await
    .unwrap();

    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create table with simple schema (id, name)
    let table_name = "test_deletion_vectors".to_string();
    let column_schemas = vec![
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        ColumnSchema::new("name".to_string(), Type::TEXT, -1, false, false),
    ];
    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    // Insert 5 rows: Alice(0), Bob(1), Charlie(2), Diana(3), Eve(4)
    let table_rows = vec![
        TableRow::new(vec![Cell::I32(1), Cell::String("Alice".to_string())]),
        TableRow::new(vec![Cell::I32(2), Cell::String("Bob".to_string())]),
        TableRow::new(vec![Cell::I32(3), Cell::String("Charlie".to_string())]),
        TableRow::new(vec![Cell::I32(4), Cell::String("Diana".to_string())]),
        TableRow::new(vec![Cell::I32(5), Cell::String("Eve".to_string())]),
    ];
    client
        .insert_rows(
            namespace.to_string(),
            table_name.clone(),
            table_rows.clone(),
        )
        .await
        .unwrap();

    // Verify all 5 rows are present
    let rows = read_all_rows(&client, namespace.to_string(), table_name.clone()).await;
    assert_eq!(rows.len(), 5, "Should have 5 rows");

    // Load the table to get metadata for deletion vector
    let table = client
        .load_table(namespace.to_string(), table_name.clone())
        .await
        .unwrap();

    // Get the data file path from the table manifest
    let data_file_paths = get_data_file_paths(&table).await.unwrap();
    assert_eq!(
        data_file_paths.len(),
        1,
        "Should have exactly one data file"
    );
    let data_file_path = &data_file_paths[0];
    let table_location = table.metadata().location();

    // Create deletion vector for rows at positions 1 and 3 (Bob and Diana)
    let row_positions_to_delete = vec![1u64, 3u64];
    let descriptor = write_deletion_vector(
        table.file_io(),
        table_location,
        &row_positions_to_delete,
        data_file_path,
    )
    .await
    .unwrap();

    // Verify descriptor has valid metadata
    assert!(
        descriptor.puffin_path.contains("delete-"),
        "Puffin path should contain 'delete-'"
    );
    assert!(
        descriptor.puffin_path.ends_with(".puffin"),
        "Puffin path should end with '.puffin'"
    );
    assert!(descriptor.file_size > 0, "File size should be positive");
    assert!(
        descriptor.content_offset >= 0,
        "Content offset should be non-negative"
    );
    assert!(
        descriptor.content_size_in_bytes > 0,
        "Content size should be positive"
    );
    assert_eq!(
        descriptor.cardinality,
        row_positions_to_delete.len() as u64,
        "Cardinality should match deleted row count"
    );

    // Create the DataFile entry for the deletion vector
    let delete_file = create_deletion_vector_data_file(&descriptor, data_file_path);

    // Verify DataFile has correct properties
    assert_eq!(
        delete_file.content_type(),
        iceberg::spec::DataContentType::PositionDeletes,
        "Content type should be PositionDeletes"
    );
    assert_eq!(
        delete_file.file_format(),
        iceberg::spec::DataFileFormat::Puffin,
        "File format should be Puffin"
    );
    assert_eq!(
        delete_file.record_count(),
        2,
        "Record count should match deleted row count"
    );
    assert_eq!(
        delete_file.file_path(),
        descriptor.puffin_path,
        "File path should match descriptor"
    );
    assert_eq!(
        delete_file.referenced_data_file(),
        Some(data_file_path.to_string()),
        "Referenced data file should match"
    );
    assert_eq!(
        delete_file.content_offset(),
        Some(descriptor.content_offset),
        "Content offset should match"
    );
    assert_eq!(
        delete_file.content_size_in_bytes(),
        Some(descriptor.content_size_in_bytes),
        "Content size should match"
    );

    // Reload the table to get fresh state for transaction
    let table = client
        .load_table(namespace.to_string(), table_name.clone())
        .await
        .unwrap();

    // Create catalog for commit attempt
    let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
        .load("RestCatalog", {
            let mut props = create_minio_props();
            props.insert(
                iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_string(),
                get_catalog_url(),
            );
            props.insert(
                iceberg_catalog_rest::REST_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse_name.clone(),
            );
            props
        })
        .await
        .unwrap();

    // commit deletion vectors
    commit_deletion_vectors(&table, &catalog, vec![delete_file])
        .await
        .unwrap();

    // Note: iceberg-rust doesn't currently support applying DVv2 deletion vectors
    // during table scans. The deletion vectors are correctly written and committed,
    // but reading back the data would require custom logic to apply the deletions.
    // This is tracked in: https://github.com/apache/iceberg-rust/issues/XXX

    // Cleanup
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Tests that get_data_file_paths correctly retrieves data file paths from table manifests.
#[tokio::test]
async fn get_data_file_paths_retrieves_files() {
    init_test_tracing();

    // Setup
    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name.clone(),
        create_minio_props(),
    )
    .await
    .unwrap();

    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    let table_name = "test_get_data_files".to_string();
    let column_schemas = vec![
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        ColumnSchema::new("value".to_string(), Type::TEXT, -1, false, false),
    ];
    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    // Test empty table returns empty list
    let table = client
        .load_table(namespace.to_string(), table_name.clone())
        .await
        .unwrap();
    let paths_empty = get_data_file_paths(&table).await.unwrap();
    assert!(
        paths_empty.is_empty(),
        "Empty table should have no data files"
    );

    // Insert first batch of rows
    let rows1 = vec![
        TableRow::new(vec![Cell::I32(1), Cell::String("A".to_string())]),
        TableRow::new(vec![Cell::I32(2), Cell::String("B".to_string())]),
    ];
    client
        .insert_rows(namespace.to_string(), table_name.clone(), rows1)
        .await
        .unwrap();

    // Should have one data file
    let table = client
        .load_table(namespace.to_string(), table_name.clone())
        .await
        .unwrap();
    let paths_after_first = get_data_file_paths(&table).await.unwrap();
    assert_eq!(
        paths_after_first.len(),
        1,
        "Should have 1 data file after first insert"
    );
    assert!(
        paths_after_first[0].ends_with(".parquet"),
        "Data file should be parquet"
    );

    // Insert second batch
    let rows2 = vec![
        TableRow::new(vec![Cell::I32(3), Cell::String("C".to_string())]),
        TableRow::new(vec![Cell::I32(4), Cell::String("D".to_string())]),
    ];
    client
        .insert_rows(namespace.to_string(), table_name.clone(), rows2)
        .await
        .unwrap();

    // Should have two data files
    let table = client
        .load_table(namespace.to_string(), table_name.clone())
        .await
        .unwrap();
    let paths_after_second = get_data_file_paths(&table).await.unwrap();
    assert_eq!(
        paths_after_second.len(),
        2,
        "Should have 2 data files after second insert"
    );

    // Cleanup
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Tests deletion vector creation with various row position patterns.
#[tokio::test]
async fn deletion_vector_various_patterns() {
    init_test_tracing();

    // Setup
    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(
        get_catalog_url(),
        warehouse_name.clone(),
        create_minio_props(),
    )
    .await
    .unwrap();

    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    let table_name = "test_dv_patterns".to_string();
    let column_schemas = vec![ColumnSchema::new(
        "id".to_string(),
        Type::INT4,
        -1,
        false,
        true,
    )];
    client
        .create_table_if_missing(namespace, table_name.clone(), &column_schemas)
        .await
        .unwrap();

    // Insert 100 rows
    let table_rows: Vec<TableRow> = (1..=100)
        .map(|i| TableRow::new(vec![Cell::I32(i)]))
        .collect();
    client
        .insert_rows(namespace.to_string(), table_name.clone(), table_rows)
        .await
        .unwrap();

    let table = client
        .load_table(namespace.to_string(), table_name.clone())
        .await
        .unwrap();
    let data_file_paths = get_data_file_paths(&table).await.unwrap();
    let data_file_path = &data_file_paths[0];
    let table_location = table.metadata().location();

    // Test 1: Single row deletion
    let single_row = vec![50u64];
    let descriptor1 =
        write_deletion_vector(table.file_io(), table_location, &single_row, data_file_path)
            .await
            .unwrap();
    let df1 = create_deletion_vector_data_file(&descriptor1, data_file_path);
    assert_eq!(df1.record_count(), 1);

    // Test 2: Consecutive rows
    let consecutive: Vec<u64> = (10..20).collect();
    let descriptor2 = write_deletion_vector(
        table.file_io(),
        table_location,
        &consecutive,
        data_file_path,
    )
    .await
    .unwrap();
    let df2 = create_deletion_vector_data_file(&descriptor2, data_file_path);
    assert_eq!(df2.record_count(), 10);

    // Test 3: Sparse/scattered rows
    let scattered = vec![0u64, 25, 50, 75, 99];
    let descriptor3 =
        write_deletion_vector(table.file_io(), table_location, &scattered, data_file_path)
            .await
            .unwrap();
    let df3 = create_deletion_vector_data_file(&descriptor3, data_file_path);
    assert_eq!(df3.record_count(), 5);

    // Test 4: All rows (stress test for RoaringTreemap)
    let all_rows: Vec<u64> = (0..100).collect();
    let descriptor4 =
        write_deletion_vector(table.file_io(), table_location, &all_rows, data_file_path)
            .await
            .unwrap();
    let df4 = create_deletion_vector_data_file(&descriptor4, data_file_path);
    assert_eq!(df4.record_count(), 100);

    // Cleanup
    client
        .drop_table_if_exists(namespace, table_name)
        .await
        .unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

/// Tests the DeletionVector struct directly for serialization/deserialization.
#[test]
fn deletion_vector_unit_tests() {
    // Test empty deletion vector
    let dv = DeletionVector::new();
    assert_eq!(dv.cardinality(), 0);
    assert!(dv.deleted_rows().is_empty());

    // Test adding rows
    let mut dv = DeletionVector::new();
    dv.delete_rows(vec![1, 5, 10, 100, 1000]);
    assert_eq!(dv.cardinality(), 5);
    assert!(dv.is_deleted(1));
    assert!(dv.is_deleted(5));
    assert!(dv.is_deleted(10));
    assert!(dv.is_deleted(100));
    assert!(dv.is_deleted(1000));
    assert!(!dv.is_deleted(0));
    assert!(!dv.is_deleted(2));
    assert!(!dv.is_deleted(999));

    // Test serialization roundtrip
    let mut dv = DeletionVector::new();
    let deleted = vec![0, 1, 5, 42, 100, 999, 10000];
    dv.delete_rows(deleted.clone());

    let blob = dv.serialize("s3://bucket/data/file-001.parquet");
    assert_eq!(blob.blob_type(), iceberg::puffin::DELETION_VECTOR_V1);

    let dv2 = DeletionVector::deserialize(blob).unwrap();
    assert_eq!(dv2.cardinality(), 7);
    assert_eq!(dv2.deleted_rows(), deleted);
}
