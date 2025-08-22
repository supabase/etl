use etl::types::{ColumnSchema, TableId, TableName, TableSchema, Type};
use etl_destinations::iceberg::IcebergClient;
use etl_telemetry::tracing::init_test_tracing;

use crate::common::lakekeeper::LakekeeperClient;

const LAKEKEEPER_URL: &str = "http://localhost:8182";

fn get_catalog_url() -> String {
    format!("{LAKEKEEPER_URL}/catalog")
}

#[tokio::test]
async fn create_namespace() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name);

    let namespace = "test_namespace";

    // namespace doesn't exist yet
    assert!(!client.namespace_exists(namespace).await.unwrap());

    // create namespace for the first time
    client.create_namespace_if_missing(namespace).await.unwrap();
    // namespace should exist now
    assert!(client.namespace_exists(namespace).await.unwrap());

    // trying to create an existing namespace is a no-op
    client.create_namespace_if_missing(namespace).await.unwrap();
    // namespace still exists
    assert!(client.namespace_exists(namespace).await.unwrap());

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn create_hierarchical_namespace() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name);

    let root_namespace = "root_namespace";

    // create namespace for the first time
    client
        .create_namespace_if_missing(root_namespace)
        .await
        .unwrap();
    // root namespace should exist now
    assert!(client.namespace_exists(root_namespace).await.unwrap());

    let child_namespace = &format!("{root_namespace}.child_namespace");

    client
        .create_namespace_if_missing(child_namespace)
        .await
        .unwrap();
    // child namespace should exist now
    assert!(client.namespace_exists(child_namespace).await.unwrap());

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client.drop_namespace(child_namespace).await.unwrap();
    client.drop_namespace(root_namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn create_table_if_missing() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = lakekeeper_client.create_warehouse().await.unwrap();
    let client = IcebergClient::new_with_rest_catalog(get_catalog_url(), warehouse_name);

    // Create namespace first
    let namespace = "test_namespace";
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema with all supported types
    let table_name = "test_table".to_string();
    let table_id = TableId::new(12345);
    let table_name_struct = TableName::new("test_schema".to_string(), table_name.clone());
    let columns = vec![
        // Primary key
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        // Boolean types
        ColumnSchema::new("bool_col".to_string(), Type::BOOL, -1, true, false),
        // String types
        ColumnSchema::new("char_col".to_string(), Type::CHAR, -1, true, false),
        ColumnSchema::new("bpchar_col".to_string(), Type::BPCHAR, -1, true, false),
        ColumnSchema::new("varchar_col".to_string(), Type::VARCHAR, -1, true, false),
        ColumnSchema::new("name_col".to_string(), Type::NAME, -1, true, false),
        ColumnSchema::new("text_col".to_string(), Type::TEXT, -1, true, false),
        // Integer types
        ColumnSchema::new("int2_col".to_string(), Type::INT2, -1, true, false),
        ColumnSchema::new("int4_col".to_string(), Type::INT4, -1, true, false),
        ColumnSchema::new("int8_col".to_string(), Type::INT8, -1, true, false),
        // Float types
        ColumnSchema::new("float4_col".to_string(), Type::FLOAT4, -1, true, false),
        ColumnSchema::new("float8_col".to_string(), Type::FLOAT8, -1, true, false),
        // Numeric type
        ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, true, false),
        // Date/Time types
        ColumnSchema::new("date_col".to_string(), Type::DATE, -1, true, false),
        ColumnSchema::new("time_col".to_string(), Type::TIME, -1, true, false),
        ColumnSchema::new(
            "timestamp_col".to_string(),
            Type::TIMESTAMP,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_col".to_string(),
            Type::TIMESTAMPTZ,
            -1,
            true,
            false,
        ),
        // UUID type
        ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, true, false),
        // JSON types
        ColumnSchema::new("json_col".to_string(), Type::JSON, -1, true, false),
        ColumnSchema::new("jsonb_col".to_string(), Type::JSONB, -1, true, false),
        // OID type
        ColumnSchema::new("oid_col".to_string(), Type::OID, -1, true, false),
        // Binary type
        ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, true, false),
        // Array types
        ColumnSchema::new(
            "bool_array_col".to_string(),
            Type::BOOL_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "char_array_col".to_string(),
            Type::CHAR_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "bpchar_array_col".to_string(),
            Type::BPCHAR_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "varchar_array_col".to_string(),
            Type::VARCHAR_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "name_array_col".to_string(),
            Type::NAME_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "text_array_col".to_string(),
            Type::TEXT_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "int2_array_col".to_string(),
            Type::INT2_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "int4_array_col".to_string(),
            Type::INT4_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "int8_array_col".to_string(),
            Type::INT8_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "float4_array_col".to_string(),
            Type::FLOAT4_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "float8_array_col".to_string(),
            Type::FLOAT8_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "numeric_array_col".to_string(),
            Type::NUMERIC_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "date_array_col".to_string(),
            Type::DATE_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "time_array_col".to_string(),
            Type::TIME_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "timestamp_array_col".to_string(),
            Type::TIMESTAMP_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "timestamptz_array_col".to_string(),
            Type::TIMESTAMPTZ_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "uuid_array_col".to_string(),
            Type::UUID_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "json_array_col".to_string(),
            Type::JSON_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "jsonb_array_col".to_string(),
            Type::JSONB_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "oid_array_col".to_string(),
            Type::OID_ARRAY,
            -1,
            true,
            false,
        ),
        ColumnSchema::new(
            "bytea_array_col".to_string(),
            Type::BYTEA_ARRAY,
            -1,
            true,
            false,
        ),
    ];
    let table_schema = TableSchema::new(table_id, table_name_struct, columns);

    // table doesn't exist yet
    assert!(
        !client
            .table_exists(namespace, table_name.to_string())
            .await
            .unwrap()
    );

    // Create table for the first time
    client
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
        .await
        .unwrap();

    // table should exist now
    assert!(
        client
            .table_exists(namespace, table_name.to_string())
            .await
            .unwrap()
    );

    // Creating the same table again should be a no-op (no error)
    client
        .create_table_if_missing(namespace, table_name.clone(), &table_schema)
        .await
        .unwrap();

    // table should still exist
    assert!(
        client
            .table_exists(namespace, table_name.to_string())
            .await
            .unwrap()
    );

    // Manual cleanup for now because lakekeeper doesn't allow cascade delete at the warehouse level
    // This feature is planned for future releases. We'll start to use it when it becomes available.
    // The cleanup is not in a Drop impl because each test has different number of object specitic to
    // that test.
    client.drop_table(namespace, table_name).await.unwrap();
    client.drop_namespace(namespace).await.unwrap();
    lakekeeper_client
        .drop_warehouse(warehouse_id)
        .await
        .unwrap();
}
