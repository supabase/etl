use etl::types::{ColumnSchema, TableId, TableName, TableSchema, Type};
use etl_destinations::iceberg::IcebergClient;
use etl_telemetry::tracing::init_test_tracing;

use crate::common::lakekeeper::LakekeeperClient;

const LAKEKEEPER_URL: &str = "http://localhost:8182";

#[tokio::test]
async fn test_create_namespace() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);

    let warehouse_name = lakekeeper_client.create_warehouse().await.unwrap();

    let client =
        IcebergClient::new_with_rest_catalog(format!("{LAKEKEEPER_URL}/catalog"), warehouse_name);

    let namespace = "test-namespace";

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
}

#[tokio::test]
async fn test_create_table_if_missing() {
    init_test_tracing();

    let lakekeeper_client = LakekeeperClient::new(LAKEKEEPER_URL);

    let warehouse_name = lakekeeper_client.create_warehouse().await.unwrap();

    let client =
        IcebergClient::new_with_rest_catalog(format!("{LAKEKEEPER_URL}/catalog"), warehouse_name);

    let namespace = "test-namespace";
    let table_name = "test_table".to_string();

    // Create namespace first
    client.create_namespace_if_missing(namespace).await.unwrap();

    // Create a sample table schema
    let table_id = TableId::new(12345);
    let table_name_struct = TableName::new("test_schema".to_string(), table_name.clone());
    let columns = vec![
        ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
        ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ColumnSchema::new("active".to_string(), Type::BOOL, -1, false, false),
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
}
