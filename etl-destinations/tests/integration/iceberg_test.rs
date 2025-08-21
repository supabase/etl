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
