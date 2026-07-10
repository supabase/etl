use etl_api::{
    configs::destination::{ApiDestinationConfig, ApiIcebergConfig},
    validation::{FailureType, validate_destination},
};
use etl_config::SerializableSecretString;
use etl_destinations::iceberg::test_utils::{
    LAKEKEEPER_URL, LakekeeperClient, MINIO_PASSWORD, MINIO_URL, MINIO_USERNAME,
    skip_if_lakekeeper_not_running,
};

use super::create_validation_context;

fn create_iceberg_config(warehouse_name: &str) -> ApiDestinationConfig {
    ApiDestinationConfig::Iceberg {
        config: ApiIcebergConfig::Rest {
            catalog_uri: format!("{LAKEKEEPER_URL}/catalog"),
            warehouse_name: warehouse_name.to_owned(),
            s3_access_key_id: SerializableSecretString::from(MINIO_USERNAME.to_owned()),
            s3_secret_access_key: SerializableSecretString::from(MINIO_PASSWORD.to_owned()),
            s3_endpoint: MINIO_URL.to_owned(),
            namespace: Some("test".to_owned()),
        },
    }
}

#[tokio::test]
async fn validate_iceberg_connection_success() {
    if skip_if_lakekeeper_not_running() {
        return;
    }
    let lakekeeper = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) =
        lakekeeper.create_warehouse().await.expect("Failed to create warehouse");

    let ctx = create_validation_context();
    let config = create_iceberg_config(&warehouse_name);
    let failures = validate_destination(&ctx, &config, None).await.unwrap();

    let _ = lakekeeper.drop_warehouse(warehouse_id).await;

    assert!(failures.is_empty(), "Expected no failures: {failures:?}");
}

#[tokio::test]
async fn validate_iceberg_connection_failure() {
    if skip_if_lakekeeper_not_running() {
        return;
    }
    let ctx = create_validation_context();
    let config = create_iceberg_config("nonexistent-warehouse");
    let failures = validate_destination(&ctx, &config, None).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Iceberg Connection Failed");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}
