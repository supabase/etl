//! Minimal tests for validation framework.

use etl_api::configs::destination::{FullApiDestinationConfig, FullApiIcebergConfig};
use etl_api::validation::validate_destination;
use etl_config::SerializableSecretString;
use etl_destinations::iceberg::test_utils::{
    LAKEKEEPER_URL, LakekeeperClient, MINIO_PASSWORD, MINIO_URL, MINIO_USERNAME,
};

const BIGQUERY_PROJECT_ID_ENV: &str = "TESTS_BIGQUERY_PROJECT_ID";
const BIGQUERY_SA_KEY_PATH_ENV: &str = "TESTS_BIGQUERY_SA_KEY_PATH";

fn get_bigquery_config() -> Option<(String, String)> {
    let project_id = std::env::var(BIGQUERY_PROJECT_ID_ENV).ok()?;
    let sa_key_path = std::env::var(BIGQUERY_SA_KEY_PATH_ENV).ok()?;
    let sa_key = std::fs::read_to_string(&sa_key_path).ok()?;
    Some((project_id, sa_key))
}

fn create_iceberg_config(warehouse_name: &str) -> FullApiDestinationConfig {
    FullApiDestinationConfig::Iceberg {
        config: FullApiIcebergConfig::Rest {
            catalog_uri: format!("{LAKEKEEPER_URL}/catalog"),
            warehouse_name: warehouse_name.to_string(),
            s3_access_key_id: SerializableSecretString::from(MINIO_USERNAME.to_string()),
            s3_secret_access_key: SerializableSecretString::from(MINIO_PASSWORD.to_string()),
            s3_endpoint: MINIO_URL.to_string(),
            namespace: Some("test".to_string()),
        },
    }
}

fn create_bigquery_config(
    project_id: &str,
    dataset_id: &str,
    sa_key: &str,
) -> FullApiDestinationConfig {
    FullApiDestinationConfig::BigQuery {
        project_id: project_id.to_string(),
        dataset_id: dataset_id.to_string(),
        service_account_key: SerializableSecretString::from(sa_key.to_string()),
        max_staleness_mins: None,
        max_concurrent_streams: None,
    }
}

#[tokio::test]
async fn validate_iceberg_connection_success() {
    let lakekeeper = LakekeeperClient::new(LAKEKEEPER_URL);
    let (warehouse_name, warehouse_id) = match lakekeeper.create_warehouse().await {
        Ok(result) => result,
        Err(_) => {
            eprintln!("Skipping test: Lakekeeper not available");
            return;
        }
    };

    let config = create_iceberg_config(&warehouse_name);
    let failures = validate_destination(&config).await;

    let _ = lakekeeper.drop_warehouse(warehouse_id).await;

    assert!(failures.is_empty(), "Expected no failures: {failures:?}");
}

#[tokio::test]
async fn validate_iceberg_connection_failure() {
    let config = create_iceberg_config("nonexistent-warehouse");
    let failures = validate_destination(&config).await;

    assert!(!failures.is_empty(), "Expected validation failure");
    assert!(failures[0].name.contains("Iceberg"));
}

#[tokio::test]
async fn validate_bigquery_connection_success() {
    let Some((project_id, sa_key)) = get_bigquery_config() else {
        eprintln!("Skipping test: BigQuery env vars not set");
        return;
    };

    let Ok(dataset_id) = std::env::var("TESTS_BIGQUERY_DATASET_ID") else {
        eprintln!("Skipping test: TESTS_BIGQUERY_DATASET_ID not set");
        return;
    };

    let config = create_bigquery_config(&project_id, &dataset_id, &sa_key);
    let failures = validate_destination(&config).await;

    assert!(failures.is_empty(), "Expected no failures: {failures:?}");
}

#[tokio::test]
async fn validate_bigquery_dataset_not_found() {
    let Some((project_id, sa_key)) = get_bigquery_config() else {
        eprintln!("Skipping test: BigQuery env vars not set");
        return;
    };

    let config = create_bigquery_config(&project_id, "nonexistent_dataset_12345", &sa_key);
    let failures = validate_destination(&config).await;

    assert!(!failures.is_empty(), "Expected validation failure");
    assert!(failures[0].name.contains("BigQuery"));
}

#[tokio::test]
async fn validate_bigquery_invalid_credentials() {
    let config = create_bigquery_config("fake-project", "fake-dataset", "{}");
    let failures = validate_destination(&config).await;

    assert!(!failures.is_empty(), "Expected validation failure");
    assert!(failures[0].name.contains("BigQuery"));
}

#[tokio::test]
async fn validate_memory_destination_always_passes() {
    let config = FullApiDestinationConfig::Memory;
    let failures = validate_destination(&config).await;

    assert!(failures.is_empty());
}
