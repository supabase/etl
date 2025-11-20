use etl_api::configs::destination::{FullApiDestinationConfig, FullApiIcebergConfig};
use etl_api::configs::source::FullApiSourceConfig;
use etl_config::SerializableSecretString;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

use crate::support::test_app::spawn_test_app;

mod support;

// ============================================================================
// Source Connection Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_source_connection_with_invalid_credentials_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiSourceConfig {
        host: "localhost".to_string(),
        port: 5430,
        username: "postgres".to_string(),
        password: Some(SerializableSecretString::from("wrong_password".to_string())),
        name: "postgres".to_string(),
    };

    // Act
    let response = app.test_source_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_source_connection_with_nonexistent_database_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiSourceConfig {
        host: "localhost".to_string(),
        port: 5430,
        username: "postgres".to_string(),
        password: Some(SerializableSecretString::from("postgres".to_string())),
        name: "nonexistent_database_12345".to_string(),
    };

    // Act
    let response = app.test_source_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_source_connection_with_wrong_port_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiSourceConfig {
        host: "localhost".to_string(),
        port: 9999, // Wrong port
        username: "postgres".to_string(),
        password: Some(SerializableSecretString::from("postgres".to_string())),
        name: "postgres".to_string(),
    };

    // Act
    let response = app.test_source_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_source_connection_with_wrong_host_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiSourceConfig {
        host: "nonexistent.host.example.com".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        password: Some(SerializableSecretString::from("postgres".to_string())),
        name: "postgres".to_string(),
    };

    // Act
    let response = app.test_source_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================================
// Destination Connection Tests - BigQuery
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_destination_connection_bigquery_with_invalid_service_account_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiDestinationConfig::BigQuery {
        project_id: "test-project".to_string(),
        dataset_id: "test_dataset".to_string(),
        service_account_key: SerializableSecretString::from("invalid-json".to_string()),
        max_staleness_mins: None,
        max_concurrent_streams: None,
    };

    // Act
    let response = app.test_destination_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_destination_connection_bigquery_with_malformed_json_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiDestinationConfig::BigQuery {
        project_id: "test-project".to_string(),
        dataset_id: "test_dataset".to_string(),
        service_account_key: SerializableSecretString::from("{not valid json".to_string()),
        max_staleness_mins: None,
        max_concurrent_streams: None,
    };

    // Act
    let response = app.test_destination_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================================
// Destination Connection Tests - Iceberg REST
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_destination_connection_iceberg_rest_with_invalid_uri_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiDestinationConfig::Iceberg {
        config: FullApiIcebergConfig::Rest {
            catalog_uri: "http://localhost:9999/nonexistent".to_string(), // Invalid port/URI
            warehouse_name: "test_warehouse".to_string(),
            namespace: None,
            s3_endpoint: "http://localhost:9010".to_string(),
            s3_access_key_id: SerializableSecretString::from("test-key".to_string()),
            s3_secret_access_key: SerializableSecretString::from("test-secret".to_string()),
        },
    };

    // Act
    let response = app.test_destination_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_destination_connection_iceberg_rest_with_invalid_host_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiDestinationConfig::Iceberg {
        config: FullApiIcebergConfig::Rest {
            catalog_uri: "http://nonexistent.host.example.com/catalog".to_string(),
            warehouse_name: "test_warehouse".to_string(),
            namespace: None,
            s3_endpoint: "http://localhost:9010".to_string(),
            s3_access_key_id: SerializableSecretString::from("test-key".to_string()),
            s3_secret_access_key: SerializableSecretString::from("test-secret".to_string()),
        },
    };

    // Act
    let response = app.test_destination_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================================
// Destination Connection Tests - Iceberg Supabase
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_destination_connection_iceberg_supabase_with_fake_credentials_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiDestinationConfig::Iceberg {
        config: FullApiIcebergConfig::Supabase {
            project_ref: "fake-project-ref".to_string(),
            warehouse_name: "test_warehouse".to_string(),
            namespace: None,
            catalog_token: SerializableSecretString::from("fake-token".to_string()),
            s3_access_key_id: SerializableSecretString::from("fake-key".to_string()),
            s3_secret_access_key: SerializableSecretString::from("fake-secret".to_string()),
            s3_region: "us-east-1".to_string(),
        },
    };

    // Act
    let response = app.test_destination_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ============================================================================
// Memory Destination Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_destination_connection_memory_succeeds() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiDestinationConfig::Memory;

    // Act
    let response = app.test_destination_connection(&config).await;

    // Assert
    // Memory destinations don't require connection testing and always succeed
    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// Response Format Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_connection_response_has_correct_format_on_error() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let config = FullApiSourceConfig {
        host: "localhost".to_string(),
        port: 9999, // Wrong port
        username: "postgres".to_string(),
        password: Some(SerializableSecretString::from("postgres".to_string())),
        name: "postgres".to_string(),
    };

    // Act
    let response = app.test_source_connection(&config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // Verify response body contains error information
    let body = response.text().await.expect("Failed to read response body");
    assert!(body.contains("error") || body.contains("Error"));
}
