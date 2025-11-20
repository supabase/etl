use etl_config::shared::IcebergConfig;
use etl_validation::{DestinationValidator, ValidationError};
use secrecy::SecretString;
use std::error::Error;

const LAKEKEEPER_URL: &str = "http://localhost:8182";
const PROJECT_ID_HEADER: &str = "x-project-id";
const PROJECT_ID: &str = "00000000-0000-0000-0000-000000000000";

fn test_lakekeeper_config() -> IcebergConfig {
    let catalog_uri = "http://localhost:8182/catalog".to_string();
    let warehouse_name = format!("test_warehouse_{}", uuid::Uuid::new_v4());

    IcebergConfig::Rest {
        catalog_uri,
        warehouse_name,
        namespace: None,
        s3_access_key_id: SecretString::from("minio-admin"),
        s3_secret_access_key: SecretString::from("minio-admin-password"),
        s3_endpoint: "http://localhost:9010".to_string(),
    }
}

async fn create_lakekeeper_warehouse(warehouse_name: String) -> Result<uuid::Uuid, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let url = format!("{}/management/v1/warehouse", LAKEKEEPER_URL);

    let key_prefix = uuid::Uuid::new_v4();
    let body = format!(
        r#"{{
            "warehouse-name": "{}",
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {{
                "type": "s3",
                "bucket": "dev-and-test",
                "key-prefix": "{}",
                "endpoint": "http://minio:9000",
                "region": "local-01",
                "path-style-access": true,
                "flavor": "minio",
                "sts-enabled": true
            }},
            "storage-credential": {{
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "minio-admin",
                "aws-secret-access-key": "minio-admin-password"
            }}
        }}"#,
        warehouse_name, key_prefix
    );

    let response = client
        .post(url)
        .header(PROJECT_ID_HEADER, PROJECT_ID)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;

    let json: serde_json::Value = response.json().await?;
    let warehouse_id = json["warehouse-id"]
        .as_str()
        .ok_or_else(|| format!("Missing warehouse-id in response: {:?}", json))?
        .parse()?;

    Ok(warehouse_id)
}

async fn delete_lakekeeper_warehouse(warehouse_id: uuid::Uuid) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let url = format!(
        "{}/management/v1/warehouse/{}",
        LAKEKEEPER_URL, warehouse_id
    );

    const MAX_RETRIES: u8 = 10;
    for _ in 0..MAX_RETRIES {
        let response = client.delete(&url).send().await?;

        if response.status().is_success() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_iceberg_rest_catalog_connection() {
    let config = test_lakekeeper_config();

    // Extract warehouse name for creation
    let warehouse_name = match &config {
        IcebergConfig::Rest { warehouse_name, .. } => warehouse_name.clone(),
        _ => panic!("Expected Rest config"),
    };

    // Create warehouse in Lakekeeper
    let warehouse_id = match create_lakekeeper_warehouse(warehouse_name).await {
        Ok(id) => id,
        Err(e) => {
            if e.to_string().contains("Connection refused") || e.to_string().contains("connect") {
                println!("WARNING: Lakekeeper not running at localhost:8182, skipping test");
                println!(
                    "Start services with: docker compose -f ./scripts/docker-compose.yaml up -d"
                );
                return;
            }
            panic!("Failed to create warehouse: {}", e);
        }
    };

    // Validate should succeed
    let result = config.validate().await;

    // Clean up warehouse
    let _ = delete_lakekeeper_warehouse(warehouse_id).await;

    match result {
        Ok(_) => {
            println!("Iceberg validation succeeded");
        }
        Err(err) => {
            panic!("Validation failed: {:?}", err);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_iceberg_catalog_uri() {
    let config = IcebergConfig::Rest {
        catalog_uri: "http://localhost:9999/nonexistent".to_string(), // Invalid port
        warehouse_name: "test-warehouse".to_string(),
        namespace: None,
        s3_access_key_id: SecretString::from("minio-admin"),
        s3_secret_access_key: SecretString::from("minio-admin-password"),
        s3_endpoint: "http://localhost:9010".to_string(),
    };

    // Validation should fail
    let result = config.validate().await;
    assert!(
        result.is_err(),
        "Validation should fail for invalid catalog URI"
    );

    match result.unwrap_err() {
        ValidationError::ConnectionFailed(msg) => {
            assert!(
                msg.contains("Failed to create Iceberg REST catalog")
                    || msg.contains("Failed to list namespaces")
                    || msg.contains("Connection refused"),
                "Expected connection error, got: {}",
                msg
            );
        }
        err => panic!("Expected ConnectionFailed error, got: {:?}", err),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_iceberg_supabase_config_structure() {
    // Test that Supabase config can be created (without actual validation)
    let config = IcebergConfig::Supabase {
        project_ref: "test-project".to_string(),
        warehouse_name: "test-warehouse".to_string(),
        namespace: None,
        catalog_token: SecretString::from("test-token"),
        s3_access_key_id: SecretString::from("test-key"),
        s3_secret_access_key: SecretString::from("test-secret"),
        s3_region: "us-east-1".to_string(),
    };

    // This will fail to connect (since it's not real Supabase), but verifies config structure
    let result = config.validate().await;
    assert!(
        result.is_err(),
        "Validation should fail for fake Supabase config"
    );

    match result.unwrap_err() {
        ValidationError::ConnectionFailed(_) => {
            // Expected - can't connect to fake Supabase
            println!("Supabase config structure is valid (connection failed as expected)");
        }
        err => panic!("Expected ConnectionFailed error, got: {:?}", err),
    }
}
