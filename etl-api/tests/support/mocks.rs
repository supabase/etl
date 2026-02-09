#![allow(dead_code)]

use etl_api::configs::destination::FullApiDestinationConfig;
use etl_api::configs::pipeline::{FullApiPipelineConfig, PartialApiPipelineConfig};
use etl_api::configs::source::FullApiSourceConfig;
use etl_api::routes::destinations::{CreateDestinationRequest, CreateDestinationResponse};
use etl_api::routes::images::{CreateImageRequest, CreateImageResponse};
use etl_api::routes::pipelines::{CreatePipelineRequest, CreatePipelineResponse};
use etl_api::routes::sources::{CreateSourceRequest, CreateSourceResponse};
use etl_config::SerializableSecretString;

use crate::support::test_app::TestApp;

/// Creates a default image and returns its id.
pub async fn create_default_image(app: &TestApp) -> i64 {
    create_image_with_name(app, "some/image".to_string(), true).await
}

/// Creates an image with the provided name and default flag and returns its id.
pub async fn create_image_with_name(app: &TestApp, name: String, is_default: bool) -> i64 {
    let image = CreateImageRequest { name, is_default };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    response.id
}

/// Destination helpers.
pub mod destinations {
    use super::*;

    /// Returns a default destination name.
    pub fn new_name() -> String {
        "BigQuery Destination".to_string()
    }

    /// Returns an updated destination name.
    pub fn updated_name() -> String {
        "BigQuery Destination (Updated)".to_string()
    }

    /// Returns an updated destination config.
    pub fn updated_destination_config() -> FullApiDestinationConfig {
        FullApiDestinationConfig::BigQuery {
            project_id: "project-id-updated".to_string(),
            dataset_id: "dataset-id-updated".to_string(),
            service_account_key: SerializableSecretString::from(
                "service-account-key-updated".to_string(),
            ),
            max_staleness_mins: Some(10),
            connection_pool_size: Some(1),
        }
    }

    /// Returns a default destination config (BigQuery).
    pub fn new_bigquery_destination_config() -> FullApiDestinationConfig {
        FullApiDestinationConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: SerializableSecretString::from("service-account-key".to_string()),
            max_staleness_mins: None,
            connection_pool_size: Some(1),
        }
    }

    /// Returns a default Iceberg Supabase destination config.
    pub fn new_iceberg_supabase_destination_config() -> FullApiDestinationConfig {
        use etl_api::configs::destination::FullApiIcebergConfig;

        FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_string(),
                warehouse_name: "my-warehouse".to_string(),
                namespace: Some("my-namespace".to_string()),
                catalog_token: SerializableSecretString::from(
                    "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ".to_string()
                ),
                s3_access_key_id: SerializableSecretString::from("9156667efc2c70d89af6588da86d2924".to_string()),
                s3_secret_access_key: SerializableSecretString::from(
                    "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_string()
                ),
                s3_region: "ap-southeast-1".to_string(),
            },
        }
    }

    /// Returns an updated Iceberg Supabase destination config.
    pub fn updated_iceberg_supabase_destination_config() -> FullApiDestinationConfig {
        use etl_api::configs::destination::FullApiIcebergConfig;

        FullApiDestinationConfig::Iceberg {
            config: FullApiIcebergConfig::Supabase {
                project_ref: "tsrqponmlkjihgfedcba".to_string(),
                warehouse_name: "my-updated-warehouse".to_string(),
                namespace: Some("my-updated-namespace".to_string()),
                catalog_token: SerializableSecretString::from(
                    "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjJlOGQxZDNjN2MyMTJkOTU4ZmEyOGU2ZDhjZDEwYTMzIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJ0c3JxcG9ubWxramloZ2ZlZGNiYSIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.UpdatedTokenSignatureForTesting".to_string()
                ),
                s3_access_key_id: SerializableSecretString::from("updated9156667efc2c70d89af6588da86d2924".to_string()),
                s3_secret_access_key: SerializableSecretString::from(
                    "updatedca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_string()
                ),
                s3_region: "us-west-2".to_string(),
            },
        }
    }

    /// Creates a destination with the provided name and config and returns its id.
    pub async fn create_destination_with_config(
        app: &TestApp,
        tenant_id: &str,
        name: String,
        config: FullApiDestinationConfig,
    ) -> i64 {
        let destination = CreateDestinationRequest { name, config };
        let response = app.create_destination(tenant_id, &destination).await;
        let response: CreateDestinationResponse = response
            .json()
            .await
            .expect("failed to deserialize response");
        response.id
    }

    /// Creates a default destination and returns its id.
    pub async fn create_destination(app: &TestApp, tenant_id: &str) -> i64 {
        create_destination_with_config(
            app,
            tenant_id,
            new_name(),
            new_bigquery_destination_config(),
        )
        .await
    }
}

/// Source helpers.
pub mod sources {
    use super::*;

    /// Returns a default source name.
    pub fn new_name() -> String {
        "Postgres Source".to_string()
    }

    /// Returns a default Postgres source config.
    pub fn new_source_config() -> FullApiSourceConfig {
        FullApiSourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "postgres".to_string(),
            username: "postgres".to_string(),
            password: Some(SerializableSecretString::from("postgres".to_string())),
        }
    }

    /// Returns an updated source name.
    pub fn updated_name() -> String {
        "Postgres Source (Updated)".to_string()
    }

    /// Returns an updated Postgres source config.
    pub fn updated_source_config() -> FullApiSourceConfig {
        FullApiSourceConfig {
            host: "example.com".to_string(),
            port: 2345,
            name: "sergtsop".to_string(),
            username: "sergtsop".to_string(),
            password: Some(SerializableSecretString::from("sergtsop".to_string())),
        }
    }

    /// Creates a default source and returns its id.
    pub async fn create_source(app: &TestApp, tenant_id: &str) -> i64 {
        create_source_with_config(app, tenant_id, new_name(), new_source_config()).await
    }

    /// Creates a source with the provided name and config and returns its id.
    pub async fn create_source_with_config(
        app: &TestApp,
        tenant_id: &str,
        name: String,
        config: FullApiSourceConfig,
    ) -> i64 {
        let source = CreateSourceRequest { name, config };
        let response = app.create_source(tenant_id, &source).await;
        let response: CreateSourceResponse = response
            .json()
            .await
            .expect("failed to deserialize response");
        response.id
    }
}

/// Tenant helpers.
pub mod tenants {
    use super::*;
    use etl_api::routes::tenants::{CreateTenantRequest, CreateTenantResponse};

    /// Creates a default tenant and returns its id.
    pub async fn create_tenant(app: &TestApp) -> String {
        create_tenant_with_id_and_name(
            app,
            "abcdefghijklmnopqrst".to_string(),
            "NewTenant".to_string(),
        )
        .await
    }

    /// Creates a tenant with a given id and name and returns its id.
    pub async fn create_tenant_with_id_and_name(app: &TestApp, id: String, name: String) -> String {
        let tenant = CreateTenantRequest { id, name };
        let response = app.create_tenant(&tenant).await;
        let response: CreateTenantResponse = response
            .json()
            .await
            .expect("failed to deserialize response");
        response.id
    }
}

/// Pipeline config helpers.
pub mod pipelines {
    use super::*;
    use etl_api::configs::{log::LogLevel, pipeline::ApiBatchConfig};
    use etl_config::shared::TableSyncCopyConfig;

    /// Returns a default pipeline config.
    pub fn new_pipeline_config() -> FullApiPipelineConfig {
        FullApiPipelineConfig {
            publication_name: "publication".to_owned(),
            batch: Some(ApiBatchConfig {
                max_size: Some(1000),
                max_fill_ms: Some(5),
            }),
            table_error_retry_delay_ms: Some(10000),
            table_error_retry_max_attempts: Some(5),
            max_table_sync_workers: Some(2),
            table_sync_copy: Some(TableSyncCopyConfig::IncludeAllTables),
            invalidated_slot_behavior: None,
            log_level: Some(LogLevel::Info),
        }
    }

    /// Returns an updated pipeline config.
    pub fn updated_pipeline_config() -> FullApiPipelineConfig {
        FullApiPipelineConfig {
            publication_name: "updated_publication".to_owned(),
            batch: Some(ApiBatchConfig {
                max_size: Some(2000),
                max_fill_ms: Some(10),
            }),
            table_error_retry_delay_ms: Some(20000),
            table_error_retry_max_attempts: Some(10),
            max_table_sync_workers: Some(4),
            table_sync_copy: Some(TableSyncCopyConfig::IncludeAllTables),
            invalidated_slot_behavior: None,
            log_level: Some(LogLevel::Info),
        }
    }

    /// Partial config update variants used in tests.
    pub enum ConfigUpdateType {
        Batch(ApiBatchConfig),
        TableErrorRetryDelayMs(u64),
        TableErrorRetryMaxAttempts(u32),
        MaxTableSyncWorkers(u16),
        LogLevel(Option<LogLevel>),
    }

    /// Returns a partial pipeline config with a single field updated.
    pub fn partially_updated_optional_pipeline_config(
        update: ConfigUpdateType,
    ) -> PartialApiPipelineConfig {
        match update {
            ConfigUpdateType::Batch(batch_config) => PartialApiPipelineConfig {
                publication_name: None,
                batch: Some(batch_config),
                table_error_retry_delay_ms: None,
                table_error_retry_max_attempts: None,
                max_table_sync_workers: None,
                table_sync_copy: None,
                invalidated_slot_behavior: None,
                log_level: None,
            },
            ConfigUpdateType::TableErrorRetryDelayMs(table_error_retry_delay_ms) => {
                PartialApiPipelineConfig {
                    publication_name: None,
                    batch: None,
                    table_error_retry_delay_ms: Some(table_error_retry_delay_ms),
                    table_error_retry_max_attempts: None,
                    max_table_sync_workers: None,
                    table_sync_copy: None,
                    invalidated_slot_behavior: None,
                    log_level: None,
                }
            }
            ConfigUpdateType::TableErrorRetryMaxAttempts(max_attempts) => {
                PartialApiPipelineConfig {
                    publication_name: None,
                    batch: None,
                    table_error_retry_delay_ms: None,
                    table_error_retry_max_attempts: Some(max_attempts),
                    max_table_sync_workers: None,
                    table_sync_copy: None,
                    invalidated_slot_behavior: None,
                    log_level: None,
                }
            }
            ConfigUpdateType::MaxTableSyncWorkers(n) => PartialApiPipelineConfig {
                publication_name: None,
                batch: None,
                table_error_retry_delay_ms: None,
                table_error_retry_max_attempts: None,
                max_table_sync_workers: Some(n),
                table_sync_copy: None,
                invalidated_slot_behavior: None,
                log_level: None,
            },
            ConfigUpdateType::LogLevel(log_level) => PartialApiPipelineConfig {
                publication_name: None,
                batch: None,
                table_error_retry_delay_ms: None,
                table_error_retry_max_attempts: None,
                max_table_sync_workers: None,
                table_sync_copy: None,
                invalidated_slot_behavior: None,
                log_level,
            },
        }
    }

    /// Returns a partial pipeline config with multiple optional fields updated.
    pub fn updated_optional_pipeline_config() -> PartialApiPipelineConfig {
        PartialApiPipelineConfig {
            publication_name: None,
            batch: Some(ApiBatchConfig {
                max_size: Some(1_000_000),
                max_fill_ms: Some(100),
            }),
            table_error_retry_delay_ms: Some(10000),
            table_error_retry_max_attempts: Some(6),
            max_table_sync_workers: Some(8),
            table_sync_copy: None,
            invalidated_slot_behavior: None,
            log_level: None,
        }
    }

    /// Creates a pipeline with the provided config and returns its id.
    pub async fn create_pipeline_with_config(
        app: &TestApp,
        tenant_id: &str,
        source_id: i64,
        destination_id: i64,
        config: FullApiPipelineConfig,
    ) -> i64 {
        let pipeline = CreatePipelineRequest {
            source_id,
            destination_id,
            config,
        };
        let response = app.create_pipeline(tenant_id, &pipeline).await;
        assert!(response.status().is_success(), "failed to created pipeline");

        let response: CreatePipelineResponse = response
            .json()
            .await
            .expect("failed to deserialize response");

        response.id
    }
}
