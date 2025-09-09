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
            max_concurrent_streams: Some(1),
        }
    }

    /// Returns a default destination config.
    pub fn new_destination_config() -> FullApiDestinationConfig {
        FullApiDestinationConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: SerializableSecretString::from("service-account-key".to_string()),
            max_staleness_mins: None,
            max_concurrent_streams: Some(1),
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
        create_destination_with_config(app, tenant_id, new_name(), new_destination_config()).await
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
    use etl_api::configs::pipeline::ApiBatchConfig;

    /// Returns a default pipeline config.
    pub fn new_pipeline_config() -> FullApiPipelineConfig {
        FullApiPipelineConfig {
            publication_name: "publication".to_owned(),
            batch: Some(ApiBatchConfig {
                max_size: Some(1000),
                max_fill_ms: Some(5),
            }),
            table_error_retry_delay_ms: Some(10000),
            max_table_sync_workers: Some(2),
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
            max_table_sync_workers: Some(4),
        }
    }

    /// Partial config update variants used in tests.
    pub enum ConfigUpdateType {
        Batch(ApiBatchConfig),
        TableErrorRetryDelayMs(u64),
        MaxTableSyncWorkers(u16),
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
                max_table_sync_workers: None,
            },
            ConfigUpdateType::TableErrorRetryDelayMs(table_error_retry_delay_ms) => {
                PartialApiPipelineConfig {
                    publication_name: None,
                    batch: None,
                    table_error_retry_delay_ms: Some(table_error_retry_delay_ms),
                    max_table_sync_workers: None,
                }
            }
            ConfigUpdateType::MaxTableSyncWorkers(n) => PartialApiPipelineConfig {
                publication_name: None,
                batch: None,
                table_error_retry_delay_ms: None,
                max_table_sync_workers: Some(n),
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
            max_table_sync_workers: Some(8),
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
