use etl_api::routes::destinations::{
    CreateDestinationRequest, CreateDestinationResponse, ReadDestinationResponse,
    ReadDestinationsResponse, UpdateDestinationRequest,
};
use etl_api::routes::pipelines::{CreatePipelineRequest, CreatePipelineResponse};
use etl_config::SerializableSecretString;
use etl_config::shared::DestinationConfig;
use etl_telemetry::init_test_tracing;
use reqwest::StatusCode;

use crate::{
    common::test_app::{TestApp, spawn_test_app},
    integration::{
        images_test::create_default_image, pipelines_test::new_pipeline_config,
        sources_test::create_source, tenants_test::create_tenant,
    },
};

pub fn new_name() -> String {
    "BigQuery Destination".to_string()
}

pub fn new_destination_config() -> DestinationConfig {
    DestinationConfig::BigQuery {
        project_id: "project-id".to_string(),
        dataset_id: "dataset-id".to_string(),
        service_account_key: SerializableSecretString::from("service-account-key".to_string()),
        max_staleness_mins: None,
    }
}

pub fn updated_name() -> String {
    "BigQuery Destination (Updated)".to_string()
}

pub fn updated_destination_config() -> DestinationConfig {
    DestinationConfig::BigQuery {
        project_id: "project-id-updated".to_string(),
        dataset_id: "dataset-id-updated".to_string(),
        service_account_key: SerializableSecretString::from(
            "service-account-key-updated".to_string(),
        ),
        max_staleness_mins: Some(10),
    }
}

pub async fn create_destination_with_config(
    app: &TestApp,
    tenant_id: &str,
    name: String,
    config: DestinationConfig,
) -> i64 {
    let destination = CreateDestinationRequest { name, config };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

pub async fn create_destination(app: &TestApp, tenant_id: &str) -> i64 {
    create_destination_with_config(app, tenant_id, new_name(), new_destination_config()).await
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let destination = CreateDestinationRequest {
        name: new_name(),
        config: new_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_destination_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination = CreateDestinationRequest {
        name: new_name(),
        config: new_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let response = app.read_destination(tenant_id, destination_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, destination.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_destination_cant_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.read_destination(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_destination_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination = CreateDestinationRequest {
        name: new_name(),
        config: new_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let updated_config = UpdateDestinationRequest {
        name: updated_name(),
        config: updated_destination_config(),
    };
    let response = app
        .update_destination(tenant_id, destination_id, &updated_config)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, updated_config.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_destination_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let updated_config = UpdateDestinationRequest {
        name: updated_name(),
        config: updated_destination_config(),
    };
    let response = app.update_destination(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_destination_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let destination = CreateDestinationRequest {
        name: new_name(),
        config: new_destination_config(),
    };
    let response = app.create_destination(tenant_id, &destination).await;
    let response: CreateDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let destination_id = response.id;

    // Act
    let response = app.delete_destination(tenant_id, destination_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_destination(tenant_id, destination_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_destination_cant_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.delete_destination(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_destinations_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let destination1_id =
        create_destination_with_config(&app, tenant_id, new_name(), new_destination_config()).await;
    let destination2_id = create_destination_with_config(
        &app,
        tenant_id,
        updated_name(),
        updated_destination_config(),
    )
    .await;

    // Act
    let response = app.read_all_destinations(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadDestinationsResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    for destination in response.destinations {
        if destination.id == destination1_id {
            let name = new_name();
            assert_eq!(&destination.tenant_id, tenant_id);
            assert_eq!(destination.name, name);
            insta::assert_debug_snapshot!(destination.config);
        } else if destination.id == destination2_id {
            let name = updated_name();
            assert_eq!(&destination.tenant_id, tenant_id);
            assert_eq!(destination.name, name);
            insta::assert_debug_snapshot!(destination.config);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_with_active_pipeline_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    // Create source and destination
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Create a pipeline that uses this destination
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let pipeline_response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(pipeline_response.status().is_success());
    let pipeline_response: CreatePipelineResponse = pipeline_response
        .json()
        .await
        .expect("failed to deserialize response");
    let _pipeline_id = pipeline_response.id;

    // Act - Try to delete the destination
    let response = app.delete_destination(tenant_id, destination_id).await;

    // Assert - Should fail due to foreign key constraint
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // Verify destination still exists
    let destination_response = app.read_destination(tenant_id, destination_id).await;
    assert!(destination_response.status().is_success());
}
