use api::routes::destinations::{
    CreateDestinationRequest, CreateDestinationResponse, ReadDestinationResponse,
    ReadDestinationsResponse, UpdateDestinationRequest,
};
use config::shared::DestinationConfig;
use config::SerializableSecretString;
use reqwest::StatusCode;

use crate::{
    common::test_app::{spawn_test_app, TestApp},
    integration::tenants_test::create_tenant,
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
