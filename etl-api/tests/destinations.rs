use etl_api::routes::destinations::{
    CreateDestinationRequest, CreateDestinationResponse, ReadDestinationResponse,
    ReadDestinationsResponse, UpdateDestinationRequest,
};
use etl_api::routes::pipelines::{CreatePipelineRequest, CreatePipelineResponse};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

use crate::{
    support::mocks::create_default_image,
    support::mocks::destinations::{
        create_destination, create_destination_with_config, new_destination_config, new_name,
        updated_destination_config, updated_name,
    },
    support::mocks::pipelines::new_pipeline_config,
    support::mocks::sources::create_source,
    support::mocks::tenants::create_tenant,
    support::test_app::spawn_test_app,
};

mod support;

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
async fn non_existing_destination_cannot_be_read() {
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
async fn non_existing_destination_cannot_be_updated() {
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
async fn non_existing_destination_cannot_be_deleted() {
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
