use etl_api::routes::pipelines::{CreatePipelineRequest, CreatePipelineResponse};
use etl_api::routes::sources::{
    CreateSourceRequest, CreateSourceResponse, ReadSourceResponse, ReadSourcesResponse,
    UpdateSourceRequest,
};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

use crate::support::mocks::create_default_image;
use crate::support::mocks::destinations::create_destination;
use crate::support::mocks::pipelines::new_pipeline_config;
use crate::support::mocks::sources::{
    create_source, create_source_with_config, new_name, new_source_config, updated_name,
    updated_source_config,
};
use crate::support::mocks::tenants::create_tenant;
use crate::support::test_app::spawn_test_app;

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn source_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let source = CreateSourceRequest {
        name: new_name(),
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_source_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest {
        name: new_name(),
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let response = app.read_source(tenant_id, source_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, source_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, source.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_source_cannot_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.read_source(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_source_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest {
        name: new_name(),
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let updated_config = UpdateSourceRequest {
        name: updated_name(),
        config: updated_source_config(),
    };
    let response = app
        .update_source(tenant_id, source_id, &updated_config)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_source(tenant_id, source_id).await;
    let response: ReadSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, source_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, updated_config.name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_source_cannot_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let updated_config = UpdateSourceRequest {
        name: updated_name(),
        config: updated_source_config(),
    };
    let response = app.update_source(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_source_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest {
        name: new_name(),
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let response = app.delete_source(tenant_id, source_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_source(tenant_id, source_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_source_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.delete_source(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_sources_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source1_id =
        create_source_with_config(&app, tenant_id, new_name(), new_source_config()).await;
    let source2_id =
        create_source_with_config(&app, tenant_id, updated_name(), updated_source_config()).await;

    // Act
    let response = app.read_all_sources(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadSourcesResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    for source in response.sources {
        if source.id == source1_id {
            let name = new_name();
            assert_eq!(&source.tenant_id, tenant_id);
            assert_eq!(source.name, name);
            insta::assert_debug_snapshot!(source.config);
        } else if source.id == source2_id {
            let name = updated_name();
            assert_eq!(&source.tenant_id, tenant_id);
            assert_eq!(source.name, name);
            insta::assert_debug_snapshot!(source.config);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_with_active_pipeline_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    // Create source and destination
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Create a pipeline that uses this source
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

    // Act - Try to delete the source
    let response = app.delete_source(tenant_id, source_id).await;

    // Assert - Should fail due to foreign key constraint
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // Verify source still exists
    let source_response = app.read_source(tenant_id, source_id).await;
    assert!(source_response.status().is_success());
}
