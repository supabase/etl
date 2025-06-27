use api::routes::destinations::ReadDestinationResponse;
use api::routes::destinations_pipelines::{
    CreateDestinationPipelineRequest, CreateDestinationPipelineResponse,
    UpdateDestinationPipelineRequest,
};
use api::routes::pipelines::{CreatePipelineRequest, ReadPipelineResponse};
use reqwest::StatusCode;
use telemetry::init_test_tracing;

use crate::{
    common::test_app::spawn_test_app,
    integration::destination_test::{
        create_destination, new_destination_config, new_name, updated_destination_config,
        updated_name,
    },
    integration::images_test::create_default_image,
    integration::pipelines_test::{new_pipeline_config, updated_pipeline_config},
    integration::sources_test::create_source,
    integration::tenants_test::{create_tenant, create_tenant_with_id_and_name},
};

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;

    // Act
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant_id, &destination_pipeline)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateDestinationPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.destination_id, 1);
    assert_eq!(response.pipeline_id, 1);

    let destination_id = response.destination_id;
    let pipeline_id = response.pipeline_id;

    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(response.name, destination_pipeline.destination_name);
    insta::assert_debug_snapshot!(response.config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: ReadPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_eq!(response.replicator_id, 1);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_with_another_tenants_source_cant_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;
    let source2_id = create_source(&app, tenant2_id).await;

    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id: source2_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant1_id, &destination_pipeline)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_destination_and_pipeline_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant_id, &destination_pipeline)
        .await;
    let response: CreateDestinationPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateDestinationPipelineResponse {
        destination_id,
        pipeline_id,
    } = response;
    let new_source_id = create_source(&app, tenant_id).await;

    // Act
    let destination_pipeline = UpdateDestinationPipelineRequest {
        destination_name: updated_name(),
        destination_config: updated_destination_config(),
        source_id: new_source_id,
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_destination_pipeline(
            tenant_id,
            destination_id,
            pipeline_id,
            &destination_pipeline,
        )
        .await;

    // Assert
    assert!(response.status().is_success());

    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(response.name, destination_pipeline.destination_name);
    insta::assert_debug_snapshot!(response.config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: ReadPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, destination_pipeline.source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_eq!(response.replicator_id, 1);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_with_another_tenants_source_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;

    let source1_id = create_source(&app, tenant1_id).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id: source1_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant1_id, &destination_pipeline)
        .await;
    let response: CreateDestinationPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateDestinationPipelineResponse {
        destination_id,
        pipeline_id,
    } = response;

    // Act
    let source2_id = create_source(&app, tenant2_id).await;
    let destination_pipeline = UpdateDestinationPipelineRequest {
        destination_name: updated_name(),
        destination_config: updated_destination_config(),
        source_id: source2_id,
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_destination_pipeline(
            tenant1_id,
            destination_id,
            pipeline_id,
            &destination_pipeline,
        )
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_with_another_tenants_destination_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;

    let source1_id = create_source(&app, tenant1_id).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id: source1_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant1_id, &destination_pipeline)
        .await;
    let response: CreateDestinationPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateDestinationPipelineResponse { pipeline_id, .. } = response;

    // Act
    let destination2_id = create_destination(&app, tenant2_id).await;
    let destination_pipeline = UpdateDestinationPipelineRequest {
        destination_name: updated_name(),
        destination_config: updated_destination_config(),
        source_id: source1_id,
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_destination_pipeline(
            tenant1_id,
            destination2_id,
            pipeline_id,
            &destination_pipeline,
        )
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_with_another_tenants_pipeline_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "tenant_1".to_string(),
    )
    .await;
    let tenant2_id = &create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "tenant_2".to_string(),
    )
    .await;

    let source1_id = create_source(&app, tenant1_id).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id: source1_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant1_id, &destination_pipeline)
        .await;
    let response: CreateDestinationPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateDestinationPipelineResponse {
        destination_id: destination1_id,
        ..
    } = response;

    let source2_id = create_source(&app, tenant2_id).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id: source2_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant2_id, &destination_pipeline)
        .await;
    let response: CreateDestinationPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let CreateDestinationPipelineResponse {
        pipeline_id: pipeline2_id,
        ..
    } = response;

    // Act
    let destination_pipeline = UpdateDestinationPipelineRequest {
        destination_name: updated_name(),
        destination_config: updated_destination_config(),
        source_id: source1_id,
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_destination_pipeline(
            tenant1_id,
            destination1_id,
            pipeline2_id,
            &destination_pipeline,
        )
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_destination_pipeline_with_same_source_cant_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;

    // Create first destination and pipeline
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app
        .create_destination_pipeline(tenant_id, &destination_pipeline)
        .await;
    assert!(response.status().is_success());
    let response: CreateDestinationPipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let first_destination_id = response.destination_id;

    // Act - Try to create another pipeline with same source and the first destination
    let pipeline_request = CreatePipelineRequest {
        source_id,
        destination_id: first_destination_id,
        config: updated_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline_request).await;

    // Assert
    assert_eq!(response.status(), StatusCode::CONFLICT);
}
