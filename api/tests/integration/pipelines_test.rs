use api::db::pipelines::PipelineConfig;
use config::shared::{BatchConfig, RetryConfig};
use reqwest::StatusCode;

use crate::{
    common::test_app::{
        spawn_test_app, CreatePipelineRequest, CreatePipelineResponse, PipelineResponse,
        PipelinesResponse, TestApp, UpdatePipelineRequest,
    },
    integration::destination_test::create_destination,
    integration::images_test::create_default_image,
    integration::sources_test::create_source,
    integration::tenants_test::create_tenant,
    integration::tenants_test::create_tenant_with_id_and_name,
};

pub fn new_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        publication_name: "publication".to_owned(),
        batch: BatchConfig {
            max_size: 1000,
            max_fill_ms: 5,
        },
        apply_worker_init_retry: RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 2000,
            backoff_factor: 0.5,
        },
    }
}

pub fn updated_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        publication_name: "updated_publication".to_owned(),
        batch: BatchConfig {
            max_size: 2000,
            max_fill_ms: 10,
        },
        apply_worker_init_retry: RetryConfig {
            max_attempts: 10,
            initial_delay_ms: 2000,
            max_delay_ms: 4000,
            backoff_factor: 1.0,
        },
    }
}

pub async fn create_pipeline_with_config(
    app: &TestApp,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
    config: PipelineConfig,
) -> i64 {
    create_default_image(app).await;
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config,
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_can_be_created() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_source_cant_be_created() {
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
    let destinaion1_id = create_destination(&app, tenant1_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id: source2_id,
        destination_id: destinaion1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_destination_cant_be_created() {
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
    let destination2_id = create_destination(&app, tenant2_id).await;

    // Act
    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        destination_id: destination2_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_read() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let response = app.read_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: PipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_ne!(response.replicator_id, 0);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_pipeline_cant_be_read() {
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.read_pipeline(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_updated() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id,
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: PipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.destination_id, destination_id);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_source_cant_be_updated() {
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
    let destination1_id = create_destination(&app, tenant1_id).await;

    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        destination_id: destination1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let source2_id = create_source(&app, tenant2_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id: source2_id,
        destination_id: destination1_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant1_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_with_another_tenants_destination_cant_be_updated() {
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
    let destination1_id = create_destination(&app, tenant1_id).await;

    let pipeline = CreatePipelineRequest {
        source_id: source1_id,
        destination_id: destination1_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant1_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let destination2_id = create_destination(&app, tenant2_id).await;
    let updated_config = UpdatePipelineRequest {
        source_id: source1_id,
        destination_id: destination2_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant1_id, pipeline_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_pipeline_cant_be_updated() {
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Act
    let updated_config = UpdatePipelineRequest {
        source_id,
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app.update_pipeline(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_deleted() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    let response = app.delete_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_pipeline_cant_be_deleted() {
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let response = app.delete_pipeline(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_pipelines_can_be_read() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source1_id = create_source(&app, tenant_id).await;
    let source2_id = create_source(&app, tenant_id).await;
    let destination1_id = create_destination(&app, tenant_id).await;
    let destination2_id = create_destination(&app, tenant_id).await;

    let pipeline1_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source1_id,
        destination1_id,
        new_pipeline_config(),
    )
    .await;
    let pipeline2_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source2_id,
        destination2_id,
        updated_pipeline_config(),
    )
    .await;

    // Act
    let response = app.read_all_pipelines(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: PipelinesResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    for pipeline in response.pipelines {
        if pipeline.id == pipeline1_id {
            assert_eq!(&pipeline.tenant_id, tenant_id);
            assert_eq!(pipeline.source_id, source1_id);
            assert_eq!(pipeline.destination_id, destination1_id);
            insta::assert_debug_snapshot!(pipeline.config);
        } else if pipeline.id == pipeline2_id {
            assert_eq!(&pipeline.tenant_id, tenant_id);
            assert_eq!(pipeline.source_id, source2_id);
            assert_eq!(pipeline.destination_id, destination2_id);
            insta::assert_debug_snapshot!(pipeline.config);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_source_cascade_deletes_the_pipeline() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    app.delete_source(tenant_id, source_id).await;

    // Assert
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_destination_cascade_deletes_the_pipeline() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Act
    app.delete_destination(tenant_id, destination_id).await;

    // Assert
    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_pipeline_with_same_source_and_destination_cant_be_created() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Create first pipeline
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(response.status().is_success());

    // Act - Try to create duplicate pipeline with same source and destination
    let duplicate_pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &duplicate_pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_pipeline_to_duplicate_source_destination_combination_fails() {
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source1_id = create_source(&app, tenant_id).await;
    let source2_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    // Create first pipeline
    let pipeline1 = CreatePipelineRequest {
        source_id: source1_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline1).await;
    assert!(response.status().is_success());

    // Create second pipeline with different source
    let pipeline2 = CreatePipelineRequest {
        source_id: source2_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline2).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline2_id = response.id;

    // Act - Try to update second pipeline to have same source as first
    let updated_config = UpdatePipelineRequest {
        source_id: source1_id, // This would create a duplicate
        destination_id,
        config: updated_pipeline_config(),
    };
    let response = app
        .update_pipeline(tenant_id, pipeline2_id, &updated_config)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::CONFLICT);
}
