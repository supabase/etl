use api::db::pipelines::PipelineConfig;
use api::db::sources::SourceConfig;
use api::routes::pipelines::{
    CreatePipelineRequest, CreatePipelineResponse, GetPipelineReplicationStatusResponse,
    ReadPipelineResponse, ReadPipelinesResponse, SimpleTableReplicationState,
    UpdatePipelineImageRequest, UpdatePipelineRequest,
};
use api::routes::sources::{CreateSourceRequest, CreateSourceResponse};
use config::SerializableSecretString;
use config::shared::{BatchConfig, RetryConfig};
use postgres::sqlx::test_utils::{create_pg_database, drop_pg_database};
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use sqlx::postgres::types::Oid;
use telemetry::init_test_tracing;
use uuid::Uuid;

use crate::{
    common::test_app::{TestApp, spawn_test_app},
    integration::destination_test::create_destination,
    integration::images_test::create_default_image,
    integration::sources_test::create_source,
    integration::tenants_test::create_tenant,
    integration::tenants_test::create_tenant_with_id_and_name,
};

pub fn new_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        publication_name: "publication".to_owned(),
        batch: Some(BatchConfig {
            max_size: 1000,
            max_fill_ms: 5,
        }),
        apply_worker_init_retry: Some(RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 2000,
            backoff_factor: 0.5,
        }),
        max_table_sync_workers: Some(2),
    }
}

pub fn updated_pipeline_config() -> PipelineConfig {
    PipelineConfig {
        publication_name: "updated_publication".to_owned(),
        batch: Some(BatchConfig {
            max_size: 2000,
            max_fill_ms: 10,
        }),
        apply_worker_init_retry: Some(RetryConfig {
            max_attempts: 10,
            initial_delay_ms: 2000,
            max_delay_ms: 4000,
            backoff_factor: 1.0,
        }),
        max_table_sync_workers: Some(4),
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
    init_test_tracing();
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
    init_test_tracing();
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
    let response: ReadPipelineResponse = response
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
    init_test_tracing();
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
    init_test_tracing();
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
    let response: ReadPipelineResponse = response
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
    init_test_tracing();
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
    init_test_tracing();
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
    init_test_tracing();
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
    init_test_tracing();
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
    let response: ReadPipelinesResponse = response
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
    init_test_tracing();
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
    init_test_tracing();
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
    init_test_tracing();
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
    init_test_tracing();
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

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_image_can_be_updated_with_specific_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;

    // Act
    let update_request = UpdatePipelineImageRequest {
        image_id: Some(1), // Use the default image ID
    };
    let response = app
        .update_pipeline_image(tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_image_can_be_updated_to_default_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;

    // Act - update to default image (no image_id specified)
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image(tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_for_non_existing_pipeline() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image(tenant_id, 42, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_for_non_existing_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;

    // Act
    let update_request = UpdatePipelineImageRequest {
        image_id: Some(999), // Non-existing image ID
    };
    let response = app
        .update_pipeline_image(tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_for_pipeline_from_another_tenant() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant1_id = &create_tenant(&app).await;

    let source1_id = create_source(&app, tenant1_id).await;
    let destination1_id = create_destination(&app, tenant1_id).await;

    let pipeline_id = create_pipeline_with_config(
        &app,
        tenant1_id,
        source1_id,
        destination1_id,
        new_pipeline_config(),
    )
    .await;

    // Act - Try to update image using wrong tenant credentials
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image("wrong-tenant-id", pipeline_id, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_image_fails_when_no_default_image_exists() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    // Don't create default image
    let tenant_id = &create_tenant(&app).await;

    // Act - Try to update to default image when none exists
    let update_request = UpdatePipelineImageRequest { image_id: None };
    let response = app
        .update_pipeline_image(tenant_id, 1, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_started() {
    init_test_tracing();
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
    let response = app.start_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_stopped() {
    init_test_tracing();
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
    let response = app.stop_pipeline(tenant_id, pipeline_id).await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn all_pipelines_can_be_stopped() {
    init_test_tracing();
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
    let _response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    // Act
    let response = app.stop_all_pipelines(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_replication_status_returns_table_states_and_names() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = &create_tenant(&app).await;

    // Create a separate database for the source using create_pg_database
    let mut source_db_config = app.database_config().clone();
    source_db_config.name = format!("fake_source_db_{}", Uuid::new_v4());
    let source_pool = create_pg_database(&source_db_config).await;
    let source_config = SourceConfig {
        host: source_db_config.host.clone(),
        port: source_db_config.port,
        name: source_db_config.name.clone(),
        username: source_db_config.username.clone(),
        password: source_db_config
            .password
            .as_ref()
            .map(|p| SerializableSecretString::from(p.expose_secret().to_string())),
    };

    let source = CreateSourceRequest {
        name: "Test Source".to_string(),
        config: source_config,
    };

    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    let destination_id = create_destination(&app, tenant_id).await;

    // Create pipeline
    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = response.id;

    // Create etl schema and replication_state table with proper enum
    sqlx::query("CREATE SCHEMA IF NOT EXISTS etl")
        .execute(&source_pool)
        .await
        .expect("Failed to create etl schema");

    // Create the table_state enum
    sqlx::query(
        r#"
        CREATE TYPE etl.table_state AS ENUM (
            'init',
            'data_sync',
            'finished_copy',
            'sync_done',
            'ready',
            'skipped'
        )
    "#,
    )
    .execute(&source_pool)
    .await
    .expect("Failed to create table_state enum");

    // Create the replication_state table
    sqlx::query(
        r#"
        CREATE TABLE etl.replication_state (
            pipeline_id BIGINT NOT NULL,
            table_id OID NOT NULL,
            state etl.table_state NOT NULL,
            sync_done_lsn TEXT NULL,
            PRIMARY KEY (pipeline_id, table_id)
        )
    "#,
    )
    .execute(&source_pool)
    .await
    .expect("Failed to create replication_state table");

    // Create some test tables to get real OIDs
    let users_table_name = "public.test_table_users";
    sqlx::query("CREATE TABLE IF NOT EXISTS test_table_users (id SERIAL PRIMARY KEY, name TEXT)")
        .execute(&source_pool)
        .await
        .expect("Failed to create test_table_users");

    let orders_table_name = "public.test_table_orders";
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_table_orders (id SERIAL PRIMARY KEY, user_id INT)",
    )
    .execute(&source_pool)
    .await
    .expect("Failed to create test_table_orders");

    // Get the OIDs of the test tables
    let users_table_oid = sqlx::query_scalar::<_, Oid>(
        "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = $1 AND n.nspname = $2"
    )
    .bind("test_table_users")
    .bind("public")
    .fetch_one(&source_pool)
    .await
    .expect("Failed to get users table OID").0 as i64;

    let orders_table_oid = sqlx::query_scalar::<_, Oid>(
        "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = $1 AND n.nspname = $2"
    )
    .bind("test_table_orders")
    .bind("public")
    .fetch_one(&source_pool)
    .await
    .expect("Failed to get orders table OID").0 as i64;

    // Insert test data into etl.replication_state table
    sqlx::query(
        r#"
        INSERT INTO etl.replication_state (pipeline_id, table_id, state, sync_done_lsn)
        VALUES 
            ($1, $2, 'data_sync'::etl.table_state, NULL),
            ($1, $3, 'ready'::etl.table_state, '0/12345678')
    "#,
    )
    .bind(pipeline_id)
    .bind(users_table_oid)
    .bind(orders_table_oid)
    .execute(&source_pool)
    .await
    .expect("Failed to insert test replication state data");

    // Test the endpoint with actual replication state data
    let response = app
        .get_pipeline_replication_status(tenant_id, pipeline_id)
        .await;
    let response: GetPipelineReplicationStatusResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    assert_eq!(response.pipeline_id, pipeline_id);
    // Should have 2 table statuses (users and orders tables)
    assert_eq!(response.table_statuses.len(), 2);

    // Find the tables and verify their data
    let users_table_status = response
        .table_statuses
        .iter()
        .find(|s| s.table_name == users_table_name)
        .expect("Users table not found in response");
    let orders_table_status = response
        .table_statuses
        .iter()
        .find(|s| s.table_name == orders_table_name)
        .expect("Orders table not found in response");

    // Verify states are correctly mapped
    assert_eq!(users_table_status.table_id, users_table_oid as u32);
    match &users_table_status.state {
        SimpleTableReplicationState::CopyingTable => {} // Expected for data_sync state
        other => panic!("Expected CopyingTable state for users table, got {other:?}"),
    }
    assert_eq!(orders_table_status.table_id, orders_table_oid as u32);
    match &orders_table_status.state {
        SimpleTableReplicationState::FollowingWal { lag: _ } => {} // Expected for ready state
        other => panic!("Expected FollowingWal state for orders table, got {other:?}"),
    }

    // We destroy the database
    drop_pg_database(&source_db_config).await;
}
