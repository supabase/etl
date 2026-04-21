use etl_api::{
    k8s::PodStatus,
    routes::{
        destinations::ReadDestinationResponse,
        destinations_pipelines::{
            CreateDestinationPipelineRequest, CreateDestinationPipelineResponse,
            DeleteDestinationPipelineResponse, UpdateDestinationPipelineRequest,
        },
        pipelines::ReadPipelineResponse,
    },
};
use etl_config::shared::PgConnectionConfig;
use etl_postgres::sqlx::test_utils::drop_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use sqlx::PgPool;

use crate::support::{
    database::{create_test_source_database, run_etl_migrations_on_source_database},
    mocks::{
        create_default_image,
        destinations::{
            create_destination, new_bigquery_destination_config,
            new_iceberg_supabase_destination_config, new_name, updated_destination_config,
            updated_iceberg_supabase_destination_config, updated_name,
        },
        pipelines::{new_pipeline_config, updated_pipeline_config},
        sources::create_source,
        tenants::{create_tenant, create_tenant_with_id_and_name},
    },
    test_app::{TestApp, spawn_test_app},
};

struct DestinationPipelineDeletionFixture {
    tenant_id: String,
    destination_id: i64,
    pipeline_id: i64,
    source_pool: PgPool,
    source_db_config: PgConnectionConfig,
}

async fn create_destination_pipeline_deletion_fixture(
    app: &TestApp,
) -> DestinationPipelineDeletionFixture {
    let tenant_id = create_tenant(app).await;
    create_default_image(app).await;

    let (source_pool, source_id, source_db_config) =
        create_test_source_database(app, &tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_bigquery_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(&tenant_id, &destination_pipeline).await;
    assert!(response.status().is_success());
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");

    DestinationPipelineDeletionFixture {
        tenant_id,
        destination_id: response.destination_id,
        pipeline_id: response.pipeline_id,
        source_pool,
        source_db_config,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bigquery_destination_and_pipeline_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;

    // Act
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_bigquery_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant_id, &destination_pipeline).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.destination_id, 1);
    assert_eq!(response.pipeline_id, 1);

    let destination_id = response.destination_id;
    let pipeline_id = response.pipeline_id;

    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(response.name, destination_pipeline.destination_name);
    insta::assert_debug_snapshot!(response.config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: ReadPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_eq!(response.replicator_id, 1);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_supabase_destination_and_pipeline_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;

    // Act
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: "Iceberg Supabase Destination".to_string(),
        destination_config: new_iceberg_supabase_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant_id, &destination_pipeline).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.destination_id, 1);
    assert_eq!(response.pipeline_id, 1);

    let destination_id = response.destination_id;
    let pipeline_id = response.pipeline_id;

    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(response.name, destination_pipeline.destination_name);
    insta::assert_debug_snapshot!(response.config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: ReadPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_eq!(response.replicator_id, 1);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_cannot_create_more_than_max_destinations_pipelines() {
    use etl_api::db::pipelines::MAX_PIPELINES_PER_TENANT;

    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;

    // Create the maximum allowed pipelines
    for idx in 0..MAX_PIPELINES_PER_TENANT {
        let destination_pipeline = CreateDestinationPipelineRequest {
            destination_name: format!("BigQuery Destination {idx}"),
            destination_config: new_bigquery_destination_config(),
            source_id,
            pipeline_config: new_pipeline_config(),
        };
        let response = app.create_destination_pipeline(tenant_id, &destination_pipeline).await;
        assert!(response.status().is_success());
    }

    // Attempt to create one more pipeline should fail
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: format!("BigQuery Destination {MAX_PIPELINES_PER_TENANT}"),
        destination_config: new_bigquery_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant_id, &destination_pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_with_another_tenants_source_cannot_be_created() {
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
        destination_config: new_bigquery_destination_config(),
        source_id: source2_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant1_id, &destination_pipeline).await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_bigquery_destination_and_pipeline_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_bigquery_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant_id, &destination_pipeline).await;
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    let CreateDestinationPipelineResponse { destination_id, pipeline_id } = response;
    let new_source_id = create_source(&app, tenant_id).await;

    // Act
    let destination_pipeline = UpdateDestinationPipelineRequest {
        destination_name: updated_name(),
        destination_config: updated_destination_config(),
        source_id: new_source_id,
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_destination_pipeline(tenant_id, destination_id, pipeline_id, &destination_pipeline)
        .await;

    // Assert
    assert!(response.status().is_success());

    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(response.name, destination_pipeline.destination_name);
    insta::assert_debug_snapshot!(response.config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: ReadPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, destination_pipeline.source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_eq!(response.replicator_id, 1);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_iceberg_supabase_destination_and_pipeline_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let source_id = create_source(&app, tenant_id).await;
    create_default_image(&app).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: "Iceberg Supabase Destination".to_string(),
        destination_config: new_iceberg_supabase_destination_config(),
        source_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant_id, &destination_pipeline).await;
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    let CreateDestinationPipelineResponse { destination_id, pipeline_id } = response;
    let new_source_id = create_source(&app, tenant_id).await;

    // Act
    let destination_pipeline = UpdateDestinationPipelineRequest {
        destination_name: "Iceberg Supabase Destination (Updated)".to_string(),
        destination_config: updated_iceberg_supabase_destination_config(),
        source_id: new_source_id,
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_destination_pipeline(tenant_id, destination_id, pipeline_id, &destination_pipeline)
        .await;

    // Assert
    assert!(response.status().is_success());

    let response = app.read_destination(tenant_id, destination_id).await;
    let response: ReadDestinationResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, destination_id);
    assert_eq!(response.name, destination_pipeline.destination_name);
    insta::assert_debug_snapshot!(response.config);

    let response = app.read_pipeline(tenant_id, pipeline_id).await;
    let response: ReadPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, pipeline_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.source_id, destination_pipeline.source_id);
    assert_eq!(response.destination_id, destination_id);
    assert_eq!(response.replicator_id, 1);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_with_another_tenants_source_cannot_be_updated() {
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
        destination_config: new_bigquery_destination_config(),
        source_id: source1_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant1_id, &destination_pipeline).await;
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    let CreateDestinationPipelineResponse { destination_id, pipeline_id } = response;

    // Act
    let source2_id = create_source(&app, tenant2_id).await;
    let destination_pipeline = UpdateDestinationPipelineRequest {
        destination_name: updated_name(),
        destination_config: updated_destination_config(),
        source_id: source2_id,
        pipeline_config: updated_pipeline_config(),
    };
    let response = app
        .update_destination_pipeline(tenant1_id, destination_id, pipeline_id, &destination_pipeline)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_with_another_tenants_destination_cannot_be_updated() {
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
        destination_config: new_bigquery_destination_config(),
        source_id: source1_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant1_id, &destination_pipeline).await;
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
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
async fn destination_and_pipeline_with_another_tenants_pipeline_cannot_be_updated() {
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
        destination_config: new_bigquery_destination_config(),
        source_id: source1_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant1_id, &destination_pipeline).await;
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    let CreateDestinationPipelineResponse { destination_id: destination1_id, .. } = response;

    let source2_id = create_source(&app, tenant2_id).await;
    let destination_pipeline = CreateDestinationPipelineRequest {
        destination_name: new_name(),
        destination_config: new_bigquery_destination_config(),
        source_id: source2_id,
        pipeline_config: new_pipeline_config(),
    };
    let response = app.create_destination_pipeline(tenant2_id, &destination_pipeline).await;
    let response: CreateDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    let CreateDestinationPipelineResponse { pipeline_id: pipeline2_id, .. } = response;

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

// TODO: Re-enable these tests once MAX_PIPELINES_PER_TENANT is lifted from 1.
// #[tokio::test(flavor = "multi_thread")]
// async fn duplicate_destination_pipeline_with_same_source_cannot_be_created()
// {     init_test_tracing();
//     // Arrange
//     let app = spawn_test_app().await;
//     create_default_image(&app).await;
//     let tenant_id = &create_tenant(&app).await;
//     let source_id = create_source(&app, tenant_id).await;
//
//     // Create first destination and pipeline
//     let destination_pipeline = CreateDestinationPipelineRequest {
//         destination_name: new_name(),
//         destination_config: new_bigquery_destination_config(),
//         source_id,
//         pipeline_config: new_pipeline_config(),
//     };
//     let response = app
//         .create_destination_pipeline(tenant_id, &destination_pipeline)
//         .await;
//     assert!(response.status().is_success());
//     let response: CreateDestinationPipelineResponse = response
//         .json()
//         .await
//         .expect("failed to deserialize response");
//     let first_destination_id = response.destination_id;
//
//     // Act - Try to create another pipeline with same source and the first
// destination     let pipeline_request = CreatePipelineRequest {
//         source_id,
//         destination_id: first_destination_id,
//         config: updated_pipeline_config(),
//     };
//     let response = app.create_pipeline(tenant_id, &pipeline_request).await;
//
//     // Assert
//     assert_eq!(response.status(), StatusCode::CONFLICT);
// }

#[tokio::test(flavor = "multi_thread")]
async fn destination_and_pipeline_can_be_deleted() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let fixture = create_destination_pipeline_deletion_fixture(&app).await;

    // Verify they exist before deletion
    let destination_response =
        app.read_destination(&fixture.tenant_id, fixture.destination_id).await;
    assert!(destination_response.status().is_success());

    let pipeline_response = app.read_pipeline(&fixture.tenant_id, fixture.pipeline_id).await;
    assert!(pipeline_response.status().is_success());

    app.k8s_state.set_pod_status(PodStatus::Stopped).await;

    // Act - Delete destination and pipeline
    let response = app
        .delete_destination_pipeline(
            &fixture.tenant_id,
            fixture.destination_id,
            fixture.pipeline_id,
        )
        .await;

    // Assert
    let status = response.status();
    assert!(status.is_success());
    let response: DeleteDestinationPipelineResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.destination_id, fixture.destination_id);
    assert_eq!(response.pipeline_id, fixture.pipeline_id);
    assert!(response.destination_deleted);

    // Verify they no longer exist
    let destination_response =
        app.read_destination(&fixture.tenant_id, fixture.destination_id).await;
    assert_eq!(destination_response.status(), StatusCode::NOT_FOUND);

    let pipeline_response = app.read_pipeline(&fixture.tenant_id, fixture.pipeline_id).await;
    assert_eq!(pipeline_response.status(), StatusCode::NOT_FOUND);

    let etl_schema_exists: bool =
        sqlx::query_scalar("select exists(select 1 from pg_namespace where nspname = 'etl')")
            .fetch_one(&fixture.source_pool)
            .await
            .expect("failed to check etl schema");
    assert!(etl_schema_exists);

    drop_pg_database(&fixture.source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn active_destination_and_pipeline_cannot_be_deleted() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let fixture = create_destination_pipeline_deletion_fixture(&app).await;

    let response = app
        .delete_destination_pipeline(
            &fixture.tenant_id,
            fixture.destination_id,
            fixture.pipeline_id,
        )
        .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let destination_response =
        app.read_destination(&fixture.tenant_id, fixture.destination_id).await;
    assert!(destination_response.status().is_success());

    let pipeline_response = app.read_pipeline(&fixture.tenant_id, fixture.pipeline_id).await;
    assert!(pipeline_response.status().is_success());

    drop_pg_database(&fixture.source_db_config).await;
}
