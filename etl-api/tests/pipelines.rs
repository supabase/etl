use crate::support::database::{
    create_test_source_database, run_etl_migrations_on_source_database,
};
use crate::{
    support::mocks::create_default_image,
    support::mocks::destinations::create_destination,
    support::mocks::sources::create_source,
    support::mocks::tenants::{create_tenant, create_tenant_with_id_and_name},
    support::test_app::{TestApp, spawn_test_app},
};
use etl_api::routes::pipelines::{
    CreatePipelineRequest, CreatePipelineResponse, GetPipelineReplicationStatusResponse,
    ReadPipelineResponse, ReadPipelinesResponse, RollbackTableStateRequest,
    RollbackTableStateResponse, RollbackType, SimpleTableReplicationState,
    UpdatePipelineConfigRequest, UpdatePipelineConfigResponse, UpdatePipelineImageRequest,
    UpdatePipelineRequest,
};
use etl_config::shared::{BatchConfig, PgConnectionConfig};
use etl_postgres::sqlx::test_utils::drop_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use sqlx::PgPool;
use sqlx::postgres::types::Oid;

use crate::support::mocks::pipelines::{
    ConfigUpdateType, create_pipeline_with_config, new_pipeline_config,
    partially_updated_optional_pipeline_config, updated_optional_pipeline_config,
    updated_pipeline_config,
};

mod support;

/// Creates a basic pipeline setup for tests that don't need source databases.
async fn setup_basic_pipeline() -> (TestApp, String, i64, i64, i64) {
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_source(&app, &tenant_id).await;
    let destination_id = create_destination(&app, &tenant_id).await;
    let pipeline_id = create_pipeline_with_config(
        &app,
        &tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;
    (app, tenant_id, source_id, destination_id, pipeline_id)
}

/// Creates a pipeline setup with a real source database for replication state tests.
async fn setup_pipeline_with_source_db() -> (TestApp, String, i64, PgPool, PgConnectionConfig) {
    let app = spawn_test_app().await;
    create_default_image(&app).await;
    let tenant_id = create_tenant(&app).await;
    let (source_db_pool, source_id, source_db_config) =
        create_test_source_database(&app, &tenant_id).await;
    let destination_id = create_destination(&app, &tenant_id).await;
    let pipeline_id = create_pipeline_with_config(
        &app,
        &tenant_id,
        source_id,
        destination_id,
        new_pipeline_config(),
    )
    .await;

    // We run the migrations to create all the tables used by `etl`.
    run_etl_migrations_on_source_database(&source_db_config).await;

    (
        app,
        tenant_id,
        pipeline_id,
        source_db_pool,
        source_db_config,
    )
}

/// Creates a table with a chain of replication states.
/// Each state in the chain becomes the previous state of the next one.
async fn create_table_with_state_chain(
    source_db_pool: &PgPool,
    pipeline_id: i64,
    table_name: &str,
    state_chain: &[(&str, &str)],
) -> Oid {
    let table_oid = create_test_table(source_db_pool, table_name).await;

    let mut prev_id: Option<i64> = None;
    for (i, (state, metadata)) in state_chain.iter().enumerate() {
        let is_current = i == state_chain.len() - 1;
        let id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current) VALUES ($1, $2, $3::etl.table_state, $4::jsonb, $5, $6) RETURNING id"
        )
        .bind(pipeline_id)
        .bind(table_oid)
        .bind(state)
        .bind(metadata)
        .bind(prev_id)
        .bind(is_current)
        .fetch_one(source_db_pool)
        .await
        .unwrap();

        if i < state_chain.len() - 1 {
            prev_id = Some(id);
        }
    }

    table_oid
}

/// Creates multiple tables with single states.
async fn create_tables_with_states(
    source_db_pool: &PgPool,
    pipeline_id: i64,
    tables: &[(&str, &str, &str)],
) -> Vec<(Oid, String)> {
    let mut results = Vec::new();

    for (table_name, state, metadata) in tables {
        let table_oid = create_test_table(source_db_pool, table_name).await;

        sqlx::query(
            "INSERT INTO etl.replication_state (pipeline_id, table_id, state, metadata, prev, is_current) VALUES ($1, $2, $3::etl.table_state, $4::jsonb, NULL, true)"
        )
        .bind(pipeline_id)
        .bind(table_oid)
        .bind(state)
        .bind(metadata)
        .execute(source_db_pool)
        .await
        .unwrap();

        results.push((table_oid, format!("test.{table_name}")));
    }

    results
}

/// Tests rollback functionality and returns response if successful.
/// Asserts the expected status code and returns the response for successful calls.
async fn test_rollback(
    app: &TestApp,
    tenant_id: &str,
    pipeline_id: i64,
    table_oid: Oid,
    rollback_type: RollbackType,
    expected_status: StatusCode,
) -> Option<RollbackTableStateResponse> {
    let response = app
        .rollback_table_state(
            tenant_id,
            pipeline_id,
            &RollbackTableStateRequest {
                table_id: table_oid.0,
                rollback_type,
            },
        )
        .await;

    assert_eq!(response.status(), expected_status);

    if expected_status.is_success() {
        Some(response.json().await.unwrap())
    } else {
        None
    }
}

async fn create_test_table(source_db_pool: &PgPool, table_name: &str) -> Oid {
    sqlx::query("create schema if not exists test")
        .execute(source_db_pool)
        .await
        .unwrap();

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS test.{table_name} (id SERIAL PRIMARY KEY, name TEXT)"
    ))
    .execute(source_db_pool)
    .await
    .expect("Failed to create test table");

    sqlx::query_scalar::<_, Oid>(
        "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = $1 AND n.nspname = $2"
    )
    .bind(table_name)
    .bind("test")
    .fetch_one(source_db_pool)
    .await
    .expect("Failed to get table OID")
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
async fn pipeline_with_another_tenants_source_cannot_be_created() {
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
async fn pipeline_with_another_tenants_destination_cannot_be_created() {
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
async fn non_existing_pipeline_cannot_be_read() {
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
async fn pipeline_with_another_tenants_source_cannot_be_updated() {
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
async fn pipeline_with_another_tenants_destination_cannot_be_updated() {
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
async fn non_existing_pipeline_cannot_be_updated() {
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
async fn non_existing_pipeline_cannot_be_deleted() {
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
async fn deleting_a_pipeline_succeeds() {
    init_test_tracing();
    let (app, tenant_id, pipeline_id, _, source_db_config) = setup_pipeline_with_source_db().await;

    // The deletion should fail.
    let response = app.delete_pipeline(&tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::OK);

    // The pipeline should still be there.
    let response = app.read_pipeline(&tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    drop_pg_database(&source_db_config).await;
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
async fn duplicate_pipeline_with_same_source_and_destination_cannot_be_created() {
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
async fn pipeline_config_can_be_updated() {
    init_test_tracing();
    let (app, tenant_id, _source_id, _destination_id, pipeline_id) = setup_basic_pipeline().await;

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: partially_updated_optional_pipeline_config(ConfigUpdateType::Batch(BatchConfig {
            max_size: 10_000,
            max_fill_ms: 100,
        })),
    };
    let response = app
        .update_pipeline_config(&tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response: UpdatePipelineConfigResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    insta::assert_debug_snapshot!(response.config);

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: partially_updated_optional_pipeline_config(
            ConfigUpdateType::TableErrorRetryDelayMs(20000),
        ),
    };
    let response = app
        .update_pipeline_config(&tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response: UpdatePipelineConfigResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    insta::assert_debug_snapshot!(response.config);

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: partially_updated_optional_pipeline_config(ConfigUpdateType::MaxTableSyncWorkers(
            8,
        )),
    };
    let response = app
        .update_pipeline_config(&tenant_id, pipeline_id, &update_request)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response: UpdatePipelineConfigResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_config_fails_for_non_existing_pipeline() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;

    // Act
    let update_request = UpdatePipelineConfigRequest {
        config: updated_optional_pipeline_config(),
    };
    let response = app
        .update_pipeline_config(tenant_id, 42, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_config_fails_for_pipeline_from_another_tenant() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
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

    // Act - Try to update config using
    let update_request = UpdatePipelineConfigRequest {
        config: updated_optional_pipeline_config(),
    };
    let response = app
        .update_pipeline_config("wrong-tenant-id", pipeline_id, &update_request)
        .await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_pipeline_can_be_started() {
    init_test_tracing();
    let (app, tenant_id, _source_id, _destination_id, pipeline_id) = setup_basic_pipeline().await;

    // Act
    let response = app.start_pipeline(&tenant_id, pipeline_id).await;

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
    let (app, tenant_id, pipeline_id, source_db_pool, source_db_config) =
        setup_pipeline_with_source_db().await;

    // Create tables with different states
    let tables = create_tables_with_states(
        &source_db_pool,
        pipeline_id,
        &[
            ("test_table_users", "data_sync", r#"{"type": "data_sync"}"#),
            ("test_table_orders", "ready", r#"{"type": "ready"}"#),
        ],
    )
    .await;

    // Test the endpoint
    let response = app
        .get_pipeline_replication_status(&tenant_id, pipeline_id)
        .await;
    let response: GetPipelineReplicationStatusResponse = response.json().await.unwrap();

    assert_eq!(response.pipeline_id, pipeline_id);
    assert_eq!(response.table_statuses.len(), 2);

    // Verify table states
    for (table_oid, table_name) in &tables {
        let table_status = response
            .table_statuses
            .iter()
            .find(|s| s.table_name == *table_name)
            .expect("Table not found in response");

        assert_eq!(table_status.table_id, table_oid.0);

        match table_name.as_str() {
            "test.test_table_users" => assert!(matches!(
                table_status.state,
                SimpleTableReplicationState::CopyingTable
            )),
            "test.test_table_orders" => assert!(matches!(
                table_status.state,
                SimpleTableReplicationState::FollowingWal { .. }
            )),
            _ => panic!("Unexpected table name: {table_name}"),
        }
    }

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_table_state_succeeds_for_manual_retry_errors() {
    init_test_tracing();
    let (app, tenant_id, pipeline_id, source_db_pool, source_db_config) =
        setup_pipeline_with_source_db().await;

    let table_oid = create_table_with_state_chain(
        &source_db_pool,
        pipeline_id,
        "test_users",
        &[
            ("ready", r#"{"type": "ready"}"#),
            (
                "errored",
                r#"{"type": "errored", "reason": "connection failed", "retry_policy": {"type": "manual_retry"}}"#,
            ),
        ],
    )
    .await;

    let response = test_rollback(
        &app,
        &tenant_id,
        pipeline_id,
        table_oid,
        RollbackType::Individual,
        StatusCode::OK,
    )
    .await
    .unwrap();

    assert_eq!(response.pipeline_id, pipeline_id);
    assert_eq!(response.table_id, table_oid.0);
    assert!(matches!(
        response.new_state,
        SimpleTableReplicationState::FollowingWal { .. }
    ));

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_table_state_fails_for_non_manual_retry_errors() {
    init_test_tracing();
    let (app, tenant_id, pipeline_id, source_db_pool, source_db_config) =
        setup_pipeline_with_source_db().await;

    let table_oid = create_table_with_state_chain(
        &source_db_pool,
        pipeline_id,
        "test_users",
        &[(
            "errored",
            r#"{"type": "errored", "reason": "connection failed", "retry_policy": {"type": "no_retry"}}"#,
        )],
    )
    .await;

    test_rollback(
        &app,
        &tenant_id,
        pipeline_id,
        table_oid,
        RollbackType::Individual,
        StatusCode::BAD_REQUEST,
    )
    .await;

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_table_state_with_full_reset_succeeds() {
    init_test_tracing();
    let (app, tenant_id, pipeline_id, source_db_pool, source_db_config) =
        setup_pipeline_with_source_db().await;

    let table_oid = create_table_with_state_chain(
        &source_db_pool,
        pipeline_id,
        "test_users",
        &[
            ("ready", r#"{"type": "ready"}"#),
            (
                "errored",
                r#"{"type": "errored", "reason": "connection failed", "retry_policy": {"type": "manual_retry"}}"#,
            ),
        ],
    )
    .await;

    let response = test_rollback(
        &app,
        &tenant_id,
        pipeline_id,
        table_oid,
        RollbackType::Full,
        StatusCode::OK,
    )
    .await
    .unwrap();

    assert_eq!(response.pipeline_id, pipeline_id);
    assert_eq!(response.table_id, table_oid.0);
    assert!(matches!(
        response.new_state,
        SimpleTableReplicationState::Queued
    ));

    // Verify only one row exists (the reset init state)
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM etl.replication_state WHERE pipeline_id = $1 AND table_id = $2",
    )
    .bind(pipeline_id)
    .bind(table_oid)
    .fetch_one(&source_db_pool)
    .await
    .unwrap();
    assert_eq!(count, 1);

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_pipeline_removes_replication_state_from_source_database() {
    init_test_tracing();
    let (app, tenant_id, pipeline_id, source_db_pool, source_db_config) =
        setup_pipeline_with_source_db().await;

    create_tables_with_states(
        &source_db_pool,
        pipeline_id,
        &[
            ("test_users", "ready", r#"{"type": "ready"}"#),
            ("test_orders", "data_sync", r#"{"type": "data_sync"}"#),
        ],
    )
    .await;

    let count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM etl.replication_state WHERE pipeline_id = $1")
            .bind(pipeline_id)
            .fetch_one(&source_db_pool)
            .await
            .unwrap();
    assert_eq!(count_before, 2);

    let response = app.delete_pipeline(&tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::OK);

    let count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM etl.replication_state WHERE pipeline_id = $1")
            .bind(pipeline_id)
            .fetch_one(&source_db_pool)
            .await
            .unwrap();
    assert_eq!(count_after, 0);

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_pipeline_removes_table_schemas_from_source_database() {
    init_test_tracing();

    let (app, tenant_id, pipeline_id, source_db_pool, source_db_config) =
        setup_pipeline_with_source_db().await;

    let table1_oid = create_test_table(&source_db_pool, "test_users").await;
    let table2_oid = create_test_table(&source_db_pool, "test_orders").await;

    // Insert table schemas using production schema
    let table_schema_id_1 = sqlx::query_scalar::<_, i64>(
        "INSERT INTO etl.table_schemas (pipeline_id, table_id, schema_name, table_name) VALUES ($1, $2, 'public', 'test_users') RETURNING id"
    ).bind(pipeline_id).bind(table1_oid).fetch_one(&source_db_pool).await.unwrap();

    let table_schema_id_2 = sqlx::query_scalar::<_, i64>(
        "INSERT INTO etl.table_schemas (pipeline_id, table_id, schema_name, table_name) VALUES ($1, $2, 'public', 'test_orders') RETURNING id"
    ).bind(pipeline_id).bind(table2_oid).fetch_one(&source_db_pool).await.unwrap();

    // Insert multiple columns for each table to test CASCADE behavior
    sqlx::query("INSERT INTO etl.table_columns (table_schema_id, column_name, column_type, type_modifier, nullable, primary_key, column_order) VALUES ($1, 'id', 'INT4', -1, false, true, 0), ($1, 'name', 'TEXT', -1, true, false, 1)")
        .bind(table_schema_id_1).execute(&source_db_pool).await.unwrap();

    sqlx::query("INSERT INTO etl.table_columns (table_schema_id, column_name, column_type, type_modifier, nullable, primary_key, column_order) VALUES ($1, 'order_id', 'INT8', -1, false, true, 0), ($1, 'amount', 'NUMERIC', -1, false, false, 1)")
        .bind(table_schema_id_2).execute(&source_db_pool).await.unwrap();

    // Verify data exists before deletion
    let schema_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM etl.table_schemas WHERE pipeline_id = $1")
            .bind(pipeline_id)
            .fetch_one(&source_db_pool)
            .await
            .unwrap();
    let column_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM etl.table_columns WHERE table_schema_id IN ($1, $2)",
    )
    .bind(table_schema_id_1)
    .bind(table_schema_id_2)
    .fetch_one(&source_db_pool)
    .await
    .unwrap();
    assert_eq!(schema_count, 2);
    assert_eq!(column_count, 4);

    let response = app.delete_pipeline(&tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Verify both schemas and their CASCADE-deleted columns are gone
    let schema_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM etl.table_schemas WHERE pipeline_id = $1")
            .bind(pipeline_id)
            .fetch_one(&source_db_pool)
            .await
            .unwrap();
    let column_count_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM etl.table_columns WHERE table_schema_id IN ($1, $2)",
    )
    .bind(table_schema_id_1)
    .bind(table_schema_id_2)
    .fetch_one(&source_db_pool)
    .await
    .unwrap();
    assert_eq!(schema_count_after, 0);
    assert_eq!(column_count_after, 0); // CASCADE delete should remove these

    drop_pg_database(&source_db_config).await;
}
