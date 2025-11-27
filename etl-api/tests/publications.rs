use etl_api::routes::pipelines::{CreatePipelineRequest, CreatePipelineResponse};
use etl_api::routes::sources::publications::ListPipelinesForPublicationResponse;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use sqlx::PgPool;

use crate::support::database::{
    create_test_source_database, run_etl_migrations_on_source_database,
};
use crate::support::mocks::create_default_image;
use crate::support::mocks::destinations::create_destination;
use crate::support::mocks::pipelines::new_pipeline_config;
use crate::support::mocks::tenants::create_tenant;
use crate::support::test_app::spawn_test_app;

mod support;

async fn create_test_table(pool: &PgPool, table_name: &str) {
    sqlx::query(&format!(
        "create table if not exists {} (id serial primary key, data text)",
        table_name
    ))
    .execute(pool)
    .await
    .expect("failed to create test table");
}

async fn create_publication_on_db(pool: &PgPool, publication_name: &str, table_name: &str) {
    sqlx::query(&format!(
        "create publication {} for table {}",
        publication_name, table_name
    ))
    .execute(pool)
    .await
    .expect("failed to create publication");
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_not_referenced_by_pipeline_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create a test table and publication on the source database
    create_test_table(&source_pool, "test_table").await;
    create_publication_on_db(&source_pool, "test_publication", "test_table").await;

    // Act - Delete the publication (not referenced by any pipeline)
    let response = app
        .delete_publication(tenant_id, source_id, "test_publication")
        .await;

    // Assert - Should succeed
    assert!(
        response.status().is_success(),
        "Expected success, got: {}",
        response.status()
    );

    // Verify publication was actually deleted from the database
    let publication_exists: bool =
        sqlx::query_scalar("select exists(select 1 from pg_publication where pubname = $1)")
            .bind("test_publication")
            .fetch_one(&source_pool)
            .await
            .expect("failed to check publication existence");

    assert!(!publication_exists, "Publication should have been deleted");
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_referenced_by_pipeline_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create a test table and publication on the source database
    create_test_table(&source_pool, "test_table").await;
    create_publication_on_db(&source_pool, "my_publication", "test_table").await;

    // Create destination
    let destination_id = create_destination(&app, tenant_id).await;

    // Create a pipeline that uses this publication
    let mut pipeline_config = new_pipeline_config();
    pipeline_config.publication_name = "my_publication".to_string();

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: pipeline_config,
    };
    let pipeline_response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(
        pipeline_response.status().is_success(),
        "Failed to create pipeline"
    );
    let pipeline_response: CreatePipelineResponse = pipeline_response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = pipeline_response.id;

    // Act - Try to delete the publication
    let response = app
        .delete_publication(tenant_id, source_id, "my_publication")
        .await;

    // Assert - Should fail with 409 Conflict
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "Expected 409 Conflict when deleting publication in use"
    );

    let error_body = response.text().await.expect("failed to read response body");
    assert!(
        error_body.contains(&pipeline_id.to_string()),
        "Error message should contain pipeline ID: {}",
        error_body
    );
    assert!(
        error_body.contains("my_publication"),
        "Error message should contain publication name"
    );

    // Verify publication still exists
    let publication_exists: bool =
        sqlx::query_scalar("select exists(select 1 from pg_publication where pubname = $1)")
            .bind("my_publication")
            .fetch_one(&source_pool)
            .await
            .expect("failed to check publication existence");

    assert!(publication_exists, "Publication should still exist");
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_referenced_by_multiple_pipelines_cannot_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create a test table and publication on the source database
    create_test_table(&source_pool, "test_table").await;
    create_publication_on_db(&source_pool, "shared_publication", "test_table").await;

    // Create two destinations
    let destination_id_1 = create_destination(&app, tenant_id).await;
    let destination_id_2 = create_destination(&app, tenant_id).await;

    // Create two pipelines that use the same publication
    let mut pipeline_config = new_pipeline_config();
    pipeline_config.publication_name = "shared_publication".to_string();

    let pipeline1 = CreatePipelineRequest {
        source_id,
        destination_id: destination_id_1,
        config: pipeline_config.clone(),
    };
    let response1 = app.create_pipeline(tenant_id, &pipeline1).await;
    assert!(
        response1.status().is_success(),
        "Failed to create pipeline 1"
    );
    let response1: CreatePipelineResponse = response1.json().await.expect("failed to deserialize");
    let pipeline_id_1 = response1.id;

    let pipeline2 = CreatePipelineRequest {
        source_id,
        destination_id: destination_id_2,
        config: pipeline_config,
    };
    let response2 = app.create_pipeline(tenant_id, &pipeline2).await;
    assert!(
        response2.status().is_success(),
        "Failed to create pipeline 2"
    );
    let response2: CreatePipelineResponse = response2.json().await.expect("failed to deserialize");
    let pipeline_id_2 = response2.id;

    // Act - Try to delete the publication
    let response = app
        .delete_publication(tenant_id, source_id, "shared_publication")
        .await;

    // Assert - Should fail with 409 Conflict
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "Expected 409 Conflict when deleting publication used by multiple pipelines"
    );

    let error_body = response.text().await.expect("failed to read response body");
    assert!(
        error_body.contains(&pipeline_id_1.to_string()),
        "Error message should contain first pipeline ID"
    );
    assert!(
        error_body.contains(&pipeline_id_2.to_string()),
        "Error message should contain second pipeline ID"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cannot_create_pipeline_with_non_existent_publication() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;
    let (_source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create destination
    let destination_id = create_destination(&app, tenant_id).await;

    // Act - Try to create pipeline with non-existent publication
    let mut pipeline_config = new_pipeline_config();
    pipeline_config.publication_name = "non_existent_publication".to_string();

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: pipeline_config,
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;

    // Assert - Should fail with 400 Bad Request
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request when creating pipeline with non-existent publication"
    );

    let error_body = response.text().await.expect("failed to read response body");
    assert!(
        error_body.contains("non_existent_publication"),
        "Error message should mention the publication name"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn can_create_pipeline_with_existing_publication() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create a test table and publication on the source database
    create_test_table(&source_pool, "test_table").await;
    create_publication_on_db(&source_pool, "valid_publication", "test_table").await;

    // Create destination
    let destination_id = create_destination(&app, tenant_id).await;

    // Act - Create pipeline with existing publication
    let mut pipeline_config = new_pipeline_config();
    pipeline_config.publication_name = "valid_publication".to_string();

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: pipeline_config,
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;

    // Assert - Should succeed
    assert!(
        response.status().is_success(),
        "Expected success when creating pipeline with valid publication, got: {}",
        response.status()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cannot_update_pipeline_to_non_existent_publication() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create a test table and publication on the source database
    create_test_table(&source_pool, "test_table").await;
    create_publication_on_db(&source_pool, "initial_publication", "test_table").await;

    // Create destination
    let destination_id = create_destination(&app, tenant_id).await;

    // Create pipeline with initial publication
    let mut pipeline_config = new_pipeline_config();
    pipeline_config.publication_name = "initial_publication".to_string();

    let pipeline = CreatePipelineRequest {
        source_id,
        destination_id,
        config: pipeline_config.clone(),
    };
    let create_response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(create_response.status().is_success());
    let create_response: CreatePipelineResponse = create_response
        .json()
        .await
        .expect("failed to deserialize response");
    let pipeline_id = create_response.id;

    // Act - Try to update pipeline to non-existent publication
    pipeline_config.publication_name = "non_existent_publication".to_string();

    let update_request = etl_api::routes::pipelines::UpdatePipelineRequest {
        source_id,
        destination_id,
        config: pipeline_config,
    };
    let response = app
        .update_pipeline(tenant_id, pipeline_id, &update_request)
        .await;

    // Assert - Should fail with 400 Bad Request
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request when updating pipeline to non-existent publication"
    );

    let error_body = response.text().await.expect("failed to read response body");
    assert!(
        error_body.contains("non_existent_publication"),
        "Error message should mention the publication name"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_pipelines_for_publication_returns_correct_pipelines() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create test tables and publications
    create_test_table(&source_pool, "test_table_a").await;
    create_test_table(&source_pool, "test_table_b").await;
    create_publication_on_db(&source_pool, "publication_a", "test_table_a").await;
    create_publication_on_db(&source_pool, "publication_b", "test_table_b").await;

    // Create destinations
    let destination_id_1 = create_destination(&app, tenant_id).await;
    let destination_id_2 = create_destination(&app, tenant_id).await;
    let destination_id_3 = create_destination(&app, tenant_id).await;

    // Create pipelines: two with publication_a, one with publication_b
    let mut pipeline_config_a = new_pipeline_config();
    pipeline_config_a.publication_name = "publication_a".to_string();

    let mut pipeline_config_b = new_pipeline_config();
    pipeline_config_b.publication_name = "publication_b".to_string();

    // Pipeline 1 with publication_a
    let pipeline1 = CreatePipelineRequest {
        source_id,
        destination_id: destination_id_1,
        config: pipeline_config_a.clone(),
    };
    let response1 = app.create_pipeline(tenant_id, &pipeline1).await;
    assert!(response1.status().is_success());
    let response1: CreatePipelineResponse = response1.json().await.expect("failed to deserialize");
    let pipeline_id_1 = response1.id;

    // Pipeline 2 with publication_a
    let pipeline2 = CreatePipelineRequest {
        source_id,
        destination_id: destination_id_2,
        config: pipeline_config_a,
    };
    let response2 = app.create_pipeline(tenant_id, &pipeline2).await;
    assert!(response2.status().is_success());
    let response2: CreatePipelineResponse = response2.json().await.expect("failed to deserialize");
    let pipeline_id_2 = response2.id;

    // Pipeline 3 with publication_b
    let pipeline3 = CreatePipelineRequest {
        source_id,
        destination_id: destination_id_3,
        config: pipeline_config_b,
    };
    let response3 = app.create_pipeline(tenant_id, &pipeline3).await;
    assert!(response3.status().is_success());

    // Act - List pipelines for publication_a
    let response = app
        .list_pipelines_for_publication(tenant_id, source_id, "publication_a")
        .await;

    // Assert - Should return only the two pipelines using publication_a
    assert!(response.status().is_success());
    let response_body: ListPipelinesForPublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    assert_eq!(
        response_body.pipelines.len(),
        2,
        "Should have exactly 2 pipelines for publication_a"
    );

    let pipeline_ids: Vec<i64> = response_body.pipelines.iter().map(|p| p.id).collect();
    assert!(
        pipeline_ids.contains(&pipeline_id_1),
        "Should include pipeline 1"
    );
    assert!(
        pipeline_ids.contains(&pipeline_id_2),
        "Should include pipeline 2"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn list_pipelines_for_unused_publication_returns_empty() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    run_etl_migrations_on_source_database(&source_db_config).await;

    // Create a test table and publication but no pipelines using it
    create_test_table(&source_pool, "test_table").await;
    create_publication_on_db(&source_pool, "unused_publication", "test_table").await;

    // Act - List pipelines for unused publication
    let response = app
        .list_pipelines_for_publication(tenant_id, source_id, "unused_publication")
        .await;

    // Assert - Should return empty list
    assert!(response.status().is_success());
    let response_body: ListPipelinesForPublicationResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    assert_eq!(
        response_body.pipelines.len(),
        0,
        "Should have no pipelines for unused publication"
    );
}
