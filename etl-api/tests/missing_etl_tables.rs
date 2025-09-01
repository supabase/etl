use etl_api::routes::pipelines::{
    CreatePipelineRequest, CreatePipelineResponse, RollbackTableStateRequest, RollbackType,
};
use etl_config::shared::PgConnectionConfig;
use etl_postgres::sqlx::test_utils::drop_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use sqlx::PgPool;

use crate::support::database::create_test_source_database;
use crate::support::mocks::create_default_image;
use crate::support::mocks::destinations::create_destination;
use crate::support::mocks::tenants::create_tenant;
use crate::support::test_app::{TestApp, spawn_test_app};

mod support;

async fn create_pipeline_with_unmigrated_source_db(
    app: &TestApp,
    tenant_id: &str,
) -> (i64, PgPool, PgConnectionConfig) {
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(app, tenant_id).await;
    let destination_id = create_destination(app, tenant_id).await;
    create_default_image(app).await;

    let req = CreatePipelineRequest {
        source_id,
        destination_id,
        config: crate::support::mocks::pipelines::new_pipeline_config(),
    };

    let response = app.create_pipeline(tenant_id, &req).await;
    let response: CreatePipelineResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    (response.id, source_pool, source_db_config)
}

#[tokio::test(flavor = "multi_thread")]
async fn replication_status_fails_when_etl_tables_missing() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let (pipeline_id, _source_pool, source_db_config) =
        create_pipeline_with_unmigrated_source_db(&app, &tenant_id).await;

    let response = app
        .get_pipeline_replication_status(&tenant_id, pipeline_id)
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("The ETL table state has not been initialized first")
    );

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rollback_fails_when_etl_tables_missing() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let (pipeline_id, _source_pool, source_db_config) =
        create_pipeline_with_unmigrated_source_db(&app, &tenant_id).await;

    let req = RollbackTableStateRequest {
        table_id: 1,
        rollback_type: RollbackType::Individual,
    };
    let response = app
        .rollback_table_state(&tenant_id, pipeline_id, &req)
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("The ETL table state has not been initialized first")
    );

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_pipeline_succeeds_when_etl_tables_missing() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let (pipeline_id, _source_pool, source_db_config) =
        create_pipeline_with_unmigrated_source_db(&app, &tenant_id).await;

    let response = app.delete_pipeline(&tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = app.read_pipeline(&tenant_id, pipeline_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    drop_pg_database(&source_db_config).await;
}
