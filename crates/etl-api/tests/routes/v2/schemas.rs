use etl_api::routes::v2::schemas::ReadSchemasResponse;
use etl_postgres::sqlx::test_utils::drop_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use sqlx::Executor;

use crate::support::{
    database::create_test_source_database, mocks::tenants::create_tenant, test_app::spawn_test_app,
};

#[tokio::test(flavor = "multi_thread")]
async fn source_schemas_v2_lists_empty_and_populated_schemas() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool.execute("create schema inspection_empty").await.unwrap();
    source_pool.execute("create schema inspection_selected").await.unwrap();
    source_pool
        .execute("create table inspection_selected.orders (id bigint primary key)")
        .await
        .unwrap();

    let response = app.read_source_schemas_v2(tenant_id, source_id).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: ReadSchemasResponse = response.json().await.unwrap();
    assert!(response.schemas.is_sorted_by(|left, right| left.name <= right.name));
    assert!(response.schemas.iter().any(|schema| schema.name == "inspection_empty"));
    assert!(response.schemas.iter().any(|schema| schema.name == "inspection_selected"));

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}
