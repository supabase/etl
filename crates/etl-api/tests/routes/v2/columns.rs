use etl_api::routes::v2::columns::ReadColumnsResponse;
use etl_postgres::sqlx::test_utils::drop_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use sqlx::Executor;

use crate::support::{
    database::create_test_source_database, mocks::tenants::create_tenant, test_app::spawn_test_app,
};

#[tokio::test(flavor = "multi_thread")]
async fn source_columns_v2_returns_column_metadata() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool.execute("create schema inspection_selected").await.unwrap();
    source_pool
        .execute(
            r#"
            create table inspection_selected.orders (
                id bigint primary key,
                amount numeric(12, 3) not null,
                note text
            )
            "#,
        )
        .await
        .unwrap();
    let orders_id: i64 =
        sqlx::query_scalar("select 'inspection_selected.orders'::regclass::oid::bigint")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    let orders_id = u32::try_from(orders_id).unwrap();
    source_pool.execute("create table inspection_selected.empty_table ()").await.unwrap();
    let empty_table_id: i64 =
        sqlx::query_scalar("select 'inspection_selected.empty_table'::regclass::oid::bigint")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    let empty_table_id = u32::try_from(empty_table_id).unwrap();

    let response = app.read_source_columns_v2(tenant_id, source_id, orders_id).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: ReadColumnsResponse = response.json().await.unwrap();
    assert_eq!(
        response
            .columns
            .iter()
            .map(|column| {
                (column.name.as_str(), column.r#type.as_str(), column.nullable, column.primary_key)
            })
            .collect::<Vec<_>>(),
        vec![
            ("id", "bigint", false, true),
            ("amount", "numeric(12,3)", false, false),
            ("note", "text", true, false),
        ]
    );

    let response = app.read_source_columns_v2(tenant_id, source_id, empty_table_id).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: ReadColumnsResponse = response.json().await.unwrap();
    assert!(response.columns.is_empty());

    let response = app.read_source_columns_v2(tenant_id, source_id, 0).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}
