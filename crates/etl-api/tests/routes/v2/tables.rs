use etl_api::{data::v2::tables::SourceTableKind, routes::v2::tables::ReadTablesResponse};
use etl_postgres::sqlx::test_utils::drop_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use sqlx::Executor;

use crate::support::{
    database::create_test_source_database, mocks::tenants::create_tenant, test_app::spawn_test_app,
};

#[tokio::test(flavor = "multi_thread")]
async fn source_tables_v2_supports_schema_filtering() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool.execute("create schema inspection_selected").await.unwrap();
    source_pool
        .execute("create table inspection_selected.orders (id bigint primary key)")
        .await
        .unwrap();
    source_pool.execute("create table public.customers (id bigint primary key)").await.unwrap();
    source_pool
        .execute(
            "create table public.events (occurred_on date not null, id bigint not null) partition \
             by range (occurred_on)",
        )
        .await
        .unwrap();
    source_pool
        .execute(
            "create table public.events_2025 partition of public.events for values from \
             ('2025-01-01') to ('2026-01-01') partition by range (occurred_on)",
        )
        .await
        .unwrap();
    source_pool
        .execute(
            "create table public.events_2025_h1 partition of public.events_2025 for values from \
             ('2025-01-01') to ('2025-07-01')",
        )
        .await
        .unwrap();
    source_pool
        .execute("create unlogged table public.unlogged_customers (id bigint primary key)")
        .await
        .unwrap();
    let orders_id: i64 =
        sqlx::query_scalar("select 'inspection_selected.orders'::regclass::oid::bigint")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    let orders_id = u32::try_from(orders_id).unwrap();

    let response =
        app.read_source_tables_v2(tenant_id, source_id, Some("inspection_selected")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: ReadTablesResponse = response.json().await.unwrap();
    assert_eq!(response.tables.len(), 1);
    assert_eq!(response.tables[0].id, orders_id);
    assert_eq!(response.tables[0].schema, "inspection_selected");
    assert_eq!(response.tables[0].name, "orders");

    let response = app.read_source_tables_v2(tenant_id, source_id, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: ReadTablesResponse = response.json().await.unwrap();
    assert!(response.tables.iter().any(|table| table.id == orders_id));
    assert!(
        response
            .tables
            .iter()
            .any(|table| { table.schema == "public" && table.name == "customers" })
    );
    assert!(!response.tables.iter().any(|table| table.name == "unlogged_customers"));
    let events = response.tables.iter().find(|table| table.name == "events").unwrap();
    let events_2025 = response.tables.iter().find(|table| table.name == "events_2025").unwrap();
    let events_2025_h1 =
        response.tables.iter().find(|table| table.name == "events_2025_h1").unwrap();
    assert_eq!(events.kind, SourceTableKind::PartitionedTable);
    assert_eq!(events.partition_parent_id, None);
    assert_eq!(events_2025.kind, SourceTableKind::PartitionedTable);
    assert_eq!(events_2025.partition_parent_id, Some(events.id));
    assert_eq!(events_2025_h1.kind, SourceTableKind::Table);
    assert_eq!(events_2025_h1.partition_parent_id, Some(events_2025.id));

    let response = app
        .read_source_tables_v2(tenant_id, source_id, Some("inspection_selected' or true --"))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: ReadTablesResponse = response.json().await.unwrap();
    assert!(response.tables.is_empty());

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}
