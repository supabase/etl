use etl_api::{
    data::v2::{
        publications::{
            PublicationConfig, PublicationDetails, PublicationGeneratedColumns,
            PublicationOperation, PublicationTableConfig, PublicationTableSelection,
        },
        tables::SourceTableKind,
    },
    routes::{ErrorMessage, v2::publications::ReadPublicationsResponse},
};
use etl_postgres::{
    sqlx::test_utils::drop_pg_database,
    version::{POSTGRES_15, POSTGRES_18},
};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use serde_json::json;
use sqlx::Executor;

use crate::support::{
    database::create_test_source_database, mocks::tenants::create_tenant, test_app::spawn_test_app,
};

#[tokio::test(flavor = "multi_thread")]
async fn publication_v2_create_is_readable_and_rejects_an_open_ended_replacement() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool
        .execute(
            "create table public.publication_orders (id bigint primary key, payload text not \
             null, \"payload$tag$\" boolean not null default true)",
        )
        .await
        .unwrap();
    source_pool
        .execute(
            "create table public.publication_hidden (id bigint primary key, \"visible$tag$\" \
             boolean not null default true)",
        )
        .await
        .unwrap();
    let table_id: i64 =
        sqlx::query_scalar("select 'public.publication_orders'::regclass::oid::bigint")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    let table_id = u32::try_from(table_id).unwrap();
    let server_version_num: i32 =
        sqlx::query_scalar("select current_setting('server_version_num')::int")
            .fetch_one(&source_pool)
            .await
            .unwrap();

    let mut table = json!({
        "id": table_id,
        "schema": "public",
        "name": "publication_orders"
    });
    if server_version_num >= POSTGRES_15 {
        table["columns"] = json!(["id", "payload"]);
        table["row_filter"] = json!("id > 0 and payload <> '); --'");
    }
    let config = json!({
        "type": "tables",
        "tables": [table],
        "operations": ["insert", "update", "delete"]
    });

    let response = app
        .create_source_publication_v2(tenant_id, source_id, "publication_orders_v2", &config)
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: PublicationDetails = response.json().await.unwrap();
    assert_eq!(created.name, "publication_orders_v2");
    assert!(!created.config.publish_via_partition_root);
    assert_eq!(
        created.config.operations,
        vec![
            PublicationOperation::Insert,
            PublicationOperation::Update,
            PublicationOperation::Delete,
        ]
    );
    let PublicationTableSelection::Tables { tables } = &created.config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].id, table_id);
    if server_version_num >= POSTGRES_15 {
        assert_eq!(
            tables[0].columns.as_ref().unwrap().iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["id", "payload"]
        );
        let row_filter = tables[0].row_filter.as_deref().unwrap();
        assert!(row_filter.contains("payload"));
        assert!(row_filter.contains("); --"));
    }

    let publication_oid: i64 =
        sqlx::query_scalar("select oid::bigint from pg_catalog.pg_publication where pubname = $1")
            .bind("publication_orders_v2")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    let publish_via_partition_root: bool =
        sqlx::query_scalar("select pubviaroot from pg_catalog.pg_publication where pubname = $1")
            .bind("publication_orders_v2")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    assert!(!publish_via_partition_root);

    let replacement = json!({
        "type": "all_tables",
        "operations": ["insert"],
        "publish_via_partition_root": true
    });
    let response = app
        .create_source_publication_v2(tenant_id, source_id, "publication_orders_v2", &replacement)
        .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = app
        .api_client
        .patch(format!("{}/v2/sources/{source_id}/publications/publication_orders_v2", app.address))
        .bearer_auth(&app.api_key)
        .header("tenant_id", tenant_id)
        .json(&replacement)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

    let oid_after_conflict: i64 =
        sqlx::query_scalar("select oid::bigint from pg_catalog.pg_publication where pubname = $1")
            .bind("publication_orders_v2")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    assert_eq!(oid_after_conflict, publication_oid);

    for (publication_name, row_filter) in [
        ("extra_table_filter_v2", "true), public.publication_hidden where (true"),
        ("option_override_filter_v2", "true) with (publish = 'truncate') --"),
        (
            "attached_dollar_quote_filter_v2",
            "payload$tag$), public.publication_hidden where (visible$tag$",
        ),
    ] {
        let unsafe_filter = json!({
            "type": "tables",
            "tables": [{
                "id": table_id,
                "row_filter": row_filter
            }],
            "operations": ["insert"]
        });
        let response = app
            .create_source_publication_v2(tenant_id, source_id, publication_name, &unsafe_filter)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorMessage = response.json().await.unwrap();
        assert_eq!(
            error.message,
            format!("Row filter for table with id {table_id} is not a single SQL expression")
        );

        let unsafe_publication_exists: bool = sqlx::query_scalar(
            "select exists(select 1 from pg_catalog.pg_publication where pubname = $1)",
        )
        .bind(publication_name)
        .fetch_one(&source_pool)
        .await
        .unwrap();
        assert!(!unsafe_publication_exists);
    }

    let postgres_invalid_filter = json!({
        "type": "tables",
        "tables": [{
            "id": table_id,
            "schema": "public",
            "name": "publication_orders",
            "row_filter": "missing_column > 0"
        }],
        "operations": ["insert"]
    });
    let response = app
        .create_source_publication_v2(
            tenant_id,
            source_id,
            "postgres_invalid_filter_v2",
            &postgres_invalid_filter,
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let invalid_publication_exists: bool = sqlx::query_scalar(
        "select exists(select 1 from pg_catalog.pg_publication where pubname = $1)",
    )
    .bind("postgres_invalid_filter_v2")
    .fetch_one(&source_pool)
    .await
    .unwrap();
    assert!(!invalid_publication_exists);

    if server_version_num >= POSTGRES_18 {
        let generated_columns = json!({
            "type": "tables",
            "tables": [{
                "id": table_id,
                "schema": "public",
                "name": "publication_orders"
            }],
            "operations": ["insert"],
            "publish_generated_columns": "stored"
        });
        let response = app
            .create_source_publication_v2(
                tenant_id,
                source_id,
                "generated_columns_v2",
                &generated_columns,
            )
            .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let response: PublicationDetails = response.json().await.unwrap();
        assert_eq!(
            response.config.publish_generated_columns,
            Some(PublicationGeneratedColumns::Stored)
        );
        let response =
            app.delete_source_publication_v2(tenant_id, source_id, "generated_columns_v2").await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    let response =
        app.read_source_publication_v2(tenant_id, source_id, "publication_orders_v2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let read: PublicationDetails = response.json().await.unwrap();
    assert_eq!(read.config.table_selection, created.config.table_selection);
    assert_eq!(read.config.operations, created.config.operations);
    assert!(!read.config.publish_via_partition_root);

    let response = app.read_source_publications_v2(tenant_id, source_id).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: ReadPublicationsResponse = response.json().await.unwrap();
    assert_eq!(response.publications.len(), 1);
    assert_eq!(response.publications[0].name, "publication_orders_v2");

    let response =
        app.delete_source_publication_v2(tenant_id, source_id, "publication_orders_v2").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response =
        app.delete_source_publication_v2(tenant_id, source_id, "publication_orders_v2").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response =
        app.read_source_publication_v2(tenant_id, source_id, "publication_orders_v2").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_v2_resolves_table_names_from_ids_after_renames() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool
        .execute("create table public.id_only_before_rename (id bigint primary key)")
        .await
        .unwrap();
    let table_id: i64 =
        sqlx::query_scalar("select 'public.id_only_before_rename'::regclass::oid::bigint")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    let table_id = u32::try_from(table_id).unwrap();
    let config = json!({
        "type": "tables",
        "tables": [{ "id": table_id }],
        "operations": ["insert"]
    });

    let response =
        app.create_source_publication_v2(tenant_id, source_id, "id_only_v2", &config).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: PublicationDetails = response.json().await.unwrap();
    let PublicationTableSelection::Tables { tables } = &created.config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    assert_eq!(tables[0].id, table_id);
    assert_eq!(tables[0].schema, "public");
    assert_eq!(tables[0].name, "id_only_before_rename");

    source_pool
        .execute("alter table public.id_only_before_rename rename to id_only_after_rename")
        .await
        .unwrap();

    // The response still carries the old display name, but the table OID is
    // the request identity and resolves to the renamed table.
    let response =
        app.create_source_publication_v2(tenant_id, source_id, "id_only_v2", &created.config).await;
    assert_eq!(response.status(), StatusCode::OK);
    let updated: PublicationDetails = response.json().await.unwrap();
    let PublicationTableSelection::Tables { tables } = updated.config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    assert_eq!(tables[0].id, table_id);
    assert_eq!(tables[0].schema, "public");
    assert_eq!(tables[0].name, "id_only_after_rename");

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_v2_ignores_client_supplied_schema_and_name() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool.execute("create table public.orders (id bigint primary key)").await.unwrap();
    let table_id: i64 = sqlx::query_scalar("select 'public.orders'::regclass::oid::bigint")
        .fetch_one(&source_pool)
        .await
        .unwrap();
    let table_id = u32::try_from(table_id).unwrap();

    // A client supplying `schema`/`name` alongside `id` (e.g. by echoing back
    // a GET response) must not influence which table is targeted, or leak
    // its bogus values into the stored/returned configuration: only `id` is
    // the request identity.
    let config = json!({
        "type": "tables",
        "tables": [{ "id": table_id, "schema": "not_a_real_schema", "name": "not_a_real_name" }],
        "operations": ["insert"]
    });

    let response = app
        .create_source_publication_v2(tenant_id, source_id, "schema_name_ignored_v2", &config)
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: PublicationDetails = response.json().await.unwrap();
    let PublicationTableSelection::Tables { tables } = &created.config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    assert_eq!(tables[0].id, table_id);
    assert_eq!(tables[0].schema, "public");
    assert_eq!(tables[0].name, "orders");

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_v2_put_replaces_an_explicit_publication_in_place() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool
        .execute("create table public.publication_a (id bigint primary key, payload text)")
        .await
        .unwrap();
    source_pool
        .execute("create table public.publication_b (id bigint primary key, payload text)")
        .await
        .unwrap();
    source_pool
        .execute("create table public.publication_c (id bigint primary key, payload text)")
        .await
        .unwrap();
    let table_rows = sqlx::query_as::<_, (i64, String)>(
        r#"
        select c.oid::bigint, c.relname
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
            and c.relname = any($1::text[])
        order by c.relname;
        "#,
    )
    .bind(["publication_a", "publication_b", "publication_c"])
    .fetch_all(&source_pool)
    .await
    .unwrap();
    let table = |name: &str| {
        let (id, _) = table_rows.iter().find(|(_, table_name)| table_name == name).unwrap();
        PublicationTableConfig {
            id: u32::try_from(*id).unwrap(),
            schema: "public".to_owned(),
            name: name.to_owned(),
            columns: None,
            row_filter: None,
        }
    };
    let initial = PublicationConfig {
        table_selection: PublicationTableSelection::Tables {
            tables: vec![table("publication_a"), table("publication_b"), table("publication_c")],
        },
        operations: vec![PublicationOperation::Insert],
        publish_via_partition_root: false,
        publish_generated_columns: None,
    };
    let response =
        app.create_source_publication_v2(tenant_id, source_id, "replace_tables_v2", &initial).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let publication_oid: i64 =
        sqlx::query_scalar("select oid::bigint from pg_catalog.pg_publication where pubname = $1")
            .bind("replace_tables_v2")
            .fetch_one(&source_pool)
            .await
            .unwrap();

    let server_version_num: i32 =
        sqlx::query_scalar("select current_setting('server_version_num')::int")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    let mut publication_c = table("publication_c");
    if server_version_num >= POSTGRES_15 {
        publication_c.columns = Some(vec!["id".to_owned()]);
        publication_c.row_filter = Some("id > 0".to_owned());
    }
    let replacement = PublicationConfig {
        table_selection: PublicationTableSelection::Tables {
            tables: vec![table("publication_b"), publication_c],
        },
        operations: vec![PublicationOperation::Insert, PublicationOperation::Delete],
        publish_via_partition_root: false,
        publish_generated_columns: None,
    };
    let response = app
        .create_source_publication_v2(tenant_id, source_id, "replace_tables_v2", &replacement)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let updated: PublicationDetails = response.json().await.unwrap();
    assert_eq!(updated.config.operations, replacement.operations);
    assert_eq!(updated.config.publish_via_partition_root, replacement.publish_via_partition_root);
    let PublicationTableSelection::Tables { tables } = updated.config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0].name, "publication_b");
    assert_eq!(tables[1].name, "publication_c");
    if server_version_num >= POSTGRES_15 {
        assert_eq!(tables[1].columns.as_deref(), Some(["id".to_owned()].as_slice()));
        assert_eq!(tables[1].row_filter.as_deref(), Some("(id > 0)"));
    }
    assert_eq!(
        updated.tables.iter().map(|table| table.name.as_str()).collect::<Vec<_>>(),
        vec!["publication_b", "publication_c"]
    );

    let publication_oid_after_update: i64 =
        sqlx::query_scalar("select oid::bigint from pg_catalog.pg_publication where pubname = $1")
            .bind("replace_tables_v2")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    assert_eq!(publication_oid_after_update, publication_oid);

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_v2_reports_postgres_effective_partition_set() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool
        .execute(
            "create table public.partition_events (occurred_on date not null, id bigint not null) \
             partition by range (occurred_on)",
        )
        .await
        .unwrap();
    source_pool
        .execute(
            "create table public.partition_events_2025 partition of public.partition_events for \
             values from ('2025-01-01') to ('2026-01-01') partition by range (occurred_on)",
        )
        .await
        .unwrap();
    source_pool
        .execute(
            "create table public.partition_events_2025_h1 partition of \
             public.partition_events_2025 for values from ('2025-01-01') to ('2025-07-01')",
        )
        .await
        .unwrap();
    source_pool
        .execute(
            "create table public.partition_events_2025_h2 partition of \
             public.partition_events_2025 for values from ('2025-07-01') to ('2026-01-01')",
        )
        .await
        .unwrap();
    source_pool
        .execute(
            "create table public.partition_events_2026 partition of public.partition_events for \
             values from ('2026-01-01') to ('2027-01-01')",
        )
        .await
        .unwrap();

    let rows = sqlx::query_as::<_, (i64, String)>(
        r#"
        select c.oid::bigint, c.relname
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
            and c.relname like 'partition_events%'
        order by c.relname;
        "#,
    )
    .fetch_all(&source_pool)
    .await
    .unwrap();
    let table = |name: &str| {
        let (id, _) = rows.iter().find(|(_, table_name)| table_name == name).unwrap();
        PublicationTableConfig {
            id: u32::try_from(*id).unwrap(),
            schema: "public".to_owned(),
            name: name.to_owned(),
            columns: None,
            row_filter: None,
        }
    };
    let mut config = PublicationConfig {
        table_selection: PublicationTableSelection::Tables {
            tables: vec![
                table("partition_events"),
                table("partition_events_2025"),
                table("partition_events_2025_h1"),
            ],
        },
        operations: vec![PublicationOperation::Insert],
        publish_via_partition_root: false,
        publish_generated_columns: None,
    };

    let response =
        app.create_source_publication_v2(tenant_id, source_id, "partition_set_v2", &config).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let publication: PublicationDetails = response.json().await.unwrap();
    assert_eq!(
        publication.tables.iter().map(|table| table.name.as_str()).collect::<Vec<_>>(),
        vec!["partition_events_2025_h1", "partition_events_2025_h2", "partition_events_2026",]
    );
    assert!(publication.tables.iter().all(|table| table.kind == SourceTableKind::Table));

    let PublicationTableSelection::Tables { tables } = &mut config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    tables[2].row_filter = Some("id < 100".to_owned());
    let response =
        app.create_source_publication_v2(tenant_id, source_id, "partition_set_v2", &config).await;
    assert_eq!(response.status(), StatusCode::OK);
    let publication: PublicationDetails = response.json().await.unwrap();
    let PublicationTableSelection::Tables { tables } = publication.config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    assert_eq!(tables[2].row_filter.as_deref(), Some("(id < 100)"));

    config.publish_via_partition_root = true;
    let response =
        app.create_source_publication_v2(tenant_id, source_id, "partition_set_v2", &config).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = app.read_source_publication_v2(tenant_id, source_id, "partition_set_v2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let publication: PublicationDetails = response.json().await.unwrap();
    assert!(!publication.config.publish_via_partition_root);
    assert_eq!(publication.tables.len(), 3);

    let mut root_config = PublicationConfig {
        table_selection: PublicationTableSelection::Tables {
            tables: vec![
                table("partition_events"),
                table("partition_events_2025"),
                table("partition_events_2025_h1"),
            ],
        },
        operations: vec![PublicationOperation::Insert],
        publish_via_partition_root: true,
        publish_generated_columns: None,
    };
    let PublicationTableSelection::Tables { tables } = &mut root_config.table_selection else {
        panic!("expected an explicit-table publication");
    };
    tables[0].row_filter = Some("id >= 0".to_owned());
    let response = app
        .create_source_publication_v2(tenant_id, source_id, "partition_root_set_v2", &root_config)
        .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let publication: PublicationDetails = response.json().await.unwrap();
    assert_eq!(publication.tables.len(), 1);
    assert_eq!(publication.tables[0].name, "partition_events");
    assert_eq!(publication.tables[0].kind, SourceTableKind::PartitionedTable);

    root_config.table_selection =
        PublicationTableSelection::Tables { tables: vec![table("partition_events_2025_h1")] };
    let response = app
        .create_source_publication_v2(tenant_id, source_id, "partition_root_set_v2", &root_config)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let publication: PublicationDetails = response.json().await.unwrap();
    assert_eq!(publication.tables.len(), 1);
    assert_eq!(publication.tables[0].name, "partition_events_2025_h1");

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_v2_supports_open_ended_table_selections() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    let (source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;

    source_pool
        .execute("create table public.before_all_tables (id bigint primary key)")
        .await
        .unwrap();
    let all_tables = PublicationConfig {
        table_selection: PublicationTableSelection::AllTables,
        operations: vec![
            PublicationOperation::Insert,
            PublicationOperation::Update,
            PublicationOperation::Delete,
            PublicationOperation::Truncate,
        ],
        publish_via_partition_root: false,
        publish_generated_columns: None,
    };
    let response =
        app.create_source_publication_v2(tenant_id, source_id, "all_tables_v2", &all_tables).await;
    assert_eq!(response.status(), StatusCode::CREATED);

    source_pool
        .execute("create table public.after_all_tables (id bigint primary key)")
        .await
        .unwrap();
    let response = app.read_source_publication_v2(tenant_id, source_id, "all_tables_v2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: PublicationDetails = response.json().await.unwrap();
    assert_eq!(response.config.table_selection, PublicationTableSelection::AllTables);
    assert!(response.tables.iter().any(|table| table.name == "before_all_tables"));
    assert!(response.tables.iter().any(|table| table.name == "after_all_tables"));
    let response =
        app.create_source_publication_v2(tenant_id, source_id, "all_tables_v2", &all_tables).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let server_version_num: i32 =
        sqlx::query_scalar("select current_setting('server_version_num')::int")
            .fetch_one(&source_pool)
            .await
            .unwrap();
    if server_version_num >= POSTGRES_15 {
        source_pool.execute("create schema publication_schema").await.unwrap();
        source_pool
            .execute("create table publication_schema.before_schema (id bigint primary key)")
            .await
            .unwrap();
        let config = PublicationConfig {
            table_selection: PublicationTableSelection::TablesInSchema {
                schemas: vec!["publication_schema".to_owned()],
            },
            operations: vec![PublicationOperation::Insert],
            publish_via_partition_root: true,
            publish_generated_columns: None,
        };
        let response = app
            .create_source_publication_v2(tenant_id, source_id, "schema_tables_v2", &config)
            .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        source_pool
            .execute("create table publication_schema.after_schema (id bigint primary key)")
            .await
            .unwrap();
        let response =
            app.read_source_publication_v2(tenant_id, source_id, "schema_tables_v2").await;
        assert_eq!(response.status(), StatusCode::OK);
        let response: PublicationDetails = response.json().await.unwrap();
        assert!(matches!(
            response.config.table_selection,
            PublicationTableSelection::TablesInSchema { .. }
        ));
        assert!(response.config.publish_via_partition_root);
        assert!(response.tables.iter().any(|table| table.name == "before_schema"));
        assert!(response.tables.iter().any(|table| table.name == "after_schema"));
        let response = app
            .create_source_publication_v2(tenant_id, source_id, "schema_tables_v2", &config)
            .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    drop(source_pool);
    drop_pg_database(&source_db_config).await;
}
