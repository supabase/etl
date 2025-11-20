use crate::support::mocks::sources::create_test_db_source;
use crate::support::mocks::tenants::create_tenant;
use crate::support::test_app::spawn_test_app;
use etl_api::db::tables::Table;
use etl_api::routes::sources::publications::{CreatePublicationRequest, UpdatePublicationRequest};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_unsupported_column_types_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    // Create a custom enum type (which will have an unknown OID to tokio-postgres)
    sqlx::query(r#"create type custom_status as enum ('pending', 'active', 'inactive')"#)
        .execute(&source_pool)
        .await
        .expect("failed to create enum type");

    sqlx::query(
        r#"
        create table test_table (
            id integer primary key,
            name text not null,
            status custom_status not null
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table");

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "test_table".to_string(),
        }],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_body = response.text().await.expect("failed to read response body");
    assert!(
        error_body.contains("unsupported column types"),
        "Expected error about unsupported types, got: {}",
        error_body
    );
    assert!(
        error_body.contains("custom_status"),
        "Expected error to mention custom_status type, got: {}",
        error_body
    );
    assert!(
        error_body.contains("public.test_table.status"),
        "Expected error to mention the problematic column, got: {}",
        error_body
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_supported_column_types_succeeds() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table test_table (
            id integer primary key,
            name text not null,
            age int4,
            created_at timestamptz,
            metadata jsonb
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table");

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "test_table".to_string(),
        }],
    };
    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_publication_with_unsupported_column_types_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table good_table (
            id integer primary key,
            name text not null
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create good_table");

    sqlx::query("create type bad_enum as enum ('a', 'b')")
        .execute(&source_pool)
        .await
        .expect("failed to create enum");

    sqlx::query(
        r#"
        create table bad_table (
            id integer primary key,
            status bad_enum not null
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create bad_table");

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "good_table".to_string(),
        }],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::OK);

    let updated_publication = UpdatePublicationRequest {
        tables: vec![
            Table {
                schema: "public".to_string(),
                name: "good_table".to_string(),
            },
            Table {
                schema: "public".to_string(),
                name: "bad_table".to_string(),
            },
        ],
    };

    let response = app
        .update_publication(
            &tenant_id,
            source_id,
            "test_publication",
            &updated_publication,
        )
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("unsupported column types"));
    assert!(error_body.contains("bad_enum"));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_nonexistent_source_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![],
    };

    let response = app
        .create_publication(&tenant_id, 99999, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("source") || error_body.contains("not found"));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_duplicate_publication_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table test_table (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table");

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "test_table".to_string(),
        }],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_empty_tables_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let publication = CreatePublicationRequest {
        name: "empty_publication".to_string(),
        tables: vec![],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("tables cannot be empty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_empty_name_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table test_table (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table");

    let publication = CreatePublicationRequest {
        name: "".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "test_table".to_string(),
        }],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("name cannot be empty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_empty_table_schema_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "".to_string(),
            name: "test_table".to_string(),
        }],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("schema cannot be empty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_empty_table_name_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "".to_string(),
        }],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("name cannot be empty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_whitespace_only_name_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table test_table (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table");

    let publication = CreatePublicationRequest {
        name: "   ".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "test_table".to_string(),
        }],
    };

    let response = app
        .create_publication(&tenant_id, source_id, &publication)
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("name cannot be empty"));
}

#[tokio::test(flavor = "multi_thread")]
async fn read_publication_succeeds() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table users (
            id integer primary key,
            name text not null
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table");

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "users".to_string(),
        }],
    };

    app.create_publication(&tenant_id, source_id, &publication)
        .await;

    let response = app
        .read_publication(&tenant_id, source_id, "test_publication")
        .await;

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.expect("failed to parse json");
    assert_eq!(body["name"], "test_publication");
    assert_eq!(body["tables"].as_array().unwrap().len(), 1);
    assert_eq!(body["tables"][0]["schema"], "public");
    assert_eq!(body["tables"][0]["name"], "users");
}

#[tokio::test(flavor = "multi_thread")]
async fn read_nonexistent_publication_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let response = app
        .read_publication(&tenant_id, source_id, "nonexistent_publication")
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_publication_with_nonexistent_source_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let response = app
        .read_publication(&tenant_id, 99999, "test_publication")
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_all_publications_with_multiple_publications_succeeds() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table table1 (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table1");

    sqlx::query(
        r#"
        create table table2 (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table2");

    let pub1 = CreatePublicationRequest {
        name: "publication_1".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "table1".to_string(),
        }],
    };
    app.create_publication(&tenant_id, source_id, &pub1).await;

    let pub2 = CreatePublicationRequest {
        name: "publication_2".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "table2".to_string(),
        }],
    };
    app.create_publication(&tenant_id, source_id, &pub2).await;

    let response = app.read_all_publications(&tenant_id, source_id).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.expect("failed to parse json");
    let publications = body["publications"].as_array().unwrap();
    assert_eq!(publications.len(), 2);

    let pub_names: Vec<&str> = publications
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();

    assert!(pub_names.contains(&"publication_1"));
    assert!(pub_names.contains(&"publication_2"));
}

#[tokio::test(flavor = "multi_thread")]
async fn read_all_publications_with_empty_list_succeeds() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let response = app.read_all_publications(&tenant_id, source_id).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = response.json().await.expect("failed to parse json");
    assert_eq!(body["publications"].as_array().unwrap().len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_all_publications_with_nonexistent_source_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let response = app.read_all_publications(&tenant_id, 99999).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_publication_succeeds() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table table1 (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table1");

    sqlx::query(
        r#"
        create table table2 (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table2");

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![Table {
            schema: "public".to_string(),
            name: "table1".to_string(),
        }],
    };
    app.create_publication(&tenant_id, source_id, &publication)
        .await;

    let updated_publication = UpdatePublicationRequest {
        tables: vec![
            Table {
                schema: "public".to_string(),
                name: "table1".to_string(),
            },
            Table {
                schema: "public".to_string(),
                name: "table2".to_string(),
            },
        ],
    };

    let response = app
        .update_publication(
            &tenant_id,
            source_id,
            "test_publication",
            &updated_publication,
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .read_publication(&tenant_id, source_id, "test_publication")
        .await;

    let body: serde_json::Value = response.json().await.expect("failed to parse json");
    assert_eq!(body["tables"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_nonexistent_publication_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    sqlx::query(
        r#"
        create table test_table (
            id integer primary key
        )
        "#,
    )
    .execute(&source_pool)
    .await
    .expect("failed to create table");

    let updated_publication = UpdatePublicationRequest {
        tables: vec![Table {
            schema: "public".to_string(),
            name: "test_table".to_string(),
        }],
    };

    let response = app
        .update_publication(
            &tenant_id,
            source_id,
            "nonexistent_publication",
            &updated_publication,
        )
        .await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_publication_with_nonexistent_source_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let updated_publication = UpdatePublicationRequest {
        tables: vec![Table {
            schema: "public".to_string(),
            name: "test_table".to_string(),
        }],
    };

    let response = app
        .update_publication(&tenant_id, 99999, "test_publication", &updated_publication)
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_publication_succeeds() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let publication = CreatePublicationRequest {
        name: "test_publication".to_string(),
        tables: vec![],
    };
    app.create_publication(&tenant_id, source_id, &publication)
        .await;

    let response = app
        .delete_publication(&tenant_id, source_id, "test_publication")
        .await;

    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .read_publication(&tenant_id, source_id, "test_publication")
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_nonexistent_publication_succeeds() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let source_id = create_test_db_source(&app, &tenant_id).await;

    let response = app
        .delete_publication(&tenant_id, source_id, "nonexistent_publication")
        .await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_publication_with_nonexistent_source_fails() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let response = app
        .delete_publication(&tenant_id, 99999, "test_publication")
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
