use crate::support::mocks::sources::new_name;
use crate::support::mocks::tenants::create_tenant;
use crate::support::test_app::spawn_test_app;
use etl_api::configs::source::FullApiSourceConfig;
use etl_api::db::tables::Table;
use etl_api::routes::sources::publications::{CreatePublicationRequest, UpdatePublicationRequest};
use etl_api::routes::sources::{CreateSourceRequest, CreateSourceResponse};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use secrecy::ExposeSecret;

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn create_publication_with_unsupported_column_types_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    // Create a source pointing to the test app's database
    let db_config = app.database_config();
    let source = CreateSourceRequest {
        name: new_name(),
        config: FullApiSourceConfig {
            host: db_config.host.clone(),
            port: db_config.port,
            name: db_config.name.clone(),
            username: db_config.username.clone(),
            password: db_config
                .password
                .as_ref()
                .map(|p| p.expose_secret().to_string().into()),
        },
    };
    let response = app.create_source(&tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Connect to the source database and create a table with a custom enum type
    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    // Create a custom enum type (which will have an unknown OID to tokio-postgres)
    sqlx::query(r#"create type custom_status as enum ('pending', 'active', 'inactive')"#)
        .execute(&source_pool)
        .await
        .expect("failed to create enum type");

    // Create a table with that custom enum type
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

    // Act: Try to create a publication including the table with custom enum
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

    // Assert: Should fail with 400 Bad Request
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

    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    // Create a source pointing to the test app's database
    let db_config = app.database_config();
    let source = CreateSourceRequest {
        name: new_name(),
        config: FullApiSourceConfig {
            host: db_config.host.clone(),
            port: db_config.port,
            name: db_config.name.clone(),
            username: db_config.username.clone(),
            password: db_config
                .password
                .as_ref()
                .map(|p| p.expose_secret().to_string().into()),
        },
    };
    let response = app.create_source(&tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Connect to the source database and create a table with only supported types
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

    // Act: Create a publication with supported types
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

    // Assert: Should succeed
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_publication_with_unsupported_column_types_fails() {
    init_test_tracing();

    // Arrange
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    // Create a source pointing to the test app's database
    let db_config = app.database_config();
    let source = CreateSourceRequest {
        name: new_name(),
        config: FullApiSourceConfig {
            host: db_config.host.clone(),
            port: db_config.port,
            name: db_config.name.clone(),
            username: db_config.username.clone(),
            password: db_config
                .password
                .as_ref()
                .map(|p| p.expose_secret().to_string().into()),
        },
    };
    let response = app.create_source(&tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    let source_pool = app.get_source_pool(&tenant_id, source_id).await;

    // Create two tables: one with supported types, one with unsupported
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

    // Create publication with only the good table
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

    // Act: Try to update publication to include bad table
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

    // Assert: Should fail with 400
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let error_body = response.text().await.expect("failed to read response body");
    assert!(error_body.contains("unsupported column types"));
    assert!(error_body.contains("bad_enum"));
}
