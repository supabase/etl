use etl_api::configs::source::FullApiSourceConfig;
use etl_api::routes::sources::ReadSourceResponse;
use etl_api::routes::tenants::ReadTenantResponse;
use etl_api::routes::tenants_sources::{CreateTenantSourceRequest, CreateTenantSourceResponse};
use etl_config::SerializableSecretString;
use etl_config::shared::PgConnectionConfig;
use etl_postgres::sqlx::test_utils::{create_pg_database, drop_pg_database};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use uuid::Uuid;

use crate::{
    support::database::{
        create_trusted_source_database, drop_trusted_source_database, get_test_db_config,
    },
    support::mocks::sources::{new_name, new_source_config},
    support::test_app::{spawn_test_app, spawn_test_app_with_trusted_username},
};

mod support;

fn source_config_from_db_config(source_db_config: &PgConnectionConfig) -> FullApiSourceConfig {
    FullApiSourceConfig {
        host: source_db_config.host.clone(),
        port: source_db_config.port,
        name: source_db_config.name.clone(),
        username: source_db_config.username.clone(),
        password: source_db_config
            .password
            .as_ref()
            .map(|password| SerializableSecretString::from(password.expose_secret().to_string())),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_and_source_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let tenant_source = CreateTenantSourceRequest {
        tenant_id: "abcdefghijklmnopqrst".to_string(),
        tenant_name: "NewTenant".to_string(),
        source_name: new_name(),
        source_config: new_source_config(),
    };
    let response = app.create_tenant_source(&tenant_source).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.tenant_id, "abcdefghijklmnopqrst");
    assert_eq!(response.source_id, 1);

    let tenant_id = &response.tenant_id;
    let source_id = response.source_id;

    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, tenant_source.tenant_name);

    let response = app.read_source(tenant_id, source_id).await;
    let response: ReadSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, source_id);
    assert_eq!(&response.tenant_id, tenant_id);
    assert_eq!(response.name, tenant_source.source_name);
    insta::assert_debug_snapshot!(response.config);
}

#[tokio::test(flavor = "multi_thread")]
async fn creating_same_tenant_source_twice_returns_409() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let tenant_source = CreateTenantSourceRequest {
        tenant_id: "abcdefghijklmnopqrst".to_string(),
        tenant_name: "DuplicateTenant".to_string(),
        source_name: new_name(),
        source_config: new_source_config(),
    };

    // Act - Create the tenant source the first time
    let response = app.create_tenant_source(&tenant_source).await;
    assert!(response.status().is_success());

    // Act - Create the same tenant source again
    let response = app.create_tenant_source(&tenant_source).await;

    // Assert - Second attempt should return 409 Conflict
    assert_eq!(response.status().as_u16(), 409);
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_and_source_creation_with_matching_trusted_username_succeeds() {
    init_test_tracing();

    let trusted_source = create_trusted_source_database().await;
    let app =
        spawn_test_app_with_trusted_username(Some(trusted_source.trusted_username.clone())).await;

    let tenant_source = CreateTenantSourceRequest {
        tenant_id: "abcdefghijklmnopqrst".to_string(),
        tenant_name: "TrustedTenant".to_string(),
        source_name: new_name(),
        source_config: source_config_from_db_config(&trusted_source.trusted_config),
    };

    let response = app.create_tenant_source(&tenant_source).await;

    assert!(
        response.status().is_success(),
        "Expected successful tenant+source creation with matching trusted username, got status: {}",
        response.status()
    );

    drop_trusted_source_database(trusted_source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_and_source_creation_with_non_matching_trusted_username_fails() {
    init_test_tracing();

    let mut source_db_config = get_test_db_config();
    source_db_config.name = format!("test_source_db_{}", Uuid::new_v4());

    let app = spawn_test_app_with_trusted_username(Some("different_user".to_string())).await;
    let _source_pool = create_pg_database(&source_db_config).await;

    let tenant_source = CreateTenantSourceRequest {
        tenant_id: "abcdefghijklmnopqrst".to_string(),
        tenant_name: "UntrustedTenant".to_string(),
        source_name: new_name(),
        source_config: source_config_from_db_config(&source_db_config),
    };

    let response = app.create_tenant_source(&tenant_source).await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_and_source_creation_with_invalid_trusted_role_profile_fails() {
    init_test_tracing();

    let mut source_db_config = get_test_db_config();
    source_db_config.name = format!("test_source_db_{}", Uuid::new_v4());

    let trusted_username = source_db_config.username.clone();
    let app = spawn_test_app_with_trusted_username(Some(trusted_username)).await;
    let _source_pool = create_pg_database(&source_db_config).await;

    let tenant_source = CreateTenantSourceRequest {
        tenant_id: "abcdefghijklmnopqrst".to_string(),
        tenant_name: "InvalidTrustedTenant".to_string(),
        source_name: new_name(),
        source_config: source_config_from_db_config(&source_db_config),
    };

    let response = app.create_tenant_source(&tenant_source).await;
    let status = response.status();
    let body = response.text().await.expect("failed to read response body");

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("ETL needs to work properly"));

    drop_pg_database(&source_db_config).await;
}
