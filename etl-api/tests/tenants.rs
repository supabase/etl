use etl_api::{
    configs::source::FullApiSourceConfig,
    k8s::PodStatus,
    routes::{
        pipelines::{CreatePipelineRequest, CreatePipelineResponse},
        sources::CreateSourceRequest,
        tenants::{
            CreateOrUpdateTenantRequest, CreateTenantRequest, CreateTenantResponse,
            ReadTenantResponse, ReadTenantsResponse, UpdateTenantRequest,
        },
    },
};
use etl_config::SerializableSecretString;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use sqlx::Executor;

use crate::support::{
    database::{
        create_test_source_database, create_trusted_source_database, drop_trusted_source_database,
        run_etl_migrations_on_source_database,
    },
    mocks::{
        create_default_image,
        destinations::create_destination,
        pipelines::new_pipeline_config,
        tenants::{create_tenant, create_tenant_with_id_and_name},
    },
    test_app::{TestApp, spawn_test_app, spawn_test_app_with_trusted_username},
};

async fn create_pipeline_for_source(
    app: &TestApp,
    tenant_id: &str,
    source_id: i64,
    destination_id: i64,
) -> i64 {
    let pipeline =
        CreatePipelineRequest { source_id, destination_id, config: new_pipeline_config() };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(response.status().is_success());
    let response: CreatePipelineResponse =
        response.json().await.expect("failed to deserialize response");

    response.id
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let tenant = CreateTenantRequest {
        id: "abcdefghijklmnopqrst".to_string(),
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, "abcdefghijklmnopqrst");

    let tenant_id = &response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse =
        response.json().await.expect("failed to deserialize response");

    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn creating_duplicate_tenant_returns_409() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let tenant = CreateTenantRequest {
        id: "abcdefghijklmnopqrst".to_string(),
        name: "DuplicateTenant".to_string(),
    };

    // Act - Create the tenant the first time
    let response = app.create_tenant(&tenant).await;
    assert!(response.status().is_success());

    // Act - Create the same tenant again
    let response = app.create_tenant(&tenant).await;

    // Assert - Second attempt should return 409 Conflict
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_or_update_tenant_creates_a_new_tenant() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let tenant_id = "abcdefghijklmnopqrst";
    let tenant = CreateOrUpdateTenantRequest { name: "NewTenant".to_string() };
    let response = app.create_or_update_tenant(tenant_id, &tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);

    let tenant_id = &response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse =
        response.json().await.expect("failed to deserialize response");

    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_or_update_tenant_updates_an_existing_tenant() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let tenant_id = "abcdefghijklmnopqrst";
    let tenant = CreateOrUpdateTenantRequest { name: "NewTenant".to_string() };
    let response = app.create_or_update_tenant(tenant_id, &tenant).await;
    assert!(response.status().is_success());
    let tenant = CreateOrUpdateTenantRequest { name: "UpdatedTenant".to_string() };
    let response = app.create_or_update_tenant(tenant_id, &tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);

    let tenant_id = &response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse =
        response.json().await.expect("failed to deserialize response");

    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_tenant_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant = CreateTenantRequest {
        id: "abcdefghijklmnopqrst".to_string(),
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse =
        response.json().await.expect("failed to deserialize response");
    let tenant_id = &response.id;

    // Act
    let response = app.read_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadTenantResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_tenant_cannot_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let response = app.read_tenant("42").await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_tenant_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant = CreateTenantRequest {
        id: "abcdefghijklmnopqrst".to_string(),
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse =
        response.json().await.expect("failed to deserialize response");
    let tenant_id = &response.id;

    // Act
    let updated_tenant = UpdateTenantRequest { name: "UpdatedTenant".to_string() };
    let response = app.update_tenant(tenant_id, &updated_tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse =
        response.json().await.expect("failed to deserialize response");
    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, updated_tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_existing_tenant_cannot_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let updated_tenant = UpdateTenantRequest { name: "UpdatedTenant".to_string() };
    let response = app.update_tenant("42", &updated_tenant).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_tenant_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant = CreateTenantRequest {
        id: "abcdefghijklmnopqrst".to_string(),
        name: "NewTenant".to_string(),
    };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse =
        response.json().await.expect("failed to deserialize response");
    let tenant_id = &response.id;

    // Act
    let response = app.delete_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_non_existing_tenant_returns_ok() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let response = app.delete_tenant("42").await;

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_with_active_pipeline_cannot_be_deleted() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    let (_source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;
    let pipeline_id = create_pipeline_for_source(&app, tenant_id, source_id, destination_id).await;

    let response = app.delete_tenant(tenant_id).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let tenant_response = app.read_tenant(tenant_id).await;
    assert!(tenant_response.status().is_success());

    let pipeline_response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert!(pipeline_response.status().is_success());

    etl_postgres::sqlx::test_utils::drop_pg_database(&source_db_config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_with_inactive_pipelines_can_be_deleted_and_uninstalls_source_state() {
    init_test_tracing();

    let trusted_source = create_trusted_source_database().await;
    let app =
        spawn_test_app_with_trusted_username(Some(trusted_source.trusted_username.clone())).await;
    let tenant_id = &create_tenant(&app).await;

    let source = CreateSourceRequest {
        name: "Test Source".to_string(),
        config: FullApiSourceConfig {
            host: trusted_source.trusted_config.host.clone(),
            port: trusted_source.trusted_config.port,
            name: trusted_source.trusted_config.name.clone(),
            username: trusted_source.trusted_config.username.clone(),
            password: trusted_source.trusted_config.password.as_ref().map(|password| {
                SerializableSecretString::from(password.expose_secret().to_string())
            }),
        },
    };
    let response = app.create_source(tenant_id, &source).await;
    assert!(response.status().is_success());
    let response: etl_api::routes::sources::CreateSourceResponse =
        response.json().await.expect("failed to deserialize response");
    let source_id = response.id;

    run_etl_migrations_on_source_database(&trusted_source.trusted_config).await;

    create_default_image(&app).await;
    let destination_id = create_destination(&app, tenant_id).await;
    let pipeline_id = create_pipeline_for_source(&app, tenant_id, source_id, destination_id).await;

    app.k8s_state.set_pod_status(PodStatus::Stopped).await;

    trusted_source
        .admin_pool
        .execute("create table public.tenant_uninstall_test_users (id bigint primary key)")
        .await
        .expect("failed to create test table");
    trusted_source
        .admin_pool
        .execute(
            "create publication tenant_uninstall_pub for table public.tenant_uninstall_test_users",
        )
        .await
        .expect("failed to create publication");

    let response = app.delete_tenant(tenant_id).await;
    assert!(response.status().is_success());

    let tenant_response = app.read_tenant(tenant_id).await;
    assert_eq!(tenant_response.status(), StatusCode::NOT_FOUND);

    let source_response = app.read_source(tenant_id, source_id).await;
    assert_eq!(source_response.status(), StatusCode::NOT_FOUND);

    let pipeline_response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(pipeline_response.status(), StatusCode::NOT_FOUND);

    let etl_schema_exists: bool =
        sqlx::query_scalar("select exists(select 1 from pg_namespace where nspname = 'etl')")
            .fetch_one(&trusted_source.admin_pool)
            .await
            .expect("failed to check etl schema");
    assert!(!etl_schema_exists);

    let publication_exists: bool = sqlx::query_scalar(
        "select exists(select 1 from pg_publication where pubname = 'tenant_uninstall_pub')",
    )
    .fetch_one(&trusted_source.admin_pool)
    .await
    .expect("failed to check publication");
    assert!(publication_exists);

    drop_trusted_source_database(trusted_source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_with_unreachable_source_can_still_be_deleted() {
    init_test_tracing();

    let app = spawn_test_app().await;
    let tenant_id = &create_tenant(&app).await;
    create_default_image(&app).await;

    let (_source_pool, source_id, source_db_config) =
        create_test_source_database(&app, tenant_id).await;
    let destination_id = create_destination(&app, tenant_id).await;
    let pipeline_id = create_pipeline_for_source(&app, tenant_id, source_id, destination_id).await;

    app.k8s_state.set_pod_status(PodStatus::Stopped).await;
    etl_postgres::sqlx::test_utils::drop_pg_database(&source_db_config).await;

    let response = app.delete_tenant(tenant_id).await;
    assert!(response.status().is_success());

    let tenant_response = app.read_tenant(tenant_id).await;
    assert_eq!(tenant_response.status(), StatusCode::NOT_FOUND);

    let pipeline_response = app.read_pipeline(tenant_id, pipeline_id).await;
    assert_eq!(pipeline_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_tenants_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let tenant1_id = create_tenant_with_id_and_name(
        &app,
        "abcdefghijklmnopqrst".to_string(),
        "Tenant1".to_string(),
    )
    .await;
    let tenant2_id = create_tenant_with_id_and_name(
        &app,
        "tsrqponmlkjihgfedcba".to_string(),
        "Tenant2".to_string(),
    )
    .await;

    // Act
    let response = app.read_all_tenants().await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadTenantsResponse =
        response.json().await.expect("failed to deserialize response");
    for tenant in response.tenants {
        if tenant.id == tenant1_id {
            assert_eq!(tenant.name, "Tenant1");
        } else if tenant.id == tenant2_id {
            assert_eq!(tenant.name, "Tenant2");
        }
    }
}
