use api::routes::tenants::{
    CreateOrUpdateTenantRequest, CreateTenantRequest, CreateTenantResponse, ReadTenantResponse,
    ReadTenantsResponse, UpdateTenantRequest,
};
use reqwest::StatusCode;
use telemetry::init_test_tracing;

use crate::common::test_app::{TestApp, spawn_test_app};

pub async fn create_tenant(app: &TestApp) -> String {
    create_tenant_with_id_and_name(
        app,
        "abcdefghijklmnopqrst".to_string(),
        "NewTenant".to_string(),
    )
    .await
}

pub async fn create_tenant_with_id_and_name(app: &TestApp, id: String, name: String) -> String {
    let tenant = CreateTenantRequest { id, name };
    let response = app.create_tenant(&tenant).await;
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
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
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, "abcdefghijklmnopqrst");

    let tenant_id = &response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn create_or_update_tenant_creates_a_new_tenant() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let tenant_id = "abcdefghijklmnopqrst";
    let tenant = CreateOrUpdateTenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_or_update_tenant(tenant_id, &tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);

    let tenant_id = &response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

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
    let tenant = CreateOrUpdateTenantRequest {
        name: "NewTenant".to_string(),
    };
    let response = app.create_or_update_tenant(tenant_id, &tenant).await;
    assert!(response.status().is_success());
    let tenant = CreateOrUpdateTenantRequest {
        name: "UpdatedTenant".to_string(),
    };
    let response = app.create_or_update_tenant(tenant_id, &tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, tenant_id);

    let tenant_id = &response.id;
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");

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
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = &response.id;

    // Act
    let response = app.read_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_tenant_cant_be_read() {
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
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = &response.id;

    // Act
    let updated_tenant = UpdateTenantRequest {
        name: "UpdatedTenant".to_string(),
    };
    let response = app.update_tenant(tenant_id, &updated_tenant).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    let response: ReadTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(&response.id, tenant_id);
    assert_eq!(response.name, updated_tenant.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_tenant_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let updated_tenant = UpdateTenantRequest {
        name: "UpdatedTenant".to_string(),
    };
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
    let response: CreateTenantResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let tenant_id = &response.id;

    // Act
    let response = app.delete_tenant(tenant_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_tenant(tenant_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_tenant_returns_ok_when_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let response = app.delete_tenant("42").await;

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
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
    let response: ReadTenantsResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    for tenant in response.tenants {
        if tenant.id == tenant1_id {
            assert_eq!(tenant.name, "Tenant1");
        } else if tenant.id == tenant2_id {
            assert_eq!(tenant.name, "Tenant2");
        }
    }
}
