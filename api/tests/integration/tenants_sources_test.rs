use api::routes::sources::ReadSourceResponse;
use api::routes::tenants::ReadTenantResponse;
use api::routes::tenants_sources::{CreateTenantSourceRequest, CreateTenantSourceResponse};

use crate::{
    common::test_app::spawn_test_app,
    integration::sources_test::{new_name, new_source_config},
};

#[tokio::test(flavor = "multi_thread")]
async fn tenant_and_source_can_be_created() {
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
