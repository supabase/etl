use etl_api::{
    configs::destination::{ApiDestinationConfig, ApiIcebergConfig},
    routes::{destinations::CreateDestinationResponse, pipelines::CreatePipelineResponse},
};
use etl_config::{SerializableSecretString, shared::ClickHouseEngine};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

use crate::support::{
    mocks::{
        create_default_image,
        destinations::{
            new_bigquery_destination_config, new_ducklake_destination_config,
            new_iceberg_supabase_destination_config, new_snowflake_destination_config,
        },
        pipelines::new_pipeline_config,
        sources::create_source,
        tenants::create_tenant,
    },
    test_app::{TestApp, spawn_test_app},
};

async fn create_pipeline(
    app: &TestApp,
    tenant_id: &str,
    destination_config: ApiDestinationConfig,
) -> (i64, i64) {
    create_pipeline_with_destination_name(app, tenant_id, "Runtime destination", destination_config)
        .await
}

async fn create_pipeline_with_destination_name(
    app: &TestApp,
    tenant_id: &str,
    destination_name: &str,
    destination_config: ApiDestinationConfig,
) -> (i64, i64) {
    let source_id = create_source(app, tenant_id).await;
    create_default_image(app).await;

    let destination = etl_api::routes::destinations::CreateDestinationRequest {
        name: destination_name.to_owned(),
        config: destination_config,
    };
    let response = app.create_destination(tenant_id, &destination).await;
    assert!(response.status().is_success());
    let destination: CreateDestinationResponse =
        response.json().await.expect("failed to deserialize destination response");

    let pipeline = etl_api::routes::pipelines::CreatePipelineRequest {
        source_id,
        destination_id: destination.id,
        config: new_pipeline_config(),
    };
    let response = app.create_pipeline(tenant_id, &pipeline).await;
    assert!(response.status().is_success());
    let pipeline: CreatePipelineResponse =
        response.json().await.expect("failed to deserialize pipeline response");

    (destination.id, pipeline.id)
}

#[tokio::test(flavor = "multi_thread")]
async fn ducklake_runtime_config_returns_credentials_without_writing_kubernetes() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let (destination_id, pipeline_id) =
        create_pipeline(&app, &tenant_id, new_ducklake_destination_config()).await;

    let k8s_create_calls = app.k8s_state.create_calls();
    let response = app.resolve_runtime_config(&tenant_id, destination_id).await;

    assert!(response.status().is_success());
    assert_eq!(response.headers()["cache-control"], "no-store");
    let response: serde_json::Value =
        response.json().await.expect("failed to deserialize runtime config response");
    assert_eq!(response["tenant_id"], tenant_id);
    assert_eq!(response["pipeline_id"], pipeline_id);
    assert_eq!(response["destination_id"], destination_id);
    assert_eq!(response["destination_name"], "Runtime destination");
    assert_eq!(response["destination"]["ducklake"]["data_path"], "s3://ducklake/");
    assert_eq!(
        response["destination"]["ducklake"]["catalog_url"],
        "postgres://postgres:postgres@localhost:5432/postgres"
    );
    assert_eq!(response["destination"]["ducklake"]["s3_access_key_id"], "access-key-id");
    assert_eq!(response["destination"]["ducklake"]["s3_secret_access_key"], "secret-access-key");
    assert!(response["source"].get("password").is_none());
    assert!(response.get("source_secret").is_none());
    assert!(response.get("destination_secret").is_none());
    assert_eq!(app.k8s_state.create_calls(), k8s_create_calls);
}

#[tokio::test(flavor = "multi_thread")]
async fn bigquery_runtime_config_returns_credentials() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let (destination_id, _) =
        create_pipeline(&app, &tenant_id, new_bigquery_destination_config()).await;

    let response = app.resolve_runtime_config(&tenant_id, destination_id).await;

    assert!(response.status().is_success());
    let response: serde_json::Value =
        response.json().await.expect("failed to deserialize runtime config response");
    assert_eq!(response["destination"]["big_query"]["project_id"], "project-id");
    assert_eq!(response["destination"]["big_query"]["service_account_key"], "service-account-key");
    assert!(response.get("destination_secret").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn clickhouse_runtime_config_returns_credentials() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let destination = ApiDestinationConfig::ClickHouse {
        url: "https://clickhouse.example.com:8443".parse().unwrap(),
        user: "etl_user".to_owned(),
        password: Some(SerializableSecretString::from("fake-clickhouse-password".to_owned())),
        database: "analytics".to_owned(),
        engine: ClickHouseEngine::default(),
    };
    let (destination_id, _) = create_pipeline(&app, &tenant_id, destination).await;

    let response = app.resolve_runtime_config(&tenant_id, destination_id).await;

    assert!(response.status().is_success());
    let response: serde_json::Value =
        response.json().await.expect("failed to deserialize runtime config response");
    assert_eq!(
        response["destination"]["clickhouse"]["url"],
        "https://clickhouse.example.com:8443/"
    );
    assert_eq!(response["destination"]["clickhouse"]["password"], "fake-clickhouse-password");
    assert!(response.get("destination_secret").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_supabase_runtime_config_returns_credentials() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let (destination_id, _) =
        create_pipeline(&app, &tenant_id, new_iceberg_supabase_destination_config()).await;

    let response = app.resolve_runtime_config(&tenant_id, destination_id).await;

    assert!(response.status().is_success());
    let response: serde_json::Value =
        response.json().await.expect("failed to deserialize runtime config response");
    let config = &response["destination"]["iceberg"]["supabase"];
    assert_eq!(config["project_ref"], "abcdefghijklmnopqrst");
    assert!(config["catalog_token"].as_str().is_some());
    assert!(config["s3_access_key_id"].as_str().is_some());
    assert!(config["s3_secret_access_key"].as_str().is_some());
    assert!(response.get("destination_secret").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_rest_runtime_config_returns_credentials() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let destination = ApiDestinationConfig::Iceberg {
        config: ApiIcebergConfig::Rest {
            catalog_uri: "https://catalog.example.com".to_owned(),
            warehouse_name: "analytics".to_owned(),
            namespace: Some("public".to_owned()),
            s3_access_key_id: SerializableSecretString::from("fake-rest-access-key".to_owned()),
            s3_secret_access_key: SerializableSecretString::from("fake-rest-secret-key".to_owned()),
            s3_endpoint: "https://s3.example.com".to_owned(),
        },
    };
    let (destination_id, _) = create_pipeline(&app, &tenant_id, destination).await;

    let response = app.resolve_runtime_config(&tenant_id, destination_id).await;

    assert!(response.status().is_success());
    let response: serde_json::Value =
        response.json().await.expect("failed to deserialize runtime config response");
    let config = &response["destination"]["iceberg"]["rest"];
    assert_eq!(config["catalog_uri"], "https://catalog.example.com");
    assert_eq!(config["s3_access_key_id"], "fake-rest-access-key");
    assert_eq!(config["s3_secret_access_key"], "fake-rest-secret-key");
    assert!(response.get("source_secret").is_none());
    assert!(response.get("destination_secret").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn snowflake_runtime_config_returns_credentials() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let (destination_id, _) =
        create_pipeline(&app, &tenant_id, new_snowflake_destination_config()).await;

    let response = app.resolve_runtime_config(&tenant_id, destination_id).await;

    assert!(response.status().is_success());
    let response: serde_json::Value =
        response.json().await.expect("failed to deserialize runtime config response");
    let config = &response["destination"]["snowflake"];
    assert_eq!(config["account_id"], "myorg-myaccount");
    assert!(config["private_key"].as_str().is_some());
    assert!(config.get("private_key_passphrase").is_none());
    assert!(response.get("destination_secret").is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_config_is_not_exposed_on_the_public_listener() {
    init_test_tracing();
    let app = spawn_test_app().await;

    let response = app
        .api_client
        .post(format!("{}/v1/internal/destinations/1/runtime-config/resolve", app.address))
        .bearer_auth(&app.api_key)
        .header("tenant_id", "abcdefghijklmnopqrst")
        .send()
        .await
        .expect("failed to execute request");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_config_requires_existing_api_authentication() {
    init_test_tracing();
    let app = spawn_test_app().await;

    let response = app
        .api_client
        .post(format!("{}/v1/internal/destinations/1/runtime-config/resolve", app.internal_address))
        .header("tenant_id", "abcdefghijklmnopqrst")
        .send()
        .await
        .expect("failed to execute request");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_config_returns_not_found_for_an_unknown_destination() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;

    let response = app.resolve_runtime_config(&tenant_id, i64::MAX).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_runtime_config_resolves_a_destination_matching_the_requested_selector() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    let (destination_id, pipeline_id) = create_pipeline_with_destination_name(
        &app,
        &tenant_id,
        "requested_destination",
        new_ducklake_destination_config(),
    )
    .await;

    let response =
        app.resolve_tenant_runtime_config(&tenant_id, "ducklake", "requested_destination").await;

    assert_eq!(response.status(), StatusCode::OK);
    let response: serde_json::Value =
        response.json().await.expect("failed to deserialize runtime config response");
    assert_eq!(response["destination_id"], destination_id);
    assert_eq!(response["pipeline_id"], pipeline_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_runtime_config_does_not_select_a_differently_named_ducklake_destination() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    create_pipeline(&app, &tenant_id, new_ducklake_destination_config()).await;

    let response =
        app.resolve_tenant_runtime_config(&tenant_id, "ducklake", "requested_destination").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_runtime_config_does_not_select_a_different_destination_kind() {
    init_test_tracing();
    let app = spawn_test_app().await;
    let tenant_id = create_tenant(&app).await;
    create_pipeline_with_destination_name(
        &app,
        &tenant_id,
        "requested_destination",
        new_bigquery_destination_config(),
    )
    .await;

    let response =
        app.resolve_tenant_runtime_config(&tenant_id, "ducklake", "requested_destination").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
