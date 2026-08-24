use etl_api::routes::ErrorMessage;
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

use crate::support::test_app::spawn_test_app;

mod columns;
mod publications;
mod schemas;
mod tables;

#[tokio::test(flavor = "multi_thread")]
async fn v2_routes_return_a_version_neutral_json_authentication_error() {
    init_test_tracing();
    let app = spawn_test_app().await;

    let response = app
        .api_client
        .get(format!("{}/v2/sources/0/tables", app.address))
        .header("tenant_id", "tenant")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[reqwest::header::WWW_AUTHENTICATE], "Bearer");
    let error: ErrorMessage = response.json().await.unwrap();
    assert_eq!(error.message, "Unauthorized");

    let response =
        app.api_client.get(format!("{}/api-docs/openapi.json", app.address)).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let openapi: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        openapi["components"]["schemas"]["PublicationTableConfig"]["properties"]["schema"]
            ["readOnly"],
        true
    );
    assert_eq!(
        openapi["components"]["schemas"]["PublicationTableConfig"]["properties"]["name"]
            ["readOnly"],
        true
    );
    let put_responses = &openapi["paths"]
        ["/v2/sources/{source_id}/publications/{publication_name}"]["put"]["responses"];
    for status in ["401", "413", "415", "422", "504"] {
        assert!(put_responses.get(status).is_some());
    }
}
