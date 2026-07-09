use etl_telemetry::tracing::init_test_tracing;
use reqwest::Url;

use crate::support::test_app::{spawn_test_app, spawn_test_app_without_k8s_client};

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_returns_200() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let client = reqwest::Client::new();

    // Act
    let response = client
        .get(local_test_url(&app.address, "/metrics"))
        .send()
        .await
        .expect("Failed to execute request.");

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_returns_200_without_k8s_client() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app_without_k8s_client().await;

    let client = reqwest::Client::new();

    // Act
    let response = client
        .get(local_test_url(&app.address, "/metrics"))
        .send()
        .await
        .expect("Failed to execute request.");

    // Assert
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_includes_http_request_metrics() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let client = reqwest::Client::new();

    // Act
    let health_response = client
        .get(local_test_url(&app.address, "/health_check"))
        .send()
        .await
        .expect("Failed to execute request.");
    let metrics = client
        .get(local_test_url(&app.address, "/metrics"))
        .send()
        .await
        .expect("Failed to execute request.")
        .text()
        .await
        .expect("Failed to read metrics response.");

    // Assert
    assert!(health_response.status().is_success());
    assert!(has_health_request_metric(&metrics, "http_requests_total{"));
    assert!(has_health_request_metric(&metrics, "http_requests_duration_seconds"));
}

#[tokio::test(flavor = "multi_thread")]
async fn unmatched_api_routes_use_bounded_metric_endpoint_label() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let client = reqwest::Client::new();

    // Act
    let missing_response = client
        .get(local_test_url(&app.address, "/v1/not-a-real-resource/secret-ish-id"))
        .send()
        .await
        .expect("Failed to execute request.");
    let metrics = client
        .get(local_test_url(&app.address, "/metrics"))
        .send()
        .await
        .expect("Failed to execute request.")
        .text()
        .await
        .expect("Failed to read metrics response.");

    // Assert
    assert_eq!(missing_response.status(), reqwest::StatusCode::NOT_FOUND);
    assert!(metrics.lines().any(|line| {
        line.starts_with("http_requests_total{")
            && line.contains(r#"endpoint="/v1/__unmatched__""#)
            && line.contains(r#"method="GET""#)
            && line.contains(r#"status="404""#)
    }));
    assert!(!metrics.contains("/v1/not-a-real-resource/secret-ish-id"));
}

fn has_health_request_metric(metrics: &str, prefix: &str) -> bool {
    metrics.lines().any(|line| {
        line.starts_with(prefix)
            && line.contains(r#"endpoint="/health_check""#)
            && line.contains(r#"method="GET""#)
            && line.contains(r#"status="200""#)
    })
}

fn local_test_url(address: &str, path: &str) -> Url {
    let address = Url::parse(address).expect("test app address should be a URL");

    assert_eq!(address.scheme(), "http");
    assert_eq!(address.host_str(), Some("127.0.0.1"));
    assert!(address.username().is_empty());
    assert!(address.password().is_none());
    assert_eq!(address.query(), None);

    let port = address.port().expect("test app address should include a port");

    Url::parse(&format!("http://127.0.0.1:{port}{path}"))
        .expect("test app endpoint URL should be valid")
}
