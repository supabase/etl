use etl_telemetry::tracing::init_test_tracing;
use reqwest::Url;

use crate::support::test_app::spawn_test_app;

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
