use etl_telemetry::tracing::init_test_tracing;

use crate::support::test_app::spawn_test_app;

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_returns_200() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let client = reqwest::Client::new();

    // Act
    let response = client
        .get(format!("{}/metrics", app.address))
        .send()
        .await
        .expect("Failed to execute request.");

    // Assert
    assert!(response.status().is_success());
}
