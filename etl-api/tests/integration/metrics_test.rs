use etl_telemetry::init_test_tracing;

use crate::common::test_app::spawn_test_app;

#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_works() {
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
