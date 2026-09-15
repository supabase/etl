//! Hotpath instrumentation for matched API routes.

use axum::{
    Router,
    extract::{MatchedPath, Request},
    http::Method,
    middleware::{self, Next},
    response::Response,
};
use tower::{Layer, ServiceExt};

/// Instruments the finished router outside its authentication and tracing
/// layers.
pub(crate) fn profile_router(router: Router) -> Router {
    router.layer(middleware::from_fn(profile_request))
}

/// Profiles only bounded route templates and standard HTTP methods.
///
/// Hotpath falls back to raw paths for unmatched requests. Skipping those
/// prevents arbitrary URLs or method tokens from becoming profiler labels.
/// ETL's regular HTTP metrics continue counting those requests.
async fn profile_request(request: Request, next: Next) -> Response {
    let matched = request.extensions().get::<MatchedPath>().is_some();
    let standard_method = matches!(
        *request.method(),
        Method::GET
            | Method::HEAD
            | Method::POST
            | Method::PUT
            | Method::DELETE
            | Method::CONNECT
            | Method::OPTIONS
            | Method::TRACE
            | Method::PATCH
    );
    if matched && standard_method {
        let response = hotpath::AxumLayer::new().layer(next).oneshot(request).await;
        match response {
            Ok(response) => response,
            Err(error) => match error {},
        }
    } else {
        next.run(request).await
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{
        Extension, Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        middleware,
        routing::get,
    };
    use etl_telemetry::{metrics::init_metrics_handle, profiling};
    use tower::ServiceExt;

    use crate::{
        http_metrics::record_http_metrics, profiling::profile_router, routes::metrics::metrics,
    };

    /// Exercises route, function, and contended-lock metrics through the API
    /// handler.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn existing_endpoint_exports_hotpath_and_bounds_route_labels() {
        let _profiler = profiling::init().unwrap();
        hotpath::tokio_runtime!();
        let handle = init_metrics_handle().unwrap();
        let mutex =
            Arc::new(hotpath::mutex!(tokio::sync::Mutex::new(()), label = "api_profile_test_lock"));
        let app = profile_router(
            Router::new()
                .nest(
                    "/v1",
                    Router::new().route(
                        "/items/{item_id}",
                        get(move || {
                            let mutex = Arc::clone(&mutex);
                            async move {
                                let _guard = mutex.lock().await;
                                measured_work().await;
                                StatusCode::OK
                            }
                        }),
                    ),
                )
                .route("/denied", get(|| async { StatusCode::UNAUTHORIZED }))
                .route("/failed", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
                .route("/metrics", get(metrics))
                .layer(Extension(handle))
                .layer(middleware::from_fn(record_http_metrics)),
        );
        let mut requests = tokio::task::JoinSet::new();
        for index in 0..8 {
            let app = app.clone();
            requests.spawn(async move {
                app.oneshot(
                    Request::builder()
                        .uri(format!("/v1/items/fake-{index}?value=placeholder-query"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
            });
        }
        while let Some(response) = requests.join_next().await {
            assert_eq!(response.unwrap(), StatusCode::OK);
        }
        for (path, status) in [
            ("/denied", StatusCode::UNAUTHORIZED),
            ("/failed", StatusCode::INTERNAL_SERVER_ERROR),
            ("/v1/placeholder-unmatched-path", StatusCode::NOT_FOUND),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), status);
        }

        let snapshot = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let response = app
                    .clone()
                    .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(
                    response.headers()[axum::http::header::CONTENT_TYPE],
                    "text/plain; version=0.0.4; charset=utf-8"
                );
                let snapshot = String::from_utf8(
                    to_bytes(response.into_body(), 4 * 1024 * 1024).await.unwrap().to_vec(),
                )
                .unwrap();
                if snapshot.contains("hotpath_mutex_wait_seconds_sum")
                    && snapshot.contains("hotpath_function_duration_seconds_sum")
                    && snapshot.contains(
                        "hotpath_server_requests_total{route=\"GET /v1/items/{item_id}\"} 8",
                    )
                {
                    break snapshot;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();

        assert!(snapshot.contains("http_requests_total{"));
        assert!(snapshot.contains("etl_hotpath_exporter_up 1"));
        assert!(
            snapshot
                .contains("hotpath_server_responses_total{route=\"GET /denied\",class=\"4xx\"} 1")
        );
        assert!(
            snapshot
                .contains("hotpath_server_responses_total{route=\"GET /failed\",class=\"5xx\"} 1")
        );
        assert!(!snapshot.contains("placeholder-unmatched-path"));
        assert!(!snapshot.contains("placeholder-query"));
        assert!(!snapshot.contains("fake-"));
        for metric in [
            "hotpath_mutex_wait_seconds_sum",
            "hotpath_mutex_acquire_seconds_sum",
            "hotpath_server_duration_seconds_sum",
        ] {
            let line = snapshot.lines().find(|line| line.starts_with(metric)).unwrap();
            let value = line.rsplit_once(' ').unwrap().1.parse::<f64>().unwrap();
            assert!(value > 0.0);
            eprintln!("{line}");
        }
    }

    /// Creates a bounded amount of awaited work under the test lock.
    #[hotpath::measure]
    async fn measured_work() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
