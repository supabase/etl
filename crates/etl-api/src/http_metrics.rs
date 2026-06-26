//! HTTP request metrics middleware.

use std::{sync::Once, time::Instant};

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use metrics::{Unit, counter, describe_counter, describe_histogram, histogram};

static REGISTER_HTTP_METRICS: Once = Once::new();

/// Total request count metric name.
const HTTP_REQUESTS_TOTAL: &str = "http_requests_total";
/// Request duration metric name.
const HTTP_REQUESTS_DURATION_SECONDS: &str = "http_requests_duration_seconds";
/// Request endpoint label key.
const ENDPOINT_LABEL: &str = "endpoint";
/// Request method label key.
const METHOD_LABEL: &str = "method";
/// Response status label key.
const STATUS_LABEL: &str = "status";
/// Bounded label for unmatched API requests.
const UNMATCHED_API_ENDPOINT: &str = "/v1/__unmatched__";
/// Bounded label for unmatched non-API requests.
const UNMATCHED_ENDPOINT: &str = "__unmatched__";

/// Records request count and latency metrics for each HTTP response.
pub(crate) async fn record_http_metrics(request: Request, next: Next) -> Response {
    register_http_metrics();

    let started_at = Instant::now();
    let endpoint = request_endpoint(&request);
    let method = request.method().as_str().to_owned();

    let response = next.run(request).await;
    let status = response.status().as_str().to_owned();
    let duration = started_at.elapsed().as_secs_f64();
    let labels = vec![(ENDPOINT_LABEL, endpoint), (METHOD_LABEL, method), (STATUS_LABEL, status)];

    histogram!(HTTP_REQUESTS_DURATION_SECONDS, &labels).record(duration);
    counter!(HTTP_REQUESTS_TOTAL, &labels).increment(1);

    response
}

/// Registers Prometheus descriptions for HTTP request metrics once.
fn register_http_metrics() {
    REGISTER_HTTP_METRICS.call_once(|| {
        describe_counter!(HTTP_REQUESTS_TOTAL, Unit::Count, "Total number of HTTP requests");
        describe_histogram!(
            HTTP_REQUESTS_DURATION_SECONDS,
            Unit::Seconds,
            "HTTP request duration in seconds for all requests"
        );
    });
}

/// Returns the low-cardinality route pattern for a request.
fn request_endpoint(request: &Request) -> String {
    request.extensions().get::<MatchedPath>().map_or_else(
        || {
            let path = request.uri().path();
            if path == "/v1" || path.starts_with("/v1/") {
                UNMATCHED_API_ENDPOINT.to_owned()
            } else {
                UNMATCHED_ENDPOINT.to_owned()
            }
        },
        |path| path.as_str().to_owned(),
    )
}
