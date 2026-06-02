use axum::{
    extract::{MatchedPath, Request},
    http::{Method, StatusCode},
    middleware::Next,
    response::Response,
};

/// Sentry event message for server-error HTTP responses.
const SERVER_ERROR_MESSAGE: &str = "API route returned a server error";
/// Sentry tag for the matched HTTP route.
const HTTP_ROUTE_TAG: &str = "http.route";
/// Sentry tag for the HTTP request method.
const HTTP_METHOD_TAG: &str = "http.method";
/// Sentry tag for the HTTP response status code.
const HTTP_STATUS_CODE_TAG: &str = "http.status_code";

/// Sentry tag used to mark routes whose payloads can contain secrets.
pub const SENSITIVE_ENDPOINT_TAG: &str = "sensitive_endpoint";

/// Sentry tag value used for sensitive endpoint routes.
pub const SENSITIVE_ENDPOINT_TAG_VALUE: &str = "true";

/// Marks the current Sentry scope as belonging to a sensitive endpoint.
///
/// This middleware is intended to wrap route groups whose request or response
/// payloads can contain credentials or source data. The binary-level Sentry
/// scrubber uses the tag to remove payloads from captured events.
pub async fn mark_sensitive_sentry_scope(request: Request, next: Next) -> Response {
    sentry::configure_scope(|scope| {
        scope.set_tag(SENSITIVE_ENDPOINT_TAG, SENSITIVE_ENDPOINT_TAG_VALUE);
    });

    next.run(request).await
}

/// Captures Sentry events for HTTP 5xx responses.
///
/// Axum route errors are converted into JSON responses before they reach Tower,
/// so this middleware restores the Actix behavior of reporting server-error
/// responses to Sentry while keeping the request scope and sensitive-route tag.
pub(crate) async fn capture_server_errors(request: Request, next: Next) -> Response {
    let route = request_route(&request);
    let method = request.method().clone();

    let response = next.run(request).await;
    let status = response.status();

    if status.is_server_error() {
        capture_server_error(&method, &route, status);
    }

    response
}

/// Captures a server-error event with low-cardinality HTTP tags.
fn capture_server_error(method: &Method, route: &str, status: StatusCode) {
    sentry::with_scope(
        |scope| {
            scope.set_tag(HTTP_METHOD_TAG, method.as_str());
            scope.set_tag(HTTP_ROUTE_TAG, route);
            scope.set_tag(HTTP_STATUS_CODE_TAG, status.as_str());
        },
        || {
            sentry::capture_message(SERVER_ERROR_MESSAGE, sentry::Level::Error);
        },
    );
}

/// Returns the route pattern used for Sentry grouping.
fn request_route(request: &Request) -> String {
    request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| request.uri().path().to_owned(), |path| path.as_str().to_owned())
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::get,
    };
    use tower::Service;

    use super::*;

    #[test]
    fn capture_server_errors_records_sentry_event_for_5xx_response() {
        let events = sentry::test::with_captured_events(|| {
            run_request(
                Router::new()
                    .route("/fail", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
                    .layer(middleware::from_fn(capture_server_errors)),
                "/fail",
            );
        });

        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().expect("event should be captured");

        assert_eq!(event.message.as_deref(), Some(SERVER_ERROR_MESSAGE));
        assert_eq!(event.level, sentry::Level::Error);
        assert_eq!(event.tags.get(HTTP_METHOD_TAG).map(String::as_str), Some("GET"));
        assert_eq!(event.tags.get(HTTP_ROUTE_TAG).map(String::as_str), Some("/fail"));
        assert_eq!(event.tags.get(HTTP_STATUS_CODE_TAG).map(String::as_str), Some("500"));
    }

    #[test]
    fn capture_server_errors_keeps_sensitive_route_tag() {
        let events = sentry::test::with_captured_events(|| {
            run_request(
                Router::new()
                    .route("/sensitive", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
                    .layer(middleware::from_fn(mark_sensitive_sentry_scope))
                    .layer(middleware::from_fn(capture_server_errors)),
                "/sensitive",
            );
        });

        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().expect("event should be captured");

        assert_eq!(
            event.tags.get(SENSITIVE_ENDPOINT_TAG).map(String::as_str),
            Some(SENSITIVE_ENDPOINT_TAG_VALUE)
        );
    }

    #[test]
    fn capture_server_errors_ignores_non_5xx_response() {
        let events = sentry::test::with_captured_events(|| {
            run_request(
                Router::new()
                    .route("/missing", get(|| async { StatusCode::NOT_FOUND }))
                    .layer(middleware::from_fn(capture_server_errors)),
                "/missing",
            );
        });

        assert!(events.is_empty());
    }

    /// Runs a single request against an Axum router.
    fn run_request(mut router: Router, path: &str) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should be created");
        let request =
            HttpRequest::builder().uri(path).body(Body::empty()).expect("request should be built");

        runtime.block_on(async move {
            let response = router.call(request).await.expect("request should complete");

            assert!(response.status().is_server_error() || response.status().is_client_error());
        });
    }
}
