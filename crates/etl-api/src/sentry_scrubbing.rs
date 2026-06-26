use std::{any::type_name, error::Error};

use axum::{
    extract::{MatchedPath, Request},
    http::{Method, StatusCode},
    middleware::Next,
    response::Response,
};
use sentry::protocol::{Event, Exception};

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

/// Sentry exception type used for lower-level source errors.
const SOURCE_EXCEPTION_TYPE: &str = "source";

/// Internal route error details to report for server-error responses.
#[derive(Clone, Debug)]
pub(crate) struct ServerErrorReport {
    /// Exception chain ordered for Sentry display.
    exceptions: Vec<ServerErrorException>,
}

/// A preformatted Sentry exception extracted from a route error.
#[derive(Clone, Debug)]
struct ServerErrorException {
    /// Exception type used for grouping and display.
    ty: String,
    /// Exception value shown in Sentry.
    value: String,
}

impl ServerErrorReport {
    /// Builds a report from an error and its source chain.
    pub(crate) fn from_error<E>(error: &E) -> Self
    where
        E: Error + 'static,
    {
        let mut exceptions = Vec::from([ServerErrorException {
            ty: type_name::<E>().to_owned(),
            value: error.to_string(),
        }]);
        let mut current = error.source();

        while let Some(error) = current {
            exceptions.push(ServerErrorException {
                ty: SOURCE_EXCEPTION_TYPE.to_owned(),
                value: error.to_string(),
            });
            current = error.source();
        }

        // Sentry treats the last exception as the top-level error. Keep the
        // route error on top while still preserving the lower-level causes.
        exceptions.reverse();

        Self { exceptions }
    }

    /// Converts the report into Sentry protocol exceptions.
    fn to_sentry_exceptions(&self) -> Vec<Exception> {
        self.exceptions
            .iter()
            .map(|exception| Exception {
                ty: exception.ty.clone(),
                value: Some(exception.value.clone()),
                ..Default::default()
            })
            .collect()
    }
}

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

/// Captures Sentry events for internal HTTP 500 responses.
///
/// Axum route errors are converted into JSON responses before they reach Tower,
/// so this middleware restores the Actix behavior of reporting server-error
/// responses to Sentry while keeping the request scope and sensitive-route tag.
pub(crate) async fn capture_server_errors(request: Request, next: Next) -> Response {
    let route = request_route(&request);
    let method = request.method().clone();

    let response = next.run(request).await;
    let status = response.status();

    if status == StatusCode::INTERNAL_SERVER_ERROR {
        capture_server_error(
            &method,
            &route,
            status,
            response.extensions().get::<ServerErrorReport>(),
        );
    }

    response
}

/// Captures a server-error event with low-cardinality HTTP tags.
fn capture_server_error(
    method: &Method,
    route: &str,
    status: StatusCode,
    report: Option<&ServerErrorReport>,
) {
    sentry::with_scope(
        |scope| {
            scope.set_tag(HTTP_METHOD_TAG, method.as_str());
            scope.set_tag(HTTP_ROUTE_TAG, route);
            scope.set_tag(HTTP_STATUS_CODE_TAG, status.as_str());
        },
        || {
            if let Some(report) = report {
                sentry::capture_event(event_from_server_error_report(report));
            } else {
                sentry::capture_message(SERVER_ERROR_MESSAGE, sentry::Level::Error);
            }
        },
    );
}

/// Builds a Sentry event from internal route error details.
fn event_from_server_error_report(report: &ServerErrorReport) -> Event<'static> {
    Event {
        exception: report.to_sentry_exceptions().into(),
        level: sentry::Level::Error,
        ..Default::default()
    }
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
        response::IntoResponse,
        routing::get,
    };
    use thiserror::Error;
    use tower::Service;

    use super::*;

    #[test]
    fn capture_server_errors_records_sentry_event_for_internal_server_error() {
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
    fn capture_server_errors_records_route_error_exception_chain() {
        let events = sentry::test::with_captured_events(|| {
            run_request(
                Router::new()
                    .route(
                        "/fail",
                        get(|| async { Result::<StatusCode, TestRouteError>::Err(test_error()) }),
                    )
                    .layer(middleware::from_fn(capture_server_errors)),
                "/fail",
            );
        });

        assert_eq!(events.len(), 1);
        let event = events.into_iter().next().expect("event should be captured");
        let exceptions = event.exception.values;

        assert_eq!(event.message, None);
        assert_eq!(event.level, sentry::Level::Error);
        assert_eq!(exceptions.len(), 2);

        let source = exceptions.first().expect("source exception should be present");
        assert_eq!(source.ty, SOURCE_EXCEPTION_TYPE);
        assert_eq!(source.value.as_deref(), Some("Database query failed"));

        let route_error = exceptions.last().expect("route exception should be present");
        assert_eq!(route_error.ty, type_name::<TestRouteError>());
        assert_eq!(
            route_error.value.as_deref(),
            Some("Route failed while reading pipeline: Database query failed")
        );
        assert_eq!(event.tags.get(HTTP_ROUTE_TAG).map(String::as_str), Some("/fail"));
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
    fn capture_server_errors_ignores_non_internal_server_error_response() {
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

    #[test]
    fn capture_server_errors_ignores_upstream_failure_response() {
        let events = sentry::test::with_captured_events(|| {
            run_request(
                Router::new()
                    .route("/upstream", get(|| async { StatusCode::BAD_GATEWAY }))
                    .layer(middleware::from_fn(capture_server_errors)),
                "/upstream",
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

    /// Returns a test route error with a source chain.
    fn test_error() -> TestRouteError {
        TestRouteError { source: TestSourceError }
    }

    /// Test error used to model an API route failure.
    #[derive(Debug, Error)]
    #[error("Route failed while reading pipeline: {source}")]
    struct TestRouteError {
        /// Lower-level source error.
        #[source]
        source: TestSourceError,
    }

    impl IntoResponse for TestRouteError {
        fn into_response(self) -> Response {
            crate::routes::error_response_with_internal_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_owned(),
                &self,
            )
        }
    }

    /// Test source error used to model a database failure.
    #[derive(Debug, Error)]
    #[error("Database query failed")]
    struct TestSourceError;
}
