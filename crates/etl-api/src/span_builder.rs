use std::time::Duration;

use axum::http::{Request, Response};
use tower_http::classify::ServerErrorsFailureClass;
use tracing::{Span, debug, error};

/// Creates the root tracing span for an API request.
///
/// The span records the `tenant_id` header as the `project` field when the
/// header is present.
pub fn make_span<B>(request: &Request<B>) -> Span {
    let span = tracing::debug_span!(
        "request",
        method = %request.method(),
        uri = %request.uri(),
        version = ?request.version(),
        project = tracing::field::Empty,
        status = tracing::field::Empty,
    );

    if let Some(project) = request.headers().get("tenant_id") {
        // We convert lossily to a string to be able to read at least part of the
        // project ref in case of invalid UTF-8. This is useful for debugging.
        let project = String::from_utf8_lossy(project.as_bytes());
        span.record("project", project.as_ref());
    }

    span
}

/// Emits the request-start tracing event.
pub fn on_request<B>(request: &Request<B>, span: &Span) {
    let _enter = span.enter();
    debug!(
        method = %request.method(),
        "http request received",
    );
}

/// Emits the request-success tracing event.
pub fn on_response<B>(response: &Response<B>, _latency: Duration, span: &Span) {
    span.record("status", response.status().as_u16());

    if response.status().is_server_error() {
        return;
    }

    let _enter = span.enter();
    debug!("http request completed successfully");
}

/// Emits the request-failure tracing event.
pub fn on_failure(error: ServerErrorsFailureClass, _latency: Duration, span: &Span) {
    let _enter = span.enter();
    error!(
        error = %error,
        "http request failed",
    );
}
