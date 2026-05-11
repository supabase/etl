use actix_web::{
    Error,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
};

/// Sentry tag used to mark routes whose payloads can contain secrets.
pub const SENSITIVE_ENDPOINT_TAG: &str = "sensitive_endpoint";

/// Sentry tag value used for sensitive endpoint routes.
pub const SENSITIVE_ENDPOINT_TAG_VALUE: &str = "true";

/// Marks the current Sentry scope as belonging to a sensitive endpoint.
///
/// This middleware is intended to wrap route groups whose request or response
/// payloads can contain credentials or source data. The binary-level Sentry
/// scrubber uses the tag to remove payloads from captured events.
pub async fn mark_sensitive_sentry_scope<B>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error>
where
    B: MessageBody + 'static,
{
    // The Actix Sentry integration creates a request-local hub before calling
    // downstream middleware. Tag that hub directly so returned handler errors
    // are still marked when the outer Sentry middleware captures them.
    sentry::configure_scope(|scope| {
        scope.set_tag(SENSITIVE_ENDPOINT_TAG, SENSITIVE_ENDPOINT_TAG_VALUE);
    });

    next.call(req).await
}
