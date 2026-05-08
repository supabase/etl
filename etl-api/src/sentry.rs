use std::sync::Arc;

use anyhow::Result;
use etl_api::{
    config::ApiConfig,
    sentry_scrubbing::{SENSITIVE_ENDPOINT_TAG, SENSITIVE_ENDPOINT_TAG_VALUE},
};
use etl_config::{Environment, load_config};
use secrecy::ExposeSecret;
use sentry::protocol::{Event, Request};
use tracing::debug;

/// Initializes Sentry error tracking and performance monitoring for the API.
///
/// Returns [`None`] when no Sentry configuration is provided.
pub(crate) fn init() -> Result<Option<sentry::ClientInitGuard>> {
    if let Ok(config) = load_config::<ApiConfig>()
        && let Some(sentry_config) = &config.sentry
    {
        debug!("initializing sentry with supplied dsn");

        let environment = Environment::load()?;
        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(sentry_config.dsn.expose_secret().parse()?),
            environment: Some(environment.to_string().into()),
            traces_sampler: Some(Arc::new(|ctx: &sentry::TransactionContext| {
                sample_trace_rate(ctx)
            })),
            before_send: Some(Arc::new(|event| Some(scrub_sensitive_event_payloads(event)))),
            max_request_body_size: sentry::MaxRequestBodySize::None,
            integrations: vec![Arc::new(sentry::integrations::panic::PanicIntegration::new())],
            attach_stacktrace: true,
            ..Default::default()
        });

        sentry::configure_scope(|scope| {
            scope.set_tag("service", "api");
        });

        return Ok(Some(guard));
    }

    debug!("sentry not configured for api, skipping initialization");

    Ok(None)
}

/// Computes the trace sampling rate based on endpoint path.
fn sample_trace_rate(ctx: &sentry::TransactionContext) -> f32 {
    let transaction_name = ctx.name();
    let endpoint = transaction_name
        .split_once(' ')
        .and_then(|(method, path)| {
            if path.starts_with('/') && method.chars().all(|c| c.is_ascii_uppercase()) {
                Some(path)
            } else {
                None
            }
        })
        .unwrap_or(transaction_name);

    match endpoint {
        "/metrics" | "/health_check" => 0.001,
        _ => 0.01,
    }
}

/// Removes request and response payload fields from events for sensitive API
/// routes.
fn scrub_sensitive_event_payloads(mut event: Event<'static>) -> Event<'static> {
    if !is_sensitive_event(&event) {
        return event;
    }

    if let Some(request) = event.request.as_mut() {
        scrub_request_payloads(request);
    }

    event.extra.retain(|key, _| !is_http_payload_key(key));
    event.contexts.retain(|key, _| !is_http_payload_key(key));

    event
}

/// Returns whether a Sentry event is for an endpoint that can carry secrets.
fn is_sensitive_event(event: &Event<'_>) -> bool {
    event
        .tags
        .get(SENSITIVE_ENDPOINT_TAG)
        .is_some_and(|value| value == SENSITIVE_ENDPOINT_TAG_VALUE)
}

/// Removes payload and credential-bearing request data.
fn scrub_request_payloads(request: &mut Request) {
    request.data = None;
    request.query_string = None;
    request.cookies = None;
    request.headers.clear();

    if let Some(url) = request.url.as_mut() {
        url.set_path("/[Filtered]");
        url.set_query(None);
        url.set_fragment(None);
    }
}

/// Returns whether an event key is likely to contain a request or response
/// payload.
fn is_http_payload_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '.'], "_");
    matches!(
        normalized.as_str(),
        "body"
            | "request"
            | "response"
            | "request_body"
            | "response_body"
            | "request_data"
            | "response_data"
            | "http_request_body"
            | "http_response_body"
            | "http_request_data"
            | "http_response_data"
    )
}

#[cfg(test)]
mod tests {
    use sentry::protocol::{Context, Map, Value};

    use super::*;

    #[test]
    fn scrub_sensitive_event_payloads_removes_payloads_for_tagged_route() {
        let mut event = sensitive_event(request_for_path("/v1/tenants-sources"));
        event.extra.insert("request_body".to_owned(), Value::String("secret".to_owned()));
        event.extra.insert("response.body".to_owned(), Value::String("secret".to_owned()));
        event.extra.insert("tenant_id".to_owned(), Value::String("project".to_owned()));
        event.contexts.insert("response".to_owned(), Context::Other(Map::default()));

        let event = scrub_sensitive_event_payloads(event);
        let request = event.request.expect("request should remain attached");

        let url = request.url.expect("url should remain attached");

        assert_eq!(url.path(), "/[Filtered]");
        assert_eq!(url.query(), None);
        assert_eq!(url.fragment(), None);
        assert_eq!(request.data, None);
        assert_eq!(request.query_string, None);
        assert_eq!(request.cookies, None);
        assert!(request.headers.is_empty());
        assert!(!event.extra.contains_key("request_body"));
        assert!(!event.extra.contains_key("response.body"));
        assert!(event.extra.contains_key("tenant_id"));
        assert!(!event.contexts.contains_key("response"));
    }

    #[test]
    fn scrub_sensitive_event_payloads_keeps_payloads_for_untagged_route() {
        let event = Event { request: Some(request_for_path("/v1/images")), ..Default::default() };

        let event = scrub_sensitive_event_payloads(event);
        let request = event.request.expect("request should remain attached");

        assert_eq!(request.data.as_deref(), Some("{\"password\":\"secret\"}"));
        assert_eq!(request.query_string.as_deref(), Some("token=secret"));
        assert_eq!(request.cookies.as_deref(), Some("session=secret"));
        assert!(request.headers.contains_key("authorization"));
    }

    #[test]
    fn sensitive_event_matches_sentry_scope_tag() {
        let event = sensitive_event(request_for_path("/v1/sources"));

        assert!(is_sensitive_event(&event));
    }

    #[test]
    fn sensitive_event_ignores_untagged_event() {
        let event = Event {
            request: Some(request_for_path("/v1/sources")),
            transaction: Some("POST /v1/sources".to_owned()),
            ..Default::default()
        };

        assert!(!is_sensitive_event(&event));
    }

    #[test]
    fn sample_trace_rate_keeps_default_rate_for_api_transactions() {
        let ctx = sentry::TransactionContext::new("POST /v1/tenants-sources", "http.server");

        assert_eq!(sample_trace_rate(&ctx), 0.01);
    }

    #[test]
    fn sample_trace_rate_keeps_low_rate_for_health_endpoints() {
        let ctx = sentry::TransactionContext::new("GET /health_check", "http.server");

        assert_eq!(sample_trace_rate(&ctx), 0.001);
    }

    fn sensitive_event(request: Request) -> Event<'static> {
        let mut event = Event { request: Some(request), ..Default::default() };
        event.tags.insert(SENSITIVE_ENDPOINT_TAG.to_owned(), SENSITIVE_ENDPOINT_TAG_VALUE.into());
        event
    }

    fn request_for_path(path: &str) -> Request {
        let mut headers = Map::default();
        headers.insert("authorization".to_owned(), "Bearer secret".to_owned());
        headers.insert("content-type".to_owned(), "application/json".to_owned());

        Request {
            url: Some(
                format!("https://api.example.com{path}?token=secret#secret").parse().unwrap(),
            ),
            method: Some("POST".to_owned()),
            data: Some("{\"password\":\"secret\"}".to_owned()),
            query_string: Some("token=secret".to_owned()),
            cookies: Some("session=secret".to_owned()),
            headers,
            ..Default::default()
        }
    }
}
