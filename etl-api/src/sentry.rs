use anyhow::Result;
use etl_api::config::ApiConfig;
use etl_config::{Environment, load_config};
use secrecy::ExposeSecret;
use std::sync::Arc;
use tracing::info;

/// Initializes Sentry error tracking and performance monitoring for the API.
///
/// Returns [`None`] when no Sentry configuration is provided.
pub fn init() -> Result<Option<sentry::ClientInitGuard>> {
    if let Ok(config) = load_config::<ApiConfig>()
        && let Some(sentry_config) = &config.sentry
    {
        info!("initializing sentry with supplied dsn");

        let environment = Environment::load()?;
        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(sentry_config.dsn.expose_secret().parse()?),
            environment: Some(environment.to_string().into()),
            traces_sampler: Some(Arc::new(|ctx: &sentry::TransactionContext| {
                sample_trace_rate(ctx)
            })),
            max_request_body_size: sentry::MaxRequestBodySize::Always,
            integrations: vec![Arc::new(
                sentry::integrations::panic::PanicIntegration::new(),
            )],
            attach_stacktrace: true,
            ..Default::default()
        });

        sentry::configure_scope(|scope| {
            scope.set_tag("service", "api");
        });

        return Ok(Some(guard));
    }

    info!("sentry not configured for api, skipping initialization");
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
