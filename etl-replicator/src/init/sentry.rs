use ::sentry::ClientInitGuard;
use etl_config::Environment;
use etl_config::shared::ReplicatorConfig;
use secrecy::ExposeSecret;
use std::sync::Arc;
use tracing::debug;

use crate::APP_VERSION_ENV_NAME;
use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes Sentry error tracking for the replicator service.
pub fn init(config: &ReplicatorConfig) -> ReplicatorResult<Option<ClientInitGuard>> {
    let Some(sentry_config) = &config.sentry else {
        debug!("sentry not configured for replicator, skipping initialization");

        return Ok(None);
    };

    debug!("initializing sentry with supplied dsn");

    let environment = Environment::load().map_err(ReplicatorError::config)?;
    let dsn = sentry_config
        .dsn
        .expose_secret()
        .parse()
        .map_err(ReplicatorError::config)?;

    let guard = ::sentry::init(::sentry::ClientOptions {
        dsn: Some(dsn),
        environment: Some(environment.to_string().into()),
        integrations: vec![Arc::new(
            ::sentry::integrations::panic::PanicIntegration::new(),
        )],
        attach_stacktrace: true,
        ..Default::default()
    });

    sentry::configure_scope(|scope| {
        scope.set_tag("service", "replicator");

        let version = std::env::var(APP_VERSION_ENV_NAME);
        if let Ok(version) = version {
            scope.set_tag("version", version);
        }
    });

    Ok(Some(guard))
}
