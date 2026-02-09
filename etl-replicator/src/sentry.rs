use etl::error::EtlError;
use etl_config::Environment;
use secrecy::ExposeSecret;
use sentry::protocol::{Event, Exception, Stacktrace};
use sentry::types::Uuid;
use std::backtrace::BacktraceStatus;
use std::sync::Arc;
use tracing::info;

use crate::APP_VERSION_ENV_NAME;
use crate::config::load_replicator_config;
use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes Sentry error tracking for the replicator service.
pub fn init() -> ReplicatorResult<Option<sentry::ClientInitGuard>> {
    if let Ok(config) = load_replicator_config()
        && let Some(sentry_config) = &config.sentry
    {
        info!("initializing sentry with supplied dsn");

        let environment = Environment::load().map_err(ReplicatorError::config)?;
        let dsn = sentry_config
            .dsn
            .expose_secret()
            .parse()
            .map_err(ReplicatorError::config)?;

        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(dsn),
            environment: Some(environment.to_string().into()),
            integrations: vec![Arc::new(
                sentry::integrations::panic::PanicIntegration::new(),
            )],
            attach_stacktrace: true,
            ..Default::default()
        });

        let version = std::env::var(APP_VERSION_ENV_NAME);

        sentry::configure_scope(|scope| {
            scope.set_tag("service", "replicator");
            if let Ok(version) = version {
                scope.set_tag("version", version);
            }
        });

        return Ok(Some(guard));
    }

    info!("sentry not configured for replicator, skipping initialization");
    Ok(None)
}

/// Captures a [`ReplicatorError`] to Sentry and returns the event ID.
pub fn capture_error(err: &ReplicatorError) -> Uuid {
    let event = event_from_replicator_error(err);
    sentry::capture_event(event)
}

/// Converts a [`ReplicatorError`] into a Sentry [`Event`].
fn event_from_replicator_error(err: &ReplicatorError) -> Event<'static> {
    let mut exceptions = Vec::new();

    match err {
        ReplicatorError::Etl(etl_err) => {
            collect_etl_exceptions(etl_err, &mut exceptions);
        }
        _ => {
            let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
            while let Some(error) = current {
                exceptions.push(Exception {
                    ty: type_name_from_debug(error),
                    value: Some(error.to_string()),
                    ..Default::default()
                });
                current = error.source();
            }
            exceptions.reverse();
        }
    }

    if let Some(stacktrace) = find_and_parse_stacktrace(err)
        && let Some(exception) = exceptions.first_mut()
    {
        exception.stacktrace = Some(stacktrace);
    }

    Event {
        exception: exceptions.into(),
        level: sentry::Level::Error,
        ..Default::default()
    }
}

/// Recursively collects Sentry exceptions from an [`EtlError`].
fn collect_etl_exceptions(error: &EtlError, exceptions: &mut Vec<Exception>) {
    if let Some(errors) = error.errors() {
        for error in errors {
            collect_etl_exceptions(error, exceptions);
        }
    } else {
        exceptions.push(Exception {
            ty: format!("{:?}", error.kind()),
            value: Some(error.to_string()),
            ..Default::default()
        });
    }
}

/// Finds the first captured backtrace in a [`ReplicatorError`] and parses it.
fn find_and_parse_stacktrace(error: &ReplicatorError) -> Option<Stacktrace> {
    let backtrace = error.backtrace()?;
    if backtrace.status() != BacktraceStatus::Captured {
        return None;
    }

    sentry::integrations::backtrace::parse_stacktrace(&backtrace.to_string())
}

/// Extracts a type name from an error's debug representation.
fn type_name_from_debug(err: &dyn std::error::Error) -> String {
    let debug = format!("{err:?}");
    debug
        .split(['{', '(', ' '])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("Error")
        .to_string()
}
