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
///
/// Loads DSN from configuration and sets up panic integration to automatically
/// capture panics. Tags all events with `service=replicator` and optionally
/// the app version from environment. Returns [`None`] if no Sentry config is
/// present, allowing the replicator to run without error tracking.
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

        // We load the version of the replicator which is specified via environment variable.
        let version = std::env::var(APP_VERSION_ENV_NAME);

        // Set service tag to differentiate replicator from other services
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
///
/// Builds a Sentry event with exception chain and stacktrace attached. For
/// [`EtlError`] Many variants, expands all contained errors into separate
/// exceptions. Searches recursively for the first captured backtrace.
pub fn capture_error(err: &ReplicatorError) -> Uuid {
    let event = event_from_replicator_error(err);
    sentry::capture_event(event)
}

/// Converts a [`ReplicatorError`] into a Sentry [`Event`].
///
/// For ETL errors, expands Many variants and uses [`ErrorKind`] as the
/// exception type. For other errors, walks the source chain and reverses
/// to put root cause first. Attaches the first captured backtrace to the
/// last exception in the list.
fn event_from_replicator_error(err: &ReplicatorError) -> Event<'static> {
    let mut exceptions = Vec::new();

    // Build exception list based on error type.
    match err {
        ReplicatorError::Etl(etl_err) => {
            // For ETL errors, collect all errors (expanding Many variants).
            // These are kept in original order since Many errors are parallel, not chained.
            collect_etl_exceptions(etl_err, &mut exceptions);
        }
        _ => {
            // For non-ETL errors, walk the standard error chain (reversed: root cause first).
            let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
            while let Some(e) = current {
                exceptions.push(Exception {
                    ty: type_name_from_debug(e),
                    value: Some(e.to_string()),
                    ..Default::default()
                });
                current = e.source();
            }
            // Reverse so root cause is first (Sentry convention for error chains).
            exceptions.reverse();
        }
    }

    // Attach the first captured backtrace to the first exception (they correspond).
    if let Some(stacktrace) = find_first_captured_backtrace(err) {
        if let Some(exception) = exceptions.first_mut() {
            exception.stacktrace = Some(stacktrace);
        }
    }

    Event {
        exception: exceptions.into(),
        level: sentry::Level::Error,
        ..Default::default()
    }
}

/// Recursively collects Sentry exceptions from an [`EtlError`].
///
/// For Many variants, recurses into each contained error. For Single variants,
/// creates an exception with [`ErrorKind`] as the type name. Preserves original
/// order since Many errors represent parallel failures, not a causal chain.
fn collect_etl_exceptions(error: &EtlError, exceptions: &mut Vec<Exception>) {
    if let Some(errors) = error.errors() {
        // Many variant: collect each individual error.
        for error in errors {
            collect_etl_exceptions(error, exceptions);
        }
    } else {
        // Single variant: add as exception with ErrorKind as type.
        exceptions.push(Exception {
            ty: format!("{:?}", error.kind()),
            value: Some(error.to_string()),
            ..Default::default()
        });
    }
}

/// Finds the first captured backtrace in a [`ReplicatorError`] and parses it for Sentry.
///
/// Delegates to [`ReplicatorError::backtrace`] which handles recursion for ETL
/// Many variants. Returns [`None`] if no backtrace was captured or parsing fails.
fn find_first_captured_backtrace(error: &ReplicatorError) -> Option<Stacktrace> {
    let backtrace = error.backtrace()?;
    if backtrace.status() != BacktraceStatus::Captured {
        return None;
    }

    sentry::integrations::backtrace::parse_stacktrace(&backtrace.to_string())
}

/// Extracts the type name from an error's Debug representation.
///
/// Parses the Debug output to find the type name before any `{`, `(`, or space.
/// Used for non-ETL errors where we cannot access the concrete type. Falls back
/// to `"Error"` if parsing fails.
fn type_name_from_debug(err: &dyn std::error::Error) -> String {
    let debug = format!("{err:?}");
    debug
        .split(|c| c == '{' || c == '(' || c == ' ')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("Error")
        .to_string()
}
