use etl::error::EtlError;
use etl_config::Environment;
use secrecy::ExposeSecret;
use sentry::protocol::{Event, Exception, Stacktrace, Value};
use sentry::types::Uuid;
use serde_json::json;
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
    let mut event = Event {
        level: sentry::Level::Error,
        ..Default::default()
    };
    let mut exceptions = Vec::new();

    match err {
        ReplicatorError::Etl(etl_err) => {
            collect_etl_exceptions(etl_err, &mut exceptions);

            if etl_err.errors().is_some() {
                let leaf_count = count_leaf_errors(etl_err);
                event.extra.insert(
                    "etl_error_leaf_count".to_string(),
                    Value::from(leaf_count as u64),
                );
                event.extra.insert(
                    "etl_error_tree".to_string(),
                    etl_error_tree_to_serde_value(etl_err),
                );
            }
        }
        _ => collect_standard_exception_chain(err, &mut exceptions),
    }

    if let Some(stacktrace) = find_and_parse_stacktrace(err)
        && let Some(exception) = exceptions.first_mut()
    {
        exception.stacktrace = Some(stacktrace);
    }

    event.exception = exceptions.into();
    event
}

/// Recursively collects leaf ETL errors as sentry exceptions.
fn collect_etl_exceptions(error: &EtlError, exceptions: &mut Vec<Exception>) {
    if let Some(children) = error.errors() {
        for child in children {
            collect_etl_exceptions(child, exceptions);
        }
        return;
    }

    exceptions.push(Exception {
        ty: format!("{:?}", error.kind()),
        value: Some(format_etl_sentry_value(error)),
        ..Default::default()
    });

    let mut source = std::error::Error::source(error);
    while let Some(current) = source {
        exceptions.push(Exception {
            ty: type_name_from_debug(current),
            value: Some(current.to_string()),
            ..Default::default()
        });
        source = current.source();
    }
}

/// Collects a standard source chain into sentry exceptions.
fn collect_standard_exception_chain(
    root: &(dyn std::error::Error + 'static),
    exceptions: &mut Vec<Exception>,
) {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(root);
    while let Some(error) = current {
        exceptions.push(Exception {
            ty: type_name_from_debug(error),
            value: Some(error.to_string()),
            ..Default::default()
        });
        current = error.source();
    }

    // We reverse because Sentry will show as top-level the last element of the list, and since we
    // want the first error in the source chain to be on top, we have to reverse everything.
    exceptions.reverse();
}

/// Formats a single ETL error for sentry aggregation.
///
/// The type is carried in the exception `ty`; this value contains only
/// `description` or `description: detail`.
fn format_etl_sentry_value(error: &EtlError) -> String {
    let description = error.description().unwrap_or("etl error");
    match error.detail() {
        Some(detail) if !detail.trim().is_empty() => {
            format!(
                "{description}: {}",
                detail.lines().collect::<Vec<_>>().join(" ")
            )
        }
        _ => description.to_string(),
    }
}

/// Counts total leaf errors contained in an ETL error tree.
fn count_leaf_errors(error: &EtlError) -> usize {
    if let Some(children) = error.errors() {
        children.iter().map(count_leaf_errors).sum()
    } else {
        1
    }
}

/// Converts an ETL error tree into a serde value which will be used as metadata for sentry.
fn etl_error_tree_to_serde_value(error: &EtlError) -> Value {
    if let Some(children) = error.errors() {
        Value::from(json!({
            "type": "Many",
            "message": format!("{error}"),
            "total_leaf_errors": count_leaf_errors(error),
            "children": children.iter().map(etl_error_tree_to_serde_value).collect::<Vec<_>>(),
        }))
    } else {
        Value::from(json!({
            "type": format!("{:?}", error.kind()),
            "message": format_etl_sentry_value(error),
        }))
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
