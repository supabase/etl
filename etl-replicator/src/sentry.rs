use etl::error::EtlError;
use etl::types::TableId;
use sentry::protocol::{Event, Exception, Stacktrace, Value};
use sentry::types::Uuid;
use serde_json::json;
use std::backtrace::BacktraceStatus;

use crate::error::ReplicatorError;

/// Sets the destination tag on the current Sentry scope.
///
/// This tag is applied at the scope level so all captured errors inherit
/// the configured destination type.
pub fn set_destination_tag(destination: &'static str) {
    sentry::configure_scope(|scope| {
        scope.set_tag("destination", destination);
    });
}

/// Captures a [`ReplicatorError`] to Sentry and returns the event ID.
pub fn capture_error(err: &ReplicatorError) -> Uuid {
    let event = event_from_replicator_error(err);
    sentry::capture_event(event)
}

/// Captures a stored table replication [`EtlError`] to Sentry.
pub fn capture_table_error(table_id: TableId, err: &EtlError) {
    sentry::with_scope(
        |scope| {
            scope.set_tag("table_id", table_id.0.to_string());
        },
        || sentry::capture_event(event_from_etl_error(err)),
    );
}

/// Converts a [`ReplicatorError`] into a Sentry [`Event`].
fn event_from_replicator_error(err: &ReplicatorError) -> Event<'static> {
    if let ReplicatorError::Etl(etl_err) = err {
        return event_from_etl_error(etl_err);
    }

    let mut event = Event {
        level: sentry::Level::Error,
        ..Default::default()
    };
    let mut exceptions = Vec::new();

    collect_standard_exception_chain(err, &mut exceptions);

    if let Some(stacktrace) = find_and_parse_backtrace(err.backtrace())
        && let Some(exception) = exceptions.first_mut()
    {
        exception.stacktrace = Some(stacktrace);
    }

    event.exception = exceptions.into();
    event
}

/// Converts an [`EtlError`] into a Sentry [`Event`].
fn event_from_etl_error(err: &EtlError) -> Event<'static> {
    let mut event = Event {
        level: sentry::Level::Error,
        ..Default::default()
    };
    let mut exceptions = Vec::new();

    if err.errors().is_some() {
        let leaf_count = count_leaf_errors(err);
        exceptions.push(Exception {
            ty: "Many".to_string(),
            value: Some(format!("{leaf_count} errors occurred")),
            ..Default::default()
        });
        event.extra.insert(
            "etl_error_tree".to_string(),
            etl_error_tree_to_serde_value(err),
        );
    } else {
        collect_single_etl_exception_chain(err, &mut exceptions);
    }

    if let Some(stacktrace) = find_and_parse_backtrace(err.backtrace())
        && let Some(exception) = exceptions.first_mut()
    {
        exception.stacktrace = Some(stacktrace);
    }

    event.exception = exceptions.into();
    event
}

/// Collects a single ETL error and its source chain into sentry exceptions.
fn collect_single_etl_exception_chain(error: &EtlError, exceptions: &mut Vec<Exception>) {
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
        json!({
            "type": "Many",
            "children": children.iter().map(etl_error_tree_to_serde_value).collect::<Vec<_>>(),
        })
    } else {
        json!({
            "type": format!("{:?}", error.kind()),
            "description": error.description(),
            "detail": error.detail(),
        })
    }
}

/// Parses a captured backtrace into a Sentry [`Stacktrace`].
fn find_and_parse_backtrace(backtrace: Option<&std::backtrace::Backtrace>) -> Option<Stacktrace> {
    let backtrace = backtrace?;
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
