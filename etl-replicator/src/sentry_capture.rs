use sentry::protocol::{Event, Exception};
use sentry::types::Uuid;
use std::backtrace::BacktraceStatus;

use crate::error::ReplicatorError;

/// Captures a [`ReplicatorError`] to Sentry with proper backtrace handling.
pub fn capture_replicator_error(err: &ReplicatorError) -> Uuid {
    let event = event_from_replicator_error(err);
    sentry::capture_event(event)
}

/// Creates a Sentry event from a [`ReplicatorError`].
fn event_from_replicator_error(err: &ReplicatorError) -> Event<'static> {
    let mut exceptions = Vec::new();

    // Walk the error chain.
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        exceptions.push(Exception {
            ty: type_name_from_debug(e),
            value: Some(e.to_string()),
            ..Default::default()
        });
        current = e.source();
    }

    // Reverse so the root cause is first (Sentry convention).
    exceptions.reverse();

    // Try to parse and attach backtrace.
    let stacktrace = err.backtrace().and_then(|bt| {
        if bt.status() == BacktraceStatus::Captured {
            sentry::integrations::backtrace::parse_stacktrace(&bt.to_string())
        } else {
            None
        }
    });

    // Attach stacktrace to the most recent exception (last in the reversed list).
    if let Some(st) = stacktrace {
        if let Some(exc) = exceptions.last_mut() {
            exc.stacktrace = Some(st);
        }
    }

    Event {
        exception: exceptions.into(),
        level: sentry::Level::Error,
        ..Default::default()
    }
}

/// Extracts type name from Debug output.
fn type_name_from_debug(err: &dyn std::error::Error) -> String {
    let debug = format!("{err:?}");
    debug
        .split(|c| c == '{' || c == '(' || c == ' ')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("Error")
        .to_string()
}
