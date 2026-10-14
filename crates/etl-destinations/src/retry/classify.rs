//! Classification of failed destination calls by retryability.
//!
//! [`classified_error`] constructs a single error from a source implementing
//! [`ClassifiedError`], preserving aggregate sources and their member
//! classifications. This crate's `clippy.toml` rejects direct
//! [`etl::error::EtlError::with_source`] calls; conversions implemented by
//! `etl` remain outside that lint's scope.

#[cfg(any(feature = "bigquery", feature = "clickhouse", feature = "ducklake", test))]
use std::borrow::Cow;
use std::{error::Error, io, num::TryFromIntError, str::Utf8Error};

use etl::{
    config::ValidationError,
    error::{ErrorKind, EtlError, Retryability},
};

/// An error that knows whether the failed call may succeed if it runs again.
///
/// Implement this for every error type a destination attaches to an
/// [`EtlError`], and classify from the error alone. When the answer depends on
/// the call that failed, override it with [`EtlError::with_retryability`]
/// where that call handles the error.
pub(crate) trait ClassifiedError {
    /// Returns whether the same call may succeed if it runs again later.
    fn retryability(&self) -> Retryability;
}

/// Creates a single error with `source` and its retryability.
///
/// The source's classification takes precedence over the default for `kind`.
/// An aggregate source retains its members and their classifications. Calling
/// [`EtlError::with_retryability`] on the returned error overrides only the
/// wrapper's classification.
#[expect(
    clippy::disallowed_methods,
    reason = "classified constructors preserve the source and record its retryability"
)]
pub(crate) fn classified_error<E>(kind: ErrorKind, description: &'static str, source: E) -> EtlError
where
    E: ClassifiedError + Error + Send + Sync + 'static,
{
    let retryability = source.retryability();
    EtlError::from((kind, description)).with_source(source).with_retryability(retryability)
}

/// Creates a single error with diagnostic detail and a classified source.
///
/// Preserves the source and its classification as [`classified_error`] does.
#[cfg(any(feature = "bigquery", feature = "clickhouse", feature = "ducklake", test))]
#[expect(
    clippy::disallowed_methods,
    reason = "classified constructors preserve the source and record its retryability"
)]
pub(crate) fn classified_error_with_detail<E, D>(
    kind: ErrorKind,
    description: &'static str,
    detail: D,
    source: E,
) -> EtlError
where
    E: ClassifiedError + Error + Send + Sync + 'static,
    D: Into<Cow<'static, str>>,
{
    let retryability = source.retryability();
    EtlError::from((kind, description, detail)).with_source(source).with_retryability(retryability)
}

/// A wrapped [`EtlError`] keeps the classification made where it happened.
impl ClassifiedError for EtlError {
    fn retryability(&self) -> Retryability {
        EtlError::retryability(self)
    }
}

/// Lost connections and interrupted I/O may succeed when retried. Other I/O
/// errors, such as denied permissions or invalid input, repeat.
impl ClassifiedError for io::Error {
    fn retryability(&self) -> Retryability {
        match self.kind() {
            io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted => Retryability::Retryable,
            _ => Retryability::Permanent,
        }
    }
}

/// Invalid UTF-8 input fails the same way every time.
impl ClassifiedError for Utf8Error {
    fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}

/// A value that does not fit its target integer type fails the same way every
/// time.
impl ClassifiedError for TryFromIntError {
    fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}

/// Invalid configuration fails the same way until the configuration changes.
impl ClassifiedError for ValidationError {
    fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use etl::error::{ErrorKind, EtlError, Retryability};

    use crate::retry::{ClassifiedError, classified_error, classified_error_with_detail};

    /// Returns the ETL error stored as an error's source.
    fn etl_source(error: &EtlError) -> &EtlError {
        error.source().unwrap().downcast_ref::<EtlError>().unwrap()
    }

    /// Source classification overrides the kind's default retry policy.
    #[test]
    fn constructor_uses_source_classification_over_kind_default() {
        let retryable = classified_error(
            ErrorKind::DestinationQueryFailed,
            "Query failed",
            io::Error::from(io::ErrorKind::ConnectionRefused),
        );
        let permanent = classified_error_with_detail(
            ErrorKind::DestinationConnectionFailed,
            "Connect failed",
            "Operation: connect",
            io::Error::from(io::ErrorKind::PermissionDenied),
        );

        assert_eq!(retryable.retryability(), Retryability::Retryable);
        assert_eq!(permanent.retryability(), Retryability::Permanent);
    }

    /// Wrapping an aggregate and overriding the wrapper never reclassifies
    /// the aggregate's members.
    #[test]
    fn wrapping_aggregate_preserves_member_classifications() {
        let aggregate = EtlError::from(vec![
            EtlError::from((ErrorKind::DestinationTimeout, "Request timed out")),
            EtlError::from((ErrorKind::InvalidData, "Invalid row")),
        ]);
        let wrapped =
            classified_error(ErrorKind::DestinationConnectionFailed, "Batch failed", aggregate);
        assert_eq!(wrapped.retryability(), Retryability::Permanent);

        let aggregate = etl_source(&wrapped);
        let members = aggregate.errors().unwrap();
        assert_eq!(members[0].retryability(), Retryability::Retryable);
        assert_eq!(members[1].retryability(), Retryability::Permanent);

        let outer = classified_error_with_detail(
            ErrorKind::DestinationTimeout,
            "Write failed",
            "Operation: write",
            wrapped,
        );
        assert_eq!(outer.retryability(), Retryability::Permanent);
        let overridden = outer.with_retryability(Retryability::Retryable);
        assert_eq!(overridden.retryability(), Retryability::Retryable);

        let wrapped = etl_source(&overridden);
        assert_eq!(wrapped.retryability(), Retryability::Permanent);
        let aggregate = etl_source(wrapped);
        let members = aggregate.errors().unwrap();
        assert_eq!(members[0].retryability(), Retryability::Retryable);
        assert_eq!(members[1].retryability(), Retryability::Permanent);
    }

    /// Lost connections and timeouts are retryable; denied and invalid
    /// requests are not.
    #[test]
    fn io_errors_classify_by_kind() {
        for kind in [io::ErrorKind::ConnectionReset, io::ErrorKind::TimedOut] {
            assert_eq!(io::Error::from(kind).retryability(), Retryability::Retryable, "{kind}");
        }
        for kind in [io::ErrorKind::PermissionDenied, io::ErrorKind::InvalidInput] {
            assert_eq!(io::Error::from(kind).retryability(), Retryability::Permanent, "{kind}");
        }
    }
}
