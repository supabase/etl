//! Classification of failed destination calls by retryability.

use std::{error::Error, io, num::TryFromIntError, str::Utf8Error};

use etl::{
    config::ValidationError,
    error::{EtlError, Retryability},
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

/// Attaches classified sources to an [`EtlError`].
pub(crate) trait EtlErrorExt {
    /// Attaches `source` and records its [`ClassifiedError::retryability`].
    ///
    /// The source's answer replaces the error's current retryability. Call
    /// [`EtlError::with_retryability`] afterwards to override it.
    fn caused_by<E>(self, source: E) -> EtlError
    where
        E: ClassifiedError + Error + Send + Sync + 'static;
}

impl EtlErrorExt for EtlError {
    fn caused_by<E>(self, source: E) -> EtlError
    where
        E: ClassifiedError + Error + Send + Sync + 'static,
    {
        let retryability = source.retryability();
        self.with_source(source).with_retryability(retryability)
    }
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
    use std::io;

    use etl::error::{ErrorKind, EtlError, Retryability};

    use crate::retry::{ClassifiedError, EtlErrorExt};

    /// Attaching a source records the source's answer, whatever the kind's
    /// default, and a wrapped error keeps the answer of the error it wraps.
    #[test]
    fn caused_by_records_the_source_classification() {
        let refused = io::Error::from(io::ErrorKind::ConnectionRefused);
        let denied = io::Error::from(io::ErrorKind::PermissionDenied);

        let retryable =
            EtlError::from((ErrorKind::DestinationQueryFailed, "Query failed")).caused_by(refused);
        let permanent = EtlError::from((ErrorKind::DestinationConnectionFailed, "Connect failed"))
            .caused_by(denied);
        let wrapped = EtlError::from((ErrorKind::DestinationError, "Batch failed"))
            .caused_by(retryable.clone());

        assert_eq!(retryable.retryability(), Retryability::Retryable);
        assert_eq!(permanent.retryability(), Retryability::Permanent);
        assert_eq!(wrapped.retryability(), Retryability::Retryable);
        assert!(std::error::Error::source(&permanent).is_some());
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
