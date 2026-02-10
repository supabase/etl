use crate::error::{ErrorKind, EtlError};

/// Retry behavior for a classified error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryDirective {
    /// The operation can be retried automatically with worker-defined timing.
    Timed,
    /// The operation should only be retried after manual intervention.
    Manual,
    /// The operation should not be retried.
    NoRetry,
}

/// Policy describing how an [`EtlError`] should be handled by workers.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ErrorHandlingPolicy {
    retry_directive: RetryDirective,
    solution: Option<&'static str>,
}

impl ErrorHandlingPolicy {
    /// Creates a new policy with all directives.
    const fn new(retry_directive: RetryDirective, solution: Option<&'static str>) -> Self {
        Self {
            retry_directive,
            solution,
        }
    }

    /// Returns the retry directive for this policy.
    pub fn retry_directive(&self) -> RetryDirective {
        self.retry_directive
    }

    /// Returns an optional operator-facing solution message.
    pub fn solution(&self) -> Option<&'static str> {
        self.solution
    }
}

/// Builds an [`ErrorHandlingPolicy`] from an [`EtlError`] to determine in a unified way how errors
/// should be handled.
pub fn build_error_handling_policy(error: &EtlError) -> ErrorHandlingPolicy {
    match error.kind() {
        // Automatically retriable errors. Keep this list narrow and limited to transient source or destination
        // connectivity/capacity failures that are expected to recover without operator intervention.
        ErrorKind::SourceConnectionFailed
        | ErrorKind::DestinationConnectionFailed
        | ErrorKind::SourceDatabaseShutdown
        | ErrorKind::SourceDatabaseInRecovery => {
            ErrorHandlingPolicy::new(RetryDirective::Timed, None)
        }

        // Manual retry errors with explicit operator guidance.
        ErrorKind::SourceAuthenticationError => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Verify source database credentials and authentication token validity."),
        ),
        ErrorKind::DestinationAuthenticationError => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Verify destination credentials and authentication token validity."),
        ),
        ErrorKind::SourceSchemaError => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Update the Postgres database schema to resolve compatibility issues."),
        ),
        ErrorKind::NullValuesNotSupportedInArrayInDestination => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Remove NULL values from array columns in the Postgres tables."),
        ),
        ErrorKind::UnsupportedValueInDestination => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Update the value in the Postgres table to make sure it's compatible."),
        ),
        ErrorKind::SourceConfigurationLimitExceeded => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some(
                "Verify the configured limits for Postgres, for example, the maximum number of replication slots.",
            ),
        ),
        ErrorKind::ReplicationSlotNotCreated => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Verify the Postgres database allows creation of new replication slots."),
        ),
        ErrorKind::SourceSnapshotTooOld => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Check replication slot status and database configuration."),
        ),

        // Special handling for fault injection tests.
        #[cfg(feature = "failpoints")]
        ErrorKind::WithNoRetry => {
            ErrorHandlingPolicy::new(RetryDirective::NoRetry, Some("Cannot retry this error."))
        }
        #[cfg(feature = "failpoints")]
        ErrorKind::WithManualRetry => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Manually trigger retry after resolving the issue."),
        ),
        #[cfg(feature = "failpoints")]
        ErrorKind::WithTimedRetry => ErrorHandlingPolicy::new(
            RetryDirective::Timed,
            Some("Will automatically retry after the configured delay."),
        ),

        // By default, require manual intervention with a generic solution.
        _ => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some(
                "There is no single prescribed solution for this error. The issue may still be recoverable with manual intervention based on the specific context. If it persists after rollback and targeted fixes, please contact support.",
            ),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(kind: ErrorKind) -> EtlError {
        EtlError::from((kind, "test error"))
    }

    #[test]
    fn classifies_source_connection_failed_as_timed_retry() {
        let policy = build_error_handling_policy(&err(ErrorKind::SourceConnectionFailed));
        assert_eq!(policy.retry_directive(), RetryDirective::Timed);
        assert_eq!(policy.solution(), None);
    }

    #[test]
    fn classifies_authentication_error_as_manual_retry() {
        let policy = build_error_handling_policy(&err(ErrorKind::SourceAuthenticationError));
        assert_eq!(policy.retry_directive(), RetryDirective::Manual);
        assert!(policy.solution().is_some());
    }

    #[test]
    fn classifies_unknown_kind_as_manual_retry() {
        let policy = build_error_handling_policy(&err(ErrorKind::InvalidState));
        assert_eq!(policy.retry_directive(), RetryDirective::Manual);
        assert!(policy.solution().is_some());
    }
}
