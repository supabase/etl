use crate::error::{ErrorClass, ErrorScope, EtlError};

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

    /// Returns `true` if this policy involves a retry.
    pub fn should_retry(&self) -> bool {
        self.retry_directive == RetryDirective::Timed
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
    match (error.scope(), error.class()) {
        // Automatically retriable errors. Keep this list narrow and limited to transient source or destination
        // connectivity/capacity failures that are expected to recover without operator intervention.
        (_, ErrorClass::ConnectionFailed)
        | (ErrorScope::Destination, ErrorClass::AtomicBatchRetryable)
        | (ErrorScope::Source, ErrorClass::DatabaseShutdown)
        | (ErrorScope::Source, ErrorClass::DatabaseInRecovery) => {
            ErrorHandlingPolicy::new(RetryDirective::Timed, None)
        }

        // Manual retry errors with explicit operator guidance.
        (ErrorScope::Source, ErrorClass::AuthenticationError) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Verify source database credentials and authentication token validity."),
        ),
        (ErrorScope::Destination, ErrorClass::AuthenticationError) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Verify destination credentials and authentication token validity."),
        ),
        (_, ErrorClass::PermissionDenied) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Grant the required source or destination permissions before retrying."),
        ),
        (ErrorScope::Source, ErrorClass::SchemaError) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Update the Postgres database schema to resolve compatibility issues."),
        ),
        (_, ErrorClass::NullValuesNotSupportedInArrayInDestination) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Remove NULL values from array columns in the Postgres tables."),
        ),
        (_, ErrorClass::UnsupportedValueInDestination) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Update the value in the Postgres table to make sure it's compatible."),
        ),
        (ErrorScope::Source, ErrorClass::ConfigurationLimitExceeded) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some(
                "Verify the configured limits for Postgres, for example, the maximum number of replication slots.",
            ),
        ),
        (ErrorScope::Source, ErrorClass::ReplicationSlotNotCreated) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Verify the Postgres database allows creation of new replication slots."),
        ),
        (ErrorScope::Source, ErrorClass::SnapshotTooOld) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Check replication slot status and database configuration."),
        ),

        // Special handling for fault injection tests.
        #[cfg(feature = "failpoints")]
        (_, ErrorClass::WithNoRetry) => {
            ErrorHandlingPolicy::new(RetryDirective::NoRetry, Some("Cannot retry this error."))
        }
        #[cfg(feature = "failpoints")]
        (_, ErrorClass::WithManualRetry) => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Manually trigger retry after resolving the issue."),
        ),
        #[cfg(feature = "failpoints")]
        (_, ErrorClass::WithTimedRetry) => ErrorHandlingPolicy::new(
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
