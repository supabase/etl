use crate::error::{ErrorKind, EtlError, Retryability};

/// Retry behavior for a classified error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum RetryDirective {
    /// The operation can be retried automatically with worker-defined timing.
    Timed,
    /// The operation should only be retried after manual intervention.
    Manual,
    /// The operation should not be retried.
    #[cfg_attr(not(feature = "failpoints"), expect(dead_code))]
    NoRetry,
}

/// Policy describing how an [`EtlError`] should be handled by workers.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ErrorHandlingPolicy {
    retry_directive: RetryDirective,
    solution: Option<&'static str>,
}

impl ErrorHandlingPolicy {
    /// Creates a new policy with all directives.
    const fn new(retry_directive: RetryDirective, solution: Option<&'static str>) -> Self {
        Self { retry_directive, solution }
    }

    /// Returns `true` if this policy involves a retry.
    pub(crate) fn should_retry(&self) -> bool {
        self.retry_directive == RetryDirective::Timed
    }

    /// Returns the retry directive for this policy.
    pub(crate) fn retry_directive(&self) -> RetryDirective {
        self.retry_directive
    }

    /// Returns an optional operator-facing solution message.
    pub(crate) fn solution(&self) -> Option<&'static str> {
        self.solution
    }
}

/// Returns the forced policy for a fault-injection error kind.
#[cfg(feature = "failpoints")]
fn failpoint_policy(kind: ErrorKind) -> Option<ErrorHandlingPolicy> {
    match kind {
        ErrorKind::WithNoRetry => Some(ErrorHandlingPolicy::new(
            RetryDirective::NoRetry,
            Some("Cannot retry this error."),
        )),
        ErrorKind::WithManualRetry => Some(ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some("Manually trigger retry after resolving the issue."),
        )),
        ErrorKind::WithTimedRetry => Some(ErrorHandlingPolicy::new(
            RetryDirective::Timed,
            Some("Will automatically retry after the configured delay."),
        )),
        _ => None,
    }
}

/// Returns the kind of the error that makes `error` permanent.
///
/// An aggregate is permanent because of its first permanent member, so its
/// operator guidance comes from that member rather than from its first error.
fn permanent_kind(error: &EtlError) -> ErrorKind {
    let permanent_member = error.errors().and_then(|errors| {
        errors.iter().find(|error| error.retryability() == Retryability::Permanent)
    });

    permanent_member.map_or_else(|| error.kind(), permanent_kind)
}

/// Returns operator guidance for a permanent error of `kind`.
fn manual_solution(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::SourceAuthenticationError => {
            "Verify source database credentials and authentication token validity."
        }
        ErrorKind::DestinationAuthenticationError => {
            "Verify destination credentials and authentication token validity."
        }
        ErrorKind::SourceSchemaError => {
            "Update the Postgres database schema to resolve compatibility issues."
        }
        ErrorKind::SourceReplicaIdentityError => {
            "Configure the affected Postgres table with the least costly replica identity \
             supported by the destination. Use REPLICA IDENTITY DEFAULT with a primary key, or \
             USING INDEX when supported, if stable key values are enough. Use REPLICA IDENTITY \
             FULL only when the destination needs full old-row images or complete replacement rows."
        }
        ErrorKind::NullValuesNotSupportedInArrayInDestination => {
            "Remove NULL values from array columns in the Postgres tables."
        }
        ErrorKind::UnsupportedValueInDestination => {
            "Update the value in the Postgres table to make sure it's compatible."
        }
        ErrorKind::SourceConfigurationLimitExceeded => {
            "Verify the configured limits for Postgres, for example, the maximum number of \
             replication slots."
        }
        ErrorKind::ReplicationSlotNotCreated => {
            "Verify the Postgres database allows creation of new replication slots."
        }
        ErrorKind::SourceSnapshotTooOld => {
            "Check replication slot status and database configuration."
        }
        ErrorKind::DestinationSchemaRewind => {
            "Resynchronize the affected table. The destination schema is ahead of the replayed \
             replication stream, so the replayed schema snapshot cannot be applied safely."
        }
        ErrorKind::TableSyncWorkerPanic => {
            "Inspect the table sync worker panic logs and manually retry the table."
        }
        ErrorKind::TaskPanic | ErrorKind::TaskCancelled => {
            "Inspect the task failure and resolve its cause before manually retrying."
        }
        _ => {
            "There is no single prescribed solution for this error. The issue may still be \
             recoverable with manual intervention based on the specific context. If it persists \
             after rollback and targeted fixes, please contact support."
        }
    }
}

/// Builds an [`ErrorHandlingPolicy`] from an [`EtlError`] to determine in a
/// unified way how errors should be handled.
///
/// The retry directive follows [`EtlError::retryability`] alone. Retry
/// attempts are bounded, so persistent failures still require intervention.
/// The [`ErrorKind`] only selects operator guidance for permanent errors.
pub(crate) fn build_error_handling_policy(error: &EtlError) -> ErrorHandlingPolicy {
    // Fault-injection kinds force a directive regardless of classification.
    #[cfg(feature = "failpoints")]
    if let Some(policy) = failpoint_policy(error.kind()) {
        return policy;
    }

    match error.retryability() {
        Retryability::Retryable => ErrorHandlingPolicy::new(RetryDirective::Timed, None),
        Retryability::Permanent => ErrorHandlingPolicy::new(
            RetryDirective::Manual,
            Some(manual_solution(permanent_kind(error))),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::{ErrorKind, EtlError, Retryability},
        runtime::error_policy::{RetryDirective, build_error_handling_policy},
    };

    /// Protocol and internal failures require intervention; connectivity and
    /// unavailable feedback use bounded retries.
    #[test]
    fn replication_feedback_errors_keep_distinct_retry_policies() {
        for (kind, retry) in [
            (ErrorKind::DeserializationError, RetryDirective::Manual),
            (ErrorKind::InvalidState, RetryDirective::Manual),
            (ErrorKind::TaskPanic, RetryDirective::Manual),
            (ErrorKind::TaskCancelled, RetryDirective::Manual),
            (ErrorKind::SourceAuthenticationError, RetryDirective::Manual),
            (ErrorKind::SourceConnectionFailed, RetryDirective::Timed),
            (ErrorKind::SourceLockTimeout, RetryDirective::Timed),
            (ErrorKind::ReplicationFeedbackUnavailable, RetryDirective::Timed),
        ] {
            let error = EtlError::from((kind, "Test replication failure"));
            assert_eq!(build_error_handling_policy(&error).retry_directive(), retry);
        }
    }

    #[test]
    fn source_replica_identity_errors_have_specific_manual_remediation() {
        let error = EtlError::from((ErrorKind::SourceReplicaIdentityError, "Replica identity"));

        let policy = build_error_handling_policy(&error);

        assert_eq!(policy.retry_directive(), RetryDirective::Manual);
        let solution = policy.solution().expect("replica identity errors should have a solution");
        assert!(solution.contains("least costly replica identity"));
        assert!(solution.contains("REPLICA IDENTITY FULL only"));
    }

    /// A classification made where the error happened decides the directive,
    /// even when the kind's default says otherwise.
    #[test]
    fn retryability_decides_the_directive_over_the_kind() {
        let rejected_timeout = EtlError::from((ErrorKind::DestinationTimeout, "Rejected"))
            .with_retryability(Retryability::Permanent);
        let busy_query = EtlError::from((ErrorKind::DestinationQueryFailed, "Busy"))
            .with_retryability(Retryability::Retryable);

        let rejected_policy = build_error_handling_policy(&rejected_timeout);
        assert_eq!(rejected_policy.retry_directive(), RetryDirective::Manual);
        assert!(rejected_policy.solution().is_some());
        let busy_policy = build_error_handling_policy(&busy_query);
        assert_eq!(busy_policy.retry_directive(), RetryDirective::Timed);
        assert_eq!(busy_policy.solution(), None);
    }

    /// An aggregate retries only when every error may succeed again, and its
    /// guidance comes from the error that makes it permanent.
    #[test]
    fn aggregate_policy_follows_its_permanent_error() {
        let lost_connection = EtlError::from((ErrorKind::DestinationConnectionFailed, "Lost"));
        let bad_credentials =
            EtlError::from((ErrorKind::DestinationAuthenticationError, "Bad credentials"));
        let aggregate = EtlError::from(vec![lost_connection, bad_credentials.clone()]);

        let policy = build_error_handling_policy(&aggregate);

        assert_eq!(policy.retry_directive(), RetryDirective::Manual);
        assert_eq!(policy.solution(), build_error_handling_policy(&bad_credentials).solution());
    }
}
