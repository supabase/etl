use crate::{
    error::{ErrorKind, EtlError},
    etl_error,
    replication::state::TableRetryPolicy,
    runtime::ErrorHandlingPolicy,
};

/// Represents an error that occurred during table replication.
///
/// Contains diagnostic information including the reason for failure, an
/// optional solution suggestion, and the retry policy to apply.
#[derive(Debug)]
pub struct TableError {
    pub(super) reason: String,
    pub(super) solution: Option<String>,
    pub(super) retry_policy: TableRetryPolicy,
    pub(super) source_err: EtlError,
}

impl TableError {
    /// Creates a new [`TableError`] with a suggested solution.
    pub fn with_solution(
        reason: impl ToString,
        solution: impl ToString,
        retry_policy: TableRetryPolicy,
    ) -> Self {
        let reason = reason.to_string();
        Self {
            reason: reason.clone(),
            solution: Some(solution.to_string()),
            retry_policy,
            source_err: etl_error!(ErrorKind::Unknown, "Table replication error", reason),
        }
    }

    /// Creates a new [`TableError`] without a suggested solution.
    pub fn without_solution(reason: impl ToString, retry_policy: TableRetryPolicy) -> Self {
        let reason = reason.to_string();
        Self {
            reason: reason.clone(),
            solution: None,
            retry_policy,
            source_err: etl_error!(ErrorKind::Unknown, "Table replication error", reason),
        }
    }

    /// Returns the retry policy for this error.
    pub fn retry_policy(&self) -> &TableRetryPolicy {
        &self.retry_policy
    }

    /// Returns a copy of the error with the provided retry policy.
    pub fn with_retry_policy(mut self, retry_policy: TableRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Builds a [`TableError`] from a shared handling policy and
    /// worker retry policy.
    pub(crate) fn from_error_policy(
        error: &EtlError,
        policy: &ErrorHandlingPolicy,
        retry_policy: TableRetryPolicy,
    ) -> Self {
        match policy.solution() {
            Some(solution) => {
                Self::with_solution(error, solution, retry_policy).with_source_err(error.clone())
            }
            None => Self::without_solution(error, retry_policy).with_source_err(error.clone()),
        }
    }

    /// Returns a copy of the error with the provided source error attached.
    fn with_source_err(mut self, source_err: EtlError) -> Self {
        self.source_err = source_err;
        self
    }
}
