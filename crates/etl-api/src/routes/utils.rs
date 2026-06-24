use std::io::ErrorKind;

use axum::http::StatusCode;
use sqlx::error::DatabaseError;

use crate::validation::{ValidationError, ValidationFailure};

/// Returns the public HTTP status code for a validation execution error.
pub fn validation_error_status_code(error: &ValidationError) -> StatusCode {
    match error {
        ValidationError::Database { source } => source_database_error_status_code(source),
        ValidationError::BigQuery(_) | ValidationError::Iceberg(_) => StatusCode::BAD_GATEWAY,
        ValidationError::TrustedRootCerts(_) | ValidationError::Environment(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Returns the public HTTP status code for source database execution errors.
pub fn source_database_error_status_code(error: &sqlx::Error) -> StatusCode {
    match error {
        sqlx::Error::Io(error) if error.kind() == ErrorKind::TimedOut => {
            StatusCode::GATEWAY_TIMEOUT
        }
        error if source_database_error_is_unavailable(error) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_GATEWAY,
    }
}

/// Returns true when a source database error means the source is unavailable.
pub fn source_database_error_is_unavailable(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Io(error) => source_database_io_error_is_unavailable(error.kind()),
        sqlx::Error::Database(error) => source_database_unavailable_error(error.as_ref()),
        _ => false,
    }
}

fn source_database_io_error_is_unavailable(error_kind: ErrorKind) -> bool {
    matches!(
        error_kind,
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
            | ErrorKind::BrokenPipe
            | ErrorKind::TimedOut
            | ErrorKind::UnexpectedEof
    )
}

fn source_database_unavailable_error(error: &dyn DatabaseError) -> bool {
    let Some(code) = error.code() else {
        return false;
    };

    code.starts_with("08")
        || matches!(code.as_ref(), "53300" | "57P01" | "57P02" | "57P03" | "57P04" | "57P05")
}

/// Returns the public error message for a validation execution error.
pub fn validation_error_message(error: &ValidationError) -> &'static str {
    match error {
        ValidationError::Database { source: sqlx::Error::PoolTimedOut } => {
            "Could not reach your source database in time"
        }
        ValidationError::Database { source: sqlx::Error::PoolClosed } => {
            "Your source database is currently unavailable"
        }
        ValidationError::Database { source: sqlx::Error::Io(error) }
            if error.kind() == ErrorKind::TimedOut =>
        {
            "Could not reach your source database in time"
        }
        ValidationError::Database { .. } => "Could not validate your source database connection",
        ValidationError::BigQuery(_) => "Could not connect to BigQuery",
        ValidationError::Iceberg(_) => "Could not connect to the Iceberg endpoint",
        ValidationError::TrustedRootCerts(_) | ValidationError::Environment(_) => {
            "Internal server error"
        }
    }
}

/// Formats validation failures for API error responses.
pub fn format_validation_failures(failures: Vec<ValidationFailure>) -> String {
    failures.into_iter().map(|failure| failure.reason).collect::<Vec<_>>().join("; ")
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, error::Error as StdError, fmt};

    use super::*;

    #[derive(Debug)]
    struct TestDatabaseError {
        code: &'static str,
    }

    impl TestDatabaseError {
        fn new(code: &'static str) -> Self {
            Self { code }
        }
    }

    impl fmt::Display for TestDatabaseError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test database error {}", self.code)
        }
    }

    impl StdError for TestDatabaseError {}

    impl DatabaseError for TestDatabaseError {
        fn message(&self) -> &str {
            "test database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }

        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[test]
    fn source_database_pool_timeout_is_reported_as_service_unavailable() {
        let error = ValidationError::from(sqlx::Error::PoolTimedOut);

        assert_eq!(validation_error_status_code(&error), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            validation_error_message(&error),
            "Could not reach your source database in time"
        );
        assert_eq!(error.to_string(), "Database query failed");
    }

    #[test]
    fn source_database_io_timeout_is_reported_as_gateway_timeout() {
        let error = ValidationError::from(sqlx::Error::Io(std::io::Error::new(
            ErrorKind::TimedOut,
            "timed out",
        )));

        assert_eq!(validation_error_status_code(&error), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(
            validation_error_message(&error),
            "Could not reach your source database in time"
        );
        assert_eq!(error.to_string(), "Database query failed");
    }

    #[test]
    fn source_database_connection_io_errors_are_reported_as_service_unavailable() {
        let error =
            sqlx::Error::Io(std::io::Error::new(ErrorKind::ConnectionReset, "connection reset"));

        assert_eq!(source_database_error_status_code(&error), StatusCode::SERVICE_UNAVAILABLE);
        assert!(source_database_error_is_unavailable(&error));
    }

    #[test]
    fn source_database_connection_sqlstate_errors_are_reported_as_service_unavailable() {
        for code in ["08006", "53300", "57P01", "57P02", "57P03", "57P04", "57P05"] {
            let error = sqlx::Error::Database(Box::new(TestDatabaseError::new(code)));

            assert_eq!(source_database_error_status_code(&error), StatusCode::SERVICE_UNAVAILABLE);
            assert!(source_database_error_is_unavailable(&error));
        }
    }

    #[test]
    fn source_database_non_connection_sqlstate_errors_are_reported_as_upstream_failures() {
        let error = sqlx::Error::Database(Box::new(TestDatabaseError::new("42501")));

        assert_eq!(source_database_error_status_code(&error), StatusCode::BAD_GATEWAY);
        assert!(!source_database_error_is_unavailable(&error));
    }

    #[test]
    fn source_database_errors_are_reported_as_upstream_failures() {
        let error = sqlx::Error::Protocol("bad source response".to_owned());

        assert_eq!(source_database_error_status_code(&error), StatusCode::BAD_GATEWAY);
    }
}
