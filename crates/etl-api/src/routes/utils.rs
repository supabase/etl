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
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => StatusCode::SERVICE_UNAVAILABLE,
        sqlx::Error::Database(error) if source_database_unavailable_error(error.as_ref()) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        sqlx::Error::Io(error) if error.kind() == ErrorKind::TimedOut => {
            StatusCode::GATEWAY_TIMEOUT
        }
        _ => StatusCode::BAD_GATEWAY,
    }
}

fn source_database_unavailable_error(error: &dyn DatabaseError) -> bool {
    matches!(error.code().as_deref(), Some("57P01" | "57P02" | "57P03"))
}

/// Returns the public error message for a validation execution error.
pub fn validation_error_message(error: &ValidationError) -> &'static str {
    match error {
        ValidationError::Database { source: sqlx::Error::PoolTimedOut } => {
            "Could not reach the source database in time"
        }
        ValidationError::Database { source: sqlx::Error::PoolClosed } => {
            "The source database is currently unavailable"
        }
        ValidationError::Database { source: sqlx::Error::Io(error) }
            if error.kind() == ErrorKind::TimedOut =>
        {
            "Could not reach the source database in time"
        }
        ValidationError::Database { .. } => "Could not validate the source database connection",
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
    use super::*;

    #[test]
    fn source_database_pool_timeout_is_reported_as_service_unavailable() {
        let error = ValidationError::from(sqlx::Error::PoolTimedOut);

        assert_eq!(validation_error_status_code(&error), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(validation_error_message(&error), "Could not reach the source database in time");
        assert_eq!(error.to_string(), "Database query failed");
    }

    #[test]
    fn source_database_io_timeout_is_reported_as_gateway_timeout() {
        let error = ValidationError::from(sqlx::Error::Io(std::io::Error::new(
            ErrorKind::TimedOut,
            "timed out",
        )));

        assert_eq!(validation_error_status_code(&error), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(validation_error_message(&error), "Could not reach the source database in time");
        assert_eq!(error.to_string(), "Database query failed");
    }

    #[test]
    fn source_database_errors_are_reported_as_upstream_failures() {
        let error = sqlx::Error::Protocol("bad source response".to_owned());

        assert_eq!(source_database_error_status_code(&error), StatusCode::BAD_GATEWAY);
    }
}
