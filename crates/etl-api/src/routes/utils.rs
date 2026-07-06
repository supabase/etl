use axum::http::StatusCode;

use crate::{
    data::source_database::{self, SourceDatabaseErrorKind},
    validation::{ValidationError, ValidationFailure},
};

/// Returns the public HTTP status code for a validation execution error.
pub fn validation_error_status_code(error: &ValidationError) -> StatusCode {
    match error {
        ValidationError::Database { source } => source_database_error_status_code(source),
        ValidationError::BigQuery(_) | ValidationError::Iceberg(_) => StatusCode::BAD_GATEWAY,
        ValidationError::Environment(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Returns the public HTTP status code for a source database error.
pub fn source_database_error_status_code(error: &sqlx::Error) -> StatusCode {
    match source_database::classify_error(error) {
        SourceDatabaseErrorKind::TimedOut => StatusCode::GATEWAY_TIMEOUT,
        SourceDatabaseErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        SourceDatabaseErrorKind::Failed => StatusCode::BAD_GATEWAY,
    }
}

/// Returns the public error message for source database query failures.
pub fn source_database_query_error_message() -> &'static str {
    "Could not query your source database"
}

/// Returns the public error message for a validation execution error.
pub fn validation_error_message(error: &ValidationError) -> &'static str {
    match error {
        ValidationError::Database { source } => source_database_validation_error_message(source),
        ValidationError::BigQuery(_) => "Could not connect to BigQuery",
        ValidationError::Iceberg(_) => "Could not connect to the Iceberg endpoint",
        ValidationError::Environment(_) => "Internal server error",
    }
}

/// Returns the public validation message for source database failures.
fn source_database_validation_error_message(error: &sqlx::Error) -> &'static str {
    match (error, source_database::classify_error(error)) {
        (sqlx::Error::PoolTimedOut, _) | (_, SourceDatabaseErrorKind::TimedOut) => {
            "Could not reach your source database in time"
        }
        (_, SourceDatabaseErrorKind::Unavailable) => {
            "Your source database is currently unavailable"
        }
        (_, SourceDatabaseErrorKind::Failed) => {
            "Could not validate your source database connection"
        }
    }
}

/// Formats validation failures for API error responses.
pub fn format_validation_failures(failures: Vec<ValidationFailure>) -> String {
    failures.into_iter().map(|failure| failure.reason).collect::<Vec<_>>().join("; ")
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

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
    fn source_database_errors_are_reported_as_upstream_failures() {
        let error = sqlx::Error::Protocol("bad source response".to_owned());

        assert_eq!(
            validation_error_status_code(&ValidationError::from(error)),
            StatusCode::BAD_GATEWAY
        );
    }
}
