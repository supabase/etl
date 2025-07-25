use std::error;
use std::fmt;

/// Convenient result type for ETL operations using [`ETLError`] as the error type.
///
/// This type alias reduces boilerplate when working with fallible ETL operations.
/// Most ETL functions return this type.
pub type ETLResult<T> = Result<T, ETLError>;

/// Main error type for ETL operations, inspired by Redis error handling patterns.
///
/// [`ETLError`] provides a comprehensive error system that can represent single errors,
/// errors with additional detail, or multiple aggregated errors. The design allows for
/// rich error information while maintaining ergonomic usage patterns.
///
/// # Examples
///
/// ```rust
/// use etl::error::{ETLError, ErrorKind};
///
/// // Simple error
/// let err = ETLError::from((ErrorKind::ConnectionFailed, "Database unreachable"));
///
/// // Error with detail
/// let err = ETLError::from((
///     ErrorKind::QueryFailed,
///     "SQL execution failed",
///     "Table 'users' does not exist".to_string(),
/// ));
///
/// // Multiple errors
/// let errors = vec![
///     ETLError::from((ErrorKind::ValidationError, "Invalid schema")),
///     ETLError::from((ErrorKind::ConversionError, "Type mismatch")),
/// ];
/// let multi_err = ETLError::many(errors);
/// ```
pub struct ETLError {
    repr: ErrorRepr,
}

/// Internal representation of error data.
///
/// This enum supports different error patterns while maintaining a unified interface.
/// Users should not interact with this type directly but use [`ETLError`] methods instead.
#[derive(Debug)]
pub enum ErrorRepr {
    /// Error with kind and static description
    WithDescription(ErrorKind, &'static str),
    /// Error with kind, static description, and dynamic detail
    WithDescriptionAndDetail(ErrorKind, &'static str, String),
    /// Multiple aggregated errors
    Many(Vec<ETLError>),
}

/// Specific categories of errors that can occur during ETL operations.
///
/// This enum provides granular error classification to enable appropriate error handling
/// strategies. Error kinds are organized by functional area and failure mode.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Database connection failed
    ConnectionFailed,
    /// Query execution failed
    QueryFailed,
    /// Source schema mismatch or validation error
    SourceSchemaError,
    /// Destination schema mismatch or validation error
    DestinationSchemaError,
    /// Missing table schema
    MissingTableSchema,
    /// Data type conversion error
    ConversionError,
    /// Configuration error
    ConfigError,
    /// Network or I/O error
    NetworkError,
    /// Serialization/deserialization error
    SerializationError,
    /// Encryption/decryption error
    EncryptionError,
    /// Invalid state error
    InvalidState,
    /// Invalid data
    InvalidData,
    /// Data validation error
    ValidationError,
    /// Apply worker error
    ApplyWorkerPanic,
    /// Table sync worker error
    TableSyncWorkerPanic,
    /// Table sync worker error
    TableSyncWorkerCaughtError,
    /// Destination-specific error
    DestinationError,
    /// Replication slot not found
    ReplicationSlotNotFound,
    /// Replication slot already exists
    ReplicationSlotAlreadyExists,
    /// Replication slot could not be created
    ReplicationSlotNotCreated,
    /// Replication slot name is invalid or too long
    ReplicationSlotInvalid,
    /// Table synchronization failed
    TableSyncFailed,
    /// Logical replication stream error
    LogicalReplicationFailed,
    /// Unknown error
    Unknown,
}

impl ETLError {
    /// Creates an [`ETLError`] containing multiple aggregated errors.
    ///
    /// This is useful when multiple operations fail and you want to report all failures
    /// rather than just the first one.
    pub fn many(errors: Vec<ETLError>) -> ETLError {
        ETLError {
            repr: ErrorRepr::Many(errors),
        }
    }

    /// Returns the [`ErrorKind`] of this error.
    ///
    /// For multiple errors, returns the kind of the first error or [`ErrorKind::Unknown`]
    /// if the error list is empty.
    pub fn kind(&self) -> ErrorKind {
        match self.repr {
            ErrorRepr::WithDescription(kind, _)
            | ErrorRepr::WithDescriptionAndDetail(kind, _, _) => kind,
            ErrorRepr::Many(ref errors) => errors
                .first()
                .map(|err| err.kind())
                .unwrap_or(ErrorKind::Unknown),
        }
    }

    /// Returns all [`ErrorKind`]s present in this error.
    ///
    /// For single errors, returns a vector with one element. For multiple errors,
    /// returns a flattened vector of all error kinds.
    pub fn kinds(&self) -> Vec<ErrorKind> {
        match self.repr {
            ErrorRepr::WithDescription(kind, _)
            | ErrorRepr::WithDescriptionAndDetail(kind, _, _) => vec![kind],
            ErrorRepr::Many(ref errors) => errors
                .iter()
                .flat_map(|err| err.kinds())
                .collect::<Vec<_>>(),
        }
    }

    /// Returns the detailed error information if available.
    ///
    /// For multiple errors, returns the detail of the first error that has one.
    /// Returns [`None`] if no detailed information is available.
    pub fn detail(&self) -> Option<&str> {
        match self.repr {
            ErrorRepr::WithDescriptionAndDetail(_, _, ref detail) => Some(detail.as_str()),
            ErrorRepr::Many(ref errors) => {
                // For multiple errors, return the detail of the first error that has one
                errors.iter().find_map(|e| e.detail())
            }
            _ => None,
        }
    }
}

impl PartialEq for ETLError {
    fn eq(&self, other: &ETLError) -> bool {
        match (&self.repr, &other.repr) {
            (ErrorRepr::WithDescription(kind_a, _), ErrorRepr::WithDescription(kind_b, _)) => {
                kind_a == kind_b
            }
            (
                ErrorRepr::WithDescriptionAndDetail(kind_a, _, _),
                ErrorRepr::WithDescriptionAndDetail(kind_b, _, _),
            ) => kind_a == kind_b,
            (ErrorRepr::Many(errors_a), ErrorRepr::Many(errors_b)) => {
                errors_a.len() == errors_b.len()
                    && errors_a.iter().zip(errors_b.iter()).all(|(a, b)| a == b)
            }
            _ => false,
        }
    }
}

impl fmt::Display for ETLError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self.repr {
            ErrorRepr::WithDescription(kind, desc) => {
                fmt::Debug::fmt(&kind, f)?;
                f.write_str(": ")?;
                desc.fmt(f)?;

                Ok(())
            }
            ErrorRepr::WithDescriptionAndDetail(kind, desc, ref detail) => {
                fmt::Debug::fmt(&kind, f)?;
                f.write_str(": ")?;
                desc.fmt(f)?;
                f.write_str(" -> ")?;
                detail.fmt(f)?;

                Ok(())
            }
            ErrorRepr::Many(ref errors) => {
                if errors.is_empty() {
                    write!(f, "Multiple errors occurred (empty)")?;
                } else if errors.len() == 1 {
                    // If there's only one error, just display it directly
                    errors[0].fmt(f)?;
                } else {
                    write!(f, "Multiple errors occurred ({} total):", errors.len())?;
                    for (i, error) in errors.iter().enumerate() {
                        write!(f, "\n  {}: {}", i + 1, error)?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for ETLError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        fmt::Display::fmt(self, f)
    }
}

impl error::Error for ETLError {
    #[allow(deprecated)]
    fn description(&self) -> &str {
        match self.repr {
            ErrorRepr::WithDescription(_, desc)
            | ErrorRepr::WithDescriptionAndDetail(_, desc, _) => desc,
            ErrorRepr::Many(ref errors) => {
                if errors.is_empty() {
                    "Multiple errors occurred (empty)"
                } else {
                    // Return the description of the first error
                    errors[0].description()
                }
            }
        }
    }
}

// Ergonomic constructors following Redis pattern

/// Creates an [`ETLError`] from an error kind and static description.
impl From<(ErrorKind, &'static str)> for ETLError {
    fn from((kind, desc): (ErrorKind, &'static str)) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescription(kind, desc),
        }
    }
}

/// Creates an [`ETLError`] from an error kind, static description, and dynamic detail.
impl From<(ErrorKind, &'static str, String)> for ETLError {
    fn from((kind, desc, detail): (ErrorKind, &'static str, String)) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(kind, desc, detail),
        }
    }
}

/// Creates an [`ETLError`] from a vector of errors for aggregation.
impl From<Vec<ETLError>> for ETLError {
    fn from(errors: Vec<ETLError>) -> ETLError {
        ETLError {
            repr: ErrorRepr::Many(errors),
        }
    }
}

// Common standard library error conversions

/// Converts [`std::io::Error`] to [`ETLError`] with [`ErrorKind::NetworkError`].
impl From<std::io::Error> for ETLError {
    fn from(err: std::io::Error) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::NetworkError,
                "I/O error occurred",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`serde_json::Error`] to [`ETLError`] with [`ErrorKind::SerializationError`].
impl From<serde_json::Error> for ETLError {
    fn from(err: serde_json::Error) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::SerializationError,
                "JSON serialization failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`std::str::Utf8Error`] to [`ETLError`] with [`ErrorKind::ConversionError`].
impl From<std::str::Utf8Error> for ETLError {
    fn from(err: std::str::Utf8Error) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "UTF-8 conversion failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`std::string::FromUtf8Error`] to [`ETLError`] with [`ErrorKind::ConversionError`].
impl From<std::string::FromUtf8Error> for ETLError {
    fn from(err: std::string::FromUtf8Error) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "UTF-8 string conversion failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`std::num::ParseIntError`] to [`ETLError`] with [`ErrorKind::ConversionError`].
impl From<std::num::ParseIntError> for ETLError {
    fn from(err: std::num::ParseIntError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Integer parsing failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`std::num::ParseFloatError`] to [`ETLError`] with [`ErrorKind::ConversionError`].
impl From<std::num::ParseFloatError> for ETLError {
    fn from(err: std::num::ParseFloatError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Float parsing failed",
                err.to_string(),
            ),
        }
    }
}

// PostgreSQL-specific error conversions

/// Converts [`tokio_postgres::Error`] to [`ETLError`].
///
/// Maps to [`ErrorKind::QueryFailed`] if an error code is present,
/// otherwise maps to [`ErrorKind::ConnectionFailed`].
impl From<tokio_postgres::Error> for ETLError {
    fn from(err: tokio_postgres::Error) -> ETLError {
        let kind = if err.code().is_some() {
            ErrorKind::QueryFailed
        } else {
            ErrorKind::ConnectionFailed
        };

        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                kind,
                "PostgreSQL client operation failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`rustls::Error`] to [`ETLError`] with [`ErrorKind::EncryptionError`].
impl From<rustls::Error> for ETLError {
    fn from(err: rustls::Error) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::EncryptionError,
                "TLS configuration failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`uuid::Error`] to [`ETLError`] with [`ErrorKind::InvalidData`].
impl From<uuid::Error> for ETLError {
    fn from(err: uuid::Error) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::InvalidData,
                "UUID parsing failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`chrono::ParseError`] to [`ETLError`] with [`ErrorKind::ConversionError`].
impl From<chrono::ParseError> for ETLError {
    fn from(err: chrono::ParseError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Chrono parse failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`bigdecimal::ParseBigDecimalError`] to [`ETLError`] with [`ErrorKind::ConversionError`].
impl From<bigdecimal::ParseBigDecimalError> for ETLError {
    fn from(err: bigdecimal::ParseBigDecimalError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "BigDecimal parsing failed",
                err.to_string(),
            ),
        }
    }
}

// SQLx error conversion

/// Converts [`sqlx::Error`] to [`ETLError`] with appropriate error kind.
///
/// Maps database errors to [`ErrorKind::QueryFailed`], I/O errors to [`ErrorKind::NetworkError`],
/// and connection pool errors to [`ErrorKind::ConnectionFailed`].
impl From<sqlx::Error> for ETLError {
    fn from(err: sqlx::Error) -> ETLError {
        let kind = match &err {
            sqlx::Error::Database(_) => ErrorKind::QueryFailed,
            sqlx::Error::Io(_) => ErrorKind::NetworkError,
            sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut => ErrorKind::ConnectionFailed,
            _ => ErrorKind::QueryFailed,
        };

        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                kind,
                "Database operation failed",
                err.to_string(),
            ),
        }
    }
}

// BigQuery error conversions (feature-gated)

/// Converts [`gcp_bigquery_client::error::BQError`] to [`ETLError`] with appropriate error kind.
///
/// Maps request errors to [`ErrorKind::NetworkError`], response errors to [`ErrorKind::QueryFailed`],
/// and other errors to [`ErrorKind::DestinationError`].
#[cfg(feature = "bigquery")]
impl From<gcp_bigquery_client::error::BQError> for ETLError {
    fn from(err: gcp_bigquery_client::error::BQError) -> ETLError {
        let kind = match &err {
            gcp_bigquery_client::error::BQError::RequestError(_) => ErrorKind::NetworkError,
            gcp_bigquery_client::error::BQError::ResponseError { .. } => ErrorKind::QueryFailed,
            _ => ErrorKind::DestinationError,
        };

        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                kind,
                "BigQuery operation failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts BigQuery row errors to [`ETLError`] with [`ErrorKind::DestinationError`].
#[cfg(feature = "bigquery")]
impl From<crate::clients::bigquery::RowErrors> for ETLError {
    fn from(err: crate::clients::bigquery::RowErrors) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::DestinationError,
                "BigQuery row errors",
                err.to_string(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bail, etl_error};

    #[test]
    fn test_simple_error_creation() {
        let err = ETLError::from((ErrorKind::ConnectionFailed, "Database connection failed"));
        assert_eq!(err.kind(), ErrorKind::ConnectionFailed);
        assert_eq!(err.detail(), None);
        assert_eq!(err.kinds(), vec![ErrorKind::ConnectionFailed]);
    }

    #[test]
    fn test_error_with_detail() {
        let err = ETLError::from((
            ErrorKind::QueryFailed,
            "SQL query execution failed",
            "Table 'users' doesn't exist".to_string(),
        ));
        assert_eq!(err.kind(), ErrorKind::QueryFailed);
        assert_eq!(err.detail(), Some("Table 'users' doesn't exist"));
        assert_eq!(err.kinds(), vec![ErrorKind::QueryFailed]);
    }

    #[test]
    fn test_multiple_errors() {
        let errors = vec![
            ETLError::from((ErrorKind::ValidationError, "Invalid schema")),
            ETLError::from((ErrorKind::ConversionError, "Type mismatch")),
            ETLError::from((ErrorKind::NetworkError, "Connection timeout")),
        ];
        let multi_err = ETLError::many(errors);

        assert_eq!(multi_err.kind(), ErrorKind::ValidationError);
        assert_eq!(
            multi_err.kinds(),
            vec![
                ErrorKind::ValidationError,
                ErrorKind::ConversionError,
                ErrorKind::NetworkError
            ]
        );
        assert_eq!(multi_err.detail(), None);
    }

    #[test]
    fn test_multiple_errors_with_detail() {
        let errors = vec![
            ETLError::from((
                ErrorKind::ValidationError,
                "Invalid schema",
                "Missing required field".to_string(),
            )),
            ETLError::from((ErrorKind::ConversionError, "Type mismatch")),
        ];
        let multi_err = ETLError::many(errors);

        assert_eq!(multi_err.detail(), Some("Missing required field"));
    }

    #[test]
    fn test_from_vector() {
        let errors = vec![
            ETLError::from((ErrorKind::ValidationError, "Error 1")),
            ETLError::from((ErrorKind::ConversionError, "Error 2")),
        ];
        let multi_err = ETLError::from(errors);
        assert_eq!(multi_err.kinds().len(), 2);
    }

    #[test]
    fn test_empty_multiple_errors() {
        let multi_err = ETLError::many(vec![]);
        assert_eq!(multi_err.kind(), ErrorKind::Unknown);
        assert_eq!(multi_err.kinds(), vec![]);
        assert_eq!(multi_err.detail(), None);
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Access denied");
        let etl_err = ETLError::from(io_err);
        assert_eq!(etl_err.kind(), ErrorKind::NetworkError);
        assert!(etl_err.detail().unwrap().contains("Access denied"));
    }

    #[test]
    fn test_from_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let etl_err = ETLError::from(json_err);
        assert_eq!(etl_err.kind(), ErrorKind::SerializationError);
        assert!(etl_err.detail().is_some());
    }

    #[test]
    fn test_from_parse_int_error() {
        let parse_err = "not_a_number".parse::<i32>().unwrap_err();
        let etl_err = ETLError::from(parse_err);
        assert_eq!(etl_err.kind(), ErrorKind::ConversionError);
        assert!(etl_err.detail().is_some());
    }

    #[test]
    fn test_from_utf8_error() {
        let invalid_utf8 = vec![0, 159, 146, 150];
        let utf8_err = std::str::from_utf8(&invalid_utf8).unwrap_err();
        let etl_err = ETLError::from(utf8_err);
        assert_eq!(etl_err.kind(), ErrorKind::ConversionError);
        assert!(etl_err.detail().is_some());
    }

    #[test]
    fn test_error_equality() {
        let err1 = ETLError::from((ErrorKind::ConnectionFailed, "Connection failed"));
        let err2 = ETLError::from((ErrorKind::ConnectionFailed, "Connection failed"));
        let err3 = ETLError::from((ErrorKind::QueryFailed, "Query failed"));

        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    #[test]
    fn test_error_display() {
        let err = ETLError::from((ErrorKind::ConnectionFailed, "Database connection failed"));
        let display_str = format!("{err}");
        assert!(display_str.contains("ConnectionFailed"));
        assert!(display_str.contains("Database connection failed"));
    }

    #[test]
    fn test_error_display_with_detail() {
        let err = ETLError::from((
            ErrorKind::QueryFailed,
            "SQL query failed",
            "Invalid table name".to_string(),
        ));
        let display_str = format!("{err}");
        assert!(display_str.contains("QueryFailed"));
        assert!(display_str.contains("SQL query failed"));
        assert!(display_str.contains("Invalid table name"));
    }

    #[test]
    fn test_multiple_errors_display() {
        let errors = vec![
            ETLError::from((ErrorKind::ValidationError, "Invalid schema")),
            ETLError::from((ErrorKind::ConversionError, "Type mismatch")),
        ];
        let multi_err = ETLError::many(errors);
        let display_str = format!("{multi_err}");
        assert!(display_str.contains("Multiple errors"));
        assert!(display_str.contains("2 total"));
    }

    #[test]
    fn test_macro_usage() {
        let err = etl_error!(ErrorKind::ValidationError, "Invalid data format");
        assert_eq!(err.kind(), ErrorKind::ValidationError);
        assert_eq!(err.detail(), None);

        let err_with_detail = etl_error!(
            ErrorKind::ConversionError,
            "Type conversion failed",
            "Cannot convert string to integer: 'abc'"
        );
        assert_eq!(err_with_detail.kind(), ErrorKind::ConversionError);
        assert!(err_with_detail.detail().unwrap().contains("Cannot convert"));
    }

    #[test]
    fn test_bail_macro() {
        fn test_function() -> ETLResult<i32> {
            bail!(ErrorKind::ValidationError, "Test error");
        }

        fn test_function_with_detail() -> ETLResult<i32> {
            bail!(
                ErrorKind::ConversionError,
                "Test error",
                "Additional detail"
            );
        }

        let result = test_function();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ValidationError);

        let result = test_function_with_detail();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConversionError);
        assert!(err.detail().unwrap().contains("Additional detail"));
    }

    #[test]
    fn test_nested_multiple_errors() {
        let inner_errors = vec![
            ETLError::from((ErrorKind::ConversionError, "Inner error 1")),
            ETLError::from((ErrorKind::ValidationError, "Inner error 2")),
        ];
        let inner_multi = ETLError::many(inner_errors);

        let outer_errors = vec![
            inner_multi,
            ETLError::from((ErrorKind::NetworkError, "Outer error")),
        ];
        let outer_multi = ETLError::many(outer_errors);

        let kinds = outer_multi.kinds();
        assert_eq!(kinds.len(), 3);
        assert!(kinds.contains(&ErrorKind::ConversionError));
        assert!(kinds.contains(&ErrorKind::ValidationError));
        assert!(kinds.contains(&ErrorKind::NetworkError));
    }
}
