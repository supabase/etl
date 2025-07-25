use std::error;
use std::fmt;

/// Main result type for ETL operations
pub type ETLResult<T> = Result<T, ETLError>;

/// Main error type for ETL operations, inspired by Redis error handling
pub struct ETLError {
    repr: ErrorRepr,
}

#[derive(Debug)]
pub enum ErrorRepr {
    WithDescription(ErrorKind, &'static str),
    WithDescriptionAndDetail(ErrorKind, &'static str, String),
    Many(Vec<ETLError>),
}

/// Comprehensive error kinds for ETL operations
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Database connection failed
    ConnectionFailed,
    /// Authentication failed
    AuthenticationFailed,
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
    /// Pipeline execution error
    PipelineError,
    /// Resource constraint error (memory, disk, etc.)
    ResourceError,
    /// Network or I/O error
    NetworkError,
    /// Serialization/deserialization error
    SerializationError,
    /// Encryption/decryption error
    EncryptionError,
    /// Timeout error
    TimeoutError,
    /// Invalid state error
    InvalidState,
    /// Data validation error
    ValidationError,
    /// Worker/concurrency error
    WorkerError,
    /// Destination-specific error
    DestinationError,
    /// Source-specific error
    SourceError,
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
}

impl ETLError {
    /// Creates a new ETLError that contains multiple errors
    pub fn many(errors: Vec<ETLError>) -> ETLError {
        ETLError {
            repr: ErrorRepr::Many(errors),
        }
    }

    /// Returns the kind of the error
    pub fn kind(&self) -> ErrorKind {
        match self.repr {
            ErrorRepr::WithDescription(kind, _)
            | ErrorRepr::WithDescriptionAndDetail(kind, _, _) => kind,
            ErrorRepr::Many(ref errors) => {
                // For multiple errors, return the kind of the first error, or WorkerError if empty
                errors
                    .first()
                    .map(|e| e.kind())
                    .unwrap_or(ErrorKind::WorkerError)
            }
        }
    }

    /// Returns the error detail if available
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

    /// Returns the name of the error category for display purposes
    pub fn category(&self) -> &str {
        match self.kind() {
            ErrorKind::ConnectionFailed => "connection failed",
            ErrorKind::AuthenticationFailed => "authentication failed",
            ErrorKind::QueryFailed => "query failed",
            ErrorKind::SchemaError => "schema error",
            ErrorKind::ConversionError => "conversion error",
            ErrorKind::ConfigError => "config error",
            ErrorKind::PipelineError => "pipeline error",
            ErrorKind::ResourceError => "resource error",
            ErrorKind::NetworkError => "network error",
            ErrorKind::SerializationError => "serialization error",
            ErrorKind::EncryptionError => "encryption error",
            ErrorKind::TimeoutError => "timeout error",
            ErrorKind::InvalidState => "invalid state",
            ErrorKind::ValidationError => "validation error",
            ErrorKind::WorkerError => "worker error",
            ErrorKind::DestinationError => "destination error",
            ErrorKind::SourceError => "source error",
            ErrorKind::ReplicationSlotNotFound => "replication slot not found",
            ErrorKind::ReplicationSlotAlreadyExists => "replication slot already exists",
            ErrorKind::ReplicationSlotInvalid => "replication slot invalid",
            ErrorKind::TableSyncFailed => "table sync failed",
            ErrorKind::LogicalReplicationFailed => "logical replication failed",
        }
    }

    /// Returns true if this is a connection-related error
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::ConnectionFailed | ErrorKind::NetworkError | ErrorKind::TimeoutError
        )
    }

    /// Returns true if this is a data-related error
    pub fn is_data_error(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::SchemaError | ErrorKind::ConversionError | ErrorKind::ValidationError
        )
    }

    /// Returns true if this is a replication-related error
    pub fn is_replication_error(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::ReplicationSlotNotFound
                | ErrorKind::ReplicationSlotAlreadyExists
                | ErrorKind::ReplicationSlotInvalid
                | ErrorKind::TableSyncFailed
                | ErrorKind::LogicalReplicationFailed
        )
    }

    /// Returns true if this is a replication slot error
    pub fn is_replication_slot_error(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::ReplicationSlotNotFound
                | ErrorKind::ReplicationSlotAlreadyExists
                | ErrorKind::ReplicationSlotInvalid
        )
    }

    /// Returns true if this error contains multiple errors
    pub fn is_many(&self) -> bool {
        matches!(self.repr, ErrorRepr::Many(_))
    }

    /// Returns the number of errors contained in this error
    pub fn error_count(&self) -> usize {
        match self.repr {
            ErrorRepr::Many(ref errors) => errors.len(),
            _ => 1,
        }
    }

    /// Returns an iterator over all errors (including nested ones)
    pub fn iter_errors(&self) -> impl Iterator<Item = &ETLError> {
        ErrorIterator::new(self)
    }

    /// Flattens multiple errors into a single vector
    pub fn flatten_errors(&self) -> Vec<&ETLError> {
        let mut errors = Vec::new();
        self.collect_errors(&mut errors);
        errors
    }

    fn collect_errors<'a>(&'a self, errors: &mut Vec<&'a ETLError>) {
        match self.repr {
            ErrorRepr::Many(ref nested_errors) => {
                for error in nested_errors {
                    error.collect_errors(errors);
                }
            }
            _ => errors.push(self),
        }
    }
}

/// Iterator over all errors in an ETLError tree
pub struct ErrorIterator<'a> {
    stack: Vec<&'a ETLError>,
}

impl<'a> ErrorIterator<'a> {
    fn new(error: &'a ETLError) -> Self {
        let mut stack = Vec::new();
        stack.push(error);
        ErrorIterator { stack }
    }
}

impl<'a> Iterator for ErrorIterator<'a> {
    type Item = &'a ETLError;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(error) = self.stack.pop() {
            match &error.repr {
                ErrorRepr::Many(ref errors) => {
                    // Add all errors to the stack in reverse order so they're processed in order
                    for error in errors.iter().rev() {
                        self.stack.push(error);
                    }
                }
                _ => return Some(error),
            }
        }
        None
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
                desc.fmt(f)?;
                f.write_str(" - ")?;
                fmt::Debug::fmt(&kind, f)
            }
            ErrorRepr::WithDescriptionAndDetail(kind, desc, ref detail) => {
                desc.fmt(f)?;
                f.write_str(" - ")?;
                fmt::Debug::fmt(&kind, f)?;
                f.write_str(": ")?;
                detail.fmt(f)
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
impl From<(ErrorKind, &'static str)> for ETLError {
    fn from((kind, desc): (ErrorKind, &'static str)) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescription(kind, desc),
        }
    }
}

impl From<(ErrorKind, &'static str, String)> for ETLError {
    fn from((kind, desc, detail): (ErrorKind, &'static str, String)) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(kind, desc, detail),
        }
    }
}

// Common standard library error conversions
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

// Tokio watch error conversion
impl From<tokio::sync::watch::error::SendError<()>> for ETLError {
    fn from(err: tokio::sync::watch::error::SendError<()>) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::WorkerError,
                "Worker shutdown failed",
                err.to_string(),
            ),
        }
    }
}

// Tokio sync error conversions
impl From<tokio::sync::AcquireError> for ETLError {
    fn from(err: tokio::sync::AcquireError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ResourceError,
                "Failed to acquire permit",
                err.to_string(),
            ),
        }
    }
}

impl From<tokio::task::JoinError> for ETLError {
    fn from(err: tokio::task::JoinError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::WorkerError,
                "Failed to join tokio task",
                err.to_string(),
            ),
        }
    }
}

// SQLx error conversion
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

// Additional error type conversions for destination and stream errors
impl From<crate::destination::base::DestinationError> for ETLError {
    fn from(err: crate::destination::base::DestinationError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::DestinationError,
                "Destination operation failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::replication::stream::TableCopyStreamError> for ETLError {
    fn from(err: crate::replication::stream::TableCopyStreamError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::TableSyncFailed,
                "Table copy stream operation failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::replication::stream::EventsStreamError> for ETLError {
    fn from(err: crate::replication::stream::EventsStreamError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::LogicalReplicationFailed,
                "Events stream operation failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::conversions::event::EventConversionError> for ETLError {
    fn from(err: crate::conversions::event::EventConversionError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Event conversion failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::replication::slot::SlotError> for ETLError {
    fn from(err: crate::replication::slot::SlotError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ReplicationSlotInvalid,
                "Replication slot operation failed",
                err.to_string(),
            ),
        }
    }
}

// Missing From implementations for conversion error types
impl From<crate::conversions::hex::ByteaHexParseError> for ETLError {
    fn from(err: crate::conversions::hex::ByteaHexParseError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Hex parsing failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::conversions::table_row::TableRowConversionError> for ETLError {
    fn from(err: crate::conversions::table_row::TableRowConversionError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Table row conversion failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::conversions::text::FromTextError> for ETLError {
    fn from(err: crate::conversions::text::FromTextError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Text conversion failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::conversions::text::ArrayParseError> for ETLError {
    fn from(err: crate::conversions::text::ArrayParseError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Array parsing failed",
                err.to_string(),
            ),
        }
    }
}

impl From<crate::conversions::bool::ParseBoolError> for ETLError {
    fn from(err: crate::conversions::bool::ParseBoolError) -> ETLError {
        ETLError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Boolean parsing failed",
                err.to_string(),
            ),
        }
    }
}

#[macro_export]
macro_rules! etl_error {
    ($kind:expr, $desc:expr) => {
        ETLError::from(($kind, $desc))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        ETLError::from(($kind, $desc, $detail.to_string()))
    };
}

#[macro_export]
macro_rules! bail {
    ($kind:expr, $desc:expr) => {
        return Err(etl_error!($kind, $desc))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        return Err(etl_error!($kind, $desc, $detail))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = ETLError::from((ErrorKind::ConnectionFailed, "Database connection failed"));
        assert_eq!(err.kind(), ErrorKind::ConnectionFailed);
        assert_eq!(err.category(), "connection failed");
        assert!(err.is_connection_error());
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
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Access denied");
        let etl_err = ETLError::from(io_err);
        assert_eq!(etl_err.kind(), ErrorKind::NetworkError);
        assert!(etl_err.detail().unwrap().contains("Access denied"));
    }

    #[test]
    fn test_macro_usage() {
        let err = etl_error!(ErrorKind::ValidationError, "Invalid data format");
        assert_eq!(err.kind(), ErrorKind::ValidationError);

        let err_with_detail = etl_error!(
            ErrorKind::ConversionError,
            "Type conversion failed",
            "Cannot convert string to integer: 'abc'"
        );
        assert_eq!(err_with_detail.kind(), ErrorKind::ConversionError);
        assert!(err_with_detail.detail().unwrap().contains("Cannot convert"));
    }

    #[test]
    fn test_error_categories() {
        let connection_err = ETLError::from((ErrorKind::ConnectionFailed, "Connection failed"));
        let data_err = ETLError::from((ErrorKind::SchemaError, "Schema mismatch"));
        let replication_err =
            ETLError::from((ErrorKind::ReplicationSlotNotFound, "Slot not found"));
        let slot_err = ETLError::from((ErrorKind::ReplicationSlotAlreadyExists, "Slot exists"));

        assert!(connection_err.is_connection_error());
        assert!(!connection_err.is_data_error());
        assert!(!connection_err.is_replication_error());

        assert!(!data_err.is_connection_error());
        assert!(data_err.is_data_error());
        assert!(!data_err.is_replication_error());

        assert!(replication_err.is_replication_error());
        assert!(replication_err.is_replication_slot_error());
        assert!(!replication_err.is_connection_error());

        assert!(slot_err.is_replication_error());
        assert!(slot_err.is_replication_slot_error());
        assert!(!slot_err.is_data_error());
    }
}
