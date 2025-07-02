use std::{borrow, error, fmt, result};
use tokio_postgres::error::SqlState;

use crate::v2::workers::base::WorkerType;

/// Type alias for convenience when using the Result type with our Error.
pub type Result<T> = result::Result<T, Error>;

/// Internal error representation with kind and optional source errors.
///
/// Uses boxing to keep the public Error type size consistent and enable
/// rich error context without performance penalties for the success path.
///
/// Supports both single and multiple source errors for comprehensive error chaining.
struct ErrorInner {
    kind: ErrorKind,
    sources: Vec<Box<dyn error::Error + Send + Sync>>,
}

/// Kind of errors that can happen in ETL.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    // === Database & Connection Errors ===
    /// Database connection failure with connection details
    ConnectionFailed { source: String },
    /// Authentication failure during database connection
    AuthenticationFailed { source: String },
    /// Database transaction operation failure
    TransactionFailed { source: String },
    /// Connection lost during ongoing operations
    ConnectionLost { source: String },
    /// TLS/SSL configuration or negotiation failure
    TlsConfigurationFailed,

    // === Replication & Streaming Errors ===
    /// Replication slot operation failure during creation or modification
    ReplicationSlotNotCreated { slot_name: String, reason: String },
    /// Replication slot not found in database
    ReplicationSlotNotFound { slot_name: String },
    /// Attempt to create replication slot that already exists
    ReplicationSlotAlreadyExists { slot_name: String },
    /// Publication not found or inaccessible
    PublicationNotFound { publication_name: String },
    /// Table copy stream processing failure
    TableCopyStreamFailed { table_name: String },
    /// Events stream processing failure
    EventsStreamFailed,
    /// LSN inconsistency indicating replication state corruption
    LsnConsistencyError { expected: String, actual: String },
    /// Transaction state mismatch during replication
    TransactionNotStarted,
    /// Unexpected event type received during replication
    EventTypeMismatch { expected: String, actual: String },
    /// Invalid table replication phase transition
    TableReplicationPhaseInvalid { expected: String, actual: String },
    /// Table replication state missing for an active table
    TableReplicationStateMissing { table_name: String },

    // === Schema & Data Structure Errors ===
    /// Table not found in database schema
    TableNotFound { table_name: String },
    /// Column not found in table schema
    ColumnNotFound {
        table_name: String,
        column_name: String,
    },
    /// Tuple data format not supported by conversion logic
    TupleDataNotSupported { type_name: String },

    // === Worker & Pipeline Errors ===
    /// Pipeline shutdown operation failed
    PipelineShutdownFailed,
    /// Worker task panicked during execution
    WorkerPanicked { worker_type: WorkerType },
    /// Worker task cancelled during execution
    WorkerCancelled { worker_type: WorkerType },
    /// Table sync worker experienced a silent failure
    WorkerFailedSilently {
        worker_type: WorkerType,
        reason: String,
    },

    // === State Management Errors ===
    /// State corruption detected in persistent storage
    StateStoreCorrupted { description: String },

    // === Data Processing Errors ===
    /// Data type conversion failure between systems
    DataConversionFailed {
        from_type: String,
        to_type: String,
        value: Option<String>,
    },

    // === System Errors ===
    /// Resource limit exceeded (memory, disk, connections)
    ResourceLimitExceeded { resource: String, limit: String },
    /// I/O operation failure
    IoError { description: String },
    /// Generic Postgres related error.
    PostgresError { description: String },

    // === Aggregate & Fallback Errors ===
    /// Multiple errors occurred and need to be collected
    Many { amount: u64 },
    /// Error that doesn't fit other categories
    Other { description: String },
}

/// Error recovery strategy hint for automated error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// No retry - permanent failure
    NoRetry,
    /// Retry
    Retry,
}

/// Formatting mode for error chains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainFormat {
    /// Debug format with full details
    Debug,
    /// Display format with basic message
    Display,
    /// Display alternate format with chain
    DisplayAlternate,
}

/// A stable error type for the ETL library using the ErrorInner pattern.
///
/// This error type provides a stable public API while allowing internal error details
/// to evolve.
pub struct Error(Box<ErrorInner>);

impl Error {
    /// Creates a new error with the specified kind.
    pub fn new(kind: ErrorKind) -> Self {
        Error(Box::new(ErrorInner {
            kind,
            sources: Vec::new(),
        }))
    }

    /// Creates a new error with the specified kind and single source error.
    pub fn with_source<E>(kind: ErrorKind, source: E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Error(Box::new(ErrorInner {
            kind,
            sources: vec![source.into()],
        }))
    }

    /// Creates a new error with the specified kind and multiple source errors.
    pub fn with_sources<I, E>(kind: ErrorKind, sources: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Error(Box::new(ErrorInner {
            kind,
            sources: sources.into_iter().map(|e| e.into()).collect(),
        }))
    }

    /// Creates a new error with kind [`ErrorKind::Many`] from a collection of other errors.
    pub fn from_many<I, E>(sources: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        let sources = sources.into_iter().map(|e| e.into()).collect::<Vec<_>>();

        Error(Box::new(ErrorInner {
            kind: ErrorKind::Many {
                amount: sources.len() as u64,
            },
            sources,
        }))
    }

    /// Creates a transaction not started error.
    pub fn transaction_not_started() -> Self {
        Self::new(ErrorKind::TransactionNotStarted)
    }

    /// Creates an event type mismatch error.
    pub fn event_type_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::new(ErrorKind::EventTypeMismatch {
            expected: expected.into(),
            actual: actual.into(),
        })
    }

    /// Creates a table not found error.
    pub fn table_not_found(table_name: impl Into<String>) -> Self {
        Self::new(ErrorKind::TableNotFound {
            table_name: table_name.into(),
        })
    }

    /// Creates a column not found error.
    pub fn column_not_found(table_name: impl Into<String>, column_name: impl Into<String>) -> Self {
        Self::new(ErrorKind::ColumnNotFound {
            table_name: table_name.into(),
            column_name: column_name.into(),
        })
    }

    /// Creates a replication slot not found error.
    pub fn replication_slot_not_found(slot_name: impl Into<String>) -> Self {
        Self::new(ErrorKind::ReplicationSlotNotFound {
            slot_name: slot_name.into(),
        })
    }

    /// Creates a fallback error for cases not covered by specific variants.
    pub fn other(description: impl Into<String>) -> Self {
        Self::new(ErrorKind::Other {
            description: description.into(),
        })
    }

    /// Returns the primary (first) source error for std::error::Error compatibility.
    pub fn primary_source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.0
            .sources
            .first()
            .map(|e| e.as_ref() as &(dyn error::Error + 'static))
    }

    /// Returns the error kind classification.
    pub fn kind(&self) -> &ErrorKind {
        &self.0.kind
    }

    /// Returns the recommended recovery strategy for automated error handling.
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        use ErrorKind::*;
        match &self.0.kind {
            AuthenticationFailed { .. }
            | TableNotFound { .. }
            | ColumnNotFound { .. }
            | TupleDataNotSupported { .. }
            | TlsConfigurationFailed
            | PublicationNotFound { .. }
            | ReplicationSlotAlreadyExists { .. }
            | WorkerPanicked { .. }
            | LsnConsistencyError { .. }
            | PipelineShutdownFailed
            | StateStoreCorrupted { .. }
            | TableReplicationPhaseInvalid { .. }
            | Other { .. }
            | Many { .. } => RecoveryStrategy::NoRetry,

            WorkerCancelled { .. }
            | TransactionNotStarted
            | EventTypeMismatch { .. }
            | ConnectionFailed { .. }
            | ConnectionLost { .. }
            | TransactionFailed { .. }
            | IoError { .. }
            | TableCopyStreamFailed { .. }
            | EventsStreamFailed
            | ReplicationSlotNotCreated { .. }
            | ReplicationSlotNotFound { .. }
            | TableReplicationStateMissing { .. }
            | PostgresError { .. }
            | ResourceLimitExceeded { .. }
            | DataConversionFailed { .. }
            | WorkerFailedSilently { .. } => RecoveryStrategy::Retry,
        }
    }

    /// Returns true if this error is likely transient and retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self.recovery_strategy(), RecoveryStrategy::Retry)
    }

    /// Returns true if this error represents a permanent failure.
    pub fn is_permanent(&self) -> bool {
        matches!(self.recovery_strategy(), RecoveryStrategy::NoRetry)
    }

    /// Returns a formatted string showing this error and all its source errors recursively.
    fn error_chain(&self, format: ChainFormat) -> String {
        self.error_chain_with_indent(format, "")
    }

    /// Internal method for error_chain that handles proper indentation for tree hierarchy.
    fn error_chain_with_indent(&self, format: ChainFormat, indent: &str) -> String {
        let mut result = self.error_message();

        // Handle multiple sources
        match self.0.sources.len() {
            0 => {} // No sources
            1 => {
                // Single source - check if it's an Error type to use proper tree formatting
                let source = &self.0.sources[0];

                if let Some(error_source) = source.downcast_ref::<Error>() {
                    // It's our Error type, use proper tree formatting with "caused by" arrow
                    result.push_str(&format!("\n{indent}  ↳ caused by: "));
                    result.push_str(
                        &error_source.error_chain_with_indent(format, &format!("{indent}    ")),
                    );
                } else {
                    // Regular error type, use traditional chaining
                    result.push_str(&format!("\n{indent}  ↳ caused by: "));
                    match format {
                        ChainFormat::Debug => result.push_str(&format!("{source:?}")),
                        ChainFormat::Display => result.push_str(&format!("{source}")),
                        ChainFormat::DisplayAlternate => result.push_str(&format!("{source:#}")),
                    }

                    // Continue with traditional chaining for nested sources
                    let mut current_source = error::Error::source(source.as_ref());
                    while let Some(nested_source) = current_source {
                        result.push_str(&format!("\n{indent}  ↳ caused by: "));
                        match format {
                            ChainFormat::Debug => result.push_str(&format!("{nested_source:?}")),
                            ChainFormat::Display => result.push_str(&format!("{nested_source}")),
                            ChainFormat::DisplayAlternate => {
                                result.push_str(&format!("{nested_source:#}"))
                            }
                        }
                        current_source = error::Error::source(nested_source);
                    }
                }
            }
            _ => {
                // Multiple sources - add the "caused by" header first
                result.push_str(&format!("\n{indent}  ↳ caused by multiple errors:"));

                // Then list them with proper tree hierarchy
                for (i, source) in self.0.sources.iter().enumerate() {
                    result.push_str(&format!("\n{}    {}. ", indent, i + 1));

                    if let Some(error_source) = source.downcast_ref::<Error>() {
                        // It's our Error type, use proper tree formatting
                        result.push_str(
                            &error_source
                                .error_chain_with_indent(format, &format!("{indent}       ")),
                        );
                    } else {
                        // Regular error type
                        match format {
                            ChainFormat::Debug => result.push_str(&format!("{source:?}")),
                            ChainFormat::Display => result.push_str(&format!("{source}")),
                            ChainFormat::DisplayAlternate => {
                                result.push_str(&format!("{source:#}"))
                            }
                        }

                        // Show nested sources for each error
                        let mut current_source = error::Error::source(source.as_ref());
                        while let Some(nested_source) = current_source {
                            result.push_str(&format!("\n{indent}  ↳ caused by: "));
                            match format {
                                ChainFormat::Debug => {
                                    result.push_str(&format!("{nested_source:?}"))
                                }
                                ChainFormat::Display => {
                                    result.push_str(&format!("{nested_source}"))
                                }
                                ChainFormat::DisplayAlternate => {
                                    result.push_str(&format!("{nested_source:#}"))
                                }
                            }
                            current_source = error::Error::source(nested_source);
                        }
                    }
                }
            }
        }

        result
    }

    /// Returns just the primary error message without source chain.
    fn error_message(&self) -> String {
        use ErrorKind::*;

        match &self.0.kind {
            ConnectionFailed { source } => {
                format!("failed to connect to database (source: {source})")
            }
            AuthenticationFailed { source } => {
                format!("authentication failed (source: {source})")
            }
            TlsConfigurationFailed => "tls configuration failed".to_string(),
            TransactionFailed { source } => {
                format!("database transaction failed (source: {source})")
            }
            ConnectionLost { source } => {
                format!("database connection lost: (source: {source})")
            }

            ReplicationSlotNotCreated { slot_name, reason } => {
                format!("failed to create replication slot '{slot_name}': {reason}")
            }
            ReplicationSlotNotFound { slot_name } => {
                format!("replication slot '{slot_name}' not found")
            }
            ReplicationSlotAlreadyExists { slot_name } => {
                format!("replication slot '{slot_name}' already exists")
            }
            PublicationNotFound { publication_name } => {
                format!("publication '{publication_name}' operation failed")
            }
            TableCopyStreamFailed { table_name } => {
                format!("table copy stream failed for table '{table_name}'")
            }
            EventsStreamFailed => "events stream processing failed".to_string(),
            LsnConsistencyError {
                expected: expected_lsn,
                actual: actual_lsn,
            } => {
                format!("lsn consistency error: expected {expected_lsn}, got {actual_lsn}")
            }
            TransactionNotStarted => {
                "transaction not started but commit message received".to_string()
            }
            EventTypeMismatch { expected, actual } => {
                format!("event type mismatch: expected {expected}, got {actual}")
            }
            TableReplicationPhaseInvalid { expected, actual } => {
                format!("table replication phase invalid: expected {expected}, got {actual}")
            }
            WorkerCancelled { worker_type } => {
                format!("{worker_type} worker cancelled")
            }
            WorkerFailedSilently {
                worker_type,
                reason,
            } => {
                format!("{worker_type} worker failed silently: {reason}")
            }

            TableNotFound { table_name } => {
                format!("table '{table_name}' not found")
            }
            ColumnNotFound {
                table_name,
                column_name,
            } => {
                format!("column '{column_name}' not found in table '{table_name}'")
            }
            TableReplicationStateMissing { table_name } => {
                format!("replication state missing for table '{table_name}'")
            }
            TupleDataNotSupported { type_name } => {
                format!("tuple data not supported for type '{type_name}'")
            }
            PipelineShutdownFailed => "pipeline shutdown failed".to_string(),
            WorkerPanicked { worker_type } => {
                format!("{worker_type} worker panicked")
            }

            StateStoreCorrupted { description } => {
                format!("state corrupted: {description}")
            }
            DataConversionFailed {
                from_type,
                to_type,
                value,
            } => match value {
                Some(value) => {
                    format!("failed to convert value '{value}' from {from_type} to {to_type}")
                }
                None => {
                    format!("failed to convert from {from_type} to {to_type}")
                }
            },

            ResourceLimitExceeded { resource, limit } => {
                format!("resource limit exceeded for '{resource}' (limit: {limit})")
            }
            IoError { description } => format!("i/o operation failed: {description}"),
            PostgresError { description } => format!("postgres operation failed: {description}"),

            Many { amount } => match amount {
                0 => "no errors occurred".to_string(),
                1 => "an error occurred".to_string(),
                _ => format!("{amount} errors occurred"),
            },
            Other { description } => description.clone(),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Always display error with chain
        write!(f, "{}", self.error_chain(ChainFormat::Debug))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display shows first error, chain only if alternate format
        if f.alternate() {
            write!(f, "{}", self.error_chain(ChainFormat::DisplayAlternate))
        } else {
            write!(f, "{}", self.error_message())
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Return the first source for std::error::Error compatibility
        self.primary_source()
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        // Extract meaningful information from tokio_postgres::Error
        let description = err.to_string();

        // Check if it's a database-specific error with more context
        if let Some(db_err) = err.as_db_error() {
            let error_kind =
                map_postgres_sqlstate_to_error_kind(db_err.code(), db_err.table(), db_err.column());

            if let Some(error_kind) = error_kind {
                return Self::with_source(error_kind, err);
            }
        }

        Self::with_source(ErrorKind::PostgresError { description }, err)
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        let description = err.to_string();

        if let sqlx::Error::Database(db_err) = &err {
            let code_str = db_err.code().unwrap_or(borrow::Cow::Borrowed("unknown"));
            let sql_state = SqlState::from_code(&code_str);
            let error_kind = map_postgres_sqlstate_to_error_kind(&sql_state, db_err.table(), None);

            if let Some(error_kind) = error_kind {
                return Self::with_source(error_kind, err);
            }
        }

        Self::with_source(ErrorKind::PostgresError { description }, err)
    }
}

impl From<rustls::Error> for Error {
    fn from(err: rustls::Error) -> Self {
        Self::with_source(ErrorKind::TlsConfigurationFailed, err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::with_source(
            ErrorKind::IoError {
                description: err.kind().to_string(),
            },
            err,
        )
    }
}

/// Maps SqlState error codes to appropriate ErrorKind variants with context information.
///
/// Extracts relevant information from database errors and constructs appropriate
/// [`ErrorKind`] variants based on the SQL state code and available context.
fn map_postgres_sqlstate_to_error_kind(
    sql_state: &SqlState,
    table_name: Option<&str>,
    column_name: Option<&str>,
) -> Option<ErrorKind> {
    let source = "postgres".to_string();
    let table_name = table_name.unwrap_or("unknown");
    let column_name = column_name.unwrap_or("unknown");

    let error_kind = match sql_state {
        // Connection errors (Class 08)
        &SqlState::CONNECTION_EXCEPTION
        | &SqlState::CONNECTION_DOES_NOT_EXIST
        | &SqlState::CONNECTION_FAILURE => ErrorKind::ConnectionLost { source },
        &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        | &SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION => {
            ErrorKind::ConnectionFailed { source }
        }

        // Authentication errors (Class 28)
        &SqlState::INVALID_AUTHORIZATION_SPECIFICATION | &SqlState::INVALID_PASSWORD => {
            ErrorKind::AuthenticationFailed { source }
        }

        // Transaction state errors (Class 25)
        &SqlState::INVALID_TRANSACTION_STATE
        | &SqlState::ACTIVE_SQL_TRANSACTION
        | &SqlState::NO_ACTIVE_SQL_TRANSACTION
        | &SqlState::IN_FAILED_SQL_TRANSACTION => ErrorKind::TransactionFailed { source },

        // Transaction rollback errors (Class 40)
        &SqlState::T_R_SERIALIZATION_FAILURE
        | &SqlState::T_R_INTEGRITY_CONSTRAINT_VIOLATION
        | &SqlState::T_R_STATEMENT_COMPLETION_UNKNOWN
        | &SqlState::T_R_DEADLOCK_DETECTED => ErrorKind::TransactionFailed { source },

        // Schema/Object errors (Class 42)
        &SqlState::UNDEFINED_TABLE => ErrorKind::TableNotFound {
            table_name: table_name.to_string(),
        },
        &SqlState::UNDEFINED_COLUMN => ErrorKind::ColumnNotFound {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
        },
        &SqlState::INSUFFICIENT_PRIVILEGE => ErrorKind::AuthenticationFailed { source },

        // System resource errors (Class 53)
        &SqlState::INSUFFICIENT_RESOURCES
        | &SqlState::DISK_FULL
        | &SqlState::OUT_OF_MEMORY
        | &SqlState::TOO_MANY_CONNECTIONS
        | &SqlState::CONFIGURATION_LIMIT_EXCEEDED => ErrorKind::ResourceLimitExceeded {
            resource: match *sql_state {
                SqlState::DISK_FULL => "disk_space".to_string(),
                SqlState::OUT_OF_MEMORY => "memory".to_string(),
                SqlState::TOO_MANY_CONNECTIONS => "connections".to_string(),
                _ => "system_resources".to_string(),
            },
            limit: "exceeded".to_string(),
        },

        // Query canceled/shutdown (Class 57)
        &SqlState::OPERATOR_INTERVENTION
        | &SqlState::ADMIN_SHUTDOWN
        | &SqlState::CRASH_SHUTDOWN
        | &SqlState::CANNOT_CONNECT_NOW => ErrorKind::ConnectionLost { source },

        // Invalid catalog/schema name (Class 3D/3F)
        &SqlState::INVALID_CATALOG_NAME | &SqlState::INVALID_SCHEMA_NAME => {
            ErrorKind::ConnectionFailed { source }
        }

        // Unhandled SQLSTATE, we return no `ErrorKind`.
        _ => {
            return None;
        }
    };

    Some(error_kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_display_formats() {
        // Test simple error without source
        let error = Error::table_not_found("users");

        // Display format (normal) - just the error message
        let display_normal = format!("{error}");
        assert_eq!(display_normal, "table 'users' not found");

        // Display format (alternate) - error with chain (same as normal since no source)
        let display_alternate = format!("{error:#}");
        assert_eq!(display_alternate, "table 'users' not found");

        // Debug format - error with chain (same as normal since no source)
        let debug = format!("{error:?}");
        assert_eq!(debug, "table 'users' not found");
    }

    #[test]
    fn test_error_display_formats_with_source() {
        let io_error = io::Error::new(io::ErrorKind::ConnectionRefused, "TCP connection refused");
        let error = Error::with_source(
            ErrorKind::ConnectionFailed {
                source: "test".to_string(),
            },
            io_error,
        );

        // Display format (normal) - just the primary error message
        let display_normal = format!("{error}");
        assert!(display_normal.contains("failed to connect to database"));

        // Display format (alternate) - error with full chain
        let display_alternate = format!("{error:#}");
        assert!(display_alternate.contains("failed to connect to database"));
        assert!(display_alternate.contains("↳ caused by: TCP connection refused"));

        // Debug format - error with full chain
        let debug = format!("{error:?}");
        assert!(debug.contains("failed to connect to database"));
        assert!(debug.contains("↳ caused by:"));
        assert!(debug.contains("TCP connection refused"));
    }
}
