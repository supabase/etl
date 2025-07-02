use crate::v2::workers::base::WorkerType;
use std::{borrow, error, fmt, result};
use tokio_postgres::error::SqlState;

/// Type alias for convenience when using the Result type with our Error.
pub type Result<T> = result::Result<T, Error>;

/// Internal error representation with kind and optional source error.
///
/// Uses boxing to keep the public Error type size consistent and enable
/// rich error context without performance penalties for the success path.
struct ErrorInner {
    kind: ErrorKind,
    source: Option<Box<dyn error::Error + Send + Sync>>,
}

/// Comprehensive error classification for ETL operations.
///
/// Groups error variants by functional area to provide structured error information
/// for proper handling and recovery strategies. Unused variants have been removed
/// to keep the enum focused on actual use cases.
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
    /// Multiple errors occurred during batch operations
    Many { amount: u64 },
    /// Error that doesn't fit other categories
    Other { description: String },
}

/// Error severity level for monitoring and alerting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Low severity - expected transient error
    Low,
    /// Medium severity - unexpected but recoverable error
    Medium,
    /// High severity - significant operational issue
    High,
    /// Critical severity - system-threatening error
    Critical,
}

/// Error recovery strategy hint for automated error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// No retry - permanent failure
    NoRetry,
    /// Immediate retry recommended
    RetryImmediate,
    /// Retry with exponential backoff
    RetryWithBackoff,
    /// Retry after specific delay
    RetryAfterDelay,
    /// Manual intervention required
    ManualIntervention,
}

/// Formatting mode for error chains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainFormat {
    /// Debug format with full details
    Debug,
    /// Display format with basic message
    Display,
    /// Display alternate format with chain
    DisplayAlternate,
}

/// Error collection for handling multiple failures in batch operations.
///
/// Displays errors with numbered formatting and recursive source chains
/// for comprehensive error reporting in batch processing scenarios.
pub struct Errors(Vec<Error>);

impl From<Vec<Error>> for Errors {
    fn from(value: Vec<Error>) -> Self {
        Errors(value)
    }
}

impl fmt::Debug for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.len() {
            0 => write!(f, "no errors"),
            1 => write!(f, "{:?}", self.0[0]),
            count => {
                writeln!(f, "batch operation failed with {count} errors:")?;
                for (i, error) in self.0.iter().enumerate() {
                    write!(f, "  {}. {}", i + 1, error.error_chain(ChainFormat::Debug))?;
                    if i < count - 1 {
                        writeln!(f)?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.len() {
            0 => write!(f, "no errors"),
            1 => write!(f, "{}", self.0[0]),
            count => {
                writeln!(f, "batch operation failed with {count} errors:")?;
                for (i, error) in self.0.iter().enumerate() {
                    write!(
                        f,
                        "  {}. {}",
                        i + 1,
                        error.error_chain(ChainFormat::Display)
                    )?;
                    if i < count - 1 {
                        writeln!(f)?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl error::Error for Errors {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Return the first error as the primary source since std::error::Error
        // doesn't support multiple sources.
        self.0.first().and_then(|err| err.source())
    }
}

/// A stable error type for the ETL library using the ErrorInner pattern.
///
/// This error type provides a stable public API while allowing internal error details
/// to evolve. It supports error chaining, structured error data, and classification
/// for recovery strategies.
pub struct Error(Box<ErrorInner>);

impl Error {
    /// Creates a new error with the specified kind.
    pub fn new(kind: ErrorKind) -> Self {
        Error(Box::new(ErrorInner { kind, source: None }))
    }

    /// Creates a new error with the specified kind and source error.
    pub fn with_source<E>(kind: ErrorKind, source: E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        Error(Box::new(ErrorInner {
            kind,
            source: Some(source.into()),
        }))
    }

    /// Creates an error containing multiple errors from batch operations.
    pub fn from_many(errors: impl Into<Errors>) -> Self {
        let errors = errors.into();
        Error::with_source(
            ErrorKind::Many {
                amount: errors.0.len() as u64,
            },
            errors,
        )
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

    /// Returns the error kind classification.
    pub fn kind(&self) -> &ErrorKind {
        &self.0.kind
    }

    /// Returns the severity level of this error for monitoring and alerting.
    pub fn severity(&self) -> ErrorSeverity {
        use ErrorKind::*;
        match &self.0.kind {
            // Critical errors - system integrity or data consistency issues that require immediate attention
            StateStoreCorrupted { .. }
            | WorkerPanicked { .. }
            | LsnConsistencyError { .. }
            | PipelineShutdownFailed => ErrorSeverity::Critical,

            // High severity errors - operational failures that significantly impact functionality
            ConnectionLost { .. }
            | ReplicationSlotNotCreated { .. }
            | AuthenticationFailed { .. }
            | TransactionFailed { .. }
            | EventsStreamFailed
            | TableCopyStreamFailed { .. }
            | TransactionNotStarted => ErrorSeverity::High,

            // Medium severity errors - recoverable issues that may affect specific operations
            ConnectionFailed { .. }
            | TableNotFound { .. }
            | ColumnNotFound { .. }
            | ReplicationSlotNotFound { .. }
            | ReplicationSlotAlreadyExists { .. }
            | PublicationNotFound { .. }
            | ResourceLimitExceeded { .. }
            | EventTypeMismatch { .. }
            | TableReplicationPhaseInvalid { .. }
            | TableReplicationStateMissing { .. }
            | TupleDataNotSupported { .. }
            | TlsConfigurationFailed => ErrorSeverity::Medium,

            // Low severity errors - transient issues or expected operational events
            DataConversionFailed { .. }
            | WorkerCancelled { .. }
            | WorkerFailedSilently { .. }
            | IoError { .. }
            | PostgresError { .. }
            | Other { .. } => ErrorSeverity::Low,

            // Aggregate errors inherit severity from their constituent errors
            Many { .. } => ErrorSeverity::Medium,
        }
    }

    /// Returns the recommended recovery strategy for automated error handling.
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        use ErrorKind::*;
        match &self.0.kind {
            // No retry - permanent failures requiring configuration fixes or schema changes
            AuthenticationFailed { .. }
            | TableNotFound { .. }
            | ColumnNotFound { .. }
            | TupleDataNotSupported { .. }
            | TlsConfigurationFailed
            | PublicationNotFound { .. }
            | ReplicationSlotAlreadyExists { .. } => RecoveryStrategy::NoRetry,

            // Immediate retry - transient operational issues that resolve quickly
            WorkerCancelled { .. } | TransactionNotStarted | EventTypeMismatch { .. } => {
                RecoveryStrategy::RetryImmediate
            }

            // Retry with backoff - infrastructure issues that may take time to resolve
            ConnectionFailed { .. }
            | ConnectionLost { .. }
            | TransactionFailed { .. }
            | IoError { .. }
            | TableCopyStreamFailed { .. }
            | EventsStreamFailed
            | ReplicationSlotNotCreated { .. }
            | ReplicationSlotNotFound { .. }
            | TableReplicationStateMissing { .. }
            | PostgresError { .. } => RecoveryStrategy::RetryWithBackoff,

            // Retry after delay - resource or capacity constraints
            ResourceLimitExceeded { .. } => RecoveryStrategy::RetryAfterDelay,

            // Manual intervention - critical system integrity issues
            WorkerPanicked { .. }
            | LsnConsistencyError { .. }
            | PipelineShutdownFailed
            | StateStoreCorrupted { .. }
            | TableReplicationPhaseInvalid { .. } => RecoveryStrategy::ManualIntervention,

            // Context-sensitive defaults
            DataConversionFailed { .. } => RecoveryStrategy::RetryWithBackoff, // May be transient data format issue
            WorkerFailedSilently { .. } => RecoveryStrategy::RetryWithBackoff, // Unknown cause, be cautious
            Other { .. } => RecoveryStrategy::RetryWithBackoff, // Unknown issue, be conservative
            Many { .. } => RecoveryStrategy::RetryAfterDelay, // Multiple failures suggest systematic issue
        }
    }

    /// Returns true if this error is likely transient and retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::RetryImmediate
                | RecoveryStrategy::RetryWithBackoff
                | RecoveryStrategy::RetryAfterDelay
        )
    }

    /// Returns true if this error represents a permanent failure.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self.recovery_strategy(),
            RecoveryStrategy::NoRetry | RecoveryStrategy::ManualIntervention
        )
    }

    /// Returns a formatted string showing this error and all its source errors recursively.
    pub fn error_chain(&self, format: ChainFormat) -> String {
        let mut result = self.error_message();
        let mut current_source = error::Error::source(self);

        while let Some(source) = current_source {
            result.push_str("\n  ↳ caused by: ");

            // Format the source error based on the requested format
            match format {
                ChainFormat::Debug => {
                    result.push_str(&format!("{source:?}"));
                }
                ChainFormat::Display => {
                    result.push_str(&format!("{source}"));
                }
                ChainFormat::DisplayAlternate => {
                    result.push_str(&format!("{source:#}"));
                }
            }

            current_source = error::Error::source(source);
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

            Many { amount } => {
                format!("batch operation failed with {amount} errors")
            }
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
        self.0
            .source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn error::Error + 'static))
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

    #[test]
    fn test_errors_collection_formats() {
        let error1 = Error::table_not_found("users");
        let error2 = Error::column_not_found("posts", "invalid_column");

        let errors = Errors::from(vec![error1, error2]);

        // Display format
        let display = format!("{errors}");
        assert!(display.contains("batch operation failed with 2 errors:"));
        assert!(display.contains("1. table 'users' not found"));
        assert!(display.contains("2. column 'invalid_column' not found in table 'posts'"));

        // Debug format
        let debug = format!("{errors:?}");
        assert!(debug.contains("batch operation failed with 2 errors:"));
        assert!(debug.contains("1. table 'users' not found"));
        assert!(debug.contains("2. column 'invalid_column' not found in table 'posts'"));
    }
}
