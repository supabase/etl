use crate::v2::workers::base::WorkerType;
use std::{borrow, error, fmt, result};

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
    ConnectionFailed {
        host: String,
        port: u16,
        database: String,
    },
    /// Authentication failure during database connection
    AuthenticationFailed { user: String, database: String },
    /// TLS/SSL configuration or negotiation failure
    TlsConfigurationFailed,
    /// SQL query execution failure
    QueryExecutionFailed { query: String },
    /// Database transaction operation failure
    TransactionFailed,
    /// Connection lost during ongoing operations
    ConnectionLost,

    // === Replication & Streaming Errors ===
    /// Replication slot operation failure during creation or modification
    ReplicationSlotNotCreated {
        slot_name: String,
        reason: String
    },
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
    LsnConsistencyError {
        expected: String,
        actual: String,
    },
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
    /// Unsupported PostgreSQL data type encountered
    UnsupportedDataType {
        type_name: String,
        type_oid: u32,
        table_name: String,
    },
    /// Schema validation failure during replication setup
    SchemaValidationFailed { table_name: String, reason: String },
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
    /// Worker pool capacity or coordination failure
    WorkerPoolFailed { reason: String },

    // === State Management Errors ===
    /// State corruption detected in persistent storage
    StateCorrupted { description: String },

    // === Data Processing Errors ===
    /// Data type conversion failure between systems
    DataConversionFailed {
        from_type: String,
        to_type: String,
        value: Option<String>,
    },

    // === System & Infrastructure Errors ===
    /// Configuration parsing or validation failure
    ConfigurationError { parameter: String, reason: String },
    /// Resource limit exceeded (memory, disk, connections)
    ResourceLimitExceeded { resource: String, limit: String },
    /// I/O operation failure
    IoError,
    /// Network operation failure
    NetworkError,
    /// Operation timeout exceeded
    Timeout { operation: String, duration_ms: u64 },

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
        f.debug_struct("Errors")
            .field("count", &self.0.len())
            .field("errors", &self.0)
            .finish()
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
                    write!(f, "  {}. {}", i + 1, error.display_chain())?;
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

    /// Returns a formatted string showing this error and all its source errors recursively.
    pub fn display_chain(&self) -> String {
        let mut result = self.to_string();
        let mut current_source = error::Error::source(self);
        
        while let Some(source) = current_source {
            result.push_str(&format!("\n  ↳ caused by: {source}"));
            current_source = error::Error::source(source);
        }
        
        result
    }

    /// Returns the severity level of this error for monitoring and alerting.
    pub fn severity(&self) -> ErrorSeverity {
        use ErrorKind::*;
        match &self.0.kind {
            // Critical errors - system integrity or consistency issues
            StateCorrupted { .. } | WorkerPanicked { .. } | LsnConsistencyError { .. } => {
                ErrorSeverity::Critical
            }

            // High severity errors - significant operational issues
            ConnectionLost
            | ReplicationSlotNotCreated { .. }
            | WorkerPoolFailed { .. }
            | PipelineShutdownFailed => ErrorSeverity::High,

            // Medium severity errors - unexpected but recoverable
            ConnectionFailed { .. }
            | AuthenticationFailed { .. }
            | QueryExecutionFailed { .. }
            | TableNotFound { .. }
            | UnsupportedDataType { .. }
            | SchemaValidationFailed { .. }
            | ResourceLimitExceeded { .. }
            | TransactionNotStarted
            | EventTypeMismatch { .. }
            | TableReplicationPhaseInvalid { .. } => ErrorSeverity::Medium,

            // Low severity errors - transient or expected during normal operations
            Timeout { .. }
            | NetworkError
            | DataConversionFailed { .. }
            | WorkerCancelled { .. }
            | WorkerFailedSilently { .. }
            | IoError => ErrorSeverity::Low,

            // Default to medium for remaining variants
            _ => ErrorSeverity::Medium,
        }
    }

    /// Returns the recommended recovery strategy for automated error handling.
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        use ErrorKind::*;
        match &self.0.kind {
            // No retry - permanent failures requiring code changes or configuration fixes
            AuthenticationFailed { .. }
            | ConfigurationError { .. }
            | UnsupportedDataType { .. }
            | StateCorrupted { .. }
            | SchemaValidationFailed { .. }
            | TupleDataNotSupported { .. } => RecoveryStrategy::NoRetry,

            // Immediate retry - transient network issues
            NetworkError | WorkerCancelled { .. } => RecoveryStrategy::RetryImmediate,

            // Retry with backoff - infrastructure issues that may resolve
            ConnectionFailed { .. }
            | ConnectionLost
            | QueryExecutionFailed { .. }
            | TransactionFailed
            | IoError
            | TableCopyStreamFailed { .. }
            | EventsStreamFailed => RecoveryStrategy::RetryWithBackoff,

            // Retry after delay - resource or capacity issues
            ResourceLimitExceeded { .. }
            | Timeout { .. } => RecoveryStrategy::RetryAfterDelay,

            // Manual intervention - critical system issues
            WorkerPanicked { .. }
            | LsnConsistencyError { .. }
            | PipelineShutdownFailed => RecoveryStrategy::ManualIntervention,

            // Default to retry with backoff for safety
            _ => RecoveryStrategy::RetryWithBackoff,
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
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.0.kind)
            .field("source", &self.0.source)
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ErrorKind::*;

        match &self.0.kind {
            ConnectionFailed {
                host,
                port,
                database,
            } => {
                write!(
                    f,
                    "failed to connect to database '{database}' at {host}:{port}"
                )
            }
            AuthenticationFailed { user, database } => {
                write!(
                    f,
                    "authentication failed for user '{user}' on database '{database}'"
                )
            }
            TlsConfigurationFailed => write!(f, "tls configuration failed"),
            QueryExecutionFailed { query } => {
                write!(f, "query execution failed: {query}")
            }
            TransactionFailed => write!(f, "database transaction failed"),
            ConnectionLost => write!(f, "database connection lost"),

            ReplicationSlotNotCreated {
                slot_name,
                reason,
            } => {
                write!(
                    f,
                    "failed to create replication slot '{slot_name}': {reason}"
                )
            }
            ReplicationSlotNotFound { slot_name } => {
                write!(f, "replication slot '{slot_name}' not found")
            }
            ReplicationSlotAlreadyExists { slot_name } => {
                write!(f, "replication slot '{slot_name}' already exists")
            }
            PublicationNotFound { publication_name } => {
                write!(f, "publication '{publication_name}' operation failed")
            }
            TableCopyStreamFailed { table_name } => {
                write!(f, "table copy stream failed for table '{table_name}'")
            }
            EventsStreamFailed => write!(f, "events stream processing failed"),
            LsnConsistencyError {
                expected: expected_lsn,
                actual: actual_lsn,
            } => {
                write!(
                    f,
                    "lsn consistency error: expected {expected_lsn}, got {actual_lsn}"
                )
            }
            TransactionNotStarted => {
                write!(f, "transaction not started but commit message received")
            }
            EventTypeMismatch { expected, actual } => {
                write!(f, "event type mismatch: expected {expected}, got {actual}")
            }
            TableReplicationPhaseInvalid { expected, actual } => {
                write!(
                    f,
                    "table replication phase invalid: expected {expected}, got {actual}"
                )
            }
            WorkerCancelled { worker_type } => {
                write!(f, "{worker_type} worker cancelled")
            }
            WorkerFailedSilently { worker_type, reason } => {
                write!(f, "{worker_type} worker failed silently: {reason}")
            }

            TableNotFound { table_name } => {
                write!(f, "table '{table_name}' not found")
            }
            ColumnNotFound {
                table_name,
                column_name,
            } => {
                write!(
                    f,
                    "column '{column_name}' not found in table '{table_name}'"
                )
            }
            UnsupportedDataType {
                type_name,
                type_oid,
                table_name,
            } => {
                write!(
                    f,
                    "unsupported data type '{type_name}' (oid: {type_oid}) in table '{table_name}'"
                )
            }
            SchemaValidationFailed { table_name, reason } => {
                write!(
                    f,
                    "schema validation failed for table '{table_name}': {reason}"
                )
            }
            TableReplicationStateMissing { table_name } => {
                write!(f, "replication state missing for table '{table_name}'")
            }
            TupleDataNotSupported { type_name } => {
                write!(f, "tuple data not supported for type '{type_name}'")
            }
            PipelineShutdownFailed => {
                write!(f, "pipeline shutdown failed")
            }
            WorkerPanicked { worker_type } => {
                write!(f, "{worker_type} worker panicked")
            }
            WorkerPoolFailed { reason } => {
                write!(f, "worker pool failed: {reason}")
            }

            StateCorrupted { description } => {
                write!(f, "state corrupted: {description}")
            }
            DataConversionFailed {
                from_type,
                to_type,
                value,
            } => match value {
                Some(value) => {
                    write!(
                        f,
                        "failed to convert value '{value}' from {from_type} to {to_type}"
                    )
                }
                None => {
                    write!(f, "failed to convert from {from_type} to {to_type}")
                }
            },

            ConfigurationError { parameter, reason } => {
                write!(
                    f,
                    "configuration error for parameter '{parameter}': {reason}"
                )
            }
            ResourceLimitExceeded { resource, limit } => {
                write!(
                    f,
                    "resource limit exceeded for '{resource}' (limit: {limit})"
                )
            }
            IoError => write!(f, "i/o operation failed"),
            NetworkError => write!(f, "network operation failed"),
            Timeout {
                operation,
                duration_ms,
            } => {
                write!(f, "operation '{operation}' timed out after {duration_ms}ms")
            }

            Many { amount } => {
                write!(f, "batch operation failed with {amount} errors")
            }
            Other { description } => {
                write!(f, "{description}")
            }
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
            match db_err.code().code() {
                // Connection errors (Class 08)
                "08000" | "08003" | "08006" => Self::with_source(ErrorKind::ConnectionLost, err),
                "08001" | "08004" => Self::with_source(
                    ErrorKind::ConnectionFailed {
                        host: "unknown".to_string(),
                        port: 5432,
                        database: "unknown".to_string(),
                    },
                    err,
                ),
                "08P01" => Self::with_source(ErrorKind::NetworkError, err),

                // Authentication errors (Class 28)
                "28000" | "28P01" => Self::with_source(
                    ErrorKind::AuthenticationFailed {
                        user: "unknown".to_string(),
                        database: "unknown".to_string(),
                    },
                    err,
                ),

                // Transaction state errors (Class 25)
                "25000" | "25001" | "25P01" | "25P02" => {
                    Self::with_source(ErrorKind::TransactionFailed, err)
                }

                // Transaction rollback errors (Class 40)
                "40001" | "40002" | "40003" | "40P01" => {
                    Self::with_source(ErrorKind::TransactionFailed, err)
                }

                // Schema/Object errors (Class 42)
                "42P01" => Self::with_source(
                    ErrorKind::TableNotFound {
                        table_name: db_err.table().unwrap_or("unknown").to_string(),
                    },
                    err,
                ),
                "42703" => Self::with_source(
                    ErrorKind::ColumnNotFound {
                        table_name: db_err.table().unwrap_or("unknown").to_string(),
                        column_name: db_err.column().unwrap_or("unknown").to_string(),
                    },
                    err,
                ),
                "42710" if description.contains("replication slot") => {
                    let slot_name = extract_slot_name_from_error(&description)
                        .unwrap_or_else(|| "unknown".to_string());
                    Self::with_source(ErrorKind::ReplicationSlotAlreadyExists { slot_name }, err)
                }
                "42501" => Self::with_source(
                    ErrorKind::AuthenticationFailed {
                        user: "unknown".to_string(),
                        database: "unknown".to_string(),
                    },
                    err,
                ),

                // System resource errors (Class 53)
                "53000" | "53100" | "53200" | "53300" | "53400" => Self::with_source(
                    ErrorKind::ResourceLimitExceeded {
                        resource: match db_err.code().code() {
                            "53100" => "disk_space".to_string(),
                            "53200" => "memory".to_string(),
                            "53300" => "connections".to_string(),
                            _ => "system_resources".to_string(),
                        },
                        limit: "exceeded".to_string(),
                    },
                    err,
                ),

                // Replication-specific errors (Class 55)
                "55000" | "55006" => Self::with_source(
                    ErrorKind::ReplicationSlotNotCreated {
                        slot_name: "unknown".to_string(),
                        reason: "replication_slot_error".to_string(),
                    },
                    err,
                ),

                // Query canceled/shutdown (Class 57)
                "57014" => Self::with_source(
                    ErrorKind::Timeout {
                        operation: "query".to_string(),
                        duration_ms: 0,
                    },
                    err,
                ),
                "57000" | "57P01" | "57P02" | "57P03" => {
                    Self::with_source(ErrorKind::ConnectionLost, err)
                }

                // Invalid catalog/schema name (Class 3D/3F)
                "3D000" | "3F000" => Self::with_source(
                    ErrorKind::ConnectionFailed {
                        host: "unknown".to_string(),
                        port: 5432,
                        database: "unknown".to_string(),
                    },
                    err,
                ),

                // Data exceptions (Class 22)
                "22000" | "22001" | "22003" | "22007" | "22012" | "22P02" | "22P03" => {
                    Self::with_source(
                        ErrorKind::DataConversionFailed {
                            from_type: "postgres".to_string(),
                            to_type: "target".to_string(),
                            value: None,
                        },
                        err,
                    )
                }

                // Constraint violations (Class 23)
                "23000" | "23502" | "23503" | "23505" | "23514" | "23P01" => Self::with_source(
                    ErrorKind::SchemaValidationFailed {
                        table_name: db_err.table().unwrap_or("unknown").to_string(),
                        reason: "constraint_violation".to_string(),
                    },
                    err,
                ),

                // Generic query execution error for unhandled cases
                _ => Self::with_source(
                    ErrorKind::QueryExecutionFailed {
                        query: "unknown".to_string(),
                    },
                    err,
                ),
            }
        } else {
            // Non-database errors (connection issues, etc.)
            if description.contains("connection") || description.contains("Connection") {
                Self::with_source(ErrorKind::ConnectionLost, err)
            } else if description.contains("authentication") || description.contains("password") {
                Self::with_source(
                    ErrorKind::AuthenticationFailed {
                        user: "unknown".to_string(),
                        database: "unknown".to_string(),
                    },
                    err,
                )
            } else if description.contains("tls") || description.contains("ssl") {
                Self::with_source(ErrorKind::TlsConfigurationFailed, err)
            } else if description.contains("timeout") || description.contains("Timeout") {
                Self::with_source(
                    ErrorKind::Timeout {
                        operation: "connection".to_string(),
                        duration_ms: 0,
                    },
                    err,
                )
            } else {
                Self::with_source(ErrorKind::Other { description }, err)
            }
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        let description = err.to_string();

        match &err {
            // Configuration errors
            sqlx::Error::Configuration(_) => Self::with_source(
                ErrorKind::ConfigurationError {
                    parameter: "database".to_string(),
                    reason: description,
                },
                err,
            ),

            // Database-specific errors with SQLSTATE handling
            sqlx::Error::Database(db_err) => {
                let code = db_err.code().unwrap_or(borrow::Cow::Borrowed("unknown"));
                match code.as_ref() {
                    // Connection errors (Class 08)
                    "08000" | "08003" | "08006" => {
                        Self::with_source(ErrorKind::ConnectionLost, err)
                    }
                    "08001" | "08004" => Self::with_source(
                        ErrorKind::ConnectionFailed {
                            host: "unknown".to_string(),
                            port: 5432,
                            database: "unknown".to_string(),
                        },
                        err,
                    ),

                    // Authentication errors (Class 28)
                    "28000" | "28P01" => Self::with_source(
                        ErrorKind::AuthenticationFailed {
                            user: "unknown".to_string(),
                            database: "unknown".to_string(),
                        },
                        err,
                    ),

                    // Schema/Object errors (Class 42)
                    "42P01" => Self::with_source(
                        ErrorKind::TableNotFound {
                            table_name: "unknown".to_string(),
                        },
                        err,
                    ),
                    "42703" => Self::with_source(
                        ErrorKind::ColumnNotFound {
                            table_name: "unknown".to_string(),
                            column_name: "unknown".to_string(),
                        },
                        err,
                    ),

                    // Transaction errors (Class 25, 40)
                    "25000" | "25001" | "25P01" | "25P02" | "40001" | "40002" | "40003"
                    | "40P01" => Self::with_source(ErrorKind::TransactionFailed, err),

                    // Data exceptions (Class 22)
                    "22000" | "22001" | "22003" | "22007" | "22012" | "22P02" | "22P03" => {
                        Self::with_source(
                            ErrorKind::DataConversionFailed {
                                from_type: "postgres".to_string(),
                                to_type: "target".to_string(),
                                value: None,
                            },
                            err,
                        )
                    }

                    // Default to query execution error
                    _ => Self::with_source(
                        ErrorKind::QueryExecutionFailed {
                            query: "unknown".to_string(),
                        },
                        err,
                    ),
                }
            }

            // Communication and infrastructure errors
            sqlx::Error::Io(_) => Self::with_source(ErrorKind::IoError, err),
            sqlx::Error::Tls(_) => Self::with_source(ErrorKind::TlsConfigurationFailed, err),
            sqlx::Error::Protocol(_) => Self::with_source(ErrorKind::NetworkError, err),

            // Data handling errors
            sqlx::Error::RowNotFound => Self::with_source(
                ErrorKind::Other {
                    description: "database row not found".to_string(),
                },
                err,
            ),
            sqlx::Error::TypeNotFound { type_name } => Self::with_source(
                ErrorKind::UnsupportedDataType {
                    type_name: type_name.clone(),
                    type_oid: 0,
                    table_name: "unknown".to_string(),
                },
                err,
            ),
            sqlx::Error::ColumnNotFound(column_name) => Self::with_source(
                ErrorKind::ColumnNotFound {
                    table_name: "unknown".to_string(),
                    column_name: column_name.clone(),
                },
                err,
            ),
            sqlx::Error::ColumnIndexOutOfBounds { index, .. } => Self::with_source(
                ErrorKind::ColumnNotFound {
                    table_name: "unknown".to_string(),
                    column_name: format!("index_{index}"),
                },
                err,
            ),

            // Data conversion errors - critical for ETL
            sqlx::Error::ColumnDecode { index, .. } => Self::with_source(
                ErrorKind::DataConversionFailed {
                    from_type: "database".to_string(),
                    to_type: "rust".to_string(),
                    value: Some(format!("column_{index}")),
                },
                err,
            ),
            sqlx::Error::Encode(_) => Self::with_source(
                ErrorKind::DataConversionFailed {
                    from_type: "rust".to_string(),
                    to_type: "database".to_string(),
                    value: None,
                },
                err,
            ),
            sqlx::Error::Decode(_) => Self::with_source(
                ErrorKind::DataConversionFailed {
                    from_type: "database".to_string(),
                    to_type: "rust".to_string(),
                    value: None,
                },
                err,
            ),

            // Connection pool errors
            sqlx::Error::PoolTimedOut => Self::with_source(
                ErrorKind::Timeout {
                    operation: "database_connection_pool".to_string(),
                    duration_ms: 0,
                },
                err,
            ),
            sqlx::Error::PoolClosed => Self::with_source(ErrorKind::ConnectionLost, err),

            // Transaction and system errors
            sqlx::Error::BeginFailed => Self::with_source(ErrorKind::TransactionFailed, err),
            sqlx::Error::WorkerCrashed => Self::with_source(
                ErrorKind::WorkerPoolFailed {
                    reason: "background_worker_crashed".to_string(),
                },
                err,
            ),

            // Catch-all for remaining variants
            _ => Self::with_source(ErrorKind::Other { description }, err),
        }
    }
}

impl From<rustls::Error> for Error {
    fn from(err: rustls::Error) -> Self {
        Self::with_source(ErrorKind::TlsConfigurationFailed, err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::with_source(ErrorKind::IoError, err)
    }
}

/// Extracts replication slot name from PostgreSQL error messages.
/// 
/// Searches for quoted slot names in error text using common PostgreSQL patterns.
fn extract_slot_name_from_error(error_msg: &str) -> Option<String> {
    if let Some(start) = error_msg.find("slot \"") {
        let start = start + 6; // Skip 'slot "'
        if let Some(end) = error_msg[start..].find('"') {
            return Some(error_msg[start..start + end].to_string());
        }
    }

    if let Some(start) = error_msg.find("slot `") {
        let start = start + 6; // Skip 'slot `'
        if let Some(end) = error_msg[start..].find('`') {
            return Some(error_msg[start..start + end].to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::workers::base::WorkerType;
    use std::io;

    #[test]
    fn test_error_kind_creation() {
        let error = Error::table_not_found("users");
        assert!(matches!(error.kind(), ErrorKind::TableNotFound { .. }));
        
        let error = Error::column_not_found("posts", "invalid_col");
        assert!(matches!(error.kind(), ErrorKind::ColumnNotFound { .. }));
        
        let error = Error::replication_slot_not_found("test_slot");
        assert!(matches!(error.kind(), ErrorKind::ReplicationSlotNotFound { .. }));
        
        let error = Error::transaction_not_started();
        assert!(matches!(error.kind(), ErrorKind::TransactionNotStarted));
        
        let error = Error::event_type_mismatch("INSERT", "DELETE");
        assert!(matches!(error.kind(), ErrorKind::EventTypeMismatch { .. }));
        
        let error = Error::other("custom error message");
        assert!(matches!(error.kind(), ErrorKind::Other { .. }));
    }

    #[test]
    fn test_error_display_messages() {
        let error = Error::new(ErrorKind::ConnectionFailed {
            host: "localhost".to_string(),
            port: 5432,
            database: "test_db".to_string(),
        });
        let display = format!("{error}");
        assert!(display.contains("failed to connect to database 'test_db' at localhost:5432"));

        let error = Error::new(ErrorKind::AuthenticationFailed {
            user: "testuser".to_string(),
            database: "testdb".to_string(),
        });
        let display = format!("{error}");
        assert!(display.contains("authentication failed for user 'testuser' on database 'testdb'"));

        let error = Error::new(ErrorKind::ReplicationSlotNotCreated {
            slot_name: "test_slot".to_string(),
            reason: "already_exists".to_string(),
        });
        let display = format!("{error}");
        assert!(display.contains("failed to create replication slot 'test_slot': already_exists"));

        let error = Error::new(ErrorKind::UnsupportedDataType {
            type_name: "unknown_type".to_string(),
            type_oid: 12345,
            table_name: "test_table".to_string(),
        });
        let display = format!("{error}");
        assert!(display.contains("unsupported data type 'unknown_type' (oid: 12345) in table 'test_table'"));

        let error = Error::new(ErrorKind::WorkerPanicked {
            worker_type: WorkerType::Apply,
        });
        let display = format!("{error}");
        assert!(display.contains("apply worker panicked"));

        let error = Error::new(ErrorKind::DataConversionFailed {
            from_type: "postgres".to_string(),
            to_type: "rust".to_string(),
            value: Some("invalid_value".to_string()),
        });
        let display = format!("{error}");
        assert!(display.contains("failed to convert value 'invalid_value' from postgres to rust"));

        let error = Error::new(ErrorKind::DataConversionFailed {
            from_type: "postgres".to_string(),
            to_type: "rust".to_string(),
            value: None,
        });
        let display = format!("{error}");
        assert!(display.contains("failed to convert from postgres to rust"));

        let error = Error::new(ErrorKind::Many { amount: 3 });
        let display = format!("{error}");
        assert!(display.contains("batch operation failed with 3 errors"));

        let error = Error::new(ErrorKind::Other {
            description: "something went wrong".to_string(),
        });
        let display = format!("{error}");
        assert_eq!(display, "something went wrong");
    }

    #[test]
    fn test_error_display_chain_single_error() {
        let error = Error::table_not_found("users");
        let chain = error.display_chain();
        assert_eq!(chain, "table 'users' not found");
    }

    #[test]
    fn test_error_display_chain_with_source() {
        let io_error = io::Error::new(io::ErrorKind::ConnectionRefused, "TCP connection refused");
        let error = Error::with_source(
            ErrorKind::ConnectionFailed {
                host: "localhost".to_string(),
                port: 5432,
                database: "test_db".to_string(),
            },
            io_error,
        );
        
        let chain = error.display_chain();
        assert!(chain.contains("failed to connect to database 'test_db' at localhost:5432"));
        assert!(chain.contains("↳ caused by: TCP connection refused"));
    }

    #[test]
    fn test_error_display_chain_nested_sources() {
        // Create a chain: IO Error -> Connection Error -> Worker Pool Error
        let io_error = io::Error::new(io::ErrorKind::TimedOut, "operation timed out");
        
        let connection_error = Error::with_source(
            ErrorKind::ConnectionFailed {
                host: "db.example.com".to_string(),
                port: 5432,
                database: "production".to_string(),
            },
            io_error,
        );
        
        let worker_error = Error::with_source(
            ErrorKind::WorkerPoolFailed {
                reason: "database_unavailable".to_string(),
            },
            connection_error,
        );
        
        let chain = worker_error.display_chain();
        let lines: Vec<&str> = chain.lines().collect();
        
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("worker pool failed: database_unavailable"));
        assert!(lines[1].contains("↳ caused by: failed to connect to database 'production' at db.example.com:5432"));
        assert!(lines[2].contains("↳ caused by: operation timed out"));
    }

    #[test]
    fn test_errors_collection_empty() {
        let errors = Errors::from(vec![]);
        let display = format!("{errors}");
        assert_eq!(display, "no errors");
    }

    #[test]
    fn test_errors_collection_single() {
        let error = Error::table_not_found("users");
        let errors = Errors::from(vec![error]);
        let display = format!("{errors}");
        assert_eq!(display, "table 'users' not found");
    }

    #[test]
    fn test_errors_collection_multiple() {
        let error1 = Error::table_not_found("users");
        let error2 = Error::column_not_found("posts", "invalid_column");
        let error3 = Error::other("custom error");
        
        let errors = Errors::from(vec![error1, error2, error3]);
        let display = format!("{errors}");
        
        let lines: Vec<&str> = display.lines().collect();
        assert!(lines[0].contains("batch operation failed with 3 errors:"));
        assert!(lines[1].contains("1. table 'users' not found"));
        assert!(lines[2].contains("2. column 'invalid_column' not found in table 'posts'"));
        assert!(lines[3].contains("3. custom error"));
    }

    #[test]
    fn test_errors_collection_with_chained_errors() {
        let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let chained_error = Error::with_source(
            ErrorKind::ConnectionFailed {
                host: "localhost".to_string(),
                port: 5432,
                database: "secure_db".to_string(),
            },
            io_error,
        );
        
        let simple_error = Error::table_not_found("missing_table");
        
        let errors = Errors::from(vec![chained_error, simple_error]);
        let display = format!("{errors}");
        
        assert!(display.contains("batch operation failed with 2 errors:"));
        assert!(display.contains("1. failed to connect to database 'secure_db' at localhost:5432"));
        assert!(display.contains("↳ caused by: access denied"));
        assert!(display.contains("2. table 'missing_table' not found"));
    }

    #[test]
    fn test_error_from_many() {
        let errors = vec![
            Error::table_not_found("users"),
            Error::column_not_found("posts", "title"),
        ];
        
        let many_error = Error::from_many(errors);
        assert!(matches!(many_error.kind(), ErrorKind::Many { amount: 2 }));
        
        let display = format!("{many_error}");
        assert!(display.contains("batch operation failed with 2 errors"));
    }

    #[test]
    fn test_error_severity_levels() {
        // Critical errors
        let error = Error::new(ErrorKind::StateCorrupted {
            description: "state file corrupted".to_string(),
        });
        assert_eq!(error.severity(), ErrorSeverity::Critical);
        
        let error = Error::new(ErrorKind::WorkerPanicked {
            worker_type: WorkerType::Apply,
        });
        assert_eq!(error.severity(), ErrorSeverity::Critical);
        
        let error = Error::new(ErrorKind::LsnConsistencyError {
            expected: "0/1234".to_string(),
            actual: "0/5678".to_string(),
        });
        assert_eq!(error.severity(), ErrorSeverity::Critical);

        // High severity errors
        let error = Error::new(ErrorKind::ConnectionLost);
        assert_eq!(error.severity(), ErrorSeverity::High);
        
        let error = Error::new(ErrorKind::ReplicationSlotNotCreated {
            slot_name: "test".to_string(),
            reason: "conflict".to_string(),
        });
        assert_eq!(error.severity(), ErrorSeverity::High);

        // Medium severity errors
        let error = Error::new(ErrorKind::ConnectionFailed {
            host: "localhost".to_string(),
            port: 5432,
            database: "test".to_string(),
        });
        assert_eq!(error.severity(), ErrorSeverity::Medium);
        
        let error = Error::new(ErrorKind::TableNotFound {
            table_name: "users".to_string(),
        });
        assert_eq!(error.severity(), ErrorSeverity::Medium);

        // Low severity errors
        let error = Error::new(ErrorKind::NetworkError);
        assert_eq!(error.severity(), ErrorSeverity::Low);
        
        let error = Error::new(ErrorKind::DataConversionFailed {
            from_type: "postgres".to_string(),
            to_type: "rust".to_string(),
            value: None,
        });
        assert_eq!(error.severity(), ErrorSeverity::Low);
    }

    #[test]
    fn test_error_recovery_strategies() {
        // No retry
        let error = Error::new(ErrorKind::AuthenticationFailed {
            user: "test".to_string(),
            database: "test".to_string(),
        });
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::NoRetry);
        assert!(error.is_permanent());
        assert!(!error.is_retryable());
        
        let error = Error::new(ErrorKind::UnsupportedDataType {
            type_name: "unknown".to_string(),
            type_oid: 0,
            table_name: "test".to_string(),
        });
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::NoRetry);

        // Immediate retry
        let error = Error::new(ErrorKind::NetworkError);
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::RetryImmediate);
        assert!(error.is_retryable());
        assert!(!error.is_permanent());

        // Retry with backoff
        let error = Error::new(ErrorKind::ConnectionLost);
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::RetryWithBackoff);
        assert!(error.is_retryable());
        
        let error = Error::new(ErrorKind::QueryExecutionFailed {
            query: "SELECT * FROM test".to_string(),
        });
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::RetryWithBackoff);

        // Retry after delay
        let error = Error::new(ErrorKind::ResourceLimitExceeded {
            resource: "memory".to_string(),
            limit: "1GB".to_string(),
        });
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::RetryAfterDelay);
        assert!(error.is_retryable());

        // Manual intervention
        let error = Error::new(ErrorKind::WorkerPanicked {
            worker_type: WorkerType::Apply,
        });
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::ManualIntervention);
        assert!(error.is_permanent());
        
        let error = Error::new(ErrorKind::LsnConsistencyError {
            expected: "0/1234".to_string(),
            actual: "0/5678".to_string(),
        });
        assert_eq!(error.recovery_strategy(), RecoveryStrategy::ManualIntervention);
    }

    #[test]
    fn test_extract_slot_name_from_error() {
        // Test double quotes
        let msg = "replication slot \"my_slot\" already exists";
        assert_eq!(extract_slot_name_from_error(msg), Some("my_slot".to_string()));
        
        // Test backticks
        let msg = "cannot create slot `another_slot` due to conflict";
        assert_eq!(extract_slot_name_from_error(msg), Some("another_slot".to_string()));
        
        // Test no slot name found
        let msg = "some other database error occurred";
        assert_eq!(extract_slot_name_from_error(msg), None);
        
        // Test malformed slot reference
        let msg = "slot without quotes mentioned here";
        assert_eq!(extract_slot_name_from_error(msg), None);
        
        // Test slot name with special characters
        let msg = "slot \"my-slot_123\" operation failed";
        assert_eq!(extract_slot_name_from_error(msg), Some("my-slot_123".to_string()));
        
        // Test empty slot name
        let msg = "slot \"\" is invalid";
        assert_eq!(extract_slot_name_from_error(msg), Some("".to_string()));
        
        // Test unclosed quote
        let msg = "slot \"unclosed_slot operation failed";
        assert_eq!(extract_slot_name_from_error(msg), None);
    }

    #[test]
    fn test_from_implementations() {
        // Test basic io::Error conversion
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
        let error: Error = io_err.into();
        
        // Should map to IoError kind
        assert!(matches!(error.kind(), ErrorKind::IoError));
        let display = format!("{error}");
        assert_eq!(display, "i/o operation failed");
        
        // Test that source is preserved
        let chain = error.display_chain();
        assert!(chain.contains("i/o operation failed"));
        assert!(chain.contains("caused by: permission denied"));
        
        // Test rustls::Error conversion
        let tls_err = rustls::Error::General("invalid certificate".to_string());
        let error: Error = tls_err.into();
        assert!(matches!(error.kind(), ErrorKind::TlsConfigurationFailed));
    }

    #[test]
    fn test_error_debug_format() {
        let error = Error::with_source(
            ErrorKind::TableNotFound {
                table_name: "users".to_string(),
            },
            io::Error::new(io::ErrorKind::NotFound, "file not found"),
        );
        
        let debug = format!("{error:?}");
        assert!(debug.contains("TableNotFound"));
        assert!(debug.contains("users"));
    }

    #[test]
    fn test_errors_debug_format() {
        let errors = Errors::from(vec![
            Error::table_not_found("users"),
            Error::column_not_found("posts", "title"),
        ]);
        
        let debug = format!("{errors:?}");
        assert!(debug.contains("count"));
        assert!(debug.contains("2"));
    }
}
