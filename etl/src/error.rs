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
/// This enum covers all major error categories found in PostgreSQL replication
/// applications, providing structured error information for proper handling
/// and recovery strategies.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
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

    /// The table replication state is missing for a table
    TableReplicationStateMissing { table_name: String },

    /// Replication slot operation failure
    ReplicationSlotNotCreated {
        slot_name: String,
        reason: String
    },
    /// Replication slot not found in database
    ReplicationSlotNotFound { slot_name: String },
    /// Attempt to create replication slot that already exists
    ReplicationSlotAlreadyExists { slot_name: String },
    /// Publication not found or empty
    PublicationNotFound { publication_name: String },
    /// CDC stream parsing or processing failure
    CdcStreamFailed,
    /// CDC stream connection timeout or loss
    CdcStreamConnectionLost,
    /// Table copy stream failure
    TableCopyStreamFailed { table_name: String },
    /// Events stream failure
    EventsStreamFailed,
    /// LSN inconsistency or invalid state transition
    LsnConsistencyError {
        expected: String,
        actual: String,
    },
    /// The transaction did not start but a `COMMIT` message was encountered
    TransactionNotStarted,
    /// The event type that we received was not expected
    EventTypeMismatch { expected: String, actual: String },
    /// The replication phase was unexpected
    TableReplicationPhaseInvalid { expected: String, actual: String },

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
    /// Missing or inadequate replica identity for table
    ReplicaIdentityIssue { table_name: String },

    /// Worker startup failure
    WorkerStartupFailed { worker_type: String },
    /// Pipeline shutdown failed
    PipelineShutdownFailed,
    /// Worker task panicked during execution
    WorkerPanicked { worker_type: WorkerType },
    /// Worker task cancelled during execution
    WorkerCancelled { worker_type: WorkerType },
    /// A table sync worker experienced a silent failure
    WorkerFailedSilently {
        worker_type: WorkerType,
        reason: String,
    },
    /// Worker pool capacity or coordination failure
    WorkerPoolFailed { reason: String },

    /// Destination connection or authentication failure
    DestinationConnectionFailed { destination_type: String },
    /// Destination table creation failure
    DestinationTableCreationFailed {
        table_name: String,
        destination_type: String,
    },
    /// Schema mismatch between source and destination
    DestinationSchemaMismatch { table_name: String, reason: String },
    /// Data insertion failure at destination
    DestinationInsertionFailed {
        table_name: String,
        destination_type: String,
    },
    /// Destination-specific provider error
    DestinationProviderError {
        provider: String,
        error_code: Option<String>,
    },
    /// Write capacity or quota exceeded
    DestinationQuotaExceeded { destination_type: String },

    /// The data of the tuple is not supported
    TupleDataNotSupported { type_name: String },

    /// State store read operation failure
    StateStoreReadFailed { key: String },
    /// State store write operation failure
    StateStoreWriteFailed { key: String },
    /// State store delete operation failure
    StateStoreDeleteFailed { key: String },
    /// State corruption detected
    StateCorrupted { description: String },
    /// State lock acquisition timeout
    StateLockTimeout { resource: String },
    /// Checkpoint operation failure
    CheckpointFailed { reason: String },
    /// Recovery operation failure
    RecoveryFailed { reason: String },

    /// Data type conversion failure
    DataConversionFailed {
        from_type: String,
        to_type: String,
        value: Option<String>,
    },
    /// JSON serialization failure
    JsonSerializationFailed,
    /// JSON deserialization failure
    JsonDeserializationFailed,
    /// Encryption operation failure
    EncryptionFailed,
    /// Decryption operation failure
    DecryptionFailed,

    /// Configuration parsing or validation failure
    ConfigurationError { parameter: String, reason: String },
    /// Resource limit exceeded (memory, disk, connections)
    ResourceLimitExceeded { resource: String, limit: String },
    /// I/O operation failure
    IoError,
    /// Network operation failure
    NetworkError,
    /// Timeout during operation
    Timeout { operation: String, duration_ms: u64 },

    /// Error that contains many errors
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
                write!(f, "{count} errors: ")?;
                for (i, error) in self.0.iter().enumerate() {
                    if i > 0 {
                        write!(f, "; ")?;
                    }
                    write!(f, "{error}")?;
                }
                Ok(())
            }
        }
    }
}

impl error::Error for Errors {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // We return only the first error as the source, since we can't do better.
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

    pub fn from_many(errors: impl Into<Errors>) -> Self {
        let errors = errors.into();
        Error::with_source(
            ErrorKind::Many {
                amount: errors.0.len() as u64,
            },
            errors,
        )
    }

    pub fn transaction_not_started() -> Self {
        Self::new(ErrorKind::TransactionNotStarted)
    }

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

    /// Creates an external error.
    pub fn other(description: impl Into<String>) -> Self {
        Self::new(ErrorKind::Other {
            description: description.into(),
        })
    }

    /// Returns the error kind.
    pub fn kind(&self) -> &ErrorKind {
        &self.0.kind
    }

    /// Returns the severity level of this error.
    pub fn severity(&self) -> ErrorSeverity {
        use ErrorKind::*;
        match &self.0.kind {
            // Critical errors
            StateCorrupted { .. } | WorkerPanicked { .. } | LsnConsistencyError { .. } => {
                ErrorSeverity::Critical
            }

            // High severity errors
            ConnectionLost
            | ReplicationSlotNotCreated { .. }
            | DestinationQuotaExceeded { .. }
            | WorkerPoolFailed { .. }
            | CheckpointFailed { .. }
            | RecoveryFailed { .. } => ErrorSeverity::High,

            // Medium severity errors
            ConnectionFailed { .. }
            | AuthenticationFailed { .. }
            | QueryExecutionFailed { .. }
            | TableNotFound { .. }
            | UnsupportedDataType { .. }
            | DestinationConnectionFailed { .. }
            | StateStoreReadFailed { .. }
            | StateStoreWriteFailed { .. }
            | ResourceLimitExceeded { .. } => ErrorSeverity::Medium,

            // Low severity errors (transient or expected)
            CdcStreamConnectionLost
            | Timeout { .. }
            | NetworkError
            | StateLockTimeout { .. }
            | DataConversionFailed { .. } => ErrorSeverity::Low,

            // Default to medium for unclassified errors
            _ => ErrorSeverity::Medium,
        }
    }

    /// Returns the recommended recovery strategy for this error.
    pub fn recovery_strategy(&self) -> RecoveryStrategy {
        use ErrorKind::*;
        match &self.0.kind {
            // No retry errors
            AuthenticationFailed { .. }
            | ConfigurationError { .. }
            | UnsupportedDataType { .. }
            | StateCorrupted { .. }
            | SchemaValidationFailed { .. }
            | ReplicaIdentityIssue { .. } => RecoveryStrategy::NoRetry,

            // Immediate retry errors
            CdcStreamConnectionLost | NetworkError => RecoveryStrategy::RetryImmediate,

            // Retry with backoff errors
            ConnectionFailed { .. }
            | ConnectionLost
            | QueryExecutionFailed { .. }
            | DestinationConnectionFailed { .. }
            | StateStoreReadFailed { .. }
            | StateStoreWriteFailed { .. }
            | IoError => RecoveryStrategy::RetryWithBackoff,

            // Retry after delay errors
            DestinationQuotaExceeded { .. }
            | ResourceLimitExceeded { .. }
            | StateLockTimeout { .. } => RecoveryStrategy::RetryAfterDelay,

            // Manual intervention required
            WorkerPanicked { .. }
            | CheckpointFailed { .. }
            | RecoveryFailed { .. }
            | LsnConsistencyError { .. } => RecoveryStrategy::ManualIntervention,

            // Default to retry with backoff
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
                operation,
            } => {
                write!(
                    f,
                    "replication slot '{slot_name}' operation '{operation}' failed"
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
            CdcStreamFailed => write!(f, "cdc stream processing failed"),
            CdcStreamConnectionLost => write!(f, "cdc stream connection lost"),
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
            ReplicaIdentityIssue { table_name } => {
                write!(f, "replica identity issue for table '{table_name}'")
            }

            WorkerStartupFailed { worker_type } => {
                write!(f, "{worker_type} worker startup failed")
            }
            PipelineShutdownFailed => {
                write!(f, "pipeline shutdown failed")
            }
            WorkerPanicked { worker_type } => {
                write!(f, "{worker_type} worker panicked")
            }
            TableSyncWorkerFailed { table_name } => {
                write!(f, "table sync worker failed for table '{table_name}'")
            }
            ApplyWorkerFailed => write!(f, "apply worker failed"),
            WorkerPoolFailed { reason } => {
                write!(f, "worker pool failed: {reason}")
            }

            DestinationConnectionFailed { destination_type } => {
                write!(f, "{destination_type} destination connection failed")
            }
            DestinationTableCreationFailed {
                table_name,
                destination_type,
            } => {
                write!(
                    f,
                    "failed to create table '{table_name}' in {destination_type} destination"
                )
            }
            DestinationSchemaMismatch { table_name, reason } => {
                write!(f, "schema mismatch for table '{table_name}': {reason}")
            }
            DestinationInsertionFailed {
                table_name,
                destination_type,
            } => {
                write!(
                    f,
                    "failed to insert data into table '{table_name}' in {destination_type} destination"
                )
            }
            DestinationProviderError {
                provider,
                error_code,
            } => match error_code {
                Some(code) => write!(f, "{provider} provider error (code: {code})"),
                None => write!(f, "{provider} provider error"),
            },
            DestinationQuotaExceeded { destination_type } => {
                write!(f, "{destination_type} destination quota exceeded")
            }

            StateStoreReadFailed { key } => {
                write!(f, "failed to read from state store (key: {key})")
            }
            StateStoreWriteFailed { key } => {
                write!(f, "failed to write to state store (key: {key})")
            }
            StateStoreDeleteFailed { key } => {
                write!(f, "failed to delete from state store (key: {key})")
            }
            StateCorrupted { description } => {
                write!(f, "state corrupted: {description}")
            }
            StateLockTimeout { resource } => {
                write!(f, "state lock timeout for resource '{resource}'")
            }
            CheckpointFailed { reason } => {
                write!(f, "checkpoint failed: {reason}")
            }
            RecoveryFailed { reason } => {
                write!(f, "recovery failed: {reason}")
            }

            DataConversionFailed {
                from_type,
                to_type,
                value,
            } => match value {
                Some(value) => {
                    write!(
                        f,
                        "failed to convert '{value}' from {from_type} to {to_type}"
                    )
                }
                None => {
                    write!(f, "failed to convert '{from_type}' to '{to_type}'")
                }
            },
            JsonSerializationFailed => write!(f, "json serialization failed"),
            JsonDeserializationFailed => write!(f, "json deserialization failed"),
            BinaryParsingFailed { data_type } => {
                write!(f, "binary parsing failed for data type '{data_type}'")
            }
            EncryptionFailed => write!(f, "encryption failed"),
            DecryptionFailed => write!(f, "decryption failed"),

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
                write!(f, "{amount} errors occurred")
            }

            Other { description } => {
                write!(f, "other error: {description}")
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
                        operation: "unknown".to_string(),
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
                ErrorKind::StateStoreReadFailed {
                    key: "unknown".to_string(),
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
                    column_name: format!("index_{}", index),
                },
                err,
            ),

            // Data conversion errors - critical for ETL
            sqlx::Error::ColumnDecode { index, .. } => Self::with_source(
                ErrorKind::DataConversionFailed {
                    from_type: "database".to_string(),
                    to_type: "rust".to_string(),
                    value: Some(format!("column_{}", index)),
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

// Helper function to extract replication slot name from error messages
fn extract_slot_name_from_error(error_msg: &str) -> Option<String> {
    // Look for patterns like 'slot "slot_name"' or 'slot `slot_name`'
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
