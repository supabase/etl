//! Error types and result definitions for ETL operations.
//!
//! Provides a comprehensive error system with classification, aggregation, and captured
//! diagnostic metadata for ETL pipeline operations. The [`EtlError`] type supports single errors,
//! errors with additional detail, and multiple aggregated errors for complex failure scenarios.

use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::panic::Location;
use std::sync::Arc;

use crate::conversions::numeric::ParseNumericError;

/// Convenient result type for ETL operations using [`EtlError`] as the error type.
///
/// This type alias reduces boilerplate when working with fallible ETL operations.
/// Most ETL functions return this type.
pub type EtlResult<T> = Result<T, EtlError>;

/// Detailed payload stored for single [`EtlError`] instances.
#[derive(Debug, Clone)]
struct ErrorPayload {
    kind: ErrorKind,
    description: Cow<'static, str>,
    detail: Option<Cow<'static, str>>,
    source: Option<Arc<dyn error::Error + Send + Sync>>,
    location: &'static Location<'static>,
    backtrace: Arc<Backtrace>,
}

impl ErrorPayload {
    /// Creates a new payload with optional dynamic detail.
    fn new(
        kind: ErrorKind,
        description: Cow<'static, str>,
        detail: Option<Cow<'static, str>>,
        source: Option<Arc<dyn error::Error + Send + Sync>>,
        location: &'static Location<'static>,
        backtrace: Arc<Backtrace>,
    ) -> Self {
        Self {
            kind,
            description,
            detail,
            source,
            location,
            backtrace,
        }
    }
}

/// Main error type for ETL operations.
///
/// [`EtlError`] provides a comprehensive error system that can represent single errors,
/// errors with additional detail, or multiple aggregated errors. The design allows for
/// rich error information while maintaining ergonomic usage patterns.
#[derive(Debug, Clone)]
pub struct EtlError {
    repr: ErrorRepr,
}

/// Internal representation of error data.
///
/// This enum supports different error patterns while maintaining a unified interface.
/// Users should not interact with this type directly but use [`EtlError`] methods instead.
#[derive(Debug, Clone)]
enum ErrorRepr {
    /// Single error payload holding rich metadata.
    Single(ErrorPayload),
    /// Multiple aggregated errors.
    ///
    /// This variant is mainly useful to capture multiple workers failures.
    Many {
        errors: Vec<EtlError>,
        location: &'static Location<'static>,
    },
}

/// Specific categories of errors that can occur during ETL operations.
///
/// This enum provides granular error classification to enable appropriate error handling
/// strategies. Error kinds are organized by functional area and failure mode.
#[derive(PartialEq, Eq, Copy, Clone, Debug, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    // Connection Errors
    SourceConnectionFailed,
    DestinationConnectionFailed,

    // Query & Execution Errors
    SourceQueryFailed,
    DestinationQueryFailed,
    SourceLockTimeout,
    SourceOperationCanceled,

    // Schema & Mapping Errors
    SourceSchemaError,
    MissingTableSchema,
    MissingTableMapping,
    DestinationTableNameInvalid,
    DestinationNamespaceAlreadyExists,
    DestinationTableAlreadyExists,
    DestinationNamespaceMissing,
    DestinationTableMissing,

    // Data & Transformation Errors
    ConversionError,
    InvalidData,
    ValidationError,
    NullValuesNotSupportedInArrayInDestination,
    UnsupportedValueInDestination,

    // Configuration & Limit Errors
    ConfigError,
    SourceConfigurationLimitExceeded,

    // IO & Serialization Errors
    IoError,
    SourceIoError,
    DestinationIoError,
    SerializationError,
    DeserializationError,

    // Security & Authentication Errors
    EncryptionError,
    AuthenticationError,
    PermissionDenied,

    // State & Workflow Errors
    InvalidState,
    ApplyWorkerPanic,
    TableSyncWorkerPanic,
    HeartbeatWorkerPanic,
    StateRollbackError,

    // Replication Errors
    ReplicationSlotNotFound,
    ReplicationSlotAlreadyExists,
    ReplicationSlotNotCreated,
    SourceSnapshotTooOld,
    SourceDatabaseInRecovery,
    SourceDatabaseShutdown,

    // General Errors
    SourceError,
    DestinationError,

    // Unknown / Uncategorized
    Unknown,

    // Special error kinds used for tests that trigger specific retry behaviors via fault injection.
    #[cfg(feature = "failpoints")]
    WithNoRetry,
    #[cfg(feature = "failpoints")]
    WithManualRetry,
    #[cfg(feature = "failpoints")]
    WithTimedRetry,
}

impl EtlError {
    /// Returns the [`ErrorKind`] of this error.
    ///
    /// For multiple errors, returns the kind of the first error or [`ErrorKind::Unknown`]
    /// if the error list is empty.
    pub fn kind(&self) -> ErrorKind {
        match self.repr {
            ErrorRepr::Single(ref payload) => payload.kind,
            ErrorRepr::Many { ref errors, .. } => errors
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
            ErrorRepr::Single(ref payload) => vec![payload.kind],
            ErrorRepr::Many { ref errors, .. } => errors
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
            ErrorRepr::Single(ref payload) => payload.detail.as_deref(),
            ErrorRepr::Many { ref errors, .. } => {
                // For multiple errors, return the detail of the first error that has one.
                errors.iter().find_map(|e| e.detail())
            }
        }
    }

    /// Returns the captured backtrace for this error.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match self.repr {
            ErrorRepr::Single(ref payload) => Some(payload.backtrace.as_ref()),
            ErrorRepr::Many { .. } => None,
        }
    }

    /// Returns the captured callsite location for this error.
    pub fn location(&self) -> &'static Location<'static> {
        match self.repr {
            ErrorRepr::Single(ref payload) => payload.location,
            ErrorRepr::Many { location, .. } => location,
        }
    }

    /// Attaches an originating [`error::Error`] to this error and returns the modified instance.
    ///
    /// The stored source is preserved across clones and exposed via [`error::Error::source`].
    /// Has no effect when called on aggregated errors because aggregates forward the first
    /// contained error as their source.
    pub fn with_source<E>(mut self, source: E) -> Self
    where
        E: error::Error + Send + Sync + 'static,
    {
        self.set_source(Some(Arc::new(source)));
        self
    }

    /// Creates an [`EtlError`] from its components.
    #[track_caller]
    fn from_components(
        kind: ErrorKind,
        description: Cow<'static, str>,
        detail: Option<Cow<'static, str>>,
        source: Option<Arc<dyn error::Error + Send + Sync>>,
    ) -> Self {
        let location = Location::caller();
        let backtrace = Arc::new(Backtrace::capture());

        EtlError {
            repr: ErrorRepr::Single(ErrorPayload::new(
                kind,
                description,
                detail,
                source,
                location,
                backtrace,
            )),
        }
    }

    /// Sets the source for this [`EtlError`].
    fn set_source(&mut self, source: Option<Arc<dyn error::Error + Send + Sync>>) {
        if let ErrorRepr::Single(ref mut payload) = self.repr {
            payload.source = source;
        }
    }
}

impl PartialEq for EtlError {
    fn eq(&self, other: &EtlError) -> bool {
        match (&self.repr, &other.repr) {
            (ErrorRepr::Single(a), ErrorRepr::Single(b)) => a.kind == b.kind,
            (
                ErrorRepr::Many {
                    errors: errors_a, ..
                },
                ErrorRepr::Many {
                    errors: errors_b, ..
                },
            ) => {
                errors_a.len() == errors_b.len()
                    && errors_a.iter().zip(errors_b.iter()).all(|(a, b)| a == b)
            }
            _ => false,
        }
    }
}

impl Hash for EtlError {
    /// Hashes the error using only its stable identifying components.
    ///
    /// Only hashes the error kind and static description, intentionally excluding:
    /// - Location information (file, line, column)
    /// - Detail field (often contains dynamic data like table names, IDs)
    /// - Source errors
    /// - Backtrace
    ///
    /// This ensures that errors of the same category produce the same hash,
    /// enabling stable grouping and deduplication across multiple occurrences.
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.repr {
            ErrorRepr::Single(payload) => {
                // Hash the discriminant to distinguish from Many variant.
                std::mem::discriminant(&self.repr).hash(state);
                // Hash only the stable components.
                payload.kind.hash(state);
                payload.description.hash(state);
            }
            ErrorRepr::Many { errors, .. } => {
                // Hash the discriminant to distinguish from Single variant.
                std::mem::discriminant(&self.repr).hash(state);
                // Hash the number of errors for differentiation.
                errors.len().hash(state);
                // Hash all errors in the aggregation.
                for error in errors {
                    error.hash(state);
                }
            }
        }
    }
}

impl fmt::Display for EtlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match &self.repr {
            ErrorRepr::Single(payload) => {
                let location = payload.location;
                write!(
                    f,
                    "[{:?}] {} @ {}:{}:{}",
                    payload.kind,
                    payload.description,
                    location.file(),
                    location.line(),
                    location.column()
                )?;

                write_detail(payload.detail.as_deref(), f, 1)?;
                write_backtrace(payload.backtrace.as_ref(), f, 1)?;

                Ok(())
            }
            ErrorRepr::Many { errors, location } => {
                let count = errors.len();
                write!(
                    f,
                    "[Many] {} error{} aggregated @ {}:{}:{}",
                    count,
                    if count == 1 { "" } else { "s" },
                    location.file(),
                    location.line(),
                    location.column()
                )?;

                if errors.is_empty() {
                    write!(f, "\n  (no inner errors provided)")?;
                } else {
                    for (index, error) in errors.iter().enumerate() {
                        let rendered = format!("{error}");
                        let mut lines = rendered.lines();
                        if let Some(first_line) = lines.next() {
                            write!(f, "\n  {}. {}", index + 1, first_line)?;
                        } else {
                            write!(f, "\n  {}.", index + 1)?;
                        }

                        for line in lines {
                            if line.is_empty() {
                                write!(f, "\n     ")?;
                            } else {
                                write!(f, "\n     {line}")?;
                            }
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

impl error::Error for EtlError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.repr {
            ErrorRepr::Single(payload) => payload
                .source
                .as_ref()
                .map(|source| source as &(dyn error::Error + 'static)),
            // For aggregated errors, we forward the first contained error as the source.
            ErrorRepr::Many { errors, .. } => errors
                .first()
                .map(|error| error as &(dyn error::Error + 'static)),
        }
    }
}

/// Writes the captured backtrace with indentation.
fn write_backtrace(
    backtrace: &Backtrace,
    f: &mut fmt::Formatter<'_>,
    indent: usize,
) -> fmt::Result {
    let indent_str = "  ".repeat(indent);

    let rendered_backtrace = format!("{backtrace}");
    if !rendered_backtrace.trim().is_empty() {
        write!(f, "\n{indent_str}Backtrace:")?;
        for line in rendered_backtrace.lines() {
            if line.trim().is_empty() {
                write!(f, "\n{indent_str}  ")?;
            } else {
                write!(f, "\n{indent_str}  {line}")?;
            }
        }
    }

    Ok(())
}

/// Writes the detail block with indentation.
fn write_detail(detail: Option<&str>, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
    if let Some(detail) = detail {
        let indent_str = "  ".repeat(indent);
        if detail.trim().is_empty() {
            write!(f, "\n{indent_str}Detail: <empty>")?;
        } else {
            write!(f, "\n{indent_str}Detail:")?;
            for line in detail.lines() {
                if line.trim().is_empty() {
                    write!(f, "\n{indent_str}  ")?;
                } else {
                    write!(f, "\n{indent_str}  {line}")?;
                }
            }
        }
    }

    Ok(())
}

/// Creates an [`EtlError`] from an error kind and static description.
impl From<(ErrorKind, &'static str)> for EtlError {
    #[track_caller]
    fn from((kind, desc): (ErrorKind, &'static str)) -> EtlError {
        EtlError::from_components(kind, Cow::Borrowed(desc), None, None)
    }
}

/// Creates an [`EtlError`] from an error kind, static description, and dynamic detail.
impl<D> From<(ErrorKind, &'static str, D)> for EtlError
where
    D: Into<Cow<'static, str>>,
{
    #[track_caller]
    fn from((kind, desc, detail): (ErrorKind, &'static str, D)) -> EtlError {
        EtlError::from_components(kind, Cow::Borrowed(desc), Some(detail.into()), None)
    }
}

/// Creates an [`EtlError`] from a vector of errors for aggregation.
///
/// If the vector contains exactly one error, returns that error directly without wrapping
/// it in the [`ErrorRepr::Many`] variant.
impl<E> From<Vec<E>> for EtlError
where
    E: Into<EtlError>,
{
    #[track_caller]
    fn from(errors: Vec<E>) -> EtlError {
        let location = Location::caller();

        let mut errors: Vec<EtlError> = errors.into_iter().map(Into::into).collect();

        if errors.len() == 1 {
            return errors.pop().expect("just checked length is 1");
        }

        EtlError {
            repr: ErrorRepr::Many { errors, location },
        }
    }
}

/// Converts [`std::io::Error`] to [`EtlError`] with [`ErrorKind::IoError`].
impl From<std::io::Error> for EtlError {
    #[track_caller]
    fn from(err: std::io::Error) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::IoError,
            Cow::Borrowed("I/O operation failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`serde_json::Error`] to [`EtlError`] with the appropriate error kind.
///
/// Maps to [`ErrorKind::SerializationError`] for serialization failures and
/// [`ErrorKind::DeserializationError`] for deserialization failures based on error classification.
impl From<serde_json::Error> for EtlError {
    #[track_caller]
    fn from(err: serde_json::Error) -> EtlError {
        let (kind, description) = match err.classify() {
            serde_json::error::Category::Io => (ErrorKind::IoError, "JSON I/O operation failed"),
            serde_json::error::Category::Syntax | serde_json::error::Category::Data => (
                ErrorKind::DeserializationError,
                "JSON deserialization failed",
            ),
            serde_json::error::Category::Eof => (
                ErrorKind::DeserializationError,
                "JSON deserialization failed",
            ),
        };

        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            kind,
            Cow::Borrowed(description),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`std::str::Utf8Error`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::str::Utf8Error> for EtlError {
    #[track_caller]
    fn from(err: std::str::Utf8Error) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::ConversionError,
            Cow::Borrowed("UTF-8 conversion failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`std::string::FromUtf8Error`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::string::FromUtf8Error> for EtlError {
    #[track_caller]
    fn from(err: std::string::FromUtf8Error) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::ConversionError,
            Cow::Borrowed("UTF-8 string conversion failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`std::num::ParseIntError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::num::ParseIntError> for EtlError {
    #[track_caller]
    fn from(err: std::num::ParseIntError) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::ConversionError,
            Cow::Borrowed("Integer parsing failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`std::num::ParseFloatError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::num::ParseFloatError> for EtlError {
    #[track_caller]
    fn from(err: std::num::ParseFloatError) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::ConversionError,
            Cow::Borrowed("Float parsing failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`tokio_postgres::Error`] to [`EtlError`] with the appropriate error kind.
///
/// Maps errors based on Postgres SQLSTATE codes to provide granular error classification
/// for better error handling in ETL operations.
impl From<tokio_postgres::Error> for EtlError {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> EtlError {
        let (kind, description) = match err.code() {
            Some(sqlstate) => {
                use tokio_postgres::error::SqlState;

                match *sqlstate {
                    // Connection errors (08xxx)
                    SqlState::CONNECTION_EXCEPTION
                    | SqlState::CONNECTION_DOES_NOT_EXIST
                    | SqlState::CONNECTION_FAILURE
                    | SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
                    | SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION => (
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL connection failed",
                    ),

                    // Authentication errors (28xxx)
                    SqlState::INVALID_AUTHORIZATION_SPECIFICATION | SqlState::INVALID_PASSWORD => (
                        ErrorKind::AuthenticationError,
                        "PostgreSQL authentication failed",
                    ),

                    // Data integrity violations (23xxx)
                    SqlState::INTEGRITY_CONSTRAINT_VIOLATION
                    | SqlState::NOT_NULL_VIOLATION
                    | SqlState::FOREIGN_KEY_VIOLATION
                    | SqlState::UNIQUE_VIOLATION
                    | SqlState::CHECK_VIOLATION => (
                        ErrorKind::ValidationError,
                        "PostgreSQL constraint violation",
                    ),

                    // Data conversion errors (22xxx)
                    SqlState::DATA_EXCEPTION
                    | SqlState::INVALID_TEXT_REPRESENTATION
                    | SqlState::INVALID_DATETIME_FORMAT
                    | SqlState::NUMERIC_VALUE_OUT_OF_RANGE
                    | SqlState::DIVISION_BY_ZERO => (
                        ErrorKind::ConversionError,
                        "PostgreSQL data conversion failed",
                    ),

                    // Schema/object not found errors (42xxx)
                    SqlState::UNDEFINED_TABLE
                    | SqlState::UNDEFINED_COLUMN
                    | SqlState::UNDEFINED_FUNCTION
                    | SqlState::UNDEFINED_SCHEMA => (
                        ErrorKind::SourceSchemaError,
                        "PostgreSQL schema object not found",
                    ),

                    // Syntax and access errors (42xxx)
                    SqlState::SYNTAX_ERROR
                    | SqlState::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
                    | SqlState::INSUFFICIENT_PRIVILEGE => (
                        ErrorKind::SourceQueryFailed,
                        "PostgreSQL syntax or access error",
                    ),

                    // Resource errors (53xxx)
                    SqlState::INSUFFICIENT_RESOURCES
                    | SqlState::OUT_OF_MEMORY
                    | SqlState::TOO_MANY_CONNECTIONS => (
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL resource limitation",
                    ),

                    // Transaction errors (40xxx, 25xxx)
                    SqlState::TRANSACTION_ROLLBACK
                    | SqlState::T_R_SERIALIZATION_FAILURE
                    | SqlState::T_R_DEADLOCK_DETECTED
                    | SqlState::INVALID_TRANSACTION_STATE => {
                        (ErrorKind::InvalidState, "PostgreSQL transaction failed")
                    }

                    // System errors (58xxx, XX xxx)
                    SqlState::SYSTEM_ERROR | SqlState::INTERNAL_ERROR => {
                        (ErrorKind::SourceQueryFailed, "PostgreSQL system error")
                    }
                    SqlState::IO_ERROR => (ErrorKind::SourceIoError, "PostgreSQL I/O error"),

                    // Operator intervention errors (57xxx)
                    SqlState::OPERATOR_INTERVENTION => (
                        ErrorKind::SourceOperationCanceled,
                        "PostgreSQL operation canceled",
                    ),
                    SqlState::QUERY_CANCELED => (
                        ErrorKind::SourceOperationCanceled,
                        "PostgreSQL query canceled",
                    ),
                    SqlState::ADMIN_SHUTDOWN => (
                        ErrorKind::SourceDatabaseShutdown,
                        "PostgreSQL administrative shutdown",
                    ),
                    SqlState::CRASH_SHUTDOWN => (
                        ErrorKind::SourceDatabaseShutdown,
                        "PostgreSQL crash shutdown",
                    ),
                    SqlState::CANNOT_CONNECT_NOW => (
                        ErrorKind::SourceDatabaseInRecovery,
                        "PostgreSQL database in recovery",
                    ),
                    SqlState::DATABASE_DROPPED => {
                        (ErrorKind::SourceSchemaError, "PostgreSQL database dropped")
                    }
                    SqlState::IDLE_SESSION_TIMEOUT => (
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL idle session timeout",
                    ),

                    // Object state errors (55xxx)
                    SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE => (
                        ErrorKind::InvalidState,
                        "PostgreSQL object not in prerequisite state",
                    ),
                    SqlState::OBJECT_IN_USE => {
                        (ErrorKind::InvalidState, "PostgreSQL object in use")
                    }
                    SqlState::LOCK_NOT_AVAILABLE => (
                        ErrorKind::SourceLockTimeout,
                        "PostgreSQL lock not available",
                    ),

                    // Program limit errors (54xxx)
                    SqlState::PROGRAM_LIMIT_EXCEEDED
                    | SqlState::STATEMENT_TOO_COMPLEX
                    | SqlState::TOO_MANY_COLUMNS
                    | SqlState::TOO_MANY_ARGUMENTS => (
                        ErrorKind::SourceQueryFailed,
                        "PostgreSQL program limit exceeded",
                    ),

                    // Configuration errors (53xxx)
                    SqlState::DISK_FULL => (ErrorKind::SourceIoError, "PostgreSQL disk full"),
                    SqlState::CONFIGURATION_LIMIT_EXCEEDED => (
                        ErrorKind::SourceConfigurationLimitExceeded,
                        "PostgreSQL configuration limit exceeded",
                    ),

                    // Transaction state errors (25xxx)
                    SqlState::ACTIVE_SQL_TRANSACTION
                    | SqlState::NO_ACTIVE_SQL_TRANSACTION
                    | SqlState::IN_FAILED_SQL_TRANSACTION
                    | SqlState::IDLE_IN_TRANSACTION_SESSION_TIMEOUT => (
                        ErrorKind::InvalidState,
                        "PostgreSQL transaction state error",
                    ),

                    // Cursor errors (24xxx, 34xxx)
                    SqlState::INVALID_CURSOR_STATE | SqlState::INVALID_CURSOR_NAME => {
                        (ErrorKind::InvalidState, "PostgreSQL cursor error")
                    }

                    // Data corruption errors (XX xxx)
                    SqlState::DATA_CORRUPTED | SqlState::INDEX_CORRUPTED => {
                        (ErrorKind::SourceIoError, "PostgreSQL data corruption")
                    }

                    // Configuration file errors (F0xxx)
                    SqlState::CONFIG_FILE_ERROR | SqlState::LOCK_FILE_EXISTS => {
                        (ErrorKind::ConfigError, "PostgreSQL configuration error")
                    }

                    // Feature not supported (0Axxx)
                    SqlState::FEATURE_NOT_SUPPORTED => (
                        ErrorKind::SourceSchemaError,
                        "PostgreSQL feature not supported",
                    ),

                    // Invalid transaction initiation (0Bxxx)
                    SqlState::INVALID_TRANSACTION_INITIATION => (
                        ErrorKind::InvalidState,
                        "PostgreSQL invalid transaction initiation",
                    ),

                    // Dependent objects errors (2Bxxx)
                    SqlState::DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST
                    | SqlState::DEPENDENT_OBJECTS_STILL_EXIST => (
                        ErrorKind::InvalidState,
                        "PostgreSQL dependent objects exist",
                    ),

                    // SQL routine errors (2Fxxx)
                    SqlState::SQL_ROUTINE_EXCEPTION
                    | SqlState::S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT
                    | SqlState::S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED
                    | SqlState::S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED
                    | SqlState::S_R_E_READING_SQL_DATA_NOT_PERMITTED => {
                        (ErrorKind::SourceQueryFailed, "PostgreSQL routine exception")
                    }

                    // External routine errors (38xxx, 39xxx)
                    SqlState::EXTERNAL_ROUTINE_EXCEPTION
                    | SqlState::E_R_E_CONTAINING_SQL_NOT_PERMITTED
                    | SqlState::E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED
                    | SqlState::E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED
                    | SqlState::E_R_E_READING_SQL_DATA_NOT_PERMITTED
                    | SqlState::EXTERNAL_ROUTINE_INVOCATION_EXCEPTION
                    | SqlState::E_R_I_E_INVALID_SQLSTATE_RETURNED
                    | SqlState::E_R_I_E_NULL_VALUE_NOT_ALLOWED
                    | SqlState::E_R_I_E_TRIGGER_PROTOCOL_VIOLATED
                    | SqlState::E_R_I_E_SRF_PROTOCOL_VIOLATED
                    | SqlState::E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED => (
                        ErrorKind::SourceQueryFailed,
                        "PostgreSQL external routine error",
                    ),

                    // PL/pgSQL errors (P0xxx)
                    SqlState::PLPGSQL_ERROR
                    | SqlState::RAISE_EXCEPTION
                    | SqlState::NO_DATA_FOUND
                    | SqlState::TOO_MANY_ROWS
                    | SqlState::ASSERT_FAILURE => {
                        (ErrorKind::SourceQueryFailed, "PostgreSQL PL/pgSQL error")
                    }

                    // Foreign Data Wrapper errors (HVxxx) - connection/schema related
                    SqlState::FDW_ERROR | SqlState::FDW_UNABLE_TO_ESTABLISH_CONNECTION => (
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL FDW connection failed",
                    ),
                    SqlState::FDW_SCHEMA_NOT_FOUND
                    | SqlState::FDW_TABLE_NOT_FOUND
                    | SqlState::FDW_COLUMN_NAME_NOT_FOUND
                    | SqlState::FDW_INVALID_COLUMN_NAME
                    | SqlState::FDW_NO_SCHEMAS => {
                        (ErrorKind::SourceSchemaError, "PostgreSQL FDW schema error")
                    }
                    SqlState::FDW_INVALID_DATA_TYPE
                    | SqlState::FDW_INVALID_DATA_TYPE_DESCRIPTORS
                    | SqlState::FDW_INVALID_STRING_FORMAT => {
                        (ErrorKind::ConversionError, "PostgreSQL FDW data type error")
                    }
                    SqlState::FDW_OUT_OF_MEMORY => (
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL FDW out of memory",
                    ),
                    SqlState::FDW_DYNAMIC_PARAMETER_VALUE_NEEDED
                    | SqlState::FDW_FUNCTION_SEQUENCE_ERROR
                    | SqlState::FDW_INCONSISTENT_DESCRIPTOR_INFORMATION
                    | SqlState::FDW_INVALID_ATTRIBUTE_VALUE
                    | SqlState::FDW_INVALID_COLUMN_NUMBER
                    | SqlState::FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER
                    | SqlState::FDW_INVALID_HANDLE
                    | SqlState::FDW_INVALID_OPTION_INDEX
                    | SqlState::FDW_INVALID_OPTION_NAME
                    | SqlState::FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH
                    | SqlState::FDW_INVALID_USE_OF_NULL_POINTER
                    | SqlState::FDW_TOO_MANY_HANDLES
                    | SqlState::FDW_OPTION_NAME_NOT_FOUND
                    | SqlState::FDW_REPLY_HANDLE
                    | SqlState::FDW_UNABLE_TO_CREATE_EXECUTION
                    | SqlState::FDW_UNABLE_TO_CREATE_REPLY => (
                        ErrorKind::SourceQueryFailed,
                        "PostgreSQL FDW operation error",
                    ),

                    // Snapshot errors (72xxx) - important for replication consistency
                    SqlState::SNAPSHOT_TOO_OLD => (
                        ErrorKind::SourceSnapshotTooOld,
                        "PostgreSQL snapshot too old",
                    ),

                    // Array errors - relevant for replication data handling
                    SqlState::ARRAY_ELEMENT_ERROR => {
                        (ErrorKind::ConversionError, "PostgreSQL array error")
                    }

                    // XML/JSON errors that could occur during replication
                    SqlState::NOT_AN_XML_DOCUMENT
                    | SqlState::INVALID_XML_DOCUMENT
                    | SqlState::INVALID_XML_CONTENT
                    | SqlState::INVALID_XML_COMMENT
                    | SqlState::INVALID_XML_PROCESSING_INSTRUCTION
                    | SqlState::DUPLICATE_JSON_OBJECT_KEY_VALUE
                    | SqlState::INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION
                    | SqlState::INVALID_JSON_TEXT
                    | SqlState::INVALID_SQL_JSON_SUBSCRIPT
                    | SqlState::MORE_THAN_ONE_SQL_JSON_ITEM
                    | SqlState::NO_SQL_JSON_ITEM
                    | SqlState::NON_NUMERIC_SQL_JSON_ITEM
                    | SqlState::NON_UNIQUE_KEYS_IN_A_JSON_OBJECT
                    | SqlState::SINGLETON_SQL_JSON_ITEM_REQUIRED
                    | SqlState::SQL_JSON_ARRAY_NOT_FOUND
                    | SqlState::SQL_JSON_MEMBER_NOT_FOUND
                    | SqlState::SQL_JSON_NUMBER_NOT_FOUND
                    | SqlState::SQL_JSON_OBJECT_NOT_FOUND
                    | SqlState::TOO_MANY_JSON_ARRAY_ELEMENTS
                    | SqlState::TOO_MANY_JSON_OBJECT_MEMBERS
                    | SqlState::SQL_JSON_SCALAR_REQUIRED
                    | SqlState::SQL_JSON_ITEM_CANNOT_BE_CAST_TO_TARGET_TYPE => {
                        (ErrorKind::ConversionError, "PostgreSQL XML/JSON error")
                    }

                    // Default for other SQL states
                    _ => (ErrorKind::SourceError, "PostgreSQL error"),
                }
            }
            // No SQL state means connection issue
            None => (
                ErrorKind::SourceConnectionFailed,
                "PostgreSQL connection failed",
            ),
        };

        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            kind,
            Cow::Borrowed(description),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`rustls::Error`] to [`EtlError`] with [`ErrorKind::EncryptionError`].
impl From<rustls::Error> for EtlError {
    #[track_caller]
    fn from(err: rustls::Error) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::EncryptionError,
            Cow::Borrowed("TLS configuration failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`rustls::pki_types::pem::Error`] to [`EtlError`] with [`ErrorKind::ConfigError`].
impl From<rustls::pki_types::pem::Error> for EtlError {
    #[track_caller]
    fn from(err: rustls::pki_types::pem::Error) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::ConfigError,
            Cow::Borrowed("PEM parsing failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`uuid::Error`] to [`EtlError`] with [`ErrorKind::InvalidData`].
impl From<uuid::Error> for EtlError {
    #[track_caller]
    fn from(err: uuid::Error) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::InvalidData,
            Cow::Borrowed("UUID parsing failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`chrono::ParseError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<chrono::ParseError> for EtlError {
    #[track_caller]
    fn from(err: chrono::ParseError) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::ConversionError,
            Cow::Borrowed("Datetime parsing failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`ParseNumericError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<ParseNumericError> for EtlError {
    #[track_caller]
    fn from(err: ParseNumericError) -> EtlError {
        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            ErrorKind::ConversionError,
            Cow::Borrowed("Numeric parsing failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`sqlx::Error`] to [`EtlError`] with the appropriate error kind.
///
/// Maps database errors to [`ErrorKind::SourceQueryFailed`], I/O errors to [`ErrorKind::IoError`],
/// and connection pool errors to [`ErrorKind::SourceConnectionFailed`].
impl From<sqlx::Error> for EtlError {
    #[track_caller]
    fn from(err: sqlx::Error) -> EtlError {
        let kind = match &err {
            sqlx::Error::Database(_) => ErrorKind::SourceQueryFailed,
            sqlx::Error::Io(_) => ErrorKind::IoError,
            sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut => {
                ErrorKind::SourceConnectionFailed
            }
            _ => ErrorKind::SourceQueryFailed,
        };

        let detail = err.to_string();
        let source = Arc::new(err);
        EtlError::from_components(
            kind,
            Cow::Borrowed("Database operation failed"),
            Some(Cow::Owned(detail)),
            Some(source),
        )
    }
}

/// Converts [`etl_postgres::replication::slots::EtlReplicationSlotError`] to [`EtlError`] with appropriate error kind.
impl From<etl_postgres::replication::slots::EtlReplicationSlotError> for EtlError {
    #[track_caller]
    fn from(err: etl_postgres::replication::slots::EtlReplicationSlotError) -> EtlError {
        match err {
            etl_postgres::replication::slots::EtlReplicationSlotError::InvalidSlotNameLength(
                slot_name,
            ) => EtlError::from_components(
                ErrorKind::ValidationError,
                Cow::Borrowed("Replication slot name exceeds maximum length"),
                Some(Cow::Owned(slot_name)),
                None,
            ),
            etl_postgres::replication::slots::EtlReplicationSlotError::InvalidSlotName(
                slot_name,
            ) => EtlError::from_components(
                ErrorKind::ValidationError,
                Cow::Borrowed("Replication slot name is invalid"),
                Some(Cow::Owned(slot_name)),
                None,
            ),
        }
    }
}
