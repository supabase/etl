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
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.repr {
            ErrorRepr::Single(payload) => {
                std::mem::discriminant(&self.repr).hash(state);
                payload.kind.hash(state);
                payload.description.hash(state);
            }
            ErrorRepr::Many { errors, .. } => {
                std::mem::discriminant(&self.repr).hash(state);
                errors.len().hash(state);
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
            ErrorRepr::Many { errors, .. } => errors
                .first()
                .map(|error| error as &(dyn error::Error + 'static)),
        }
    }
}

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

impl From<(ErrorKind, &'static str)> for EtlError {
    #[track_caller]
    fn from((kind, desc): (ErrorKind, &'static str)) -> EtlError {
        EtlError::from_components(kind, Cow::Borrowed(desc), None, None)
    }
}

impl<D> From<(ErrorKind, &'static str, D)> for EtlError
where
    D: Into<Cow<'static, str>>,
{
    #[track_caller]
    fn from((kind, desc, detail): (ErrorKind, &'static str, D)) -> EtlError {
        EtlError::from_components(kind, Cow::Borrowed(desc), Some(detail.into()), None)
    }
}

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

impl From<tokio_postgres::Error> for EtlError {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> EtlError {
        let (kind, description) = match err.code() {
            Some(sqlstate) => {
                use tokio_postgres::error::SqlState;

                match *sqlstate {
                    SqlState::CONNECTION_EXCEPTION
                    | SqlState::CONNECTION_DOES_NOT_EXIST
                    | SqlState::CONNECTION_FAILURE
                    | SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
                    | SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION => (
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL connection failed",
                    ),

                    SqlState::INVALID_AUTHORIZATION_SPECIFICATION | SqlState::INVALID_PASSWORD => (
                        ErrorKind::AuthenticationError,
                        "PostgreSQL authentication failed",
                    ),

                    SqlState::INTEGRITY_CONSTRAINT_VIOLATION
                    | SqlState::NOT_NULL_VIOLATION
                    | SqlState::FOREIGN_KEY_VIOLATION
                    | SqlState::UNIQUE_VIOLATION
                    | SqlState::CHECK_VIOLATION => (
                        ErrorKind::ValidationError,
                        "PostgreSQL constraint violation",
                    ),

                    SqlState::DATA_EXCEPTION
                    | SqlState::INVALID_TEXT_REPRESENTATION
                    | SqlState::INVALID_DATETIME_FORMAT
                    | SqlState::NUMERIC_VALUE_OUT_OF_RANGE
                    | SqlState::DIVISION_BY_ZERO => (
                        ErrorKind::ConversionError,
                        "PostgreSQL data conversion failed",
                    ),

                    SqlState::UNDEFINED_TABLE
                    | SqlState::UNDEFINED_COLUMN
                    | SqlState::UNDEFINED_FUNCTION
                    | SqlState::UNDEFINED_SCHEMA => (
                        ErrorKind::SourceSchemaError,
                        "PostgreSQL schema object not found",
                    ),

                    SqlState::SYNTAX_ERROR
                    | SqlState::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
                    | SqlState::INSUFFICIENT_PRIVILEGE => (
                        ErrorKind::SourceQueryFailed,
                        "PostgreSQL syntax or access error",
                    ),

                    SqlState::INSUFFICIENT_RESOURCES
                    | SqlState::OUT_OF_MEMORY
                    | SqlState::TOO_MANY_CONNECTIONS => (
                        ErrorKind::SourceConnectionFailed,
                        "PostgreSQL resource limitation",
                    ),

                    SqlState::TRANSACTION_ROLLBACK
                    | SqlState::T_R_SERIALIZATION_FAILURE
                    | SqlState::T_R_DEADLOCK_DETECTED
                    | SqlState::INVALID_TRANSACTION_STATE => {
                        (ErrorKind::InvalidState, "PostgreSQL transaction failed")
                    }

                    SqlState::SYSTEM_ERROR | SqlState::INTERNAL_ERROR => {
                        (ErrorKind::SourceQueryFailed, "PostgreSQL system error")
                    }
                    SqlState::IO_ERROR => (ErrorKind::SourceIoError, "PostgreSQL I/O error"),

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

                    SqlState::PROGRAM_LIMIT_EXCEEDED
                    | SqlState::STATEMENT_TOO_COMPLEX
                    | SqlState::TOO_MANY_COLUMNS
                    | SqlState::TOO_MANY_ARGUMENTS => (
                        ErrorKind::SourceQueryFailed,
                        "PostgreSQL program limit exceeded",
                    ),

                    SqlState::DISK_FULL => (ErrorKind::SourceIoError, "PostgreSQL disk full"),
                    SqlState::CONFIGURATION_LIMIT_EXCEEDED => (
                        ErrorKind::SourceConfigurationLimitExceeded,
                        "PostgreSQL configuration limit exceeded",
                    ),

                    SqlState::ACTIVE_SQL_TRANSACTION
                    | SqlState::NO_ACTIVE_SQL_TRANSACTION
                    | SqlState::IN_FAILED_SQL_TRANSACTION
                    | SqlState::IDLE_IN_TRANSACTION_SESSION_TIMEOUT => (
                        ErrorKind::InvalidState,
                        "PostgreSQL transaction state error",
                    ),

                    SqlState::INVALID_CURSOR_STATE | SqlState::INVALID_CURSOR_NAME => {
                        (ErrorKind::InvalidState, "PostgreSQL cursor error")
                    }

                    SqlState::DATA_CORRUPTED | SqlState::INDEX_CORRUPTED => {
                        (ErrorKind::SourceIoError, "PostgreSQL data corruption")
                    }

                    SqlState::CONFIG_FILE_ERROR | SqlState::LOCK_FILE_EXISTS => {
                        (ErrorKind::ConfigError, "PostgreSQL configuration error")
                    }

                    SqlState::FEATURE_NOT_SUPPORTED => (
                        ErrorKind::SourceSchemaError,
                        "PostgreSQL feature not supported",
                    ),

                    SqlState::INVALID_TRANSACTION_INITIATION => (
                        ErrorKind::InvalidState,
                        "PostgreSQL invalid transaction initiation",
                    ),

                    SqlState::DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST
                    | SqlState::DEPENDENT_OBJECTS_STILL_EXIST => (
                        ErrorKind::InvalidState,
                        "PostgreSQL dependent objects exist",
                    ),

                    SqlState::SQL_ROUTINE_EXCEPTION
                    | SqlState::S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT
                    | SqlState::S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED
                    | SqlState::S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED
                    | SqlState::S_R_E_READING_SQL_DATA_NOT_PERMITTED => {
                        (ErrorKind::SourceQueryFailed, "PostgreSQL routine exception")
                    }

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

                    SqlState::PLPGSQL_ERROR
                    | SqlState::RAISE_EXCEPTION
                    | SqlState::NO_DATA_FOUND
                    | SqlState::TOO_MANY_ROWS
                    | SqlState::ASSERT_FAILURE => {
                        (ErrorKind::SourceQueryFailed, "PostgreSQL PL/pgSQL error")
                    }

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

                    SqlState::SNAPSHOT_TOO_OLD => (
                        ErrorKind::SourceSnapshotTooOld,
                        "PostgreSQL snapshot too old",
                    ),

                    SqlState::ARRAY_ELEMENT_ERROR => {
                        (ErrorKind::ConversionError, "PostgreSQL array error")
                    }

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

                    _ => (ErrorKind::SourceError, "PostgreSQL error"),
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bail, etl_error};
    use std::error::Error as _;

    #[test]
    fn test_simple_error_creation() {
        let err = EtlError::from((
            ErrorKind::SourceConnectionFailed,
            "Database connection failed",
        ));
        assert_eq!(err.kind(), ErrorKind::SourceConnectionFailed);
        assert_eq!(err.detail(), None);
        assert_eq!(err.kinds(), vec![ErrorKind::SourceConnectionFailed]);
    }

    #[test]
    fn test_error_with_detail() {
        let err = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "SQL query execution failed",
            "Table 'users' doesn't exist".to_string(),
        ));
        assert_eq!(err.kind(), ErrorKind::SourceQueryFailed);
        assert_eq!(err.detail(), Some("Table 'users' doesn't exist"));
        assert_eq!(err.kinds(), vec![ErrorKind::SourceQueryFailed]);
    }

    #[test]
    fn test_multiple_errors() {
        let errors = vec![
            EtlError::from((ErrorKind::ValidationError, "Invalid schema")),
            EtlError::from((ErrorKind::ConversionError, "Type mismatch")),
            EtlError::from((ErrorKind::IoError, "Connection timeout")),
        ];
        let multi_err: EtlError = errors.into();

        assert_eq!(multi_err.kind(), ErrorKind::ValidationError);
        assert_eq!(
            multi_err.kinds(),
            vec![
                ErrorKind::ValidationError,
                ErrorKind::ConversionError,
                ErrorKind::IoError
            ]
        );
        assert_eq!(multi_err.detail(), None);
    }

    #[test]
    fn test_from_vector_single_error_not_wrapped() {
        let error = EtlError::from((ErrorKind::ValidationError, "Single error"));
        let errors = vec![error];
        let result = EtlError::from(errors);

        assert_eq!(result.kinds().len(), 1);
        assert_eq!(result.kind(), ErrorKind::ValidationError);
    }

    #[test]
    fn test_error_equality() {
        let err1 = EtlError::from((ErrorKind::SourceConnectionFailed, "Connection failed"));
        let err2 = EtlError::from((ErrorKind::SourceConnectionFailed, "Connection failed"));
        let err3 = EtlError::from((ErrorKind::SourceQueryFailed, "Query failed"));

        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    #[test]
    fn test_error_display() {
        let err = EtlError::from((
            ErrorKind::SourceConnectionFailed,
            "Database connection failed",
        ));
        let display_str = format!("{err}");
        assert!(display_str.contains("ConnectionFailed"));
        assert!(display_str.contains("Database connection failed"));
        assert!(display_str.contains(" @ "));
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
        fn test_function() -> EtlResult<i32> {
            bail!(ErrorKind::ValidationError, "Test error");
        }

        let result = test_function();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ValidationError);
    }
}
