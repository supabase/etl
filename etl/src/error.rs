use std::error;
use std::fmt;

use crate::conversions::numeric::ParseNumericError;

/// Convenient result type for ETL operations using [`EtlError`] as the error type.
///
/// This type alias reduces boilerplate when working with fallible ETL operations.
/// Most ETL functions return this type.
pub type EtlResult<T> = Result<T, EtlError>;

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
    /// Error with kind and static description
    WithDescription(ErrorKind, &'static str),
    /// Error with kind, static description, and dynamic detail
    WithDescriptionAndDetail(ErrorKind, &'static str, String),
    /// Multiple aggregated errors
    Many(Vec<EtlError>),
}

/// Specific categories of errors that can occur during ETL operations.
///
/// This enum provides granular error classification to enable appropriate error handling
/// strategies. Error kinds are organized by functional area and failure mode.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
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

    // Data & Transformation Errors
    ConversionError,
    InvalidData,
    NullValuesNotSupportedInArray,
    ValidationError,

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
    /// Creates an [`EtlError`] containing multiple aggregated errors.
    ///
    /// This is useful when multiple operations fail and you want to report all failures
    /// rather than just the first one.
    pub fn many(errors: Vec<EtlError>) -> EtlError {
        EtlError {
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

impl PartialEq for EtlError {
    fn eq(&self, other: &EtlError) -> bool {
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

impl fmt::Display for EtlError {
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

impl error::Error for EtlError {}

/// Creates an [`EtlError`] from an error kind and static description.
impl From<(ErrorKind, &'static str)> for EtlError {
    fn from((kind, desc): (ErrorKind, &'static str)) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescription(kind, desc),
        }
    }
}

/// Creates an [`EtlError`] from an error kind, static description, and dynamic detail.
impl From<(ErrorKind, &'static str, String)> for EtlError {
    fn from((kind, desc, detail): (ErrorKind, &'static str, String)) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(kind, desc, detail),
        }
    }
}

/// Creates an [`EtlError`] from a vector of errors for aggregation.
impl<E> From<Vec<E>> for EtlError
where
    E: Into<EtlError>,
{
    fn from(errors: Vec<E>) -> EtlError {
        EtlError {
            repr: ErrorRepr::Many(errors.into_iter().map(Into::into).collect()),
        }
    }
}

// Common standard library error conversions

/// Converts [`std::io::Error`] to [`EtlError`] with [`ErrorKind::IoError`].
impl From<std::io::Error> for EtlError {
    fn from(err: std::io::Error) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::IoError,
                "I/O error occurred",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`serde_json::Error`] to [`EtlError`] with appropriate error kind.
///
/// Maps to [`ErrorKind::SerializationError`] for serialization failures and
/// [`ErrorKind::DeserializationError`] for deserialization failures based on error classification.
impl From<serde_json::Error> for EtlError {
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

        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(kind, description, err.to_string()),
        }
    }
}

/// Converts [`std::str::Utf8Error`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::str::Utf8Error> for EtlError {
    fn from(err: std::str::Utf8Error) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "UTF-8 conversion failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`std::string::FromUtf8Error`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::string::FromUtf8Error> for EtlError {
    fn from(err: std::string::FromUtf8Error) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "UTF-8 string conversion failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`std::num::ParseIntError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::num::ParseIntError> for EtlError {
    fn from(err: std::num::ParseIntError) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Integer parsing failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`std::num::ParseFloatError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<std::num::ParseFloatError> for EtlError {
    fn from(err: std::num::ParseFloatError) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Float parsing failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`tokio_postgres::Error`] to [`EtlError`] with appropriate error kind.
///
/// Maps errors based on PostgreSQL SQLSTATE codes to provide granular error classification
/// for better error handling in ETL operations.
impl From<tokio_postgres::Error> for EtlError {
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
                        "PostgreSQL connection error",
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
                        "PostgreSQL data conversion error",
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
                        (ErrorKind::InvalidState, "PostgreSQL transaction error")
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
                        "PostgreSQL admin shutdown",
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
                        "PostgreSQL FDW connection error",
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

        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(kind, description, err.to_string()),
        }
    }
}

/// Converts [`rustls::Error`] to [`EtlError`] with [`ErrorKind::EncryptionError`].
impl From<rustls::Error> for EtlError {
    fn from(err: rustls::Error) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::EncryptionError,
                "TLS configuration failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`uuid::Error`] to [`EtlError`] with [`ErrorKind::InvalidData`].
impl From<uuid::Error> for EtlError {
    fn from(err: uuid::Error) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::InvalidData,
                "UUID parsing failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`chrono::ParseError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<chrono::ParseError> for EtlError {
    fn from(err: chrono::ParseError) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Chrono parse failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`ParseNumericError`] to [`EtlError`] with [`ErrorKind::ConversionError`].
impl From<ParseNumericError> for EtlError {
    fn from(err: ParseNumericError) -> EtlError {
        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                ErrorKind::ConversionError,
                "Numeric parsing failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`sqlx::Error`] to [`EtlError`] with appropriate error kind.
///
/// Maps database errors to [`ErrorKind::SourceQueryFailed`], I/O errors to [`ErrorKind::IoError`],
/// and connection pool errors to [`ErrorKind::SourceConnectionFailed`].
impl From<sqlx::Error> for EtlError {
    fn from(err: sqlx::Error) -> EtlError {
        let kind = match &err {
            sqlx::Error::Database(_) => ErrorKind::SourceQueryFailed,
            sqlx::Error::Io(_) => ErrorKind::IoError,
            sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut => {
                ErrorKind::SourceConnectionFailed
            }
            _ => ErrorKind::SourceQueryFailed,
        };

        EtlError {
            repr: ErrorRepr::WithDescriptionAndDetail(
                kind,
                "Database operation failed",
                err.to_string(),
            ),
        }
    }
}

/// Converts [`etl_postgres::replication::slots::SlotError`] to [`EtlError`] with appropriate error kind.
impl From<etl_postgres::replication::slots::SlotError> for EtlError {
    fn from(err: etl_postgres::replication::slots::SlotError) -> EtlError {
        match err {
            etl_postgres::replication::slots::SlotError::InvalidSlotNameLength(slot_name) => {
                EtlError {
                    repr: ErrorRepr::WithDescriptionAndDetail(
                        ErrorKind::ValidationError,
                        "Replication slot name exceeds maximum length",
                        slot_name,
                    ),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{bail, etl_error};

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
        let multi_err = EtlError::many(errors);

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
    fn test_multiple_errors_with_detail() {
        let errors = vec![
            EtlError::from((
                ErrorKind::ValidationError,
                "Invalid schema",
                "Missing required field".to_string(),
            )),
            EtlError::from((ErrorKind::ConversionError, "Type mismatch")),
        ];
        let multi_err = EtlError::many(errors);

        assert_eq!(multi_err.detail(), Some("Missing required field"));
    }

    #[test]
    fn test_from_vector() {
        let errors = vec![
            EtlError::from((ErrorKind::ValidationError, "Error 1")),
            EtlError::from((ErrorKind::ConversionError, "Error 2")),
        ];
        let multi_err = EtlError::from(errors);
        assert_eq!(multi_err.kinds().len(), 2);
    }

    #[test]
    fn test_empty_multiple_errors() {
        let multi_err = EtlError::many(vec![]);
        assert_eq!(multi_err.kind(), ErrorKind::Unknown);
        assert_eq!(multi_err.kinds(), vec![]);
        assert_eq!(multi_err.detail(), None);
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
    }

    #[test]
    fn test_error_display_with_detail() {
        let err = EtlError::from((
            ErrorKind::SourceQueryFailed,
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
            EtlError::from((ErrorKind::ValidationError, "Invalid schema")),
            EtlError::from((ErrorKind::ConversionError, "Type mismatch")),
        ];
        let multi_err = EtlError::many(errors);
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
        fn test_function() -> EtlResult<i32> {
            bail!(ErrorKind::ValidationError, "Test error");
        }

        fn test_function_with_detail() -> EtlResult<i32> {
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
            EtlError::from((ErrorKind::ConversionError, "Inner error 1")),
            EtlError::from((ErrorKind::ValidationError, "Inner error 2")),
        ];
        let inner_multi = EtlError::many(inner_errors);

        let outer_errors = vec![
            inner_multi,
            EtlError::from((ErrorKind::IoError, "Outer error")),
        ];
        let outer_multi = EtlError::many(outer_errors);

        let kinds = outer_multi.kinds();
        assert_eq!(kinds.len(), 3);
        assert!(kinds.contains(&ErrorKind::ConversionError));
        assert!(kinds.contains(&ErrorKind::ValidationError));
        assert!(kinds.contains(&ErrorKind::IoError));
    }

    #[test]
    fn test_json_error_classification() {
        // Test syntax error during deserialization
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let etl_err = EtlError::from(json_err);
        assert_eq!(etl_err.kind(), ErrorKind::DeserializationError);
        assert!(etl_err.detail().unwrap().contains("expected"));

        // Test data error during deserialization
        let json_err = serde_json::from_str::<bool>("\"not_a_bool\"").unwrap_err();
        let etl_err = EtlError::from(json_err);
        assert_eq!(etl_err.kind(), ErrorKind::DeserializationError);
        assert!(etl_err.detail().is_some());
    }
}
