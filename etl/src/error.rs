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
pub type EtlResult&lt;T&gt; = Result&lt;T, EtlError&gt;;

/// Detailed payload stored for single [`EtlError`] instances.
#[derive(Debug, Clone)]
struct ErrorPayload {
    kind: ErrorKind,
    description: Cow&lt;'static, str&gt;,
    detail: Option&lt;Cow&lt;'static, str&gt;&gt;,
    source: Option&lt;Arc&lt;dyn error::Error + Send + Sync&gt;&gt;,
    location: &amp;'static Location&lt;'static&gt;,
    backtrace: Arc&lt;Backtrace&gt;,
}

impl ErrorPayload {
    /// Creates a new payload with optional dynamic detail.
    fn new(
        kind: ErrorKind,
        description: Cow&lt;'static, str&gt;,
        detail: Option&lt;Cow&lt;'static, str&gt;&gt;,
        source: Option&lt;Arc&lt;dyn error::Error + Send + Sync&gt;&gt;,
        location: &amp;'static Location&lt;'static&gt;,
        backtrace: Arc&lt;Backtrace&gt;,
    ) -&gt; Self {
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
        errors: Vec&lt;EtlError&gt;,
        location: &amp;'static Location&lt;'static&gt;,
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

    // Query &amp; Execution Errors
    SourceQueryFailed,
    DestinationQueryFailed,
    SourceLockTimeout,
    SourceOperationCanceled,

    // Schema &amp; Mapping Errors
    SourceSchemaError,
    MissingTableSchema,
    MissingTableMapping,
    DestinationTableNameInvalid,
    DestinationNamespaceAlreadyExists,
    DestinationTableAlreadyExists,
    DestinationNamespaceMissing,
    DestinationTableMissing,

    // Data &amp; Transformation Errors
    ConversionError,
    InvalidData,
    ValidationError,
    NullValuesNotSupportedInArrayInDestination,
    UnsupportedValueInDestination,

    // Configuration &amp; Limit Errors
    ConfigError,
    SourceConfigurationLimitExceeded,

    // IO &amp; Serialization Errors
    IoError,
    SourceIoError,
    DestinationIoError,
    SerializationError,
    DeserializationError,

    // Security &amp; Authentication Errors
    EncryptionError,
    AuthenticationError,
    PermissionDenied,

    // State &amp; Workflow Errors
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
