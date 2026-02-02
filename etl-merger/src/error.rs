//! Error types for the etl-merger crate.

use thiserror::Error;

/// Errors that can occur during merge operations.
#[derive(Error, Debug)]
pub enum MergerError {
    /// Error from the Iceberg library.
    #[error("Iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// Error from Arrow operations.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Error from the ETL core library.
    #[error("ETL error: {0}")]
    Etl(#[from] etl::error::EtlError),

    /// Invalid schema encountered.
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    /// Table is missing a primary key.
    #[error("Missing primary key for table: {0}")]
    MissingPrimaryKey(String),

    /// Invalid CDC operation value.
    #[error("Invalid CDC operation: {0}")]
    InvalidCdcOperation(String),

    /// Index lookup failed for an operation that requires it.
    #[error("Index lookup failed: primary key not found for {operation} operation")]
    IndexLookupFailed { operation: String },

    /// Sequence number parsing error.
    #[error("Sequence number parse error: {0}")]
    SequenceNumberParse(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Column not found in schema.
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// Table not found.
    #[error("Table not found: {namespace}.{table}")]
    TableNotFound { namespace: String, table: String },
}

/// Result type for merger operations.
pub type MergerResult<T> = Result<T, MergerError>;
