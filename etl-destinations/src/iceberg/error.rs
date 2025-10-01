use arrow::error::ArrowError;
use etl::{
    error::{ErrorKind, EtlError},
    etl_error,
};

/// Converts iceberg errors to ETL errors with appropriate classification.
///
/// Maps iceberg error types to ETL error kinds for consistent error handling.
pub(crate) fn iceberg_error_to_etl_error(err: iceberg::Error) -> EtlError {
    let (kind, description) = match err.kind() {
        iceberg::ErrorKind::PreconditionFailed => {
            (ErrorKind::InvalidState, "Iceberg precondition failed")
        }
        iceberg::ErrorKind::Unexpected => (ErrorKind::Unknown, "An unexpected error occurred"),
        iceberg::ErrorKind::DataInvalid => (ErrorKind::InvalidData, "Invalid iceberg data"),
        iceberg::ErrorKind::NamespaceAlreadyExists => (
            ErrorKind::DestinationNamespaceAlreadyExists,
            "Iceberg namespace already exists",
        ),
        iceberg::ErrorKind::TableAlreadyExists => (
            ErrorKind::DestinationTableAlreadyExists,
            "Iceberg table already exists",
        ),
        iceberg::ErrorKind::NamespaceNotFound => (
            ErrorKind::DestinationNamespaceMissing,
            "Iceberg namespace missing",
        ),
        iceberg::ErrorKind::TableNotFound => {
            (ErrorKind::DestinationTableMissing, "Iceberg table missing")
        }
        iceberg::ErrorKind::FeatureUnsupported => {
            (ErrorKind::Unknown, "Unsupported iceberg feature was used")
        }
        iceberg::ErrorKind::CatalogCommitConflicts => (
            ErrorKind::DestinationError,
            "Iceberg commit conflicts occurred",
        ),
        _ => (ErrorKind::Unknown, "Unknown iceberg error"),
    };

    etl_error!(kind, description, err.to_string())
}

/// Converts Arrow errors to ETL errors with appropriate classification.
///
/// Maps Arrow error types to ETL error kinds for consistent error handling.
pub(crate) fn arrow_error_to_etl_error(err: ArrowError) -> EtlError {
    let (kind, description) = match err {
        ArrowError::NotYetImplemented(_) => {
            (ErrorKind::Unknown, "Arrow feature not yet implemented")
        }
        ArrowError::ExternalError(_) => (ErrorKind::Unknown, "External Arrow error"),
        ArrowError::CastError(_) => (ErrorKind::InvalidData, "Arrow type cast failed"),
        ArrowError::MemoryError(_) => (ErrorKind::Unknown, "Arrow memory error"),
        ArrowError::ParseError(_) => (ErrorKind::InvalidData, "Arrow parsing failed"),
        ArrowError::SchemaError(_) => (ErrorKind::InvalidData, "Arrow schema error"),
        ArrowError::ComputeError(_) => (ErrorKind::Unknown, "Arrow computation failed"),
        ArrowError::DivideByZero => (ErrorKind::InvalidData, "Arrow divide by zero"),
        ArrowError::ArithmeticOverflow(_) => (ErrorKind::InvalidData, "Arrow arithmetic overflow"),
        ArrowError::CsvError(_) => (ErrorKind::InvalidData, "Arrow CSV error"),
        ArrowError::JsonError(_) => (ErrorKind::InvalidData, "Arrow JSON error"),
        ArrowError::IoError(_, _) => (ErrorKind::Unknown, "Arrow IO error"),
        ArrowError::IpcError(_) => (ErrorKind::Unknown, "Arrow IPC error"),
        ArrowError::InvalidArgumentError(_) => (ErrorKind::InvalidData, "Arrow invalid argument"),
        ArrowError::ParquetError(_) => (ErrorKind::InvalidData, "Arrow Parquet error"),
        ArrowError::CDataInterface(_) => (ErrorKind::Unknown, "Arrow C Data Interface error"),
        ArrowError::DictionaryKeyOverflowError => {
            (ErrorKind::InvalidData, "Arrow dictionary key overflow")
        }
        ArrowError::RunEndIndexOverflowError => {
            (ErrorKind::InvalidData, "Arrow run end index overflow")
        }
    };

    etl_error!(kind, description, err.to_string())
}
