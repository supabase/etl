use arrow::error::ArrowError;
use etl::{
    error::{ErrorClass, ErrorScope, EtlError},
    etl_error,
};

/// Converts iceberg errors to ETL errors with appropriate classification.
///
/// Maps iceberg error types to ETL error kinds for consistent error handling.
pub(crate) fn iceberg_error_to_etl_error(err: iceberg::Error) -> EtlError {
    let (scope, class, description) = match err.kind() {
        iceberg::ErrorKind::PreconditionFailed => (
            ErrorScope::Destination,
            ErrorClass::InvalidState,
            "Iceberg precondition failed",
        ),
        iceberg::ErrorKind::Unexpected => (
            ErrorScope::Destination,
            ErrorClass::Unknown,
            "An unexpected error occurred",
        ),
        iceberg::ErrorKind::DataInvalid => (
            ErrorScope::Destination,
            ErrorClass::InvalidData,
            "Invalid iceberg data",
        ),
        iceberg::ErrorKind::NamespaceAlreadyExists => (
            ErrorScope::Destination,
            ErrorClass::NamespaceAlreadyExists,
            "Iceberg namespace already exists",
        ),
        iceberg::ErrorKind::TableAlreadyExists => (
            ErrorScope::Destination,
            ErrorClass::TableAlreadyExists,
            "Iceberg table already exists",
        ),
        iceberg::ErrorKind::NamespaceNotFound => (
            ErrorScope::Destination,
            ErrorClass::NamespaceMissing,
            "Iceberg namespace missing",
        ),
        iceberg::ErrorKind::TableNotFound => (
            ErrorScope::Destination,
            ErrorClass::TableMissing,
            "Iceberg table missing",
        ),
        iceberg::ErrorKind::FeatureUnsupported => (
            ErrorScope::Destination,
            ErrorClass::Unknown,
            "Unsupported iceberg feature was used",
        ),
        iceberg::ErrorKind::CatalogCommitConflicts => (
            ErrorScope::Destination,
            ErrorClass::Unknown,
            "Iceberg commit conflicts occurred",
        ),
        _ => (
            ErrorScope::Destination,
            ErrorClass::Unknown,
            "Unknown iceberg error",
        ),
    };

    etl_error!(scope: scope, class, description, err.to_string())
}

/// Converts Arrow errors to ETL errors with appropriate classification.
///
/// Maps Arrow error types to ETL error kinds for consistent error handling.
pub(crate) fn arrow_error_to_etl_error(err: ArrowError) -> EtlError {
    let (scope, class, description) = match err {
        ArrowError::NotYetImplemented(_) => (
            ErrorScope::Internal,
            ErrorClass::Unknown,
            "Arrow feature not yet implemented",
        ),
        ArrowError::ExternalError(_) => (
            ErrorScope::Internal,
            ErrorClass::Unknown,
            "External Arrow error",
        ),
        ArrowError::CastError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow type cast failed",
        ),
        ArrowError::MemoryError(_) => (
            ErrorScope::Internal,
            ErrorClass::Unknown,
            "Arrow memory error",
        ),
        ArrowError::ParseError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow parsing failed",
        ),
        ArrowError::SchemaError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow schema error",
        ),
        ArrowError::ComputeError(_) => (
            ErrorScope::Internal,
            ErrorClass::Unknown,
            "Arrow computation failed",
        ),
        ArrowError::DivideByZero => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow divide by zero",
        ),
        ArrowError::ArithmeticOverflow(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow arithmetic overflow",
        ),
        ArrowError::CsvError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow CSV error",
        ),
        ArrowError::JsonError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow JSON error",
        ),
        ArrowError::IoError(_, _) => (ErrorScope::Internal, ErrorClass::IoError, "Arrow IO error"),
        ArrowError::IpcError(_) => (ErrorScope::Internal, ErrorClass::Unknown, "Arrow IPC error"),
        ArrowError::InvalidArgumentError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow invalid argument",
        ),
        ArrowError::ParquetError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow Parquet error",
        ),
        ArrowError::CDataInterface(_) => (
            ErrorScope::Internal,
            ErrorClass::Unknown,
            "Arrow C Data Interface error",
        ),
        ArrowError::DictionaryKeyOverflowError => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow dictionary key overflow",
        ),
        ArrowError::RunEndIndexOverflowError => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow run end index overflow",
        ),
        ArrowError::AvroError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow Avro error",
        ),
        ArrowError::OffsetOverflowError(_) => (
            ErrorScope::Internal,
            ErrorClass::InvalidData,
            "Arrow offset overflow error",
        ),
    };

    etl_error!(scope: scope, class, description, err.to_string())
}
