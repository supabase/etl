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
