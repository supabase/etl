use arrow::error::ArrowError;
use etl::{
    error::{ErrorKind, EtlError, Retryability},
    etl_error,
};

use crate::retry::{ClassifiedError, EtlErrorExt};

/// Iceberg marks the errors that may succeed when retried, such as commit
/// conflicts. It leaves a commit whose outcome is unknown unmarked, because
/// retrying that commit could apply it twice.
impl ClassifiedError for iceberg::Error {
    fn retryability(&self) -> Retryability {
        if self.retryable() { Retryability::Retryable } else { Retryability::Permanent }
    }
}

/// Arrow errors come from encoding rows and fail the same way for the same
/// rows.
impl ClassifiedError for ArrowError {
    fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}

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
        iceberg::ErrorKind::NamespaceAlreadyExists => {
            (ErrorKind::DestinationNamespaceAlreadyExists, "Iceberg namespace already exists")
        }
        iceberg::ErrorKind::TableAlreadyExists => {
            (ErrorKind::DestinationTableAlreadyExists, "Iceberg table already exists")
        }
        iceberg::ErrorKind::NamespaceNotFound => {
            (ErrorKind::DestinationNamespaceMissing, "Iceberg namespace missing")
        }
        iceberg::ErrorKind::TableNotFound => {
            (ErrorKind::DestinationTableMissing, "Iceberg table missing")
        }
        iceberg::ErrorKind::FeatureUnsupported => {
            (ErrorKind::Unknown, "Unsupported iceberg feature was used")
        }
        iceberg::ErrorKind::CatalogCommitConflicts => {
            (ErrorKind::DestinationError, "Iceberg commit conflicts occurred")
        }
        _ => (ErrorKind::Unknown, "Unknown iceberg error"),
    };

    etl_error!(kind, description).caused_by(err)
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
        ArrowError::AvroError(_) => (ErrorKind::InvalidData, "Arrow Avro error"),
        ArrowError::OffsetOverflowError(_) => {
            (ErrorKind::InvalidData, "Arrow offset overflow error")
        }
    };

    etl_error!(kind, description).caused_by(err)
}

#[cfg(test)]
mod tests {
    use etl::error::{ErrorKind, Retryability};

    use crate::iceberg::error::iceberg_error_to_etl_error;

    /// Errors iceberg marks retryable, such as commit conflicts, are retryable
    /// after conversion; unmarked errors are permanent.
    #[test]
    fn iceberg_errors_keep_their_retryable_flag() {
        let conflict =
            iceberg::Error::new(iceberg::ErrorKind::CatalogCommitConflicts, "Commit conflict")
                .with_retryable(true);
        let unknown_commit =
            iceberg::Error::new(iceberg::ErrorKind::CatalogCommitConflicts, "Commit unknown");
        let invalid = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "Invalid data");

        let conflict = iceberg_error_to_etl_error(conflict);
        assert_eq!(conflict.kind(), ErrorKind::DestinationError);
        assert_eq!(conflict.retryability(), Retryability::Retryable);
        assert_eq!(
            iceberg_error_to_etl_error(unknown_commit).retryability(),
            Retryability::Permanent
        );
        assert_eq!(iceberg_error_to_etl_error(invalid).retryability(), Retryability::Permanent);
    }
}
