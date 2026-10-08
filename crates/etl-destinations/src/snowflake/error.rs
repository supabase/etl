use etl::{
    error::{ErrorKind, EtlError},
    schema::TableId,
};
use reqwest::StatusCode;
use serde::Deserialize;

use crate::snowflake::encoding::CdcOperation;

/// Name and serialized length of the largest column in a rejected row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargestColumn {
    /// Destination column name.
    pub name: String,
    /// Serialized JSON length of the value, including string quotes.
    pub serialized_bytes: usize,
}

/// Formats the largest-column clause of [`Error::RowTooLarge`].
fn describe_largest_column(largest_column: &Option<LargestColumn>) -> String {
    match largest_column {
        Some(column) => {
            format!(", largest column {} {} B", column.name, column.serialized_bytes)
        }
        None => String::new(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP transport error: {0}")]
    HttpTransport(#[from] reqwest::Error),

    #[error("HTTP status {status}: {body}")]
    HttpStatus { status: StatusCode, body: String },

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("SQL error{}: {message}", statement_handle.as_ref().map(|h| format!(" (handle {h})")).unwrap_or_default())]
    Sql { statement_handle: Option<String>, message: String },

    #[error(transparent)]
    Snowpipe(#[from] SnowpipeError),

    #[error("Channel error: {0}")]
    Channel(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    /// A PostgreSQL JSON array contains a SQL-null element with no faithful
    /// representation in the Snowpipe JSON request.
    #[error(
        "SQL NULL at index {element_index} in JSON array column '{column_name}' cannot be encoded \
         without becoming JSON null"
    )]
    NullJsonArrayElement {
        /// Source column containing the array.
        column_name: String,
        /// Zero-based position of the first SQL-null element.
        element_index: usize,
    },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Snowflake table '{table_name}' is missing column '{column_name}'")]
    MissingTableColumn { table_name: String, column_name: String },

    #[error("Snowflake table '{table_name}' has unexpected column '{column_name}'")]
    UnexpectedTableColumn { table_name: String, column_name: String },

    #[error("database '{0}' not found")]
    DatabaseNotFound(String),

    #[error("schema '{schema}' not found in database '{database}'")]
    SchemaNotFound { database: String, schema: String },

    /// A single row cannot be sent: its complete compressed frame exceeds the
    /// Snowflake request limit at the configured compression level.
    #[error(
        "Row for table {table_id} ({operation}, {column_count} columns, {serialized_bytes} B \
         serialized{}) compresses to at least {compressed_lower_bound} B, over the \
         {request_limit} B Snowflake request limit",
        describe_largest_column(largest_column)
    )]
    RowTooLarge {
        /// Source table whose row was rejected.
        table_id: TableId,
        /// CDC operation the row carried.
        operation: CdcOperation,
        /// Number of destination columns in the row.
        column_count: usize,
        /// Exact NDJSON line length, including the newline.
        serialized_bytes: usize,
        /// Largest column by serialized length, when it could be measured.
        largest_column: Option<LargestColumn>,
        /// Output length at which compression was stopped.
        compressed_lower_bound: usize,
        /// Request limit the frame had to fit.
        request_limit: usize,
    },
}

/// Stable, low-cardinality classification for a failed Snowpipe append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AppendFailureType {
    /// Authentication or token refresh failed.
    Authentication,
    /// The HTTP request failed before Snowflake returned a response.
    Transport,
    /// Snowflake returned an unsuccessful HTTP or SQL response.
    Provider,
    /// Snowpipe returned a structured API failure.
    SnowpipeApi,
    /// Channel state or lifecycle validation failed.
    Channel,
    /// Request or response encoding failed.
    Encoding,
    /// A single row cannot fit the request limit.
    RowTooLarge,
    /// Destination configuration is invalid or incomplete.
    Configuration,
}

impl AppendFailureType {
    /// Returns the stable metric label value for this failure type.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Authentication => "authentication",
            Self::Transport => "transport",
            Self::Provider => "provider",
            Self::SnowpipeApi => "snowpipe_api",
            Self::Channel => "channel",
            Self::Encoding => "encoding",
            Self::RowTooLarge => "row_too_large",
            Self::Configuration => "configuration",
        }
    }
}

impl Error {
    /// Classifies this error for append-failure metrics.
    pub(super) const fn append_failure_type(&self) -> AppendFailureType {
        match self {
            Self::HttpTransport(_) => AppendFailureType::Transport,
            Self::HttpStatus { .. } | Self::Sql { .. } => AppendFailureType::Provider,
            Self::Auth(_) | Self::Snowpipe(SnowpipeError::AuthenticationExpired) => {
                AppendFailureType::Authentication
            }
            Self::Snowpipe(
                SnowpipeError::StaleContinuation
                | SnowpipeError::ChannelInvalidated
                | SnowpipeError::ChannelHasUncommittedRows
                | SnowpipeError::ChannelNotFound,
            )
            | Self::Channel(_) => AppendFailureType::Channel,
            Self::Snowpipe(SnowpipeError::ApiStatus { .. }) => AppendFailureType::SnowpipeApi,
            Self::Snowpipe(SnowpipeError::HttpStatus { .. }) => AppendFailureType::Provider,
            Self::Encoding(_) | Self::NullJsonArrayElement { .. } => AppendFailureType::Encoding,
            Self::RowTooLarge { .. } => AppendFailureType::RowTooLarge,
            Self::Config(_)
            | Self::MissingTableColumn { .. }
            | Self::UnexpectedTableColumn { .. }
            | Self::DatabaseNotFound(_)
            | Self::SchemaNotFound { .. } => AppendFailureType::Configuration,
        }
    }
}

impl From<Error> for EtlError {
    fn from(err: Error) -> Self {
        if matches!(&err, Error::MissingTableColumn { .. } | Error::UnexpectedTableColumn { .. }) {
            return etl::etl_error!(
                ErrorKind::CorruptedTableSchema,
                "Snowflake table schema is incompatible",
                source: err
            );
        }
        if matches!(&err, Error::RowTooLarge { .. }) {
            return etl::etl_error!(
                ErrorKind::UnsupportedValueInDestination,
                "Snowflake cannot accept a row of this size",
                source: err
            );
        }

        if matches!(&err, Error::NullJsonArrayElement { .. }) {
            return etl::etl_error!(
                ErrorKind::NullValuesNotSupportedInArrayInDestination,
                "Snowflake cannot preserve SQL NULL elements in JSON arrays",
                source: err
            );
        }

        let (kind, description) = match &err {
            Error::HttpTransport(_) => {
                (ErrorKind::DestinationError, "Snowflake HTTP transport error")
            }
            Error::HttpStatus { status, .. } if status.is_server_error() => {
                (ErrorKind::DestinationError, "Snowflake server error")
            }
            Error::HttpStatus { .. } => (ErrorKind::DestinationError, "Snowflake HTTP error"),
            Error::Auth(_) => (ErrorKind::DestinationError, "Snowflake authentication failed"),
            Error::Sql { .. } => (ErrorKind::DestinationError, "Snowflake SQL execution failed"),
            Error::Snowpipe(_) => (ErrorKind::DestinationError, "Snowpipe streaming error"),
            Error::Channel(_) => (ErrorKind::DestinationError, "Snowflake channel error"),
            Error::Encoding(_) => (ErrorKind::InvalidData, "Snowflake encoding error"),
            Error::Config(_) => (ErrorKind::ConfigError, "Snowflake configuration error"),
            Error::MissingTableColumn { .. }
            | Error::UnexpectedTableColumn { .. }
            | Error::RowTooLarge { .. }
            | Error::NullJsonArrayElement { .. } => {
                unreachable!("schema and unsupported value errors return above")
            }
            Error::DatabaseNotFound(_) => (ErrorKind::ConfigError, "Snowflake database not found"),
            Error::SchemaNotFound { .. } => (ErrorKind::ConfigError, "Snowflake schema not found"),
        };
        etl::etl_error!(kind, description, err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Snowpipe Streaming API failure classified for lifecycle and retry handling.
#[derive(Debug, thiserror::Error)]
pub enum SnowpipeError {
    /// The continuation token is older than Snowflake expects for the channel.
    #[error("Snowpipe stale continuation token")]
    StaleContinuation,

    /// Another client superseded or otherwise invalidated this channel.
    #[error("Snowpipe channel invalidated")]
    ChannelInvalidated,

    /// A safe open or drop was refused because the channel has uncommitted
    /// rows.
    #[error("Snowpipe channel has uncommitted rows")]
    ChannelHasUncommittedRows,

    /// The requested streaming channel does not exist.
    #[error("Snowpipe channel not found")]
    ChannelNotFound,

    /// Snowflake reported that authentication expired for the request.
    #[error("Snowpipe authentication expired")]
    AuthenticationExpired,

    /// Snowpipe returned a numeric API status code.
    #[error("Snowpipe API error code {status_code}: {message}")]
    ApiStatus { status_code: u32, message: String },

    /// Snowpipe returned an unsuccessful HTTP status without a known API code.
    #[error("Snowpipe HTTP status {status}")]
    HttpStatus { status: StatusCode },
}

impl SnowpipeError {
    /// Classifies an unsuccessful Snowpipe Streaming HTTP response.
    pub fn from_response(status: StatusCode, body: String) -> Self {
        let response = serde_json::from_str::<SnowpipeErrorResponse>(&body).ok();
        match (status, response.as_ref().and_then(|response| response.code.as_deref())) {
            (StatusCode::BAD_REQUEST, Some("STALE_CONTINUATION_TOKEN_SEQUENCER")) => {
                return Self::StaleContinuation;
            }
            (StatusCode::CONFLICT, Some("ERR_CHANNEL_HAS_UNCOMMITTED_DATA")) => {
                return Self::ChannelHasUncommittedRows;
            }
            (StatusCode::CONFLICT, _) => return Self::ChannelInvalidated,
            (StatusCode::NOT_FOUND, _) => return Self::ChannelNotFound,
            _ => {}
        }

        if let Some(status_code) = response.as_ref().and_then(|response| response.status_code) {
            Self::from_api_status_code(
                status_code,
                "Snowpipe API returned an unsuccessful status.".to_owned(),
            )
        } else {
            Self::HttpStatus { status }
        }
    }

    fn from_api_status_code(status_code: u32, message: String) -> Self {
        match status_code {
            3 => Self::AuthenticationExpired,
            4 => Self::StaleContinuation,
            _ => Self::ApiStatus { status_code, message },
        }
    }

    /// Returns whether the channel can be reopened for this error.
    pub fn is_reopenable_channel_error(&self) -> bool {
        matches!(self, Self::StaleContinuation | Self::ChannelInvalidated | Self::ChannelNotFound)
    }

    /// Returns whether this error is an authentication failure.
    pub fn is_authentication_expired(&self) -> bool {
        matches!(self, Self::AuthenticationExpired)
    }
}

/// Minimal Snowpipe error response envelope used for classification.
#[derive(Deserialize)]
struct SnowpipeErrorResponse {
    /// String error code, when Snowflake returns one.
    #[serde(default)]
    code: Option<String>,
    /// Numeric Snowpipe API status code, when Snowflake returns one.
    #[serde(default)]
    status_code: Option<u32>,
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    /// Unsupported array elements retain their typed cause and stable kind.
    #[test]
    fn json_array_null_error_is_preserved() {
        let error =
            Error::NullJsonArrayElement { column_name: "payload".to_owned(), element_index: 2 };
        assert_eq!(error.append_failure_type(), AppendFailureType::Encoding);
        let error = EtlError::from(error);
        assert_eq!(error.kind(), ErrorKind::NullValuesNotSupportedInArrayInDestination);
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn table_schema_error_is_preserved() {
        let error = Error::MissingTableColumn {
            table_name: "events".to_owned(),
            column_name: "id".to_owned(),
        };

        let error = EtlError::from(error);
        let source = error.source().expect("Snowflake error should be preserved");

        assert_eq!(error.kind(), ErrorKind::CorruptedTableSchema);
        assert_eq!(source.to_string(), "Snowflake table 'events' is missing column 'id'");
    }

    #[test]
    fn row_too_large_is_an_unsupported_value_with_structural_details() {
        let error = Error::RowTooLarge {
            table_id: etl::schema::TableId::new(42),
            operation: crate::snowflake::CdcOperation::Update,
            column_count: 3,
            serialized_bytes: 13_613_900,
            largest_column: Some(LargestColumn {
                name: "payload".to_owned(),
                serialized_bytes: 13_613_000,
            }),
            compressed_lower_bound: 5_452_596,
            request_limit: 4_194_304,
        };
        assert_eq!(error.append_failure_type(), AppendFailureType::RowTooLarge);
        assert_eq!(
            error.to_string(),
            "Row for table 42 (update, 3 columns, 13613900 B serialized, largest column payload \
             13613000 B) compresses to at least 5452596 B, over the 4194304 B Snowflake request \
             limit"
        );

        let error = EtlError::from(error);
        assert_eq!(error.kind(), ErrorKind::UnsupportedValueInDestination);
        let source = error.source().unwrap();
        assert!(source.to_string().starts_with("Row for table 42"));
    }

    #[test]
    fn response_errors_are_classified_by_stable_protocol_signals() {
        enum Expected {
            StaleContinuation,
            ChannelHasUncommittedRows,
            ChannelInvalidated,
            ChannelNotFound,
            AuthenticationExpired,
            ApiStatus(u32),
            HttpStatus(StatusCode),
        }

        let cases = [
            (
                "stale channel sequencer",
                StatusCode::BAD_REQUEST,
                r#"{"code":"STALE_CONTINUATION_TOKEN_SEQUENCER","status_code":3}"#,
                Expected::StaleContinuation,
            ),
            (
                "uncommitted rows conflict",
                StatusCode::CONFLICT,
                r#"{"code":"ERR_CHANNEL_HAS_UNCOMMITTED_DATA","status_code":4}"#,
                Expected::ChannelHasUncommittedRows,
            ),
            (
                "other channel conflict",
                StatusCode::CONFLICT,
                r#"{"code":"ERR_CHANNEL_MUST_BE_REOPENED","status_code":3}"#,
                Expected::ChannelInvalidated,
            ),
            (
                "unstructured channel conflict",
                StatusCode::CONFLICT,
                "not JSON",
                Expected::ChannelInvalidated,
            ),
            (
                "missing channel",
                StatusCode::NOT_FOUND,
                r#"{"status_code":3}"#,
                Expected::ChannelNotFound,
            ),
            (
                "expired authentication API status",
                StatusCode::BAD_REQUEST,
                r#"{"status_code":3}"#,
                Expected::AuthenticationExpired,
            ),
            (
                "stale continuation API status",
                StatusCode::BAD_REQUEST,
                r#"{"status_code":4}"#,
                Expected::StaleContinuation,
            ),
            (
                "other API status",
                StatusCode::BAD_REQUEST,
                r#"{"status_code":99}"#,
                Expected::ApiStatus(99),
            ),
            (
                "unstructured HTTP error",
                StatusCode::INTERNAL_SERVER_ERROR,
                "not JSON",
                Expected::HttpStatus(StatusCode::INTERNAL_SERVER_ERROR),
            ),
        ];

        for (case, status, body, expected) in cases {
            let error = SnowpipeError::from_response(status, body.to_owned());
            let matches = match (expected, &error) {
                (Expected::StaleContinuation, SnowpipeError::StaleContinuation)
                | (Expected::ChannelHasUncommittedRows, SnowpipeError::ChannelHasUncommittedRows)
                | (Expected::ChannelInvalidated, SnowpipeError::ChannelInvalidated)
                | (Expected::ChannelNotFound, SnowpipeError::ChannelNotFound)
                | (Expected::AuthenticationExpired, SnowpipeError::AuthenticationExpired) => true,
                (Expected::ApiStatus(expected), SnowpipeError::ApiStatus { status_code, .. }) => {
                    expected == *status_code
                }
                (Expected::HttpStatus(expected), SnowpipeError::HttpStatus { status }) => {
                    expected == *status
                }
                _ => false,
            };

            assert!(matches, "{case}: {error:?}");
        }
    }
}
