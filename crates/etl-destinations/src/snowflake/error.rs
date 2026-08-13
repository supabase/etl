use etl::error::{ErrorKind, EtlError};
use reqwest::StatusCode;
use serde::Deserialize;

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
            Self::Encoding(_) => AppendFailureType::Encoding,
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
            Error::MissingTableColumn { .. } | Error::UnexpectedTableColumn { .. } => {
                unreachable!("schema errors return above")
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
