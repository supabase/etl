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

    #[error("database '{0}' not found")]
    DatabaseNotFound(String),

    #[error("schema '{schema}' not found in database '{database}'")]
    SchemaNotFound { database: String, schema: String },
}

impl From<Error> for EtlError {
    fn from(err: Error) -> Self {
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
        if let Some(status_code) = response.as_ref().and_then(|response| response.status_code) {
            return Self::from_api_status_code(
                status_code,
                "Snowpipe API returned an unsuccessful status.".to_owned(),
            );
        }

        match (status, response.and_then(|response| response.code)) {
            (StatusCode::BAD_REQUEST, Some(code))
                if code == "STALE_CONTINUATION_TOKEN_SEQUENCER" =>
            {
                Self::StaleContinuation
            }
            (StatusCode::CONFLICT, Some(code)) if code == "ERR_CHANNEL_HAS_UNCOMMITTED_DATA" => {
                Self::ChannelHasUncommittedRows
            }
            (StatusCode::NOT_FOUND, _) => Self::ChannelNotFound,
            _ => Self::HttpStatus { status },
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
        matches!(self, Self::StaleContinuation | Self::ChannelNotFound)
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
