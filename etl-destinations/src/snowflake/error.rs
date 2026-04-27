use etl::error::{ErrorKind, EtlError};

#[derive(Debug, thiserror::Error)]
pub enum SnowflakeError {
    #[error("HTTP transport error: {0}")]
    HttpTransport(#[from] reqwest::Error),

    #[error("HTTP status {status}: {body}")]
    HttpStatus { status: u16, body: String },

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("SQL error{}: {message}", statement_handle.as_ref().map(|h| format!(" (handle {h})")).unwrap_or_default())]
    Sql { statement_handle: Option<String>, message: String },

    #[error("Snowpipe error (code {status_code}): {message}")]
    Snowpipe { status_code: u32, message: String },

    #[error("channel error: {0}")]
    Channel(String),

    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("configuration error: {0}")]
    Config(String),
}

impl From<SnowflakeError> for EtlError {
    fn from(err: SnowflakeError) -> Self {
        let (kind, description) = match &err {
            SnowflakeError::HttpTransport(_) => {
                (ErrorKind::DestinationError, "Snowflake HTTP transport error")
            }
            SnowflakeError::HttpStatus { status, .. } if *status >= 500 => {
                (ErrorKind::DestinationError, "Snowflake server error")
            }
            SnowflakeError::HttpStatus { .. } => {
                (ErrorKind::DestinationError, "Snowflake HTTP error")
            }
            SnowflakeError::Auth(_) => {
                (ErrorKind::DestinationError, "Snowflake authentication failed")
            }
            SnowflakeError::Sql { .. } => {
                (ErrorKind::DestinationError, "Snowflake SQL execution failed")
            }
            SnowflakeError::Snowpipe { .. } => {
                (ErrorKind::DestinationError, "Snowpipe streaming error")
            }
            SnowflakeError::Channel(_) => (ErrorKind::DestinationError, "Snowflake channel error"),
            SnowflakeError::Encoding(_) => (ErrorKind::InvalidData, "Snowflake encoding error"),
            SnowflakeError::Config(_) => (ErrorKind::ConfigError, "Snowflake configuration error"),
        };
        etl::etl_error!(kind, description, err.to_string())
    }
}
