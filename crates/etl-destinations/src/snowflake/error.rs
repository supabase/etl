use etl::error::{ErrorKind, EtlError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
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

impl From<Error> for EtlError {
    fn from(err: Error) -> Self {
        let (kind, description) = match &err {
            Error::HttpTransport(_) => {
                (ErrorKind::DestinationError, "Snowflake HTTP transport error")
            }
            Error::HttpStatus { status, .. } if *status >= 500 => {
                (ErrorKind::DestinationError, "Snowflake server error")
            }
            Error::HttpStatus { .. } => (ErrorKind::DestinationError, "Snowflake HTTP error"),
            Error::Auth(_) => (ErrorKind::DestinationError, "Snowflake authentication failed"),
            Error::Sql { .. } => (ErrorKind::DestinationError, "Snowflake SQL execution failed"),
            Error::Snowpipe { .. } => (ErrorKind::DestinationError, "Snowpipe streaming error"),
            Error::Channel(_) => (ErrorKind::DestinationError, "Snowflake channel error"),
            Error::Encoding(_) => (ErrorKind::InvalidData, "Snowflake encoding error"),
            Error::Config(_) => (ErrorKind::ConfigError, "Snowflake configuration error"),
        };
        etl::etl_error!(kind, description, err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
