use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Validation error: {0}")]
    Other(String),
}
