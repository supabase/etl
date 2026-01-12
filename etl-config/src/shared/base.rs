use thiserror::Error;

/// Configuration validation errors.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// Maximum table sync workers cannot be zero.
    #[error("`max_table_sync_workers` cannot be zero")]
    MaxTableSyncWorkersZero,
    /// Maximum retry attempts for table errors cannot be zero.
    #[error("`table_error_retry_max_attempts` cannot be zero")]
    TableErrorRetryMaxAttemptsZero,
    /// TLS is enabled but no trusted root certificates are provided.
    #[error("Invalid TLS config: `trusted_root_certs` must be set when `enabled` is true")]
    MissingTrustedRootCerts,
    /// Invalid heartbeat configuration.
    #[error("Invalid heartbeat config: {0}")]
    HeartbeatConfig(String),
}
