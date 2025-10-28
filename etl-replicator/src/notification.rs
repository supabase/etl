//! Error notification system for sending error reports to Supabase API.
//!
//! Provides functionality to notify external systems about errors that occur during
//! replication. Errors are hashed to provide a stable identifier for grouping and
//! deduplication purposes.

use etl::error::EtlError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use tracing::{error, info, warn};

/// Request payload for error notifications.
///
/// Contains error information to be sent to the Supabase API for tracking
/// and monitoring purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationRequest {
    /// Human-readable error message describing the failure.
    pub error_message: String,
    /// Stable hash of the error for grouping and deduplication.
    ///
    /// The hash is computed from error kind, description, and detail to
    /// provide a consistent identifier across multiple occurrences of the
    /// same error type.
    pub error_hash: String,
    /// Unique identifier for the pipeline that encountered the error.
    pub pipeline_id: String,
    /// Supabase project reference identifier.
    pub project_ref: String,
}

/// Client for sending error notifications to Supabase API.
///
/// Provides async methods to notify external systems about errors that occur
/// during replication. Uses reqwest for HTTP communication and handles
/// errors gracefully without blocking pipeline operations.
#[derive(Debug, Clone)]
pub struct ErrorNotificationClient {
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Supabase API URL for error notifications.
    api_url: String,
    /// Supabase API key for authentication.
    api_key: String,
    /// Supabase project reference.
    project_ref: String,
    /// Pipeline identifier.
    pipeline_id: String,
}

impl ErrorNotificationClient {
    /// Creates a new error notification client.
    ///
    /// The client is configured with the necessary credentials and endpoints
    /// to send error notifications to the Supabase API.
    pub fn new(api_url: String, api_key: String, project_ref: String, pipeline_id: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            api_url,
            api_key,
            project_ref,
            pipeline_id,
        }
    }

    /// Sends an error notification to the Supabase API.
    ///
    /// This method is fire-and-forget - it logs any failures but does not
    /// propagate them to avoid disrupting the pipeline. The notification is
    /// sent asynchronously without blocking pipeline operations.
    pub async fn notify_error(&self, error: &EtlError) {
        let error_message = format!("{}", error);
        let error_hash = compute_error_hash(error);

        let notification = NotificationRequest {
            error_message,
            error_hash,
            pipeline_id: self.pipeline_id.clone(),
            project_ref: self.project_ref.clone(),
        };

        info!(
            pipeline_id = %self.pipeline_id,
            error_hash = %notification.error_hash,
            "sending error notification to Supabase API"
        );

        match self.send_notification(notification).await {
            Ok(_) => {
                info!(
                    pipeline_id = %self.pipeline_id,
                    "error notification sent successfully"
                );
            }
            Err(e) => {
                warn!(
                    pipeline_id = %self.pipeline_id,
                    error = %e,
                    "failed to send error notification, continuing without notification"
                );
            }
        }
    }

    /// Sends the notification request to the API endpoint.
    async fn send_notification(&self, notification: NotificationRequest) -> Result<(), Box<dyn Error>> {
        let response = self
            .client
            .post(&self.api_url)
            .header("apikey", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&notification)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "<unable to read body>".to_string());
            error!(
                status = %status,
                body = %body,
                "error notification request failed"
            );
            return Err(format!("API returned status {}: {}", status, body).into());
        }

        Ok(())
    }
}

/// Computes a stable hash for an error.
///
/// Uses the [`Hash`] trait implementation from [`EtlError`], which hashes only
/// the error kind and static description (intentionally excluding location and
/// detail fields). The hash is computed using SHA-256 for stability and consistency.
///
/// This provides a consistent identifier across multiple occurrences of the
/// same error type, enabling grouping and deduplication in monitoring systems.
pub fn compute_error_hash(error: &EtlError) -> String {
    // First compute the hash using EtlError's Hash implementation.
    let mut std_hasher = DefaultHasher::new();
    error.hash(&mut std_hasher);
    let hash_value = std_hasher.finish();

    // Convert to SHA-256 hex string for consistency and to avoid collisions.
    let mut sha_hasher = Sha256::new();
    sha_hasher.update(hash_value.to_le_bytes());
    format!("{:x}", sha_hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::error::ErrorKind;

    #[test]
    fn test_compute_error_hash_stability() {
        let err1 = EtlError::from((
            ErrorKind::SourceConnectionFailed,
            "Database connection failed",
        ));
        let err2 = EtlError::from((
            ErrorKind::SourceConnectionFailed,
            "Database connection failed",
        ));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        // Hashes should be identical for the same error kind and description.
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_compute_error_hash_with_detail() {
        let err1 = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "Query execution failed",
            "Table 'users' not found".to_string(),
        ));
        let err2 = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "Query execution failed",
            "Table 'users' not found".to_string(),
        ));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_compute_error_hash_different_errors() {
        let err1 = EtlError::from((
            ErrorKind::SourceConnectionFailed,
            "Database connection failed",
        ));
        let err2 = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "Query execution failed",
        ));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        // Different errors should produce different hashes.
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_compute_error_hash_ignores_detail() {
        let err1 = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "Query failed",
            "Table 'users' not found".to_string(),
        ));
        let err2 = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "Query failed",
            "Table 'orders' not found".to_string(),
        ));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        // Details are ignored for stable hashing - same kind + description = same hash.
        assert_eq!(hash1, hash2);
    }
}
