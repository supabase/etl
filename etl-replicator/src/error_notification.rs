use etl::error::EtlResult;
use etl::state::table::TableReplicationPhase;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{TableId, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// The endpoint of the Supabase API to which error notifications are sent.
const API_ENDPOINT: &str = "system/etl/error-notification";

/// Request payload for error notifications.
///
/// Contains error information to be sent to the Supabase API for tracking
/// and monitoring purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationRequest {
    /// Unique identifier for the pipeline that encountered the error.
    pub pipeline_id: String,
    /// Supabase project reference identifier.
    pub project_ref: String,
    /// Human-readable error message describing the failure.
    pub error_message: String,
    /// Stable hash of the error for grouping and deduplication.
    ///
    /// The hash is computed from error kind, description, and detail to
    /// provide a consistent identifier across multiple occurrences of the
    /// same error type.
    pub error_hash: String,
}

/// Response from the error notification API.
///
/// Contains information about whether the notification was successfully
/// processed and if it was deduplicated based on the error hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationResponse {
    /// Success message from the API.
    pub message: String,
    /// Whether the notification was deduplicated based on the error hash.
    pub deduplicated: bool,
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
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            client,
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
    pub async fn notify_error<H: Hash>(&self, error_message: String, error_hash: H) {
        let error_hash = compute_error_hash(error_hash);

        let notification = NotificationRequest {
            pipeline_id: self.pipeline_id.clone(),
            project_ref: self.project_ref.clone(),
            error_message,
            error_hash,
        };

        info!(
            pipeline_id = %self.pipeline_id,
            error_hash = %notification.error_hash,
            "sending error notification to supabase api"
        );

        match self.send_notification(notification).await {
            Ok(response) => {
                info!(
                    pipeline_id = %self.pipeline_id,
                    message = %response.message,
                    deduplicated = %response.deduplicated,
                    "error notification sent successfully"
                );
            }
            Err(err) => {
                warn!(
                    pipeline_id = %self.pipeline_id,
                    error = %err,
                    "failed to send error notification, continuing without notification"
                );
            }
        }
    }

    /// Returns the URL for the error notification endpoint.
    fn error_notification_url(&self) -> String {
        format!("{}/{}", self.api_url, API_ENDPOINT)
    }

    /// Sends the notification request to the API endpoint.
    async fn send_notification(
        &self,
        notification: NotificationRequest,
    ) -> Result<NotificationResponse, Box<dyn Error>> {
        let response = self
            .client
            .post(self.error_notification_url())
            .header("apikey", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&notification)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unable to read body>".to_string());
            return Err(format!("API returned status {status}: {body}").into());
        }

        let notification_response = response.json::<NotificationResponse>().await?;
        Ok(notification_response)
    }
}

/// State store wrapper that sends notifications when tables transition to `Errored`.
#[derive(Debug, Clone)]
pub struct ErrorNotifyingStateStore<S> {
    inner: S,
    notification_client: Option<Arc<ErrorNotificationClient>>,
}

impl<S> ErrorNotifyingStateStore<S> {
    /// Creates a new wrapper around `inner`.
    pub fn new(inner: S, notification_client: Option<ErrorNotificationClient>) -> Self {
        Self {
            inner,
            notification_client: notification_client.map(Arc::new),
        }
    }

    /// Sends notifications for errored table state updates after they were stored successfully.
    async fn notify_errored_updates(&self, updates: &[(TableId, TableReplicationPhase)]) {
        let Some(notification_client) = &self.notification_client else {
            return;
        };

        for (table_id, phase) in updates {
            let TableReplicationPhase::Errored { source_err, .. } = phase else {
                continue;
            };

            info!(
                table_id = table_id.0,
                "sending notification for table replication error"
            );

            notification_client
                .notify_error(source_err.to_string(), source_err)
                .await;
        }
    }
}

impl<S> StateStore for ErrorNotifyingStateStore<S>
where
    S: StateStore + Send + Sync,
{
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        self.inner.get_table_replication_state(table_id).await
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        self.inner.get_table_replication_states().await
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        self.inner.load_table_replication_states().await
    }

    async fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> EtlResult<()> {
        self.inner
            .update_table_replication_states(updates.clone())
            .await?;
        self.notify_errored_updates(&updates).await;

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        self.inner.rollback_table_replication_state(table_id).await
    }

    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        self.inner.get_table_mapping(source_table_id).await
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        self.inner.get_table_mappings().await
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        self.inner.load_table_mappings().await
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        self.inner
            .store_table_mapping(source_table_id, destination_table_id)
            .await
    }
}

impl<S> SchemaStore for ErrorNotifyingStateStore<S>
where
    S: SchemaStore + Send + Sync,
{
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        self.inner.get_table_schema(table_id).await
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        self.inner.get_table_schemas().await
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        self.inner.load_table_schemas().await
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        self.inner.store_table_schema(table_schema).await
    }
}

impl<S> CleanupStore for ErrorNotifyingStateStore<S>
where
    S: CleanupStore + Send + Sync,
{
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        self.inner.cleanup_table_state(table_id).await
    }
}

/// Computes a stable hash for an error.
///
/// This provides a consistent identifier across multiple occurrences of the
/// same error type, enabling grouping and deduplication in monitoring systems.
pub fn compute_error_hash<H: Hash>(error_hash: H) -> String {
    let mut hasher = DefaultHasher::new();
    error_hash.hash(&mut hasher);
    let hash_value = hasher.finish();

    format!("{hash_value:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::error::{ErrorKind, EtlError};

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
        let err2 = EtlError::from((ErrorKind::SourceQueryFailed, "Query execution failed"));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        // Different errors should produce different hashes.
        assert_ne!(hash1, hash2);
    }
}
