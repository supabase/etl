use std::{
    collections::hash_map::DefaultHasher,
    error::Error,
    hash::{Hash, Hasher},
    time::Duration,
};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Postmark template alias used for pipeline notifications.
const PIPELINE_NOTIFICATION_TEMPLATE_ALIAS: &str = "etl-pipeline-notification";
/// Prefix that distinguishes pipeline error deduplication keys.
const PIPELINE_ERROR_DEDUP_KEY_PREFIX: &str = "pipeline-error";
/// Customer-facing title for pipeline error emails.
const PIPELINE_ERROR_NOTIFICATION_TITLE: &str = "Replication pipeline error";
/// Customer-facing explanation for pipeline error emails.
const PIPELINE_ERROR_NOTIFICATION_MESSAGE: &str = "Your replication pipeline encountered an error \
                                                   during execution. Review the details below and \
                                                   check the logs for more information.";
/// Customer-facing label displayed above pipeline error details.
const PIPELINE_ERROR_NOTIFICATION_DETAILS_LABEL: &str = "Error details";

/// Request payload for an ETL email.
#[derive(Debug, Serialize)]
struct EtlEmailRequest<'a> {
    /// Postmark template alias to use for the email.
    template_alias: &'a str,
    /// Caller-provided key used to deduplicate the email.
    dedup_key: &'a str,
    /// Fields passed to the selected Postmark template.
    fields: PipelineErrorEmailFields<'a>,
}

/// Template fields for a pipeline error email.
#[derive(Debug, Serialize)]
struct PipelineErrorEmailFields<'a> {
    /// Unique identifier for the pipeline that encountered the error.
    pipeline_id: &'a str,
    /// Customer-facing email title.
    notification_title: &'a str,
    /// Customer-facing explanation of the notification.
    notification_message: &'a str,
    /// Label displayed above the error details.
    notification_details_label: &'a str,
    /// Human-readable error message describing the failure.
    notification_details: &'a str,
}

/// Response from the ETL email API.
///
/// Contains information about whether the email was successfully processed and
/// whether it was deduplicated.
#[derive(Debug, Deserialize)]
struct EtlEmailResponse {
    /// Whether the email was deduplicated.
    deduplicated: bool,
}

/// Client for sending error notifications to Supabase API.
///
/// Provides async methods to notify external systems about errors that occur
/// during replication. Uses reqwest for HTTP communication and handles errors
/// gracefully without blocking pipeline operations.
#[derive(Debug, Clone)]
pub(crate) struct ErrorNotificationClient {
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Supabase API URL for error notifications.
    api_url: String,
    /// Supabase API key for authentication.
    api_key: SecretString,
    /// Supabase project reference.
    project_ref: String,
    /// Pipeline identifier.
    pipeline_id: String,
}

impl ErrorNotificationClient {
    /// Creates a new error notification client.
    ///
    /// The client is configured with the necessary credentials and endpoints to
    /// send error notifications to the Supabase API.
    pub(crate) fn new(
        api_url: String,
        api_key: String,
        project_ref: String,
        pipeline_id: String,
    ) -> Result<Self, reqwest::Error> {
        // Custom authentication headers must never follow redirects to another
        // host.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        Ok(Self {
            client,
            api_url: api_url.trim_end_matches('/').to_owned(),
            api_key: api_key.into(),
            project_ref,
            pipeline_id,
        })
    }

    /// Sends an error notification to the Supabase API.
    ///
    /// This method is fire-and-forget - it logs any failures but does not
    /// propagate them to avoid disrupting the pipeline. The notification is
    /// sent asynchronously without blocking pipeline operations.
    pub(crate) async fn notify_error<H: Hash>(&self, error_message: String, error_hash: H) {
        let error_hash = compute_error_hash(error_hash);
        let dedup_key = pipeline_error_dedup_key(&self.pipeline_id, &error_hash);

        let email = self.pipeline_error_email_request(&dedup_key, &error_message);

        info!(
            error_hash = %error_hash,
            "sending error notification to supabase api"
        );

        match self.send_email(&email).await {
            Ok(response) => {
                info!(
                    deduplicated = %response.deduplicated,
                    "error notification sent successfully"
                );
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to send error notification, continuing without notification"
                );
            }
        }
    }

    /// Returns the URL for the ETL email endpoint.
    fn etl_email_url(&self) -> String {
        format!("{}/system/replication/{}/emails", self.api_url, self.project_ref)
    }

    /// Builds a pipeline error email request.
    fn pipeline_error_email_request<'a>(
        &'a self,
        dedup_key: &'a str,
        error_message: &'a str,
    ) -> EtlEmailRequest<'a> {
        EtlEmailRequest {
            template_alias: PIPELINE_NOTIFICATION_TEMPLATE_ALIAS,
            dedup_key,
            fields: PipelineErrorEmailFields {
                pipeline_id: &self.pipeline_id,
                notification_title: PIPELINE_ERROR_NOTIFICATION_TITLE,
                notification_message: PIPELINE_ERROR_NOTIFICATION_MESSAGE,
                notification_details_label: PIPELINE_ERROR_NOTIFICATION_DETAILS_LABEL,
                notification_details: error_message,
            },
        }
    }

    /// Builds an HTTP request for an ETL email.
    fn build_email_request(
        &self,
        email: &EtlEmailRequest<'_>,
    ) -> Result<reqwest::Request, reqwest::Error> {
        let mut request = self
            .client
            .post(self.etl_email_url())
            .header("apikey", self.api_key.expose_secret())
            .json(email)
            .build()?;

        if let Some(api_key) = request.headers_mut().get_mut("apikey") {
            api_key.set_sensitive(true);
        }

        Ok(request)
    }

    /// Sends an ETL email request to the API endpoint.
    async fn send_email(
        &self,
        email: &EtlEmailRequest<'_>,
    ) -> Result<EtlEmailResponse, Box<dyn Error>> {
        let request = self.build_email_request(email)?;
        let response = self.client.execute(request).await.map_err(reqwest::Error::without_url)?;

        if !response.status().is_success() {
            let status = response.status();

            return Err(std::io::Error::other(format!("API returned status {status}")).into());
        }

        let body = response.bytes().await.map_err(reqwest::Error::without_url)?;

        // Serde's type errors can quote values from malformed responses.
        serde_json::from_slice(&body).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid notification API response; expected a deduplicated boolean",
            )
            .into()
        })
    }
}

/// Builds the caller-owned deduplication key for a pipeline error.
fn pipeline_error_dedup_key(pipeline_id: &str, error_hash: &str) -> String {
    format!("{PIPELINE_ERROR_DEDUP_KEY_PREFIX}:{pipeline_id}:{error_hash}")
}

/// Computes a stable hash for an error.
///
/// This provides a consistent identifier across multiple occurrences of the
/// same error type, enabling grouping and deduplication in monitoring systems.
fn compute_error_hash<H: Hash>(error_hash: H) -> String {
    let mut hasher = DefaultHasher::new();
    error_hash.hash(&mut hasher);
    let hash_value = hasher.finish();

    format!("{hash_value:016x}")
}

#[cfg(test)]
mod tests {
    use axum::{Router, http::StatusCode, response::IntoResponse, routing::post};
    use etl::error::{ErrorKind, EtlError};
    use tokio::net::TcpListener;
    use tokio_util::task::AbortOnDropHandle;

    use super::*;

    fn error_notification_client(api_url: &str) -> ErrorNotificationClient {
        ErrorNotificationClient::new(
            api_url.to_owned(),
            "fake-api-key".to_owned(),
            "abcdefghijklmnopqrst".to_owned(),
            "42".to_owned(),
        )
        .unwrap()
    }

    /// Serves one notification endpoint without involving external services.
    async fn notification_server(
        status: StatusCode,
        body: &'static str,
    ) -> (String, AbortOnDropHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route(
            "/system/replication/abcdefghijklmnopqrst/emails",
            post(move || async move {
                let mut response = (status, body).into_response();
                response.headers_mut().insert(
                    axum::http::header::LOCATION,
                    "http://127.0.0.1:1/placeholder-redirect".parse().unwrap(),
                );

                response
            }),
        );
        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));

        (url, server)
    }

    /// Failed or malformed responses cannot echo secrets into logs, and
    /// redirects are not followed.
    #[tokio::test]
    async fn notification_errors_omit_response_bodies_and_reject_redirects() {
        for (status, body) in [
            (StatusCode::BAD_REQUEST, "placeholder-response-secret"),
            (StatusCode::TEMPORARY_REDIRECT, "placeholder-response-secret"),
            (StatusCode::OK, r#"{"deduplicated":"placeholder-response-secret"}"#),
        ] {
            let (url, server) = notification_server(status, body).await;
            let client = error_notification_client(&url);
            let email = client.pipeline_error_email_request("test-key", "test-error");

            let error = client.send_email(&email).await.unwrap_err();

            if !status.is_success() {
                assert!(error.to_string().contains(status.as_str()));
            }

            assert!(!format!("{error:?}").contains("placeholder-response-secret"));

            etl::task::abort_and_join(server).await.unwrap();
        }

        let (url, server) = notification_server(StatusCode::OK, r#"{"deduplicated":true}"#).await;
        let client = error_notification_client(&url);
        let email = client.pipeline_error_email_request("test-key", "test-error");

        let response = client.send_email(&email).await.unwrap();

        assert!(response.deduplicated);

        etl::task::abort_and_join(server).await.unwrap();
    }

    #[test]
    fn builds_pipeline_error_email_request() {
        let client = error_notification_client("https://api.supabase.com/");
        let dedup_key = pipeline_error_dedup_key("42", "abc123");
        let email = client
            .pipeline_error_email_request(&dedup_key, "Connection timeout to source database");
        let request = client.build_email_request(&email).expect("request should build");

        assert_eq!(request.method(), reqwest::Method::POST);
        assert!(request.headers()["apikey"].is_sensitive());
        assert_eq!(
            request.url().as_str(),
            "https://api.supabase.com/system/replication/abcdefghijklmnopqrst/emails"
        );
        assert_eq!(
            request.headers().get("apikey").expect("apikey header should be present"),
            "fake-api-key"
        );
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .expect("content-type header should be present"),
            "application/json"
        );

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                request
                    .body()
                    .and_then(reqwest::Body::as_bytes)
                    .expect("request body should be buffered"),
            )
            .expect("request body should contain valid JSON"),
            serde_json::json!({
                "template_alias": "etl-pipeline-notification",
                "dedup_key": "pipeline-error:42:abc123",
                "fields": {
                    "pipeline_id": "42",
                    "notification_title": "Replication pipeline error",
                    "notification_message": PIPELINE_ERROR_NOTIFICATION_MESSAGE,
                    "notification_details_label": "Error details",
                    "notification_details": "Connection timeout to source database",
                },
            })
        );
    }

    #[test]
    fn builds_pipeline_error_dedup_key() {
        assert_eq!(pipeline_error_dedup_key("42", "abc123"), "pipeline-error:42:abc123");
    }

    #[test]
    fn compute_error_hash_stability() {
        let err1 =
            EtlError::from((ErrorKind::SourceConnectionFailed, "Database connection failed"));
        let err2 =
            EtlError::from((ErrorKind::SourceConnectionFailed, "Database connection failed"));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        // Hashes should be identical for the same error kind and description.
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compute_error_hash_with_detail() {
        let err1 = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "Query execution failed",
            "Table 'users' not found".to_owned(),
        ));
        let err2 = EtlError::from((
            ErrorKind::SourceQueryFailed,
            "Query execution failed",
            "Table 'users' not found".to_owned(),
        ));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compute_error_hash_different_errors() {
        let err1 =
            EtlError::from((ErrorKind::SourceConnectionFailed, "Database connection failed"));
        let err2 = EtlError::from((ErrorKind::SourceQueryFailed, "Query execution failed"));

        let hash1 = compute_error_hash(&err1);
        let hash2 = compute_error_hash(&err2);

        // Different errors should produce different hashes.
        assert_ne!(hash1, hash2);
    }
}
