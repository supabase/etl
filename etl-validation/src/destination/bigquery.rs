use crate::error::ValidationError;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use secrecy::{ExposeSecret, SecretString};

pub(super) async fn validate(
    project_id: &str,
    dataset_id: &str,
    service_account_key: &SecretString,
) -> Result<(), ValidationError> {
    let sa_key = parse_service_account_key(service_account_key.expose_secret()).map_err(|e| {
        ValidationError::AuthenticationFailed(format!("Invalid service account key format: {}", e))
    })?;

    let client = Client::from_service_account_key(sa_key, false)
        .await
        .map_err(|e| {
            ValidationError::AuthenticationFailed(format!(
                "Failed to create BigQuery client: {}",
                e
            ))
        })?;

    let _dataset = client
        .dataset()
        .get(project_id, dataset_id)
        .await
        .map_err(|e| {
            ValidationError::ConnectionFailed(format!(
                "Cannot access dataset {}.{}: {}",
                project_id, dataset_id, e
            ))
        })?;

    client
        .table()
        .list(project_id, dataset_id, Default::default())
        .await
        .map_err(|e| {
            ValidationError::PermissionDenied(format!(
                "Service account lacks permissions to access dataset: {}",
                e
            ))
        })?;

    Ok(())
}
