use async_trait::async_trait;
use etl_destinations::bigquery::BigQueryClient;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates BigQuery destination connectivity and dataset accessibility.
#[derive(Debug)]
pub(super) struct BigQueryValidator {
    project_id: String,
    dataset_id: String,
    service_account_key: String,
}

impl BigQueryValidator {
    pub(super) fn new(project_id: String, dataset_id: String, service_account_key: String) -> Self {
        Self { project_id, dataset_id, service_account_key }
    }
}

#[async_trait]
impl Validator for BigQueryValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let Ok(client) =
            BigQueryClient::new_with_key(self.project_id.clone(), &self.service_account_key, 1)
                .await
        else {
            return Ok(vec![ValidationFailure::critical(
                "BigQuery Authentication Failed",
                "Unable to authenticate with BigQuery.\n\nPlease verify:\n(1) The service account \
                 key is valid JSON\n(2) The key has not expired or been revoked\n(3) The project \
                 ID is correct",
            )]);
        };

        match client.dataset_exists(&self.dataset_id).await {
            Ok(true) => Ok(vec![]),
            Ok(false) => Ok(vec![ValidationFailure::critical(
                "BigQuery Dataset Not Found",
                format!(
                    "Dataset '{}' does not exist in project '{}'.\n\nPlease verify:\n(1) The \
                     dataset name is correct\n(2) The dataset exists in the specified \
                     project\n(3) The service account has permission to access it",
                    self.dataset_id, self.project_id
                ),
            )]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "BigQuery Connection Failed",
                "Unable to connect to BigQuery.\n\nPlease verify:\n(1) Network connectivity to \
                 Google Cloud\n(2) The service account has the required permissions (BigQuery \
                 Data Editor, BigQuery Job User)\n(3) BigQuery API is enabled for your project",
            )]),
        }
    }
}
