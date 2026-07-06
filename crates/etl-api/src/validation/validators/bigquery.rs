use async_trait::async_trait;
use etl_config::shared::validate_gcs_bucket_name;
use etl_destinations::bigquery::BigQueryClient;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates BigQuery destination connectivity and dataset accessibility.
#[derive(Debug)]
pub(super) struct BigQueryValidator {
    project_id: String,
    dataset_id: String,
    gcs_staging_bucket: Option<String>,
    service_account_key: String,
}

impl BigQueryValidator {
    pub(super) fn new(
        project_id: String,
        dataset_id: String,
        gcs_staging_bucket: Option<String>,
        service_account_key: String,
    ) -> Self {
        Self { project_id, dataset_id, gcs_staging_bucket, service_account_key }
    }
}

#[async_trait]
impl Validator for BigQueryValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        if let Some(bucket) = &self.gcs_staging_bucket
            && validate_gcs_bucket_name(bucket).is_err()
        {
            return Ok(vec![ValidationFailure::critical(
                "BigQuery GCS Staging Bucket Invalid",
                "The BigQuery GCS staging bucket must be a bucket name only. Do not include \
                 `gs://` or an object path such as `/initial-copy`.",
            )]);
        }

        let Ok(client) =
            BigQueryClient::new_with_key(self.project_id.clone(), &self.service_account_key, 1)
                .await
        else {
            return Ok(vec![ValidationFailure::critical(
                "BigQuery Authentication Failed",
                format!(
                    "We couldn't authenticate with BigQuery using the service account \
                     key.\n\nCheck that the key is valid JSON, has not been revoked, and belongs \
                     to a service account with access to project `{}`.",
                    self.project_id
                ),
            )]);
        };

        match client.dataset_exists(&self.dataset_id).await {
            Ok(true) => Ok(vec![]),
            Ok(false) => Ok(vec![ValidationFailure::critical(
                "BigQuery Dataset Not Found",
                format!(
                    "BigQuery dataset `{}` was not found in project `{}`.\n\nCheck the dataset \
                     name and make sure the service account can access it.",
                    self.dataset_id, self.project_id
                ),
            )]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "BigQuery Connection Failed",
                "We couldn't reach BigQuery or confirm that the dataset exists.\n\nCheck network \
                 access to Google Cloud, that the BigQuery API is enabled, and that the service \
                 account has `BigQuery Data Editor` and `BigQuery Job User` permissions.",
            )]),
        }
    }
}
