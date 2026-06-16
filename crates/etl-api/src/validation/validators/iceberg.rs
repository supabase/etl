use std::collections::HashMap;

use async_trait::async_trait;
use etl_destinations::iceberg::{
    IcebergClient, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY,
};
use secrecy::ExposeSecret;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};
use crate::configs::destination::FullApiIcebergConfig;

/// Validates Iceberg destination connectivity.
#[derive(Debug)]
pub(super) struct IcebergValidator {
    config: FullApiIcebergConfig,
}

impl IcebergValidator {
    pub(super) fn new(config: FullApiIcebergConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Validator for IcebergValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let client = match &self.config {
            FullApiIcebergConfig::Supabase {
                project_ref,
                warehouse_name,
                catalog_token,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                ..
            } => {
                IcebergClient::new_with_supabase_catalog(
                    project_ref,
                    ctx.environment.get_supabase_domain(),
                    catalog_token.expose_secret().to_owned(),
                    warehouse_name.clone(),
                    s3_access_key_id.expose_secret().to_owned(),
                    s3_secret_access_key.expose_secret().to_owned(),
                    s3_region.clone(),
                )
                .await
            }
            FullApiIcebergConfig::Rest {
                catalog_uri,
                warehouse_name,
                s3_access_key_id,
                s3_secret_access_key,
                s3_endpoint,
                ..
            } => {
                let mut props = HashMap::new();
                props.insert(
                    S3_ACCESS_KEY_ID.to_owned(),
                    s3_access_key_id.expose_secret().to_owned(),
                );
                props.insert(
                    S3_SECRET_ACCESS_KEY.to_owned(),
                    s3_secret_access_key.expose_secret().to_owned(),
                );
                props.insert(S3_ENDPOINT.to_owned(), s3_endpoint.clone());

                IcebergClient::new_with_rest_catalog(
                    catalog_uri.clone(),
                    warehouse_name.clone(),
                    props,
                )
                .await
            }
        };
        let Ok(client) = client else {
            return Ok(vec![ValidationFailure::critical(
                "Iceberg Authentication Failed",
                "We couldn't authenticate with the Iceberg catalog.\n\nCheck that the catalog \
                 token is valid, the catalog URI is correct, and the `S3 access key` and `S3 \
                 secret key` are correct.",
            )]);
        };

        match client.validate_connectivity().await {
            Ok(()) => Ok(vec![]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "Iceberg Connection Failed",
                "We couldn't connect to the Iceberg catalog or warehouse.\n\nCheck that the \
                 catalog and `S3 endpoint` are reachable, the warehouse exists, and the \
                 configured credentials can access it.",
            )]),
        }
    }
}
