mod bigquery;
mod iceberg;

use crate::error::ValidationError;
use async_trait::async_trait;
use etl_config::shared::{DestinationConfig, IcebergConfig};
use secrecy::ExposeSecret;
use std::collections::HashMap;

#[async_trait]
pub trait DestinationValidator {
    async fn validate(&self) -> Result<(), ValidationError>;
}

#[async_trait]
impl DestinationValidator for DestinationConfig {
    async fn validate(&self) -> Result<(), ValidationError> {
        match self {
            DestinationConfig::Memory => Ok(()),

            DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                ..
            } => bigquery::validate(project_id, dataset_id, service_account_key).await,

            DestinationConfig::Iceberg { config } => config.validate().await,
        }
    }
}

#[async_trait]
impl DestinationValidator for IcebergConfig {
    async fn validate(&self) -> Result<(), ValidationError> {
        match self {
            IcebergConfig::Rest {
                catalog_uri,
                warehouse_name,
                s3_access_key_id,
                s3_secret_access_key,
                s3_endpoint,
                ..
            } => {
                let mut properties = HashMap::new();
                properties.insert(
                    "s3.access-key-id".to_string(),
                    s3_access_key_id.expose_secret().to_string(),
                );
                properties.insert(
                    "s3.secret-access-key".to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
                );
                properties.insert("s3.endpoint".to_string(), s3_endpoint.to_string());

                iceberg::validate_rest(catalog_uri, warehouse_name, &properties).await
            }

            IcebergConfig::Supabase {
                project_ref,
                warehouse_name,
                catalog_token,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                ..
            } => {
                iceberg::validate_supabase(
                    project_ref,
                    warehouse_name,
                    catalog_token.expose_secret(),
                    s3_access_key_id.expose_secret(),
                    s3_secret_access_key.expose_secret(),
                    s3_region,
                )
                .await
            }
        }
    }
}
