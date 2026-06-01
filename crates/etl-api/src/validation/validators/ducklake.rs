use async_trait::async_trait;
use etl::store::both::memory::MemoryStore;
use etl_config::parse_ducklake_url;
use etl_destinations::ducklake::{DuckLakeDestination, S3Config as DucklakeS3Config};

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates DuckLake destination connectivity.
#[derive(Debug)]
pub(super) struct DucklakeValidator {
    catalog_url: String,
    data_path: String,
    pool_size: u32,
    s3_access_key_id: Option<String>,
    s3_secret_access_key: Option<String>,
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_url_style: Option<String>,
    s3_use_ssl: Option<bool>,
    metadata_schema: Option<String>,
    duckdb_memory_cache_limit: Option<String>,
    maintenance_target_file_size: Option<String>,
    expire_snapshots_older_than: Option<String>,
}

impl DucklakeValidator {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        catalog_url: String,
        data_path: String,
        pool_size: u32,
        s3_access_key_id: Option<String>,
        s3_secret_access_key: Option<String>,
        s3_region: Option<String>,
        s3_endpoint: Option<String>,
        s3_url_style: Option<String>,
        s3_use_ssl: Option<bool>,
        metadata_schema: Option<String>,
        duckdb_memory_cache_limit: Option<String>,
        maintenance_target_file_size: Option<String>,
        expire_snapshots_older_than: Option<String>,
    ) -> Self {
        Self {
            catalog_url,
            data_path,
            pool_size,
            s3_access_key_id,
            s3_secret_access_key,
            s3_region,
            s3_endpoint,
            s3_url_style,
            s3_use_ssl,
            metadata_schema,
            duckdb_memory_cache_limit,
            maintenance_target_file_size,
            expire_snapshots_older_than,
        }
    }
}

#[async_trait]
impl Validator for DucklakeValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let (s3_access_key_id, s3_secret_access_key) =
            match (&self.s3_access_key_id, &self.s3_secret_access_key) {
                (Some(s3_access_key_id), Some(s3_secret_access_key)) => {
                    (s3_access_key_id.clone(), s3_secret_access_key.clone())
                }
                _ => {
                    return Ok(vec![ValidationFailure::critical(
                        "Ducklake S3 Configuration Invalid",
                        "DuckLake S3 credentials are required.",
                    )]);
                }
            };

        if s3_access_key_id.is_empty() || s3_secret_access_key.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Ducklake S3 Configuration Invalid",
                "DuckLake S3 credentials are required.",
            )]);
        }

        let catalog_url = match parse_ducklake_url(&self.catalog_url) {
            Ok(url) => url,
            Err(error) => {
                return Ok(vec![ValidationFailure::critical(
                    "Ducklake Catalog Url Invalid",
                    error.to_string(),
                )]);
            }
        };

        let data_path = match parse_ducklake_url(&self.data_path) {
            Ok(url) => url,
            Err(error) => {
                return Ok(vec![ValidationFailure::critical(
                    "Ducklake Data Path Invalid",
                    error.to_string(),
                )]);
            }
        };

        let s3_config = Some(DucklakeS3Config {
            access_key_id: s3_access_key_id,
            secret_access_key: s3_secret_access_key,
            region: self.s3_region.clone().unwrap_or_else(|| "us-east-1".to_owned()),
            endpoint: self.s3_endpoint.clone(),
            url_style: self.s3_url_style.clone().unwrap_or_else(|| "path".to_owned()),
            use_ssl: self.s3_use_ssl.unwrap_or(false),
        });

        match DuckLakeDestination::new(
            catalog_url,
            data_path,
            self.pool_size,
            s3_config,
            self.metadata_schema.clone(),
            self.duckdb_memory_cache_limit.clone(),
            self.maintenance_target_file_size.clone(),
            self.expire_snapshots_older_than.clone(),
            MemoryStore::new(),
        )
        .await
        {
            Ok(_) => Ok(vec![]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "Ducklake Connection Failed",
                "Unable to connect to DuckLake.\n\nPlease verify:\n(1) The catalog URL and data \
                 path are valid and reachable\n(2) DuckLake catalog credentials are embedded \
                 correctly in the catalog URL\n(3) The S3-compatible credentials and endpoint are \
                 correct when using object storage",
            )]),
        }
    }
}
