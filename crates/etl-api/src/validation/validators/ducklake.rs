use async_trait::async_trait;
use etl::store::MemoryStore;
use etl_config::{parse_ducklake_s3_data_path, parse_ducklake_url};
use etl_destinations::ducklake::{DuckLakeDestination, S3Config as DucklakeS3Config};
use sqlx::{PgPool, postgres::PgPoolOptions};
use url::Url;

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
                        "DuckLake S3 Configuration Invalid",
                        "DuckLake needs both an `S3 access key ID` and `S3 secret access key` to \
                         read and write the data path.",
                    )]);
                }
            };

        if s3_access_key_id.is_empty() || s3_secret_access_key.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "DuckLake S3 Configuration Invalid",
                "DuckLake needs both an `S3 access key ID` and `S3 secret access key` to read and \
                 write the data path.",
            )]);
        }

        let catalog_url = match parse_ducklake_url(&self.catalog_url) {
            Ok(url) => url,
            Err(error) => {
                return Ok(vec![ValidationFailure::critical(
                    "DuckLake Catalog URL Invalid",
                    error.to_string(),
                )]);
            }
        };

        let data_path = match parse_ducklake_s3_data_path(&self.data_path) {
            Ok(url) => url,
            Err(error) => {
                return Ok(vec![ValidationFailure::critical(
                    "DuckLake Data Path Invalid",
                    error.to_string(),
                )]);
            }
        };

        if let Some(failure) =
            metadata_schema_reuse_warning(&catalog_url, self.metadata_schema.as_deref()).await?
        {
            return Ok(vec![failure]);
        }

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
            self.maintenance_target_file_size.clone(),
            self.expire_snapshots_older_than.clone(),
            MemoryStore::new(),
        )
        .await
        {
            Ok(_) => Ok(vec![]),
            Err(_) => Ok(vec![ducklake_connection_failed()]),
        }
    }
}

/// Returns a warning when the configured PostgreSQL metadata schema exists.
async fn metadata_schema_reuse_warning(
    catalog_url: &Url,
    metadata_schema: Option<&str>,
) -> Result<Option<ValidationFailure>, ValidationError> {
    let Some(metadata_schema) = metadata_schema else {
        return Ok(None);
    };

    if !matches!(catalog_url.scheme(), "postgres" | "postgresql") {
        return Ok(None);
    }

    let Ok(pool) = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(catalog_url.as_str())
        .await
    else {
        return Ok(Some(ducklake_connection_failed()));
    };

    let Ok(schema_exists) = metadata_schema_exists(&pool, metadata_schema).await else {
        return Ok(Some(ducklake_connection_failed()));
    };

    if !schema_exists {
        return Ok(None);
    }

    let Ok(table_names) = existing_ducklake_metadata_tables(&pool, metadata_schema).await else {
        return Ok(Some(ducklake_connection_failed()));
    };

    let table_detail = if table_names.is_empty() {
        String::new()
    } else {
        format!(" Detected DuckLake metadata tables: {}.", table_names.join(", "))
    };

    Ok(Some(ValidationFailure::warning(
        "DuckLake Metadata Schema Already Exists",
        format!(
            "DuckLake metadata schema `{metadata_schema}` already exists in the catalog \
             database.{table_detail} Check whether this schema is already used for a DuckLake \
             catalog. If you intend to reuse that DuckLake, you can proceed. Otherwise, choose \
             another DuckLake metadata schema name in the destination config or drop the schema \
             from the database."
        ),
    )))
}

/// Returns whether a PostgreSQL schema exists in the catalog database.
async fn metadata_schema_exists(pool: &PgPool, metadata_schema: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        select exists (
            select 1
            from pg_catalog.pg_namespace
            where nspname = $1
        )
        "#,
    )
    .bind(metadata_schema)
    .fetch_one(pool)
    .await
}

/// Returns existing DuckLake-owned table names in a PostgreSQL schema.
async fn existing_ducklake_metadata_tables(
    pool: &PgPool,
    metadata_schema: &str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        select c.relname
        from pg_catalog.pg_namespace n
        join pg_catalog.pg_class c on c.relnamespace = n.oid
        where n.nspname = $1
          and c.relkind in ('r', 'p')
          and (
              c.relname like 'ducklake\_%' escape '\'
              or c.relname in ('__etl_applied_table_batches', '__etl_streaming_progress')
          )
        order by c.relname
        "#,
    )
    .bind(metadata_schema)
    .fetch_all(pool)
    .await
}

/// Returns the generic DuckLake connection validation failure.
fn ducklake_connection_failed() -> ValidationFailure {
    ValidationFailure::critical(
        "DuckLake Connection Failed",
        "We couldn't connect to DuckLake with this catalog and data path.\n\nCheck that the \
         catalog URL is reachable, catalog credentials are present in the URL, and the \
         `S3-compatible endpoint` and credentials are correct.",
    )
}

#[cfg(test)]
mod tests {
    use etl_config::Environment;

    use super::*;
    use crate::validation::ValidationContext;

    fn validator_with_data_path(data_path: &str) -> DucklakeValidator {
        DucklakeValidator::new(
            "postgres://user:pass@localhost:5432/ducklake_catalog".to_owned(),
            data_path.to_owned(),
            4,
            Some("access-key".to_owned()),
            Some("secret-key".to_owned()),
            Some("us-east-1".to_owned()),
            None,
            Some("path".to_owned()),
            Some(false),
            None,
            None,
            None,
        )
    }

    #[tokio::test]
    async fn rejects_file_data_path() {
        let validator = validator_with_data_path("file:///tmp/lake");
        let ctx = ValidationContext::builder(Environment::Dev).build();

        let failures = validator.validate(&ctx).await.unwrap();

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "DuckLake Data Path Invalid");
        assert_eq!(failures[0].reason, "DuckLake data path must use the s3:// scheme, got file://");
    }
}
