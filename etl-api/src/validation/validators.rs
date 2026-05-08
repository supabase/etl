//! Built-in validators for ETL pipeline and destination prerequisites.

use std::collections::HashMap;

use async_trait::async_trait;
use etl::store::both::memory::MemoryStore;
use etl_config::{SerializableSecretString, parse_ducklake_url};
use etl_destinations::{
    bigquery::BigQueryClient,
    clickhouse::ClickHouseClient,
    ducklake::{DuckLakeDestination, S3Config as DucklakeS3Config},
    iceberg::{IcebergClient, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY},
};
use secrecy::ExposeSecret;
use url::Url;

use super::{
    ValidationContext, ValidationError, ValidationFailure, Validator,
    source::{
        audit_source_role, current_user, current_user_has_replication_permission,
        publication_exists, publication_table_summary, publication_tables_with_generated_columns,
        publication_tables_without_primary_keys, replication_slot_counts, wal_level,
    },
};
use crate::configs::{
    destination::{FullApiDestinationConfig, FullApiIcebergConfig},
    pipeline::FullApiPipelineConfig,
};

const ETL_SCHEMA_NAME: &str = "etl";

/// Validates the connected source role profile for ETL.
#[derive(Debug)]
pub(super) struct SourceValidator;

#[async_trait]
impl Validator for SourceValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let Some(expected_username) = ctx.trusted_username.as_ref() else {
            return Ok(vec![]);
        };

        let source_client =
            ctx.source_client.as_ref().expect("source client required for source validation");

        let current_user = current_user(source_client).await?;

        if current_user != *expected_username {
            return Ok(vec![ValidationFailure::critical(
                "Invalid source username",
                format!("Connected as '{current_user}' but expected '{expected_username}'"),
            )]);
        }

        // This validation is best effort: it relies on catalog metadata and
        // privilege checks to confirm the trusted role profile without running
        // invasive probes against the customer database.
        let audit = audit_source_role(source_client, expected_username, ETL_SCHEMA_NAME).await?;

        let Some(audit) = audit else {
            return Ok(vec![ValidationFailure::critical(
                "Invalid source role attributes",
                "Role not found",
            )]);
        };

        let has_required_role_attributes = audit.rolcanlogin
            && audit.rolreplication
            && audit.rolbypassrls
            && !audit.rolcreaterole
            && !audit.rolcreatedb
            && audit.rolinherit
            && audit.rolvaliduntil_is_null;

        let mut failures = Vec::new();
        if !has_required_role_attributes {
            failures.push(ValidationFailure::critical(
                "Invalid source role attributes",
                "The source database does not grant the trusted username role all permissions ETL \
                 needs to work properly.",
            ));
        }

        let has_required_etl_schema_permissions = if audit.etl_schema_exists {
            audit.etl_schema_usage == Some(true)
                && audit.etl_schema_create == Some(true)
                && audit.controls_all_existing_etl_tables == Some(true)
        } else {
            audit.can_create_schema_if_missing == Some(true)
        };

        if !has_required_etl_schema_permissions {
            failures.push(ValidationFailure::critical(
                "Invalid source etl schema permissions",
                format!(
                    "The source database does not grant the trusted username role all permissions \
                     ETL needs to manage schema {ETL_SCHEMA_NAME} properly."
                ),
            ));
        }

        Ok(failures)
    }
}

/// Validates that the required publication exists in the source database.
#[derive(Debug)]
pub(super) struct PublicationExistsValidator {
    publication_name: String,
}

impl PublicationExistsValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationExistsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_client =
            ctx.source_client.as_ref().expect("source client required for publication validation");

        let exists = publication_exists(source_client, &self.publication_name).await?;

        if exists {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Publication Not Found",
                format!(
                    "Publication '{}' does not exist in the source database. Create it with: \
                     CREATE PUBLICATION {} FOR TABLE <table_name>, ...",
                    self.publication_name, self.publication_name
                ),
            )])
        }
    }
}

/// Validates that there are enough free replication slots for the pipeline.
#[derive(Debug)]
pub(super) struct ReplicationSlotsValidator {
    max_table_sync_workers: u16,
}

impl ReplicationSlotsValidator {
    pub(super) fn new(max_table_sync_workers: u16) -> Self {
        Self { max_table_sync_workers }
    }
}

#[async_trait]
impl Validator for ReplicationSlotsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_client = ctx
            .source_client
            .as_ref()
            .expect("source client required for replication slots validation");

        let (max_slots, used_slots) = replication_slot_counts(source_client).await?;

        let free_slots = max_slots - used_slots;
        // We need 1 slot for the apply worker plus at most `max_table_sync_workers`
        // other slots for table sync workers.
        let required_slots = self.max_table_sync_workers as i64 + 1;

        if required_slots <= free_slots {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Insufficient Replication Slots",
                format!(
                    "Not enough replication slots available.\nFound {free_slots} free slots, but \
                     {required_slots} are required at most during initial table copy \
                     ({used_slots}/{max_slots} currently in use).\nOnce all tables are copied, \
                     only 1 slot will be used.\n\nPlease verify:\n(1) max_replication_slots in \
                     postgresql.conf is sufficient\n(2) Unused replication slots can be \
                     removed\n(3) max_table_sync_workers can be reduced if needed",
                ),
            )])
        }
    }
}

/// Validates that the WAL level is set to 'logical' for replication.
#[derive(Debug)]
pub(super) struct WalLevelValidator;

#[async_trait]
impl Validator for WalLevelValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_client =
            ctx.source_client.as_ref().expect("source client required for WAL level validation");

        let wal_level = wal_level(source_client).await?;

        if wal_level == "logical" {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Invalid WAL Level",
                format!(
                    "WAL level is set to '{wal_level}', but must be 'logical' for replication. \
                     Update postgresql.conf with: wal_level = 'logical' and restart PostgreSQL"
                ),
            )])
        }
    }
}

/// Validates that the database user has replication permissions.
#[derive(Debug)]
pub(super) struct ReplicationPermissionsValidator;

#[async_trait]
impl Validator for ReplicationPermissionsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_client = ctx
            .source_client
            .as_ref()
            .expect("source client required for replication permissions validation");

        let has_permission = current_user_has_replication_permission(source_client).await?;

        if has_permission {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Missing Replication Permission",
                "The database user does not have replication privileges",
            )])
        }
    }
}

/// Validates that a publication contains at least one table.
#[derive(Debug)]
pub(super) struct PublicationHasTablesValidator {
    publication_name: String,
}

impl PublicationHasTablesValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationHasTablesValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_client = ctx
            .source_client
            .as_ref()
            .expect("source client required for publication tables validation");

        let result = publication_table_summary(source_client, &self.publication_name).await?;

        // If publication doesn't exist, skip this check (PublicationExistsValidator
        // handles it)
        let Some((puballtables, table_count)) = result else {
            return Ok(vec![]);
        };

        if puballtables || table_count > 0 {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Publication Empty",
                format!(
                    "Publication '{}' exists but contains no tables.\n\nAdd tables with: ALTER \
                     PUBLICATION {} ADD TABLE <table_name>",
                    self.publication_name, self.publication_name
                ),
            )])
        }
    }
}

/// Validates that all tables in a publication have primary keys.
#[derive(Debug)]
pub(super) struct PrimaryKeysValidator {
    publication_name: String,
}

impl PrimaryKeysValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PrimaryKeysValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_client =
            ctx.source_client.as_ref().expect("source client required for primary keys validation");

        let tables_without_pk =
            publication_tables_without_primary_keys(source_client, &self.publication_name).await?;

        if tables_without_pk.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::warning(
                "Tables Missing Primary Keys",
                format!(
                    "Tables without primary keys: {}\n\nPrimary keys are required for UPDATE and \
                     DELETE replication.",
                    tables_without_pk.join(", ")
                ),
            )])
        }
    }
}

/// Validates that tables in a publication don't have generated columns.
#[derive(Debug)]
pub(super) struct GeneratedColumnsValidator {
    publication_name: String,
}

impl GeneratedColumnsValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for GeneratedColumnsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_client = ctx
            .source_client
            .as_ref()
            .expect("source client required for generated columns validation");

        let tables_with_generated =
            publication_tables_with_generated_columns(source_client, &self.publication_name)
                .await?;

        if tables_with_generated.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::warning(
                "Tables With Generated Columns",
                format!(
                    "Tables with generated columns: {}\n\nGenerated columns cannot be replicated \
                     and will be excluded from the destination.",
                    tables_with_generated.join(", ")
                ),
            )])
        }
    }
}

/// Composite validator for pipeline prerequisites.
#[derive(Debug)]
pub(super) struct PipelineValidator {
    config: FullApiPipelineConfig,
}

impl PipelineValidator {
    pub(super) fn new(config: FullApiPipelineConfig) -> Self {
        Self { config }
    }

    fn sub_validators(&self) -> Vec<Box<dyn Validator>> {
        let max_table_sync_workers = self.config.max_table_sync_workers.unwrap_or(4);
        let publication_name = self.config.publication_name.clone();

        vec![
            Box::new(WalLevelValidator),
            Box::new(ReplicationPermissionsValidator),
            Box::new(PublicationExistsValidator::new(publication_name.clone())),
            Box::new(PublicationHasTablesValidator::new(publication_name.clone())),
            Box::new(PrimaryKeysValidator::new(publication_name.clone())),
            Box::new(GeneratedColumnsValidator::new(publication_name)),
            Box::new(ReplicationSlotsValidator::new(max_table_sync_workers)),
        ]
    }
}

#[async_trait]
impl Validator for PipelineValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let mut failures = Vec::new();

        for validator in self.sub_validators() {
            failures.extend(validator.validate(ctx).await?);
        }

        Ok(failures)
    }
}

/// Validates BigQuery destination connectivity and dataset accessibility.
#[derive(Debug)]
struct BigQueryValidator {
    project_id: String,
    dataset_id: String,
    service_account_key: String,
}

impl BigQueryValidator {
    fn new(project_id: String, dataset_id: String, service_account_key: String) -> Self {
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

/// Validates Clickhouse destination connectivity and dataset accessibility.
#[derive(Debug)]
struct ClickHouseValidator {
    url: Url,
    user: String,
    password: Option<SerializableSecretString>,
    database: String,
}

impl ClickHouseValidator {
    fn new(
        url: Url,
        user: String,
        password: Option<SerializableSecretString>,
        database: String,
    ) -> Self {
        Self { url, user, password, database }
    }
}

#[async_trait]
impl Validator for ClickHouseValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let client = ClickHouseClient::new(
            self.url.clone(),
            self.user.clone(),
            self.password.as_ref().map(|password| password.expose_secret().to_owned()),
            self.database.clone(),
        );
        match client.validate_connectivity().await {
            Ok(_) => Ok(Vec::new()),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "ClickHouse Connection Failed",
                "Unable to create clickhouse client.\n\nPlease verify:\n(1) The url is valid and \
                 accessible\n(2) The username is correct\n(3) You set the right password\n(4) You \
                 set the right database name
                    ",
            )]),
        }
    }
}

/// Validates Iceberg destination connectivity.
#[derive(Debug)]
struct IcebergValidator {
    config: FullApiIcebergConfig,
}

/// Validates DuckLake destination connectivity.
#[derive(Debug)]
struct DucklakeValidator {
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
    fn new(
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
        match (&self.s3_access_key_id, &self.s3_secret_access_key) {
            (Some(_), None) | (None, Some(_)) => {
                return Ok(vec![ValidationFailure::critical(
                    "Ducklake S3 Configuration Invalid",
                    "DuckLake S3 credentials must include both access key ID and secret access \
                     key.",
                )]);
            }
            _ => {}
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

        let s3_config = self.s3_access_key_id.clone().map(|access_key_id| DucklakeS3Config {
            access_key_id,
            secret_access_key: self
                .s3_secret_access_key
                .clone()
                .expect("ducklake s3 secret access key should be present"),
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

impl IcebergValidator {
    fn new(config: FullApiIcebergConfig) -> Self {
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
                "Unable to authenticate with Iceberg.\n\nPlease verify:\n(1) The catalog token is \
                 valid and has not expired\n(2) The S3 access key and secret key are correct\n(3) \
                 The catalog URI is properly formatted",
            )]);
        };

        match client.validate_connectivity().await {
            Ok(()) => Ok(vec![]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "Iceberg Connection Failed",
                "Unable to connect to Iceberg catalog.\n\nPlease verify:\n(1) Network \
                 connectivity to the catalog and S3\n(2) The warehouse name exists in the \
                 catalog\n(3) You have the required permissions to access the warehouse\n(4) The \
                 S3 endpoint is reachable",
            )]),
        }
    }
}

/// Composite validator for destination prerequisites.
#[derive(Debug)]
pub(super) struct DestinationValidator {
    config: FullApiDestinationConfig,
}

impl DestinationValidator {
    pub(super) fn new(config: FullApiDestinationConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Validator for DestinationValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        match &self.config {
            FullApiDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                ..
            } => {
                let validator = BigQueryValidator::new(
                    project_id.clone(),
                    dataset_id.clone(),
                    service_account_key.expose_secret().to_owned(),
                );
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::ClickHouse { url, user, password, database } => {
                let validator = ClickHouseValidator::new(
                    url.clone(),
                    user.clone(),
                    password.clone(),
                    database.clone(),
                );
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::Iceberg { config } => {
                let validator = IcebergValidator::new(config.clone());
                validator.validate(ctx).await
            }
            FullApiDestinationConfig::Ducklake {
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
            } => {
                let validator = DucklakeValidator::new(
                    catalog_url.clone(),
                    data_path.clone(),
                    pool_size.unwrap_or(
                        etl_config::shared::DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE,
                    ),
                    s3_access_key_id.as_ref().map(|value| value.expose_secret().to_owned()),
                    s3_secret_access_key.as_ref().map(|value| value.expose_secret().to_owned()),
                    s3_region.clone(),
                    s3_endpoint.clone(),
                    s3_url_style.clone(),
                    *s3_use_ssl,
                    metadata_schema.clone(),
                    duckdb_memory_cache_limit.clone(),
                    maintenance_target_file_size.clone(),
                    expire_snapshots_older_than.clone(),
                );
                validator.validate(ctx).await
            }
        }
    }
}
