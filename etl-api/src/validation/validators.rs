//! Built-in validators for ETL pipeline and destination prerequisites.

use std::collections::HashMap;

use async_trait::async_trait;
use etl::store::both::memory::MemoryStore;
use etl_config::parse_ducklake_url;
use etl_destinations::bigquery::BigQueryClient;
use etl_destinations::ducklake::{DuckLakeDestination, S3Config as DucklakeS3Config};
use etl_destinations::iceberg::{
    IcebergClient, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_SECRET_ACCESS_KEY,
};
use secrecy::ExposeSecret;
use sqlx::FromRow;

use crate::configs::destination::{FullApiDestinationConfig, FullApiIcebergConfig};
use crate::configs::pipeline::FullApiPipelineConfig;

use super::{ValidationContext, ValidationError, ValidationFailure, Validator};

const ETL_SCHEMA_NAME: &str = "etl";

/// Validates the connected source role profile for ETL.
#[derive(Debug)]
pub struct SourceValidator;

#[derive(Debug, FromRow)]
struct SourceRoleAudit {
    rolcanlogin: bool,
    rolreplication: bool,
    rolbypassrls: bool,
    rolcreaterole: bool,
    rolcreatedb: bool,
    rolinherit: bool,
    rolvaliduntil_is_null: bool,
    etl_schema_exists: bool,
    etl_schema_usage: Option<bool>,
    etl_schema_create: Option<bool>,
    controls_all_existing_etl_tables: Option<bool>,
    can_create_schema_if_missing: Option<bool>,
}

#[async_trait]
impl Validator for SourceValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let Some(expected_username) = ctx.trusted_username.as_ref() else {
            return Ok(vec![]);
        };

        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for source validation");

        let current_user: String = sqlx::query_scalar("select current_user")
            .fetch_one(source_pool)
            .await?;

        if current_user != *expected_username {
            return Ok(vec![ValidationFailure::critical(
                "Invalid source username",
                format!("connected as '{current_user}' but expected '{expected_username}'"),
            )]);
        }

        // This validation is best effort: it relies on catalog metadata and
        // privilege checks to confirm the trusted role profile without running
        // invasive probes against the customer database.
        let audit = sqlx::query_as::<_, SourceRoleAudit>(
            r#"
            with target as (
              -- Load the direct role attributes for the trusted ETL user.
              select
                oid,
                rolcanlogin,
                rolreplication,
                rolbypassrls,
                rolcreaterole,
                rolcreatedb,
                rolinherit,
                rolvaliduntil is null as rolvaliduntil_is_null
              from pg_roles
              where rolname = $1
            ),
            etl_schema as (
              -- Check whether the etl schema already exists.
              select oid
              from pg_namespace
              where nspname = $2
            ),
            etl_tables as (
              -- List the existing tables in the etl schema, if present.
              select
                c.relowner
              from pg_class c
              join etl_schema s on s.oid = c.relnamespace
              where c.relkind in ('r', 'p')
            ),
            etl_table_ownership as (
              -- Determine whether the trusted role controls every existing ETL table.
              -- pg_has_role(..., 'USAGE') means the owning role's privileges are
              -- immediately available without requiring SET ROLE, which matches
              -- how ETL connects and operates.
              select
                coalesce(bool_and(pg_has_role($1, relowner, 'USAGE')), true)
                  as controls_all_existing_etl_tables
              from etl_tables
            )
            select
              t.rolcanlogin,
              t.rolreplication,
              t.rolbypassrls,
              t.rolcreaterole,
              t.rolcreatedb,
              t.rolinherit,
              t.rolvaliduntil_is_null,
              exists(select 1 from etl_schema) as etl_schema_exists,
              case
                when exists(select 1 from etl_schema)
                then has_schema_privilege($1, $2, 'USAGE')
                else null
              end as etl_schema_usage,
              case
                when exists(select 1 from etl_schema)
                then has_schema_privilege($1, $2, 'CREATE')
                else null
              end as etl_schema_create,
              case
                when exists(select 1 from etl_schema)
                then (select controls_all_existing_etl_tables from etl_table_ownership)
                else null
              end as controls_all_existing_etl_tables,
              case
                when not exists(select 1 from etl_schema)
                then has_database_privilege($1, current_database(), 'CREATE')
                else null
              end as can_create_schema_if_missing
            from target t
            "#,
        )
        .bind(expected_username)
        .bind(ETL_SCHEMA_NAME)
        .fetch_optional(source_pool)
        .await?;

        let Some(audit) = audit else {
            return Ok(vec![ValidationFailure::critical(
                "Invalid source role attributes",
                "role not found",
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
                "The source database does not grant the trusted username role all permissions ETL needs to work properly.",
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
                    "The source database does not grant the trusted username role all permissions ETL needs to manage schema {ETL_SCHEMA_NAME} properly."
                ),
            ));
        }

        Ok(failures)
    }
}

/// Validates that the required publication exists in the source database.
#[derive(Debug)]
pub struct PublicationExistsValidator {
    publication_name: String,
}

impl PublicationExistsValidator {
    pub fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationExistsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for publication validation");

        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from pg_publication where pubname = $1)")
                .bind(&self.publication_name)
                .fetch_one(source_pool)
                .await?;

        if exists {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Publication Not Found",
                format!(
                    "Publication '{}' does not exist in the source database. \
                    Create it with: CREATE PUBLICATION {} FOR TABLE <table_name>, ...",
                    self.publication_name, self.publication_name
                ),
            )])
        }
    }
}

/// Validates that there are enough free replication slots for the pipeline.
#[derive(Debug)]
pub struct ReplicationSlotsValidator {
    max_table_sync_workers: u16,
}

impl ReplicationSlotsValidator {
    pub fn new(max_table_sync_workers: u16) -> Self {
        Self {
            max_table_sync_workers,
        }
    }
}

#[async_trait]
impl Validator for ReplicationSlotsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for replication slots validation");

        let max_slots: i32 = sqlx::query_scalar(
            "select setting::int from pg_settings where name = 'max_replication_slots'",
        )
        .fetch_one(source_pool)
        .await?;

        let used_slots: i64 = sqlx::query_scalar("select count(*) from pg_replication_slots")
            .fetch_one(source_pool)
            .await?;

        let free_slots = max_slots as i64 - used_slots;
        // We need 1 slot for the apply worker plus at most `max_table_sync_workers` other slots
        // for table sync workers.
        let required_slots = self.max_table_sync_workers as i64 + 1;

        if required_slots <= free_slots {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Insufficient Replication Slots",
                format!(
                    "Not enough replication slots available.\n\
                    Found {free_slots} free slots, but {required_slots} are required at most during initial table copy ({used_slots}/{max_slots} currently in use).\n\
                    Once all tables are copied, only 1 slot will be used.\n\n\
                    Please verify:\n\
                    (1) max_replication_slots in postgresql.conf is sufficient\n\
                    (2) Unused replication slots can be removed\n\
                    (3) max_table_sync_workers can be reduced if needed",
                ),
            )])
        }
    }
}

/// Validates that the WAL level is set to 'logical' for replication.
#[derive(Debug)]
pub struct WalLevelValidator;

#[async_trait]
impl Validator for WalLevelValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for WAL level validation");

        let wal_level: String = sqlx::query_scalar("select current_setting('wal_level')")
            .fetch_one(source_pool)
            .await?;

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
pub struct ReplicationPermissionsValidator;

#[async_trait]
impl Validator for ReplicationPermissionsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for replication permissions validation");

        // Check if user is superuser OR has replication privilege
        let has_permission: bool = sqlx::query_scalar(
            "select rolsuper or rolreplication from pg_roles where rolname = current_user",
        )
        .fetch_one(source_pool)
        .await?;

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
pub struct PublicationHasTablesValidator {
    publication_name: String,
}

impl PublicationHasTablesValidator {
    pub fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationHasTablesValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for publication tables validation");

        // Check if publication publishes all tables or has specific tables
        let result: Option<(bool, i64)> = sqlx::query_as(
            r#"
            select
                p.puballtables,
                (select count(*) from pg_publication_tables pt where pt.pubname = p.pubname)
            from pg_publication p
            where p.pubname = $1
            "#,
        )
        .bind(&self.publication_name)
        .fetch_optional(source_pool)
        .await?;

        // If publication doesn't exist, skip this check (PublicationExistsValidator handles it)
        let Some((puballtables, table_count)) = result else {
            return Ok(vec![]);
        };

        if puballtables || table_count > 0 {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Publication Empty",
                format!(
                    "Publication '{}' exists but contains no tables.\n\n\
                    Add tables with: ALTER PUBLICATION {} ADD TABLE <table_name>",
                    self.publication_name, self.publication_name
                ),
            )])
        }
    }
}

/// Validates that all tables in a publication have primary keys.
#[derive(Debug)]
pub struct PrimaryKeysValidator {
    publication_name: String,
}

impl PrimaryKeysValidator {
    pub fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PrimaryKeysValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for primary keys validation");

        // Find tables without primary keys using pg_publication_rel for direct OID access
        let tables_without_pk: Vec<String> = sqlx::query_scalar(
            r#"
            select n.nspname || '.' || c.relname
            from pg_publication_rel pr
            join pg_publication p on p.oid = pr.prpubid
            join pg_class c on c.oid = pr.prrelid
            join pg_namespace n on n.oid = c.relnamespace
            where p.pubname = $1
              and not exists (
                select 1
                from pg_constraint con
                where con.conrelid = pr.prrelid
                  and con.contype = 'p'
              )
            order by n.nspname, c.relname
            limit 100
            "#,
        )
        .bind(&self.publication_name)
        .fetch_all(source_pool)
        .await?;

        if tables_without_pk.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::warning(
                "Tables Missing Primary Keys",
                format!(
                    "Tables without primary keys: {}\n\n\
                    Primary keys are required for UPDATE and DELETE replication.",
                    tables_without_pk.join(", ")
                ),
            )])
        }
    }
}

/// Validates that tables in a publication don't have generated columns.
#[derive(Debug)]
pub struct GeneratedColumnsValidator {
    publication_name: String,
}

impl GeneratedColumnsValidator {
    pub fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for GeneratedColumnsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for generated columns validation");

        // Find tables with generated columns using pg_publication_rel for direct OID access
        let tables_with_generated: Vec<String> = sqlx::query_scalar(
            r#"
            select distinct n.nspname || '.' || c.relname
            from pg_publication_rel pr
            join pg_publication p on p.oid = pr.prpubid
            join pg_class c on c.oid = pr.prrelid
            join pg_namespace n on n.oid = c.relnamespace
            where p.pubname = $1
              and exists (
                select 1
                from pg_attribute a
                where a.attrelid = pr.prrelid
                  and a.attnum > 0
                  and not a.attisdropped
                  and a.attgenerated != ''
              )
            order by 1
            limit 100
            "#,
        )
        .bind(&self.publication_name)
        .fetch_all(source_pool)
        .await?;

        if tables_with_generated.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::warning(
                "Tables With Generated Columns",
                format!(
                    "Tables with generated columns: {}\n\n\
                    Generated columns cannot be replicated and will be excluded from the destination.",
                    tables_with_generated.join(", ")
                ),
            )])
        }
    }
}

/// Composite validator for pipeline prerequisites.
#[derive(Debug)]
pub struct PipelineValidator {
    config: FullApiPipelineConfig,
}

impl PipelineValidator {
    pub fn new(config: FullApiPipelineConfig) -> Self {
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
        Self {
            project_id,
            dataset_id,
            service_account_key,
        }
    }
}

#[async_trait]
impl Validator for BigQueryValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let client = match BigQueryClient::new_with_key(
            self.project_id.clone(),
            &self.service_account_key,
            1,
        )
        .await
        {
            Ok(client) => client,
            Err(_) => {
                return Ok(vec![ValidationFailure::critical(
                    "BigQuery Authentication Failed",
                    "Unable to authenticate with BigQuery.\n\n\
                    Please verify:\n\
                    (1) The service account key is valid JSON\n\
                    (2) The key has not expired or been revoked\n\
                    (3) The project ID is correct",
                )]);
            }
        };

        match client.dataset_exists(&self.dataset_id).await {
            Ok(true) => Ok(vec![]),
            Ok(false) => Ok(vec![ValidationFailure::critical(
                "BigQuery Dataset Not Found",
                format!(
                    "Dataset '{}' does not exist in project '{}'.\n\n\
                    Please verify:\n\
                    (1) The dataset name is correct\n\
                    (2) The dataset exists in the specified project\n\
                    (3) The service account has permission to access it",
                    self.dataset_id, self.project_id
                ),
            )]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "BigQuery Connection Failed",
                "Unable to connect to BigQuery.\n\n\
                Please verify:\n\
                (1) Network connectivity to Google Cloud\n\
                (2) The service account has the required permissions (BigQuery Data Editor, BigQuery Job User)\n\
                (3) BigQuery API is enabled for your project",
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
                    "DuckLake S3 credentials must include both access key ID and secret access key.",
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

        let s3_config = self
            .s3_access_key_id
            .clone()
            .map(|access_key_id| DucklakeS3Config {
                access_key_id,
                secret_access_key: self
                    .s3_secret_access_key
                    .clone()
                    .expect("ducklake s3 secret access key should be present"),
                region: self
                    .s3_region
                    .clone()
                    .unwrap_or_else(|| "us-east-1".to_string()),
                endpoint: self.s3_endpoint.clone(),
                url_style: self
                    .s3_url_style
                    .clone()
                    .unwrap_or_else(|| "path".to_string()),
                use_ssl: self.s3_use_ssl.unwrap_or(false),
            });

        match DuckLakeDestination::new(
            catalog_url,
            data_path,
            self.pool_size,
            s3_config,
            self.metadata_schema.clone(),
            MemoryStore::new(),
        )
        .await
        {
            Ok(_) => Ok(vec![]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "Ducklake Connection Failed",
                "Unable to connect to DuckLake.\n\n\
                Please verify:\n\
                (1) The catalog URL and data path are valid and reachable\n\
                (2) DuckLake catalog credentials are embedded correctly in the catalog URL\n\
                (3) The S3-compatible credentials and endpoint are correct when using object storage",
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
                    catalog_token.expose_secret().to_string(),
                    warehouse_name.clone(),
                    s3_access_key_id.expose_secret().to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
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
                    S3_ACCESS_KEY_ID.to_string(),
                    s3_access_key_id.expose_secret().to_string(),
                );
                props.insert(
                    S3_SECRET_ACCESS_KEY.to_string(),
                    s3_secret_access_key.expose_secret().to_string(),
                );
                props.insert(S3_ENDPOINT.to_string(), s3_endpoint.clone());

                IcebergClient::new_with_rest_catalog(
                    catalog_uri.clone(),
                    warehouse_name.clone(),
                    props,
                )
                .await
            }
        };

        let client = match client {
            Ok(client) => client,
            Err(_) => {
                return Ok(vec![ValidationFailure::critical(
                    "Iceberg Authentication Failed",
                    "Unable to authenticate with Iceberg.\n\n\
                    Please verify:\n\
                    (1) The catalog token is valid and has not expired\n\
                    (2) The S3 access key and secret key are correct\n\
                    (3) The catalog URI is properly formatted",
                )]);
            }
        };

        match client.validate_connectivity().await {
            Ok(()) => Ok(vec![]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "Iceberg Connection Failed",
                "Unable to connect to Iceberg catalog.\n\n\
                Please verify:\n\
                (1) Network connectivity to the catalog and S3\n\
                (2) The warehouse name exists in the catalog\n\
                (3) You have the required permissions to access the warehouse\n\
                (4) The S3 endpoint is reachable",
            )]),
        }
    }
}

/// Composite validator for destination prerequisites.
#[derive(Debug)]
pub struct DestinationValidator {
    config: FullApiDestinationConfig,
}

impl DestinationValidator {
    pub fn new(config: FullApiDestinationConfig) -> Self {
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
                    service_account_key.expose_secret().to_string(),
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
            } => {
                let validator = DucklakeValidator::new(
                    catalog_url.clone(),
                    data_path.clone(),
                    pool_size.unwrap_or(
                        etl_config::shared::DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE,
                    ),
                    s3_access_key_id
                        .as_ref()
                        .map(|value| value.expose_secret().to_string()),
                    s3_secret_access_key
                        .as_ref()
                        .map(|value| value.expose_secret().to_string()),
                    s3_region.clone(),
                    s3_endpoint.clone(),
                    s3_url_style.clone(),
                    *s3_use_ssl,
                    metadata_schema.clone(),
                );
                validator.validate(ctx).await
            }
        }
    }
}
