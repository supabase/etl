use async_trait::async_trait;
use etl_postgres::replication::catalog::ETL_SCHEMA_NAME;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

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
        let source_pool =
            ctx.source_pool.as_ref().expect("source pool required for publication validation");

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
        let source_pool =
            ctx.source_pool.as_ref().expect("source pool required for WAL level validation");

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
pub(super) struct ReplicationPermissionsValidator;

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

/// Validates that a publication does not include ETL-owned tables.
#[derive(Debug)]
pub(super) struct PublicationExcludesEtlTablesValidator {
    publication_name: String,
}

impl PublicationExcludesEtlTablesValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationExcludesEtlTablesValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool =
            ctx.source_pool.as_ref().expect("source pool required for publication validation");

        let Some(puballtables) = sqlx::query_scalar::<_, bool>(
            "select puballtables from pg_publication where pubname = $1",
        )
        .bind(&self.publication_name)
        .fetch_optional(source_pool)
        .await?
        else {
            // If publication doesn't exist, skip this check. The existence
            // validator reports the actionable failure.
            return Ok(vec![]);
        };

        if puballtables {
            return Ok(vec![ValidationFailure::critical(
                "Publication Includes ETL Tables",
                format!(
                    "Publication '{}' is defined FOR ALL TABLES, which also publishes ETL's \
                     internal schema tables when they exist. Use an explicit table list or FOR \
                     TABLES IN SCHEMA for customer-owned schemas, excluding the \
                     '{ETL_SCHEMA_NAME}' schema.",
                    self.publication_name
                ),
            )]);
        }

        let published_etl_tables: Vec<String> = sqlx::query_scalar(
            r#"
            select pt.schemaname || '.' || pt.tablename
            from pg_publication_tables pt
            where pt.pubname = $1
              and pt.schemaname = $2
            order by pt.tablename
            limit 100
            "#,
        )
        .bind(&self.publication_name)
        .bind(ETL_SCHEMA_NAME)
        .fetch_all(source_pool)
        .await?;

        if !published_etl_tables.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Publication Includes ETL Tables",
                format!(
                    "Publication '{}' includes ETL internal tables: {}.\n\nRemove the \
                     '{ETL_SCHEMA_NAME}' schema from the publication before starting this \
                     pipeline. ETL state tables are implementation details and must not be \
                     replicated.",
                    self.publication_name,
                    published_etl_tables.join(", ")
                ),
            )]);
        }

        let server_version_num: i32 =
            sqlx::query_scalar("select current_setting('server_version_num')::int")
                .fetch_one(source_pool)
                .await?;

        if server_version_num >= 15_00_00 {
            let publishes_etl_schema: bool = sqlx::query_scalar(
                r#"
                select exists (
                    select 1
                    from pg_publication_namespace pn
                    join pg_publication p on p.oid = pn.pnpubid
                    join pg_namespace n on n.oid = pn.pnnspid
                    where p.pubname = $1
                      and n.nspname = $2
                )
                "#,
            )
            .bind(&self.publication_name)
            .bind(ETL_SCHEMA_NAME)
            .fetch_one(source_pool)
            .await?;

            if publishes_etl_schema {
                return Ok(vec![ValidationFailure::critical(
                    "Publication Includes ETL Tables",
                    format!(
                        "Publication '{}' includes the '{ETL_SCHEMA_NAME}' schema. Remove that \
                         schema from the publication and publish only customer-owned schemas or \
                         explicit customer tables.",
                        self.publication_name
                    ),
                )]);
            }
        }

        Ok(vec![])
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
        let source_pool =
            ctx.source_pool.as_ref().expect("source pool required for primary keys validation");

        // Find tables without primary keys using pg_publication_rel for direct OID
        // access
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
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for generated columns validation");

        // Find tables with generated columns using pg_publication_rel for direct OID
        // access
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
                    "Tables with generated columns: {}\n\nGenerated columns cannot be replicated \
                     and will be excluded from the destination.",
                    tables_with_generated.join(", ")
                ),
            )])
        }
    }
}
