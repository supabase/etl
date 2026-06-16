use async_trait::async_trait;
use etl_postgres::replication::catalog::ETL_SCHEMA_NAME;
use sqlx::FromRow;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates the connected source role profile for ETL.
#[derive(Debug)]
pub(crate) struct SourceValidator;

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

        let source_pool =
            ctx.source_pool.as_ref().expect("source pool required for source validation");

        let current_user: String =
            sqlx::query_scalar("select current_user").fetch_one(source_pool).await?;

        if current_user != *expected_username {
            return Ok(vec![ValidationFailure::critical(
                "Invalid Source Username",
                format!(
                    "The source connection authenticated as '{current_user}', but this API server \
                     expects '{expected_username}'.\n\nUpdate the source credentials to use the \
                     trusted ETL role."
                ),
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
                "Invalid Source Role Attributes",
                format!(
                    "The trusted ETL role '{expected_username}' was not found in the source \
                     database.\n\nCreate the role or update the source credentials to use the \
                     configured trusted role."
                ),
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
                "Invalid Source Role Attributes",
                "The trusted ETL role does not have the required source database \
                 attributes.\n\nIt must be able to log in, use replication, bypass RLS, inherit \
                 privileges, and avoid superuser-style role or database creation permissions.",
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
                "Invalid Source ETL Schema Permissions",
                format!(
                    "The trusted ETL role cannot manage the '{ETL_SCHEMA_NAME}' schema in the \
                     source database.\n\nGrant it permission to create the schema if it does not \
                     exist, or USAGE and CREATE on the schema plus ownership access to existing \
                     ETL tables."
                ),
            ));
        }

        Ok(failures)
    }
}
