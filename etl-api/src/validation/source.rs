//! Source database helper queries used by validators.

use etl_postgres::tokio::{PgSourceClient, PgSourceError};

/// Source role audit information used by validation.
#[derive(Debug, Clone)]
pub(super) struct SourceRoleAudit {
    /// Whether the role can log in.
    pub(super) rolcanlogin: bool,
    /// Whether the role has replication permission.
    pub(super) rolreplication: bool,
    /// Whether the role bypasses row-level security.
    pub(super) rolbypassrls: bool,
    /// Whether the role can create roles.
    pub(super) rolcreaterole: bool,
    /// Whether the role can create databases.
    pub(super) rolcreatedb: bool,
    /// Whether the role inherits privileges.
    pub(super) rolinherit: bool,
    /// Whether the role has no expiration time.
    pub(super) rolvaliduntil_is_null: bool,
    /// Whether the ETL schema already exists.
    pub(super) etl_schema_exists: bool,
    /// Whether the role has usage on the ETL schema.
    pub(super) etl_schema_usage: Option<bool>,
    /// Whether the role has create on the ETL schema.
    pub(super) etl_schema_create: Option<bool>,
    /// Whether the role controls existing ETL tables.
    pub(super) controls_all_existing_etl_tables: Option<bool>,
    /// Whether the role can create the ETL schema if it is missing.
    pub(super) can_create_schema_if_missing: Option<bool>,
}

/// Returns the current source database user.
pub(super) async fn current_user(source_client: &PgSourceClient) -> Result<String, PgSourceError> {
    Ok(source_client.query_one("select current_user", &[]).await?.get(0))
}

/// Audits a source role profile.
pub(super) async fn audit_source_role(
    source_client: &PgSourceClient,
    role_name: &str,
    etl_schema_name: &str,
) -> Result<Option<SourceRoleAudit>, PgSourceError> {
    let row = source_client
        .query_opt(
            r#"
            with target as (
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
              select oid
              from pg_namespace
              where nspname = $2
            ),
            etl_tables as (
              select c.relowner
              from pg_class c
              join etl_schema s on s.oid = c.relnamespace
              where c.relkind in ('r', 'p')
            ),
            etl_table_ownership as (
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
            &[&role_name, &etl_schema_name],
        )
        .await?;

    Ok(row.map(|row| SourceRoleAudit {
        rolcanlogin: row.get("rolcanlogin"),
        rolreplication: row.get("rolreplication"),
        rolbypassrls: row.get("rolbypassrls"),
        rolcreaterole: row.get("rolcreaterole"),
        rolcreatedb: row.get("rolcreatedb"),
        rolinherit: row.get("rolinherit"),
        rolvaliduntil_is_null: row.get("rolvaliduntil_is_null"),
        etl_schema_exists: row.get("etl_schema_exists"),
        etl_schema_usage: row.get("etl_schema_usage"),
        etl_schema_create: row.get("etl_schema_create"),
        controls_all_existing_etl_tables: row.get("controls_all_existing_etl_tables"),
        can_create_schema_if_missing: row.get("can_create_schema_if_missing"),
    }))
}

/// Returns whether a publication exists.
pub(super) async fn publication_exists(
    source_client: &PgSourceClient,
    publication_name: &str,
) -> Result<bool, PgSourceError> {
    Ok(source_client
        .query_one(
            "select exists(select 1 from pg_publication where pubname = $1)",
            &[&publication_name],
        )
        .await?
        .get(0))
}

/// Returns publication table summary information.
pub(super) async fn publication_table_summary(
    source_client: &PgSourceClient,
    publication_name: &str,
) -> Result<Option<(bool, i64)>, PgSourceError> {
    let row = source_client
        .query_opt(
            r#"
            select
                p.puballtables,
                (select count(*) from pg_publication_tables pt where pt.pubname = p.pubname)
            from pg_publication p
            where p.pubname = $1
            "#,
            &[&publication_name],
        )
        .await?;

    Ok(row.map(|row| (row.get(0), row.get(1))))
}

/// Returns replication slot capacity and current usage.
pub(super) async fn replication_slot_counts(
    source_client: &PgSourceClient,
) -> Result<(i64, i64), PgSourceError> {
    let max_slots: i32 = source_client
        .query_one("select setting::int from pg_settings where name = 'max_replication_slots'", &[])
        .await?
        .get(0);
    let used_slots: i64 =
        source_client.query_one("select count(*) from pg_replication_slots", &[]).await?.get(0);

    Ok((max_slots as i64, used_slots))
}

/// Returns the source WAL level.
pub(super) async fn wal_level(source_client: &PgSourceClient) -> Result<String, PgSourceError> {
    Ok(source_client.query_one("select current_setting('wal_level')", &[]).await?.get(0))
}

/// Returns whether the current user can use replication.
pub(super) async fn current_user_has_replication_permission(
    source_client: &PgSourceClient,
) -> Result<bool, PgSourceError> {
    Ok(source_client
        .query_one(
            "select rolsuper or rolreplication from pg_roles where rolname = current_user",
            &[],
        )
        .await?
        .get(0))
}

/// Lists publication tables without primary keys.
pub(super) async fn publication_tables_without_primary_keys(
    source_client: &PgSourceClient,
    publication_name: &str,
) -> Result<Vec<String>, PgSourceError> {
    let rows = source_client
        .query(
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
            &[&publication_name],
        )
        .await?;

    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

/// Lists publication tables with generated columns.
pub(super) async fn publication_tables_with_generated_columns(
    source_client: &PgSourceClient,
    publication_name: &str,
) -> Result<Vec<String>, PgSourceError> {
    let rows = source_client
        .query(
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
            &[&publication_name],
        )
        .await?;

    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}
