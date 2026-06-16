//! Source primary-key validation for destination compatibility.

use async_trait::async_trait;
use etl_postgres::version::POSTGRES_15;

use crate::validation::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates source primary keys for destinations that require them.
#[derive(Debug)]
pub(super) struct PrimaryKeyValidator {
    /// Name of the publication whose tables should be checked.
    publication_name: String,
    /// User-facing destination name included in validation messages.
    destination_name: &'static str,
    /// Explanation of why the destination needs source primary keys.
    reason: &'static str,
    /// Whether every publication table must have a primary key.
    require_primary_key: bool,
}

impl PrimaryKeyValidator {
    /// Creates a destination-specific primary-key validator.
    pub(super) fn new(
        publication_name: String,
        destination_name: &'static str,
        reason: &'static str,
        require_primary_key: bool,
    ) -> Self {
        Self { publication_name, destination_name, reason, require_primary_key }
    }
}

#[async_trait]
impl Validator for PrimaryKeyValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let Some(source_pool) = ctx.source_pool.as_ref() else {
            return Ok(vec![]);
        };

        let publication_exists: bool = sqlx::query_scalar(
            r#"
            select exists(
                select 1
                from pg_publication
                where pubname = $1
            )
            "#,
        )
        .bind(&self.publication_name)
        .fetch_one(source_pool)
        .await?;

        if !publication_exists {
            // If the publication doesn't exist, skip this check. Pipeline
            // validation reports the actionable publication failure.
            return Ok(vec![]);
        }

        let tables_without_pk: Vec<String> = sqlx::query_scalar(
            r#"
            select n.nspname || '.' || c.relname
            from pg_publication p
            cross join lateral pg_get_publication_tables(p.pubname) gpt
            join pg_class c on c.oid = gpt.relid
            join pg_namespace n on n.oid = c.relnamespace
            left join lateral (
                with direct_parent as (
                    select i.inhparent as parent_oid
                    from pg_inherits i
                    where i.inhrelid = c.oid
                    order by i.inhseqno
                    limit 1
                ),
                primary_key_cols as (
                    select x.attnum::int4 as attnum, x.n::int4 as position
                    from pg_constraint con
                    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
                    where con.conrelid = c.oid
                      and con.contype = 'p'
                ),
                parent_primary_key_cols as (
                    select x.attnum::int4 as attnum, x.n::int4 as position
                    from direct_parent dp
                    join pg_constraint con
                      on con.conrelid = dp.parent_oid
                     and con.contype = 'p'
                    cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
                ),
                effective_primary_key_cols as (
                    select pkc.attnum, pkc.position
                    from primary_key_cols pkc
                    union all
                    select ppkc.attnum, ppkc.position
                    from parent_primary_key_cols ppkc
                    where not exists (
                        select 1
                        from primary_key_cols pkc
                    )
                )
                select array_agg(epkc.attnum order by epkc.position) as primary_key_attnums
                from effective_primary_key_cols epkc
            ) pk on true
            where p.pubname = $1
              and cardinality(coalesce(pk.primary_key_attnums, array[]::int4[])) = 0
            order by n.nspname, c.relname
            limit 100
            "#,
        )
        .bind(&self.publication_name)
        .fetch_all(source_pool)
        .await?;

        let server_version_num: i32 =
            sqlx::query_scalar("select current_setting('server_version_num')::int")
                .fetch_one(source_pool)
                .await?;
        let tables_with_omitted_pk_columns = if server_version_num >= POSTGRES_15 {
            sqlx::query_as::<_, (String, String)>(
                r#"
                with publication_tables as (
                    select
                        n.nspname || '.' || c.relname as table_name,
                        c.oid as table_oid,
                        coalesce(pt.attnames, array[]::name[]) as replicated_column_names
                    from pg_publication p
                    cross join lateral pg_get_publication_tables(p.pubname) gpt
                    join pg_class c on c.oid = gpt.relid
                    join pg_namespace n on n.oid = c.relnamespace
                    left join pg_publication_tables pt
                      on pt.pubname = p.pubname
                     and pt.schemaname = n.nspname
                     and pt.tablename = c.relname
                    where p.pubname = $1
                ),
                primary_key_cols as (
                    select
                        pt.table_name,
                        pt.table_oid,
                        pt.replicated_column_names,
                        epkc.attnum,
                        epkc.position
                    from publication_tables pt
                    cross join lateral (
                        with direct_parent as (
                            select i.inhparent as parent_oid
                            from pg_inherits i
                            where i.inhrelid = pt.table_oid
                            order by i.inhseqno
                            limit 1
                        ),
                        table_primary_key_cols as (
                            select x.attnum::int4 as attnum, x.n::int4 as position
                            from pg_constraint con
                            cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
                            where con.conrelid = pt.table_oid
                              and con.contype = 'p'
                        ),
                        parent_primary_key_cols as (
                            select x.attnum::int4 as attnum, x.n::int4 as position
                            from direct_parent dp
                            join pg_constraint con
                              on con.conrelid = dp.parent_oid
                             and con.contype = 'p'
                            cross join lateral unnest(con.conkey) with ordinality as x(attnum, n)
                        )
                        select tpkc.attnum, tpkc.position
                        from table_primary_key_cols tpkc
                        union all
                        select ppkc.attnum, ppkc.position
                        from parent_primary_key_cols ppkc
                        where not exists (
                            select 1
                            from table_primary_key_cols tpkc
                        )
                    ) epkc
                )
                select
                    pk.table_name,
                    string_agg(a.attname::text, ', ' order by pk.position) as omitted_columns
                from primary_key_cols pk
                join pg_attribute a
                  on a.attrelid = pk.table_oid
                 and a.attnum = pk.attnum
                where cardinality(pk.replicated_column_names) > 0
                  and a.attname <> all(pk.replicated_column_names)
                group by pk.table_name
                order by pk.table_name
                limit 100
                "#,
            )
            .bind(&self.publication_name)
            .fetch_all(source_pool)
            .await?
        } else {
            Vec::new()
        };

        let mut failures = Vec::new();
        if self.require_primary_key && !tables_without_pk.is_empty() {
            failures.push(ValidationFailure::critical(
                "Source Primary Keys Required",
                format!(
                    "{} can only replicate these publication tables when they have a primary key: \
                     {}.\n\nAdd a primary key to each listed table, or remove the table from the \
                     publication before starting the pipeline. {}",
                    self.destination_name,
                    tables_without_pk.join(", "),
                    self.reason
                ),
            ));
        }

        if !tables_with_omitted_pk_columns.is_empty() {
            let formatted_tables = tables_with_omitted_pk_columns
                .iter()
                .map(|(table_name, omitted_columns)| format!("{table_name} ({omitted_columns})"))
                .collect::<Vec<_>>()
                .join(", ");
            failures.push(ValidationFailure::critical(
                "Source Primary Key Columns Required",
                format!(
                    "{} can only replicate publication tables when every source primary-key \
                     column is included in the publication column list. These tables omit \
                     primary-key columns: {}.\n\nAdd the listed columns to the publication column \
                     list, remove the column list so all columns are replicated, or remove the \
                     table from the publication before starting the pipeline. {}",
                    self.destination_name, formatted_tables, self.reason
                ),
            ));
        }

        Ok(failures)
    }
}
