//! Source primary-key validation for destination compatibility.

use async_trait::async_trait;
use etl_postgres::version::POSTGRES_15;

use super::MAX_REPORTED_OBJECTS;
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
    /// Numeric PostgreSQL server version used to select supported catalogs.
    server_version_num: i32,
}

impl PrimaryKeyValidator {
    /// Creates a destination-specific primary-key validator.
    pub(super) fn new(
        publication_name: String,
        destination_name: &'static str,
        reason: &'static str,
        require_primary_key: bool,
        server_version_num: i32,
    ) -> Self {
        Self { publication_name, destination_name, reason, require_primary_key, server_version_num }
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

        let tables_without_pk: Vec<String> = sqlx::query_scalar(
            r#"
            select format('%I.%I', n.nspname, c.relname)
            from pg_publication p
            cross join lateral pg_get_publication_tables(p.pubname) gpt
            join pg_class c on c.oid = gpt.relid
            join pg_namespace n on n.oid = c.relnamespace
            where p.pubname = $1
              and not exists (
                select 1
                from pg_index i
                where i.indrelid = c.oid
                  and i.indisprimary
                  and i.indisvalid
              )
            order by n.nspname, c.relname
            limit $2
            "#,
        )
        .bind(&self.publication_name)
        .bind(MAX_REPORTED_OBJECTS)
        .fetch_all(source_pool)
        .await?;

        let tables_with_omitted_pk_columns = if self.server_version_num >= POSTGRES_15 {
            sqlx::query_as::<_, (String, String)>(
                r#"
                with publication_tables as (
                    select
                        format('%I.%I', n.nspname, c.relname) as table_name,
                        c.oid as table_oid,
                        gpt.attrs::smallint[] as replicated_attnums
                    from pg_publication p
                    cross join lateral pg_get_publication_tables(p.pubname) gpt
                    join pg_class c on c.oid = gpt.relid
                    join pg_namespace n on n.oid = c.relnamespace
                    where p.pubname = $1
                ),
                primary_key_cols as (
                    select
                        pt.table_name,
                        pt.table_oid,
                        pt.replicated_attnums,
                        x.attnum,
                        x.n::int4 as position
                    from publication_tables pt
                    join pg_index i
                      on i.indrelid = pt.table_oid
                     and i.indisprimary
                     and i.indisvalid
                    cross join lateral unnest(i.indkey) with ordinality as x(attnum, n)
                    where x.n <= i.indnkeyatts
                      and x.attnum > 0
                )
                select
                    pk.table_name,
                    string_agg(a.attname::text, ', ' order by pk.position) as omitted_columns
                from primary_key_cols pk
                join pg_attribute a
                  on a.attrelid = pk.table_oid
                 and a.attnum = pk.attnum
                -- Before PostgreSQL 18, NULL means that no publication column
                -- list was specified, so no primary-key column is omitted.
                where pk.replicated_attnums is not null
                  and pk.attnum <> all(pk.replicated_attnums)
                group by pk.table_name
                order by pk.table_name
                limit $2
                "#,
            )
            .bind(&self.publication_name)
            .bind(MAX_REPORTED_OBJECTS)
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
                     {}.\n\nAdd a stable, unique primary key to each listed source table with \
                     `ALTER TABLE <schema.table> ADD PRIMARY KEY (<column>, ...)`, or have the \
                     publication owner remove the table with `ALTER PUBLICATION {} DROP TABLE \
                     <schema.table>` before starting the pipeline. {}",
                    self.destination_name,
                    format_code_list(&tables_without_pk),
                    self.publication_name,
                    self.reason
                ),
            ));
        }

        if !tables_with_omitted_pk_columns.is_empty() {
            let formatted_tables = tables_with_omitted_pk_columns
                .iter()
                .map(|(table_name, omitted_columns)| {
                    format!("`{table_name}` (`{omitted_columns}`)")
                })
                .collect::<Vec<_>>()
                .join(", ");
            failures.push(ValidationFailure::critical(
                "Source Primary Key Columns Required",
                format!(
                    "{} can only replicate publication tables when every source primary-key \
                     column is included in the publication column list. These tables omit \
                     primary-key columns: {}.\n\nAdd the listed columns to the publication column \
                     list by dropping and re-adding the table with `ALTER PUBLICATION {} DROP \
                     TABLE <schema.table>` followed by `ALTER PUBLICATION {} ADD TABLE \
                     <schema.table> (<columns>)`. Re-add it without a column list to publish \
                     every column, or leave it out of the publication. {}",
                    self.destination_name,
                    formatted_tables,
                    self.publication_name,
                    self.publication_name,
                    self.reason
                ),
            ));
        }

        Ok(failures)
    }
}

/// Formats validation values as inline code for UI rendering.
fn format_code_list(values: &[String]) -> String {
    values.iter().map(|value| format!("`{value}`")).collect::<Vec<_>>().join(", ")
}
