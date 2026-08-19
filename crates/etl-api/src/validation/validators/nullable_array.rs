//! Nullable source-array validation for destination compatibility.

use async_trait::async_trait;
use etl_postgres::version::POSTGRES_15;

use crate::validation::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Destination behavior for a top-level NULL array value.
#[derive(Debug)]
pub(super) enum NullableArrayBehavior {
    /// The destination stores NULL as an empty array.
    CoercesToEmpty,
    /// The destination wire format cannot encode the value.
    CannotEncode,
}

/// Warns about published nullable arrays whose NULL values have destination-
/// specific behavior.
#[derive(Debug)]
pub(super) struct NullableArrayValidator {
    /// Name of the publication whose columns should be checked.
    publication_name: String,
    /// Destination behavior described in the warning.
    behavior: NullableArrayBehavior,
}

impl NullableArrayValidator {
    /// Creates a nullable-array warning validator for a destination.
    pub(super) fn new(publication_name: String, behavior: NullableArrayBehavior) -> Self {
        Self { publication_name, behavior }
    }
}

#[async_trait]
impl Validator for NullableArrayValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let Some(source_pool) = ctx.source_pool.as_ref() else {
            return Ok(vec![]);
        };

        let server_version_num: i32 =
            sqlx::query_scalar("select current_setting('server_version_num')::int")
                .fetch_one(source_pool)
                .await?;
        let nullable_arrays: Vec<(String, String)> = if server_version_num >= POSTGRES_15 {
            sqlx::query_as(
                r#"
                select n.nspname || '.' || c.relname, a.attname::text
                from pg_publication p
                cross join lateral pg_get_publication_tables(p.pubname) gpt
                join pg_class c on c.oid = gpt.relid
                join pg_namespace n on n.oid = c.relnamespace
                join pg_attribute a on a.attrelid = c.oid
                join pg_type t on t.oid = a.atttypid
                left join pg_publication_tables pt
                  on pt.pubname = p.pubname
                 and pt.schemaname = n.nspname
                 and pt.tablename = c.relname
                where p.pubname = $1
                  and a.attnum > 0
                  and not a.attisdropped
                  and not a.attnotnull
                  and t.typcategory = 'A'
                  and (
                    cardinality(coalesce(pt.attnames, array[]::name[])) = 0
                    or a.attname = any(pt.attnames)
                  )
                order by n.nspname, c.relname, a.attnum
                limit 100
                "#,
            )
            .bind(&self.publication_name)
            .fetch_all(source_pool)
            .await?
        } else {
            sqlx::query_as(
                r#"
                select n.nspname || '.' || c.relname, a.attname::text
                from pg_publication p
                cross join lateral pg_get_publication_tables(p.pubname) gpt
                join pg_class c on c.oid = gpt.relid
                join pg_namespace n on n.oid = c.relnamespace
                join pg_attribute a on a.attrelid = c.oid
                join pg_type t on t.oid = a.atttypid
                where p.pubname = $1
                  and a.attnum > 0
                  and not a.attisdropped
                  and not a.attnotnull
                  and t.typcategory = 'A'
                order by n.nspname, c.relname, a.attnum
                limit 100
                "#,
            )
            .bind(&self.publication_name)
            .fetch_all(source_pool)
            .await?
        };

        if nullable_arrays.is_empty() {
            return Ok(vec![]);
        }

        let columns = nullable_arrays
            .iter()
            .map(|(table, column)| format!("`{table}.{column}`"))
            .collect::<Vec<_>>()
            .join(", ");

        let reason = match self.behavior {
            NullableArrayBehavior::CoercesToEmpty => format!(
                "BigQuery stores top-level NULL values as empty arrays in these array columns: \
                 {columns}.\n\nThe pipeline can start, but NULL and empty arrays become \
                 indistinguishable in BigQuery. Make the source column `NOT NULL` if that \
                 distinction matters."
            ),
            NullableArrayBehavior::CannotEncode => format!(
                "ClickHouse RowBinary cannot encode top-level NULL values for these array \
                 columns: {columns}.\n\nThe pipeline can start because non-NULL arrays are \
                 supported, but replication fails if an affected column contains a top-level NULL \
                 value. Ensure producers always write an array value, or make the source column \
                 `NOT NULL` after replacing existing NULL values. Empty arrays remain supported."
            ),
        };

        Ok(vec![ValidationFailure::warning("Nullable Array Semantics Differ", reason)])
    }
}
