//! Source primary-key validation for destination compatibility.

use async_trait::async_trait;

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
}

impl PrimaryKeyValidator {
    /// Creates a destination-specific primary-key validator.
    pub(super) fn new(
        publication_name: String,
        destination_name: &'static str,
        reason: &'static str,
    ) -> Self {
        Self { publication_name, destination_name, reason }
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

        if tables_without_pk.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Source Primary Keys Required",
                format!(
                    "{} can only replicate these publication tables when they have a primary key: \
                     {}.\n\nAdd a primary key to each listed table, or remove the table from the \
                     publication before starting the pipeline. {}",
                    self.destination_name,
                    tables_without_pk.join(", "),
                    self.reason
                ),
            )])
        }
    }
}
