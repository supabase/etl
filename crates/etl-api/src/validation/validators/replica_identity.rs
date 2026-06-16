//! Replica identity validation for destination compatibility.
//!
//! The validator classifies each publication table into semantic identity
//! types so destinations can declare the row identities they can safely apply.

use async_trait::async_trait;
use etl_postgres::types::IdentityType;
use sqlx::FromRow;

use crate::validation::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates that publication tables use destination-supported replica
/// identities.
#[derive(Debug)]
pub(super) struct ReplicaIdentityValidator {
    /// Name of the publication whose tables should be checked.
    publication_name: String,
    /// User-facing destination name included in validation messages.
    destination_name: &'static str,
    /// Semantic replica identity types accepted by the destination.
    supported_identity_types: &'static [IdentityType],
}

impl ReplicaIdentityValidator {
    /// Creates a replica identity validator for a destination.
    pub(super) fn new(
        publication_name: String,
        destination_name: &'static str,
        supported_identity_types: &'static [IdentityType],
    ) -> Self {
        Self { publication_name, destination_name, supported_identity_types }
    }
}

/// Replica identity catalog data for one publication table.
#[derive(Debug, FromRow)]
struct TableReplicaIdentityAudit {
    /// Schema-qualified table name for warning output.
    table_name: String,
    /// Raw `pg_class.relreplident` value for the table.
    relreplident: String,
    /// Table primary-key column numbers from `pg_constraint.conkey`.
    primary_key_attnums: Vec<i32>,
    /// Key column numbers from the index marked by `pg_index.indisreplident`.
    replica_identity_index_attnums: Vec<i32>,
}

#[async_trait]
impl Validator for ReplicaIdentityValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let Some(source_pool) = ctx.source_pool.as_ref() else {
            return Ok(vec![]);
        };

        let Some(publication_publishes_updates_or_deletes): Option<bool> = sqlx::query_scalar(
            r#"
            select pubupdate or pubdelete
            from pg_publication
            where pubname = $1
            "#,
        )
        .bind(&self.publication_name)
        .fetch_optional(source_pool)
        .await?
        else {
            // If the publication doesn't exist, skip this check. Pipeline
            // validation reports the actionable publication failure.
            return Ok(vec![]);
        };

        let table_identities = sqlx::query_as::<_, TableReplicaIdentityAudit>(
            r#"
            select
                n.nspname || '.' || c.relname as table_name,
                c.relreplident::text as relreplident,
                coalesce(pk.primary_key_attnums, array[]::int4[]) as primary_key_attnums,
                coalesce(ri.replica_identity_index_attnums, array[]::int4[])
                    as replica_identity_index_attnums
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
            left join lateral (
                select array_agg(x.attnum::int4 order by x.n) as replica_identity_index_attnums
                from pg_index i
                cross join lateral unnest(i.indkey) with ordinality as x(attnum, n)
                where i.indrelid = c.oid
                  and i.indisreplident
                  and x.n <= i.indnkeyatts
                  and x.attnum > 0
            ) ri on true
            where p.pubname = $1
            order by n.nspname, c.relname
            "#,
        )
        .bind(&self.publication_name)
        .fetch_all(source_pool)
        .await?;

        let unsupported_tables = table_identities
            .into_iter()
            .filter_map(|table_identity| {
                let identity_type = identity_type_for_table(
                    &table_identity.relreplident,
                    &table_identity.primary_key_attnums,
                    &table_identity.replica_identity_index_attnums,
                );

                if self.supported_identity_types.contains(&identity_type) {
                    None
                } else {
                    Some(format!("{} ({})", table_identity.table_name, identity_type))
                }
            })
            .collect::<Vec<_>>();

        if unsupported_tables.is_empty() {
            Ok(vec![])
        } else if publication_publishes_updates_or_deletes {
            Ok(vec![ValidationFailure::critical(
                "Unsupported Replica Identity",
                format!(
                    "{} cannot safely replicate UPDATE or DELETE changes for these publication \
                     tables because their replica identity is unsupported: {}.\n\nSet each table \
                     to {} before starting the pipeline.",
                    self.destination_name,
                    format_unsupported_tables(&unsupported_tables),
                    format_supported_identity_types(self.supported_identity_types),
                ),
            )])
        } else {
            Ok(vec![ValidationFailure::warning(
                "Unsupported Replica Identity",
                format!(
                    "{} can start because this publication only replicates INSERT changes, but \
                     these tables use replica identities that would not support UPDATE or DELETE \
                     replication: {}.\n\nSet each table to {} before enabling UPDATE or DELETE on \
                     the publication.",
                    self.destination_name,
                    format_unsupported_tables(&unsupported_tables),
                    format_supported_identity_types(self.supported_identity_types),
                ),
            )])
        }
    }
}

/// Formats unsupported table identities for a compact warning message.
fn format_unsupported_tables(tables: &[String]) -> String {
    const MAX_DISPLAYED_TABLES: usize = 20;

    let displayed_tables =
        tables.iter().take(MAX_DISPLAYED_TABLES).map(String::as_str).collect::<Vec<_>>().join(", ");
    let remaining_count = tables.len().saturating_sub(MAX_DISPLAYED_TABLES);

    if remaining_count == 0 {
        displayed_tables
    } else {
        format!("{displayed_tables}, and {remaining_count} more")
    }
}

/// Converts raw Postgres replica identity metadata into a semantic identity.
fn identity_type_for_table(
    relreplident: &str,
    primary_key_attnums: &[i32],
    replica_identity_index_attnums: &[i32],
) -> IdentityType {
    match relreplident {
        "f" => IdentityType::Full,
        "d" if primary_key_attnums.is_empty() => IdentityType::Missing,
        "d" => IdentityType::PrimaryKey,
        "i" if replica_identity_index_attnums.is_empty() => IdentityType::Missing,
        "i" if attnums_match(replica_identity_index_attnums, primary_key_attnums) => {
            IdentityType::PrimaryKey
        }
        "i" => IdentityType::AlternativeKey,
        "n" => IdentityType::Missing,
        _ => IdentityType::Missing,
    }
}

/// Returns whether two attnum lists refer to the same table columns.
fn attnums_match(left: &[i32], right: &[i32]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_unstable();
    right.sort_unstable();

    left == right
}

/// Formats supported identity types as user-facing suggestions.
fn format_supported_identity_types(identity_types: &[IdentityType]) -> String {
    match identity_types {
        [] => "no".to_owned(),
        [identity_type] => format_identity_type_suggestion(*identity_type).to_owned(),
        [first, second] => {
            format!(
                "{} or {}",
                format_identity_type_suggestion(*first),
                format_identity_type_suggestion(*second)
            )
        }
        [first, middle @ .., last] => {
            let mut formatted = Vec::with_capacity(identity_types.len());
            formatted.push(format_identity_type_suggestion(*first).to_owned());
            formatted.extend(
                middle.iter().map(|identity_type| {
                    format_identity_type_suggestion(*identity_type).to_owned()
                }),
            );
            format!("{}, or {}", formatted.join(", "), format_identity_type_suggestion(*last))
        }
    }
}

/// Formats a single identity type as a user-facing suggestion.
fn format_identity_type_suggestion(identity_type: IdentityType) -> &'static str {
    match identity_type {
        IdentityType::PrimaryKey => {
            "a primary-key identity (`REPLICA IDENTITY DEFAULT` on a table with a primary key, or \
             `REPLICA IDENTITY USING INDEX` with the primary-key index)"
        }
        IdentityType::AlternativeKey => {
            "an alternative-key identity (`REPLICA IDENTITY USING INDEX` with a unique, \
             non-primary-key index)"
        }
        IdentityType::Full => "`REPLICA IDENTITY FULL`",
        IdentityType::Missing => "no replica identity",
    }
}
