//! Replica identity validation for destination compatibility.
//!
//! The validator classifies each publication table into semantic identity
//! types so destinations can declare the row identities they can safely apply.

use async_trait::async_trait;
use etl::schema::IdentityType;
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
    /// Semantic replica identity types accepted for update events.
    update_identity_types: &'static [IdentityType],
    /// Semantic replica identity types accepted for delete events.
    delete_identity_types: &'static [IdentityType],
}

impl ReplicaIdentityValidator {
    /// Creates a replica identity validator for a destination.
    pub(super) fn new(
        publication_name: String,
        destination_name: &'static str,
        update_identity_types: &'static [IdentityType],
        delete_identity_types: &'static [IdentityType],
    ) -> Self {
        Self { publication_name, destination_name, update_identity_types, delete_identity_types }
    }
}

/// Replica identity catalog data for one publication table.
#[derive(Debug, FromRow)]
struct TableReplicaIdentityAudit {
    /// Whether the publication emits update events.
    publication_publishes_updates: bool,
    /// Whether the publication emits delete events.
    publication_publishes_deletes: bool,
    /// Schema-qualified table name for warning output.
    table_name: String,
    /// Raw `pg_class.relreplident` value for the table.
    relreplident: String,
    /// Table primary-key column numbers from `pg_index.indkey`.
    primary_key_attnums: Vec<i32>,
    /// Key column numbers from the index marked by `pg_index.indisreplident`.
    replica_identity_index_attnums: Vec<i32>,
}

/// Unsupported replica identity details for one publication table.
#[derive(Debug)]
struct UnsupportedTableIdentity {
    /// Formatted table and identity, without operation details.
    table_identity: String,
    /// Operations this identity cannot support.
    unsupported_operations: Vec<&'static str>,
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

        let table_identities = sqlx::query_as::<_, TableReplicaIdentityAudit>(
            r#"
            select
                p.pubupdate as publication_publishes_updates,
                p.pubdelete as publication_publishes_deletes,
                format('%I.%I', n.nspname, c.relname) as table_name,
                c.relreplident::text as relreplident,
                array(
                    select x.attnum::int4
                    from pg_index i
                    cross join lateral unnest(i.indkey) with ordinality as x(attnum, n)
                    where i.indrelid = c.oid
                      and i.indisprimary
                      and i.indisvalid
                      and x.n <= i.indnkeyatts
                      and x.attnum > 0
                    order by x.n
                ) as primary_key_attnums,
                array(
                    select x.attnum::int4
                    from pg_index i
                    cross join lateral unnest(i.indkey) with ordinality as x(attnum, n)
                    where i.indrelid = c.oid
                      and i.indisreplident
                      and i.indisvalid
                      and i.indislive
                      and x.n <= i.indnkeyatts
                      and x.attnum > 0
                    order by x.n
                ) as replica_identity_index_attnums
            from pg_publication p
            cross join lateral pg_get_publication_tables(p.pubname) gpt
            join pg_class c on c.oid = gpt.relid
            join pg_namespace n on n.oid = c.relnamespace
            where p.pubname = $1
            order by n.nspname, c.relname
            "#,
        )
        .bind(&self.publication_name)
        .fetch_all(source_pool)
        .await?;

        let Some(first_table_identity) = table_identities.first() else {
            // A missing or empty publication has no table identities to
            // validate. Pipeline validation reports either prerequisite.
            return Ok(vec![]);
        };
        let publication_publishes_updates = first_table_identity.publication_publishes_updates;
        let publication_publishes_deletes = first_table_identity.publication_publishes_deletes;

        let mut currently_unsupported_tables = Vec::new();
        let mut warning_tables = Vec::new();
        for table_identity in table_identities {
            let identity_type = identity_type_for_table(
                &table_identity.relreplident,
                &table_identity.primary_key_attnums,
                &table_identity.replica_identity_index_attnums,
            );

            let unsupported_current_operations = self.unsupported_operations(
                identity_type,
                publication_publishes_updates,
                publication_publishes_deletes,
            );
            if !unsupported_current_operations.is_empty() {
                currently_unsupported_tables.push(UnsupportedTableIdentity {
                    table_identity: format!("`{} ({})`", table_identity.table_name, identity_type),
                    unsupported_operations: unsupported_current_operations,
                });
                continue;
            }

            let unsupported_future_operations =
                self.unsupported_operations(identity_type, true, true);
            if !unsupported_future_operations.is_empty() {
                warning_tables.push(UnsupportedTableIdentity {
                    table_identity: format!("`{} ({})`", table_identity.table_name, identity_type),
                    unsupported_operations: unsupported_future_operations,
                });
            }
        }

        let mut failures = Vec::new();
        if !currently_unsupported_tables.is_empty() {
            failures.push(ValidationFailure::warning(
                "Unsupported Replica Identity",
                format!(
                    "{} can start, but these publication tables can emit {} changes whose replica \
                     identity is unsupported: {}. If such a change is received, streaming \
                     replication will fail before the destination write.\n\nSet each table to {} \
                     to support those changes. If a table has columns with large values (TOAST \
                     columns), `REPLICA IDENTITY FULL` is recommended. A table owner can apply it \
                     with `ALTER TABLE <schema.table> REPLICA IDENTITY FULL`; alternatively, use \
                     `USING INDEX <unique_index>` when that identity type is listed as supported.",
                    self.destination_name,
                    format_operations(&published_operations(
                        publication_publishes_updates,
                        publication_publishes_deletes,
                    )),
                    format_unsupported_tables(&currently_unsupported_tables),
                    format_supported_identity_types(&self.supported_identity_types_for_operations(
                        publication_publishes_updates,
                        publication_publishes_deletes,
                    )),
                ),
            ));
        }

        if !warning_tables.is_empty() {
            let warning_prefix = if !publication_publishes_updates && !publication_publishes_deletes
            {
                format!(
                    "{} can start because this publication does not replicate UPDATE or DELETE \
                     changes, but these tables use replica identities that would not support \
                     future mutation replication",
                    self.destination_name
                )
            } else {
                format!(
                    "{} can start with the current publication operations, but these tables use \
                     replica identities that would not support future mutation replication",
                    self.destination_name
                )
            };

            failures.push(ValidationFailure::warning(
                "Unsupported Replica Identity",
                format!(
                    "{}: {}.\n\nSet each table to {} before enabling those operations on the \
                     publication. If a table has columns with large values (TOAST columns), \
                     `REPLICA IDENTITY FULL` is recommended. A table owner can apply it with \
                     `ALTER TABLE <schema.table> REPLICA IDENTITY FULL`; alternatively, use \
                     `USING INDEX <unique_index>` when that identity type is listed as supported.",
                    warning_prefix,
                    format_unsupported_tables(&warning_tables),
                    format_supported_identity_types(
                        &self.supported_identity_types_for_operations(true, true),
                    ),
                ),
            ));
        }

        Ok(failures)
    }
}

impl ReplicaIdentityValidator {
    /// Returns the operations not supported by `identity_type`.
    fn unsupported_operations(
        &self,
        identity_type: IdentityType,
        check_update: bool,
        check_delete: bool,
    ) -> Vec<&'static str> {
        let mut operations = Vec::new();

        if check_update && !self.update_identity_types.contains(&identity_type) {
            operations.push("UPDATE");
        }

        if check_delete && !self.delete_identity_types.contains(&identity_type) {
            operations.push("DELETE");
        }

        operations
    }

    /// Returns the identity types supported by all selected operations.
    fn supported_identity_types_for_operations(
        &self,
        include_update: bool,
        include_delete: bool,
    ) -> Vec<IdentityType> {
        let mut supported = Vec::new();

        for identity_type in [
            IdentityType::PrimaryKey,
            IdentityType::AlternativeKey,
            IdentityType::Full,
            IdentityType::Missing,
        ] {
            let update_supported =
                !include_update || self.update_identity_types.contains(&identity_type);
            let delete_supported =
                !include_delete || self.delete_identity_types.contains(&identity_type);

            if update_supported && delete_supported {
                supported.push(identity_type);
            }
        }

        supported
    }
}

/// Formats unsupported table identities for a compact warning message.
fn format_unsupported_tables(tables: &[UnsupportedTableIdentity]) -> String {
    const MAX_DISPLAYED_TABLES: usize = 20;

    let displayed_tables =
        tables.iter().take(MAX_DISPLAYED_TABLES).map(format_unsupported_table).collect::<Vec<_>>();
    let remaining_count = tables.len().saturating_sub(MAX_DISPLAYED_TABLES);

    let displayed_tables = displayed_tables.join(", ");
    if remaining_count == 0 {
        displayed_tables
    } else {
        format!("{displayed_tables}, and {remaining_count} more")
    }
}

/// Formats one unsupported table identity.
fn format_unsupported_table(table: &UnsupportedTableIdentity) -> String {
    format!(
        "{} (unsupported for {})",
        table.table_identity,
        format_operations(&table.unsupported_operations)
    )
}

/// Returns publication operations as labels.
fn published_operations(publishes_updates: bool, publishes_deletes: bool) -> Vec<&'static str> {
    let mut operations = Vec::new();

    if publishes_updates {
        operations.push("UPDATE");
    }

    if publishes_deletes {
        operations.push("DELETE");
    }

    operations
}

/// Formats operation labels for user-facing messages.
fn format_operations(operations: &[&str]) -> String {
    match operations {
        [] => "mutation".to_owned(),
        [operation] => (*operation).to_owned(),
        [first, second] => format!("{first} or {second}"),
        [first, middle @ .., last] => {
            let mut formatted = Vec::with_capacity(operations.len());
            formatted.push((*first).to_owned());
            formatted.extend(middle.iter().map(|operation| (*operation).to_owned()));
            format!("{}, or {}", formatted.join(", "), last)
        }
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
