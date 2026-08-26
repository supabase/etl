use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use utoipa::ToSchema;

use crate::data::tables::{EXCLUDED_SOURCE_SCHEMAS, TablesDbError};

/// The physical role of a source table in a partition hierarchy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceTableKind {
    /// A table that cannot own partitions.
    Table,
    /// A partitioned table that can own current and future partitions.
    PartitionedTable,
}

/// A table available for publication selection or exposed by a publication.
///
/// [`SourceTable::kind`] and [`SourceTable::partition_parent_id`] describe any
/// depth of declarative partition hierarchy without flattening it: a
/// partitioned table without a parent is a root, a partitioned table with a
/// parent is an intermediate node, a non-partitioned table with a parent is a
/// leaf, and a non-partitioned table without a parent is an ordinary table.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SourceTable {
    /// The table's Postgres OID in this source database.
    pub id: u32,
    /// The Postgres schema containing the table.
    pub schema: String,
    /// The unqualified Postgres table name.
    pub name: String,
    /// Whether the table can own partitions.
    pub kind: SourceTableKind,
    /// The direct partition parent's OID, or `null` for a partition root or
    /// ordinary table. When discovery is schema-filtered, the referenced
    /// parent can be outside the response.
    pub partition_parent_id: Option<u32>,
}

/// A source table decoded from PostgreSQL's catalogs.
#[derive(FromRow)]
pub(super) struct SourceTableRow {
    /// The table OID represented as a signed PostgreSQL integer.
    pub(super) id: i64,
    /// The schema containing the table.
    pub(super) schema: String,
    /// The unqualified table name.
    pub(super) name: String,
    /// Whether the table can own partitions.
    pub(super) is_partitioned: bool,
    /// The direct partition parent's OID.
    pub(super) partition_parent_id: Option<i64>,
}

/// Builds a [`SourceTable`] from a catalog row.
pub(super) fn source_table_from_row(row: SourceTableRow) -> SourceTable {
    SourceTable {
        id: u32::try_from(row.id).expect("Postgres OIDs fit in u32"),
        schema: row.schema,
        name: row.name,
        kind: if row.is_partitioned {
            SourceTableKind::PartitionedTable
        } else {
            SourceTableKind::Table
        },
        partition_parent_id: row
            .partition_parent_id
            .map(|id| u32::try_from(id).expect("Postgres OIDs fit in u32")),
    }
}

/// Returns publication-eligible source tables and their partition hierarchy.
pub(crate) async fn read_source_tables(
    pool: &PgPool,
    schema: Option<&str>,
) -> Result<Vec<SourceTable>, TablesDbError> {
    let rows = sqlx::query_as::<_, SourceTableRow>(
        r#"
        select
            c.oid::bigint as id,
            n.nspname as schema,
            c.relname as name,
            c.relkind = 'p' as is_partitioned,
            case when c.relispartition then i.inhparent::bigint end as partition_parent_id
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        left join pg_catalog.pg_inherits i
            on i.inhrelid = c.oid
            and c.relispartition
        where c.relkind in ('r', 'p')
            and c.relpersistence = 'p'
            and not (n.nspname = any($2::text[]))
            and n.nspname !~ '^pg_'
            and ($1::text is null or n.nspname = $1)
        order by n.nspname, c.relname;
        "#,
    )
    .bind(schema)
    .bind(EXCLUDED_SOURCE_SCHEMAS)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(source_table_from_row).collect())
}
