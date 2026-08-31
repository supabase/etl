//! V2 source-column data access.

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::types::Oid};
use thiserror::Error;
use utoipa::ToSchema;

use crate::data::tables::EXCLUDED_SOURCE_SCHEMAS;

/// An error returned while reading columns from a source database.
#[derive(Debug, Error)]
pub enum ColumnsDbError {
    /// The source database query failed.
    #[error("Error while interacting with Postgres for columns")]
    Database(#[from] sqlx::Error),
}

/// A column discovered in a source table.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SourceColumn {
    /// The column name.
    pub name: String,
    /// The Postgres-formatted column type.
    pub r#type: String,
    /// Whether the column accepts null values.
    pub nullable: bool,
    /// Whether the column belongs to the table's primary key.
    pub primary_key: bool,
}

/// A nullable row returned by the seeded source-column catalog query.
#[derive(FromRow)]
struct SourceColumnRow {
    /// Whether the requested table exists and is eligible for publication.
    table_exists: bool,
    /// The column name, when the table has a column.
    name: Option<String>,
    /// The PostgreSQL-formatted column type.
    r#type: Option<String>,
    /// Whether the column accepts null values.
    nullable: Option<bool>,
    /// Whether the column belongs to the table's primary key.
    primary_key: bool,
}

/// Returns the current columns for a source table, ordered by ordinal position.
pub async fn get_table_columns(
    pool: &PgPool,
    table_id: u32,
) -> Result<Option<Vec<SourceColumn>>, ColumnsDbError> {
    let query = r#"
        with table_ref as (
            select c.oid
            from pg_catalog.pg_class c
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where c.oid = $1
                and c.relkind in ('r', 'p')
                and c.relpersistence = 'p'
                and not (n.nspname = any($2::text[]))
                and n.nspname !~ '^pg_'
        )
        select
            t.oid is not null as table_exists,
            a.attname as name,
            pg_catalog.format_type(typ.oid, a.atttypmod) as type,
            not a.attnotnull as nullable,
            exists (
                select 1
                from pg_catalog.pg_index i
                where i.indrelid = a.attrelid
                    and i.indisprimary
                    and a.attnum = any(i.indkey)
            ) as primary_key
        from (values (true)) seed(present)
        left join table_ref t on seed.present
        left join pg_catalog.pg_attribute a
            on a.attrelid = t.oid
            and a.attnum > 0
            and not a.attisdropped
        left join pg_catalog.pg_type typ on typ.oid = a.atttypid
        order by a.attnum;
        "#;

    let rows = sqlx::query_as::<_, SourceColumnRow>(query)
        .bind(Oid(table_id))
        .bind(EXCLUDED_SOURCE_SCHEMAS)
        .fetch_all(pool)
        .await?;
    let table_exists: bool =
        rows.first().expect("seeded source-column query returns at least one row").table_exists;
    if !table_exists {
        return Ok(None);
    }

    let columns = rows
        .into_iter()
        .filter_map(|row| {
            let name = row.name?;
            Some(SourceColumn {
                name,
                r#type: row.r#type.expect("a source column has a PostgreSQL type"),
                nullable: row.nullable.expect("a source column has nullability metadata"),
                primary_key: row.primary_key,
            })
        })
        .collect();

    Ok(Some(columns))
}
