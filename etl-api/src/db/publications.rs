use pg_escape::{quote_identifier, quote_literal};
use serde::Serialize;
use sqlx::{Executor, PgPool, Row};
use std::collections::HashMap;
use thiserror::Error;
use tokio_postgres::types::Type;
use utoipa::ToSchema;

use crate::db::tables::Table;

#[derive(Debug, Error)]
pub enum PublicationsDbError {
    #[error("Error while interacting with Postgres for publications: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Publication contains tables with unsupported column types: {0}")]
    UnsupportedColumnTypes(String),
}

#[derive(Serialize, ToSchema)]
pub struct Publication {
    pub name: String,
    pub tables: Vec<Table>,
}

/// Validates that all columns in the publication tables have supported types.
///
/// Queries `pg_attribute` to get type OIDs for all columns in the specified tables.
/// Returns an error if any column has a type OID that is unknown to `tokio_postgres`.
/// These unknown types would silently fall back to TEXT during replication, causing
/// data corruption.
pub async fn validate_publication_column_types(
    publication: &Publication,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    if publication.tables.is_empty() {
        return Ok(());
    }

    let mut unsupported_columns = Vec::new();

    for table in &publication.tables {
        let quoted_schema = quote_literal(&table.schema);
        let quoted_name = quote_literal(&table.name);

        let query = format!(
            r#"
            select
                a.attname as column_name,
                a.atttypid::int as type_oid,
                t.typname as type_name
            from pg_attribute a
            join pg_class c on a.attrelid = c.oid
            join pg_namespace n on c.relnamespace = n.oid
            join pg_type t on a.atttypid = t.oid
            where n.nspname = {quoted_schema}
                and c.relname = {quoted_name}
                and a.attnum > 0
                and not a.attisdropped
            order by a.attnum
            "#
        );

        let rows = pool.fetch_all(query.as_str()).await?;

        for row in rows {
            let column_name: String = row.get("column_name");
            // OID type in Postgres is stored as i32 in sqlx but represents u32
            let type_oid_raw: i32 = row.get("type_oid");
            let type_oid = type_oid_raw as u32;
            let type_name: String = row.get("type_name");

            if Type::from_oid(type_oid).is_none() {
                unsupported_columns.push(format!(
                    "{}.{}.{} (type: {}, oid: {})",
                    table.schema, table.name, column_name, type_name, type_oid
                ));
            }
        }
    }

    if !unsupported_columns.is_empty() {
        return Err(PublicationsDbError::UnsupportedColumnTypes(
            unsupported_columns.join(", "),
        ));
    }

    Ok(())
}

pub async fn create_publication(
    publication: &Publication,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    let mut query = String::new();
    let quoted_publication_name = quote_identifier(&publication.name);
    query.push_str("create publication ");
    query.push_str(&quoted_publication_name);
    if !publication.tables.is_empty() {
        query.push_str(" for table only ");
    }

    for (i, table) in publication.tables.iter().enumerate() {
        let quoted_schema = quote_identifier(&table.schema);
        let quoted_name = quote_identifier(&table.name);
        query.push_str(&quoted_schema);
        query.push('.');
        query.push_str(&quoted_name);

        if i < publication.tables.len() - 1 {
            query.push(',')
        }
    }

    // Ensure partitioned tables publish via ancestor/root schema for logical replication
    query.push_str(" with (publish_via_partition_root = true)");

    pool.execute(query.as_str()).await?;
    Ok(())
}

pub async fn update_publication(
    publication: &Publication,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    let mut query = String::new();
    let quoted_publication_name = quote_identifier(&publication.name);
    query.push_str("alter publication ");
    query.push_str(&quoted_publication_name);
    query.push_str(" set table only ");

    for (i, table) in publication.tables.iter().enumerate() {
        let quoted_schema = quote_identifier(&table.schema);
        let quoted_name = quote_identifier(&table.name);
        query.push_str(&quoted_schema);
        query.push('.');
        query.push_str(&quoted_name);

        if i < publication.tables.len() - 1 {
            query.push(',')
        }
    }

    pool.execute(query.as_str()).await?;
    Ok(())
}

pub async fn drop_publication(
    publication_name: &str,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    let mut query = String::new();
    query.push_str("drop publication if exists ");
    let quoted_publication_name = quote_identifier(publication_name);
    query.push_str(&quoted_publication_name);

    pool.execute(query.as_str()).await?;
    Ok(())
}

pub async fn read_publication(
    publication_name: &str,
    pool: &PgPool,
) -> Result<Option<Publication>, PublicationsDbError> {
    let mut query = String::new();
    query.push_str(
        r#"
        select p.pubname,
            pt.schemaname as "schemaname?",
            pt.tablename as "tablename?"
        from pg_publication p
        left join pg_publication_tables pt on p.pubname = pt.pubname
        where
           	p.puballtables = false
           	and p.pubinsert = true
           	and p.pubupdate = true
           	and p.pubdelete = true
           	and p.pubtruncate = true
            and p.pubname =
	   "#,
    );

    let quoted_publication_name = quote_literal(publication_name);
    query.push_str(&quoted_publication_name);

    let mut tables = vec![];
    let mut name: Option<String> = None;

    for row in pool.fetch_all(query.as_str()).await? {
        let pub_name: String = row.get("pubname");
        if let Some(ref name) = name {
            assert_eq!(name.as_str(), pub_name);
        } else {
            name = Some(pub_name);
        }
        let schema: Option<String> = row.get("schemaname?");
        let table_name: Option<String> = row.get("tablename?");
        if let (Some(schema), Some(table_name)) = (schema, table_name) {
            tables.push(Table {
                schema,
                name: table_name,
            });
        }
    }

    let publication = name.map(|name| Publication { name, tables });
    Ok(publication)
}

pub async fn read_all_publications(pool: &PgPool) -> Result<Vec<Publication>, PublicationsDbError> {
    let query = r#"
        select p.pubname,
            pt.schemaname as "schemaname?",
            pt.tablename as "tablename?"
        from pg_publication p
        left join pg_publication_tables pt on p.pubname = pt.pubname
        where
           	p.puballtables = false
           	and p.pubinsert = true
           	and p.pubupdate = true
           	and p.pubdelete = true
           	and p.pubtruncate = true;
	   "#;

    let mut pub_name_to_tables: HashMap<String, Vec<Table>> = HashMap::new();

    for row in pool.fetch_all(query).await? {
        let pub_name: String = row.get("pubname");
        let schema: Option<String> = row.get("schemaname?");
        let table_name: Option<String> = row.get("tablename?");
        let tables = pub_name_to_tables.entry(pub_name).or_default();

        if let (Some(schema), Some(table_name)) = (schema, table_name) {
            tables.push(Table {
                schema,
                name: table_name,
            });
        }
    }

    let publications = pub_name_to_tables
        .into_iter()
        .map(|(name, tables)| Publication { name, tables })
        .collect();

    Ok(publications)
}
