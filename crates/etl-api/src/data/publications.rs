use std::collections::HashMap;

use pg_escape::quote_identifier;
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgPool, Row};
use thiserror::Error;
use utoipa::ToSchema;

use crate::data::tables::{SourceTable, Table};

#[derive(Debug, Error)]
pub enum PublicationsDbError {
    #[error("Error while interacting with Postgres for publications: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Publication {
    pub name: String,
    pub tables: Vec<SourceTable>,
}

pub struct PublicationDefinition {
    pub name: String,
    pub tables: Vec<Table>,
}

pub async fn create_publication(
    publication: &PublicationDefinition,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    let mut query = String::new();
    let quoted_publication_name = quote_identifier(&publication.name);
    query.push_str("create publication ");
    query.push_str(&quoted_publication_name);
    if !publication.tables.is_empty() {
        query.push_str(" for table ");
    }

    for (i, table) in publication.tables.iter().enumerate() {
        let quoted_schema = quote_identifier(&table.schema);
        let quoted_name = quote_identifier(&table.name);
        query.push_str(&quoted_schema);
        query.push('.');
        query.push_str(&quoted_name);

        if i < publication.tables.len() - 1 {
            query.push(',');
        }
    }

    // Default API-created publications to logical partition root semantics.
    query.push_str(" with (publish_via_partition_root = true)");

    sqlx::query(AssertSqlSafe(query)).execute(pool).await?;
    Ok(())
}

pub async fn update_publication(
    publication: &PublicationDefinition,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    let mut query = String::new();
    let quoted_publication_name = quote_identifier(&publication.name);
    query.push_str("alter publication ");
    query.push_str(&quoted_publication_name);
    query.push_str(" set table ");

    for (i, table) in publication.tables.iter().enumerate() {
        let quoted_schema = quote_identifier(&table.schema);
        let quoted_name = quote_identifier(&table.name);
        query.push_str(&quoted_schema);
        query.push('.');
        query.push_str(&quoted_name);

        if i < publication.tables.len() - 1 {
            query.push(',');
        }
    }

    sqlx::query(AssertSqlSafe(query)).execute(pool).await?;
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

    sqlx::query(AssertSqlSafe(query)).execute(pool).await?;
    Ok(())
}

pub async fn read_publication(
    publication_name: &str,
    pool: &PgPool,
) -> Result<Option<Publication>, PublicationsDbError> {
    let query = r#"
        select p.pubname,
            c.oid::bigint as "table_id?",
            pt.schemaname as "schemaname?",
            pt.tablename as "tablename?"
        from pg_publication p
        left join pg_publication_tables pt on p.pubname = pt.pubname
        left join pg_namespace n on n.nspname = pt.schemaname
        left join pg_class c on c.relnamespace = n.oid and c.relname = pt.tablename
        where p.pubname = $1;
	   "#;

    let mut tables = vec![];
    let mut name: Option<String> = None;

    for row in sqlx::query(query).bind(publication_name).fetch_all(pool).await? {
        let pub_name: String = row.get("pubname");
        if let Some(ref name) = name {
            assert_eq!(name.as_str(), pub_name);
        } else {
            name = Some(pub_name);
        }
        let table_id: Option<i64> = row.get("table_id?");
        let schema: Option<String> = row.get("schemaname?");
        let table_name: Option<String> = row.get("tablename?");
        if let (Some(table_id), Some(schema), Some(table_name)) = (table_id, schema, table_name) {
            tables.push(SourceTable {
                id: u32::try_from(table_id).expect("Postgres OIDs fit in u32"),
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
            c.oid::bigint as "table_id?",
            pt.schemaname as "schemaname?",
            pt.tablename as "tablename?"
        from pg_publication p
        left join pg_publication_tables pt on p.pubname = pt.pubname
        left join pg_namespace n on n.nspname = pt.schemaname
        left join pg_class c on c.relnamespace = n.oid and c.relname = pt.tablename;
	   "#;

    let mut pub_name_to_tables: HashMap<String, Vec<SourceTable>> = HashMap::new();

    for row in sqlx::query(query).fetch_all(pool).await? {
        let pub_name: String = row.get("pubname");
        let table_id: Option<i64> = row.get("table_id?");
        let schema: Option<String> = row.get("schemaname?");
        let table_name: Option<String> = row.get("tablename?");
        let tables = pub_name_to_tables.entry(pub_name).or_default();

        if let (Some(table_id), Some(schema), Some(table_name)) = (table_id, schema, table_name) {
            tables.push(SourceTable {
                id: u32::try_from(table_id).expect("Postgres OIDs fit in u32"),
                schema,
                name: table_name,
            });
        }
    }

    let publications =
        pub_name_to_tables.into_iter().map(|(name, tables)| Publication { name, tables }).collect();

    Ok(publications)
}

pub async fn add_tables_to_publication(
    publication: &PublicationDefinition,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    let query = format!(
        "alter publication {} add table {}",
        quote_identifier(&publication.name),
        format_table_list(&publication.tables),
    );
    sqlx::query(AssertSqlSafe(query)).execute(pool).await?;
    Ok(())
}

pub async fn drop_tables_from_publication(
    publication: &PublicationDefinition,
    pool: &PgPool,
) -> Result<(), PublicationsDbError> {
    let query = format!(
        "alter publication {} drop table {}",
        quote_identifier(&publication.name),
        format_table_list(&publication.tables),
    );
    sqlx::query(AssertSqlSafe(query)).execute(pool).await?;
    Ok(())
}

fn format_table_list(tables: &[Table]) -> String {
    tables
        .iter()
        .map(|t| format!("{}.{}", quote_identifier(&t.schema), quote_identifier(&t.name)))
        .collect::<Vec<_>>()
        .join(", ")
}
