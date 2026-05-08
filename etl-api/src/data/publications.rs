use std::collections::HashMap;

use etl_postgres::tokio::{PgSourceClient, PgSourceError};
use pg_escape::quote_identifier;
use serde::Serialize;
use thiserror::Error;
use utoipa::ToSchema;

use crate::data::tables::Table;

#[derive(Debug, Error)]
pub enum PublicationsDbError {
    #[error("Error while interacting with Postgres for publications: {0}")]
    Database(#[from] PgSourceError),
}

#[derive(Serialize, ToSchema)]
pub struct Publication {
    pub name: String,
    pub tables: Vec<Table>,
}

pub async fn create_publication(
    publication: &Publication,
    source_client: &PgSourceClient,
) -> Result<(), PublicationsDbError> {
    let query = create_publication_query(publication);
    source_client.execute(query.as_str(), &[]).await?;
    Ok(())
}

pub async fn update_publication(
    publication: &Publication,
    source_client: &PgSourceClient,
) -> Result<(), PublicationsDbError> {
    let query = update_publication_query(publication);
    source_client.execute(query.as_str(), &[]).await?;
    Ok(())
}

pub async fn drop_publication(
    publication_name: &str,
    source_client: &PgSourceClient,
) -> Result<(), PublicationsDbError> {
    let query = drop_publication_query(publication_name);
    source_client.execute(query.as_str(), &[]).await?;
    Ok(())
}

pub async fn read_publication(
    publication_name: &str,
    source_client: &PgSourceClient,
) -> Result<Option<Publication>, PublicationsDbError> {
    let rows = source_client
        .query(
            r#"
            select p.pubname,
                pt.schemaname,
                pt.tablename
            from pg_publication p
            left join pg_publication_tables pt on p.pubname = pt.pubname
            where p.pubname = $1
            "#,
            &[&publication_name],
        )
        .await?;

    Ok(publication_from_rows(rows).into_iter().next())
}

pub async fn read_all_publications(
    source_client: &PgSourceClient,
) -> Result<Vec<Publication>, PublicationsDbError> {
    let rows = source_client
        .query(
            r#"
            select p.pubname,
                pt.schemaname,
                pt.tablename
            from pg_publication p
            left join pg_publication_tables pt on p.pubname = pt.pubname
            "#,
            &[],
        )
        .await?;

    Ok(publication_from_rows(rows))
}

fn publication_from_rows(rows: Vec<tokio_postgres::Row>) -> Vec<Publication> {
    let mut pub_name_to_tables: HashMap<String, Vec<Table>> = HashMap::new();
    for row in rows {
        let pub_name: String = row.get("pubname");
        let schema: Option<String> = row.get("schemaname");
        let table_name: Option<String> = row.get("tablename");
        let tables = pub_name_to_tables.entry(pub_name).or_default();

        if let (Some(schema), Some(table_name)) = (schema, table_name) {
            tables.push(Table { schema, name: table_name });
        }
    }

    pub_name_to_tables.into_iter().map(|(name, tables)| Publication { name, tables }).collect()
}

fn create_publication_query(publication: &Publication) -> String {
    let mut query = String::new();
    query.push_str("create publication ");
    query.push_str(&quote_identifier(&publication.name));
    if !publication.tables.is_empty() {
        query.push_str(" for table only ");
    }

    push_qualified_tables(&mut query, &publication.tables);
    query.push_str(" with (publish_via_partition_root = true)");
    query
}

fn update_publication_query(publication: &Publication) -> String {
    let mut query = String::new();
    query.push_str("alter publication ");
    query.push_str(&quote_identifier(&publication.name));
    query.push_str(" set table only ");
    push_qualified_tables(&mut query, &publication.tables);
    query
}

fn drop_publication_query(publication_name: &str) -> String {
    format!("drop publication if exists {}", quote_identifier(publication_name))
}

fn push_qualified_tables(query: &mut String, tables: &[Table]) {
    for (i, table) in tables.iter().enumerate() {
        query.push_str(&quote_identifier(&table.schema));
        query.push('.');
        query.push_str(&quote_identifier(&table.name));

        if i < tables.len() - 1 {
            query.push(',');
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use etl_config::shared::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
    use etl_postgres::{tokio::test_utils::PgDatabase, types::TableName};
    use tokio_postgres::Client;

    use super::*;

    fn test_connection_config() -> PgConnectionConfig {
        let database_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();

        PgConnectionConfig {
            host: std::env::var("TESTS_DATABASE_HOST").unwrap_or_else(|_| "localhost".to_owned()),
            hostaddr: None,
            port: std::env::var("TESTS_DATABASE_PORT")
                .unwrap_or_else(|_| "5430".to_owned())
                .parse()
                .expect("TESTS_DATABASE_PORT must be a valid port number"),
            name: format!("api_publications_{database_suffix}"),
            username: std::env::var("TESTS_DATABASE_USERNAME")
                .unwrap_or_else(|_| "postgres".to_owned()),
            password: std::env::var("TESTS_DATABASE_PASSWORD")
                .ok()
                .or(Some("postgres".to_owned()))
                .map(Into::into),
            tls: TlsConfig::disabled(),
            keepalive: TcpKeepaliveConfig::default(),
        }
    }

    #[test]
    fn publication_queries_quote_identifiers() {
        let publication = Publication {
            name: "pub\"; drop schema public; --".to_owned(),
            tables: vec![Table {
                schema: "schema\"; drop schema public; --".to_owned(),
                name: "table\"; drop schema public; --".to_owned(),
            }],
        };

        assert_eq!(
            create_publication_query(&publication),
            "create publication \"pub\"\"; drop schema public; --\" for table only \"schema\"\"; \
             drop schema public; --\".\"table\"\"; drop schema public; --\" with \
             (publish_via_partition_root = true)"
        );
        assert_eq!(
            update_publication_query(&publication),
            "alter publication \"pub\"\"; drop schema public; --\" set table only \"schema\"\"; \
             drop schema public; --\".\"table\"\"; drop schema public; --\""
        );
        assert_eq!(
            drop_publication_query(&publication.name),
            "drop publication if exists \"pub\"\"; drop schema public; --\""
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publication_helpers_manage_publications() {
        let database = PgDatabase::<Client>::new(test_connection_config()).await;
        database.run_sql("create schema publication_test").await.unwrap();
        database
            .create_table(
                TableName::new("publication_test".to_owned(), "table_1".to_owned()),
                true,
                &[("name", "text")],
            )
            .await
            .unwrap();
        database
            .create_table(
                TableName::new("publication_test".to_owned(), "table_2".to_owned()),
                true,
                &[("name", "text")],
            )
            .await
            .unwrap();

        let source_client = PgSourceClient::connect(&database.config).await.unwrap();
        let publication = Publication {
            name: "api_publication_pub".to_owned(),
            tables: vec![Table {
                schema: "publication_test".to_owned(),
                name: "table_1".to_owned(),
            }],
        };

        create_publication(&publication, &source_client).await.unwrap();

        let read = read_publication(&publication.name, &source_client).await.unwrap().unwrap();
        assert_eq!(read.name, publication.name);
        assert_eq!(read.tables.len(), 1);
        assert_eq!(read.tables[0].name, "table_1");

        let updated_publication = Publication {
            name: publication.name.clone(),
            tables: vec![Table {
                schema: "publication_test".to_owned(),
                name: "table_2".to_owned(),
            }],
        };
        update_publication(&updated_publication, &source_client).await.unwrap();

        let read = read_publication(&publication.name, &source_client).await.unwrap().unwrap();
        assert_eq!(read.tables.len(), 1);
        assert_eq!(read.tables[0].name, "table_2");

        drop_publication(&publication.name, &source_client).await.unwrap();
        assert!(read_publication(&publication.name, &source_client).await.unwrap().is_none());
    }
}
