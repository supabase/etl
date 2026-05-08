use std::collections::HashMap;

use thiserror::Error;
use tokio_postgres::Transaction;

pub use crate::db::extract_server_version;
use crate::types::{TableId, TableName};

/// Errors that can occur during table lookups.
#[derive(Debug, Error)]
pub enum TableLookupError {
    /// A database operation failed.
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    /// A table ID was not present in Postgres catalogs.
    #[error("Table with ID {0} not found")]
    TableNotFound(TableId),
}

/// Reads source table names for table IDs.
pub async fn table_names_from_table_ids(
    txn: &Transaction<'_>,
    table_ids: &[TableId],
) -> Result<HashMap<TableId, TableName>, TableLookupError> {
    if table_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let ids = table_ids.to_vec();
    let rows = txn
        .query(
            r#"
            select c.oid as oid, n.nspname as schema_name, c.relname as table_name
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where c.oid = any($1::oid[])
            "#,
            &[&ids],
        )
        .await?;

    let mut result = HashMap::with_capacity(rows.len());
    for row in rows {
        let table_id = row.get("oid");
        let schema_name: String = row.get("schema_name");
        let table_name: String = row.get("table_name");
        result.insert(table_id, TableName { schema: schema_name, name: table_name });
    }

    Ok(result)
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use etl_config::shared::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
    use tokio_postgres::Client;

    use super::*;
    use crate::tokio::{PgSourceClient, test_utils::PgDatabase};

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
            name: format!("source_db_helpers_{database_suffix}"),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn table_names_from_table_ids_reads_names_in_transaction() {
        let database = PgDatabase::<Client>::new(test_connection_config()).await;
        let mut source_client = PgSourceClient::connect(&database.config).await.unwrap();
        source_client
            .batch_execute("create table public.items (id bigint primary key)")
            .await
            .unwrap();

        let table_id: TableId = source_client
            .query_one("select 'public.items'::regclass::oid", &[])
            .await
            .unwrap()
            .get(0);

        let tx = source_client.transaction().await.unwrap();
        let table_names = table_names_from_table_ids(&tx, &[table_id]).await.unwrap();
        tx.commit().await.unwrap();

        assert_eq!(
            table_names.get(&table_id),
            Some(&TableName::new("public".to_owned(), "items".to_owned()))
        );
    }
}
