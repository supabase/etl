use etl::{error::EtlResult, schema::TableName};

use crate::{
    snowflake::{Config, Result, auth::TokenProvider, sql_client::SqlClient, view::view_names},
    table_name::try_stringify_table_name,
};

/// Load a [`Config`] from environment variables for integration tests.
pub fn load_test_config() -> Config {
    Config::require_tests_env().unwrap_or_else(|error| panic!("{error}"))
}

/// Executes a SQL statement and returns its result rows.
///
/// Data queries require an active warehouse, while metadata commands such as
/// `SHOW` do not.
pub async fn query_rows<T: TokenProvider>(
    client: &SqlClient<T>,
    sql: &str,
) -> Result<Vec<Vec<serde_json::Value>>> {
    let resp = client.execute_statement(sql).await?;
    Ok(resp.data.unwrap_or_default())
}

/// Drops the destination table and companion views for a source table.
///
/// Objects are dropped regardless of which pipeline created them, so callers
/// that reuse one schema across pipelines can free the names for the next
/// pipeline. Missing objects are ignored.
pub async fn drop_destination_table<T: TokenProvider>(
    client: &SqlClient<T>,
    table_name: &TableName,
) -> EtlResult<()> {
    let table = try_stringify_table_name(table_name)?.to_uppercase();
    let (view, pending) = view_names(&table);
    for view in [view, pending] {
        client
            .execute_ddl(&format!("drop view if exists {}", client.fully_qualified_name(&view)))
            .await?;
    }
    client.drop_table(&table).await?;

    Ok(())
}
