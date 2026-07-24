use crate::snowflake::{Config, Result, auth::TokenProvider, sql_client::SqlClient};

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
