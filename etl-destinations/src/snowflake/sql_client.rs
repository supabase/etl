use std::{sync::Arc, time::Duration};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    retry::{RetryDecision, RetryPolicy, retry_with_backoff},
    snowflake::{Config, Error, Result, auth::TokenProvider},
};

/// Retry policy for transient HTTP errors (408, 429, 5xx) during SQL API calls.
const SQL_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 3,
    initial_delay: Duration::from_millis(500),
    max_delay: Duration::from_secs(10),
};

/// Starting delay between polls when waiting for an async statement (HTTP 202).
const POLL_INITIAL_DELAY: Duration = Duration::from_millis(100);

/// Upper bound on exponential backoff between poll requests.
const POLL_MAX_DELAY: Duration = Duration::from_secs(5);

/// Hard deadline for async statement completion before returning a timeout
/// error.
const POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Sent with every request to the Snowflake SQL REST API.
const USER_AGENT: &str = "supabase-etl/0.1.0";

/// Executes DDL and metadata operations against Snowflake's SQL REST API.
///
/// All DDL runs on Snowflake's Cloud Services layer (no warehouse required).
pub struct SqlClient<T: TokenProvider> {
    config: Config,
    http: Client,
    auth: Arc<T>,
}

#[derive(Debug, Serialize)]
struct StatementRequest<'a> {
    statement: &'a str,
    database: &'a str,
    schema: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'a str>,
}

/// Snowflake SQL REST API response body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatementResponse {
    #[serde(default)]
    statement_handle: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Option<Vec<Vec<serde_json::Value>>>,
}

impl<T: TokenProvider> SqlClient<T> {
    pub fn new(config: Config, auth: Arc<T>, http: Client) -> Self {
        Self { config, http, auth }
    }

    /// Execute a DDL statement (runs on Cloud Services, no warehouse required).
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        self.execute_statement(sql).await?;
        Ok(())
    }

    /// Create a table with the given columns, enabling schema evolution.
    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
        column_defs: &str,
    ) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {fqn} ({column_defs}) ENABLE_SCHEMA_EVOLUTION = TRUE"
        );
        self.execute_ddl(&sql).await
    }

    /// Remove all rows from a table without dropping it.
    pub async fn truncate_table(&self, table_name: &str) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        self.execute_ddl(&format!("TRUNCATE TABLE {fqn}")).await
    }

    /// Drop a table if it exists.
    pub async fn drop_table(&self, table_name: &str) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        self.execute_ddl(&format!("DROP TABLE IF EXISTS {fqn}")).await
    }

    /// Check whether a table exists in the configured database and schema.
    pub async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let db = quote_identifier(&self.config.database);
        let schema = quote_identifier(&self.config.schema);
        let escaped = escape_like_pattern(table_name);
        let sql = format!("SHOW TABLES LIKE '{escaped}' IN SCHEMA {db}.{schema}");
        let resp = self.execute_statement(&sql).await?;
        Ok(resp.data.is_some_and(|rows| !rows.is_empty()))
    }

    /// Add a nullable column to an existing table.
    pub async fn add_column(
        &self,
        table_name: &str,
        column_name: &str,
        column_type: &str,
    ) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        let col = quote_identifier(column_name);
        self.execute_ddl(&format!("ALTER TABLE {fqn} ADD COLUMN {col} {column_type}")).await
    }

    /// Remove a column from an existing table.
    pub async fn drop_column(&self, table_name: &str, column_name: &str) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        let col = quote_identifier(column_name);
        self.execute_ddl(&format!("ALTER TABLE {fqn} DROP COLUMN {col}")).await
    }

    /// Rename a column in an existing table.
    pub async fn rename_column(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        let old = quote_identifier(old_name);
        let new = quote_identifier(new_name);
        self.execute_ddl(&format!("ALTER TABLE {fqn} RENAME COLUMN {old} TO {new}")).await
    }

    fn fully_qualified_name(&self, name: &str) -> String {
        format!(
            "{}.{}.{}",
            quote_identifier(&self.config.database),
            quote_identifier(&self.config.schema),
            quote_identifier(name),
        )
    }

    /// Submit a SQL statement and return a resolved response.
    ///
    /// Handles the full Snowflake SQL REST API contract:
    /// - HTTP 200: success, return parsed `StatementResponse`.
    /// - HTTP 202: async execution, poll until 200 or 422.
    /// - HTTP 422: SQL execution error, return `Error::Sql`.
    /// - HTTP 401: invalidate cached token, retry once.
    /// - HTTP 408/429/5xx: retriable via backoff.
    /// - Other 4xx: non-retriable `Error::HttpStatus`.
    async fn execute_statement(&self, sql: &str) -> Result<StatementResponse> {
        let url = format!("{}/api/v2/statements", self.config.account_url);

        let body = StatementRequest {
            statement: sql,
            database: &self.config.database,
            schema: &self.config.schema,
            role: self.config.role.as_deref(),
        };

        retry_with_backoff(
            SQL_RETRY_POLICY,
            classify_for_retry,
            |d| d,
            |attempt| {
                warn!(
                    retry = attempt.retry_index,
                    max = attempt.max_retries,
                    delay_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying SQL REST API request"
                );
            },
            || self.attempt_statement(&url, &body),
        )
        .await
        .map_err(|f| f.last_error)
    }

    /// Submit a statement and interpret the HTTP response.
    ///
    /// On a 401, invalidates the cached token and retries once.
    async fn attempt_statement(
        &self,
        url: &str,
        body: &StatementRequest<'_>,
    ) -> Result<StatementResponse> {
        let mut retried_auth = false;

        loop {
            let token = self.auth.get_token().await?;
            let http_resp = self.send_post(url, &token, body).await?;
            let status = http_resp.status().as_u16();

            match status {
                200 => return http_resp.json().await.map_err(Error::HttpTransport),

                202 => {
                    let resp: StatementResponse =
                        http_resp.json().await.map_err(Error::HttpTransport)?;
                    return if let Some(ref handle) = resp.statement_handle {
                        debug!(statement_handle = %handle, "statement executing asynchronously, polling");
                        self.poll_until_complete(handle).await
                    } else {
                        Err(Error::Sql {
                            statement_handle: None,
                            message: "received 202 without a statement handle".into(),
                        })
                    };
                }

                422 => {
                    let resp: StatementResponse =
                        http_resp.json().await.map_err(Error::HttpTransport)?;
                    return Err(Error::Sql {
                        statement_handle: resp.statement_handle,
                        message: resp.message.unwrap_or_default(),
                    });
                }

                401 if !retried_auth => {
                    warn!("received 401 from SQL REST API, invalidating cached token");
                    self.auth.invalidate_token().await;
                    retried_auth = true;
                    continue;
                }

                _ => {
                    let body_text = http_resp.text().await.unwrap_or_default();
                    return Err(Error::HttpStatus { status, body: body_text });
                }
            }
        }
    }

    async fn send_post(
        &self,
        url: &str,
        token: &str,
        body: &StatementRequest<'_>,
    ) -> Result<reqwest::Response> {
        self.http
            .post(url)
            .bearer_auth(token)
            .header("User-Agent", USER_AGENT)
            .json(body)
            .send()
            .await
            .map_err(Error::HttpTransport)
    }

    /// Poll an async statement until Snowflake returns 200 (success) or 422
    /// (failure).
    async fn poll_until_complete(&self, statement_handle: &str) -> Result<StatementResponse> {
        let url = format!("{}/api/v2/statements/{}", self.config.account_url, statement_handle);
        let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
        let mut delay = POLL_INITIAL_DELAY;

        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Sql {
                    statement_handle: Some(statement_handle.to_string()),
                    message: format!(
                        "statement did not complete within {}s",
                        POLL_TIMEOUT.as_secs()
                    ),
                });
            }

            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(POLL_MAX_DELAY);

            let token = self.auth.get_token().await?;
            let http_resp = self
                .http
                .get(&url)
                .bearer_auth(&token)
                .header("User-Agent", USER_AGENT)
                .send()
                .await
                .map_err(Error::HttpTransport)?;

            match http_resp.status().as_u16() {
                200 => return http_resp.json().await.map_err(Error::HttpTransport),
                202 => {
                    debug!(statement_handle, "statement still running, continuing poll");
                    continue;
                }
                422 => {
                    let resp: StatementResponse =
                        http_resp.json().await.map_err(Error::HttpTransport)?;
                    return Err(Error::Sql {
                        statement_handle: resp.statement_handle,
                        message: resp.message.unwrap_or_default(),
                    });
                }
                429 => {
                    debug!(statement_handle, "rate limited during poll, backing off");
                    continue;
                }
                other => {
                    let body_text = http_resp.text().await.unwrap_or_default();
                    return Err(Error::HttpStatus { status: other, body: body_text });
                }
            }
        }
    }
}

/// Double-quote a SQL identifier, escaping internal double-quotes.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Escape a value for use in a Snowflake `LIKE` pattern inside single quotes.
fn escape_like_pattern(name: &str) -> String {
    name.replace('\\', "\\\\").replace('\'', "''").replace('_', "\\_").replace('%', "\\%")
}

fn classify_for_retry(error: &Error) -> RetryDecision {
    match error {
        Error::HttpTransport(_) => RetryDecision::Retry,
        Error::HttpStatus { status, .. } => match *status {
            408 | 429 => RetryDecision::Retry,
            s if s >= 500 => RetryDecision::Retry,
            _ => RetryDecision::Stop,
        },
        _ => RetryDecision::Stop,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_cases() {
        let cases =
            [("my_table", r#""my_table""#), (r#"my"table"#, r#""my""table""#), ("", r#""""#)];
        for (input, expected) in cases {
            assert_eq!(quote_identifier(input), expected, "input: {input:?}");
        }
    }

    #[test]
    fn escape_like_pattern_cases() {
        let cases = [
            ("my_table", r"my\_table"),
            ("100%done", r"100\%done"),
            ("it's", "it''s"),
            (r"a\b", r"a\\b"),
        ];
        for (input, expected) in cases {
            assert_eq!(escape_like_pattern(input), expected, "input: {input:?}");
        }
    }

    #[test]
    fn classify_for_retry_cases() {
        let cases = [
            (Error::HttpStatus { status: 500, body: String::new() }, RetryDecision::Retry),
            (Error::HttpStatus { status: 429, body: String::new() }, RetryDecision::Retry),
            (Error::HttpStatus { status: 408, body: String::new() }, RetryDecision::Retry),
            (Error::HttpStatus { status: 400, body: String::new() }, RetryDecision::Stop),
            (Error::Sql { statement_handle: None, message: String::new() }, RetryDecision::Stop),
        ];
        for (error, expected) in cases {
            assert_eq!(classify_for_retry(&error), expected, "error: {error:?}");
        }
    }
}
