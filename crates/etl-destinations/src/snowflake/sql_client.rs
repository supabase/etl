use std::{collections::BTreeSet, sync::Arc, time::Duration};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    retry::{RetryDecision, RetryPolicy, retry_with_backoff},
    snowflake::{
        Config, Error, Result,
        auth::TokenProvider,
        sql::{quote_identifier, quote_string_literal},
    },
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
pub struct SqlClient<T> {
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
pub(crate) struct StatementResponse {
    #[serde(default)]
    pub(crate) statement_handle: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<String>,
    #[serde(default)]
    pub(crate) data: Option<Vec<Vec<serde_json::Value>>>,
    /// Names and positions for values in each result row.
    #[serde(default)]
    result_set_meta_data: Option<ResultSetMetadata>,
}

/// Column metadata returned with a Snowflake SQL result set.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultSetMetadata {
    /// Total rows across every result partition.
    #[serde(default)]
    num_rows: Option<u64>,
    /// Result columns in row order.
    #[serde(default)]
    row_type: Vec<ResultColumnMetadata>,
}

/// One named Snowflake SQL result column.
#[derive(Debug, Deserialize)]
struct ResultColumnMetadata {
    /// Column name supplied by Snowflake.
    name: String,
}

impl<T: TokenProvider> SqlClient<T> {
    pub fn new(config: Config, auth: Arc<T>, http: Client) -> Self {
        Self { config: config.without_credentials(), http, auth }
    }

    /// Execute a DDL statement (runs on Cloud Services, no warehouse required).
    ///
    /// Generates one request ID for this invocation so Snowflake can reconcile
    /// transport retries. A later invocation gets a new ID: recovery across
    /// invocations requires an operation-specific caller-owned identity.
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let request_id = Uuid::new_v4();
        self.execute_statement_with_request_id(sql, request_id).await?;
        Ok(())
    }

    /// Create a table with the given columns and provider-driven evolution
    /// disabled.
    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
        column_defs: &str,
    ) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {fqn} ({column_defs}) ENABLE_SCHEMA_EVOLUTION = FALSE"
        );
        self.execute_ddl(&sql).await
    }

    /// Validates a table's write schema.
    pub(crate) async fn validate_table_schema(
        &self,
        table_name: &str,
        expected_column_names: &[&str],
    ) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        let columns_response =
            self.execute_statement(&format!("SHOW COLUMNS IN TABLE {fqn}")).await?;
        let columns = parse_show_columns(&columns_response)?;

        validate_column_names(table_name, expected_column_names, &columns)
    }

    /// Remove all rows from a table without dropping it.
    ///
    /// `request_id` must remain stable while one execution has an unknown
    /// outcome and must change for each later physical truncate attempt.
    pub async fn truncate_table(&self, table_name: &str, request_id: Uuid) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        self.execute_statement_with_request_id(&format!("TRUNCATE TABLE {fqn}"), request_id)
            .await?;
        Ok(())
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
        let name_prefix = quote_string_literal(table_name);
        let sql = format!("SHOW TERSE TABLES IN SCHEMA {db}.{schema} STARTS WITH {name_prefix}");
        let resp = self.execute_statement(&sql).await?;
        let table_names = parse_show_table_names(&resp)?;
        Ok(table_names.contains(&table_name))
    }

    /// Add a nullable column to an existing table.
    ///
    /// When `default_clause` is present, Snowflake populates existing rows with
    /// that default as part of the add-column operation.
    pub async fn add_column(
        &self,
        table_name: &str,
        column_name: &str,
        column_type: &str,
        default_clause: Option<&str>,
    ) -> Result<()> {
        let fqn = self.fully_qualified_name(table_name);
        let col = quote_identifier(column_name);
        let default_clause = default_clause.unwrap_or_default();
        self.execute_ddl(&format!(
            "ALTER TABLE {fqn} ADD COLUMN {col} {column_type}{default_clause}"
        ))
        .await
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
    pub(crate) async fn execute_statement(&self, sql: &str) -> Result<StatementResponse> {
        let url = format!("{}/api/v2/statements", self.config.account_url());
        self.execute_statement_at_url(sql, &url).await
    }

    /// Submit a retry-safe SQL statement with a caller-owned request ID.
    async fn execute_statement_with_request_id(
        &self,
        sql: &str,
        request_id: Uuid,
    ) -> Result<StatementResponse> {
        let url = self.statement_url_with_request_id(request_id);
        self.execute_statement_at_url(sql, &url).await
    }

    /// Build the retry-safe SQL statement URL for one logical request.
    fn statement_url_with_request_id(&self, request_id: Uuid) -> String {
        format!("{}/api/v2/statements?requestId={request_id}&retry=true", self.config.account_url())
    }

    /// Submit a SQL statement to a prebuilt URL and reuse it across retries.
    async fn execute_statement_at_url(&self, sql: &str, url: &str) -> Result<StatementResponse> {
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
                    "retrying sql rest api request"
                );
            },
            || self.attempt_statement(url, &body),
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
            let status = http_resp.status();

            match status {
                StatusCode::OK => return http_resp.json().await.map_err(Error::HttpTransport),

                StatusCode::ACCEPTED => {
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

                StatusCode::UNPROCESSABLE_ENTITY => {
                    let resp: StatementResponse =
                        http_resp.json().await.map_err(Error::HttpTransport)?;
                    return Err(Error::Sql {
                        statement_handle: resp.statement_handle,
                        message: resp.message.unwrap_or_default(),
                    });
                }

                StatusCode::UNAUTHORIZED if !retried_auth => {
                    warn!("received 401 from sql rest api, invalidating cached token");
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
        let url = format!("{}/api/v2/statements/{}", self.config.account_url(), statement_handle);
        let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
        let mut delay = POLL_INITIAL_DELAY;

        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Sql {
                    statement_handle: Some(statement_handle.to_owned()),
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

            match http_resp.status() {
                StatusCode::OK => return http_resp.json().await.map_err(Error::HttpTransport),
                StatusCode::ACCEPTED => {
                    debug!(statement_handle, "statement still running, continuing poll");
                    continue;
                }
                StatusCode::UNPROCESSABLE_ENTITY => {
                    let resp: StatementResponse =
                        http_resp.json().await.map_err(Error::HttpTransport)?;
                    return Err(Error::Sql {
                        statement_handle: resp.statement_handle,
                        message: resp.message.unwrap_or_default(),
                    });
                }
                StatusCode::TOO_MANY_REQUESTS => {
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

/// Parse one named string column from a `SHOW` response.
fn parse_named_show_column<'a>(
    response: &'a StatementResponse,
    command: &str,
    result_column: &str,
) -> Result<Vec<&'a str>> {
    fn malformed_show_response(response: &StatementResponse, message: String) -> Error {
        Error::Sql { statement_handle: response.statement_handle.clone(), message }
    }

    let metadata = response.result_set_meta_data.as_ref().ok_or_else(|| {
        malformed_show_response(response, format!("{command} omitted result metadata"))
    })?;
    let result_column_index = metadata
        .row_type
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(result_column))
        .ok_or_else(|| {
            malformed_show_response(
                response,
                format!("{command} result metadata omitted column '{result_column}'"),
            )
        })?;
    let num_rows = metadata.num_rows.ok_or_else(|| {
        malformed_show_response(response, format!("{command} result metadata omitted numRows"))
    })?;
    let rows = response.data.as_deref().ok_or_else(|| {
        malformed_show_response(response, format!("{command} omitted result data"))
    })?;
    let actual_rows = u64::try_from(rows.len()).map_err(|_| {
        malformed_show_response(response, format!("{command} result row count overflowed"))
    })?;

    // Exact validation must not treat the first result partition as the whole
    // table.
    if actual_rows != num_rows {
        return Err(malformed_show_response(
            response,
            format!(
                "{command} returned {actual_rows} rows, but metadata declared {num_rows} total \
                 rows"
            ),
        ));
    }

    let mut columns = Vec::with_capacity(rows.len());

    for (row_index, row) in rows.iter().enumerate() {
        let value =
            row.get(result_column_index).and_then(serde_json::Value::as_str).ok_or_else(|| {
                malformed_show_response(
                    response,
                    format!("{command} row {row_index} has no string {result_column}"),
                )
            })?;
        columns.push(value);
    }

    Ok(columns)
}

/// Parse exact column identifiers from `SHOW COLUMNS`.
fn parse_show_columns(response: &StatementResponse) -> Result<Vec<&str>> {
    parse_named_show_column(response, "SHOW COLUMNS", "column_name")
}

/// Parse exact table identifiers from `SHOW TERSE TABLES`.
fn parse_show_table_names(response: &StatementResponse) -> Result<Vec<&str>> {
    parse_named_show_column(response, "SHOW TERSE TABLES", "name")
}

/// Validates exact column names without relying on column order.
fn validate_column_names(
    table_name: &str,
    expected_column_names: &[&str],
    actual_column_names: &[&str],
) -> Result<()> {
    let expected_column_names = expected_column_names.iter().copied().collect::<BTreeSet<_>>();
    let actual_column_names = actual_column_names.iter().copied().collect::<BTreeSet<_>>();

    if let Some(column_name) = expected_column_names.difference(&actual_column_names).next() {
        return Err(Error::MissingTableColumn {
            table_name: table_name.to_owned(),
            column_name: (*column_name).to_owned(),
        });
    }

    if let Some(column_name) = actual_column_names.difference(&expected_column_names).next() {
        return Err(Error::UnexpectedTableColumn {
            table_name: table_name.to_owned(),
            column_name: (*column_name).to_owned(),
        });
    }

    Ok(())
}

fn classify_for_retry(error: &Error) -> RetryDecision {
    match error {
        Error::HttpTransport(_) => RetryDecision::Retry,
        Error::HttpStatus { status, .. } => {
            if *status == StatusCode::REQUEST_TIMEOUT
                || *status == StatusCode::TOO_MANY_REQUESTS
                || status.is_server_error()
            {
                RetryDecision::Retry
            } else {
                RetryDecision::Stop
            }
        }
        _ => RetryDecision::Stop,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Token provider used by hermetic SQL client tests.
    struct TestTokenProvider;

    impl TokenProvider for TestTokenProvider {
        async fn get_token(&self) -> Result<String> {
            Ok("test-token".to_owned())
        }

        async fn invalidate_token(&self) {}
    }

    /// Build a SQL client with deterministic, non-secret configuration.
    fn test_client() -> SqlClient<TestTokenProvider> {
        let config = Config::new("example-account", "test-user", "test-db", "test-schema")
            .expect("test configuration should be valid");
        SqlClient::new(config, Arc::new(TestTokenProvider), Client::new())
    }

    /// Build a resolved statement response with named result columns.
    fn statement_response(
        column_names: &[&str],
        data: Vec<Vec<serde_json::Value>>,
    ) -> StatementResponse {
        let row_count = u64::try_from(data.len()).expect("test result row count should fit in u64");
        StatementResponse {
            statement_handle: Some("test-statement".to_owned()),
            message: None,
            data: Some(data),
            result_set_meta_data: Some(ResultSetMetadata {
                num_rows: Some(row_count),
                row_type: column_names
                    .iter()
                    .map(|name| ResultColumnMetadata { name: (*name).to_owned() })
                    .collect(),
            }),
        }
    }

    #[test]
    fn parses_show_columns_by_result_metadata_name() {
        let response = statement_response(
            &["ignored", "column_name"],
            vec![vec![serde_json::Value::Null, serde_json::Value::String("id".to_owned())]],
        );

        let columns = parse_show_columns(&response).expect("SHOW COLUMNS should parse");

        assert_eq!(columns, vec!["id"]);
    }

    #[test]
    fn parses_show_table_names_by_result_metadata_name() {
        let response = statement_response(
            &["name", "created_on"],
            vec![vec![serde_json::Value::String("EVENTS".to_owned()), serde_json::Value::Null]],
        );

        let table_names =
            parse_show_table_names(&response).expect("SHOW TERSE TABLES should parse");

        assert_eq!(table_names, vec!["EVENTS"]);
    }

    #[test]
    fn malformed_show_table_metadata_is_a_structural_sql_error() {
        let response = statement_response(&["created_on"], vec![vec![serde_json::Value::Null]]);

        let error = parse_show_table_names(&response).expect_err("name metadata is required");

        assert!(matches!(
            error,
            Error::Sql {
                statement_handle: Some(handle),
                message,
            } if handle == "test-statement"
                && message == "SHOW TERSE TABLES result metadata omitted column 'name'"
        ));
    }

    #[test]
    fn table_column_validation_is_order_independent() {
        let expected = ["id", "_cdc_operation", "_cdc_sequence_number"];
        let actual = ["_cdc_sequence_number", "id", "_cdc_operation"];

        validate_column_names("events", &expected, &actual)
            .expect("physical column order should not affect compatibility");
    }

    #[test]
    fn table_column_validation_rejects_contract_mismatches() {
        let expected = ["id", "_cdc_operation", "_cdc_sequence_number"];

        let missing = ["_cdc_operation", "_cdc_sequence_number"];
        assert!(matches!(
            validate_column_names("events", &expected, &missing),
            Err(Error::MissingTableColumn { table_name, column_name })
                if table_name == "events" && column_name == "id"
        ));

        let unexpected = ["id", "_cdc_operation", "_cdc_sequence_number", "extra"];
        assert!(matches!(
            validate_column_names("events", &expected, &unexpected),
            Err(Error::UnexpectedTableColumn { table_name, column_name })
                if table_name == "events" && column_name == "extra"
        ));

        let differently_cased = ["ID", "_cdc_operation", "_cdc_sequence_number"];
        assert!(matches!(
            validate_column_names("events", &expected, &differently_cased),
            Err(Error::MissingTableColumn { table_name, column_name })
                if table_name == "events" && column_name == "id"
        ));
    }

    #[test]
    fn malformed_show_metadata_is_a_structural_sql_error() {
        let response = statement_response(&["ignored"], vec![vec![serde_json::Value::Null]]);

        let error = parse_show_columns(&response).expect_err("column_name metadata is required");

        assert!(matches!(
            error,
            Error::Sql {
                statement_handle: Some(handle),
                message,
            } if handle == "test-statement"
                && message == "SHOW COLUMNS result metadata omitted column 'column_name'"
        ));
    }

    #[test]
    fn incomplete_show_result_fails_closed() {
        let mut response = statement_response(
            &["column_name"],
            vec![vec![serde_json::Value::String("id".to_owned())]],
        );
        let metadata =
            response.result_set_meta_data.as_mut().expect("test response should contain metadata");
        metadata.num_rows = Some(2);

        let error = parse_show_columns(&response).expect_err("incomplete SHOW must fail closed");

        assert!(matches!(
            error,
            Error::Sql { message, .. }
                if message == "SHOW COLUMNS returned 1 rows, but metadata declared 2 total rows"
        ));
    }

    #[test]
    fn classify_for_retry_cases() {
        let cases = [
            (
                Error::HttpStatus {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    body: String::new(),
                },
                RetryDecision::Retry,
            ),
            (
                Error::HttpStatus { status: StatusCode::TOO_MANY_REQUESTS, body: String::new() },
                RetryDecision::Retry,
            ),
            (
                Error::HttpStatus { status: StatusCode::REQUEST_TIMEOUT, body: String::new() },
                RetryDecision::Retry,
            ),
            (
                Error::HttpStatus { status: StatusCode::BAD_REQUEST, body: String::new() },
                RetryDecision::Stop,
            ),
            (Error::Sql { statement_handle: None, message: String::new() }, RetryDecision::Stop),
        ];
        for (error, expected) in cases {
            assert_eq!(classify_for_retry(&error), expected, "error: {error:?}");
        }
    }

    /// Request-aware statement URLs carry Snowflake's retry parameters.
    #[test]
    fn statement_url_with_request_id_includes_retry_parameters() {
        let client = test_client();
        let request_id = Uuid::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8")
            .expect("request ID should be valid");

        let url = reqwest::Url::parse(&client.statement_url_with_request_id(request_id))
            .expect("statement URL should be valid");
        let query_pairs = url
            .query_pairs()
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();

        assert_eq!(url.path(), "/api/v2/statements");
        assert_eq!(
            query_pairs,
            vec![
                ("requestId".to_owned(), request_id.to_string()),
                ("retry".to_owned(), "true".to_owned()),
            ]
        );
    }
}
