//! Test utilities for BigQuery destinations.
//!
//! Provides a database wrapper for managing BigQuery datasets and constants for
//! connecting to Google Cloud BigQuery in test environments.

use std::{fmt, future::Future, path::Path, str::FromStr, time::Duration};

use etl::{pipeline::PipelineId, schema::TableName, store::DestinationStore};
use gcp_bigquery_client::{
    Client,
    client_builder::ClientBuilder,
    error::BQError,
    model::{
        dataset::Dataset, query_request::QueryRequest, table_cell::TableCell, table_row::TableRow,
    },
};
use tokio::{runtime::Handle, time::sleep};
use uuid::Uuid;

use crate::{
    bigquery::{BigQueryDestination, table_name_to_bigquery_table_id},
    retry::{RetryDecision, RetryPolicy, retry_with_backoff},
};

/// Maximum number of times we re-run a verification query.
///
/// Sized generously because a view dropped and recreated under the same name
/// can serve stale NOT_FOUND responses well past the first few seconds.
const BIGQUERY_QUERY_MAX_ATTEMPTS: u32 = 120;
/// Maximum number of times we poll for a table to report zero rows.
///
/// Kept short: the expected end state is observable as soon as the deletes
/// are applied, so this only absorbs propagation noise.
const BIGQUERY_NO_ROWS_MAX_ATTEMPTS: u32 = 30;
/// Delay in milliseconds between verification attempts when querying BigQuery.
const BIGQUERY_QUERY_RETRY_DELAY_MS: u64 = 500;
/// BigQuery response reasons that are transient even when surfaced with a 4xx
/// status code.
const TRANSIENT_BIGQUERY_RESPONSE_REASONS: &[&str] = &["backendError", "jobBackendError"];

/// Retry policy for raw BigQuery operations in tests.
const BIGQUERY_TEST_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 4,
    initial_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(4),
};

/// Returns whether a `BQError` is transient and worth retrying.
fn is_transient_bq_error(err: &BQError) -> RetryDecision {
    match err {
        BQError::RequestError(_) => RetryDecision::Retry,
        BQError::ResponseError { error } if error.error.code >= 500 => RetryDecision::Retry,
        BQError::ResponseError { error }
            if error.error.errors.iter().any(|nested_error| {
                nested_error.get("reason").is_some_and(|reason| {
                    TRANSIENT_BIGQUERY_RESPONSE_REASONS.contains(&reason.as_str())
                })
            }) =>
        {
            RetryDecision::Retry
        }
        _ => RetryDecision::Stop,
    }
}

/// Runs a raw BigQuery test operation with transient-error retries.
async fn retry_bigquery_test_operation<T, AttemptFn, AttemptFut>(
    operation: &'static str,
    attempt_fn: AttemptFn,
) -> Result<T, BQError>
where
    AttemptFn: FnMut() -> AttemptFut,
    AttemptFut: Future<Output = Result<T, BQError>>,
{
    retry_with_backoff(
        BIGQUERY_TEST_RETRY_POLICY,
        is_transient_bq_error,
        |delay| delay,
        |attempt| {
            eprintln!(
                "bigquery {operation} failed with transient error (attempt {}/{}), retrying in \
                 {:?}: {}",
                attempt.retry_index, attempt.max_retries, attempt.sleep_delay, attempt.error,
            );
        },
        attempt_fn,
    )
    .await
    .map_err(|failure| failure.last_error)
}

/// Environment variable name for the BigQuery project ID.
pub const BIGQUERY_PROJECT_ID_ENV: &str = "TESTS_BIGQUERY_PROJECT_ID";

/// Environment variable name for the BigQuery service account key path.
pub const BIGQUERY_SA_KEY_PATH_ENV: &str = "TESTS_BIGQUERY_SA_KEY_PATH";

/// When set, tests panic instead of skipping when BigQuery credentials are
/// missing.
pub const REQUIRE_BIGQUERY_CREDENTIALS_ENV: &str = "REQUIRE_BIGQUERY_CREDENTIALS";

/// Returns whether BigQuery integration tests should be skipped.
///
/// Prints a warning and returns `true` when credentials are unavailable.
/// Panics if [`REQUIRE_BIGQUERY_CREDENTIALS_ENV`] is set, and credentials are
/// not provided.
pub fn skip_if_missing_bigquery_env_vars() -> bool {
    let sa_key_path = std::env::var_os(BIGQUERY_SA_KEY_PATH_ENV);
    let has_project_id = std::env::var_os(BIGQUERY_PROJECT_ID_ENV).is_some();
    let has_sa_key_path = sa_key_path.is_some();
    let has_sa_key_file = sa_key_path.as_ref().is_some_and(|path| Path::new(path).is_file());
    if has_sa_key_file && has_project_id {
        return false;
    }

    let require = std::env::var_os(REQUIRE_BIGQUERY_CREDENTIALS_ENV).is_some_and(|v| !v.is_empty());
    let mut missing_env_vars = Vec::new();
    if !has_sa_key_path {
        missing_env_vars.push(BIGQUERY_SA_KEY_PATH_ENV);
    } else if !has_sa_key_file {
        if require {
            panic!(
                "BigQuery credentials required but {BIGQUERY_SA_KEY_PATH_ENV} does not point to \
                 an existing file"
            );
        }
        eprintln!(
            "skipping bigquery integration test: {BIGQUERY_SA_KEY_PATH_ENV} does not point to an \
             existing file"
        );

        return true;
    }

    if !has_project_id {
        missing_env_vars.push(BIGQUERY_PROJECT_ID_ENV);
    }

    if require {
        panic!("BigQuery credentials required but missing: {}", missing_env_vars.join(", "));
    }

    eprintln!("skipping bigquery integration test: missing {}", missing_env_vars.join(", "));

    true
}

/// Returns the BigQuery project ID from the environment.
///
/// # Panics
///
/// Panics if the `TESTS_BIGQUERY_PROJECT_ID` environment variable is not set.
pub fn get_project_id() -> String {
    std::env::var(BIGQUERY_PROJECT_ID_ENV)
        .unwrap_or_else(|_| panic!("{BIGQUERY_PROJECT_ID_ENV} must be set"))
}

/// Returns the BigQuery service account key path from the environment.
///
/// # Panics
///
/// Panics if the `TESTS_BIGQUERY_SA_KEY_PATH` environment variable is not set.
pub fn get_sa_key_path() -> String {
    std::env::var(BIGQUERY_SA_KEY_PATH_ENV)
        .unwrap_or_else(|_| panic!("{BIGQUERY_SA_KEY_PATH_ENV} must be set"))
}

/// Generates a unique dataset ID for test isolation.
pub fn random_dataset_id() -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("etl_tests_{uuid}")
}

/// BigQuery database connection for testing.
///
/// Provides a wrapper around the BigQuery client with automatic dataset cleanup
/// on drop.
pub struct BigQueryDatabase {
    client: Client,
    project_id: String,
    sa_key_path: String,
    dataset_id: String,
}

impl BigQueryDatabase {
    /// Creates a new BigQuery database instance.
    ///
    /// Does not create a dataset - call `create_dataset()` to initialize one.
    ///
    /// # Panics
    ///
    /// Panics if the `TESTS_BIGQUERY_PROJECT_ID` environment variable is not
    /// set or if client creation fails.
    pub async fn new(sa_key_path: &str) -> Self {
        let project_id = get_project_id();
        let client = retry_bigquery_test_operation("client creation", || async {
            ClientBuilder::new().build_from_service_account_key_file(sa_key_path).await
        })
        .await
        .expect("Failed to create BigQuery client");
        let dataset_id = random_dataset_id();

        Self { client, project_id, sa_key_path: sa_key_path.to_owned(), dataset_id }
    }

    /// Creates the dataset in BigQuery, retrying on transient errors.
    pub async fn create_dataset(&self) {
        retry_bigquery_test_operation("dataset creation", || async {
            let dataset = Dataset::new(&self.project_id, &self.dataset_id);
            self.client.dataset().create(dataset).await.map(|_| ())
        })
        .await
        .unwrap_or_else(|error| panic!("Failed to create BigQuery dataset: {error}"));
    }

    /// Drops the dataset and all its contents.
    ///
    /// This function will not panic on errors - it logs them and continues.
    pub async fn drop_dataset(&self) {
        if let Err(e) = retry_bigquery_test_operation("dataset deletion", || async {
            self.client.dataset().delete(&self.project_id, &self.dataset_id, true).await
        })
        .await
        {
            eprintln!("warning: failed to delete BigQuery dataset {}: {e}", self.dataset_id);
        }
    }

    /// Returns the project ID.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Returns the dataset ID.
    pub fn dataset_id(&self) -> &str {
        &self.dataset_id
    }

    /// Returns the service account key path.
    pub fn sa_key_path(&self) -> &str {
        &self.sa_key_path
    }

    /// Returns the [`String`] version of the SA key at the `sa_key_path`.
    pub fn sa_key(&self) -> String {
        std::fs::read_to_string(&self.sa_key_path).unwrap_or_else(|_| {
            panic!("Failed to read service account key file at {}", self.sa_key_path)
        })
    }

    /// Returns a reference to the underlying BigQuery client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Executes a SELECT * query against the specified table.
    ///
    /// Returns all rows from the table in the test dataset, polling until
    /// BigQuery surfaces the streamed data or the retry budget is exhausted.
    /// A 404 is retried within the same budget because a table queried right
    /// after being dropped and recreated can return a stale NOT_FOUND while
    /// BigQuery metadata propagates. Use
    /// [`BigQueryDatabase::wait_for_no_rows`] to assert that a table is
    /// empty; this method treats absence as not-yet-visible.
    pub async fn query_table(&self, table_name: TableName) -> Option<Vec<TableRow>> {
        let table_id = table_name_to_bigquery_table_id(&table_name).unwrap();
        let full_table_path = format!("`{}.{}.{}`", self.project_id, self.dataset_id, table_id);

        let query = format!("select * from {full_table_path}");
        let mut attempts_remaining = BIGQUERY_QUERY_MAX_ATTEMPTS;

        loop {
            let (rows, not_found) = match retry_bigquery_test_operation("table query", || {
                let request = QueryRequest::new(query.clone());
                async { self.client.job().query(&self.project_id, request).await }
            })
            .await
            {
                Ok(response) => (response.rows, false),
                Err(BQError::ResponseError { error }) if error.error.code == 404 => (None, true),
                Err(err) => panic!("Failed to query BigQuery table: {err:?}"),
            };

            if rows.is_some() || attempts_remaining == 1 {
                return rows;
            }

            if attempts_remaining.is_multiple_of(10) {
                let reason = if not_found { "table not found" } else { "no rows" };
                eprintln!(
                    "bigquery table query for {full_table_path}: {reason}, retrying \
                     ({attempts_remaining} attempts left)"
                );
            }

            attempts_remaining -= 1;
            sleep(Duration::from_millis(BIGQUERY_QUERY_RETRY_DELAY_MS)).await;
        }
    }

    /// Polls until a query against the table reports zero visible rows.
    ///
    /// Returns true once an empty result is observed and false if no empty
    /// result is seen before the retry budget runs out. A 404 is retried
    /// like a non-empty result: stale metadata can serve NOT_FOUND for a
    /// table that still has rows, so only an actual empty response proves
    /// the deletes were applied. Use this to assert that deletes emptied a
    /// table; [`BigQueryDatabase::query_table`] waits for data instead.
    pub async fn wait_for_no_rows(&self, table_name: TableName) -> bool {
        let table_id = table_name_to_bigquery_table_id(&table_name).unwrap();
        let full_table_path = format!("`{}.{}.{}`", self.project_id, self.dataset_id, table_id);

        let query = format!("select * from {full_table_path}");
        let mut attempts_remaining = BIGQUERY_NO_ROWS_MAX_ATTEMPTS;

        loop {
            // Only an observed empty result proves emptiness: a stale 404
            // could hide a table that still has rows, so it is retried like
            // a non-empty result.
            let observed_empty = match retry_bigquery_test_operation("table no-rows check", || {
                let request = QueryRequest::new(query.clone());
                async { self.client.job().query(&self.project_id, request).await }
            })
            .await
            {
                Ok(response) => response.rows.is_none_or(|rows| rows.is_empty()),
                Err(BQError::ResponseError { error }) if error.error.code == 404 => false,
                Err(err) => panic!("Failed to query BigQuery table: {err:?}"),
            };

            if observed_empty {
                return true;
            }

            if attempts_remaining == 1 {
                return false;
            }

            attempts_remaining -= 1;
            sleep(Duration::from_millis(BIGQUERY_QUERY_RETRY_DELAY_MS)).await;
        }
    }

    /// Queries the schema (column metadata) for a table.
    ///
    /// Returns the column names and data types from INFORMATION_SCHEMA.COLUMNS.
    /// The table name pattern matches using REGEXP_CONTAINS to match the
    /// sequenced table name format: `{table_id}_{sequence_number}`.
    pub async fn query_table_schema(&self, table_name: TableName) -> Option<BigQueryTableSchema> {
        let project_id = self.project_id();
        let dataset_id = self.dataset_id();
        let table_id = table_name_to_bigquery_table_id(&table_name).unwrap();

        // Use REGEXP_CONTAINS to match the sequenced table name format.
        // BigQuery table names have format: {schema}_{table}_{sequence_number}
        // The regex matches the table_id followed by underscore and one or more digits.
        let query = format!(
            "SELECT column_name, data_type, ordinal_position FROM \
             `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` WHERE \
             REGEXP_CONTAINS(table_name, r'^{table_id}_[0-9]+$') ORDER BY ordinal_position"
        );

        let mut attempts_remaining = BIGQUERY_QUERY_MAX_ATTEMPTS;

        loop {
            let rows = match retry_bigquery_test_operation("table schema query", || {
                let request = QueryRequest::new(query.clone());
                async { self.client.job().query(project_id, request).await }
            })
            .await
            {
                Ok(response) => response.rows,
                Err(BQError::ResponseError { error }) if error.error.code == 404 => return None,
                Err(err) => panic!("Failed to query BigQuery table schema: {err:?}"),
            };

            if rows.is_some() || attempts_remaining == 1 {
                return rows.map(|r| BigQueryTableSchema::new(parse_bigquery_table_rows(r)));
            }

            attempts_remaining -= 1;
            sleep(Duration::from_millis(BIGQUERY_QUERY_RETRY_DELAY_MS)).await;
        }
    }

    /// Gets the logical view schema from the BigQuery table metadata API.
    pub async fn get_view_schema(&self, table_name: TableName) -> Option<BigQueryTableSchema> {
        let table_id = table_name_to_bigquery_table_id(&table_name).unwrap();

        self.get_table_schema_by_id(&table_id).await
    }

    /// Gets a table or view schema from the BigQuery table metadata API.
    pub async fn get_table_schema_by_id(&self, table_id: &str) -> Option<BigQueryTableSchema> {
        let table = match retry_bigquery_test_operation("table metadata query", || async {
            self.client.table().get(&self.project_id, &self.dataset_id, table_id, None).await
        })
        .await
        {
            Ok(table) => table,
            Err(BQError::ResponseError { error }) if error.error.code == 404 => return None,
            Err(err) => panic!("Failed to get BigQuery table metadata: {err:?}"),
        };

        Some(BigQueryTableSchema::from_table_fields(table.schema.fields.unwrap_or_default()))
    }

    /// Queries BigQuery column defaults for an exact table ID.
    pub async fn query_column_defaults_by_id(&self, table_id: &str) -> Vec<BigQueryColumnDefault> {
        let query = format!(
            "select column_name, column_default from `{}.{}.INFORMATION_SCHEMA.COLUMNS` where \
             table_name = '{}' order by ordinal_position",
            self.project_id, self.dataset_id, table_id
        );

        let rows = retry_bigquery_test_operation("table default metadata query", || {
            let request = QueryRequest::new(query.clone());
            async { self.client.job().query(&self.project_id, request).await }
        })
        .await
        .unwrap_or_else(|err| panic!("Failed to query BigQuery column defaults: {err:?}"))
        .rows
        .unwrap_or_default();

        parse_bigquery_table_rows(rows)
    }

    /// Manually creates a table in the test dataset using column definitions.
    ///
    /// Creates a table by generating a DDL statement from the provided column
    /// specifications. Each column is specified as a tuple of (column_name,
    /// bigquery_type).
    pub async fn create_table(&self, table_id: &str, columns: &[(&str, &str)]) {
        let column_definitions: Vec<String> =
            columns.iter().map(|(name, data_type)| format!("{name} {data_type}")).collect();

        let ddl = format!(
            "create table `{}.{}.{}` ({})",
            self.project_id,
            self.dataset_id,
            table_id,
            column_definitions.join(", ")
        );

        retry_bigquery_test_operation("manual table creation", || {
            let request = QueryRequest::new(ddl.clone());
            async { self.client.job().query(&self.project_id, request).await.map(|_| ()) }
        })
        .await
        .expect("Failed to create BigQuery table");
    }

    /// Creates a [`BigQueryDestination`] configured for this database instance.
    ///
    /// Returns a destination suitable for ETL operations, configured with
    /// zero staleness to ensure immediate consistency for testing.
    pub async fn build_destination<S>(
        &self,
        pipeline_id: PipelineId,
        schema_store: S,
    ) -> BigQueryDestination<S>
    where
        S: DestinationStore,
    {
        BigQueryDestination::new_with_key_path(
            self.project_id.clone(),
            self.dataset_id.clone(),
            &self.sa_key_path,
            Some(0), // Zero staleness for immediate consistency in tests
            10,      // Allow concurrent streams for testing
            pipeline_id,
            schema_store,
        )
        .await
        .expect("Failed to create BigQuery destination")
    }
}

impl Drop for BigQueryDatabase {
    fn drop(&mut self) {
        let project_id = self.project_id.clone();
        let dataset_id = self.dataset_id.clone();

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                Handle::current().block_on(async {
                    if let Err(e) = retry_bigquery_test_operation("dataset deletion", || async {
                        self.client.dataset().delete(&project_id, &dataset_id, true).await
                    })
                    .await
                    {
                        eprintln!("warning: failed to delete BigQuery dataset {dataset_id}: {e}");
                    }
                });
            });
        }));
    }
}

/// Represents a column in a BigQuery table schema.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct BigQueryColumnSchema {
    pub column_name: String,
    pub data_type: String,
    pub ordinal_position: i64,
}

impl From<TableRow> for BigQueryColumnSchema {
    fn from(value: TableRow) -> Self {
        let columns = value.columns.unwrap();

        BigQueryColumnSchema {
            column_name: parse_table_cell(columns[0].clone()).unwrap(),
            data_type: parse_table_cell(columns[1].clone()).unwrap(),
            ordinal_position: parse_table_cell(columns[2].clone()).unwrap(),
        }
    }
}

/// BigQuery column default metadata returned from `INFORMATION_SCHEMA.COLUMNS`.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct BigQueryColumnDefault {
    pub column_name: String,
    pub column_default: Option<String>,
}

impl From<TableRow> for BigQueryColumnDefault {
    fn from(value: TableRow) -> Self {
        let columns = value.columns.unwrap();
        // BigQuery query rows encode a null STRING value as the string `NULL`.
        let column_default = parse_table_cell(columns[1].clone())
            .filter(|column_default: &String| column_default != "NULL");

        BigQueryColumnDefault {
            column_name: parse_table_cell(columns[0].clone()).unwrap(),
            column_default,
        }
    }
}

/// Wrapper around a BigQuery table schema for cleaner assertions in tests.
///
/// Provides convenient methods to check column presence and absence, making
/// schema validation in tests more readable and reducing boilerplate.
#[derive(Debug)]
pub struct BigQueryTableSchema(Vec<BigQueryColumnSchema>);

impl BigQueryTableSchema {
    /// Creates a new schema wrapper from a vector of column schemas.
    pub fn new(columns: Vec<BigQueryColumnSchema>) -> Self {
        Self(columns)
    }

    /// Creates a schema wrapper from BigQuery table metadata fields.
    pub fn from_table_fields(
        fields: Vec<gcp_bigquery_client::model::table_field_schema::TableFieldSchema>,
    ) -> Self {
        let columns = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| BigQueryColumnSchema {
                column_name: field.name,
                data_type: format!("{:?}", field.r#type),
                ordinal_position: index as i64 + 1,
            })
            .collect();

        Self(columns)
    }

    /// Returns true if a column with the given name exists in the schema.
    pub fn has_column(&self, name: &str) -> bool {
        self.0.iter().any(|c| c.column_name == name)
    }

    /// Asserts that a column with the given name does not exist in the schema.
    ///
    /// Panics with a descriptive message if the column is found.
    pub fn assert_no_column(&self, name: &str) {
        assert!(
            !self.has_column(name),
            "expected column '{}' to not exist in schema, but it was found. Columns: {:?}",
            name,
            self.column_names()
        );
    }

    /// Asserts that the schema contains exactly the specified columns (by
    /// name).
    ///
    /// The order of columns does not matter. CDC columns (`_CHANGE_TYPE` and
    /// `_CHANGE_SEQUENCE_NUMBER`) are excluded from the comparison.
    pub fn assert_columns(&self, expected: &[&str]) {
        let actual: Vec<&str> = self
            .0
            .iter()
            .map(|c| c.column_name.as_str())
            .filter(|name| !name.starts_with("_CHANGE"))
            .collect();

        let mut expected_sorted: Vec<&str> = expected.to_vec();
        expected_sorted.sort();

        let mut actual_sorted: Vec<&str> = actual.clone();
        actual_sorted.sort();

        assert_eq!(
            actual_sorted, expected_sorted,
            "schema columns mismatch. Expected: {expected_sorted:?}, Actual: {actual_sorted:?}"
        );
    }

    /// Returns the names of all columns in the schema.
    pub fn column_names(&self) -> Vec<&str> {
        self.0.iter().map(|c| c.column_name.as_str()).collect()
    }
}

/// Sets up a BigQuery database connection for testing.
///
/// Creates a fresh dataset for test isolation. The dataset is automatically
/// cleaned up when the returned `BigQueryDatabase` is dropped.
///
/// # Panics
///
/// Panics if the `TESTS_BIGQUERY_SA_KEY_PATH` environment variable is not set.
pub async fn setup_bigquery_database() -> BigQueryDatabase {
    let sa_key_path = get_sa_key_path();
    let db = BigQueryDatabase::new(&sa_key_path).await;
    db.create_dataset().await;
    db
}

/// Sets up a BigQuery database connection for testing without creating a
/// dataset.
///
/// Useful for validation tests that don't need an actual dataset.
/// The dataset ID is still generated but not created in BigQuery.
///
/// # Panics
///
/// Panics if the `TESTS_BIGQUERY_SA_KEY_PATH` environment variable is not set.
pub async fn setup_bigquery_database_without_dataset() -> BigQueryDatabase {
    let sa_key_path = get_sa_key_path();
    BigQueryDatabase::new(&sa_key_path).await
}

pub fn parse_table_cell<O>(table_cell: TableCell) -> Option<O>
where
    O: FromStr,
    <O as FromStr>::Err: fmt::Debug,
{
    table_cell.value.map(|value| value.as_str().unwrap().parse().unwrap())
}

pub fn parse_bigquery_table_rows<T>(table_rows: Vec<TableRow>) -> Vec<T>
where
    T: Ord,
    T: From<TableRow>,
{
    let mut parsed_table_rows = Vec::with_capacity(table_rows.len());

    for table_row in table_rows {
        parsed_table_rows.push(table_row.into());
    }
    parsed_table_rows.sort();

    parsed_table_rows
}
