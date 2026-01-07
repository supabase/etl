//! Test utilities for BigQuery destinations.
//!
//! Provides a database wrapper for managing BigQuery datasets and constants for
//! connecting to Google Cloud BigQuery in test environments.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::bigquery::{BigQueryDestination, table_name_to_bigquery_table_id};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::TableName;
use gcp_bigquery_client::Client;
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::model::dataset::Dataset;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_cell::TableCell;
use gcp_bigquery_client::model::table_row::TableRow;
use tokio::runtime::Handle;
use tokio::time::sleep;
use uuid::Uuid;

/// Maximum number of times we re-run a verification query.
const BIGQUERY_QUERY_MAX_ATTEMPTS: u32 = 30;
/// Delay in milliseconds between verification attempts when querying BigQuery.
const BIGQUERY_QUERY_RETRY_DELAY_MS: u64 = 500;

/// Environment variable name for the BigQuery project ID.
pub const BIGQUERY_PROJECT_ID_ENV: &str = "TESTS_BIGQUERY_PROJECT_ID";

/// Environment variable name for the BigQuery service account key path.
pub const BIGQUERY_SA_KEY_PATH_ENV: &str = "TESTS_BIGQUERY_SA_KEY_PATH";

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
/// Provides a wrapper around the BigQuery client with automatic dataset cleanup on drop.
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
    /// Panics if the `TESTS_BIGQUERY_PROJECT_ID` environment variable is not set
    /// or if client creation fails.
    pub async fn new(sa_key_path: &str) -> Self {
        let project_id = get_project_id();
        let client = ClientBuilder::new()
            .build_from_service_account_key_file(sa_key_path)
            .await
            .expect("Failed to create BigQuery client");
        let dataset_id = random_dataset_id();

        Self {
            client,
            project_id,
            sa_key_path: sa_key_path.to_string(),
            dataset_id,
        }
    }

    /// Creates the dataset in BigQuery.
    pub async fn create_dataset(&self) {
        let dataset = Dataset::new(&self.project_id, &self.dataset_id);
        self.client
            .dataset()
            .create(dataset)
            .await
            .expect("Failed to create dataset");
    }

    /// Drops the dataset and all its contents.
    ///
    /// This function will not panic on errors - it logs them and continues.
    pub async fn drop_dataset(&self) {
        if let Err(e) = self
            .client
            .dataset()
            .delete(&self.project_id, &self.dataset_id, true)
            .await
        {
            eprintln!(
                "warning: failed to delete BigQuery dataset {}: {e}",
                self.dataset_id
            );
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
            panic!(
                "Failed to read service account key file at {}",
                self.sa_key_path
            )
        })
    }

    /// Returns a reference to the underlying BigQuery client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Executes a SELECT * query against the specified table.
    ///
    /// Returns all rows from the table in the test dataset, polling until BigQuery
    /// surfaces the streamed data or a short retry budget is exhausted.
    pub async fn query_table(&self, table_name: TableName) -> Option<Vec<TableRow>> {
        let table_id = table_name_to_bigquery_table_id(&table_name);
        let full_table_path = format!("`{}.{}.{}`", self.project_id, self.dataset_id, table_id);

        let query = format!("select * from {full_table_path}");
        let mut attempts_remaining = BIGQUERY_QUERY_MAX_ATTEMPTS;

        loop {
            let rows = self
                .client
                .job()
                .query(&self.project_id, QueryRequest::new(query.clone()))
                .await
                .unwrap()
                .rows;

            if rows.is_some() || attempts_remaining == 1 {
                return rows;
            }

            attempts_remaining -= 1;
            sleep(Duration::from_millis(BIGQUERY_QUERY_RETRY_DELAY_MS)).await;
        }
    }

    /// Queries the schema (column metadata) for a table.
    ///
    /// Returns the column names and data types from INFORMATION_SCHEMA.COLUMNS.
    /// The table name pattern matches using REGEXP_CONTAINS to match the sequenced
    /// table name format: `{table_id}_{sequence_number}`.
    pub async fn query_table_schema(&self, table_name: TableName) -> Option<BigQueryTableSchema> {
        let project_id = self.project_id();
        let dataset_id = self.dataset_id();
        let table_id = table_name_to_bigquery_table_id(&table_name);

        // Use REGEXP_CONTAINS to match the sequenced table name format.
        // BigQuery table names have format: {schema}_{table}_{sequence_number}
        // The regex matches the table_id followed by underscore and one or more digits.
        let query = format!(
            "SELECT column_name, data_type, ordinal_position \
             FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` \
             WHERE REGEXP_CONTAINS(table_name, r'^{table_id}_[0-9]+$') \
             ORDER BY ordinal_position"
        );

        let mut attempts_remaining = BIGQUERY_QUERY_MAX_ATTEMPTS;

        loop {
            let rows = self
                .client
                .job()
                .query(project_id, QueryRequest::new(query.clone()))
                .await
                .unwrap()
                .rows;

            if rows.is_some() || attempts_remaining == 1 {
                return rows.map(|r| BigQueryTableSchema::new(parse_bigquery_table_rows(r)));
            }

            attempts_remaining -= 1;
            sleep(Duration::from_millis(BIGQUERY_QUERY_RETRY_DELAY_MS)).await;
        }
    }

    /// Manually creates a table in the test dataset using column definitions.
    ///
    /// Creates a table by generating a DDL statement from the provided column specifications.
    /// Each column is specified as a tuple of (column_name, bigquery_type).
    pub async fn create_table(&self, table_id: &str, columns: &[(&str, &str)]) {
        let column_definitions: Vec<String> = columns
            .iter()
            .map(|(name, data_type)| format!("{name} {data_type}"))
            .collect();

        let ddl = format!(
            "create table `{}.{}.{}` ({})",
            self.project_id,
            self.dataset_id,
            table_id,
            column_definitions.join(", ")
        );

        self.client
            .job()
            .query(&self.project_id, QueryRequest::new(ddl))
            .await
            .unwrap();
    }

    /// Creates a [`BigQueryDestination`] configured for this database instance.
    ///
    /// Returns a destination suitable for ETL operations, configured with
    /// zero staleness to ensure immediate consistency for testing.
    pub async fn build_destination<S>(&self, schema_store: S) -> BigQueryDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
    {
        BigQueryDestination::new_with_key_path(
            self.project_id.clone(),
            self.dataset_id.clone(),
            &self.sa_key_path,
            Some(0), // Zero staleness for immediate consistency in tests
            10,      // Allow concurrent streams for testing
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
                    if let Err(e) = self
                        .client
                        .dataset()
                        .delete(&project_id, &dataset_id, true)
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

    /// Returns true if a column with the given name exists in the schema.
    pub fn has_column(&self, name: &str) -> bool {
        self.0.iter().any(|c| c.column_name == name)
    }

    /// Asserts that a column with the given name exists in the schema.
    ///
    /// Panics with a descriptive message if the column is not found.
    pub fn assert_has_column(&self, name: &str) {
        assert!(
            self.has_column(name),
            "expected column '{}' to exist in schema, but it was not found. Columns: {:?}",
            name,
            self.column_names()
        );
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

    /// Asserts that the schema contains exactly the specified columns (by name).
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

    /// Returns a reference to the underlying column schemas.
    pub fn columns(&self) -> &[BigQueryColumnSchema] {
        &self.0
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

/// Sets up a BigQuery database connection for testing without creating a dataset.
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
    table_cell
        .value
        .map(|value| value.as_str().unwrap().parse().unwrap())
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
