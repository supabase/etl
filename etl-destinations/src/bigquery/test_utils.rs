//! Test utilities for BigQuery destinations.
//!
//! Provides a database wrapper for managing BigQuery datasets and constants for
//! connecting to Google Cloud BigQuery in test environments.

use std::time::Duration;

use crate::bigquery::{BigQueryDestination, table_name_to_bigquery_table_id};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{PipelineId, TableName};
use gcp_bigquery_client::Client;
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::model::dataset::Dataset;
use gcp_bigquery_client::model::query_request::QueryRequest;
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
    pub async fn build_destination<S>(
        &self,
        pipeline_id: PipelineId,
        schema_store: S,
    ) -> BigQueryDestination<S>
    where
        S: StateStore + SchemaStore + Send + Sync,
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
