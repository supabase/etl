use gcp_bigquery_client::Client;
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::model::dataset::Dataset;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_row::TableRow;
use postgres::schema::TableName;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::destination::bigquery::BigQueryDestination;

/// Environment variable name for the BigQuery project id.
const BIGQUERY_PROJECT_ID_ENV_NAME: &str = "TESTS_BIGQUERY_PROJECT_ID";
/// Environment variable name for the BigQuery service account key path.
const BIGQUERY_SA_KEY_PATH_ENV_NAME: &str = "TESTS_BIGQUERY_SA_KEY_PATH";

/// Generates a unique dataset ID for test isolation.
///
/// Creates a random dataset name prefixed with "etl_tests_" to ensure
/// each test run uses a fresh dataset and avoid conflicts.
fn random_dataset_id() -> String {
    let uuid = Uuid::new_v4().simple().to_string();

    format!("etl_tests_{uuid}")
}

/// BigQuery database connection for testing using real Google Cloud BigQuery.
///
/// Provides a unified interface for BigQuery operations in tests, automatically
/// handling setup and teardown of test datasets using actual Google Cloud credentials.
pub struct BigQueryDatabase {
    client: Option<Client>,
    sa_key_path: String,
    project_id: String,
    dataset_id: String,
}

impl BigQueryDatabase {
    /// Creates a new real BigQuery database instance.
    ///
    /// Sets up a [`BigQueryDatabase`] that connects to Google Cloud BigQuery
    /// using the provided service account key file path and project ID from
    /// environment variables.
    ///
    /// # Panics
    ///
    /// Panics if the `TESTS_BIGQUERY_PROJECT_ID` environment variable is not set.
    async fn new_real(sa_key_path: String) -> Self {
        let project_id = std::env::var(BIGQUERY_PROJECT_ID_ENV_NAME).unwrap_or_else(|_| {
            panic!("The env variable {BIGQUERY_PROJECT_ID_ENV_NAME} to be set to a project id")
        });
        let dataset_id = random_dataset_id();

        let client = ClientBuilder::new()
            .build_from_service_account_key_file(&sa_key_path)
            .await
            .unwrap();

        initialize_bigquery(&client, &project_id, &dataset_id).await;

        Self {
            client: Some(client),
            sa_key_path,
            project_id,
            dataset_id,
        }
    }

    /// Creates a [`BigQueryDestination`] configured for this database instance.
    ///
    /// Returns a destination suitable for ETL operations, configured with
    /// zero staleness to ensure immediate consistency for testing.
    pub async fn build_destination(&self) -> BigQueryDestination {
        BigQueryDestination::new_with_key_path(
            self.project_id.clone(),
            self.dataset_id.clone(),
            &self.sa_key_path,
            // We set a `max_staleness_mins` to 0 since we want the changes to be applied at
            // query time.
            Some(0),
        )
        .await
        .unwrap()
    }

    /// Executes a SELECT * query against the specified table.
    ///
    /// Returns all rows from the table in the test dataset. Useful for
    /// verifying data after ETL operations in tests.
    pub async fn query_table(&self, table_name: TableName) -> Option<Vec<TableRow>> {
        let client = self.client().unwrap();

        let project_id = self.project_id();
        let dataset_id = self.dataset_id();
        let table_id = table_name.as_bigquery_table_id();

        let full_table_path = format!("`{project_id}.{dataset_id}.{table_id}`");

        let query = format!("SELECT * FROM {full_table_path}");
        client
            .job()
            .query(project_id, QueryRequest::new(query))
            .await
            .unwrap()
            .rows
    }

    /// Returns the Google Cloud project ID for this database instance.
    fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Returns the BigQuery dataset ID for this database instance.
    fn dataset_id(&self) -> &str {
        &self.dataset_id
    }

    /// Returns a reference to the BigQuery client, if available.
    fn client(&self) -> Option<&Client> {
        self.client.as_ref()
    }

    /// Takes ownership of the BigQuery client, leaving [`None`] in its place.
    ///
    /// Used during cleanup to move the client out for async dataset deletion.
    fn take_client(&mut self) -> Option<Client> {
        self.client.take()
    }
}

impl Drop for BigQueryDatabase {
    /// Cleans up the test dataset when the database instance is dropped.
    ///
    /// Automatically deletes the BigQuery dataset and all its tables to
    /// ensure test isolation and prevent resource leaks.
    fn drop(&mut self) {
        // We take out the client since during destruction we know that the struct won't be used
        // anymore.
        let Some(client) = self.take_client() else {
            return;
        };

        // To use `block_in_place,` we need a multithreaded runtime since when a blocking
        // task is issued, the runtime will offload existing tasks to another worker.
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async move {
                destroy_bigquery(&client, self.project_id(), self.dataset_id()).await;
            });
        });
    }
}

/// Creates a new BigQuery dataset for testing.
///
/// Sets up a fresh dataset in the specified project that will be used
/// for all table operations during the test.
async fn initialize_bigquery(client: &Client, project_id: &str, dataset_id: &str) {
    let dataset = Dataset::new(project_id, dataset_id);
    client.dataset().create(dataset).await.unwrap();
}

/// Deletes a BigQuery dataset and all its contents.
///
/// Removes the test dataset and all tables within it to clean up
/// resources after testing.
async fn destroy_bigquery(client: &Client, project_id: &str, dataset_id: &str) {
    client
        .dataset()
        .delete(project_id, dataset_id, true)
        .await
        .unwrap();
}

/// Sets up a BigQuery database connection for testing.
///
/// Connects to real Google Cloud BigQuery using the service account key path
/// from the `TESTS_BIGQUERY_SA_KEY_PATH` environment variable.
///
/// Creates a fresh dataset for test isolation.
///
/// # Panics
///
/// Panics if the `TESTS_BIGQUERY_SA_KEY_PATH` environment variable is not set.
pub async fn setup_bigquery_connection() -> BigQueryDatabase {
    let sa_key_path = std::env::var(BIGQUERY_SA_KEY_PATH_ENV_NAME)
        .unwrap_or_else(|_| panic!("The env variable {BIGQUERY_SA_KEY_PATH_ENV_NAME} must be set to a service account key file path"));

    BigQueryDatabase::new_real(sa_key_path).await
}
