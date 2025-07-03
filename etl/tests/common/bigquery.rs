use etl::v2::destination::bigquery::BigQueryDestination;
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::model::dataset::Dataset;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_row::TableRow;
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::Client;
use postgres::schema::TableName;
use serde::Serialize;
use std::ops::Deref;
use tokio::runtime::Handle;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate, Times};

/// Environment variable name for the BigQuery project id.
const BIGQUERY_PROJECT_ID_ENV_NAME: &str = "TESTS_BIGQUERY_PROJECT_ID";
/// Environment variable name for the BigQuery service account key path.
const BIGQUERY_SA_KEY_PATH_ENV_NAME: &str = "TESTS_BIGQUERY_SA_KEY_PATH";
/// Project ID that is used for the local emulated BigQuery.
const TEST_PROJECT_ID: &str = "local-project";
/// Endpoint which the tests are mocking.
const AUTH_TOKEN_ENDPOINT: &str = "/:o/oauth2/token";
/// URL of the local instance of the BigQuery emulator HTTP server.
const V2_BASE_URL: &str = "http://localhost:9050";

/// Mock server for Google OAuth authentication endpoints used in BigQuery testing.
///
/// Provides a mock implementation of Google's OAuth token endpoint to enable
/// testing without requiring real Google Cloud credentials.
pub struct GoogleAuthMock {
    server: MockServer,
}

impl GoogleAuthMock {
    /// Creates and starts a new mock Google OAuth server.
    pub async fn start() -> Self {
        Self {
            server: MockServer::start().await,
        }
    }

    /// Configures the mock server to respond to OAuth token requests.
    ///
    /// Sets up the server to return fake OAuth tokens for the specified number
    /// of requests to the token endpoint.
    pub async fn mock_token<T: Into<Times>>(&self, n_times: T) {
        let response = ResponseTemplate::new(200).set_body_json(Token::fake());

        Mock::given(method("POST"))
            .and(path(AUTH_TOKEN_ENDPOINT))
            .respond_with(response)
            .named("mock token")
            .expect(n_times)
            .mount(self)
            .await;
    }
}

impl Deref for GoogleAuthMock {
    type Target = MockServer;

    /// Provides access to the underlying mock server for additional configuration.
    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

/// OAuth access token structure used for mocking Google Cloud authentication.
///
/// Represents the JSON response returned by Google's OAuth token endpoint.
#[derive(Eq, PartialEq, Serialize, Debug, Clone)]
pub struct Token {
    access_token: String,
    token_type: String,
    expires_in: u32,
}

impl Token {
    /// Creates a fake OAuth token for testing purposes.
    ///
    /// Returns a [`Token`] with dummy values suitable for mocking authentication
    /// in test scenarios.
    fn fake() -> Self {
        Self {
            access_token: "aaaa".to_string(),
            token_type: "bearer".to_string(),
            expires_in: 9999999,
        }
    }
}

/// Generates a mock Google Cloud service account key JSON.
///
/// Creates a fake service account key with dummy credentials and configures
/// OAuth endpoints to point to the provided mock server URL.
fn mock_sa_key(oauth_server: &str) -> serde_json::Value {
    let oauth_endpoint = format!("{oauth_server}/:o/oauth2");

    serde_json::json!({
      "type": "service_account",
      "project_id": "dummy",
      "private_key_id": "dummy",
      "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
      "client_email": "dummy@developer.gserviceaccount.com",
      "client_id": "dummy",
      "auth_uri": format!("{oauth_endpoint}/auth"),
      "token_uri": format!("{}{}", oauth_server, AUTH_TOKEN_ENDPOINT),
      "auth_provider_x509_cert_url": format!("{oauth_endpoint}/v1/certs"),
      "client_x509_cert_url": format!("{oauth_server}/robot/v1/metadata/x509/457015483506-compute%40developer.gserviceaccount.com")
    })
}

/// Generates a unique dataset ID for test isolation.
///
/// Creates a random dataset name prefixed with "etl_tests_" to ensure
/// each test run uses a fresh dataset and avoid conflicts.
fn random_dataset_id() -> String {
    let uuid = Uuid::new_v4().simple().to_string();

    format!("etl_tests_{uuid}")
}

/// BigQuery database connection for testing, supporting both emulated and real instances.
///
/// Provides a unified interface for BigQuery operations in tests, automatically
/// handling setup and teardown of test datasets. Can connect to either a local
/// BigQuery emulator or a real Google Cloud BigQuery instance based on environment
/// variable configuration.
pub enum BigQueryDatabase {
    /// Emulated BigQuery using local mock servers and fake credentials.
    Emulated {
        client: Option<Client>,
        google_auth: GoogleAuthMock,
        sa_key: String,
        project_id: String,
        dataset_id: String,
    },
    /// Real BigQuery instance using actual Google Cloud credentials.
    Real {
        client: Option<Client>,
        sa_key_path: String,
        project_id: String,
        dataset_id: String,
    },
}

impl BigQueryDatabase {
    /// Creates a new emulated BigQuery database instance.
    ///
    /// Sets up a [`BigQueryDatabase`] that connects to a local BigQuery emulator
    /// using the provided mock authentication server and service account key.
    async fn new_emulated(google_auth: GoogleAuthMock, sa_key: String) -> Self {
        let parsed_sa_key = parse_service_account_key(&sa_key).unwrap();

        let project_id = TEST_PROJECT_ID.to_owned();
        let dataset_id = random_dataset_id();

        let client = ClientBuilder::new()
            .with_auth_base_url(google_auth.uri())
            .with_v2_base_url(V2_BASE_URL.to_owned())
            .build_from_service_account_key(parsed_sa_key, false)
            .await
            .unwrap();

        initialize_bigquery(&client, &project_id, &dataset_id).await;

        Self::Emulated {
            client: Some(client),
            google_auth,
            sa_key,
            project_id,
            dataset_id,
        }
    }

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

        Self::Real {
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
        match self {
            Self::Emulated {
                client: _,
                google_auth,
                sa_key,
                project_id,
                dataset_id,
            } => BigQueryDestination::new_with_urls(
                project_id.clone(),
                dataset_id.clone(),
                google_auth.uri(),
                V2_BASE_URL.to_owned(),
                sa_key,
                // We set a `max_staleness_mins` to 0 since we want the changes to be applied at
                // query time.
                Some(0),
            )
            .await
            .unwrap(),
            Self::Real {
                client: _,
                sa_key_path,
                project_id,
                dataset_id,
            } => BigQueryDestination::new_with_key_path(
                project_id.clone(),
                dataset_id.clone(),
                sa_key_path,
                // We set a `max_staleness_mins` to 0 since we want the changes to be applied at
                // query time.
                Some(0),
            )
            .await
            .unwrap(),
        }
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
        match self {
            Self::Emulated { project_id, .. } => project_id,
            Self::Real { project_id, .. } => project_id,
        }
    }

    /// Returns the BigQuery dataset ID for this database instance.
    fn dataset_id(&self) -> &str {
        match self {
            Self::Emulated { dataset_id, .. } => dataset_id,
            Self::Real { dataset_id, .. } => dataset_id,
        }
    }

    /// Returns a reference to the BigQuery client, if available.
    fn client(&self) -> Option<&Client> {
        match self {
            Self::Emulated { client, .. } => client.as_ref(),
            Self::Real { client, .. } => client.as_ref(),
        }
    }

    /// Takes ownership of the BigQuery client, leaving [`None`] in its place.
    ///
    /// Used during cleanup to move the client out for async dataset deletion.
    fn take_client(&mut self) -> Option<Client> {
        match self {
            Self::Emulated { client, .. } => client.take(),
            Self::Real { client, .. } => client.take(),
        }
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
/// Automatically detects whether to use real Google Cloud BigQuery or the local
/// emulator based on environment variables. If `TESTS_BIGQUERY_SA_KEY_PATH` is
/// set, connects to real BigQuery; otherwise uses the emulated version.
///
/// Creates a fresh dataset for test isolation and configures mock authentication
/// when using the emulator.
pub async fn setup_bigquery_connection() -> BigQueryDatabase {
    let sa_key_path = std::env::var(BIGQUERY_SA_KEY_PATH_ENV_NAME);

    match sa_key_path {
        Ok(sa_key_path) => BigQueryDatabase::new_real(sa_key_path).await,
        Err(_) => {
            let google_auth = GoogleAuthMock::start().await;
            google_auth.mock_token(1).await;

            let sa_key = serde_json::to_string_pretty(&mock_sa_key(&google_auth.uri())).unwrap();

            BigQueryDatabase::new_emulated(google_auth, sa_key).await
        }
    }
}
