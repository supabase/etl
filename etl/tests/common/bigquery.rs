use etl::v2::destination::bigquery::BigQueryDestination;
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::model::dataset::Dataset;
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::Client;
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

pub struct GoogleAuthMock {
    server: MockServer,
}

impl GoogleAuthMock {
    pub async fn start() -> Self {
        Self {
            server: MockServer::start().await,
        }
    }

    /// Mock token, given how many times the endpoint will be called.
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

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

#[derive(Eq, PartialEq, Serialize, Debug, Clone)]
pub struct Token {
    access_token: String,
    token_type: String,
    expires_in: u32,
}

impl Token {
    fn fake() -> Self {
        Self {
            access_token: "aaaa".to_string(),
            token_type: "bearer".to_string(),
            expires_in: 9999999,
        }
    }
}

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

fn random_dataset_id() -> String {
    let uuid = Uuid::new_v4().simple().to_string();

    format!("etl_tests_{}", uuid)
}

pub enum BigQueryDatabase {
    Emulated {
        client: Option<Client>,
        google_auth: GoogleAuthMock,
        sa_key: String,
        project_id: String,
        dataset_id: String,
    },
    Real {
        client: Option<Client>,
        sa_key_path: String,
        project_id: String,
        dataset_id: String,
    },
}

impl BigQueryDatabase {
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

    async fn new_real(sa_key_path: String) -> Self {
        let project_id = std::env::var(BIGQUERY_PROJECT_ID_ENV_NAME).expect(&format!(
            "The env variable {} to be set to a project id",
            BIGQUERY_PROJECT_ID_ENV_NAME
        ));
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
                1,
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
                1,
            )
            .await
            .unwrap(),
        }
    }

    fn project_id(&self) -> &str {
        match self {
            Self::Emulated { project_id, .. } => project_id,
            Self::Real { project_id, .. } => project_id,
        }
    }

    fn dataset_id(&self) -> &str {
        match self {
            Self::Emulated { dataset_id, .. } => dataset_id,
            Self::Real { dataset_id, .. } => dataset_id,
        }
    }

    fn take_client(&mut self) -> Option<Client> {
        match self {
            Self::Emulated { client, .. } => client.take(),
            Self::Real { client, .. } => client.take(),
        }
    }
}

impl Drop for BigQueryDatabase {
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

async fn initialize_bigquery(client: &Client, project_id: &str, dataset_id: &str) {
    let dataset = Dataset::new(project_id, dataset_id);
    client.dataset().create(dataset).await.unwrap();
}

async fn destroy_bigquery(client: &Client, project_id: &str, dataset_id: &str) {
    client
        .dataset()
        .delete(project_id, dataset_id, true)
        .await
        .unwrap();
}

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
