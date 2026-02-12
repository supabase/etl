#![allow(dead_code)]

use aws_lc_rs::aead::{AES_256_GCM, RandomizedNonceKey};
use aws_lc_rs::rand::fill;
use base64::prelude::*;
use etl_api::config::{ApiConfig, ApplicationSettings, EncryptionKey as ConfigEncryptionKey};
use etl_api::k8s::{K8sClient, TrustedRootCertsCache};
use etl_api::routes::destinations::{CreateDestinationRequest, UpdateDestinationRequest};
use etl_api::routes::destinations_pipelines::{
    CreateDestinationPipelineRequest, UpdateDestinationPipelineRequest,
};
use etl_api::routes::images::{CreateImageRequest, UpdateImageRequest};
use etl_api::routes::pipelines::{
    CreatePipelineRequest, RollbackTablesRequest, UpdatePipelineRequest,
    UpdatePipelineVersionRequest,
};
use etl_api::routes::sources::{CreateSourceRequest, UpdateSourceRequest};
use etl_api::routes::tenants::{
    CreateOrUpdateTenantRequest, CreateTenantRequest, UpdateTenantRequest,
};
use etl_api::routes::tenants_sources::CreateTenantSourceRequest;
use etl_api::{configs::encryption, startup::run};
use etl_config::Environment;
use etl_config::shared::PgConnectionConfig;
use etl_postgres::sqlx::test_utils::drop_pg_database;
use reqwest::{IntoUrl, RequestBuilder};
use std::io;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::sleep;

use crate::support::database::{create_etl_api_database, get_test_db_config};
use crate::support::k8s_client::MockK8sClient;

pub struct TestApp {
    pub address: String,
    pub api_client: reqwest::Client,
    pub api_key: String,
    config: ApiConfig,
    server_handle: tokio::task::JoinHandle<io::Result<()>>,
}

impl TestApp {
    fn get_authenticated<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.api_client.get(url).bearer_auth(self.api_key.clone())
    }

    fn post_authenticated<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.api_client.post(url).bearer_auth(self.api_key.clone())
    }

    fn put_authenticated<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.api_client.put(url).bearer_auth(self.api_key.clone())
    }

    fn delete_authenticated<U: IntoUrl>(&self, url: U) -> RequestBuilder {
        self.api_client
            .delete(url)
            .bearer_auth(self.api_key.clone())
    }

    pub fn database_config(&self) -> &PgConnectionConfig {
        &self.config.database
    }

    pub async fn create_tenant(&self, tenant: &CreateTenantRequest) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/tenants", &self.address))
            .json(tenant)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn create_tenant_source(
        &self,
        tenant_source: &CreateTenantSourceRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/tenants-sources", &self.address))
            .json(tenant_source)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn create_or_update_tenant(
        &self,
        tenant_id: &str,
        tenant: &CreateOrUpdateTenantRequest,
    ) -> reqwest::Response {
        self.put_authenticated(format!("{}/v1/tenants/{tenant_id}", &self.address))
            .json(tenant)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_tenant(&self, tenant_id: &str) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/tenants/{tenant_id}", &self.address))
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_tenant(
        &self,
        tenant_id: &str,
        tenant: &UpdateTenantRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/tenants/{tenant_id}", &self.address))
            .json(tenant)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn delete_tenant(&self, tenant_id: &str) -> reqwest::Response {
        self.delete_authenticated(format!("{}/v1/tenants/{tenant_id}", &self.address))
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_all_tenants(&self) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/tenants", &self.address))
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn create_source(
        &self,
        tenant_id: &str,
        source: &CreateSourceRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/sources", &self.address))
            .header("tenant_id", tenant_id)
            .json(source)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_source(&self, tenant_id: &str, source_id: i64) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_source(
        &self,
        tenant_id: &str,
        source_id: i64,
        source: &UpdateSourceRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .json(source)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn delete_source(&self, tenant_id: &str, source_id: i64) -> reqwest::Response {
        self.delete_authenticated(format!("{}/v1/sources/{source_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_all_sources(&self, tenant_id: &str) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/sources", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn create_destination(
        &self,
        tenant_id: &str,
        destination: &CreateDestinationRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/destinations", &self.address))
            .header("tenant_id", tenant_id)
            .json(destination)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_destination(
        &self,
        tenant_id: &str,
        destination_id: i64,
    ) -> reqwest::Response {
        self.get_authenticated(format!(
            "{}/v1/destinations/{destination_id}",
            &self.address
        ))
        .header("tenant_id", tenant_id)
        .send()
        .await
        .expect("failed to execute request")
    }

    pub async fn update_destination(
        &self,
        tenant_id: &str,
        destination_id: i64,
        destination: &UpdateDestinationRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!(
            "{}/v1/destinations/{destination_id}",
            &self.address
        ))
        .header("tenant_id", tenant_id)
        .json(destination)
        .send()
        .await
        .expect("failed to execute request")
    }

    pub async fn delete_destination(
        &self,
        tenant_id: &str,
        destination_id: i64,
    ) -> reqwest::Response {
        self.delete_authenticated(format!(
            "{}/v1/destinations/{destination_id}",
            &self.address
        ))
        .header("tenant_id", tenant_id)
        .send()
        .await
        .expect("Failed to execute request.")
    }

    pub async fn read_all_destinations(&self, tenant_id: &str) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/destinations", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn create_pipeline(
        &self,
        tenant_id: &str,
        pipeline: &CreatePipelineRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/pipelines", &self.address))
            .header("tenant_id", tenant_id)
            .json(pipeline)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_pipeline(&self, tenant_id: &str, pipeline_id: i64) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/pipelines/{pipeline_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: i64,
        pipeline: &UpdatePipelineRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/pipelines/{pipeline_id}", &self.address))
            .header("tenant_id", tenant_id)
            .json(pipeline)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn delete_pipeline(&self, tenant_id: &str, pipeline_id: i64) -> reqwest::Response {
        self.delete_authenticated(format!("{}/v1/pipelines/{pipeline_id}", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn start_pipeline(&self, tenant_id: &str, pipeline_id: i64) -> reqwest::Response {
        self.post_authenticated(format!(
            "{}/v1/pipelines/{pipeline_id}/start",
            &self.address
        ))
        .header("tenant_id", tenant_id)
        .send()
        .await
        .expect("failed to execute request")
    }

    pub async fn stop_pipeline(&self, tenant_id: &str, pipeline_id: i64) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/pipelines/{pipeline_id}/stop", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn stop_all_pipelines(&self, tenant_id: &str) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/pipelines/stop", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn read_all_pipelines(&self, tenant_id: &str) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/pipelines", &self.address))
            .header("tenant_id", tenant_id)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn create_destination_pipeline(
        &self,
        tenant_id: &str,
        destination_pipeline: &CreateDestinationPipelineRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/destinations-pipelines", &self.address))
            .header("tenant_id", tenant_id)
            .json(destination_pipeline)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn update_destination_pipeline(
        &self,
        tenant_id: &str,
        destination_id: i64,
        pipeline_id: i64,
        destination_pipeline: &UpdateDestinationPipelineRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!(
            "{}/v1/destinations-pipelines/{destination_id}/{pipeline_id}",
            &self.address
        ))
        .header("tenant_id", tenant_id)
        .json(destination_pipeline)
        .send()
        .await
        .expect("Failed to execute request.")
    }

    pub async fn delete_destination_pipeline(
        &self,
        tenant_id: &str,
        destination_id: i64,
        pipeline_id: i64,
    ) -> reqwest::Response {
        self.delete_authenticated(format!(
            "{}/v1/destinations-pipelines/{destination_id}/{pipeline_id}",
            &self.address
        ))
        .header("tenant_id", tenant_id)
        .send()
        .await
        .expect("Failed to execute request.")
    }

    pub async fn create_image(&self, image: &CreateImageRequest) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/images", &self.address))
            .json(image)
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_image(&self, image_id: i64) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/images/{image_id}", &self.address))
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_image(
        &self,
        image_id: i64,
        image: &UpdateImageRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!("{}/v1/images/{image_id}", &self.address))
            .json(image)
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn delete_image(&self, image_id: i64) -> reqwest::Response {
        self.delete_authenticated(format!("{}/v1/images/{image_id}", &self.address))
            .send()
            .await
            .expect("Failed to execute request.")
    }

    pub async fn read_all_images(&self) -> reqwest::Response {
        self.get_authenticated(format!("{}/v1/images", &self.address))
            .send()
            .await
            .expect("failed to execute request")
    }

    pub async fn update_pipeline_version(
        &self,
        tenant_id: &str,
        pipeline_id: i64,
        update_request: &UpdatePipelineVersionRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!(
            "{}/v1/pipelines/{pipeline_id}/version",
            &self.address
        ))
        .header("tenant_id", tenant_id)
        .json(update_request)
        .send()
        .await
        .expect("Failed to execute request.")
    }

    pub async fn get_pipeline_replication_status(
        &self,
        tenant_id: &str,
        pipeline_id: i64,
    ) -> reqwest::Response {
        self.get_authenticated(format!(
            "{}/v1/pipelines/{}/replication-status",
            &self.address, pipeline_id
        ))
        .header("tenant_id", tenant_id)
        .send()
        .await
        .expect("failed to execute request")
    }

    pub async fn get_pipeline_version(
        &self,
        tenant_id: &str,
        pipeline_id: i64,
    ) -> reqwest::Response {
        self.get_authenticated(format!(
            "{}/v1/pipelines/{}/version",
            &self.address, pipeline_id
        ))
        .header("tenant_id", tenant_id)
        .send()
        .await
        .expect("failed to execute request")
    }

    pub async fn rollback_tables(
        &self,
        tenant_id: &str,
        pipeline_id: i64,
        rollback_request: &RollbackTablesRequest,
    ) -> reqwest::Response {
        self.post_authenticated(format!(
            "{}/v1/pipelines/{}/rollback-tables",
            &self.address, pipeline_id
        ))
        .header("tenant_id", tenant_id)
        .json(rollback_request)
        .send()
        .await
        .expect("failed to execute request")
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        // First, abort the server task to ensure it's terminated.
        self.server_handle.abort();

        // To use `block_in_place,` we need a multithreaded runtime since when a blocking
        // task is issued, the runtime will offload existing tasks to another worker.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| {
                Handle::current().block_on(async {
                    // Give server time to shut down gracefully.
                    sleep(Duration::from_millis(100)).await;

                    drop_pg_database(&self.config.database).await;
                });
            });
        }));
    }
}

fn generate_random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    fill(&mut bytes).expect("failed to generate random bytes");
    bytes
}

pub async fn spawn_test_app() -> TestApp {
    spawn_test_app_with_trusted_username(None).await
}

pub async fn spawn_test_app_with_trusted_username(
    trusted_source_username: Option<String>,
) -> TestApp {
    Environment::Dev.set();

    let base_address = "127.0.0.1";
    let listener =
        TcpListener::bind(format!("{base_address}:0")).expect("failed to bind random port");
    let port = listener.local_addr().unwrap().port();

    let database_config = get_test_db_config();
    let api_db_pool = create_etl_api_database(&database_config).await;

    // Generate encryption key bytes and create both the key and its base64 representation
    let key_bytes = generate_random_bytes::<32>();
    let key =
        RandomizedNonceKey::new(&AES_256_GCM, &key_bytes).expect("failed to create encryption key");
    let encryption_key = encryption::EncryptionKey { id: 0, key };

    // Generate a random API key for tests
    let api_key_bytes = generate_random_bytes::<32>();
    let api_key = BASE64_STANDARD.encode(api_key_bytes);

    let config = ApiConfig {
        database: database_config,
        application: ApplicationSettings {
            host: base_address.to_string(),
            port,
        },
        encryption_key: ConfigEncryptionKey {
            id: 0,
            key: BASE64_STANDARD.encode(key_bytes),
        },
        api_keys: vec![api_key.clone()],
        sentry: None,
        supabase_api_url: None,
        configcat_sdk_key: None,
        source_tls_enabled: false,
        trusted_source_username,
    };

    let k8s_client: Arc<dyn K8sClient> = Arc::new(MockK8sClient);
    let trusted_root_certs_cache = TrustedRootCertsCache::new(k8s_client.clone());

    let server = run(
        config.clone(),
        listener,
        api_db_pool,
        encryption_key,
        Some(k8s_client),
        Some(trusted_root_certs_cache),
        None,
    )
    .await
    .expect("failed to bind address");

    let server_handle = tokio::spawn(server);

    TestApp {
        address: format!("http://{base_address}:{port}"),
        api_client: reqwest::Client::new(),
        api_key,
        config,
        server_handle,
    }
}
