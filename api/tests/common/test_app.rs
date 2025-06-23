use crate::common::database::create_etl_api_database;
use api::{
    config::ApiConfig,
    db::{pipelines::PipelineConfig, sources::SourceConfig},
    encryption::{self, generate_random_key},
    startup::run,
};
use config::load_config;
use config::shared::DestinationConfig;
use postgres::sqlx::config::PgConnectionConfig;
use postgres::sqlx::test_utils::drop_pg_database;
use reqwest::{IntoUrl, RequestBuilder};
use serde::{Deserialize, Serialize};
use std::io;
use std::net::TcpListener;
use tokio::runtime::Handle;
use uuid::Uuid;

#[derive(Serialize)]
pub struct CreateTenantRequest {
    pub id: String,
    pub name: String,
}

#[derive(Serialize)]
pub struct UpdateTenantRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct CreateTenantResponse {
    pub id: String,
}

#[derive(Deserialize)]
pub struct TenantResponse {
    pub id: String,
    pub name: String,
}

#[derive(Deserialize)]
pub struct TenantsResponse {
    pub tenants: Vec<TenantResponse>,
}

#[derive(Serialize)]
pub struct CreateSourceRequest {
    pub name: String,
    pub config: SourceConfig,
}

#[derive(Deserialize)]
pub struct CreateSourceResponse {
    pub id: i64,
}

#[derive(Serialize)]
pub struct UpdateSourceRequest {
    pub name: String,
    pub config: SourceConfig,
}

#[derive(Deserialize)]
pub struct SourceResponse {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: SourceConfig,
}

#[derive(Deserialize)]
pub struct SourcesResponse {
    pub sources: Vec<SourceResponse>,
}

#[derive(Serialize)]
pub struct CreateTenantSourceRequest {
    pub tenant_id: String,
    pub tenant_name: String,
    pub source_name: String,
    pub source_config: SourceConfig,
}

#[derive(Deserialize)]
pub struct CreateTenantSourceResponse {
    pub tenant_id: String,
    pub source_id: i64,
}

#[derive(Serialize)]
pub struct PostDestinationPipelineRequest {
    pub destination_name: String,
    pub destination_config: DestinationConfig,
    pub source_id: i64,
    pub pipeline_config: PipelineConfig,
}

#[derive(Deserialize)]
pub struct CreateDestinationPipelineResponse {
    pub destination_id: i64,
    pub pipeline_id: i64,
}

#[derive(Serialize)]
pub struct CreateDestinationRequest {
    pub name: String,
    pub config: DestinationConfig,
}

#[derive(Deserialize)]
pub struct CreateDestinationResponse {
    pub id: i64,
}

#[derive(Serialize)]
pub struct UpdateDestinationRequest {
    pub name: String,
    pub config: DestinationConfig,
}

#[derive(Deserialize)]
pub struct DestinationResponse {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: DestinationConfig,
}

#[derive(Deserialize)]
pub struct DestinationsResponse {
    pub destinations: Vec<DestinationResponse>,
}

#[derive(Serialize)]
pub struct CreatePipelineRequest {
    pub source_id: i64,
    pub destination_id: i64,
    pub config: PipelineConfig,
}

#[derive(Deserialize)]
pub struct CreatePipelineResponse {
    pub id: i64,
}

#[derive(Deserialize)]
pub struct PipelineResponse {
    pub id: i64,
    pub tenant_id: String,
    pub source_id: i64,
    pub destination_id: i64,
    pub replicator_id: i64,
    pub config: PipelineConfig,
}

#[derive(Deserialize)]
pub struct PipelinesResponse {
    pub pipelines: Vec<PipelineResponse>,
}

#[derive(Serialize)]
pub struct UpdatePipelineRequest {
    pub source_id: i64,
    pub destination_id: i64,
    pub config: PipelineConfig,
}

#[derive(Serialize)]
pub struct CreateImageRequest {
    pub name: String,
    pub is_default: bool,
}

#[derive(Deserialize)]
pub struct CreateImageResponse {
    pub id: i64,
}

#[derive(Deserialize)]
pub struct ImageResponse {
    pub id: i64,
    pub name: String,
    pub is_default: bool,
}

#[derive(Deserialize)]
pub struct ImagesResponse {
    pub images: Vec<ImageResponse>,
}

#[derive(Serialize)]
pub struct UpdateImageRequest {
    pub name: String,
    pub is_default: bool,
}

pub struct TestApp {
    pub address: String,
    pub api_client: reqwest::Client,
    pub api_key: String,
    config: PgConnectionConfig,
    server_handle: tokio::task::JoinHandle<io::Result<()>>,
}

impl Drop for TestApp {
    fn drop(&mut self) {
        // First, abort the server task to ensure it's terminated.
        self.server_handle.abort();

        // To use `block_in_place,` we need a multithreaded runtime since when a blocking
        // task is issued, the runtime will offload existing tasks to another worker.
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { drop_pg_database(&self.config).await });
        });
    }
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
        tenant: &UpdateTenantRequest,
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
        destination_pipeline: &PostDestinationPipelineRequest,
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
        destination_pipeline: &PostDestinationPipelineRequest,
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
}

pub async fn spawn_test_app() -> TestApp {
    let base_address = "127.0.0.1";
    let listener =
        TcpListener::bind(format!("{base_address}:0")).expect("failed to bind random port");
    let port = listener.local_addr().unwrap().port();

    let mut config = load_config::<ApiConfig>().expect("Failed to read configuration");
    // We use a random database name.
    config.database.name = Uuid::new_v4().to_string();

    let connection_pool = create_etl_api_database(&config.database).await;

    let key = generate_random_key::<32>().expect("failed to generate random key");
    let encryption_key = encryption::EncryptionKey { id: 0, key };
    let api_key = "XOUbHmWbt9h7nWl15wWwyWQnctmFGNjpawMc3lT5CFs=".to_string();

    let server = run(
        listener,
        connection_pool,
        encryption_key,
        api_key.clone(),
        None,
    )
    .await
    .expect("failed to bind address");

    let server_handle = tokio::spawn(server);

    TestApp {
        address: format!("http://{base_address}:{port}"),
        api_client: reqwest::Client::new(),
        api_key,
        config: config.database,
        server_handle,
    }
}
