use std::{net::TcpListener, sync::Arc};

use actix_web::{App, HttpServer, dev::Server, web};
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_metrics::ActixWebMetricsBuilder;
use aws_lc_rs::aead::{AES_256_GCM, RandomizedNonceKey};
use base64::{Engine, prelude::BASE64_STANDARD};
use etl_config::Environment;
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use etl_telemetry::metrics::init_metrics_handle;
use kube::config::KubeConfigOptions;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::{error, info, warn};
use tracing_actix_web::TracingLogger;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::feature_flags::init_feature_flags;
use crate::k8s::http::HttpK8sClient;
use crate::k8s::{K8sClient, K8sError, TrustedRootCertsCache};
use crate::{
    authentication::auth_validator,
    config::ApiConfig,
    configs::encryption,
    db::publications::Publication,
    routes::{
        destinations::{
            CreateDestinationRequest, CreateDestinationResponse, ReadDestinationResponse,
            ReadDestinationsResponse, UpdateDestinationRequest, ValidateDestinationRequest,
            ValidateDestinationResponse, create_destination, delete_destination,
            read_all_destinations, read_destination, update_destination, validate_destination,
        },
        destinations_pipelines::{
            CreateDestinationPipelineRequest, CreateDestinationPipelineResponse,
            UpdateDestinationPipelineRequest, create_destination_and_pipeline,
            delete_destination_and_pipeline, update_destination_and_pipeline,
        },
        health_check::health_check,
        images::{
            CreateImageRequest, CreateImageResponse, ReadImageResponse, ReadImagesResponse,
            UpdateImageRequest, create_image, delete_image, read_all_images, read_image,
            update_image,
        },
        metrics::metrics,
        pipelines::{
            CreatePipelineRequest, CreatePipelineResponse, GetPipelineReplicationStatusResponse,
            GetPipelineStatusResponse, GetPipelineVersionResponse, ReadPipelineResponse,
            ReadPipelinesResponse, SimpleTableReplicationState, TableReplicationStatus,
            UpdatePipelineRequest, UpdatePipelineVersionRequest, ValidatePipelineRequest,
            ValidatePipelineResponse, create_pipeline, delete_pipeline,
            get_pipeline_replication_status, get_pipeline_status, get_pipeline_version,
            read_all_pipelines, read_pipeline, rollback_tables, start_pipeline, stop_all_pipelines,
            stop_pipeline, update_pipeline, update_pipeline_config, update_pipeline_version,
            validate_pipeline,
        },
        sources::{
            CreateSourceRequest, CreateSourceResponse, ReadSourceResponse, ReadSourcesResponse,
            UpdateSourceRequest, create_source, delete_source,
            publications::{
                CreatePublicationRequest, UpdatePublicationRequest, create_publication,
                delete_publication, read_all_publications, read_publication, update_publication,
            },
            read_all_sources, read_source,
            tables::read_table_names,
            update_source,
        },
        tenants::{
            CreateOrUpdateTenantRequest, CreateOrUpdateTenantResponse, CreateTenantRequest,
            CreateTenantResponse, ReadTenantResponse, ReadTenantsResponse, UpdateTenantRequest,
            create_or_update_tenant, create_tenant, delete_tenant, read_all_tenants, read_tenant,
            update_tenant,
        },
        tenants_sources::{
            CreateTenantSourceRequest, CreateTenantSourceResponse, create_tenant_and_source,
        },
    },
    span_builder::ApiRootSpanBuilder,
};

/// ETL API application server wrapper.
///
/// Manages the HTTP server lifecycle including startup, migration, and shutdown.
pub struct Application {
    port: u16,
    server: Server,
}

impl Application {
    /// Builds and configures the API application server.
    ///
    /// Sets up database connections, encryption, Kubernetes client, and HTTP server
    /// with all routes and middleware configured.
    pub async fn build(config: ApiConfig) -> anyhow::Result<Self> {
        let connection_pool = get_connection_pool(&config.database);

        let address = format!("{}:{}", config.application.host, config.application.port);
        let listener = TcpListener::bind(address)?;
        let port = listener.local_addr()?.port();

        let key_bytes = BASE64_STANDARD.decode(&config.encryption_key.key)?;
        let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)?;
        let encryption_key = encryption::EncryptionKey {
            id: config.encryption_key.id,
            key,
        };

        // Try to create Kubernetes client, but continue without it if unavailable
        let kube_client_result = match Environment::load() {
            Ok(Environment::Staging) | Ok(Environment::Prod) => {
                kube::Client::try_default().await.ok()
            }
            Ok(Environment::Dev) => {
                async {
                    let options = KubeConfigOptions {
                        context: Some("orbstack".to_string()),
                        cluster: Some("orbstack".to_string()),
                        user: Some("orbstack".to_string()),
                    };
                    let kube_config = kube::config::Config::from_kubeconfig(&options).await.ok()?;
                    let kube_client: kube::Client = kube_config.try_into().ok()?;
                    test_orbstack_connection(&kube_client).await.ok()?;
                    Some(kube_client)
                }
                .await
            }
            Err(_) => None,
        };

        let k8s_client = match kube_client_result {
            Some(client) => match HttpK8sClient::new(client).await {
                Ok(client) => Some(Arc::new(client) as Arc<dyn K8sClient>),
                Err(e) => {
                    warn!(
                        error = %e,
                        "failed to create kubernetes client, running without kubernetes support"
                    );
                    None
                }
            },
            None => {
                warn!("kubernetes client unavailable, running without kubernetes support");
                None
            }
        };

        let feature_flags_client = init_feature_flags(config.configcat_sdk_key.as_deref())?;

        let trusted_root_certs_cache = k8s_client.clone().map(TrustedRootCertsCache::new);

        let server = run(
            config,
            listener,
            connection_pool,
            encryption_key,
            k8s_client,
            trusted_root_certs_cache,
            feature_flags_client,
        )
        .await?;

        Ok(Self { port, server })
    }

    /// Runs database migrations using the provided configuration.
    ///
    /// Applies all pending SQLx migrations from the migrations directory.
    pub async fn migrate_database(config: PgConnectionConfig) -> Result<(), anyhow::Error> {
        let connection_pool = get_connection_pool(&config);

        sqlx::migrate!("./migrations").run(&connection_pool).await?;

        Ok(())
    }

    /// Returns the port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Runs the server until it receives a shutdown signal.
    pub async fn run_until_stopped(self) -> Result<(), std::io::Error> {
        self.server.await
    }
}

async fn test_orbstack_connection(client: &kube::Client) -> Result<(), K8sError> {
    match client.apiserver_version().await {
        Ok(version) => {
            info!(
                major = %version.major,
                minor = %version.minor,
                "connected to orbstack kubernetes api server"
            );
        }
        Err(e) => {
            error!(
                "failed to connect to orbstack, ensure orbstack is installed and kubernetes is enabled"
            );
            return Err(e.into());
        }
    }

    Ok(())
}

/// Creates a Postgres connection pool from the provided configuration.
///
/// Connects to the API's own metadata database using server defaults (no custom options).
pub fn get_connection_pool(config: &PgConnectionConfig) -> PgPool {
    PgPoolOptions::new().connect_lazy_with(config.with_db(None))
}

/// Creates and configures the HTTP server with all routes and middleware.
///
/// Sets up authentication, tracing, Swagger UI, and all API endpoints.
/// The Kubernetes client and trusted root certs cache are optional to support testing scenarios.
pub async fn run(
    config: ApiConfig,
    listener: TcpListener,
    connection_pool: PgPool,
    encryption_key: encryption::EncryptionKey,
    k8s_client: Option<Arc<dyn K8sClient>>,
    trusted_root_certs_cache: Option<TrustedRootCertsCache>,
    feature_flags_client: Option<configcat::Client>,
) -> Result<Server, anyhow::Error> {
    let prometheus_handle = web::ThinData(init_metrics_handle()?);
    let config = web::Data::new(config);
    let connection_pool = web::Data::new(connection_pool);
    let encryption_key = web::Data::new(encryption_key);
    let k8s_client: Option<web::Data<dyn K8sClient>> = k8s_client.map(Into::into);
    let trusted_root_certs_cache = trusted_root_certs_cache.map(web::Data::new);
    let feature_flags_client = feature_flags_client.map(web::Data::new);

    #[derive(OpenApi)]
    #[openapi(
        paths(
            crate::routes::health_check::health_check,
            crate::routes::metrics::metrics,
        ),
        components(schemas(
            CreateImageRequest,
            CreateImageResponse,
            UpdateImageRequest,
            ReadImageResponse,
            ReadImagesResponse,
            CreatePipelineRequest,
            CreatePipelineResponse,
            UpdatePipelineRequest,
            ReadPipelineResponse,
            ReadPipelinesResponse,
            GetPipelineVersionResponse,
            UpdatePipelineVersionRequest,
            GetPipelineStatusResponse,
            GetPipelineReplicationStatusResponse,
            TableReplicationStatus,
            SimpleTableReplicationState,
            CreateTenantRequest,
            CreateTenantResponse,
            CreateOrUpdateTenantRequest,
            CreateOrUpdateTenantResponse,
            UpdateTenantRequest,
            ReadTenantResponse,
            ReadTenantsResponse,
            CreateSourceRequest,
            CreateSourceResponse,
            UpdateSourceRequest,
            ReadSourceResponse,
            ReadSourcesResponse,
            CreatePublicationRequest,
            UpdatePublicationRequest,
            Publication,
            CreateDestinationRequest,
            CreateDestinationResponse,
            UpdateDestinationRequest,
            ReadDestinationResponse,
            ReadDestinationsResponse,
            CreateTenantSourceRequest,
            CreateTenantSourceResponse,
            CreateDestinationPipelineRequest,
            CreateDestinationPipelineResponse,
            UpdateDestinationPipelineRequest,
            ValidateDestinationRequest,
            ValidateDestinationResponse,
            ValidatePipelineRequest,
            ValidatePipelineResponse,
        )),
        nest(
            (path = "/v1", api = ApiV1)
        )
    )]
    struct ApiDoc;

    #[derive(OpenApi)]
    #[openapi(paths(
        crate::routes::images::create_image,
        crate::routes::images::read_image,
        crate::routes::images::update_image,
        crate::routes::images::delete_image,
        crate::routes::images::read_all_images,
        crate::routes::pipelines::create_pipeline,
        crate::routes::pipelines::read_pipeline,
        crate::routes::pipelines::update_pipeline,
        crate::routes::pipelines::delete_pipeline,
        crate::routes::pipelines::read_all_pipelines,
        crate::routes::pipelines::get_pipeline_status,
        crate::routes::pipelines::get_pipeline_version,
        crate::routes::pipelines::get_pipeline_replication_status,
        crate::routes::pipelines::update_pipeline_version,
        crate::routes::tenants::create_tenant,
        crate::routes::tenants::create_or_update_tenant,
        crate::routes::tenants::read_tenant,
        crate::routes::tenants::update_tenant,
        crate::routes::tenants::delete_tenant,
        crate::routes::tenants::read_all_tenants,
        crate::routes::sources::create_source,
        crate::routes::sources::read_source,
        crate::routes::sources::update_source,
        crate::routes::sources::delete_source,
        crate::routes::sources::read_all_sources,
        crate::routes::sources::publications::create_publication,
        crate::routes::sources::publications::read_publication,
        crate::routes::sources::publications::update_publication,
        crate::routes::sources::publications::delete_publication,
        crate::routes::sources::publications::read_all_publications,
        crate::routes::sources::tables::read_table_names,
        crate::routes::destinations::create_destination,
        crate::routes::destinations::read_destination,
        crate::routes::destinations::update_destination,
        crate::routes::destinations::delete_destination,
        crate::routes::destinations::read_all_destinations,
        crate::routes::destinations::validate_destination,
        crate::routes::tenants_sources::create_tenant_and_source,
        crate::routes::destinations_pipelines::create_destination_and_pipeline,
        crate::routes::destinations_pipelines::update_destination_and_pipeline,
        crate::routes::destinations_pipelines::delete_destination_and_pipeline,
        crate::routes::pipelines::validate_pipeline,
    ))]
    struct ApiV1;

    let openapi = ApiDoc::openapi();

    let server = HttpServer::new(move || {
        let actix_metrics = ActixWebMetricsBuilder::new().build();
        let tracing_logger = TracingLogger::<ApiRootSpanBuilder>::new();
        let authentication = HttpAuthentication::bearer(auth_validator);
        let app = App::new()
            .wrap(actix_metrics.clone())
            .wrap(tracing_logger)
            .wrap(
                sentry::integrations::actix::Sentry::builder()
                    .capture_server_errors(true)
                    .start_transaction(true)
                    .finish(),
            )
            .service(health_check)
            .service(metrics)
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-docs/openapi.json", openapi.clone()),
            )
            .service(
                web::scope("v1")
                    .wrap(authentication)
                    // tenants
                    .service(create_tenant)
                    .service(create_or_update_tenant)
                    .service(read_tenant)
                    .service(update_tenant)
                    .service(delete_tenant)
                    .service(read_all_tenants)
                    // sources
                    .service(create_source)
                    .service(read_source)
                    .service(update_source)
                    .service(delete_source)
                    .service(read_all_sources)
                    //destinations
                    .service(validate_destination)
                    .service(create_destination)
                    .service(read_destination)
                    .service(update_destination)
                    .service(delete_destination)
                    .service(read_all_destinations)
                    // pipelines
                    .service(validate_pipeline)
                    .service(create_pipeline)
                    .service(read_pipeline)
                    .service(update_pipeline)
                    .service(delete_pipeline)
                    .service(read_all_pipelines)
                    .service(start_pipeline)
                    .service(stop_pipeline)
                    .service(stop_all_pipelines)
                    .service(get_pipeline_status)
                    .service(get_pipeline_version)
                    .service(get_pipeline_replication_status)
                    .service(rollback_tables)
                    .service(update_pipeline_version)
                    .service(update_pipeline_config)
                    // tables
                    .service(read_table_names)
                    // publications
                    .service(create_publication)
                    .service(read_publication)
                    .service(update_publication)
                    .service(delete_publication)
                    .service(read_all_publications)
                    // images
                    .service(create_image)
                    .service(read_image)
                    .service(update_image)
                    .service(delete_image)
                    .service(read_all_images)
                    // tenants_sources
                    .service(create_tenant_and_source)
                    // destinations-pipelines
                    .service(create_destination_and_pipeline)
                    .service(update_destination_and_pipeline)
                    .service(delete_destination_and_pipeline),
            )
            .app_data(prometheus_handle.clone())
            .app_data(config.clone())
            .app_data(connection_pool.clone())
            .app_data(encryption_key.clone());

        let app = if let Some(k8s_client) = k8s_client.clone() {
            app.app_data(k8s_client)
        } else {
            app
        };

        let app = if let Some(trusted_root_certs_cache) = trusted_root_certs_cache.clone() {
            app.app_data(trusted_root_certs_cache)
        } else {
            app
        };

        if let Some(feature_flags_client) = feature_flags_client.clone() {
            app.app_data(feature_flags_client)
        } else {
            app
        }
    })
    .listen(listener)?
    .run();

    Ok(server)
}
