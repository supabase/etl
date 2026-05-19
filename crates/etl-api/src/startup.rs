use std::{io, net::TcpListener, sync::Arc};

use aws_lc_rs::aead::{AES_256_GCM, RandomizedNonceKey};
use axum::{
    Extension, Router, middleware,
    routing::{get, post, put},
};
use base64::{Engine, prelude::BASE64_STANDARD};
use etl_config::{
    Environment,
    shared::{IntoConnectOptions, PgConnectionConfig},
};
use etl_telemetry::metrics::init_metrics_handle;
use kube::config::KubeConfigOptions;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    authentication::auth_validator,
    config::ApiConfig,
    configs::encryption,
    data::publications::Publication,
    feature_flags::{FeatureFlagsClient, init_feature_flags},
    k8s::{K8sClient, K8sError, TrustedRootCertsCache, http::HttpK8sClient},
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
            stop_pipeline, update_pipeline, update_pipeline_version, validate_pipeline,
        },
        sources::{
            CreateSourceRequest, CreateSourceResponse, ReadSourceResponse, ReadSourcesResponse,
            UpdateSourceRequest, ValidateSourceRequest, ValidateSourceResponse, create_source,
            delete_source,
            publications::{
                CreatePublicationRequest, UpdatePublicationRequest, create_publication,
                delete_publication, read_all_publications, read_publication, update_publication,
            },
            read_all_sources, read_source,
            tables::read_table_names,
            update_source, validate_source,
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
    sentry_scrubbing::mark_sensitive_sentry_scope,
    span_builder,
};

/// Running API server task.
pub type Server = tokio::task::JoinHandle<io::Result<()>>;

/// ETL API application server wrapper.
///
/// Manages the HTTP server lifecycle including startup, migration, and
/// shutdown.
pub struct Application {
    port: u16,
    server: Server,
}

impl Application {
    /// Builds and configures the API application server.
    ///
    /// Sets up database connections, encryption, Kubernetes client, and HTTP
    /// server with all routes and middleware configured.
    pub async fn build(config: ApiConfig) -> anyhow::Result<Self> {
        let connection_pool = get_connection_pool(&config.database);

        let address = format!("{}:{}", config.application.host, config.application.port);
        let listener = TcpListener::bind(address)?;
        let port = listener.local_addr()?.port();

        let key_bytes = BASE64_STANDARD.decode(&config.encryption_key.key)?;
        let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)?;
        let encryption_key = encryption::EncryptionKey { id: config.encryption_key.id, key };

        // Try to create Kubernetes client, but continue without it if unavailable
        let kube_client_result = match Environment::load() {
            Ok(Environment::Staging | Environment::Prod) => kube::Client::try_default().await.ok(),
            Ok(Environment::Dev) => {
                async {
                    let options = KubeConfigOptions {
                        context: Some("orbstack".to_owned()),
                        cluster: Some("orbstack".to_owned()),
                        user: Some("orbstack".to_owned()),
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

        let feature_flags_client = init_feature_flags(config.configcat_sdk_key.as_deref())?;

        let k8s_client = match kube_client_result {
            Some(client) => match HttpK8sClient::new(client, config.k8s.clone()) {
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

        let trusted_root_certs_cache = k8s_client.clone().map(TrustedRootCertsCache::new);

        let server = run(
            config,
            listener,
            connection_pool,
            encryption_key,
            k8s_client,
            trusted_root_certs_cache,
            feature_flags_client,
        )?;

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
    pub async fn run_until_stopped(self) -> io::Result<()> {
        self.server.await.map_err(io::Error::other)?
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
                "failed to connect to orbstack, ensure orbstack is installed and kubernetes is \
                 enabled"
            );
            return Err(e.into());
        }
    }

    Ok(())
}

/// Creates a Postgres connection pool from the provided configuration.
///
/// Connects to the API's own metadata database using server defaults (no custom
/// options).
pub fn get_connection_pool(config: &PgConnectionConfig) -> PgPool {
    PgPoolOptions::new().connect_lazy_with(config.with_db(None))
}

/// Creates and configures the HTTP server with all routes and middleware.
///
/// Sets up authentication, tracing, Swagger UI, and all API endpoints.
/// The Kubernetes client and trusted root certs cache are optional to support
/// testing scenarios.
pub fn run(
    config: ApiConfig,
    listener: TcpListener,
    connection_pool: PgPool,
    encryption_key: encryption::EncryptionKey,
    k8s_client: Option<Arc<dyn K8sClient>>,
    trusted_root_certs_cache: Option<TrustedRootCertsCache>,
    feature_flags_client: Option<FeatureFlagsClient>,
) -> Result<Server, anyhow::Error> {
    let prometheus_handle = init_metrics_handle()?;
    let config = Arc::new(config);
    let encryption_key = Arc::new(encryption_key);
    let trusted_root_certs_cache = trusted_root_certs_cache.map(Arc::new);

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
            ValidateSourceRequest,
            ValidateSourceResponse,
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
        crate::routes::sources::validate_source,
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

    let sensitive_routes = Router::new()
        // sources
        .route("/sources", post(create_source).get(read_all_sources))
        .route("/sources/validate", post(validate_source))
        .route("/sources/{source_id}", get(read_source).post(update_source).delete(delete_source))
        .route("/sources/{source_id}/tables", get(read_table_names))
        // publications
        .route(
            "/sources/{source_id}/publications",
            post(create_publication).get(read_all_publications),
        )
        .route(
            "/sources/{source_id}/publications/{publication_name}",
            get(read_publication).post(update_publication).delete(delete_publication),
        )
        // destinations
        .route("/destinations", post(create_destination).get(read_all_destinations))
        .route("/destinations/validate", post(validate_destination))
        .route(
            "/destinations/{destination_id}",
            get(read_destination).post(update_destination).delete(delete_destination),
        )
        // pipelines
        .route("/pipelines", post(create_pipeline).get(read_all_pipelines))
        .route("/pipelines/validate", post(validate_pipeline))
        .route("/pipelines/stop", post(stop_all_pipelines))
        .route(
            "/pipelines/{pipeline_id}",
            get(read_pipeline).post(update_pipeline).delete(delete_pipeline),
        )
        .route("/pipelines/{pipeline_id}/start", post(start_pipeline))
        .route("/pipelines/{pipeline_id}/stop", post(stop_pipeline))
        .route("/pipelines/{pipeline_id}/status", get(get_pipeline_status))
        .route(
            "/pipelines/{pipeline_id}/version",
            get(get_pipeline_version).post(update_pipeline_version),
        )
        .route("/pipelines/{pipeline_id}/replication-status", get(get_pipeline_replication_status))
        .route("/pipelines/{pipeline_id}/rollback-tables", post(rollback_tables))
        // tenants_sources
        .route("/tenants-sources", post(create_tenant_and_source))
        // destinations-pipelines
        .route("/destinations-pipelines", post(create_destination_and_pipeline))
        .route(
            "/destinations-pipelines/{destination_id}/{pipeline_id}",
            post(update_destination_and_pipeline).delete(delete_destination_and_pipeline),
        )
        .layer(middleware::from_fn(mark_sensitive_sentry_scope));

    let v1_routes = Router::new()
        // tenants
        .route("/tenants", post(create_tenant).get(read_all_tenants))
        .route(
            "/tenants/{tenant_id}",
            put(create_or_update_tenant).get(read_tenant).post(update_tenant).delete(delete_tenant),
        )
        // images
        .route("/images", post(create_image).get(read_all_images))
        .route("/images/{image_id}", get(read_image).post(update_image).delete(delete_image))
        .merge(sensitive_routes)
        .layer(middleware::from_fn_with_state(Arc::clone(&config), auth_validator));

    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(span_builder::make_span)
        .on_request(span_builder::on_request)
        .on_response(span_builder::on_response)
        .on_failure(span_builder::on_failure);
    let sentry_layer =
        ServiceBuilder::new()
            .layer(
                sentry::integrations::tower::NewSentryLayer::<axum::extract::Request>::new_from_top(
                ),
            )
            .layer(sentry::integrations::tower::SentryHttpLayer::new().enable_transaction());

    let app = Router::new()
        .route("/health_check", get(health_check))
        .route("/metrics", get(metrics))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi))
        .nest("/v1", v1_routes)
        .layer(Extension(prometheus_handle))
        .layer(Extension(Arc::clone(&config)))
        .layer(Extension(connection_pool))
        .layer(Extension(Arc::clone(&encryption_key)))
        .layer(sentry_layer)
        .layer(trace_layer);

    let app =
        if let Some(k8s_client) = k8s_client { app.layer(Extension(k8s_client)) } else { app };

    let app = if let Some(trusted_root_certs_cache) = trusted_root_certs_cache {
        app.layer(Extension(trusted_root_certs_cache))
    } else {
        app
    };

    let app = if let Some(feature_flags_client) = feature_flags_client {
        app.layer(Extension(feature_flags_client))
    } else {
        app
    };

    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let server = tokio::spawn(async move { axum::serve(listener, app.into_make_service()).await });

    Ok(server)
}
