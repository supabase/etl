use std::{
    future::Future,
    io,
    net::{SocketAddr, TcpListener},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::Context;
use aws_lc_rs::aead::{AES_256_GCM, RandomizedNonceKey};
use axum::{
    Extension, Router, middleware,
    routing::{get, post, put},
    serve::Listener,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use etl_config::{
    Environment,
    shared::{IntoConnectOptions, PgConnectionConfig},
};
use etl_telemetry::metrics::init_metrics_handle;
use kube::config::KubeConfigOptions;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener as TokioTcpListener, TcpStream},
};
use tokio_rustls::{Accept, TlsAcceptor, server::TlsStream as ServerTlsStream};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    authentication::auth_validator,
    config::{ApiConfig, EncryptionKeyConfig, InternalTlsSettings},
    configs::encryption,
    data::{
        publications::Publication,
        v2::{
            publications::{
                PublicationConfig as V2PublicationConfig,
                PublicationConfigInput as V2PublicationConfigInput,
                PublicationDetails as V2PublicationDetails,
                PublicationGeneratedColumns as V2PublicationGeneratedColumns,
                PublicationOperation as V2PublicationOperation,
                PublicationSummary as V2PublicationSummary,
                PublicationTableConfig as V2PublicationTableConfig,
                PublicationTableConfigInput as V2PublicationTableConfigInput,
                PublicationTableSelection as V2PublicationTableSelection,
                PublicationTableSelectionInput as V2PublicationTableSelectionInput,
            },
            schemas::SourceSchema as V2SourceSchema,
            tables::{SourceTable as V2SourceTable, SourceTableKind as V2SourceTableKind},
        },
    },
    feature_flags::{FeatureFlagsClient, init_feature_flags},
    http_metrics::record_http_metrics,
    k8s::{K8sClient, K8sError, SourceTlsConfig, http::HttpK8sClient},
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
            ReadPipelinesResponse, SimpleTableState, TableStatus, UpdatePipelineRequest,
            UpdatePipelineVersionRequest, ValidatePipelineRequest, ValidatePipelineResponse,
            create_pipeline, delete_pipeline, get_pipeline_replication_status, get_pipeline_status,
            get_pipeline_version, read_all_pipelines, read_pipeline, restart_pipeline,
            rollback_tables, start_pipeline, stop_all_pipelines, stop_pipeline, update_pipeline,
            update_pipeline_version, validate_pipeline,
        },
        runtime_config::{resolve_runtime_config, resolve_tenant_runtime_config},
        sources::{
            CreateSourceRequest, CreateSourceResponse, ReadSourceResponse, ReadSourcesResponse,
            UpdateSourceRequest, ValidateSourceRequest, ValidateSourceResponse, create_source,
            delete_source,
            publications::{
                CreatePublicationRequest, UpdatePublicationRequest, add_tables_to_publication,
                create_publication, delete_publication, drop_tables_from_publication,
                read_all_publications, read_publication, set_publication_tables,
                update_publication,
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
        v2::{
            columns::{
                ReadColumnsResponse as V2ReadColumnsResponse, read_columns as read_columns_v2,
            },
            publications::{
                ReadPublicationsResponse as V2ReadPublicationsResponse,
                delete_publication as delete_publication_v2, put_publication as put_publication_v2,
                read_publication as read_publication_v2, read_publications as read_publications_v2,
            },
            schemas::{
                ReadSchemasResponse as V2ReadSchemasResponse, read_schemas as read_schemas_v2,
            },
            tables::{ReadTablesResponse as V2ReadTablesResponse, read_tables as read_tables_v2},
        },
    },
    sentry_scrubbing::{capture_server_errors, mark_sensitive_sentry_scope},
    span_builder,
};

/// Running API server task.
pub type Server = tokio::task::JoinHandle<io::Result<()>>;

/// Public and cluster-internal listeners used by the API application.
pub struct ApplicationListeners {
    /// Listener serving the public API surface.
    pub public: TcpListener,
    /// Listener serving cluster-internal routes.
    pub internal: TcpListener,
}

/// Listener that accepts internal TCP connections without waiting for their TLS
/// handshakes to complete.
struct TlsListener {
    /// TCP listener serving the internal API.
    listener: TokioTcpListener,
    /// TLS acceptor used to start each connection handshake.
    acceptor: TlsAcceptor,
}

impl Listener for TlsListener {
    type Io = LazyTlsStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, address) = match self.listener.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    error!(%error, "failed to accept internal API TCP connection");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            let stream = LazyTlsStream::new(self.acceptor.accept(stream), address);
            return (stream, address);
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

/// TLS connection that defers its handshake until Axum polls its I/O.
struct LazyTlsStream {
    /// Current handshake or streaming state.
    state: LazyTlsStreamState,
    /// Remote peer address used for handshake diagnostics.
    address: SocketAddr,
}

/// State of a lazily negotiated internal TLS connection.
enum LazyTlsStreamState {
    /// TLS handshake waiting to be polled by the connection task.
    Handshaking(Pin<Box<Accept<TcpStream>>>),
    /// Established TLS stream.
    Streaming(Box<ServerTlsStream<TcpStream>>),
    /// Terminal state after a failed handshake.
    Failed,
}

impl LazyTlsStream {
    /// Creates a stream that will negotiate TLS on its first I/O poll.
    fn new(handshake: Accept<TcpStream>, address: SocketAddr) -> Self {
        Self { state: LazyTlsStreamState::Handshaking(Box::pin(handshake)), address }
    }

    /// Polls the handshake and returns the established TLS stream when ready.
    fn poll_stream(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> Poll<io::Result<&mut ServerTlsStream<TcpStream>>> {
        if let LazyTlsStreamState::Handshaking(handshake) = &mut self.state {
            match handshake.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(stream)) => {
                    self.state = LazyTlsStreamState::Streaming(Box::new(stream));
                }
                Poll::Ready(Err(error)) => {
                    tracing::debug!(error = %error, address = %self.address, "rejected internal API TLS connection");
                    self.state = LazyTlsStreamState::Failed;
                    return Poll::Ready(Err(error));
                }
            }
        }

        match &mut self.state {
            LazyTlsStreamState::Streaming(stream) => Poll::Ready(Ok(stream.as_mut())),
            LazyTlsStreamState::Failed => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "TLS handshake failed",
            ))),
            LazyTlsStreamState::Handshaking(_) => unreachable!("pending handshake returned early"),
        }
    }
}

impl AsyncRead for LazyTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut().poll_stream(cx) {
            Poll::Ready(Ok(stream)) => Pin::new(stream).poll_read(cx, buffer),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for LazyTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut().poll_stream(cx) {
            Poll::Ready(Ok(stream)) => Pin::new(stream).poll_write(cx, buffer),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().poll_stream(cx) {
            Poll::Ready(Ok(stream)) => Pin::new(stream).poll_flush(cx),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().poll_stream(cx) {
            Poll::Ready(Ok(stream)) => Pin::new(stream).poll_shutdown(cx),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Builds the TLS acceptor used by the internal API listener.
fn internal_tls_acceptor(settings: &InternalTlsSettings) -> anyhow::Result<TlsAcceptor> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    let certificates = CertificateDer::pem_file_iter(&settings.cert_path)
        .with_context(|| {
            format!("Opening internal API TLS certificate {}", settings.cert_path.display())
        })?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| {
            format!("Reading internal API TLS certificate {}", settings.cert_path.display())
        })?;
    let private_key = PrivateKeyDer::from_pem_file(&settings.key_path).with_context(|| {
        format!("Reading internal API TLS private key {}", settings.key_path.display())
    })?;
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .context("Building internal API TLS server configuration")?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// Returns whether sensitive internal runtime routes may be exposed.
fn internal_runtime_routes_enabled(environment: Environment, tls_enabled: bool) -> bool {
    matches!(environment, Environment::Dev) || tls_enabled
}

/// Minimum number of connections for the API metadata database pool.
///
/// The API pool is lazy and should not keep metadata database connections open
/// while the server is idle.
const MIN_DATABASE_POOL_CONNECTIONS: u32 = 0;

/// ETL API application server wrapper.
///
/// Manages the HTTP server lifecycle including startup, migration, and
/// shutdown.
pub struct Application {
    port: u16,
    internal_port: u16,
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
        let internal_address =
            format!("{}:{}", config.application.host, config.application.internal_port);
        let internal_listener = TcpListener::bind(internal_address)?;
        let internal_port = internal_listener.local_addr()?.port();

        let encryption_keyring = build_encryption_keyring(&config)?;

        let feature_flags_client = init_feature_flags(config.configcat_sdk_key.as_deref())?;

        let kube_client = create_kubernetes_client().await.context(
            "Failed to initialize Kubernetes client. ETL API requires access to an active \
             Kubernetes cluster",
        )?;
        let k8s_client = HttpK8sClient::new(kube_client, config.k8s.clone())
            .context("Failed to configure Kubernetes resource client")?;
        k8s_client.preflight().await.context("Kubernetes prerequisite validation failed")?;
        let k8s_client = Arc::new(k8s_client) as Arc<dyn K8sClient>;

        let source_tls_config = SourceTlsConfig::new(config.source.tls.clone())
            .context("Resolving source TLS configuration")?;

        let server = run(
            config,
            ApplicationListeners { public: listener, internal: internal_listener },
            connection_pool,
            encryption_keyring,
            k8s_client,
            source_tls_config,
            feature_flags_client,
        )?;

        Ok(Self { port, internal_port, server })
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

    /// Returns the port the cluster-internal server is listening on.
    pub fn internal_port(&self) -> u16 {
        self.internal_port
    }

    /// Runs the server until it receives a shutdown signal.
    pub async fn run_until_stopped(self) -> io::Result<()> {
        self.server.await.map_err(io::Error::other)?
    }
}

async fn create_kubernetes_client() -> anyhow::Result<kube::Client> {
    match Environment::load().context("Failed to load application environment")? {
        Environment::Staging | Environment::Prod => Ok(kube::Client::try_default().await?),
        Environment::Dev => {
            let options = KubeConfigOptions {
                context: Some("orbstack".to_owned()),
                cluster: Some("orbstack".to_owned()),
                user: Some("orbstack".to_owned()),
            };
            let kube_config = kube::config::Config::from_kubeconfig(&options).await?;
            let kube_client = kube::Client::try_from(kube_config)?;
            test_orbstack_connection(&kube_client).await?;

            Ok(kube_client)
        }
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

/// Builds the encryption keyring from configured encryption keys.
pub fn build_encryption_keyring(
    config: &ApiConfig,
) -> Result<encryption::EncryptionKeyring, anyhow::Error> {
    let keys =
        config.encryption_keys.iter().map(decode_encryption_key).collect::<Result<Vec<_>, _>>()?;

    encryption::EncryptionKeyring::new(keys).map_err(Into::into)
}

/// Decodes one configured encryption key into runtime key material.
fn decode_encryption_key(
    key_config: &EncryptionKeyConfig,
) -> Result<encryption::EncryptionKey, anyhow::Error> {
    let key_bytes = BASE64_STANDARD
        .decode(&key_config.key)
        .with_context(|| format!("Decoding encryption key {}", key_config.id))?;

    if key_bytes.len() != AES_256_GCM.key_len() {
        anyhow::bail!(
            "Encryption key {} must decode to {} bytes, got {}",
            key_config.id,
            AES_256_GCM.key_len(),
            key_bytes.len()
        );
    }

    let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)
        .with_context(|| format!("Creating encryption key {}", key_config.id))?;

    Ok(encryption::EncryptionKey { id: key_config.id, key })
}

/// Creates a Postgres connection pool from the provided configuration.
///
/// Connects to the API's own metadata database using server defaults (no custom
/// options).
pub fn get_connection_pool(config: &PgConnectionConfig) -> PgPool {
    PgPoolOptions::new()
        .min_connections(MIN_DATABASE_POOL_CONNECTIONS)
        .connect_lazy_with(config.with_db(None))
}

/// Creates and configures the HTTP server with all routes and middleware.
///
/// Sets up authentication, tracing, Swagger UI, and all API endpoints. The
/// The Kubernetes client and source TLS configuration are fully initialized
/// before the server starts accepting requests.
pub fn run(
    config: ApiConfig,
    listeners: ApplicationListeners,
    connection_pool: PgPool,
    encryption_keyring: encryption::EncryptionKeyring,
    k8s_client: Arc<dyn K8sClient>,
    source_tls_config: SourceTlsConfig,
    feature_flags_client: Option<FeatureFlagsClient>,
) -> Result<Server, anyhow::Error> {
    let ApplicationListeners { public: listener, internal: internal_listener } = listeners;
    let internal_tls_acceptor =
        config.application.internal_tls.as_ref().map(internal_tls_acceptor).transpose()?;
    let internal_runtime_routes_enabled = internal_runtime_routes_enabled(
        Environment::load().context("Failed to load application environment")?,
        internal_tls_acceptor.is_some(),
    );
    let prometheus_handle = init_metrics_handle()?;
    let config = Arc::new(config);
    let encryption_keyring = Arc::new(encryption_keyring);
    let source_tls_config = Arc::new(source_tls_config);

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
            TableStatus,
            SimpleTableState,
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
            V2PublicationConfig,
            V2PublicationConfigInput,
            V2PublicationDetails,
            V2PublicationGeneratedColumns,
            V2PublicationOperation,
            V2PublicationSummary,
            V2PublicationTableConfig,
            V2PublicationTableConfigInput,
            V2PublicationTableSelection,
            V2PublicationTableSelectionInput,
            V2ReadColumnsResponse,
            V2ReadPublicationsResponse,
            V2ReadSchemasResponse,
            V2ReadTablesResponse,
            V2SourceSchema,
            V2SourceTable,
            V2SourceTableKind,
        )),
        nest(
            (path = "/v1", api = ApiV1),
            (path = "/v2", api = ApiV2)
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
        crate::routes::pipelines::restart_pipeline,
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
        crate::routes::sources::publications::add_tables_to_publication,
        crate::routes::sources::publications::drop_tables_from_publication,
        crate::routes::sources::publications::set_publication_tables,
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

    #[derive(OpenApi)]
    #[openapi(paths(
        crate::routes::v2::publications::read_publications,
        crate::routes::v2::publications::read_publication,
        crate::routes::v2::publications::put_publication,
        crate::routes::v2::publications::delete_publication,
        crate::routes::v2::schemas::read_schemas,
        crate::routes::v2::tables::read_tables,
        crate::routes::v2::columns::read_columns,
    ))]
    struct ApiV2;

    let openapi = ApiDoc::openapi();

    // Routes in this scope can carry source/destination credentials,
    // connection config, table/publication metadata, replication config, or
    // source-derived data. Keep new routes here when their request, response,
    // path/query values, validation errors, or Sentry extras may include secrets
    // or customer data. Leave only low-sensitivity metadata routes outside.
    let sensitive_routes = Router::new()
        .route("/sources", post(create_source).get(read_all_sources))
        .route("/sources/validate", post(validate_source))
        .route("/sources/{source_id}", get(read_source).post(update_source).delete(delete_source))
        .route("/sources/{source_id}/tables", get(read_table_names))
        .route(
            "/sources/{source_id}/publications",
            post(create_publication).get(read_all_publications),
        )
        .route(
            "/sources/{source_id}/publications/{publication_name}",
            get(read_publication).post(update_publication).delete(delete_publication),
        )
        .route(
            "/sources/{source_id}/publications/{publication_name}/tables",
            post(add_tables_to_publication)
                .put(set_publication_tables)
                .delete(drop_tables_from_publication),
        )
        .route("/destinations", post(create_destination).get(read_all_destinations))
        .route("/destinations/validate", post(validate_destination))
        .route(
            "/destinations/{destination_id}",
            get(read_destination).post(update_destination).delete(delete_destination),
        )
        .route("/pipelines", post(create_pipeline).get(read_all_pipelines))
        .route("/pipelines/validate", post(validate_pipeline))
        .route("/pipelines/stop", post(stop_all_pipelines))
        .route(
            "/pipelines/{pipeline_id}",
            get(read_pipeline).post(update_pipeline).delete(delete_pipeline),
        )
        .route("/pipelines/{pipeline_id}/start", post(start_pipeline))
        .route("/pipelines/{pipeline_id}/restart", post(restart_pipeline))
        .route("/pipelines/{pipeline_id}/stop", post(stop_pipeline))
        .route("/pipelines/{pipeline_id}/status", get(get_pipeline_status))
        .route(
            "/pipelines/{pipeline_id}/version",
            get(get_pipeline_version).post(update_pipeline_version),
        )
        .route("/pipelines/{pipeline_id}/replication-status", get(get_pipeline_replication_status))
        .route("/pipelines/{pipeline_id}/rollback-tables", post(rollback_tables))
        .route("/tenants-sources", post(create_tenant_and_source))
        .route("/destinations-pipelines", post(create_destination_and_pipeline))
        .route(
            "/destinations-pipelines/{destination_id}/{pipeline_id}",
            post(update_destination_and_pipeline).delete(delete_destination_and_pipeline),
        )
        .layer(middleware::from_fn(mark_sensitive_sentry_scope));

    let v1_routes = Router::new()
        .route("/tenants", post(create_tenant).get(read_all_tenants))
        .route(
            "/tenants/{tenant_id}",
            put(create_or_update_tenant).get(read_tenant).post(update_tenant).delete(delete_tenant),
        )
        .route("/images", post(create_image).get(read_all_images))
        .route("/images/{image_id}", get(read_image).post(update_image).delete(delete_image))
        .merge(sensitive_routes)
        .layer(middleware::from_fn_with_state(Arc::clone(&config), auth_validator));

    let v2_routes = Router::new()
        .route("/sources/{source_id}/publications", get(read_publications_v2))
        .route(
            "/sources/{source_id}/publications/{publication_name}",
            get(read_publication_v2).put(put_publication_v2).delete(delete_publication_v2),
        )
        .route("/sources/{source_id}/schemas", get(read_schemas_v2))
        .route("/sources/{source_id}/tables", get(read_tables_v2))
        .route("/sources/{source_id}/tables/{table_id}/columns", get(read_columns_v2))
        .layer(middleware::from_fn(mark_sensitive_sentry_scope))
        .layer(middleware::from_fn_with_state(Arc::clone(&config), auth_validator));

    let internal_routes = Router::new()
        .route(
            "/destinations/{destination_kind}/{destination_name}/config",
            get(resolve_tenant_runtime_config),
        )
        .route("/destinations/{destination_id}/config", get(resolve_runtime_config))
        .layer(middleware::from_fn(mark_sensitive_sentry_scope))
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
        .nest("/v2", v2_routes)
        .layer(Extension(prometheus_handle.clone()))
        .layer(Extension(Arc::clone(&config)))
        .layer(Extension(connection_pool.clone()))
        .layer(Extension(Arc::clone(&encryption_keyring)))
        .layer(middleware::from_fn(record_http_metrics))
        .layer(middleware::from_fn(capture_server_errors))
        .layer(sentry_layer.clone())
        .layer(trace_layer.clone());

    let app = app.layer(Extension(Arc::clone(&k8s_client)));

    let app = app.layer(Extension(Arc::clone(&source_tls_config)));

    let app = if let Some(feature_flags_client) = feature_flags_client {
        app.layer(Extension(feature_flags_client))
    } else {
        app
    };

    let internal_app = Router::new().route("/health_check", get(health_check));
    let internal_app = if internal_runtime_routes_enabled {
        internal_app.nest("/v1/internal", internal_routes)
    } else {
        info!("internal runtime routes disabled because internal TLS is not configured");
        internal_app
    };
    let internal_app = internal_app
        .layer(Extension(prometheus_handle))
        .layer(Extension(config))
        .layer(Extension(connection_pool))
        .layer(Extension(encryption_keyring))
        .layer(Extension(k8s_client))
        .layer(Extension(source_tls_config))
        .layer(middleware::from_fn(record_http_metrics))
        .layer(middleware::from_fn(capture_server_errors))
        .layer(sentry_layer)
        .layer(trace_layer);

    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;
    internal_listener.set_nonblocking(true)?;
    let internal_listener = tokio::net::TcpListener::from_std(internal_listener)?;
    let server = tokio::spawn(async move {
        let public_server = axum::serve(listener, app.into_make_service());
        if let Some(acceptor) = internal_tls_acceptor {
            let internal_server = axum::serve(
                TlsListener { listener: internal_listener, acceptor },
                internal_app.into_make_service(),
            );
            tokio::try_join!(public_server, internal_server).map(|_| ())
        } else {
            let internal_server = axum::serve(internal_listener, internal_app.into_make_service());
            tokio::try_join!(public_server, internal_server).map(|_| ())
        }
    });

    Ok(server)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::serve::Listener;
    use etl_config::Environment;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::TlsAcceptor;

    use super::{TlsListener, internal_runtime_routes_enabled};

    #[tokio::test]
    async fn tls_listener_accepts_connections_without_waiting_for_handshakes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(rustls::server::ResolvesServerCertUsingSni::new()));
        let mut listener =
            TlsListener { listener, acceptor: TlsAcceptor::from(Arc::new(server_config)) };

        let _first_stalled_client = TcpStream::connect(address).await.unwrap();
        let _first_connection = tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .expect("TCP acceptance should not wait for the TLS handshake");
        let _second_stalled_client = TcpStream::connect(address).await.unwrap();
        let _second_connection =
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .expect("a stalled TLS handshake should not block later TCP connections");
    }

    #[test]
    fn internal_runtime_routes_require_tls_outside_development() {
        assert!(internal_runtime_routes_enabled(Environment::Dev, false));
        assert!(internal_runtime_routes_enabled(Environment::Staging, true));
        assert!(internal_runtime_routes_enabled(Environment::Prod, true));
        assert!(!internal_runtime_routes_enabled(Environment::Staging, false));
        assert!(!internal_runtime_routes_enabled(Environment::Prod, false));
    }
}
