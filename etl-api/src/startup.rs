use std::{net::TcpListener, sync::Arc};

use actix_web::{App, HttpServer, dev::Server, web};
use actix_web_httpauth::middleware::HttpAuthentication;
use actix_web_metrics::ActixWebMetricsBuilder;
use aws_lc_rs::aead::{AES_256_GCM, RandomizedNonceKey};
use base64::{Engine, prelude::BASE64_STANDARD};
use etl_config::Environment;
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use etl_telemetry::metrics::init_metrics_handle;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
use kube::Api;
use kube::api::{Patch, PatchParams};
use kube::config::KubeConfigOptions;
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::{error, info, warn};
use tracing_actix_web::TracingLogger;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::k8s::http::{
    DATA_PLANE_NAMESPACE, HttpK8sClient, TRUSTED_ROOT_CERT_CONFIG_MAP_NAME,
    TRUSTED_ROOT_CERT_KEY_NAME,
};
use crate::k8s::{K8sClient, K8sError};
use crate::{
    authentication::auth_validator,
    config::ApiConfig,
    configs::encryption,
    db::publications::Publication,
    routes::{
        destinations::{
            CreateDestinationRequest, CreateDestinationResponse, ReadDestinationResponse,
            ReadDestinationsResponse, UpdateDestinationRequest, create_destination,
            delete_destination, read_all_destinations, read_destination, update_destination,
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
            UpdatePipelineRequest, UpdatePipelineVersionRequest, create_pipeline, delete_pipeline,
            get_pipeline_replication_status, get_pipeline_status, get_pipeline_version,
            read_all_pipelines, read_pipeline, rollback_table_state, start_pipeline,
            stop_all_pipelines, stop_pipeline, update_pipeline, update_pipeline_config,
            update_pipeline_version,
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
    pub async fn build(config: ApiConfig) -> Result<Self, anyhow::Error> {
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

        let kube_client = match Environment::load()? {
            Environment::Staging | Environment::Prod => kube::Client::try_default().await?,
            Environment::Dev => {
                let options = KubeConfigOptions {
                    context: Some("orbstack".to_string()),
                    cluster: Some("orbstack".to_string()),
                    user: Some("orbstack".to_string()),
                };
                let kube_config = kube::config::Config::from_kubeconfig(&options).await?;
                let kube_client: kube::Client = kube_config.try_into()?;

                prepare_dev_env(&kube_client).await?;

                kube_client
            }
        };

        let k8s_client = match HttpK8sClient::new(kube_client).await {
            Ok(client) => Some(Arc::new(client) as Arc<dyn K8sClient>),
            Err(e) => {
                warn!(
                    "Failed to create Kubernetes client: {}. Running without Kubernetes support.",
                    e
                );
                None
            }
        };

        let server = run(
            config,
            listener,
            connection_pool,
            encryption_key,
            k8s_client,
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

// TODO: in dev env pod is not scheduled because the node needs a label nodeType=workloads
// to schedule the pod, either remove this requirement from the pod or add this label to
// the node.
//
// TODO:
async fn prepare_dev_env(client: &kube::Client) -> Result<(), K8sError> {
    test_connection(client).await?;
    create_etl_data_plane_ns_if_missing(client).await?;
    create_trusted_root_certs_cm_if_missing(client).await?;
    // label_node_if_missing(client).await?;
    // create_vector_cm_if_missing(client).await?;
    // create_logflare_secret_if_missing(client).await?;

    Ok(())
}

async fn test_connection(client: &kube::Client) -> Result<(), K8sError> {
    match client.apiserver_version().await {
        Ok(version) => {
            info!(
                "successfully connected to orbstack kubernetes api server version: {}.{}",
                version.major, version.minor
            );
        }
        Err(e) => {
            error!(
                "failed to connect to orbstack, make sure you have orbstack installed and kubernetes enabled in it."
            );
            return Err(e.into());
        }
    }

    Ok(())
}

async fn create_etl_data_plane_ns_if_missing(client: &kube::Client) -> Result<(), K8sError> {
    let api: Api<Namespace> = Api::all(client.clone());

    if api.get_opt(DATA_PLANE_NAMESPACE).await?.is_none() {
        let data_plane_ns_json = create_etl_data_plane_ns_json();
        let ns: Namespace = serde_json::from_value(data_plane_ns_json)?;
        let pp = PatchParams::apply(DATA_PLANE_NAMESPACE);
        api.patch(DATA_PLANE_NAMESPACE, &pp, &Patch::Apply(ns))
            .await?;
    }

    Ok(())
}

fn create_etl_data_plane_ns_json() -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name":DATA_PLANE_NAMESPACE
        }
    })
}

async fn create_trusted_root_certs_cm_if_missing(client: &kube::Client) -> Result<(), K8sError> {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), DATA_PLANE_NAMESPACE);

    if api
        .get_opt(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
        .await?
        .is_none()
    {
        let trusted_root_certs_cm = create_trusted_root_certs_cm_json();
        let cm: ConfigMap = serde_json::from_value(trusted_root_certs_cm)?;
        let pp = PatchParams::apply(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME);
        api.patch(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME, &pp, &Patch::Apply(cm))
            .await?;
    }

    Ok(())
}

fn create_trusted_root_certs_cm_json() -> serde_json::Value {
    json!({
        "kind": "ConfigMap",
        "apiVersion": "v1",
        "metadata": {
        "name": TRUSTED_ROOT_CERT_CONFIG_MAP_NAME,
        "namespace": DATA_PLANE_NAMESPACE
        },
        "data": {
        TRUSTED_ROOT_CERT_KEY_NAME:
            r#"-----BEGIN CERTIFICATE-----
            MIID1DCCArygAwIBAgIUbYRdq/8/uNq8G9stMCdOFSBgA2MwDQYJKoZIhvcNAQEL
            BQAwczELMAkGA1UEBhMCVVMxEDAOBgNVBAgMB0RlbHdhcmUxEzARBgNVBAcMCk5l
            dyBDYXN0bGUxFTATBgNVBAoMDFN1cGFiYXNlIEluYzEmMCQGA1UEAwwdU3VwYWJh
            c2UgU3RhZ2luZyBSb290IDIwMjEgQ0EwHhcNMjEwNDI4MTAzNjEzWhcNMzEwNDI2
            MTAzNjEzWjBzMQswCQYDVQQGEwJVUzEQMA4GA1UECAwHRGVsd2FyZTETMBEGA1UE
            BwwKTmV3IENhc3RsZTEVMBMGA1UECgwMU3VwYWJhc2UgSW5jMSYwJAYDVQQDDB1T
            dXBhYmFzZSBTdGFnaW5nIFJvb3QgMjAyMSBDQTCCASIwDQYJKoZIhvcNAQEBBQAD
            ggEPADCCAQoCggEBAN0AKRE8a56O8LaZxiOAcHFUFnwiKUvPoXPq26Ifw+Nv+7zg
            N2V5WnMZbbw24q61Os60ZUn0XmbVtuIeJ+stPHsO7qxxuL+bmPR+qU5tkDrIOyEe
            YD/2u8/q6ssVv42k4XcXbhM6RVz7CkCDY0TiBm1bMtRZso3xB6E9wAjxDf43XfV5
            PAGs3JI+Zo/vyqCDlN0hHOrB/aBl01JXqQWI84Gia5ooucq4SjA1CyawBcQ2IAvG
            rXuy1BouY+xM3zRuNvtfFP6rb5Mta+jCYEMh1AZ8yP8sYUWAyhxX6k9EbOb009wQ
            aZljbUCh/UglGWuBxdzePavx+zPjzWXB1NyVkpkCAwEAAaNgMF4wCwYDVR0PBAQD
            AgEGMB0GA1UdDgQWBBQFx+PHLf27iIo/PMfIfGqXF7Zb+DAfBgNVHSMEGDAWgBQF
            x+PHLf27iIo/PMfIfGqXF7Zb+DAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEB
            CwUAA4IBAQB/xIiz5dDqzGXjqYqXZYx4iSfSxsVayeOPDMfmaiCfSMJEUG4cUiwG
            OvMPGztaUEYeip5SCvSKuAAjVkXyP7ahKR7t7lZ9mErVXyxSZoVLbOd578CuYiZk
            OgT17UjPv66WMzEKEr8wGpomTYWWfEkuqt8ENdiM1Z4LNFahdKj36+jm6/a+9R8K
            25VIL68DTaQpBxFWG6ixC1HRMHJ12lDhKsshIi099BVpkGibESlxPrQOdKKqBB/J
            vIX+/Hb+mS4H5zYMeK2wX0onp+GBcD6X9L1UJuXMVd+BRan8RFidXL5s3++xXjQq
            Nzbc6lnA69urKffvcT07YwMsY/OmHzVa
            -----END CERTIFICATE-----"#
        }
    })
}

// async fn label_node_if_missing(client: &kube::Client) -> Result<(), K8sError> {
//     let api: Api<Node> = Api::all(client.clone());

//     if api.get_opt("orbstack").await?.is_none() {
//         let node_json = create_node_json();
//         let node: Node = serde_json::from_value(node_json)?;
//         let pp = PatchParams::apply("orbstack");
//         api.patch("orbstack", &pp, &Patch::Apply(node)).await?;
//     }

//     Ok(())
// }

// fn create_node_json() -> serde_json::Value {
//     json!({
//         "apiVersion": "v1",
//         "kind": "Node",
//         "metadata": {
//             "name": "orbstack",
//             "labels": {
//                 "nodeType": "workloads"
//             }
//         }
//     })
// }
// async fn create_vector_cm_if_missing(client: &kube::Client) -> Result<(), K8sError> {
//     let api: Api<ConfigMap> = Api::namespaced(client.clone(), DATA_PLANE_NAMESPACE);

//     if api.get_opt(VECTOR_CONFIG_MAP_NAME).await?.is_none() {
//         let vector_cm = create_vector_cm_json();
//         let cm: ConfigMap = serde_json::from_value(vector_cm)?;
//         let pp = PatchParams::apply(VECTOR_CONFIG_MAP_NAME);
//         api.patch(VECTOR_CONFIG_MAP_NAME, &pp, &Patch::Apply(cm))
//             .await?;
//     }

//     Ok(())
// }

// fn create_vector_cm_json() -> serde_json::Value {
//     json!({
//         "apiVersion": "v1",
//         "kind": "ConfigMap",
//         "metadata": {
//             "name": VECTOR_CONFIG_MAP_NAME,
//             "namespace": DATA_PLANE_NAMESPACE
//         },
//         "data": {
//         "vector.yaml":
//             r#"
//             data_dir: "/var/lib/vector"
//             sources:
//                 replicator_logs:
//                     type: "file"
//                     ignore_older_secs: 600
//                     include: ["/var/log/*.log"]
//                     read_from: "beginning"

//             transforms:
//                 transform_replicator_logs:
//                     inputs: ["replicator_logs"]
//                     type: "remap"
//                     source: |
//                         # parse the message field as json or assign a default
//                         message = parse_json(.message) ?? {"fields":{"message":"<missing>"},"timestamp":now(),"level":"info","span":{}}

//                         .message = message.fields.message
//                         del(message.fields.message)
//                         .timestamp = message.timestamp

//                         if exists(message.project) {
//                             .project = message.project
//                             del(message.project)
//                         }

//                         if exists(message.pipeline_id) {
//                             .pipeline_id = message.pipeline_id
//                             del(message.pipeline_id)
//                         }

//                         .fields = message.fields
//                         .level = message.level
//                         .span = message.span
//                         .spans = message.spans
//                         .vector_file = .file
//                         .vector_host = .host
//                         .vector_timestamp = .timestamp

//                         # delete top-level fields which are not required
//                         del(.file)
//                         del(.host)
//                         del(.source_type)

//                 # Split logs into two based on whether or not it is an egress metric
//                 split_route:
//                     inputs: ["transform_replicator_logs"]
//                     type: "route"
//                     route:
//                         egress_logs: '.fields.egress_metric == true'
//                         non_egress_logs: '.fields.egress_metric != true'

//             sinks:
//                 logflare_non_egress:
//                     type: "console"
//                     inputs: ["split_route.non_egress_logs"]
//                     encoding:
//                         codec: "json"
//                 logflare_egress:
//                     type: "http"
//                     inputs: ["split_route.egress_logs"]
//                     encoding:
//                         codec: "json"
//             "#
//         }
//     })
// }

// async fn create_logflare_secret_if_missing(client: &kube::Client) -> Result<(), K8sError> {
//     let api: Api<Secret> = Api::namespaced(client.clone(), DATA_PLANE_NAMESPACE);

//     if api.get_opt(LOGFLARE_SECRET_NAME).await?.is_none() {
//         let logflare_secret = create_logflare_api_key_secret_json();
//         let secret: Secret = serde_json::from_value(logflare_secret)?;
//         let pp = PatchParams::apply(LOGFLARE_SECRET_NAME);
//         api.patch(LOGFLARE_SECRET_NAME, &pp, &Patch::Apply(secret))
//             .await?;
//     }

//     Ok(())
// }

// fn create_logflare_api_key_secret_json() -> serde_json::Value {
//     json!({
//         "apiVersion": "v1",
//         "data": {
//             "key": "bm90LWEta2V5"
//         },
//         "kind": "Secret",
//         "metadata": {
//             "name": LOGFLARE_SECRET_NAME,
//             "namespace": DATA_PLANE_NAMESPACE,
//         },
//         "type": "Opaque"
//     })
// }

/// Creates a Postgres connection pool from the provided configuration.
pub fn get_connection_pool(config: &PgConnectionConfig) -> PgPool {
    PgPoolOptions::new().connect_lazy_with(config.with_db())
}

/// Creates and configures the HTTP server with all routes and middleware.
///
/// Sets up authentication, tracing, Swagger UI, and all API endpoints.
/// The Kubernetes client is optional to support testing scenarios.
pub async fn run(
    config: ApiConfig,
    listener: TcpListener,
    connection_pool: PgPool,
    encryption_key: encryption::EncryptionKey,
    http_k8s_client: Option<Arc<dyn K8sClient>>,
) -> Result<Server, anyhow::Error> {
    let prometheus_handle = web::ThinData(init_metrics_handle()?);
    let config = web::Data::new(config);
    let connection_pool = web::Data::new(connection_pool);
    let encryption_key = web::Data::new(encryption_key);
    let k8s_client: Option<web::Data<dyn K8sClient>> = http_k8s_client.map(Into::into);

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
        crate::routes::pipelines::rollback_table_state,
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
        crate::routes::tenants_sources::create_tenant_and_source,
        crate::routes::destinations_pipelines::create_destination_and_pipeline,
        crate::routes::destinations_pipelines::update_destination_and_pipeline,
        crate::routes::destinations_pipelines::delete_destination_and_pipeline,
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
                    //tenants
                    .service(create_tenant)
                    .service(create_or_update_tenant)
                    .service(read_tenant)
                    .service(update_tenant)
                    .service(delete_tenant)
                    .service(read_all_tenants)
                    //sources
                    .service(create_source)
                    .service(read_source)
                    .service(update_source)
                    .service(delete_source)
                    .service(read_all_sources)
                    //destinations
                    .service(create_destination)
                    .service(read_destination)
                    .service(update_destination)
                    .service(delete_destination)
                    .service(read_all_destinations)
                    //pipelines
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
                    .service(rollback_table_state)
                    .service(update_pipeline_version)
                    .service(update_pipeline_config)
                    //tables
                    .service(read_table_names)
                    //publications
                    .service(create_publication)
                    .service(read_publication)
                    .service(update_publication)
                    .service(delete_publication)
                    .service(read_all_publications)
                    //images
                    .service(create_image)
                    .service(read_image)
                    .service(update_image)
                    .service(delete_image)
                    .service(read_all_images)
                    //tenants_sources
                    .service(create_tenant_and_source)
                    //destinations-pipelines
                    .service(create_destination_and_pipeline)
                    .service(update_destination_and_pipeline)
                    .service(delete_destination_and_pipeline),
            )
            .app_data(prometheus_handle.clone())
            .app_data(config.clone())
            .app_data(connection_pool.clone())
            .app_data(encryption_key.clone());

        if let Some(k8s_client) = k8s_client.clone() {
            app.app_data(k8s_client.clone())
        } else {
            app
        }
    })
    .listen(listener)?
    .run();

    Ok(server)
}
