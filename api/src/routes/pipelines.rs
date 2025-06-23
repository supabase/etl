use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse, Responder, ResponseError,
};
use config::shared::{
    DestinationConfig, PipelineConfig as SharedPipelineConfig, ReplicatorConfig,
    SourceConfig as SharedSourceConfig, StateStoreConfig, SupabaseConfig, TlsConfig,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db;
use crate::db::destinations::{destination_exists, Destination, DestinationsDbError};
use crate::db::images::{Image, ImagesDbError};
use crate::db::pipelines::{Pipeline, PipelineConfig, PipelinesDbError};
use crate::db::replicators::{Replicator, ReplicatorsDbError};
use crate::db::sources::{source_exists, Source, SourceConfig, SourcesDbError};
use crate::encryption::EncryptionKey;
use crate::k8s_client::{
    HttpK8sClient, K8sClient, K8sError, PodPhase, TRUSTED_ROOT_CERT_CONFIG_MAP_NAME,
};
use crate::routes::{extract_tenant_id, ErrorMessage, TenantIdError};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Secrets {
    pub postgres_password: String,
    pub big_query_service_account_key: Option<String>,
}

#[derive(Debug, Error)]
enum PipelineError {
    #[error("The pipeline with id {0} was not found")]
    PipelineNotFound(i64),

    #[error("The source with id {0} was not found")]
    SourceNotFound(i64),

    #[error("The destination with id {0} was not found")]
    DestinationNotFound(i64),

    #[error("The replicator with pipeline id {0} was not found")]
    ReplicatorNotFound(i64),

    #[error("The image with replicator id {0} was not found")]
    ImageNotFound(i64),

    #[error("No default image was found")]
    NoDefaultImageFound,

    #[error(transparent)]
    TenantId(#[from] TenantIdError),

    #[error("invalid destination config")]
    InvalidConfig(#[from] serde_json::Error),

    #[error("A K8s error occurred: {0}")]
    K8s(#[from] K8sError),

    #[error(transparent)]
    SourcesDb(#[from] SourcesDbError),

    #[error(transparent)]
    DestinationsDb(#[from] DestinationsDbError),

    #[error(transparent)]
    PipelinesDb(PipelinesDbError),

    #[error(transparent)]
    ReplicatorsDb(#[from] ReplicatorsDbError),

    #[error(transparent)]
    ImagesDb(#[from] ImagesDbError),

    #[error("The trusted root certs config was not found")]
    TrustedRootCertsConfigMissing,

    #[error("A pipeline already exists for this source and destination combination")]
    DuplicatePipeline,
}

impl From<PipelinesDbError> for PipelineError {
    fn from(e: PipelinesDbError) -> Self {
        match e {
            PipelinesDbError::Database(e) if db::pipelines::is_duplicate_pipeline_error(&e) => {
                Self::DuplicatePipeline
            }
            e => Self::PipelinesDb(e),
        }
    }
}

impl PipelineError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            PipelineError::SourcesDb(SourcesDbError::Database(_))
            | PipelineError::DestinationsDb(DestinationsDbError::Database(_))
            | PipelineError::PipelinesDb(PipelinesDbError::Database(_))
            | PipelineError::ReplicatorsDb(ReplicatorsDbError::Database(_))
            | PipelineError::ImagesDb(ImagesDbError::Database(_)) => {
                "internal server error".to_string()
            }
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for PipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            PipelineError::InvalidConfig(_)
            | PipelineError::ReplicatorNotFound(_)
            | PipelineError::ImageNotFound(_)
            | PipelineError::NoDefaultImageFound
            | PipelineError::SourcesDb(_)
            | PipelineError::DestinationsDb(_)
            | PipelineError::PipelinesDb(_)
            | PipelineError::ReplicatorsDb(_)
            | PipelineError::ImagesDb(_)
            | PipelineError::K8s(_)
            | PipelineError::TrustedRootCertsConfigMissing => StatusCode::INTERNAL_SERVER_ERROR,
            PipelineError::PipelineNotFound(_) => StatusCode::NOT_FOUND,
            PipelineError::TenantId(_)
            | PipelineError::SourceNotFound(_)
            | PipelineError::DestinationNotFound(_) => StatusCode::BAD_REQUEST,
            PipelineError::DuplicatePipeline => StatusCode::CONFLICT,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let error_message = ErrorMessage {
            error: self.to_message(),
        };
        let body =
            serde_json::to_string(&error_message).expect("failed to serialize error message");
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(body)
    }
}

#[derive(Deserialize, ToSchema)]
pub struct PostPipelineRequest {
    pub source_id: i64,
    pub destination_id: i64,
    pub config: PipelineConfig,
}

#[derive(Serialize, ToSchema)]
pub struct PostPipelineResponse {
    id: i64,
}

#[derive(Serialize, ToSchema)]
pub struct GetPipelineResponse {
    id: i64,
    tenant_id: String,
    source_id: i64,
    source_name: String,
    destination_id: i64,
    destination_name: String,
    replicator_id: i64,
    config: PipelineConfig,
}

#[derive(Serialize, ToSchema)]
pub struct GetPipelinesResponse {
    pipelines: Vec<GetPipelineResponse>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostPipelineRequest,
    responses(
        (status = 200, description = "Create new pipeline", body = PostPipelineResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines")]
pub async fn create_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline: Json<PostPipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let pipeline = pipeline.0;
    let tenant_id = extract_tenant_id(&req)?;
    let config = pipeline.config;

    if !source_exists(&pool, tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(&pool, tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    let image = db::images::read_default_image(&pool)
        .await?
        .ok_or(PipelineError::NoDefaultImageFound)?;

    let id = db::pipelines::create_pipeline(
        &pool,
        tenant_id,
        pipeline.source_id,
        pipeline.destination_id,
        image.id,
        config,
    )
    .await?;

    let response = PostPipelineResponse { id };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
    ),
    responses(
        (status = 200, description = "Return pipeline with id = pipeline_id", body = GetPipelineResponse),
        (status = 404, description = "Pipeline not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/pipelines/{pipeline_id}")]
pub async fn read_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let response = db::pipelines::read_pipeline(&pool, tenant_id, pipeline_id)
        .await?
        .map(|s| {
            Ok::<GetPipelineResponse, serde_json::Error>(GetPipelineResponse {
                id: s.id,
                tenant_id: s.tenant_id,
                source_id: s.source_id,
                source_name: s.source_name,
                destination_id: s.destination_id,
                destination_name: s.destination_name,
                replicator_id: s.replicator_id,
                config: s.config,
            })
        })
        .transpose()?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostDestinationRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
    ),
    responses(
        (status = 200, description = "Update pipeline with id = pipeline_id"),
        (status = 404, description = "Pipeline not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id:\\d+}")]
pub async fn update_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
    pipeline: Json<PostPipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let pipeline = pipeline.0;
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let config = &pipeline.config;
    let source_id = pipeline.source_id;
    let destination_id = pipeline.destination_id;

    if !source_exists(&pool, tenant_id, source_id).await? {
        return Err(PipelineError::SourceNotFound(source_id));
    }

    if !destination_exists(&pool, tenant_id, destination_id).await? {
        return Err(PipelineError::DestinationNotFound(destination_id));
    }

    db::pipelines::update_pipeline(
        &pool,
        tenant_id,
        pipeline_id,
        source_id,
        destination_id,
        config,
    )
    .await?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("pipeline_id" = i64, Path, description = "Id of the pipeline"),
    ),
    responses(
        (status = 200, description = "Delete pipeline with id = pipeline_id"),
        (status = 404, description = "Pipeline not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[delete("/pipelines/{pipeline_id}")]
pub async fn delete_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    db::pipelines::delete_pipeline(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Return all pipelines"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/pipelines")]
pub async fn read_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;

    let mut pipelines = vec![];
    for pipeline in db::pipelines::read_all_pipelines(&pool, tenant_id).await? {
        let pipeline = GetPipelineResponse {
            id: pipeline.id,
            tenant_id: pipeline.tenant_id,
            source_id: pipeline.source_id,
            source_name: pipeline.source_name,
            destination_id: pipeline.destination_id,
            destination_name: pipeline.destination_name,
            replicator_id: pipeline.replicator_id,
            config: pipeline.config,
        };
        pipelines.push(pipeline);
    }

    let response = GetPipelinesResponse { pipelines };

    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Start a pipeline"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id}/start")]
pub async fn start_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let (pipeline, replicator, image, source, destination) =
        read_data(&pool, tenant_id, pipeline_id, &encryption_key).await?;

    // TODO: load actual state store config.
    let state_store = StateStoreConfig::Memory;

    // We wrap Supabase's related information within its own struct for a clear separation.
    let supabase_config = SupabaseConfig {
        project_ref: tenant_id.to_owned(),
    };

    let secrets = build_secrets(&source.config, &destination.config);

    let replicator_config = build_replicator_config(
        &k8s_client,
        source.config,
        state_store,
        destination.config,
        pipeline,
        supabase_config,
    )
    .await?;

    let prefix = create_prefix(tenant_id, replicator.id);
    create_or_update_secrets(&k8s_client, &prefix, secrets).await?;
    create_or_update_config(&k8s_client, &prefix, replicator_config).await?;
    create_or_update_replicator(&k8s_client, &prefix, image.name).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Stop a pipeline"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/{pipeline_id}/stop")]
pub async fn stop_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let replicator = db::replicators::read_replicator_by_pipeline_id(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_prefix(tenant_id, replicator.id);
    delete_secrets(&k8s_client, &prefix).await?;
    delete_config(&k8s_client, &prefix).await?;
    delete_replicator(&k8s_client, &prefix).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Stop all pipelines"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/pipelines/stop")]
pub async fn stop_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let replicators = db::replicators::read_replicators(&pool, tenant_id).await?;
    for replicator in replicators {
        let prefix = create_prefix(tenant_id, replicator.id);
        delete_secrets(&k8s_client, &prefix).await?;
        delete_config(&k8s_client, &prefix).await?;
        delete_replicator(&k8s_client, &prefix).await?;
    }
    Ok(HttpResponse::Ok().finish())
}

#[derive(Serialize, ToSchema)]
pub enum PipelineStatus {
    Stopped,
    Starting,
    Started,
    Stopping,
    Unknown,
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Get pipeline status"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/pipelines/{pipeline_id}/status")]
pub async fn get_pipeline_status(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<Arc<HttpK8sClient>>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let replicator = db::replicators::read_replicator_by_pipeline_id(&pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_prefix(tenant_id, replicator.id);

    let pod_phase = k8s_client.get_pod_phase(&prefix).await?;

    let status = match pod_phase {
        PodPhase::Pending => PipelineStatus::Starting,
        PodPhase::Running => PipelineStatus::Started,
        PodPhase::Succeeded => PipelineStatus::Stopped,
        PodPhase::Failed => PipelineStatus::Stopped,
        PodPhase::Unknown => PipelineStatus::Unknown,
    };

    Ok(Json(status))
}

async fn read_data(
    pool: &PgPool,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<(Pipeline, Replicator, Image, Source, Destination), PipelineError> {
    let pipeline = db::pipelines::read_pipeline(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    let replicator = db::replicators::read_replicator_by_pipeline_id(pool, tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let image = db::images::read_image_by_replicator_id(pool, replicator.id)
        .await?
        .ok_or(PipelineError::ImageNotFound(replicator.id))?;

    let source_id = pipeline.source_id;
    let source = db::sources::read_source(pool, tenant_id, source_id, encryption_key)
        .await?
        .ok_or(PipelineError::SourceNotFound(source_id))?;

    let destination_id = pipeline.destination_id;
    let destination =
        db::destinations::read_destination(pool, tenant_id, destination_id, encryption_key)
            .await?
            .ok_or(PipelineError::DestinationNotFound(destination_id))?;

    Ok((pipeline, replicator, image, source, destination))
}

fn build_secrets(source_config: &SourceConfig, destination_config: &DestinationConfig) -> Secrets {
    let postgres_password = source_config.password.clone().unwrap_or_default();
    let mut big_query_service_account_key = None;
    if let DestinationConfig::BigQuery {
        service_account_key,
        ..
    } = destination_config
    {
        big_query_service_account_key = Some(service_account_key.clone());
    };

    Secrets {
        postgres_password,
        big_query_service_account_key,
    }
}

async fn build_replicator_config(
    k8s_client: &Arc<HttpK8sClient>,
    source_config: SourceConfig,
    state_store_config: StateStoreConfig,
    destination_config: DestinationConfig,
    pipeline: Pipeline,
    supabase_config: SupabaseConfig,
) -> Result<ReplicatorConfig, PipelineError> {
    let trusted_root_certs = k8s_client
        .get_config_map(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
        .await?
        .data
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .get("trusted_root_certs")
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .clone();

    let source_config = SharedSourceConfig {
        host: source_config.host,
        port: source_config.port,
        name: source_config.name,
        username: source_config.username,
        password: source_config.password,
        tls: TlsConfig {
            trusted_root_certs,
            enabled: true,
        },
    };

    let pipeline_config = SharedPipelineConfig {
        // We are safe to perform this conversion, since the i64 -> u64 conversion performs wrap
        // around, and we won't have two different values map to the same u64, since the domain size
        // is the same.
        id: pipeline.id as u64,
        publication_name: pipeline.config.publication_name,
        batch: pipeline.config.batch,
        apply_worker_init_retry: pipeline.config.apply_worker_init_retry,
    };

    let config = ReplicatorConfig {
        source: source_config,
        state_store: state_store_config,
        destination: destination_config,
        pipeline: pipeline_config,
        supabase: Some(supabase_config),
    };

    Ok(config)
}

fn create_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

async fn create_or_update_secrets(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    secrets: Secrets,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_postgres_secret(prefix, &secrets.postgres_password)
        .await?;

    if let Some(bigquery_service_account_key) = secrets.big_query_service_account_key {
        k8s_client
            .create_or_update_bq_secret(prefix, &bigquery_service_account_key)
            .await?;
    }

    Ok(())
}

async fn create_or_update_config(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    config: ReplicatorConfig,
) -> Result<(), PipelineError> {
    let base_config = "";
    let prod_config = serde_json::to_string(&config)?;
    k8s_client
        .create_or_update_config_map(prefix, base_config, &prod_config)
        .await?;
    Ok(())
}

async fn create_or_update_replicator(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
    replicator_image: String,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_stateful_set(prefix, &replicator_image)
        .await?;

    Ok(())
}

async fn delete_secrets(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_postgres_secret(prefix).await?;
    k8s_client.delete_bq_secret(prefix).await?;
    Ok(())
}

async fn delete_config(k8s_client: &Arc<HttpK8sClient>, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_config_map(prefix).await?;
    Ok(())
}

async fn delete_replicator(
    k8s_client: &Arc<HttpK8sClient>,
    prefix: &str,
) -> Result<(), PipelineError> {
    k8s_client.delete_stateful_set(prefix).await?;
    Ok(())
}
