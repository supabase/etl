use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError, delete, get,
    http::{StatusCode, header::ContentType},
    post,
    web::{Data, Json, Path},
};
use etl_config::shared::{
    DestinationConfig, PgConnectionConfig, PipelineConfig as SharedPipelineConfig,
    ReplicatorConfig, SupabaseConfig, TlsConfig,
};
use etl_postgres::replication::{TableLookupError, get_table_name_from_oid, state};
use etl_postgres::schema::TableId;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, PgTransaction};
use std::ops::DerefMut;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db::destinations::{Destination, DestinationsDbError, destination_exists};
use crate::db::images::{Image, ImagesDbError};
use crate::db::pipelines::{Pipeline, PipelineConfig, PipelinesDbError};
use crate::db::replicators::{Replicator, ReplicatorsDbError};
use crate::db::sources::{Source, SourceConfig, SourcesDbError, source_exists};
use crate::db::{self, pipelines::OptionalPipelineConfig};
use crate::encryption::EncryptionKey;
use crate::k8s_client::TRUSTED_ROOT_CERT_KEY_NAME;
use crate::k8s_client::{K8sClient, K8sError, PodPhase, TRUSTED_ROOT_CERT_CONFIG_MAP_NAME};
use crate::routes::{
    ErrorMessage, TenantIdError, connect_to_source_database_with_defaults, extract_tenant_id,
};

#[derive(Debug, Error)]
pub enum PipelineError {
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

    #[error("The table replication state is not present")]
    MissingTableReplicationState,

    #[error("The table replication state is not valid: {0}")]
    InvalidTableReplicationState(serde_json::Error),

    #[error("The table state is not rollbackable: {0}")]
    NotRollbackable(String),

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

    #[error("The specified image with id {0} was not found")]
    ImageNotFoundById(i64),

    #[error("There was an error while looking up table information in the source database: {0}")]
    TableLookup(#[from] TableLookupError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
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
            | PipelineError::ImagesDb(ImagesDbError::Database(_))
            | PipelineError::Database(_) => "internal server error".to_string(),
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
            | PipelineError::TrustedRootCertsConfigMissing
            | PipelineError::Database(_)
            | PipelineError::TableLookup(_)
            | PipelineError::InvalidTableReplicationState(_)
            | PipelineError::MissingTableReplicationState => StatusCode::INTERNAL_SERVER_ERROR,
            PipelineError::NotRollbackable(_) => StatusCode::BAD_REQUEST,
            PipelineError::PipelineNotFound(_) | PipelineError::ImageNotFoundById(_) => {
                StatusCode::NOT_FOUND
            }
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreatePipelineRequest {
    #[schema(example = 1, required = true)]
    pub source_id: i64,
    #[schema(example = 1, required = true)]
    pub destination_id: i64,
    #[schema(required = true)]
    pub config: PipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreatePipelineResponse {
    #[schema(example = 1)]
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePipelineRequest {
    #[schema(example = 1, required = true)]
    pub source_id: i64,
    #[schema(example = 1, required = true)]
    pub destination_id: i64,
    #[schema(required = true)]
    pub config: PipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePipelineConfigRequest {
    #[schema(required = true)]
    pub config: OptionalPipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePipelineConfigResponse {
    pub config: PipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadPipelineResponse {
    #[schema(example = 1)]
    pub id: i64,
    #[schema(example = "abczjjlmfsijwrlnwatw")]
    pub tenant_id: String,
    #[schema(example = 1)]
    pub source_id: i64,
    #[schema(example = "My Postgres Source")]
    pub source_name: String,
    #[schema(example = 1)]
    pub destination_id: i64,
    #[schema(example = "My BigQuery Destination")]
    pub destination_name: String,
    #[schema(example = 1)]
    pub replicator_id: i64,
    pub config: PipelineConfig,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadPipelinesResponse {
    pub pipelines: Vec<ReadPipelineResponse>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdatePipelineImageRequest {
    #[schema(example = 1)]
    pub image_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetPipelineStatusResponse {
    #[schema(example = 1)]
    pub pipeline_id: i64,
    pub status: PipelineStatus,
}

/// UI-friendly representation of table replication state
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "name")]
pub enum SimpleTableReplicationState {
    Queued,
    CopyingTable,
    CopiedTable,
    FollowingWal {
        lag: u64,
    },
    Error {
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        solution: Option<String>,
        retry_policy: SimpleRetryPolicy,
    },
}

/// Simplified retry policy for UI display
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "policy")]
pub enum SimpleRetryPolicy {
    NoRetry,
    ManualRetry,
    TimedRetry {
        #[schema(example = "2023-12-25T12:00:00Z")]
        next_retry: String,
    },
}

impl From<state::TableReplicationState> for SimpleTableReplicationState {
    fn from(state: state::TableReplicationState) -> Self {
        match state {
            state::TableReplicationState::Init => SimpleTableReplicationState::Queued,
            state::TableReplicationState::DataSync => SimpleTableReplicationState::CopyingTable,
            state::TableReplicationState::FinishedCopy => SimpleTableReplicationState::CopiedTable,
            // TODO: add lag metric when available.
            state::TableReplicationState::SyncDone { .. } => {
                SimpleTableReplicationState::FollowingWal { lag: 0 }
            }
            state::TableReplicationState::Ready => {
                SimpleTableReplicationState::FollowingWal { lag: 0 }
            }
            state::TableReplicationState::Errored {
                reason,
                solution,
                retry_policy,
            } => {
                let simple_retry_policy = match retry_policy {
                    state::RetryPolicy::NoRetry => SimpleRetryPolicy::NoRetry,
                    state::RetryPolicy::ManualRetry => SimpleRetryPolicy::ManualRetry,
                    state::RetryPolicy::TimedRetry { next_retry } => {
                        SimpleRetryPolicy::TimedRetry {
                            next_retry: next_retry.to_rfc3339(),
                        }
                    }
                };

                SimpleTableReplicationState::Error {
                    reason,
                    solution,
                    retry_policy: simple_retry_policy,
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TableReplicationStatus {
    #[schema(example = 1)]
    pub table_id: u32,
    #[schema(example = "public.users")]
    pub table_name: String,
    pub state: SimpleTableReplicationState,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetPipelineReplicationStatusResponse {
    #[schema(example = 1)]
    pub pipeline_id: i64,
    pub table_statuses: Vec<TableReplicationStatus>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RollbackType {
    Individual,
    Full,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RollbackTableStateRequest {
    #[schema(example = 1)]
    pub table_id: u32,
    pub rollback_type: RollbackType,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RollbackTableStateResponse {
    #[schema(example = 1)]
    pub pipeline_id: i64,
    #[schema(example = 1)]
    pub table_id: u32,
    pub new_state: SimpleTableReplicationState,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "name")]
pub enum PipelineStatus {
    Stopped,
    Starting,
    Started,
    Stopping,
    Unknown,
    Failed,
}

#[utoipa::path(
    summary = "Create a pipeline",
    description = "Creates a pipeline linking a source to a destination.",
    request_body = CreatePipelineRequest,
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline created successfully", body = CreatePipelineResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Pipelines"
)]
#[post("/pipelines")]
pub async fn create_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline: Json<CreatePipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline = pipeline.into_inner();

    let mut txn = pool.begin().await?;
    if !source_exists(txn.deref_mut(), tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(txn.deref_mut(), tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    let image = db::images::read_default_image(txn.deref_mut())
        .await?
        .ok_or(PipelineError::NoDefaultImageFound)?;

    let id = db::pipelines::create_pipeline(
        &mut txn,
        tenant_id,
        pipeline.source_id,
        pipeline.destination_id,
        image.id,
        pipeline.config,
    )
    .await?;
    txn.commit().await?;

    let response = CreatePipelineResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    summary = "Retrieve a pipeline",
    description = "Returns a pipeline by ID for the given tenant.",
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline retrieved successfully", body = ReadPipelineResponse),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_id}")]
pub async fn read_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let response = db::pipelines::read_pipeline(&**pool, tenant_id, pipeline_id)
        .await?
        .map(|s| {
            Ok::<ReadPipelineResponse, serde_json::Error>(ReadPipelineResponse {
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
    summary = "Update a pipeline",
    description = "Updates a pipeline's source, destination, or configuration.",
    request_body = UpdatePipelineRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline updated successfully"),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
// Forcing {pipeline_id} to be all digits by appending :\\d+
// to avoid this route clashing with /pipelines/stop
#[post("/pipelines/{pipeline_id:\\d+}")]
pub async fn update_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
    pipeline: Json<UpdatePipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let pipeline = pipeline.into_inner();

    let mut txn = pool.begin().await?;
    if !source_exists(txn.deref_mut(), tenant_id, pipeline.source_id).await? {
        return Err(PipelineError::SourceNotFound(pipeline.source_id));
    }

    if !destination_exists(txn.deref_mut(), tenant_id, pipeline.destination_id).await? {
        return Err(PipelineError::DestinationNotFound(pipeline.destination_id));
    }

    db::pipelines::update_pipeline(
        txn.deref_mut(),
        tenant_id,
        pipeline_id,
        pipeline.source_id,
        pipeline.destination_id,
        &pipeline.config,
    )
    .await?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Delete a pipeline",
    description = "Deletes a pipeline and its associated resources.",
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline deleted successfully"),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[delete("/pipelines/{pipeline_id}")]
pub async fn delete_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let mut txn = pool.begin().await?;

    let pipeline = db::pipelines::read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    let source = db::sources::read_source(
        txn.deref_mut(),
        tenant_id,
        pipeline.source_id,
        &encryption_key,
    )
    .await?
    .ok_or(PipelineError::SourceNotFound(pipeline.source_id))?;

    db::pipelines::delete_pipeline_cascading(txn, tenant_id, &pipeline, &source, None).await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "List pipelines",
    description = "Returns all pipelines for the specified tenant.",
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipelines listed successfully", body = ReadPipelinesResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines")]
pub async fn read_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;

    let mut pipelines = vec![];
    for pipeline in db::pipelines::read_all_pipelines(&**pool, tenant_id).await? {
        let pipeline = ReadPipelineResponse {
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

    let response = ReadPipelinesResponse { pipelines };

    Ok(Json(response))
}

#[utoipa::path(
    summary = "Start a pipeline",
    description = "Starts the pipeline by deploying its replicator.",
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline started successfully"),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/start")]
pub async fn start_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let (pipeline, replicator, image, source, destination) =
        read_all_required_data(&mut txn, tenant_id, pipeline_id, &encryption_key).await?;

    // We update the pipeline in K8s.
    create_or_update_pipeline_in_k8s(
        k8s_client.as_ref(),
        tenant_id,
        pipeline,
        replicator,
        image,
        source,
        destination,
    )
    .await?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Stop a pipeline",
    description = "Stops the pipeline by terminating its replicator.",
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline stopped successfully"),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/stop")]
pub async fn stop_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let replicator =
        db::replicators::read_replicator_by_pipeline_id(txn.deref_mut(), tenant_id, pipeline_id)
            .await?
            .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    delete_pipeline_in_k8s(k8s_client.as_ref(), tenant_id, replicator).await?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Stop all pipelines",
    description = "Stops all pipelines for the specified tenant.",
    params(
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "All pipelines stopped successfully"),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/stop")]
pub async fn stop_all_pipelines(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<dyn K8sClient>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let replicators = db::replicators::read_replicators(txn.deref_mut(), tenant_id).await?;
    for replicator in replicators {
        delete_pipeline_in_k8s(k8s_client.as_ref(), tenant_id, replicator).await?;
    }
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Check pipeline status",
    description = "Returns the current status of the pipeline's replicator.",
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline status retrieved successfully", body = GetPipelineStatusResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_id}/status")]
pub async fn get_pipeline_status(
    req: HttpRequest,
    pool: Data<PgPool>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let k8s_client = k8s_client.into_inner();

    let replicator =
        db::replicators::read_replicator_by_pipeline_id(&**pool, tenant_id, pipeline_id)
            .await?
            .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    // We load the pod phase.
    let pod_phase = k8s_client.get_pod_phase(&prefix).await?;

    // Check if there's a container error.
    //
    // We are not exposing the error to not leak any internal information of the pod and because we
    // will expose it through logs.
    let has_container_error = k8s_client.has_replicator_container_error(&prefix).await?;

    let status = if has_container_error {
        PipelineStatus::Failed
    } else {
        match pod_phase {
            PodPhase::Pending => PipelineStatus::Starting,
            PodPhase::Running => PipelineStatus::Started,
            PodPhase::Succeeded => PipelineStatus::Stopped,
            PodPhase::Failed => PipelineStatus::Failed,
            PodPhase::Unknown => PipelineStatus::Unknown,
        }
    };

    let response = GetPipelineStatusResponse {
        pipeline_id,
        status,
    };

    Ok(Json(response))
}

#[utoipa::path(
    summary = "Get replication status",
    description = "Returns the replication status for all tables in the pipeline.",
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Replication status retrieved successfully", body = GetPipelineReplicationStatusResponse),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[get("/pipelines/{pipeline_id}/replication-status")]
pub async fn get_pipeline_replication_status(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    pipeline_id: Path<i64>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();

    let mut txn = pool.begin().await?;

    // Read the pipeline to ensure it exists and get the source configuration
    let pipeline = db::pipelines::read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    // Get the source configuration
    let source = db::sources::read_source(
        txn.deref_mut(),
        tenant_id,
        pipeline.source_id,
        &encryption_key,
    )
    .await?
    .ok_or(PipelineError::SourceNotFound(pipeline.source_id))?;

    txn.commit().await?;

    // Connect to the source database to read replication state
    let source_pool =
        connect_to_source_database_with_defaults(&source.config.into_connection_config()).await?;

    // Fetch replication state for all tables in this pipeline
    let state_rows = state::get_table_replication_state_rows(&source_pool, pipeline_id).await?;

    // Convert database states to UI-friendly format and fetch table names
    let mut tables: Vec<TableReplicationStatus> = Vec::new();
    for row in state_rows {
        let table_id = row.table_id.0;
        let table_name =
            get_table_name_from_oid(&source_pool, TableId::new(row.table_id.0)).await?;

        // Extract the metadata row from the database
        let table_replication_state = row
            .deserialize_metadata()
            .map_err(PipelineError::InvalidTableReplicationState)?
            .ok_or(PipelineError::MissingTableReplicationState)?;

        tables.push(TableReplicationStatus {
            table_id,
            table_name: table_name.to_string(),
            state: table_replication_state.into(),
        });
    }

    let response = GetPipelineReplicationStatusResponse {
        pipeline_id,
        table_statuses: tables,
    };

    Ok(Json(response))
}

#[utoipa::path(
    summary = "Roll back table state",
    description = "Rolls back the replication state of a specific table in the pipeline.",
    request_body = RollbackTableStateRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Table state rolled back successfully", body = RollbackTableStateResponse),
        (status = 400, description = "Bad request – state not rollbackable", body = ErrorMessage),
        (status = 404, description = "Pipeline or table not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/rollback-table-state")]
pub async fn rollback_table_state(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    pipeline_id: Path<i64>,
    rollback_request: Json<RollbackTableStateRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let table_id = rollback_request.table_id;
    let rollback_type = rollback_request.rollback_type;

    let mut txn = pool.begin().await?;

    // Read the pipeline to ensure it exists and get the source configuration
    let pipeline = db::pipelines::read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    // Get the source configuration
    let source = db::sources::read_source(
        txn.deref_mut(),
        tenant_id,
        pipeline.source_id,
        &encryption_key,
    )
    .await?
    .ok_or(PipelineError::SourceNotFound(pipeline.source_id))?;

    txn.commit().await?;

    // Connect to the source database to perform rollback
    let source_pool =
        connect_to_source_database_with_defaults(&source.config.into_connection_config()).await?;

    // First, check current state to ensure it's rollbackable (manual retry policy)
    let state_rows = state::get_table_replication_state_rows(&source_pool, pipeline_id).await?;
    let current_row = state_rows
        .into_iter()
        .find(|row| row.table_id.0 == table_id)
        .ok_or(PipelineError::MissingTableReplicationState)?;

    // Check if the current state is rollbackable (has ManualRetry policy)
    let current_state = current_row
        .deserialize_metadata()
        .map_err(PipelineError::InvalidTableReplicationState)?
        .ok_or(PipelineError::MissingTableReplicationState)?;
    if !current_state.supports_manual_retry() {
        return Err(PipelineError::NotRollbackable(
            "Only manual retry errors can be rolled back".to_string(),
        ));
    }

    let new_state_row = match rollback_type {
        RollbackType::Individual => {
            let Some(new_state_row) = state::rollback_replication_state(
                &source_pool,
                pipeline_id,
                TableId::new(table_id),
            )
            .await?
            else {
                return Err(PipelineError::NotRollbackable(
                    "No previous state to rollback to".to_string(),
                ));
            };

            new_state_row
        }
        RollbackType::Full => {
            state::reset_replication_state(&source_pool, pipeline_id, TableId::new(table_id))
                .await?
        }
    };

    // We extract the state from the metadata of the row
    let new_state = new_state_row
        .deserialize_metadata()
        .map_err(PipelineError::InvalidTableReplicationState)?
        .ok_or(PipelineError::MissingTableReplicationState)?;

    let response = RollbackTableStateResponse {
        pipeline_id,
        table_id,
        new_state: new_state.into(),
    };

    Ok(Json(response))
}

#[utoipa::path(
    summary = "Update pipeline image",
    description = "Updates the pipeline's container image while preserving its state.",
    request_body = UpdatePipelineImageRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline image updated successfully"),
        (status = 400, description = "Bad request or pipeline not running", body = ErrorMessage),
        (status = 404, description = "Pipeline or image not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/update-image")]
pub async fn update_pipeline_image(
    req: HttpRequest,
    pool: Data<PgPool>,
    encryption_key: Data<EncryptionKey>,
    k8s_client: Data<dyn K8sClient>,
    pipeline_id: Path<i64>,
    update_request: Json<UpdatePipelineImageRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let update_request = update_request.into_inner();
    let k8s_client = k8s_client.into_inner();

    let mut txn = pool.begin().await?;
    let (pipeline, replicator, current_image, source, destination) =
        read_all_required_data(&mut txn, tenant_id, pipeline_id, &encryption_key).await?;

    let target_image = match update_request.image_id {
        Some(image_id) => db::images::read_image(txn.deref_mut(), image_id)
            .await?
            .ok_or(PipelineError::ImageNotFoundById(image_id))?,
        None => db::images::read_default_image(txn.deref_mut())
            .await?
            .ok_or(PipelineError::NoDefaultImageFound)?,
    };

    // If the image ids are different, we change the database entry.
    if target_image.id != current_image.id {
        db::replicators::update_replicator_image(
            txn.deref_mut(),
            tenant_id,
            replicator.id,
            target_image.id,
        )
        .await?
        .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;
    }

    // If the images have equal name, we don't care about their id from the K8S perspective, so we
    // won't update any resources.
    if target_image.name == current_image.name {
        txn.commit().await?;

        return Ok(HttpResponse::Ok().finish());
    }

    // We update the pipeline in K8s if client is available.
    create_or_update_pipeline_in_k8s(
        k8s_client.as_ref(),
        tenant_id,
        pipeline,
        replicator,
        target_image,
        source,
        destination,
    )
    .await?;
    txn.commit().await?;

    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    summary = "Update pipeline config",
    description = "Updates the pipeline's configuration while preserving its running state.",
    context_path = "/v1",
    request_body = UpdatePipelineConfigRequest,
    params(
        ("pipeline_id" = i64, Path, description = "Unique ID of the pipeline"),
        ("tenant_id" = String, Header, description = "Tenant ID used to scope the request")
    ),
    responses(
        (status = 200, description = "Pipeline configuration updated successfully", body = UpdatePipelineConfigResponse),
        (status = 400, description = "Bad request or pipeline not running", body = ErrorMessage),
        (status = 404, description = "Pipeline not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage)
    ),
    tag = "Pipelines"
)]
#[post("/pipelines/{pipeline_id}/update-config")]
pub async fn update_pipeline_config(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline_id: Path<i64>,
    update_request: Json<UpdatePipelineConfigRequest>,
) -> Result<impl Responder, PipelineError> {
    let tenant_id = extract_tenant_id(&req)?;
    let pipeline_id = pipeline_id.into_inner();
    let update_request = update_request.into_inner();
    let mut txn = pool.begin().await?;

    let config = db::pipelines::update_pipeline_config(
        &mut txn,
        tenant_id,
        pipeline_id,
        update_request.config,
    )
    .await?
    .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    txn.commit().await?;

    let response = UpdatePipelineConfigResponse { config };

    Ok(Json(response))
}

#[derive(Debug, Serialize, Deserialize)]
struct Secrets {
    postgres_password: String,
    big_query_service_account_key: Option<String>,
}

async fn create_or_update_pipeline_in_k8s(
    k8s_client: &dyn K8sClient,
    tenant_id: &str,
    pipeline: Pipeline,
    replicator: Replicator,
    image: Image,
    source: Source,
    destination: Destination,
) -> Result<(), PipelineError> {
    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    // We create the secrets.
    let secrets = build_secrets(&source.config, &destination.config);
    create_or_update_secrets(k8s_client, &prefix, secrets).await?;

    // We create the replicator configuration.
    let replicator_config = build_replicator_config(
        k8s_client,
        source.config,
        destination.config,
        pipeline,
        SupabaseConfig {
            project_ref: tenant_id.to_owned(),
        },
    )
    .await?;
    create_or_update_config(k8s_client, &prefix, replicator_config).await?;

    // We create the replicator stateful set.
    create_or_update_replicator(k8s_client, &prefix, image.name).await?;

    Ok(())
}

async fn delete_pipeline_in_k8s(
    k8s_client: &dyn K8sClient,
    tenant_id: &str,
    replicator: Replicator,
) -> Result<(), PipelineError> {
    let prefix = create_k8s_object_prefix(tenant_id, replicator.id);

    delete_secrets(k8s_client, &prefix).await?;
    delete_config(k8s_client, &prefix).await?;
    delete_replicator(k8s_client, &prefix).await?;

    Ok(())
}

async fn read_all_required_data(
    txn: &mut PgTransaction<'_>,
    tenant_id: &str,
    pipeline_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<(Pipeline, Replicator, Image, Source, Destination), PipelineError> {
    let pipeline = db::pipelines::read_pipeline(txn.deref_mut(), tenant_id, pipeline_id)
        .await?
        .ok_or(PipelineError::PipelineNotFound(pipeline_id))?;

    let replicator =
        db::replicators::read_replicator_by_pipeline_id(txn.deref_mut(), tenant_id, pipeline_id)
            .await?
            .ok_or(PipelineError::ReplicatorNotFound(pipeline_id))?;

    let image = db::images::read_image_by_replicator_id(txn.deref_mut(), replicator.id)
        .await?
        .ok_or(PipelineError::ImageNotFound(replicator.id))?;

    let source_id = pipeline.source_id;
    let source = db::sources::read_source(txn.deref_mut(), tenant_id, source_id, encryption_key)
        .await?
        .ok_or(PipelineError::SourceNotFound(source_id))?;

    let destination_id = pipeline.destination_id;
    let destination = db::destinations::read_destination(
        txn.deref_mut(),
        tenant_id,
        destination_id,
        encryption_key,
    )
    .await?
    .ok_or(PipelineError::DestinationNotFound(destination_id))?;

    Ok((pipeline, replicator, image, source, destination))
}

fn build_secrets(source_config: &SourceConfig, destination_config: &DestinationConfig) -> Secrets {
    let postgres_password = source_config
        .password
        .as_ref()
        .map(|p| p.expose_secret().to_owned())
        .unwrap_or_default();
    let mut big_query_service_account_key = None;
    if let DestinationConfig::BigQuery {
        service_account_key,
        ..
    } = destination_config
    {
        big_query_service_account_key = Some(service_account_key.expose_secret().to_owned());
    };

    Secrets {
        postgres_password,
        big_query_service_account_key,
    }
}

async fn build_replicator_config(
    k8s_client: &dyn K8sClient,
    source_config: SourceConfig,
    destination_config: DestinationConfig,
    pipeline: Pipeline,
    supabase_config: SupabaseConfig,
) -> Result<ReplicatorConfig, PipelineError> {
    // We load the trusted root certificates from the config map.
    let trusted_root_certs = k8s_client
        .get_config_map(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
        .await?
        .data
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .get(TRUSTED_ROOT_CERT_KEY_NAME)
        .ok_or(PipelineError::TrustedRootCertsConfigMissing)?
        .clone();

    let pg_connection = PgConnectionConfig {
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
        pg_connection,
        // If these configs are not set, we default to the most recent default values.
        //
        // The reason for using `Option` fields in the config instead of automatically applying defaults
        // is that we want to persist a config in storage with some unset values, allowing us to interpret
        // them differently after deserialization without needing to run database migrations.
        batch: pipeline.config.batch.unwrap_or_default(),
        // Hardcoding 10s for now
        table_error_retry_delay_ms: pipeline.config.table_error_retry_delay_ms.unwrap_or(10000),
        // Hardcoding a value of 4 for now for maximum number of parallel table sync workers
        max_table_sync_workers: pipeline.config.max_table_sync_workers.unwrap_or(4),
    };

    let config = ReplicatorConfig {
        destination: destination_config,
        pipeline: pipeline_config,
        // The Sentry config will be injected via env variables for security purposes.
        sentry: None,
        supabase: Some(supabase_config),
    };

    Ok(config)
}

fn create_k8s_object_prefix(tenant_id: &str, replicator_id: i64) -> String {
    format!("{tenant_id}-{replicator_id}")
}

async fn create_or_update_secrets(
    k8s_client: &dyn K8sClient,
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
    k8s_client: &dyn K8sClient,
    prefix: &str,
    config: ReplicatorConfig,
) -> Result<(), PipelineError> {
    // For now the base config is empty.
    let base_config = "";
    let prod_config = serde_json::to_string(&config)?;
    k8s_client
        .create_or_update_config_map(prefix, base_config, &prod_config)
        .await?;

    Ok(())
}

async fn create_or_update_replicator(
    k8s_client: &dyn K8sClient,
    prefix: &str,
    replicator_image: String,
) -> Result<(), PipelineError> {
    k8s_client
        .create_or_update_stateful_set(prefix, &replicator_image)
        .await?;

    Ok(())
}

async fn delete_secrets(k8s_client: &dyn K8sClient, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_postgres_secret(prefix).await?;
    k8s_client.delete_bq_secret(prefix).await?;

    Ok(())
}

async fn delete_config(k8s_client: &dyn K8sClient, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_config_map(prefix).await?;

    Ok(())
}

async fn delete_replicator(k8s_client: &dyn K8sClient, prefix: &str) -> Result<(), PipelineError> {
    k8s_client.delete_stateful_set(prefix).await?;

    Ok(())
}
