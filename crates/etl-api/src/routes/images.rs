use axum::{
    Extension, Json,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    data,
    data::images::ImagesDbError,
    routes::{ErrorMessage, IntoInner, error_response},
};

#[derive(Debug, Error)]
pub(crate) enum ImageError {
    #[error("The image with id {0} was not found")]
    ImageNotFound(i64),

    #[error(transparent)]
    ImagesDb(#[from] ImagesDbError),
}

impl ImageError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            ImageError::ImagesDb(ImagesDbError::Database(_)) => "Internal server error".to_owned(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl IntoResponse for ImageError {
    fn into_response(self) -> Response {
        let status_code = match &self {
            ImageError::ImagesDb(ImagesDbError::CannotDeleteDefault) => StatusCode::BAD_REQUEST,
            ImageError::ImagesDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ImageError::ImageNotFound(_) => StatusCode::NOT_FOUND,
        };

        error_response(status_code, self.to_message())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateImageRequest {
    #[schema(example = "supabase/replicator:1.2.3", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
    #[schema(example = true, required = true)]
    pub is_default: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateImageResponse {
    #[schema(example = 1)]
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateImageRequest {
    #[schema(example = "supabase/replicator:1.2.4", required = true)]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
    #[schema(example = false, required = true)]
    pub is_default: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadImageResponse {
    #[schema(example = 1)]
    pub id: i64,
    #[schema(example = "supabase/replicator:1.2.3")]
    pub name: String,
    #[schema(example = true)]
    pub is_default: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ReadImagesResponse {
    pub images: Vec<ReadImageResponse>,
}

#[utoipa::path(
    post,
    path = "/images",
    summary = "Create an image",
    description = "Creates an image entry; can be marked as the default.",
    request_body = CreateImageRequest,
    responses(
        (status = 200, description = "Image created successfully", body = CreateImageResponse),
        (status = 400, description = "Bad request", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Images"
)]
pub(crate) async fn create_image(
    Extension(pool): Extension<PgPool>,
    image: Json<CreateImageRequest>,
) -> Result<impl IntoResponse, ImageError> {
    let image = image.into_inner();

    let id = data::images::create_image(&pool, &image.name, image.is_default).await?;

    let response = CreateImageResponse { id };

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/images/{image_id}",
    summary = "Retrieve an image",
    description = "Returns an image identified by its ID.",
    params(
        ("image_id" = i64, Path, description = "Unique ID of the image"),
    ),
    responses(
        (status = 200, description = "Image retrieved successfully", body = ReadImageResponse),
        (status = 404, description = "Image not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Images"
)]
pub(crate) async fn read_image(
    Extension(pool): Extension<PgPool>,
    image_id: Path<i64>,
) -> Result<impl IntoResponse, ImageError> {
    let image_id = image_id.into_inner();

    let response = data::images::read_image(&pool, image_id)
        .await?
        .map(|s| ReadImageResponse { id: s.id, name: s.name, is_default: s.is_default })
        .ok_or(ImageError::ImageNotFound(image_id))?;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/images/{image_id}",
    summary = "Update an image",
    description = "Updates an image's name and default flag.",
    request_body = UpdateImageRequest,
    params(
        ("image_id" = i64, Path, description = "Unique ID of the image"),
    ),
    responses(
        (status = 200, description = "Image updated successfully"),
        (status = 404, description = "Image not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Images"
)]
pub(crate) async fn update_image(
    Extension(pool): Extension<PgPool>,
    image_id: Path<i64>,
    image: Json<UpdateImageRequest>,
) -> Result<impl IntoResponse, ImageError> {
    let image_id = image_id.into_inner();
    let image = image.into_inner();

    data::images::update_image(&pool, image_id, &image.name, image.is_default)
        .await?
        .ok_or(ImageError::ImageNotFound(image_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    delete,
    path = "/images/{image_id}",
    summary = "Delete an image",
    description = "Deletes an image by ID. Default images cannot be deleted.",
    params(
        ("image_id" = i64, Path, description = "Unique ID of the image"),
    ),
    responses(
        (status = 200, description = "Image deleted successfully"),
        (status = 404, description = "Image not found", body = ErrorMessage),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Images"
)]
pub(crate) async fn delete_image(
    Extension(pool): Extension<PgPool>,
    image_id: Path<i64>,
) -> Result<impl IntoResponse, ImageError> {
    let image_id = image_id.into_inner();

    data::images::delete_image(&pool, image_id)
        .await?
        .ok_or(ImageError::ImageNotFound(image_id))?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    get,
    path = "/images",
    summary = "List images",
    description = "Returns all available images.",
    responses(
        (status = 200, description = "Images listed successfully", body = ReadImagesResponse),
        (status = 500, description = "Internal server error", body = ErrorMessage),
    ),
    tag = "Images"
)]
pub(crate) async fn read_all_images(
    Extension(pool): Extension<PgPool>,
) -> Result<impl IntoResponse, ImageError> {
    let mut images = vec![];
    for image in data::images::read_all_images(&pool).await? {
        let image =
            ReadImageResponse { id: image.id, name: image.name, is_default: image.is_default };
        images.push(image);
    }

    let response = ReadImagesResponse { images };

    Ok(Json(response))
}
