use actix_web::{
    delete, get,
    http::{header::ContentType, StatusCode},
    post,
    web::{Data, Json, Path},
    HttpResponse, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use utoipa::ToSchema;

use crate::db;
use crate::db::images::ImagesDbError;
use crate::routes::ErrorMessage;

#[derive(Debug, Error)]
enum ImageError {
    #[error("The image with id {0} was not found")]
    ImageNotFound(i64),

    #[error(transparent)]
    ImagesDb(#[from] ImagesDbError),
}

impl ImageError {
    fn to_message(&self) -> String {
        match self {
            // Do not expose internal database details in error messages
            ImageError::ImagesDb(ImagesDbError::Database(_)) => "internal server error".to_string(),
            // Every other message is ok, as they do not divulge sensitive information
            e => e.to_string(),
        }
    }
}

impl ResponseError for ImageError {
    fn status_code(&self) -> StatusCode {
        match self {
            ImageError::ImagesDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ImageError::ImageNotFound(_) => StatusCode::NOT_FOUND,
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
pub struct PostImageRequest {
    #[schema(example = "supabase/replicator:1.2.3")]
    pub name: String,
    #[schema(example = true)]
    pub is_default: bool,
}

#[derive(Serialize, ToSchema)]
pub struct PostImageResponse {
    id: i64,
}

#[derive(Serialize, ToSchema)]
pub struct GetImageResponse {
    id: i64,
    name: String,
    is_default: bool,
}

#[derive(Serialize, ToSchema)]
pub struct GetImagesResponse {
    images: Vec<GetImageResponse>,
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostImageRequest,
    responses(
        (status = 200, description = "Create new image", body = PostImageResponse),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/images")]
pub async fn create_image(
    pool: Data<PgPool>,
    image: Json<PostImageRequest>,
) -> Result<impl Responder, ImageError> {
    let image = image.0;
    let id = db::images::create_image(&pool, &image.name, image.is_default).await?;
    let response = PostImageResponse { id };
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("image_id" = i64, Path, description = "Id of the image"),
    ),
    responses(
        (status = 200, description = "Return image with id = image_id", body = GetImageResponse),
        (status = 404, description = "Image not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/images/{image_id}")]
pub async fn read_image(
    pool: Data<PgPool>,
    image_id: Path<i64>,
) -> Result<impl Responder, ImageError> {
    let image_id = image_id.into_inner();
    let response = db::images::read_image(&pool, image_id)
        .await?
        .map(|s| GetImageResponse {
            id: s.id,
            name: s.name,
            is_default: s.is_default,
        })
        .ok_or(ImageError::ImageNotFound(image_id))?;
    Ok(Json(response))
}

#[utoipa::path(
    context_path = "/v1",
    request_body = PostImageRequest,
    params(
        ("image_id" = i64, Path, description = "Id of the image"),
    ),
    responses(
        (status = 200, description = "Update image with id = image_id"),
        (status = 404, description = "Image not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[post("/images/{image_id}")]
pub async fn update_image(
    pool: Data<PgPool>,
    image_id: Path<i64>,
    image: Json<PostImageRequest>,
) -> Result<impl Responder, ImageError> {
    let image_id = image_id.into_inner();
    db::images::update_image(&pool, image_id, &image.name, image.is_default)
        .await?
        .ok_or(ImageError::ImageNotFound(image_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    params(
        ("image_id" = i64, Path, description = "Id of the image"),
    ),
    responses(
        (status = 200, description = "Delete image with id = image_id"),
        (status = 404, description = "Image not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[delete("/images/{image_id}")]
pub async fn delete_image(
    pool: Data<PgPool>,
    image_id: Path<i64>,
) -> Result<impl Responder, ImageError> {
    let image_id = image_id.into_inner();
    db::images::delete_image(&pool, image_id)
        .await?
        .ok_or(ImageError::ImageNotFound(image_id))?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    context_path = "/v1",
    responses(
        (status = 200, description = "Return all images"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/images")]
pub async fn read_all_images(pool: Data<PgPool>) -> Result<impl Responder, ImageError> {
    let mut images = vec![];
    for image in db::images::read_all_images(&pool).await? {
        let image = GetImageResponse {
            id: image.id,
            name: image.name,
            is_default: image.is_default,
        };
        images.push(image);
    }
    let response = GetImagesResponse { images };
    Ok(Json(response))
}
