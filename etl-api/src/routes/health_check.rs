use actix_web::{HttpResponse, Responder, get};

#[utoipa::path(
    tag = "Health",
    responses(
        (status = 200, description = "API is healthy", body = String),
    )
)]
#[get("/health_check")]
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("ok")
}
