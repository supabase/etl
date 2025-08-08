use actix_web::{HttpResponse, Responder, get};

#[utoipa::path(
    summary = "API health status",
    description = "Returns 'ok' when the API is available and responding.",
    responses(
        (status = 200, description = "Health check passed; returns 'ok'.", body = String),
    ),
    tag = "Health",
)]
#[get("/health_check")]
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("ok")
}
