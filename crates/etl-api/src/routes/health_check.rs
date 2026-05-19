use axum::response::IntoResponse;

#[utoipa::path(
    get,
    path = "/health_check",
    summary = "API health status",
    description = "Returns 'ok' when the API is available and responding.",
    responses(
        (status = 200, description = "Health check passed; returns 'ok'.", body = String),
    ),
    tag = "Health",
)]
pub(crate) async fn health_check() -> impl IntoResponse {
    "ok"
}
