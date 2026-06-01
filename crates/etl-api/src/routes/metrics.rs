use axum::{Extension, response::IntoResponse};
use metrics_exporter_prometheus::PrometheusHandle;

#[utoipa::path(
    get,
    path = "/metrics",
    summary = "Get prometheus metrics",
    description = "Returns prometheus metrics collected since the last call to this endpoint.",
    responses(
        (status = 200, description = "Metrics returned successfully", body = String),
    ),
    tag = "Metrics"
)]
pub(crate) async fn metrics(
    Extension(metrics_handle): Extension<PrometheusHandle>,
) -> impl IntoResponse {
    metrics_handle.render()
}
