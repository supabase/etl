use axum::{Extension, response::IntoResponse};
use metrics_exporter_prometheus::PrometheusHandle;

#[utoipa::path(
    get,
    path = "/metrics",
    summary = "Get prometheus metrics",
    description = "Returns the current prometheus metrics snapshot.",
    responses(
        (status = 200, description = "Metrics returned successfully", body = String),
    ),
    tag = "Metrics"
)]
pub(crate) async fn metrics(
    Extension(metrics_handle): Extension<PrometheusHandle>,
) -> impl IntoResponse {
    #[cfg(feature = "hotpath")]
    let rendered = etl_telemetry::profiling::render_metrics(&metrics_handle, &[]).await;
    #[cfg(not(feature = "hotpath"))]
    let rendered = metrics_handle.render();
    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], rendered)
}
