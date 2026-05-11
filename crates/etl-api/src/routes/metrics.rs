use actix_web::{Responder, get, web};
use metrics_exporter_prometheus::PrometheusHandle;

#[utoipa::path(
    summary = "Get prometheus metrics",
    description = "Returns prometheus metrics collected since the last call to this endpoint.",
    responses(
        (status = 200, description = "Metrics returned successfully", body = String),
    ),
    tag = "Metrics"
)]
#[get("/metrics")]
pub(crate) async fn metrics(metrics_handle: web::ThinData<PrometheusHandle>) -> impl Responder {
    metrics_handle.render()
}
