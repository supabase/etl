use std::{
    io,
    sync::{Mutex, PoisonError},
    time::Duration,
};

use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::any,
};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use thiserror::Error;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, trace};

use crate::listener::bind_listener;

/// HTTP port for the standalone metrics endpoint.
const METRICS_PORT: u16 = 9000;

/// Interval for maintaining the recorder's metric storage.
const UPKEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Errors while initializing the standalone metrics endpoint.
#[derive(Debug, Error)]
pub enum MetricsError {
    /// The HTTP listener could not be bound.
    #[error("Failed to bind metrics listener")]
    Listener(#[source] io::Error),
    /// The recorder or exporter could not be installed.
    #[error(transparent)]
    Build(#[from] BuildError),
}

// Global cache for the Prometheus handle used by [`init_metrics_handle`].
//
// A [`Mutex`] is used instead of [`Once`], [`OnceCell`], or [`OnceLock`]
// because the initialization code is fallible. Ideally, we would use
// `OnceLock::get_or_try_init`, which allows fallible initialization, but it is
// currently unstable.
//
// The reason we must initialize only once is that
// [`PrometheusBuilder::install_recorder`] installs a global metrics recorder,
// and any later calls to it fail. While [`init_metrics`] is not called multiple
// times during normal operations, it is called multiple times during tests, so
// this caching mechanism is essential.
static PROMETHEUS_HANDLE: Mutex<Option<PrometheusHandle>> = Mutex::new(None);
/// Global handle for the Prometheus upkeep task.
static PROMETHEUS_UPKEEP_TASK: Mutex<Option<AbortOnDropHandle<()>>> = Mutex::new(None);

/// Initializes metrics with manual endpoint management and returns a handle for
/// rendering.
///
/// This function is designed for web services that need to integrate metrics
/// into their existing HTTP framework (e.g., Actix Web, Axum). Unlike
/// [`init_metrics`], this does not automatically start an HTTP server. Instead,
/// it returns a [`PrometheusHandle`] that the caller uses to manually render
/// metrics at a custom endpoint.
///
/// # Thread Safety
///
/// Multiple threads can safely call this method to get a handle. Initialization
/// happens only once, and subsequent calls return cloned handles from the
/// cache.
///
/// # Use Case
///
/// Use this when you want to:
/// - Integrate metrics into an existing web framework.
/// - Control the metrics endpoint path (e.g., `/metrics`, `/v1/metrics`).
/// - Apply middleware or authentication to the metrics endpoint.
pub fn init_metrics_handle() -> Result<PrometheusHandle, BuildError> {
    let mut prometheus_handle = PROMETHEUS_HANDLE
        .lock()
        // We still get the poisoned lock since we assume that a poisoned lock doesn't invalidate
        // the handle contents.
        .unwrap_or_else(PoisonError::into_inner);

    if let Some(handle) = &*prometheus_handle {
        return Ok(handle.clone());
    }

    let builder = PrometheusBuilder::new();

    let handle = builder.install_recorder()?;
    *prometheus_handle = Some(handle.clone());

    let handle_clone = handle.clone();

    // This task periodically performs upkeep to avoid unbounded memory growth
    // due to metrics collection.
    let upkeep_task = AbortOnDropHandle::new(tokio::spawn(async move {
        loop {
            tokio::time::sleep(UPKEEP_INTERVAL).await;
            trace!("running metrics upkeep");
            handle_clone.run_upkeep();
        }
    }));
    *PROMETHEUS_UPKEEP_TASK.lock().unwrap_or_else(PoisonError::into_inner) = Some(upkeep_task);

    Ok(handle)
}

/// Renders metrics without blocking the async runtime's worker threads.
async fn render_metrics(
    State(handle): State<PrometheusHandle>,
) -> Result<impl IntoResponse, StatusCode> {
    let body = tokio::task::spawn_blocking(move || handle.render()).await.map_err(|error| {
        error!(error = %error, "metrics rendering failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(([(header::CONTENT_TYPE, "text/plain")], body))
}

/// Installs the recorder and serves metrics on port 9000 over IPv4 and IPv6.
///
/// When provided, `project_ref`, `pipeline_id`, and `destination` are attached
/// as global labels to all exported metrics for the current process.
///
/// Must be called inside a Tokio runtime. The caller owns the returned server
/// task, which also performs recorder upkeep, and must stop it on shutdown.
pub fn init_metrics(
    project_ref: Option<&str>,
    pipeline_id: Option<u64>,
    destination: Option<&str>,
) -> Result<AbortOnDropHandle<io::Result<()>>, MetricsError> {
    let listener = bind_listener(METRICS_PORT).map_err(MetricsError::Listener)?;
    let mut builder = PrometheusBuilder::new();

    if let Some(project_ref) = project_ref {
        builder = builder.add_global_label("project", project_ref);
    }

    if let Some(pipeline_id) = pipeline_id {
        builder = builder.add_global_label("pipeline_id", pipeline_id.to_string());
    }

    if let Some(destination) = destination {
        builder = builder.add_global_label("destination", destination);
    }

    let handle = builder.install_recorder()?;
    // Preserve the exporter's health route and metrics on every other path.
    let router = Router::new()
        .route("/health", any(|| async { ([(header::CONTENT_TYPE, "text/plain")], "OK") }))
        .fallback(render_metrics)
        .with_state(handle.clone());

    let metrics_http_listener = AbortOnDropHandle::new(tokio::spawn(async move {
        let server = axum::serve(listener, router).into_future();
        tokio::pin!(server);

        loop {
            tokio::select! {
                result = &mut server => return result,

                _ = tokio::time::sleep(UPKEEP_INTERVAL) => {
                    trace!("running metrics upkeep");

                    handle.run_upkeep();
                }
            }
        }
    }));

    Ok(metrics_http_listener)
}
