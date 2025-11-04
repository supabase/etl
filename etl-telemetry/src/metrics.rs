use std::{sync::Mutex, time::Duration};

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use tracing::trace;

// Global cache for the Prometheus handle used by [`init_metrics_handle`].
//
// A [`Mutex`] is used instead of [`Once`], [`OnceCell`], or [`OnceLock`] because the
// initialization code is fallible. Ideally, we would use `OnceLock::get_or_try_init`,
// which allows fallible initialization, but it is currently unstable.
//
// The reason we must initialize only once is that [`PrometheusBuilder::install_recorder`]
// installs a global metrics recorder, and any later calls to it fail. While
// [`init_metrics`] is not called multiple times during normal operations, it is called
// multiple times during tests, so this caching mechanism is essential.
static PROMETHEUS_HANDLE: Mutex<Option<PrometheusHandle>> = Mutex::new(None);

/// Initializes metrics with manual endpoint management and returns a handle for rendering.
///
/// This function is designed for web services that need to integrate metrics into their
/// existing HTTP framework (e.g., Actix Web, Axum). Unlike [`init_metrics`], this does
/// not automatically start an HTTP server. Instead, it returns a [`PrometheusHandle`]
/// that the caller uses to manually render metrics at a custom endpoint.
///
/// # Thread Safety
///
/// Multiple threads can safely call this method to get a handle. Initialization happens
/// only once, and subsequent calls return cloned handles from the cache.
///
/// # Use Case
///
/// Use this when you want to:
/// - Integrate metrics into an existing web framework.
/// - Control the metrics endpoint path (e.g., `/metrics`, `/v1/metrics`).
/// - Apply middleware or authentication to the metrics endpoint.
pub fn init_metrics_handle() -> Result<PrometheusHandle, BuildError> {
    let mut prometheus_handle = PROMETHEUS_HANDLE.lock().unwrap();

    if let Some(handle) = &*prometheus_handle {
        return Ok(handle.clone());
    }

    let builder = PrometheusBuilder::new();

    let handle = builder.install_recorder()?;
    *prometheus_handle = Some(handle.clone());

    let handle_clone = handle.clone();

    // This task periodically performs upkeep to avoid unbounded memory growth due to
    // metrics collection.
    tokio::spawn(async move {
        loop {
            // upkeep_timeout hardcoded for now. Will make it configurable later if it creates a problem
            let upkeep_timeout = Duration::from_secs(5);
            tokio::time::sleep(upkeep_timeout).await;
            trace!("running metrics upkeep");
            handle_clone.run_upkeep();
        }
    });

    Ok(handle)
}

/// Initializes metrics with an automatic HTTP server on port 9000.
///
/// This function is designed for standalone services where metrics should be exposed
/// automatically without manual endpoint management. It installs a global metrics recorder
/// and starts an HTTP server that listens on `[::]:9000/metrics`, making metrics available
/// for Prometheus scraping.
///
/// # Use Case
///
/// Use this when you want to:
/// - Expose metrics from a standalone service (e.g., etl-replicator).
/// - Automatically start a dedicated metrics endpoint without custom routing.
/// - Let Prometheus scrape metrics directly from a fixed port.
pub fn init_metrics(project_ref: Option<&str>) -> Result<(), BuildError> {
    let mut builder = PrometheusBuilder::new().with_http_listener(std::net::SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        9000,
    ));

    if let Some(project_ref) = project_ref {
        builder = builder.add_global_label("project", project_ref);
    }

    builder.install()?;

    Ok(())
}
