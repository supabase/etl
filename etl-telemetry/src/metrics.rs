use std::{sync::Mutex, time::Duration};

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use tracing::trace;

// One time initialization objects like Once, OnceCell, OnceLock or other variants
// are not used here and instead a mutex is used because the initialization code
// is fallible and ideally we'd like to use OnceLock::get_or_try_init which
// allows fallibe code to run as part of initialization but it is currently
// unstable.
//
// The reason we want to only initialize this once is because the call to
// builder.install_recorder installs a global metrics recorder and any subsequent
// calls to it fail. init_metrics will not be called multiple times during normal
// operations but is called multiple times during tests.
static PROMETHEUS_HANDLE: Mutex<Option<PrometheusHandle>> = Mutex::new(None);

/// This method initializes metrics by:
///
/// 1. Installing a global metrics recorder.
/// 2. Running a background task to run upkeep on the collected metrics to avoid unbounded memory growth
/// 3. Returning a handle to the recorder.
///
/// The handle can be used by the caller to render metrics in a /metrics endpoint.
/// Multiple threads can safely call this method to get a handle. This method ensures initialization
/// happens only once and return cloned handles to all callers.
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
    // metrics collection
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

/// This method initialized metrics by installing a global metrics recorder.
/// It also starts listening on an http endpoint at `0.0.0.0:9000/metrics`
/// for scrapers to collect metrics from. If the passed project_ref
/// is not none, it is set as a global label named "project".
pub fn init_metrics(project_ref: Option<String>) -> Result<(), BuildError> {
    let mut builder = PrometheusBuilder::new();

    if let Some(project_ref) = project_ref {
        builder = builder.add_global_label("project", project_ref);
    }

    builder.install()?;

    Ok(())
}
