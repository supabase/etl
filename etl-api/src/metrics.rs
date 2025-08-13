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

pub fn init_metrics() -> Result<PrometheusHandle, BuildError> {
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
