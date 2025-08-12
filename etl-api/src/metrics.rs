use std::time::Duration;

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use tracing::trace;

pub fn init_metrics() -> Result<PrometheusHandle, BuildError> {
    let builder = PrometheusBuilder::new();

    let handle = builder.install_recorder()?;
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
