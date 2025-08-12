use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

pub fn init_metrics() -> Result<PrometheusHandle, BuildError> {
    // TODO: run upkeep task at regular intervals
    PrometheusBuilder::new().install_recorder()
}
