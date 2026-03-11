//! Metrics initialization for the replicator.

use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes the Prometheus recorder and HTTP listener.
pub fn init(replicator_config: &ReplicatorConfig) -> ReplicatorResult<()> {
    init_metrics(replicator_config.project_ref()).map_err(ReplicatorError::config)
}
