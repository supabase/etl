use etl::destination::Destination;
use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes the Prometheus recorder and HTTP listener.
pub(crate) fn init<D: Destination>(replicator_config: &ReplicatorConfig) -> ReplicatorResult<()> {
    init_metrics(
        replicator_config.project_ref(),
        Some(replicator_config.pipeline.id),
        Some(D::name()),
    )
    .map_err(ReplicatorError::config)
}
