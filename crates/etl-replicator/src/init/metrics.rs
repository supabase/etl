use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;

use crate::{
    error::{ReplicatorError, ReplicatorResult},
    init,
};

/// Initializes the Prometheus recorder and HTTP listener.
pub(crate) fn init(replicator_config: &ReplicatorConfig) -> ReplicatorResult<()> {
    init_metrics(
        replicator_config.project_ref(),
        Some(replicator_config.pipeline.id),
        Some(init::destination_name(&replicator_config.destination)),
    )
    .map_err(ReplicatorError::config)
}
