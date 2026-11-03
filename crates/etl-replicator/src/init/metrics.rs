use std::io;

use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;
use tokio_util::task::AbortOnDropHandle;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes the Prometheus recorder and HTTP listener.
pub(crate) fn init(
    replicator_config: &ReplicatorConfig,
) -> ReplicatorResult<AbortOnDropHandle<io::Result<()>>> {
    let pipeline_id = replicator_config.pipeline.id.to_string();
    init_metrics(
        replicator_config.project_ref(),
        Some(&pipeline_id),
        Some(replicator_config.destination.kind().as_str()),
    )
    .map_err(ReplicatorError::config)
}
