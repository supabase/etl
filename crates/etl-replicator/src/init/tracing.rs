use etl_config::shared::ReplicatorConfig;
use etl_telemetry::tracing::{LogFlusher, init_tracing_with_top_level_fields};

use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes tracing with top-level fields from replicator config.
pub(crate) fn init(replicator_config: &ReplicatorConfig) -> ReplicatorResult<LogFlusher> {
    let pipeline_id = replicator_config.pipeline.id.to_string();
    init_tracing_with_top_level_fields(
        env!("CARGO_BIN_NAME"),
        replicator_config.project_ref(),
        Some(&pipeline_id),
    )
    .map_err(ReplicatorError::config)
}
