use etl_config::shared::ReplicatorConfig;
use etl_telemetry::tracing::{LogFlusher, init_tracing_with_top_level_fields};

use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes tracing with top-level fields from replicator config.
pub fn init(replicator_config: &ReplicatorConfig) -> ReplicatorResult<LogFlusher> {
    init_tracing_with_top_level_fields(
        env!("CARGO_BIN_NAME"),
        replicator_config.project_ref(),
        Some(replicator_config.pipeline.id),
    )
    .map_err(ReplicatorError::config)
}
