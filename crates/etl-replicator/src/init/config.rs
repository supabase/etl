use etl_config::{load_config, shared::ReplicatorConfig};

use crate::error::{ReplicatorError, ReplicatorResult};

/// Loads and validates the replicator configuration.
pub(crate) fn init() -> ReplicatorResult<ReplicatorConfig> {
    let config = load_config::<ReplicatorConfig>().map_err(ReplicatorError::config)?;
    config.validate().map_err(ReplicatorError::config)?;

    Ok(config)
}
