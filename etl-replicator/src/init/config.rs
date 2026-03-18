use etl_config::load_config;
use etl_config::shared::ReplicatorConfig;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Loads and validates the replicator configuration.
pub fn init() -> ReplicatorResult<ReplicatorConfig> {
    let config = load_config::<ReplicatorConfig>().map_err(ReplicatorError::config)?;
    config.validate().map_err(ReplicatorError::config)?;

    Ok(config)
}
