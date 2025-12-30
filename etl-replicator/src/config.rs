use etl_config::load_config;
use etl_config::shared::ReplicatorConfig;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Loads and validates the replicator configuration.
///
/// Uses the standard configuration loading mechanism from [`etl_config`] and
/// validates the resulting [`ReplicatorConfig`] before returning it.
pub fn load_replicator_config() -> ReplicatorResult<ReplicatorConfig> {
    let config = load_config::<ReplicatorConfig>().map_err(ReplicatorError::config)?;
    config.validate().map_err(ReplicatorError::config)?;

    Ok(config)
}
