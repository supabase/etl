use etl_config::load_config;
use etl_config::shared::ReplicatorConfig;

/// Loads and validates the replicator configuration.
///
/// Uses the standard configuration loading mechanism from [`etl_config`] and
/// validates the resulting [`ReplicatorConfig`] before returning it.
pub fn load_replicator_config() -> anyhow::Result<ReplicatorConfig> {
    let config = load_config::<ReplicatorConfig>()?;
    config.validate()?;

    Ok(config)
}
