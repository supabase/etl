use config::load_config;
use config::shared::ReplicatorConfig;

/// Loads the [`ReplicatorConfig`] and validates it.
pub fn load_replicator_config() -> anyhow::Result<ReplicatorConfig> {
    let config = load_config::<ReplicatorConfig>()?;
    config.validate()?;

    Ok(config)
}
