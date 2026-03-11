use etl_config::shared::ReplicatorConfig;
use tracing::debug;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes the ConfigCat client for feature flag evaluation in the replicator.
///
/// This must run after the Tokio runtime has started because ConfigCat spins
/// up its auto-polling task during client construction.
pub fn init(replicator_config: &ReplicatorConfig) -> ReplicatorResult<Option<configcat::Client>> {
    let configcat_sdk_key = replicator_config
        .supabase
        .as_ref()
        .and_then(|supabase_config| supabase_config.configcat_sdk_key.as_deref());

    let Some(configcat_sdk_key) = configcat_sdk_key else {
        debug!("configcat not configured for replicator, skipping initialization");

        return Ok(None);
    };

    debug!("initializing configcat with supplied sdk key");

    let builder = configcat::Client::builder(configcat_sdk_key);

    let builder = if let Some(project_ref) = replicator_config.project_ref() {
        debug!("setting project_ref as default user for feature flags");
        let user = configcat::User::new(project_ref);
        builder.default_user(user)
    } else {
        builder
    };

    let client = builder.build().map_err(ReplicatorError::config)?;
    Ok(Some(client))
}
