use tracing::info;

use crate::error::{ReplicatorError, ReplicatorResult};

/// Initializes the ConfigCat client for feature flag evaluation in the replicator.
pub fn init_feature_flags(
    configcat_sdk_key: &str,
    project_ref: Option<&str>,
) -> ReplicatorResult<configcat::Client> {
    info!("initializing configcat with supplied sdk key");

    let builder = configcat::Client::builder(configcat_sdk_key);

    let builder = if let Some(project_ref) = project_ref {
        info!("setting project_ref as default user for feature flags");
        let user = configcat::User::new(project_ref);
        builder.default_user(user)
    } else {
        builder
    };

    let client = builder.build().map_err(ReplicatorError::config)?;
    Ok(client)
}
