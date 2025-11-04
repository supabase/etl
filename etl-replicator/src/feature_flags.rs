use tracing::info;

/// Initializes the ConfigCat client for feature flag evaluation.
///
/// Creates and returns a ConfigCat client if an SDK key is provided in the configuration.
/// If a project reference is provided, it will be set as the default user identifier for feature flag targeting.
/// If no SDK key is configured, returns [`None`] and the replicator operates without feature flag support.
pub fn init_feature_flags(
    configcat_sdk_key: &str,
    project_ref: Option<&str>,
) -> anyhow::Result<configcat::Client> {
    info!("initializing configcat with supplied sdk key");

    let builder = configcat::Client::builder(configcat_sdk_key);

    let builder = if let Some(project_ref) = project_ref {
        info!("setting project_ref as default user for feature flags");
        let user = configcat::User::new(project_ref);
        builder.default_user(user)
    } else {
        builder
    };

    let client = builder.build()?;
    Ok(client)
}
