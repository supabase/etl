use tracing::info;

/// Initializes the ConfigCat client for feature flag evaluation.
///
/// Creates and returns a ConfigCat client if an SDK key is provided in the configuration.
/// If a project reference is provided, it will be set as the default user identifier for feature flag targeting.
/// If no SDK key is configured, returns [`None`] and the API operates without feature flag support.
pub fn init_feature_flags(
    configcat_sdk_key: Option<&str>,
) -> Option<configcat::Client> {
    match configcat_sdk_key {
        Some(key) => {
            info!("initializing configcat with supplied sdk key");

            let builder = configcat::Client::builder(key);
            match builder.build() {
                Ok(client) => Some(client),
                Err(err) => {
                    tracing::error!("failed to initialize configcat client: {err}");
                    None
                }
            }
        }
        None => {
            info!("configcat not configured for api, skipping initialization");
            None
        }
    }
}
