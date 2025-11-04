use tracing::info;

/// Initializes the ConfigCat client for feature flag evaluation in the API.
pub fn init_feature_flags(
    configcat_sdk_key: Option<&str>,
) -> anyhow::Result<Option<configcat::Client>> {
    match configcat_sdk_key {
        Some(key) => {
            info!("initializing configcat with supplied sdk key");

            let builder = configcat::Client::builder(key);

            let client = builder.build()?;
            Ok(Some(client))
        }
        None => {
            info!("configcat not configured for api, skipping initialization");
            Ok(None)
        }
    }
}
