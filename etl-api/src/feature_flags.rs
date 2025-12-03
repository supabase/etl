use actix_web::web::Data;
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

/// Returns the maximum number of pipelines allowed per tenant.
///
/// Checks the `maximumPipelinesPerTenant` feature flag and falls back to
/// the default if the flag is not set or the client is unavailable.
pub async fn get_max_pipelines_per_tenant(
    client: Option<&Data<configcat::Client>>,
    tenant_id: &str,
    default_value: i64,
) -> i64 {
    match client {
        Some(client) => {
            let user = configcat::User::new(tenant_id);
            client.get_value("maximumPipelinesPerTenant", default_value, Some(user))
                .await
        }
        None => default_value,
    }
}
