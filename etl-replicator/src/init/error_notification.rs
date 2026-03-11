use etl_config::shared::ReplicatorConfig;
use secrecy::ExposeSecret;
use tracing::warn;

use crate::error_notification::ErrorNotificationClient;

/// Initializes the optional error notification client from replicator config.
pub fn init(replicator_config: &ReplicatorConfig) -> Option<ErrorNotificationClient> {
    replicator_config.supabase.as_ref().and_then(
        |supabase_config| match (&supabase_config.api_url, &supabase_config.api_key) {
            (Some(api_url), Some(api_key)) => Some(ErrorNotificationClient::new(
                api_url.clone(),
                api_key.expose_secret().to_owned(),
                supabase_config.project_ref.clone(),
                replicator_config.pipeline.id.to_string(),
            )),
            _ => {
                warn!(
                    "missing supabase api url and/or key, failure notifications will not be sent"
                );

                None
            }
        },
    )
}
