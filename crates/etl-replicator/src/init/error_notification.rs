use etl_config::shared::{ReplicatorConfig, ValidationError};
use secrecy::ExposeSecret;

use crate::{
    error::{ReplicatorError, ReplicatorResult},
    error_notification::ErrorNotificationClient,
};

/// Initializes optional error notifications, rejecting incomplete credentials.
pub(crate) fn init(
    replicator_config: &ReplicatorConfig,
) -> ReplicatorResult<Option<ErrorNotificationClient>> {
    let Some(supabase_config) = &replicator_config.supabase else {
        return Ok(None);
    };
    let Some(api_url) = &supabase_config.api_url else {
        return Ok(None);
    };
    let api_key = supabase_config
        .api_key
        .as_ref()
        .filter(|key| !key.expose_secret().trim().is_empty())
        .ok_or_else(|| {
            ReplicatorError::config(ValidationError::InvalidFieldValue {
                field: "supabase.api_key".to_owned(),
                constraint: "must not be blank when supabase.api_url is configured".to_owned(),
            })
        })?;

    Ok(Some(ErrorNotificationClient::new(
        api_url.clone(),
        api_key.expose_secret().to_owned(),
        supabase_config.project_ref.clone(),
        replicator_config.pipeline.id.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use etl_config::shared::{
        ReplicatorConfig, ReplicatorConfigWithoutSecrets, SupabaseConfig, Validate,
    };
    use secrecy::SecretString;
    use serde_json::json;

    use crate::{error::ReplicatorError, init::error_notification::init};

    /// Only notification initialization requires credentials, not shared config
    /// validation.
    #[test]
    fn validates_notification_credentials_only_when_initializing_notifications() {
        let mut config: ReplicatorConfig = serde_json::from_value(json!({
            "destination": {"ducklake": {
                "catalog_url": "ducklake:postgres:host=example.com",
                "data_path": "s3://example-bucket/data",
                "maintenance_mode": "kubernetes"
            }},
            "pipeline": {
                "id": 1,
                "publication_name": "example_publication",
                "pg_connection": {
                    "host": "example.com",
                    "port": 5432,
                    "name": "example_database",
                    "username": "example_user",
                    "tls": {"enabled": false, "trusted_root_certs": ""}
                }
            }
        }))
        .unwrap();
        config.validate().unwrap();
        assert!(init(&config).unwrap().is_none());

        for (api_url, api_key, valid) in [
            (None, None, true),
            (None, Some(""), true),
            (None, Some(" \t\n"), true),
            (None, Some("placeholder-token"), true),
            (Some("https://example.com"), None, false),
            (Some("https://example.com"), Some(""), false),
            (Some("https://example.com"), Some(" \t\n"), false),
            (Some("https://example.com"), Some("placeholder-token"), true),
        ] {
            config.supabase = Some(SupabaseConfig {
                project_ref: "example-project".to_owned(),
                api_url: api_url.map(str::to_owned),
                api_key: api_key.map(SecretString::from),
                configcat_sdk_key: None,
            });

            // Maintenance jobs reuse this config without notification
            // credentials.
            config.validate().unwrap();
            ReplicatorConfigWithoutSecrets::from(config.clone()).validate().unwrap();

            if valid {
                assert_eq!(init(&config).unwrap().is_some(), api_url.is_some());
            } else {
                let error = init(&config).unwrap_err();
                assert!(matches!(error, ReplicatorError::Config(..)));
                assert_eq!(
                    error.to_string(),
                    "Configuration error: Field `supabase.api_key` must not be blank when \
                     supabase.api_url is configured"
                );
            }
        }
    }
}
