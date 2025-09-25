use anyhow::Context;
use etl_config::shared::{EmailNotificationConfig, ReplicatorConfig, SupabaseConfig};
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// Represents the JSON payload accepted by the email service.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct SendEmailRequest {
    /// Target email addresses.
    addresses: Vec<String>,
    /// Postmark template alias.
    template_alias: String,
    /// Arbitrary template variables.
    custom_properties: BTreeMap<String, Value>,
}

/// Dispatches an email notification when the replicator terminates with an error.
///
/// The notification is only sent when the `email_notifications` configuration is present.
/// Any failure encountered while preparing or sending the request is surfaced to the caller
/// so it can be logged without masking the original error.
pub fn send_error_notification(
    replicator_config: &ReplicatorConfig,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    let Some(supabase_config) = &replicator_config.supabase else {
        return Ok(());
    };

    let Some(email_config) = supabase_config.notifications.as_ref().map(|n| &n.email) else {
        return Ok(());
    };

    let payload = build_send_email_request(supabase_config, email_config, replicator_config, error);

    let client = Client::builder()
        .build()
        .context("failed to build HTTP client for email notifications")?;

    let endpoint = format!(
        "{}/system/email/send",
        email_config.base_url.trim_end_matches('/'),
    );

    let response = client
        .post(&endpoint)
        .json(&payload)
        .send()
        .with_context(|| format!("failed to send error notification to {endpoint}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .unwrap_or_else(|_| "<unavailable>".to_string());

        anyhow::bail!("email notification endpoint responded with {status} and body `{body}`",);
    }

    Ok(())
}

/// Builds the request payload combining static configuration with runtime context.
fn build_send_email_request(
    supabase_config: &SupabaseConfig,
    email_config: &EmailNotificationConfig,
    replicator_config: &ReplicatorConfig,
    error: &anyhow::Error,
) -> Option<SendEmailRequest> {
    let mut custom_properties = email_config.custom_properties.clone();
    custom_properties.insert(
        "project_ref".into(),
        Value::String(supabase_config.project_ref.clone()),
    );
    custom_properties.insert(
        "pipeline_id".into(),
        Value::String(replicator_config.pipeline.id.to_string()),
    );
    custom_properties.insert("error_message".into(), Value::String(error.to_string()));

    Some(SendEmailRequest {
        addresses: email_config.addresses.clone(),
        template_alias: email_config.template_alias.clone(),
        custom_properties,
    })
}
