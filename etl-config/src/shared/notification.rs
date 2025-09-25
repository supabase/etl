use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Configuration for sending notifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationsConfig {
    /// Emails configuration for notifications.
    pub email: EmailNotificationConfig,
}

/// Configuration for sending error notifications via email.
///
/// Carries the endpoint information and template details required to enqueue
/// emails whenever the replicator reports a failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailNotificationConfig {
    /// Base URL of the environment-specific API that receives email requests.
    pub base_url: String,
    /// The API key to authenticate with the API that sends emails.
    pub api_key: String,
    /// Email addresses that should receive the notification.
    pub addresses: Vec<String>,
    /// Postmark template alias that should be used to render the message.
    pub template_alias: String,
    /// Additional properties passed with every notification.
    #[serde(default)]
    pub custom_properties: BTreeMap<String, Value>,
}
