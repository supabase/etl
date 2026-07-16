use async_trait::async_trait;
use etl_config::SerializableSecretString;
use etl_destinations::clickhouse::{ClickHouseClient, ClickHouseClientConfig};
use secrecy::ExposeSecret;
use url::Url;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates a ClickHouse destination.
#[derive(Debug)]
pub(super) struct ClickHouseValidator {
    url: Url,
    user: String,
    password: Option<SerializableSecretString>,
    database: String,
}

impl ClickHouseValidator {
    pub(super) fn new(
        url: Url,
        user: String,
        password: Option<SerializableSecretString>,
        database: String,
    ) -> Self {
        Self { url, user, password, database }
    }
}

#[async_trait]
impl Validator for ClickHouseValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        // Structural pre-checks: cheap, no network. Bail before opening a
        // connection if the config can't possibly work.
        let scheme = self.url.scheme();
        if scheme != "https" {
            return Ok(vec![ValidationFailure::critical(
                "ClickHouse URL Invalid",
                format!(
                    "ClickHouse URL must use `https`, but this URL uses `{scheme}`.\n\nETL \
                     requires TLS to protect credentials and replicated rows. Update the \
                     destination URL to the ClickHouse HTTPS endpoint, including its port when \
                     required."
                ),
            )]);
        }

        if self.user.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "ClickHouse User Required",
                "Enter the ClickHouse user that ETL should connect with.",
            )]);
        }

        if self.database.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "ClickHouse Database Required",
                "Choose the ClickHouse database where replicated tables should be written.",
            )]);
        }

        // Probe with an unscoped client so a missing target database surfaces
        // as its own failure rather than masquerading as a connectivity error.
        let client = ClickHouseClient::new_without_database(
            self.url.clone(),
            self.user.clone(),
            self.password.as_ref().map(|password| password.expose_secret().to_owned()),
            ClickHouseClientConfig::default(),
        );

        if client.validate_connectivity().await.is_err() {
            return Ok(vec![ValidationFailure::critical(
                "ClickHouse Connection Failed",
                "We couldn't connect to ClickHouse with this URL and user.\n\nCheck that the host \
                 is reachable and that the username and password are correct.",
            )]);
        }

        match client.database_exists(&self.database).await {
            Ok(true) => Ok(vec![]),
            Ok(false) => Ok(vec![ValidationFailure::critical(
                "ClickHouse Database Not Found",
                format!(
                    "ClickHouse database `{}` was not found.\n\nCreate the database, or choose an \
                     existing database this user can access.",
                    self.database
                ),
            )]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "ClickHouse Connection Failed",
                "We connected to ClickHouse, but couldn't confirm whether the target database \
                 exists.\n\nGrant this user permission to read `system.databases` and try again.",
            )]),
        }
    }
}
