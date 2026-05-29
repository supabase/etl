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
                    "ClickHouse URL must use 'https'. Got '{scheme}'. ETL requires TLS for all \
                     ClickHouse destinations to keep credentials and replicated rows off the wire \
                     in cleartext."
                ),
            )]);
        }

        if self.user.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "ClickHouse User Required",
                "ClickHouse user must not be empty.",
            )]);
        }

        if self.database.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "ClickHouse Database Required",
                "ClickHouse database must not be empty.",
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
                "Cannot connect to ClickHouse.\n\nPlease verify:\n(1) The URL is correct and \
                 reachable\n(2) The user name is correct\n(3) The password is correct",
            )]);
        }

        match client.database_exists(&self.database).await {
            Ok(true) => Ok(vec![]),
            Ok(false) => Ok(vec![ValidationFailure::critical(
                "ClickHouse Database Not Found",
                format!(
                    "ClickHouse database '{}' does not exist.\n\nPlease verify:\n(1) The database \
                     name is correct\n(2) The database has been created on the server\n(3) The \
                     user has permission to use it",
                    self.database
                ),
            )]),
            Err(_) => Ok(vec![ValidationFailure::critical(
                "ClickHouse Connection Failed",
                "Cannot query ClickHouse to confirm the target database exists.\n\nPlease \
                 verify:\n(1) The user has permission to read system.databases\n(2) The server is \
                 healthy and reachable",
            )]),
        }
    }
}
