use async_trait::async_trait;
use etl_config::SerializableSecretString;
use etl_destinations::snowflake;
use secrecy::ExposeSecret;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// Validates a Snowflake destination.
#[derive(Debug)]
pub(super) struct SnowflakeValidator {
    account_id: String,
    user: String,
    private_key: SerializableSecretString,
    private_key_passphrase: Option<SerializableSecretString>,
    database: String,
    schema: String,
    role: Option<String>,
}

impl SnowflakeValidator {
    pub(super) fn new(
        account_id: String,
        user: String,
        private_key: SerializableSecretString,
        private_key_passphrase: Option<SerializableSecretString>,
        database: String,
        schema: String,
        role: Option<String>,
    ) -> Self {
        Self { account_id, user, private_key, private_key_passphrase, database, schema, role }
    }
}

#[async_trait]
impl Validator for SnowflakeValidator {
    async fn validate(
        &self,
        _ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        if self.account_id.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Snowflake Account ID Required",
                "Enter the Snowflake account identifier from your account URL or connection \
                 settings.",
            )]);
        }

        if self.user.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Snowflake User Required",
                "Enter the Snowflake user that ETL should connect with.",
            )]);
        }

        if self.database.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Snowflake Database Required",
                "Choose the Snowflake database where replicated tables should be written.",
            )]);
        }

        if self.schema.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Snowflake Schema Required",
                "Choose the Snowflake schema where replicated tables should be written.",
            )]);
        }

        let mut config = match snowflake::Config::new(
            &self.account_id,
            &self.user,
            &self.database,
            &self.schema,
        ) {
            Ok(config) => config,
            Err(err) => {
                return Ok(vec![ValidationFailure::critical(
                    "Invalid Snowflake Account ID",
                    format!(
                        "The Snowflake account identifier is not valid.\n\nUse the account \
                         identifier from your Snowflake account URL or connection settings. \
                         Snowflake returned: {err}"
                    ),
                )]);
            }
        };
        if let Some(role) = &self.role {
            config = config.with_role(role);
        }
        config = config.with_private_key(
            self.private_key.expose_secret().to_owned(),
            self.private_key_passphrase
                .as_ref()
                .map(|passphrase| passphrase.expose_secret().to_owned().into()),
        );

        match snowflake::Client::validate_connectivity(config).await {
            Ok(()) => Ok(vec![]),
            Err(snowflake::Error::Auth(msg)) => Ok(vec![ValidationFailure::critical(
                "Snowflake Authentication Failed",
                format!(
                    "We couldn't authenticate with Snowflake using this user and private \
                     key.\n\nCheck that the private key is a valid PEM-encoded RSA key, the \
                     passphrase is correct if the key is encrypted, and the public key is \
                     registered for this Snowflake user. Snowflake returned: {msg}"
                ),
            )]),
            Err(snowflake::Error::DatabaseNotFound(db)) => Ok(vec![ValidationFailure::critical(
                "Snowflake Database Not Found",
                format!(
                    "Snowflake database '{db}' was not found.\n\nCreate the database, choose an \
                     existing database, or grant this user USAGE on it."
                ),
            )]),
            Err(snowflake::Error::SchemaNotFound { database, schema }) => {
                Ok(vec![ValidationFailure::critical(
                    "Snowflake Schema Not Found",
                    format!(
                        "Snowflake schema '{schema}' was not found in database \
                         '{database}'.\n\nCreate the schema, choose an existing schema, or grant \
                         this user USAGE on it."
                    ),
                )])
            }
            Err(err) => Ok(vec![ValidationFailure::critical(
                "Snowflake Connection Failed",
                format!(
                    "We couldn't connect to Snowflake with this account identifier and \
                     user.\n\nCheck the account identifier, role, network access, and \
                     warehouse/database permissions. Snowflake returned: {err}"
                ),
            )]),
        }
    }
}
