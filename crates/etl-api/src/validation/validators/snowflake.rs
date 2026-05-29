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
                "Snowflake account identifier must not be empty.",
            )]);
        }

        if self.user.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Snowflake User Required",
                "Snowflake user must not be empty.",
            )]);
        }

        if self.database.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Snowflake Database Required",
                "Snowflake database must not be empty.",
            )]);
        }

        if self.schema.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Snowflake Schema Required",
                "Snowflake schema must not be empty.",
            )]);
        }

        let mut config =
            snowflake::Config::new(&self.account_id, &self.user, &self.database, &self.schema);
        if let Some(role) = &self.role {
            config = config.with_role(role);
        }

        match snowflake::Client::validate_connectivity(
            &config,
            self.private_key.expose_secret(),
            self.private_key_passphrase.as_deref(),
        )
        .await
        {
            Ok(()) => Ok(vec![]),
            Err(snowflake::Error::Auth(msg)) => Ok(vec![ValidationFailure::critical(
                "Snowflake Authentication Failed",
                format!(
                    "Failed to authenticate with Snowflake.\n\nPlease verify:\n(1) The private \
                     key is a valid PEM-encoded RSA key\n(2) The passphrase is correct (if the \
                     key is encrypted)\n(3) The key is registered for this user\n\nError: {msg}"
                ),
            )]),
            Err(snowflake::Error::DatabaseNotFound(db)) => Ok(vec![ValidationFailure::critical(
                "Snowflake Database Not Found",
                format!(
                    "Database '{db}' does not exist.\n\nPlease verify:\n(1) The database name is \
                     correct\n(2) The user has USAGE privilege on the database"
                ),
            )]),
            Err(snowflake::Error::SchemaNotFound { database, schema }) => {
                Ok(vec![ValidationFailure::critical(
                    "Snowflake Schema Not Found",
                    format!(
                        "Schema '{schema}' not found in database '{database}'.\n\nPlease \
                         verify:\n(1) The schema name is correct\n(2) The user has USAGE \
                         privilege on the schema"
                    ),
                )])
            }
            Err(err) => Ok(vec![ValidationFailure::critical(
                "Snowflake Connection Failed",
                format!(
                    "Cannot connect to Snowflake.\n\nPlease verify:\n(1) The account identifier \
                     is correct\n(2) Network connectivity to Snowflake\n\nError: {err}"
                ),
            )]),
        }
    }
}
