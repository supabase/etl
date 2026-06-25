use std::time::Duration;

use etl_config::shared::{ValidationError, validate_snowflake_account_id};
use secrecy::SecretString;

use crate::snowflake;

pub(crate) const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);

/// Connection parameters for a Snowflake account.
#[derive(Debug)]
pub struct Config {
    /// Full Snowflake account URL, always HTTPS.
    account_url: String,

    /// Snowflake account identifier (e.g. `ORGNAME-ACCTNAME`).
    ///
    /// Used in JWT claims and API routing.
    /// Uppercased internally where Snowflake requires it.
    pub(crate) account_id: String,

    /// Snowflake login name used for key-pair authentication.
    ///
    /// This is the user identity that owns the RSA key pair.
    pub(crate) username: String,

    /// Target database for all operations.
    pub(crate) database: String,

    /// Target schema within the database.
    pub(crate) schema: String,

    /// Snowflake role to assume after connecting.
    ///
    /// When `None`, the user's default role is used.
    pub(crate) role: Option<String>,

    /// PEM-encoded private key used to create an
    /// [`crate::snowflake::AuthManager`].
    private_key: Option<SecretString>,

    /// Passphrase for encrypted private keys.
    private_key_passphrase: Option<SecretString>,
}

impl Config {
    /// JSON Snowflake connection string used by tests and examples.
    pub const TESTS_CONNECTION_ENV: &str = "TESTS_SNOWFLAKE_CONNECTION";

    /// JSON Snowflake connection string used by benchmarks.
    pub const BENCH_CONNECTION_ENV: &str = "BENCH_SNOWFLAKE_CONNECTION";

    /// Create a config with the required connection parameters.
    pub fn new(
        account_id: &str,
        username: &str,
        database: &str,
        schema: &str,
    ) -> Result<Self, ValidationError> {
        // The account id is interpolated into the account URL, so this rejects values
        // that could take over the host (SSRF).
        validate_snowflake_account_id(account_id)?;

        let account_url = format!("https://{}.snowflakecomputing.com", account_id.to_uppercase());

        Ok(Self {
            account_url,
            account_id: account_id.to_owned(),
            username: username.to_owned(),
            database: database.to_owned(),
            schema: schema.to_owned(),
            role: None,
            private_key: None,
            private_key_passphrase: None,
        })
    }

    /// Attach key-pair authentication credentials.
    pub fn with_private_key(
        mut self,
        private_key: impl Into<SecretString>,
        private_key_passphrase: Option<SecretString>,
    ) -> Self {
        self.private_key = Some(private_key.into());
        self.private_key_passphrase = private_key_passphrase;
        self
    }

    /// Load required [`Self::TESTS_CONNECTION_ENV`] credentials.
    pub fn require_tests_env() -> snowflake::Result<Self> {
        Self::require_env(Self::TESTS_CONNECTION_ENV)
    }

    /// Load required [`Self::BENCH_CONNECTION_ENV`] credentials.
    pub fn require_bench_env() -> snowflake::Result<Self> {
        Self::require_env(Self::BENCH_CONNECTION_ENV)
    }

    /// Return a copy without authentication credentials.
    pub fn clone_without_credentials(&self) -> Self {
        Self {
            account_url: self.account_url.clone(),
            account_id: self.account_id.clone(),
            username: self.username.clone(),
            database: self.database.clone(),
            schema: self.schema.clone(),
            role: self.role.clone(),
            private_key: None,
            private_key_passphrase: None,
        }
    }

    /// Strip authentication credentials from this config.
    pub(crate) fn without_credentials(mut self) -> Self {
        self.private_key = None;
        self.private_key_passphrase = None;
        self
    }

    /// Remove and return required authentication credentials.
    pub(crate) fn take_credentials(
        &mut self,
    ) -> snowflake::Result<(SecretString, Option<SecretString>)> {
        let private_key = self.private_key.take().ok_or_else(|| {
            snowflake::Error::Config("Snowflake private key must be set".to_owned())
        })?;
        let private_key_passphrase = self.private_key_passphrase.take();

        Ok((private_key, private_key_passphrase))
    }

    /// Return the Snowflake account identifier.
    pub fn account(&self) -> &str {
        &self.account_id
    }

    /// Return the Snowflake user.
    pub fn user(&self) -> &str {
        &self.username
    }

    /// HTTPS-only account URL used for all API requests.
    pub fn account_url(&self) -> &str {
        &self.account_url
    }

    /// Target database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Target schema name.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Return the Snowflake role, when configured.
    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    /// Return the private key PEM text, when configured.
    pub fn private_key(&self) -> Option<&SecretString> {
        self.private_key.as_ref()
    }

    /// Return the private key passphrase, when configured.
    pub fn private_key_passphrase(&self) -> Option<&SecretString> {
        self.private_key_passphrase.as_ref()
    }

    /// Set the role to assume after connecting.
    pub fn with_role(mut self, role: &str) -> Self {
        self.role = Some(role.to_owned());
        self
    }

    /// Parse a JSON Snowflake connection string from `value` for `source_name`.
    fn parse_connection_json(value: &str, source_name: &str) -> snowflake::Result<Self> {
        let mut connection: ConnectionJson = serde_json::from_str(value).map_err(|error| {
            snowflake::Error::Config(format!(
                "{source_name} must be a JSON Snowflake connection string: {error}"
            ))
        })?;

        for (name, value) in [
            ("account", &connection.account),
            ("user", &connection.user),
            ("database", &connection.database),
            ("schema", &connection.schema),
            ("private_key", &connection.private_key),
        ] {
            if value.trim().is_empty() {
                return Err(snowflake::Error::Config(format!(
                    "{source_name} field `{name}` must not be empty"
                )));
            }
        }

        connection.role = normalize_optional(connection.role);
        connection.private_key_passphrase = normalize_optional(connection.private_key_passphrase);

        let mut config = Self::new(
            &connection.account,
            &connection.user,
            &connection.database,
            &connection.schema,
        )
        .map_err(|error| {
            snowflake::Error::Config(format!("Invalid Snowflake account identifier: {error}"))
        })?;

        if let Some(role) = connection.role {
            config = config.with_role(&role);
        }

        Ok(config.with_private_key(
            connection.private_key,
            connection.private_key_passphrase.map(Into::into),
        ))
    }

    /// Load and parse a JSON Snowflake connection string from `env_name`.
    fn from_env(env_name: &str) -> snowflake::Result<Option<Self>> {
        let Ok(value) = std::env::var(env_name) else {
            return Ok(None);
        };

        Self::parse_connection_json(&value, env_name).map(Some)
    }

    /// Load a required JSON Snowflake connection string from `env_name`.
    fn require_env(env_name: &str) -> snowflake::Result<Self> {
        Self::from_env(env_name)?
            .ok_or_else(|| snowflake::Error::Config(format!("{env_name} must be set")))
    }
}

/// JSON representation of Snowflake connection settings.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionJson {
    /// Snowflake account identifier.
    account: String,
    /// Snowflake login name.
    user: String,
    /// Target database for all operations.
    database: String,
    /// Target schema within the database.
    schema: String,
    /// Snowflake role to assume after connecting.
    role: Option<String>,
    /// PEM-encoded private key.
    private_key: String,
    /// Passphrase for encrypted private keys.
    private_key_passphrase: Option<String>,
}

/// Normalize optional connection fields by treating empty strings as absent.
fn normalize_optional(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    const PRIVATE_KEY: &str = "fake-private-key";

    #[test]
    fn new_builds_account_url_for_valid_id() {
        let config = Config::new("myorg-myaccount", "USER", "DB", "PUBLIC").expect("valid id");
        assert_eq!(config.account_url(), "https://MYORG-MYACCOUNT.snowflakecomputing.com");
    }

    #[test]
    fn new_rejects_injection_in_account_id() {
        for id in ["127.0.0.1:8443/x", "attacker.example/foo", "169.254.169.254#", "evil@host", ""]
        {
            assert!(
                Config::new(id, "USER", "DB", "PUBLIC").is_err(),
                "should reject account_id {id:?}"
            );
        }
    }

    #[test]
    fn parses_connection_json() {
        let config = Config::parse_connection_json(
            &format!(
                r#"{{
                    "account": "org-account",
                    "user": "etl_user",
                    "database": "ETL_DEV",
                    "schema": "PUBLIC",
                    "role": "ETL_TEST_ROLE",
                    "private_key_passphrase": "secret",
                    "private_key": {PRIVATE_KEY:?}
                }}"#,
            ),
            "Snowflake connection string",
        )
        .expect("connection should parse");

        assert_eq!(config.account(), "org-account");
        assert_eq!(config.user(), "etl_user");
        assert_eq!(config.database(), "ETL_DEV");
        assert_eq!(config.schema(), "PUBLIC");
        assert_eq!(config.role(), Some("ETL_TEST_ROLE"));
        assert_eq!(config.private_key().unwrap().expose_secret(), PRIVATE_KEY);
        assert_eq!(config.private_key_passphrase().unwrap().expose_secret(), "secret");
    }

    #[test]
    fn rejects_missing_required_fields() {
        for connection in [
            "{}",
            r#"{"account":"","user":"user","database":"db","schema":"schema","private_key":"key"}"#,
            r#"{"account":"org","user":"user","database":"db","schema":"schema","private_key":""}"#,
        ] {
            assert!(
                Config::parse_connection_json(connection, "Snowflake connection string").is_err()
            );
        }
    }

    #[test]
    fn clone_without_credentials_drops_private_key_material() {
        let config = Config::new("org-account", "etl_user", "ETL_DEV", "PUBLIC")
            .expect("valid config")
            .with_private_key("secret-key", Some("secret-passphrase".to_owned().into()));

        let sanitized = config.clone_without_credentials();

        assert_eq!(sanitized.account(), "org-account");
        assert!(sanitized.private_key().is_none());
        assert!(sanitized.private_key_passphrase().is_none());
    }
}
