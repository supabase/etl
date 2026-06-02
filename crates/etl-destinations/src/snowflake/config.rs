use std::time::Duration;

use etl_config::shared::{ValidationError, validate_snowflake_account_id};

pub(crate) const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);

/// Connection parameters for a Snowflake account.
#[derive(Debug, Clone)]
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
}

impl Config {
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
        })
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

    /// Set the role to assume after connecting.
    pub fn with_role(mut self, role: &str) -> Self {
        self.role = Some(role.to_owned());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
