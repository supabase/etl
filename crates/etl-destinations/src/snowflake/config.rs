use std::time::Duration;

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
    pub account_id: String,

    /// Snowflake login name used for key-pair authentication.
    ///
    /// This is the user identity that owns the RSA key pair.
    pub username: String,

    /// Target database for all operations.
    pub database: String,

    /// Target schema within the database.
    pub schema: String,

    /// Snowflake role to assume after connecting.
    ///
    /// When `None`, the user's default role is used.
    pub role: Option<String>,
}

impl Config {
    /// Create a config with the required connection parameters.
    pub fn new(account_id: &str, username: &str, database: &str, schema: &str) -> Self {
        let account_url = format!("https://{}.snowflakecomputing.com", account_id.to_uppercase());

        Self {
            account_url,
            account_id: account_id.to_owned(),
            username: username.to_owned(),
            database: database.to_owned(),
            schema: schema.to_owned(),
            role: None,
        }
    }

    /// HTTPS-only account URL used for all API requests.
    pub fn account_url(&self) -> &str {
        &self.account_url
    }

    /// Set the role to assume after connecting.
    pub fn with_role(mut self, role: &str) -> Self {
        self.role = Some(role.to_owned());
        self
    }
}
