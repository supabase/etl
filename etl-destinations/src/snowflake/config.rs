/// Connection parameters for a Snowflake account.
#[derive(Debug, Clone)]
pub struct Config {
    /// Full Snowflake account URL (e.g. `https://ORG-ACCT.snowflakecomputing.com`).
    pub account_url: String,

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
            account_id: account_id.to_string(),
            username: username.to_string(),
            database: database.to_string(),
            schema: schema.to_string(),
            role: None,
        }
    }

    /// Override the derived URL (useful for private-link or local testing).
    pub fn with_account_url(mut self, url: &str) -> Self {
        self.account_url = url.to_string();
        self
    }

    /// Set the role to assume after connecting.
    pub fn with_role(mut self, role: &str) -> Self {
        self.role = Some(role.to_string());
        self
    }
}
