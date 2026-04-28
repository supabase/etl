use std::path::PathBuf;

/// Snowflake account identifier (e.g. `org-account`).
pub const SNOWFLAKE_ACCOUNT_ENV: &str = "TESTS_SNOWFLAKE_ACCOUNT";

/// Snowflake login user name.
pub const SNOWFLAKE_USER_ENV: &str = "TESTS_SNOWFLAKE_USER";

/// Path to the PEM-encoded private key used for key-pair authentication.
pub const SNOWFLAKE_PRIVATE_KEY_PATH_ENV: &str = "TESTS_SNOWFLAKE_PRIVATE_KEY_PATH";

/// Target database. Falls back to `ETL_DEV` when unset.
pub const SNOWFLAKE_DATABASE_ENV: &str = "TESTS_SNOWFLAKE_DATABASE";

/// Target schema. Falls back to `PUBLIC` when unset.
pub const SNOWFLAKE_SCHEMA_ENV: &str = "TESTS_SNOWFLAKE_SCHEMA";

/// Optional virtual warehouse to use for queries.
pub const SNOWFLAKE_WAREHOUSE_ENV: &str = "TESTS_SNOWFLAKE_WAREHOUSE";

/// Optional role to assume after connecting.
pub const SNOWFLAKE_ROLE_ENV: &str = "TESTS_SNOWFLAKE_ROLE";

const DEFAULT_DATABASE: &str = "ETL_DEV";
const DEFAULT_SCHEMA: &str = "PUBLIC";

/// Connection parameters resolved from environment variables for Snowflake
/// integration tests.
pub struct SnowflakeTestConfig {
    pub account: String,
    pub user: String,
    pub private_key_path: PathBuf,
    pub database: String,
    pub schema: String,
    pub warehouse: Option<String>,
    pub role: Option<String>,
}

/// Loads a [`SnowflakeTestConfig`] from environment variables, panicking on
/// missing required vars.
pub fn load_test_config() -> SnowflakeTestConfig {
    SnowflakeTestConfig {
        account: std::env::var(SNOWFLAKE_ACCOUNT_ENV)
            .unwrap_or_else(|_| panic!("{SNOWFLAKE_ACCOUNT_ENV} must be set")),
        user: std::env::var(SNOWFLAKE_USER_ENV)
            .unwrap_or_else(|_| panic!("{SNOWFLAKE_USER_ENV} must be set")),
        private_key_path: PathBuf::from(
            std::env::var(SNOWFLAKE_PRIVATE_KEY_PATH_ENV)
                .unwrap_or_else(|_| panic!("{SNOWFLAKE_PRIVATE_KEY_PATH_ENV} must be set")),
        ),
        database: std::env::var(SNOWFLAKE_DATABASE_ENV)
            .unwrap_or_else(|_| DEFAULT_DATABASE.to_string()),
        schema: std::env::var(SNOWFLAKE_SCHEMA_ENV).unwrap_or_else(|_| DEFAULT_SCHEMA.to_string()),
        warehouse: std::env::var(SNOWFLAKE_WAREHOUSE_ENV).ok(),
        role: std::env::var(SNOWFLAKE_ROLE_ENV).ok(),
    }
}
