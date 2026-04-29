use std::path::PathBuf;

use crate::snowflake::Config;

/// Snowflake account identifier (e.g. `org-account`).
const SNOWFLAKE_ACCOUNT_ENV: &str = "TESTS_SNOWFLAKE_ACCOUNT";

/// Snowflake login user name.
const SNOWFLAKE_USER_ENV: &str = "TESTS_SNOWFLAKE_USER";

/// Path to the PEM-encoded private key used for key-pair authentication.
const SNOWFLAKE_PRIVATE_KEY_PATH_ENV: &str = "TESTS_SNOWFLAKE_PRIVATE_KEY_PATH";

/// Target database. Falls back to `ETL_DEV` when unset.
const SNOWFLAKE_DATABASE_ENV: &str = "TESTS_SNOWFLAKE_DATABASE";

/// Target schema. Falls back to `PUBLIC` when unset.
const SNOWFLAKE_SCHEMA_ENV: &str = "TESTS_SNOWFLAKE_SCHEMA";

/// Optional role to assume after connecting.
const SNOWFLAKE_ROLE_ENV: &str = "TESTS_SNOWFLAKE_ROLE";

const DEFAULT_DATABASE: &str = "ETL_DEV";
const DEFAULT_SCHEMA: &str = "PUBLIC";

/// Load a [`Config`] from environment variables for integration tests.
pub fn load_test_config() -> Config {
    let account_id = std::env::var(SNOWFLAKE_ACCOUNT_ENV)
        .unwrap_or_else(|_| panic!("{SNOWFLAKE_ACCOUNT_ENV} must be set"));
    let username = std::env::var(SNOWFLAKE_USER_ENV)
        .unwrap_or_else(|_| panic!("{SNOWFLAKE_USER_ENV} must be set"));
    let database =
        std::env::var(SNOWFLAKE_DATABASE_ENV).unwrap_or_else(|_| DEFAULT_DATABASE.to_string());
    let schema = std::env::var(SNOWFLAKE_SCHEMA_ENV).unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());

    let mut config = Config::new(&account_id, &username, &database, &schema);
    if let Ok(role) = std::env::var(SNOWFLAKE_ROLE_ENV) {
        config = config.with_role(&role);
    }
    config
}

/// Load the private key path from `TESTS_SNOWFLAKE_PRIVATE_KEY_PATH`.
pub fn load_test_private_key_path() -> PathBuf {
    PathBuf::from(
        std::env::var(SNOWFLAKE_PRIVATE_KEY_PATH_ENV)
            .unwrap_or_else(|_| panic!("{SNOWFLAKE_PRIVATE_KEY_PATH_ENV} must be set")),
    )
}
