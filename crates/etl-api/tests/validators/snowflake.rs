use etl_api::{
    configs::destination::FullApiDestinationConfig,
    validation::{FailureType, validate_destination},
};
use etl_config::SerializableSecretString;

use super::create_validation_context;

fn skip_if_missing_snowflake_env_vars() -> bool {
    std::env::var("TESTS_SNOWFLAKE_ACCOUNT").is_err()
}

struct SnowflakeTestEnv {
    account_id: String,
    user: String,
    private_key: String,
    database: String,
    schema: String,
    role: Option<String>,
}

impl SnowflakeTestEnv {
    fn load() -> Self {
        Self {
            account_id: std::env::var("TESTS_SNOWFLAKE_ACCOUNT")
                .expect("TESTS_SNOWFLAKE_ACCOUNT must be set"),
            user: std::env::var("TESTS_SNOWFLAKE_USER").expect("TESTS_SNOWFLAKE_USER must be set"),
            private_key: std::fs::read_to_string(
                std::env::var("TESTS_SNOWFLAKE_PRIVATE_KEY_PATH")
                    .expect("TESTS_SNOWFLAKE_PRIVATE_KEY_PATH must be set"),
            )
            .expect("failed to read private key file"),
            database: std::env::var("TESTS_SNOWFLAKE_DATABASE")
                .unwrap_or_else(|_| "ETL_DEV".to_owned()),
            schema: std::env::var("TESTS_SNOWFLAKE_SCHEMA").unwrap_or_else(|_| "PUBLIC".to_owned()),
            role: std::env::var("TESTS_SNOWFLAKE_ROLE").ok(),
        }
    }

    fn config(&self, database: &str, schema: &str) -> FullApiDestinationConfig {
        create_snowflake_config(
            &self.account_id,
            &self.user,
            &self.private_key,
            None,
            database,
            schema,
            self.role.as_deref(),
        )
    }
}

fn create_snowflake_config(
    account_id: &str,
    user: &str,
    private_key: &str,
    private_key_passphrase: Option<&str>,
    database: &str,
    schema: &str,
    role: Option<&str>,
) -> FullApiDestinationConfig {
    FullApiDestinationConfig::Snowflake {
        account_id: account_id.to_owned(),
        user: user.to_owned(),
        private_key: SerializableSecretString::from(private_key.to_owned()),
        private_key_passphrase: private_key_passphrase
            .map(|p| SerializableSecretString::from(p.to_owned())),
        database: database.to_owned(),
        schema: schema.to_owned(),
        role: role.map(|r| r.to_owned()),
    }
}

#[tokio::test]
async fn validate_snowflake_empty_account_id() {
    let ctx = create_validation_context();
    let config = create_snowflake_config("", "ETL_USER", "fake-key", None, "DB", "PUBLIC", None);
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Snowflake Account ID Required");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
async fn validate_snowflake_empty_user() {
    let ctx = create_validation_context();
    let config = create_snowflake_config("ORG-ACCT", "", "fake-key", None, "DB", "PUBLIC", None);
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Snowflake User Required");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
async fn validate_snowflake_invalid_private_key() {
    let ctx = create_validation_context();
    let config = create_snowflake_config(
        "ORG-ACCT",
        "ETL_USER",
        "not-a-valid-pem-key",
        None,
        "ANALYTICS",
        "PUBLIC",
        None,
    );
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Snowflake Authentication Failed");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
#[ignore]
async fn validate_snowflake_connection_success() {
    if skip_if_missing_snowflake_env_vars() {
        return;
    }
    let env = SnowflakeTestEnv::load();
    let ctx = create_validation_context();
    let config = env.config(&env.database, &env.schema);
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(failures.is_empty(), "Expected no validation failures, got: {failures:?}");
}

#[tokio::test]
#[ignore]
async fn validate_snowflake_wrong_database() {
    if skip_if_missing_snowflake_env_vars() {
        return;
    }
    let env = SnowflakeTestEnv::load();
    let ctx = create_validation_context();
    let config = env.config("NONEXISTENT_DB_12345", &env.schema);
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Snowflake Database Not Found");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
#[ignore]
async fn validate_snowflake_wrong_schema() {
    if skip_if_missing_snowflake_env_vars() {
        return;
    }
    let env = SnowflakeTestEnv::load();
    let ctx = create_validation_context();
    let config = env.config(&env.database, "NONEXISTENT_SCHEMA_12345");
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Snowflake Schema Not Found");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}

#[tokio::test]
#[ignore]
async fn validate_snowflake_empty_database() {
    if skip_if_missing_snowflake_env_vars() {
        return;
    }
    let env = SnowflakeTestEnv::load();
    let ctx = create_validation_context();
    let config = env.config("", &env.schema);
    let failures = validate_destination(&ctx, &config).await.unwrap();

    assert!(!failures.is_empty(), "Expected validation failure");
    assert_eq!(failures[0].name, "Snowflake Database Required");
    assert_eq!(failures[0].failure_type, FailureType::Critical);
}
