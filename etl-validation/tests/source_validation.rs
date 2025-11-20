use etl_config::shared::{PgConnectionConfig, TlsConfig};
use etl_postgres::sqlx::test_utils::{create_pg_database, drop_pg_database};
use etl_validation::{SourceValidator, ValidationError};

fn test_pg_config() -> PgConnectionConfig {
    PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5430,
        username: "postgres".to_string(),
        password: Some("postgres".to_string().into()),
        name: format!("test_validator_{}", uuid::Uuid::new_v4()),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_postgres_connection() {
    let config = test_pg_config();
    let _pool = create_pg_database(&config).await;

    let result = config.validate().await;
    assert!(result.is_ok(), "Validation should succeed: {:?}", result);

    drop_pg_database(&config).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_postgres_connection_failure() {
    let mut config = test_pg_config();
    config.password = Some("wrong_password".to_string().into());

    let result = config.validate().await;
    assert!(
        result.is_err(),
        "Validation should fail with wrong password"
    );

    match result.unwrap_err() {
        ValidationError::ConnectionFailed(msg) => {
            assert!(
                msg.contains("password authentication failed") || msg.contains("error"),
                "Expected connection error, got: {}",
                msg
            );
        }
        err => panic!("Expected ConnectionFailed error, got: {:?}", err),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_postgres_nonexistent_database() {
    let mut config = test_pg_config();
    config.name = format!("nonexistent_db_{}", uuid::Uuid::new_v4());

    let result = config.validate().await;
    assert!(
        result.is_err(),
        "Validation should fail for nonexistent database"
    );

    match result.unwrap_err() {
        ValidationError::ConnectionFailed(msg) => {
            assert!(
                msg.contains("database") || msg.contains("does not exist") || msg.contains("error"),
                "Expected database error, got: {}",
                msg
            );
        }
        err => panic!("Expected ConnectionFailed error, got: {:?}", err),
    }
}
