use etl_api::{
    configs::destination::ApiDestinationConfig,
    validation::{FailureType, validate_destination},
};
use etl_config::{SerializableSecretString, shared::DuckLakeMaintenanceMode};
use etl_postgres::sqlx::test_utils::drop_pg_database;
use pg_escape::quote_identifier;
use secrecy::ExposeSecret;
use sqlx::AssertSqlSafe;
use url::Url;

use super::create_validation_context_with_source;

/// Builds a DuckLake catalog URL for a test Postgres database.
fn catalog_url_from_config(config: &etl_config::shared::PgConnectionConfig) -> Url {
    let mut url = Url::parse("postgres://localhost").unwrap();
    url.set_host(Some(&config.host)).unwrap();
    url.set_port(Some(config.port)).unwrap();
    url.set_username(&config.username).unwrap();
    if let Some(password) = &config.password {
        url.set_password(Some(password.expose_secret())).unwrap();
    }
    url.set_path(&config.name);
    url
}

/// Creates a DuckLake destination config that points at a test catalog.
fn create_ducklake_config(
    catalog_url: Url,
    metadata_schema: Option<String>,
) -> ApiDestinationConfig {
    ApiDestinationConfig::Ducklake {
        catalog_url: SerializableSecretString::from(catalog_url.to_string()),
        data_path: "s3://bucket/path".to_owned(),
        pool_size: Some(1),
        s3_access_key_id: Some(SerializableSecretString::from("access-key".to_owned())),
        s3_secret_access_key: Some(SerializableSecretString::from("secret-key".to_owned())),
        s3_region: Some("us-east-1".to_owned()),
        s3_endpoint: None,
        s3_url_style: Some("path".to_owned()),
        s3_use_ssl: Some(false),
        metadata_schema,
        maintenance_target_file_size: None,
        expire_snapshots_older_than: None,
        maintenance_mode: DuckLakeMaintenanceMode::Disabled,
    }
}

#[tokio::test]
async fn validate_destination_warns_when_ducklake_metadata_schema_already_exists() {
    let (ctx, pool, config) = create_validation_context_with_source().await;
    let metadata_schema = "existing_ducklake";

    sqlx::query(AssertSqlSafe(format!("create schema {};", quote_identifier(metadata_schema))))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "create table {}.ducklake_snapshot (snapshot_id bigint);",
        quote_identifier(metadata_schema),
    )))
    .execute(&pool)
    .await
    .unwrap();

    let failures = validate_destination(
        &ctx,
        &create_ducklake_config(catalog_url_from_config(&config), Some(metadata_schema.to_owned())),
        None,
    )
    .await
    .unwrap();

    let failure = failures
        .iter()
        .find(|failure| failure.name == "DuckLake Metadata Schema Already Exists")
        .expect("Should warn when DuckLake metadata schema already exists in the catalog");
    assert_eq!(failure.failure_type, FailureType::Warning);
    assert!(failure.reason.contains(metadata_schema));
    assert!(failure.reason.contains("ducklake_snapshot"));
    assert!(failure.reason.contains("reuse that DuckLake"));
    assert!(failure.reason.contains("choose another DuckLake metadata schema name"));
    assert!(failure.reason.contains("drop the schema from the database"));

    drop(pool);
    drop_pg_database(&config).await;
}
