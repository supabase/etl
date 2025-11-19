use async_trait::async_trait;
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

use crate::error::ValidationError;

#[async_trait]
pub trait SourceValidator {
    async fn validate(&self) -> Result<(), ValidationError>;
}

#[async_trait]
impl SourceValidator for PgConnectionConfig {
    async fn validate(&self) -> Result<(), ValidationError> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(self.with_db())
            .await
            .map_err(|e| ValidationError::ConnectionFailed(e.to_string()))?;

        let version: String = sqlx::query_scalar("SELECT version()")
            .fetch_one(&pool)
            .await
            .map_err(|e| ValidationError::ConnectionFailed(e.to_string()))?;

        let version_major =
            parse_postgres_version(&version).map_err(ValidationError::InvalidConfig)?;

        if ![14, 15, 16, 17].contains(&version_major) {
            return Err(ValidationError::InvalidConfig(format!(
                "PostgreSQL version {} is not supported. Supported versions: 14, 15, 16, 17",
                version_major
            )));
        }

        let wal_level: String = sqlx::query_scalar("SHOW wal_level")
            .fetch_one(&pool)
            .await
            .map_err(|e| ValidationError::ConnectionFailed(e.to_string()))?;

        if wal_level != "logical" {
            return Err(ValidationError::InvalidConfig(format!(
                "wal_level is '{}', must be 'logical' for replication",
                wal_level
            )));
        }

        let has_replication: bool =
            sqlx::query_scalar("SELECT rolreplication FROM pg_roles WHERE rolname = current_user")
                .fetch_one(&pool)
                .await
                .map_err(|e| ValidationError::ConnectionFailed(e.to_string()))?;

        if !has_replication {
            return Err(ValidationError::PermissionDenied(
                "User lacks REPLICATION privilege".to_string(),
            ));
        }

        let available_slots: i64 = sqlx::query_scalar(
            "SELECT
                (SELECT setting::int FROM pg_settings WHERE name = 'max_replication_slots') -
                (SELECT COUNT(*) FROM pg_replication_slots)",
        )
        .fetch_one(&pool)
        .await
        .map_err(|e| ValidationError::ConnectionFailed(e.to_string()))?;

        if available_slots <= 0 {
            return Err(ValidationError::InvalidConfig(
                "No replication slots available. Increase max_replication_slots setting"
                    .to_string(),
            ));
        }

        Ok(())
    }
}

fn parse_postgres_version(version_string: &str) -> Result<u32, String> {
    let parts: Vec<&str> = version_string.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(format!("Invalid version string: {}", version_string));
    }

    let version_part = parts[1];
    let major_version_str = version_part
        .split('.')
        .next()
        .ok_or_else(|| format!("Could not extract major version from: {}", version_part))?;

    major_version_str
        .parse::<u32>()
        .map_err(|e| format!("Failed to parse version number: {}", e))
}
