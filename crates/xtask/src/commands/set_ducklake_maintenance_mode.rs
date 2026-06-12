use anyhow::{Context, Result, bail};
use clap::Args;
use etl_api::{
    config::ApiConfig,
    configs::{
        destination::{EncryptedStoredDestinationConfig, StoredDestinationConfig},
        encryption::EncryptionKeyring,
        serde::{decrypt_and_deserialize_from_value, encrypt_and_serialize},
    },
    startup::{build_encryption_keyring, get_connection_pool},
};
use etl_config::{load_config, shared::DuckLakeMaintenanceMode};
use sqlx::{PgPool, Row};

#[derive(Args)]
pub(crate) struct SetDucklakeMaintenanceModeArgs {
    /// Pipeline ID whose destination should be updated.
    #[arg(long)]
    pipeline_id: i64,

    /// Maintenance mode to set (disabled, kubernetes, postgres).
    #[arg(long, default_value = "disabled")]
    mode: String,

    /// Print what would change without updating the row.
    #[arg(long)]
    dry_run: bool,
}

impl SetDucklakeMaintenanceModeArgs {
    pub(crate) async fn run(self) -> Result<()> {
        let mode = parse_maintenance_mode(&self.mode)?;
        let config = load_config::<ApiConfig>().context("Loading API configuration")?;
        let encryption_keyring =
            build_encryption_keyring(&config).context("Building encryption keyring")?;
        let pool = get_connection_pool(&config.database);
        pool.acquire().await.context("Connecting to database")?;

        set_ducklake_maintenance_mode(&pool, &encryption_keyring, self.pipeline_id, mode, self.dry_run).await
    }
}

fn parse_maintenance_mode(s: &str) -> Result<DuckLakeMaintenanceMode> {
    match s {
        "disabled" => Ok(DuckLakeMaintenanceMode::Disabled),
        "kubernetes" => Ok(DuckLakeMaintenanceMode::Kubernetes),
        "postgres" => Ok(DuckLakeMaintenanceMode::Postgres),
        other => bail!("Unknown maintenance mode {other:?}; expected one of: disabled, kubernetes, postgres"),
    }
}

async fn set_ducklake_maintenance_mode(
    pool: &PgPool,
    encryption_keyring: &EncryptionKeyring,
    pipeline_id: i64,
    mode: DuckLakeMaintenanceMode,
    dry_run: bool,
) -> Result<()> {
    println!("[set-ducklake-maintenance-mode] starting");
    println!("  pipeline_id: {pipeline_id}");
    println!("  mode: {mode:?}");
    println!("  dry_run: {}", if dry_run { "dry run (no database updates)" } else { "live update" });
    println!();

    let record = sqlx::query(
        "SELECT d.id, d.config \
         FROM app.destinations d \
         JOIN app.pipelines p ON p.destination_id = d.id \
         WHERE p.id = $1",
    )
    .bind(pipeline_id)
    .fetch_optional(pool)
    .await
    .context("Failed to query destination from database")?;

    let Some(record) = record else {
        bail!("No destination found for pipeline id {pipeline_id}");
    };

    let destination_id: i64 = record.get("id");
    let config_value: sqlx::types::JsonValue = record.get("config");

    println!("Found destination:");
    println!("  id: {destination_id}");

    let dest_config: StoredDestinationConfig = decrypt_and_deserialize_from_value::<
        EncryptedStoredDestinationConfig,
        StoredDestinationConfig,
    >(config_value, encryption_keyring)
    .context("Failed to decrypt and deserialize destination config")?;

    let StoredDestinationConfig::Ducklake { maintenance_mode: current_mode, .. } = &dest_config
    else {
        bail!("Destination {destination_id} is not a DuckLake destination");
    };

    println!("Current maintenance_mode: {current_mode:?}");

    if *current_mode == mode {
        println!("\n[set-ducklake-maintenance-mode] already set to {mode:?}, nothing to do");
        return Ok(());
    }

    println!("New maintenance_mode:     {mode:?}");

    let updated_config = match dest_config {
        StoredDestinationConfig::Ducklake {
            catalog_url,
            data_path,
            pool_size,
            s3_access_key_id,
            s3_secret_access_key,
            s3_region,
            s3_endpoint,
            s3_url_style,
            s3_use_ssl,
            metadata_schema,
            maintenance_target_file_size,
            expire_snapshots_older_than,
            maintenance_mode: _,
        } => StoredDestinationConfig::Ducklake {
            catalog_url,
            data_path,
            pool_size,
            s3_access_key_id,
            s3_secret_access_key,
            s3_region,
            s3_endpoint,
            s3_url_style,
            s3_use_ssl,
            metadata_schema,
            maintenance_target_file_size,
            expire_snapshots_older_than,
            maintenance_mode: mode,
        },
        _ => unreachable!(),
    };

    if dry_run {
        println!(
            "\n[set-ducklake-maintenance-mode] dry run completed successfully; no database rows were updated."
        );
        return Ok(());
    }

    let encrypted_config =
        encrypt_and_serialize::<StoredDestinationConfig, EncryptedStoredDestinationConfig>(
            updated_config,
            encryption_keyring,
        )
        .context("Failed to encrypt and serialize updated destination config")?;

    sqlx::query("UPDATE app.destinations SET config = $1 WHERE id = $2")
        .bind(encrypted_config)
        .bind(destination_id)
        .execute(pool)
        .await
        .context("Failed to update destination in database")?;

    println!("\n[set-ducklake-maintenance-mode] successfully set maintenance_mode to {mode:?} on destination {destination_id} (pipeline {pipeline_id})");

    Ok(())
}
