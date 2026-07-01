use anyhow::{Context, Result, bail};
use clap::Args;
use etl_api::{
    config::ApiConfig,
    configs::{
        encryption::EncryptionKeyring,
        serde::{decrypt_and_deserialize_from_value, encrypt_and_serialize},
        source::{EncryptedStoredSourceConfig, StoredSourceConfig},
    },
    startup::{build_encryption_keyring, get_connection_pool},
};
use etl_config::load_config;
use sqlx::{PgPool, Row};

#[derive(Args)]
pub(crate) struct RemoveHostaddrArgs {
    /// Pipeline ID whose sources should be updated.
    #[arg(long)]
    pipeline_id: i64,

    /// Print what would change without updating the rows.
    #[arg(long)]
    dry_run: bool,
}

impl RemoveHostaddrArgs {
    pub(crate) async fn run(self) -> Result<()> {
        let config = load_config::<ApiConfig>().context("Loading API configuration")?;
        let encryption_keyring =
            build_encryption_keyring(&config).context("Building encryption keyring")?;
        let pool = get_connection_pool(&config.database);
        pool.acquire().await.context("Connecting to database")?;

        remove_hostaddr_from_sources(&pool, &encryption_keyring, self.pipeline_id, self.dry_run)
            .await
    }
}

async fn remove_hostaddr_from_sources(
    pool: &PgPool,
    encryption_keyring: &EncryptionKeyring,
    pipeline_id: i64,
    dry_run: bool,
) -> Result<()> {
    println!("[remove-hostaddr] starting");
    println!("  pipeline_id: {pipeline_id}");
    println!(
        "  dry_run: {}",
        if dry_run { "dry run (no database updates)" } else { "live update" }
    );
    println!();

    let records = sqlx::query(
        "SELECT s.id, s.tenant_id, s.config FROM app.sources s JOIN app.pipelines p ON \
         p.source_id = s.id WHERE p.id = $1",
    )
    .bind(pipeline_id)
    .fetch_all(pool)
    .await
    .context("Failed to query sources from database")?;

    if records.is_empty() {
        bail!("No sources found for pipeline id {pipeline_id}");
    }

    println!("Found {} source(s) for pipeline {pipeline_id}", records.len());
    println!();

    let mut updated_count = 0;

    for record in records {
        let source_id: i64 = record.get("id");
        let stored_tenant_id: String = record.get("tenant_id");
        let config_value: sqlx::types::JsonValue = record.get("config");

        println!("Processing source:");
        println!("  id: {source_id}");
        println!("  tenant_id: {stored_tenant_id}");

        let source_config: StoredSourceConfig = decrypt_and_deserialize_from_value::<
            EncryptedStoredSourceConfig,
            StoredSourceConfig,
        >(config_value, encryption_keyring)
        .context("Failed to decrypt and deserialize source config")?;

        println!("Current config:");
        println!("  host: {}", source_config.host);
        println!(
            "  hostaddr: {}",
            source_config.hostaddr.map_or_else(|| "None".to_owned(), |addr| addr.to_string())
        );
        println!("  port: {}", source_config.port);
        println!("  name: {}", source_config.name);
        println!("  username: {}", source_config.username);

        if source_config.hostaddr.is_none() {
            println!("  → hostaddr is already None, skipping");
            println!();
            continue;
        }

        let mut updated_config = source_config;
        updated_config.hostaddr = None;

        println!("Updated config:");
        println!("  hostaddr: None (removed)");

        if !dry_run {
            let encrypted_config = encrypt_and_serialize::<
                StoredSourceConfig,
                EncryptedStoredSourceConfig,
            >(updated_config, encryption_keyring)
            .context("Failed to encrypt and serialize updated source config")?;

            sqlx::query("UPDATE app.sources SET config = $1 WHERE id = $2")
                .bind(encrypted_config)
                .bind(source_id)
                .execute(pool)
                .await
                .context("Failed to update source in database")?;

            println!("  → successfully updated");
            updated_count += 1;
        } else {
            println!("  → would be updated (dry run)");
        }

        println!();
    }

    if dry_run {
        println!(
            "[remove-hostaddr] dry run completed successfully; no database rows were updated. \
             {updated_count} source(s) would be updated.",
        );
    } else {
        println!(
            "[remove-hostaddr] successfully removed hostaddr from {updated_count} source(s) for \
             pipeline {pipeline_id}"
        );
    }

    Ok(())
}
