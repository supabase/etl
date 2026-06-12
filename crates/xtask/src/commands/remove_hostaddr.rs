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
    /// Source ID to update.
    #[arg(long)]
    source_id: i64,

    /// Tenant ID (project ref) that owns the source.
    #[arg(long)]
    tenant_id: Option<String>,

    /// Print what would change without updating the row.
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

        remove_hostaddr_from_source(
            &pool,
            &encryption_keyring,
            self.source_id,
            self.tenant_id.as_deref(),
            self.dry_run,
        )
        .await
    }
}

async fn remove_hostaddr_from_source(
    pool: &PgPool,
    encryption_keyring: &EncryptionKeyring,
    source_id: i64,
    tenant_id: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    println!("[remove-hostaddr] starting");
    println!("  source_id: {source_id}");
    println!("  tenant_id: {}", tenant_id.unwrap_or("any"));
    println!("  mode: {}", if dry_run { "dry run (no database updates)" } else { "live update" });
    println!();

    let record = if let Some(tenant_id) = tenant_id {
        sqlx::query(
            "SELECT id, tenant_id, config FROM app.sources WHERE id = $1 AND tenant_id = $2",
        )
        .bind(source_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .context("Failed to query source from database")?
    } else {
        sqlx::query("SELECT id, tenant_id, config FROM app.sources WHERE id = $1")
            .bind(source_id)
            .fetch_optional(pool)
            .await
            .context("Failed to query source from database")?
    };

    let Some(record) = record else {
        bail!("Source with id {source_id} not found");
    };

    let id: i64 = record.get("id");
    let stored_tenant_id: String = record.get("tenant_id");
    let config_value: sqlx::types::JsonValue = record.get("config");

    println!("Found source:");
    println!("  id: {id}");
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
        println!("\n[remove-hostaddr] hostaddr is already None, nothing to do");
        return Ok(());
    }

    let mut updated_config = source_config;
    updated_config.hostaddr = None;

    println!("\nUpdated config:");
    println!("  hostaddr: None (removed)");

    if dry_run {
        println!(
            "\n[remove-hostaddr] dry run completed successfully; no database rows were updated."
        );
        return Ok(());
    }

    let encrypted_config =
        encrypt_and_serialize::<StoredSourceConfig, EncryptedStoredSourceConfig>(
            updated_config,
            encryption_keyring,
        )
        .context("Failed to encrypt and serialize updated source config")?;

    sqlx::query("UPDATE app.sources SET config = $1 WHERE id = $2")
        .bind(encrypted_config)
        .bind(source_id)
        .execute(pool)
        .await
        .context("Failed to update source in database")?;

    println!("\n[remove-hostaddr] successfully removed hostaddr from source {source_id}");

    Ok(())
}
