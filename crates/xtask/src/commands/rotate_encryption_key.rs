use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use etl_api::{
    config::ApiConfig,
    configs::{
        destination::{EncryptedStoredDestinationConfig, StoredDestinationConfig},
        encryption::EncryptionKeyring,
        serde::{decrypt_and_deserialize_from_value, encrypt_and_serialize},
        source::{EncryptedStoredSourceConfig, StoredSourceConfig},
    },
    startup::{build_encryption_keyring, get_connection_pool},
};
use etl_config::load_config;
use serde_json::Value;
use sqlx::PgPool;

#[derive(Args)]
pub(crate) struct RotateEncryptionKeyArgs {
    /// Print what would change without updating rows.
    #[arg(long)]
    dry_run: bool,

    /// Number of rows to read per batch.
    #[arg(long, default_value_t = 100)]
    batch_size: i64,

    /// Resource table to rotate.
    #[arg(long, value_enum, default_value_t = RotationTarget::All)]
    target: RotationTarget,

    /// Tenant/project ref to rotate. When omitted, all tenants are scanned.
    #[arg(long)]
    tenant_id: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum RotationTarget {
    /// Rotate source and destination configs.
    All,
    /// Rotate source configs.
    Sources,
    /// Rotate destination configs.
    Destinations,
}

impl RotateEncryptionKeyArgs {
    pub(crate) async fn run(self) -> Result<()> {
        if self.batch_size <= 0 {
            bail!("batch size must be greater than zero");
        }

        let config = load_config::<ApiConfig>().context("Loading API configuration")?;
        let encryption_keyring =
            build_encryption_keyring(&config).context("Building encryption keyring")?;
        let latest_key_id = encryption_keyring.latest_key_id();
        let pool = connect_to_database(&config).await?;

        println!(
            "[rotate-encryption-key] target key id: {latest_key_id}; dry run: {}; tenant_id: {}",
            self.dry_run,
            self.tenant_id.as_deref().unwrap_or("all")
        );

        let mut total_stats = RotationStats::default();

        if matches!(self.target, RotationTarget::All | RotationTarget::Sources) {
            let stats = rotate_sources(
                &pool,
                &encryption_keyring,
                self.batch_size,
                self.dry_run,
                self.tenant_id.as_deref(),
            )
            .await?;
            stats.print("sources");
            total_stats += stats;
        }

        if matches!(self.target, RotationTarget::All | RotationTarget::Destinations) {
            let stats = rotate_destinations(
                &pool,
                &encryption_keyring,
                self.batch_size,
                self.dry_run,
                self.tenant_id.as_deref(),
            )
            .await?;
            stats.print("destinations");
            total_stats += stats;
        }

        total_stats.print("total");

        Ok(())
    }
}

#[derive(Default)]
struct RotationStats {
    scanned_rows: u64,
    unchanged_rows: u64,
    rotated_rows: u64,
    concurrently_changed_rows: u64,
    encrypted_values_by_key_id: BTreeMap<u32, u64>,
}

impl RotationStats {
    fn record_key_ids(&mut self, key_ids: &[u32]) {
        for key_id in key_ids {
            *self.encrypted_values_by_key_id.entry(*key_id).or_default() += 1;
        }
    }

    fn print(&self, label: &str) {
        println!(
            "[rotate-encryption-key] {label}: scanned={}, rotated={}, unchanged={}, \
             concurrently_changed={}",
            self.scanned_rows,
            self.rotated_rows,
            self.unchanged_rows,
            self.concurrently_changed_rows
        );

        if self.encrypted_values_by_key_id.is_empty() {
            println!("[rotate-encryption-key] {label}: observed encrypted values by key id: none");
            return;
        }

        let counts = self
            .encrypted_values_by_key_id
            .iter()
            .map(|(key_id, count)| format!("{key_id}={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("[rotate-encryption-key] {label}: observed encrypted values by key id: {counts}");
    }
}

impl std::ops::AddAssign for RotationStats {
    fn add_assign(&mut self, rhs: Self) {
        self.scanned_rows += rhs.scanned_rows;
        self.unchanged_rows += rhs.unchanged_rows;
        self.rotated_rows += rhs.rotated_rows;
        self.concurrently_changed_rows += rhs.concurrently_changed_rows;

        for (key_id, count) in rhs.encrypted_values_by_key_id {
            *self.encrypted_values_by_key_id.entry(key_id).or_default() += count;
        }
    }
}

async fn connect_to_database(config: &ApiConfig) -> Result<PgPool> {
    let pool = get_connection_pool(&config.database);
    pool.acquire().await.context("Connecting to API metadata database from config")?;

    Ok(pool)
}

async fn rotate_sources(
    pool: &PgPool,
    encryption_keyring: &EncryptionKeyring,
    batch_size: i64,
    dry_run: bool,
    tenant_id: Option<&str>,
) -> Result<RotationStats> {
    let latest_key_id = encryption_keyring.latest_key_id();
    let mut stats = RotationStats::default();
    let mut last_id = 0_i64;

    loop {
        let records = sqlx::query_as::<_, ResourceRecord>(SOURCE_BATCH_QUERY)
            .bind(last_id)
            .bind(batch_size)
            .bind(tenant_id)
            .fetch_all(pool)
            .await
            .context("Fetching source config batch")?;

        if records.is_empty() {
            break;
        }

        for record in records {
            last_id = record.id;
            stats.scanned_rows += 1;

            let key_ids = encrypted_value_key_ids(&record.config);
            stats.record_key_ids(&key_ids);

            if key_ids.iter().all(|key_id| *key_id == latest_key_id) {
                stats.unchanged_rows += 1;
                continue;
            }

            let new_config = rotate_source_config(record.config.clone(), encryption_keyring)
                .with_context(|| format!("Rotating source config {}", record.id))?;

            if dry_run {
                stats.rotated_rows += 1;
                continue;
            }

            let rows_affected = sqlx::query(SOURCE_UPDATE_QUERY)
                .bind(new_config)
                .bind(record.id)
                .bind(record.config)
                .execute(pool)
                .await
                .with_context(|| format!("Updating source config {}", record.id))?
                .rows_affected();

            if rows_affected == 0 {
                stats.concurrently_changed_rows += 1;
            } else {
                stats.rotated_rows += 1;
            }
        }
    }

    Ok(stats)
}

async fn rotate_destinations(
    pool: &PgPool,
    encryption_keyring: &EncryptionKeyring,
    batch_size: i64,
    dry_run: bool,
    tenant_id: Option<&str>,
) -> Result<RotationStats> {
    let latest_key_id = encryption_keyring.latest_key_id();
    let mut stats = RotationStats::default();
    let mut last_id = 0_i64;

    loop {
        let records = sqlx::query_as::<_, ResourceRecord>(DESTINATION_BATCH_QUERY)
            .bind(last_id)
            .bind(batch_size)
            .bind(tenant_id)
            .fetch_all(pool)
            .await
            .context("Fetching destination config batch")?;

        if records.is_empty() {
            break;
        }

        for record in records {
            last_id = record.id;
            stats.scanned_rows += 1;

            let key_ids = encrypted_value_key_ids(&record.config);
            stats.record_key_ids(&key_ids);

            if key_ids.iter().all(|key_id| *key_id == latest_key_id) {
                stats.unchanged_rows += 1;
                continue;
            }

            let new_config =
                rotate_destination_config(record.config.clone(), encryption_keyring)
                    .with_context(|| format!("Rotating destination config {}", record.id))?;

            if dry_run {
                stats.rotated_rows += 1;
                continue;
            }

            let rows_affected = sqlx::query(DESTINATION_UPDATE_QUERY)
                .bind(new_config)
                .bind(record.id)
                .bind(record.config)
                .execute(pool)
                .await
                .with_context(|| format!("Updating destination config {}", record.id))?
                .rows_affected();

            if rows_affected == 0 {
                stats.concurrently_changed_rows += 1;
            } else {
                stats.rotated_rows += 1;
            }
        }
    }

    Ok(stats)
}

fn rotate_source_config(config: Value, encryption_keyring: &EncryptionKeyring) -> Result<Value> {
    let config = decrypt_and_deserialize_from_value::<
        EncryptedStoredSourceConfig,
        StoredSourceConfig,
    >(config, encryption_keyring)?;
    encrypt_and_serialize::<StoredSourceConfig, EncryptedStoredSourceConfig>(
        config,
        encryption_keyring,
    )
    .map_err(Into::into)
}

fn rotate_destination_config(
    config: Value,
    encryption_keyring: &EncryptionKeyring,
) -> Result<Value> {
    let config = decrypt_and_deserialize_from_value::<
        EncryptedStoredDestinationConfig,
        StoredDestinationConfig,
    >(config, encryption_keyring)?;
    encrypt_and_serialize::<StoredDestinationConfig, EncryptedStoredDestinationConfig>(
        config,
        encryption_keyring,
    )
    .map_err(Into::into)
}

fn encrypted_value_key_ids(value: &Value) -> Vec<u32> {
    let mut key_ids = Vec::new();
    collect_encrypted_value_key_ids(value, &mut key_ids);
    key_ids
}

fn collect_encrypted_value_key_ids(value: &Value, key_ids: &mut Vec<u32>) {
    match value {
        Value::Object(map) => {
            if let (Some(id), Some(Value::String(_)), Some(Value::String(_))) =
                (map.get("id").and_then(Value::as_u64), map.get("nonce"), map.get("value"))
                && let Ok(id) = u32::try_from(id)
            {
                key_ids.push(id);
                return;
            }

            for value in map.values() {
                collect_encrypted_value_key_ids(value, key_ids);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_encrypted_value_key_ids(value, key_ids);
            }
        }
        _ => {}
    }
}

#[derive(sqlx::FromRow)]
struct ResourceRecord {
    id: i64,
    config: Value,
}

const SOURCE_BATCH_QUERY: &str = r#"
select id, config
from app.sources
where id > $1
  and ($3::text is null or tenant_id = $3)
order by id
limit $2
"#;

const DESTINATION_BATCH_QUERY: &str = r#"
select id, config
from app.destinations
where id > $1
  and ($3::text is null or tenant_id = $3)
order by id
limit $2
"#;

const SOURCE_UPDATE_QUERY: &str = r#"
update app.sources
set config = $1, updated_at = now()
where id = $2 and config = $3
"#;

const DESTINATION_UPDATE_QUERY: &str = r#"
update app.destinations
set config = $1, updated_at = now()
where id = $2 and config = $3
"#;
