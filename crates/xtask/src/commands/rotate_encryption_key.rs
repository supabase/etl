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

impl RotationTarget {
    fn label(self) -> &'static str {
        match self {
            Self::All => "sources and destinations",
            Self::Sources => "sources",
            Self::Destinations => "destinations",
        }
    }
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

        println!("[rotate-encryption-key] starting");
        println!(
            "  mode: {}",
            if self.dry_run { "dry run (no database updates)" } else { "live update" }
        );
        println!("  target: {}", self.target.label());
        println!("  target key id: {latest_key_id}");
        println!("  tenant id: {}", self.tenant_id.as_deref().unwrap_or("all"));
        println!("  batch size: {}", self.batch_size);
        println!();

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
            stats.print("sources", self.dry_run, true);
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
            stats.print("destinations", self.dry_run, true);
            total_stats += stats;
        }

        total_stats.print("total", self.dry_run, false);

        if total_stats.failed_rows > 0 {
            bail!(
                "Encryption key rotation completed with {} failed rows; see failed source and \
                 destination row ids above",
                total_stats.failed_rows
            );
        }

        if !self.dry_run && total_stats.concurrently_changed_rows > 0 {
            println!(
                "[rotate-encryption-key] completed with concurrent changes; rerun the command to \
                 retry skipped rows."
            );
        } else if self.dry_run {
            println!(
                "[rotate-encryption-key] dry run completed successfully; no database rows were \
                 updated."
            );
        } else {
            println!("[rotate-encryption-key] rotation completed successfully.");
        }

        Ok(())
    }
}

#[derive(Default)]
struct RotationStats {
    scanned_rows: u64,
    unchanged_rows: u64,
    rotated_rows: u64,
    rows_with_outdated_encrypted_values: u64,
    concurrently_changed_rows: u64,
    failed_rows: u64,
    failed_row_ids: Vec<i64>,
    concurrently_changed_row_ids: Vec<i64>,
    encrypted_values_by_key_id: BTreeMap<u32, u64>,
}

impl RotationStats {
    fn record_key_ids(&mut self, key_ids: &[u32]) {
        for key_id in key_ids {
            *self.encrypted_values_by_key_id.entry(*key_id).or_default() += 1;
        }
    }

    fn record_concurrently_changed(&mut self, row_id: i64) {
        self.concurrently_changed_rows += 1;
        self.concurrently_changed_row_ids.push(row_id);
    }

    fn record_failure(&mut self, row_id: i64) {
        self.failed_rows += 1;
        self.failed_row_ids.push(row_id);
    }

    fn record_rotation_reasons(&mut self, reasons: RotationReasons) {
        if reasons.outdated_encrypted_values {
            self.rows_with_outdated_encrypted_values += 1;
        }
    }

    fn print(&self, label: &str, dry_run: bool, print_row_ids: bool) {
        let rotation_label = if dry_run { "would rotate rows" } else { "rotated rows" };

        println!("[rotate-encryption-key] {label} summary");
        println!("  scanned rows: {}", self.scanned_rows);
        println!("  {rotation_label}: {}", self.rotated_rows);
        println!("  unchanged rows: {}", self.unchanged_rows);
        println!(
            "  rows with outdated encrypted values: {}",
            self.rows_with_outdated_encrypted_values
        );
        println!("  failed rows: {}", self.failed_rows);

        if !dry_run {
            println!("  concurrently changed rows: {}", self.concurrently_changed_rows);
        }

        println!(
            "  observed encrypted values by key id: {}",
            format_key_id_counts(&self.encrypted_values_by_key_id)
        );

        if print_row_ids {
            println!("  failed row ids: {}", format_row_ids(&self.failed_row_ids));

            if !dry_run {
                println!(
                    "  concurrently changed row ids: {}",
                    format_row_ids(&self.concurrently_changed_row_ids)
                );
            }
        }

        println!();
    }
}

impl std::ops::AddAssign for RotationStats {
    fn add_assign(&mut self, rhs: Self) {
        self.scanned_rows += rhs.scanned_rows;
        self.unchanged_rows += rhs.unchanged_rows;
        self.rotated_rows += rhs.rotated_rows;
        self.rows_with_outdated_encrypted_values += rhs.rows_with_outdated_encrypted_values;
        self.concurrently_changed_rows += rhs.concurrently_changed_rows;
        self.failed_rows += rhs.failed_rows;
        self.failed_row_ids.extend(rhs.failed_row_ids);
        self.concurrently_changed_row_ids.extend(rhs.concurrently_changed_row_ids);

        for (key_id, count) in rhs.encrypted_values_by_key_id {
            *self.encrypted_values_by_key_id.entry(key_id).or_default() += count;
        }
    }
}

fn format_key_id_counts(counts: &BTreeMap<u32, u64>) -> String {
    if counts.is_empty() {
        return "none".to_owned();
    }

    counts.iter().map(|(key_id, count)| format!("{key_id}={count}")).collect::<Vec<_>>().join(", ")
}

fn format_row_ids(row_ids: &[i64]) -> String {
    if row_ids.is_empty() {
        return "none".to_owned();
    }

    row_ids.iter().map(i64::to_string).collect::<Vec<_>>().join(", ")
}

#[derive(Clone, Copy, Default)]
struct RotationReasons {
    outdated_encrypted_values: bool,
}

impl RotationReasons {
    fn needs_rotation(self) -> bool {
        self.outdated_encrypted_values
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

            let reasons = config_rotation_reasons(&record.config, latest_key_id);
            if !reasons.needs_rotation() {
                stats.unchanged_rows += 1;
                continue;
            }
            stats.record_rotation_reasons(reasons);

            let Ok(new_config) = rotate_source_config(record.config.clone(), encryption_keyring)
            else {
                stats.record_failure(record.id);
                eprintln!(
                    "[rotate-encryption-key] sources: failed to rotate row id={}; config could \
                     not be decrypted, deserialized, or re-encrypted",
                    record.id
                );
                continue;
            };

            if dry_run {
                stats.rotated_rows += 1;
                continue;
            }

            let update_result = sqlx::query(SOURCE_UPDATE_QUERY)
                .bind(new_config)
                .bind(record.id)
                .bind(record.config)
                .execute(pool)
                .await;

            let rows_affected = match update_result {
                Ok(result) => result.rows_affected(),
                Err(_) => {
                    stats.record_failure(record.id);
                    eprintln!(
                        "[rotate-encryption-key] sources: failed to update row id={}; database \
                         update failed",
                        record.id
                    );
                    continue;
                }
            };

            if rows_affected == 0 {
                stats.record_concurrently_changed(record.id);
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

            let reasons = config_rotation_reasons(&record.config, latest_key_id);
            if !reasons.needs_rotation() {
                stats.unchanged_rows += 1;
                continue;
            }
            stats.record_rotation_reasons(reasons);

            let Ok(new_config) =
                rotate_destination_config(record.config.clone(), encryption_keyring)
            else {
                stats.record_failure(record.id);
                eprintln!(
                    "[rotate-encryption-key] destinations: failed to rotate row id={}: config \
                     could not be decrypted, deserialized, or re-encrypted",
                    record.id
                );
                continue;
            };

            if dry_run {
                stats.rotated_rows += 1;
                continue;
            }

            let update_result = sqlx::query(DESTINATION_UPDATE_QUERY)
                .bind(new_config)
                .bind(record.id)
                .bind(record.config)
                .execute(pool)
                .await;

            let rows_affected = match update_result {
                Ok(result) => result.rows_affected(),
                Err(_) => {
                    stats.record_failure(record.id);
                    eprintln!(
                        "[rotate-encryption-key] destinations: failed to update row id={}: \
                         database update failed",
                        record.id
                    );
                    continue;
                }
            };

            if rows_affected == 0 {
                stats.record_concurrently_changed(record.id);
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

fn config_rotation_reasons(config: &Value, latest_key_id: u32) -> RotationReasons {
    RotationReasons {
        outdated_encrypted_values: encrypted_value_key_ids(config)
            .iter()
            .any(|key_id| *key_id != latest_key_id),
    }
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
  and ($3::text is null or tenant_id = $3::text)
order by id
limit $2
"#;

const DESTINATION_BATCH_QUERY: &str = r#"
select id, config
from app.destinations
where id > $1
  and ($3::text is null or tenant_id = $3::text)
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use etl_api::configs::encryption::{EncryptionKey, EncryptionKeyring, generate_random_key};
    use serde_json::json;

    use crate::commands::rotate_encryption_key::{
        RotationReasons, RotationStats, config_rotation_reasons, format_key_id_counts,
        format_row_ids, rotate_destination_config,
    };

    #[test]
    fn format_row_ids_prints_none_for_empty_ids() {
        assert_eq!(format_row_ids(&[]), "none");
    }

    #[test]
    fn format_key_id_counts_prints_sorted_counts() {
        let counts = BTreeMap::from([(2, 7), (1, 3)]);

        assert_eq!(format_key_id_counts(&counts), "1=3, 2=7");
    }

    #[test]
    fn rotation_stats_records_failed_and_concurrently_changed_row_ids() {
        let mut stats = RotationStats::default();

        stats.record_key_ids(&[2, 1, 2]);
        stats.record_rotation_reasons(config_rotation_reasons(
            &json!({
                "password": {
                    "id": 1,
                    "nonce": "nonce",
                    "value": "value"
                }
            }),
            2,
        ));
        stats.record_failure(41);
        stats.record_concurrently_changed(42);

        assert_eq!(stats.rows_with_outdated_encrypted_values, 1);
        assert_eq!(stats.failed_rows, 1);
        assert_eq!(stats.failed_row_ids, vec![41]);
        assert_eq!(stats.concurrently_changed_rows, 1);
        assert_eq!(stats.concurrently_changed_row_ids, vec![42]);
        assert_eq!(format_key_id_counts(&stats.encrypted_values_by_key_id), "1=1, 2=2");
    }

    #[test]
    fn rotation_stats_totals_merge_counts_without_losing_row_ids() {
        let mut total = RotationStats {
            scanned_rows: 3,
            unchanged_rows: 1,
            rotated_rows: 1,
            ..RotationStats::default()
        };
        total.record_key_ids(&[1]);
        total.record_rotation_reasons(RotationReasons { outdated_encrypted_values: true });
        total.record_failure(10);

        let mut destinations =
            RotationStats { scanned_rows: 4, rotated_rows: 2, ..RotationStats::default() };
        destinations.record_key_ids(&[1, 2]);
        destinations.record_rotation_reasons(RotationReasons { outdated_encrypted_values: true });
        destinations.record_failure(20);
        destinations.record_concurrently_changed(21);

        total += destinations;

        assert_eq!(total.scanned_rows, 7);
        assert_eq!(total.unchanged_rows, 1);
        assert_eq!(total.rotated_rows, 3);
        assert_eq!(total.rows_with_outdated_encrypted_values, 2);
        assert_eq!(total.failed_rows, 2);
        assert_eq!(total.failed_row_ids, vec![10, 20]);
        assert_eq!(total.concurrently_changed_rows, 1);
        assert_eq!(total.concurrently_changed_row_ids, vec![21]);
        assert_eq!(format_key_id_counts(&total.encrypted_values_by_key_id), "1=2, 2=1");
    }

    #[test]
    fn source_config_rotation_reasons_detects_old_key_ids() {
        let config = json!({
            "host": "localhost",
            "port": 5432,
            "name": "postgres",
            "username": "postgres",
            "password": {
                "id": 1,
                "nonce": "nonce",
                "value": "value"
            }
        });

        let old_key_reasons = config_rotation_reasons(&config, 2);
        assert!(old_key_reasons.needs_rotation());
        assert!(old_key_reasons.outdated_encrypted_values);

        assert!(!config_rotation_reasons(&config, 1).needs_rotation());
    }

    #[test]
    fn rotate_destination_config_rejects_plaintext_ducklake_catalog_url() {
        let keyring = EncryptionKeyring::from(EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        });
        let config = json!({
            "ducklake": {
                "catalog_url": "plaintext-catalog-url",
                "data_path": "s3://example-bucket/path",
                "pool_size": 8
            }
        });

        let error = rotate_destination_config(config, &keyring).unwrap_err();

        assert!(error.to_string().contains("expected struct EncryptedValue"));
    }
}
