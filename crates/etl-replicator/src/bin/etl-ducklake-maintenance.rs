//! One-shot DuckLake maintenance binary for Kubernetes maintenance Jobs.

use std::{env, error::Error, process::ExitCode};

use etl_config::{
    load_config, parse_ducklake_url,
    shared::{DestinationConfig, ReplicatorConfig},
};
use etl_destinations::ducklake::{
    DuckLakeMaintenanceConfig, InlineFlushMaintenanceConfig, MergeAdjacentFilesMaintenanceConfig,
    RewriteDataFilesMaintenanceConfig, S3Config as DuckLakeS3Config, run_maintenance_once,
};
use rustls::crypto::aws_lc_rs;
use secrecy::ExposeSecret;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

type MaintenanceResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Runs the maintenance binary.
fn main() -> ExitCode {
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ducklake maintenance failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Loads configuration and runs one maintenance attempt.
fn try_main() -> MaintenanceResult<()> {
    init_crypto_provider();
    init_stdout_tracing();
    let config = load_config::<ReplicatorConfig>()?;
    config.validate()?;

    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run(config))
}

/// Runs one async maintenance attempt.
async fn run(config: ReplicatorConfig) -> MaintenanceResult<()> {
    let DestinationConfig::Ducklake {
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
        duckdb_memory_cache_limit,
        maintenance_target_file_size,
        ..
    } = config.destination
    else {
        return Err("etl-ducklake-maintenance requires a DuckLake destination".into());
    };

    let s3 = match (s3_access_key_id, s3_secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(DuckLakeS3Config {
            access_key_id: access_key_id.expose_secret().to_owned(),
            secret_access_key: secret_access_key.expose_secret().to_owned(),
            region: s3_region.unwrap_or_else(|| "us-east-1".to_owned()),
            endpoint: s3_endpoint,
            url_style: s3_url_style.unwrap_or_else(|| "path".to_owned()),
            use_ssl: s3_use_ssl.unwrap_or(false),
        }),
        (None, None) => None,
        _ => {
            return Err("ducklake s3 credentials must include both access key id and secret \
                        access key"
                .into());
        }
    };

    let maintenance_config = DuckLakeMaintenanceConfig {
        catalog_url: parse_ducklake_url(&catalog_url)?,
        data_path: parse_ducklake_url(&data_path)?,
        pool_size,
        s3,
        metadata_schema,
        duckdb_memory_cache_limit,
        maintenance_target_file_size,
        inline_flush: InlineFlushMaintenanceConfig {
            enabled: env_bool("ETL_DUCKLAKE_MAINTENANCE__INLINE_FLUSH__ENABLED", true),
            min_inlined_bytes: env_u64(
                "ETL_DUCKLAKE_MAINTENANCE__INLINE_FLUSH__MIN_INLINED_BYTES",
                10_000_000,
            )?,
        },
        merge_adjacent_files: MergeAdjacentFilesMaintenanceConfig {
            enabled: env_bool("ETL_DUCKLAKE_MAINTENANCE__MERGE_ADJACENT_FILES__ENABLED", true),
            max_compacted_files: env_u32(
                "ETL_DUCKLAKE_MAINTENANCE__MERGE_ADJACENT_FILES__MAX_COMPACTED_FILES",
                32,
            )?,
            max_tables_per_run: env_u32(
                "ETL_DUCKLAKE_MAINTENANCE__MERGE_ADJACENT_FILES__MAX_TABLES_PER_RUN",
                8,
            )?,
            target_file_size: env::var(
                "ETL_DUCKLAKE_MAINTENANCE__MERGE_ADJACENT_FILES__TARGET_FILE_SIZE",
            )
            .unwrap_or_else(|_| "10MB".to_owned()),
        },
        rewrite_data_files: RewriteDataFilesMaintenanceConfig {
            enabled: env_bool("ETL_DUCKLAKE_MAINTENANCE__REWRITE_DATA_FILES__ENABLED", true),
            min_active_data_files: env_i64(
                "ETL_DUCKLAKE_MAINTENANCE__REWRITE_DATA_FILES__MIN_ACTIVE_DATA_FILES",
                40,
            )?,
            max_tables_per_run: env_u32(
                "ETL_DUCKLAKE_MAINTENANCE__REWRITE_DATA_FILES__MAX_TABLES_PER_RUN",
                8,
            )?,
        },
    };

    info!(
        pipeline_id = config.pipeline.id,
        inline_flush_enabled = maintenance_config.inline_flush.enabled,
        inline_flush_min_inlined_bytes = maintenance_config.inline_flush.min_inlined_bytes,
        merge_adjacent_files_enabled = maintenance_config.merge_adjacent_files.enabled,
        merge_adjacent_files_max_compacted_files =
            maintenance_config.merge_adjacent_files.max_compacted_files,
        merge_adjacent_files_max_tables_per_run =
            maintenance_config.merge_adjacent_files.max_tables_per_run,
        rewrite_data_files_enabled = maintenance_config.rewrite_data_files.enabled,
        rewrite_data_files_min_active_data_files =
            maintenance_config.rewrite_data_files.min_active_data_files,
        rewrite_data_files_max_tables_per_run =
            maintenance_config.rewrite_data_files.max_tables_per_run,
        "ducklake external maintenance job starting"
    );

    let outcome = run_maintenance_once(maintenance_config).await?;
    info!(
        applied = outcome.applied(),
        inline_flush_tables = outcome.inline_flush_tables,
        inline_flush_rows = outcome.inline_flush_rows,
        merge_adjacent_files_tables = outcome.merge_adjacent_files_tables,
        merge_adjacent_files_created = outcome.merge_adjacent_files_created,
        rewrite_data_files_tables = outcome.rewrite_data_files_tables,
        rewrite_data_files_created = outcome.rewrite_data_files_created,
        "ducklake external maintenance job finished"
    );
    println!(
        "{{\"applied\":{},\"inlineFlushRows\":{},\"mergeAdjacentFilesCreated\":{},\"\
         rewriteDataFilesCreated\":{}}}",
        outcome.applied(),
        outcome.inline_flush_rows,
        outcome.merge_adjacent_files_created,
        outcome.rewrite_data_files_created
    );
    Ok(())
}

/// Installs the process-wide Rustls crypto provider.
fn init_crypto_provider() {
    let _ = aws_lc_rs::default_provider().install_default();
}

/// Initializes direct stdout logging for short-lived Kubernetes Jobs.
fn init_stdout_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("etl_ducklake_maintenance=info,etl_destinations::ducklake=info")
    });
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .try_init();
}

/// Reads a boolean environment variable.
fn env_bool(name: &str, default: bool) -> bool {
    env::var(name).ok().and_then(|value| value.parse::<bool>().ok()).unwrap_or(default)
}

/// Reads a `u64` environment variable.
fn env_u64(name: &str, default: u64) -> MaintenanceResult<u64> {
    Ok(match env::var(name) {
        Ok(value) => value.parse()?,
        Err(_) => default,
    })
}

/// Reads a `u32` environment variable.
fn env_u32(name: &str, default: u32) -> MaintenanceResult<u32> {
    Ok(match env::var(name) {
        Ok(value) => value.parse()?,
        Err(_) => default,
    })
}

/// Reads an `i64` environment variable.
fn env_i64(name: &str, default: i64) -> MaintenanceResult<i64> {
    Ok(match env::var(name) {
        Ok(value) => value.parse()?,
        Err(_) => default,
    })
}
