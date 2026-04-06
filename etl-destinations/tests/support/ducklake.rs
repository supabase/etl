#![allow(dead_code)]

use duckdb::{Config, Connection};
use pg_escape::quote_literal;
use std::path::{Path, PathBuf};

const DUCKDB_EXTENSION_VERSION: &str = "1.5.1";
const DUCKLAKE_EXTENSION_FILE: &str = "ducklake.duckdb_extension";
const JSON_EXTENSION_FILE: &str = "json.duckdb_extension";
const PARQUET_EXTENSION_FILE: &str = "parquet.duckdb_extension";

fn current_vendored_extension_dir() -> Option<PathBuf> {
    let platform_dir = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64" | "amd64") => "linux_amd64",
        ("linux", "aarch64" | "arm64") => "linux_arm64",
        ("macos", "x86_64" | "amd64") => "osx_amd64",
        ("macos", "aarch64" | "arm64") => "osx_arm64",
        _ => return None,
    };
    let env_override = std::env::var_os("ETL_DUCKDB_EXTENSION_ROOT").map(PathBuf::from);
    let candidate_roots = env_override
        .into_iter()
        .chain([
            PathBuf::from("/app/duckdb_extensions"),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/duckdb/extensions"),
        ])
        .collect::<Vec<_>>();

    for root in candidate_roots {
        let extension_dir = root.join(DUCKDB_EXTENSION_VERSION).join(platform_dir);
        let ducklake_extension = extension_dir.join(DUCKLAKE_EXTENSION_FILE);
        let json_extension = extension_dir.join(JSON_EXTENSION_FILE);
        let parquet_extension = extension_dir.join(PARQUET_EXTENSION_FILE);

        if ducklake_extension.is_file() && json_extension.is_file() && parquet_extension.is_file() {
            return Some(extension_dir);
        }
    }

    None
}

pub fn open_verification_connection() -> Connection {
    let duckdb_dir = tempfile::Builder::new()
        .prefix("etl_ducklake_verify_")
        .tempdir()
        .expect("failed to create verification duckdb dir")
        .keep();
    let duckdb_path = duckdb_dir.join("verify.duckdb");

    if current_vendored_extension_dir().is_some() {
        return Connection::open_with_flags(
            &duckdb_path,
            Config::default()
                .enable_autoload_extension(false)
                .expect("failed to disable DuckDB extension autoload"),
        )
        .expect("failed to open verification DuckDB");
    }

    Connection::open(&duckdb_path).expect("failed to open verification DuckDB")
}

pub fn ducklake_load_sql() -> String {
    if let Some(extension_dir) = current_vendored_extension_dir() {
        let ducklake_extension = extension_dir.join(DUCKLAKE_EXTENSION_FILE);
        let json_extension = extension_dir.join(JSON_EXTENSION_FILE);
        let parquet_extension = extension_dir.join(PARQUET_EXTENSION_FILE);

        return format!(
            "LOAD {}; LOAD {}; LOAD {};",
            quote_literal(&ducklake_extension.display().to_string()),
            quote_literal(&json_extension.display().to_string()),
            quote_literal(&parquet_extension.display().to_string()),
        );
    }

    "INSTALL ducklake; LOAD ducklake; INSTALL json; LOAD json; INSTALL parquet; LOAD parquet;"
        .to_string()
}
