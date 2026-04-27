#![allow(dead_code)]

use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
};

use duckdb::{Config, Connection};
use etl::config::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
use etl_postgres::tokio::test_utils::PgDatabase;
use pg_escape::quote_literal;
use tokio_postgres::{
    Client, Config as PgConfig,
    config::{Host, SslMode},
};
use url::Url;
use uuid::Uuid;

const DUCKDB_EXTENSION_VERSION: &str = "1.5.2";
const DUCKLAKE_EXTENSION_FILE: &str = "ducklake.duckdb_extension";
const POSTGRES_SCANNER_EXTENSION_FILE: &str = "postgres_scanner.duckdb_extension";

pub struct DuckLakeTestEnv {
    _catalog_database: PgDatabase<Client>,
    pub catalog_url: Url,
    pub data_dir: PathBuf,
    pub data_url: Url,
}

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
        let postgres_scanner_extension = extension_dir.join(POSTGRES_SCANNER_EXTENSION_FILE);

        if ducklake_extension.is_file() && postgres_scanner_extension.is_file() {
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
        let postgres_scanner_extension = extension_dir.join(POSTGRES_SCANNER_EXTENSION_FILE);

        return format!(
            "LOAD {}; LOAD {}; LOAD json; LOAD parquet;",
            quote_literal(&ducklake_extension.display().to_string()),
            quote_literal(&postgres_scanner_extension.display().to_string()),
        );
    }

    "INSTALL ducklake; LOAD ducklake; INSTALL json; LOAD json; INSTALL parquet; LOAD parquet; \
     INSTALL postgres_scanner; LOAD postgres_scanner;"
        .to_string()
}

pub async fn create_test_lake(test_name: &str) -> DuckLakeTestEnv {
    let dir = tempfile::Builder::new()
        .prefix(&format!("etl_ducklake_{test_name}_"))
        .tempdir()
        .expect("failed to create temp dir")
        .keep();
    let data_dir = dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("failed to create DuckLake data dir");

    let (catalog_database, catalog_url) = create_catalog_database().await;
    let data_url = path_to_file_url(&data_dir);

    println!("[{test_name}] catalog : {catalog_url}");
    println!("[{test_name}] data    : {}", data_dir.display());

    DuckLakeTestEnv { _catalog_database: catalog_database, catalog_url, data_dir, data_url }
}

pub fn catalog_attach_target(catalog_url: &Url) -> String {
    match catalog_url.scheme() {
        "postgres" | "postgresql" => postgres_catalog_attach_target(catalog_url),
        scheme => panic!("unsupported DuckLake catalog URL scheme `{scheme}`"),
    }
}

pub fn path_to_file_url(path: &Path) -> Url {
    Url::from_file_path(path).expect("failed to convert path to file url")
}

async fn create_catalog_database() -> (PgDatabase<Client>, Url) {
    let database_name = Uuid::new_v4().to_string();
    let host = env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set");
    let port = env::var("TESTS_DATABASE_PORT")
        .expect("TESTS_DATABASE_PORT must be set")
        .parse()
        .expect("TESTS_DATABASE_PORT must be a valid port number");
    let username =
        env::var("TESTS_DATABASE_USERNAME").expect("TESTS_DATABASE_USERNAME must be set");
    let password = env::var("TESTS_DATABASE_PASSWORD").ok();
    let database = PgDatabase::new(local_pg_connection_config(database_name.clone())).await;

    let mut catalog_url = Url::parse("postgres://localhost").expect("failed to parse base url");
    catalog_url.set_host(Some(&host)).expect("failed to set catalog host");
    catalog_url.set_port(Some(port)).expect("failed to set catalog port");
    catalog_url.set_username(&username).expect("failed to set catalog username");
    catalog_url.set_password(password.as_deref()).expect("failed to set catalog password");
    catalog_url.set_path(&database_name);

    (database, catalog_url)
}

fn local_pg_connection_config(database_name: String) -> PgConnectionConfig {
    PgConnectionConfig {
        host: env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set"),
        port: env::var("TESTS_DATABASE_PORT")
            .expect("TESTS_DATABASE_PORT must be set")
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number"),
        name: database_name,
        username: env::var("TESTS_DATABASE_USERNAME").expect("TESTS_DATABASE_USERNAME must be set"),
        password: env::var("TESTS_DATABASE_PASSWORD").ok().map(Into::into),
        tls: TlsConfig { trusted_root_certs: String::new(), enabled: false },
        keepalive: TcpKeepaliveConfig::default(),
    }
}

fn postgres_catalog_attach_target(catalog_url: &Url) -> String {
    let config =
        PgConfig::from_str(catalog_url.as_str()).expect("failed to parse Postgres catalog url");
    let mut parts = Vec::new();

    let hosts = serialize_hosts(config.get_hosts());
    if !hosts.is_empty() {
        push_conninfo_pair(&mut parts, "host", &hosts);
    }

    let ports = config
        .get_ports()
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    if !ports.is_empty() {
        push_conninfo_pair(&mut parts, "port", &ports);
    }

    if let Some(dbname) = config.get_dbname() {
        push_conninfo_pair(&mut parts, "dbname", dbname);
    }
    if let Some(user) = config.get_user() {
        push_conninfo_pair(&mut parts, "user", user);
    }
    if let Some(password) = config.get_password() {
        let password = std::str::from_utf8(password).expect("catalog password must be valid utf-8");
        push_conninfo_pair(&mut parts, "password", password);
    }
    if config.get_ssl_mode() != SslMode::Prefer {
        parts.push(format!("sslmode={}", ssl_mode_to_str(config.get_ssl_mode())));
    }

    format!("postgres:{}", parts.join(" "))
}

fn serialize_hosts(hosts: &[Host]) -> String {
    let mut values = Vec::with_capacity(hosts.len());
    for host in hosts {
        match host {
            Host::Tcp(host) => values.push(host.clone()),
            #[cfg(unix)]
            Host::Unix(path) => values.push(
                path.to_str().expect("catalog unix socket path must be valid utf-8").to_owned(),
            ),
        }
    }

    values.join(",")
}

fn push_conninfo_pair(parts: &mut Vec<String>, key: &str, value: &str) {
    parts.push(format!("{key}={}", quote_libpq_conninfo_value(value)));
}

fn quote_libpq_conninfo_value(value: &str) -> String {
    let mut quoted = String::from("'");
    for ch in value.chars() {
        if matches!(ch, '\'' | '\\') {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

fn ssl_mode_to_str(ssl_mode: SslMode) -> &'static str {
    match ssl_mode {
        SslMode::Disable => "disable",
        SslMode::Prefer => "prefer",
        SslMode::Require => "require",
        SslMode::VerifyCa => "verify-ca",
        SslMode::VerifyFull => "verify-full",
        _ => panic!("unsupported sslmode"),
    }
}
