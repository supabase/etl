//! One-shot DuckLake maintenance execution.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, error, fmt,
    path::{Path, PathBuf},
    process,
    str::FromStr,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use metrics::{gauge, histogram};
use pg_escape::{quote_identifier, quote_literal};
use regex::Regex;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
use tokio::{
    sync::{Semaphore, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tokio_postgres::{
    Config as PgConfig,
    config::{Host, SslMode},
};
use tracing::{debug, info, trace, warn};
use url::Url;

const LAKE_CATALOG: &str = "lake";
const ATTACH_DATA_INLINING_ROW_LIMIT: u64 = 10_000;
const DUCKDB_EXTENSION_ROOT_ENV_VAR: &str = "ETL_DUCKDB_EXTENSION_ROOT";
const CONTAINER_DUCKDB_EXTENSION_ROOT: &str = "/app/duckdb_extensions";
const DUCKDB_EXTENSION_VERSION: &str = "1.5.2";
const DUCKLAKE_EXTENSION_FILE: &str = "ducklake.duckdb_extension";
const HTTPFS_EXTENSION_FILE: &str = "httpfs.duckdb_extension";
const POSTGRES_SCANNER_EXTENSION_FILE: &str = "postgres_scanner.duckdb_extension";
const TARGET_FILE_SIZE_OPTION_NAME: &str = "target_file_size";
const MAINTENANCE_TARGET_FILE_SIZE: &str = "10MB";
const PARQUET_COMPRESSION_OPTION_NAME: &str = "parquet_compression";
const PARQUET_COMPRESSION_OPTION_VALUE: &str = "zstd";
const PARQUET_ROW_GROUP_SIZE_BYTES_OPTION_NAME: &str = "parquet_row_group_size_bytes";
const PARQUET_ROW_GROUP_SIZE_BYTES_OPTION_VALUE: &str = "10MB";
const PARQUET_VERSION_OPTION_NAME: &str = "parquet_version";
const PARQUET_VERSION_OPTION_VALUE: u8 = 2;
const PRESERVE_INSERTION_ORDER_OPTION_NAME: &str = "preserve_insertion_order";
const MAINTENANCE_QUERY_TIMEOUT: Duration = Duration::from_secs(3 * 60);
const BLOCKING_ABORT_GRACE: Duration = Duration::from_secs(30);
const DUCKDB_MAINTENANCE_OPERATION_KIND: &str = "maintenance";
const ETL_DUCKLAKE_INLINE_FLUSH_ROWS: &str = "etl_ducklake_inline_flush_rows";
const ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS: &str =
    "etl_ducklake_inline_flush_duration_seconds";
const ETL_DUCKLAKE_TABLE_ACTIVE_INLINED_DATA_BYTES: &str =
    "etl_ducklake_table_active_inlined_data_bytes";
const RESULT_LABEL: &str = "result";
const TABLE_LABEL: &str = "table";
const SMALL_FILE_SIZE_BYTES: i64 = 5 * 1024 * 1024;

static POSTGRES_PASSWORD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"password=(?:'([^'\\]|\\.)*'|[^\s,);]+)")
        .expect("postgres password redaction regex should compile")
});

/// S3-compatible storage credentials for DuckDB's httpfs extension.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 access key id.
    pub access_key_id: String,
    /// S3 secret access key.
    pub secret_access_key: String,
    /// AWS region or equivalent.
    pub region: String,
    /// Optional S3-compatible endpoint.
    pub endpoint: Option<String>,
    /// S3 URL style.
    pub url_style: String,
    /// Whether to use HTTPS.
    pub use_ssl: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DuckLakeSetupStep {
    label: &'static str,
    sql: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DuckLakeSetupPlan {
    steps: Vec<DuckLakeSetupStep>,
}

impl DuckLakeSetupPlan {
    fn steps(&self) -> &[DuckLakeSetupStep] {
        &self.steps
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DuckDbExtensionStrategy {
    VendoredLocal { platform_dir: &'static str },
    InstallFromRepository,
}

impl DuckDbExtensionStrategy {
    fn disables_autoload(self) -> bool {
        matches!(self, Self::VendoredLocal { .. })
    }
}

fn configure_writer_session_sql() -> String {
    format!("SET {PRESERVE_INSERTION_ORDER_OPTION_NAME} = false;")
}

fn configure_parquet_settings_sql() -> String {
    format!(
        "CALL {LAKE_CATALOG}.set_option({}, {}); CALL {LAKE_CATALOG}.set_option({}, {}); CALL \
         {LAKE_CATALOG}.set_option({}, {});",
        quote_literal(PARQUET_COMPRESSION_OPTION_NAME),
        quote_literal(PARQUET_COMPRESSION_OPTION_VALUE),
        quote_literal(PARQUET_ROW_GROUP_SIZE_BYTES_OPTION_NAME),
        quote_literal(PARQUET_ROW_GROUP_SIZE_BYTES_OPTION_VALUE),
        quote_literal(PARQUET_VERSION_OPTION_NAME),
        PARQUET_VERSION_OPTION_VALUE,
    )
}

fn resolve_maintenance_target_file_size(maintenance_target_file_size: Option<&str>) -> &str {
    maintenance_target_file_size.unwrap_or(MAINTENANCE_TARGET_FILE_SIZE)
}

fn maintenance_target_file_size_sql(maintenance_target_file_size: Option<&str>) -> String {
    format!(
        "CALL ducklake_set_option({}, {}, {});",
        quote_literal(LAKE_CATALOG),
        quote_literal(TARGET_FILE_SIZE_OPTION_NAME),
        quote_literal(resolve_maintenance_target_file_size(maintenance_target_file_size,)),
    )
}

fn current_duckdb_extension_strategy() -> EtlResult<DuckDbExtensionStrategy> {
    duckdb_extension_strategy(
        env::consts::OS,
        env::consts::ARCH,
        env::var_os(DUCKDB_EXTENSION_ROOT_ENV_VAR).map(PathBuf::from),
        Path::new(CONTAINER_DUCKDB_EXTENSION_ROOT),
        &repo_vendored_extension_root(),
    )
}

fn duckdb_extension_strategy(
    os: &str,
    arch: &str,
    env_override: Option<PathBuf>,
    container_root: &Path,
    repo_root: &Path,
) -> EtlResult<DuckDbExtensionStrategy> {
    match os {
        "linux" => {
            let platform_dir = match arch {
                "x86_64" | "amd64" => "linux_amd64",
                "aarch64" | "arm64" => "linux_arm64",
                _ => {
                    return Err(etl_error!(
                        ErrorKind::ConfigError,
                        "Unsupported DuckDB extension platform",
                        format!(
                            "linux architecture `{arch}` is not supported for vendored DuckDB \
                             extensions"
                        )
                    ));
                }
            };

            if vendored_extension_dir(platform_dir, env_override, container_root, repo_root)?
                .is_some()
            {
                Ok(DuckDbExtensionStrategy::VendoredLocal { platform_dir })
            } else {
                Ok(DuckDbExtensionStrategy::InstallFromRepository)
            }
        }
        "macos" => {
            let platform_dir = match arch {
                "x86_64" | "amd64" => "osx_amd64",
                "aarch64" | "arm64" => "osx_arm64",
                _ => {
                    return Err(etl_error!(
                        ErrorKind::ConfigError,
                        "Unsupported DuckDB extension platform",
                        format!(
                            "macos architecture `{arch}` is not supported for vendored DuckDB \
                             extensions"
                        )
                    ));
                }
            };

            if vendored_extension_dir(platform_dir, env_override, container_root, repo_root)?
                .is_some()
            {
                Ok(DuckDbExtensionStrategy::VendoredLocal { platform_dir })
            } else {
                Ok(DuckDbExtensionStrategy::InstallFromRepository)
            }
        }
        "windows" => Ok(DuckDbExtensionStrategy::InstallFromRepository),
        _ => Err(etl_error!(
            ErrorKind::ConfigError,
            "Unsupported DuckDB extension platform",
            format!("operating system `{os}` is not supported for DuckDB extensions")
        )),
    }
}

fn repo_vendored_extension_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")))
        .join("vendor/duckdb/extensions")
}

fn vendored_extension_dir(
    platform_dir: &str,
    env_override: Option<PathBuf>,
    container_root: &Path,
    repo_root: &Path,
) -> EtlResult<Option<PathBuf>> {
    if let Some(root) = env_override {
        let directory = root.join(DUCKDB_EXTENSION_VERSION).join(platform_dir);
        ensure_vendored_extension_dir(&directory)?;
        return Ok(Some(directory));
    }

    for root in [container_root, repo_root] {
        let directory = root.join(DUCKDB_EXTENSION_VERSION).join(platform_dir);
        if !directory.exists() {
            continue;
        }
        ensure_vendored_extension_dir(&directory)?;
        return Ok(Some(directory));
    }

    Ok(None)
}

fn require_vendored_extension_dir(
    platform_dir: &str,
    env_override: Option<PathBuf>,
    container_root: &Path,
    repo_root: &Path,
) -> EtlResult<PathBuf> {
    vendored_extension_dir(platform_dir, env_override, container_root, repo_root)?.ok_or_else(
        || {
            etl_error!(
                ErrorKind::ConfigError,
                "Vendored DuckDB extensions not found",
                format!(
                    "expected vendored DuckDB extensions in one of: {}, {}",
                    container_root.join(DUCKDB_EXTENSION_VERSION).join(platform_dir).display(),
                    repo_root.join(DUCKDB_EXTENSION_VERSION).join(platform_dir).display()
                )
            )
        },
    )
}

fn ensure_vendored_extension_dir(directory: &Path) -> EtlResult<()> {
    let missing = [DUCKLAKE_EXTENSION_FILE]
        .into_iter()
        .filter(|filename| !directory.join(filename).is_file())
        .collect::<Vec<_>>();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(etl_error!(
            ErrorKind::ConfigError,
            "Vendored DuckDB extensions not found",
            format!("missing {} in `{}`", missing.join(", "), directory.display())
        ))
    }
}

fn vendored_extension_path(extension_dir: &Path, filename: &str) -> EtlResult<String> {
    extension_dir.join(filename).to_str().map(std::string::ToString::to_string).ok_or_else(|| {
        etl_error!(
            ErrorKind::ConfigError,
            "Vendored DuckDB extension path contains non-utf8 characters",
            extension_dir.display().to_string()
        )
    })
}

fn catalog_conninfo_from_url(catalog_url: &Url) -> EtlResult<String> {
    match catalog_url.scheme() {
        "postgres" | "postgresql" => {}
        scheme => {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "Unsupported DuckLake catalog URL scheme",
                format!("catalog URL scheme `{scheme}` is not supported")
            ));
        }
    }

    let config = PgConfig::from_str(catalog_url.as_str()).map_err(|source| {
        etl_error!(
            ErrorKind::ConfigError,
            "Invalid DuckLake PostgreSQL catalog URL",
            source: source
        )
    })?;
    let explicit_query_options = explicit_query_options(catalog_url);
    reject_unsupported_query_options(&explicit_query_options)?;

    let mut parts = Vec::new();

    let hosts = serialize_hosts(config.get_hosts())?;
    if !hosts.is_empty() {
        push_conninfo_pair(&mut parts, "host", &hosts);
    }

    let hostaddrs = config
        .get_hostaddrs()
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    if !hostaddrs.is_empty() {
        push_conninfo_pair(&mut parts, "hostaddr", &hostaddrs);
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
        let password = std::str::from_utf8(password).map_err(|source| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 password",
                source: source
            )
        })?;
        push_conninfo_pair(&mut parts, "password", password);
    }
    if let Some(options) = config.get_options() {
        push_conninfo_pair(&mut parts, "options", options);
    }
    if let Some(application_name) = config.get_application_name() {
        push_conninfo_pair(&mut parts, "application_name", application_name);
    }
    if let Some(ssl_cert) = config.get_ssl_cert() {
        let ssl_cert = std::str::from_utf8(ssl_cert).map_err(|source| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 sslcert",
                source: source
            )
        })?;
        push_conninfo_pair(&mut parts, "sslcert", ssl_cert);
    }
    if let Some(ssl_key) = config.get_ssl_key() {
        let ssl_key = std::str::from_utf8(ssl_key).map_err(|source| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 sslkey",
                source: source
            )
        })?;
        push_conninfo_pair(&mut parts, "sslkey", ssl_key);
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        let ssl_root_cert = std::str::from_utf8(ssl_root_cert).map_err(|source| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 sslrootcert",
                source: source
            )
        })?;
        push_conninfo_pair(&mut parts, "sslrootcert", ssl_root_cert);
    }

    if explicit_query_options.contains("sslmode") {
        parts.push(format!("sslmode={}", ssl_mode_to_str(config.get_ssl_mode())?));
    }

    if explicit_query_options.contains("connect_timeout")
        && let Some(connect_timeout) = config.get_connect_timeout()
    {
        parts.push(format!("connect_timeout={}", connect_timeout.as_secs()));
    }
    if explicit_query_options.contains("tcp_user_timeout")
        && let Some(tcp_user_timeout) = config.get_tcp_user_timeout()
    {
        parts.push(format!("tcp_user_timeout={}", tcp_user_timeout.as_millis()));
    }

    if explicit_query_options.contains("keepalives") {
        parts.push(format!("keepalives={}", if config.get_keepalives() { "1" } else { "0" }));
    }
    if explicit_query_options.contains("keepalives_idle") {
        parts.push(format!("keepalives_idle={}", config.get_keepalives_idle().as_secs()));
    }
    if explicit_query_options.contains("keepalives_interval")
        && let Some(keepalives_interval) = config.get_keepalives_interval()
    {
        parts.push(format!("keepalives_interval={}", keepalives_interval.as_secs()));
    }
    if explicit_query_options.contains("keepalives_retries")
        && let Some(keepalives_retries) = config.get_keepalives_retries()
    {
        parts.push(format!("keepalives_retries={keepalives_retries}"));
    }

    Ok(format!("postgres:{}", parts.join(" ")))
}

fn explicit_query_options(catalog_url: &Url) -> BTreeSet<String> {
    catalog_url.query_pairs().map(|(key, _)| key.into_owned()).collect()
}

fn reject_unsupported_query_options(explicit_query_options: &BTreeSet<String>) -> EtlResult<()> {
    let unsupported =
        ["channel_binding", "load_balance_hosts", "replication", "target_session_attrs"]
            .into_iter()
            .filter(|key| explicit_query_options.contains(*key))
            .collect::<Vec<_>>();

    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake PostgreSQL catalog URL uses unsupported query parameters",
            format!("unsupported parameters: {}", unsupported.join(", "))
        ))
    }
}

fn catalog_attach_target(catalog_url: &Url) -> EtlResult<String> {
    match catalog_url.scheme() {
        "file" => Ok(catalog_url.as_str().to_owned()),
        "postgres" | "postgresql" => catalog_conninfo_from_url(catalog_url),
        scheme => Err(etl_error!(
            ErrorKind::ConfigError,
            "Unsupported DuckLake catalog URL scheme",
            format!("catalog URL scheme `{scheme}` is not supported")
        )),
    }
}

fn serialize_hosts(hosts: &[Host]) -> EtlResult<String> {
    let mut values = Vec::with_capacity(hosts.len());
    for host in hosts {
        match host {
            Host::Tcp(host) => values.push(host.clone()),
            #[cfg(unix)]
            Host::Unix(path) => {
                let path = path.to_str().ok_or_else(|| {
                    etl_error!(
                        ErrorKind::ConfigError,
                        "DuckLake PostgreSQL catalog URL contains non-utf8 unix socket path"
                    )
                })?;
                values.push(path.to_owned());
            }
        }
    }

    Ok(values.join(","))
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

fn ssl_mode_to_str(ssl_mode: SslMode) -> EtlResult<&'static str> {
    match ssl_mode {
        SslMode::Disable => Ok("disable"),
        SslMode::Prefer => Ok("prefer"),
        SslMode::Require => Ok("require"),
        SslMode::VerifyCa => Ok("verify-ca"),
        SslMode::VerifyFull => Ok("verify-full"),
        _ => Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake PostgreSQL catalog URL uses an unsupported sslmode"
        )),
    }
}

fn validate_data_path(data_path: &Url) -> EtlResult<&str> {
    match data_path.scheme() {
        "file" | "s3" | "gs" => Ok(data_path.as_str()),
        scheme => Err(etl_error!(
            ErrorKind::ConfigError,
            "Unsupported DuckLake data URL scheme",
            format!("data URL scheme `{scheme}` is not supported")
        )),
    }
}

fn build_setup_plan(
    catalog_url: &Url,
    data_path: &Url,
    s3: Option<&S3Config>,
    metadata_schema: Option<&str>,
) -> EtlResult<DuckLakeSetupPlan> {
    let strategy = current_duckdb_extension_strategy()?;
    let vendored_root = match strategy {
        DuckDbExtensionStrategy::VendoredLocal { platform_dir } => {
            Some(require_vendored_extension_dir(
                platform_dir,
                env::var_os(DUCKDB_EXTENSION_ROOT_ENV_VAR).map(PathBuf::from),
                Path::new(CONTAINER_DUCKDB_EXTENSION_ROOT),
                &repo_vendored_extension_root(),
            )?)
        }
        DuckDbExtensionStrategy::InstallFromRepository => None,
    };
    build_setup_plan_with_strategy(
        catalog_url,
        data_path,
        s3,
        metadata_schema,
        strategy,
        vendored_root.as_deref(),
    )
}

fn build_setup_plan_with_strategy(
    catalog_url: &Url,
    data_path: &Url,
    s3: Option<&S3Config>,
    metadata_schema: Option<&str>,
    strategy: DuckDbExtensionStrategy,
    vendored_root: Option<&Path>,
) -> EtlResult<DuckLakeSetupPlan> {
    let catalog_target = catalog_attach_target(catalog_url)?;
    let data_path = validate_data_path(data_path)?;

    let needs_postgres = matches!(catalog_url.scheme(), "postgres" | "postgresql");
    let needs_httpfs = matches!(data_path.split(':').next(), Some("s3" | "gs"));
    let lake_catalog = quote_identifier(LAKE_CATALOG);
    let mut steps = vec![DuckLakeSetupStep {
        label: "configure_writer_session",
        sql: configure_writer_session_sql(),
    }];
    let mut secret_options = BTreeMap::from([
        ("KEY_ID", quote_literal(s3.map(|s| s.access_key_id.as_str()).unwrap_or_default())),
        ("REGION", quote_literal(s3.map(|s| s.region.as_str()).unwrap_or_default())),
        ("SECRET", quote_literal(s3.map(|s| s.secret_access_key.as_str()).unwrap_or_default())),
        ("URL_STYLE", quote_literal(s3.map(|s| s.url_style.as_str()).unwrap_or_default())),
    ]);

    let extension_sql = match strategy {
        DuckDbExtensionStrategy::VendoredLocal { .. } => {
            let extension_root = vendored_root.ok_or_else(|| {
                etl_error!(
                    ErrorKind::ConfigError,
                    "Vendored DuckDB extensions not found",
                    "Missing vendored DuckDB extension root"
                )
            })?;
            let ducklake_extension =
                vendored_extension_path(extension_root, DUCKLAKE_EXTENSION_FILE)?;
            let mut sql = format!("LOAD {};", quote_literal(&ducklake_extension));
            if needs_postgres {
                let postgres_extension =
                    vendored_extension_path(extension_root, POSTGRES_SCANNER_EXTENSION_FILE)?;
                sql.push_str(&format!(" LOAD {};", quote_literal(&postgres_extension)));
            }
            if needs_httpfs {
                let httpfs_extension =
                    vendored_extension_path(extension_root, HTTPFS_EXTENSION_FILE)?;
                sql.push_str(&format!(" LOAD {};", quote_literal(&httpfs_extension)));
            }
            sql
        }
        DuckDbExtensionStrategy::InstallFromRepository => {
            let mut sql = String::from("INSTALL ducklake; LOAD ducklake;");
            if needs_postgres {
                sql.push_str(" INSTALL postgres; LOAD postgres;");
            }
            if needs_httpfs {
                sql.push_str(" INSTALL httpfs; LOAD httpfs;");
            }
            sql
        }
    };
    steps.push(DuckLakeSetupStep { label: "load_extensions", sql: extension_sql });

    if needs_httpfs && let Some(s3) = s3 {
        let secret_name = quote_identifier("ducklake_s3");
        if let Some(endpoint) = &s3.endpoint {
            secret_options.insert("ENDPOINT", quote_literal(endpoint));
        }

        let secret_body = secret_options
            .iter()
            .map(|(key, value)| format!("{key} {value}"))
            .collect::<Vec<_>>()
            .join(", ");

        steps.push(DuckLakeSetupStep {
            label: "configure_object_store",
            sql: format!(
                "SET enable_http_metadata_cache = true; SET parquet_metadata_cache = true; CREATE \
                 OR REPLACE SECRET {secret_name} (TYPE S3, {secret_body}, USE_SSL {});",
                if s3.use_ssl { "true" } else { "false" }
            ),
        });
    }
    let metadata_schema_clause = metadata_schema
        .map(|schema| format!(", METADATA_SCHEMA {}", quote_literal(schema)))
        .unwrap_or_default();

    steps.push(DuckLakeSetupStep {
        label: "attach_catalog",
        sql: format!(
            "ATTACH {} AS {lake_catalog} (DATA_PATH {}, DATA_INLINING_ROW_LIMIT {}, \
             AUTOMATIC_MIGRATION true{metadata_schema_clause});",
            quote_literal(&format!("ducklake:{catalog_target}")),
            quote_literal(data_path),
            ATTACH_DATA_INLINING_ROW_LIMIT
        ),
    });
    steps.push(DuckLakeSetupStep {
        label: "configure_parquet",
        sql: configure_parquet_settings_sql(),
    });

    Ok(DuckLakeSetupPlan { steps })
}

#[derive(Debug)]
struct DuckLakeConnectionError {
    message: String,
}

impl DuckLakeConnectionError {
    fn setup_phase(step: &DuckLakeSetupStep, error: duckdb::Error) -> Self {
        let error_message = error.to_string();
        let error_message = if attach_step_uses_postgres_catalog(step) {
            POSTGRES_PASSWORD_REGEX.replace_all(&error_message, "password='[redacted]'").to_string()
        } else {
            error_message
        };

        Self {
            message: format!(
                "ducklake duckdb connection setup phase `{}` failed: {error_message}",
                step.label
            ),
        }
    }

    fn validation(error: duckdb::Error) -> Self {
        Self { message: format!("DuckLake DuckDB connection validation failed: {error}") }
    }
}

impl fmt::Display for DuckLakeConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(&self.message)
    }
}

impl error::Error for DuckLakeConnectionError {}

fn remaining_ms_until(deadline: Instant) -> u64 {
    deadline.checked_duration_since(Instant::now()).unwrap_or(Duration::ZERO).as_millis() as u64
}

/// One DuckDB connection tracked by the maintenance pool.
struct ManagedDuckLakeConnection {
    conn: duckdb::Connection,
    broken: bool,
}

/// Async watchdog that interrupts one timed maintenance query when its deadline expires.
struct DuckDbMaintenanceWatchdog {
    timeout: Duration,
    timed_out: Arc<AtomicBool>,
    interrupt_tx: Option<oneshot::Sender<Arc<duckdb::InterruptHandle>>>,
    done_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl DuckDbMaintenanceWatchdog {
    fn spawn(deadline: Instant, timeout: Duration) -> Self {
        let timed_out = Arc::new(AtomicBool::new(false));
        let timeout_flag = Arc::clone(&timed_out);
        let (interrupt_tx, interrupt_rx) = oneshot::channel::<Arc<duckdb::InterruptHandle>>();
        let (done_tx, done_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timeout_ms = timeout.as_millis() as u64,
                deadline_remaining_ms = remaining_ms_until(deadline),
                "ducklake maintenance query watchdog task started: operation_kind={}, \
                 timeout_ms={}, deadline_remaining_ms={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                timeout.as_millis(),
                remaining_ms_until(deadline)
            );
            let mut interrupt_rx = Box::pin(interrupt_rx);
            let mut done_rx = Box::pin(done_rx);
            let interrupt_handle = tokio::select! {
                biased;
                _ = &mut done_rx => {
                    info!(
                        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                        "ducklake maintenance query watchdog finished before interrupt handle: \
                         operation_kind={}",
                        DUCKDB_MAINTENANCE_OPERATION_KIND
                    );
                    return;
                },
                result = &mut interrupt_rx => match result {
                    Ok(handle) => {
                        info!(
                            operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                            deadline_remaining_ms = remaining_ms_until(deadline),
                            "ducklake maintenance query watchdog received interrupt handle before \
                             deadline: operation_kind={}, deadline_remaining_ms={}",
                            DUCKDB_MAINTENANCE_OPERATION_KIND,
                            remaining_ms_until(deadline)
                        );
                        handle
                    },
                    Err(_) => {
                        warn!(
                            operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                            "ducklake maintenance query watchdog interrupt sender dropped before \
                             deadline: operation_kind={}",
                            DUCKDB_MAINTENANCE_OPERATION_KIND
                        );
                        return;
                    },
                },
                _ = tokio::time::sleep_until(deadline) => {
                    timeout_flag.store(true, Ordering::Relaxed);
                    warn!(
                        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                        timeout_ms = timeout.as_millis() as u64,
                        "ducklake maintenance query watchdog deadline elapsed before interrupt \
                         handle: operation_kind={}, timeout_ms={}",
                        DUCKDB_MAINTENANCE_OPERATION_KIND,
                        timeout.as_millis()
                    );
                    tokio::select! {
                        biased;
                        _ = &mut done_rx => {
                            info!(
                                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                                "ducklake maintenance query watchdog received done after deadline \
                                 before interrupt handle: operation_kind={}",
                                DUCKDB_MAINTENANCE_OPERATION_KIND
                            );
                            return;
                        },
                        result = &mut interrupt_rx => match result {
                            Ok(handle) => {
                                warn!(
                                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                                    "ducklake maintenance query watchdog received interrupt handle \
                                     after deadline: operation_kind={}",
                                    DUCKDB_MAINTENANCE_OPERATION_KIND
                                );
                                handle
                            },
                            Err(_) => {
                                warn!(
                                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                                    "ducklake maintenance query watchdog interrupt sender dropped \
                                     after deadline: operation_kind={}",
                                    DUCKDB_MAINTENANCE_OPERATION_KIND
                                );
                                return;
                            },
                        },
                    }
                },
            };

            if timeout_flag.load(Ordering::Relaxed) {
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    "ducklake maintenance query watchdog calling interrupt after timeout: \
                     operation_kind={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND
                );
                interrupt_handle.interrupt();
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    "ducklake maintenance query watchdog interrupt returned after timeout: \
                     operation_kind={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND
                );
                return;
            }

            tokio::select! {
                biased;
                _ = &mut done_rx => {
                    info!(
                        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                        deadline_remaining_ms = remaining_ms_until(deadline),
                        "ducklake maintenance query watchdog received done before deadline after \
                         interrupt handle: operation_kind={}, deadline_remaining_ms={}",
                        DUCKDB_MAINTENANCE_OPERATION_KIND,
                        remaining_ms_until(deadline)
                    );
                }
                _ = tokio::time::sleep_until(deadline) => {
                    timeout_flag.store(true, Ordering::Relaxed);
                    warn!(
                        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                        timeout_ms = timeout.as_millis() as u64,
                        "ducklake maintenance query watchdog deadline elapsed after interrupt \
                         handle; calling interrupt: operation_kind={}, timeout_ms={}",
                        DUCKDB_MAINTENANCE_OPERATION_KIND,
                        timeout.as_millis()
                    );
                    interrupt_handle.interrupt();
                    warn!(
                        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                        "ducklake maintenance query watchdog interrupt returned after \
                         handle/deadline path: operation_kind={}",
                        DUCKDB_MAINTENANCE_OPERATION_KIND
                    );
                }
            }
        });

        Self {
            timeout,
            timed_out,
            interrupt_tx: Some(interrupt_tx),
            done_tx: Some(done_tx),
            task: Some(task),
        }
    }

    fn publish_interrupt_handle(&mut self, handle: Arc<duckdb::InterruptHandle>) {
        if let Some(interrupt_tx) = self.interrupt_tx.take() {
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timeout_ms = self.timeout.as_millis() as u64,
                "ducklake maintenance query watchdog publishing interrupt handle: \
                 operation_kind={}, timeout_ms={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                self.timeout.as_millis()
            );
            let _ = interrupt_tx.send(handle);
        } else {
            warn!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                "ducklake maintenance query watchdog interrupt handle publish skipped because sender \
                 is gone: operation_kind={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND
            );
        }
    }

    fn finish(&mut self) {
        if let Some(done_tx) = self.done_tx.take() {
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timed_out = self.timed_out(),
                "ducklake maintenance query watchdog finish signal sent: operation_kind={}, \
                 timed_out={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                self.timed_out()
            );
            let _ = done_tx.send(());
        } else {
            warn!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timed_out = self.timed_out(),
                "ducklake maintenance query watchdog finish skipped because sender is gone: \
                 operation_kind={}, timed_out={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                self.timed_out()
            );
        }
    }

    fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Relaxed)
    }

    fn async_task_handle(&mut self) -> EtlResult<JoinHandle<()>> {
        self.task.take().ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationError,
                "Cannot get async task handle from DuckLake maintenance watchdog: task is None"
            )
        })
    }
}

fn duckdb_maintenance_timeout_error(timeout: Duration, stage: &'static str) -> EtlError {
    etl_error!(
        ErrorKind::DestinationQueryFailed,
        "DuckLake maintenance blocking operation timed out",
        format!(
            "operation_kind={}, stage={stage}, timeout_ms={}",
            DUCKDB_MAINTENANCE_OPERATION_KIND,
            timeout.as_millis()
        )
    )
}

fn abort_stuck_duckdb_maintenance_operation(timeout: Duration, abort_grace: Duration) -> ! {
    tracing::error!(
        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
        timeout_ms = timeout.as_millis() as u64,
        abort_grace_ms = abort_grace.as_millis() as u64,
        "ducklake maintenance blocking operation did not return after interrupt grace; aborting \
         process: operation_kind={}, timeout_ms={}, abort_grace_ms={}",
        DUCKDB_MAINTENANCE_OPERATION_KIND,
        timeout.as_millis(),
        abort_grace.as_millis()
    );
    process::abort();
}

#[derive(Clone)]
struct DuckLakeConnectionManager {
    setup_plan: Arc<DuckLakeSetupPlan>,
    disable_extension_autoload: bool,
}

impl r2d2::ManageConnection for DuckLakeConnectionManager {
    type Connection = ManagedDuckLakeConnection;
    type Error = DuckLakeConnectionError;

    fn connect(&self) -> Result<ManagedDuckLakeConnection, DuckLakeConnectionError> {
        let conn = if self.disable_extension_autoload {
            duckdb::Connection::open_in_memory_with_flags(
                duckdb::Config::default()
                    .enable_autoload_extension(false)
                    .map_err(DuckLakeConnectionError::validation)?,
            )
            .map_err(DuckLakeConnectionError::validation)?
        } else {
            duckdb::Connection::open_in_memory().map_err(DuckLakeConnectionError::validation)?
        };
        for step in self.setup_plan.steps() {
            info!(phase = step.label, "starting ducklake duckdb connection setup phase");
            conn.execute_batch(&step.sql)
                .map_err(|error| DuckLakeConnectionError::setup_phase(step, error))?;
            info!(phase = step.label, "ducklake duckdb connection setup phase finished");
        }
        Ok(ManagedDuckLakeConnection { conn, broken: false })
    }

    fn is_valid(
        &self,
        conn: &mut ManagedDuckLakeConnection,
    ) -> Result<(), DuckLakeConnectionError> {
        conn.conn.execute_batch("SELECT 1").map_err(DuckLakeConnectionError::validation)
    }

    fn has_broken(&self, conn: &mut ManagedDuckLakeConnection) -> bool {
        conn.broken
    }
}

#[derive(Clone)]
struct DuckDbMaintenanceExecutor {
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
}

impl DuckDbMaintenanceExecutor {
    async fn run<R, F>(&self, operation: F) -> EtlResult<R>
    where
        R: Send + 'static,
        F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
    {
        self.run_with_timeout(MAINTENANCE_QUERY_TIMEOUT, operation).await
    }

    async fn run_with_timeout<R, F>(&self, timeout: Duration, operation: F) -> EtlResult<R>
    where
        R: Send + 'static,
        F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
    {
        let pool = Arc::clone(&self.pool);
        let blocking_slots = Arc::clone(&self.blocking_slots);
        let deadline = Instant::now() + timeout;

        info!(
            operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
            timeout_ms = timeout.as_millis() as u64,
            abort_grace_ms = BLOCKING_ABORT_GRACE.as_millis() as u64,
            available_permits = blocking_slots.available_permits(),
            "ducklake maintenance blocking operation starting: operation_kind={}, timeout_ms={}, \
             abort_grace_ms={}, available_permits={}",
            DUCKDB_MAINTENANCE_OPERATION_KIND,
            timeout.as_millis(),
            BLOCKING_ABORT_GRACE.as_millis(),
            blocking_slots.available_permits()
        );

        let slot_wait_started = Instant::now();
        info!(
            operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
            deadline_remaining_ms = remaining_ms_until(deadline),
            available_permits = blocking_slots.available_permits(),
            "ducklake maintenance blocking operation waiting for semaphore slot: \
             operation_kind={}, deadline_remaining_ms={}, available_permits={}",
            DUCKDB_MAINTENANCE_OPERATION_KIND,
            remaining_ms_until(deadline),
            blocking_slots.available_permits()
        );
        let permit = match tokio::time::timeout_at(
            deadline,
            Arc::clone(&blocking_slots).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                tracing::error!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    "ducklake maintenance blocking operation semaphore closed: operation_kind={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND
                );
                return Err(etl_error!(
                    ErrorKind::ApplyWorkerPanic,
                    "DuckLake maintenance blocking slot acquisition failed"
                ));
            }
            Err(_) => {
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    timeout_ms = timeout.as_millis() as u64,
                    slot_wait_ms = slot_wait_started.elapsed().as_millis() as u64,
                    "ducklake maintenance blocking operation timed out waiting for semaphore slot: \
                     operation_kind={}, timeout_ms={}, slot_wait_ms={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND,
                    timeout.as_millis(),
                    slot_wait_started.elapsed().as_millis()
                );
                return Err(duckdb_maintenance_timeout_error(timeout, "slot_wait"));
            }
        };
        trace!(
            wait_ms = slot_wait_started.elapsed().as_millis() as u64,
            "wait for ducklake maintenance blocking slot"
        );
        info!(
            operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
            slot_wait_ms = slot_wait_started.elapsed().as_millis() as u64,
            deadline_remaining_ms = remaining_ms_until(deadline),
            "ducklake maintenance blocking operation acquired semaphore slot: operation_kind={}, \
             slot_wait_ms={}, deadline_remaining_ms={}",
            DUCKDB_MAINTENANCE_OPERATION_KIND,
            slot_wait_started.elapsed().as_millis(),
            remaining_ms_until(deadline)
        );

        let mut watchdog = DuckDbMaintenanceWatchdog::spawn(deadline, timeout);
        let watchdog_task = watchdog.async_task_handle()?;
        let watchdog_timed_out = Arc::clone(&watchdog.timed_out);
        let abort_deadline = deadline + BLOCKING_ABORT_GRACE;

        let blocking_task = tokio::task::spawn_blocking(move || -> EtlResult<R> {
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                deadline_remaining_ms = remaining_ms_until(deadline),
                "ducklake maintenance blocking operation entered spawn_blocking task: \
                 operation_kind={}, deadline_remaining_ms={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                remaining_ms_until(deadline)
            );
            let _permit = permit;
            let checkout_timeout =
                deadline.checked_duration_since(Instant::now()).unwrap_or(Duration::ZERO);
            if checkout_timeout.is_zero() {
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    "ducklake maintenance blocking operation deadline reached before pool \
                     checkout: operation_kind={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND
                );
                return Err(duckdb_maintenance_timeout_error(timeout, "pool_checkout"));
            }

            let checkout_started = Instant::now();
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                checkout_timeout_ms = checkout_timeout.as_millis() as u64,
                "ducklake maintenance blocking operation checking out pooled connection: \
                 operation_kind={}, checkout_timeout_ms={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                checkout_timeout.as_millis()
            );
            let mut pooled_conn = match pool.get_timeout(checkout_timeout) {
                Ok(pooled_conn) => pooled_conn,
                Err(error) if Instant::now() >= deadline => {
                    warn!(
                        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                        checkout_wait_ms = checkout_started.elapsed().as_millis() as u64,
                        timeout_ms = timeout.as_millis() as u64,
                        error = %error,
                        "ducklake maintenance blocking operation timed out checking out pooled \
                         connection: operation_kind={}, checkout_wait_ms={}, timeout_ms={}, \
                         error={}",
                        DUCKDB_MAINTENANCE_OPERATION_KIND,
                        checkout_started.elapsed().as_millis(),
                        timeout.as_millis(),
                        error
                    );
                    return Err(duckdb_maintenance_timeout_error(timeout, "pool_checkout"));
                }
                Err(error) => {
                    warn!(
                        operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                        checkout_wait_ms = checkout_started.elapsed().as_millis() as u64,
                        error = %error,
                        "ducklake maintenance blocking operation failed checking out pooled \
                         connection: operation_kind={}, checkout_wait_ms={}, error={}",
                        DUCKDB_MAINTENANCE_OPERATION_KIND,
                        checkout_started.elapsed().as_millis(),
                        error
                    );
                    return Err(etl_error!(
                        ErrorKind::DestinationConnectionFailed,
                        "Failed to check out DuckLake maintenance connection",
                        source: error
                    ));
                }
            };
            trace!(
                wait_ms = checkout_started.elapsed().as_millis() as u64,
                "wait for ducklake maintenance pool checkout"
            );
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                checkout_wait_ms = checkout_started.elapsed().as_millis() as u64,
                deadline_remaining_ms = remaining_ms_until(deadline),
                "ducklake maintenance blocking operation checked out pooled connection: \
                 operation_kind={}, checkout_wait_ms={}, deadline_remaining_ms={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                checkout_started.elapsed().as_millis(),
                remaining_ms_until(deadline)
            );

            let operation_timeout =
                deadline.checked_duration_since(Instant::now()).unwrap_or(Duration::ZERO);
            if operation_timeout.is_zero() {
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    "ducklake maintenance blocking operation deadline reached before query \
                     execution: operation_kind={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND
                );
                pooled_conn.broken = true;
                return Err(duckdb_maintenance_timeout_error(timeout, "query_execution"));
            }

            let interrupt_handle = pooled_conn.conn.interrupt_handle();
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                operation_timeout_ms = operation_timeout.as_millis() as u64,
                "ducklake maintenance blocking operation publishing interrupt handle before query \
                 execution: operation_kind={}, operation_timeout_ms={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                operation_timeout.as_millis()
            );
            watchdog.publish_interrupt_handle(interrupt_handle);
            if watchdog.timed_out() {
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    "ducklake maintenance blocking operation timed out before query started; \
                     marking pooled connection broken: operation_kind={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND
                );
                pooled_conn.broken = true;
                return Err(duckdb_maintenance_timeout_error(timeout, "query_execution"));
            }

            let operation_started = Instant::now();
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                deadline_remaining_ms = remaining_ms_until(deadline),
                "ducklake maintenance blocking operation invoking DuckDB closure: \
                 operation_kind={}, deadline_remaining_ms={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                remaining_ms_until(deadline)
            );
            let res = operation(&pooled_conn.conn);
            let operation_duration_ms = operation_started.elapsed().as_millis() as u64;
            info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                duration_ms = operation_duration_ms,
                timed_out = watchdog.timed_out(),
                result_is_error = res.is_err(),
                "ducklake maintenance blocking operation DuckDB closure returned: \
                 operation_kind={}, duration_ms={}, timed_out={}, result_is_error={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                operation_duration_ms,
                watchdog.timed_out(),
                res.is_err()
            );
            watchdog.finish();
            trace!(
                duration_ms = operation_duration_ms,
                "ducklake maintenance blocking operation finished"
            );
            if watchdog.timed_out() {
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    duration_ms = operation_duration_ms,
                    "ducklake maintenance blocking operation returned after timeout; marking \
                     pooled connection broken: operation_kind={}, duration_ms={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND,
                    operation_duration_ms
                );
                pooled_conn.broken = true;
                return Err(duckdb_maintenance_timeout_error(timeout, "query_execution"));
            }
            if res.is_err() {
                warn!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    duration_ms = operation_duration_ms,
                    "ducklake maintenance blocking operation returned error; marking pooled \
                     connection broken: operation_kind={}, duration_ms={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND,
                    operation_duration_ms
                );
                pooled_conn.broken = true;
            } else {
                info!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    duration_ms = operation_duration_ms,
                    "ducklake maintenance blocking operation returned success; pooled connection \
                     remains healthy: operation_kind={}, duration_ms={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND,
                    operation_duration_ms
                );
            }

            res
        });

        info!(
            operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
            abort_deadline_remaining_ms = remaining_ms_until(abort_deadline),
            "ducklake maintenance blocking operation waiting for blocking task or abort deadline: \
             operation_kind={}, abort_deadline_remaining_ms={}",
            DUCKDB_MAINTENANCE_OPERATION_KIND,
            remaining_ms_until(abort_deadline)
        );
        let blocking_result = tokio::select! {
            biased;
            result = blocking_task => result,
            _ = tokio::time::sleep_until(abort_deadline) => {
                abort_stuck_duckdb_maintenance_operation(timeout, BLOCKING_ABORT_GRACE);
            }
        };

        match &blocking_result {
            Ok(Ok(_)) => info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timed_out = watchdog_timed_out.load(Ordering::Relaxed),
                "ducklake maintenance blocking operation task joined with success: \
                 operation_kind={}, timed_out={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                watchdog_timed_out.load(Ordering::Relaxed)
            ),
            Ok(Err(error)) => warn!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timed_out = watchdog_timed_out.load(Ordering::Relaxed),
                error = %error,
                "ducklake maintenance blocking operation task joined with error: operation_kind={}, \
                 timed_out={}, error={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                watchdog_timed_out.load(Ordering::Relaxed),
                error
            ),
            Err(error) => tracing::error!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timed_out = watchdog_timed_out.load(Ordering::Relaxed),
                error = %error,
                "ducklake maintenance blocking operation task join failed: operation_kind={}, \
                 timed_out={}, error={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                watchdog_timed_out.load(Ordering::Relaxed),
                error
            ),
        }

        info!(
            operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
            timed_out = watchdog_timed_out.load(Ordering::Relaxed),
            "ducklake maintenance blocking operation awaiting watchdog task: operation_kind={}, \
             timed_out={}",
            DUCKDB_MAINTENANCE_OPERATION_KIND,
            watchdog_timed_out.load(Ordering::Relaxed)
        );
        match watchdog_task.await {
            Ok(()) => info!(
                operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                timed_out = watchdog_timed_out.load(Ordering::Relaxed),
                "ducklake maintenance blocking operation watchdog task joined: operation_kind={}, \
                 timed_out={}",
                DUCKDB_MAINTENANCE_OPERATION_KIND,
                watchdog_timed_out.load(Ordering::Relaxed)
            ),
            Err(error) => {
                tracing::error!(
                    operation_kind = DUCKDB_MAINTENANCE_OPERATION_KIND,
                    timed_out = watchdog_timed_out.load(Ordering::Relaxed),
                    error = %error,
                    "ducklake maintenance blocking operation watchdog task panicked: \
                     operation_kind={}, timed_out={}, error={}",
                    DUCKDB_MAINTENANCE_OPERATION_KIND,
                    watchdog_timed_out.load(Ordering::Relaxed),
                    error
                );
                return Err(etl_error!(
                    ErrorKind::ApplyWorkerPanic,
                    "DuckLake maintenance query watchdog task panicked"
                ));
            }
        }

        blocking_result.map_err(|_| {
            etl_error!(
                ErrorKind::ApplyWorkerPanic,
                "DuckLake maintenance blocking operation task panicked"
            )
        })?
    }
}

async fn build_warm_ducklake_pool(
    manager: DuckLakeConnectionManager,
    pool_size: u32,
) -> EtlResult<r2d2::Pool<DuckLakeConnectionManager>> {
    tokio::task::spawn_blocking(move || -> EtlResult<_> {
        let pool = r2d2::Pool::builder()
            .max_size(pool_size)
            .min_idle(Some(0))
            .connection_timeout(Duration::from_secs(4 * 60))
            .test_on_check_out(true)
            .error_handler(Box::new(r2d2::NopErrorHandler))
            .build(manager)
            .map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to build DuckLake maintenance connection pool",
                    source: source
                )
            })?;

        let mut warmed_connections = Vec::with_capacity(pool_size as usize);
        for _ in 0..pool_size {
            warmed_connections.push(pool.get().map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to warm DuckLake maintenance connection pool",
                    source: source
                )
            })?);
        }
        drop(warmed_connections);

        trace!(pool_size, "ducklake maintenance connection pool warmed");
        Ok(pool)
    })
    .await
    .map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake maintenance connection pool initialization task panicked"
        )
    })?
}

fn format_query_error_detail(sql: &str) -> String {
    let compact_sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("sql: {compact_sql}")
}

fn attach_step_uses_postgres_catalog(step: &DuckLakeSetupStep) -> bool {
    step.label == "attach_catalog" && step.sql.contains("ducklake:postgres:")
}

/// Configuration for one external DuckLake maintenance run.
#[derive(Clone, Debug)]
pub struct DuckLakeMaintenanceConfig {
    /// DuckLake PostgreSQL catalog URL.
    pub catalog_url: Url,
    /// DuckLake data path.
    pub data_path: Url,
    /// DuckDB connection pool size for the one-shot runner.
    pub pool_size: u32,
    /// Optional S3-compatible storage config.
    pub s3: Option<S3Config>,
    /// Optional DuckLake metadata schema.
    pub metadata_schema: Option<String>,
    /// Optional DuckDB memory cache limit retained for config compatibility.
    pub duckdb_memory_cache_limit: Option<String>,
    /// DuckLake `target_file_size` used by compaction.
    pub maintenance_target_file_size: Option<String>,
    /// Inline flush operation config.
    pub inline_flush: InlineFlushMaintenanceConfig,
    /// Merge-adjacent-files operation config.
    pub merge_adjacent_files: MergeAdjacentFilesMaintenanceConfig,
    /// Rewrite-data-files operation config.
    pub rewrite_data_files: RewriteDataFilesMaintenanceConfig,
    /// Snapshot-expiration operation config.
    pub expire_snapshots: ExpireSnapshotsMaintenanceConfig,
    /// Old-file cleanup operation config.
    pub cleanup_old_files: CleanupOldFilesMaintenanceConfig,
}

/// Inline flush operation config.
#[derive(Clone, Copy, Debug)]
pub struct InlineFlushMaintenanceConfig {
    /// Whether inline flush is enabled.
    pub enabled: bool,
    /// Minimum pending inlined bytes before flushing a table.
    pub min_inlined_bytes: u64,
}

/// Merge-adjacent-files operation config.
#[derive(Clone, Debug)]
pub struct MergeAdjacentFilesMaintenanceConfig {
    /// Whether merge-adjacent-files is enabled.
    pub enabled: bool,
    /// Maximum compacted output files per table.
    pub max_compacted_files: u32,
    /// Maximum tables selected in one run.
    pub max_tables_per_run: u32,
    /// Target file size used during compaction.
    pub target_file_size: String,
}

/// Rewrite-data-files operation config.
#[derive(Clone, Copy, Debug)]
pub struct RewriteDataFilesMaintenanceConfig {
    /// Whether rewrite-data-files is enabled.
    pub enabled: bool,
    /// Minimum active data-file count before rewrite is attempted.
    pub min_active_data_files: i64,
    /// Maximum tables selected in one run.
    pub max_tables_per_run: u32,
}

/// Snapshot-expiration operation config.
#[derive(Clone, Debug)]
pub struct ExpireSnapshotsMaintenanceConfig {
    /// Whether snapshot expiration is enabled.
    pub enabled: bool,
    /// Retention window passed to DuckLake.
    pub older_than: String,
}

/// Old-file cleanup operation config.
#[derive(Clone, Debug)]
pub struct CleanupOldFilesMaintenanceConfig {
    /// Whether old-file cleanup is enabled.
    pub enabled: bool,
    /// Retention window passed to DuckLake.
    pub older_than: String,
}

/// Structured outcome for one external maintenance run.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DuckLakeMaintenanceOutcome {
    /// Tables whose inline data was flushed.
    pub inline_flush_tables: u32,
    /// Rows flushed from inlined storage.
    pub inline_flush_rows: u64,
    /// Tables passed to merge-adjacent-files.
    pub merge_adjacent_files_tables: u32,
    /// Files created by merge-adjacent-files.
    pub merge_adjacent_files_created: u64,
    /// Tables passed to rewrite-data-files.
    pub rewrite_data_files_tables: u32,
    /// Files created by rewrite-data-files.
    pub rewrite_data_files_created: u64,
    /// Snapshots expired by snapshot expiration.
    pub expired_snapshots: u64,
    /// Files removed by old-file cleanup.
    pub cleaned_up_files: u64,
}

impl DuckLakeMaintenanceOutcome {
    /// Returns whether any operation did work.
    pub fn applied(&self) -> bool {
        self.inline_flush_rows > 0
            || self.merge_adjacent_files_created > 0
            || self.rewrite_data_files_tables > 0
            || self.rewrite_data_files_created > 0
            || self.expired_snapshots > 0
            || self.cleaned_up_files > 0
    }
}

/// Runs one external DuckLake maintenance attempt.
pub async fn run_maintenance_once(
    config: DuckLakeMaintenanceConfig,
) -> EtlResult<DuckLakeMaintenanceOutcome> {
    validate_config(&config)?;
    let cleanup_old_files_enabled =
        config.cleanup_old_files.enabled || config.rewrite_data_files.enabled;

    info!(
        pool_size = config.pool_size,
        metadata_schema = config.metadata_schema.as_deref(),
        inline_flush_enabled = config.inline_flush.enabled,
        inline_flush_min_inlined_bytes = config.inline_flush.min_inlined_bytes,
        merge_adjacent_files_enabled = config.merge_adjacent_files.enabled,
        merge_adjacent_files_max_compacted_files = config.merge_adjacent_files.max_compacted_files,
        merge_adjacent_files_max_tables_per_run = config.merge_adjacent_files.max_tables_per_run,
        merge_adjacent_files_target_file_size = %config.merge_adjacent_files.target_file_size,
        rewrite_data_files_enabled = config.rewrite_data_files.enabled,
        rewrite_data_files_min_active_data_files = config.rewrite_data_files.min_active_data_files,
        rewrite_data_files_max_tables_per_run = config.rewrite_data_files.max_tables_per_run,
        expire_snapshots_enabled = config.expire_snapshots.enabled,
        expire_snapshots_older_than = %config.expire_snapshots.older_than,
        cleanup_old_files_enabled,
        cleanup_old_files_explicitly_enabled = config.cleanup_old_files.enabled,
        cleanup_old_files_older_than = %config.cleanup_old_files.older_than,
        "ducklake external maintenance runner configured"
    );

    let duckdb = open_maintenance_executor(&config).await?;
    let metadata_schema = match config.metadata_schema.clone() {
        Some(metadata_schema) => metadata_schema,
        None => resolve_metadata_schema(&duckdb).await?,
    };
    info!(
        metadata_schema = %metadata_schema,
        "ducklake external maintenance metadata schema resolved"
    );
    let metadata_pg_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(config.catalog_url.as_str())
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "DuckLake catalog metadata pool configuration failed",
                source: source
            )
        })?;
    let table_names = list_ducklake_tables(&metadata_pg_pool, &metadata_schema).await?;
    info!(
        table_count = table_names.len(),
        tables = ?table_names,
        "ducklake external maintenance discovered active tables"
    );
    let mut outcome = DuckLakeMaintenanceOutcome::default();

    if config.inline_flush.enabled {
        run_inline_flush(
            &duckdb,
            &metadata_pg_pool,
            &metadata_schema,
            &table_names,
            config.inline_flush,
            &mut outcome,
        )
        .await?;
    }

    if config.merge_adjacent_files.enabled {
        run_merge_adjacent_files(
            &duckdb,
            &metadata_pg_pool,
            &metadata_schema,
            &table_names,
            &config.merge_adjacent_files,
            &mut outcome,
        )
        .await?;
    }

    if config.rewrite_data_files.enabled {
        merge_adjacent_files_for_rewrite(&duckdb).await?;
        run_rewrite_data_files(
            &duckdb,
            &metadata_pg_pool,
            &metadata_schema,
            &table_names,
            config.rewrite_data_files,
            &mut outcome,
        )
        .await?;
    }

    if config.expire_snapshots.enabled {
        run_expire_snapshots(&duckdb, &config.expire_snapshots, &mut outcome).await?;
    }

    if cleanup_old_files_enabled {
        run_cleanup_old_files(&duckdb, &config.cleanup_old_files, &mut outcome).await?;
    }

    info!(outcome = ?outcome, applied = outcome.applied(), "ducklake external maintenance completed");
    Ok(outcome)
}

/// Validates one maintenance runner config.
fn validate_config(config: &DuckLakeMaintenanceConfig) -> EtlResult<()> {
    if !matches!(config.catalog_url.scheme(), "postgres" | "postgresql") {
        return Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake external maintenance requires a PostgreSQL catalog",
            format!("unsupported catalog URL scheme `{}`", config.catalog_url.scheme())
        ));
    }
    if config.pool_size == 0 {
        return Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake external maintenance pool size must be greater than zero"
        ));
    }
    Ok(())
}

/// Opens initialized DuckDB connections for maintenance.
async fn open_maintenance_executor(
    config: &DuckLakeMaintenanceConfig,
) -> EtlResult<DuckDbMaintenanceExecutor> {
    let extension_strategy = current_duckdb_extension_strategy()?;
    let target_file_size = config
        .maintenance_target_file_size
        .as_deref()
        .or(Some(config.merge_adjacent_files.target_file_size.as_str()))
        .unwrap_or(MAINTENANCE_TARGET_FILE_SIZE);
    info!(target_file_size, "opening ducklake external maintenance connection");
    let setup_plan = Arc::new(build_setup_plan(
        &config.catalog_url,
        &config.data_path,
        config.s3.as_ref(),
        config.metadata_schema.as_deref(),
    )?);
    let manager = DuckLakeConnectionManager {
        setup_plan,
        disable_extension_autoload: extension_strategy.disables_autoload(),
    };
    let pool = Arc::new(build_warm_ducklake_pool(manager, config.pool_size).await?);
    let blocking_slots = Arc::new(Semaphore::new(config.pool_size as usize));
    let executor = DuckDbMaintenanceExecutor { pool, blocking_slots };
    let sql = maintenance_target_file_size_sql(Some(target_file_size));
    executor
        .run(move |conn| {
            conn.execute_batch(&sql).map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake target_file_size configuration failed",
                    format_query_error_detail(&sql),
                    source: source
                )
            })?;
            Ok(())
        })
        .await?;
    info!(target_file_size, "ducklake external maintenance connection ready");
    Ok(executor)
}

/// Resolves the hidden DuckLake metadata schema.
async fn resolve_metadata_schema(duckdb: &DuckDbMaintenanceExecutor) -> EtlResult<String> {
    duckdb.run(resolve_ducklake_metadata_schema_blocking).await
}

/// Lists active DuckLake table names from the metadata catalog.
async fn list_ducklake_tables(
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
) -> EtlResult<Vec<String>> {
    let sql = format!(
        "SELECT table_name FROM {}.{} WHERE end_snapshot IS NULL ORDER BY table_name",
        quote_identifier(metadata_schema),
        quote_identifier("ducklake_table")
    );
    let rows: Vec<(String,)> =
        sqlx::query_as(AssertSqlSafe(sql)).fetch_all(metadata_pg_pool).await.map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake table list query failed",
                format!("metadata_schema={metadata_schema}"),
                source: source
            )
        })?;
    Ok(rows.into_iter().map(|(table_name,)| table_name).collect())
}

/// Runs inline flush for tables that crossed the pending-inline threshold.
async fn run_inline_flush(
    duckdb: &DuckDbMaintenanceExecutor,
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    config: InlineFlushMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        min_inlined_bytes = config.min_inlined_bytes,
        table_count = table_names.len(),
        "ducklake inline flush evaluation started"
    );
    let sampler =
        DuckLakePendingInlineSizeSampler::new(metadata_schema.to_owned(), metadata_pg_pool.clone());
    for table_name in table_names {
        let sizes = sampler.sample_table(table_name).await?;
        if sizes.inlined_bytes < config.min_inlined_bytes {
            info!(
                table = %table_name,
                inlined_bytes = sizes.inlined_bytes,
                min_inlined_bytes = config.min_inlined_bytes,
                "ducklake inline flush skipped below threshold"
            );
            continue;
        }
        info!(
            table = %table_name,
            inlined_bytes = sizes.inlined_bytes,
            min_inlined_bytes = config.min_inlined_bytes,
            "ducklake inline flush executing"
        );
        let table_name_for_query = table_name.clone();
        let rows =
            duckdb.run(move |conn| flush_table_inlined_data(conn, &table_name_for_query)).await?;
        outcome.inline_flush_tables = outcome.inline_flush_tables.saturating_add(1);
        outcome.inline_flush_rows = outcome.inline_flush_rows.saturating_add(rows);
        info!(
            table = %table_name,
            rows,
            "ducklake inline flush completed"
        );
    }
    info!(
        inline_flush_tables = outcome.inline_flush_tables,
        inline_flush_rows = outcome.inline_flush_rows,
        "ducklake inline flush evaluation finished"
    );
    Ok(())
}

/// Runs bounded merge-adjacent-files on selected tables.
async fn run_merge_adjacent_files(
    duckdb: &DuckDbMaintenanceExecutor,
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    config: &MergeAdjacentFilesMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        max_compacted_files = config.max_compacted_files,
        max_tables_per_run = config.max_tables_per_run,
        table_count = table_names.len(),
        "ducklake merge-adjacent-files evaluation started"
    );
    let selected = select_merge_tables(
        metadata_pg_pool,
        metadata_schema,
        table_names,
        config.max_tables_per_run,
    )
    .await?;
    info!(
        selected_tables = ?selected,
        selected_count = selected.len(),
        "ducklake merge-adjacent-files selected tables"
    );
    for table_name in selected {
        info!(
            table = %table_name,
            max_compacted_files = config.max_compacted_files,
            "ducklake merge-adjacent-files executing"
        );
        let table_name_for_query = table_name.clone();
        let max_compacted_files = config.max_compacted_files;
        let files_created = duckdb
            .run(move |conn| merge_adjacent_files(conn, &table_name_for_query, max_compacted_files))
            .await?;
        outcome.merge_adjacent_files_tables = outcome.merge_adjacent_files_tables.saturating_add(1);
        outcome.merge_adjacent_files_created =
            outcome.merge_adjacent_files_created.saturating_add(files_created);
        info!(
            table = %table_name,
            files_created,
            "ducklake merge-adjacent-files completed"
        );
    }
    Ok(())
}

/// Runs DuckLake's whole-lake adjacent-file merge before rewrite-data-files.
async fn merge_adjacent_files_for_rewrite(duckdb: &DuckDbMaintenanceExecutor) -> EtlResult<()> {
    let sql = format!("CALL ducklake_merge_adjacent_files({});", quote_literal(LAKE_CATALOG));
    info!(
        sql = %sql,
        "ducklake rewrite-triggered merge-adjacent-files executing"
    );
    duckdb
        .run(move |conn| {
            conn.execute_batch(&sql).map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake rewrite-triggered merge adjacent files failed",
                    format_query_error_detail(&sql),
                    source: source
                )
            })?;
            Ok(())
        })
        .await?;
    info!("ducklake rewrite-triggered merge-adjacent-files completed");
    Ok(())
}

/// Runs bounded rewrite-data-files on selected tables.
async fn run_rewrite_data_files(
    duckdb: &DuckDbMaintenanceExecutor,
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    config: RewriteDataFilesMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        min_active_data_files = config.min_active_data_files,
        max_tables_per_run = config.max_tables_per_run,
        table_count = table_names.len(),
        "ducklake rewrite-data-files evaluation started"
    );
    let selected = select_rewrite_tables(
        metadata_pg_pool,
        metadata_schema,
        table_names,
        config.min_active_data_files,
        config.max_tables_per_run,
    )
    .await?;
    info!(
        selected_tables = ?selected,
        selected_count = selected.len(),
        "ducklake rewrite-data-files selected tables"
    );
    for table_name in selected {
        info!(
            table = %table_name,
            "ducklake rewrite-data-files executing"
        );
        let table_name_for_query = table_name.clone();
        let files_created =
            duckdb.run(move |conn| rewrite_data_files(conn, &table_name_for_query)).await?;
        outcome.rewrite_data_files_tables = outcome.rewrite_data_files_tables.saturating_add(1);
        outcome.rewrite_data_files_created =
            outcome.rewrite_data_files_created.saturating_add(files_created);
        info!(
            table = %table_name,
            files_created,
            "ducklake rewrite-data-files completed"
        );
    }
    Ok(())
}

/// Runs DuckLake snapshot expiration.
async fn run_expire_snapshots(
    duckdb: &DuckDbMaintenanceExecutor,
    config: &ExpireSnapshotsMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        older_than = %config.older_than,
        "ducklake expire-snapshots executing"
    );
    let older_than = config.older_than.clone();
    let expired_snapshots = duckdb.run(move |conn| expire_snapshots(conn, &older_than)).await?;
    outcome.expired_snapshots = outcome.expired_snapshots.saturating_add(expired_snapshots);
    info!(
        older_than = %config.older_than,
        expired_snapshots,
        "ducklake expire-snapshots completed"
    );
    Ok(())
}

/// Runs DuckLake old-file cleanup.
async fn run_cleanup_old_files(
    duckdb: &DuckDbMaintenanceExecutor,
    config: &CleanupOldFilesMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        older_than = %config.older_than,
        "ducklake cleanup-old-files executing"
    );
    let older_than = config.older_than.clone();
    let cleaned_up_files = duckdb.run(move |conn| cleanup_old_files(conn, &older_than)).await?;
    outcome.cleaned_up_files = outcome.cleaned_up_files.saturating_add(cleaned_up_files);
    info!(
        older_than = %config.older_than,
        cleaned_up_files,
        "ducklake cleanup-old-files completed"
    );
    Ok(())
}

/// Selects tables with small-file pressure for merge-adjacent-files.
async fn select_merge_tables(
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    max_tables_per_run: u32,
) -> EtlResult<Vec<String>> {
    let mut selected = Vec::new();
    for table_name in table_names {
        if is_etl_internal_table(table_name) {
            info!(
                table = %table_name,
                "ducklake rewrite-data-files table skipped because it is internal ETL metadata"
            );
            continue;
        }

        let metrics =
            query_table_storage_metrics(metadata_pg_pool, metadata_schema, table_name).await?;
        if metrics.active_data_files > 1 && metrics.small_file_ratio() > 0.0 {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                small_file_ratio = metrics.small_file_ratio(),
                "ducklake merge-adjacent-files table selected"
            );
            selected.push(table_name.clone());
        } else {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                small_file_ratio = metrics.small_file_ratio(),
                "ducklake merge-adjacent-files table skipped"
            );
        }
        if selected.len() >= max_tables_per_run as usize {
            break;
        }
    }
    Ok(selected)
}

/// Selects tables with delete pressure for rewrite-data-files.
async fn select_rewrite_tables(
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    min_active_data_files: i64,
    max_tables_per_run: u32,
) -> EtlResult<Vec<String>> {
    let mut selected = Vec::new();
    for table_name in table_names {
        if is_etl_internal_table(table_name) {
            info!(
                table = %table_name,
                "ducklake rewrite-data-files table skipped because it is internal ETL metadata"
            );
            continue;
        }

        let metrics =
            query_table_storage_metrics(metadata_pg_pool, metadata_schema, table_name).await?;
        if should_rewrite(&metrics, min_active_data_files) {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                active_delete_files = metrics.active_delete_files,
                deleted_row_ratio = metrics.deleted_row_ratio(),
                min_active_data_files,
                "ducklake rewrite-data-files table selected"
            );
            selected.push(table_name.clone());
        } else {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                active_delete_files = metrics.active_delete_files,
                deleted_row_ratio = metrics.deleted_row_ratio(),
                min_active_data_files,
                "ducklake rewrite-data-files table skipped"
            );
        }
        if selected.len() >= max_tables_per_run as usize {
            break;
        }
    }
    Ok(selected)
}

fn is_etl_internal_table(table_name: &str) -> bool {
    table_name.starts_with("__etl_")
}

/// Returns whether a table should be rewritten.
fn should_rewrite(metrics: &DuckLakeTableStorageMetrics, min_active_data_files: i64) -> bool {
    metrics.active_data_files > min_active_data_files
}

/// Flushes inlined user data for one table.
#[doc(hidden)]
pub fn flush_table_inlined_data(conn: &duckdb::Connection, table_name: &str) -> EtlResult<u64> {
    let flush_started = std::time::Instant::now();
    let sql = format!(
        r#"SELECT COALESCE(SUM(rows_flushed), 0)
         FROM ducklake_flush_inlined_data({}, table_name => {});"#,
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
    );
    let rows_flushed: i64 = conn.query_row(&sql, [], |row| row.get(0)).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake inlined data flush failed",
            format_query_error_detail(&sql),
            source: source
        )
    })?;
    let rows_flushed = rows_flushed.max(0) as u64;
    let flush_result = if rows_flushed > 0 { "flushed" } else { "noop" };
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_ROWS,
        RESULT_LABEL => flush_result,
    )
    .record(rows_flushed as f64);
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
        RESULT_LABEL => flush_result,
    )
    .record(flush_started.elapsed().as_secs_f64());

    if rows_flushed > 0 {
        debug!(
            table = %table_name,
            rows_flushed,
            "ducklake inlined data flushed"
        );
    } else {
        debug!(
            table = %table_name,
            "ducklake inlined data already flushed"
        );
    }
    Ok(rows_flushed)
}

/// Calls DuckLake merge-adjacent-files for one table.
fn merge_adjacent_files(
    conn: &duckdb::Connection,
    table_name: &str,
    max_compacted_files: u32,
) -> EtlResult<u64> {
    let sql = format!(
        "SELECT COALESCE(SUM(files_created), 0) FROM ducklake_merge_adjacent_files({}, {}, \
         max_compacted_files => {});",
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
        max_compacted_files
    );
    count_maintenance_files(conn, &sql, "DuckLake merge adjacent files failed")
}

/// Calls DuckLake rewrite-data-files for one table.
fn rewrite_data_files(conn: &duckdb::Connection, table_name: &str) -> EtlResult<u64> {
    let sql = format!(
        "CALL ducklake_rewrite_data_files({}, {});",
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name)
    );
    conn.execute_batch(&sql).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake rewrite data files failed",
            format_query_error_detail(&sql),
            source: source
        )
    })?;
    Ok(0)
}

/// Calls DuckLake snapshot expiration.
fn expire_snapshots(conn: &duckdb::Connection, older_than: &str) -> EtlResult<u64> {
    let sql = format!(
        "CALL ducklake_expire_snapshots({}, older_than => CAST(now() AS TIMESTAMP) - CAST({} AS \
         INTERVAL));",
        quote_literal(LAKE_CATALOG),
        quote_literal(older_than),
    );
    count_maintenance_rows(conn, &sql, "DuckLake expire snapshots failed")
}

/// Calls DuckLake old-file cleanup.
fn cleanup_old_files(conn: &duckdb::Connection, older_than: &str) -> EtlResult<u64> {
    let sql = format!(
        "CALL ducklake_cleanup_old_files({}, older_than => CAST(now() AS TIMESTAMP) - CAST({} AS \
         INTERVAL));",
        quote_literal(LAKE_CATALOG),
        quote_literal(older_than),
    );
    count_maintenance_rows(conn, &sql, "DuckLake cleanup old files failed")
}

/// Counts rows returned by one DuckLake maintenance call.
fn count_maintenance_rows(
    conn: &duckdb::Connection,
    sql: &str,
    description: &'static str,
) -> EtlResult<u64> {
    let mut statement = conn.prepare(sql).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql),
            source: source
        )
    })?;
    let mut rows = statement.query([]).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql),
            source: source
        )
    })?;
    let mut count = 0u64;

    while let Some(_row) = rows.next().map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql),
            source: source
        )
    })? {
        count = count.saturating_add(1);
    }

    Ok(count)
}

/// Counts files returned by one DuckLake maintenance function.
fn count_maintenance_files(
    conn: &duckdb::Connection,
    sql: &str,
    description: &'static str,
) -> EtlResult<u64> {
    let files_created: i64 = conn.query_row(sql, [], |row| row.get(0)).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql),
            source: source
        )
    })?;
    Ok(files_created.max(0) as u64)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DuckLakePendingInlineDataSizes {
    inlined_bytes: u64,
}

#[derive(Clone)]
struct DuckLakePendingInlineSizeSampler {
    metadata_schema: String,
    pool: PgPool,
}

impl DuckLakePendingInlineSizeSampler {
    fn new(metadata_schema: String, pool: PgPool) -> Self {
        Self { metadata_schema, pool }
    }

    async fn sample_table(&self, table_name: &str) -> EtlResult<DuckLakePendingInlineDataSizes> {
        let sql = pending_inline_data_bytes_query(&self.metadata_schema);
        let inlined_bytes: i64 = sqlx::query_scalar(AssertSqlSafe(sql))
            .bind(table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake inline-size sampler query failed",
                    source: source
                )
            })?;
        Ok(record_pending_inline_data_sizes(inlined_bytes, table_name))
    }
}

fn record_pending_inline_data_sizes(
    inlined_bytes: i64,
    table_name: &str,
) -> DuckLakePendingInlineDataSizes {
    let inlined_bytes = inlined_bytes.max(0) as u64;
    gauge!(ETL_DUCKLAKE_TABLE_ACTIVE_INLINED_DATA_BYTES, TABLE_LABEL => table_name.to_owned())
        .set(inlined_bytes as f64);
    DuckLakePendingInlineDataSizes { inlined_bytes }
}

fn pending_inline_data_bytes_query(metadata_schema: &str) -> String {
    let metadata_schema_literal = quote_literal(metadata_schema);
    let metadata_schema = quote_identifier(metadata_schema);
    let ducklake_table = quote_identifier("ducklake_table");
    let ducklake_inlined_data_tables = quote_identifier("ducklake_inlined_data_tables");

    format!(
        r"WITH target_table AS (
             SELECT table_id
             FROM {metadata_schema}.{ducklake_table}
             WHERE end_snapshot IS NULL AND table_name = $1
             LIMIT 1
         ),
         target_inline_tables AS (
             SELECT DISTINCT table_name
             FROM {metadata_schema}.{ducklake_inlined_data_tables}
             WHERE table_id = (SELECT table_id FROM target_table)
         ),
         inline_data_bytes AS (
             SELECT COALESCE(
                 SUM(
                     pg_total_relation_size(
                         to_regclass(
                             format('%I.%I', {metadata_schema_literal}, table_name)
                         )
                     )
                 ),
                 0
             )::BIGINT AS total_bytes
             FROM target_inline_tables
         ),
         inline_delete_bytes AS (
             SELECT COALESCE(
                 pg_total_relation_size(
                     to_regclass(
                         format(
                             '%I.%I',
                             {metadata_schema_literal},
                             format('ducklake_inlined_delete_%s', (SELECT table_id FROM target_table))
                         )
                     )
                 ),
                 0
             )::BIGINT AS total_bytes
         )
         SELECT
             (SELECT total_bytes FROM inline_data_bytes)
             + (SELECT total_bytes FROM inline_delete_bytes);"
    )
}

#[derive(Clone, Debug)]
struct DuckLakeTableStorageMetrics {
    active_data_files: i64,
    small_data_files: i64,
    active_data_rows: i64,
    active_delete_files: i64,
    deleted_rows: i64,
}

impl DuckLakeTableStorageMetrics {
    fn small_file_ratio(&self) -> f64 {
        if self.active_data_files > 0 {
            self.small_data_files.max(0) as f64 / self.active_data_files as f64
        } else {
            0.0
        }
    }

    fn deleted_row_ratio(&self) -> f64 {
        if self.active_data_rows > 0 {
            self.deleted_rows.max(0) as f64 / self.active_data_rows as f64
        } else {
            0.0
        }
    }
}

async fn query_table_storage_metrics(
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_name: &str,
) -> EtlResult<DuckLakeTableStorageMetrics> {
    let sql = table_storage_metrics_query(metadata_schema);
    let (
        active_data_files,
        _active_data_bytes,
        small_data_files,
        active_data_rows,
        active_delete_files,
        _active_delete_bytes,
        deleted_rows,
    ): (i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(AssertSqlSafe(sql))
        .bind(table_name)
        .fetch_one(metadata_pg_pool)
        .await
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake table storage metrics query failed",
                format!("table={table_name}, metadata_schema={metadata_schema}"),
                source: source
            )
        })?;

    Ok(DuckLakeTableStorageMetrics {
        active_data_files,
        small_data_files,
        active_data_rows,
        active_delete_files,
        deleted_rows,
    })
}

fn table_storage_metrics_query(metadata_schema: &str) -> String {
    let metadata_schema = quote_identifier(metadata_schema);
    let ducklake_table = quote_identifier("ducklake_table");
    let ducklake_data_file = quote_identifier("ducklake_data_file");
    let ducklake_delete_file = quote_identifier("ducklake_delete_file");

    format!(
        r#"WITH target_table AS (
             SELECT table_id
             FROM {metadata_schema}.{ducklake_table}
             WHERE end_snapshot IS NULL AND table_name = $1
             LIMIT 1
         ),
         data_stats AS (
             SELECT
                 COUNT(*)::BIGINT AS active_data_files,
                 COALESCE(SUM(file_size_bytes), 0)::BIGINT AS active_data_bytes,
                 COALESCE(SUM(CASE WHEN file_size_bytes < {SMALL_FILE_SIZE_BYTES} THEN 1 ELSE 0 END), 0)::BIGINT AS small_data_files,
                 COALESCE(SUM(record_count), 0)::BIGINT AS active_data_rows
             FROM {metadata_schema}.{ducklake_data_file}
             WHERE end_snapshot IS NULL AND table_id = (SELECT table_id FROM target_table)
         ),
         delete_stats AS (
             SELECT
                 COUNT(*)::BIGINT AS active_delete_files,
                 COALESCE(SUM(file_size_bytes), 0)::BIGINT AS active_delete_bytes,
                 COALESCE(SUM(delete_count), 0)::BIGINT AS deleted_rows
             FROM {metadata_schema}.{ducklake_delete_file}
             WHERE end_snapshot IS NULL AND table_id = (SELECT table_id FROM target_table)
         )
         SELECT
             active_data_files,
             active_data_bytes,
             small_data_files,
             active_data_rows,
             active_delete_files,
             active_delete_bytes,
             deleted_rows
         FROM data_stats CROSS JOIN delete_stats;"#
    )
}

fn resolve_ducklake_metadata_schema_blocking(conn: &duckdb::Connection) -> EtlResult<String> {
    let metadata_catalog = format!("__ducklake_metadata_{LAKE_CATALOG}");
    let sql = format!(
        r#"SELECT table_schema
           FROM information_schema.tables
           WHERE table_catalog = {}
             AND table_name = 'ducklake_snapshot'
           ORDER BY CASE
               WHEN table_schema = 'main' THEN 0
               WHEN table_schema = 'ducklake' THEN 1
               ELSE 2
           END,
           table_schema
           LIMIT 1;"#,
        quote_literal(&metadata_catalog),
    );
    conn.query_row(&sql, [], |row| row.get::<_, String>(0)).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake metadata schema query failed",
            format_query_error_detail(&sql),
            source: source
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Semaphore;

    use super::*;

    fn metrics(
        active_data_files: i64,
        active_delete_files: i64,
        deleted_rows: i64,
    ) -> DuckLakeTableStorageMetrics {
        DuckLakeTableStorageMetrics {
            active_data_files,
            small_data_files: 0,
            active_data_rows: 100,
            active_delete_files,
            deleted_rows,
        }
    }

    fn make_maintenance_test_executor() -> DuckDbMaintenanceExecutor {
        let manager = DuckLakeConnectionManager {
            setup_plan: Arc::new(DuckLakeSetupPlan::default()),
            disable_extension_autoload: cfg!(target_os = "linux"),
        };
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .min_idle(Some(0))
            .connection_timeout(Duration::from_secs(1))
            .test_on_check_out(true)
            .error_handler(Box::new(r2d2::NopErrorHandler))
            .build(manager)
            .expect("failed to build maintenance test pool");

        DuckDbMaintenanceExecutor {
            pool: Arc::new(pool),
            blocking_slots: Arc::new(Semaphore::new(1)),
        }
    }

    #[test]
    fn should_rewrite_requires_only_file_count() {
        assert!(!should_rewrite(&metrics(39, 1, 50), 40));
        assert!(!should_rewrite(&metrics(40, 1, 50), 40));
        assert!(should_rewrite(&metrics(41, 0, 0), 40));
    }

    #[test]
    fn outcome_reports_applied_work() {
        assert!(!DuckLakeMaintenanceOutcome::default().applied());
        assert!(
            DuckLakeMaintenanceOutcome {
                inline_flush_rows: 1,
                ..DuckLakeMaintenanceOutcome::default()
            }
            .applied()
        );
        assert!(
            DuckLakeMaintenanceOutcome {
                rewrite_data_files_tables: 1,
                ..DuckLakeMaintenanceOutcome::default()
            }
            .applied()
        );
        assert!(
            DuckLakeMaintenanceOutcome {
                expired_snapshots: 1,
                ..DuckLakeMaintenanceOutcome::default()
            }
            .applied()
        );
        assert!(
            DuckLakeMaintenanceOutcome {
                cleaned_up_files: 1,
                ..DuckLakeMaintenanceOutcome::default()
            }
            .applied()
        );
    }

    #[tokio::test]
    async fn maintenance_executor_timeout_releases_resources_for_follow_up_queries() {
        let executor = make_maintenance_test_executor();

        let error = executor
            .run_with_timeout(Duration::from_millis(50), |_conn| -> EtlResult<()> {
                std::thread::sleep(Duration::from_millis(100));
                Ok(())
            })
            .await
            .expect_err("expected maintenance operation timeout");

        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(error.description(), Some("DuckLake maintenance blocking operation timed out"));
        assert!(
            error.detail().is_some_and(|detail| {
                detail.contains("stage=pool_checkout") || detail.contains("stage=query_execution")
            }),
            "unexpected error detail: {error:?}"
        );

        let value = executor
            .run_with_timeout(Duration::from_secs(1), |conn| -> EtlResult<i64> {
                conn.query_row("SELECT 1;", [], |row| row.get::<_, i64>(0)).map_err(|source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake maintenance timeout verification query failed",
                        source: source
                    )
                })
            })
            .await
            .expect("expected follow-up query to succeed");

        assert_eq!(value, 1);
    }
}
