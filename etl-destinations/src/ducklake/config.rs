use std::collections::BTreeMap;
use std::str::FromStr;

use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use pg_escape::{quote_identifier, quote_literal};
use tokio_postgres::Config as PgConfig;
use tokio_postgres::config::{
    ChannelBinding, Host, LoadBalanceHosts, ReplicationMode, SslMode, TargetSessionAttrs,
};
use url::Url;

use crate::ducklake::core::LAKE_CATALOG;

/// S3-compatible storage credentials for DuckDB's httpfs extension.
///
/// Used when `data_path` points to an S3, GCS, or Azure URI. The `endpoint`
/// field supports an optional path prefix (e.g. `localhost:5000/s3` for
/// Supabase Storage or other S3-compatible services mounted at a sub-path).
#[derive(Debug, Clone)]
pub struct S3Config {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// AWS region or equivalent (e.g. `"us-east-1"`).
    pub region: String,
    /// Host, host:port, or host:port/path of the S3-compatible endpoint.
    /// Defaults to AWS S3 if not set.
    pub endpoint: Option<String>,
    /// `"path"` (default for MinIO / Supabase Storage) or `"vhost"` (AWS S3 default).
    pub url_style: String,
    /// Whether to use HTTPS. Set to `false` for local S3-compatible services.
    pub use_ssl: bool,
}

/// Serializes a PostgreSQL catalog [`Url`] to the libpq key=value format that
/// DuckLake expects for `postgres:` catalog attachments.
pub(super) fn catalog_conninfo_from_url(catalog_url: &Url) -> EtlResult<String> {
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

    let config = PgConfig::from_str(catalog_url.as_str()).map_err(|e| {
        etl_error!(
            ErrorKind::ConfigError,
            "Invalid DuckLake PostgreSQL catalog URL",
            source: e
        )
    })?;

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
        let password = std::str::from_utf8(password).map_err(|e| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 password",
                source: e
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
        let ssl_cert = std::str::from_utf8(ssl_cert).map_err(|e| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 sslcert",
                source: e
            )
        })?;
        push_conninfo_pair(&mut parts, "sslcert", ssl_cert);
    }
    if let Some(ssl_key) = config.get_ssl_key() {
        let ssl_key = std::str::from_utf8(ssl_key).map_err(|e| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 sslkey",
                source: e
            )
        })?;
        push_conninfo_pair(&mut parts, "sslkey", ssl_key);
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        let ssl_root_cert = std::str::from_utf8(ssl_root_cert).map_err(|e| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake PostgreSQL catalog URL contains non-utf8 sslrootcert",
                source: e
            )
        })?;
        push_conninfo_pair(&mut parts, "sslrootcert", ssl_root_cert);
    }

    parts.push(format!(
        "sslmode={}",
        ssl_mode_to_str(config.get_ssl_mode())?
    ));

    if let Some(connect_timeout) = config.get_connect_timeout() {
        parts.push(format!("connect_timeout={}", connect_timeout.as_secs()));
    }
    if let Some(tcp_user_timeout) = config.get_tcp_user_timeout() {
        parts.push(format!("tcp_user_timeout={}", tcp_user_timeout.as_secs()));
    }

    parts.push(format!(
        "keepalives={}",
        if config.get_keepalives() { "1" } else { "0" }
    ));
    parts.push(format!(
        "keepalives_idle={}",
        config.get_keepalives_idle().as_secs()
    ));
    if let Some(keepalives_interval) = config.get_keepalives_interval() {
        parts.push(format!(
            "keepalives_interval={}",
            keepalives_interval.as_secs()
        ));
    }
    if let Some(keepalives_retries) = config.get_keepalives_retries() {
        parts.push(format!("keepalives_retries={keepalives_retries}"));
    }

    parts.push(format!(
        "target_session_attrs={}",
        target_session_attrs_to_str(config.get_target_session_attrs())?
    ));
    parts.push(format!(
        "channel_binding={}",
        channel_binding_to_str(config.get_channel_binding())?
    ));
    parts.push(format!(
        "load_balance_hosts={}",
        load_balance_hosts_to_str(config.get_load_balance_hosts())?
    ));

    if let Some(replication_mode) = config.get_replication_mode() {
        parts.push(format!(
            "replication={}",
            replication_mode_to_str(replication_mode)?
        ));
    }

    Ok(format!("postgres:{}", parts.join(" ")))
}

/// Converts a supported DuckLake catalog URL into the target string used by
/// DuckDB's `ATTACH 'ducklake:...'`.
pub(super) fn catalog_attach_target(catalog_url: &Url) -> EtlResult<String> {
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

/// Serializes hosts from [`PgConfig`] into a libpq-compatible host string.
pub(super) fn serialize_hosts(hosts: &[Host]) -> EtlResult<String> {
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

/// Adds a `key='value'` pair to a libpq conninfo string.
pub(super) fn push_conninfo_pair(parts: &mut Vec<String>, key: &str, value: &str) {
    parts.push(format!("{key}={}", quote_libpq_conninfo_value(value)));
}

/// Quotes a libpq conninfo value.
///
/// This is not SQL quoting. DuckLake expects the PostgreSQL catalog connection
/// as libpq `key=value` conninfo.
pub(super) fn quote_libpq_conninfo_value(value: &str) -> String {
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

pub(super) fn ssl_mode_to_str(ssl_mode: SslMode) -> EtlResult<&'static str> {
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

pub(super) fn target_session_attrs_to_str(
    target_session_attrs: TargetSessionAttrs,
) -> EtlResult<&'static str> {
    match target_session_attrs {
        TargetSessionAttrs::Any => Ok("any"),
        TargetSessionAttrs::ReadWrite => Ok("read-write"),
        TargetSessionAttrs::ReadOnly => Ok("read-only"),
        _ => Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake PostgreSQL catalog URL uses unsupported target_session_attrs"
        )),
    }
}

pub(super) fn channel_binding_to_str(channel_binding: ChannelBinding) -> EtlResult<&'static str> {
    match channel_binding {
        ChannelBinding::Disable => Ok("disable"),
        ChannelBinding::Prefer => Ok("prefer"),
        ChannelBinding::Require => Ok("require"),
        _ => Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake PostgreSQL catalog URL uses unsupported channel_binding"
        )),
    }
}

pub(super) fn load_balance_hosts_to_str(
    load_balance_hosts: LoadBalanceHosts,
) -> EtlResult<&'static str> {
    match load_balance_hosts {
        LoadBalanceHosts::Disable => Ok("disable"),
        LoadBalanceHosts::Random => Ok("random"),
        _ => Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake PostgreSQL catalog URL uses unsupported load_balance_hosts"
        )),
    }
}

pub(super) fn replication_mode_to_str(
    replication_mode: ReplicationMode,
) -> EtlResult<&'static str> {
    match replication_mode {
        ReplicationMode::Physical => Ok("true"),
        ReplicationMode::Logical => Ok("database"),
        _ => Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake PostgreSQL catalog URL uses unsupported replication mode"
        )),
    }
}

/// Returns a validated DuckLake data path string.
pub(super) fn validate_data_path(data_path: &Url) -> EtlResult<&str> {
    match data_path.scheme() {
        "file" | "s3" | "gs" | "az" => Ok(data_path.as_str()),
        scheme => Err(etl_error!(
            ErrorKind::ConfigError,
            "Unsupported DuckLake data URL scheme",
            format!("data URL scheme `{scheme}` is not supported")
        )),
    }
}

/// Builds the one-time setup SQL executed for each new pool connection.
///
/// - Always installs and loads the `ducklake` extension.
/// - Installs the `postgres` extension when the catalog uses a PostgreSQL URL.
/// - Installs `httpfs` and configures S3 credentials when the data path uses
///   a cloud URL (`s3://`, `gs://`, `az://`).
pub(super) fn build_setup_sql(
    catalog_url: &Url,
    data_path: &Url,
    s3: Option<&S3Config>,
    metadata_schema: Option<&str>,
) -> EtlResult<String> {
    let catalog_target = catalog_attach_target(catalog_url)?;
    let data_path = validate_data_path(data_path)?;

    let needs_postgres = matches!(catalog_url.scheme(), "postgres" | "postgresql");
    let needs_httpfs = matches!(data_path.split(':').next(), Some("s3" | "gs" | "az"));
    let lake_catalog = quote_identifier(LAKE_CATALOG);
    let mut secret_options = BTreeMap::from([
        (
            "KEY_ID",
            quote_literal(s3.map(|s| s.access_key_id.as_str()).unwrap_or_default()),
        ),
        (
            "REGION",
            quote_literal(s3.map(|s| s.region.as_str()).unwrap_or_default()),
        ),
        (
            "SECRET",
            quote_literal(s3.map(|s| s.secret_access_key.as_str()).unwrap_or_default()),
        ),
        (
            "URL_STYLE",
            quote_literal(s3.map(|s| s.url_style.as_str()).unwrap_or_default()),
        ),
    ]);

    let mut sql = String::from("INSTALL ducklake; LOAD ducklake;");
    if needs_postgres {
        sql.push_str(" INSTALL postgres; LOAD postgres;");
    }
    if needs_httpfs {
        sql.push_str(" INSTALL httpfs; LOAD httpfs;");
        if let Some(s3) = s3 {
            let secret_name = quote_identifier("ducklake_s3");
            if let Some(endpoint) = &s3.endpoint {
                secret_options.insert("ENDPOINT", quote_literal(endpoint));
            }

            let secret_body = secret_options
                .iter()
                .map(|(key, value)| format!("{key} {value}"))
                .collect::<Vec<_>>()
                .join(", ");

            sql.push_str(&format!(
                " CREATE OR REPLACE SECRET {secret_name} (TYPE S3, {secret_body}, USE_SSL {});",
                if s3.use_ssl { "true" } else { "false" }
            ));
        }
    }
    let metadata_schema_clause = metadata_schema
        .map(|schema| format!(", METADATA_SCHEMA {}", quote_literal(schema)))
        .unwrap_or_default();

    sql.push_str(&format!(
        " ATTACH {} AS {lake_catalog} (DATA_PATH {}{metadata_schema_clause});",
        quote_literal(&format!("ducklake:{catalog_target}")),
        quote_literal(data_path)
    ));
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_conninfo_from_file_url() {
        let catalog_url = Url::from_file_path("/tmp/catalog.ducklake").unwrap();
        assert_eq!(
            catalog_attach_target(&catalog_url).unwrap(),
            catalog_url.as_str()
        );
    }

    #[test]
    fn test_quote_libpq_conninfo_value() {
        assert_eq!(
            quote_libpq_conninfo_value("pa'ss\\word"),
            "'pa\\'ss\\\\word'"
        );
        assert_eq!(
            quote_libpq_conninfo_value("value with spaces"),
            "'value with spaces'"
        );
    }

    #[test]
    fn test_catalog_conninfo_from_postgres_url_round_trip() {
        let url = Url::parse("postgres://bnj@localhost:5432/ducklake_catalog").unwrap();
        let conninfo = catalog_conninfo_from_url(&url).unwrap();
        let parsed = PgConfig::from_str(conninfo.strip_prefix("postgres:").unwrap()).unwrap();

        assert_eq!(parsed.get_user(), Some("bnj"));
        assert_eq!(parsed.get_dbname(), Some("ducklake_catalog"));
        assert_eq!(parsed.get_ports(), &[5432]);
        assert_eq!(serialize_hosts(parsed.get_hosts()).unwrap(), "localhost");
    }

    #[test]
    fn test_catalog_conninfo_from_postgres_url_with_password_and_query_params_round_trip() {
        let url = Url::parse(
            "postgres://user:pa%27ss%5Cword@localhost:5433/mydb?sslmode=disable&connect_timeout=10&application_name=myapp",
        )
        .unwrap();
        let conninfo = catalog_conninfo_from_url(&url).unwrap();
        let parsed = PgConfig::from_str(conninfo.strip_prefix("postgres:").unwrap()).unwrap();

        assert_eq!(parsed.get_user(), Some("user"));
        assert_eq!(parsed.get_dbname(), Some("mydb"));
        assert_eq!(parsed.get_ports(), &[5433]);
        assert_eq!(
            std::str::from_utf8(parsed.get_password().unwrap()).unwrap(),
            "pa'ss\\word"
        );
        assert_eq!(parsed.get_ssl_mode(), SslMode::Disable);
        assert_eq!(parsed.get_connect_timeout().unwrap().as_secs(), 10);
        assert_eq!(parsed.get_application_name(), Some("myapp"));
    }

    #[test]
    fn test_catalog_conninfo_rejects_unsupported_catalog_scheme() {
        let err =
            catalog_attach_target(&Url::parse("https://example.com/catalog").unwrap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigError);
    }

    #[test]
    fn test_validate_data_path_rejects_unsupported_scheme() {
        let err = build_setup_sql(
            &Url::from_file_path("/tmp/catalog.ducklake").unwrap(),
            &Url::parse("https://example.com/lake").unwrap(),
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigError);
    }

    #[test]
    fn test_build_setup_sql_local() {
        let catalog_url = Url::from_file_path("/tmp/catalog.ducklake").unwrap();
        let data_url = Url::from_file_path("/tmp/lake_data").unwrap();
        let sql = build_setup_sql(&catalog_url, &data_url, None, None).unwrap();

        assert!(sql.contains("INSTALL ducklake"));
        assert!(sql.contains("LOAD ducklake"));
        assert!(!sql.contains("postgres"));
        assert!(!sql.contains("httpfs"));
        assert!(sql.contains(&quote_literal(&format!(
            "ducklake:{}",
            catalog_url.as_str()
        ))));
        assert!(sql.contains(&format!("DATA_PATH {}", quote_literal(data_url.as_str()))));
    }

    #[test]
    fn test_build_setup_sql_postgres_local_data() {
        let catalog_url = Url::parse("postgres://bnj@localhost:5432/ducklake_catalog").unwrap();
        let data_url = Url::from_file_path("/tmp/lake_data").unwrap();
        let sql = build_setup_sql(&catalog_url, &data_url, None, None).unwrap();

        assert!(sql.contains("INSTALL postgres"));
        assert!(sql.contains("LOAD postgres"));
        assert!(!sql.contains("httpfs"));
        assert!(!sql.contains("ducklake:postgres://"));
        assert!(sql.contains("ducklake:postgres:"));
        assert!(sql.contains(&format!("DATA_PATH {}", quote_literal(data_url.as_str()))));
    }

    #[test]
    fn test_build_setup_sql_postgres_s3_data() {
        let catalog_url = Url::parse("postgres://user:pass@host/db").unwrap();
        let data_url = Url::parse("s3://my-bucket/lake/").unwrap();
        let sql = build_setup_sql(&catalog_url, &data_url, None, None).unwrap();

        assert!(sql.contains("INSTALL postgres"));
        assert!(sql.contains("INSTALL httpfs"));
        assert!(sql.contains("LOAD httpfs"));
        assert!(!sql.contains("ducklake:postgres://"));
        assert!(sql.contains(&format!("DATA_PATH {}", quote_literal(data_url.as_str()))));
    }

    #[test]
    fn test_build_setup_sql_uses_pg_escape_for_literals() {
        let catalog_url = Url::parse("postgres://user:pa%27ss%5Cword@localhost:5432/mydb").unwrap();
        let data_url = Url::parse("s3://bucket/path").unwrap();
        let s3 = S3Config {
            access_key_id: "key'with".to_owned(),
            secret_access_key: "sec\\ret".to_owned(),
            region: "eu-west-1".to_owned(),
            endpoint: Some("localhost:9000/o'hare".to_owned()),
            url_style: "path".to_owned(),
            use_ssl: false,
        };
        let metadata_schema = "duck'lake";
        let sql =
            build_setup_sql(&catalog_url, &data_url, Some(&s3), Some(metadata_schema)).unwrap();

        assert!(sql.contains(&format!("KEY_ID {}", quote_literal(&s3.access_key_id))));
        assert!(sql.contains(&format!("SECRET {}", quote_literal(&s3.secret_access_key))));
        assert!(sql.contains(&format!(
            "ENDPOINT {}",
            quote_literal(s3.endpoint.as_deref().unwrap())
        )));
        assert!(sql.contains(&format!(
            "METADATA_SCHEMA {}",
            quote_literal(metadata_schema)
        )));
    }
}
