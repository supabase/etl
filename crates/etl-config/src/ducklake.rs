//! DuckLake-specific configuration helpers.

use std::{io, path::PathBuf, str::FromStr};

use sqlx::postgres::{PgConnectOptions, PgSslMode};
use thiserror::Error;
use tokio_postgres::{Config as PgConfig, config::SslMode};
use url::Url;

/// Errors returned by [`parse_ducklake_url`].
#[derive(Debug, Error)]
pub enum ParseDucklakeUrlError {
    /// Resolving the current working directory failed.
    #[error("{0}")]
    CurrentDir(#[source] io::Error),

    /// Parsing the provided URL failed.
    #[error("{0}")]
    Parse(#[source] url::ParseError),

    /// Converting a filesystem path into a `file://` URL failed.
    #[error("Failed to convert path `{}` to a file url", .0.display())]
    FilePath(PathBuf),

    /// DuckLake data path did not use S3 storage.
    #[error("DuckLake data path must use the s3:// scheme, got {0}://")]
    UnsupportedDataPathScheme(String),
}

/// Errors returned by [`ducklake_catalog_metadata_connect_options`].
#[derive(Debug, Error)]
pub enum DuckLakeCatalogConnectOptionsError {
    /// The catalog URL could not be parsed as a tokio-postgres connection.
    #[error("Invalid DuckLake PostgreSQL catalog URL")]
    TokioPostgres(#[source] tokio_postgres::Error),

    /// The catalog URL could not be parsed as a sqlx Postgres connection.
    #[error("Invalid DuckLake PostgreSQL catalog URL")]
    Sqlx(#[source] sqlx::Error),

    /// The catalog URL uses an SSL mode that cannot be represented for sqlx.
    #[error("DuckLake PostgreSQL catalog URL uses an unsupported sslmode")]
    UnsupportedSslMode,
}

/// Parses a DuckLake URL or local path into a normalized [`Url`].
///
/// Values containing `://` are parsed as URLs directly. Plain local paths are
/// resolved against the current working directory and converted into absolute
/// `file://` URLs.
pub fn parse_ducklake_url(value: &str) -> Result<Url, ParseDucklakeUrlError> {
    if value.contains("://") {
        return Url::parse(value).map_err(ParseDucklakeUrlError::Parse);
    }

    if let Ok(url) = Url::parse(value) {
        return Ok(url);
    }

    let path = PathBuf::from(value);
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir().map_err(ParseDucklakeUrlError::CurrentDir)?.join(path)
    };

    Url::from_file_path(&path).map_err(|_| ParseDucklakeUrlError::FilePath(path))
}

/// Returns a TCP host in the form expected by libpq connection strings.
///
/// IPv6 hosts are commonly wrapped in brackets in URLs. Libpq expects the bare
/// address in the `host` parameter, so bracketed IPv6 values are unwrapped.
pub fn libpq_tcp_host(host: &str) -> &str {
    match host.strip_prefix('[').and_then(|host| host.strip_suffix(']')) {
        Some(unbracketed) if unbracketed.contains(':') => unbracketed,
        _ => host,
    }
}

/// Builds sqlx connection options for DuckLake metadata catalog queries.
pub fn ducklake_catalog_metadata_connect_options(
    catalog_url: &Url,
) -> Result<PgConnectOptions, DuckLakeCatalogConnectOptionsError> {
    let config = PgConfig::from_str(catalog_url.as_str())
        .map_err(DuckLakeCatalogConnectOptionsError::TokioPostgres)?;
    let mut options = PgConnectOptions::from_str(catalog_url.as_str())
        .map_err(DuckLakeCatalogConnectOptionsError::Sqlx)?;

    if let Some(hostaddr) = config.get_hostaddrs().first() {
        options = options.host(&hostaddr.to_string());
    } else if let Some(host) = catalog_url.host_str() {
        options = options.host(libpq_tcp_host(host));
    }

    options = options.ssl_mode(sqlx_ssl_mode(config.get_ssl_mode(), config.get_hostaddrs())?);

    Ok(options)
}

/// Maps libpq-style SSL modes onto sqlx's Postgres SSL modes.
fn sqlx_ssl_mode(
    ssl_mode: SslMode,
    hostaddrs: &[std::net::IpAddr],
) -> Result<PgSslMode, DuckLakeCatalogConnectOptionsError> {
    match ssl_mode {
        SslMode::Disable => Ok(PgSslMode::Disable),
        SslMode::Prefer => Ok(PgSslMode::Prefer),
        SslMode::Require => Ok(PgSslMode::Require),
        SslMode::VerifyCa => Ok(PgSslMode::VerifyCa),
        SslMode::VerifyFull if hostaddrs.is_empty() => Ok(PgSslMode::VerifyFull),
        // sqlx does not expose libpq's separate hostaddr field. When dialing a
        // numeric address, verify the CA but avoid hostname verification
        // against the IP literal.
        SslMode::VerifyFull => Ok(PgSslMode::VerifyCa),
        _ => Err(DuckLakeCatalogConnectOptionsError::UnsupportedSslMode),
    }
}

/// Parses a DuckLake data path and requires an `s3://` URL.
pub fn parse_ducklake_s3_data_path(value: &str) -> Result<Url, ParseDucklakeUrlError> {
    let url = parse_ducklake_url(value)?;

    if url.scheme() != "s3" {
        return Err(ParseDucklakeUrlError::UnsupportedDataPathScheme(url.scheme().to_owned()));
    }

    Ok(url)
}

/// Returns the default DuckDB S3 URL style for DuckLake object storage.
pub fn default_ducklake_s3_url_style(endpoint: Option<&str>) -> &'static str {
    if endpoint.is_some() { "path" } else { "vhost" }
}

/// Returns the default DuckDB S3 SSL setting for DuckLake object storage.
pub const fn default_ducklake_s3_use_ssl() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use sqlx::postgres::PgSslMode;
    use tempfile::TempDir;

    use super::*;

    struct CurrentDirGuard(PathBuf);

    impl CurrentDirGuard {
        fn capture() -> Self {
            Self(std::env::current_dir().unwrap())
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.0).unwrap();
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn parse_ducklake_url_keeps_explicit_urls() {
        let url =
            parse_ducklake_url("postgres://postgres:postgres@localhost:5432/ducklake_catalog")
                .unwrap();

        assert_eq!(url.as_str(), "postgres://postgres:postgres@localhost:5432/ducklake_catalog");
    }

    #[test]
    fn parse_ducklake_url_converts_relative_paths_to_absolute_file_urls() {
        let _guard = env_lock().lock().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let _current_dir_guard = CurrentDirGuard::capture();

        std::env::set_current_dir(temp_dir.path()).unwrap();
        let expected_path = std::env::current_dir().unwrap().join("lake_data");

        let url = parse_ducklake_url("./lake_data").unwrap();

        assert_eq!(url, Url::from_file_path(expected_path).unwrap());
    }

    #[test]
    fn parse_ducklake_s3_data_path_accepts_s3_urls() {
        let url = parse_ducklake_s3_data_path("s3://bucket/path").unwrap();

        assert_eq!(url.as_str(), "s3://bucket/path");
    }

    #[test]
    fn parse_ducklake_s3_data_path_rejects_file_urls() {
        let error = parse_ducklake_s3_data_path("file:///tmp/lake").unwrap_err();

        assert!(matches!(
            error,
            ParseDucklakeUrlError::UnsupportedDataPathScheme(scheme) if scheme == "file"
        ));
    }

    #[test]
    fn parse_ducklake_s3_data_path_rejects_plain_paths() {
        let error = parse_ducklake_s3_data_path("./lake_data").unwrap_err();

        assert!(matches!(
            error,
            ParseDucklakeUrlError::UnsupportedDataPathScheme(scheme) if scheme == "file"
        ));
    }

    #[test]
    fn default_ducklake_s3_url_style_uses_vhost_for_aws_s3() {
        assert_eq!(default_ducklake_s3_url_style(None), "vhost");
    }

    #[test]
    fn default_ducklake_s3_url_style_uses_path_for_custom_endpoints() {
        assert_eq!(default_ducklake_s3_url_style(Some("storage.example.com")), "path");
    }

    #[test]
    fn default_ducklake_s3_use_ssl_is_enabled() {
        assert!(default_ducklake_s3_use_ssl());
    }

    #[test]
    fn ducklake_catalog_metadata_connect_options_uses_catalog_host_without_hostaddr() {
        let catalog_url = Url::parse(
            "postgres://user:password@catalog.example.test:5432/ducklake?sslmode=verify-full",
        )
        .unwrap();

        let options = ducklake_catalog_metadata_connect_options(&catalog_url).unwrap();

        assert_eq!(options.get_host(), "catalog.example.test");
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyFull));
    }

    #[test]
    fn ducklake_catalog_metadata_connect_options_uses_ipv4_hostaddr_as_network_target() {
        let catalog_url = Url::parse(
            "postgres://user:password@catalog.example.test:5432/ducklake?hostaddr=192.0.2.10&\
             sslmode=verify-full",
        )
        .unwrap();

        let options = ducklake_catalog_metadata_connect_options(&catalog_url).unwrap();

        assert_eq!(options.get_host(), "192.0.2.10");
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyCa));
    }

    #[test]
    fn ducklake_catalog_metadata_connect_options_uses_ipv6_hostaddr_as_network_target() {
        let catalog_url = Url::parse(
            "postgres://user:password@catalog.example.test:5432/ducklake?hostaddr=2001:db8::10&\
             sslmode=verify-full",
        )
        .unwrap();

        let options = ducklake_catalog_metadata_connect_options(&catalog_url).unwrap();

        assert_eq!(options.get_host(), "2001:db8::10");
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyCa));
    }

    #[test]
    fn ducklake_catalog_metadata_connect_options_supports_ipv6_literal_host() {
        let catalog_url = Url::parse(
            "postgres://postgres:FAKE_SECRET@[2a05:dddd:c3c:b703:e52f:e170:4155:c8dd]:5432/\
             postgres?sslmode=prefer",
        )
        .unwrap();

        let options = ducklake_catalog_metadata_connect_options(&catalog_url).unwrap();

        assert_eq!(options.get_host(), "2a05:dddd:c3c:b703:e52f:e170:4155:c8dd");
        assert!(matches!(options.get_ssl_mode(), PgSslMode::Prefer));
    }
}
