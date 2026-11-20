//! DuckLake-specific configuration helpers.

use std::{io, path::PathBuf, str::FromStr, time::Duration};

use sqlx::{
    ConnectOptions,
    postgres::{PgConnectOptions, PgSslMode},
};
use thiserror::Error;
use tokio_postgres::{
    Config as PgConfig,
    config::{Host, SslMode},
};
use url::Url;

/// Longest wait for a pooled DuckLake catalog connection.
///
/// SQLx counts connect retries, TLS, startup, and its liveness ping against
/// this bound. Cold connections to remote Postgres take several seconds in
/// production, which leaves little margin under a 5 s bound. This stays below
/// SQLx's 30 s default because an unreachable pooler holds the first metadata
/// read for this long before it falls back to the direct catalog connection.
pub const DUCKLAKE_CATALOG_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);

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
///
/// Uses the first configured address and host from libpq lists, with query
/// hosts taking precedence over the URL authority. The host remains the TLS
/// identity and password-file lookup key.
pub fn ducklake_catalog_metadata_connect_options(
    catalog_url: &Url,
) -> Result<PgConnectOptions, DuckLakeCatalogConnectOptionsError> {
    let config = PgConfig::from_str(catalog_url.as_str())
        .map_err(DuckLakeCatalogConnectOptionsError::TokioPostgres)?;

    // Normalize before SQLx performs password-file lookup. SQLx accepts only
    // one address, while libpq accepts comma-separated and repeated entries.
    let mut url = catalog_url.clone();
    url.query_pairs_mut()
        .clear()
        .extend_pairs(catalog_url.query_pairs().filter(|(key, _)| key != "hostaddr"));

    let host = catalog_url
        .query_pairs()
        .filter(|(key, _)| key == "host")
        .last()
        .map(|(_, value)| value.into_owned())
        .or_else(|| match config.get_hosts().first() {
            Some(Host::Tcp(host)) => Some(host.clone()),
            _ => None,
        });
    if let Some(host) = host {
        let host = host.split(',').next().unwrap_or_default();
        if host.is_empty()
            && let Some(hostaddr) = config.get_hostaddrs().first()
        {
            url.query_pairs_mut().append_pair("host", &hostaddr.to_string());
        } else {
            url.query_pairs_mut().append_pair("host", libpq_tcp_host(host));
        }
    } else if config.get_hosts().is_empty()
        && let Some(hostaddr) = config.get_hostaddrs().first()
    {
        url.query_pairs_mut().append_pair("host", &hostaddr.to_string());
    }

    let mut options = PgConnectOptions::from_url(&url)
        .map_err(DuckLakeCatalogConnectOptionsError::Sqlx)?
        .clear_hostaddr();

    if let Some(hostaddr) = config.get_hostaddrs().first() {
        options = options.hostaddr(*hostaddr);
    }

    options = options.ssl_mode(sqlx_ssl_mode(config.get_ssl_mode())?);

    Ok(options)
}

/// Maps libpq-style SSL modes onto sqlx's Postgres SSL modes.
fn sqlx_ssl_mode(ssl_mode: SslMode) -> Result<PgSslMode, DuckLakeCatalogConnectOptionsError> {
    match ssl_mode {
        SslMode::Disable => Ok(PgSslMode::Disable),
        SslMode::Prefer => Ok(PgSslMode::Prefer),
        SslMode::Require => Ok(PgSslMode::Require),
        SslMode::VerifyCa => Ok(PgSslMode::VerifyCa),
        SslMode::VerifyFull => Ok(PgSslMode::VerifyFull),
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
    use std::{
        fs::write,
        process::Command,
        sync::{Mutex, OnceLock},
    };

    use sqlx::postgres::PgSslMode;
    use tempfile::{NamedTempFile, TempDir};

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

        assert_eq!(options.get_host(), "catalog.example.test");
        assert!(
            options
                .to_url_lossy()
                .query_pairs()
                .any(|(key, value)| { key == "hostaddr" && value == "192.0.2.10" })
        );
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyFull));
    }

    #[test]
    fn ducklake_catalog_metadata_connect_options_uses_ipv6_hostaddr_as_network_target() {
        let catalog_url = Url::parse(
            "postgres://user:password@catalog.example.test:5432/ducklake?hostaddr=2001:db8::10&\
             sslmode=verify-full",
        )
        .unwrap();

        let options = ducklake_catalog_metadata_connect_options(&catalog_url).unwrap();

        assert_eq!(options.get_host(), "catalog.example.test");
        assert!(
            options
                .to_url_lossy()
                .query_pairs()
                .any(|(key, value)| { key == "hostaddr" && value == "2001:db8::10" })
        );
        assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyFull));
    }

    /// Catalog metadata uses the first address for both libpq list spellings.
    #[test]
    fn catalog_metadata_uses_first_configured_address() {
        for addresses in ["192.0.2.10,2001:db8::11", "192.0.2.10&hostaddr=2001:db8::11"] {
            let url = Url::parse(&format!(
                "postgres://catalog.example/catalog?hostaddr={addresses}&sslmode=verify-full"
            ))
            .unwrap();

            let options = ducklake_catalog_metadata_connect_options(&url).unwrap();

            assert_eq!(options.get_host(), "catalog.example");
            assert!(
                options
                    .to_url_lossy()
                    .query_pairs()
                    .any(|(key, value)| { key == "hostaddr" && value == "192.0.2.10" })
            );
            assert!(matches!(options.get_ssl_mode(), PgSslMode::VerifyFull));
        }
    }

    /// The selected address keeps its canonical host, including URL overrides.
    #[test]
    fn catalog_metadata_selects_canonical_host() {
        for (url, host) in [
            (
                "postgres://first.example,second.example/db?hostaddr=192.0.2.10,192.0.2.11",
                "first.example",
            ),
            (
                "postgres://authority.example/db?host=first.example,second.example&hostaddr=192.0.\
                 2.10",
                "first.example",
            ),
            (
                "postgres://authority.example/db?hostaddr=192.0.2.10&host=query.example",
                "query.example",
            ),
            (
                "postgres://authority.example/db?host=query.example&hostaddr=192.0.2.10",
                "query.example",
            ),
            (
                "postgres://authority.example/db?host=first.example&host=query.example&\
                 hostaddr=192.0.2.10",
                "query.example",
            ),
            ("postgres:///db?hostaddr=192.0.2.10,192.0.2.11", "192.0.2.10"),
            ("postgres:///db?hostaddr=2001:db8::10", "2001:db8::10"),
            ("postgres:///db?host=,second.example&hostaddr=192.0.2.10,192.0.2.11", "192.0.2.10"),
            ("postgres://authority.example/db?host=query.example", "query.example"),
        ] {
            let url = Url::parse(url).unwrap();

            let options = ducklake_catalog_metadata_connect_options(&url).unwrap();

            assert_eq!(options.get_host(), host);
        }
    }

    /// Password-file lookup uses the final identity, never the ambient pin.
    #[test]
    fn catalog_metadata_uses_canonical_password_file_entry() {
        if std::env::var_os("ETL_CONFIG_TEST_CATALOG_CHILD").is_none() {
            let file = NamedTempFile::new().unwrap();
            write(
                file.path(),
                "192.0.2.10:5432:db:user:addresspassword\ncatalog.example:5432:db:user:\
                 hostnamepassword\n",
            )
            .unwrap();

            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ducklake::tests::catalog_metadata_uses_canonical_password_file_entry",
                ])
                .env("ETL_CONFIG_TEST_CATALOG_CHILD", "1")
                .env("PGPASSFILE", file.path())
                .env("PGHOSTADDR", "192.0.2.99")
                .env_remove("PGPASSWORD")
                .output()
                .unwrap();

            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(output.status.success(), "{stdout}\n{stderr}");
            assert!(stdout.contains("1 passed;"), "{stdout}");
            return;
        }

        for url in [
            "postgres://user@catalog.example:5432/db?hostaddr=192.0.2.10,192.0.2.11",
            "postgres://user@catalog.example,second.example:5432/db?hostaddr=192.0.2.10",
            "postgres://user@authority.example:5432/db?hostaddr=192.0.2.10&host=catalog.example",
            "postgres://user@authority.example:5432/db?host=catalog.example&hostaddr=192.0.2.10",
        ] {
            let url = Url::parse(url).unwrap();

            let options = ducklake_catalog_metadata_connect_options(&url).unwrap();
            let parsed = options.to_url_lossy();

            assert_eq!(parsed.password(), Some("hostnamepassword"));
            assert!(
                parsed.query_pairs().any(|(key, value)| key == "hostaddr" && value == "192.0.2.10")
            );
        }

        let url = Url::parse("postgres:///db?user=user&port=5432&hostaddr=192.0.2.10").unwrap();
        let options = ducklake_catalog_metadata_connect_options(&url).unwrap();

        assert_eq!(options.to_url_lossy().password(), Some("addresspassword"));

        let url = Url::parse("postgres://user@catalog.example:5432/db").unwrap();
        let options = ducklake_catalog_metadata_connect_options(&url).unwrap();

        assert!(!options.to_url_lossy().query_pairs().any(|(key, _)| key == "hostaddr"));
    }

    /// Removing the downgrade does not change explicitly requested TLS modes.
    #[test]
    fn catalog_metadata_preserves_explicit_ssl_modes() {
        for mode in ["disable", "prefer", "require", "verify-ca", "verify-full"] {
            let url = Url::parse(&format!(
                "postgres://catalog.example/db?hostaddr=192.0.2.10&sslmode={mode}"
            ))
            .unwrap();

            let options = ducklake_catalog_metadata_connect_options(&url).unwrap();

            assert!(
                options
                    .to_url_lossy()
                    .query_pairs()
                    .any(|(key, value)| { key == "sslmode" && value == mode })
            );
        }
    }

    /// Unix sockets retain SQLx's precedence over a TCP address pin.
    #[test]
    fn catalog_metadata_preserves_unix_socket() {
        for host in ["postgres://%2Ftmp/db?", "postgres:///db?host=/tmp&"] {
            let url = Url::parse(&format!("{host}hostaddr=192.0.2.10")).unwrap();

            let options = ducklake_catalog_metadata_connect_options(&url).unwrap();

            assert_eq!(options.get_socket().unwrap(), &PathBuf::from("/tmp"));
        }
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
