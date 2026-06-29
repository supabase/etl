//! DuckLake-specific configuration helpers.

use std::{io, path::PathBuf};

use thiserror::Error;
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

/// Parses a DuckLake data path and requires an `s3://` URL.
pub fn parse_ducklake_s3_data_path(value: &str) -> Result<Url, ParseDucklakeUrlError> {
    let url = parse_ducklake_url(value)?;

    if url.scheme() != "s3" {
        return Err(ParseDucklakeUrlError::UnsupportedDataPathScheme(url.scheme().to_owned()));
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

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
}
