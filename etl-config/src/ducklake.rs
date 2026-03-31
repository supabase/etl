//! DuckLake-specific configuration helpers.

use std::io;
use std::path::PathBuf;

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
    #[error("failed to convert path `{}` to a file url", .0.display())]
    FilePath(PathBuf),
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
        std::env::current_dir()
            .map_err(ParseDucklakeUrlError::CurrentDir)?
            .join(path)
    };

    Url::from_file_path(&path).map_err(|_| ParseDucklakeUrlError::FilePath(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use tempfile::TempDir;

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

        assert_eq!(
            url.as_str(),
            "postgres://postgres:postgres@localhost:5432/ducklake_catalog"
        );
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
}
