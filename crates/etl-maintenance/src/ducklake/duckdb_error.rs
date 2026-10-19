//! DuckDB errors with catalog credentials removed.
//!
//! DuckDB's Postgres extension writes the full catalog connection string,
//! password included, into the messages of errors such as a lost catalog
//! connection. DuckLake must convert every [`duckdb::Error`] into a
//! [`RedactedDuckDbError`] before it logs the error or attaches it to an
//! [`etl::error::EtlError`].

use std::{borrow::Cow, error, fmt, sync::LazyLock};

use regex::Regex;

/// Text that replaces a redacted credential.
const REDACTED: &str = "[redacted]";

/// Matches one libpq `password` field and its value.
///
/// Values may be single-quoted with backslash escapes (libpq conninfo),
/// doubled-quoted with `\''` escapes (conninfo inside a SQL string literal),
/// unterminated, which redacts the rest of the line, or unquoted.
static PASSWORD_FIELD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)\bpassword\s*=\s*(?:''(?:[^'\\]|\\''|\\.)*''|'(?:[^'\\]|\\.)*'|'[^\r\n]*|[^\s'",);]+)"#,
    )
    .expect("password field redaction regex should compile")
});

/// Matches the password in the userinfo of a URL such as
/// `postgres://user:password@host`.
static URL_PASSWORD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\b([a-z][a-z0-9+.-]*://[^\s:/@'"]*):[^\s@/'"]*@"#)
        .expect("url password redaction regex should compile")
});

/// Returns `message` with every Postgres password replaced by `[redacted]`.
///
/// Covers libpq conninfo fields, including conninfo embedded in a SQL string
/// literal, and URL userinfo.
pub fn redact_credentials(message: &str) -> Cow<'_, str> {
    let message = PASSWORD_FIELD_REGEX.replace_all(message, format!("password='{REDACTED}'"));
    match URL_PASSWORD_REGEX.replace_all(&message, format!("$1:{REDACTED}@")) {
        Cow::Borrowed(_) => message,
        Cow::Owned(redacted) => Cow::Owned(redacted),
    }
}

/// A failed DuckDB call whose message has its credentials removed.
///
/// Keeps no source: the wrapped [`duckdb::Error`] would expose the original
/// message to anything that walks the error chain.
#[derive(Debug)]
pub struct RedactedDuckDbError {
    message: String,
}

impl RedactedDuckDbError {
    /// Returns the redacted message.
    ///
    /// DuckDB starts the message with its exception type, which redaction
    /// leaves in place.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<duckdb::Error> for RedactedDuckDbError {
    fn from(error: duckdb::Error) -> Self {
        Self { message: redact_credentials(&error.to_string()).into_owned() }
    }
}

impl fmt::Display for RedactedDuckDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl error::Error for RedactedDuckDbError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_credentials_removes_every_password_form() {
        for (message, expected) in [
            (
                r#"IO Error: Unable to connect to Postgres at "host='localhost' port='5432' dbname='lake' user='etl' password='placeholder-secret'": connection refused"#,
                r#"IO Error: Unable to connect to Postgres at "host='localhost' port='5432' dbname='lake' user='etl' password='[redacted]'": connection refused"#,
            ),
            (
                r"host=localhost password=placeholder-secret dbname=lake",
                r"host=localhost password='[redacted]' dbname=lake",
            ),
            (
                r"host='h' password='pla\'ce\\holder' dbname='lake'",
                r"host='h' password='[redacted]' dbname='lake'",
            ),
            (
                r"ATTACH 'ducklake:postgres:host=''h'' password=''placeholder-secret'' dbname=''lake''' AS lake",
                r"ATTACH 'ducklake:postgres:host=''h'' password='[redacted]' dbname=''lake''' AS lake",
            ),
            (
                r"ATTACH 'ducklake:postgres:password=''pla\''ceholder'' dbname=''lake'''",
                r"ATTACH 'ducklake:postgres:password='[redacted]' dbname=''lake'''",
            ),
            (r"host=h PASSWORD = 'placeholder'", r"host=h password='[redacted]'"),
            (r"password='unterminated secret", r"password='[redacted]'"),
            ("password='pässwörd' dbname=lake", "password='[redacted]' dbname=lake"),
            (
                "failed: postgres://etl:placeholder-secret@localhost:5432/lake",
                "failed: postgres://etl:[redacted]@localhost:5432/lake",
            ),
        ] {
            assert_eq!(redact_credentials(message), expected, "{message}");
        }
    }

    #[test]
    fn redact_credentials_keeps_messages_without_credentials() {
        for message in [
            "",
            "Catalog Error: Table with name t does not exist!",
            "connection to Postgres refused: password authentication failed for user \"etl\"",
            "s3://bucket/path/file.parquet",
        ] {
            assert!(matches!(redact_credentials(message), Cow::Borrowed(_)), "{message}");
        }
    }

    #[test]
    fn redacted_duckdb_error_keeps_type_prefix_without_password() {
        let error = RedactedDuckDbError::from(duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "IO Error: Unable to connect to Postgres at \"password='placeholder'\"".to_owned(),
            ),
        ));

        assert_eq!(
            error.to_string(),
            "IO Error: Unable to connect to Postgres at \"password='[redacted]'\""
        );
    }
}
