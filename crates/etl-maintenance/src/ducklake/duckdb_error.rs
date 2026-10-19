//! DuckDB errors with credentials removed.
//!
//! DuckDB's Postgres extension writes the full catalog connection string,
//! password included, into the messages of errors such as a lost catalog
//! connection, and DuckDB can echo a failed statement, such as the
//! `CREATE SECRET` that carries the S3 credentials. DuckLake must convert every
//! [`duckdb::Error`] into a [`RedactedDuckDbError`] before it logs the error or
//! attaches it to an [`etl::error::EtlError`].

use std::{borrow::Cow, error, fmt, sync::LazyLock};

use regex::Regex;

/// Replaces a redacted libpq `password` field.
const PASSWORD_FIELD_REPLACEMENT: &str = "password='[redacted]'";
/// Replaces the password in URL userinfo, keeping the scheme and user.
const URL_PASSWORD_REPLACEMENT: &str = "$1:[redacted]@";
/// Replaces a redacted DuckDB secret option value, keeping the option name.
const SECRET_OPTION_REPLACEMENT: &str = "$1 '[redacted]'";

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

/// Matches one DuckDB secret option that carries a credential, such as
/// `KEY_ID 'value'` or `SECRET 'value'`, and its SQL string literal.
///
/// Literals may double quotes and backslashes and carry an `E` prefix, as
/// `pg_escape::quote_literal` writes them, or be cut off where DuckDB truncates
/// an echoed statement, which redacts the rest of the line. A secret's name is
/// a double-quoted identifier and stays visible.
static SECRET_OPTION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)\b(\w*SECRET|KEY_ID|SESSION_TOKEN)(?:\s+|\s*=\s*)E?'(?:(?:[^'\\]|''|\\.)*'|[^\r\n]*)"#,
    )
    .expect("secret option redaction regex should compile")
});

/// Returns `message` with every credential replaced by `[redacted]`.
///
/// Covers libpq conninfo passwords, including conninfo embedded in a SQL string
/// literal, URL userinfo, and DuckDB secret options.
pub fn redact_credentials(message: &str) -> Cow<'_, str> {
    let mut redacted = Cow::Borrowed(message);
    for (regex, replacement) in [
        (&*PASSWORD_FIELD_REGEX, PASSWORD_FIELD_REPLACEMENT),
        (&*URL_PASSWORD_REGEX, URL_PASSWORD_REPLACEMENT),
        (&*SECRET_OPTION_REGEX, SECRET_OPTION_REPLACEMENT),
    ] {
        // Only an owned result changed the message.
        let replaced = match regex.replace_all(&redacted, replacement) {
            Cow::Owned(replaced) => Some(replaced),
            Cow::Borrowed(_) => None,
        };
        if let Some(replaced) = replaced {
            redacted = Cow::Owned(replaced);
        }
    }

    redacted
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
    fn redact_credentials_removes_secret_option_values() {
        for (message, expected) in [
            (
                r#"CREATE OR REPLACE SECRET "ducklake_s3" (TYPE S3, KEY_ID 'placeholder-key-id', REGION 'us-east-1', SECRET 'placeholder/secret+key', SCOPE 's3://bucket/')"#,
                r#"CREATE OR REPLACE SECRET "ducklake_s3" (TYPE S3, KEY_ID '[redacted]', REGION 'us-east-1', SECRET '[redacted]', SCOPE 's3://bucket/')"#,
            ),
            (r"SECRET E'pla\\ce''holder', REGION 'r'", r"SECRET '[redacted]', REGION 'r'"),
            (r"SECRET 'pla''ceholder'", r"SECRET '[redacted]'"),
            (r"secret = 'placeholder' and more", r"secret '[redacted]' and more"),
            (r"SESSION_TOKEN 'placeholder-token'", r"SESSION_TOKEN '[redacted]'"),
            (r"CLIENT_SECRET 'placeholder'", r"CLIENT_SECRET '[redacted]'"),
            (
                "LINE 1: ...S3, SECRET 'placeholder-trunc\nnext",
                "LINE 1: ...S3, SECRET '[redacted]'\nnext",
            ),
        ] {
            assert_eq!(redact_credentials(message), expected, "{message}");
        }
    }

    /// Pins redaction against the bundled DuckDB, which echoes the failing
    /// statement in parser errors.
    #[test]
    fn bundled_duckdb_parser_errors_redact_secret_options() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let error = conn
            .execute_batch(
                "CREATE OR REPLACE SECRET \"ducklake_s3\" (TYPE S3, KEY_ID 'placeholder-key-id', \
                 SECRET 'placeholder-secret-key' trailing garbage);",
            )
            .unwrap_err();
        // Guards the test itself: DuckDB echoed the secret.
        assert!(error.to_string().contains("placeholder-secret-key"));

        let error = RedactedDuckDbError::from(error);

        assert!(!error.to_string().contains("placeholder-secret-key"));
        assert!(!error.to_string().contains("placeholder-key-id"));
        assert!(!format!("{error:?}").contains("placeholder-secret-key"));
    }

    #[test]
    fn redact_credentials_keeps_messages_without_credentials() {
        for message in [
            "",
            "Catalog Error: Table with name t does not exist!",
            "connection to Postgres refused: password authentication failed for user \"etl\"",
            "s3://bucket/path/file.parquet",
            r#"Invalid Input Error: Secret "ducklake_s3" does not exist"#,
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
