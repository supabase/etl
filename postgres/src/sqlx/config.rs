use std::fmt::Display;

use secrecy::{ExposeSecret, Secret};
use serde::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgSslMode as SqlxPgSslMode};

/// Connection config for a PostgreSQL database to be used with `sqlx`.
///
/// Contains the connection parameters needed to establish a connection to a PostgreSQL
/// database server, including network location, authentication credentials, and security
/// settings.
#[derive(Debug, Clone, Deserialize)]
pub struct PgConnectionConfig {
    /// Host name or IP address of the PostgreSQL server
    pub host: String,
    /// Port number that the PostgreSQL server listens on
    pub port: u16,
    /// Name of the target database
    pub name: String,
    /// Username for authentication
    pub username: String,
    /// Optional password for authentication, wrapped in [`Secret`] for secure handling
    pub password: Option<Secret<String>>,
    /// TLS config for the connection
    pub tls_config: PgTlsConfig,
}

impl PgConnectionConfig {
    /// Creates connection options for connecting to the PostgreSQL server without
    /// specifying a database.
    ///
    /// Returns [`PgConnectOptions`] configured with the host, port, username, SSL mode
    /// and optional password from this instance. Useful for administrative operations
    /// that must be performed before connecting to a specific database, like database
    /// creation.
    pub fn without_db(&self) -> PgConnectOptions {
        let options = PgConnectOptions::new_without_pgpass()
            .host(&self.host)
            .username(&self.username)
            .port(self.port)
            .ssl_mode(self.tls_config.ssl_mode.clone().into())
            .ssl_root_cert_from_pem(self.tls_config.trusted_root_certs.clone());

        if let Some(password) = &self.password {
            options.password(password.expose_secret())
        } else {
            options
        }
    }

    /// Creates connection options for connecting to a specific database.
    ///
    /// Returns [`PgConnectOptions`] configured with all connection parameters including
    /// the database name from this instance.
    pub fn with_db(&self) -> PgConnectOptions {
        self.without_db().database(&self.name)
    }
}

/// We use our own type because the sqlx enum doesn't implement Deserialize
#[derive(Debug, Clone, Deserialize)]
pub enum PgSslMode {
    /// Only try a non-SSL connection.
    Disable,

    /// First try a non-SSL connection; if that fails, try an SSL connection.
    Allow,

    /// First try an SSL connection; if that fails, try a non-SSL connection.
    ///
    Prefer,

    /// Only try an SSL connection. If a root CA file is present, verify the connection
    /// in the same way as if `VerifyCa` was specified.
    Require,

    /// Only try an SSL connection, and verify that the server certificate is issued by a
    /// trusted certificate authority (CA).
    VerifyCa,

    /// Only try an SSL connection; verify that the server certificate is issued by a trusted
    /// CA and that the requested server host name matches that in the certificate.
    VerifyFull,
}

impl Display for PgSslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                PgSslMode::Disable => "disable",
                PgSslMode::Allow => "allow",
                PgSslMode::Prefer => "prefer",
                PgSslMode::Require => "require",
                PgSslMode::VerifyCa => "verify-ca",
                PgSslMode::VerifyFull => "verify-full",
            }
        )
    }
}

impl From<PgSslMode> for SqlxPgSslMode {
    fn from(value: PgSslMode) -> Self {
        match value {
            PgSslMode::Disable => SqlxPgSslMode::Disable,
            PgSslMode::Allow => SqlxPgSslMode::Allow,
            PgSslMode::Prefer => SqlxPgSslMode::Prefer,
            PgSslMode::Require => SqlxPgSslMode::Require,
            PgSslMode::VerifyCa => SqlxPgSslMode::VerifyCa,
            PgSslMode::VerifyFull => SqlxPgSslMode::VerifyFull,
        }
    }
}

impl From<SqlxPgSslMode> for PgSslMode {
    fn from(value: SqlxPgSslMode) -> Self {
        match value {
            SqlxPgSslMode::Disable => PgSslMode::Disable,
            SqlxPgSslMode::Allow => PgSslMode::Allow,
            SqlxPgSslMode::Prefer => PgSslMode::Prefer,
            SqlxPgSslMode::Require => PgSslMode::Require,
            SqlxPgSslMode::VerifyCa => PgSslMode::VerifyCa,
            SqlxPgSslMode::VerifyFull => PgSslMode::VerifyFull,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PgTlsConfig {
    /// The SSL verification to use when making a TLS connection to Postgres
    pub ssl_mode: PgSslMode,

    /// The trusted root certificates to use for the TLS connection verification.
    /// The certificate should be in pem format
    pub trusted_root_certs: Vec<u8>,
}
