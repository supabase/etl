use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions as SqlxConnectOptions, PgSslMode as SqlxSslMode};
use tokio_postgres::{Config as TokioPgConnectOptions, config::SslMode as TokioPgSslMode};

use crate::SerializableSecretString;
use crate::shared::ValidationError;

/// Configuration for connecting to a Postgres database.
///
/// This struct holds all necessary connection parameters and settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PgConnectionConfig {
    /// Hostname or IP address of the Postgres server.
    pub host: String,
    /// Port number on which the Postgres server is listening.
    pub port: u16,
    /// Name of the Postgres database to connect to.
    pub name: String,
    /// Username for authenticating with the Postgres server.
    pub username: String,
    /// Password for the specified user. This field is sensitive and redacted in debug output.
    pub password: Option<SerializableSecretString>,
    /// TLS configuration for secure connections.
    pub tls: TlsConfig,
}

/// TLS settings for secure Postgres connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TlsConfig {
    /// PEM-encoded trusted root certificates. Sensitive and redacted in debug output.
    pub trusted_root_certs: String,
    /// Whether TLS is enabled for the connection.
    pub enabled: bool,
}

impl TlsConfig {
    /// Validates the [`TlsConfig`].
    ///
    /// If [`TlsConfig::enabled`] is true, this method checks that [`TlsConfig::trusted_root_certs`] is not empty.
    ///
    /// Returns [`ValidationError::MissingTrustedRootCerts`] if TLS is enabled but no certificates are provided.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.enabled && self.trusted_root_certs.is_empty() {
            return Err(ValidationError::MissingTrustedRootCerts);
        }

        Ok(())
    }
}

/// A trait which can be used to convert the implementation into a crate
/// specific connect options. Since we have two crates for Postgres: sqlx
/// and tokio_postgres, we keep the connection options centralized in
/// [`PgConnectionConfig`] and implement this trait twice, once for each
/// sqlx and tokio_postgres connect options.
pub trait IntoConnectOptions<Output> {
    /// Creates connection options for connecting to the PostgreSQL server without
    /// specifying a database.
    ///
    /// Returns [`Output`] configured with the host, port, username, SSL mode
    /// and optional password from this instance. Useful for administrative operations
    /// that must be performed before connecting to a specific database, like database
    /// creation.
    fn without_db(&self) -> Output;

    /// Creates connection options for connecting to a specific database.
    ///
    /// Returns [`Output`] configured with all connection parameters including
    /// the database name from this instance.
    fn with_db(&self) -> Output;
}

impl IntoConnectOptions<SqlxConnectOptions> for PgConnectionConfig {
    fn without_db(&self) -> SqlxConnectOptions {
        let ssl_mode = if self.tls.enabled {
            SqlxSslMode::VerifyFull
        } else {
            SqlxSslMode::Prefer
        };
        let options = SqlxConnectOptions::new_without_pgpass()
            .host(&self.host)
            .username(&self.username)
            .port(self.port)
            .ssl_mode(ssl_mode)
            .ssl_root_cert_from_pem(self.tls.trusted_root_certs.clone().into_bytes());

        if let Some(password) = &self.password {
            options.password(password.expose_secret())
        } else {
            options
        }
    }

    fn with_db(&self) -> SqlxConnectOptions {
        let options: SqlxConnectOptions = self.without_db();
        options.database(&self.name)
    }
}

impl IntoConnectOptions<TokioPgConnectOptions> for PgConnectionConfig {
    fn without_db(&self) -> TokioPgConnectOptions {
        let ssl_mode = if self.tls.enabled {
            TokioPgSslMode::VerifyFull
        } else {
            TokioPgSslMode::Prefer
        };
        let mut config = TokioPgConnectOptions::new();
        config
            .host(self.host.clone())
            .port(self.port)
            .user(self.username.clone())
            //
            // We set only ssl_mode from the tls config here and not trusted_root_certs
            // because we are using rustls for tls connections and rust_postgres
            // crate doesn't yet support rustls. See the following for details:
            //
            // * PgReplicationClient::connect_tls method
            // * https://github.com/sfackler/rust-postgres/issues/421
            //
            // TODO: Does setting ssl mode has an effect here?
            .ssl_mode(ssl_mode);

        if let Some(password) = &self.password {
            config.password(password.expose_secret());
        }

        config
    }

    fn with_db(&self) -> TokioPgConnectOptions {
        let mut options: TokioPgConnectOptions = self.without_db();
        options.dbname(self.name.clone());
        options
    }
}
