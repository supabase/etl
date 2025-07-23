use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions as SqlxConnectOptions, PgSslMode as SqlxSslMode};
use tokio_postgres::{Config as TokioPgConnectOptions, config::SslMode as TokioPgSslMode};

use crate::SerializableSecretString;
use crate::shared::ValidationError;

/// Static PostgreSQL connection options that ensure sane defaults.
///
/// These options are applied to all PostgreSQL connections to ensure consistent
/// behavior across different PostgreSQL installations.
pub struct DefaultPgConnectionOptions;

impl DefaultPgConnectionOptions {
    /// Returns the options as a string suitable for tokio-postgres options parameter.
    ///
    /// Returns a space-separated list of `-c key=value` pairs.
    pub fn to_options_string() -> String {
        "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3 -c client_encoding=UTF8"
            .to_string()
    }

    /// Returns the options as key-value pairs suitable for sqlx.
    ///
    /// Returns a vector of (key, value) tuples.
    pub fn to_key_value_pairs() -> Vec<(String, String)> {
        vec![
            ("datestyle".to_string(), "ISO".to_string()),
            ("intervalstyle".to_string(), "postgres".to_string()),
            ("extra_float_digits".to_string(), "3".to_string()),
            ("client_encoding".to_string(), "UTF8".to_string()),
        ]
    }
}

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
        let mut options = SqlxConnectOptions::new_without_pgpass()
            .host(&self.host)
            .username(&self.username)
            .port(self.port)
            .ssl_mode(ssl_mode)
            .ssl_root_cert_from_pem(self.tls.trusted_root_certs.clone().into_bytes())
            .options(DefaultPgConnectionOptions::to_key_value_pairs());

        if let Some(password) = &self.password {
            options = options.password(password.expose_secret());
        }

        options
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
            .options(DefaultPgConnectionOptions::to_options_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_string_format() {
        let options_string = DefaultPgConnectionOptions::to_options_string();

        assert_eq!(
            options_string,
            "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3 -c client_encoding=UTF8"
        );
    }

    #[test]
    fn test_key_value_pairs() {
        let pairs = DefaultPgConnectionOptions::to_key_value_pairs();

        assert_eq!(pairs.len(), 4);
        assert!(pairs.contains(&("datestyle".to_string(), "ISO".to_string())));
        assert!(pairs.contains(&("intervalstyle".to_string(), "postgres".to_string())));
        assert!(pairs.contains(&("extra_float_digits".to_string(), "3".to_string())));
        assert!(pairs.contains(&("client_encoding".to_string(), "UTF8".to_string())));
    }
}
