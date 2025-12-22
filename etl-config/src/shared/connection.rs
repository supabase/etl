use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions as SqlxConnectOptions, PgSslMode as SqlxSslMode};
use std::time::Duration;
use tokio_postgres::{Config as TokioPgConnectOptions, config::SslMode as TokioPgSslMode};

use crate::Config;
use crate::shared::ValidationError;

/// The name identifying the application using this connection.
const ETL_APPLICATION_NAME: &str = "supabase_etl";

/// Postgres server options for ETL workloads.
///
/// Configures session-specific settings that are applied during connection
/// establishment to optimize Postgres behavior for ETL operations.
#[derive(Debug, Clone)]
pub struct PgConnectionOptions {
    /// Sets the display format for date values.
    pub datestyle: String,
    /// Sets the display format for interval values.
    pub intervalstyle: String,
    /// Controls the number of digits displayed for floating-point values.
    pub extra_float_digits: i32,
    /// Sets the client-side character set encoding.
    pub client_encoding: String,
    /// Sets the time zone for displaying and interpreting time stamps.
    pub timezone: String,
    /// Aborts any statement that takes more than the specified number of milliseconds.
    pub statement_timeout: u32,
    /// Aborts any statement that waits longer than the specified milliseconds to acquire a lock.
    pub lock_timeout: u32,
    /// Terminates any session that has been idle within a transaction for longer than the specified milliseconds.
    pub idle_in_transaction_session_timeout: u32,
    /// Sets the application name to be reported in statistics views and logs for connection identification.
    pub application_name: String,
}

impl Default for PgConnectionOptions {
    /// Returns default configuration values optimized for ETL/replication workloads.
    ///
    /// These defaults ensure consistent behavior across different Postgres installations
    /// and are specifically tuned for ETL systems that perform logical replication and
    /// large data operations:
    ///
    /// - `datestyle = "ISO"`: Provides consistent date formatting for reliable parsing
    /// - `intervalstyle = "postgres"`: Uses standard Postgres interval format
    /// - `extra_float_digits = 3`: Ensures sufficient precision for numeric replication
    /// - `client_encoding = "UTF8"`: Supports international character sets
    /// - `timezone = "UTC"`: Eliminates timezone ambiguity in distributed ETL systems
    /// - `statement_timeout = 0`: Disables the timeout, which allows large COPY operations to continue without being interrupted
    /// - `lock_timeout = 30000` (30 seconds): Prevents indefinite blocking on table locks during replication
    /// - `idle_in_transaction_session_timeout = 0`: Disables the timeout, which allows large COPY operations to continue without being interrupted since
    ///   they are ran in a transaction
    /// - `application_name = "etl"`: Enables easy identification in monitoring and pg_stat_activity
    fn default() -> Self {
        Self {
            datestyle: "ISO".to_string(),
            intervalstyle: "postgres".to_string(),
            extra_float_digits: 3,
            client_encoding: "UTF8".to_string(),
            timezone: "UTC".to_string(),
            statement_timeout: 0,
            lock_timeout: 30_000, // 30 seconds in milliseconds
            idle_in_transaction_session_timeout: 0,
            application_name: ETL_APPLICATION_NAME.to_string(),
        }
    }
}

impl PgConnectionOptions {
    /// Formats options as a string for tokio-postgres connection.
    ///
    /// Returns space-separated `-c key=value` pairs suitable for the options parameter.
    pub fn to_options_string(&self) -> String {
        format!(
            "-c datestyle={} -c intervalstyle={} -c extra_float_digits={} -c client_encoding={} -c timezone={} -c statement_timeout={} -c lock_timeout={} -c idle_in_transaction_session_timeout={} -c application_name={}",
            self.datestyle,
            self.intervalstyle,
            self.extra_float_digits,
            self.client_encoding,
            self.timezone,
            self.statement_timeout,
            self.lock_timeout,
            self.idle_in_transaction_session_timeout,
            self.application_name
        )
    }

    /// Formats options as key-value pairs for sqlx connection.
    ///
    /// Returns a vector of (key, value) tuples suitable for sqlx configuration.
    pub fn to_key_value_pairs(&self) -> Vec<(String, String)> {
        vec![
            ("datestyle".to_string(), self.datestyle.clone()),
            ("intervalstyle".to_string(), self.intervalstyle.clone()),
            (
                "extra_float_digits".to_string(),
                self.extra_float_digits.to_string(),
            ),
            ("client_encoding".to_string(), self.client_encoding.clone()),
            ("timezone".to_string(), self.timezone.clone()),
            (
                "statement_timeout".to_string(),
                self.statement_timeout.to_string(),
            ),
            ("lock_timeout".to_string(), self.lock_timeout.to_string()),
            (
                "idle_in_transaction_session_timeout".to_string(),
                self.idle_in_transaction_session_timeout.to_string(),
            ),
            (
                "application_name".to_string(),
                self.application_name.clone(),
            ),
        ]
    }
}

/// Postgres database connection configuration.
///
/// Contains all parameters required to establish a connection to a Postgres
/// database, including authentication, TLS settings, and connection options.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Debug, Clone, Deserialize)]
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
    pub password: Option<SecretString>,
    /// TLS configuration for secure connections.
    pub tls: TlsConfig,
    /// TCP keepalive configuration for connection health monitoring.
    /// When `None`, TCP keepalives are disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive: Option<TcpKeepaliveConfig>,
}

impl Config for PgConnectionConfig {
    const LIST_PARSE_KEYS: &'static [&'static str] = &[];
}

/// Same as [`PgConnectionConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConnectionConfigWithoutSecrets {
    /// Hostname or IP address of the Postgres server.
    pub host: String,
    /// Port number on which the Postgres server is listening.
    pub port: u16,
    /// Name of the Postgres database to connect to.
    pub name: String,
    /// Username for authenticating with the Postgres server.
    pub username: String,
    /// TLS configuration for secure connections.
    pub tls: TlsConfig,
    /// TCP keepalive configuration for connection health monitoring.
    /// When `None`, TCP keepalives are disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive: Option<TcpKeepaliveConfig>,
}

impl From<PgConnectionConfig> for PgConnectionConfigWithoutSecrets {
    fn from(value: PgConnectionConfig) -> Self {
        PgConnectionConfigWithoutSecrets {
            host: value.host,
            port: value.port,
            name: value.name,
            username: value.username,
            tls: value.tls,
            keepalive: value.keepalive,
        }
    }
}

/// TLS configuration for secure Postgres connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// PEM-encoded trusted root certificates.
    pub trusted_root_certs: String,
    /// Whether TLS is enabled for the connection.
    pub enabled: bool,
}

impl TlsConfig {
    /// Returns a TLS configuration that disables TLS.
    pub fn disabled() -> Self {
        Self {
            trusted_root_certs: "".to_string(),
            enabled: false,
        }
    }

    /// Validates TLS configuration consistency.
    ///
    /// Ensures that when TLS is enabled, trusted root certificates are provided.
    /// This validation is required because the TLS implementation uses `rustls`,
    /// which does not automatically fall back to system CA certificates. An empty
    /// root certificate store would cause all TLS handshakes to fail since no
    /// server certificates would be trusted.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.enabled && self.trusted_root_certs.is_empty() {
            return Err(ValidationError::MissingTrustedRootCerts);
        }

        Ok(())
    }
}

/// TCP keepalive configuration for Postgres connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpKeepaliveConfig {
    /// Time in seconds a connection must be idle before sending keepalive probes.
    pub idle_secs: u64,
    /// Time in seconds between individual keepalive probes.
    pub interval_secs: u64,
    /// Number of keepalive probes to send before considering the connection dead.
    pub retries: u32,
}

impl Default for TcpKeepaliveConfig {
    fn default() -> Self {
        Self {
            idle_secs: 30,
            interval_secs: 30,
            retries: 3,
        }
    }
}

/// Trait for converting configuration to database client connection options.
///
/// Provides a common interface for creating connection options for different
/// Postgres client libraries (sqlx and tokio-postgres) while centralizing
/// configuration in [`PgConnectionConfig`].
pub trait IntoConnectOptions<Output> {
    /// Creates connection options without specifying a database.
    ///
    /// Used for administrative operations like database creation that require
    /// connecting to the Postgres server without targeting a specific database.
    fn without_db(&self) -> Output;

    /// Creates connection options for connecting to the configured database.
    ///
    /// Includes all connection parameters including the specific database name.
    fn with_db(&self) -> Output;
}

impl IntoConnectOptions<SqlxConnectOptions> for PgConnectionConfig {
    /// Creates sqlx connection options without database name.
    fn without_db(&self) -> SqlxConnectOptions {
        let ssl_mode = if self.tls.enabled {
            SqlxSslMode::VerifyFull
        } else {
            SqlxSslMode::Prefer
        };
        let default_pg_options = PgConnectionOptions::default();
        let mut options = SqlxConnectOptions::new_without_pgpass()
            .host(&self.host)
            .username(&self.username)
            .port(self.port)
            .ssl_mode(ssl_mode)
            .ssl_root_cert_from_pem(self.tls.trusted_root_certs.clone().into_bytes())
            .options(default_pg_options.to_key_value_pairs());

        if let Some(password) = &self.password {
            options = options.password(password.expose_secret());
        }

        options
    }

    /// Creates sqlx connection options with database name.
    fn with_db(&self) -> SqlxConnectOptions {
        let options: SqlxConnectOptions = self.without_db();
        options.database(&self.name)
    }
}

impl IntoConnectOptions<TokioPgConnectOptions> for PgConnectionConfig {
    /// Creates tokio-postgres connection options without database name.
    fn without_db(&self) -> TokioPgConnectOptions {
        let ssl_mode = if self.tls.enabled {
            TokioPgSslMode::VerifyFull
        } else {
            TokioPgSslMode::Prefer
        };
        let default_pg_options = PgConnectionOptions::default();
        let mut config = TokioPgConnectOptions::new();
        config
            .host(self.host.clone())
            .port(self.port)
            .user(self.username.clone())
            .options(default_pg_options.to_options_string())
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

        if let Some(keepalive) = &self.keepalive {
            config
                .keepalives(true)
                .keepalives_idle(Duration::from_secs(keepalive.idle_secs))
                .keepalives_interval(Duration::from_secs(keepalive.interval_secs))
                .keepalives_retries(keepalive.retries);
        }

        config
    }

    /// Creates tokio-postgres connection options with database name.
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
        let options = PgConnectionOptions::default();
        let options_string = options.to_options_string();

        assert_eq!(
            options_string,
            "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3 -c client_encoding=UTF8 -c timezone=UTC -c statement_timeout=0 -c lock_timeout=30000 -c idle_in_transaction_session_timeout=0 -c application_name=supabase_etl"
        );
    }

    #[test]
    fn test_key_value_pairs() {
        let options = PgConnectionOptions::default();
        let pairs = options.to_key_value_pairs();

        assert_eq!(pairs.len(), 9);
        assert!(pairs.contains(&("datestyle".to_string(), "ISO".to_string())));
        assert!(pairs.contains(&("intervalstyle".to_string(), "postgres".to_string())));
        assert!(pairs.contains(&("extra_float_digits".to_string(), "3".to_string())));
        assert!(pairs.contains(&("client_encoding".to_string(), "UTF8".to_string())));
        assert!(pairs.contains(&("timezone".to_string(), "UTC".to_string())));
        assert!(pairs.contains(&("statement_timeout".to_string(), "0".to_string())));
        assert!(pairs.contains(&("lock_timeout".to_string(), "30000".to_string())));
        assert!(pairs.contains(&(
            "idle_in_transaction_session_timeout".to_string(),
            "0".to_string()
        )));
        assert!(pairs.contains(&("application_name".to_string(), "supabase_etl".to_string())));
    }
}
