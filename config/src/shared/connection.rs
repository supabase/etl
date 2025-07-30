use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions as SqlxConnectOptions, PgSslMode as SqlxSslMode};
use tokio_postgres::{Config as TokioPgConnectOptions, config::SslMode as TokioPgSslMode};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::SerializableSecretString;
use crate::shared::ValidationError;

/// PostgreSQL connection options for customizing server behavior.
///
/// These options are passed to PostgreSQL during connection establishment to configure
/// session-specific settings that affect how the server processes queries and data.
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
    /// These defaults ensure consistent behavior across different PostgreSQL installations
    /// and are specifically tuned for ETL systems that perform logical replication and
    /// large data operations:
    ///
    /// - `datestyle = "ISO"`: Provides consistent date formatting for reliable parsing
    /// - `intervalstyle = "postgres"`: Uses standard PostgreSQL interval format
    /// - `extra_float_digits = 3`: Ensures sufficient precision for numeric replication
    /// - `client_encoding = "UTF8"`: Supports international character sets
    /// - `timezone = "UTC"`: Eliminates timezone ambiguity in distributed ETL systems
    /// - `statement_timeout = 0`: Disables the timeout, which allows large COPY operations to continue without being interrupted
    /// - `lock_timeout = 30000` (30 seconds): Prevents indefinite blocking on table locks during replication
    /// - `idle_in_transaction_session_timeout = 300000` (5 minutes): Cleans up abandoned transactions that could block VACUUM
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
            idle_in_transaction_session_timeout: 300_000, // 5 minutes in milliseconds
            application_name: "etl".to_string(),
        }
    }
}

impl PgConnectionOptions {
    /// Returns the options as a string suitable for tokio-postgres options parameter.
    ///
    /// Returns a space-separated list of `-c key=value` pairs.
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

    /// Returns the options as key-value pairs suitable for sqlx.
    ///
    /// Returns a vector of (key, value) tuples.
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

/// Configuration for connecting to a Postgres database.
///
/// This struct holds all necessary connection parameters and settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct PgConnectionConfig {
    /// Hostname or IP address of the Postgres server.
    #[cfg_attr(feature = "utoipa", schema(example = "localhost"))]
    pub host: String,
    /// Port number on which the Postgres server is listening.
    #[cfg_attr(feature = "utoipa", schema(example = 5432))]
    pub port: u16,
    /// Name of the Postgres database to connect to.
    #[cfg_attr(feature = "utoipa", schema(example = "mydb"))]
    pub name: String,
    /// Username for authenticating with the Postgres server.
    #[cfg_attr(feature = "utoipa", schema(example = "postgres"))]
    pub username: String,
    /// Password for the specified user. This field is sensitive and redacted in debug output.
    #[cfg_attr(feature = "utoipa", schema(example = "secret123"))]
    pub password: Option<SerializableSecretString>,
    /// TLS configuration for secure connections.
    pub tls: TlsConfig,
}

/// TLS settings for secure Postgres connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct TlsConfig {
    /// PEM-encoded trusted root certificates. Sensitive and redacted in debug output.
    #[cfg_attr(
        feature = "utoipa",
        schema(
            example = "-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgK..."
        )
    )]
    pub trusted_root_certs: String,
    /// Whether TLS is enabled for the connection.
    #[cfg_attr(feature = "utoipa", schema(example = true))]
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
        let options = PgConnectionOptions::default();
        let options_string = options.to_options_string();

        assert_eq!(
            options_string,
            "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3 -c client_encoding=UTF8 -c timezone=UTC -c statement_timeout=0 -c lock_timeout=30000 -c idle_in_transaction_session_timeout=300000 -c application_name=etl"
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
            "300000".to_string()
        )));
        assert!(pairs.contains(&("application_name".to_string(), "etl".to_string())));
    }
}
