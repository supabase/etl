use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions as SqlxConnectOptions, PgSslMode as SqlxSslMode};
use std::sync::LazyLock;
use std::time::Duration;
use tokio_postgres::{Config as TokioPgConnectOptions, config::SslMode as TokioPgSslMode};

use crate::Config;
use crate::shared::ValidationError;

/// Common Postgres settings shared across all ETL connection types.
///
/// These settings ensure consistent behavior across different Postgres installations:
/// - `datestyle = "ISO"`: Provides consistent date formatting for reliable parsing
/// - `intervalstyle = "postgres"`: Uses standard Postgres interval format
/// - `extra_float_digits = 3`: Ensures sufficient precision for numeric replication
/// - `client_encoding = "UTF8"`: Supports international character sets
/// - `timezone = "UTC"`: Eliminates timezone ambiguity in distributed ETL systems
const COMMON_DATESTYLE: &str = "ISO";
const COMMON_INTERVALSTYLE: &str = "postgres";
const COMMON_EXTRA_FLOAT_DIGITS: i32 = 3;
const COMMON_CLIENT_ENCODING: &str = "UTF8";
const COMMON_TIMEZONE: &str = "UTC";

/// Application name for ETL API connections.
const APP_NAME_API: &str = "supabase_etl_api";

/// Application name for ETL replicator migration connections.
const APP_NAME_REPLICATOR_MIGRATIONS: &str = "supabase_etl_replicator_migrations";

/// Application name for ETL state management connections.
const APP_NAME_REPLICATOR_STATE: &str = "supabase_etl_replicator_state";

/// Application name for ETL logical replication streaming connections.
const APP_NAME_REPLICATOR_STREAMING: &str = "supabase_etl_replicator_streaming";

/// Application name for ETL heartbeat connections to the primary.
const APP_NAME_HEARTBEAT: &str = "supabase_etl_heartbeat";

/// Connection options for the API's metadata database.
///
/// Uses strict timeouts (30s statement, 5s lock, 60s idle) to maintain responsiveness
/// and fail fast when contention occurs, preventing API request timeouts.
pub static ETL_API_OPTIONS: LazyLock<PgConnectionOptions> = LazyLock::new(|| PgConnectionOptions {
    datestyle: COMMON_DATESTYLE.to_string(),
    intervalstyle: COMMON_INTERVALSTYLE.to_string(),
    extra_float_digits: COMMON_EXTRA_FLOAT_DIGITS,
    client_encoding: COMMON_CLIENT_ENCODING.to_string(),
    timezone: COMMON_TIMEZONE.to_string(),
    statement_timeout: 30_000,
    lock_timeout: 5_000,
    idle_in_transaction_session_timeout: 60_000,
    application_name: APP_NAME_API.to_string(),
});

/// Connection options for database migrations.
///
/// Uses extended statement timeout (5 minutes) to accommodate long-running DDL operations
/// while maintaining moderate lock and idle timeouts (10s lock, 60s idle).
pub static ETL_MIGRATION_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions {
        datestyle: COMMON_DATESTYLE.to_string(),
        intervalstyle: COMMON_INTERVALSTYLE.to_string(),
        extra_float_digits: COMMON_EXTRA_FLOAT_DIGITS,
        client_encoding: COMMON_CLIENT_ENCODING.to_string(),
        timezone: COMMON_TIMEZONE.to_string(),
        statement_timeout: 300_000,
        lock_timeout: 10_000,
        idle_in_transaction_session_timeout: 60_000,
        application_name: APP_NAME_REPLICATOR_MIGRATIONS.to_string(),
    });

/// Connection options for logical replication streams.
///
/// Disables statement and idle timeouts to allow large COPY operations and long-running
/// transactions during initial table synchronization and WAL streaming.
pub static ETL_REPLICATION_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions {
        datestyle: COMMON_DATESTYLE.to_string(),
        intervalstyle: COMMON_INTERVALSTYLE.to_string(),
        extra_float_digits: COMMON_EXTRA_FLOAT_DIGITS,
        client_encoding: COMMON_CLIENT_ENCODING.to_string(),
        timezone: COMMON_TIMEZONE.to_string(),
        statement_timeout: 0,
        lock_timeout: 30_000,
        idle_in_transaction_session_timeout: 0,
        application_name: APP_NAME_REPLICATOR_STREAMING.to_string(),
    });

/// Connection options for accessing ETL state metadata in the source database.
///
/// Applies moderate timeouts (30s statement, 10s lock, 60s idle) since metadata queries
/// execute quickly and should not block other operations.
pub static ETL_STATE_MANAGEMENT_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions {
        datestyle: COMMON_DATESTYLE.to_string(),
        intervalstyle: COMMON_INTERVALSTYLE.to_string(),
        extra_float_digits: COMMON_EXTRA_FLOAT_DIGITS,
        client_encoding: COMMON_CLIENT_ENCODING.to_string(),
        timezone: COMMON_TIMEZONE.to_string(),
        statement_timeout: 30_000,
        lock_timeout: 10_000,
        idle_in_transaction_session_timeout: 60_000,
        application_name: APP_NAME_REPLICATOR_STATE.to_string(),
    });

/// Connection options for heartbeat operations to the primary database.
///
/// Uses short timeouts (5s statement, 5s lock, 30s idle) since heartbeat operations
/// are lightweight and should fail fast to allow quick reconnection attempts.
/// The heartbeat worker uses these settings when connecting to the primary to emit
/// `pg_logical_emit_message()` calls that keep the replication slot active.
pub static ETL_HEARTBEAT_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions {
        datestyle: COMMON_DATESTYLE.to_string(),
        intervalstyle: COMMON_INTERVALSTYLE.to_string(),
        extra_float_digits: COMMON_EXTRA_FLOAT_DIGITS,
        client_encoding: COMMON_CLIENT_ENCODING.to_string(),
        timezone: COMMON_TIMEZONE.to_string(),
        statement_timeout: 5_000,
        lock_timeout: 5_000,
        idle_in_transaction_session_timeout: 30_000,
        application_name: APP_NAME_HEARTBEAT.to_string(),
    });

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
    ///
    /// When `options` is `None`, no Postgres session parameters are applied.
    /// When `options` is `Some`, applies connection-type-specific options.
    fn without_db(&self, options: Option<&PgConnectionOptions>) -> Output;

    /// Creates connection options for connecting to the configured database.
    ///
    /// Includes all connection parameters including the specific database name.
    ///
    /// When `options` is `None`, no Postgres session parameters are applied.
    /// When `options` is `Some`, applies connection-type-specific options.
    fn with_db(&self, options: Option<&PgConnectionOptions>) -> Output;
}

impl IntoConnectOptions<SqlxConnectOptions> for PgConnectionConfig {
    /// Creates sqlx connection options without database name.
    fn without_db(&self, options: Option<&PgConnectionOptions>) -> SqlxConnectOptions {
        let ssl_mode = if self.tls.enabled {
            SqlxSslMode::VerifyFull
        } else {
            SqlxSslMode::Prefer
        };
        let mut connect_options = SqlxConnectOptions::new_without_pgpass()
            .host(&self.host)
            .username(&self.username)
            .port(self.port)
            .ssl_mode(ssl_mode)
            .ssl_root_cert_from_pem(self.tls.trusted_root_certs.clone().into_bytes());

        if let Some(password) = &self.password {
            connect_options = connect_options.password(password.expose_secret());
        }

        // Apply options if provided
        if let Some(opts) = options {
            connect_options = connect_options.options(opts.to_key_value_pairs());
        }

        connect_options
    }

    /// Creates sqlx connection options with database name.
    fn with_db(&self, options: Option<&PgConnectionOptions>) -> SqlxConnectOptions {
        let connect_options: SqlxConnectOptions = self.without_db(options);
        connect_options.database(&self.name)
    }
}

impl IntoConnectOptions<TokioPgConnectOptions> for PgConnectionConfig {
    /// Creates tokio-postgres connection options without database name.
    fn without_db(&self, options: Option<&PgConnectionOptions>) -> TokioPgConnectOptions {
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

        if let Some(keepalive) = &self.keepalive {
            config
                .keepalives(true)
                .keepalives_idle(Duration::from_secs(keepalive.idle_secs))
                .keepalives_interval(Duration::from_secs(keepalive.interval_secs))
                .keepalives_retries(keepalive.retries);
        }

        // Apply options if provided
        if let Some(opts) = options {
            config.options(opts.to_options_string());
        }

        config
    }

    /// Creates tokio-postgres connection options with database name.
    fn with_db(&self, options: Option<&PgConnectionOptions>) -> TokioPgConnectOptions {
        let mut config: TokioPgConnectOptions = self.without_db(options);
        config.dbname(self.name.clone());
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_options_string_format() {
        let options_string = ETL_REPLICATION_OPTIONS.to_options_string();

        assert_eq!(
            options_string,
            "-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3 -c client_encoding=UTF8 -c timezone=UTC -c statement_timeout=0 -c lock_timeout=30000 -c idle_in_transaction_session_timeout=0 -c application_name=supabase_etl_replicator_streaming"
        );
    }

    #[test]
    fn test_state_management_options_key_value_pairs() {
        let pairs = ETL_STATE_MANAGEMENT_OPTIONS.to_key_value_pairs();

        assert_eq!(pairs.len(), 9);
        assert!(pairs.contains(&("datestyle".to_string(), "ISO".to_string())));
        assert!(pairs.contains(&("intervalstyle".to_string(), "postgres".to_string())));
        assert!(pairs.contains(&("extra_float_digits".to_string(), "3".to_string())));
        assert!(pairs.contains(&("client_encoding".to_string(), "UTF8".to_string())));
        assert!(pairs.contains(&("timezone".to_string(), "UTC".to_string())));
        assert!(pairs.contains(&("statement_timeout".to_string(), "30000".to_string())));
        assert!(pairs.contains(&("lock_timeout".to_string(), "10000".to_string())));
        assert!(pairs.contains(&(
            "idle_in_transaction_session_timeout".to_string(),
            "60000".to_string()
        )));
        assert!(pairs.contains(&(
            "application_name".to_string(),
            "supabase_etl_replicator_state".to_string()
        )));
    }

    #[test]
    fn test_api_options_application_name() {
        assert_eq!(ETL_API_OPTIONS.application_name, "supabase_etl_api");
    }

    #[test]
    fn test_heartbeat_options() {
        assert_eq!(ETL_HEARTBEAT_OPTIONS.statement_timeout, 5_000);
        assert_eq!(ETL_HEARTBEAT_OPTIONS.lock_timeout, 5_000);
        assert_eq!(ETL_HEARTBEAT_OPTIONS.idle_in_transaction_session_timeout, 30_000);
        assert_eq!(ETL_HEARTBEAT_OPTIONS.application_name, "supabase_etl_heartbeat");
    }
}
