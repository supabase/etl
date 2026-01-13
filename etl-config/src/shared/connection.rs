use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions as SqlxConnectOptions, PgSslMode as SqlxSslMode};
use std::sync::LazyLock;
use std::time::Duration;
use tokio_postgres::{Config as TokioPgConnectOptions, config::SslMode as TokioPgSslMode};

use crate::Config;

/// Common Postgres settings shared across all ETL connection types.
const COMMON_DATESTYLE: &str = "ISO";
const COMMON_INTERVALSTYLE: &str = "postgres";
const COMMON_EXTRA_FLOAT_DIGITS: i32 = 3;
const COMMON_CLIENT_ENCODING: &str = "UTF8";
const COMMON_TIMEZONE: &str = "UTC";

const APP_NAME_API: &str = "supabase_etl_api";
const APP_NAME_REPLICATOR_MIGRATIONS: &str = "supabase_etl_replicator_migrations";
const APP_NAME_REPLICATOR_STATE: &str = "supabase_etl_replicator_state";
const APP_NAME_REPLICATOR_STREAMING: &str = "supabase_etl_replicator_streaming";
const APP_NAME_HEARTBEAT: &str = "supabase_etl_heartbeat";

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

#[derive(Debug, Clone)]
pub struct PgConnectionOptions {
    pub datestyle: String,
    pub intervalstyle: String,
    pub extra_float_digits: i32,
    pub client_encoding: String,
    pub timezone: String,
    pub statement_timeout: u32,
    pub lock_timeout: u32,
    pub idle_in_transaction_session_timeout: u32,
    pub application_name: String,
}

impl PgConnectionOptions {
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

    pub fn to_key_value_pairs(&self) -> Vec<(String, String)> {
        vec![
            ("datestyle".to_string(), self.datestyle.clone()),
            ("intervalstyle".to_string(), self.intervalstyle.clone()),
            ("extra_float_digits".to_string(), self.extra_float_digits.to_string()),
            ("client_encoding".to_string(), self.client_encoding.clone()),
            ("timezone".to_string(), self.timezone.clone()),
            ("statement_timeout".to_string(), self.statement_timeout.to_string()),
            ("lock_timeout".to_string(), self.lock_timeout.to_string()),
            ("idle_in_transaction_session_timeout".to_string(), self.idle_in_transaction_session_timeout.to_string()),
            ("application_name".to_string(), self.application_name.clone()),
        ]
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PgConnectionConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    pub password: Option<SecretString>,
    pub tls: TlsConfig,
    /// TCP keepalive configuration for connection health monitoring.
    /// When `None`, TCP keepalives are disabled.
    pub keepalive: Option<TcpKeepaliveConfig>,
}

impl Config for PgConnectionConfig {
    const LIST_PARSE_KEYS: &'static [&'static str] = &[];
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConnectionConfigWithoutSecrets {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub trusted_root_certs: String,
    pub enabled: bool,
}

impl TlsConfig {
    pub fn disabled() -> Self {
        Self {
            trusted_root_certs: "".to_string(),
            enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpKeepaliveConfig {
    pub idle_secs: u64,
    pub interval_secs: u64,
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

pub trait IntoConnectOptions<Output> {
    fn without_db(&self, options: Option<&PgConnectionOptions>) -> Output;
    fn with_db(&self, options: Option<&PgConnectionOptions>) -> Output;
}

impl IntoConnectOptions<SqlxConnectOptions> for PgConnectionConfig {
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

        if let Some(opts) = options {
            connect_options = connect_options.options(opts.to_key_value_pairs());
        }

        connect_options
    }

    fn with_db(&self, options: Option<&PgConnectionOptions>) -> SqlxConnectOptions {
        let connect_options: SqlxConnectOptions = self.without_db(options);
        connect_options.database(&self.name)
    }
}

impl IntoConnectOptions<TokioPgConnectOptions> for PgConnectionConfig {
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

        if let Some(opts) = options {
            config.options(opts.to_options_string());
        }

        config
    }

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
    fn test_heartbeat_options() {
        assert_eq!(ETL_HEARTBEAT_OPTIONS.statement_timeout, 5_000);
        assert_eq!(ETL_HEARTBEAT_OPTIONS.lock_timeout, 5_000);
        assert_eq!(ETL_HEARTBEAT_OPTIONS.idle_in_transaction_session_timeout, 30_000);
        assert_eq!(ETL_HEARTBEAT_OPTIONS.application_name, "supabase_etl_heartbeat");
    }
}
