use rustls::pki_types::CertificateDer;
use secrecy::{ExposeSecret, Secret};
use tokio_postgres::Config;
use tokio_postgres::config::SslMode;

/// Connection config for a PostgreSQL database to be used with `tokio`.
///
/// Contains the connection parameters needed to establish a connection to a PostgreSQL
/// database server, including network location, authentication credentials, and security
/// settings.
#[derive(Debug, Clone)]
pub struct PgConnectionConfig {
    /// Host name or IP address of the PostgreSQL server
    pub host: String,
    /// Port number that the PostgreSQL server listens on
    pub port: u16,
    /// Name of the target database
    pub name: String,
    /// Username for authentication
    pub username: String,
    /// Optional password for authentication
    pub password: Option<Secret<String>>,
    /// TLS config for the connection
    pub tls_config: PgTlsConfig,
}

impl PgConnectionConfig {
    /// Creates connection options for connecting to the PostgreSQL server without
    /// specifying a database.
    ///
    /// Returns [`Config`] configured with the host, port, username, SSL mode and optional
    /// password from this instance. The database name is set to the username as per
    /// PostgreSQL convention. Useful for administrative operations that must be performed
    /// before connecting to a specific database, like database creation.
    pub fn without_db(&self) -> Config {
        let mut this = self.clone();
        // Postgres requires a database, so we default to the database which is equal to the username
        // since this seems to be the standard.
        this.name = this.username.clone();

        this.into()
    }

    /// Creates connection options for connecting to a specific database.
    ///
    /// Returns [`Config`] configured with all connection parameters including the database
    /// name from this instance.
    pub fn with_db(&self) -> Config {
        self.clone().into()
    }
}

impl From<PgConnectionConfig> for Config {
    /// Converts [`PgConnectionConfig`] into a [`Config`] instance.
    ///
    /// Sets all connection parameters including host, port, database name, username,
    /// SSL mode, and optional password.
    fn from(value: PgConnectionConfig) -> Self {
        let mut config = Config::new();
        config
            .host(value.host)
            .port(value.port)
            .dbname(value.name)
            .user(value.username)
            //
            // We set only ssl_mode from the tls config here and not trusted_root_certs
            // because we are using rustls for tls connections and rust_postgres
            // crate doesn't yet support rustls. See the following for details:
            //
            // * PgReplicationClient::connect_tls method for details
            // * https://github.com/sfackler/rust-postgres/issues/421
            //
            // TODO: Does setting ssl mode has an effect here?
            .ssl_mode(value.tls_config.ssl_mode);

        if let Some(password) = value.password {
            config.password(password.expose_secret());
        }

        config
    }
}

#[derive(Debug, Clone)]
pub struct PgTlsConfig {
    /// The SSL verification to use when making a TLS connection to Postgres
    pub ssl_mode: SslMode,

    /// The trusted root certificates to use for the TLS connection verification.
    pub trusted_root_certs: Vec<CertificateDer<'static>>,
}
