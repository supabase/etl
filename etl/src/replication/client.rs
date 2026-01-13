//! PostgreSQL replication client for connecting to source databases.

use crate::error::EtlResult;
use etl_config::shared::{PgConnectionConfig, ETL_HEARTBEAT_OPTIONS};
use postgres_replication::ReplicationClient;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use rustls::RootCertStore;
use std::sync::Arc;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::info;

/// Default connection options for replication connections.
const ETL_REPLICATION_OPTIONS: &str = concat!(
    "application_name=etl",
    " statement_timeout=60000",
    " lock_timeout=60000",
    " idle_in_transaction_session_timeout=60000",
);

/// Client for PostgreSQL logical replication connections.
pub struct PgReplicationClient;

impl PgReplicationClient {
    /// Connects to PostgreSQL in replication mode.
    ///
    /// This establishes a connection suitable for logical replication streaming.
    pub async fn connect(pg_connection_config: PgConnectionConfig) -> EtlResult<ReplicationClient> {
        let config = Self::build_config(&pg_connection_config, ETL_REPLICATION_OPTIONS, true);

        let client = if pg_connection_config.tls.enabled {
            Self::connect_with_tls(config, &pg_connection_config, "replication mode").await?
        } else {
            Self::connect_no_tls(config, "replication mode").await?
        };

        Ok(ReplicationClient::new(client))
    }

    /// Connects to PostgreSQL in regular (non-replication) mode.
    ///
    /// This establishes a standard connection suitable for queries like
    /// heartbeat emissions. Uses shorter timeouts optimized for health checks.
    pub async fn connect_regular(pg_connection_config: PgConnectionConfig) -> EtlResult<Client> {
        let config = Self::build_config(&pg_connection_config, ETL_HEARTBEAT_OPTIONS, false);

        let client = if pg_connection_config.tls.enabled {
            Self::connect_with_tls(config, &pg_connection_config, "regular mode").await?
        } else {
            Self::connect_no_tls(config, "regular mode").await?
        };

        Ok(client)
    }

    /// Builds a tokio-postgres Config from connection parameters.
    fn build_config(
        pg_connection_config: &PgConnectionConfig,
        options: &str,
        replication_mode: bool,
    ) -> Config {
        let mut config = Config::new();
        config
            .host(&pg_connection_config.host)
            .port(pg_connection_config.port)
            .dbname(&pg_connection_config.database)
            .user(&pg_connection_config.user)
            .password(&pg_connection_config.password)
            .options(options);

        if replication_mode {
            config.replication_mode(postgres_replication::ReplicationMode::Logical);
        }

        if pg_connection_config.tls.enabled {
            config.ssl_mode(SslMode::Require);
        }

        config
    }

    /// Connects with TLS, shared implementation for both modes.
    async fn connect_with_tls(
        config: Config,
        pg_connection_config: &PgConnectionConfig,
        mode: &'static str,
    ) -> EtlResult<Client> {
        let root_cert_store = Self::build_root_cert_store(pg_connection_config)?;

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let tls = MakeRustlsConnect::new(tls_config);
        let (client, connection) = config.connect(tls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "{mode} connection error");
            }
        });

        info!("connected to PostgreSQL with TLS ({mode})");
        Ok(client)
    }

    /// Connects without TLS.
    async fn connect_no_tls(config: Config, mode: &'static str) -> EtlResult<Client> {
        let (client, connection) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "{mode} connection error");
            }
        });

        info!("connected to PostgreSQL without TLS ({mode})");
        Ok(client)
    }

    /// Builds a root certificate store from the TLS configuration.
    fn build_root_cert_store(
        pg_connection_config: &PgConnectionConfig,
    ) -> EtlResult<Arc<RootCertStore>> {
        let mut root_cert_store = RootCertStore::empty();

        if let Some(ref ca_cert) = pg_connection_config.tls.ca_cert {
            let certs = CertificateDer::pem_slice_iter(ca_cert.as_bytes())
                .collect::<Result<Vec<_>, _>>()?;
            for cert in certs {
                root_cert_store.add(cert)?;
            }
        }

        Ok(Arc::new(root_cert_store))
    }
}
