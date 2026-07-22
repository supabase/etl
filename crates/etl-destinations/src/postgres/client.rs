//! Postgres destination connection helpers.

use std::sync::Arc;

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use etl_postgres::tokio::tls::MakeRustlsConnect;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, pem::PemObject},
};
use tokio::sync::Mutex as TokioMutex;
use tokio_postgres::{Client, NoTls};
use tracing::debug;

use crate::postgres::encoding::map_postgres_error;

/// Shared Postgres client with reconnect support.
#[derive(Clone)]
pub(crate) struct PostgresClient {
    config: Arc<PgConnectionConfig>,
    inner: Arc<TokioMutex<Option<Client>>>,
    /// Serializes reconnect attempts across clone-sharing workers.
    reconnect_lock: Arc<TokioMutex<()>>,
}

impl PostgresClient {
    /// Creates a disconnected client handle.
    pub(crate) fn new(config: PgConnectionConfig) -> Self {
        Self {
            config: Arc::new(config),
            inner: Arc::new(TokioMutex::new(None)),
            reconnect_lock: Arc::new(TokioMutex::new(())),
        }
    }

    /// Executes a DDL/DML statement without bind parameters.
    pub(crate) async fn execute_simple(
        &self,
        sql: &str,
        description: &'static str,
    ) -> EtlResult<()> {
        let mut guard = self.ensure_connected().await?;
        let client = guard.as_mut().expect("connected client must be present");
        client.batch_execute(sql).await.map_err(|error| map_postgres_error(error, description))?;
        Ok(())
    }

    /// Executes a parameterized statement.
    pub(crate) async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        description: &'static str,
    ) -> EtlResult<u64> {
        let mut guard = self.ensure_connected().await?;
        let client = guard.as_mut().expect("connected client must be present");
        client.execute(sql, params).await.map_err(|error| map_postgres_error(error, description))
    }

    /// Ensures a live client is available, reconnecting when needed.
    async fn ensure_connected(&self) -> EtlResult<tokio::sync::MutexGuard<'_, Option<Client>>> {
        {
            let guard = self.inner.lock().await;
            if let Some(client) = guard.as_ref()
                && !client.is_closed()
            {
                return Ok(guard);
            }
        }

        let _reconnect = self.reconnect_lock.lock().await;
        let mut guard = self.inner.lock().await;
        if let Some(client) = guard.as_ref()
            && !client.is_closed()
        {
            return Ok(guard);
        }

        debug!("connecting postgres destination client");
        let client = connect_postgres_client(&self.config).await?;
        *guard = Some(client);
        Ok(guard)
    }
}

/// Opens a tokio-postgres client using the shared TLS helper pattern.
pub(crate) async fn connect_postgres_client(config: &PgConnectionConfig) -> EtlResult<Client> {
    let pg_config: tokio_postgres::Config = config.with_db(None);

    if config.tls.enabled {
        let mut root_store = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(config.tls.trusted_root_certs.as_bytes()) {
            let cert = cert.map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Invalid Postgres destination TLS certificate",
                    source: error
                )
            })?;
            root_store.add(cert).map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to add Postgres destination TLS certificate",
                    source: error
                )
            })?;
        }

        let tls_config =
            ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth();
        let (client, connection) =
            pg_config.connect(MakeRustlsConnect::new(tls_config)).await.map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to connect to Postgres destination",
                    source: error
                )
            })?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::info!(error = %error, "postgres destination connection closed");
            }
        });

        Ok(client)
    } else {
        let (client, connection) = pg_config.connect(NoTls).await.map_err(|error| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to connect to Postgres destination",
                source: error
            )
        })?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::info!(error = %error, "postgres destination connection closed");
            }
        });

        Ok(client)
    }
}
