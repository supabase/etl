use std::{fmt, ops::Deref};

use etl_config::shared::{
    ETL_API_OPTIONS, IntoConnectOptions, PgConnectionConfig, PgConnectionOptions,
};
use rustls::{
    ClientConfig,
    pki_types::{CertificateDer, pem::PemObject},
};
use thiserror::Error;
use tokio_postgres::{
    Client, Config, Connection, NoTls, Row, Socket, Transaction, tls::MakeTlsConnect, types::ToSql,
};
use tracing::{error, info};

use crate::tokio::MakeRustlsConnect;

/// Source-side Postgres client errors.
#[derive(Debug, Error)]
pub enum PgSourceError {
    /// A database operation failed.
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    /// A TLS configuration or handshake operation failed.
    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),

    /// A trusted root certificate failed PEM parsing.
    #[error("Invalid trusted root certificate: {0}")]
    Pem(#[from] rustls::pki_types::pem::Error),

    /// Source data could not be decoded into the expected type.
    #[error("Invalid source data: {0}")]
    InvalidData(String),
}

fn tls_config(config: &PgConnectionConfig) -> Result<ClientConfig, PgSourceError> {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(config.tls.trusted_root_certs.as_bytes()) {
        root_store.add(cert?)?;
    }

    Ok(ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth())
}

fn spawn_connection<T>(connection: Connection<Socket, T::Stream>)
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    tokio::spawn(async move {
        match connection.await {
            Ok(()) => info!("source postgres connection terminated"),
            Err(error) => error!(error = %error, "source postgres connection error"),
        }
    });
}

/// A tokio-postgres source database client.
pub struct PgSourceClient {
    client: Client,
}

/// A source database transaction.
pub struct PgSourceTransaction<'a> {
    inner: Transaction<'a>,
}

impl fmt::Debug for PgSourceTransaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgSourceTransaction").finish_non_exhaustive()
    }
}

impl PgSourceTransaction<'_> {
    /// Commits the transaction.
    pub async fn commit(self) -> Result<(), PgSourceError> {
        Ok(self.inner.commit().await?)
    }

    /// Rolls back the transaction.
    pub async fn rollback(self) -> Result<(), PgSourceError> {
        Ok(self.inner.rollback().await?)
    }
}

impl<'a> Deref for PgSourceTransaction<'a> {
    type Target = Transaction<'a>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl fmt::Debug for PgSourceClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgSourceClient").finish_non_exhaustive()
    }
}

impl PgSourceClient {
    /// Connects to a source database using API connection options.
    pub async fn connect(config: &PgConnectionConfig) -> Result<Self, PgSourceError> {
        Self::connect_with_options(config, Some(&ETL_API_OPTIONS)).await
    }

    /// Connects to a source database with explicit session options.
    pub async fn connect_with_options(
        config: &PgConnectionConfig,
        options: Option<&PgConnectionOptions>,
    ) -> Result<Self, PgSourceError> {
        let pg_config: Config = config.with_db(options);

        let client = if config.tls.enabled {
            let tls_config = tls_config(config)?;
            let (client, connection) =
                pg_config.connect(MakeRustlsConnect::new(tls_config)).await?;
            spawn_connection::<MakeRustlsConnect>(connection);
            client
        } else {
            let (client, connection) = pg_config.connect(NoTls).await?;
            spawn_connection::<NoTls>(connection);
            client
        };

        info!("connected to source postgres database");

        Ok(Self { client })
    }

    /// Begins a source database transaction.
    pub async fn begin(&mut self) -> Result<PgSourceTransaction<'_>, PgSourceError> {
        Ok(PgSourceTransaction { inner: self.client.transaction().await? })
    }

    /// Executes a statement and returns the affected row count.
    pub async fn execute(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PgSourceError> {
        Ok(self.client.execute(statement, params).await?)
    }

    /// Executes multiple SQL statements in a single batch.
    pub async fn batch_execute(&self, query: &str) -> Result<(), PgSourceError> {
        Ok(self.client.batch_execute(query).await?)
    }

    /// Executes a query and returns all rows.
    pub async fn query(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PgSourceError> {
        Ok(self.client.query(query, params).await?)
    }

    /// Executes a query and returns exactly one row.
    pub async fn query_one(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PgSourceError> {
        Ok(self.client.query_one(query, params).await?)
    }

    /// Executes a query and returns at most one row.
    pub async fn query_opt(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, PgSourceError> {
        Ok(self.client.query_opt(query, params).await?)
    }
}
