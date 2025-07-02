use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use async_trait::async_trait;
use config::shared::PgConnectionConfig;
use futures::{ready, Stream};
use pin_project_lite::pin_project;
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres_replication::LogicalReplicationStream;
use rustls::pki_types::CertificateDer;
use thiserror::Error;
use tokio_postgres::{types::PgLsn, CopyOutStream};
use tracing::info;

use crate::{
    clients::postgres::{ReplicationClient, ReplicationClientError},
    conversions::{
        cdc_event::{CdcEvent, CdcEventConversionError, CdcEventConverter},
        table_row::{TableRow, TableRowConversionError, TableRowConverter},
    },
};

use super::{Source, SourceError};

pub enum TableNamesFrom {
    Vec(Vec<TableName>),
    Publication(String),
}

#[derive(Debug, Error)]
pub enum PostgresSourceError {
    #[error("replication client error: {0}")]
    ReplicationClient(#[from] ReplicationClientError),

    #[error("cdc stream can only be started with a publication")]
    MissingPublication,

    #[error("cdc stream can only be started with a slot_name")]
    MissingSlotName,
}

impl SourceError for PostgresSourceError {}

pub struct PostgresSource {
    replication_client: ReplicationClient,
    table_schemas: HashMap<TableId, TableSchema>,
    slot_name: Option<String>,
    publication: Option<String>,
}

impl PostgresSource {
    pub async fn new(
        config: PgConnectionConfig,
        trusted_root_certs: Vec<CertificateDer<'static>>,
        slot_name: Option<String>,
        table_names_from: TableNamesFrom,
    ) -> Result<PostgresSource, PostgresSourceError> {
        let mut replication_client = match config.tls.enabled {
            true => ReplicationClient::connect_tls(config, trusted_root_certs).await?,
            false => ReplicationClient::connect_no_tls(config).await?,
        };

        // TODO: we have to fix this whole block which starts the transaction and loads the data.
        //  We will not do this here in the future but rather let the pipeline drive this based on
        //  its internal state.
        replication_client.begin_readonly_transaction().await?;
        if let Some(ref slot_name) = slot_name {
            replication_client.get_or_create_slot(slot_name).await?;
        }

        let (table_names, publication) =
            Self::get_table_names_and_publication(&replication_client, table_names_from).await?;
        let table_schemas = replication_client
            .get_table_schemas(&table_names, publication.as_deref())
            .await?;

        Ok(PostgresSource {
            replication_client,
            table_schemas,
            publication,
            slot_name,
        })
    }

    fn publication(&self) -> Option<&String> {
        self.publication.as_ref()
    }

    fn slot_name(&self) -> Option<&String> {
        self.slot_name.as_ref()
    }

    async fn get_table_names_and_publication(
        replication_client: &ReplicationClient,
        table_names_from: TableNamesFrom,
    ) -> Result<(Vec<TableName>, Option<String>), ReplicationClientError> {
        Ok(match table_names_from {
            TableNamesFrom::Vec(table_names) => (table_names, None),
            TableNamesFrom::Publication(publication) => {
                if !replication_client.publication_exists(&publication).await? {
                    return Err(ReplicationClientError::MissingPublication(
                        publication.to_string(),
                    ));
                }
                (
                    replication_client
                        .get_publication_table_names(&publication)
                        .await?,
                    Some(publication),
                )
            }
        })
    }
}

#[async_trait]
impl Source for PostgresSource {
    type Error = PostgresSourceError;

    fn get_table_schemas(&self) -> &HashMap<TableId, TableSchema> {
        &self.table_schemas
    }

    async fn get_table_copy_stream(
        &self,
        table_name: &TableName,
        column_schemas: &[ColumnSchema],
    ) -> Result<TableCopyStream, Self::Error> {
        info!("starting table copy stream for table {table_name}");

        let stream = self
            .replication_client
            .get_table_copy_stream(table_name, column_schemas)
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;

        Ok(TableCopyStream {
            stream,
            column_schemas: column_schemas.to_vec(),
        })
    }

    async fn commit_transaction(&mut self) -> Result<(), Self::Error> {
        self.replication_client
            .commit_txn()
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;
        Ok(())
    }

    async fn get_cdc_stream(&self, start_lsn: PgLsn) -> Result<CdcStream, Self::Error> {
        info!("starting cdc stream at lsn {start_lsn}");

        let publication = self
            .publication()
            .ok_or(PostgresSourceError::MissingPublication)?;
        let slot_name = self
            .slot_name()
            .ok_or(PostgresSourceError::MissingSlotName)?;

        let stream = self
            .replication_client
            .get_logical_replication_stream(publication, slot_name, start_lsn)
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;

        const TIME_SEC_CONVERSION: u64 = 946_684_800;
        let postgres_epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        Ok(CdcStream {
            stream,
            table_schemas: self.table_schemas.clone(),
            postgres_epoch,
        })
    }
}

#[derive(Debug, Error)]
pub enum TableCopyStreamError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("conversion error: {0}")]
    ConversionError(TableRowConversionError),
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream {
        #[pin]
        stream: CopyOutStream,
        column_schemas: Vec<ColumnSchema>,
    }
}

impl Stream for TableCopyStream {
    type Item = Result<TableRow, TableCopyStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(row)) => match TableRowConverter::try_from(&row, this.column_schemas) {
                Ok(row) => Poll::Ready(Some(Ok(row))),
                Err(e) => {
                    let e = TableCopyStreamError::ConversionError(e);
                    Poll::Ready(Some(Err(e)))
                }
            },
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug, Error)]
pub enum CdcStreamError {
    #[error("tokio_postgres error: {0}")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[error("cdc event conversion error: {0}")]
    CdcEventConversion(#[from] CdcEventConversionError),
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct CdcStream {
        #[pin]
        stream: LogicalReplicationStream,
        table_schemas: HashMap<TableId, TableSchema>,
        postgres_epoch: SystemTime,
    }
}

#[derive(Debug, Error)]
pub enum StatusUpdateError {
    #[error("system time error: {0}")]
    SystemTime(#[from] SystemTimeError),

    #[error("tokio_postgres error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),
}

impl CdcStream {
    pub async fn send_status_update(
        self: Pin<&mut Self>,
        lsn: PgLsn,
    ) -> Result<(), StatusUpdateError> {
        let this = self.project();
        let ts = this.postgres_epoch.elapsed()?.as_micros() as i64;
        this.stream
            .standby_status_update(lsn, lsn, lsn, ts, 0)
            .await?;

        Ok(())
    }
}

impl Stream for CdcStream {
    type Item = Result<CdcEvent, CdcStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(msg)) => match CdcEventConverter::try_from(msg, this.table_schemas) {
                Ok(row) => Poll::Ready(Some(Ok(row))),
                Err(e) => Poll::Ready(Some(Err(e.into()))),
            },
            Some(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            None => Poll::Ready(None),
        }
    }
}
