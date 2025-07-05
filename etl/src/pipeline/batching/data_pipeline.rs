use futures::StreamExt;
use postgres::schema::TableId;
use std::sync::Arc;
use std::{collections::HashSet, time::Instant};
use tokio::pin;
use tokio::sync::Notify;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

use crate::{
    conversions::cdc_event::{CdcEvent, CdcEventConversionError},
    pipeline::{
        PipelineAction, PipelineError,
        batching::stream::BatchTimeoutStream,
        destinations::BatchDestination,
        sources::{CommonSourceError, Source, postgres::CdcStreamError},
    },
};

use super::BatchConfig;

#[derive(Debug, Clone)]
pub struct BatchDataPipelineHandle {
    copy_tables_stream_stop: Arc<Notify>,
    cdc_stream_stop: Arc<Notify>,
}

impl BatchDataPipelineHandle {
    pub fn stop(&self) {
        self.copy_tables_stream_stop.notify_one();
        self.cdc_stream_stop.notify_one();
    }
}

pub struct BatchDataPipeline<Src: Source, Dst: BatchDestination> {
    source: Src,
    destination: Dst,
    action: PipelineAction,
    batch_config: BatchConfig,
    copy_tables_stream_stop: Arc<Notify>,
    cdc_stream_stop: Arc<Notify>,
}

impl<Src: Source, Dst: BatchDestination> BatchDataPipeline<Src, Dst> {
    pub fn new(
        source: Src,
        destination: Dst,
        action: PipelineAction,
        batch_config: BatchConfig,
    ) -> Self {
        BatchDataPipeline {
            source,
            destination,
            action,
            batch_config,
            copy_tables_stream_stop: Arc::new(Notify::new()),
            cdc_stream_stop: Arc::new(Notify::new()),
        }
    }

    async fn copy_table_schemas(&mut self) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        let table_schemas = self.source.get_table_schemas();
        let table_schemas = table_schemas.clone();

        if !table_schemas.is_empty() {
            self.destination
                .write_table_schemas(table_schemas)
                .await
                .map_err(PipelineError::Destination)?;
        }

        Ok(())
    }

    async fn copy_tables(
        &mut self,
        copied_tables: &HashSet<TableId>,
    ) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        let start = Instant::now();
        let table_schemas = self.source.get_table_schemas();

        let mut keys: Vec<u32> = table_schemas.keys().copied().collect();
        keys.sort();

        for key in keys {
            let table_schema = table_schemas.get(&key).expect("failed to get table key");
            if copied_tables.contains(&table_schema.id) {
                info!("table {} already copied.", table_schema.name);
                continue;
            }

            self.destination
                .truncate_table(table_schema.id)
                .await
                .map_err(PipelineError::Destination)?;

            let table_rows = self
                .source
                .get_table_copy_stream(&table_schema.name, &table_schema.column_schemas)
                .await
                .map_err(PipelineError::Source)?;

            let batch_timeout_stream = BatchTimeoutStream::new(
                table_rows,
                self.batch_config.clone(),
                self.copy_tables_stream_stop.notified(),
            );

            pin!(batch_timeout_stream);

            while let Some(batch) = batch_timeout_stream.next().await {
                info!("got {} table copy events in a batch", batch.len());
                //TODO: Avoid a vec copy
                let mut rows = Vec::with_capacity(batch.len());
                for row in batch {
                    rows.push(row.map_err(CommonSourceError::TableCopyStream)?);
                }
                self.destination
                    .write_table_rows(rows, table_schema.id)
                    .await
                    .map_err(PipelineError::Destination)?;
            }

            self.destination
                .table_copied(table_schema.id)
                .await
                .map_err(PipelineError::Destination)?;
        }
        self.source
            .commit_transaction()
            .await
            .map_err(PipelineError::Source)?;

        let end = Instant::now();
        let seconds = (end - start).as_secs();
        debug!("took {seconds} seconds to copy tables");

        Ok(())
    }

    async fn copy_cdc_events(
        &mut self,
        last_lsn: PgLsn,
    ) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        self.source
            .commit_transaction()
            .await
            .map_err(PipelineError::Source)?;

        let mut last_lsn: u64 = last_lsn.into();
        last_lsn += 1;

        let cdc_events = self
            .source
            .get_cdc_stream(last_lsn.into())
            .await
            .map_err(PipelineError::Source)?;
        pin!(cdc_events);

        let batch_timeout_stream = BatchTimeoutStream::new(
            cdc_events,
            self.batch_config.clone(),
            self.cdc_stream_stop.notified(),
        );
        pin!(batch_timeout_stream);

        while let Some(batch) = batch_timeout_stream.next().await {
            info!("got {} cdc events in a batch", batch.len());
            let mut send_status_update = false;
            let mut events = Vec::with_capacity(batch.len());
            for event in batch {
                if let Err(CdcStreamError::CdcEventConversion(
                    CdcEventConversionError::MissingSchema(_),
                )) = event
                {
                    continue;
                }
                let event = event.map_err(CommonSourceError::CdcStream)?;
                if let CdcEvent::KeepAliveRequested { reply } = event {
                    send_status_update = reply;
                };
                events.push(event);
            }
            let last_lsn = self
                .destination
                .write_cdc_events(events)
                .await
                .map_err(PipelineError::Destination)?;
            if send_status_update {
                info!("sending status update with lsn: {last_lsn}");
                let inner = unsafe {
                    batch_timeout_stream
                        .as_mut()
                        .get_unchecked_mut()
                        .get_inner_mut()
                };
                inner
                    .as_mut()
                    .send_status_update(last_lsn)
                    .await
                    .map_err(CommonSourceError::StatusUpdate)?;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        let resumption_state = self
            .destination
            .get_resumption_state()
            .await
            .map_err(PipelineError::Destination)?;

        match self.action {
            PipelineAction::TableCopiesOnly => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
            }
            PipelineAction::CdcOnly => {
                self.copy_table_schemas().await?;
                self.copy_cdc_events(resumption_state.last_lsn).await?;
            }
            PipelineAction::Both => {
                self.copy_table_schemas().await?;
                self.copy_tables(&resumption_state.copied_tables).await?;
                self.copy_cdc_events(resumption_state.last_lsn).await?;
            }
        }

        Ok(())
    }

    pub fn handle(&self) -> BatchDataPipelineHandle {
        BatchDataPipelineHandle {
            copy_tables_stream_stop: self.copy_tables_stream_stop.clone(),
            cdc_stream_stop: self.cdc_stream_stop.clone(),
        }
    }

    pub fn source(&self) -> &Src {
        &self.source
    }

    pub fn destination(&self) -> &Dst {
        &self.destination
    }
}
