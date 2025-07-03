use std::{collections::HashMap, path::Path};

use tokio::sync::mpsc::{Receiver, Sender, channel};

use async_trait::async_trait;
use postgres::schema::{TableId, TableSchema};
use tokio_postgres::types::PgLsn;

use crate::{
    clients::duckdb::DuckDbClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{PipelineResumptionState, destinations::BatchDestination},
};

use super::{
    DuckDbRequest,
    executor::{DuckDbExecutor, DuckDbExecutorError, DuckDbResponse},
};
pub struct DuckDbDestination {
    req_sender: Sender<DuckDbRequest>,
    res_receiver: Receiver<DuckDbResponse>,
}

const CHANNEL_SIZE: usize = 32;

impl DuckDbDestination {
    pub async fn file<P: AsRef<Path>>(file_name: P) -> Result<DuckDbDestination, duckdb::Error> {
        let (req_sender, req_receiver) = channel(CHANNEL_SIZE);
        let (res_sender, res_receiver) = channel(CHANNEL_SIZE);
        let client = DuckDbClient::open_file(file_name)?;
        let executor = DuckDbExecutor {
            client,
            req_receiver,
            res_sender,
            table_schemas: None,
            final_lsn: None,
            committed_lsn: None,
        };
        executor.start();
        Ok(DuckDbDestination {
            req_sender,
            res_receiver,
        })
    }

    pub async fn mother_duck(
        access_token: &str,
        db_name: &str,
    ) -> Result<DuckDbDestination, duckdb::Error> {
        let (req_sender, req_receiver) = channel(CHANNEL_SIZE);
        let (res_sender, res_receiver) = channel(CHANNEL_SIZE);
        let client = DuckDbClient::open_mother_duck(access_token, db_name)?;
        let executor = DuckDbExecutor {
            client,
            req_receiver,
            res_sender,
            table_schemas: None,
            final_lsn: None,
            committed_lsn: None,
        };
        executor.start();
        Ok(DuckDbDestination {
            req_sender,
            res_receiver,
        })
    }

    pub async fn in_memory() -> Result<DuckDbDestination, duckdb::Error> {
        let (req_sender, req_receiver) = channel(CHANNEL_SIZE);
        let (res_sender, res_receiver) = channel(CHANNEL_SIZE);
        let client = DuckDbClient::open_in_memory()?;
        let executor = DuckDbExecutor {
            client,
            req_receiver,
            res_sender,
            table_schemas: None,
            final_lsn: None,
            committed_lsn: None,
        };
        executor.start();
        Ok(DuckDbDestination {
            req_sender,
            res_receiver,
        })
    }

    pub async fn execute(
        &mut self,
        req: DuckDbRequest,
    ) -> Result<DuckDbResponse, DuckDbExecutorError> {
        self.req_sender.send(req).await?;
        if let Some(res) = self.res_receiver.recv().await {
            Ok(res)
        } else {
            Err(DuckDbExecutorError::NoResponseReceived)
        }
    }
}

#[async_trait]
impl BatchDestination for DuckDbDestination {
    type Error = DuckDbExecutorError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        let req = DuckDbRequest::GetResumptionState;
        match self.execute(req).await? {
            DuckDbResponse::ResumptionState(res) => {
                let resumption_state = res?;
                Ok(resumption_state)
            }
            _ => panic!("invalid response to GetResumptionState request"),
        }
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        let req = DuckDbRequest::CreateTables(table_schemas);
        match self.execute(req).await? {
            DuckDbResponse::CreateTablesResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to CreateTables request"),
        }

        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        //TODO: use batching
        for row in rows {
            let req = DuckDbRequest::InsertRow(row, table_id);
            match self.execute(req).await? {
                DuckDbResponse::InsertRowResponse(res) => {
                    let _ = res?;
                }
                _ => panic!("invalid response to InsertRow request"),
            }
        }

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        //TODO: use batching
        let mut last_lsn = None;
        for event in events {
            let req = DuckDbRequest::HandleCdcEvent(event);
            last_lsn = Some(match self.execute(req).await? {
                DuckDbResponse::HandleCdcEventResponse(res) => res?,
                _ => panic!("invalid response to HandleCdcEvent request"),
            });
        }
        Ok(last_lsn.expect("no last_lsn"))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        let req = DuckDbRequest::TableCopied(table_id);
        match self.execute(req).await? {
            DuckDbResponse::TableCopiedResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to TableCopied request"),
        }
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        let req = DuckDbRequest::TruncateTable(table_id);
        match self.execute(req).await? {
            DuckDbResponse::TruncateTableResponse(res) => {
                let _ = res?;
            }
            _ => panic!("invalid response to TruncateTable request"),
        }
        Ok(())
    }
}
