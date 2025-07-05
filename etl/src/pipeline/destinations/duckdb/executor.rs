use std::collections::HashMap;

use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender, error::SendError};
use tokio_postgres::types::{PgLsn, Type};
use tracing::error;

use crate::{
    clients::duckdb::DuckDbClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{PipelineResumptionState, destinations::DestinationError},
};

pub enum DuckDbRequest {
    GetResumptionState,
    CreateTables(HashMap<TableId, TableSchema>),
    InsertRow(TableRow, TableId),
    HandleCdcEvent(CdcEvent),
    TableCopied(TableId),
    TruncateTable(TableId),
}

pub enum DuckDbResponse {
    ResumptionState(Result<PipelineResumptionState, DuckDbExecutorError>),
    CreateTablesResponse(Result<(), DuckDbExecutorError>),
    InsertRowResponse(Result<(), DuckDbExecutorError>),
    HandleCdcEventResponse(Result<PgLsn, DuckDbExecutorError>),
    TableCopiedResponse(Result<(), DuckDbExecutorError>),
    TruncateTableResponse(Result<(), DuckDbExecutorError>),
}

#[derive(Debug, Error)]
pub enum DuckDbExecutorError {
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,

    #[error("no response received")]
    NoResponseReceived,

    #[error("failed to send duckdb request")]
    SendError(#[from] SendError<DuckDbRequest>),
}

impl DestinationError for DuckDbExecutorError {}

pub(super) struct DuckDbExecutor {
    pub(super) client: DuckDbClient,
    pub(super) req_receiver: Receiver<DuckDbRequest>,
    pub(super) res_sender: Sender<DuckDbResponse>,
    pub(super) table_schemas: Option<HashMap<TableId, TableSchema>>,
    pub(super) final_lsn: Option<PgLsn>,
    pub(super) committed_lsn: Option<PgLsn>,
}

impl DuckDbExecutor {
    pub fn start(mut self) {
        tokio::spawn(async move {
            while let Some(req) = self.req_receiver.recv().await {
                match req {
                    DuckDbRequest::GetResumptionState => {
                        let result = self.get_resumption_state();
                        let result = result.inspect(|rs| {
                            self.committed_lsn = Some(rs.last_lsn);
                        });
                        let response = DuckDbResponse::ResumptionState(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::CreateTables(table_schemas) => {
                        let result = self.create_tables(&table_schemas);
                        self.table_schemas = Some(table_schemas);
                        let response = DuckDbResponse::CreateTablesResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::InsertRow(row, table_id) => {
                        let result = self.insert_row(table_id, row);
                        let response = DuckDbResponse::InsertRowResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::HandleCdcEvent(event) => {
                        let result = match event {
                            CdcEvent::Begin(begin_body) => {
                                let final_lsn = begin_body.final_lsn();
                                self.final_lsn = Some(final_lsn.into());
                                self.begin_transaction()
                            }
                            CdcEvent::Commit(commit_body) => {
                                let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                                if let Some(final_lsn) = self.final_lsn {
                                    if commit_lsn == final_lsn {
                                        let res =
                                            self.set_last_lsn_and_commit_transaction(commit_lsn);
                                        self.committed_lsn = Some(commit_lsn);
                                        res
                                    } else {
                                        Err(DuckDbExecutorError::IncorrectCommitLsn(
                                            commit_lsn, final_lsn,
                                        ))
                                    }
                                } else {
                                    Err(DuckDbExecutorError::CommitWithoutBegin)
                                }
                            }
                            CdcEvent::Insert((table_id, table_row)) => {
                                self.insert_row(table_id, table_row)
                            }
                            CdcEvent::Update((table_id, table_row)) => {
                                self.update_row(table_id, table_row)
                            }
                            CdcEvent::Delete((table_id, table_row)) => {
                                self.delete_row(table_id, table_row)
                            }
                            CdcEvent::Origin(_) => Ok(()),
                            CdcEvent::Truncate(truncate_body) => {
                                let truncate_table_fn = || -> Result<(), DuckDbExecutorError> {
                                    for table_id in truncate_body.rel_ids() {
                                        self.truncate_table(*table_id)?;
                                    }
                                    Ok(())
                                };
                                truncate_table_fn()
                            }
                            CdcEvent::Relation(_) => Ok(()),
                            CdcEvent::KeepAliveRequested { reply: _ } => Ok(()),
                            CdcEvent::Type(_) => Ok(()),
                        };

                        let committed_lsn = self.committed_lsn.expect("committed lsn is none");
                        let result = result.map(|_| committed_lsn);
                        let response = DuckDbResponse::HandleCdcEventResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::TableCopied(table_id) => {
                        let result = self.table_copied(table_id);
                        let response = DuckDbResponse::TableCopiedResponse(result);
                        self.send_response(response).await;
                    }
                    DuckDbRequest::TruncateTable(table_id) => {
                        let result = self.truncate_table(table_id);
                        let response = DuckDbResponse::TruncateTableResponse(result);
                        self.send_response(response).await;
                    }
                }
            }
        });
    }

    async fn send_response(&mut self, response: DuckDbResponse) {
        match self.res_sender.send(response).await {
            Ok(_) => {}
            Err(e) => error!("failed to send response: {e}"),
        }
    }

    fn get_resumption_state(&self) -> Result<PipelineResumptionState, DuckDbExecutorError> {
        let copied_tables_table_name = TableName {
            schema: "etl".to_string(),
            name: "copied_tables".to_string(),
        };
        let copied_table_column_schemas = [ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            primary: true,
        }];
        self.client
            .create_schema_if_missing(&copied_tables_table_name.schema)?;

        self.client
            .create_table_if_missing(&copied_tables_table_name, &copied_table_column_schemas)?;

        let last_lsn_table_name = TableName {
            schema: "etl".to_string(),
            name: "last_lsn".to_string(),
        };
        let last_lsn_column_schemas = [ColumnSchema {
            name: "lsn".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            primary: true,
        }];
        if self
            .client
            .create_table_if_missing(&last_lsn_table_name, &last_lsn_column_schemas)?
        {
            self.client.insert_last_lsn_row()?;
        }

        let copied_tables = self.client.get_copied_table_ids()?;
        let last_lsn = self.client.get_last_lsn()?;

        Ok(PipelineResumptionState {
            copied_tables,
            last_lsn,
        })
    }

    fn create_tables(
        &self,
        table_schemas: &HashMap<TableId, TableSchema>,
    ) -> Result<(), DuckDbExecutorError> {
        for table_schema in table_schemas.values() {
            let schema = &table_schema.name.schema;

            self.client.create_schema_if_missing(schema)?;
            self.client
                .create_table_if_missing(&table_schema.name, &table_schema.column_schemas)?;
        }

        Ok(())
    }

    fn insert_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.insert_row(&table_schema.name, &table_row)?;
        Ok(())
    }

    fn update_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.update_row(table_schema, &table_row)?;
        Ok(())
    }

    fn delete_row(
        &self,
        table_id: TableId,
        table_row: TableRow,
    ) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.delete_row(table_schema, &table_row)?;
        Ok(())
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, DuckDbExecutorError> {
        self.table_schemas
            .as_ref()
            .ok_or(DuckDbExecutorError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(DuckDbExecutorError::MissingTableId(table_id))
    }

    fn table_copied(&self, table_id: TableId) -> Result<(), DuckDbExecutorError> {
        self.client.insert_into_copied_tables(table_id)?;
        Ok(())
    }

    fn truncate_table(&self, table_id: TableId) -> Result<(), DuckDbExecutorError> {
        let table_schema = self.get_table_schema(table_id)?;
        self.client.truncate_table(&table_schema.name)?;
        Ok(())
    }

    fn begin_transaction(&self) -> Result<(), DuckDbExecutorError> {
        self.client.begin_transaction()?;
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), DuckDbExecutorError> {
        self.client.commit_transaction()?;
        Ok(())
    }

    fn set_last_lsn_and_commit_transaction(
        &self,
        last_lsn: PgLsn,
    ) -> Result<(), DuckDbExecutorError> {
        self.client.set_last_lsn(last_lsn)?;
        self.commit_transaction()?;
        Ok(())
    }
}
