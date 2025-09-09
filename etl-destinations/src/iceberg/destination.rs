use std::{collections::HashSet, sync::Arc};

use etl::{
    destination::Destination,
    error::{ErrorKind, EtlResult},
    etl_error,
    store::{schema::SchemaStore, state::StateStore},
    types::{Event, TableId, TableRow},
};
use tokio::sync::Mutex;

/// Iceberg table identifier.
pub type IcebergTableId = String;

struct Inner {
    /// Cache of table IDs that have been successfully created or verified to exist.
    created_tables: HashSet<IcebergTableId>,
}

pub struct IcebergDestination<S> {
    store: S,
    inner: Arc<Mutex<Inner>>,
}

impl<S> IcebergDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("No schema found for table {table_id}")
                )
            })?;
        todo!()
    }
}

impl<S> Destination for IcebergDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    async fn truncate_table(&self, _table_id: etl::types::TableId) -> EtlResult<()> {
        todo!()
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows(table_id, table_rows).await?;

        Ok(())
    }

    async fn write_events(&self, _events: Vec<Event>) -> EtlResult<()> {
        todo!()
    }
}
