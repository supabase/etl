use std::{collections::HashMap, sync::Arc};

use etl_postgres::types::TableId;
use tracing::info;

use crate::{
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteSnapshotBatchResult, WriteStreamBatchesResult},
    },
    error::EtlResult,
    state::DestinationTableMetadata,
    store::state::StateStore,
    types::{ReplicationMask, StreamBatch},
};

/// In-memory destination for tests that only logs incoming data.
#[derive(Debug, Clone)]
pub struct MemoryDestination<S> {
    state_store: S,
}

impl<S> MemoryDestination<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    /// Creates a new memory destination.
    pub fn new(state_store: S) -> Self {
        Self { state_store }
    }
}

impl<S> Destination for MemoryDestination<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    fn name() -> &'static str {
        "memory"
    }

    async fn truncate_table(
        &self,
        table_id: TableId,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        info!(table_id = table_id.0, "truncating table");
        async_result.send(Ok(()));

        Ok(())
    }

    async fn write_snapshot_batch(
        &self,
        batch: crate::types::TableArrowBatch,
        async_result: WriteSnapshotBatchResult<()>,
    ) -> EtlResult<()> {
        let table_id = batch.table_id;
        let metadata = DestinationTableMetadata::new_applied(
            format!("memory_destination_table_{}", table_id.0),
            batch.table_schema.snapshot_id,
            ReplicationMask::all(&batch.table_schema),
        );
        let result = self.state_store.store_destination_table_metadata(table_id, metadata).await;

        if result.is_ok() {
            info!(
                table_id = table_id.0,
                row_count = batch.batch.num_rows(),
                "writing snapshot batch"
            );
        }

        async_result.send(result);

        Ok(())
    }

    async fn write_stream_batches(
        &self,
        batches: Vec<StreamBatch>,
        async_result: WriteStreamBatchesResult<()>,
    ) -> EtlResult<()> {
        let result = async {
            let mut table_schemas = HashMap::new();
            for batch in &batches {
                match batch {
                    StreamBatch::Changes(change_set) => {
                        if let Some(group) = change_set.groups.first() {
                            table_schemas
                                .insert(change_set.table_id, Arc::clone(&group.rows.table_schema));
                        }
                    }
                    StreamBatch::Truncate(truncate) => {
                        let _ = truncate;
                    }
                }
            }

            for (table_id, table_schema) in table_schemas {
                let metadata = DestinationTableMetadata::new_applied(
                    format!("memory_destination_table_{}", table_id.0),
                    table_schema.snapshot_id,
                    ReplicationMask::all(&table_schema),
                );
                self.state_store.store_destination_table_metadata(table_id, metadata).await?;
            }

            info!(batch_count = batches.len(), "writing stream batches");

            Ok(())
        }
        .await;

        async_result.send(result);

        Ok(())
    }
}
