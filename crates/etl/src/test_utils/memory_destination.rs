use std::collections::HashMap;

use tracing::info;

use crate::{
    destination::{
        Destination,
        async_result::{
            DropTableForCopyResult, WriteSnapshotBatchResult, WriteStreamBatchesResult,
        },
    },
    error::EtlResult,
    state::DestinationTableMetadata,
    store::SharedStateStore,
    types::{ReplicatedTableSchema, StreamBatch},
};

/// In-memory destination for tests that only logs incoming data.
#[derive(Debug, Clone)]
pub struct MemoryDestination<S> {
    state_store: S,
}

impl<S> MemoryDestination<S>
where
    S: SharedStateStore,
{
    /// Creates a new memory destination.
    pub fn new(state_store: S) -> Self {
        Self { state_store }
    }
}

impl<S> Destination for MemoryDestination<S>
where
    S: SharedStateStore,
{
    fn name() -> &'static str {
        "memory"
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();
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
            batch.replicated_table_schema.inner().snapshot_id,
            batch.replicated_table_schema.replication_mask().clone(),
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
                        // A single normalized stream batch can contain multiple
                        // groups for the same table across schema snapshots.
                        // The destination metadata should reflect the latest
                        // schema applied by the batch.
                        if let Some(group) = change_set.groups.last() {
                            table_schemas.insert(
                                change_set.table_id,
                                group.rows.replicated_table_schema.clone(),
                            );
                        }
                    }
                    StreamBatch::Truncate(truncate) => {
                        let _ = truncate;
                    }
                }
            }

            for (table_id, replicated_table_schema) in table_schemas {
                let metadata = DestinationTableMetadata::new_applied(
                    format!("memory_destination_table_{}", table_id.0),
                    replicated_table_schema.inner().snapshot_id,
                    replicated_table_schema.replication_mask().clone(),
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
