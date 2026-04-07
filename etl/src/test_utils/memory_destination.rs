use etl_postgres::types::TableId;
use std::collections::HashSet;
use tracing::info;

use crate::destination::Destination;
use crate::destination::async_result::{
    TruncateTableResult, WriteSnapshotBatchResult, WriteStreamBatchesResult,
};
use crate::error::EtlResult;
use crate::store::state::StateStore;
use crate::types::StreamBatch;

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
        let result = self
            .state_store
            .store_table_mapping(table_id, format!("memory_destination_table_{}", table_id.0))
            .await;

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
            let mut table_ids = HashSet::new();
            for batch in &batches {
                match batch {
                    StreamBatch::Changes(change_set) => {
                        table_ids.insert(change_set.table_id);
                    }
                    StreamBatch::Truncate(truncate) => {
                        for table_id in &truncate.rel_ids {
                            table_ids.insert(*table_id);
                        }
                    }
                }
            }

            for table_id in table_ids {
                self.state_store
                    .store_table_mapping(
                        table_id,
                        format!("memory_destination_table_{}", table_id.0),
                    )
                    .await?;
            }

            info!(batch_count = batches.len(), "writing stream batches");

            Ok(())
        }
        .await;

        async_result.send(result);

        Ok(())
    }
}
