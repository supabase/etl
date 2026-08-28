use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::{
    data::TableRow,
    destination::{
        Destination, DestinationTableMetadata, DestinationWriteStatus, DropTableForCopyResult,
        TableCopyBatchId, WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    event::Event,
    schema::{ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId},
    store::SharedStateStore,
};

/// Display name used in destination schema-validation errors.
const DESTINATION_NAME: &str = "memory";

/// Validates that a relation can advance an applied destination schema.
fn ensure_relation_schema_transition(
    table_id: TableId,
    applied_snapshot_id: SnapshotId,
    applied_replication_mask: &ReplicationMask,
    received_snapshot_id: SnapshotId,
    received_replication_mask: &ReplicationMask,
) -> EtlResult<()> {
    if received_snapshot_id < applied_snapshot_id {
        return Err(etl_error!(
            ErrorKind::DestinationSchemaRewind,
            "Destination schema is newer than the replayed schema snapshot",
            format!(
                "{DESTINATION_NAME} table {table_id} received schema snapshot \
                 {received_snapshot_id}, but the destination already applied snapshot \
                 {applied_snapshot_id}. Reverse DDL is not executed because it could delete newer \
                 column data; resynchronize the table to recover."
            )
        ));
    }

    if received_snapshot_id == applied_snapshot_id
        && received_replication_mask != applied_replication_mask
    {
        return Err(etl_error!(
            ErrorKind::DestinationSchemaRewind,
            "Relation reused an applied schema snapshot with a different replication mask",
            format!(
                "{DESTINATION_NAME} table {table_id} received schema snapshot \
                 {received_snapshot_id} with replication mask {received_replication_mask}, but \
                 the destination already applied the same snapshot with replication mask \
                 {applied_replication_mask}. Supported publication column-list changes use a \
                 newer schema snapshot, and equal-snapshot relations have no ordering with which \
                 to choose a mask; resynchronize the table to recover."
            )
        ));
    }

    Ok(())
}

/// Requires an arriving row schema to match the exact destination metadata.
fn ensure_destination_schema_matches_metadata(
    table_id: TableId,
    metadata: &DestinationTableMetadata,
    received_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    let received_snapshot_id = received_schema.inner().snapshot_id;
    let received_replication_mask = received_schema.replication_mask();
    if metadata.snapshot_id() == received_snapshot_id
        && metadata.replication_mask() == received_replication_mask
    {
        return Ok(());
    }

    Err(etl_error!(
        ErrorKind::DestinationSchemaRewind,
        "Destination metadata does not match the received schema",
        format!(
            "{DESTINATION_NAME} table {table_id} has destination metadata for snapshot {} and \
             replication mask {}, but received snapshot {received_snapshot_id} and replication \
             mask {received_replication_mask}. Recover the recorded destination operation or \
             resynchronize the table before retrying.",
            metadata.snapshot_id(),
            metadata.replication_mask(),
        )
    ))
}

#[derive(Debug)]
struct Inner {
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
}

/// In-memory destination for testing and development purposes.
///
/// [`MemoryDestination`] stores all replicated data in memory, making it ideal
/// for testing ETL pipelines, debugging replication behavior, and development
/// workflows. All data is held in memory and will be lost when the process
/// terminates.
///
/// Like real destinations (BigQuery, Iceberg), this destination tracks table
/// metadata (snapshot IDs and replication masks) in a state store and validates
/// incoming schemas against that metadata. Relation events may advance an
/// applied snapshot; row events must match the applied snapshot and
/// replication mask exactly.
#[derive(Clone)]
pub struct MemoryDestination<S> {
    inner: Arc<Mutex<Inner>>,
    store: S,
}

impl<S> MemoryDestination<S>
where
    S: SharedStateStore,
{
    /// Creates a new memory destination with a state store.
    ///
    /// The state store is used to track table metadata (snapshot IDs and
    /// replication masks), mirroring the behavior of real destinations like
    /// BigQuery and Iceberg.
    pub fn new(store: S) -> Self {
        let inner = Inner { events: Vec::new(), table_rows: HashMap::new() };

        Self { inner: Arc::new(Mutex::new(inner)), store }
    }

    /// Returns a copy of all events stored in this destination.
    ///
    /// This method is useful for testing and verification of pipeline behavior.
    /// It provides access to all replication events that have been written
    /// to this destination since creation or the last clear operation.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn events(&self) -> Vec<Event> {
        let inner = self.inner.lock().await;
        inner.events.clone()
    }

    /// Returns a copy of all table rows stored in this destination.
    ///
    /// This method is useful for testing and verification of pipeline behavior.
    /// It provides access to all table row data that has been written
    /// to this destination, organized by table ID.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        let inner = self.inner.lock().await;
        inner.table_rows.clone()
    }

    /// Clears all stored events and table rows.
    ///
    /// This method is useful for resetting the destination state between tests
    /// or during development workflows.
    pub async fn clear(&self) {
        let mut inner = self.inner.lock().await;
        inner.events.clear();
        inner.table_rows.clear();
    }

    /// Applies a relation event against stored destination table metadata.
    ///
    /// Missing metadata is treated as a broken invariant because initial copy
    /// should have recorded it. A newer snapshot replaces the applied
    /// metadata. An older snapshot, or an equal snapshot with a different
    /// replication mask, is rejected.
    async fn apply_relation_schema(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();
        let received_snapshot_id = replicated_table_schema.inner().snapshot_id;
        let received_replication_mask = replicated_table_schema.replication_mask();
        let Some(metadata) = self.store.get_destination_table_metadata(table_id).await? else {
            return Err(etl_error!(
                ErrorKind::CorruptedTableSchema,
                "Destination metadata missing for memory schema change",
                format!(
                    "{DESTINATION_NAME} table {table_id} received schema snapshot \
                     {received_snapshot_id}, but destination metadata from initial \
                     synchronization was not found."
                )
            ));
        };

        ensure_relation_schema_transition(
            table_id,
            metadata.snapshot_id(),
            metadata.replication_mask(),
            received_snapshot_id,
            received_replication_mask,
        )?;

        if metadata.snapshot_id() == received_snapshot_id {
            debug!(
                table_id = %table_id,
                snapshot_id = %received_snapshot_id,
                replication_mask = %received_replication_mask,
                "memory table schema unchanged"
            );
            return Ok(());
        }

        info!(
            table_id = %table_id,
            current_snapshot_id = %metadata.snapshot_id(),
            new_snapshot_id = %received_snapshot_id,
            current_replication_mask = %metadata.replication_mask(),
            new_replication_mask = %received_replication_mask,
            "memory table schema change applied"
        );

        let metadata = DestinationTableMetadata::new_applied(
            metadata.table_id().to_owned(),
            received_snapshot_id,
            received_replication_mask.clone(),
        );
        self.store.store_destination_table_metadata(table_id, metadata).await?;

        Ok(())
    }

    /// Requires a row schema to match applied destination metadata.
    ///
    /// The first write for a table records applied metadata, matching real
    /// destinations that create the destination table during initial copy.
    async fn ensure_row_schema(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();
        match self.store.get_destination_table_metadata(table_id).await? {
            Some(metadata) => ensure_destination_schema_matches_metadata(
                table_id,
                &metadata,
                replicated_table_schema,
            ),
            None => {
                self.store
                    .store_destination_table_metadata(
                        table_id,
                        Self::build_destination_table_metadata(replicated_table_schema),
                    )
                    .await
            }
        }
    }

    /// Builds applied destination table metadata for a memory-backed table.
    fn build_destination_table_metadata(
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> DestinationTableMetadata {
        let table_id = replicated_table_schema.id();
        let snapshot_id = replicated_table_schema.inner().snapshot_id;
        let replication_mask = replicated_table_schema.replication_mask().clone();
        let destination_table_id = format!("memory_{}", table_id.into_inner());

        DestinationTableMetadata::new_applied(destination_table_id, snapshot_id, replication_mask)
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
        // For table drops, we simulate removing all table rows for a specific table and
        // also the events of that table.
        let mut inner = self.inner.lock().await;

        let table_id = replicated_table_schema.id();
        info!(%table_id, "dropping table for copy");

        inner.table_rows.remove(&table_id);
        inner.events.retain_mut(|event| {
            let has_table_id = event.has_table_id(&table_id);
            if let Event::Truncate(truncate_event) = event
                && has_table_id
            {
                truncate_event.truncated_tables.retain(|s| s.id() != table_id);
                if truncate_event.truncated_tables.is_empty() {
                    return false;
                }

                return true;
            }

            !has_table_id
        });

        async_result.send(Ok(()));

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        _batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();

        self.ensure_row_schema(replicated_table_schema).await?;

        let mut inner = self.inner.lock().await;
        info!(%table_id, row_count = table_rows.len(), "writing table rows");
        inner.table_rows.insert(table_id, table_rows);

        async_result.send(Ok(DestinationWriteStatus::Durable));

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        for event in &events {
            match event {
                Event::Relation(event) => {
                    self.apply_relation_schema(&event.replicated_table_schema).await?;
                }
                Event::Insert(event) => {
                    self.ensure_row_schema(&event.replicated_table_schema).await?;
                }
                Event::Update(event) => {
                    self.ensure_row_schema(&event.replicated_table_schema).await?;
                }
                Event::Delete(event) => {
                    self.ensure_row_schema(&event.replicated_table_schema).await?;
                }
                Event::Truncate(event) => {
                    for replicated_table_schema in &event.truncated_tables {
                        self.ensure_row_schema(replicated_table_schema).await?;
                    }
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }
        }

        let mut inner = self.inner.lock().await;
        info!(event_count = events.len(), "writing events");
        inner.events.extend(events);

        async_result.send(Ok(DestinationWriteStatus::Durable));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        destination::WriteEventsDurability,
        event::{InsertEvent, RelationEvent},
        schema::{
            ColumnSchema, PgLsn, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId,
            TableName, TableSchema, Type,
        },
        store::{MemoryStore, StateStore},
        test_utils::destination::{write_events, write_table_rows},
    };

    fn test_snapshot_id(commit_lsn: u64, message_lsn: u64) -> SnapshotId {
        SnapshotId::new(PgLsn::from(commit_lsn), PgLsn::from(message_lsn))
    }

    fn test_schema(snapshot_id: SnapshotId) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::with_snapshot_id(
            TableId::new(7),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false)],
            snapshot_id,
        ));
        ReplicatedTableSchema::all(table_schema)
    }

    fn relation_event(schema: ReplicatedTableSchema) -> Event {
        Event::Relation(RelationEvent { replicated_table_schema: schema })
    }

    fn insert_event(schema: ReplicatedTableSchema) -> Event {
        Event::Insert(InsertEvent {
            commit_lsn: PgLsn::from(1),
            tx_ordinal: 0,
            replicated_table_schema: schema,
            table_row: TableRow::new(Vec::new()),
        })
    }

    #[tokio::test]
    async fn relation_event_advances_applied_destination_metadata() {
        let store = MemoryStore::new();
        let destination = MemoryDestination::new(store.clone());
        let initial_schema = test_schema(test_snapshot_id(100, 100));
        let next_schema = test_schema(test_snapshot_id(200, 200));

        write_table_rows(&destination, &initial_schema, Vec::new()).await.unwrap();
        write_events(
            &destination,
            WriteEventsDurability::RequireDurable,
            vec![relation_event(next_schema.clone())],
        )
        .await
        .unwrap();

        let metadata =
            store.get_destination_table_metadata(TableId::new(7)).await.unwrap().unwrap();
        assert_eq!(metadata.snapshot_id(), next_schema.inner().snapshot_id);
        assert_eq!(metadata.replication_mask(), next_schema.replication_mask());
    }

    #[tokio::test]
    async fn relation_then_insert_in_the_same_batch_uses_the_new_schema() {
        let store = MemoryStore::new();
        let destination = MemoryDestination::new(store);
        let initial_schema = test_schema(test_snapshot_id(100, 100));
        let next_schema = test_schema(test_snapshot_id(200, 200));

        write_table_rows(&destination, &initial_schema, Vec::new()).await.unwrap();
        write_events(
            &destination,
            WriteEventsDurability::RequireDurable,
            vec![relation_event(next_schema.clone()), insert_event(next_schema)],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn insert_without_relation_rejects_a_newer_schema() {
        let store = MemoryStore::new();
        let destination = MemoryDestination::new(store);
        let initial_schema = test_schema(test_snapshot_id(100, 100));
        let next_schema = test_schema(test_snapshot_id(200, 200));

        write_table_rows(&destination, &initial_schema, Vec::new()).await.unwrap();
        let error = write_events(
            &destination,
            WriteEventsDurability::RequireDurable,
            vec![insert_event(next_schema)],
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DestinationSchemaRewind);
    }

    #[tokio::test]
    async fn relation_event_rejects_an_older_schema_snapshot() {
        let store = MemoryStore::new();
        let destination = MemoryDestination::new(store);
        let applied_schema = test_schema(test_snapshot_id(200, 200));
        let older_schema = test_schema(test_snapshot_id(100, 100));

        write_table_rows(&destination, &applied_schema, Vec::new()).await.unwrap();
        let error = write_events(
            &destination,
            WriteEventsDurability::RequireDurable,
            vec![relation_event(older_schema)],
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DestinationSchemaRewind);
    }

    #[tokio::test]
    async fn relation_event_rejects_a_different_mask_at_the_same_snapshot() {
        let store = MemoryStore::new();
        let destination = MemoryDestination::new(store);
        let snapshot_id = test_snapshot_id(200, 200);
        let applied_schema = test_schema(snapshot_id);
        let conflicting_schema = ReplicatedTableSchema::from_mask(
            Arc::new(applied_schema.inner().clone()),
            ReplicationMask::from_bytes(vec![0]),
        );

        write_table_rows(&destination, &applied_schema, Vec::new()).await.unwrap();
        let error = write_events(
            &destination,
            WriteEventsDurability::RequireDurable,
            vec![relation_event(conflicting_schema)],
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DestinationSchemaRewind);
    }

    #[tokio::test]
    async fn relation_event_requires_destination_metadata_from_copy() {
        let store = MemoryStore::new();
        let destination = MemoryDestination::new(store);
        let schema = test_schema(test_snapshot_id(100, 100));

        let error = write_events(
            &destination,
            WriteEventsDurability::RequireDurable,
            vec![relation_event(schema)],
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::CorruptedTableSchema);
    }
}
