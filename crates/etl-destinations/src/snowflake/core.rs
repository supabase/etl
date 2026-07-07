use std::collections::HashMap;

use etl::{
    bail,
    data::{OldTableRow, TableRow, UpdatedTableRow},
    destination::{
        DestinationTableMetadata, DestinationTableSchemaStatus, DestinationWriteStatus,
        DropTableForCopyResult, TaskSet, WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    event::{DeleteEvent, Event, InsertEvent, UpdateEvent},
    schema::{ColumnSchema, ReplicatedTableSchema, TableId},
    store::DestinationStore,
};
use tracing::{info, warn};

use crate::{
    snowflake::{
        Client,
        auth::{AuthManager, HttpExchanger, TokenProvider},
        encoding::{CdcMeta, CdcOperation},
        metrics::register_metrics,
        schema,
        streaming::{OffsetToken, RestStreamClient, RowBatchBuilder, StreamClient},
    },
    table_name::try_stringify_table_name,
};

type EventIter = std::iter::Peekable<std::vec::IntoIter<Event>>;

/// Postgres replication to Snowflake via Snowpipe Streaming.
///
/// Thin adapter between the ETL [`etl::destination::Destination`] trait and
/// [`Client`]. Translates replication events into client operations and manages
/// the state store bookkeeping.
pub struct Destination<S, T = AuthManager<HttpExchanger>, C = RestStreamClient<T>> {
    client: Client<T, C>,
    store: S,
    tasks: TaskSet,
}

impl<S: Clone, T: TokenProvider, C: StreamClient> Clone for Destination<S, T, C> {
    fn clone(&self) -> Self {
        Self { client: self.client.clone(), store: self.store.clone(), tasks: self.tasks.clone() }
    }
}

impl<S, T, C> Destination<S, T, C>
where
    S: DestinationStore,
    T: TokenProvider + 'static,
    C: StreamClient,
{
    /// Create a new destination.
    pub fn new(client: Client<T, C>, store: S) -> Self {
        register_metrics();
        Self { client, store, tasks: TaskSet::new() }
    }

    /// Ensure the Snowflake table and streaming channel exist for this table.
    /// Operation is idempotent.
    pub async fn prepare_table_for_streaming(
        &self,
        table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = table_schema.id();
        let table_name = try_stringify_table_name(table_schema.name())?.to_uppercase();
        let columns: Vec<_> = table_schema.column_schemas().cloned().collect();

        let table_is_new = self
            .client
            .ensure_table(table_id, &table_name, &columns)
            .await
            .map_err(EtlError::from)?;

        if table_is_new {
            let snapshot_id = table_schema.inner().snapshot_id;
            let replication_mask = table_schema.replication_mask().clone();
            let metadata = DestinationTableMetadata::new_applied(
                table_name.clone(),
                snapshot_id,
                replication_mask,
            );
            self.store.store_destination_table_metadata(table_id, metadata).await?;
        }

        Ok(())
    }

    /// Write rows during the initial snapshot (table copy) phase.
    ///
    /// All rows are stamped as inserts with a zero offset since the table
    /// starts empty.
    pub async fn write_table_rows(
        &self,
        schema: &ReplicatedTableSchema,
        rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // Table must exist even for empty snapshots, CDC events may arrive later.
        self.prepare_table_for_streaming(schema).await?;

        if rows.is_empty() {
            return Ok(());
        }

        let table_id = schema.id();
        let columns: Vec<_> = schema.column_schemas().cloned().collect();

        // Build row batches. Snowflake has limits on max size of input, so we slice
        // into proper batches, when necessary.
        let zero = OffsetToken::zero();
        let mut builder = RowBatchBuilder::new();
        for row in &rows {
            builder
                .push_row(&columns, row, CdcMeta::new(CdcOperation::Insert, zero.as_ref()), &zero)
                .map_err(EtlError::from)?;
        }

        let batches = builder.finish().map_err(EtlError::from)?;
        self.client.insert_batches(table_id, batches).await.map_err(EtlError::from)?;

        Ok(())
    }

    /// Process  CDC events.
    ///
    /// All events (inserts, updates, deletes, truncates, and schema changes)
    /// from the replication stream are processed here.
    pub async fn process_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut iter = events.into_iter().peekable();

        while iter.peek().is_some() {
            let builders = self.accumulate_data_events(&mut iter).await?;
            self.flush_batches(builders).await?;
            self.apply_relation_events(&mut iter).await?;
            self.apply_truncate_events(&mut iter).await?;
        }

        Ok(())
    }

    /// Last offset committed by Snowflake for this table's channel.
    pub async fn committed_offset(&self, table_id: TableId) -> EtlResult<Option<OffsetToken>> {
        self.client.committed_offset(table_id).await.map_err(EtlError::from)
    }

    async fn accumulate_data_events(
        &self,
        iter: &mut EventIter,
    ) -> EtlResult<HashMap<TableId, RowBatchBuilder>> {
        let mut builders: HashMap<TableId, RowBatchBuilder> = HashMap::new();
        let mut column_cache: HashMap<TableId, Vec<ColumnSchema>> = HashMap::new();

        // Consume data events (insert/update/delete) into per-table batch builders,
        // stopping at barrier events (truncate, relation) that require a flush before
        // they can be applied.
        while let Some(event) = iter.peek() {
            if matches!(event, Event::Truncate(_) | Event::Relation(_)) {
                break;
            }
            let event = iter.next().expect("iterator is non-empty after peek");
            match event {
                Event::Insert(e) => self.encode_insert(e, &mut builders, &mut column_cache).await?,
                Event::Update(e) => self.encode_update(e, &mut builders, &mut column_cache).await?,
                Event::Delete(e) => self.encode_delete(e, &mut builders, &mut column_cache).await?,
                _ => {}
            }
        }

        Ok(builders)
    }

    async fn flush_batches(&self, builders: HashMap<TableId, RowBatchBuilder>) -> EtlResult<()> {
        for (table_id, builder) in builders {
            let batches = builder.finish().map_err(EtlError::from)?;
            self.client.insert_batches(table_id, batches).await.map_err(EtlError::from)?;
        }
        Ok(())
    }

    async fn apply_relation_events(&self, iter: &mut EventIter) -> EtlResult<()> {
        while matches!(iter.peek(), Some(Event::Relation(_))) {
            if let Some(Event::Relation(rel)) = iter.next() {
                self.handle_relation_event(&rel.replicated_table_schema).await?;
            }
        }
        Ok(())
    }

    async fn apply_truncate_events(&self, iter: &mut EventIter) -> EtlResult<()> {
        // Collect and dedup tables to be truncated.
        let mut truncated: HashMap<TableId, ReplicatedTableSchema> = HashMap::new();
        while matches!(iter.peek(), Some(Event::Truncate(_))) {
            if let Some(Event::Truncate(t)) = iter.next() {
                for schema in t.truncated_tables {
                    truncated.insert(schema.id(), schema);
                }
            }
        }

        // Truncate tables.
        for (_, schema) in truncated {
            let table_name = try_stringify_table_name(schema.name())?.to_uppercase();
            self.client.truncate_table(schema.id(), &table_name).await.map_err(EtlError::from)?;
        }

        Ok(())
    }

    async fn encode_insert(
        &self,
        e: InsertEvent,
        builders: &mut HashMap<TableId, RowBatchBuilder>,
        column_cache: &mut HashMap<TableId, Vec<ColumnSchema>>,
    ) -> EtlResult<()> {
        let table_id = e.replicated_table_schema.id();
        self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema).await?;

        let cols = &column_cache[&table_id];
        let offset = OffsetToken::new(e.commit_lsn, e.tx_ordinal);
        builders
            .entry(table_id)
            .or_default()
            .push_row(
                cols,
                &e.table_row,
                CdcMeta::new(CdcOperation::Insert, offset.as_ref()),
                &offset,
            )
            .map_err(EtlError::from)
    }

    async fn encode_update(
        &self,
        e: UpdateEvent,
        builders: &mut HashMap<TableId, RowBatchBuilder>,
        column_cache: &mut HashMap<TableId, Vec<ColumnSchema>>,
    ) -> EtlResult<()> {
        let full_row = snowflake_update_row(&e.replicated_table_schema, e.updated_table_row)?;

        let table_id = e.replicated_table_schema.id();
        self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema).await?;

        let cols = &column_cache[&table_id];
        let offset = OffsetToken::new(e.commit_lsn, e.tx_ordinal);
        builders
            .entry(table_id)
            .or_default()
            .push_row(cols, &full_row, CdcMeta::new(CdcOperation::Update, offset.as_ref()), &offset)
            .map_err(EtlError::from)
    }

    async fn encode_delete(
        &self,
        e: DeleteEvent,
        builders: &mut HashMap<TableId, RowBatchBuilder>,
        column_cache: &mut HashMap<TableId, Vec<ColumnSchema>>,
    ) -> EtlResult<()> {
        let table_id = e.replicated_table_schema.id();
        let offset = OffsetToken::new(e.commit_lsn, e.tx_ordinal);

        match snowflake_delete_row(&e.replicated_table_schema, e.old_table_row)? {
            SnowflakeDeleteRow::Full(row) => {
                self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema)
                    .await?;
                let cols = &column_cache[&table_id];
                builders
                    .entry(table_id)
                    .or_default()
                    .push_row(
                        cols,
                        &row,
                        CdcMeta::new(CdcOperation::Delete, offset.as_ref()),
                        &offset,
                    )
                    .map_err(EtlError::from)
            }
            SnowflakeDeleteRow::Key(key_row) => {
                self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema)
                    .await?;
                let identity_cols: Vec<_> =
                    e.replicated_table_schema.identity_column_schemas().cloned().collect();
                builders
                    .entry(table_id)
                    .or_default()
                    .push_row(
                        &identity_cols,
                        &key_row,
                        CdcMeta::new(CdcOperation::Delete, offset.as_ref()),
                        &offset,
                    )
                    .map_err(EtlError::from)
            }
        }
    }

    async fn ensure_column_cache(
        &self,
        column_cache: &mut HashMap<TableId, Vec<ColumnSchema>>,
        table_id: TableId,
        table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        #[allow(clippy::map_entry)]
        if !column_cache.contains_key(&table_id) {
            self.prepare_table_for_streaming(table_schema).await?;
            let cols: Vec<_> = table_schema.column_schemas().cloned().collect();
            column_cache.insert(table_id, cols);
        }
        Ok(())
    }

    async fn handle_relation_event(&self, new_schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let table_id = new_schema.id();
        let new_snapshot_id = new_schema.inner().snapshot_id;

        let Some(metadata) = self.store.get_applied_destination_table_metadata(table_id).await?
        else {
            bail!(
                ErrorKind::CorruptedTableSchema,
                "Destination metadata missing for Snowflake schema change",
                format!(
                    "Table {table_id} received schema snapshot {new_snapshot_id}, but destination \
                     metadata from initial synchronization was not found."
                )
            );
        };

        let current_snapshot_id = metadata.snapshot_id;
        let current_replication_mask = metadata.replication_mask.clone();
        let new_replication_mask = new_schema.replication_mask().clone();

        if current_snapshot_id == new_snapshot_id
            && current_replication_mask == new_replication_mask
        {
            info!(table_id = ?table_id, "schema unchanged, skipping relation event");
            return Ok(());
        }

        info!(
            table_id = ?table_id,
            "schema change detected: snapshot_id {current_snapshot_id} -> {new_snapshot_id}"
        );

        let current_table_schema = self
            .store
            .get_table_schema(&table_id, current_snapshot_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Stored schema snapshot missing for Snowflake schema change",
                    format!(
                        "Table {table_id} needs stored schema snapshot {current_snapshot_id} to \
                         compare with incoming snapshot {new_snapshot_id}, but it was not found."
                    )
                )
            })?;

        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );

        let table_name = try_stringify_table_name(new_schema.name())?.to_uppercase();

        let updated_metadata = DestinationTableMetadata::new_applied(
            metadata.destination_table_id.clone(),
            current_snapshot_id,
            current_replication_mask,
        )
        .with_schema_change(
            new_snapshot_id,
            new_replication_mask,
            DestinationTableSchemaStatus::Applying,
        );
        self.store.store_destination_table_metadata(table_id, updated_metadata.clone()).await?;

        let diff = current_schema.diff(new_schema);
        if let Err(err) =
            self.client.apply_schema_diff(&table_name, &diff).await.map_err(EtlError::from)
        {
            warn!(
                table_id = ?table_id,
                error = %err,
                "schema change failed, manual intervention may be required"
            );
            return Err(err);
        }

        self.store
            .store_destination_table_metadata(table_id, updated_metadata.to_applied())
            .await?;

        let new_columns: Vec<_> = new_schema.column_schemas().cloned().collect();
        schema::validate_no_cdc_collisions(&new_columns).map_err(EtlError::from)?;
        self.client.refresh_table(&table_id).await.map_err(EtlError::from)?;

        info!(table_id = ?table_id, "schema change applied");
        Ok(())
    }
}

/// Delete row payloads Snowflake can encode.
#[derive(Debug)]
enum SnowflakeDeleteRow {
    /// A full old row image.
    Full(TableRow),
    /// A key-only old row image.
    Key(TableRow),
}

/// Returns the full new row required for a Snowflake update row.
fn snowflake_update_row(
    replicated_table_schema: &ReplicatedTableSchema,
    updated_table_row: UpdatedTableRow,
) -> EtlResult<TableRow> {
    match updated_table_row {
        UpdatedTableRow::Full(row) => Ok(row),
        UpdatedTableRow::Partial(_) => Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Snowflake update requires a full new row image",
            format!(
                "Table '{}' emitted a partial update row. Snowflake update rows must include all \
                 replicated column values.",
                replicated_table_schema.name()
            )
        )),
    }
}

/// Returns the old row image required for a Snowflake delete row.
fn snowflake_delete_row(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: Option<OldTableRow>,
) -> EtlResult<SnowflakeDeleteRow> {
    match old_table_row {
        Some(OldTableRow::Full(row)) => Ok(SnowflakeDeleteRow::Full(row)),
        Some(OldTableRow::Key(row)) => Ok(SnowflakeDeleteRow::Key(row)),
        None => Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Snowflake delete requires an old row image",
            format!(
                "Table '{}' emitted a delete without an old row image. Snowflake deletes need \
                 either a full old row or a key image.",
                replicated_table_schema.name()
            )
        )),
    }
}

impl<S, T, C> etl::destination::Destination for Destination<S, T, C>
where
    S: DestinationStore,
    T: TokenProvider + 'static,
    C: StreamClient,
{
    fn name() -> &'static str {
        etl_config::shared::DestinationKind::Snowflake.as_str()
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.tasks.shutdown().await
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let table_name = try_stringify_table_name(replicated_table_schema.name())?.to_uppercase();
        let result = self
            .client
            .drop_table_for_copy(replicated_table_schema.id(), &table_name)
            .await
            .map_err(EtlError::from);
        async_result.send(result);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_table_rows(replicated_table_schema, table_rows).await;
        async_result.send(result);
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let destination = self.clone();
        self.tasks
            .spawn(async move {
                let result = destination.process_events(events).await;
                async_result.send(result.map(|_| DestinationWriteStatus::Durable));
            })
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::{
        data::{Cell, PartialTableRow},
        schema::{IdentityMask, ReplicationMask, TableName, TableSchema, Type},
    };

    use super::*;

    fn replicated_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![1, 0]);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    #[test]
    fn snowflake_update_row_accepts_full_new_row() {
        let schema = replicated_schema();
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let result = snowflake_update_row(&schema, UpdatedTableRow::Full(row.clone())).unwrap();

        assert_eq!(result, row);
    }

    #[test]
    fn snowflake_update_row_rejects_partial_new_row() {
        let schema = replicated_schema();
        let partial_row = PartialTableRow::new(2, TableRow::new(vec![Cell::I32(1)]), vec![1]);

        let error =
            snowflake_update_row(&schema, UpdatedTableRow::Partial(partial_row)).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn snowflake_delete_row_accepts_full_old_row() {
        let schema = replicated_schema();
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let result = snowflake_delete_row(&schema, Some(OldTableRow::Full(row.clone()))).unwrap();

        match result {
            SnowflakeDeleteRow::Full(result) => assert_eq!(result, row),
            SnowflakeDeleteRow::Key(_) => panic!("expected full old row"),
        }
    }

    #[test]
    fn snowflake_delete_row_accepts_key_only_old_row() {
        let schema = replicated_schema();
        let row = TableRow::new(vec![Cell::I32(1)]);

        let result = snowflake_delete_row(&schema, Some(OldTableRow::Key(row.clone()))).unwrap();

        match result {
            SnowflakeDeleteRow::Key(result) => assert_eq!(result, row),
            SnowflakeDeleteRow::Full(_) => panic!("expected key old row"),
        }
    }

    #[test]
    fn snowflake_delete_row_rejects_missing_old_row() {
        let schema = replicated_schema();

        let error = snowflake_delete_row(&schema, None).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }
}
