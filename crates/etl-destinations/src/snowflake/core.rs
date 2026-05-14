use std::collections::HashMap;

use etl::{
    bail,
    concurrency::TaskSet,
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    state::destination_metadata::{DestinationTableMetadata, DestinationTableSchemaStatus},
    store::{schema::SchemaStore, state::StateStore},
    types::{
        ColumnSchema, DeleteEvent, Event, InsertEvent, OldTableRow, ReplicatedTableSchema, TableId,
        TableRow, UpdateEvent, UpdatedTableRow,
    },
};
use tracing::{info, warn};

use crate::{
    snowflake::{
        auth::{AuthManager, HttpExchanger, TokenProvider},
        client::SnowflakeClient,
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
/// Thin adapter between the ETL [`Destination`] trait and [`SnowflakeClient`].
/// Translates replication events into client operations and manages the state
/// store bookkeeping.
pub struct SnowflakeDestination<
    S,
    T: TokenProvider = AuthManager<HttpExchanger>,
    C: StreamClient = RestStreamClient<T>,
> {
    client: SnowflakeClient<T, C>,
    store: S,
    tasks: TaskSet,
}

impl<S: Clone, T: TokenProvider, C: StreamClient> Clone for SnowflakeDestination<S, T, C> {
    fn clone(&self) -> Self {
        Self { client: self.client.clone(), store: self.store.clone(), tasks: self.tasks.clone() }
    }
}

impl<S, T, C> SnowflakeDestination<S, T, C>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    T: TokenProvider + 'static,
    C: StreamClient,
{
    /// Create a new destination.
    pub fn new(client: SnowflakeClient<T, C>, store: S) -> Self {
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
            let event = iter.next().unwrap();
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
            .or_insert_with(RowBatchBuilder::new)
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
        // Accept only full rows, otherwise NULL will be recorded for missing columns
        // (not that they are not changed).
        let full_row = match e.updated_table_row {
            UpdatedTableRow::Full(row) => row,
            UpdatedTableRow::Partial(_) => {
                bail!(
                    ErrorKind::InvalidData,
                    "Partial update rows not supported",
                    "Snowflake destination requires REPLICA IDENTITY FULL for update events"
                );
            }
        };

        let table_id = e.replicated_table_schema.id();
        self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema).await?;

        let cols = &column_cache[&table_id];
        let offset = OffsetToken::new(e.commit_lsn, e.tx_ordinal);
        builders
            .entry(table_id)
            .or_insert_with(RowBatchBuilder::new)
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

        match e.old_table_row {
            Some(OldTableRow::Full(row)) => {
                self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema)
                    .await?;
                let cols = &column_cache[&table_id];
                builders
                    .entry(table_id)
                    .or_insert_with(RowBatchBuilder::new)
                    .push_row(
                        cols,
                        &row,
                        CdcMeta::new(CdcOperation::Delete, offset.as_ref()),
                        &offset,
                    )
                    .map_err(EtlError::from)
            }
            Some(OldTableRow::Key(key_row)) => {
                self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema)
                    .await?;
                let identity_cols: Vec<_> =
                    e.replicated_table_schema.identity_column_schemas().cloned().collect();
                builders
                    .entry(table_id)
                    .or_insert_with(RowBatchBuilder::new)
                    .push_row(
                        &identity_cols,
                        &key_row,
                        CdcMeta::new(CdcOperation::Delete, offset.as_ref()),
                        &offset,
                    )
                    .map_err(EtlError::from)
            }
            None => {
                info!(table_id = ?table_id, "delete event has no old row data, skipping");
                Ok(())
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
                "Missing destination table metadata",
                format!(
                    "No destination table metadata found for table {table_id} when processing \
                     schema change"
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

        let current_table_schema =
            self.store.get_table_schema(&table_id, current_snapshot_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "Old schema not found",
                        format!(
                            "Could not find schema for table {table_id} at snapshot_id \
                             {current_snapshot_id}"
                        )
                    )
                },
            )?;

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

impl<S, T, C> Destination for SnowflakeDestination<S, T, C>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    T: TokenProvider + 'static,
    C: StreamClient,
{
    fn name() -> &'static str {
        "snowflake"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.tasks.shutdown().await
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        self.prepare_table_for_streaming(replicated_table_schema).await?;
        let table_name = try_stringify_table_name(replicated_table_schema.name())?.to_uppercase();
        let result = self
            .client
            .truncate_table(replicated_table_schema.id(), &table_name)
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
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let destination = self.clone();
        self.tasks
            .spawn(async move {
                let result = destination.process_events(events).await;
                async_result.send(result);
            })
            .await;

        Ok(())
    }
}
