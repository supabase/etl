use std::{collections::HashMap, sync::Arc, time::Duration};

use etl::{
    bail,
    data::{OldTableRow, TableRow, UpdatedTableRow},
    destination::{
        DestinationTableMetadata, DestinationTableSchemaStatus, DestinationWriteStatus,
        DropTableForCopyResult, TaskSet, WriteEventsDurability, WriteEventsResult,
        WriteTableRowsResult,
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    event::{DeleteEvent, Event, InsertEvent, UpdateEvent},
    schema::{ColumnSchema, ReplicatedTableSchema, TableId},
    store::DestinationStore,
};
use tokio::{
    sync::{Mutex, Semaphore},
    time::timeout,
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

/// Maximum time allowed to drain Snowflake event tasks and retire local table
/// state before a reset.
const RESET_PREPARATION_TIMEOUT: Duration = Duration::from_secs(180);

/// Coordinates the initial Snowflake setup of each table.
///
/// Copy partitions can concurrently observe missing destination metadata. A
/// per-table gate gives one caller ownership of the complete `Applying` ->
/// remote setup -> `Applied` transition so a stale caller cannot overwrite
/// completed metadata with `Applying`. The registry mutex is released before a
/// caller waits for its table gate.
#[derive(Clone)]
struct TableInitializer {
    /// Stable initialization gates retained for this destination's lifetime.
    gates: Arc<Mutex<HashMap<TableId, Arc<Semaphore>>>>,
}

impl TableInitializer {
    /// Creates an empty table-initialization registry.
    fn new() -> Self {
        Self { gates: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Ensures the Snowflake table, channel, and durable metadata exist.
    async fn ensure<S, T, C>(
        &self,
        client: &Client<T, C>,
        store: &S,
        table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()>
    where
        S: DestinationStore,
        T: TokenProvider + 'static,
        C: StreamClient,
    {
        let table_id = table_schema.id();
        let table_name = try_stringify_table_name(table_schema.name())?.to_uppercase();
        let columns: Vec<_> = table_schema.column_schemas().cloned().collect();

        // Applied metadata needs no transition coordination, but this process
        // may still need to reopen its local channel state.
        if store
            .get_destination_table_metadata(table_id)
            .await?
            .is_some_and(|metadata| metadata.is_applied())
        {
            client.ensure_table(table_id, &table_name, &columns).await.map_err(EtlError::from)?;
            return Ok(());
        }

        // Hold the registry only long enough to obtain the stable gate for this
        // table; unrelated tables must not wait on its remote setup.
        let gate = {
            let mut gates = self.gates.lock().await;
            Arc::clone(gates.entry(table_id).or_insert_with(|| Arc::new(Semaphore::new(1))))
        };

        // The permit owns the complete Applying -> remote setup -> Applied
        // transition for this table.
        let _permit =
            gate.acquire_owned().await.expect("table initialization gates are never closed");

        // Re-read under the table gate because another copy partition may have
        // completed setup after the fast-path read.
        let snapshot_id = table_schema.inner().snapshot_id;
        let replication_mask = table_schema.replication_mask().clone();
        let applying_metadata = match store.get_destination_table_metadata(table_id).await? {
            None => {
                // Record ownership before creating remote state so a restart can
                // find and remove a partial setup.
                let metadata = DestinationTableMetadata::new_applying(
                    table_name.clone(),
                    snapshot_id,
                    replication_mask.clone(),
                );
                store.store_destination_table_metadata(table_id, metadata.clone()).await?;
                Some(metadata)
            }
            Some(metadata) if metadata.is_applied() => {
                // Another copy partition completed setup while this caller waited.
                None
            }
            Some(metadata) if metadata.previous_snapshot_id.is_none() => {
                // Resume a matching initial setup left by an earlier failure or
                // cancellation.
                if metadata.destination_table_id != table_name
                    || metadata.snapshot_id != snapshot_id
                    || metadata.replication_mask != replication_mask
                {
                    bail!(
                        ErrorKind::CorruptedTableSchema,
                        "Interrupted Snowflake table setup metadata does not match current schema",
                        format!(
                            "Table {table_id} has interrupted initial setup metadata for snapshot \
                             {}, but the current setup uses snapshot {snapshot_id}.",
                            metadata.snapshot_id
                        )
                    );
                }

                Some(metadata)
            }
            Some(_) => {
                // Applying metadata with a previous snapshot belongs to schema
                // evolution, which requires its separate recovery path.
                bail!(
                    ErrorKind::InvalidState,
                    "Snowflake table schema is still being applied",
                    format!(
                        "Table {table_id} has an interrupted schema change and cannot be prepared \
                         for streaming until that change is recovered."
                    )
                );
            }
        };

        // Remote setup is retry-safe and remains inside the per-table permit.
        client.ensure_table(table_id, &table_name, &columns).await.map_err(EtlError::from)?;

        // Publish completion only after both the table and channel exist.
        if let Some(metadata) = applying_metadata {
            store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;
        }

        Ok(())
    }
}

/// Execution context captured by Snowflake background event tasks.
///
/// Before resetting a table, [`Destination`] retains exclusive access to its
/// [`TaskSet`] while waiting for every admitted event task to finish. A task
/// that captured the complete destination could later access that same task
/// registry, causing the reset to wait for the task while the task waits for
/// the reset-held registry.
///
/// This type contains the state needed to execute writes but deliberately omits
/// [`TaskSet`], making that recursive registry access unavailable through the
/// task's execution context.
struct DestinationWriter<S, T, C> {
    /// Snowflake API client shared by foreground and event writes.
    client: Client<T, C>,
    /// Destination metadata store.
    store: S,
    /// Per-table initial-setup coordinator.
    table_initializer: TableInitializer,
}

impl<S: Clone, T: TokenProvider, C: StreamClient> Clone for DestinationWriter<S, T, C> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            store: self.store.clone(),
            table_initializer: self.table_initializer.clone(),
        }
    }
}

/// Postgres replication to Snowflake via Snowpipe Streaming.
///
/// Thin adapter between the ETL [`etl::destination::Destination`] trait and
/// [`Client`]. Translates replication events into client operations and manages
/// the state store bookkeeping.
pub struct Destination<S, T = AuthManager<HttpExchanger>, C = RestStreamClient<T>> {
    writer: DestinationWriter<S, T, C>,
    tasks: TaskSet,
}

impl<S: Clone, T: TokenProvider, C: StreamClient> Clone for Destination<S, T, C> {
    fn clone(&self) -> Self {
        Self { writer: self.writer.clone(), tasks: self.tasks.clone() }
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
        Self {
            writer: DestinationWriter { client, store, table_initializer: TableInitializer::new() },
            tasks: TaskSet::new(),
        }
    }

    /// Fetches the latest committed offset for this table's channel.
    pub async fn fetch_committed_offset(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<OffsetToken>> {
        self.writer.client.fetch_committed_offset(table_id).await.map_err(EtlError::from)
    }
}

impl<S, T, C> DestinationWriter<S, T, C>
where
    S: DestinationStore,
    T: TokenProvider + 'static,
    C: StreamClient,
{
    /// Ensures the Snowflake table and streaming channel exist for this table.
    async fn prepare_table_for_streaming(
        &self,
        table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        self.table_initializer.ensure(&self.client, &self.store, table_schema).await
    }

    /// Processes an event batch after its task was admitted into [`TaskSet`].
    ///
    /// This execution context intentionally cannot access the task registry. A
    /// table reset may retain that registry while waiting for this method to
    /// finish.
    async fn process_admitted_events(
        &self,
        events: Vec<Event>,
    ) -> EtlResult<DestinationWriteStatus> {
        let mut iter = events.into_iter().peekable();
        // Schema and truncate side effects must close the pending streaming window.
        let mut requires_durability_wait = false;

        while iter.peek().is_some() {
            let builders = self.accumulate_data_events(&mut iter).await?;
            self.flush_batches(builders).await?;
            requires_durability_wait |= self.apply_relation_events(&mut iter).await?;
            requires_durability_wait |= self.apply_truncate_events(&mut iter).await?;
        }

        if requires_durability_wait || self.client.pending_durability_limits_reached().await {
            self.client.wait_for_pending_durability().await.map_err(EtlError::from)?;
            Ok(DestinationWriteStatus::Durable)
        } else if self.client.pending_durability_is_empty().await {
            Ok(DestinationWriteStatus::Durable)
        } else {
            Ok(DestinationWriteStatus::Accepted)
        }
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
            self.client.send_streaming_batches(table_id, batches).await.map_err(EtlError::from)?;
        }
        Ok(())
    }

    async fn apply_relation_events(&self, iter: &mut EventIter) -> EtlResult<bool> {
        let mut applied_schema_change = false;
        while matches!(iter.peek(), Some(Event::Relation(_))) {
            if let Some(Event::Relation(rel)) = iter.next() {
                applied_schema_change |=
                    self.handle_relation_event(&rel.replicated_table_schema).await?;
            }
        }
        Ok(applied_schema_change)
    }

    async fn apply_truncate_events(&self, iter: &mut EventIter) -> EtlResult<bool> {
        // Collect and dedup tables to be truncated.
        let mut truncated: HashMap<TableId, ReplicatedTableSchema> = HashMap::new();
        while matches!(iter.peek(), Some(Event::Truncate(_))) {
            if let Some(Event::Truncate(t)) = iter.next() {
                for schema in t.truncated_tables {
                    truncated.insert(schema.id(), schema);
                }
            }
        }
        let had_truncates = !truncated.is_empty();

        // Truncate tables.
        for (_, schema) in truncated {
            let table_name = try_stringify_table_name(schema.name())?.to_uppercase();
            self.client.truncate_table(schema.id(), &table_name).await.map_err(EtlError::from)?;
        }

        Ok(had_truncates)
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
        if self.client.is_offset_committed(table_id, &offset).await.map_err(EtlError::from)? {
            return Ok(());
        }

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
        if self.client.is_offset_committed(table_id, &offset).await.map_err(EtlError::from)? {
            return Ok(());
        }

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
        self.ensure_column_cache(column_cache, table_id, &e.replicated_table_schema).await?;
        if self.client.is_offset_committed(table_id, &offset).await.map_err(EtlError::from)? {
            return Ok(());
        }

        match snowflake_delete_row(&e.replicated_table_schema, e.old_table_row)? {
            SnowflakeDeleteRow::Full(row) => {
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

    async fn handle_relation_event(&self, new_schema: &ReplicatedTableSchema) -> EtlResult<bool> {
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

        // Snowflake waits for all preceding row batches to become durable
        // before applying schema DDL. On replay, those rows are at or below the
        // channel's restored committed offset and will be skipped. The older
        // relation event is therefore already reflected in the destination,
        // diffing backwards could drop newer columns and delete their data.
        if new_snapshot_id < current_snapshot_id {
            info!(
                table_id = %table_id,
                received_snapshot_id = %new_snapshot_id,
                applied_snapshot_id = %current_snapshot_id,
                "skipping stale Snowflake relation event"
            );
            return Ok(false);
        }

        if current_snapshot_id == new_snapshot_id
            && current_replication_mask == new_replication_mask
        {
            info!(table_id = ?table_id, "schema unchanged, skipping relation event");
            return Ok(false);
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
        self.client.wait_for_pending_durability().await.map_err(EtlError::from)?;

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
        Ok(true)
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
        let table_name = try_stringify_table_name(replicated_table_schema.name())?.to_uppercase();
        let (task_guard, detached) = timeout(RESET_PREPARATION_TIMEOUT, async {
            // Acquire the task registry before any client lock. Event tasks have no
            // registry access, so they can finish while reset waits for them.
            let task_guard = self.tasks.drain().await?;
            let detached = self
                .writer
                .client
                .detach_table_for_copy(replicated_table_schema.id(), &table_name)
                .await
                .map_err(EtlError::from)?;
            Ok::<_, EtlError>((task_guard, detached))
        })
        .await
        .map_err(|error| {
            etl_error!(
                ErrorKind::DestinationTimeout,
                "Snowflake table reset preparation timed out",
                source: error
            )
        })??;

        // Keep event registration closed through the remote drop. This operation
        // stays outside the local drain timeout because cancelling remote SQL does
        // not prove whether Snowflake completed it.
        let result =
            self.writer.client.drop_detached_table_for_copy(detached).await.map_err(EtlError::from);

        // Publish the remote result before allowing another event task to run.
        async_result.send(result);
        drop(task_guard);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        let result: EtlResult<DestinationWriteStatus> = async {
            // Table must exist even for empty snapshots, CDC events may arrive later.
            self.writer.prepare_table_for_streaming(replicated_table_schema).await?;

            if table_rows.is_empty() {
                self.writer
                    .client
                    .wait_for_table_copy_durability(replicated_table_schema.id())
                    .await
                    .map_err(EtlError::from)?;
                return Ok(DestinationWriteStatus::Durable);
            }

            let table_id = replicated_table_schema.id();
            let columns: Vec<_> = replicated_table_schema.column_schemas().cloned().collect();

            // Build row batches. Snowflake has limits on max size of input, so we slice
            // into proper batches, when necessary.
            let zero = OffsetToken::zero();
            let mut builder = RowBatchBuilder::new();
            for row in &table_rows {
                builder
                    .push_row(
                        &columns,
                        row,
                        CdcMeta::new(CdcOperation::Insert, zero.as_ref()),
                        &zero,
                    )
                    .map_err(EtlError::from)?;
            }

            let batches = builder.finish().map_err(EtlError::from)?;
            self.writer
                .client
                .send_table_copy_batches(table_id, batches)
                .await
                .map_err(EtlError::from)?;

            Ok(DestinationWriteStatus::Accepted)
        }
        .await;

        async_result.send(result);
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let writer = self.writer.clone();
        self.tasks
            .spawn(async move {
                let result = async {
                    let status = writer.process_admitted_events(events).await?;
                    if durability == WriteEventsDurability::RequireDurable
                        && status == DestinationWriteStatus::Accepted
                    {
                        writer
                            .client
                            .wait_for_pending_durability()
                            .await
                            .map_err(EtlError::from)?;

                        Ok(DestinationWriteStatus::Durable)
                    } else {
                        Ok(status)
                    }
                }
                .await;
                async_result.send(result);
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
        event::RelationEvent,
        pipeline::PipelineId,
        schema::{IdentityMask, PgLsn, ReplicationMask, SnapshotId, TableName, TableSchema, Type},
        store::StateStore,
        test_utils::notifying_store::NotifyingStore,
    };

    use super::*;
    use crate::snowflake::{Config, Error, SqlClient};

    /// Token provider that fails if a no-network unit test reaches HTTP setup.
    struct UnusedTokenProvider;

    impl TokenProvider for UnusedTokenProvider {
        async fn get_token(&self) -> crate::snowflake::Result<String> {
            Err(Error::Auth("Unused test token provider.".to_owned()))
        }

        async fn invalidate_token(&self) {}
    }

    /// Builds a destination whose HTTP clients must remain unused.
    fn test_destination() -> (
        Destination<NotifyingStore, UnusedTokenProvider, RestStreamClient<UnusedTokenProvider>>,
        NotifyingStore,
    ) {
        let config = Config::new("example-account", "test-user", "test-db", "test-schema").unwrap();
        let auth = Arc::new(UnusedTokenProvider);
        let http = reqwest::Client::new();
        let sql_client =
            SqlClient::new(config.clone_without_credentials(), Arc::clone(&auth), http.clone());
        let stream_client =
            Arc::new(RestStreamClient::new(config.account_url().to_owned(), auth, http));
        let client = Client::with_clients(
            sql_client,
            stream_client,
            config.database().to_owned(),
            config.schema().to_owned(),
            PipelineId::from(1_u64),
        );
        let store = NotifyingStore::new();
        let destination = Destination::new(client, store.clone());

        (destination, store)
    }

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

    #[tokio::test]
    async fn initial_setup_failure_leaves_metadata_applying() {
        let (destination, store) = test_destination();
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(2),
            TableName::new("public".to_owned(), "reserved_column".to_owned()),
            vec![ColumnSchema::new("_cdc_operation".to_owned(), Type::TEXT, -1, 1, true)],
        ));
        let schema = ReplicatedTableSchema::all(table_schema);

        let error = destination.writer.prepare_table_for_streaming(&schema).await.unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ConfigError);
        let metadata = store
            .get_destination_table_metadata(schema.id())
            .await
            .unwrap()
            .expect("initial setup metadata should remain available for recovery");
        assert!(metadata.is_applying());
        assert_eq!(metadata.previous_snapshot_id, None);
        assert_eq!(metadata.snapshot_id, schema.inner().snapshot_id);
        assert_eq!(metadata.replication_mask, *schema.replication_mask());
        assert_eq!(
            metadata.destination_table_id,
            try_stringify_table_name(schema.name()).unwrap().to_uppercase()
        );
    }

    /// A stale relation event must not drive reverse Snowflake DDL.
    #[tokio::test]
    async fn stale_relation_event_is_skipped() {
        let (destination, store) = test_destination();
        let table_id = TableId::new(3);
        let table_name = TableName::new("public".to_owned(), "users".to_owned());
        let stale_table_schema = Arc::new(TableSchema::with_snapshot_id(
            table_id,
            table_name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
            SnapshotId::new(PgLsn::from(100_u64)),
        ));
        let stale_schema = ReplicatedTableSchema::all(stale_table_schema);
        let applied_table_schema = Arc::new(TableSchema::with_snapshot_id(
            table_id,
            table_name,
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
            ],
            SnapshotId::new(PgLsn::from(200_u64)),
        ));
        let applied_schema = ReplicatedTableSchema::all(applied_table_schema);
        let metadata = DestinationTableMetadata::new_applied(
            "PUBLIC_USERS".to_owned(),
            applied_schema.inner().snapshot_id,
            applied_schema.replication_mask().clone(),
        );
        store.store_destination_table_metadata(table_id, metadata.clone()).await.unwrap();

        let status = destination
            .writer
            .process_admitted_events(vec![Event::Relation(RelationEvent {
                start_lsn: PgLsn::from(100_u64),
                commit_lsn: PgLsn::from(100_u64),
                tx_ordinal: 0,
                replicated_table_schema: stale_schema,
            })])
            .await
            .expect("stale relation event should be skipped");

        assert_eq!(status, DestinationWriteStatus::Durable);
        assert_eq!(store.get_destination_table_metadata(table_id).await.unwrap(), Some(metadata));
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
