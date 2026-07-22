//! Postgres destination implementation.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use etl::{
    bail,
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    destination::{
        Destination, DestinationTableMetadata, DestinationTableSchemaStatus,
        DestinationWriteStatus, DropTableForCopyResult, WriteEventsDurability, WriteEventsResult,
        WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    event::Event,
    schema::{ColumnSchema, IdentityType, ReplicatedTableSchema, SchemaDiff, TableId, TableName},
    store::{SchemaStore, StateStore},
};
use etl_config::shared::PgConnectionConfig;
use parking_lot::{Mutex, RwLock};
use tracing::{debug, info, warn};

use crate::postgres::{
    client::PostgresClient,
    encoding::{cells_to_postgres_values, values_as_tosql_params},
    schema::{
        create_schema_sql, create_table_sql, delete_by_pk_sql, drop_table_sql,
        ensure_has_primary_key, schema_diff_statements, truncate_table_sql, upsert_sql,
    },
};

/// Pending row operation for streaming CDC.
enum PendingOp {
    Upsert(Vec<Cell>),
    Delete(Vec<Cell>),
}

/// CDC-capable Postgres destination that replicates tables with UPSERT
/// semantics.
#[derive(Clone)]
pub struct PostgresDestination<S> {
    client: PostgresClient,
    store: Arc<S>,
    destination_schema: Option<String>,
    /// Table ids that have been ensured in this process.
    ensured_tables: Arc<RwLock<HashSet<TableId>>>,
    /// Per-`table_id` locks serializing first-time table creation.
    create_locks: Arc<Mutex<HashMap<TableId, Arc<tokio::sync::Mutex<()>>>>>,
}

impl<S> PostgresDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new Postgres destination.
    ///
    /// When `destination_schema` is `Some`, all tables are placed in that
    /// schema while preserving source table names. When `None`, source
    /// `schema.table` names are preserved.
    pub fn new(
        pg_connection: PgConnectionConfig,
        destination_schema: Option<String>,
        store: S,
    ) -> Self {
        Self {
            client: PostgresClient::new(pg_connection),
            store: Arc::new(store),
            destination_schema,
            ensured_tables: Arc::new(RwLock::new(HashSet::new())),
            create_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resolves the destination [`TableName`] for a replicated source table.
    fn destination_table_name(&self, schema: &ReplicatedTableSchema) -> TableName {
        match &self.destination_schema {
            Some(override_schema) => {
                TableName::new(override_schema.clone(), schema.name().name.clone())
            }
            None => schema.name().clone(),
        }
    }

    /// Ensures the destination schema and table exist, recovering interrupted
    /// DDL.
    async fn ensure_table_exists(&self, schema: &ReplicatedTableSchema) -> EtlResult<TableName> {
        ensure_has_primary_key(schema)?;
        let table_id = schema.id();
        let destination_table = self.destination_table_name(schema);

        if self.ensured_tables.read().contains(&table_id) {
            return Ok(destination_table);
        }

        let table_lock = {
            let mut guard = self.create_locks.lock();
            Arc::clone(guard.entry(table_id).or_default())
        };
        let _create_guard = table_lock.lock().await;

        if self.ensured_tables.read().contains(&table_id) {
            return Ok(destination_table);
        }

        let destination_table_id = destination_table.as_quoted_identifier();

        match self.store.get_destination_table_metadata(table_id).await? {
            None => {
                self.create_table_with_metadata(
                    table_id,
                    &destination_table,
                    &destination_table_id,
                    schema,
                    schema.inner().snapshot_id,
                    schema.replication_mask().clone(),
                )
                .await?;
            }
            Some(metadata) => {
                if metadata.is_applying() {
                    self.recover_applying_metadata(table_id, &destination_table, schema, metadata)
                        .await?;
                }
            }
        }

        self.ensured_tables.write().insert(table_id);
        Ok(destination_table)
    }

    async fn create_table_with_metadata(
        &self,
        table_id: TableId,
        destination_table: &TableName,
        destination_table_id: &str,
        schema: &ReplicatedTableSchema,
        snapshot_id: etl::schema::SnapshotId,
        replication_mask: etl::schema::ReplicationMask,
    ) -> EtlResult<()> {
        let metadata = DestinationTableMetadata::new_applying(
            destination_table_id.to_owned(),
            snapshot_id,
            replication_mask,
        );
        self.store.store_destination_table_metadata(table_id, metadata.clone()).await?;
        self.issue_create_table(destination_table, schema).await?;
        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;
        Ok(())
    }

    async fn issue_create_table(
        &self,
        destination_table: &TableName,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        self.client
            .execute_simple(
                &create_schema_sql(&destination_table.schema),
                "Postgres create schema failed",
            )
            .await?;
        let create_sql = create_table_sql(destination_table, schema)?;
        self.client.execute_simple(&create_sql, "Postgres create table failed").await
    }

    async fn recover_applying_metadata(
        &self,
        table_id: TableId,
        destination_table: &TableName,
        schema: &ReplicatedTableSchema,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        warn!("table {} has Applying metadata, recovering interrupted operation", table_id);

        match metadata.previous_snapshot_id {
            Some(prev_snapshot_id) => {
                let old_table_schema =
                    self.store.get_table_schema(&table_id, prev_snapshot_id).await?.ok_or_else(
                        || {
                            etl_error!(
                                ErrorKind::InvalidState,
                                "Stored schema snapshot missing for Postgres schema recovery",
                                format!(
                                    "Table {} needs stored schema snapshot {} to recover the \
                                     destination table, but it was not found.",
                                    table_id, prev_snapshot_id
                                )
                            )
                        },
                    )?;
                let old_schema = ReplicatedTableSchema::from_mask(
                    old_table_schema,
                    metadata.replication_mask.clone(),
                );
                let diff = old_schema.diff(schema);
                self.apply_schema_diff(destination_table, &diff).await?;
            }
            None => {
                self.issue_create_table(destination_table, schema).await?;
            }
        }

        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;
        Ok(())
    }

    async fn handle_relation_event(&self, new_schema: &ReplicatedTableSchema) -> EtlResult<()> {
        ensure_has_primary_key(new_schema)?;

        let table_id = new_schema.id();
        let new_snapshot_id = new_schema.inner().snapshot_id;
        let new_replication_mask = new_schema.replication_mask().clone();

        let metadata =
            self.store.get_applied_destination_table_metadata(table_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::CorruptedTableSchema,
                        "Destination metadata missing for Postgres schema change",
                        format!(
                            "Table {} received schema snapshot {}, but destination metadata from \
                             initial synchronization was not found.",
                            table_id, new_snapshot_id
                        )
                    )
                },
            )?;

        let current_snapshot_id = metadata.snapshot_id;
        let current_replication_mask = metadata.replication_mask.clone();

        if current_snapshot_id == new_snapshot_id
            && current_replication_mask == new_replication_mask
        {
            info!("schema for table {} unchanged (snapshot_id: {})", table_id, new_snapshot_id);
            return Ok(());
        }

        info!(
            "schema change detected for table {}: snapshot_id {} -> {}",
            table_id, current_snapshot_id, new_snapshot_id
        );

        // Serialize with [`Self::ensure_table_exists`] so concurrent first-write
        // CREATE and Relation DDL cannot race on the same destination table.
        let table_lock = {
            let mut guard = self.create_locks.lock();
            Arc::clone(guard.entry(table_id).or_default())
        };
        let _create_guard = table_lock.lock().await;

        let current_table_schema =
            self.store.get_table_schema(&table_id, current_snapshot_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "Stored schema snapshot missing for Postgres schema change",
                        format!(
                            "Table {} needs stored schema snapshot {} to compare with incoming \
                             snapshot {}, but it was not found.",
                            table_id, current_snapshot_id, new_snapshot_id
                        )
                    )
                },
            )?;

        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );
        let destination_table = self.destination_table_name(new_schema);
        let destination_table_id = destination_table.as_quoted_identifier();

        let updated_metadata = DestinationTableMetadata::new_applied(
            destination_table_id,
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
        self.apply_schema_diff(&destination_table, &diff).await?;

        self.store
            .store_destination_table_metadata(table_id, updated_metadata.to_applied())
            .await?;
        self.ensured_tables.write().insert(table_id);

        info!(
            "schema change completed for table {}: snapshot_id {} applied",
            table_id, new_snapshot_id
        );

        Ok(())
    }

    async fn apply_schema_diff(
        &self,
        destination_table: &TableName,
        diff: &SchemaDiff,
    ) -> EtlResult<()> {
        for sql in schema_diff_statements(destination_table, diff) {
            self.client.execute_simple(&sql, "Postgres alter table failed").await?;
        }
        Ok(())
    }

    async fn write_table_rows_inner(
        &self,
        schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let destination_table = self.ensure_table_exists(schema).await?;
        let sql = upsert_sql(&destination_table, schema)?;

        for table_row in table_rows {
            let values = cells_to_postgres_values(table_row.into_values())?;
            let params = values_as_tosql_params(&values);
            self.client.execute(&sql, &params, "Postgres upsert during table copy failed").await?;
        }

        Ok(())
    }

    async fn upsert_row(
        &self,
        destination_table: &TableName,
        schema: &ReplicatedTableSchema,
        cells: Vec<Cell>,
    ) -> EtlResult<()> {
        let sql = upsert_sql(destination_table, schema)?;
        let values = cells_to_postgres_values(cells)?;
        let params = values_as_tosql_params(&values);
        self.client.execute(&sql, &params, "Postgres upsert failed").await?;
        Ok(())
    }

    async fn delete_row(
        &self,
        destination_table: &TableName,
        schema: &ReplicatedTableSchema,
        pk_cells: Vec<Cell>,
    ) -> EtlResult<()> {
        let sql = delete_by_pk_sql(destination_table, schema)?;
        let values = cells_to_postgres_values(pk_cells)?;
        let params = values_as_tosql_params(&values);
        self.client.execute(&sql, &params, "Postgres delete failed").await?;
        Ok(())
    }

    async fn truncate_table_inner(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let destination_table = self.ensure_table_exists(schema).await?;
        self.client
            .execute_simple(&truncate_table_sql(&destination_table), "Postgres truncate failed")
            .await
    }

    async fn drop_table_for_copy_inner(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let destination_table = self.destination_table_name(schema);
        self.client
            .execute_simple(&drop_table_sql(&destination_table), "Postgres drop table failed")
            .await?;
        self.ensured_tables.write().remove(&schema.id());
        Ok(())
    }

    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut pending: HashMap<TableId, (ReplicatedTableSchema, Vec<PendingOp>)> =
                HashMap::new();

            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_) | Event::Relation(_)) {
                    break;
                }

                let event = event_iter.next().expect("peeked event must be present");
                match event {
                    Event::Insert(insert) => {
                        let table_id = insert.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (insert.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingOp::Upsert(insert.table_row.into_values()));
                    }
                    Event::Update(update) => {
                        let ops = postgres_update_ops(
                            &update.replicated_table_schema,
                            update.updated_table_row,
                            update.old_table_row,
                        )?;
                        let table_id = update.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (update.replicated_table_schema, Vec::new()));
                        entry.1.extend(ops);
                    }
                    Event::Delete(delete) => {
                        let pk_cells = postgres_delete_pk_cells(
                            &delete.replicated_table_schema,
                            delete.old_table_row,
                        )?;
                        let table_id = delete.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (delete.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingOp::Delete(pk_cells));
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            for (schema, ops) in pending.into_values() {
                let destination_table = self.ensure_table_exists(&schema).await?;
                for op in ops {
                    match op {
                        PendingOp::Upsert(cells) => {
                            self.upsert_row(&destination_table, &schema, cells).await?;
                        }
                        PendingOp::Delete(pk_cells) => {
                            self.delete_row(&destination_table, &schema, pk_cells).await?;
                        }
                    }
                }
            }

            while let Some(Event::Relation(_)) = event_iter.peek() {
                if let Some(Event::Relation(relation)) = event_iter.next() {
                    self.handle_relation_event(&relation.replicated_table_schema).await?;
                }
            }

            let mut truncate_schemas: HashMap<TableId, ReplicatedTableSchema> = HashMap::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for schema in truncate_event.truncated_tables {
                        truncate_schemas.entry(schema.id()).or_insert(schema);
                    }
                }
            }

            for schema in truncate_schemas.values() {
                self.truncate_table_inner(schema).await?;
            }
        }

        Ok(())
    }
}

impl<S> Destination for PostgresDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        etl_config::shared::DestinationKind::Postgres.as_str()
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        let result = self.drop_table_for_copy_inner(replicated_table_schema).await;
        async_result.send(result);
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        let result = self.write_table_rows_inner(replicated_table_schema, table_rows).await;
        async_result.send(result.map(|_| DestinationWriteStatus::Durable));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        let result = self.write_events_inner(events).await;
        async_result.send(result.map(|_| DestinationWriteStatus::Durable));
        Ok(())
    }
}

/// Builds pending delete/upsert ops for a Postgres update event.
fn postgres_update_ops(
    replicated_table_schema: &ReplicatedTableSchema,
    updated_table_row: UpdatedTableRow,
    old_table_row: Option<OldTableRow>,
) -> EtlResult<Vec<PendingOp>> {
    let new_table_row = postgres_update_row(replicated_table_schema, updated_table_row)?;
    let primary_key_changed = match old_table_row.as_ref() {
        // PostgreSQL omits the old-side image only when the publisher
        // determined it was unnecessary. For primary-key identity, that means
        // the destination key did not change. `FULL` updates are expected to
        // carry an old row from pgoutput.
        Some(old_table_row) => {
            postgres_primary_key_changed(replicated_table_schema, old_table_row, &new_table_row)?
        }
        None => {
            ensure_postgres_update_without_old_row_can_skip_delete(replicated_table_schema)?;
            false
        }
    };

    let mut ops = Vec::with_capacity(1 + usize::from(primary_key_changed));
    if primary_key_changed {
        let Some(old_table_row) = old_table_row else {
            bail!(
                ErrorKind::InvalidState,
                "Postgres primary key change is missing old row",
                format!(
                    "Table '{}' primary key change was detected without an old row image",
                    replicated_table_schema.name()
                )
            );
        };

        ops.push(PendingOp::Delete(postgres_delete_pk_cells(
            replicated_table_schema,
            Some(old_table_row),
        )?));
    }

    ops.push(PendingOp::Upsert(new_table_row.into_values()));
    Ok(ops)
}

/// Returns the full new row required for a Postgres update upsert.
fn postgres_update_row(
    replicated_table_schema: &ReplicatedTableSchema,
    updated_table_row: UpdatedTableRow,
) -> EtlResult<TableRow> {
    match updated_table_row {
        UpdatedTableRow::Full(row) => Ok(row),
        UpdatedTableRow::Partial(_) => Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Postgres update requires a full new row image",
            format!(
                        "Table '{}' emitted a partial update row. Postgres UPSERT does not \
                         preserve                  omitted columns.",
                        replicated_table_schema.name()
                    )
        )),
    }
}

/// Verifies that a Postgres update without an old row cannot have changed the
/// destination primary key.
fn ensure_postgres_update_without_old_row_can_skip_delete(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if matches!(replicated_table_schema.identity_type(), IdentityType::PrimaryKey) {
        Ok(())
    } else {
        Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Postgres update requires old primary-key values",
            format!(
                    "Table '{}' emitted an update without an old row image for replica identity \
                     {:?}.                  Postgres can only skip the generated delete when the \
                     source replica identity                  matches the primary key.",
                    replicated_table_schema.name(),
                    replicated_table_schema.identity_type()
                )
        ))
    }
}

/// Verifies that a key-only row image carries source primary-key values.
fn ensure_postgres_key_image_matches_primary_key(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if matches!(replicated_table_schema.identity_type(), IdentityType::PrimaryKey) {
        Ok(())
    } else {
        Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Postgres key image does not match the source primary key",
            format!(
                    "Table '{}' emitted a key image for replica identity {:?}, but Postgres rows \
                     are                  keyed by the source primary key",
                    replicated_table_schema.name(),
                    replicated_table_schema.identity_type()
                )
        ))
    }
}

/// Returns whether an update changed the destination primary key.
fn postgres_primary_key_changed(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: &OldTableRow,
    new_table_row: &TableRow,
) -> EtlResult<bool> {
    let column_count = replicated_table_schema.column_schemas().len();
    if new_table_row.values().len() != column_count {
        bail!(
            ErrorKind::InvalidState,
            "Postgres full row image does not match the replicated schema",
            format!(
                "Expected {} values for table '{}', got {}",
                column_count,
                replicated_table_schema.name(),
                new_table_row.values().len()
            )
        );
    }

    match old_table_row {
        OldTableRow::Full(row) => {
            if row.values().len() != column_count {
                bail!(
                    ErrorKind::InvalidState,
                    "Postgres full row image does not match the replicated schema",
                    format!(
                        "Expected {} values for table '{}', got {}",
                        column_count,
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                );
            }

            Ok(replicated_table_schema
                .column_schemas()
                .zip(row.values())
                .zip(new_table_row.values())
                .any(|((column_schema, old_value), new_value)| {
                    column_schema.primary_key() && old_value != new_value
                }))
        }
        OldTableRow::Key(row) => {
            let primary_key_column_count =
                replicated_table_schema.primary_key_column_schemas().len();
            let old_key_values = row.values();
            if old_key_values.len() != primary_key_column_count {
                bail!(
                    ErrorKind::InvalidState,
                    "Postgres key image does not match the source primary key",
                    format!(
                        "Expected {} key values for table '{}', got {}",
                        primary_key_column_count,
                        replicated_table_schema.name(),
                        old_key_values.len()
                    )
                );
            }

            ensure_postgres_key_image_matches_primary_key(replicated_table_schema)?;

            let mut new_primary_key_values = replicated_table_schema
                .column_schemas()
                .zip(new_table_row.values())
                .filter(|(column_schema, _)| column_schema.primary_key())
                .map(|(_, value)| value);

            for old_value in old_key_values {
                let Some(new_value) = new_primary_key_values.next() else {
                    bail!(
                        ErrorKind::InvalidState,
                        "Postgres primary key schema mismatch",
                        format!(
                            "Table '{}' did not expose enough primary key columns",
                            replicated_table_schema.name()
                        )
                    );
                };

                if old_value != new_value {
                    return Ok(true);
                }
            }

            Ok(false)
        }
    }
}

/// Extracts primary-key cells for a delete statement.
fn postgres_delete_pk_cells(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: Option<OldTableRow>,
) -> EtlResult<Vec<Cell>> {
    ensure_has_primary_key(replicated_table_schema)?;

    let old_table_row = old_table_row.ok_or_else(|| {
        etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Postgres delete requires an old row image",
            format!(
                "Table '{}' emitted a delete without an old row image.",
                replicated_table_schema.name()
            )
        )
    })?;

    match old_table_row {
        OldTableRow::Full(row) => pk_cells_from_full_row(replicated_table_schema, row),
        OldTableRow::Key(row) => pk_cells_from_key_row(replicated_table_schema, row),
    }
}

fn pk_cells_from_full_row(schema: &ReplicatedTableSchema, row: TableRow) -> EtlResult<Vec<Cell>> {
    let values = row.into_values();
    let pk_columns: Vec<&ColumnSchema> = schema.primary_key_column_schemas().collect();
    let replicated_columns: Vec<&ColumnSchema> = schema.column_schemas().collect();
    let mut pk_cells = Vec::with_capacity(pk_columns.len());
    for pk_column in pk_columns {
        let index = replicated_columns
            .iter()
            .position(|column| column.ordinal_position == pk_column.ordinal_position)
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Postgres delete could not locate primary-key column",
                    format!(
                        "Primary-key column '{}' missing from replicated schema for table '{}'",
                        pk_column.name,
                        schema.name()
                    )
                )
            })?;
        pk_cells.push(values.get(index).cloned().unwrap_or(Cell::Null));
    }

    Ok(pk_cells)
}

fn pk_cells_from_key_row(schema: &ReplicatedTableSchema, row: TableRow) -> EtlResult<Vec<Cell>> {
    match schema.identity_type() {
        IdentityType::PrimaryKey | IdentityType::Full => {}
        identity_type => {
            return Err(etl_error!(
                ErrorKind::SourceReplicaIdentityError,
                "Postgres delete requires primary-key or full replica identity",
                format!(
                    "Table '{}' uses replica identity {identity_type}. Configure REPLICA IDENTITY \
                     DEFAULT or FULL so deletes can target the destination primary key.",
                    schema.name()
                )
            ));
        }
    }

    let pk_columns: Vec<&ColumnSchema> = schema.primary_key_column_schemas().collect();
    let key_values = row.into_values();
    if key_values.len() != pk_columns.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "Postgres key image does not match the source primary key",
            format!(
                "Expected {} key values for table '{}', got {}",
                pk_columns.len(),
                schema.name(),
                key_values.len()
            )
        ));
    }

    Ok(key_values)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::{
        data::{Cell, PartialTableRow, TableRow, UpdatedTableRow},
        error::ErrorKind,
        schema::{
            ColumnSchema, IdentityMask, IdentityType, ReplicatedTableSchema, TableId, TableName,
            TableSchema, Type,
        },
    };

    use super::postgres_update_row;

    fn replicated_schema(identity_type: IdentityType) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let replication_mask = etl::schema::ReplicationMask::all(&table_schema);
        let identity_mask = match identity_type {
            IdentityType::Full => IdentityMask::from_bytes(vec![1, 1]),
            IdentityType::PrimaryKey => IdentityMask::from_bytes(vec![1, 0]),
            IdentityType::AlternativeKey => IdentityMask::from_bytes(vec![0, 1]),
            IdentityType::Missing => IdentityMask::from_bytes(vec![0, 0]),
        };

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    #[test]
    fn postgres_update_row_rejects_partial_rows() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let partial_row = PartialTableRow::new(2, TableRow::new(vec![Cell::I32(1)]), vec![1]);

        let error =
            postgres_update_row(&replicated_table_schema, UpdatedTableRow::Partial(partial_row))
                .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert!(error.to_string().contains("emitted a partial update row"));
    }
}
