use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use etl::{
    concurrency::TaskSet,
    destination::{
        Destination,
        async_result::{DropTableForCopyResult, WriteEventsResult, WriteTableRowsResult},
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    state::destination_table_metadata::DestinationTableMetadata,
    store::SharedStateStore,
    types::{
        ArrayCell, Cell, ColumnSchema, Event, IdentityType, OldTableRow, ReplicatedTableSchema,
        TableId, TableName, TableRow, Type, UpdatedTableRow,
    },
};
use tokio::{sync::Mutex, task::JoinSet};
use tracing::{debug, warn};

#[cfg(feature = "egress")]
use crate::egress::{PROCESSING_TYPE_STREAMING, PROCESSING_TYPE_TABLE_COPY, log_processed_bytes};
use crate::{
    iceberg::{IcebergClient, error::iceberg_error_to_etl_error},
    table_name::try_stringify_table_name,
};

/// Type alias for Iceberg table names.
type IcebergTableName = String;

/// Converts a source table name to an Iceberg table name.
///
/// Creates a standardized naming convention for Iceberg tables. When all
/// source schemas share one Iceberg namespace, the source schema is encoded
/// into the table name to avoid collisions.
pub fn table_name_to_iceberg_table_name(
    table_name: &TableName,
    single_destination_namespace: bool,
) -> EtlResult<IcebergTableName> {
    if single_destination_namespace {
        return try_stringify_table_name(table_name);
    }

    Ok(table_name.name.clone())
}

/// Namespace in the destination where the tables will be copied.
#[derive(Debug)]
pub enum DestinationNamespace {
    /// A single namespace for all tables in the source.
    Single(String),
    /// One namespace for each schema in the source.
    OnePerSchema,
}

impl DestinationNamespace {
    /// Returns the configured namespace or falls back to the table namespace.
    fn get_or<'a>(&'a self, table_namespace: &'a str) -> &'a str {
        match self {
            DestinationNamespace::Single(ns) => ns,
            DestinationNamespace::OnePerSchema => table_namespace,
        }
    }

    /// Returns whether all tables are written into one namespace.
    pub fn is_single(&self) -> bool {
        match self {
            DestinationNamespace::Single(_) => true,
            DestinationNamespace::OnePerSchema => false,
        }
    }
}

/// Internal state for the Iceberg destination.
///
/// Contains mutable state that requires synchronization across concurrent
/// operations. Wrapped in a mutex to ensure thread-safe access while keeping
/// the main destination struct cloneable.
#[derive(Debug)]
struct Inner {
    /// Cache of source tables we already created/verified in the destination.
    ///
    /// Prevents redundant table existence checks and creation attempts.
    /// Tables are keyed by source [`TableId`] so same-named tables in different
    /// destination namespaces are checked independently.
    created_tables: HashSet<TableId>,
    /// Cache of namespaces we already created/verified in the destination.
    ///
    /// Prevents redundant namespace existence checks and creation attempts.
    /// Namespaces are added to this cache after successful creation or
    /// verification.
    created_namespaces: HashSet<String>,
    /// Namespace where the tables will be replicated. Depending on the variant
    /// either all tables will go in one namespace or there will be one
    /// namespace per source schema.
    namespace: DestinationNamespace,
}

/// An Iceberg destination that implements the ETL [`Destination`] trait.
///
/// Provides Postgres-to-Iceberg data pipeline functionality including table
/// copy, streaming inserts, updates, deletes, and truncates.
///
/// Designed for high concurrency with minimal locking:
/// - Client is accessible without locks.
/// - Only caches require synchronization.
/// - Multiple write operations can execute concurrently.
#[derive(Debug, Clone)]
pub struct IcebergDestination<S> {
    client: IcebergClient,
    store: S,
    inner: Arc<Mutex<Inner>>,
    tasks: TaskSet,
}

impl<S> IcebergDestination<S>
where
    S: SharedStateStore,
{
    /// Creates a new Iceberg destination instance.
    ///
    /// Initializes the destination with an Iceberg client, target namespace,
    /// and state/schema store. The destination starts with an empty table
    /// creation cache and is ready to handle streaming operations.
    pub fn new(
        client: IcebergClient,
        namespace: DestinationNamespace,
        store: S,
    ) -> IcebergDestination<S> {
        IcebergDestination {
            client,
            store,
            inner: Arc::new(Mutex::new(Inner {
                created_tables: HashSet::new(),
                created_namespaces: HashSet::new(),
                namespace,
            })),
            tasks: TaskSet::new(),
        }
    }

    /// Truncates an Iceberg table by dropping and recreating it.
    ///
    /// Removes all data from the target table by dropping the existing Iceberg
    /// table and creating a fresh empty table with the same schema. Updates
    /// the internal table creation cache to reflect the new table state.
    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        let table_id = replicated_table_schema.id();

        // Check if metadata exists for this table.
        //
        // If no metadata exists, it means the table was never created in Iceberg (e.g.,
        // due to errors during copy). In this case, we skip the truncate since
        // there's nothing to truncate.
        let Some(metadata) = self.store.get_applied_destination_table_metadata(table_id).await?
        else {
            warn!(
                %table_id,
                "skipping truncate because no metadata exists (table was likely never created)",
            );
            return Ok(());
        };
        let iceberg_table_name = metadata.destination_table_id;

        let table_namespace = schema_to_namespace(&replicated_table_schema.name().schema);
        let namespace = inner.namespace.get_or(&table_namespace).to_owned();

        self.client
            .drop_table_if_exists(&namespace, iceberg_table_name.clone())
            .await
            .map_err(iceberg_error_to_etl_error)?;
        inner.created_tables.remove(&table_id);

        // We recreate the table with the same schema.
        self.prepare_table_for_streaming(&mut inner, replicated_table_schema).await?;

        Ok(())
    }

    /// Drops an Iceberg table before restarting a table copy.
    async fn drop_table_for_copy_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();
        let table_name = replicated_table_schema.name();
        let table_namespace = schema_to_namespace(&table_name.schema);
        let (default_table_name, namespace) = {
            let inner = self.inner.lock().await;
            let default_table_name =
                table_name_to_iceberg_table_name(table_name, inner.namespace.is_single())?;
            let namespace = inner.namespace.get_or(&table_namespace).to_owned();

            (default_table_name, namespace)
        };
        let iceberg_table_name =
            if let Some(metadata) = self.store.get_destination_table_metadata(table_id).await? {
                metadata.destination_table_id
            } else {
                default_table_name
            };

        self.client
            .drop_table_if_exists(&namespace, iceberg_table_name.clone())
            .await
            .map_err(iceberg_error_to_etl_error)?;

        let mut inner = self.inner.lock().await;
        inner.created_tables.remove(&table_id);

        Ok(())
    }

    /// Writes table-copy rows to the Iceberg destination.
    ///
    /// Prepares the target table for streaming and appends rows to the Iceberg
    /// table without CDC metadata columns.
    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let (namespace, iceberg_table_name) = {
            // We hold the lock for the entire preparation to avoid race conditions since
            // the consistency of this code path is critical.
            let mut inner = self.inner.lock().await;
            self.prepare_table_for_streaming(&mut inner, replicated_table_schema).await?
        };

        if !table_rows.is_empty() {
            #[allow(unused_variables)]
            let bytes_sent =
                self.client.insert_rows(namespace, iceberg_table_name, table_rows).await?;

            #[cfg(feature = "egress")]
            log_processed_bytes(Self::name(), PROCESSING_TYPE_TABLE_COPY, bytes_sent, 0);
        }

        Ok(())
    }

    /// Processes and writes CDC events to Iceberg tables.
    ///
    /// Handles a stream of CDC events by batching non-truncate events by table
    /// ID and processing them concurrently. Truncate events are processed
    /// separately and deduplicated for efficiency. Inserts and updates append
    /// data files, while updates and deletes add equality-delete files over the
    /// source primary key.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut events_iter = events.into_iter().peekable();

        while events_iter.peek().is_some() {
            // Maps table ID to (schema, delta); schema is the first one seen for that
            // table. Once schema change support is implemented, we will
            // re-implement this.
            let mut table_id_to_data: HashMap<TableId, (ReplicatedTableSchema, TableDelta)> =
                HashMap::new();

            // Process events until we hit a truncate event or run out of events.
            while let Some(event) = events_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let Some(event) = events_iter.next() else {
                    break;
                };
                match event {
                    Event::Insert(insert) => {
                        validate_iceberg_table_identity(&insert.replicated_table_schema)?;
                        let table_id = insert.replicated_table_schema.id();
                        let key_indices = primary_key_indices(&insert.replicated_table_schema);
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (insert.replicated_table_schema.clone(), TableDelta::default())
                        });
                        entry.1.insert(
                            insert.table_row,
                            &insert.replicated_table_schema,
                            &key_indices,
                        );
                    }
                    Event::Update(update) => {
                        validate_iceberg_table_identity(&update.replicated_table_schema)?;
                        let key_indices = primary_key_indices(&update.replicated_table_schema);
                        let updated_table_row = match update.updated_table_row {
                            UpdatedTableRow::Full(row) => row,
                            UpdatedTableRow::Partial(_) => {
                                return Err(etl_error!(
                                    ErrorKind::InvalidState,
                                    "Iceberg update requires a full new row image",
                                    format!(
                                        "Table '{}' emitted a partial update row. Configure \
                                         replication so all updated values are available before \
                                         writing to Iceberg.",
                                        update.replicated_table_schema.name()
                                    )
                                ));
                            }
                        };
                        let delete_table_row = match update.old_table_row {
                            Some(old_table_row) => old_table_row_to_delete_row(
                                old_table_row,
                                &update.replicated_table_schema,
                                &key_indices,
                            )?,
                            None => equality_delete_row_from_full_row(
                                &updated_table_row,
                                &update.replicated_table_schema,
                                &key_indices,
                            ),
                        };

                        let table_id = update.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (update.replicated_table_schema.clone(), TableDelta::default())
                        });
                        entry.1.update(delete_table_row, updated_table_row, &key_indices);
                    }
                    Event::Delete(delete) => {
                        validate_iceberg_table_identity(&delete.replicated_table_schema)?;
                        let key_indices = primary_key_indices(&delete.replicated_table_schema);
                        let Some(old_table_row) = delete.old_table_row else {
                            return Err(etl_error!(
                                ErrorKind::InvalidState,
                                "Iceberg delete requires an old row image",
                                format!(
                                    "Table '{}' emitted a delete without an old row image even \
                                     though Iceberg requires primary-key row identity.",
                                    delete.replicated_table_schema.name()
                                )
                            ));
                        };
                        let delete_table_row = old_table_row_to_delete_row(
                            old_table_row,
                            &delete.replicated_table_schema,
                            &key_indices,
                        )?;

                        let table_id = delete.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (delete.replicated_table_schema.clone(), TableDelta::default())
                        });
                        entry.1.delete(delete_table_row, &key_indices);
                    }
                    Event::Relation(relation) => {
                        validate_iceberg_table_identity(&relation.replicated_table_schema)?;

                        // Check if schema has changed - if so, error since Iceberg doesn't
                        // support schema changes yet.
                        let table_id = relation.replicated_table_schema.id();
                        let new_snapshot_id = relation.replicated_table_schema.inner().snapshot_id;
                        let new_replication_mask =
                            relation.replicated_table_schema.replication_mask();

                        if let Some(metadata) =
                            self.store.get_applied_destination_table_metadata(table_id).await?
                            && (metadata.snapshot_id != new_snapshot_id
                                || &metadata.replication_mask != new_replication_mask)
                        {
                            return Err(etl_error!(
                                ErrorKind::CorruptedTableSchema,
                                "Schema changes not supported",
                                format!(
                                    "Iceberg destination does not support schema changes. Table \
                                     {} schema changed from snapshot_id {} to {}.",
                                    table_id, metadata.snapshot_id, new_snapshot_id
                                )
                            ));
                        }
                    }
                    event => {
                        // Every other event type is currently not supported.
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            // Process accumulated events for each table.
            if !table_id_to_data.is_empty() {
                let mut join_set = JoinSet::new();

                for (_, (replicated_table_schema, table_delta)) in table_id_to_data {
                    let (namespace, iceberg_table_name) = {
                        // We hold the lock for the entire preparation to avoid race conditions
                        // since the consistency of this code path is
                        // critical.
                        let mut inner = self.inner.lock().await;
                        self.prepare_table_for_streaming(&mut inner, &replicated_table_schema)
                            .await?
                    };

                    let client = self.client.clone();

                    let (data_rows, delete_rows) = table_delta.into_parts();
                    join_set.spawn(async move {
                        client
                            .apply_row_delta(namespace, iceberg_table_name, data_rows, delete_rows)
                            .await
                    });
                }

                #[cfg_attr(not(feature = "egress"), allow(unused_mut, unused_variables))]
                let mut bytes_sent = 0;
                #[cfg_attr(not(feature = "egress"), allow(unused_assignments))]
                while let Some(insert_result) = join_set.join_next().await {
                    bytes_sent += insert_result
                        .map_err(|_| etl_error!(ErrorKind::Unknown, "Failed to join future"))??;
                }

                #[cfg(feature = "egress")]
                log_processed_bytes(Self::name(), PROCESSING_TYPE_STREAMING, bytes_sent, 0);
            }

            // Collect and deduplicate schemas from all truncate events.
            //
            // This is done as an optimization since if we have multiple tables being
            // truncated in a row without applying other events in the
            // meanwhile, it doesn't make any sense to create new empty tables
            // for each of them.
            let mut truncate_schemas: HashMap<TableId, ReplicatedTableSchema> = HashMap::new();

            while let Some(Event::Truncate(_)) = events_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = events_iter.next() {
                    for schema in truncate_event.truncated_tables {
                        truncate_schemas.insert(schema.id(), schema);
                    }
                }
            }

            for (_, schema) in truncate_schemas {
                self.truncate_table(&schema).await?;
            }
        }

        Ok(())
    }

    /// Prepares a table for Iceberg writes with schema-aware table creation.
    ///
    /// Ensures the corresponding Iceberg table exists in the namespace. Also
    /// validates that the source table exposes primary-key row identity, which
    /// is required for equality-delete based CDC replay. Uses caching to avoid
    /// redundant table creation checks and holds a lock during the entire
    /// preparation to prevent race conditions.
    ///
    /// Follows the applying -> applied pattern for crash recovery:
    /// 1. Store metadata with `Applying` status before creating the table
    /// 2. Create the table
    /// 3. Update metadata to `Applied` after successful creation
    async fn prepare_table_for_streaming(
        &self,
        inner: &mut Inner,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<(String, IcebergTableName)> {
        validate_iceberg_table_identity(replicated_table_schema)?;

        let table_id = replicated_table_schema.id();
        let table_name = replicated_table_schema.name();
        let snapshot_id = replicated_table_schema.inner().snapshot_id;
        let replication_mask = replicated_table_schema.replication_mask().clone();
        let column_schemas: Vec<_> = replicated_table_schema.column_schemas().cloned().collect();

        let iceberg_table_name =
            table_name_to_iceberg_table_name(table_name, inner.namespace.is_single())?;
        let iceberg_table_name = if let Some(metadata) =
            self.store.get_applied_destination_table_metadata(table_id).await?
        {
            metadata.destination_table_id
        } else {
            iceberg_table_name
        };

        // We prepare the namespace.
        let namespace = schema_to_namespace(&table_name.schema);
        let namespace = inner.namespace.get_or(&namespace).to_owned();
        let namespace = self.create_namespace_if_missing(inner, namespace).await?;

        // If the table is already in the cache, we skip the creation. This works
        // assuming that etl is the only system managing the underlying tables.
        if inner.created_tables.contains(&table_id) {
            debug!(
                "iceberg table {iceberg_table_name} found in creation cache, skipping existence \
                 check"
            );

            return Ok((namespace, iceberg_table_name));
        }

        // Create metadata with applying status before creating the table.
        let metadata = DestinationTableMetadata::new_applying(
            iceberg_table_name.clone(),
            snapshot_id,
            replication_mask,
        );
        self.store.store_destination_table_metadata(table_id, metadata.clone()).await?;

        self.client
            .create_table_if_missing(&namespace, iceberg_table_name.clone(), &column_schemas)
            .await
            .map_err(iceberg_error_to_etl_error)?;

        // Mark as applied after successful table creation.
        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;

        // We add the table to the cache.
        inner.created_tables.insert(table_id);

        debug!("iceberg table {iceberg_table_name} added to creation cache");

        Ok((namespace, iceberg_table_name))
    }

    /// Creates a namespace if it is missing in the destination.
    ///
    /// Once created, adds it to the created namespace cache.
    async fn create_namespace_if_missing(
        &self,
        inner: &mut Inner,
        namespace: String,
    ) -> EtlResult<String> {
        if inner.created_namespaces.contains(&namespace) {
            return Ok(namespace);
        }

        self.client
            .create_namespace_if_missing(&namespace)
            .await
            .map_err(iceberg_error_to_etl_error)?;

        inner.created_namespaces.insert(namespace.clone());

        Ok(namespace)
    }
}

impl<S> Destination for IcebergDestination<S>
where
    S: SharedStateStore,
{
    /// Returns the identifier name for this destination type.
    fn name() -> &'static str {
        "iceberg"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.tasks.shutdown().await
    }

    /// Drops the specified table before a fresh copy.
    ///
    /// The table sync worker clears ETL metadata after this succeeds, and the
    /// next copy recreates the table from the fresh `0/0` schema.
    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        let result = self.drop_table_for_copy_inner(replicated_table_schema).await;
        async_result.send(result);

        Ok(())
    }

    /// Writes table-copy rows to the destination.
    ///
    /// Rows are appended directly to the corresponding Iceberg table.
    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result =
            IcebergDestination::write_table_rows(self, replicated_table_schema, table_rows).await;
        async_result.send(result);

        Ok(())
    }

    /// Processes and writes CDC events to the destination tables.
    ///
    /// Handles insert, update, delete, and truncate events by converting
    /// them to appropriate Iceberg operations. Events are batched by table
    /// and processed concurrently for optimal performance.
    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let destination = self.clone();
        self.tasks
            .spawn(async move {
                let result = destination.write_events(events).await;
                async_result.send(result);
            })
            .await;

        Ok(())
    }
}

/// Pending CDC changes for one Iceberg table.
#[derive(Default)]
struct TableDelta {
    /// Pending changes keyed by the current primary-key value.
    changes: HashMap<String, PendingChange>,
}

impl TableDelta {
    /// Records an inserted row as an upsert.
    fn insert(
        &mut self,
        table_row: TableRow,
        replicated_table_schema: &ReplicatedTableSchema,
        key_indices: &[usize],
    ) {
        let key = row_key(&table_row, key_indices);
        let delete_row =
            equality_delete_row_from_full_row(&table_row, replicated_table_schema, key_indices);
        let mut change = self.changes.remove(&key).unwrap_or_default();

        change.delete_rows.push(delete_row);
        change.data_row = Some(table_row);

        self.merge_at_key(key, change);
    }

    /// Records an updated row as delete-old-key plus write-new-row.
    fn update(&mut self, delete_row: TableRow, data_row: TableRow, key_indices: &[usize]) {
        let old_key = row_key(&delete_row, key_indices);
        let new_key = row_key(&data_row, key_indices);
        let mut change =
            self.changes.remove(&old_key).unwrap_or_else(|| PendingChange::with_delete(delete_row));

        change.data_row = Some(data_row);

        self.merge_at_key(new_key, change);
    }

    /// Records a deleted row.
    fn delete(&mut self, delete_row: TableRow, key_indices: &[usize]) {
        let key = row_key(&delete_row, key_indices);
        let mut change =
            self.changes.remove(&key).unwrap_or_else(|| PendingChange::with_delete(delete_row));

        change.data_row = None;
        self.merge_at_key(key, change);
    }

    /// Merges a pending change into the entry for `key`.
    fn merge_at_key(&mut self, key: String, change: PendingChange) {
        if let Some(existing) = self.changes.get_mut(&key) {
            existing.delete_rows.extend(change.delete_rows);
            existing.data_row = change.data_row;
        } else {
            self.changes.insert(key, change);
        }
    }

    /// Splits pending changes into rows to write and rows to equality-delete.
    fn into_parts(self) -> (Vec<TableRow>, Vec<TableRow>) {
        let mut data_rows = Vec::new();
        let mut delete_rows = Vec::new();
        let mut seen_delete_keys = HashSet::new();

        for change in self.changes.into_values() {
            if let Some(data_row) = change.data_row {
                data_rows.push(data_row);
            }

            for delete_row in change.delete_rows {
                let key = row_key_all_values(&delete_row);
                if seen_delete_keys.insert(key) {
                    delete_rows.push(delete_row);
                }
            }
        }

        (data_rows, delete_rows)
    }
}

/// Pending change for one logical source row.
#[derive(Default)]
struct PendingChange {
    /// Equality-delete rows for prior snapshots.
    delete_rows: Vec<TableRow>,
    /// Final row value when the row remains present after the batch.
    data_row: Option<TableRow>,
}

impl PendingChange {
    /// Creates a pending change that deletes an existing key.
    fn with_delete(delete_row: TableRow) -> Self {
        Self { delete_rows: vec![delete_row], data_row: None }
    }
}

/// Converts an old-row image into a full-width equality-delete row.
fn old_table_row_to_delete_row(
    old_table_row: OldTableRow,
    replicated_table_schema: &ReplicatedTableSchema,
    key_indices: &[usize],
) -> EtlResult<TableRow> {
    match old_table_row {
        OldTableRow::Full(table_row) => {
            Ok(equality_delete_row_from_full_row(&table_row, replicated_table_schema, key_indices))
        }
        OldTableRow::Key(table_row) => {
            expand_key_row(replicated_table_schema, table_row, key_indices)
        }
    }
}

/// Builds a full-width equality-delete row from a full row.
fn equality_delete_row_from_full_row(
    table_row: &TableRow,
    replicated_table_schema: &ReplicatedTableSchema,
    key_indices: &[usize],
) -> TableRow {
    let mut values = placeholder_row_values(replicated_table_schema);
    for &index in key_indices {
        values[index] = table_row.values()[index].clone();
    }

    TableRow::new(values)
}

/// Expands a dense primary-key row into the full replicated table width.
fn expand_key_row(
    replicated_table_schema: &ReplicatedTableSchema,
    key_row: TableRow,
    key_indices: &[usize],
) -> EtlResult<TableRow> {
    if key_row.values().len() != key_indices.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "Iceberg key row width does not match the source primary key",
            format!(
                "Table '{}' emitted {} key values for {} primary-key columns.",
                replicated_table_schema.name(),
                key_row.values().len(),
                key_indices.len()
            )
        ));
    }

    let mut values = placeholder_row_values(replicated_table_schema);
    for (&index, value) in key_indices.iter().zip(key_row.into_values()) {
        values[index] = value;
    }

    Ok(TableRow::new(values))
}

/// Builds full-width placeholder values for an equality-delete input row.
fn placeholder_row_values(replicated_table_schema: &ReplicatedTableSchema) -> Vec<Cell> {
    replicated_table_schema.column_schemas().map(placeholder_cell_for_column).collect()
}

/// Returns a type-correct placeholder value for a non-key delete column.
fn placeholder_cell_for_column(column_schema: &ColumnSchema) -> Cell {
    match column_schema.typ {
        Type::BOOL => Cell::Bool(false),
        Type::INT2 => Cell::I16(0),
        Type::INT4 => Cell::I32(0),
        Type::INT8 => Cell::I64(0),
        Type::OID => Cell::U32(0),
        Type::FLOAT4 => Cell::F32(0.0),
        Type::FLOAT8 => Cell::F64(0.0),
        Type::BYTEA => Cell::Bytes(Vec::new()),
        Type::DATE => Cell::Date(chrono::NaiveDate::default()),
        Type::TIME => Cell::Time(chrono::NaiveTime::default()),
        Type::TIMESTAMP => Cell::Timestamp(chrono::NaiveDateTime::default()),
        Type::TIMESTAMPTZ => Cell::TimestampTz(chrono::DateTime::<chrono::Utc>::default()),
        Type::UUID => Cell::Uuid(uuid::Uuid::default()),
        Type::BOOL_ARRAY => Cell::Array(ArrayCell::Bool(Vec::new())),
        Type::CHAR_ARRAY
        | Type::BPCHAR_ARRAY
        | Type::VARCHAR_ARRAY
        | Type::NAME_ARRAY
        | Type::TEXT_ARRAY => Cell::Array(ArrayCell::String(Vec::new())),
        Type::INT2_ARRAY => Cell::Array(ArrayCell::I16(Vec::new())),
        Type::INT4_ARRAY => Cell::Array(ArrayCell::I32(Vec::new())),
        Type::INT8_ARRAY => Cell::Array(ArrayCell::I64(Vec::new())),
        Type::FLOAT4_ARRAY => Cell::Array(ArrayCell::F32(Vec::new())),
        Type::FLOAT8_ARRAY => Cell::Array(ArrayCell::F64(Vec::new())),
        Type::NUMERIC_ARRAY => Cell::Array(ArrayCell::Numeric(Vec::new())),
        Type::DATE_ARRAY => Cell::Array(ArrayCell::Date(Vec::new())),
        Type::TIME_ARRAY => Cell::Array(ArrayCell::Time(Vec::new())),
        Type::TIMESTAMP_ARRAY => Cell::Array(ArrayCell::Timestamp(Vec::new())),
        Type::TIMESTAMPTZ_ARRAY => Cell::Array(ArrayCell::TimestampTz(Vec::new())),
        Type::UUID_ARRAY => Cell::Array(ArrayCell::Uuid(Vec::new())),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => Cell::Array(ArrayCell::Json(Vec::new())),
        Type::OID_ARRAY => Cell::Array(ArrayCell::U32(Vec::new())),
        Type::BYTEA_ARRAY => Cell::Array(ArrayCell::Bytes(Vec::new())),
        Type::NUMERIC | Type::JSON | Type::JSONB => Cell::String(String::new()),
        _ => Cell::String(String::new()),
    }
}

/// Returns replicated-column indexes for the source primary key.
fn primary_key_indices(replicated_table_schema: &ReplicatedTableSchema) -> Vec<usize> {
    replicated_table_schema
        .column_schemas()
        .enumerate()
        .filter_map(|(index, column_schema)| column_schema.primary_key().then_some(index))
        .collect()
}

/// Returns a stable key string for the primary-key values in `table_row`.
fn row_key(table_row: &TableRow, key_indices: &[usize]) -> String {
    let mut key = String::new();
    let values = table_row.values();
    for &index in key_indices {
        push_row_key_value(&mut key, &values[index]);
    }

    key
}

/// Returns a stable key string for all values in `table_row`.
fn row_key_all_values(table_row: &TableRow) -> String {
    let mut key = String::new();
    for value in table_row.values() {
        push_row_key_value(&mut key, value);
    }

    key
}

/// Appends one length-prefixed cell representation to a row key.
fn push_row_key_value(key: &mut String, cell: &Cell) {
    let value = format!("{cell:?}");
    key.push_str(&value.len().to_string());
    key.push(':');
    key.push_str(&value);
    key.push(';');
}

/// Validates that a replicated table schema can be applied in Iceberg.
///
/// Iceberg CDC replay uses v2 equality-delete files over the source primary
/// key. Source tables therefore need a replicated primary key and either a
/// primary-key-equivalent replica identity or full old-row images.
fn validate_iceberg_table_identity(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if replicated_table_schema.primary_key_column_schemas().len() == 0 {
        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Iceberg requires a source primary key",
            format!(
                "Table '{}' does not have replicated primary-key columns, but Iceberg equality \
                 deletes require stable identifier fields.",
                replicated_table_schema.name()
            )
        ));
    }

    if !replicated_table_schema.all_primary_key_columns_replicated() {
        let missing_columns = replicated_table_schema
            .unreplicated_primary_key_column_schemas()
            .map(|column_schema| column_schema.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "Iceberg requires all primary-key columns to be replicated",
            format!(
                "Table '{}' does not replicate primary-key column(s) '{}'.",
                replicated_table_schema.name(),
                missing_columns
            )
        ));
    }

    if replicated_table_schema.identity_type() == IdentityType::Full
        || replicated_table_schema.identity_matches_primary_key()
    {
        return Ok(());
    }

    Err(etl_error!(
        ErrorKind::SourceReplicaIdentityError,
        "Iceberg requires primary-key row identity",
        format!(
            "Table '{}' uses replica identity {:?}, but Iceberg equality deletes require FULL or \
             primary-key-equivalent replica identity.",
            replicated_table_schema.name(),
            replicated_table_schema.identity_type()
        )
    ))
}

/// Converts a Postgres schema name to a valid S3 Tables namespace.
fn schema_to_namespace(schema: &str) -> String {
    // Convert to lowercase and replace invalid characters with underscores.
    // S3 Tables namespaces can only contain lowercase letters, numbers, and
    // underscores.
    let mut namespace: String = schema
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' { c } else { '_' })
        .collect();

    // S3 Tables namespaces must not start with 'aws'.
    if namespace.starts_with("aws") {
        namespace = format!("s_{namespace}");
    }

    // Ensure namespace starts with a letter or number.
    if let Some(first_char) = namespace.chars().next()
        && !first_char.is_ascii_lowercase()
        && !first_char.is_ascii_digit()
    {
        namespace = format!("s{namespace}");
    }

    // Ensure namespace ends with a letter or number.
    if let Some(last_char) = namespace.chars().last()
        && !last_char.is_ascii_lowercase()
        && !last_char.is_ascii_digit()
    {
        namespace = format!("{namespace}e");
    }

    namespace
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::{
        error::ErrorKind,
        types::{
            ColumnSchema, IdentityMask, IdentityType, ReplicatedTableSchema, ReplicationMask,
            TableId, TableName, TableSchema, Type,
        },
    };

    use crate::iceberg::core::{schema_to_namespace, validate_iceberg_table_identity};

    fn replicated_schema(identity_type: IdentityType) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
            ],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = match identity_type {
            IdentityType::Full => IdentityMask::from_bytes(vec![1, 1]),
            IdentityType::PrimaryKey => IdentityMask::from_bytes(vec![1, 0]),
            IdentityType::AlternativeKey => IdentityMask::from_bytes(vec![0, 1]),
            IdentityType::Missing => IdentityMask::from_bytes(vec![0, 0]),
        };

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    #[test]
    fn schema_to_namespace_valid_names() {
        // Valid names that don't need transformation.
        assert_eq!(schema_to_namespace("public"), "public");
        assert_eq!(schema_to_namespace("my_schema"), "my_schema");
        assert_eq!(schema_to_namespace("schema123"), "schema123");
        assert_eq!(schema_to_namespace("a"), "a");
        assert_eq!(schema_to_namespace("test_123_schema"), "test_123_schema");
    }

    #[test]
    fn schema_to_namespace_case_conversion() {
        // PostgreSQL unquoted identifiers are case-insensitive and folded to lowercase.
        assert_eq!(schema_to_namespace("UserSchema"), "userschema");
        assert_eq!(schema_to_namespace("MYSCHEMA"), "myschema");
        assert_eq!(schema_to_namespace("MixedCase"), "mixedcase");
        assert_eq!(schema_to_namespace("CamelCaseSchema"), "camelcaseschema");
    }

    #[test]
    fn schema_to_namespace_invalid_chars_replaced() {
        // Invalid characters should be replaced with underscores.
        assert_eq!(schema_to_namespace("my-schema"), "my_schema");
        assert_eq!(schema_to_namespace("data$store"), "data_store");
        assert_eq!(schema_to_namespace("user schema"), "user_schema");
        assert_eq!(schema_to_namespace("schema.name"), "schema_name");
        assert_eq!(schema_to_namespace("test@schema"), "test_schema");
        assert_eq!(schema_to_namespace("schema#1"), "schema_1");
        assert_eq!(schema_to_namespace("my-complex.schema$name"), "my_complex_schema_name");
    }

    #[test]
    fn schema_to_namespace_non_ascii_replaced() {
        // Non-ASCII characters should be replaced with underscores (one char = one
        // underscore).
        assert_eq!(schema_to_namespace("schémas"), "sch_mas");
        // All non-ASCII becomes underscores, then fixed to start/end with
        // letter/number.
        assert_eq!(schema_to_namespace("データ"), "s___e");
        assert_eq!(schema_to_namespace("test_α_beta"), "test___beta");
    }

    #[test]
    fn schema_to_namespace_starts_with_underscore() {
        // Names starting with underscore should be prefixed with 's'.
        assert_eq!(schema_to_namespace("_private"), "s_private");
        assert_eq!(schema_to_namespace("_temp"), "s_temp");
        assert_eq!(schema_to_namespace("__double"), "s__double");
        assert_eq!(schema_to_namespace("___triple"), "s___triple");
    }

    #[test]
    fn schema_to_namespace_ends_with_underscore() {
        // Names ending with underscore should be suffixed with 'e'.
        assert_eq!(schema_to_namespace("temp_"), "temp_e");
        assert_eq!(schema_to_namespace("schema__"), "schema__e");
        assert_eq!(schema_to_namespace("test___"), "test___e");
    }

    #[test]
    fn schema_to_namespace_starts_and_ends_with_underscore() {
        // Names both starting and ending with underscore.
        assert_eq!(schema_to_namespace("_temp_"), "s_temp_e");
        assert_eq!(schema_to_namespace("__test__"), "s__test__e");
        assert_eq!(schema_to_namespace("_schema_name_"), "s_schema_name_e");
    }

    #[test]
    fn schema_to_namespace_starts_with_aws() {
        // Names starting with 'aws' should be prefixed with 's_'.
        assert_eq!(schema_to_namespace("aws_internal"), "s_aws_internal");
        assert_eq!(schema_to_namespace("aws"), "s_aws");
        assert_eq!(schema_to_namespace("awsschema"), "s_awsschema");
        assert_eq!(schema_to_namespace("AWS"), "s_aws");
    }

    #[test]
    fn schema_to_namespace_starts_with_underscore_and_aws() {
        // Names starting with underscore followed by 'aws'.
        assert_eq!(schema_to_namespace("_aws_internal"), "s_aws_internal");
        assert_eq!(schema_to_namespace("_aws"), "s_aws");
        // After removing leading underscore, it becomes 'aws', so needs 's_' prefix.
        assert_eq!(schema_to_namespace("__aws"), "s__aws");
    }

    #[test]
    fn schema_to_namespace_all_underscores() {
        // Schema names that become all underscores after transformation.
        assert_eq!(schema_to_namespace("___"), "s___e");
        assert_eq!(schema_to_namespace("____"), "s____e");
    }

    #[test]
    fn schema_to_namespace_only_invalid_chars_at_edges() {
        // Invalid characters only at start/end.
        assert_eq!(schema_to_namespace("-schema"), "s_schema");
        assert_eq!(schema_to_namespace("schema-"), "schema_e");
        assert_eq!(schema_to_namespace("-schema-"), "s_schema_e");
        assert_eq!(schema_to_namespace("$test$"), "s_test_e");
    }

    #[test]
    fn schema_to_namespace_numeric_start() {
        // PostgreSQL allows identifiers starting with numbers in quoted form.
        // These are valid and should pass through.
        assert_eq!(schema_to_namespace("123schema"), "123schema");
        assert_eq!(schema_to_namespace("42"), "42");
        assert_eq!(schema_to_namespace("1_test"), "1_test");
    }

    #[test]
    fn schema_to_namespace_consecutive_underscores() {
        // Consecutive underscores should be preserved.
        assert_eq!(schema_to_namespace("test__schema"), "test__schema");
        assert_eq!(schema_to_namespace("a___b"), "a___b");
        assert_eq!(schema_to_namespace("my____schema"), "my____schema");
    }

    #[test]
    fn schema_to_namespace_multiple_invalid_chars_consecutive() {
        // Multiple consecutive invalid characters become multiple underscores.
        assert_eq!(schema_to_namespace("test---schema"), "test___schema");
        assert_eq!(schema_to_namespace("my...schema"), "my___schema");
        assert_eq!(schema_to_namespace("data$$$store"), "data___store");
    }

    #[test]
    fn schema_to_namespace_max_length() {
        // PostgreSQL max identifier length is 63 bytes (NAMEDATALEN-1).
        // Test with a 63-character schema name.
        let long_schema = "a".repeat(63);
        let result = schema_to_namespace(&long_schema);
        assert_eq!(result, long_schema);
        assert_eq!(result.len(), 63);
    }

    #[test]
    fn schema_to_namespace_common_patterns() {
        // Common real-world schema naming patterns.
        assert_eq!(schema_to_namespace("auth"), "auth");
        assert_eq!(schema_to_namespace("realtime"), "realtime");
        assert_eq!(schema_to_namespace("storage"), "storage");
        assert_eq!(schema_to_namespace("pg_catalog"), "pg_catalog");
        assert_eq!(schema_to_namespace("information_schema"), "information_schema");
    }

    #[test]
    fn validate_iceberg_table_identity_accepts_full() {
        let replicated_table_schema = replicated_schema(IdentityType::Full);

        validate_iceberg_table_identity(&replicated_table_schema).unwrap();
    }

    #[test]
    fn validate_iceberg_table_identity_accepts_primary_key() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);

        validate_iceberg_table_identity(&replicated_table_schema).unwrap();
    }

    #[test]
    fn validate_iceberg_table_identity_rejects_alternative_key() {
        let replicated_table_schema = replicated_schema(IdentityType::AlternativeKey);

        let error = validate_iceberg_table_identity(&replicated_table_schema).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn validate_iceberg_table_identity_rejects_missing() {
        let replicated_table_schema = replicated_schema(IdentityType::Missing);

        let error = validate_iceberg_table_identity(&replicated_table_schema).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }
}
