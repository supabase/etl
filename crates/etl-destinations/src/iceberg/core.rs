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
    state::destination_table_metadata::{DestinationTableMetadata, DestinationTableSchemaStatus},
    store::{SchemaStore, SharedStateStore},
    types::{
        ArrayCell, Cell, ColumnSchema, Event, IdentityType, OldTableRow, ReplicatedTableSchema,
        TableId, TableName, TableRow, Type, UpdatedTableRow,
    },
};
use tokio::{sync::Mutex, task::JoinSet};
use tracing::{debug, info, warn};

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
/// Provides experimental Postgres-to-Iceberg materialized table functionality
/// including table copy, streaming inserts, updates, deletes, and truncates.
/// This destination is not production supported and does not run Iceberg
/// compaction, snapshot expiration, manifest rewrites, or orphan-file cleanup.
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
    S: SharedStateStore + SchemaStore,
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

    /// Handles a relation event by applying an Iceberg schema update.
    ///
    /// Incoming relation events are processed only after pending row events
    /// have been flushed. The state store is moved to `Applying` before the
    /// Iceberg catalog commit and to `Applied` only after the commit succeeds.
    /// The diff is computed from the ETL schema snapshot recorded in applied
    /// destination metadata, not from the destination table's current schema.
    async fn handle_relation_event(
        &self,
        new_replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        validate_iceberg_table_identity(new_replicated_table_schema)?;

        let table_id = new_replicated_table_schema.id();
        let new_snapshot_id = new_replicated_table_schema.inner().snapshot_id;
        let new_replication_mask = new_replicated_table_schema.replication_mask().clone();
        let new_column_schemas: Vec<_> =
            new_replicated_table_schema.column_schemas().cloned().collect();

        let mut inner = self.inner.lock().await;
        let Some(metadata) = self.store.get_applied_destination_table_metadata(table_id).await?
        else {
            return Err(etl_error!(
                ErrorKind::CorruptedTableSchema,
                "Missing destination table metadata",
                format!(
                    "No destination table metadata found for table {} when processing Iceberg \
                     schema change.",
                    table_id
                )
            ));
        };

        let current_snapshot_id = metadata.snapshot_id;
        let current_replication_mask = metadata.replication_mask.clone();

        if current_snapshot_id == new_snapshot_id
            && current_replication_mask == new_replication_mask
        {
            debug!(%table_id, snapshot_id = %new_snapshot_id, "iceberg schema unchanged");
            return Ok(());
        }

        let current_table_schema =
            self.store.get_table_schema(&table_id, current_snapshot_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "Old schema not found",
                        format!(
                            "Could not find schema for table {} at snapshot_id {}",
                            table_id, current_snapshot_id
                        )
                    )
                },
            )?;
        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );
        let current_column_schemas: Vec<_> = current_schema.column_schemas().cloned().collect();
        let diff = current_schema.diff(new_replicated_table_schema);

        let updated_metadata = DestinationTableMetadata::new_applied(
            metadata.destination_table_id.clone(),
            current_snapshot_id,
            current_replication_mask,
        )
        .with_schema_change(
            new_snapshot_id,
            new_replication_mask.clone(),
            DestinationTableSchemaStatus::Applying,
        );
        self.store.store_destination_table_metadata(table_id, updated_metadata.clone()).await?;

        let table_name = new_replicated_table_schema.name();
        let table_namespace = schema_to_namespace(&table_name.schema);
        let namespace = inner.namespace.get_or(&table_namespace).to_owned();
        let namespace = self.create_namespace_if_missing(&mut inner, namespace).await?;
        let iceberg_table_name = updated_metadata.destination_table_id.clone();

        info!(
            %table_id,
            snapshot_id = %new_snapshot_id,
            "applying iceberg schema change"
        );

        self.client
            .evolve_table_schema(
                &namespace,
                iceberg_table_name,
                &current_column_schemas,
                &new_column_schemas,
                &diff,
            )
            .await
            .map_err(iceberg_error_to_etl_error)?;

        self.store
            .store_destination_table_metadata(table_id, updated_metadata.to_applied())
            .await?;
        inner.created_tables.insert(table_id);

        info!(
            %table_id,
            snapshot_id = %new_snapshot_id,
            "iceberg schema change applied"
        );

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
            // Maps table ID to pending changes for that table.
            let mut table_id_to_data: HashMap<TableId, TableEventBatch> = HashMap::new();

            // Process events until we hit a truncate or relation event, or run
            // out of events. Truncate and relation events require flushing all
            // batched data first to preserve source ordering.
            while let Some(event) = events_iter.peek() {
                if matches!(event, Event::Truncate(_) | Event::Relation(_)) {
                    break;
                }

                let Some(event) = events_iter.next() else {
                    break;
                };
                match event {
                    Event::Insert(insert) => {
                        validate_iceberg_table_identity(&insert.replicated_table_schema)?;
                        let table_id = insert.replicated_table_schema.id();
                        let batch = table_id_to_data.entry(table_id).or_insert_with(|| {
                            TableEventBatch::new(insert.replicated_table_schema.clone())
                        });
                        batch.table_delta.insert(
                            insert.table_row,
                            &batch.replicated_table_schema,
                            &batch.key_indices,
                        );
                    }
                    Event::Update(update) => {
                        validate_iceberg_table_identity(&update.replicated_table_schema)?;
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
                        let table_id = update.replicated_table_schema.id();
                        let batch = table_id_to_data.entry(table_id).or_insert_with(|| {
                            TableEventBatch::new(update.replicated_table_schema.clone())
                        });
                        let delete_table_row = match update.old_table_row {
                            Some(old_table_row) => old_table_row_to_delete_row(
                                old_table_row,
                                &batch.replicated_table_schema,
                                &batch.key_indices,
                            )?,
                            None => equality_delete_row_from_full_row(
                                &updated_table_row,
                                &batch.replicated_table_schema,
                                &batch.key_indices,
                            ),
                        };

                        batch.table_delta.update(
                            delete_table_row,
                            updated_table_row,
                            &batch.replicated_table_schema,
                            &batch.key_indices,
                        );
                    }
                    Event::Delete(delete) => {
                        validate_iceberg_table_identity(&delete.replicated_table_schema)?;
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
                        let table_id = delete.replicated_table_schema.id();
                        let batch = table_id_to_data.entry(table_id).or_insert_with(|| {
                            TableEventBatch::new(delete.replicated_table_schema.clone())
                        });
                        let delete_table_row = old_table_row_to_delete_row(
                            old_table_row,
                            &batch.replicated_table_schema,
                            &batch.key_indices,
                        )?;

                        batch.table_delta.delete(delete_table_row, &batch.key_indices);
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

                for (_, batch) in table_id_to_data {
                    let TableEventBatch { replicated_table_schema, table_delta, .. } = batch;
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

            // Process any relation events that caused the batch to flush.
            // Multiple consecutive relation events are processed sequentially.
            while let Some(Event::Relation(_)) = events_iter.peek() {
                if let Some(Event::Relation(relation)) = events_iter.next() {
                    self.handle_relation_event(&relation.replicated_table_schema).await?;
                }
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
    S: SharedStateStore + SchemaStore,
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

/// Pending CDC event batch for one Iceberg table.
struct TableEventBatch {
    /// Source table schema used for this batch.
    replicated_table_schema: ReplicatedTableSchema,
    /// Replicated-column indexes for the source primary key.
    key_indices: Vec<usize>,
    /// Pending row-level changes for the table.
    table_delta: TableDelta,
}

impl TableEventBatch {
    /// Creates an empty table event batch.
    fn new(replicated_table_schema: ReplicatedTableSchema) -> Self {
        let key_indices = primary_key_indices(&replicated_table_schema);

        Self { replicated_table_schema, key_indices, table_delta: TableDelta::default() }
    }
}

/// Pending CDC changes for one Iceberg table.
#[derive(Default)]
struct TableDelta {
    /// Pending changes keyed by the current primary-key value.
    changes: HashMap<RowKey, PendingChange>,
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

        change.delete_rows.push(PendingDelete::new(key.clone(), delete_row));
        change.data_row = Some(table_row);

        self.merge_at_key(key, change);
    }

    /// Records an updated row as delete-old-key plus upsert-new-row.
    fn update(
        &mut self,
        delete_row: TableRow,
        data_row: TableRow,
        replicated_table_schema: &ReplicatedTableSchema,
        key_indices: &[usize],
    ) {
        let old_key = row_key(&delete_row, key_indices);
        let new_key = row_key(&data_row, key_indices);
        let new_key_delete_row = (old_key != new_key).then(|| {
            equality_delete_row_from_full_row(&data_row, replicated_table_schema, key_indices)
        });
        let mut change = self
            .changes
            .remove(&old_key)
            .unwrap_or_else(|| PendingChange::with_delete(old_key, delete_row));

        if let Some(delete_row) = new_key_delete_row {
            change.delete_rows.push(PendingDelete::new(new_key.clone(), delete_row));
        }
        change.data_row = Some(data_row);

        self.merge_at_key(new_key, change);
    }

    /// Records a deleted row.
    fn delete(&mut self, delete_row: TableRow, key_indices: &[usize]) {
        let key = row_key(&delete_row, key_indices);
        let mut change = self
            .changes
            .remove(&key)
            .unwrap_or_else(|| PendingChange::with_delete(key.clone(), delete_row));

        change.data_row = None;
        self.merge_at_key(key, change);
    }

    /// Merges a pending change into the entry for `key`.
    fn merge_at_key(&mut self, key: RowKey, change: PendingChange) {
        if let Some(existing) = self.changes.get_mut(&key) {
            existing.delete_rows.extend(change.delete_rows);
            existing.data_row = change.data_row;
        } else {
            self.changes.insert(key, change);
        }
    }

    /// Splits pending changes into rows to write and rows to equality-delete.
    fn into_parts(self) -> (Vec<TableRow>, Vec<TableRow>) {
        let data_row_count =
            self.changes.values().filter(|change| change.data_row.is_some()).count();
        let delete_row_count = self.changes.values().map(|change| change.delete_rows.len()).sum();
        let mut data_rows = Vec::with_capacity(data_row_count);
        let mut delete_rows = Vec::with_capacity(delete_row_count);
        let mut seen_delete_keys = HashSet::with_capacity(delete_row_count);

        for change in self.changes.into_values() {
            if let Some(data_row) = change.data_row {
                data_rows.push(data_row);
            }

            for delete_row in change.delete_rows {
                if seen_delete_keys.insert(delete_row.key) {
                    delete_rows.push(delete_row.row);
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
    delete_rows: Vec<PendingDelete>,
    /// Final row value when the row remains present after the batch.
    data_row: Option<TableRow>,
}

impl PendingChange {
    /// Creates a pending change that deletes an existing key.
    fn with_delete(key: RowKey, delete_row: TableRow) -> Self {
        Self { delete_rows: vec![PendingDelete::new(key, delete_row)], data_row: None }
    }
}

/// Pending equality delete for a primary-key value.
struct PendingDelete {
    /// Primary key targeted by the equality delete.
    key: RowKey,
    /// Full-width row passed to the Iceberg equality-delete writer.
    row: TableRow,
}

impl PendingDelete {
    /// Creates a pending equality delete.
    fn new(key: RowKey, row: TableRow) -> Self {
        Self { key, row }
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

/// Primary-key values used for in-memory CDC coalescing.
#[derive(Clone, Eq, Hash, PartialEq)]
struct RowKey {
    /// Values that make up the source primary key.
    values: Vec<RowKeyValue>,
}

/// Returns a typed primary-key value for `table_row`.
fn row_key(table_row: &TableRow, key_indices: &[usize]) -> RowKey {
    let mut values = Vec::with_capacity(key_indices.len());
    let row_values = table_row.values();
    for &index in key_indices {
        values.push(RowKeyValue::from_cell(&row_values[index]));
    }

    RowKey { values }
}

/// Value inside an in-memory row key.
#[derive(Clone, Eq, Hash, PartialEq)]
enum RowKeyValue {
    /// Null key value.
    Null,
    /// Boolean key value.
    Bool(bool),
    /// Text key value.
    String(String),
    /// 16-bit integer key value.
    I16(i16),
    /// 32-bit integer key value.
    I32(i32),
    /// 32-bit unsigned integer key value.
    U32(u32),
    /// 64-bit integer key value.
    I64(i64),
    /// Normalized `f32` bits.
    F32(u32),
    /// Normalized `f64` bits.
    F64(u64),
    /// Debug-encoded numeric key value.
    Numeric(String),
    /// Date key value.
    Date(chrono::NaiveDate),
    /// Time key value.
    Time(chrono::NaiveTime),
    /// Timestamp key value.
    Timestamp(chrono::NaiveDateTime),
    /// Timestamp with timezone key value.
    TimestampTz(chrono::DateTime<chrono::Utc>),
    /// UUID key value.
    Uuid(uuid::Uuid),
    /// Debug-encoded JSON key value.
    Json(String),
    /// Binary key value.
    Bytes(Vec<u8>),
    /// Debug-encoded array key value.
    Array(String),
}

impl RowKeyValue {
    /// Converts a source cell into a hashable row-key value.
    fn from_cell(cell: &Cell) -> Self {
        match cell {
            Cell::Null => RowKeyValue::Null,
            Cell::Bool(value) => RowKeyValue::Bool(*value),
            Cell::String(value) => RowKeyValue::String(value.clone()),
            Cell::I16(value) => RowKeyValue::I16(*value),
            Cell::I32(value) => RowKeyValue::I32(*value),
            Cell::U32(value) => RowKeyValue::U32(*value),
            Cell::I64(value) => RowKeyValue::I64(*value),
            Cell::F32(value) => RowKeyValue::F32(float32_key_bits(*value)),
            Cell::F64(value) => RowKeyValue::F64(float64_key_bits(*value)),
            Cell::Numeric(value) => RowKeyValue::Numeric(debug_key(value)),
            Cell::Date(value) => RowKeyValue::Date(*value),
            Cell::Time(value) => RowKeyValue::Time(*value),
            Cell::Timestamp(value) => RowKeyValue::Timestamp(*value),
            Cell::TimestampTz(value) => RowKeyValue::TimestampTz(*value),
            Cell::Uuid(value) => RowKeyValue::Uuid(*value),
            Cell::Json(value) => RowKeyValue::Json(debug_key(value)),
            Cell::Bytes(value) => RowKeyValue::Bytes(value.clone()),
            Cell::Array(value) => RowKeyValue::Array(debug_key(value)),
        }
    }
}

/// Returns normalized `f32` bits for key equality and hashing.
fn float32_key_bits(value: f32) -> u32 {
    if value == 0.0 {
        0.0_f32.to_bits()
    } else if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

/// Returns normalized `f64` bits for key equality and hashing.
fn float64_key_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

/// Encodes uncommon key values through their debug representation.
fn debug_key(value: &impl std::fmt::Debug) -> String {
    format!("{value:?}")
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
            Cell, ColumnSchema, IdentityMask, IdentityType, ReplicatedTableSchema, ReplicationMask,
            TableId, TableName, TableRow, TableSchema, Type,
        },
    };

    use crate::iceberg::core::{
        TableDelta, equality_delete_row_from_full_row, schema_to_namespace,
        validate_iceberg_table_identity,
    };

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

    fn replicated_composite_key_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("tenant".to_owned(), Type::TEXT, -1, 1, Some(1), false),
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 2, Some(2), false),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 3, None, true),
            ],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![1, 1, 0]);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    fn test_row(id: i32, name: &str) -> TableRow {
        TableRow::new(vec![Cell::I32(id), Cell::String(name.to_owned())])
    }

    fn composite_key_row(tenant: &str, id: i32, name: &str) -> TableRow {
        TableRow::new(vec![
            Cell::String(tenant.to_owned()),
            Cell::I32(id),
            Cell::String(name.to_owned()),
        ])
    }

    fn delete_row(
        table_row: &TableRow,
        replicated_table_schema: &ReplicatedTableSchema,
        key_indices: &[usize],
    ) -> TableRow {
        equality_delete_row_from_full_row(table_row, replicated_table_schema, key_indices)
    }

    #[test]
    fn table_delta_repeated_insert_keeps_latest_row() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let key_indices = [0];
        let first_row = test_row(1, "alice");
        let second_row = test_row(1, "alice-updated");
        let mut table_delta = TableDelta::default();

        table_delta.insert(first_row, &replicated_table_schema, &key_indices);
        table_delta.insert(second_row.clone(), &replicated_table_schema, &key_indices);

        let (data_rows, delete_rows) = table_delta.into_parts();
        assert_eq!(data_rows, vec![second_row]);
        assert_eq!(delete_rows.len(), 1);
    }

    #[test]
    fn table_delta_insert_then_delete_writes_only_delete() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let key_indices = [0];
        let row = test_row(1, "alice");
        let delete_row = delete_row(&row, &replicated_table_schema, &key_indices);
        let mut table_delta = TableDelta::default();

        table_delta.insert(row, &replicated_table_schema, &key_indices);
        table_delta.delete(delete_row.clone(), &key_indices);

        let (data_rows, delete_rows) = table_delta.into_parts();
        assert!(data_rows.is_empty());
        assert_eq!(delete_rows, vec![delete_row]);
    }

    #[test]
    fn table_delta_delete_then_insert_writes_delete_and_final_row() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let key_indices = [0];
        let old_row = test_row(1, "alice");
        let new_row = test_row(1, "alice-updated");
        let delete_row = delete_row(&old_row, &replicated_table_schema, &key_indices);
        let mut table_delta = TableDelta::default();

        table_delta.delete(delete_row.clone(), &key_indices);
        table_delta.insert(new_row.clone(), &replicated_table_schema, &key_indices);

        let (data_rows, delete_rows) = table_delta.into_parts();
        assert_eq!(data_rows, vec![new_row]);
        assert_eq!(delete_rows, vec![delete_row]);
    }

    #[test]
    fn table_delta_non_key_update_keeps_updated_row() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let key_indices = [0];
        let old_row = test_row(1, "alice");
        let new_row = test_row(1, "alice-updated");
        let delete_row = delete_row(&old_row, &replicated_table_schema, &key_indices);
        let mut table_delta = TableDelta::default();

        table_delta.update(
            delete_row.clone(),
            new_row.clone(),
            &replicated_table_schema,
            &key_indices,
        );

        let (data_rows, delete_rows) = table_delta.into_parts();
        assert_eq!(data_rows, vec![new_row]);
        assert_eq!(delete_rows, vec![delete_row]);
    }

    #[test]
    fn table_delta_update_then_delete_writes_old_and_new_key_deletes() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let key_indices = [0];
        let old_row = test_row(1, "alice");
        let updated_row = test_row(2, "alice");
        let old_delete_row = delete_row(&old_row, &replicated_table_schema, &key_indices);
        let updated_delete_row = delete_row(&updated_row, &replicated_table_schema, &key_indices);
        let expected_updated_delete_row = updated_delete_row.clone();
        let mut table_delta = TableDelta::default();

        table_delta.update(
            old_delete_row.clone(),
            updated_row,
            &replicated_table_schema,
            &key_indices,
        );
        table_delta.delete(updated_delete_row, &key_indices);

        let (data_rows, delete_rows) = table_delta.into_parts();
        assert!(data_rows.is_empty());
        assert_eq!(delete_rows, vec![old_delete_row, expected_updated_delete_row]);
    }

    #[test]
    fn table_delta_composite_primary_key_keeps_distinct_rows() {
        let replicated_table_schema = replicated_composite_key_schema();
        let key_indices = [0, 1];
        let first_row = composite_key_row("tenant_1", 1, "alice");
        let second_row = composite_key_row("tenant_2", 1, "alice");
        let mut table_delta = TableDelta::default();

        table_delta.insert(first_row.clone(), &replicated_table_schema, &key_indices);
        table_delta.insert(second_row.clone(), &replicated_table_schema, &key_indices);

        let (mut data_rows, delete_rows) = table_delta.into_parts();
        data_rows.sort_by(|left, right| {
            format!("{:?}", left.values()).cmp(&format!("{:?}", right.values()))
        });

        assert_eq!(data_rows, vec![first_row, second_row]);
        assert_eq!(delete_rows.len(), 2);
    }

    #[test]
    fn table_delta_primary_key_update_chain_keeps_final_row() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let key_indices = [0];
        let row_1 = test_row(1, "alice");
        let row_2 = test_row(2, "alice");
        let row_3 = test_row(3, "alice");
        let delete_row_1 = delete_row(&row_1, &replicated_table_schema, &key_indices);
        let delete_row_2 = delete_row(&row_2, &replicated_table_schema, &key_indices);
        let mut table_delta = TableDelta::default();

        table_delta.update(delete_row_1.clone(), row_2, &replicated_table_schema, &key_indices);
        table_delta.update(delete_row_2, row_3.clone(), &replicated_table_schema, &key_indices);

        let (data_rows, delete_rows) = table_delta.into_parts();
        assert_eq!(data_rows, vec![row_3]);
        assert_eq!(
            delete_rows,
            vec![
                delete_row_1,
                delete_row(&test_row(2, "alice"), &replicated_table_schema, &key_indices),
                delete_row(&test_row(3, "alice"), &replicated_table_schema, &key_indices),
            ]
        );
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
