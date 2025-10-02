use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use crate::iceberg::IcebergClient;
use crate::iceberg::error::iceberg_error_to_etl_error;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{
    Cell, ColumnSchema, Event, TableId, TableName, TableRow, TableSchema, Type,
    generate_sequence_number,
};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, info};

/// CDC operation types for Iceberg changelog tables.
///
/// Represents the type of change operation that occurred in the source database.
/// These values are stored in the `cdc_operation` column of changelog tables
/// to distinguish between data modifications and deletions.
#[derive(Debug, Clone, Copy)]
enum IcebergOperationType {
    /// Represents insert or update operations from the source database.
    /// Both inserts and updates are treated as upserts in the changelog.
    Upsert,
    /// Represents delete operations from the source database.
    /// Captures the final state of deleted rows for audit purposes.
    Delete,
}

impl IcebergOperationType {
    /// Converts the operation type into a table cell for storage.
    ///
    /// Transforms the enum variant into a string cell that can be inserted
    /// into the `cdc_operation` column of changelog tables.
    fn into_cell(self) -> Cell {
        Cell::String(self.to_string())
    }
}

impl fmt::Display for IcebergOperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IcebergOperationType::Upsert => write!(f, "UPSERT"),
            IcebergOperationType::Delete => write!(f, "DELETE"),
        }
    }
}

/// Type alias for Iceberg table names.
type IcebergTableName = String;

/// Converts a source table name to an Iceberg changelog table name.
///
/// Creates a standardized naming convention for Iceberg tables by combining
/// the schema and table name with a `_changelog` suffix to distinguish
/// CDC tables from regular data tables.
fn table_name_to_iceberg_table_name(table_name: &TableName) -> IcebergTableName {
    format!("{}_{}_changelog", table_name.schema, table_name.name)
}

/// An iceberg destination that implements the ETL [`Destination`] trait.
///
/// Provides Postgres-to-Iceberg data pipeline functionality including streaming inserts
/// and CDC operation handling.
///
/// Designed for high concurrency with minimal locking:
/// - Client is accessible without locks
/// - Only caches require synchronization
/// - Multiple write operations can execute concurrently
#[derive(Debug, Clone)]
pub struct IcebergDestination<S> {
    client: IcebergClient,
    namespace: String,
    store: S,
    inner: Arc<Mutex<Inner>>,
}

/// Internal state for the Iceberg destination.
///
/// Contains mutable state that requires synchronization across concurrent operations.
/// Wrapped in a mutex to ensure thread-safe access while keeping the main
/// destination struct cloneable.
#[derive(Debug)]
struct Inner {
    /// Cache of table names we already created/verified in the namespace.
    ///
    /// Prevents redundant table existence checks and creation attempts.
    /// Tables are added to this cache after successful creation or verification.
    created_tables: HashSet<IcebergTableName>,
}

impl<S> IcebergDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new Iceberg destination instance.
    ///
    /// Initializes the destination with an Iceberg client, target namespace,
    /// and state/schema store. The destination starts with an empty table
    /// creation cache and is ready to handle streaming operations.
    pub fn new(client: IcebergClient, namespace: String, store: S) -> IcebergDestination<S> {
        IcebergDestination {
            client,
            namespace,
            store,
            inner: Arc::new(Mutex::new(Inner {
                created_tables: HashSet::new(),
            })),
        }
    }

    /// Truncates an Iceberg table by dropping and recreating it.
    ///
    /// Removes all data from the target table by dropping the existing Iceberg table
    /// and creating a fresh empty table with the same schema. Updates the internal
    /// table creation cache to reflect the new table state.
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        if let Some(iceberg_table_name) = self.store.get_table_mapping(&table_id).await? {
            self.client
                .drop_table(&self.namespace, iceberg_table_name.clone())
                .await
                .map_err(iceberg_error_to_etl_error)?;

            inner.created_tables.remove(&iceberg_table_name);

            drop(inner);

            self.prepare_table_for_streaming(table_id).await?;
        }

        Ok(())
    }

    /// Writes table rows to the Iceberg destination as upsert operations.
    ///
    /// Prepares the target table for streaming, augments each row with CDC metadata
    /// (operation type and sequence number), and inserts the rows into the Iceberg table.
    /// All rows are treated as upsert operations in this context.
    async fn write_table_rows(
        &self,
        table_id: TableId,
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let iceberg_table_name = self.prepare_table_for_streaming(table_id).await?;

        for row in &mut table_rows {
            let sequence_number = generate_sequence_number(0.into(), 0.into());
            row.values.push(IcebergOperationType::Upsert.into_cell());
            row.values.push(Cell::String(sequence_number));
        }

        self.client
            .insert_rows(self.namespace.clone(), iceberg_table_name, table_rows)
            .await?;

        Ok(())
    }

    /// Processes and writes CDC events to Iceberg tables.
    ///
    /// Handles a stream of CDC events by batching non-truncate events by table ID
    /// and processing them concurrently. Truncate events are processed separately
    /// and deduplicated for efficiency. Each event is augmented with CDC metadata
    /// including operation type and sequence number based on LSN information.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_table_rows = HashMap::new();

            // Process events until we hit a truncate event or run out of events
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(mut insert) => {
                        let sequence_number =
                            generate_sequence_number(insert.start_lsn, insert.commit_lsn);
                        insert
                            .table_row
                            .values
                            .push(IcebergOperationType::Upsert.into_cell());
                        insert.table_row.values.push(Cell::String(sequence_number));

                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(insert.table_id).or_default();
                        table_rows.push(insert.table_row);
                    }
                    Event::Update(mut update) => {
                        let sequence_number =
                            generate_sequence_number(update.start_lsn, update.commit_lsn);
                        update
                            .table_row
                            .values
                            .push(IcebergOperationType::Upsert.into_cell());
                        update.table_row.values.push(Cell::String(sequence_number));

                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(update.table_id).or_default();
                        table_rows.push(update.table_row);
                    }
                    Event::Delete(delete) => {
                        let Some((_, mut old_table_row)) = delete.old_table_row else {
                            info!("the `DELETE` event has no row, so it was skipped");
                            continue;
                        };

                        let sequence_number =
                            generate_sequence_number(delete.start_lsn, delete.commit_lsn);
                        old_table_row
                            .values
                            .push(IcebergOperationType::Delete.into_cell());
                        old_table_row.values.push(Cell::String(sequence_number));

                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(delete.table_id).or_default();
                        table_rows.push(old_table_row);
                    }
                    _ => {
                        // Every other event type is currently not supported.
                        debug!("skipping unsupported event in iceberg");
                    }
                }
            }

            // Process accumulated events for each table.
            if !table_id_to_table_rows.is_empty() {
                let mut join_set = JoinSet::new();

                for (table_id, table_rows) in table_id_to_table_rows {
                    self.prepare_table_for_streaming(table_id).await?;
                    let iceberg_table_name =
                        self.store
                            .get_table_mapping(&table_id)
                            .await?
                            .ok_or(etl_error!(
                                ErrorKind::MissingTableMapping,
                                "Table mapping not found",
                                format!("The table mapping for table {table_id} was not found")
                            ))?;

                    let namespace = self.namespace.clone();
                    let client = self.client.clone();

                    join_set.spawn(async move {
                        client
                            .insert_rows(namespace, iceberg_table_name, table_rows)
                            .await
                    });
                }

                while let Some(insert_result) = join_set.join_next().await {
                    insert_result
                        .map_err(|_| etl_error!(ErrorKind::Unknown, "Failed to join future"))??;

                    //TODO:add egress metrics
                    // Logs with egress_metric = true can be used to identify egress logs.
                    // This can e.g. be used to send egress logs to a location different
                    // than the other logs. These logs should also have bytes_sent set to
                    // the number of bytes sent to the destination.
                    // info!(
                    //     bytes_sent,
                    //     bytes_received,
                    //     phase = "apply",
                    //     egress_metric = true,
                    //     "wrote cdc events to bigquery"
                    // );
                }
            }

            // Collect and deduplicate all table IDs from all truncate events.
            //
            // This is done as an optimization since if we have multiple table ids being truncated in a
            // row without applying other events in the meanwhile, it doesn't make any sense to create
            // new empty tables for each of them.
            let mut truncate_table_ids = HashSet::new();

            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for table_id in truncate_event.rel_ids {
                        truncate_table_ids.insert(TableId::new(table_id));
                    }
                }
            }

            for table_id in truncate_table_ids {
                self.truncate_table(table_id).await?;
            }
        }

        Ok(())
    }

    /// Prepares a table for CDC streaming operations with schema-aware table creation.
    ///
    /// Retrieves the table schema from the store, augments it with CDC columns,
    /// and ensures the corresponding Iceberg table exists in the namespace.
    /// Uses caching to avoid redundant table creation checks and holds a lock
    /// during the entire preparation to prevent race conditions.
    async fn prepare_table_for_streaming(&self, table_id: TableId) -> EtlResult<IcebergTableName> {
        // We hold the lock for the entire preparation to avoid race conditions since the consistency
        // of this code path is critical.
        let mut inner = self.inner.lock().await;

        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("No schema found for table {table_id}")
                )
            })?;

        let table_schema = Self::add_cdc_columns(&table_schema);

        let iceberg_table_name = table_name_to_iceberg_table_name(&table_schema.name);
        let iceberg_table_name = self
            .get_or_create_iceberg_table_name(&table_id, iceberg_table_name)
            .await?;

        if inner.created_tables.contains(&iceberg_table_name) {
            return Ok(iceberg_table_name);
        }

        self.client
            .create_table_if_missing(
                &self.namespace,
                iceberg_table_name.clone(),
                &table_schema.column_schemas,
            )
            .await
            .map_err(iceberg_error_to_etl_error)?;

        Self::add_to_created_tables_cache(&mut inner, &iceberg_table_name);

        Ok(iceberg_table_name)
    }

    /// Derives a CDC table schema by adding CDC-specific columns.
    ///
    /// Creates a new table schema based on the source table schema with two
    /// additional columns for CDC operations:
    /// - `cdc_operation`: Tracks whether the row represents an upsert or delete
    /// - `sequence_number`: Provides ordering information based on WAL LSN
    ///
    /// These columns enable CDC consumers to understand the chronological order
    /// of changes and distinguish between different types of operations.
    fn add_cdc_columns(table_schema: &TableSchema) -> TableSchema {
        let mut final_schema = table_schema.clone();
        // Add cdc specific columns
        final_schema.add_column_schema(ColumnSchema {
            name: "cdc_operation".to_string(), //TODO: fix the case when the source table already has a column with this name
            typ: Type::TEXT,
            modifier: -1,
            nullable: false,
            primary: false,
        });
        final_schema.add_column_schema(ColumnSchema {
            name: "sequence_number".to_string(), //TODO: fix the case when the source table already has a column with this name
            typ: Type::TEXT,
            modifier: -1,
            nullable: false,
            primary: false,
        });
        final_schema
    }

    /// Adds a table to the creation cache to avoid redundant existence checks.
    ///
    /// Updates the internal cache to mark a table as created or verified.
    /// This optimization prevents unnecessary table existence checks and creation
    /// attempts for tables that are already known to exist in the namespace.
    fn add_to_created_tables_cache(inner: &mut Inner, table_name: &IcebergTableName) {
        if inner.created_tables.contains(table_name) {
            return;
        }

        inner.created_tables.insert(table_name.clone());
    }

    /// Retrieves or creates a table mapping for the Iceberg table name.
    ///
    /// Checks if a table mapping already exists for the given table ID.
    /// If no mapping exists, creates a new mapping with the provided
    /// Iceberg table name. This ensures consistent table name resolution
    /// across multiple operations on the same logical table.
    async fn get_or_create_iceberg_table_name(
        &self,
        table_id: &TableId,
        iceberg_table_name: IcebergTableName,
    ) -> EtlResult<IcebergTableName> {
        let Some(iceberg_table_name) = self.store.get_table_mapping(table_id).await? else {
            self.store
                .store_table_mapping(*table_id, iceberg_table_name.to_string())
                .await?;

            return Ok(iceberg_table_name);
        };

        Ok(iceberg_table_name)
    }
}

impl<S> Destination for IcebergDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Returns the identifier name for this destination type.
    fn name() -> &'static str {
        "iceberg"
    }

    /// Truncates the specified table by dropping and recreating it.
    ///
    /// Removes all data from the target Iceberg table while preserving
    /// the table schema structure for continued CDC operations.
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.truncate_table(table_id).await?;

        Ok(())
    }

    /// Writes table rows to the destination as upsert operations.
    ///
    /// Augments each row with CDC metadata and inserts them into the
    /// corresponding Iceberg changelog table. All rows are treated
    /// as upsert operations with generated sequence numbers.
    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows(table_id, table_rows).await?;

        Ok(())
    }

    /// Processes and writes CDC events to the destination tables.
    ///
    /// Handles insert, update, delete, and truncate events by converting
    /// them to appropriate Iceberg operations. Events are batched by table
    /// and processed concurrently for optimal performance.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        self.write_events(events).await?;

        Ok(())
    }
}
