use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use etl::{
    destination::Destination,
    error::{ErrorKind, EtlResult},
    etl_error,
    store::{schema::SchemaStore, state::StateStore},
    types::{
        Cell, ColumnSchema, Event, TableId, TableName, TableRow, TableSchema, Type,
        generate_sequence_number,
    },
};
use tokio::{sync::Mutex, task::JoinSet};
use tracing::{debug, info};

use crate::iceberg::encoding::rows_to_record_batch;
use crate::iceberg::error::arrow_error_to_etl_error;
use arrow::datatypes::Schema as ArrowSchema;
use iceberg::arrow::schema_to_arrow_schema as iceberg_schema_to_arrow;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::iceberg::{IcebergClient, error::iceberg_error_to_etl_error};

/// Delimiter separating schema from table name in Iceberg table identifiers.
const ICEBERG_TABLE_ID_DELIMITER: &str = "_";
/// Replacement string for escaping underscores in PostgreSQL names.
const ICEBERG_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";

/// Returns the Iceberg table identifier for a supplied [`TableName`].
///
/// Escapes underscores in schema/table names to create valid Iceberg table identifiers.
pub fn table_name_to_iceberg_table_name(table_name: &TableName) -> IcebergTableName {
    let escaped_schema = table_name.schema.replace(
        ICEBERG_TABLE_ID_DELIMITER,
        ICEBERG_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
    );
    let escaped_table = table_name.name.replace(
        ICEBERG_TABLE_ID_DELIMITER,
        ICEBERG_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
    );

    format!("{escaped_schema}_{escaped_table}")
}

/// Change Data Capture operation types for iceberg streaming.
#[derive(Debug)]
pub enum IcebergOperationType {
    Upsert,
    Delete,
}

impl IcebergOperationType {
    /// Converts the operation type into a [`Cell`] for streaming.
    pub fn into_cell(self) -> Cell {
        Cell::String(self.to_string())
    }
}

impl fmt::Display for IcebergOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IcebergOperationType::Upsert => write!(f, "UPSERT"),
            IcebergOperationType::Delete => write!(f, "DELETE"),
        }
    }
}

/// Iceberg table identifier.
pub type IcebergTableName = String;

#[derive(Debug, Clone)]
struct Inner {
    /// Cache of table IDs that have been successfully created or verified to exist.
    created_tables: HashSet<IcebergTableName>,
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

impl<S> IcebergDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
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

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        if let Some(base_table_name) = self.store.get_table_mapping(&table_id).await? {
            self.client
                .drop_table(&self.namespace, base_table_name.clone())
                .await
                .map_err(iceberg_error_to_etl_error)?;

            inner.created_tables.remove(&base_table_name);

            drop(inner);

            self.prepare_table_for_streaming(table_id).await?;
        }

        Ok(())
    }

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
                // let mut insert_rows_futures = Vec::with_capacity(table_id_to_table_rows.len());
                let mut join_set = JoinSet::new();

                for (table_id, table_rows) in table_id_to_table_rows {
                    let table_name =
                        self.store
                            .get_table_mapping(&table_id)
                            .await?
                            .ok_or(etl_error!(
                                ErrorKind::MissingTableMapping,
                                "Table mapping not found",
                                format!("The table mapping for table {table_id} was not found")
                            ))?;

                    // Retrieve table schema to determine primary key columns
                    let table_schema =
                        self.store
                            .get_table_schema(&table_id)
                            .await?
                            .ok_or(etl_error!(
                                ErrorKind::MissingTableSchema,
                                "Table schema not found",
                                format!("No schema found for table {table_id}")
                            ))?;

                    // Collect primary key column names and their indexes in the original schema
                    let mut pk_col_names: Vec<String> = Vec::new();
                    let mut pk_col_indexes: Vec<usize> = Vec::new();
                    for (idx, col) in table_schema.column_schemas.iter().enumerate() {
                        if col.primary {
                            pk_col_names.push(col.name.clone());
                            pk_col_indexes.push(idx);
                        }
                    }

                    let namespace = self.namespace.clone();
                    let client = self.client.clone();

                    join_set.spawn(async move {
                        // If we have PKs and rows, write an equality delete file before inserting
                        if !pk_col_names.is_empty() && !table_rows.is_empty() {
                            // Load table to derive field IDs and Arrow schema with field metadata
                            let table = client
                                .load_table(namespace.clone(), table_name.clone())
                                .await
                                .map_err(iceberg_error_to_etl_error)?;

                            // Convert Iceberg schema to Arrow schema and extract field IDs from metadata
                            let iceberg_schema = table.metadata().current_schema();
                            let full_arrow_schema = iceberg_schema_to_arrow(iceberg_schema)
                                .map_err(iceberg_error_to_etl_error)?;
                            // Create a map name -> Field to pick by name
                            let mut name_to_field: HashMap<&str, arrow::datatypes::Field> =
                                HashMap::new();
                            for f in full_arrow_schema.fields() {
                                name_to_field.insert(f.name().as_str(), f.as_ref().clone());
                            }
                            // Build equality_ids in PK order using Arrow metadata keys
                            let mut equality_ids: Vec<i32> = Vec::with_capacity(pk_col_names.len());
                            for name in &pk_col_names {
                                match name_to_field
                                    .get(name.as_str())
                                    .and_then(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY))
                                    .and_then(|s| s.parse::<i32>().ok())
                                {
                                    Some(id) => equality_ids.push(id),
                                    None => {
                                        // If any PK column is missing or ID parsing fails, skip equality delete
                                        equality_ids.clear();
                                        break;
                                    }
                                }
                            }

                            if !equality_ids.is_empty() {
                                // Build a projected Arrow schema containing only PK fields, in the same
                                // order as equality_ids (which follows pk_col_names order)
                                let mut projected_fields: Vec<arrow::datatypes::Field> =
                                    Vec::with_capacity(pk_col_names.len());
                                for name in &pk_col_names {
                                    if let Some(f) = name_to_field.get(name.as_str()) {
                                        projected_fields.push(f.clone());
                                    }
                                }
                                let projected_arrow_schema = ArrowSchema::new(projected_fields);

                                // Build PK-only rows aligned with the projected schema order
                                let mut pk_only_rows: Vec<TableRow> =
                                    Vec::with_capacity(table_rows.len());
                                for row in &table_rows {
                                    let mut values = Vec::with_capacity(pk_col_indexes.len());
                                    for &pk_idx in &pk_col_indexes {
                                        // Safe: CDC columns are appended at the end, so original indexes remain valid
                                        values.push(row.values[pk_idx].clone());
                                    }
                                    pk_only_rows.push(TableRow { values });
                                }

                                // Convert to RecordBatch
                                let equality_rows =
                                    rows_to_record_batch(&pk_only_rows, projected_arrow_schema)
                                        .map_err(arrow_error_to_etl_error)?;

                                // Write equality delete file
                                client
                                    .write_equality_delete_file(&table, equality_ids, equality_rows)
                                    .await
                                    .map_err(iceberg_error_to_etl_error)?;
                            }
                        }

                        // Now insert the full rows
                        client.insert_rows(namespace, table_name, table_rows).await
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
    /// Retrieves the table schema from the store, creates or verifies the iceberg table exists.
    /// Uses caching to avoid redundant table creation checks.
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
            .create_table_if_missing(&self.namespace, iceberg_table_name.clone(), &table_schema)
            .await
            .map_err(iceberg_error_to_etl_error)?;

        Self::add_to_created_tables_cache(&mut inner, &iceberg_table_name);

        Ok(iceberg_table_name)
    }

    /// Derive cdc table's schema from the final table's schema by:
    ///
    /// 1. Marking all columns as nullable apart from the primary key columns
    /// 2. Adding a cdc_operation column to represent the cdc operation type (upsert/delete)
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
    fn add_to_created_tables_cache(inner: &mut Inner, table_name: &IcebergTableName) {
        if inner.created_tables.contains(table_name) {
            return;
        }

        inner.created_tables.insert(table_name.clone());
    }

    /// Retrieves the current iceberg table name stored in the table mapping,
    /// or updates the table mapping if the table name doesn't exist in it.
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
    fn name() -> &'static str {
        "iceberg"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.truncate_table(table_id).await?;

        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows(table_id, table_rows).await?;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        self.write_events(events).await?;

        Ok(())
    }
}
