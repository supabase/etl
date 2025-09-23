use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
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

/// Extra CDC operation column values used for Iceberg ingestion
#[derive(Debug, Clone, Copy)]
enum IcebergOperationType {
    Upsert,
    Delete,
}

impl IcebergOperationType {
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

type IcebergTableName = String;

fn table_name_to_iceberg_table_name(table_name: &TableName) -> IcebergTableName {
    format!("{}_{}", table_name.schema, table_name.name)
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

#[derive(Debug)]
struct Inner {
    /// Cache of table names we already created/verified in the namespace
    created_tables: HashSet<IcebergTableName>,
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
                        // Delete-by-PK then insert full rows
                        client
                            .delete_rows(
                                namespace.clone(),
                                table_name.clone(),
                                pk_col_names,
                                pk_col_indexes.clone(),
                                &table_rows,
                            )
                            .await?;
                        // Deduplicate by PK keeping only the latest event (highest sequence_number)
                        let deduped_rows = IcebergDestination::<S>::dedup_latest_by_pk(
                            table_rows,
                            &pk_col_indexes,
                        );
                        client
                            .insert_rows(namespace, table_name, deduped_rows)
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

    /// Deduplicate table rows by primary key, keeping only the latest event per PK.
    ///
    /// Uses the provided primary key column indexes to identify duplicate rows and keeps
    /// the last occurrence of each primary key combination (last-write-wins semantics).
    ///
    /// Uses a struct containing the actual primary key cell values for correct equality comparison
    /// while still implementing efficient hashing.
    ///
    /// Assumptions:
    /// - `pk_col_indexes` refer to positions in the original table schema (before CDC columns),
    ///   which still match the same positions in `TableRow::values` since CDC columns are appended.
    fn dedup_latest_by_pk(table_rows: Vec<TableRow>, pk_col_indexes: &[usize]) -> Vec<TableRow> {
        // If no primary key columns specified, return all rows as-is
        if pk_col_indexes.is_empty() {
            return table_rows;
        }

        #[derive(Clone)]
        struct PrimaryKey {
            cells: Vec<Cell>,
        }

        impl PartialEq for PrimaryKey {
            fn eq(&self, other: &Self) -> bool {
                self.cells == other.cells
            }
        }

        impl Eq for PrimaryKey {}

        impl Hash for PrimaryKey {
            fn hash<H: Hasher>(&self, state: &mut H) {
                for cell in &self.cells {
                    hash_cell(cell, state);
                }
            }
        }

        fn hash_cell<H: Hasher>(cell: &Cell, hasher: &mut H) {
            match cell {
                Cell::Null => 0u8.hash(hasher),
                Cell::Bool(b) => (1u8, b).hash(hasher),
                Cell::String(s) => (2u8, s).hash(hasher),
                Cell::I16(i) => (3u8, i).hash(hasher),
                Cell::I32(i) => (4u8, i).hash(hasher),
                Cell::U32(u) => (5u8, u).hash(hasher),
                Cell::I64(i) => (6u8, i).hash(hasher),
                Cell::F32(f) => (7u8, f.to_bits()).hash(hasher),
                Cell::F64(f) => (8u8, f.to_bits()).hash(hasher),
                Cell::Numeric(n) => (9u8, format!("{:?}", n)).hash(hasher), // Fallback for complex type
                Cell::Date(d) => (10u8, format!("{:?}", d)).hash(hasher),
                Cell::Time(t) => (11u8, format!("{:?}", t)).hash(hasher), // Use debug format for simplicity
                Cell::Timestamp(ts) => (
                    12u8,
                    ts.and_utc().timestamp(),
                    ts.and_utc().timestamp_subsec_nanos(),
                )
                    .hash(hasher),
                Cell::TimestampTz(ts) => {
                    (13u8, ts.timestamp(), ts.timestamp_subsec_nanos()).hash(hasher)
                }
                Cell::Uuid(uuid) => (14u8, uuid.as_bytes()).hash(hasher),
                Cell::Json(json) => (15u8, json.to_string()).hash(hasher), // Fallback for complex type
                Cell::Bytes(bytes) => (16u8, bytes).hash(hasher),
                Cell::Array(arr) => (17u8, format!("{:?}", arr)).hash(hasher), // Fallback for complex type
            }
        }

        fn extract_primary_key(row_values: &[Cell], pk_col_indexes: &[usize]) -> PrimaryKey {
            let cells = pk_col_indexes
                .iter()
                .map(|&idx| row_values[idx].clone())
                .collect();
            PrimaryKey { cells }
        }

        // Key: actual PK cell values. Value: index into `deduped` vector.
        let mut index_by_pk: HashMap<PrimaryKey, usize> = HashMap::with_capacity(table_rows.len());
        let mut deduped: Vec<TableRow> = Vec::with_capacity(table_rows.len());

        for row in table_rows.into_iter() {
            let key = extract_primary_key(&row.values, pk_col_indexes);

            // Use last-write-wins semantics: replace any existing row with the same PK
            if let Some(&existing_idx) = index_by_pk.get(&key) {
                deduped[existing_idx] = row;
            } else {
                let idx = deduped.len();
                deduped.push(row);
                index_by_pk.insert(key, idx);
            }
        }

        deduped
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
