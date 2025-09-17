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

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let iceberg_table_name = self.prepare_table_for_streaming(table_id).await?;

        self.client
            .insert_rows(self.namespace.clone(), iceberg_table_name, table_rows)
            .await?;

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
                        debug!("skipping unsupported event in BigQuery");
                    }
                }
            }

            // Process accumulated events for each table.
            if !table_id_to_table_rows.is_empty() {
                // let mut insert_rows_futures = Vec::with_capacity(table_id_to_table_rows.len());
                let mut join_set = JoinSet::new();

                for (table_id, table_rows) in table_id_to_table_rows {
                    let iceberg_table_name = self.prepare_cdc_table_for_streaming(table_id).await?;

                    let namespace = self.namespace.clone();
                    let client = self.client.clone();

                    join_set.spawn(async move {
                        client
                            .insert_rows(namespace, iceberg_table_name, table_rows)
                            .await
                    });
                    // let table_batch = self.client.create_table_batch(
                    //     &self.dataset_id,
                    //     &sequenced_bigquery_table_id.to_string(),
                    //     table_descriptor.clone(),
                    //     table_rows,
                    // )?;
                    // table_batches.push(table_batch);
                }
                while let Some(insert_result) = join_set.join_next().await {
                    insert_result
                        .map_err(|_| etl_error!(ErrorKind::Unknown, "Failed to join future"))??;
                    // batch_results.push(batch_result);

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

                // if !table_batches.is_empty() {
                // let (bytes_sent, bytes_received) = self
                //     .client
                //     .stream_table_batches_concurrent(table_batches, self.max_concurrent_streams)
                //     .await?;

                // }
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

            if !truncate_table_ids.is_empty() {
                // self.process_truncate_for_table_ids(truncate_table_ids.into_iter(), true)
                //     .await?;
            }
        }

        Ok(())
    }

    async fn prepare_cdc_table_for_streaming(
        &self,
        table_id: TableId,
    ) -> EtlResult<IcebergTableName> {
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

        let cdc_table_name = TableName {
            schema: table_schema.name.schema.clone(),
            name: format!("{}_cdc", table_schema.name.name),
        };
        let cdc_table_name = table_name_to_iceberg_table_name(&cdc_table_name);

        if inner.created_tables.contains(&cdc_table_name) {
            return Ok(cdc_table_name);
        }

        let cdc_table_schema = Self::derive_cdc_table_schema(&table_schema);
        self.client
            .create_table_if_missing(&self.namespace, cdc_table_name.clone(), &cdc_table_schema)
            .await
            .map_err(iceberg_error_to_etl_error)?;

        Self::add_to_created_tables_cache(&mut inner, &cdc_table_name);

        Ok(cdc_table_name)
    }

    /// Derive cdc table's schema from the final table's schema by:
    ///
    /// 1. Marking all columns as nullable apart from the primary key columns
    /// 2. Adding a cdc_operation column to represent the cdc operation type (upsert/delete)
    fn derive_cdc_table_schema(final_table_schema: &TableSchema) -> TableSchema {
        let mut cdc_table_schema = final_table_schema.clone();
        // Make all non-primary columns nullabe because the delete event will have null for those columns
        for column_schema in &mut cdc_table_schema.column_schemas {
            if !column_schema.primary {
                column_schema.nullable = true;
            }
        }
        cdc_table_schema.add_column_schema(ColumnSchema {
            name: "cdc_operation".to_string(), //TODO: fix the case when the source table already has a column with this name
            typ: Type::TEXT,
            modifier: -1,
            nullable: false,
            primary: false,
        });
        cdc_table_schema
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

    async fn truncate_table(&self, _table_id: etl::types::TableId) -> EtlResult<()> {
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
