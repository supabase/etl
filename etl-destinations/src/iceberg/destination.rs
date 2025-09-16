use std::{collections::HashSet, fmt, sync::Arc};

use etl::{
    destination::Destination,
    error::{ErrorKind, EtlResult},
    etl_error,
    store::{schema::SchemaStore, state::StateStore},
    types::{Cell, Event, TableId, TableName, TableRow},
};
use tokio::sync::Mutex;

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
    // Delete,
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
            // IcebergOperationType::Delete => write!(f, "DELETE"),
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
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let iceberg_table_name = self.prepare_table_for_streaming(table_id).await?;

        // Add CDC operation type to all rows (no lock needed).
        for table_row in table_rows.iter_mut() {
            table_row
                .values
                .push(IcebergOperationType::Upsert.into_cell());
        }

        self.client
            .insert_rows(self.namespace.clone(), iceberg_table_name, &table_rows)
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

    async fn write_events(&self, _events: Vec<Event>) -> EtlResult<()> {
        Ok(())
    }
}
