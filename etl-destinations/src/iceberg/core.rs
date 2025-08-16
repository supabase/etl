//! Core Iceberg destination implementation mirroring BigQuery patterns.

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Cell, Event, PgLsn, TableId, TableName, TableRow};
use etl::{bail, etl_error};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::iceberg::client::{IcebergClient, IcebergOperationType};

/// Delimiter separating schema from table name in Iceberg table identifiers.
const ICEBERG_TABLE_ID_DELIMITER: &str = "_";
/// Replacement string for escaping underscores in PostgreSQL names.
const ICEBERG_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";

/// Creates a hex-encoded sequence number from PostgreSQL LSNs to ensure correct event ordering.
///
/// Uses the same format as BigQuery for consistency across destinations.
fn generate_sequence_number(start_lsn: PgLsn, commit_lsn: PgLsn) -> String {
    let start_lsn = u64::from(start_lsn);
    let commit_lsn = u64::from(commit_lsn);

    format!("{commit_lsn:016x}/{start_lsn:016x}")
}

/// Returns the Iceberg table identifier for a supplied [`TableName`].
///
/// Follows the same escaping strategy as BigQuery to ensure consistency.
pub fn table_name_to_iceberg_table_id(table_name: &TableName) -> IcebergTableId {
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

/// Iceberg table identifier.
pub type IcebergTableId = String;

/// An Iceberg table identifier with version sequence for truncate operations.
///
/// Mirrors BigQuery's versioning approach for truncate operations.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct SequencedIcebergTableId(IcebergTableId, u64);

impl SequencedIcebergTableId {
    /// Creates a new sequenced table ID starting at version 0.
    pub fn new(table_id: IcebergTableId) -> Self {
        Self(table_id, 0)
    }

    /// Returns the next version of this sequenced table ID.
    pub fn next(&self) -> Self {
        Self(self.0.clone(), self.1 + 1)
    }

    /// Extracts the base Iceberg table ID without the sequence number.
    pub fn to_iceberg_table_id(&self) -> IcebergTableId {
        self.0.clone()
    }
}

impl FromStr for SequencedIcebergTableId {
    type Err = EtlError;

    /// Parses a sequenced table ID from string format `table_name_sequence`.
    fn from_str(table_id: &str) -> Result<Self, Self::Err> {
        if let Some(last_underscore) = table_id.rfind('_') {
            let table_name = &table_id[..last_underscore];
            let sequence_str = &table_id[last_underscore + 1..];

            if table_name.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced Iceberg table ID format",
                    format!(
                        "Table name cannot be empty in sequenced table ID '{table_id}'. Expected format: 'table_name_sequence'"
                    )
                )
            }

            if sequence_str.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced Iceberg table ID format",
                    format!(
                        "Sequence number cannot be empty in sequenced table ID '{table_id}'. Expected format: 'table_name_sequence'"
                    )
                )
            }

            let sequence_number = sequence_str
                .parse::<u64>()
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationTableNameInvalid,
                        "Invalid sequence number in Iceberg table ID",
                        format!(
                            "Failed to parse sequence number '{sequence_str}' in table ID '{table_id}': {e}. Expected a non-negative integer (0-{max})",
                            max = u64::MAX
                        )
                    )
                })?;

            Ok(SequencedIcebergTableId(
                table_name.to_string(),
                sequence_number,
            ))
        } else {
            bail!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid sequenced Iceberg table ID format",
                format!(
                    "No underscore found in table ID '{table_id}'. Expected format: 'table_name_sequence' where sequence is a non-negative integer"
                )
            )
        }
    }
}

impl Display for SequencedIcebergTableId {
    /// Formats the sequenced table ID as `table_name_sequence`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.0, self.1)
    }
}

/// Internal state for [`IcebergDestination`] wrapped in `Arc<Mutex<>>`.
///
/// Mirrors BigQuery's internal structure for consistency.
#[derive(Debug)]
struct Inner<S> {
    client: IcebergClient,
    namespace: String,
    store: S,
    /// Cache of table IDs that have been successfully created or verified to exist.
    created_tables: HashSet<SequencedIcebergTableId>,
    /// Cache of current table versions for truncate handling.
    current_versions: HashMap<IcebergTableId, SequencedIcebergTableId>,
}

/// An Iceberg destination that implements the ETL [`Destination`] trait.
///
/// Provides PostgreSQL-to-Iceberg data pipeline functionality including streaming writes
/// and CDC operation handling, mirroring BigQuery's interface.
#[derive(Debug, Clone)]
pub struct IcebergDestination<S> {
    inner: Arc<Mutex<Inner<S>>>,
}

impl<S> IcebergDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new [`IcebergDestination`] with REST catalog configuration.
    ///
    /// Mirrors BigQuery's simple constructor pattern.
    pub async fn new(
        catalog_uri: String,
        warehouse: String,
        namespace: String,
        auth_token: Option<String>,
        store: S,
    ) -> EtlResult<Self> {
        let client = IcebergClient::new_with_rest_catalog(
            catalog_uri,
            warehouse,
            namespace.clone(),
            auth_token,
        ).await?;

        let inner = Inner {
            client,
            namespace,
            store,
            created_tables: HashSet::new(),
            current_versions: HashMap::new(),
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Prepares a table for CDC streaming operations with schema-aware table creation.
    ///
    /// Mirrors BigQuery's prepare_table_for_streaming method.
    async fn prepare_table_for_streaming(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        
        // Get table schema to access the TableName
        let table_schema = inner
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

        let iceberg_table_id = table_name_to_iceberg_table_id(&table_schema.name);
        let sequenced_table_id = inner
            .current_versions
            .get(&iceberg_table_id)
            .cloned()
            .unwrap_or_else(|| SequencedIcebergTableId::new(iceberg_table_id.clone()));

        // Check if table is already created
        if inner.created_tables.contains(&sequenced_table_id) {
            return Ok(());
        }

        // Create table if it doesn't exist
        inner
            .client
            .create_table_if_not_exists(&sequenced_table_id.to_string(), &table_schema)
            .await?;

        // Cache the created table
        inner.created_tables.insert(sequenced_table_id.clone());
        inner
            .current_versions
            .insert(iceberg_table_id, sequenced_table_id);

        Ok(())
    }

    /// Handles streaming write of table rows with CDC metadata.
    ///
    /// Mirrors BigQuery's streaming write pattern.
    async fn stream_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
        operation_type: IcebergOperationType,
        lsn: PgLsn,
    ) -> EtlResult<()> {
        // Ensure table exists
        self.prepare_table_for_streaming(table_id).await?;

        let inner = self.inner.lock().await;
        
        // Get table schema to access the TableName (we know it exists from prepare_table_for_streaming)
        let table_schema = inner
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
        
        let iceberg_table_id = table_name_to_iceberg_table_id(&table_schema.name);
        let sequenced_table_id = inner
            .current_versions
            .get(&iceberg_table_id)
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table not prepared for streaming",
                    format!("Table {table_id} not found in current versions")
                )
            })?;

        // Add CDC metadata to rows
        let mut enriched_rows = Vec::new();
        for mut row in rows {
            // Add CDC columns (matching BigQuery column names)
            row.values.push(operation_type.clone().into_cell());
            row.values.push(Cell::String(generate_sequence_number(lsn, lsn)));
            enriched_rows.push(row);
        }

        let row_count = enriched_rows.len();
        
        // Stream to Iceberg with retry logic for production robustness
        const MAX_RETRIES: u32 = 3;
        let mut retry_count = 0;
        
        loop {
            match inner
                .client
                .stream_rows(&sequenced_table_id.to_string(), enriched_rows.clone())
                .await
            {
                Ok(_) => {
                    info!(
                        table = %table_id,
                        iceberg_table = %sequenced_table_id,
                        rows = row_count,
                        "Successfully streamed rows to Iceberg table"
                    );
                    break;
                }
                Err(e) if retry_count < MAX_RETRIES => {
                    warn!(
                        table = %table_id,
                        error = %e,
                        retry = retry_count + 1,
                        "Failed to stream rows, retrying"
                    );
                    retry_count += 1;
                    
                    // Check if table needs to be recreated
                    if !inner.client.table_exists(&sequenced_table_id.to_string()).await? {
                        info!(
                            table = %table_id,
                            "Table missing, recreating before retry"
                        );
                        inner.client.create_table_if_not_exists(
                            &sequenced_table_id.to_string(),
                            &table_schema
                        ).await?;
                    }
                    
                    // Exponential backoff
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * (1 << retry_count))).await;
                }
                Err(e) => {
                    error!(
                        table = %table_id,
                        error = %e,
                        retries = retry_count,
                        "Failed to stream rows after retries"
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

impl<S> Destination for IcebergDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!(table = %table_id, "Truncating Iceberg table");

        let mut inner = self.inner.lock().await;
        
        // Get table schema for new table creation
        let table_schema = inner
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found for truncate",
                    format!("No schema found for table {table_id}")
                )
            })?;
        
        let iceberg_table_id = table_name_to_iceberg_table_id(&table_schema.name);
        
        // Get current version and create next version
        let current_version = inner
            .current_versions
            .get(&iceberg_table_id)
            .cloned()
            .unwrap_or_else(|| SequencedIcebergTableId::new(iceberg_table_id.clone()));
        
        let next_version = current_version.next();

        // Create new empty table with next version
        inner
            .client
            .create_table_if_not_exists(&next_version.to_string(), &table_schema)
            .await?;

        // Update current version tracking
        inner.current_versions.insert(iceberg_table_id, next_version.clone());
        inner.created_tables.insert(next_version.clone());

        // Remove old version from cache
        inner.created_tables.remove(&current_version);

        info!(
            table = %table_id,
            new_version = %next_version,
            "Successfully truncated Iceberg table by creating new version"
        );

        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, table_rows: Vec<TableRow>) -> EtlResult<()> {
        debug!(
            table = %table_id,
            row_count = table_rows.len(),
            "Writing table rows to Iceberg destination"
        );

        self.stream_table_rows(
            table_id,
            table_rows,
            IcebergOperationType::Upsert,
            PgLsn::from(0u64), // Initial sync doesn't have real LSN
        )
        .await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        debug!(event_count = events.len(), "Processing events for Iceberg destination");

        for event in events {
            match event {
                Event::Insert(insert) => {
                    self.stream_table_rows(
                        insert.table_id,
                        vec![insert.table_row],
                        IcebergOperationType::Upsert,
                        insert.start_lsn,
                    )
                    .await?;
                }
                Event::Update(update) => {
                    self.stream_table_rows(
                        update.table_id,
                        vec![update.table_row],
                        IcebergOperationType::Upsert,
                        update.start_lsn,
                    )
                    .await?;
                }
                Event::Delete(delete) => {
                    // For deletes, use the old_table_row data if available
                    if let Some((_, old_row)) = delete.old_table_row {
                        self.stream_table_rows(
                            delete.table_id,
                            vec![old_row],
                            IcebergOperationType::Delete,
                            delete.start_lsn,
                        )
                        .await?;
                    } else {
                        warn!("Delete event has no old_table_row data, skipping");
                    }
                }
                Event::Truncate(truncate) => {
                    // Truncate can affect multiple tables
                    for rel_id in &truncate.rel_ids {
                        // Convert rel_id to TableId
                        let table_id = TableId::new(*rel_id);
                        self.truncate_table(table_id).await?;
                    }
                }
                Event::Begin(_) | Event::Commit(_) => {
                    // Transaction boundaries - could be used for batching in future
                    debug!("Transaction boundary event processed");
                }
                Event::Relation(_) => {
                    // Schema change events - could be used for schema evolution in future
                    debug!("Schema relation event processed");
                }
                Event::Unsupported => {
                    // Unsupported events - log and continue
                    debug!("Unsupported event processed");
                }
            }
        }

        Ok(())
    }
}