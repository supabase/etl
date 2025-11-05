use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Cell, Event, TableId, TableName, TableRow, generate_sequence_number};
use etl::{bail, etl_error};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::databend::client::DatabendClient;

/// Delimiter separating schema from table name in Databend table identifiers.
const DATABEND_TABLE_ID_DELIMITER: &str = "_";
/// Replacement string for escaping underscores in Postgres names.
const DATABEND_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";

/// Special column name for Change Data Capture operations in Databend.
pub const DATABEND_CDC_OPERATION_COLUMN: &str = "_etl_operation";
/// Special column name for Change Data Capture sequence ordering in Databend.
pub const DATABEND_CDC_SEQUENCE_COLUMN: &str = "_etl_sequence";

/// CDC operation types for Databend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabendOperationType {
    Insert,
    Update,
    Delete,
}

impl DatabendOperationType {
    /// Converts the operation type into a [`Cell`] for insertion.
    pub fn into_cell(self) -> Cell {
        Cell::String(self.to_string())
    }
}

impl Display for DatabendOperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabendOperationType::Insert => write!(f, "INSERT"),
            DatabendOperationType::Update => write!(f, "UPDATE"),
            DatabendOperationType::Delete => write!(f, "DELETE"),
        }
    }
}

/// Returns the [`DatabendTableId`] for a supplied [`TableName`].
///
/// Escapes underscores in schema and table names to prevent collisions when combining them.
/// Original underscores become double underscores, and a single underscore separates schema from table.
/// This ensures that `a_b.c` and `a.b_c` map to different Databend table names.
pub fn table_name_to_databend_table_id(table_name: &TableName) -> String {
    let escaped_schema = table_name.schema.replace(
        DATABEND_TABLE_ID_DELIMITER,
        DATABEND_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
    );
    let escaped_table = table_name.name.replace(
        DATABEND_TABLE_ID_DELIMITER,
        DATABEND_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
    );

    format!("{}{}{}", escaped_schema, DATABEND_TABLE_ID_DELIMITER, escaped_table)
}

/// A Databend table identifier with version sequence for truncate operations.
///
/// Combines a base table name with a sequence number to enable versioned tables.
/// Used for truncate handling where each truncate creates a new table version.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct SequencedDatabendTableId(String, u64);

impl SequencedDatabendTableId {
    /// Creates a new sequenced table ID starting at version 0.
    pub fn new(table_id: String) -> Self {
        Self(table_id, 0)
    }

    /// Returns the next version of this sequenced table ID.
    pub fn next(&self) -> Self {
        Self(self.0.clone(), self.1 + 1)
    }

    /// Extracts the base Databend table ID without the sequence number.
    pub fn to_databend_table_id(&self) -> String {
        self.0.clone()
    }
}

impl FromStr for SequencedDatabendTableId {
    type Err = EtlError;

    /// Parses a sequenced table ID from string format `table_name_sequence`.
    fn from_str(table_id: &str) -> Result<Self, Self::Err> {
        if let Some(last_underscore) = table_id.rfind('_') {
            let table_name = &table_id[..last_underscore];
            let sequence_str = &table_id[last_underscore + 1..];

            if table_name.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced Databend table ID format",
                    format!(
                        "Table name cannot be empty in sequenced table ID '{table_id}'. Expected format: 'table_name_sequence'"
                    )
                )
            }

            if sequence_str.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced Databend table ID format",
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
                        "Invalid sequence number in Databend table ID",
                        format!(
                            "Failed to parse sequence number '{sequence_str}' in table ID '{table_id}': {e}. Expected a non-negative integer (0-{max})",
                            max = u64::MAX
                        )
                    )
                })?;

            Ok(SequencedDatabendTableId(
                table_name.to_string(),
                sequence_number,
            ))
        } else {
            bail!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid sequenced Databend table ID format",
                format!(
                    "No underscore found in table ID '{table_id}'. Expected format: 'table_name_sequence' where sequence is a non-negative integer"
                )
            )
        }
    }
}

impl Display for SequencedDatabendTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.0, self.1)
    }
}

/// Internal state for [`DatabendDestination`] wrapped in `Arc<Mutex<>>`.
#[derive(Debug)]
struct Inner {
    /// Cache of table IDs that have been successfully created or verified to exist.
    created_tables: HashSet<SequencedDatabendTableId>,
    /// Cache of views that have been created and the versioned table they point to.
    created_views: HashMap<String, SequencedDatabendTableId>,
}

/// A Databend destination that implements the ETL [`Destination`] trait.
///
/// Provides Postgres-to-Databend data pipeline functionality including batch inserts
/// and CDC operation handling.
#[derive(Debug, Clone)]
pub struct DatabendDestination<S> {
    client: DatabendClient,
    store: S,
    inner: Arc<Mutex<Inner>>,
}

impl<S> DatabendDestination<S>
where
    S: StateStore + SchemaStore,
{
    /// Creates a new [`DatabendDestination`] with a DSN connection string.
    ///
    /// The DSN should follow Databend's connection string format:
    /// `databend://<user>:<password>@<host>:<port>/<database>?<params>`
    pub async fn new(dsn: String, database: String, store: S) -> EtlResult<Self> {
        let client = DatabendClient::new(dsn, database).await?;
        let inner = Inner {
            created_tables: HashSet::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            client,
            store,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Prepares a table for data operations with schema-aware table creation.
    ///
    /// Retrieves the table schema from the store, creates or verifies the Databend table exists,
    /// and ensures the view points to the current versioned table.
    async fn prepare_table_for_operations(
        &self,
        table_id: &TableId,
        with_cdc_columns: bool,
    ) -> EtlResult<SequencedDatabendTableId> {
        let mut inner = self.inner.lock().await;

        // Load the schema of the table
        let table_schema = self
            .store
            .get_table_schema(table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table not found in the schema store",
                    format!(
                        "The table schema for table {table_id} was not found in the schema store"
                    )
                )
            })?;

        // Determine the Databend table ID with the current sequence number
        let databend_table_id = table_name_to_databend_table_id(&table_schema.name);
        let sequenced_databend_table_id = self
            .get_or_create_sequenced_databend_table_id(table_id, &databend_table_id)
            .await?;

        // Skip table creation if we've already seen this sequenced table
        if !inner.created_tables.contains(&sequenced_databend_table_id) {
            // Prepare column schemas with CDC columns if needed
            let mut column_schemas = table_schema.column_schemas.clone();
            if with_cdc_columns {
                column_schemas.push(etl::types::ColumnSchema {
                    name: DATABEND_CDC_OPERATION_COLUMN.to_string(),
                    typ: etl::types::Type::Text,
                    optional: false,
                });
                column_schemas.push(etl::types::ColumnSchema {
                    name: DATABEND_CDC_SEQUENCE_COLUMN.to_string(),
                    typ: etl::types::Type::Text,
                    optional: false,
                });
            }

            self.client
                .create_table_if_missing(
                    &sequenced_databend_table_id.to_string(),
                    &column_schemas,
                )
                .await?;

            Self::add_to_created_tables_cache(&mut inner, &sequenced_databend_table_id);

            debug!("sequenced table {} added to creation cache", sequenced_databend_table_id);
        } else {
            debug!(
                "sequenced table {} found in creation cache, skipping existence check",
                sequenced_databend_table_id
            );
        }

        // Ensure view points to this sequenced table
        self.ensure_view_points_to_table(
            &mut inner,
            &databend_table_id,
            &sequenced_databend_table_id,
        )
        .await?;

        Ok(sequenced_databend_table_id)
    }

    /// Adds a table to the creation cache.
    fn add_to_created_tables_cache(inner: &mut Inner, table_id: &SequencedDatabendTableId) {
        if inner.created_tables.contains(table_id) {
            return;
        }

        inner.created_tables.insert(table_id.clone());
    }

    /// Retrieves the current sequenced table ID or creates a new one starting at version 0.
    async fn get_or_create_sequenced_databend_table_id(
        &self,
        table_id: &TableId,
        databend_table_id: &str,
    ) -> EtlResult<SequencedDatabendTableId> {
        let Some(sequenced_databend_table_id) =
            self.get_sequenced_databend_table_id(table_id).await?
        else {
            let sequenced_databend_table_id =
                SequencedDatabendTableId::new(databend_table_id.to_string());
            self.store
                .store_table_mapping(*table_id, sequenced_databend_table_id.to_string())
                .await?;

            return Ok(sequenced_databend_table_id);
        };

        Ok(sequenced_databend_table_id)
    }

    /// Retrieves the current sequenced table ID from the state store.
    async fn get_sequenced_databend_table_id(
        &self,
        table_id: &TableId,
    ) -> EtlResult<Option<SequencedDatabendTableId>> {
        let Some(current_table_id) = self.store.get_table_mapping(table_id).await? else {
            return Ok(None);
        };

        let sequenced_databend_table_id = current_table_id.parse()?;

        Ok(Some(sequenced_databend_table_id))
    }

    /// Ensures a view points to the specified target table, creating or updating as needed.
    async fn ensure_view_points_to_table(
        &self,
        inner: &mut Inner,
        view_name: &str,
        target_table_id: &SequencedDatabendTableId,
    ) -> EtlResult<bool> {
        if let Some(current_target) = inner.created_views.get(view_name)
            && current_target == target_table_id
        {
            debug!(
                "view {} already points to {}, skipping creation",
                view_name, target_table_id
            );

            return Ok(false);
        }

        // Create a view using CREATE OR REPLACE VIEW
        let view_full_name = self.client.full_table_name(view_name);
        let target_full_name = self.client.full_table_name(&target_table_id.to_string());

        let query = format!(
            "CREATE OR REPLACE VIEW {} AS SELECT * FROM {}",
            view_full_name, target_full_name
        );

        // Use client's internal execute method through a workaround
        // Since we don't have direct access to the driver client, we'll use the client's execute method
        let conn_result = databend_driver::Client::new(self.client.dsn.clone()).get_conn().await;
        let conn = conn_result.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationWriteFailed,
                "Failed to get Databend connection",
                e.to_string()
            )
        })?;

        conn.exec(&query).await.map_err(|e| {
            etl_error!(
                ErrorKind::DestinationWriteFailed,
                "Failed to create or replace view",
                e.to_string()
            )
        })?;

        inner
            .created_views
            .insert(view_name.to_string(), target_table_id.clone());

        debug!(
            "view {} created/updated to point to {}",
            view_name, target_table_id
        );

        Ok(true)
    }

    /// Writes table rows for initial table synchronization.
    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let sequenced_databend_table_id = self.prepare_table_for_operations(&table_id, false).await?;

        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("Table schema for table {} not found", table_id)
                )
            })?;

        let rows_inserted = self
            .client
            .insert_rows(
                &sequenced_databend_table_id.to_string(),
                &table_schema.column_schemas,
                table_rows,
            )
            .await?;

        info!(
            rows = rows_inserted,
            phase = "table_copy",
            "wrote table rows to Databend"
        );

        Ok(())
    }

    /// Processes CDC events in batches with proper ordering and truncate handling.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_table_rows: HashMap<TableId, Vec<TableRow>> = HashMap::new();

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
                            .push(DatabendOperationType::Insert.into_cell());
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
                            .push(DatabendOperationType::Update.into_cell());
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
                            .push(DatabendOperationType::Delete.into_cell());
                        old_table_row.values.push(Cell::String(sequence_number));

                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(delete.table_id).or_default();
                        table_rows.push(old_table_row);
                    }
                    _ => {
                        debug!("skipping unsupported event in Databend");
                    }
                }
            }

            // Process accumulated events for each table
            if !table_id_to_table_rows.is_empty() {
                for (table_id, table_rows) in table_id_to_table_rows {
                    let sequenced_databend_table_id =
                        self.prepare_table_for_operations(&table_id, true).await?;

                    let table_schema = self
                        .store
                        .get_table_schema(&table_id)
                        .await?
                        .ok_or_else(|| {
                            etl_error!(
                                ErrorKind::MissingTableSchema,
                                "Table schema not found",
                                format!("Table schema for table {} not found", table_id)
                            )
                        })?;

                    // Add CDC columns to schema for validation
                    let mut column_schemas = table_schema.column_schemas.clone();
                    column_schemas.push(etl::types::ColumnSchema {
                        name: DATABEND_CDC_OPERATION_COLUMN.to_string(),
                        typ: etl::types::Type::Text,
                        optional: false,
                    });
                    column_schemas.push(etl::types::ColumnSchema {
                        name: DATABEND_CDC_SEQUENCE_COLUMN.to_string(),
                        typ: etl::types::Type::Text,
                        optional: false,
                    });

                    self.client
                        .insert_rows(
                            &sequenced_databend_table_id.to_string(),
                            &column_schemas,
                            table_rows,
                        )
                        .await?;
                }

                info!(
                    phase = "apply",
                    "wrote CDC events to Databend"
                );
            }

            // Collect and deduplicate all table IDs from all truncate events
            let mut truncate_table_ids = HashSet::new();

            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for table_id in truncate_event.rel_ids {
                        truncate_table_ids.insert(TableId::new(table_id));
                    }
                }
            }

            if !truncate_table_ids.is_empty() {
                self.process_truncate_for_table_ids(truncate_table_ids.into_iter(), true)
                    .await?;
            }
        }

        Ok(())
    }

    /// Handles table truncation by creating new versioned tables and updating views.
    async fn process_truncate_for_table_ids(
        &self,
        table_ids: impl IntoIterator<Item = TableId>,
        is_cdc_truncate: bool,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        for table_id in table_ids {
            let table_schema = self.store.get_table_schema(&table_id).await?;

            if !is_cdc_truncate {
                if table_schema.is_none() {
                    warn!(
                        "the table schema for table {} was not found in the schema store while processing truncate events for Databend",
                        table_id.to_string()
                    );
                    continue;
                }
            }

            let table_schema = table_schema.ok_or_else(|| etl_error!(
                ErrorKind::MissingTableSchema,
                "Table not found in the schema store",
                format!(
                    "The table schema for table {} was not found in the schema store while processing truncate events for Databend",
                    table_id.to_string()
                )
            ))?;

            let sequenced_databend_table_id =
                self.get_sequenced_databend_table_id(&table_id)
                    .await?
                    .ok_or_else(|| etl_error!(
                        ErrorKind::MissingTableMapping,
                        "Table mapping not found",
                        format!(
                            "The table mapping for table id {} was not found while processing truncate events for Databend",
                            table_id.to_string()
                        )
                    ))?;

            let next_sequenced_databend_table_id = sequenced_databend_table_id.next();

            info!(
                "processing truncate for table {}: creating new version {}",
                table_id, next_sequenced_databend_table_id
            );

            // Create or replace the new table
            self.client
                .create_or_replace_table(
                    &next_sequenced_databend_table_id.to_string(),
                    &table_schema.column_schemas,
                )
                .await?;
            Self::add_to_created_tables_cache(&mut inner, &next_sequenced_databend_table_id);

            // Update the view to point to the new table
            self.ensure_view_points_to_table(
                &mut inner,
                &sequenced_databend_table_id.to_databend_table_id(),
                &next_sequenced_databend_table_id,
            )
            .await?;

            // Update the store table mappings
            self.store
                .store_table_mapping(table_id, next_sequenced_databend_table_id.to_string())
                .await?;

            info!(
                "successfully processed truncate for {}: new table {}, view updated",
                table_id, next_sequenced_databend_table_id
            );

            // Remove the old table from the cache
            inner.created_tables.remove(&sequenced_databend_table_id);

            // Schedule cleanup of the previous table
            let client = self.client.clone();
            tokio::spawn(async move {
                if let Err(err) = client
                    .drop_table(&sequenced_databend_table_id.to_string())
                    .await
                {
                    warn!(
                        "failed to drop previous table {}: {}",
                        sequenced_databend_table_id, err
                    );
                } else {
                    info!(
                        "successfully cleaned up previous table {}",
                        sequenced_databend_table_id
                    );
                }
            });
        }

        Ok(())
    }
}

impl<S> Destination for DatabendDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "databend"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.process_truncate_for_table_ids(iter::once(table_id), false)
            .await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_to_databend_table_id_no_underscores() {
        let table_name = TableName::new("schema".to_string(), "table".to_string());
        assert_eq!(table_name_to_databend_table_id(&table_name), "schema_table");
    }

    #[test]
    fn test_table_name_to_databend_table_id_with_underscores() {
        let table_name = TableName::new("a_b".to_string(), "c_d".to_string());
        assert_eq!(table_name_to_databend_table_id(&table_name), "a__b_c__d");
    }

    #[test]
    fn test_table_name_to_databend_table_id_collision_prevention() {
        let table_name1 = TableName::new("a_b".to_string(), "c".to_string());
        let table_name2 = TableName::new("a".to_string(), "b_c".to_string());

        let id1 = table_name_to_databend_table_id(&table_name1);
        let id2 = table_name_to_databend_table_id(&table_name2);

        assert_eq!(id1, "a__b_c");
        assert_eq!(id2, "a_b__c");
        assert_ne!(id1, id2, "Table IDs should not collide");
    }

    #[test]
    fn test_sequenced_databend_table_id_new() {
        let table_id = SequencedDatabendTableId::new("users_table".to_string());
        assert_eq!(table_id.to_databend_table_id(), "users_table");
        assert_eq!(table_id.1, 0);
    }

    #[test]
    fn test_sequenced_databend_table_id_next() {
        let table_id = SequencedDatabendTableId::new("users_table".to_string());
        let next_table_id = table_id.next();

        assert_eq!(table_id.1, 0);
        assert_eq!(next_table_id.1, 1);
        assert_eq!(next_table_id.to_databend_table_id(), "users_table");
    }

    #[test]
    fn test_sequenced_databend_table_id_from_str() {
        let table_id = "users_table_123";
        let parsed = table_id.parse::<SequencedDatabendTableId>().unwrap();
        assert_eq!(parsed.to_databend_table_id(), "users_table");
        assert_eq!(parsed.1, 123);
    }

    #[test]
    fn test_sequenced_databend_table_id_display() {
        let table_id = SequencedDatabendTableId("users_table".to_string(), 123);
        assert_eq!(table_id.to_string(), "users_table_123");
    }

    #[test]
    fn test_databend_operation_type_display() {
        assert_eq!(DatabendOperationType::Insert.to_string(), "INSERT");
        assert_eq!(DatabendOperationType::Update.to_string(), "UPDATE");
        assert_eq!(DatabendOperationType::Delete.to_string(), "DELETE");
    }
}
