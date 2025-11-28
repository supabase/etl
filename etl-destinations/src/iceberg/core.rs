use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use crate::iceberg::IcebergClient;
use crate::iceberg::error::iceberg_error_to_etl_error;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{
    Cell, ColumnSchema, Event, TableId, TableName, TableRow, TableSchema, Type,
    generate_sequence_number,
};
use etl::{bail, etl_error};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::log::warn;
use tracing::{debug, info};

/// CDC operation types for Iceberg changelog tables.
///
/// Represents the type of change operation that occurred in the source database.
/// These values are stored in the `cdc_operation` column of changelog tables
/// to distinguish between data modifications and deletions.
#[derive(Debug, Clone, Copy)]
pub enum IcebergOperationType {
    /// Represents insert operations from the source database.
    Insert,
    /// Represents update operations from the source database.
    Update,
    /// Represents delete operations from the source database.
    Delete,
}

impl From<IcebergOperationType> for Cell {
    fn from(value: IcebergOperationType) -> Self {
        Cell::String(value.to_string())
    }
}

impl fmt::Display for IcebergOperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IcebergOperationType::Insert => write!(f, "INSERT"),
            IcebergOperationType::Update => write!(f, "UPDATE"),
            IcebergOperationType::Delete => write!(f, "DELETE"),
        }
    }
}

/// Type alias for Iceberg table names.
type IcebergTableName = String;

/// Delimiter separating schema from table name in iceberg table identifiers.
const ICEBERG_TABLE_ID_DELIMITER: &str = "_";
/// Replacement string for escaping underscores in Postgres names.
const ICEBERG_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";
/// Suffix for changelog tables
const ICEBERG_CHANGELOG_TABLE_SUFFIX: &str = "changelog";

/// CDC operation column name
const CDC_OPERATION_COLUMN_NAME: &str = "cdc_operation";
/// CDC operation column name
const SEQUENCE_NUMBER_COLUMN_NAME: &str = "sequence_number";

/// Converts a source table name to an Iceberg changelog table name.
///
/// Creates a standardized naming convention for Iceberg tables by combining
/// the schema and table name with a `_changelog` suffix to distinguish
/// CDC tables from regular data tables.
pub fn table_name_to_iceberg_table_name(
    table_name: &TableName,
    single_destination_namespace: bool,
) -> IcebergTableName {
    if single_destination_namespace {
        let escaped_schema = table_name.schema.replace(
            ICEBERG_TABLE_ID_DELIMITER,
            ICEBERG_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
        );
        let escaped_table = table_name.name.replace(
            ICEBERG_TABLE_ID_DELIMITER,
            ICEBERG_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
        );

        format!("{escaped_schema}_{escaped_table}_{ICEBERG_CHANGELOG_TABLE_SUFFIX}")
    } else {
        format!("{}_{ICEBERG_CHANGELOG_TABLE_SUFFIX}", table_name.name)
    }
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
    store: S,
    inner: Arc<Mutex<Inner>>,
}

/// Namespace in the destination where the tables will be copied
#[derive(Debug)]
pub enum DestinationNamespace {
    /// A single namespace for all tables in the source
    Single(String),
    /// One namespace for each schema in the source
    OnePerSchema,
}

impl DestinationNamespace {
    fn get_or<'a>(&'a self, table_namespace: &'a str) -> &'a str {
        match self {
            DestinationNamespace::Single(ns) => ns,
            DestinationNamespace::OnePerSchema => table_namespace,
        }
    }

    pub fn is_single(&self) -> bool {
        match self {
            DestinationNamespace::Single(_) => true,
            DestinationNamespace::OnePerSchema => false,
        }
    }
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

    /// Cache of namespaces we already created/verified in the destination
    ///
    /// Prevents redundant namespace existence checks and creation attempts.
    /// Namespaces are added to this cache after successful creation or verification.
    created_namespaces: HashSet<String>,

    /// Namespace where the tables will be replicated. Depending on the variant either
    /// all tables will go in one namespace or there will be one namespace per
    /// source schema.
    namespace: DestinationNamespace,
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
        }
    }

    /// Truncates an Iceberg table by dropping and recreating it.
    ///
    /// Removes all data from the target table by dropping the existing Iceberg table
    /// and creating a fresh empty table with the same schema. Updates the internal
    /// table creation cache to reflect the new table state.
    async fn truncate_table(&self, table_id: TableId, is_cdc_truncate: bool) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        let Some(table_schema) = self.store.get_table_schema(&table_id).await? else {
            // If this is not a cdc truncate event, we just raise a warning since it could be that the
            // table schema is not there.
            if !is_cdc_truncate {
                warn!(
                    "the table schema for table {table_id} was not found in the schema store while processing truncate events for Iceberg",
                );

                return Ok(());
            }

            // If this is a cdc truncate event, the table schema must be there, so we raise an error.
            bail!(
                ErrorKind::MissingTableSchema,
                "Table not found in the schema store",
                format!(
                    "The table schema for table {table_id} was not found in the schema store while processing truncate events for Iceberg"
                )
            );
        };

        let Some(iceberg_table_name) = self.store.get_table_mapping(&table_id).await? else {
            // If this is not a cdc truncate event, we just raise a warning since it could be that the
            // table mapping is not there.
            if !is_cdc_truncate {
                warn!(
                    "the table mapping for table {table_id} was not found in the state store while processing truncate events for Iceberg",
                );

                return Ok(());
            }

            // If this is a cdc truncate event, the table mapping must be there, so we raise an error.
            bail!(
                ErrorKind::MissingTableMapping,
                "Table mapping not found",
                format!(
                    "The table mapping for table id {table_id} was not found while processing truncate events for Iceberg"
                )
            );
        };

        let namespace = schema_to_namespace(&table_schema.name.schema);
        let namespace = inner.namespace.get_or(&namespace);

        self.client
            .drop_table_if_exists(namespace, iceberg_table_name.clone())
            .await
            .map_err(iceberg_error_to_etl_error)?;
        inner.created_tables.remove(&iceberg_table_name);

        // We recreate the table with the same schema.
        self.prepare_table_for_streaming(&mut inner, table_id)
            .await?;

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
        let (namespace, iceberg_table_name) = {
            // We hold the lock for the entire preparation to avoid race conditions since the consistency
            // of this code path is critical.
            let mut inner = self.inner.lock().await;
            self.prepare_table_for_streaming(&mut inner, table_id)
                .await?
        };

        for row in &mut table_rows {
            let sequence_number = generate_sequence_number(0.into(), 0.into());
            row.values.push(IcebergOperationType::Insert.into());
            row.values.push(Cell::String(sequence_number));
        }

        if !table_rows.is_empty() {
            let bytes_sent = self
                .client
                .insert_rows(namespace, iceberg_table_name, table_rows)
                .await?;

            // Logs with egress_metric = true can be used to identify egress logs.
            // This can e.g. be used to send egress logs to a location different
            // than the other logs. These logs should also have bytes_sent set to
            // the number of bytes sent to the destination.
            info!(
                bytes_sent,
                phase = "table_copy",
                egress_metric = true,
                "wrote table rows to iceberg"
            );
        }

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
                            .push(IcebergOperationType::Insert.into());
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
                            .push(IcebergOperationType::Update.into());
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
                            .push(IcebergOperationType::Delete.into());
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
                    let (namespace, iceberg_table_name) = {
                        // We hold the lock for the entire preparation to avoid race conditions since the consistency
                        // of this code path is critical.
                        let mut inner = self.inner.lock().await;
                        self.prepare_table_for_streaming(&mut inner, table_id)
                            .await?
                    };

                    let client = self.client.clone();

                    join_set.spawn(async move {
                        client
                            .insert_rows(namespace, iceberg_table_name, table_rows)
                            .await
                    });
                }

                let mut bytes_sent = 0;
                while let Some(insert_result) = join_set.join_next().await {
                    bytes_sent += insert_result
                        .map_err(|_| etl_error!(ErrorKind::Unknown, "Failed to join future"))??;
                }

                // Logs with egress_metric = true can be used to identify egress logs.
                // This can e.g. be used to send egress logs to a location different
                // than the other logs. These logs should also have bytes_sent set to
                // the number of bytes sent to the destination.
                info!(
                    bytes_sent,
                    phase = "apply",
                    egress_metric = true,
                    "wrote cdc events to iceberg"
                );
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
                self.truncate_table(table_id, true).await?;
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
    async fn prepare_table_for_streaming(
        &self,
        inner: &mut Inner,
        table_id: TableId,
    ) -> EtlResult<(String, IcebergTableName)> {
        let table_schema = self.get_table_schema(table_id).await?;
        let table_schema = Self::modify_schema_with_cdc_columns(&table_schema);

        let iceberg_table_name =
            table_name_to_iceberg_table_name(&table_schema.name, inner.namespace.is_single());
        let iceberg_table_name = self
            .get_or_create_iceberg_table_name(&table_id, iceberg_table_name)
            .await?;

        let namespace = schema_to_namespace(&table_schema.name.schema);
        let namespace = inner.namespace.get_or(&namespace).to_string();
        let namespace = self.create_namespace_if_missing(inner, namespace).await?;

        let iceberg_table_name = self
            .create_table_if_missing(inner, iceberg_table_name, &namespace, &table_schema)
            .await?;

        Ok((namespace, iceberg_table_name))
    }

    async fn get_table_schema(&self, table_id: TableId) -> EtlResult<Arc<TableSchema>> {
        self.store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("No schema found for table {table_id}")
                )
            })
    }

    /// Creates a namespace if it is missing in the destination.
    /// Once created adds it to the created_namesapces HashMap to
    /// avoid creating it again.
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

    /// Creates a table if it is missing in the destination.
    /// Once created adds it to the created_trees HashMap to
    /// avoid creating it again.
    async fn create_table_if_missing(
        &self,
        inner: &mut Inner,
        iceberg_table_name: String,
        namespace: &str,
        table_schema: &TableSchema,
    ) -> EtlResult<String> {
        if inner.created_tables.contains(&iceberg_table_name) {
            return Ok(iceberg_table_name);
        }

        self.client
            .create_table_if_missing(
                namespace,
                iceberg_table_name.clone(),
                &table_schema.column_schemas,
            )
            .await
            .map_err(iceberg_error_to_etl_error)?;

        inner.created_tables.insert(iceberg_table_name.clone());

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
    fn modify_schema_with_cdc_columns(table_schema: &TableSchema) -> TableSchema {
        let mut final_schema = table_schema.clone();

        // Add cdc specific columns.
        let cdc_operation_col =
            find_unique_column_name(&final_schema.column_schemas, CDC_OPERATION_COLUMN_NAME);
        let sequence_number_col =
            find_unique_column_name(&final_schema.column_schemas, SEQUENCE_NUMBER_COLUMN_NAME);

        final_schema.add_column_schema(ColumnSchema::new(
            cdc_operation_col,
            Type::TEXT,
            -1,
            0,
            None,
            false,
            true,
        ));
        final_schema.add_column_schema(ColumnSchema::new(
            sequence_number_col,
            Type::TEXT,
            -1,
            0,
            None,
            false,
            true,
        ));
        final_schema
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
        self.truncate_table(table_id, false).await?;

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

/// Creates a unique columns name with prefix `new_column_name` to avoid collissions with
/// existing columns in `column_schemas` by adding a numeric suffix.
fn find_unique_column_name(column_schemas: &[ColumnSchema], new_column_name: &str) -> String {
    let mut suffix = None;

    loop {
        let final_name = match suffix {
            Some(s) => format!("{new_column_name}_{s}"),
            None => new_column_name.to_string(),
        };

        let found = column_schemas.iter().any(|cs| cs.name == final_name);
        if found {
            if let Some(s) = &mut suffix {
                *s += 1;
            } else {
                suffix = Some(1);
            }
        } else {
            return final_name;
        }
    }
}

/// Converts a Postgres schema name to a S3 tables namespace
/// such that the naming rules of the namespace are followed.
fn schema_to_namespace(schema: &str) -> String {
    // Convert to lowercase and replace invalid characters with underscores.
    // S3 Tables namespaces can only contain lowercase letters, numbers, and underscores.
    let mut namespace: String = schema
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
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
    use etl::types::{ColumnSchema, Type};

    use crate::iceberg::core::{
        CDC_OPERATION_COLUMN_NAME, find_unique_column_name, schema_to_namespace,
    };

    /// Creates a test column schema with common defaults.
    ///
    /// This helper simplifies column schema creation in tests by providing sensible
    /// defaults for fields that are typically not relevant to the test logic.
    fn test_column(name: &str, typ: Type, nullable: bool, primary_key: bool) -> ColumnSchema {
        ColumnSchema::new(
            name.to_string(),
            typ,
            -1,
            0,
            if primary_key { Some(1) } else { None },
            nullable,
            true,
        )
    }

    #[test]
    fn can_find_unique_column_name() {
        let column_schemas = vec![];
        let col_name = find_unique_column_name(&column_schemas, CDC_OPERATION_COLUMN_NAME);
        assert_eq!(col_name, CDC_OPERATION_COLUMN_NAME.to_string());

        let column_schemas = vec![test_column("id", Type::BOOL, false, true)];
        let col_name = find_unique_column_name(&column_schemas, CDC_OPERATION_COLUMN_NAME);
        assert_eq!(col_name, CDC_OPERATION_COLUMN_NAME.to_string());

        let column_schemas = vec![
            test_column("id", Type::BOOL, false, true),
            test_column(CDC_OPERATION_COLUMN_NAME, Type::BOOL, false, true),
        ];
        let col_name = find_unique_column_name(&column_schemas, CDC_OPERATION_COLUMN_NAME);
        assert_eq!(col_name, format!("{CDC_OPERATION_COLUMN_NAME}_1"));

        let column_schemas = vec![
            test_column("id", Type::BOOL, false, true),
            test_column(CDC_OPERATION_COLUMN_NAME, Type::BOOL, false, true),
            test_column(
                &format!("{CDC_OPERATION_COLUMN_NAME}_1"),
                Type::BOOL,
                false,
                true,
            ),
        ];
        let col_name = find_unique_column_name(&column_schemas, CDC_OPERATION_COLUMN_NAME);
        assert_eq!(col_name, format!("{CDC_OPERATION_COLUMN_NAME}_2"));
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
        assert_eq!(
            schema_to_namespace("my-complex.schema$name"),
            "my_complex_schema_name"
        );
    }

    #[test]
    fn schema_to_namespace_non_ascii_replaced() {
        // Non-ASCII characters should be replaced with underscores (one char = one underscore).
        assert_eq!(schema_to_namespace("schémas"), "sch_mas");
        // All non-ASCII becomes underscores, then fixed to start/end with letter/number.
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
        assert_eq!(
            schema_to_namespace("information_schema"),
            "information_schema"
        );
    }
}
