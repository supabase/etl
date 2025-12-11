use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::{DestinationSchemaState, DestinationSchemaStateType, StateStore};
use etl::types::{
    Cell, Event, ReplicatedTableSchema, SchemaDiff, TableId, TableName, TableRow,
    generate_sequence_number,
};
use etl::{bail, etl_error};
use gcp_bigquery_client::storage::TableDescriptor;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::bigquery::client::{BigQueryClient, BigQueryOperationType};
use crate::bigquery::{BigQueryDatasetId, BigQueryTableId};

/// Delimiter separating schema from table name in BigQuery table identifiers.
const BIGQUERY_TABLE_ID_DELIMITER: &str = "_";
/// Replacement string for escaping underscores in Postgres names.
const BIGQUERY_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";

/// Returns the [`BigQueryTableId`] for a supplied [`TableName`].
///
/// Escapes underscores in schema and table names to prevent collisions when combining them.
/// Original underscores become double underscores, and a single underscore separates schema from table.
/// This ensures that `a_b.c` and `a.b_c` map to different BigQuery table names.
///
/// We opted for this escaping strategy since it's easy to undo on the reading end. Just split at a
/// single `_` and revert each `__` into `_`.
///
/// BigQuery accepts up to 1024 UTF-8 characters, whereas Postgres names operate with a maximum size
/// determined by `NAMEDATALEN`. We assume that most people are running this as default value, which
/// is 63, meaning that in the worst case of a schema name and table name containing only _, the resulting
/// string will be made up of (63 * 2) + 1 + (63 * 2) = 253 characters which is much less than 1024.
pub fn table_name_to_bigquery_table_id(table_name: &TableName) -> BigQueryTableId {
    let escaped_schema = table_name.schema.replace(
        BIGQUERY_TABLE_ID_DELIMITER,
        BIGQUERY_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
    );
    let escaped_table = table_name.name.replace(
        BIGQUERY_TABLE_ID_DELIMITER,
        BIGQUERY_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT,
    );

    format!("{escaped_schema}_{escaped_table}")
}

/// A BigQuery table identifier with version sequence for truncate operations.
///
/// Combines a base table name with a sequence number to enable versioned tables.
/// Used for truncate handling where each truncate creates a new table version.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct SequencedBigQueryTableId(BigQueryTableId, u64);

impl SequencedBigQueryTableId {
    /// Creates a new sequenced table ID starting at version 0.
    pub fn new(table_id: BigQueryTableId) -> Self {
        Self(table_id, 0)
    }

    /// Returns the next version of this sequenced table ID.
    pub fn next(&self) -> Self {
        Self(self.0.clone(), self.1 + 1)
    }

    /// Extracts the base BigQuery table ID without the sequence number.
    pub fn to_bigquery_table_id(&self) -> BigQueryTableId {
        self.0.clone()
    }
}

impl FromStr for SequencedBigQueryTableId {
    type Err = EtlError;

    /// Parses a sequenced table ID from string format `table_name_sequence`.
    ///
    /// Expects the last underscore to separate the table name from the sequence number.
    fn from_str(table_id: &str) -> Result<Self, Self::Err> {
        if let Some(last_underscore) = table_id.rfind('_') {
            let table_name = &table_id[..last_underscore];
            let sequence_str = &table_id[last_underscore + 1..];

            if table_name.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced BigQuery table ID format",
                    format!(
                        "Table name cannot be empty in sequenced table ID '{table_id}'. Expected format: 'table_name_sequence'"
                    )
                )
            }

            if sequence_str.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced BigQuery table ID format",
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
                        "Invalid sequence number in BigQuery table ID",
                        format!(
                            "Failed to parse sequence number '{sequence_str}' in table ID '{table_id}': {e}. Expected a non-negative integer (0-{max})",
                            max = u64::MAX
                        )
                    )
                })?;

            Ok(SequencedBigQueryTableId(
                table_name.to_string(),
                sequence_number,
            ))
        } else {
            bail!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid sequenced BigQuery table ID format",
                format!(
                    "No underscore found in table ID '{table_id}'. Expected format: 'table_name_sequence' where sequence is a non-negative integer"
                )
            )
        }
    }
}

impl Display for SequencedBigQueryTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.0, self.1)
    }
}

/// Internal state for [`BigQueryDestination`] wrapped in `Arc<Mutex<>>`.
///
/// Contains caches and state that require synchronization across concurrent operations.
/// The main BigQuery client and configuration are stored directly in the outer struct
/// to allow lock-free access during streaming operations.
#[derive(Debug)]
struct Inner {
    /// Cache of table IDs that have been successfully created or verified to exist.
    /// This avoids redundant `create_table_if_missing` calls for known tables.
    created_tables: HashSet<SequencedBigQueryTableId>,
    /// Cache of views that have been created and the versioned table they point to.
    /// This avoids redundant `CREATE OR REPLACE VIEW` calls for views that already point to the correct table.
    /// Maps view name to the versioned table it currently points to.
    ///
    /// # Example
    /// `{ users_table: users_table_10, orders_table: orders_table_3 }`
    created_views: HashMap<BigQueryTableId, SequencedBigQueryTableId>,
}

/// A BigQuery destination that implements the ETL [`Destination`] trait.
///
/// Provides Postgres-to-BigQuery data pipeline functionality including streaming inserts
/// and CDC operation handling.
///
/// Designed for high concurrency with minimal locking:
/// - Configuration and client are accessible without locks
/// - Only caches and state mappings require synchronization
/// - Multiple write operations can execute concurrently
#[derive(Debug, Clone)]
pub struct BigQueryDestination<S> {
    client: BigQueryClient,
    dataset_id: BigQueryDatasetId,
    max_staleness_mins: Option<u16>,
    max_concurrent_streams: usize,
    state_store: S,
    inner: Arc<Mutex<Inner>>,
}

impl<S> BigQueryDestination<S>
where
    S: StateStore + SchemaStore,
{
    /// Creates a new [`BigQueryDestination`] using a service account key file path.
    ///
    /// Initializes the BigQuery client with the provided credentials and project settings.
    /// The `max_staleness_mins` parameter controls table metadata cache freshness.
    /// The `max_concurrent_streams` parameter controls parallelism for streaming operations
    /// and determines how table rows are split into batches for concurrent processing.
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
        max_concurrent_streams: usize,
        state_store: S,
    ) -> EtlResult<Self> {
        let client = BigQueryClient::new_with_key_path(project_id, sa_key).await?;
        let inner = Inner {
            created_tables: HashSet::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            max_concurrent_streams,
            state_store,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Creates a new [`BigQueryDestination`] using a service account key JSON string.
    ///
    /// Similar to [`BigQueryDestination::new_with_key_path`] but accepts the key content directly
    /// rather than a file path. Useful when credentials are stored in environment variables.
    /// The `max_concurrent_streams` parameter controls parallelism for streaming operations
    /// and determines how table rows are split into batches for concurrent processing.
    pub async fn new_with_key(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
        max_concurrent_streams: usize,
        state_store: S,
    ) -> EtlResult<Self> {
        let client = BigQueryClient::new_with_key(project_id, sa_key).await?;
        let inner = Inner {
            created_tables: HashSet::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            max_concurrent_streams,
            state_store,
            inner: Arc::new(Mutex::new(inner)),
        })
    }
    /// Creates a new [`BigQueryDestination`] using Application Default Credentials (ADC).
    ///
    /// Initializes the BigQuery client with the default credentials and project settings.
    /// The `max_staleness_mins` parameter controls table metadata cache freshness.
    /// The `max_concurrent_streams` parameter controls parallelism for streaming operations
    /// and determines how table rows are split into batches for concurrent processing.
    pub async fn new_with_adc(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        max_staleness_mins: Option<u16>,
        max_concurrent_streams: usize,
        state_store: S,
    ) -> EtlResult<Self> {
        let client = BigQueryClient::new_with_adc(project_id).await?;
        let inner = Inner {
            created_tables: HashSet::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            max_concurrent_streams,
            state_store,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Creates a new [`BigQueryDestination`] using an installed flow authenticator.
    ///
    /// Initializes the BigQuery client with a flow authenticator using the provided secret and persistent file path.
    /// The `max_staleness_mins` parameter controls table metadata cache freshness.
    /// The `max_concurrent_streams` parameter controls parallelism for streaming operations
    /// and determines how table rows are split into batches for concurrent processing.
    pub async fn new_with_flow_authenticator<Secret, Path>(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        secret: Secret,
        persistent_file_path: Path,
        max_staleness_mins: Option<u16>,
        max_concurrent_streams: usize,
        store: S,
    ) -> EtlResult<Self>
    where
        Secret: AsRef<[u8]>,
        Path: Into<std::path::PathBuf>,
    {
        let client =
            BigQueryClient::new_with_flow_authenticator(project_id, secret, persistent_file_path)
                .await?;
        let inner = Inner {
            created_tables: HashSet::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            max_concurrent_streams,
            state_store: store,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Prepares a table for CDC streaming operations with schema-aware table creation.
    ///
    /// Creates or verifies the BigQuery table exists using the provided schema,
    /// and ensures the view points to the current versioned table. Uses caching to avoid
    /// redundant table creation checks.
    async fn prepare_table_for_streaming(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<(SequencedBigQueryTableId, Arc<TableDescriptor>)> {
        // We hold the lock for the entire preparation to avoid race conditions since the consistency
        // of this code path is critical.
        let mut inner = self.inner.lock().await;

        let table_id = replicated_table_schema.id();

        // We determine the BigQuery table ID for the table together with the current sequence number.
        let bigquery_table_id = table_name_to_bigquery_table_id(replicated_table_schema.name());
        let sequenced_bigquery_table_id = self
            .get_or_create_sequenced_bigquery_table_id(&table_id, &bigquery_table_id)
            .await?;

        // Optimistically skip table creation if we've already seen this sequenced table.
        //
        // Note that if the table is deleted outside ETL and the cache marks it as created, the
        // inserts will fail because the table will be missing and won't be created.
        if !inner.created_tables.contains(&sequenced_bigquery_table_id) {
            self.client
                .create_table_if_missing(
                    &self.dataset_id,
                    // TODO: down the line we might want to reduce an allocation here.
                    &sequenced_bigquery_table_id.to_string(),
                    replicated_table_schema,
                    self.max_staleness_mins,
                )
                .await?;

            // Add the sequenced table to the cache.
            Self::add_to_created_tables_cache(&mut inner, &sequenced_bigquery_table_id);

            debug!("sequenced table {sequenced_bigquery_table_id} added to creation cache");
        } else {
            debug!(
                "sequenced table {sequenced_bigquery_table_id} found in creation cache, skipping existence check"
            );
        }

        // Ensure view points to this sequenced table (uses cache to avoid redundant operations)
        self.ensure_view_points_to_table(
            &mut inner,
            &bigquery_table_id,
            &sequenced_bigquery_table_id,
        )
        .await?;

        let table_descriptor = BigQueryClient::column_schemas_to_table_descriptor(
            replicated_table_schema,
            use_cdc_sequence_column,
        );

        Ok((sequenced_bigquery_table_id, Arc::new(table_descriptor)))
    }

    /// Adds a table to the creation cache to avoid redundant existence checks.
    fn add_to_created_tables_cache(inner: &mut Inner, table_id: &SequencedBigQueryTableId) {
        if inner.created_tables.contains(table_id) {
            return;
        }

        inner.created_tables.insert(table_id.clone());
    }

    /// Retrieves the current sequenced table ID or creates a new one starting at version 0.
    async fn get_or_create_sequenced_bigquery_table_id(
        &self,
        table_id: &TableId,
        bigquery_table_id: &BigQueryTableId,
    ) -> EtlResult<SequencedBigQueryTableId> {
        let Some(sequenced_bigquery_table_id) =
            self.get_sequenced_bigquery_table_id(table_id).await?
        else {
            let sequenced_bigquery_table_id =
                SequencedBigQueryTableId::new(bigquery_table_id.clone());
            self.state_store
                .store_table_mapping(*table_id, sequenced_bigquery_table_id.to_string())
                .await?;

            return Ok(sequenced_bigquery_table_id);
        };

        Ok(sequenced_bigquery_table_id)
    }

    /// Retrieves the current sequenced table ID from the state store.
    async fn get_sequenced_bigquery_table_id(
        &self,
        table_id: &TableId,
    ) -> EtlResult<Option<SequencedBigQueryTableId>> {
        let Some(current_table_id) = self.state_store.get_table_mapping(table_id).await? else {
            return Ok(None);
        };

        let sequenced_bigquery_table_id = current_table_id.parse()?;

        Ok(Some(sequenced_bigquery_table_id))
    }

    /// Ensures a view points to the specified target table, creating or updating as needed.
    ///
    /// Returns `true` if the view was created or updated, `false` if already correct.
    async fn ensure_view_points_to_table(
        &self,
        inner: &mut Inner,
        view_name: &BigQueryTableId,
        target_table_id: &SequencedBigQueryTableId,
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

        self.client
            .create_or_replace_view(&self.dataset_id, view_name, &target_table_id.to_string())
            .await?;

        // We insert/overwrite the new (view -> sequenced bigquery table id) mapping
        inner
            .created_views
            .insert(view_name.clone(), target_table_id.clone());

        debug!(
            "view {} created/updated to point to {}",
            view_name, target_table_id
        );

        Ok(true)
    }

    /// Writes table rows with CDC metadata for non-event streaming operations.
    ///
    /// Adds an `Upsert` operation type to each row, splits them into optimal batches based on
    /// `max_concurrent_streams`, and streams to BigQuery using concurrent processing.
    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // Prepare table for streaming.
        let (sequenced_bigquery_table_id, table_descriptor) = self
            .prepare_table_for_streaming(replicated_table_schema, false)
            .await?;

        // Add the CDC operation type to all rows (no lock needed).
        for table_row in table_rows.iter_mut() {
            table_row
                .values
                .push(BigQueryOperationType::Upsert.into_cell());
        }

        // Split table rows into optimal batches for parallel execution.
        let table_rows_batches = split_table_rows(table_rows, self.max_concurrent_streams);

        // Create table batches from the split rows.
        let mut table_batches = Vec::with_capacity(table_rows_batches.len());
        for table_rows in table_rows_batches {
            if !table_rows.is_empty() {
                let table_batch = self.client.create_table_batch(
                    &self.dataset_id,
                    &sequenced_bigquery_table_id.to_string(),
                    table_descriptor.clone(),
                    table_rows,
                )?;
                table_batches.push(table_batch);
            }
        }

        // Stream all the batches concurrently.
        if !table_batches.is_empty() {
            let (bytes_sent, bytes_received) = self
                .client
                .stream_table_batches_concurrent(table_batches, self.max_concurrent_streams)
                .await?;

            // Logs with egress_metric = true can be used to identify egress logs.
            // This can e.g. be used to send egress logs to a location different
            // than the other logs. These logs should also have bytes_sent set to
            // the number of bytes sent to the destination.
            info!(
                bytes_sent,
                bytes_received,
                phase = "table_copy",
                egress_metric = true,
                "wrote table rows to bigquery"
            );
        }

        Ok(())
    }

    /// Handles a schema change event (Relation) by computing the diff and applying changes.
    ///
    /// This method:
    /// 1. Gets the current destination schema state from the state store.
    /// 2. If no state exists, this is the initial schema - just records it as applied.
    /// 3. If state exists and snapshot_id differs, computes the diff and applies changes.
    /// 4. If state is in `Applying`, a previous change was interrupted - returns an error.
    async fn handle_relation_event(
        &self,
        new_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = new_schema.id();
        let new_snapshot_id = new_schema.get_inner().snapshot_id;

        // Get current destination schema state.
        let current_state = self
            .state_store
            .get_destination_schema_state(&table_id)
            .await?;

        match current_state {
            None => {
                // No state exists - this is a broken invariant since the schema should
                // have been recorded during write_table_rows. For now, just record it.
                warn!(
                    "no destination schema state found for table {} - recording initial state (snapshot_id: {})",
                    table_id, new_snapshot_id
                );

                self.state_store
                    .store_destination_schema_state(
                        table_id,
                        DestinationSchemaState {
                            state: DestinationSchemaStateType::Applied,
                            snapshot_id: new_snapshot_id,
                        },
                    )
                    .await?;
            }
            Some(state) if state.state == DestinationSchemaStateType::Applying => {
                // A previous schema change was interrupted - require manual intervention.
                // The previous valid snapshot_id can be derived from table_schemas table
                // by finding the second-highest snapshot_id for this table.
                bail!(
                    ErrorKind::InvalidState,
                    "Schema change recovery required",
                    format!(
                        "A previous schema change for table {} was interrupted at snapshot_id {}. \
                         Manual intervention is required to resolve the destination schema state. \
                         The previous valid snapshot can be derived from the table_schemas table.",
                        table_id, state.snapshot_id
                    )
                );
            }
            Some(state) if state.state == DestinationSchemaStateType::Applied => {
                let current_snapshot_id = state.snapshot_id;

                if current_snapshot_id == new_snapshot_id {
                    // Schema hasn't changed - nothing to do.
                    debug!(
                        "schema for table {} unchanged (snapshot_id: {})",
                        table_id, new_snapshot_id
                    );
                    return Ok(());
                }

                info!(
                    "schema change detected for table {}: {} -> {}",
                    table_id, current_snapshot_id, new_snapshot_id
                );

                // Get the old schema from the schema store to compute the diff.
                let old_table_schema = self
                    .state_store
                    .get_table_schema(&table_id, current_snapshot_id)
                    .await?
                    .ok_or_else(|| {
                        etl_error!(
                            ErrorKind::InvalidState,
                            "Old schema not found",
                            format!(
                                "Could not find schema for table {} at snapshot_id {}",
                                table_id, current_snapshot_id
                            )
                        )
                    })?;

                // Build a ReplicatedTableSchema from the old TableSchema.
                // For diffing purposes, we consider all columns as replicated.
                let old_schema = ReplicatedTableSchema::all(old_table_schema);

                // Mark as applying before making changes (with the NEW snapshot_id).
                self.state_store
                    .store_destination_schema_state(
                        table_id,
                        DestinationSchemaState {
                            state: DestinationSchemaStateType::Applying,
                            snapshot_id: new_snapshot_id,
                        },
                    )
                    .await?;

                // Compute and apply the diff.
                let diff = old_schema.diff(new_schema);
                if let Err(err) = self.apply_schema_diff(&table_id, &diff).await {
                    warn!(
                        "schema change failed for table {}: {}. Manual intervention may be required.",
                        table_id, err
                    );
                    return Err(err);
                }

                // Mark as applied after successful changes.
                self.state_store
                    .store_destination_schema_state(
                        table_id,
                        DestinationSchemaState {
                            state: DestinationSchemaStateType::Applied,
                            snapshot_id: new_snapshot_id,
                        },
                    )
                    .await?;

                info!(
                    "schema change completed for table {}: snapshot_id {} applied",
                    table_id, new_snapshot_id
                );
            }
            Some(_) => unreachable!("All state types are covered"),
        }

        Ok(())
    }

    /// Applies a schema diff to the BigQuery table.
    ///
    /// Executes the necessary DDL operations (ADD COLUMN, DROP COLUMN, RENAME COLUMN)
    /// to transform the destination schema.
    async fn apply_schema_diff(&self, table_id: &TableId, diff: &SchemaDiff) -> EtlResult<()> {
        if diff.is_empty() {
            debug!("no schema changes to apply for table {}", table_id);
            return Ok(());
        }

        // Get the BigQuery table ID for this table.
        let bigquery_table_id = self
            .get_sequenced_bigquery_table_id(table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Table not found",
                    format!(
                        "No BigQuery table mapping found for table {}. Schema changes cannot be applied to a non-existent table.",
                        table_id
                    )
                )
            })?;

        info!(
            "applying schema changes to table {}: {} additions, {} removals, {} renames",
            bigquery_table_id,
            diff.columns_to_add.len(),
            diff.columns_to_remove.len(),
            diff.columns_to_rename.len()
        );

        // Apply column additions first (safest operation).
        for column in &diff.columns_to_add {
            self.client
                .add_column(&self.dataset_id, &bigquery_table_id.to_string(), column)
                .await?;
        }

        // Apply column renames (must be done before removals in case of position conflicts).
        for rename in &diff.columns_to_rename {
            self.client
                .rename_column(
                    &self.dataset_id,
                    &bigquery_table_id.to_string(),
                    &rename.old_name,
                    &rename.new_name,
                )
                .await?;
        }

        // Apply column removals last.
        for column in &diff.columns_to_remove {
            self.client
                .drop_column(&self.dataset_id, &bigquery_table_id.to_string(), &column.name)
                .await?;
        }

        info!(
            "schema changes applied successfully to table {}",
            bigquery_table_id
        );

        Ok(())
    }

    /// Processes CDC events in batches with proper ordering and truncate handling.
    ///
    /// Groups streaming operations (insert/update/delete) by table and processes them together,
    /// then handles truncate events separately by creating new versioned tables. Uses the schema
    /// from the first event of each table for table creation and descriptor building.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut events_iter = events.into_iter().peekable();

        while events_iter.peek().is_some() {
            // Maps table ID to (schema, rows); schema is the first one seen for that table. Once
            // schema change support is implemented, we will re-implement this.
            let mut table_id_to_data: HashMap<TableId, (ReplicatedTableSchema, Vec<TableRow>)> =
                HashMap::new();

            // Process events until we hit a truncate event or run out of events
            while let Some(event) = events_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let event = events_iter.next().unwrap();
                match event {
                    Event::Insert(mut insert) => {
                        let sequence_number =
                            generate_sequence_number(insert.start_lsn, insert.commit_lsn);
                        insert
                            .table_row
                            .values
                            .push(BigQueryOperationType::Upsert.into_cell());
                        insert.table_row.values.push(Cell::String(sequence_number));

                        let table_id = insert.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (insert.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(insert.table_row);
                    }
                    Event::Update(mut update) => {
                        let sequence_number =
                            generate_sequence_number(update.start_lsn, update.commit_lsn);
                        update
                            .table_row
                            .values
                            .push(BigQueryOperationType::Upsert.into_cell());
                        update.table_row.values.push(Cell::String(sequence_number));

                        let table_id = update.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (update.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(update.table_row);
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
                            .push(BigQueryOperationType::Delete.into_cell());
                        old_table_row.values.push(Cell::String(sequence_number));

                        let table_id = delete.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (delete.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(old_table_row);
                    }
                    Event::Relation(relation) => {
                        // Handle schema change event.
                        self.handle_relation_event(&relation.replicated_table_schema)
                            .await?;
                    }
                    _ => {
                        // Begin, Commit, Unsupported events are skipped.
                        debug!("skipping non-data event in BigQuery");
                    }
                }
            }

            // Process accumulated events for each table.
            if !table_id_to_data.is_empty() {
                let mut table_batches = Vec::with_capacity(table_id_to_data.len());

                for (_, (replicated_table_schema, table_rows)) in table_id_to_data {
                    let (sequenced_bigquery_table_id, table_descriptor) = self
                        .prepare_table_for_streaming(&replicated_table_schema, true)
                        .await?;

                    let table_batch = self.client.create_table_batch(
                        &self.dataset_id,
                        &sequenced_bigquery_table_id.to_string(),
                        table_descriptor.clone(),
                        table_rows,
                    )?;
                    table_batches.push(table_batch);
                }

                if !table_batches.is_empty() {
                    let (bytes_sent, bytes_received) = self
                        .client
                        .stream_table_batches_concurrent(table_batches, self.max_concurrent_streams)
                        .await?;

                    // Logs with egress_metric = true can be used to identify egress logs.
                    // This can e.g. be used to send egress logs to a location different
                    // than the other logs. These logs should also have bytes_sent set to
                    // the number of bytes sent to the destination.
                    info!(
                        bytes_sent,
                        bytes_received,
                        phase = "apply",
                        egress_metric = true,
                        "wrote cdc events to bigquery"
                    );
                }
            }

            // Collect and deduplicate schemas from all truncate events.
            //
            // This is done as an optimization since if we have multiple tables being truncated in a
            // row without applying other events in the meanwhile, it doesn't make any sense to create
            // new empty tables for each of them.
            let mut truncate_schemas: HashMap<TableId, ReplicatedTableSchema> = HashMap::new();

            while let Some(Event::Truncate(_)) = events_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = events_iter.next() {
                    for schema in truncate_event.truncated_tables {
                        truncate_schemas.insert(schema.id(), schema);
                    }
                }
            }

            if !truncate_schemas.is_empty() {
                self.process_truncate_for_schemas(truncate_schemas.into_values())
                    .await?;
            }
        }

        Ok(())
    }

    /// Handles table truncation by creating new versioned tables and updating views.
    ///
    /// Creates fresh empty tables with incremented version numbers, updates views to point
    /// to new tables, and schedules cleanup of old table versions. Uses the provided schemas
    /// directly instead of looking them up from a store.
    async fn process_truncate_for_schemas(
        &self,
        replicated_table_schemas: impl IntoIterator<Item = ReplicatedTableSchema>,
    ) -> EtlResult<()> {
        // We want to lock for the entire processing to ensure that we don't have any race conditions
        // and possible errors are easier to reason about.
        let mut inner = self.inner.lock().await;

        for replicated_table_schema in replicated_table_schemas {
            let table_id = replicated_table_schema.id();

            // We need to determine the current sequenced table ID for this table.
            //
            // If no mapping exists, it means the table was never created in BigQuery (e.g., due to
            // validation errors during copy). In this case, we skip the truncate since there's
            // nothing to truncate.
            let Some(sequenced_bigquery_table_id) =
                self.get_sequenced_bigquery_table_id(&table_id).await?
            else {
                warn!(
                    "skipping truncate for table {}: no mapping exists (table was likely never created)",
                    table_id
                );
                continue;
            };

            // We compute the new sequence table ID since we want a new table for each truncate event.
            let next_sequenced_bigquery_table_id = sequenced_bigquery_table_id.next();

            info!(
                "processing truncate for table {}: creating new version {}",
                table_id, next_sequenced_bigquery_table_id
            );

            // Create or replace the new table.
            //
            // We unconditionally replace the table if it's there because here we know that
            // we need the table to be empty given the truncation.
            self.client
                .create_or_replace_table(
                    &self.dataset_id,
                    &next_sequenced_bigquery_table_id.to_string(),
                    &replicated_table_schema,
                    self.max_staleness_mins,
                )
                .await?;
            Self::add_to_created_tables_cache(&mut inner, &next_sequenced_bigquery_table_id);

            // Update the view to point to the new table.
            self.ensure_view_points_to_table(
                &mut inner,
                // We convert the sequenced table ID to a BigQuery table ID since the view will have
                // the name of the BigQuery table id (without the sequence number).
                &sequenced_bigquery_table_id.to_bigquery_table_id(),
                &next_sequenced_bigquery_table_id,
            )
            .await?;

            // Update the store table mappings to point to the new table.
            self.state_store
                .store_table_mapping(table_id, next_sequenced_bigquery_table_id.to_string())
                .await?;

            // Please note that the three statements above are not transactional, so if one fails,
            // there might be combinations of failures that require manual intervention. For example,
            // - Table created, but view update failed -> in this case the system will still point to
            //   table 'n', so the restart will reprocess events on table 'n', the table 'n + 1' will
            //   be recreated and the view will be updated to point to the new table. No mappings are
            //   changed.
            // - Table created, view updated, but mapping update failed -> in this case the system will
            //   still point to table 'n' but the customer will see the empty state of table 'n + 1' until the
            //   system heals. Healing happens when the system is restarted, the mapping points to 'n'
            //   meaning that events will be reprocessed and applied on table 'n' and then once the truncate
            //   is successfully processed, the system should be consistent.

            info!(
                "successfully processed truncate for {}: new table {}, view updated",
                table_id, next_sequenced_bigquery_table_id
            );

            // We remove the old table from the cache since it's no longer necessary.
            inner.created_tables.remove(&sequenced_bigquery_table_id);

            // Schedule cleanup of the previous table. We do not care to track this task since
            // if it fails, users can clean up the table on their own, but the view will still point
            // to the new data.
            let client = self.client.clone();
            let dataset_id = self.dataset_id.clone();
            tokio::spawn(async move {
                if let Err(err) = client
                    .drop_table(&dataset_id, &sequenced_bigquery_table_id.to_string())
                    .await
                {
                    warn!(
                        "failed to drop previous table {}: {}",
                        sequenced_bigquery_table_id, err
                    );
                } else {
                    info!(
                        "successfully cleaned up previous table {}",
                        sequenced_bigquery_table_id
                    );
                }
            });
        }

        Ok(())
    }
}

impl<S> Destination for BigQueryDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "bigquery"
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        self.process_truncate_for_schemas(iter::once(replicated_table_schema.clone()))
            .await
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows(replicated_table_schema, table_rows)
            .await?;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        self.write_events(events).await?;

        Ok(())
    }
}

/// Splits table rows into optimal sub-batches for parallel execution.
///
/// Calculates the optimal distribution of rows across batches to maximize
/// utilization of available concurrent streams. Creates approximately equal-sized
/// sub-batches when splitting is beneficial for parallelism.
fn split_table_rows(
    table_rows: Vec<TableRow>,
    max_concurrent_streams: usize,
) -> Vec<Vec<TableRow>> {
    let total_rows = table_rows.len();

    if total_rows == 0 {
        return vec![];
    }

    if total_rows <= 1 || max_concurrent_streams == 1 || total_rows <= max_concurrent_streams {
        return vec![table_rows];
    }

    // Calculate optimal rows per batch to maximize parallelism.
    let optimal_rows_per_batch = total_rows.div_ceil(max_concurrent_streams);

    if optimal_rows_per_batch == 0 {
        return vec![table_rows];
    }

    // Split the rows into smaller sub-batches.
    let num_sub_batches = total_rows.div_ceil(optimal_rows_per_batch);
    let rows_per_sub_batch = total_rows / num_sub_batches;
    let extra_rows = total_rows % num_sub_batches;

    let mut batches = Vec::with_capacity(num_sub_batches);
    let mut start_idx = 0;
    for i in 0..num_sub_batches {
        let mut end_idx = start_idx + rows_per_sub_batch;

        // Distribute extra rows evenly across the first few batches
        if i < extra_rows {
            end_idx += 1;
        }

        let sub_batch_rows = table_rows[start_idx..end_idx].to_vec();
        batches.push(sub_batch_rows);
        start_idx = end_idx;
    }

    batches
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_to_bigquery_table_id_no_underscores() {
        let table_name = TableName::new("schema".to_string(), "table".to_string());
        assert_eq!(table_name_to_bigquery_table_id(&table_name), "schema_table");
    }

    #[test]
    fn test_table_name_to_bigquery_table_id_with_underscores() {
        let table_name = TableName::new("a_b".to_string(), "c_d".to_string());
        assert_eq!(table_name_to_bigquery_table_id(&table_name), "a__b_c__d");
    }

    #[test]
    fn test_table_name_to_bigquery_table_id_collision_prevention() {
        // These two cases previously collided to "a_b_c"
        let table_name1 = TableName::new("a_b".to_string(), "c".to_string());
        let table_name2 = TableName::new("a".to_string(), "b_c".to_string());

        let id1 = table_name_to_bigquery_table_id(&table_name1);
        let id2 = table_name_to_bigquery_table_id(&table_name2);

        assert_eq!(id1, "a__b_c");
        assert_eq!(id2, "a_b__c");
        assert_ne!(id1, id2, "Table IDs should not collide");
    }

    #[test]
    fn test_table_name_to_bigquery_table_id_multiple_underscores() {
        let table_name = TableName::new("a__b".to_string(), "c__d".to_string());
        assert_eq!(
            table_name_to_bigquery_table_id(&table_name),
            "a____b_c____d"
        );
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_valid() {
        let table_id = "users_table_123";
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "users_table");
        assert_eq!(parsed.1, 123);
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_zero_sequence() {
        let table_id = "simple_table_0";
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "simple_table");
        assert_eq!(parsed.1, 0);
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_large_sequence() {
        let table_id = "test_table_18446744073709551615"; // u64::MAX
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "test_table");
        assert_eq!(parsed.1, u64::MAX);
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_escaped_underscores() {
        let table_id = "a__b_c__d_42";
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "a__b_c__d");
        assert_eq!(parsed.1, 42);
    }

    #[test]
    fn test_sequenced_bigquery_table_id_display_formatting() {
        let table_id = SequencedBigQueryTableId("users_table".to_string(), 123);
        assert_eq!(table_id.to_string(), "users_table_123");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_display_zero_sequence() {
        let table_id = SequencedBigQueryTableId("simple_table".to_string(), 0);
        assert_eq!(table_id.to_string(), "simple_table_0");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_display_large_sequence() {
        let table_id = SequencedBigQueryTableId("test_table".to_string(), u64::MAX);
        assert_eq!(table_id.to_string(), "test_table_18446744073709551615");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_display_with_escaped_underscores() {
        let table_id = SequencedBigQueryTableId("a__b_c__d".to_string(), 42);
        assert_eq!(table_id.to_string(), "a__b_c__d_42");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_new() {
        let table_id = SequencedBigQueryTableId::new("users_table".to_string());
        assert_eq!(table_id.to_bigquery_table_id(), "users_table");
        assert_eq!(table_id.1, 0);
    }

    #[test]
    fn test_sequenced_bigquery_table_id_new_with_underscores() {
        let table_id = SequencedBigQueryTableId::new("a__b_c__d".to_string());
        assert_eq!(table_id.to_bigquery_table_id(), "a__b_c__d");
        assert_eq!(table_id.1, 0);
    }

    #[test]
    fn test_sequenced_bigquery_table_id_next() {
        let table_id = SequencedBigQueryTableId::new("users_table".to_string());
        let next_table_id = table_id.next();

        assert_eq!(table_id.1, 0);
        assert_eq!(next_table_id.1, 1);
        assert_eq!(next_table_id.to_bigquery_table_id(), "users_table");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_next_increments_correctly() {
        let table_id = SequencedBigQueryTableId("test_table".to_string(), 42);
        let next_table_id = table_id.next();

        assert_eq!(next_table_id.1, 43);
        assert_eq!(next_table_id.to_bigquery_table_id(), "test_table");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_next_max_value() {
        let table_id = SequencedBigQueryTableId("test_table".to_string(), u64::MAX - 1);
        let next_table_id = table_id.next();

        assert_eq!(next_table_id.1, u64::MAX);
        assert_eq!(next_table_id.to_bigquery_table_id(), "test_table");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_to_bigquery_table_id() {
        let table_id = SequencedBigQueryTableId("users_table".to_string(), 123);
        assert_eq!(table_id.to_bigquery_table_id(), "users_table");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_to_bigquery_table_id_with_underscores() {
        let table_id = SequencedBigQueryTableId("a__b_c__d".to_string(), 42);
        assert_eq!(table_id.to_bigquery_table_id(), "a__b_c__d");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_to_bigquery_table_id_zero_sequence() {
        let table_id = SequencedBigQueryTableId("simple_table".to_string(), 0);
        assert_eq!(table_id.to_bigquery_table_id(), "simple_table");
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_no_underscore() {
        let result = "tablewithoutsequence".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("No underscore found"));
        assert!(err.to_string().contains("tablewithoutsequence"));
        assert!(
            err.to_string()
                .contains("Expected format: 'table_name_sequence'")
        );
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_invalid_sequence_number() {
        let result = "users_table_not_a_number".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Failed to parse sequence number"));
        assert!(err.to_string().contains("not_a_number"));
        assert!(err.to_string().contains("users_table_not_a_number"));
        assert!(err.to_string().contains("Expected a non-negative integer"));
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_sequence_is_word() {
        let result = "table_word".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Failed to parse sequence number"));
        assert!(err.to_string().contains("word"));
        assert!(err.to_string().contains("table_word"));
        assert!(err.to_string().contains("Expected a non-negative integer"));
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_negative_sequence() {
        let result = "users_table_-123".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Failed to parse sequence number"));
        assert!(err.to_string().contains("-123"));
        assert!(err.to_string().contains("users_table_-123"));
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_sequence_overflow() {
        let result = "users_table_18446744073709551616".parse::<SequencedBigQueryTableId>(); // u64::MAX + 1
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Failed to parse sequence number"));
        assert!(err.to_string().contains("18446744073709551616"));
        assert!(err.to_string().contains("users_table_18446744073709551616"));
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_empty_string() {
        let result = "".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("No underscore found"));
        assert!(err.to_string().contains("''"));
        assert!(
            err.to_string()
                .contains("Expected format: 'table_name_sequence'")
        );
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_empty_sequence() {
        let result = "users_table_".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Sequence number cannot be empty"));
        assert!(err.to_string().contains("users_table_"));
        assert!(
            err.to_string()
                .contains("Expected format: 'table_name_sequence'")
        );
    }

    #[test]
    fn test_sequenced_bigquery_table_id_from_str_empty_table_name() {
        let result = "_123".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Table name cannot be empty"));
        assert!(err.to_string().contains("_123"));
        assert!(
            err.to_string()
                .contains("Expected format: 'table_name_sequence'")
        );
    }

    #[test]
    fn test_sequenced_bigquery_table_id_round_trip() {
        let original = "users_table_123";
        let parsed = original.parse::<SequencedBigQueryTableId>().unwrap();
        let formatted = parsed.to_string();
        assert_eq!(original, formatted);
    }

    #[test]
    fn test_sequenced_bigquery_table_id_round_trip_complex() {
        let original = "a__b_c__d_999";
        let parsed = original.parse::<SequencedBigQueryTableId>().unwrap();
        let formatted = parsed.to_string();
        assert_eq!(original, formatted);
        assert_eq!(parsed.to_bigquery_table_id(), "a__b_c__d");
        assert_eq!(parsed.1, 999);
    }

    #[test]
    fn test_split_table_rows_empty_input() {
        let rows = vec![];
        let result = split_table_rows(rows, 4);
        assert_eq!(result, Vec::<Vec<TableRow>>::new());
    }

    #[test]
    fn test_split_table_rows_zero_concurrent_streams() {
        let rows = vec![TableRow::new(vec![])];
        let result = split_table_rows(rows.clone(), 0);
        assert_eq!(result, vec![rows]);
    }

    #[test]
    fn test_split_table_rows_single_concurrent_stream() {
        let rows = vec![TableRow::new(vec![]), TableRow::new(vec![])];
        let result = split_table_rows(rows.clone(), 1);
        assert_eq!(result, vec![rows]);
    }

    #[test]
    fn test_split_table_rows_fewer_rows_than_streams() {
        let rows = vec![TableRow::new(vec![]), TableRow::new(vec![])];
        let result = split_table_rows(rows.clone(), 5);
        assert_eq!(result, vec![rows]);
    }

    #[test]
    fn test_split_table_rows_equal_distribution() {
        let rows = vec![
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
        ];
        let result = split_table_rows(rows, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 2);
        assert_eq!(result[1].len(), 2);
    }

    #[test]
    fn test_split_table_rows_uneven_distribution() {
        let rows = vec![
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
        ];
        let result = split_table_rows(rows, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].len(), 2); // Gets extra row
        assert_eq!(result[1].len(), 2); // Gets extra row
        assert_eq!(result[2].len(), 1);
    }

    #[test]
    fn test_split_table_rows_many_streams() {
        let rows = vec![
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
            TableRow::new(vec![]),
        ];
        let result = split_table_rows(rows, 4);
        assert_eq!(result.len(), 4);

        // Verify all rows are accounted for
        let total_rows: usize = result.iter().map(|batch| batch.len()).sum();
        assert_eq!(total_rows, 10);

        // Verify approximately equal distribution
        assert_eq!(result[0].len(), 3); // Gets extra row
        assert_eq!(result[1].len(), 3); // Gets extra row
        assert_eq!(result[2].len(), 2);
        assert_eq!(result[3].len(), 2);
    }

    #[test]
    fn test_split_table_rows_single_row() {
        let rows = vec![TableRow::new(vec![])];
        let result = split_table_rows(rows.clone(), 5);
        assert_eq!(result, vec![rows]);
    }
}
