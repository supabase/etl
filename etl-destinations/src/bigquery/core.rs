use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    iter,
    str::FromStr,
    sync::Arc,
};

use etl::{
    bail,
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
        task_set::DestinationTaskSet,
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    state::destination_metadata::{DestinationTableMetadata, DestinationTableSchemaStatus},
    store::{schema::SchemaStore, state::StateStore},
    types::{
        Cell, Event, IdentityType, OldTableRow, PipelineId, ReplicatedTableSchema, SchemaDiff,
        TableId, TableName, TableRow, UpdatedTableRow,
    },
};
use gcp_bigquery_client::storage::{MAX_BATCH_SIZE_BYTES, TableDescriptor};
use metrics::histogram;
use prost::Message;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

#[cfg(feature = "egress")]
use crate::egress::{PROCESSING_TYPE_STREAMING, PROCESSING_TYPE_TABLE_COPY, log_processed_bytes};
use crate::{
    bigquery::{
        BigQueryDatasetId, BigQueryTableId,
        client::{BigQueryClient, BigQueryOperationType},
        encoding::BigQueryTableRow,
        metrics::{ETL_BQ_APPEND_BATCHES_BATCH_SIZE, register_metrics},
    },
    table_name::try_stringify_table_name,
};

/// Returns the [`BigQueryTableId`] for a supplied [`TableName`].
///
/// Uses the shared underscore-escaped destination naming logic from
/// [`crate::table_name`].
pub fn table_name_to_bigquery_table_id(table_name: &TableName) -> EtlResult<BigQueryTableId> {
    try_stringify_table_name(table_name)
}

/// A BigQuery table identifier with version sequence for truncate operations.
///
/// Combines a base table name with a sequence number to enable versioned
/// tables. Used for truncate handling where each truncate creates a new table
/// version.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct SequencedBigQueryTableId(BigQueryTableId, u64);

impl SequencedBigQueryTableId {
    /// Creates a new sequenced table ID starting at version 0.
    fn new(table_id: BigQueryTableId) -> Self {
        Self(table_id, 0)
    }

    /// Returns the next version of this sequenced table ID.
    fn next(&self) -> Self {
        Self(self.0.clone(), self.1 + 1)
    }

    /// Extracts the base BigQuery table ID without the sequence number.
    fn to_bigquery_table_id(&self) -> BigQueryTableId {
        self.0.clone()
    }
}

impl FromStr for SequencedBigQueryTableId {
    type Err = EtlError;

    /// Parses a sequenced table ID from string format `table_name_sequence`.
    ///
    /// Expects the last underscore to separate the table name from the sequence
    /// number.
    fn from_str(table_id: &str) -> Result<Self, Self::Err> {
        if let Some(last_underscore) = table_id.rfind('_') {
            let table_name = &table_id[..last_underscore];
            let sequence_str = &table_id[last_underscore + 1..];

            if table_name.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced BigQuery table ID format",
                    format!(
                        "Table name cannot be empty in sequenced table ID '{table_id}'. Expected \
                         format: 'table_name_sequence'"
                    )
                )
            }

            if sequence_str.is_empty() {
                bail!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequenced BigQuery table ID format",
                    format!(
                        "Sequence number cannot be empty in sequenced table ID '{table_id}'. \
                         Expected format: 'table_name_sequence'"
                    )
                )
            }

            let sequence_number = sequence_str.parse::<u64>().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationTableNameInvalid,
                    "Invalid sequence number in BigQuery table ID",
                    format!(
                        "Failed to parse sequence number '{sequence_str}' in table ID \
                         '{table_id}': {e}. Expected a non-negative integer (0-{max})",
                        max = u64::MAX
                    )
                )
            })?;

            Ok(SequencedBigQueryTableId(table_name.to_string(), sequence_number))
        } else {
            bail!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid sequenced BigQuery table ID format",
                format!(
                    "No underscore found in table ID '{table_id}'. Expected format: \
                     'table_name_sequence' where sequence is a non-negative integer"
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
/// Contains caches and state that require synchronization across concurrent
/// operations. The main BigQuery client and configuration are stored directly
/// in the outer struct to allow lock-free access during streaming operations.
#[derive(Debug)]
struct Inner {
    /// Cache of table IDs that have been successfully created or verified to
    /// exist. This avoids redundant `create_table_if_missing` calls for
    /// known tables.
    created_tables: HashSet<SequencedBigQueryTableId>,
    /// Cache of views that have been created and the versioned table they point
    /// to. This avoids redundant `CREATE OR REPLACE VIEW` calls for views
    /// that already point to the correct table. Maps view name to the
    /// versioned table it currently points to.
    ///
    /// # Example
    /// `{ users_table: users_table_10, orders_table: orders_table_3 }`
    created_views: HashMap<BigQueryTableId, SequencedBigQueryTableId>,
}

/// A BigQuery destination that implements the ETL [`Destination`] trait.
///
/// Provides Postgres-to-BigQuery data pipeline functionality including
/// streaming inserts and CDC operation handling.
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
    pipeline_id: PipelineId,
    state_store: S,
    inner: Arc<Mutex<Inner>>,
    streaming_tasks: DestinationTaskSet,
}

impl<S> BigQueryDestination<S>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    /// Creates a new [`BigQueryDestination`] with a pre-configured client.
    ///
    /// Accepts an existing [`BigQueryClient`] instance, allowing the caller to
    /// control client creation separately from destination initialization.
    /// This is useful for validation scenarios where you want to create and
    /// validate the client first.
    pub fn new(
        client: BigQueryClient,
        dataset_id: BigQueryDatasetId,
        max_staleness_mins: Option<u16>,
        pipeline_id: PipelineId,
        state_store: S,
    ) -> Self {
        register_metrics();

        let inner = Inner { created_tables: HashSet::new(), created_views: HashMap::new() };

        Self {
            client,
            dataset_id,
            max_staleness_mins,
            pipeline_id,
            state_store,
            inner: Arc::new(Mutex::new(inner)),
            streaming_tasks: DestinationTaskSet::new(),
        }
    }

    /// Creates a new [`BigQueryDestination`] using a service account key file
    /// path.
    ///
    /// Initializes the BigQuery client with the provided credentials and
    /// project settings. The `max_staleness_mins` parameter controls table
    /// metadata cache freshness. The `connection_pool_size` parameter
    /// controls the connection pool size.
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
        connection_pool_size: usize,
        pipeline_id: PipelineId,
        state_store: S,
    ) -> EtlResult<Self> {
        register_metrics();

        let client =
            BigQueryClient::new_with_key_path(project_id, sa_key, connection_pool_size).await?;
        let inner = Inner { created_tables: HashSet::new(), created_views: HashMap::new() };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            pipeline_id,
            state_store,
            inner: Arc::new(Mutex::new(inner)),
            streaming_tasks: DestinationTaskSet::new(),
        })
    }

    /// Creates a new [`BigQueryDestination`] using a service account key JSON
    /// string.
    ///
    /// Similar to [`BigQueryDestination::new_with_key_path`] but accepts the
    /// key content directly rather than a file path. Useful when
    /// credentials are stored in environment variables.
    /// The `connection_pool_size` parameter controls the connection pool size.
    pub async fn new_with_key(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
        connection_pool_size: usize,
        pipeline_id: PipelineId,
        state_store: S,
    ) -> EtlResult<Self> {
        register_metrics();

        let client = BigQueryClient::new_with_key(project_id, sa_key, connection_pool_size).await?;
        let inner = Inner { created_tables: HashSet::new(), created_views: HashMap::new() };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            pipeline_id,
            state_store,
            inner: Arc::new(Mutex::new(inner)),
            streaming_tasks: DestinationTaskSet::new(),
        })
    }
    /// Creates a new [`BigQueryDestination`] using Application Default
    /// Credentials (ADC).
    ///
    /// Initializes the BigQuery client with the default credentials and project
    /// settings. The `max_staleness_mins` parameter controls table metadata
    /// cache freshness. The `connection_pool_size` parameter controls the
    /// connection pool size.
    pub async fn new_with_adc(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        max_staleness_mins: Option<u16>,
        connection_pool_size: usize,
        pipeline_id: PipelineId,
        state_store: S,
    ) -> EtlResult<Self> {
        register_metrics();

        let client = BigQueryClient::new_with_adc(project_id, connection_pool_size).await?;
        let inner = Inner { created_tables: HashSet::new(), created_views: HashMap::new() };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            pipeline_id,
            state_store,
            inner: Arc::new(Mutex::new(inner)),
            streaming_tasks: DestinationTaskSet::new(),
        })
    }

    /// Creates a new [`BigQueryDestination`] using an installed flow
    /// authenticator.
    ///
    /// Initializes the BigQuery client with a flow authenticator using the
    /// provided secret and persistent file path. The `max_staleness_mins`
    /// parameter controls table metadata cache freshness.
    /// The `connection_pool_size` parameter controls the connection pool size.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_flow_authenticator<Secret, Path>(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        secret: Secret,
        persistent_file_path: Path,
        max_staleness_mins: Option<u16>,
        connection_pool_size: usize,
        pipeline_id: PipelineId,
        store: S,
    ) -> EtlResult<Self>
    where
        Secret: AsRef<[u8]>,
        Path: Into<std::path::PathBuf>,
    {
        register_metrics();

        let client = BigQueryClient::new_with_flow_authenticator(
            project_id,
            secret,
            persistent_file_path,
            connection_pool_size,
        )
        .await?;
        let inner = Inner { created_tables: HashSet::new(), created_views: HashMap::new() };

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            pipeline_id,
            state_store: store,
            inner: Arc::new(Mutex::new(inner)),
            streaming_tasks: DestinationTaskSet::new(),
        })
    }

    /// Prepares a table for BigQuery writes with schema-aware table creation.
    ///
    /// Creates or verifies the BigQuery table exists using the provided schema,
    /// ensures the view points to the current versioned table, and validates
    /// that the source replica identity is compatible with BigQuery deletes and
    /// upserts. Uses caching to avoid redundant table creation checks.
    async fn prepare_table_for_streaming(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<(SequencedBigQueryTableId, TableDescriptor)> {
        validate_bigquery_replica_identity(replicated_table_schema)?;

        // We hold the lock for the entire preparation to avoid race conditions since
        // the consistency of this code path is critical.
        let mut inner = self.inner.lock().await;

        let table_id = replicated_table_schema.id();

        // We determine the BigQuery table ID for the table together with the current
        // sequence number.
        let bigquery_table_id = table_name_to_bigquery_table_id(replicated_table_schema.name())?;
        let snapshot_id = replicated_table_schema.inner().snapshot_id;
        let replication_mask = replicated_table_schema.replication_mask().clone();

        // Check if we have existing metadata for this table.
        let existing_metadata =
            self.state_store.get_applied_destination_table_metadata(table_id).await?;

        let sequenced_bigquery_table_id = match &existing_metadata {
            Some(metadata) => metadata.destination_table_id.parse()?,
            None => SequencedBigQueryTableId::new(bigquery_table_id.clone()),
        };

        // Optimistically skip table creation if we've already seen this sequenced
        // table.
        //
        // Note that if the table is deleted outside ETL and the cache marks it as
        // created, the inserts will fail because the table will be missing and
        // won't be created.
        if !inner.created_tables.contains(&sequenced_bigquery_table_id) {
            // Create metadata with applying status. For new tables, this is the initial
            // insert. For existing tables, this updates the status.
            let metadata = DestinationTableMetadata::new_applying(
                sequenced_bigquery_table_id.to_string(),
                snapshot_id,
                replication_mask.clone(),
            );

            // Store or update metadata before creating the table.
            self.state_store.store_destination_table_metadata(table_id, metadata.clone()).await?;

            self.client
                .create_table_if_missing(
                    &self.dataset_id,
                    &sequenced_bigquery_table_id.to_string(),
                    replicated_table_schema,
                    self.max_staleness_mins,
                )
                .await?;

            // Mark as applied after successful table creation.
            self.state_store
                .store_destination_table_metadata(table_id, metadata.to_applied())
                .await?;

            // Add the sequenced table to the cache.
            Self::add_to_created_tables_cache(&mut inner, &sequenced_bigquery_table_id);

            debug!(%sequenced_bigquery_table_id, "sequenced table added to creation cache");
        } else {
            debug!(
                %sequenced_bigquery_table_id,
                "sequenced table found in creation cache, skipping existence check"
            );
        }

        // Ensure view points to this sequenced table (uses cache to avoid redundant
        // operations)
        self.ensure_view_points_to_table(
            &mut inner,
            &bigquery_table_id,
            &sequenced_bigquery_table_id,
        )
        .await?;

        // Note: We return TableDescriptor by value for simplicity, which means callers
        // clone it when creating multiple batches. This is acceptable because
        // the descriptor is small (one String per column) and the cost is
        // negligible compared to network I/O. If profiling shows this is a
        // bottleneck, we could wrap it in Arc here and use Arc::unwrap_or_clone
        // at the call site to avoid redundant clones.
        let table_descriptor = BigQueryClient::column_schemas_to_table_descriptor(
            replicated_table_schema,
            use_cdc_sequence_column,
        );

        Ok((sequenced_bigquery_table_id, table_descriptor))
    }

    /// Adds a table to the creation cache to avoid redundant existence checks.
    fn add_to_created_tables_cache(inner: &mut Inner, table_id: &SequencedBigQueryTableId) {
        if inner.created_tables.contains(table_id) {
            return;
        }

        inner.created_tables.insert(table_id.clone());
    }

    /// Retrieves the current sequenced table ID from the destination metadata.
    async fn get_sequenced_bigquery_table_id(
        &self,
        table_id: &TableId,
    ) -> EtlResult<Option<SequencedBigQueryTableId>> {
        let Some(metadata) =
            self.state_store.get_applied_destination_table_metadata(*table_id).await?
        else {
            return Ok(None);
        };

        let sequenced_bigquery_table_id = metadata.destination_table_id.parse()?;

        Ok(Some(sequenced_bigquery_table_id))
    }

    /// Ensures a view points to the specified target table, creating or
    /// updating as needed.
    ///
    /// Returns `true` if the view was created or updated, `false` if already
    /// correct.
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
                %view_name,
                %target_table_id,
                "view already points to target, skipping creation"
            );

            return Ok(false);
        }

        self.client
            .create_or_replace_view(&self.dataset_id, view_name, &target_table_id.to_string())
            .await?;

        // We insert/overwrite the new (view -> sequenced bigquery table id) mapping
        inner.created_views.insert(view_name.clone(), target_table_id.clone());

        debug!(
            %view_name,
            %target_table_id,
            "view created/updated to point to target"
        );

        Ok(true)
    }

    /// Writes table-copy rows using the BigQuery upsert row format.
    ///
    /// Adds an `Upsert` operation type to each row, splits them into optimal
    /// batches based on estimated row size to maximize the 10MB per request
    /// limit, and streams them to BigQuery concurrently.
    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // Prepare table for streaming.
        let (sequenced_bigquery_table_id, table_descriptor) =
            self.prepare_table_for_streaming(replicated_table_schema, false).await?;

        // Add the CDC operation type to all rows (no lock needed).
        for table_row in &mut table_rows {
            table_row.values_mut().push(BigQueryOperationType::Upsert.into_cell());
        }
        let table_rows = table_rows
            .into_iter()
            .map(BigQueryTableRow::try_from)
            .collect::<EtlResult<Vec<BigQueryTableRow>>>()?;

        // Calculate optimal target batches based on estimated row size.
        let target_batches = calculate_target_batches_for_table_copy(&table_rows)?;

        // Split table rows into optimal batches for parallel execution.
        let table_rows_batches = split_table_rows(table_rows, target_batches);
        let sequenced_bigquery_table_id_string = sequenced_bigquery_table_id.to_string();

        // Create append requests from the split rows.
        let mut append_requests = Vec::with_capacity(table_rows_batches.len());
        for (batch_index, table_rows) in table_rows_batches.into_iter().enumerate() {
            if !table_rows.is_empty() {
                histogram!(ETL_BQ_APPEND_BATCHES_BATCH_SIZE).record(table_rows.len() as f64);
                let append_request = self.client.create_batch_append_request(
                    self.pipeline_id,
                    batch_index,
                    &self.dataset_id,
                    &sequenced_bigquery_table_id_string,
                    table_descriptor.clone(),
                    table_rows,
                )?;
                append_requests.push(append_request);
            }
        }

        #[allow(unused_variables)]
        let (bytes_sent, bytes_received) = if append_requests.is_empty() {
            (0, 0)
        } else {
            self.client.append_table_batches(append_requests).await?
        };

        if bytes_sent > 0 {
            #[cfg(feature = "egress")]
            log_processed_bytes(
                Self::name(),
                PROCESSING_TYPE_TABLE_COPY,
                bytes_sent as u64,
                bytes_received as u64,
            );
        }

        Ok(())
    }

    /// Handles a schema change event (Relation) by computing the diff and
    /// applying changes.
    ///
    /// This method retrieves the current applied destination schema state via
    /// [`StateStore::get_applied_destination_table_metadata`]. Missing metadata
    /// is treated as an invariant violation since the metadata should have
    /// been recorded during initial table synchronization in
    /// [`Self::write_table_rows`]. If the snapshot ID or replication mask
    /// differs from the incoming [`ReplicatedTableSchema`], the method
    /// computes and applies the schema diff.
    async fn handle_relation_event(
        &self,
        new_replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        validate_bigquery_replica_identity(new_replicated_table_schema)?;

        let table_id = new_replicated_table_schema.id();
        let new_snapshot_id = new_replicated_table_schema.inner().snapshot_id;

        // Get current applied destination metadata. If the table is still in
        // `Applying`, the state store surfaces that as an error.
        let Some(metadata) =
            self.state_store.get_applied_destination_table_metadata(table_id).await?
        else {
            // No metadata exists, this is a broken invariant since the metadata should
            // have been recorded during write_table_rows before any Relation event.
            bail!(
                ErrorKind::CorruptedTableSchema,
                "Missing destination table metadata",
                format!(
                    "No destination table metadata found for table {} when processing schema \
                     change. This indicates a broken invariant, the metadata should have been \
                     recorded during initial table synchronization.",
                    table_id
                )
            );
        };

        let current_snapshot_id = metadata.snapshot_id;
        let current_replication_mask = metadata.replication_mask.clone();
        let new_replication_mask = new_replicated_table_schema.replication_mask().clone();
        // Check both snapshot_id and replication mask - the mask can change
        // independently if columns are added/removed from the publication.
        if current_snapshot_id == new_snapshot_id
            && current_replication_mask == new_replication_mask
        {
            // Schema hasn't changed, nothing to do.
            info!(
                "schema for table {} unchanged (snapshot_id: {}, replication_mask: {})",
                table_id, new_snapshot_id, new_replication_mask
            );

            return Ok(());
        }

        info!(
            "schema change detected for table {}: snapshot_id {} -> {}, mask {} -> {}",
            table_id,
            current_snapshot_id,
            new_snapshot_id,
            current_replication_mask,
            new_replication_mask
        );

        // Get the current schema from the schema store to compute the diff.
        let current_table_schema =
            self.state_store.get_table_schema(&table_id, current_snapshot_id).await?.ok_or_else(
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

        // Build a ReplicatedTableSchema using the stored replication mask.
        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );

        let bigquery_table_id = metadata.destination_table_id.parse()?;

        // Mark as applying before making changes (with the NEW snapshot_id and mask).
        //
        // NOTE: BigQuery does not support transactional DDL, so if the system crashes
        // while in 'Applying' state, the destination table may be in an inconsistent
        // state and manual intervention may be required. The `previous_snapshot_id`
        // is stored for debugging purposes but automatic recovery is not possible.
        let updated_metadata = DestinationTableMetadata::new_applied(
            metadata.destination_table_id.clone(),
            current_snapshot_id,
            current_replication_mask.clone(),
        )
        .with_schema_change(
            new_snapshot_id,
            new_replication_mask.clone(),
            DestinationTableSchemaStatus::Applying,
        );
        self.state_store
            .store_destination_table_metadata(table_id, updated_metadata.clone())
            .await?;

        // Compute and apply the diff.
        let diff = current_schema.diff(new_replicated_table_schema);
        if let Err(err) = self.apply_schema_diff(&table_id, &bigquery_table_id, &diff).await {
            warn!(
                "schema change failed for table {}: {}. Manual intervention may be required.",
                table_id, err
            );
            return Err(err);
        }

        // Mark as applied after successful changes.
        self.state_store
            .store_destination_table_metadata(table_id, updated_metadata.to_applied())
            .await?;

        // We must invalidate all connections here to make sure that caches on the
        // connection side on BigQuery do not interfere with the schema changes.
        // Each worker processes invalidations in channel order and this call
        // waits for all workers to ack them. Parallel invalidations may
        // interleave, but once this returns, later appends will
        // use fresh connections.
        self.client.invalidate_all_connections().await;

        info!(
            "schema change completed for table {}: snapshot_id {} applied",
            table_id, new_snapshot_id
        );

        Ok(())
    }

    /// Applies a schema diff to the BigQuery table.
    ///
    /// Executes the necessary DDL operations (ADD COLUMN, DROP COLUMN, RENAME
    /// COLUMN) to transform the destination schema.
    async fn apply_schema_diff(
        &self,
        table_id: &TableId,
        bigquery_table_id: &SequencedBigQueryTableId,
        diff: &SchemaDiff,
    ) -> EtlResult<()> {
        if diff.is_empty() {
            debug!(%table_id, "no schema changes to apply for table");
            return Ok(());
        }

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

        // Apply column renames (must be done before removals in case of position
        // conflicts).
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

        info!("schema changes applied successfully to table {}", bigquery_table_id);

        Ok(())
    }

    /// Processes CDC events in batches with proper ordering and truncate
    /// handling.
    ///
    /// Groups streaming operations (insert/update/delete) by table and
    /// processes them together, then handles truncate events separately by
    /// creating new versioned tables. Uses the schema from the first event
    /// of each table for table creation and descriptor building.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut events_iter = events.into_iter().peekable();

        while events_iter.peek().is_some() {
            // Maps table ID to (schema, rows). We are assuming that the table schema is the
            // same for all events within two Relation event boundaries.
            let mut table_id_to_data: HashMap<
                TableId,
                (ReplicatedTableSchema, Vec<BigQueryTableRow>),
            > = HashMap::new();

            // Process events until we hit a truncate or relation event, or run out of
            // events. Truncate and Relation events require flushing all batched
            // data first before they can be processed, to maintain correct
            // ordering.
            while let Some(event) = events_iter.peek() {
                if matches!(event, Event::Truncate(_) | Event::Relation(_)) {
                    break;
                }

                let Some(event) = events_iter.next() else {
                    break;
                };
                match event {
                    Event::Insert(mut insert) => {
                        let sequence_key = insert.event_sequence_key().to_string();
                        insert
                            .table_row
                            .values_mut()
                            .push(BigQueryOperationType::Upsert.into_cell());
                        insert.table_row.values_mut().push(Cell::String(sequence_key));

                        let table_id = insert.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (insert.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(BigQueryTableRow::try_from(insert.table_row)?);
                    }
                    Event::Update(update) => {
                        validate_bigquery_replica_identity(&update.replicated_table_schema)?;
                        let sequence_key = update.event_sequence_key().to_string();
                        let table_id = update.replicated_table_schema.id();
                        let table_row = match update.updated_table_row {
                            UpdatedTableRow::Full(row) => row,
                            UpdatedTableRow::Partial(_) => {
                                return Err(etl_error!(
                                    ErrorKind::InvalidState,
                                    "BigQuery update requires a full new row image",
                                    format!(
                                        "Table '{}' emitted a partial update row. BigQuery CDC \
                                         UPSERT does not preserve omitted columns.",
                                        update.replicated_table_schema.name()
                                    )
                                ));
                            }
                        };

                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (update.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.extend(bigquery_update_rows(
                            &update.replicated_table_schema,
                            table_row,
                            update.old_table_row,
                            sequence_key,
                        )?);
                    }
                    Event::Delete(delete) => {
                        validate_bigquery_replica_identity(&delete.replicated_table_schema)?;
                        let sequence_key = delete.event_sequence_key().to_string();
                        let old_table_row = delete.old_table_row.ok_or_else(|| {
                            etl_error!(
                                ErrorKind::InvalidState,
                                "BigQuery delete requires an old row image",
                                format!(
                                    "Table '{}' emitted a delete without an old row image. \
                                     BigQuery deletes are keyed by the source primary key and \
                                     cannot be applied safely without it.",
                                    delete.replicated_table_schema.name()
                                )
                            )
                        })?;

                        let table_id = delete.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (delete.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(bigquery_delete_row(
                            &delete.replicated_table_schema,
                            old_table_row,
                            sequence_key,
                        )?);
                    }
                    event => {
                        // Every other event type is currently not supported.
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            // Process accumulated events for each table.
            if !table_id_to_data.is_empty() {
                let mut append_requests = Vec::with_capacity(table_id_to_data.len());

                for (batch_index, (_, (replicated_table_schema, table_rows))) in
                    table_id_to_data.into_iter().enumerate()
                {
                    let (sequenced_bigquery_table_id, table_descriptor) =
                        self.prepare_table_for_streaming(&replicated_table_schema, true).await?;
                    let sequenced_bigquery_table_id_string =
                        sequenced_bigquery_table_id.to_string();
                    histogram!(ETL_BQ_APPEND_BATCHES_BATCH_SIZE).record(table_rows.len() as f64);

                    let append_request = self.client.create_batch_append_request(
                        self.pipeline_id,
                        batch_index,
                        &self.dataset_id,
                        &sequenced_bigquery_table_id_string,
                        table_descriptor,
                        table_rows,
                    )?;
                    append_requests.push(append_request);
                }

                #[allow(unused_variables)]
                let (bytes_sent, bytes_received) = if append_requests.is_empty() {
                    (0, 0)
                } else {
                    self.client.append_table_batches(append_requests).await?
                };

                if bytes_sent > 0 {
                    #[cfg(feature = "egress")]
                    log_processed_bytes(
                        Self::name(),
                        PROCESSING_TYPE_STREAMING,
                        bytes_sent as u64,
                        bytes_received as u64,
                    );
                }
            }

            // Process any Relation events (schema changes) that caused the batch to flush.
            // Multiple consecutive Relation events are processed sequentially.
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

            if !truncate_schemas.is_empty() {
                self.process_truncate_for_schemas(truncate_schemas.into_values()).await?;
            }
        }

        Ok(())
    }

    /// Handles table truncation by creating new versioned tables and updating
    /// views.
    ///
    /// Creates fresh empty tables with incremented version numbers, updates
    /// views to point to new tables, and schedules cleanup of old table
    /// versions. Uses the provided schemas directly instead of looking them
    /// up from a store.
    async fn process_truncate_for_schemas(
        &self,
        replicated_table_schemas: impl IntoIterator<Item = ReplicatedTableSchema>,
    ) -> EtlResult<()> {
        // We want to lock for the entire processing to ensure that we don't have any
        // race conditions and possible errors are easier to reason about.
        let mut inner = self.inner.lock().await;

        for replicated_table_schema in replicated_table_schemas {
            let table_id = replicated_table_schema.id();

            // We need to determine the current sequenced table ID for this table.
            //
            // If no mapping exists, it means the table was never created in BigQuery (e.g.,
            // due to validation errors during copy). In this case, we skip the
            // truncate since there's nothing to truncate.
            let Some(sequenced_bigquery_table_id) =
                self.get_sequenced_bigquery_table_id(&table_id).await?
            else {
                warn!(
                    %table_id,
                    "table schema not found in schema store while processing truncate events for bigquery"
                );
                continue;
            };

            // We compute the new sequence table ID since we want a new table for each
            // truncate event.
            let next_sequenced_bigquery_table_id = sequenced_bigquery_table_id.next();

            info!(
                table_id = table_id.0,
                %next_sequenced_bigquery_table_id,
                "processing truncate, creating new version"
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
                // We convert the sequenced table ID to a BigQuery table ID since the view will
                // have the name of the BigQuery table id (without the sequence
                // number).
                &sequenced_bigquery_table_id.to_bigquery_table_id(),
                &next_sequenced_bigquery_table_id,
            )
            .await?;

            // Update the metadata to point to the new table.
            let metadata = DestinationTableMetadata::new_applied(
                next_sequenced_bigquery_table_id.to_string(),
                replicated_table_schema.inner().snapshot_id,
                replicated_table_schema.replication_mask().clone(),
            );
            self.state_store.store_destination_table_metadata(table_id, metadata).await?;

            // Please note that the three statements above are not transactional, so if one
            // fails, there might be combinations of failures that require
            // manual intervention. For example,
            // - Table created, but view update failed -> in this case the system will still
            //   point to table 'n', so the restart will reprocess events on table 'n', the
            //   table 'n + 1' will be recreated and the view will be updated to point to
            //   the new table. No mappings are changed.
            // - Table created, view updated, but mapping update failed -> in this case the
            //   system will still point to table 'n' but the customer will see the empty
            //   state of table 'n + 1' until the system heals. Healing happens when the
            //   system is restarted, the mapping points to 'n' meaning that events will be
            //   reprocessed and applied on table 'n' and then once the truncate is
            //   successfully processed, the system should be consistent.

            info!(
                table_id = table_id.0,
                %next_sequenced_bigquery_table_id,
                "successfully processed truncate, view updated"
            );

            // We remove the old table from the cache since it's no longer necessary.
            inner.created_tables.remove(&sequenced_bigquery_table_id);

            // Schedule cleanup of the previous table. We do not care to track this task
            // since if it fails, users can clean up the table on their own, but
            // the view will still point to the new data.
            let client = self.client.clone();
            let dataset_id = self.dataset_id.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    client.drop_table(&dataset_id, &sequenced_bigquery_table_id.to_string()).await
                {
                    warn!(
                        %sequenced_bigquery_table_id,
                        error = %err,
                        "failed to drop previous table"
                    );
                } else {
                    info!(
                        %sequenced_bigquery_table_id,
                        "successfully cleaned up previous table"
                    );
                }
            });
        }

        Ok(())
    }
}

/// Validates that a replicated table schema can be applied in BigQuery.
///
/// BigQuery matches CDC rows by the destination table primary key, so the
/// source table must always expose that key.
/// PostgreSQL replica identity may either:
/// - match the source primary key directly, or
/// - request full old-row images (`FULL`), which still let us recover the
///   source primary key for deletes and primary-key-changing updates.
///
/// Alternative replica-identity indexes are not compatible with BigQuery's
/// destination key because BigQuery would deduplicate against a different row
/// identity than the source publisher.
fn validate_bigquery_replica_identity(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if replicated_table_schema.primary_key_column_schemas().len() == 0 {
        bail!(
            ErrorKind::SourceSchemaError,
            "BigQuery requires a source primary key",
            format!(
                "Table '{}' does not expose any replicated primary-key columns",
                replicated_table_schema.name()
            )
        );
    }

    match replicated_table_schema.identity_type() {
        IdentityType::PrimaryKey | IdentityType::Full => Ok(()),
        identity_type => {
            bail!(
                ErrorKind::SourceSchemaError,
                "BigQuery requires primary-key or full replica identity",
                format!(
                    "Table '{}' uses replica identity {:?}, but BigQuery only supports source \
                     primary-key identity or full old-row images",
                    replicated_table_schema.name(),
                    identity_type
                )
            );
        }
    }
}

impl<S> Destination for BigQueryDestination<S>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    fn name() -> &'static str {
        "bigquery"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.streaming_tasks.shutdown().await
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let result =
            self.process_truncate_for_schemas(iter::once(replicated_table_schema.clone())).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result =
            BigQueryDestination::write_table_rows(self, replicated_table_schema, table_rows).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        self.streaming_tasks.try_reap().await?;

        let destination = self.clone();
        self.streaming_tasks
            .spawn(async move {
                let result = destination.write_events(events).await;
                async_result.send(result);
            })
            .await;

        Ok(())
    }
}

/// Builds a BigQuery CDC upsert row.
fn bigquery_upsert_row(
    mut table_row: TableRow,
    sequence_key: String,
) -> EtlResult<BigQueryTableRow> {
    table_row.values_mut().push(BigQueryOperationType::Upsert.into_cell());
    table_row.values_mut().push(Cell::String(sequence_key));

    BigQueryTableRow::try_from(table_row)
}

/// Builds one or two BigQuery CDC rows for an update.
///
/// BigQuery applies CDC rows by the destination primary key, so if the source
/// update changes that key we must first delete the old key before upserting
/// the new row.
fn bigquery_update_rows(
    replicated_table_schema: &ReplicatedTableSchema,
    new_table_row: TableRow,
    old_table_row: Option<OldTableRow>,
    sequence_key: String,
) -> EtlResult<Vec<BigQueryTableRow>> {
    let primary_key_changed = match old_table_row.as_ref() {
        // PostgreSQL omits the old-side image only when the publisher
        // determined it was unnecessary. For primary-key identity, that means
        // the destination key did not change. `FULL` updates are expected to
        // carry an old row from pgoutput.
        Some(old_table_row) => {
            bigquery_primary_key_changed(replicated_table_schema, old_table_row, &new_table_row)?
        }
        None => false,
    };

    let mut rows = Vec::with_capacity(1 + usize::from(primary_key_changed));
    if primary_key_changed {
        let Some(old_table_row) = old_table_row else {
            bail!(
                ErrorKind::InvalidState,
                "BigQuery primary key change is missing old row",
                format!(
                    "Table '{}' primary key change was detected without an old row image",
                    replicated_table_schema.name()
                )
            );
        };

        rows.push(bigquery_delete_row(
            replicated_table_schema,
            old_table_row,
            sequence_key.clone(),
        )?);
    }
    rows.push(bigquery_upsert_row(new_table_row, sequence_key)?);

    Ok(rows)
}

/// Returns whether an update changed the destination primary key.
fn bigquery_primary_key_changed(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: &OldTableRow,
    new_table_row: &TableRow,
) -> EtlResult<bool> {
    let column_count = replicated_table_schema.column_schemas().len();
    if new_table_row.values().len() != column_count {
        bail!(
            ErrorKind::InvalidState,
            "BigQuery full row image does not match the replicated schema",
            format!(
                "Expected {} values for table '{}', got {}",
                column_count,
                replicated_table_schema.name(),
                new_table_row.values().len()
            )
        );
    }

    match old_table_row {
        OldTableRow::Full(row) => {
            if row.values().len() != column_count {
                bail!(
                    ErrorKind::InvalidState,
                    "BigQuery full row image does not match the replicated schema",
                    format!(
                        "Expected {} values for table '{}', got {}",
                        column_count,
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                );
            }

            Ok(replicated_table_schema
                .column_schemas()
                .zip(row.values())
                .zip(new_table_row.values())
                .any(|((column_schema, old_value), new_value)| {
                    column_schema.primary_key() && old_value != new_value
                }))
        }
        OldTableRow::Key(row) => {
            if !replicated_table_schema.identity_matches_primary_key() {
                bail!(
                    ErrorKind::InvalidState,
                    "BigQuery key image does not match the source primary key",
                    format!(
                        "Table '{}' emitted a key image for replica identity {:?}, but BigQuery \
                         rows are keyed by the source primary key",
                        replicated_table_schema.name(),
                        replicated_table_schema.identity_type()
                    )
                );
            }

            let primary_key_column_count =
                replicated_table_schema.primary_key_column_schemas().len();
            let old_key_values = row.values();
            if old_key_values.len() != primary_key_column_count {
                bail!(
                    ErrorKind::InvalidState,
                    "BigQuery key image does not match the source primary key",
                    format!(
                        "Expected {} key values for table '{}', got {}",
                        primary_key_column_count,
                        replicated_table_schema.name(),
                        old_key_values.len()
                    )
                );
            }

            let mut new_primary_key_values = replicated_table_schema
                .column_schemas()
                .zip(new_table_row.values())
                .filter(|(column_schema, _)| column_schema.primary_key())
                .map(|(_, value)| value);

            for old_value in old_key_values {
                let Some(new_value) = new_primary_key_values.next() else {
                    bail!(
                        ErrorKind::InvalidState,
                        "BigQuery primary key schema mismatch",
                        format!(
                            "Table '{}' did not expose enough primary key columns",
                            replicated_table_schema.name()
                        )
                    );
                };

                if old_value != new_value {
                    return Ok(true);
                }
            }

            Ok(false)
        }
    }
}

/// Extracts tagged primary-key cells from an old row image.
fn bigquery_primary_key_tagged_cells_from_old_row(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: OldTableRow,
) -> EtlResult<Vec<(usize, Cell)>> {
    match old_table_row {
        OldTableRow::Full(row) => {
            let column_count = replicated_table_schema.column_schemas().len();
            let primary_key_column_count =
                replicated_table_schema.primary_key_column_schemas().len();
            if row.values().len() != column_count {
                bail!(
                    ErrorKind::InvalidState,
                    "BigQuery full row image does not match the replicated schema",
                    format!(
                        "Expected {} values for table '{}', got {}",
                        column_count,
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                );
            }

            let mut tagged_cells = Vec::with_capacity(primary_key_column_count);
            for (column_index, (column_schema, value)) in
                replicated_table_schema.column_schemas().zip(row.into_values()).enumerate()
            {
                if column_schema.primary_key() {
                    tagged_cells.push((column_index + 1, value));
                }
            }

            Ok(tagged_cells)
        }
        OldTableRow::Key(row) => {
            if !replicated_table_schema.identity_matches_primary_key() {
                bail!(
                    ErrorKind::InvalidState,
                    "BigQuery key image does not match the source primary key",
                    format!(
                        "Table '{}' emitted a key image for replica identity {:?}, but BigQuery \
                         rows are keyed by the source primary key",
                        replicated_table_schema.name(),
                        replicated_table_schema.identity_type()
                    )
                );
            }

            let primary_key_column_count =
                replicated_table_schema.primary_key_column_schemas().len();
            if row.values().len() != primary_key_column_count {
                bail!(
                    ErrorKind::InvalidState,
                    "BigQuery delete key image does not match the source primary key",
                    format!(
                        "Expected {} key values for table '{}', got {}",
                        primary_key_column_count,
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                );
            }

            let mut tagged_cells = Vec::with_capacity(primary_key_column_count);
            let mut primary_key_columns =
                replicated_table_schema.primary_key_column_schemas().peekable();
            let mut key_values = row.into_values().into_iter();
            for (column_index, column_schema) in
                replicated_table_schema.column_schemas().enumerate()
            {
                if primary_key_columns.peek().is_some_and(|primary_key_column| {
                    primary_key_column.ordinal_position == column_schema.ordinal_position
                }) {
                    primary_key_columns.next();

                    let Some(value) = key_values.next() else {
                        bail!(
                            ErrorKind::InvalidState,
                            "BigQuery delete key image shape is inconsistent",
                            format!(
                                "Table '{}' key image ended before all primary key values",
                                replicated_table_schema.name()
                            )
                        );
                    };

                    tagged_cells.push((column_index + 1, value));
                }
            }

            if key_values.next().is_some() {
                bail!(
                    ErrorKind::InvalidState,
                    "BigQuery delete key image has leftover values",
                    format!(
                        "Table '{}' key image contained more values than its primary key",
                        replicated_table_schema.name()
                    )
                );
            }

            Ok(tagged_cells)
        }
    }
}

/// Builds a CDC delete row for BigQuery from an old row image.
fn bigquery_delete_row(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: OldTableRow,
    sequence_key: String,
) -> EtlResult<BigQueryTableRow> {
    let source_column_count = replicated_table_schema.column_schemas().len();
    let mut tagged_cells =
        bigquery_primary_key_tagged_cells_from_old_row(replicated_table_schema, old_table_row)?;
    tagged_cells.push((source_column_count + 1, BigQueryOperationType::Delete.into_cell()));
    tagged_cells.push((source_column_count + 2, Cell::String(sequence_key)));

    BigQueryTableRow::try_from_tagged_cells(tagged_cells)
}

/// Calculates the optimal number of batches for table copy operations.
///
/// Estimates the size of a single row by encoding the first row, then
/// calculates how many rows would fit in approximately 10MB per batch.
fn calculate_target_batches_for_table_copy(table_rows: &[BigQueryTableRow]) -> EtlResult<usize> {
    let Some(encoded_row) = table_rows.first() else {
        return Ok(0);
    };
    let total_rows = table_rows.len();

    let estimated_row_size = encoded_row.encoded_len();

    // Calculate how many rows would fit in target batch size.
    let rows_per_batch =
        if estimated_row_size > 0 { MAX_BATCH_SIZE_BYTES / estimated_row_size } else { total_rows };

    // Calculate target number of batches, ensuring at least 1. We don't care about
    // the limit of inflight requests since that is enforced by the client.
    let target_batches = if rows_per_batch > 0 { total_rows.div_ceil(rows_per_batch) } else { 1 };

    debug!(
        total_rows,
        estimated_row_size,
        rows_per_batch,
        target_batches,
        "calculated target batches for table copy"
    );

    Ok(target_batches)
}

/// Splits table rows into optimal sub-batches for parallel execution.
///
/// Calculates the optimal distribution of rows across batches to produce the
/// target amount of batches.
fn split_table_rows(
    table_rows: Vec<BigQueryTableRow>,
    target_batches: usize,
) -> Vec<Vec<BigQueryTableRow>> {
    let total_rows = table_rows.len();

    if total_rows == 0 {
        return vec![];
    }

    if total_rows <= 1 || target_batches == 1 || total_rows <= target_batches {
        return vec![table_rows];
    }

    // Calculate optimal rows per batch to maximize parallelism.
    let optimal_rows_per_batch = total_rows.div_ceil(target_batches);

    if optimal_rows_per_batch == 0 {
        return vec![table_rows];
    }

    // Split the rows into smaller sub-batches.
    let num_sub_batches = total_rows.div_ceil(optimal_rows_per_batch);
    let rows_per_sub_batch = total_rows / num_sub_batches;
    let extra_rows = total_rows % num_sub_batches;

    let mut batches = Vec::with_capacity(num_sub_batches);
    let mut remaining = table_rows;
    for i in 0..num_sub_batches {
        // Distribute extra rows evenly across the first few batches.
        let batch_size = rows_per_sub_batch + if i < extra_rows { 1 } else { 0 };
        let rest = remaining.split_off(batch_size);
        batches.push(remaining);
        remaining = rest;
    }

    batches
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::types::{CellNonOptional, ColumnSchema, IdentityMask, TableId, TableSchema, Type};
    use prost::Message;

    use super::*;

    fn replicated_schema(identity_type: IdentityType) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, true),
            ],
        ));
        let replication_mask = etl::types::ReplicationMask::all(&table_schema);
        let identity_mask = match identity_type {
            IdentityType::Full => IdentityMask::from_bytes(vec![1, 1]),
            IdentityType::PrimaryKey => IdentityMask::from_bytes(vec![1, 0]),
            IdentityType::AlternativeKey => IdentityMask::from_bytes(vec![0, 1]),
            IdentityType::Missing => IdentityMask::from_bytes(vec![0, 0]),
        };

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    #[test]
    fn table_name_to_bigquery_table_id_no_underscores() {
        let table_name = TableName::new("schema".to_string(), "table".to_string());
        assert_eq!(table_name_to_bigquery_table_id(&table_name).unwrap(), "schema_table");
    }

    #[test]
    fn table_name_to_bigquery_table_id_with_underscores() {
        let table_name = TableName::new("a_b".to_string(), "c_d".to_string());
        assert_eq!(table_name_to_bigquery_table_id(&table_name).unwrap(), "a__b_c__d");
    }

    #[test]
    fn table_name_to_bigquery_table_id_collision_prevention() {
        // These two cases previously collided to "a_b_c"
        let table_name1 = TableName::new("a_b".to_string(), "c".to_string());
        let table_name2 = TableName::new("a".to_string(), "b_c".to_string());

        let id1 = table_name_to_bigquery_table_id(&table_name1).unwrap();
        let id2 = table_name_to_bigquery_table_id(&table_name2).unwrap();

        assert_eq!(id1, "a__b_c");
        assert_eq!(id2, "a_b__c");
        assert_ne!(id1, id2, "Table IDs should not collide");
    }

    #[test]
    fn table_name_to_bigquery_table_id_multiple_underscores() {
        let table_name = TableName::new("a__b".to_string(), "c__d".to_string());
        assert_eq!(table_name_to_bigquery_table_id(&table_name).unwrap(), "a____b_c____d");
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_valid() {
        let table_id = "users_table_123";
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "users_table");
        assert_eq!(parsed.1, 123);
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_zero_sequence() {
        let table_id = "simple_table_0";
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "simple_table");
        assert_eq!(parsed.1, 0);
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_large_sequence() {
        let table_id = "test_table_18446744073709551615"; // u64::MAX
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "test_table");
        assert_eq!(parsed.1, u64::MAX);
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_escaped_underscores() {
        let table_id = "a__b_c__d_42";
        let parsed = table_id.parse::<SequencedBigQueryTableId>().unwrap();
        assert_eq!(parsed.to_bigquery_table_id(), "a__b_c__d");
        assert_eq!(parsed.1, 42);
    }

    #[test]
    fn sequenced_bigquery_table_id_new() {
        let table_id = SequencedBigQueryTableId::new("users_table".to_string());
        assert_eq!(table_id.to_bigquery_table_id(), "users_table");
        assert_eq!(table_id.1, 0);
    }

    #[test]
    fn sequenced_bigquery_table_id_new_with_underscores() {
        let table_id = SequencedBigQueryTableId::new("a__b_c__d".to_string());
        assert_eq!(table_id.to_bigquery_table_id(), "a__b_c__d");
        assert_eq!(table_id.1, 0);
    }

    #[test]
    fn sequenced_bigquery_table_id_next() {
        let table_id = SequencedBigQueryTableId::new("users_table".to_string());
        let next_table_id = table_id.next();

        assert_eq!(table_id.1, 0);
        assert_eq!(next_table_id.1, 1);
        assert_eq!(next_table_id.to_bigquery_table_id(), "users_table");
    }

    #[test]
    fn sequenced_bigquery_table_id_next_increments_correctly() {
        let table_id = SequencedBigQueryTableId("test_table".to_string(), 42);
        let next_table_id = table_id.next();

        assert_eq!(next_table_id.1, 43);
        assert_eq!(next_table_id.to_bigquery_table_id(), "test_table");
    }

    #[test]
    fn sequenced_bigquery_table_id_next_max_value() {
        let table_id = SequencedBigQueryTableId("test_table".to_string(), u64::MAX - 1);
        let next_table_id = table_id.next();

        assert_eq!(next_table_id.1, u64::MAX);
        assert_eq!(next_table_id.to_bigquery_table_id(), "test_table");
    }

    #[test]
    fn sequenced_bigquery_table_id_to_bigquery_table_id() {
        let table_id = SequencedBigQueryTableId("users_table".to_string(), 123);
        assert_eq!(table_id.to_bigquery_table_id(), "users_table");
    }

    #[test]
    fn sequenced_bigquery_table_id_to_bigquery_table_id_with_underscores() {
        let table_id = SequencedBigQueryTableId("a__b_c__d".to_string(), 42);
        assert_eq!(table_id.to_bigquery_table_id(), "a__b_c__d");
    }

    #[test]
    fn sequenced_bigquery_table_id_to_bigquery_table_id_zero_sequence() {
        let table_id = SequencedBigQueryTableId("simple_table".to_string(), 0);
        assert_eq!(table_id.to_bigquery_table_id(), "simple_table");
    }

    #[test]
    fn validate_bigquery_replica_identity_accepts_primary_key() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);

        validate_bigquery_replica_identity(&replicated_table_schema).unwrap();
    }

    #[test]
    fn validate_bigquery_replica_identity_accepts_full() {
        let replicated_table_schema = replicated_schema(IdentityType::Full);

        validate_bigquery_replica_identity(&replicated_table_schema).unwrap();
    }

    #[test]
    fn validate_bigquery_replica_identity_rejects_alternative_key() {
        let replicated_table_schema = replicated_schema(IdentityType::AlternativeKey);

        let error = validate_bigquery_replica_identity(&replicated_table_schema).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
    }

    #[test]
    fn validate_bigquery_replica_identity_rejects_missing() {
        let replicated_table_schema = replicated_schema(IdentityType::Missing);

        let error = validate_bigquery_replica_identity(&replicated_table_schema).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_no_underscore() {
        let result = "tablewithoutsequence".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("No underscore found"));
        assert!(err.to_string().contains("tablewithoutsequence"));
        assert!(err.to_string().contains("Expected format: 'table_name_sequence'"));
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_invalid_sequence_number() {
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
    fn sequenced_bigquery_table_id_from_str_sequence_is_word() {
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
    fn sequenced_bigquery_table_id_from_str_negative_sequence() {
        let result = "users_table_-123".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Failed to parse sequence number"));
        assert!(err.to_string().contains("-123"));
        assert!(err.to_string().contains("users_table_-123"));
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_sequence_overflow() {
        let result = "users_table_18446744073709551616".parse::<SequencedBigQueryTableId>(); // u64::MAX + 1
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Failed to parse sequence number"));
        assert!(err.to_string().contains("18446744073709551616"));
        assert!(err.to_string().contains("users_table_18446744073709551616"));
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_empty_string() {
        let result = "".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("No underscore found"));
        assert!(err.to_string().contains("''"));
        assert!(err.to_string().contains("Expected format: 'table_name_sequence'"));
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_empty_sequence() {
        let result = "users_table_".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Sequence number cannot be empty"));
        assert!(err.to_string().contains("users_table_"));
        assert!(err.to_string().contains("Expected format: 'table_name_sequence'"));
    }

    #[test]
    fn sequenced_bigquery_table_id_from_str_empty_table_name() {
        let result = "_123".parse::<SequencedBigQueryTableId>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
        assert!(err.to_string().contains("Table name cannot be empty"));
        assert!(err.to_string().contains("_123"));
        assert!(err.to_string().contains("Expected format: 'table_name_sequence'"));
    }

    #[test]
    fn sequenced_bigquery_table_id_round_trip() {
        let original = "users_table_123";
        let parsed = original.parse::<SequencedBigQueryTableId>().unwrap();
        let formatted = parsed.to_string();
        assert_eq!(original, formatted);
    }

    #[test]
    fn sequenced_bigquery_table_id_round_trip_complex() {
        let original = "a__b_c__d_999";
        let parsed = original.parse::<SequencedBigQueryTableId>().unwrap();
        let formatted = parsed.to_string();
        assert_eq!(original, formatted);
        assert_eq!(parsed.to_bigquery_table_id(), "a__b_c__d");
        assert_eq!(parsed.1, 999);
    }

    #[test]
    fn split_table_rows_empty_input() {
        let rows: Vec<BigQueryTableRow> = vec![];
        let result = split_table_rows(rows, 4);
        assert!(result.is_empty());
    }

    #[test]
    fn split_table_rows_zero_concurrent_streams() {
        let rows = vec![BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap()];
        let result = split_table_rows(rows, 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
    }

    #[test]
    fn split_table_rows_single_concurrent_stream() {
        let rows = vec![
            BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap(),
            BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap(),
        ];
        let result = split_table_rows(rows, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2);
    }

    #[test]
    fn split_table_rows_fewer_rows_than_streams() {
        let rows = vec![
            BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap(),
            BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap(),
        ];
        let result = split_table_rows(rows, 5);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2);
    }

    #[test]
    fn split_table_rows_equal_distribution() {
        let rows =
            (0..4).map(|_| BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap()).collect();
        let result = split_table_rows(rows, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].len(), 2);
        assert_eq!(result[1].len(), 2);
    }

    #[test]
    fn split_table_rows_uneven_distribution() {
        let rows =
            (0..5).map(|_| BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap()).collect();
        let result = split_table_rows(rows, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].len(), 2); // Gets extra row
        assert_eq!(result[1].len(), 2); // Gets extra row
        assert_eq!(result[2].len(), 1);
    }

    #[test]
    fn split_table_rows_many_streams() {
        let rows =
            (0..10).map(|_| BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap()).collect();
        let result = split_table_rows(rows, 4);
        assert_eq!(result.len(), 4);

        // Verify all rows are accounted for
        let total_rows: usize = result.iter().map(Vec::len).sum();
        assert_eq!(total_rows, 10);

        // Verify approximately equal distribution
        assert_eq!(result[0].len(), 3); // Gets extra row
        assert_eq!(result[1].len(), 3); // Gets extra row
        assert_eq!(result[2].len(), 2);
        assert_eq!(result[3].len(), 2);
    }

    #[test]
    fn split_table_rows_single_row() {
        let rows = vec![BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap()];
        let result = split_table_rows(rows, 5);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 1);
    }

    #[test]
    fn calculate_target_batches_empty_rows() {
        let rows: Vec<BigQueryTableRow> = vec![];
        let result = calculate_target_batches_for_table_copy(&rows).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn calculate_target_batches_single_small_row() {
        // Create a single row with one small string value
        let rows = vec![
            BigQueryTableRow::try_from(TableRow::new(vec![Cell::String("test".to_string())]))
                .unwrap(),
        ];
        let result = calculate_target_batches_for_table_copy(&rows).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn calculate_target_batches_many_small_rows() {
        // Create many rows with small values (estimated ~50 bytes each when encoded)
        let rows: Vec<BigQueryTableRow> = (0..100_000)
            .map(|i| {
                BigQueryTableRow::try_from(TableRow::new(vec![Cell::String(format!("value_{i}"))]))
                    .unwrap()
            })
            .collect();

        let result = calculate_target_batches_for_table_copy(&rows).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn calculate_target_batches_large_rows() {
        // Create rows with large string values (each ~1MB)
        let large_string = "x".repeat(1024 * 1024); // 1MB string
        let rows: Vec<BigQueryTableRow> = (0..50)
            .map(|_| {
                BigQueryTableRow::try_from(TableRow::new(vec![Cell::String(large_string.clone())]))
                    .unwrap()
            })
            .collect();

        let result = calculate_target_batches_for_table_copy(&rows).unwrap();
        assert_eq!(result, 7);
    }

    #[test]
    fn calculate_target_batches_very_large_single_row() {
        // Create a row larger than max batch size (>10MB)
        let huge_string = "x".repeat(15 * 1024 * 1024); // 15MB string
        let rows = vec![
            BigQueryTableRow::try_from(TableRow::new(vec![Cell::String(huge_string)])).unwrap(),
        ];

        let result = calculate_target_batches_for_table_copy(&rows).unwrap();
        // Even though the row is too large, we should still get 1 batch
        // (the actual send will fail, but batching logic should handle it)
        assert_eq!(result, 1);
    }

    #[test]
    fn bigquery_delete_key_row_encodes_identity_and_cdc_tags_against_descriptor_numbers() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, false),
                ColumnSchema::new("age".to_string(), Type::INT4, -1, 3, None, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            table_schema,
            etl::types::ReplicationMask::from_bytes(vec![1, 1, 1]),
            IdentityMask::from_bytes(vec![1, 0, 0]),
        );

        let row = bigquery_delete_row(
            &replicated_table_schema,
            OldTableRow::Key(TableRow::new(vec![Cell::I32(42)])),
            "lsn:1".to_string(),
        )
        .unwrap();

        let encoded = row.encode_to_vec();

        // Field tags must line up with the descriptor numbering:
        // 1 => id, 4 => _CHANGE_TYPE, 5 => _CHANGE_SEQUENCE_NUMBER.
        assert!(encoded.windows(2).any(|window| window == [0x08, 0x2a]));
        assert!(
            encoded
                .windows(8)
                .any(|window| window == [0x22, 0x06, b'D', b'E', b'L', b'E', b'T', b'E'])
        );
        assert!(
            encoded.windows(7).any(|window| window == [0x2a, 0x05, b'l', b's', b'n', b':', b'1'])
        );

        // Non-identity source columns are intentionally omitted from sparse
        // delete rows.
        assert!(!encoded.contains(&0x12));
        assert!(!encoded.contains(&0x18));
    }

    #[test]
    fn bigquery_delete_full_row_omits_non_primary_key_source_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, false),
                ColumnSchema::new("age".to_string(), Type::INT4, -1, 3, None, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            table_schema,
            etl::types::ReplicationMask::from_bytes(vec![1, 1, 1]),
            IdentityMask::from_bytes(vec![1, 1, 1]),
        );

        let row = bigquery_delete_row(
            &replicated_table_schema,
            OldTableRow::Full(TableRow::new(vec![
                Cell::I32(42),
                Cell::String("alice".to_string()),
                Cell::I32(7),
            ])),
            "lsn:1".to_string(),
        )
        .unwrap();

        assert_eq!(
            row.debug_cells(),
            vec![
                (1, CellNonOptional::I32(42)),
                (4, CellNonOptional::String("DELETE".to_string())),
                (5, CellNonOptional::String("lsn:1".to_string())),
            ]
        );
    }

    #[test]
    fn bigquery_update_rows_emits_delete_before_upsert_when_primary_key_changes() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);

        let rows = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_string())]),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
            "lsn:1".to_string(),
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].debug_cells(),
            vec![
                (1, CellNonOptional::I32(1)),
                (3, CellNonOptional::String("DELETE".to_string())),
                (4, CellNonOptional::String("lsn:1".to_string())),
            ]
        );
        assert_eq!(
            rows[1].debug_cells(),
            vec![
                (1, CellNonOptional::I32(2)),
                (2, CellNonOptional::String("updated".to_string())),
                (3, CellNonOptional::String("UPSERT".to_string())),
                (4, CellNonOptional::String("lsn:1".to_string())),
            ]
        );
    }

    #[test]
    fn bigquery_update_rows_skips_delete_when_primary_key_is_unchanged() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);

        let rows = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(1), Cell::String("updated".to_string())]),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
            "lsn:1".to_string(),
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].debug_cells(),
            vec![
                (1, CellNonOptional::I32(1)),
                (2, CellNonOptional::String("updated".to_string())),
                (3, CellNonOptional::String("UPSERT".to_string())),
                (4, CellNonOptional::String("lsn:1".to_string())),
            ]
        );
    }

    #[test]
    fn bigquery_update_rows_emits_delete_before_upsert_for_full_identity_primary_key_change() {
        let replicated_table_schema = replicated_schema(IdentityType::Full);

        let rows = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_string())]),
            Some(OldTableRow::Full(TableRow::new(vec![
                Cell::I32(1),
                Cell::String("before".to_string()),
            ]))),
            "lsn:1".to_string(),
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].debug_cells(),
            vec![
                (1, CellNonOptional::I32(1)),
                (3, CellNonOptional::String("DELETE".to_string())),
                (4, CellNonOptional::String("lsn:1".to_string())),
            ]
        );
        assert_eq!(
            rows[1].debug_cells(),
            vec![
                (1, CellNonOptional::I32(2)),
                (2, CellNonOptional::String("updated".to_string())),
                (3, CellNonOptional::String("UPSERT".to_string())),
                (4, CellNonOptional::String("lsn:1".to_string())),
            ]
        );
    }
}
