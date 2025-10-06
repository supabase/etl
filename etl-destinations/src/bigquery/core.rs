use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{
    Cell, Event, PgLsn, RelationChange, RelationEvent, SchemaVersion, TableId, TableName, TableRow,
    VersionedTableSchema,
};
use etl::{bail, etl_error};
use gcp_bigquery_client::storage::{TableBatch, TableDescriptor};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::iter;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::bigquery::client::{BigQueryClient, BigQueryOperationType};
use crate::bigquery::encoding::BigQueryTableRow;
use crate::bigquery::{BigQueryDatasetId, BigQueryTableId};

/// Delimiter separating schema from table name in BigQuery table identifiers.
const BIGQUERY_TABLE_ID_DELIMITER: &str = "_";
/// Replacement string for escaping underscores in Postgres names.
const BIGQUERY_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";
/// Maximum number of BigQuery streaming attempts when schema propagation lags behind.
const MAX_SCHEMA_MISMATCH_ATTEMPTS: usize = 5;
/// Delay in milliseconds between retry attempts triggered by BigQuery schema mismatches.
const SCHEMA_MISMATCH_RETRY_DELAY_MS: u64 = 500;

/// Creates a hex-encoded sequence number from Postgres LSNs to ensure correct event ordering.
///
/// Creates a hex-encoded sequence number that ensures events are processed in the correct order
/// even when they have the same system time. The format is compatible with BigQuery's
/// `_CHANGE_SEQUENCE_NUMBER` column requirements.
///
/// The rationale for using the LSN is that BigQuery will preserve the highest sequence number
/// in case of equal primary key, which is what we want since in case of updates, we want the
/// latest update in Postgres order to be the winner. We have first the `commit_lsn` in the key
/// so that BigQuery can first order operations based on the LSN at which the transaction committed
/// and if two operations belong to the same transaction (meaning they have the same LSN), the
/// `start_lsn` will be used. We first order by `commit_lsn` to preserve the order in which operations
/// are received by the pipeline since transactions are ordered by commit time and not interleaved.
fn generate_sequence_number(start_lsn: PgLsn, commit_lsn: PgLsn) -> String {
    let start_lsn = u64::from(start_lsn);
    let commit_lsn = u64::from(commit_lsn);

    format!("{commit_lsn:016x}/{start_lsn:016x}")
}

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
    store: S,
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
        store: S,
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
            store,
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
        store: S,
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
            store,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Prepares a table for any operations.
    async fn prepare_table(
        &self,
        table_id: &TableId,
        schema_version: Option<SchemaVersion>,
    ) -> EtlResult<(SequencedBigQueryTableId, Arc<VersionedTableSchema>)> {
        // We hold the lock for the entire preparation to avoid race conditions since the consistency
        // of this code path is critical.
        let mut inner = self.inner.lock().await;

        // We load the schema of the table, if present. This is needed to create the table in BigQuery
        // and also prepare the table descriptor for CDC streaming.
        let table_schema = match schema_version {
            Some(schema_version) => {
                self.store
                    .get_table_schema(table_id, schema_version)
                    .await?
            }
            None => self.store.get_latest_table_schema(table_id).await?,
        }
        .ok_or_else(|| {
            etl_error!(
                ErrorKind::MissingTableSchema,
                "Table not found in the schema store",
                format!("The table schema for table {table_id} was not found in the schema store")
            )
        })?;

        // We determine the BigQuery table ID for the table together with the current sequence number.
        let bigquery_table_id = table_name_to_bigquery_table_id(&table_schema.name);
        let sequenced_bigquery_table_id = self
            .get_or_create_sequenced_bigquery_table_id(table_id, &bigquery_table_id)
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
                    &table_schema.column_schemas,
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

        // Ensure view points to this sequenced table.
        self.ensure_view_points_to_table(
            &mut inner,
            &bigquery_table_id,
            &sequenced_bigquery_table_id,
        )
        .await?;

        Ok((sequenced_bigquery_table_id, table_schema))
    }

    /// Prepares a table for CDC streaming operations with the table descriptor.
    async fn prepare_table_for_streaming(
        &self,
        table_id: &TableId,
        schema_version: Option<SchemaVersion>,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<(SequencedBigQueryTableId, Arc<TableDescriptor>)> {
        let (sequenced_bigquery_table_id, table_schema) =
            self.prepare_table(table_id, schema_version).await?;

        let table_descriptor = BigQueryClient::column_schemas_to_table_descriptor(
            &table_schema.column_schemas,
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
            self.store
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
        let Some(current_table_id) = self.store.get_table_mapping(table_id).await? else {
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
        table_id: TableId,
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // For table rows copy, we load the last table schema that we have available.
        let (sequenced_bigquery_table_id, table_descriptor) = self
            .prepare_table_for_streaming(&table_id, None, false)
            .await?;

        // Add CDC operation type to all rows (no lock needed).
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
            let (bytes_sent, bytes_received) = self.stream_with_schema_retry(table_batches).await?;

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

    /// Persists the accumulated CDC batches for each table to BigQuery.
    async fn process_table_events(
        &self,
        table_batches_by_id: HashMap<(TableId, SchemaVersion), Vec<TableRow>>,
    ) -> EtlResult<()> {
        if table_batches_by_id.is_empty() {
            return Ok(());
        }

        let mut table_batches = Vec::with_capacity(table_batches_by_id.len());
        for ((table_id, schema_version), table_rows) in table_batches_by_id {
            if table_rows.is_empty() {
                continue;
            }

            let (sequenced_bigquery_table_id, table_descriptor) = self
                .prepare_table_for_streaming(&table_id, Some(schema_version), true)
                .await?;

            let table_batch = self.client.create_table_batch(
                &self.dataset_id,
                &sequenced_bigquery_table_id.to_string(),
                table_descriptor.clone(),
                table_rows,
            )?;
            table_batches.push(table_batch);
        }

        if table_batches.is_empty() {
            return Ok(());
        }

        let (bytes_sent, bytes_received) = self.stream_with_schema_retry(table_batches).await?;

        // Logs with egress_metric = true can be used to identify egress logs.
        info!(
            bytes_sent,
            bytes_received,
            phase = "apply",
            egress_metric = true,
            "wrote cdc events to bigquery"
        );

        Ok(())
    }

    /// Streams table batches to BigQuery, retrying when schema mismatch errors occur.
    ///
    /// The rationale is that per BigQuery docs, the Storage Write API detects schema changes after
    /// a short time, on the order of minutes.
    async fn stream_with_schema_retry<T>(&self, table_batches: T) -> EtlResult<(usize, usize)>
    where
        T: Into<Arc<[TableBatch<BigQueryTableRow>]>>,
    {
        let retry_delay = Duration::from_millis(SCHEMA_MISMATCH_RETRY_DELAY_MS);
        let mut attempts = 0;

        let table_batches = table_batches.into();
        loop {
            match self
                .client
                .stream_table_batches_concurrent(table_batches.clone(), self.max_concurrent_streams)
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if !Self::is_schema_mismatch_error(&error) {
                        return Err(error);
                    }

                    attempts += 1;

                    if attempts >= MAX_SCHEMA_MISMATCH_ATTEMPTS {
                        return Err(error);
                    }

                    warn!(
                        attempt = attempts,
                        max_attempts = MAX_SCHEMA_MISMATCH_ATTEMPTS,
                        error = %error,
                        "schema mismatch detected while streaming to BigQuery; retrying"
                    );

                    sleep(retry_delay).await;
                }
            }
        }
    }

    /// Returns `true` when the error or one of its aggregated errors indicates a schema mismatch.
    fn is_schema_mismatch_error(error: &EtlError) -> bool {
        error
            .kinds()
            .contains(&ErrorKind::DestinationSchemaMismatch)
    }

    async fn apply_relation_event_changes(&self, relation_event: RelationEvent) -> EtlResult<()> {
        // We build the list of changes for this relation event, this way, we can express the event
        // in terms of a minimal set of operations to apply on the destination table schema.
        let changes = relation_event.build_changes();
        if changes.is_empty() {
            debug!(
                table_id = %relation_event.table_id,
                "relation event contained no schema changes; skipping"
            );

            return Ok(());
        }

        // We prepare the table for the changes. We don't make any assumptions on the table state, we
        // just want to work on an existing table and apply changes to it.
        let (sequenced_bigquery_table_id, _table_schema) = self
            .prepare_table(
                &relation_event.table_id,
                Some(relation_event.new_table_schema.version),
            )
            .await?;

        let sequenced_table_name = sequenced_bigquery_table_id.to_string();
        for change in changes {
            match change {
                RelationChange::AddColumn(column_schema) => {
                    let column_name = column_schema.name.clone();

                    self.client
                        .add_column(&self.dataset_id, &sequenced_table_name, &column_schema)
                        .await?;

                    debug!(
                        table = %sequenced_table_name,
                        column = %column_name,
                        "added column in BigQuery"
                    );
                }
                RelationChange::DropColumn(column_schema) => {
                    let column_name = column_schema.name.clone();

                    self.client
                        .drop_column(&self.dataset_id, &sequenced_table_name, &column_schema.name)
                        .await?;

                    debug!(
                        table = %sequenced_table_name,
                        column = %column_name,
                        "dropped column in BigQuery"
                    );
                }
                RelationChange::AlterColumn(previous_column_schema, latest_column_schema) => {
                    if previous_column_schema.typ != latest_column_schema.typ {
                        self.client
                            .alter_column_type(
                                &self.dataset_id,
                                &sequenced_table_name,
                                &latest_column_schema,
                            )
                            .await?;

                        debug!(
                            table = %sequenced_table_name,
                            column = %latest_column_schema.name,
                            "updated column type in BigQuery"
                        );
                    }
                }
            }
        }

        // TODO: implement primary key synchronization.
        // self.client
        //     .sync_primary_key(
        //         &self.dataset_id,
        //         &sequenced_table_name,
        //         &relation_event.new_table_schema.column_schemas,
        //     )
        //     .await?;
        //
        // debug!(
        //     table = %sequenced_table_name,
        //     "synchronized primary key definition in BigQuery"
        // );

        info!(
            table_id = %relation_event.table_id,
            table = %sequenced_table_name,
            "applied relation changes in BigQuery"
        );

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn push_dml_statement(
        table_batches: &mut HashMap<(TableId, SchemaVersion), Vec<TableRow>>,
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        table_id: TableId,
        mut table_row: TableRow,
        schema_version: SchemaVersion,
        operation_type: BigQueryOperationType,
    ) -> EtlResult<()> {
        // BigQuery CDC extra fields.
        let sequence_number = generate_sequence_number(start_lsn, commit_lsn);
        table_row.values.push(operation_type.into_cell());
        table_row.values.push(Cell::String(sequence_number));

        let key = (table_id, schema_version);
        if let Some(rows) = table_batches.get_mut(&key) {
            rows.push(table_row);
            return Ok(());
        }

        // TODO: maybe we want to remove this check and always postively assume that it works.
        // Preserve per-table ordering and enforce a consistent schema version per table.
        if table_batches
            .keys()
            .any(|(existing_table_id, existing_schema_version)| {
                *existing_table_id == table_id && *existing_schema_version != schema_version
            })
        {
            bail!(
                ErrorKind::InvalidState,
                "Multiple schema versions for table in batch",
                format!(
                    "Encountered schema version {schema_version} after a different version for table {table_id}"
                )
            );
        }

        table_batches.insert(key, vec![table_row]);

        Ok(())
    }

    /// Flushes the batch of events.
    async fn flush_batch(
        &self,
        table_batches_by_id: &mut HashMap<(TableId, SchemaVersion), Vec<TableRow>>,
    ) -> EtlResult<()> {
        if table_batches_by_id.is_empty() {
            return Ok(());
        }

        let table_batches_by_id = mem::take(table_batches_by_id);
        self.process_table_events(table_batches_by_id).await
    }

    /// Processes CDC events in batches with proper ordering and truncate handling.
    ///
    /// Groups streaming operations (insert/update/delete) by table and processes them together,
    /// then handles truncate events separately by creating new versioned tables.
    pub async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        // Accumulates rows for the current batch, grouped by table.
        let mut table_batches_by_id: HashMap<(TableId, SchemaVersion), Vec<TableRow>> =
            HashMap::new();

        // Process stream.
        for event in events {
            match event {
                // DML events.
                Event::Insert(insert) => {
                    Self::push_dml_statement(
                        &mut table_batches_by_id,
                        insert.start_lsn,
                        insert.commit_lsn,
                        insert.table_id,
                        insert.table_row,
                        insert.schema_version,
                        BigQueryOperationType::Upsert,
                    )?;
                }
                Event::Update(update) => {
                    Self::push_dml_statement(
                        &mut table_batches_by_id,
                        update.start_lsn,
                        update.commit_lsn,
                        update.table_id,
                        update.table_row,
                        update.schema_version,
                        BigQueryOperationType::Upsert,
                    )?;
                }
                Event::Delete(delete) => {
                    if let Some((_, old_row)) = delete.old_table_row {
                        Self::push_dml_statement(
                            &mut table_batches_by_id,
                            delete.start_lsn,
                            delete.commit_lsn,
                            delete.table_id,
                            old_row,
                            delete.schema_version,
                            BigQueryOperationType::Delete,
                        )?;
                    } else {
                        warn!("the `DELETE` event has no row, so it was skipped");
                    }
                }

                // Batch breaker events.
                Event::Relation(relation) => {
                    // Finish the current batch before applying schema change.
                    self.flush_batch(&mut table_batches_by_id).await?;

                    // Apply relation change before processing subsequent DML.
                    self.apply_relation_event_changes(relation).await?;
                }
                Event::Truncate(truncate) => {
                    // Finish the current batch before a TRUNCATE (it affects the table state).
                    self.flush_batch(&mut table_batches_by_id).await?;

                    self.process_truncate_for_table_ids(truncate.table_ids.into_iter(), true)
                        .await?;
                }

                // Unsupported events.
                other => {
                    debug!("skipping unsupported event {other:?} in BigQuery");
                }
            }
        }

        // Flush any trailing DML.
        self.flush_batch(&mut table_batches_by_id).await?;

        Ok(())
    }

    /// Handles table truncation by creating new versioned tables and updating views.
    ///
    /// Creates fresh empty tables with incremented version numbers, updates views to point
    /// to new tables, and schedules cleanup of old table versions. Deduplicates table IDs
    /// to optimize multiple truncates of the same table.
    async fn process_truncate_for_table_ids(
        &self,
        table_ids: impl IntoIterator<Item = (TableId, SchemaVersion)>,
        is_cdc_truncate: bool,
    ) -> EtlResult<()> {
        // We want to lock for the entire processing to ensure that we don't have any race conditions
        // and possible errors are easier to reason about.
        let mut inner = self.inner.lock().await;

        for (table_id, schema_version) in table_ids {
            let table_schema = self
                .store
                .get_table_schema(&table_id, schema_version)
                .await?;

            // If we are not doing CDC, it means that this truncation has been issued while recovering
            // from a failed data sync operation. In that case, we could have failed before table schemas
            // were stored in the schema store, so if we don't find a table schema we just continue
            // and emit a warning. If we are doing CDC, it's a problem if the schema disappears while
            // streaming, so we error out.
            if table_schema.is_none() && !is_cdc_truncate {
                warn!(
                    "the table schema for table {table_id} was not found in the schema store while processing truncate events for BigQuery",
                    table_id = table_id.to_string()
                );

                continue;
            }

            let table_schema = table_schema.ok_or_else(|| etl_error!(
                ErrorKind::MissingTableSchema,
                    "Table not found in the schema store",
                    format!(
                        "The table schema for table {table_id} was not found in the schema store while processing truncate events for BigQuery"
                    )
            ))?;

            // We need to determine the current sequenced table ID for this table.
            let sequenced_bigquery_table_id =
                self.get_sequenced_bigquery_table_id(&table_id)
                    .await?
                    .ok_or_else(|| etl_error!(
                        ErrorKind::MissingTableMapping,
                        "Table mapping not found",
                        format!(
                            "The table mapping for table id {table_id} was not found while processing truncate events for BigQuery"
                        )
                    ))?;

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
                    &table_schema.column_schemas,
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
            self.store
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

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let latest_schema = self
            .store
            .get_latest_table_schema(&table_id)
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

        self.process_truncate_for_table_ids(iter::once((table_id, latest_schema.version)), false)
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
    fn test_generate_sequence_number() {
        assert_eq!(
            generate_sequence_number(PgLsn::from(0), PgLsn::from(0)),
            "0000000000000000/0000000000000000"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(1), PgLsn::from(0)),
            "0000000000000000/0000000000000001"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(255), PgLsn::from(0)),
            "0000000000000000/00000000000000ff"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(65535), PgLsn::from(0)),
            "0000000000000000/000000000000ffff"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(u64::MAX), PgLsn::from(0)),
            "0000000000000000/ffffffffffffffff"
        );
    }

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
