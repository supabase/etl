use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Cell, Event, PgLsn, TableId, TableName, TableRow};
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
use crate::metrics::register_metrics;

/// Delimiter separating schema from table name in BigQuery table identifiers.
const BIGQUERY_TABLE_ID_DELIMITER: &str = "_";
/// Replacement string for escaping underscores in PostgreSQL names.
const BIGQUERY_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";

/// Creates a hex-encoded sequence number from PostgreSQL LSNs to ensure correct event ordering.
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
    /// Formats the sequenced table ID as `table_name_sequence`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.0, self.1)
    }
}

/// Internal state for [`BigQueryDestination`] wrapped in `Arc<Mutex<>>`.
///
/// Contains the BigQuery client, dataset configuration, injected schema cache,
/// and table creation state cache for performance optimization.
#[derive(Debug)]
struct Inner<S> {
    client: BigQueryClient,
    dataset_id: BigQueryDatasetId,
    max_staleness_mins: Option<u16>,
    store: S,
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
/// Provides PostgreSQL-to-BigQuery data pipeline functionality including streaming inserts
/// and CDC operation handling.
#[derive(Debug, Clone)]
pub struct BigQueryDestination<S> {
    inner: Arc<Mutex<Inner<S>>>,
}

impl<S> BigQueryDestination<S>
where
    S: StateStore + SchemaStore,
{
    /// Creates a new [`BigQueryDestination`] using a service account key file path.
    ///
    /// Initializes the BigQuery client with the provided credentials and project settings.
    /// The `max_staleness_mins` parameter controls table metadata cache freshness.
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
        store: S,
    ) -> EtlResult<Self> {
        // Registring metrics here to avoid the callers having to remember to call this before
        // creating a destination.
        register_metrics();
        let client = BigQueryClient::new_with_key_path(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            store,
            created_tables: HashSet::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Creates a new [`BigQueryDestination`] using a service account key JSON string.
    ///
    /// Similar to [`BigQueryDestination::new_with_key_path`] but accepts the key content directly
    /// rather than a file path. Useful when credentials are stored in environment variables.
    pub async fn new_with_key(
        project_id: String,
        dataset_id: BigQueryDatasetId,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
        store: S,
    ) -> EtlResult<Self> {
        // Registring metrics here to avoid the callers having to remember to call this before
        // creating a destination.
        register_metrics();
        let client = BigQueryClient::new_with_key(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            store,
            created_tables: HashSet::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Prepares a table for CDC streaming operations with schema-aware table creation.
    ///
    /// Retrieves the table schema from the store, creates or verifies the BigQuery table exists,
    /// and ensures the view points to the current versioned table. Uses caching to avoid
    /// redundant table creation checks.
    async fn prepare_cdc_streaming_for_table(
        inner: &mut Inner<S>,
        table_id: &TableId,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<(SequencedBigQueryTableId, TableDescriptor)> {
        // We load the schema of the table, if present. This is needed to create the table in BigQuery
        // and also prepare the table descriptor for CDC streaming.
        let table_schema = inner
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

        // We determine the BigQuery table ID for the table together with the current sequence number.
        let bigquery_table_id = table_name_to_bigquery_table_id(&table_schema.name);
        let sequenced_bigquery_table_id =
            Self::get_or_create_sequenced_bigquery_table_id(inner, table_id, &bigquery_table_id)
                .await?;

        // Optimistically skip table creation if we've already seen this sequenced table.
        if !inner.created_tables.contains(&sequenced_bigquery_table_id) {
            inner
                .client
                .create_table_if_missing(
                    &inner.dataset_id,
                    // TODO: down the line we might want to reduce an allocation here.
                    &sequenced_bigquery_table_id.to_string(),
                    &table_schema.column_schemas,
                    inner.max_staleness_mins,
                )
                .await?;

            // Add the sequenced table to the cache.
            Self::add_to_created_tables_cache(inner, &sequenced_bigquery_table_id);

            debug!("sequenced table {sequenced_bigquery_table_id} added to creation cache");
        } else {
            debug!(
                "sequenced table {sequenced_bigquery_table_id} found in creation cache, skipping existence check"
            );
        }

        // Ensure view points to this sequenced table (uses cache to avoid redundant operations)
        Self::ensure_view_points_to_table(inner, &bigquery_table_id, &sequenced_bigquery_table_id)
            .await?;

        let table_descriptor = BigQueryClient::column_schemas_to_table_descriptor(
            &table_schema.column_schemas,
            use_cdc_sequence_column,
        );

        Ok((sequenced_bigquery_table_id, table_descriptor))
    }

    /// Adds a table to the creation cache to avoid redundant existence checks.
    fn add_to_created_tables_cache(
        inner: &mut Inner<impl SchemaStore>,
        table_id: &SequencedBigQueryTableId,
    ) {
        if inner.created_tables.contains(table_id) {
            return;
        }

        inner.created_tables.insert(table_id.clone());
    }

    /// Removes a table from the creation cache when it's found to not exist.
    fn remove_from_created_tables_cache(
        inner: &mut Inner<impl SchemaStore>,
        table_id: &SequencedBigQueryTableId,
    ) {
        inner.created_tables.remove(table_id);
    }

    /// Retrieves the current sequenced table ID or creates a new one starting at version 0.
    async fn get_or_create_sequenced_bigquery_table_id(
        inner: &Inner<S>,
        table_id: &TableId,
        bigquery_table_id: &BigQueryTableId,
    ) -> EtlResult<SequencedBigQueryTableId> {
        let Some(sequenced_bigquery_table_id) =
            Self::get_sequenced_bigquery_table_id(inner, table_id).await?
        else {
            let sequenced_bigquery_table_id =
                SequencedBigQueryTableId::new(bigquery_table_id.clone());
            inner
                .store
                .store_table_mapping(*table_id, sequenced_bigquery_table_id.to_string())
                .await?;

            return Ok(sequenced_bigquery_table_id);
        };

        Ok(sequenced_bigquery_table_id)
    }

    /// Retrieves the current sequenced table ID from the state store.
    async fn get_sequenced_bigquery_table_id(
        inner: &Inner<S>,
        table_id: &TableId,
    ) -> EtlResult<Option<SequencedBigQueryTableId>> {
        let Some(current_table_id) = inner.store.get_table_mapping(table_id).await? else {
            return Ok(None);
        };

        let sequenced_bigquery_table_id = current_table_id.parse()?;

        Ok(Some(sequenced_bigquery_table_id))
    }

    /// Ensures a view points to the specified target table, creating or updating as needed.
    ///
    /// Returns `true` if the view was created or updated, `false` if already correct.
    async fn ensure_view_points_to_table(
        inner: &mut Inner<impl SchemaStore>,
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

        inner
            .client
            .create_or_replace_view(&inner.dataset_id, view_name, &target_table_id.to_string())
            .await?;

        inner
            .created_views
            .insert(view_name.clone(), target_table_id.clone());

        debug!(
            "view {} created/updated to point to {}",
            view_name, target_table_id
        );

        Ok(true)
    }

    /// Streams rows to BigQuery with automatic retry on missing table errors.
    ///
    /// First attempts optimistic streaming. If the table is missing (detected via permission denied),
    /// clears the cache, recreates the table, and retries the operation.
    async fn stream_rows_with_fallback(
        inner: &mut Inner<S>,
        dataset_id: &BigQueryDatasetId,
        table_id: &SequencedBigQueryTableId,
        table_descriptor: &TableDescriptor,
        table_rows: Vec<TableRow>,
        orig_table_id: &TableId,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<()> {
        // First attempt - optimistically assume the table exists
        let result = inner
            .client
            .stream_rows(
                dataset_id,
                &table_id.to_string(),
                table_descriptor,
                table_rows.clone(),
            )
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                // From our testing, when trying to send data to a missing table, this is the error that is
                // returned:
                // `Status { code: PermissionDenied, message: "Permission 'TABLES_UPDATE_DATA' denied on
                // resource 'x' (or it may not exist).", source: None }`
                //
                // If we get permission denied, we assume that the table doesn't exist.
                if err.kind() == ErrorKind::PermissionDenied {
                    warn!(
                        "table {table_id} not found during streaming, removing from cache and recreating"
                    );

                    // Remove the table from our cache since it doesn't exist
                    Self::remove_from_created_tables_cache(inner, table_id);

                    // Recreate the table and table descriptor
                    let (new_table_id, new_table_descriptor) =
                        Self::prepare_cdc_streaming_for_table(
                            inner,
                            orig_table_id,
                            use_cdc_sequence_column,
                        )
                        .await?;

                    // Retry the streaming operation
                    inner
                        .client
                        .stream_rows(
                            dataset_id,
                            &new_table_id.to_string(),
                            &new_table_descriptor,
                            table_rows,
                        )
                        .await
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Writes table rows with CDC metadata for non-event streaming operations.
    ///
    /// Adds an `Upsert` operation type to each row and streams to the appropriate BigQuery table.
    async fn write_table_rows(
        &self,
        table_id: TableId,
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        let (sequenced_bigquery_table_id, table_descriptor) =
            Self::prepare_cdc_streaming_for_table(&mut inner, &table_id, false).await?;

        let dataset_id = inner.dataset_id.clone();
        for table_row in table_rows.iter_mut() {
            table_row
                .values
                .push(BigQueryOperationType::Upsert.into_cell());
        }

        Self::stream_rows_with_fallback(
            &mut inner,
            &dataset_id,
            &sequenced_bigquery_table_id,
            &table_descriptor,
            table_rows,
            &table_id,
            false,
        )
        .await?;

        Ok(())
    }

    /// Processes CDC events in batches with proper ordering and truncate handling.
    ///
    /// Groups streaming operations (insert/update/delete) by table and processes them together,
    /// then handles truncate events separately by creating new versioned tables.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_table_rows = HashMap::new();

            // Collect and deduplicate all table IDs from all truncate events.
            //
            // This is done as an optimization since if we have multiple table ids being truncated in a
            // row without applying other events in the meanwhile, it doesn't make any sense to create
            // new empty tables for each of them.
            let mut truncate_table_ids = HashSet::new();

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
                            .push(BigQueryOperationType::Upsert.into_cell());
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
                            .push(BigQueryOperationType::Upsert.into_cell());
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
                            .push(BigQueryOperationType::Delete.into_cell());
                        old_table_row.values.push(Cell::String(sequence_number));

                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(delete.table_id).or_default();
                        table_rows.push(old_table_row);
                    }
                    _ => {
                        // Every other event type is currently not supported.
                    }
                }
            }

            // Process accumulated streaming operations
            if !table_id_to_table_rows.is_empty() {
                let mut inner = self.inner.lock().await;

                for (table_id, table_rows) in table_id_to_table_rows {
                    let (bq_table_id, table_descriptor) =
                        Self::prepare_cdc_streaming_for_table(&mut inner, &table_id, true).await?;

                    let dataset_id = inner.dataset_id.clone();
                    Self::stream_rows_with_fallback(
                        &mut inner,
                        &dataset_id,
                        &bq_table_id,
                        &table_descriptor,
                        table_rows,
                        &table_id,
                        true,
                    )
                    .await?;
                }
            }

            // Collect all consecutive truncate events
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for table_id in truncate_event.rel_ids {
                        truncate_table_ids.insert(TableId::new(table_id));
                    }
                }
            }

            // Process truncate events
            if !truncate_table_ids.is_empty() {
                self.process_truncate_for_table_ids(truncate_table_ids.into_iter())
                    .await?;
            }
        }

        Ok(())
    }

    /// Handles table truncation by creating new versioned tables and updating views.
    ///
    /// Creates fresh empty tables with incremented version numbers, updates views to point
    /// to new tables, and schedules cleanup of old table versions. Deduplicates table IDs
    /// to optimize multiple truncates of the same table.
    async fn process_truncate_for_table_ids(
        &self,
        table_ids: impl IntoIterator<Item = TableId>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        for table_id in table_ids {
            let table_schema = inner.store.get_table_schema(&table_id).await?.ok_or_else(|| etl_error!(
                ErrorKind::MissingTableSchema,
                    "Table not found in the schema store",
                    format!(
                        "The table schema for table {table_id} was not found in the schema store while processing truncate events for BigQuery"
                    )
            ))?;

            // We need to determine the current sequenced table ID for this table.
            let sequenced_bigquery_table_id =
                Self::get_sequenced_bigquery_table_id(&inner, &table_id)
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
            inner
                .client
                .create_or_replace_table(
                    &inner.dataset_id,
                    &next_sequenced_bigquery_table_id.to_string(),
                    &table_schema.column_schemas,
                    inner.max_staleness_mins,
                )
                .await?;
            Self::add_to_created_tables_cache(&mut inner, &next_sequenced_bigquery_table_id);

            // Update the view to point to the new table.
            Self::ensure_view_points_to_table(
                &mut inner,
                // We convert the sequenced table ID to a BigQuery table ID since the view will have
                // the name of the BigQuery table id (without the sequence number).
                &sequenced_bigquery_table_id.to_bigquery_table_id(),
                &next_sequenced_bigquery_table_id,
            )
            .await?;

            // Update the store table mappings to point to the new table.
            inner
                .store
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
            Self::remove_from_created_tables_cache(&mut inner, &sequenced_bigquery_table_id);

            // Schedule cleanup of the previous table. We do not care to track this task since
            // if it fails, users can clean up the table on their own, but the view will still point
            // to the new data.
            let client = inner.client.clone();
            let dataset_id = inner.dataset_id.clone();
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
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.process_truncate_for_table_ids(iter::once(table_id))
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
}
