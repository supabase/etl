use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::types::{Cell, Event, PgLsn, TableId, TableName, TableRow, TruncateEvent};
use gcp_bigquery_client::storage::TableDescriptor;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::bigquery::client::{BigQueryClient, BigQueryOperationType};
use crate::bigquery::{BigQueryDatasetId, BigQueryTableId};

/// The delimiter used when generating table names in BigQuery that splits the schema from the name
/// of the table.
const BIGQUERY_TABLE_ID_DELIMITER: &str = "_";
/// The replacement string used to escape characters in the original schema and table names.
const BIGQUERY_TABLE_ID_DELIMITER_ESCAPE_REPLACEMENT: &str = "__";

/// Generates a sequence number from the LSNs of an event.
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

/// Internal state for [`BigQueryDestination`] wrapped in `Arc<Mutex<>>`.
///
/// Contains the BigQuery client, dataset configuration, injected schema cache,
/// and table creation state cache for performance optimization.
#[derive(Debug)]
struct Inner<S> {
    client: BigQueryClient,
    dataset_id: BigQueryDatasetId,
    max_staleness_mins: Option<u16>,
    schema_store: S,
    /// Cache of table IDs that have been successfully created or verified to exist.
    /// This avoids redundant `create_table_if_missing` calls for known tables.
    created_tables: HashSet<BigQueryTableId>,
    /// Tracks the current version number for each table to support truncation.
    /// Maps base table name to its current version number (e.g., "schema_table" -> 2).
    // TODO: add mapping in the store.
    table_versions: HashMap<BigQueryTableId, u32>,
    /// Cache of views that have been created and the versioned table they point to.
    /// This avoids redundant `CREATE OR REPLACE VIEW` calls for views that already point to the correct table.
    /// Maps view name to the versioned table it currently points to.
    ///
    /// # Example
    /// `{ users_table: users_table_10, orders_table: orders_table_3 }`
    created_views: HashMap<BigQueryTableId, BigQueryTableId>,
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
    S: SchemaStore,
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
        schema_store: S,
    ) -> EtlResult<Self> {
        let client = BigQueryClient::new_with_key_path(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_store,
            created_tables: HashSet::new(),
            table_versions: HashMap::new(),
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
        schema_store: S,
    ) -> EtlResult<Self> {
        let client = BigQueryClient::new_with_key(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_store,
            created_tables: HashSet::new(),
            table_versions: HashMap::new(),
            created_views: HashMap::new(),
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Prepares the table for CDC streaming with optimistic table creation caching.
    ///
    /// Loads the table schema and creates the table in BigQuery if missing. Uses an internal
    /// cache to avoid redundant existence checks for tables that have already been created.
    /// If streaming fails with a table not found error, the cache entry is invalidated.
    async fn prepare_cdc_streaming_for_table(
        inner: &mut Inner<S>,
        table_id: &TableId,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<(BigQueryTableId, TableDescriptor)> {
        // We load the schema of the table, if present. This is needed to create the table in BigQuery
        // and also prepare the table descriptor for CDC streaming.
        let table_schema = inner
            .schema_store
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

        let base_table_id = table_name_to_bigquery_table_id(&table_schema.name);
        let versioned_table_id = Self::get_versioned_table_name(inner, &base_table_id);

        // Optimistically skip table creation if we've already seen this versioned table
        if !inner.created_tables.contains(&versioned_table_id) {
            inner
                .client
                .create_table_if_missing(
                    &inner.dataset_id,
                    &versioned_table_id,
                    &table_schema.column_schemas,
                    inner.max_staleness_mins,
                )
                .await?;

            // Add the versioned table to the cache
            Self::add_to_created_tables_cache(inner, versioned_table_id.clone());

            debug!("versioned table {versioned_table_id} added to creation cache");
        } else {
            debug!(
                "versioned table {versioned_table_id} found in creation cache, skipping existence check"
            );
        }

        // Ensure view points to this versioned table (uses cache to avoid redundant operations)
        Self::ensure_view_points_to_table(inner, &base_table_id, &versioned_table_id).await?;

        let table_descriptor = BigQueryClient::column_schemas_to_table_descriptor(
            &table_schema.column_schemas,
            use_cdc_sequence_column,
        );

        Ok((versioned_table_id, table_descriptor))
    }

    /// Adds a table ID to the created tables cache if not present.
    fn add_to_created_tables_cache(inner: &mut Inner<impl SchemaStore>, table_id: BigQueryTableId) {
        if inner.created_tables.contains(&table_id) {
            return;
        }

        inner.created_tables.insert(table_id);
    }

    /// Removes a table ID from the created tables cache.
    ///
    /// Used when a table is found to not exist during streaming operations.
    fn remove_from_created_tables_cache(
        inner: &mut Inner<impl SchemaStore>,
        table_id: &BigQueryTableId,
    ) {
        inner.created_tables.remove(table_id);
    }

    /// Returns the versioned table name for the current version of a base table.
    ///
    /// If no version exists for the table, initializes it to version 0.
    fn get_versioned_table_name(
        inner: &mut Inner<impl SchemaStore>,
        base_table_id: &BigQueryTableId,
    ) -> BigQueryTableId {
        let version = inner
            .table_versions
            .entry(base_table_id.clone())
            .or_insert(0);
        format!("{base_table_id}_{version}")
    }

    /// Returns the previous versioned table name for cleanup purposes.
    fn get_previous_versioned_table_name(
        inner: &Inner<impl SchemaStore>,
        base_table_id: &BigQueryTableId,
    ) -> Option<BigQueryTableId> {
        inner
            .table_versions
            .get(base_table_id)
            .and_then(|&version| {
                if version > 0 {
                    Some(format!("{}_{}", base_table_id, version - 1))
                } else {
                    None
                }
            })
    }

    /// Increments the version for a table and returns the new versioned table name.
    fn increment_table_version(
        inner: &mut Inner<impl SchemaStore>,
        base_table_id: &BigQueryTableId,
    ) -> BigQueryTableId {
        let version = inner
            .table_versions
            .entry(base_table_id.clone())
            .or_insert(0);
        *version += 1;

        format!("{base_table_id}_{version}")
    }

    /// Checks if a view needs to be created or updated and performs the operation if needed.
    ///
    /// Returns `true` if the view was created/updated, `false` if it was already pointing to the correct table.
    async fn ensure_view_points_to_table(
        inner: &mut Inner<impl SchemaStore>,
        view_name: &BigQueryTableId,
        target_table_id: &BigQueryTableId,
    ) -> EtlResult<bool> {
        // Check if the view already points to the correct table
        if let Some(current_target) = inner.created_views.get(view_name)
            && current_target == target_table_id
        {
            debug!(
                "view {} already points to {}, skipping creation",
                view_name, target_table_id
            );

            return Ok(false);
        }

        // Create or replace the view
        inner
            .client
            .create_or_replace_view(&inner.dataset_id, view_name, target_table_id)
            .await?;

        // Update cache
        inner
            .created_views
            .insert(view_name.clone(), target_table_id.clone());

        debug!(
            "view {} created/updated to point to {}",
            view_name, target_table_id
        );

        Ok(true)
    }

    /// Handles streaming rows with fallback table creation on missing table errors.
    ///
    /// Attempts to stream rows to BigQuery. If streaming fails with a table not found error,
    /// removes the table from the cache and retries with table creation.
    async fn stream_rows_with_fallback(
        inner: &mut Inner<S>,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        table_descriptor: &TableDescriptor,
        table_rows: Vec<TableRow>,
        orig_table_id: &TableId,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<()> {
        // First attempt - optimistically assume the table exists
        let result = inner
            .client
            .stream_rows(dataset_id, table_id, table_descriptor, table_rows.clone())
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
                        .stream_rows(dataset_id, &new_table_id, &new_table_descriptor, table_rows)
                        .await
                } else {
                    // Not a table not found error, propagate the original error
                    Err(err)
                }
            }
        }
    }

    /// Writes data rows to a BigQuery table, adding CDC mode metadata.
    ///
    /// Each row gets a CDC mode column and sequence number appended before streaming to BigQuery.
    async fn write_table_rows(
        &self,
        table_id: TableId,
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        let (bq_table_id, table_descriptor) =
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
            &bq_table_id,
            &table_descriptor,
            table_rows,
            &table_id,
            false,
        )
        .await?;

        Ok(())
    }

    /// Processes and writes a batch of CDC events to BigQuery.
    ///
    /// Groups events by type, handles inserts/updates/deletes via streaming, and processes truncates separately.
    /// Adds sequence numbers to ensure proper ordering of events with the same system time.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_table_rows = HashMap::new();
            let mut truncate_events = Vec::new();

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
                    truncate_events.push(truncate_event);
                }
            }

            // Process truncate events
            if !truncate_events.is_empty() {
                info!(
                    "Processing {} 'TRUNCATE' events with versioned table recreation",
                    truncate_events.len()
                );
                self.process_truncate_events(truncate_events).await?;
            }
        }

        Ok(())
    }

    /// Processes truncate events by creating new versioned tables and updating views.
    ///
    /// Maps PostgreSQL table OIDs to BigQuery table names, creates new versioned tables,
    /// updates views to point to new tables, and schedules old table cleanup.
    /// Deduplicates table IDs to avoid redundant truncate operations on the same table.
    async fn process_truncate_events(&self, truncate_events: Vec<TruncateEvent>) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        // Collect and deduplicate all table IDs from all truncate events.
        //
        // This is done as an optimization since if we have multiple table ids being truncated in a
        // row without applying other events in the meanwhile, it doesn't make any sense to create
        // new empty tables for each of them.
        let mut unique_table_ids = HashSet::new();
        for truncate_event in truncate_events {
            for table_id in truncate_event.rel_ids {
                unique_table_ids.insert(table_id);
            }
        }

        for table_id in unique_table_ids {
            let Some(table_schema) = inner
                .schema_store
                .get_table_schema(&TableId::new(table_id))
                .await?
            else {
                info!(
                    "table schema not found for table_id: {}, skipping truncate",
                    table_id
                );
                continue;
            };

            let base_table_id = table_name_to_bigquery_table_id(&table_schema.name);

            // Get the previous table name for cleanup
            let previous_table_id = Self::get_previous_versioned_table_name(&inner, &base_table_id);

            // Create the new versioned table
            let new_versioned_table_id = Self::increment_table_version(&mut inner, &base_table_id);

            info!(
                "processing truncate for table {}: creating new version {}",
                base_table_id, new_versioned_table_id
            );

            // Create or replace the new table.
            //
            // We unconditionally replace the table if it's there because here we know that
            // we need the table to be empty given the truncation.
            inner
                .client
                .create_or_replace_table(
                    &inner.dataset_id,
                    &new_versioned_table_id,
                    &table_schema.column_schemas,
                    inner.max_staleness_mins,
                )
                .await?;

            // Update the view to point to the new table.
            //
            // We do this after the table has been created to that in case of failure, the
            // view can be manually updated to point to the new table.
            //
            // Unfortunately, BigQuery doesn't seem to offer transactions for DDL operations so our
            // implementation is best effort.
            Self::ensure_view_points_to_table(&mut inner, &base_table_id, &new_versioned_table_id)
                .await?;
            Self::add_to_created_tables_cache(&mut inner, new_versioned_table_id.clone());

            info!(
                "successfully processed truncate for {}: new table {}, view updated",
                base_table_id, new_versioned_table_id
            );

            if let Some(prev_table_id) = previous_table_id {
                Self::remove_from_created_tables_cache(&mut inner, &prev_table_id);

                let client = inner.client.clone();
                let dataset_id = inner.dataset_id.clone();

                // Schedule cleanup of the previous table. We do not care to track this task since
                // if it fails, users can clean up the table on their own, but the view will still point
                // to the new data.
                tokio::spawn(async move {
                    if let Err(err) = client.drop_table(&dataset_id, &prev_table_id).await {
                        warn!("failed to drop previous table {}: {}", prev_table_id, err);
                    } else {
                        info!("successfully cleaned up previous table {}", prev_table_id);
                    }
                });
            }
        }

        Ok(())
    }
}

impl<S> Destination for BigQueryDestination<S>
where
    S: SchemaStore + Send + Sync,
{
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
}
