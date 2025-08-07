use crate::bigquery::client::{BigQueryClient, BigQueryOperationType};
use crate::bigquery::{BigQueryDatasetId, BigQueryTableId};
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::types::{Cell, Event, PgLsn, TableId, TableName, TableRow, TruncateEvent};
use gcp_bigquery_client::storage::TableDescriptor;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

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
/// Contains the BigQuery client, dataset configuration, and injected schema cache.
#[derive(Debug)]
struct Inner<S> {
    client: BigQueryClient,
    dataset_id: BigQueryDatasetId,
    max_staleness_mins: Option<u16>,
    schema_store: S,
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
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Prepares the table for CDC streaming.
    ///
    /// This function loads the table schema, crates the table in BigQuery (if missing) and sets up
    /// the required table id and table descriptor that are required by BigQuery for CDC streaming.
    async fn prepare_cdc_streaming_for_table<I: Deref<Target = Inner<S>>>(
        inner: &I,
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

        let table_id = table_name_to_bigquery_table_id(&table_schema.name);

        // TODO: store the fact that the table was created to avoid every time the check, if we then
        //  try to create the value and fail, we can re-create the table.
        inner
            .client
            .create_table_if_missing(
                &inner.dataset_id,
                &table_id,
                &table_schema.column_schemas,
                inner.max_staleness_mins,
            )
            .await?;

        let table_descriptor = BigQueryClient::column_schemas_to_table_descriptor(
            &table_schema.column_schemas,
            use_cdc_sequence_column,
        );

        Ok((table_id, table_descriptor))
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

        let (table_id, table_descriptor) =
            Self::prepare_cdc_streaming_for_table(&inner, &table_id, false).await?;

        let dataset_id = inner.dataset_id.clone();
        for table_row in table_rows.iter_mut() {
            table_row
                .values
                .push(BigQueryOperationType::Upsert.into_cell());
        }
        inner
            .client
            .stream_rows(&dataset_id, &table_id, &table_descriptor, table_rows)
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

            // Process events until we hit a truncate or run out of events
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
                    let (table_id, table_descriptor) =
                        Self::prepare_cdc_streaming_for_table(&inner, &table_id, true).await?;

                    let dataset_id = inner.dataset_id.clone();
                    inner
                        .client
                        .stream_rows(&dataset_id, &table_id, &table_descriptor, table_rows)
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
                // Right now we are not processing truncate messages, but we do the streaming split
                // just to try out if splitting the streaming affects performance so that we might
                // need it down the line once we figure out a solution for truncation.
                warn!(
                    "'TRUNCATE' events are not supported, skipping apply of {} 'TRUNCATE' events",
                    truncate_events.len()
                );
            }
        }

        Ok(())
    }

    /// Processes truncate events by executing `TRUNCATE TABLE` statements in BigQuery.
    ///
    /// Maps PostgreSQL table OIDs to BigQuery table names and issues truncate commands.
    #[allow(dead_code)]
    async fn process_truncate_events(&self, truncate_events: Vec<TruncateEvent>) -> EtlResult<()> {
        let inner = self.inner.lock().await;

        for truncate_event in truncate_events {
            for table_id in truncate_event.rel_ids {
                if let Some(table_schema) = inner
                    .schema_store
                    .get_table_schema(&TableId::new(table_id))
                    .await?
                {
                    inner
                        .client
                        .truncate_table(
                            &inner.dataset_id,
                            &table_name_to_bigquery_table_id(&table_schema.name),
                        )
                        .await?;
                } else {
                    info!(
                        "table schema not found for table_id: {}, skipping truncate",
                        table_id
                    );
                }
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
