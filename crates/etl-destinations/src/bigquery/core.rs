use std::{collections::HashMap, fmt::Display, str::FromStr, sync::Arc};

use etl::{
    bail,
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    destination::{
        Destination, DestinationTableMetadata, DestinationWriteStatus, DropTableForCopyResult,
        TableCopyBatchId, WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    event::{Event, EventPosition},
    pipeline::PipelineId,
    schema::{
        ColumnAlterationKind, ColumnOperation, ColumnPresenceChangeReason, IdentityType,
        ReplicatedTableSchema, ReplicationMask, SchemaPlan, SnapshotId, TableId, TableName,
        is_array_type,
    },
    store::DestinationStore,
    task::TaskRegistry,
};
use etl_config::shared::{BigQueryTableOptions, BigQueryTableOptionsConfig};
use gcp_bigquery_client::storage::{MAX_BATCH_SIZE_BYTES, TableDescriptor};
use metrics::histogram;
use prost::Message;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    bigquery::{
        BIGQUERY_COLUMN_NAME_MAPPING, BigQueryDatasetId, BigQueryTableId,
        client::{BigQueryClient, BigQueryOperationType},
        encoding::BigQueryTableRow,
        metrics::{ETL_BQ_APPEND_BATCHES_BATCH_SIZE, register_metrics},
        schema::{WriteMode, column_default_sql, column_schemas_to_table_descriptor},
    },
    recovery::{
        ensure_destination_schema_matches_metadata, ensure_relation_schema_transition,
        ensure_reset_table_name_is_unambiguous, warn_unsupported_column_type_change,
    },
    table_name::try_stringify_table_name as table_name_to_bigquery_view_id,
};

/// Width of a random table-ID suffix encoding a full 128-bit UUID in base 36.
const RANDOM_TABLE_ID_SUFFIX_LENGTH: usize = 25;

/// Internal CDC sequence ordinal for single-row changes and generated deletes.
const BIGQUERY_SEQUENCE_ORDINAL_FIRST: u64 = 0;

/// Internal CDC sequence ordinal for generated upserts after generated deletes.
const BIGQUERY_SEQUENCE_ORDINAL_SECOND: u64 = 1;

/// Snapshot rows use a baseline sequence below every WAL event.
///
/// BigQuery compares sequences only within a primary key. Distinct copied keys
/// remain distinct with this shared baseline; retries of one key contain the
/// same snapshot values. Copy batch IDs and row offsets do not add ordering
/// information because copy partitions read disjoint rows from one snapshot.
const BIGQUERY_COPY_SEQUENCE_NUMBER: &str = "0/0/0";

/// BigQuery-reserved column prefixes, compared case-insensitively.
const BIGQUERY_RESERVED_COLUMN_PREFIXES: [&str; 9] = [
    "_TABLE_",
    "_FILE_",
    "_PARTITION",
    "_ROW_TIMESTAMP",
    "__ROOT__",
    "_COLIDENTIFIER",
    "_CHANGE_SEQUENCE_NUMBER",
    "_CHANGE_TYPE",
    "_CHANGE_TIMESTAMP",
];

/// The suffix distinguishing physical tables created under the same view ID.
///
/// Existing metadata may contain a legacy numeric suffix; new tables always
/// receive a random suffix.
#[derive(Debug, PartialEq)]
enum TableIdSuffix {
    /// A legacy numeric suffix, retained only for reading existing metadata.
    Legacy(u64),
    /// A random identity independent of clocks, resets, and metadata history.
    Random(Uuid),
}

impl Display for TableIdSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(suffix) => write!(f, "{suffix}"),
            Self::Random(uuid) => {
                // Base 36 retains every UUID bit in 25 lowercase characters.
                // Fixed width distinguishes it from every legacy u64 suffix;
                // lowercase also works in case-insensitive BigQuery datasets.
                let mut value = uuid.as_u128();
                let mut digits = ['0'; RANDOM_TABLE_ID_SUFFIX_LENGTH];
                for digit in digits.iter_mut().rev() {
                    // The remainder is below 36, so both conversions succeed.
                    let remainder = u32::try_from(value % 36).unwrap();
                    *digit = char::from_digit(remainder, 36).unwrap();
                    value /= 36;
                }
                for digit in digits {
                    write!(f, "{digit}")?;
                }
                Ok(())
            }
        }
    }
}

/// A physical BigQuery table identifier distinct from its stable logical view.
///
/// Each creation attempt receives a fresh UUID. Physical names are never
/// deliberately reused, avoiding stale table and ingestion references while
/// deletion and creation propagate through BigQuery.
///
/// The prefix records the view ID at creation and remains unchanged when the
/// view is renamed. Both legacy numeric and random suffixes identify physical
/// tables.
#[derive(Debug, PartialEq)]
struct PhysicalBigQueryTableId(BigQueryTableId, TableIdSuffix);

impl PhysicalBigQueryTableId {
    /// Allocates a fresh physical table ID using the initial view ID as a
    /// prefix.
    fn new(view_id: BigQueryTableId) -> Self {
        Self(view_id, TableIdSuffix::Random(Uuid::new_v4()))
    }
}

impl FromStr for PhysicalBigQueryTableId {
    type Err = EtlError;

    /// Parses legacy numeric suffixes and fixed-width lowercase base-36 UUIDs.
    fn from_str(table_id: &str) -> Result<Self, Self::Err> {
        let parsed = table_id.rsplit_once('_').and_then(|(name_prefix, suffix)| {
            if name_prefix.is_empty() || suffix.is_empty() {
                return None;
            }

            let suffix = if suffix.len() == RANDOM_TABLE_ID_SUFFIX_LENGTH {
                if !suffix.bytes().all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase()) {
                    return None;
                }
                TableIdSuffix::Random(Uuid::from_u128(u128::from_str_radix(suffix, 36).ok()?))
            } else {
                if suffix.len() > 20
                    || (suffix.len() > 1 && suffix.starts_with('0'))
                    || !suffix.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return None;
                }
                TableIdSuffix::Legacy(suffix.parse().ok()?)
            };

            Some(Self(name_prefix.to_owned(), suffix))
        });

        parsed.ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid physical BigQuery table ID",
                format!(
                    "Table ID '{table_id}' must end in a u64 suffix or a 25-character lowercase \
                     base-36 suffix"
                )
            )
        })
    }
}

impl Display for PhysicalBigQueryTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.0, self.1)
    }
}

/// Resolves the physical target independently of its current logical view.
///
/// A source rename changes the view name but retains the physical identity.
fn parse_table_id_from_metadata(
    metadata: &DestinationTableMetadata,
) -> EtlResult<PhysicalBigQueryTableId> {
    metadata.table_id().parse()
}

/// Attempts physical-table deletion without making replication depend on
/// cleanup.
///
/// Call only for an obsolete table after switching the view and persisting
/// metadata, or after dropping the logical view during an explicit reset.
/// Failed deletions can leave orphans requiring manual cleanup.
async fn delete_obsolete_table(
    client: &BigQueryClient,
    dataset_id: &BigQueryDatasetId,
    table_id: PhysicalBigQueryTableId,
) {
    if let Err(error) = client.drop_table_if_exists(dataset_id, &table_id.to_string()).await {
        warn!(
            %dataset_id,
            %table_id,
            error = %error,
            "failed to delete obsolete bigquery table; manual cleanup may be required"
        );
    } else {
        info!(%dataset_id, %table_id, "deleted obsolete bigquery table");
    }
}

/// Ensures BigQuery metadata is ready for streaming operations.
fn ensure_bigquery_metadata_applied(metadata: &DestinationTableMetadata) -> EtlResult<()> {
    if metadata.is_applied() {
        return Ok(());
    }

    warn!(
        destination_table_id = metadata.table_id(),
        table_schema = ?metadata.table_schema(),
        "bigquery table has unfinished metadata; automatic recovery is not implemented"
    );

    Err(etl_error!(
        ErrorKind::InvalidState,
        "BigQuery destination table schema is not applied",
        format!(
            "Table '{}' has schema state '{:?}'. Stop the pipeline, resolve any outstanding \
             BigQuery jobs, and reset the table for a fresh copy. Preserve destination metadata \
             until reset completes. Reconcile interrupted table renames before resetting.",
            metadata.table_id(),
            metadata.table_schema()
        )
    ))
}

/// Immutable physical write identity and logical name for a source table.
#[derive(Debug)]
struct TableTarget {
    /// Rendered physical table ID used by requests and destination metadata.
    table_id: BigQueryTableId,
    /// Source name identifying the logical view for this applied snapshot.
    source_table_name: TableName,
}

impl TableTarget {
    /// Renders a physical table ID once for all batches targeting it.
    fn new(table_id: PhysicalBigQueryTableId, source_table_name: TableName) -> Self {
        Self { table_id: table_id.to_string(), source_table_name }
    }
}

impl Display for TableTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.table_id)
    }
}

/// A completed table setup, retained until its next lifecycle transition.
///
/// The metadata fixes the accepted source endpoint; the target fixes the
/// logical view and physical write identity. Only Applied metadata can enter
/// this cache.
#[derive(Debug)]
struct AppliedTable {
    /// Completed destination endpoint, trusted without rereading the store.
    metadata: DestinationTableMetadata,
    /// Shared immutable physical target and logical source name.
    target: Arc<TableTarget>,
}

impl AppliedTable {
    /// Loads an applied physical identity without probing or repairing
    /// BigQuery.
    fn from_metadata(
        metadata: DestinationTableMetadata,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<Self> {
        ensure_bigquery_metadata_applied(&metadata)?;
        ensure_destination_schema_matches_metadata("BigQuery", schema.id(), &metadata, schema)?;

        let target = Arc::new(TableTarget::new(
            parse_table_id_from_metadata(&metadata)?,
            schema.name().clone(),
        ));

        Ok(Self { metadata, target })
    }

    /// Resolves a batch only when it matches this completed source endpoint.
    fn target_for_schema(&self, schema: &ReplicatedTableSchema) -> EtlResult<Arc<TableTarget>> {
        ensure_destination_schema_matches_metadata(
            "BigQuery",
            schema.id(),
            &self.metadata,
            schema,
        )?;

        if &self.target.source_table_name != schema.name() {
            bail!(
                ErrorKind::InvalidState,
                "BigQuery table name does not match the applied schema",
                format!(
                    "Table {} received name '{}', but its applied name is '{}'",
                    schema.id(),
                    schema.name(),
                    self.target.source_table_name
                )
            );
        }

        Ok(Arc::clone(&self.target))
    }
}

/// Cached Storage Write descriptor for one source schema endpoint and write
/// mode.
#[derive(Debug)]
struct PreparedTableSchema {
    /// Immutable source schema version represented by the descriptor.
    snapshot_id: SnapshotId,
    /// Replicated columns represented by the descriptor.
    replication_mask: ReplicationMask,
    /// Replication phase whose field-presence rules the descriptor represents.
    write_mode: WriteMode,
    /// Shared descriptor, cloned into SDK requests only after unlocking.
    descriptor: Arc<TableDescriptor>,
}

impl PreparedTableSchema {
    /// Checks the snapshot, replication mask, and copy/CDC mode before reuse.
    fn matches(&self, schema: &ReplicatedTableSchema, write_mode: WriteMode) -> bool {
        self.snapshot_id == schema.inner().snapshot_id
            && self.replication_mask == *schema.replication_mask()
            && self.write_mode == write_mode
    }
}

/// Applied table and descriptor cache protected by one table's lifecycle lock.
///
/// Warm operations trust this state. A cache miss loads durable metadata;
/// lifecycle transitions invalidate the cache before changing that metadata
/// or BigQuery and repopulate it only after Applied persists. External changes
/// to metadata or destination objects are not reconciled with a warm cache.
#[derive(Debug, Default)]
struct TableState {
    /// Completed setup; absent while a lifecycle transition may be incomplete.
    applied: Option<AppliedTable>,
    /// Last compiled row schema; cleared before view DDL, including resets.
    prepared_schema: Option<PreparedTableSchema>,
}

impl TableState {
    /// Clears the applied endpoint and descriptor before a lifecycle
    /// transition.
    fn invalidate(&mut self) {
        *self = Self::default();
    }

    /// Compiles once per endpoint and mode; warm batches only clone an Arc.
    fn prepare_descriptor(
        &mut self,
        schema: &ReplicatedTableSchema,
        write_mode: WriteMode,
    ) -> Arc<TableDescriptor> {
        if let Some(prepared) = &self.prepared_schema
            && prepared.matches(schema, write_mode)
        {
            return Arc::clone(&prepared.descriptor);
        }

        let descriptor = Arc::new(column_schemas_to_table_descriptor(schema, write_mode));
        self.prepared_schema = Some(PreparedTableSchema {
            snapshot_id: schema.inner().snapshot_id,
            replication_mask: schema.replication_mask().clone(),
            write_mode,
            descriptor: Arc::clone(&descriptor),
        });

        descriptor
    }
}

/// A BigQuery destination that implements the ETL [`Destination`] trait.
///
/// Provides Postgres-to-BigQuery data pipeline functionality including
/// streaming inserts and CDC operation handling.
///
/// A short registry lock locates each table's state. Preparation and lifecycle
/// operations serialize per table, so unrelated tables can proceed during DDL.
///
/// ETL waits for each streaming completion before dispatching its successor,
/// including across retryable apply failures. Table locks therefore cover
/// preparation and lifecycle transitions without serializing row appends.
/// Copy partitions append disjoint rows from one snapshot in parallel; a
/// restarted copy receives a fresh physical table identity.
///
/// Submitted view DDL can outlive a cancelled copy attempt and race a later
/// reset of the same logical view. Remote job reconciliation is not
/// implemented.
#[derive(Debug, Clone)]
pub struct BigQueryDestination<S> {
    /// Client for BigQuery table management, queries, and Storage Write
    /// requests.
    client: BigQueryClient,
    /// Dataset containing the logical views and their physical tables.
    dataset_id: BigQueryDatasetId,
    /// Optional maximum CDC query staleness, in minutes, for created tables.
    max_staleness_mins: Option<u16>,
    /// Partitioning and clustering options keyed by source table identity.
    table_options: Arc<HashMap<TableId, BigQueryTableOptions>>,
    /// Pipeline identity included in Storage Write request trace IDs.
    pipeline_id: PipelineId,
    /// Store for source schemas and authoritative destination table metadata.
    state_store: S,
    /// Stable per-table locks shared by all destination clones. Entries remain
    /// allocated across resets so existing waiters cannot acquire an old lock
    /// while new callers acquire a replacement lock for the same table.
    tables: Arc<Mutex<HashMap<TableId, Arc<Mutex<TableState>>>>>,
    /// Tracks streaming writes and obsolete-table cleanup for reaping and
    /// shutdown.
    tasks: TaskRegistry,
}

impl<S> BigQueryDestination<S>
where
    S: DestinationStore,
{
    /// Creates a new [`BigQueryDestination`] with a pre-configured client.
    ///
    /// Accepts an existing [`BigQueryClient`] instance, allowing the caller to
    /// control client creation separately from destination initialization. This
    /// is useful for validation scenarios where you want to create and validate
    /// the client first.
    pub fn new(
        client: BigQueryClient,
        dataset_id: BigQueryDatasetId,
        max_staleness_mins: Option<u16>,
        pipeline_id: PipelineId,
        state_store: S,
    ) -> Self {
        register_metrics();

        Self {
            client,
            dataset_id,
            max_staleness_mins,
            table_options: Arc::new(HashMap::new()),
            pipeline_id,
            state_store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            tasks: TaskRegistry::new(),
        }
    }

    /// Creates a new [`BigQueryDestination`] using a service account key file
    /// path.
    ///
    /// Initializes the BigQuery client with the provided credentials and
    /// project settings. The `max_staleness_mins` parameter controls the
    /// maximum acceptable staleness of BigQuery query results for tables
    /// created by this destination. The `connection_pool_size` parameter
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

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            table_options: Arc::new(HashMap::new()),
            pipeline_id,
            state_store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            tasks: TaskRegistry::new(),
        })
    }

    /// Creates a new [`BigQueryDestination`] using a service account key JSON
    /// string.
    ///
    /// Similar to [`BigQueryDestination::new_with_key_path`] but accepts the
    /// key content directly rather than a file path. Useful when credentials
    /// are stored in environment variables. The `connection_pool_size`
    /// parameter controls the connection pool size.
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

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            table_options: Arc::new(HashMap::new()),
            pipeline_id,
            state_store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            tasks: TaskRegistry::new(),
        })
    }

    /// Creates a new [`BigQueryDestination`] using Application Default
    /// Credentials (ADC).
    ///
    /// Initializes the BigQuery client with the default credentials and project
    /// settings. The `max_staleness_mins` parameter controls the maximum
    /// acceptable staleness of BigQuery query results for tables created by
    /// this destination. The `connection_pool_size` parameter controls the
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

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            table_options: Arc::new(HashMap::new()),
            pipeline_id,
            state_store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            tasks: TaskRegistry::new(),
        })
    }

    /// Creates a new [`BigQueryDestination`] using an installed flow
    /// authenticator.
    ///
    /// Initializes the BigQuery client with a flow authenticator using the
    /// provided secret and persistent file path. The `max_staleness_mins`
    /// parameter controls the maximum acceptable staleness of BigQuery query
    /// results for tables created by this destination. The
    /// `connection_pool_size` parameter controls the connection pool size.
    #[expect(clippy::too_many_arguments)]
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

        Ok(Self {
            client,
            dataset_id,
            max_staleness_mins,
            table_options: Arc::new(HashMap::new()),
            pipeline_id,
            state_store: store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            tasks: TaskRegistry::new(),
        })
    }

    /// Configures per-table partitioning and clustering for physical tables
    /// created by this destination.
    pub fn with_table_options(mut self, table_options: BigQueryTableOptionsConfig) -> Self {
        self.table_options = Arc::new(
            table_options
                .tables
                .into_iter()
                .map(|table| (TableId::new(table.table_id), table))
                .collect(),
        );

        self
    }

    /// Locks one table without holding the registry during preparation or DDL.
    async fn lock_table(&self, source_table_id: TableId) -> OwnedMutexGuard<TableState> {
        let table = {
            let mut tables = self.tables.lock().await;
            Arc::clone(tables.entry(source_table_id).or_default())
        };

        table.lock_owned().await
    }

    /// Schedules optimistic deletion only after the replacement is
    /// authoritative.
    ///
    /// Only the recorded physical identity is eligible. Failures are logged
    /// and can leave orphaned tables requiring manual cleanup; there is no
    /// rediscovery scan or persistent cleanup queue.
    async fn schedule_table_cleanup(&self, obsolete_table_id: PhysicalBigQueryTableId) {
        let client = self.client.clone();
        let dataset_id = self.dataset_id.clone();

        self.tasks
            .spawn_with(move || async move {
                delete_obsolete_table(&client, &dataset_id, obsolete_table_id).await;
            })
            .await;
    }

    /// Records a new physical identity before issuing create-only table DDL.
    ///
    /// The caller must invalidate any applied cache before this transition.
    /// Creating remains durable until the logical view and setup both finish.
    async fn create_physical_table(
        &self,
        schema: &ReplicatedTableSchema,
        target: &TableTarget,
    ) -> EtlResult<DestinationTableMetadata> {
        let creating = DestinationTableMetadata::new_creating(
            target.table_id.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );

        self.state_store.store_destination_table_metadata(schema.id(), creating.clone()).await?;

        self.client
            .create_table(
                &self.dataset_id,
                &target.table_id,
                schema,
                self.max_staleness_mins,
                self.table_options.get(&schema.id()),
            )
            .await?;

        Ok(creating)
    }

    /// Creates a fresh physical table and view before publishing Applied.
    ///
    /// Creating reserves the physical identity before remote DDL. Failures
    /// retain that record and never populate the applied cache. Both objects
    /// use create-only DDL so an occupied name cannot be silently adopted.
    async fn create_table_for_schema(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<AppliedTable> {
        validate_bigquery_table_shape(schema)?;

        let view_id = table_name_to_bigquery_view_id(schema.name())?;

        if self.client.table_exists(&self.dataset_id, &view_id).await? {
            bail!(
                ErrorKind::DestinationTableAlreadyExists,
                "BigQuery destination view already exists",
                format!("Cannot create view '{view_id}' without applied ownership metadata")
            );
        }

        let target = Arc::new(TableTarget::new(
            PhysicalBigQueryTableId::new(view_id.clone()),
            schema.name().clone(),
        ));

        let creating_metadata = self.create_physical_table(schema, &target).await?;
        self.client.create_view(&self.dataset_id, &view_id, &target.table_id).await?;

        let applied_metadata = creating_metadata.to_applied();
        self.state_store
            .store_destination_table_metadata(schema.id(), applied_metadata.clone())
            .await?;

        debug!(%target, "created bigquery table and view");

        Ok(AppliedTable { metadata: applied_metadata, target })
    }

    /// Loads an applied endpoint on a cache miss or creates a new table.
    ///
    /// Existing Applied metadata is trusted without remote DDL. Pending
    /// metadata warns and fails; creation recovery is deliberately separate
    /// from normal preparation and is not implemented.
    async fn load_or_create_table(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<AppliedTable> {
        match self.state_store.get_destination_table_metadata(schema.id()).await? {
            Some(metadata) => AppliedTable::from_metadata(metadata, schema),
            None => self.create_table_for_schema(schema).await,
        }
    }

    /// Prepares writes from the applied cache, loading metadata only on a miss.
    ///
    /// Every batch must match the cached snapshot and replication mask. Copy
    /// partitions serialize preparation for this table, then append in
    /// parallel.
    async fn prepare_table_for_writes(
        &self,
        schema: &ReplicatedTableSchema,
        write_mode: WriteMode,
    ) -> EtlResult<(Arc<TableTarget>, Arc<TableDescriptor>)> {
        let mut table = self.lock_table(schema.id()).await;

        let target = if let Some(applied) = &table.applied {
            applied.target_for_schema(schema)?
        } else {
            let applied = self.load_or_create_table(schema).await?;
            let target = applied.target_for_schema(schema)?;
            table.applied = Some(applied);

            target
        };

        let descriptor = table.prepare_descriptor(schema, write_mode);

        Ok((target, descriptor))
    }

    /// Resolves lifecycle metadata from the applied cache or the durable store.
    async fn get_applied_destination_table_metadata(
        &self,
        source_table_id: TableId,
        table: &TableState,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        if let Some(applied) = &table.applied {
            return Ok(Some(applied.metadata.clone()));
        }
        let metadata = self.state_store.get_destination_table_metadata(source_table_id).await?;
        if let Some(metadata) = &metadata {
            ensure_bigquery_metadata_applied(metadata)?;
        }

        Ok(metadata)
    }

    /// Writes table-copy rows using the BigQuery upsert row format.
    ///
    /// Adds an `Upsert` operation type to each row, splits them into optimal
    /// batches based on the client's row-data target, and streams them to
    /// BigQuery concurrently.
    ///
    /// All rows use [`BIGQUERY_COPY_SEQUENCE_NUMBER`]. Request retries preserve
    /// the encoded rows and physical target; a restarted copy uses a fresh
    /// physical target for its new source snapshot.
    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // Copy partitions share preparation, but append independently to the
        // same snapshot's physical target.
        let (table_target, table_descriptor) =
            self.prepare_table_for_writes(replicated_table_schema, WriteMode::Copy).await?;

        // BigQuery requires sequence numbers consistently for custom ordering.
        // A snapshot row is older than every subsequent WAL mutation, and its
        // retries always contain the same values for a given primary key.
        let table_rows = table_rows
            .into_iter()
            .map(|row| bigquery_upsert_row(row, BIGQUERY_COPY_SEQUENCE_NUMBER.to_owned()))
            .collect::<EtlResult<Vec<BigQueryTableRow>>>()?;

        // Calculate optimal target batches based on estimated row size.
        let target_batches = calculate_target_batches_for_table_copy(&table_rows)?;

        // Split table rows into optimal batches for parallel execution.
        let table_rows_batches = split_table_rows(table_rows, target_batches);
        let table_id = &table_target.table_id;

        // Create append requests from the split rows.
        let mut append_requests = Vec::with_capacity(table_rows_batches.len());
        for (request_index, table_rows) in table_rows_batches.into_iter().enumerate() {
            if !table_rows.is_empty() {
                histogram!(ETL_BQ_APPEND_BATCHES_BATCH_SIZE).record(table_rows.len() as f64);
                let append_request = self.client.create_append_request(
                    self.pipeline_id,
                    request_index,
                    &self.dataset_id,
                    table_id,
                    table_descriptor.as_ref().clone(),
                    table_rows,
                )?;
                append_requests.push(append_request);
            }
        }

        if !append_requests.is_empty() {
            self.client.append(append_requests).await?;
        }

        Ok(())
    }

    /// Handles a schema change event (Relation) by computing the diff and
    /// applying changes.
    ///
    /// This method retrieves the current destination metadata and requires its
    /// schema to be applied. Missing metadata is treated as an invariant
    /// violation since the metadata should have been recorded during initial
    /// table synchronization in [`Self::write_table_rows`]. A newer incoming
    /// snapshot computes and applies the schema diff. An older snapshot, or an
    /// equal snapshot with a different replication mask, is rejected before
    /// DDL.
    async fn handle_relation_event(&self, after_schema: &ReplicatedTableSchema) -> EtlResult<()> {
        validate_bigquery_table_capabilities(after_schema)?;

        let source_table_id = after_schema.id();
        let mut table = self.lock_table(source_table_id).await;
        let after_snapshot_id = after_schema.inner().snapshot_id;

        let Some(current_metadata) =
            self.get_applied_destination_table_metadata(source_table_id, &table).await?
        else {
            // No metadata exists, this is a broken invariant since the metadata
            // should have been recorded during write_table_rows before any
            // Relation event.
            bail!(
                ErrorKind::CorruptedTableSchema,
                "Destination metadata missing for BigQuery schema change",
                format!(
                    "Table {} received schema snapshot {}, but destination metadata from initial \
                     synchronization was not found.",
                    source_table_id, after_snapshot_id
                )
            );
        };
        let before_snapshot_id = current_metadata.snapshot_id();
        let before_replication_mask = current_metadata.replication_mask().clone();
        let after_replication_mask = after_schema.replication_mask().clone();

        // BigQuery CDC sequence numbers order row mutations, but they do not
        // make destructive or ambiguously ordered DDL safe. A relation has no
        // DML sequence number of its own, so reject older snapshots and
        // equal-snapshot mask conflicts before computing a diff.
        ensure_relation_schema_transition(
            "BigQuery",
            source_table_id,
            before_snapshot_id,
            &before_replication_mask,
            after_snapshot_id,
            &after_replication_mask,
        )?;

        // The guard above proves that equal snapshots have equal masks.
        if before_snapshot_id == after_snapshot_id {
            if let Some(applied) = &table.applied {
                applied.target_for_schema(after_schema)?;
            } else {
                table.applied = Some(AppliedTable::from_metadata(current_metadata, after_schema)?);
            }

            // Schema hasn't changed, nothing to do.
            debug!(
                table_id = %source_table_id,
                snapshot_id = %after_snapshot_id,
                replication_mask = %after_replication_mask,
                "bigquery table schema unchanged"
            );

            return Ok(());
        }

        info!(
            table_id = %source_table_id,
            before_snapshot_id = %before_snapshot_id,
            after_snapshot_id = %after_snapshot_id,
            before_replication_mask = %before_replication_mask,
            after_replication_mask = %after_replication_mask,
            "bigquery table schema change detected"
        );

        // Get the current schema from the schema store to compute the diff.
        let before_table_schema = self
            .state_store
            .get_table_schema(&source_table_id, before_snapshot_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Stored schema snapshot missing for BigQuery schema change",
                    format!(
                        "Table {} needs stored schema snapshot {} to compare with incoming \
                         snapshot {}, but it was not found.",
                        source_table_id, before_snapshot_id, after_snapshot_id
                    )
                )
            })?;

        // Build a ReplicatedTableSchema using the stored replication mask.
        let before_schema =
            ReplicatedTableSchema::from_mask(before_table_schema, before_replication_mask.clone());

        // Run validations on the schema changes.
        ensure_bigquery_primary_key_unchanged(&before_schema, after_schema)?;

        // Plan the schema changes and validate.
        let plan = before_schema.plan_schema_change(after_schema, BIGQUERY_COLUMN_NAME_MAPPING)?;
        validate_bigquery_schema_plan(after_schema, &plan)?;

        // The applied snapshot identifies the logical view; metadata resolves
        // the physical target even after that view has been renamed.
        let before_view_id = table_name_to_bigquery_view_id(before_schema.name())?;
        let after_view_id = if let Some(change) = plan.diff().table_name_change() {
            table_name_to_bigquery_view_id(change.after_name())?
        } else {
            before_view_id.clone()
        };

        if before_view_id != after_view_id
            && self.client.table_exists(&self.dataset_id, &after_view_id).await?
        {
            bail!(
                ErrorKind::DestinationTableAlreadyExists,
                "BigQuery rename target already exists",
                format!(
                    "Cannot rename view '{before_view_id}' to occupied destination \
                     '{after_view_id}'"
                )
            );
        }

        // A source rename changes the view ID while preserving the physical
        // table ID recorded in metadata.
        let table_id = parse_table_id_from_metadata(&current_metadata)?;

        // Mark as applying before making changes with the after snapshot and
        // replication mask.
        //
        // NOTE: BigQuery does not support transactional DDL, so if the system
        // crashes while in 'Applying' state, the destination table may be in an
        // partially changed state. Automatic recovery is not implemented:
        // stop the pipeline, resolve outstanding jobs, and reset for a fresh
        // copy. A pending table rename requires reconciliation before reset.
        let updated_metadata = DestinationTableMetadata::new_applied(
            current_metadata.table_id().to_owned(),
            before_snapshot_id,
            before_replication_mask.clone(),
        )
        .with_schema_change(after_snapshot_id, after_replication_mask.clone())?;

        // Cached preparation is valid only for Applied metadata. Invalidate
        // before persisting Applying or mutating the logical view.
        table.invalidate();

        self.state_store
            .store_destination_table_metadata(source_table_id, updated_metadata.clone())
            .await?;

        // BigQuery cannot rename views. Recently streamed physical tables can
        // require hours of downtime before renaming, so retain the
        // physical identity and move only the logical view.
        if before_view_id != after_view_id {
            self.client
                .create_view(&self.dataset_id, &after_view_id, &table_id.to_string())
                .await?;
            self.client.drop_view_if_exists(&self.dataset_id, &before_view_id).await?;
        }

        if let Err(err) = self.apply_schema_plan(&source_table_id, &table_id, &plan).await {
            warn!(
                table_id = %source_table_id,
                error = %err,
                "bigquery table schema change failed; manual intervention may be required"
            );
            return Err(err);
        }

        // We create or replace the view unconditionally since we always want to
        // have it up to date after any schema change.
        let table_target = Arc::new(TableTarget::new(table_id, after_schema.name().clone()));
        self.client
            .create_or_replace_view(&self.dataset_id, &after_view_id, &table_target.table_id)
            .await?;

        // We must invalidate all connections here to make sure that caches on
        // the connection side on BigQuery do not interfere with the schema
        // changes. Each worker processes invalidations in channel order and
        // this call waits for all workers to ack them. Parallel invalidations
        // may interleave, but once this returns, later appends will use fresh
        // connections.
        self.client.invalidate_all_connections().await;

        // Applied includes both the logical view and refreshed ingestion state.
        let applied_metadata = updated_metadata.to_applied();
        self.state_store
            .store_destination_table_metadata(source_table_id, applied_metadata.clone())
            .await?;
        table.applied = Some(AppliedTable { metadata: applied_metadata, target: table_target });

        info!(
            table_id = %source_table_id,
            snapshot_id = %after_snapshot_id,
            "bigquery table schema change completed"
        );

        Ok(())
    }

    /// Executes the plan's ordered column operations on the physical table.
    ///
    /// Operations execute in the supplied order. Shared planning already owns
    /// name-equivalence validation; this layer only applies BigQuery behavior
    /// for structural, nullability, and default operations and warns when
    /// skipping an unsupported type alteration. The caller handles logical
    /// view-name changes separately.
    async fn apply_schema_plan(
        &self,
        source_table_id: &TableId,
        table_id: &PhysicalBigQueryTableId,
        plan: &SchemaPlan,
    ) -> EtlResult<()> {
        if plan.ordered_column_operations().is_empty() {
            debug!(table_id = %source_table_id, "no schema changes to apply for table");
            return Ok(());
        }

        debug!(
            source_table_id = %source_table_id,
            destination_table_id = %table_id,
            added_column_count = plan.diff().added_columns.len(),
            removed_column_count = plan.diff().dropped_columns.len(),
            altered_column_count = plan.diff().altered_columns.len(),
            "applying bigquery table schema plan"
        );

        let table_id = table_id.to_string();

        // Apply the shared plan without regrouping operations.
        for operation in plan.ordered_column_operations() {
            match operation {
                ColumnOperation::DropColumn { before_column_schema, reason: _ } => {
                    self.client
                        .drop_column(&self.dataset_id, &table_id, &before_column_schema.name)
                        .await?;
                }
                ColumnOperation::AddColumn { after_column_schema, reason: _ } => {
                    self.client
                        .add_column(&self.dataset_id, &table_id, after_column_schema)
                        .await?;

                    // BigQuery leaves existing scalar values NULL and arrays
                    // empty. SET DEFAULT affects only future inserts, so
                    // supported defaults are safe for both physical table
                    // additions and publication-mask additions.
                    if let Some(default_expression) = column_default_sql(after_column_schema) {
                        self.client
                            .set_column_default(
                                &self.dataset_id,
                                &table_id,
                                &after_column_schema.name,
                                &default_expression,
                            )
                            .await?;
                    } else if after_column_schema.default_expression.is_some() {
                        warn!(
                            table_id = %source_table_id,
                            column_name = %after_column_schema.name,
                            "skipping unsupported source column default for bigquery"
                        );
                    }
                }
                ColumnOperation::AlterColumn { alteration } => {
                    let before = alteration.before_column_schema();
                    let after = alteration.after_column_schema();
                    match alteration.kind() {
                        ColumnAlterationKind::Rename => {
                            self.client
                                .rename_column(
                                    &self.dataset_id,
                                    &table_id,
                                    &before.name,
                                    &after.name,
                                )
                                .await?;
                        }
                        ColumnAlterationKind::Type => {
                            warn_unsupported_column_type_change("bigquery", &table_id, alteration);
                        }
                        ColumnAlterationKind::Nullability => {
                            // Unsupported metadata changes can leave BigQuery
                            // intentionally divergent, so later operations must
                            // tolerate that state.
                            if !before.nullable && after.nullable {
                                self.client
                                    .drop_column_not_null(&self.dataset_id, &table_id, &before.name)
                                    .await?;
                            } else {
                                warn!(
                                    table_id = %source_table_id,
                                    column_name = %before.name,
                                    "bigquery does not support setting not null on an existing \
                                     column; keeping the destination column nullable"
                                );
                            }
                        }
                        ColumnAlterationKind::Default => {
                            if before.default_expression.is_some() {
                                self.client
                                    .clear_column_default(&self.dataset_id, &table_id, &before.name)
                                    .await?;
                            }

                            if after.default_expression.is_some() {
                                if let Some(default_expression) = column_default_sql(after) {
                                    self.client
                                        .set_column_default(
                                            &self.dataset_id,
                                            &table_id,
                                            &before.name,
                                            &default_expression,
                                        )
                                        .await?;
                                } else {
                                    warn!(
                                        table_id = %source_table_id,
                                        column_name = %before.name,
                                        "skipping unsupported source column default for bigquery"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        debug!(
            destination_table_id = %table_id,
            "bigquery table schema plan applied"
        );

        Ok(())
    }

    /// Processes CDC events in batches with proper ordering and truncate
    /// handling.
    ///
    /// Groups streaming operations (insert/update/delete) by table and
    /// processes them together, then handles truncate events separately by
    /// creating new physical tables. Uses the schema from the first event of
    /// each table for table creation and descriptor building.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut events_iter = events.into_iter().peekable();

        while events_iter.peek().is_some() {
            // Maps table ID to (schema, rows). We are assuming that the table
            // schema is the same for all events within two Relation event
            // boundaries.
            let mut table_data: HashMap<TableId, (ReplicatedTableSchema, Vec<BigQueryTableRow>)> =
                HashMap::new();

            // Process events until we hit a truncate or relation event, or run
            // out of events. Truncate and Relation events require flushing all
            // batched data first before they can be processed, to maintain
            // correct ordering.
            while let Some(event) = events_iter.peek() {
                if matches!(event, Event::Truncate(_) | Event::Relation(_)) {
                    break;
                }

                let Some(event) = events_iter.next() else {
                    break;
                };
                match event {
                    Event::Insert(mut insert) => {
                        let sequence_number = bigquery_sequence_number(
                            insert.event_position(),
                            BIGQUERY_SEQUENCE_ORDINAL_FIRST,
                        );
                        insert
                            .table_row
                            .values_mut()
                            .push(BigQueryOperationType::Upsert.into_cell());
                        insert.table_row.values_mut().push(Cell::String(sequence_number));

                        let source_table_id = insert.replicated_table_schema.id();
                        let entry = table_data.entry(source_table_id).or_insert_with(|| {
                            (insert.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(BigQueryTableRow::try_from(insert.table_row)?);
                    }
                    Event::Update(update) => {
                        let event_position = update.event_position();
                        let source_table_id = update.replicated_table_schema.id();
                        let table_row = bigquery_update_new_row(
                            &update.replicated_table_schema,
                            update.updated_table_row,
                        )?;

                        let entry = table_data.entry(source_table_id).or_insert_with(|| {
                            (update.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.extend(bigquery_update_rows(
                            &update.replicated_table_schema,
                            table_row,
                            update.old_table_row,
                            event_position,
                        )?);
                    }
                    Event::Delete(delete) => {
                        let sequence_number = bigquery_sequence_number(
                            delete.event_position(),
                            BIGQUERY_SEQUENCE_ORDINAL_FIRST,
                        );
                        let old_table_row = bigquery_delete_old_row(
                            &delete.replicated_table_schema,
                            delete.old_table_row,
                        )?;

                        let source_table_id = delete.replicated_table_schema.id();
                        let entry = table_data.entry(source_table_id).or_insert_with(|| {
                            (delete.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(bigquery_delete_row(
                            &delete.replicated_table_schema,
                            old_table_row,
                            sequence_number,
                        )?);
                    }
                    event => {
                        // Every other event type is currently not supported.
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            // Process accumulated events for each table.
            if !table_data.is_empty() {
                let mut append_requests = Vec::with_capacity(table_data.len());

                for (request_index, (_, (replicated_table_schema, table_rows))) in
                    table_data.into_iter().enumerate()
                {
                    let (table_target, table_descriptor) = self
                        .prepare_table_for_writes(&replicated_table_schema, WriteMode::Cdc)
                        .await?;
                    let table_id = &table_target.table_id;
                    histogram!(ETL_BQ_APPEND_BATCHES_BATCH_SIZE).record(table_rows.len() as f64);

                    let append_request = self.client.create_append_request(
                        self.pipeline_id,
                        request_index,
                        &self.dataset_id,
                        table_id,
                        table_descriptor.as_ref().clone(),
                        table_rows,
                    )?;
                    append_requests.push(append_request);
                }

                if !append_requests.is_empty() {
                    self.client.append(append_requests).await?;
                }
            }

            // Process any Relation events (schema changes) that caused the
            // batch to flush. Multiple consecutive Relation events are
            // processed sequentially.
            while let Some(Event::Relation(_)) = events_iter.peek() {
                if let Some(Event::Relation(relation)) = events_iter.next() {
                    self.handle_relation_event(&relation.replicated_table_schema).await?;
                }
            }

            // Collect and deduplicate schemas from all truncate events.
            //
            // This is done as an optimization since if we have multiple tables
            // being truncated in a row without applying other events in the
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

    /// Handles table truncation by creating new physical tables and updating
    /// views.
    ///
    /// Creates fresh empty tables with independently allocated IDs, updates
    /// views to point to them, and schedules cleanup of the old tables.
    /// Uses the provided schemas directly instead of looking them up from a
    /// store.
    async fn process_truncate_for_schemas(
        &self,
        replicated_table_schemas: impl IntoIterator<Item = ReplicatedTableSchema>,
    ) -> EtlResult<()> {
        for replicated_table_schema in replicated_table_schemas {
            let source_table_id = replicated_table_schema.id();
            let mut table = self.lock_table(source_table_id).await;
            let view_id = table_name_to_bigquery_view_id(replicated_table_schema.name())?;

            // If no destination table metadata exists, it means the table was
            // never created in BigQuery (e.g., due to validation errors during
            // copy). In this case, we skip the truncate since there's nothing
            // to truncate.
            let Some(metadata) =
                self.get_applied_destination_table_metadata(source_table_id, &table).await?
            else {
                warn!(
                    table_id = %source_table_id,
                    "destination metadata not found while processing bigquery truncate event"
                );
                continue;
            };
            ensure_destination_schema_matches_metadata(
                "BigQuery",
                source_table_id,
                &metadata,
                &replicated_table_schema,
            )?;

            // Capture the recorded target before creation while holding
            // serialization. Name prefixes are not ownership: another source
            // can reuse a name after a rename while this table
            // retains its physical name.
            let obsolete_table_id = parse_table_id_from_metadata(&metadata)?;
            let next_table_target = Arc::new(TableTarget::new(
                PhysicalBigQueryTableId::new(view_id.clone()),
                replicated_table_schema.name().clone(),
            ));

            // Recreation follows the same pending-state contract as initial
            // creation. A failed or cancelled transition cannot reload an old
            // Applied record while its logical view may point elsewhere.
            table.invalidate();
            let creating =
                self.create_physical_table(&replicated_table_schema, &next_table_target).await?;

            // Update the view to point to the new table.
            self.client
                .create_or_replace_view(&self.dataset_id, &view_id, &next_table_target.table_id)
                .await?;

            // Update the metadata to point to the new table.
            let metadata = creating.to_applied();
            self.state_store
                .store_destination_table_metadata(source_table_id, metadata.clone())
                .await?;
            table.applied = Some(AppliedTable { metadata, target: Arc::clone(&next_table_target) });

            // These operations are not transactional. Interrupted recreation
            // stays Creating and requires reconciliation. If Applied commits
            // before acknowledgement or checkpointing fails, replay creates
            // another fresh table for the truncate. Cleanup targets only the
            // recorded obsolete identity after the replacement is Applied.

            info!(
                table_id = source_table_id.0,
                %next_table_target,
                "successfully processed truncate, view updated"
            );

            self.schedule_table_cleanup(obsolete_table_id).await;
        }

        Ok(())
    }

    /// Drops the logical view and optimistically deletes its physical table.
    ///
    /// View deletion must succeed before reset can proceed. Physical deletion
    /// failures are logged and may leave tables for manual cleanup.
    ///
    /// The table-sync lifecycle owns recovery: it retains the original metadata
    /// until this operation succeeds, then clears copy state before recreating
    /// the table. A failed or interrupted reset is retried from the same
    /// durable copy phase. Creating is reserved for fresh physical table
    /// creation.
    async fn drop_table_for_copy_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let mut table = self.lock_table(replicated_table_schema.id()).await;
        let source_table_id = replicated_table_schema.id();
        let metadata = match &table.applied {
            Some(applied) => Some(applied.metadata.clone()),
            None => self.state_store.get_destination_table_metadata(source_table_id).await?,
        };
        let Some(metadata) = metadata else {
            table.invalidate();
            return Ok(());
        };
        ensure_reset_table_name_is_unambiguous(
            "BigQuery",
            &self.state_store,
            source_table_id,
            &metadata,
        )
        .await?;
        let table_id = parse_table_id_from_metadata(&metadata)?;
        let snapshot = metadata.snapshot_id();
        let name = if snapshot == replicated_table_schema.inner().snapshot_id {
            replicated_table_schema.name().clone()
        } else {
            self.state_store
                .get_table_schema(&source_table_id, snapshot)
                .await?
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "Stored schema snapshot missing for BigQuery reset",
                        format!(
                            "Table {source_table_id} needs snapshot {snapshot} to resolve its \
                             owned view"
                        )
                    )
                })?
                .name
                .clone()
        };
        let view_id = table_name_to_bigquery_view_id(&name)?;

        // Invalidate before DDL: cancellation or an ambiguous error can leave
        // the view removed even when this call does not report success. Keep
        // metadata unchanged so the table-sync worker can retry deletion after
        // a restart, before it permits copy or streaming writes again.
        table.invalidate();

        // Removing the logical view is required. Once it is gone, failures to
        // delete its old physical target cannot expose stale data after
        // recopy. Explicit reset trusts the recorded logical name; reusing it
        // for another source before reset completes requires operator
        // reconciliation. This path does not discover or repair ownership.
        self.client.drop_view_if_exists(&self.dataset_id, &view_id).await?;

        delete_obsolete_table(&self.client, &self.dataset_id, table_id).await;

        // Release Storage Write API connections that may still refer to the
        // deleted physical table. Subsequent copies allocate fresh names.
        self.client.invalidate_all_connections().await;

        info!(table_id = source_table_id.0, "removed bigquery view before copy");

        Ok(())
    }
}

/// Returns the reserved BigQuery prefix matched by a source column name.
fn bigquery_reserved_column_prefix(column_name: &str) -> Option<&'static str> {
    BIGQUERY_RESERVED_COLUMN_PREFIXES.into_iter().find(|prefix| {
        column_name
            .get(..prefix.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
    })
}

/// Validates a source column name against BigQuery's reserved prefixes.
///
/// The Storage Write client maps flexible BigQuery column names to
/// protobuf-safe placeholders using BigQuery's `column_name` field option.
fn validate_bigquery_column_name(
    replicated_table_schema: &ReplicatedTableSchema,
    column_name: &str,
) -> EtlResult<()> {
    if let Some(reserved_prefix) = bigquery_reserved_column_prefix(column_name) {
        bail!(
            ErrorKind::SourceSchemaError,
            "BigQuery source column name uses a reserved prefix",
            format!(
                "Table '{}' column '{}' begins with BigQuery-reserved prefix '{}'.",
                replicated_table_schema.name(),
                column_name,
                reserved_prefix
            )
        );
    }

    Ok(())
}

/// Validates BigQuery-specific schema capabilities.
///
/// Shared planning owns destination name-equivalence validation during schema
/// changes. This check owns reserved names and the source primary-key
/// requirements used by BigQuery CDC.
fn validate_bigquery_table_capabilities(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    for column_schema in replicated_table_schema.column_schemas() {
        validate_bigquery_column_name(replicated_table_schema, &column_schema.name)?;
    }

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

    if !replicated_table_schema.all_primary_key_columns_replicated() {
        let omitted_columns = replicated_table_schema
            .unreplicated_primary_key_column_schemas()
            .map(|column_schema| column_schema.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        bail!(
            ErrorKind::SourceSchemaError,
            "BigQuery requires all source primary-key columns to be replicated",
            format!(
                "Table '{}' omits source primary-key columns from replication: {}",
                replicated_table_schema.name(),
                omitted_columns
            )
        );
    }

    Ok(())
}

/// Validates a complete schema before creating or writing a BigQuery table.
fn validate_bigquery_table_shape(replicated_table_schema: &ReplicatedTableSchema) -> EtlResult<()> {
    replicated_table_schema.validate_destination_column_names(BIGQUERY_COLUMN_NAME_MAPPING)?;
    validate_bigquery_table_capabilities(replicated_table_schema)
}

/// Validates BigQuery-specific semantics of a schema plan before changing
/// state.
///
/// A PostgreSQL array is a BigQuery `REPEATED` field, which cannot represent
/// `NULL`. BigQuery initializes existing rows to an empty array when such a
/// field is added. That would turn an unknown value from publication filtering
/// into invented data, so this transition requires a table resynchronization.
fn validate_bigquery_schema_plan(
    new_schema: &ReplicatedTableSchema,
    plan: &SchemaPlan,
) -> EtlResult<()> {
    for operation in plan.ordered_column_operations() {
        let ColumnOperation::AddColumn { after_column_schema, reason } = operation else {
            continue;
        };

        if *reason == ColumnPresenceChangeReason::ReplicationMask
            && is_array_type(&after_column_schema.typ)
        {
            bail!(
                ErrorKind::SourceSchemaError,
                "BigQuery cannot add a source array through publication filtering",
                format!(
                    "Table '{}' column '{}' is an array newly included by the publication. \
                     BigQuery repeated fields cannot be NULL and initialize existing rows to an \
                     empty array, so ETL cannot preserve unknown historical values. Resynchronize \
                     the table to replicate this column.",
                    new_schema.name(),
                    after_column_schema.name
                )
            );
        }
    }

    Ok(())
}

/// Rejects source primary-key changes that BigQuery cannot apply in place.
///
/// BigQuery CDC matches rows through the primary-key constraint created during
/// the initial table copy. BigQuery does not allow primary-key columns to be
/// renamed, and ETL does not rebuild the constraint during streaming schema
/// changes, so the complete key definition must remain stable.
fn ensure_bigquery_primary_key_unchanged(
    current_schema: &ReplicatedTableSchema,
    new_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    let current_primary_key = current_schema.primary_key_column_schemas().collect::<Vec<_>>();
    let new_primary_key = new_schema.primary_key_column_schemas().collect::<Vec<_>>();
    let primary_key_changed = current_primary_key.len() != new_primary_key.len()
        || current_primary_key.iter().zip(&new_primary_key).any(|(current, new)| {
            current.ordinal_position != new.ordinal_position
                || current.primary_key_ordinal_position != new.primary_key_ordinal_position
                || !BIGQUERY_COLUMN_NAME_MAPPING.equivalent(&current.name, &new.name)
        });

    if primary_key_changed {
        bail!(
            ErrorKind::SourceSchemaError,
            "BigQuery does not support changing a primary key during replication",
            format!(
                "Table '{}' changed its primary-key columns, destination identifiers, or order. \
                 BigQuery CDC uses the destination primary-key constraint, which ETL cannot alter \
                 safely in place. Restore the source primary key or resync the table.",
                new_schema.name()
            )
        );
    }

    Ok(())
}

impl<S> Destination for BigQueryDestination<S>
where
    S: DestinationStore,
{
    fn name() -> &'static str {
        etl_config::shared::DestinationKind::BigQuery.as_str()
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.tasks.shutdown().await
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let result = self.drop_table_for_copy_inner(replicated_table_schema).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        _batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let result =
            BigQueryDestination::write_table_rows(self, replicated_table_schema, table_rows).await;
        async_result.send(result.map(|_| DestinationWriteStatus::Durable));

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let destination = self.clone();
        self.tasks
            .spawn_with(move || async move {
                let result = destination.write_events(events).await;
                async_result.send(result.map(|_| DestinationWriteStatus::Durable));
            })
            .await;

        Ok(())
    }
}

/// Builds a BigQuery CDC sequence number with destination-internal ordering.
///
/// BigQuery applies CDC records for the same primary key by comparing
/// `_CHANGE_SEQUENCE_NUMBER` sections from left to right as unsigned
/// hexadecimal numbers. [`EventPosition`] supplies the source ordering as
/// `commit_lsn/tx_ordinal`; the extra section gives this destination a stable
/// place to order multiple BigQuery rows generated from one source event.
///
/// This matters for primary-key-changing updates, which are represented as a
/// generated `DELETE` for the old key followed by a generated `UPSERT` for the
/// new key. Because both rows come from the same source event, they would share
/// the same source event position without this extra section. The internal
/// ordinal makes the ordering within generated rows explicit instead of relying
/// on append order or BigQuery ingestion-time tie-breaking.
fn bigquery_sequence_number(event_position: EventPosition, internal_ordinal: u64) -> String {
    let commit_lsn = u64::from(event_position.commit_lsn);
    format!("{commit_lsn:016x}/{:016x}/{internal_ordinal:016x}", event_position.tx_ordinal)
}

/// Builds a BigQuery CDC upsert row.
fn bigquery_upsert_row(
    mut table_row: TableRow,
    sequence_number: String,
) -> EtlResult<BigQueryTableRow> {
    table_row.values_mut().push(BigQueryOperationType::Upsert.into_cell());
    table_row.values_mut().push(Cell::String(sequence_number));

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
    event_position: EventPosition,
) -> EtlResult<Vec<BigQueryTableRow>> {
    let primary_key_changed = match old_table_row.as_ref() {
        // PostgreSQL omits the old-side image only when the publisher determined it was
        // unnecessary. For primary-key identity, that means the destination key did not change.
        // `FULL` updates are expected to carry an old row from pgoutput.
        Some(old_table_row) => {
            bigquery_primary_key_changed(replicated_table_schema, old_table_row, &new_table_row)?
        }
        None => {
            ensure_bigquery_update_without_old_row_can_skip_delete(replicated_table_schema)?;
            false
        }
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
            bigquery_sequence_number(event_position, BIGQUERY_SEQUENCE_ORDINAL_FIRST),
        )?);
    }
    let upsert_ordinal = if primary_key_changed {
        BIGQUERY_SEQUENCE_ORDINAL_SECOND
    } else {
        BIGQUERY_SEQUENCE_ORDINAL_FIRST
    };
    rows.push(bigquery_upsert_row(
        new_table_row,
        bigquery_sequence_number(event_position, upsert_ordinal),
    )?);

    Ok(rows)
}

/// Returns the full new row required for a BigQuery update upsert.
fn bigquery_update_new_row(
    replicated_table_schema: &ReplicatedTableSchema,
    updated_table_row: UpdatedTableRow,
) -> EtlResult<TableRow> {
    match updated_table_row {
        UpdatedTableRow::Full(row) => Ok(row),
        UpdatedTableRow::Partial(_) => Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "BigQuery update requires a full new row image",
            format!(
                "Table '{}' emitted a partial update row. BigQuery CDC UPSERT does not preserve \
                 omitted columns.",
                replicated_table_schema.name()
            )
        )),
    }
}

/// Returns the old row image required for a BigQuery delete.
fn bigquery_delete_old_row(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: Option<OldTableRow>,
) -> EtlResult<OldTableRow> {
    old_table_row.ok_or_else(|| {
        etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "BigQuery delete requires an old row image",
            format!(
                "Table '{}' emitted a delete without an old row image. BigQuery deletes are keyed \
                 by the source primary key and cannot be applied safely without it.",
                replicated_table_schema.name()
            )
        )
    })
}

/// Verifies that a BigQuery update without an old row cannot have changed the
/// destination primary key.
fn ensure_bigquery_update_without_old_row_can_skip_delete(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if matches!(replicated_table_schema.identity_type(), IdentityType::PrimaryKey) {
        Ok(())
    } else {
        Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "BigQuery update requires old primary-key values",
            format!(
                "Table '{}' emitted an update without an old row image for replica identity {:?}. \
                 BigQuery can only skip the generated delete when the source replica identity \
                 matches the primary key.",
                replicated_table_schema.name(),
                replicated_table_schema.identity_type()
            )
        ))
    }
}

/// Verifies that a key-only row image carries source primary-key values.
fn ensure_bigquery_key_image_matches_primary_key(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if matches!(replicated_table_schema.identity_type(), IdentityType::PrimaryKey) {
        Ok(())
    } else {
        Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "BigQuery key image does not match the source primary key",
            format!(
                "Table '{}' emitted a key image for replica identity {:?}, but BigQuery rows are \
                 keyed by the source primary key",
                replicated_table_schema.name(),
                replicated_table_schema.identity_type()
            )
        ))
    }
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

            ensure_bigquery_key_image_matches_primary_key(replicated_table_schema)?;

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

            ensure_bigquery_key_image_matches_primary_key(replicated_table_schema)?;

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
    sequence_number: String,
) -> EtlResult<BigQueryTableRow> {
    let source_column_count = replicated_table_schema.column_schemas().len();
    let mut tagged_cells =
        bigquery_primary_key_tagged_cells_from_old_row(replicated_table_schema, old_table_row)?;
    tagged_cells.push((source_column_count + 1, BigQueryOperationType::Delete.into_cell()));
    tagged_cells.push((source_column_count + 2, Cell::String(sequence_number)));

    BigQueryTableRow::try_from_tagged_cells(tagged_cells)
}

/// Calculates the optimal number of batches for table copy operations.
///
/// Estimates the size of a single row by encoding the first row, then
/// calculates how many rows would fit within the client's batch target.
fn calculate_target_batches_for_table_copy(table_rows: &[BigQueryTableRow]) -> EtlResult<usize> {
    let Some(encoded_row) = table_rows.first() else {
        return Ok(0);
    };
    let total_rows = table_rows.len();

    let estimated_row_size = encoded_row.encoded_len();

    // Calculate how many rows would fit in target batch size.
    let rows_per_batch = MAX_BATCH_SIZE_BYTES.checked_div(estimated_row_size).unwrap_or(total_rows);

    // Calculate target number of batches, ensuring at least 1. We don't care
    // about the limit of inflight requests since that is enforced by the
    // client.
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
    // Split from the end so each moved row is copied once, and retain the
    // original allocation for the first batch.
    for i in (1..num_sub_batches).rev() {
        let batch_size = rows_per_sub_batch + usize::from(i < extra_rows);
        batches.push(remaining.split_off(remaining.len() - batch_size));
    }
    batches.push(remaining);
    batches.reverse();

    batches
}

#[cfg(test)]
mod tests {
    use std::{future::pending, sync::Arc};

    use etl::{
        data::Cell,
        schema::{ColumnSchema, IdentityMask, PgLsn, SnapshotId, TableId, TableSchema, Type},
        store::{MemoryStore, SchemaStore, StateStore},
        test_utils::destination::write_table_rows,
    };
    use futures::FutureExt;
    use gcp_bigquery_client::{
        auth::Authenticator,
        client_builder::ClientBuilder,
        error::BQError,
        storage::{ColumnMode, StorageApiConfig},
    };
    use prost::Message;
    use tokio::sync::{Semaphore, mpsc};

    use super::*;

    /// Stops requests at authentication so lifecycle tests never contact
    /// BigQuery.
    #[derive(Clone)]
    struct UnavailableAuthenticator {
        /// Leaves authentication pending when testing cancellation.
        pending: bool,
    }

    #[async_trait::async_trait]
    impl Authenticator for UnavailableAuthenticator {
        async fn access_token(&self) -> Result<String, BQError> {
            if self.pending { pending().await } else { Err(BQError::NoToken) }
        }
    }

    /// Pauses real SDK requests before authentication can reach the network.
    #[derive(Clone)]
    struct PausedAuthenticator {
        /// Reports when an append has passed table preparation.
        reached: mpsc::UnboundedSender<()>,
        /// Closing this gate releases all requests with an authentication
        /// error.
        release: Arc<Semaphore>,
    }

    #[async_trait::async_trait]
    impl Authenticator for PausedAuthenticator {
        async fn access_token(&self) -> Result<String, BQError> {
            let _ = self.reached.send(());
            let _ = self.release.acquire().await;
            Err(BQError::NoToken)
        }
    }

    /// Builds a destination with controlled authentication and two SDK workers.
    async fn destination_with_authenticator(
        authenticator: Arc<dyn Authenticator>,
    ) -> BigQueryDestination<MemoryStore> {
        let client = ClientBuilder::new()
            .with_v2_base_url("http://127.0.0.1:1".to_owned())
            .with_storage_config(StorageApiConfig::default().with_connection_pool_size(2))
            .build_from_authenticator(authenticator)
            .await
            .unwrap();
        BigQueryDestination::new(
            BigQueryClient::from_test_client(client),
            "test_dataset".to_owned(),
            None,
            1,
            MemoryStore::new(),
        )
    }

    /// Builds a destination whose remote operations fail or remain pending.
    async fn unavailable_destination(pending: bool) -> BigQueryDestination<MemoryStore> {
        destination_with_authenticator(Arc::new(UnavailableAuthenticator { pending })).await
    }

    /// Builds a cached physical target matching the test schema's logical name.
    fn table_target(schema: &ReplicatedTableSchema) -> Arc<TableTarget> {
        Arc::new(TableTarget::new(
            PhysicalBigQueryTableId::new(table_name_to_bigquery_view_id(schema.name()).unwrap()),
            schema.name().clone(),
        ))
    }

    fn replicated_schema(identity_type: IdentityType) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let replication_mask = etl::schema::ReplicationMask::all(&table_schema);
        let identity_mask = match identity_type {
            IdentityType::Full => IdentityMask::from_bytes(vec![1, 1]),
            IdentityType::PrimaryKey => IdentityMask::from_bytes(vec![1, 0]),
            IdentityType::AlternativeKey => IdentityMask::from_bytes(vec![0, 1]),
            IdentityType::Missing => IdentityMask::from_bytes(vec![0, 0]),
        };

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    /// Builds distinct keyed tables for concurrency tests.
    fn schema_for_table(id: u32) -> ReplicatedTableSchema {
        let mut schema = replicated_schema(IdentityType::PrimaryKey).inner().clone();
        schema.id = TableId::new(id);
        schema.name = TableName::new("public".to_owned(), format!("table_{id}"));
        ReplicatedTableSchema::all(Arc::new(schema))
    }

    /// Seeds durable metadata and the applied cache so requests reach append
    /// without DDL.
    async fn seed_table(
        destination: &BigQueryDestination<MemoryStore>,
        schema: &ReplicatedTableSchema,
    ) {
        let target = table_target(schema);
        let metadata = DestinationTableMetadata::new_applied(
            target.table_id.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), metadata.clone())
            .await
            .unwrap();
        destination.lock_table(schema.id()).await.applied = Some(AppliedTable { metadata, target });
    }

    /// Builds a keyed schema with one caller-named non-key column.
    fn replicated_schema_with_column_name(column_name: &str) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new(column_name.to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));

        ReplicatedTableSchema::all(table_schema)
    }

    /// Encodes tagged cells the same way [`BigQueryTableRow`] does, for
    /// comparison against a row produced by the code under test.
    fn expected_row_bytes(tagged_cells: Vec<(usize, Cell)>) -> Vec<u8> {
        BigQueryTableRow::try_from_tagged_cells(tagged_cells).unwrap().encode_to_vec()
    }

    fn replicated_schema_with_partial_primary_key() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key(1),
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 2, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 3, true),
            ],
        ));
        let replication_mask = etl::schema::ReplicationMask::from_bytes(vec![0, 1, 1]);
        let identity_mask = IdentityMask::from_bytes(vec![0, 1, 0]);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    /// Builds a two-column schema with caller-selected primary-key membership
    /// and order.
    fn replicated_schema_with_primary_key_positions(
        first_position: Option<i32>,
        second_position: Option<i32>,
    ) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key_ordinal_position(first_position),
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 2, false)
                    .with_primary_key_ordinal_position(second_position),
            ],
        ));
        let replication_mask = etl::schema::ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![
            u8::from(first_position.is_some()),
            u8::from(second_position.is_some()),
        ]);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    /// Disjoint copy partitions must both reach append while the first is
    /// pending; whole streaming-batch ordering is unnecessary for snapshot
    /// rows.
    #[tokio::test]
    async fn copy_partitions_append_concurrently() {
        let (reached, mut requests) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let destination = destination_with_authenticator(Arc::new(PausedAuthenticator {
            reached,
            release: Arc::clone(&release),
        }))
        .await;
        let schema = schema_for_table(1);
        seed_table(&destination, &schema).await;

        let first = write_table_rows(
            &destination,
            &schema,
            vec![TableRow::new(vec![Cell::I32(1), Cell::Null])],
        );
        tokio::pin!(first);
        assert!(futures::poll!(&mut first).is_pending());

        requests.recv().await.unwrap();

        let second = write_table_rows(
            &destination,
            &schema,
            vec![TableRow::new(vec![Cell::I32(2), Cell::Null])],
        );
        tokio::pin!(second);
        assert!(futures::poll!(&mut second).is_pending());

        requests.recv().await.unwrap();

        release.close();

        assert!(first.await.is_err());
        assert!(second.await.is_err());

        destination.shutdown().await.unwrap();
    }

    /// Pending DDL for one table must not block or invalidate another table's
    /// cached write preparation.
    #[tokio::test]
    async fn pending_table_ddl_does_not_block_other_tables() {
        let destination = unavailable_destination(true).await;
        let cold_schema = schema_for_table(1);
        let warm_schema = schema_for_table(2);
        seed_table(&destination, &warm_schema).await;
        let (target, descriptor) =
            destination.prepare_table_for_writes(&warm_schema, WriteMode::Copy).await.unwrap();

        let pending = destination.prepare_table_for_writes(&cold_schema, WriteMode::Copy);
        tokio::pin!(pending);
        assert!(pending.as_mut().now_or_never().is_none());

        let (reused_target, reused_descriptor) = destination
            .prepare_table_for_writes(&warm_schema, WriteMode::Copy)
            .now_or_never()
            .unwrap()
            .unwrap();

        assert!(Arc::ptr_eq(&target, &reused_target));
        assert!(Arc::ptr_eq(&descriptor, &reused_descriptor));
    }

    /// Warm descriptors are reused, but each schema endpoint and write mode
    /// must produce the corresponding wire fields.
    #[test]
    fn descriptor_cache_tracks_snapshot_mask_and_write_mode() {
        let schema = replicated_schema(IdentityType::PrimaryKey);
        let mut table = TableState::default();
        let copy = table.prepare_descriptor(&schema, WriteMode::Copy);
        assert!(Arc::ptr_eq(&copy, &table.prepare_descriptor(&schema, WriteMode::Copy)));
        assert_eq!(copy.field_descriptors.len(), 4);
        assert!(matches!(copy.field_descriptors[0].mode, ColumnMode::Required));

        let cdc = table.prepare_descriptor(&schema, WriteMode::Cdc);
        assert!(!Arc::ptr_eq(&copy, &cdc));
        assert!(Arc::ptr_eq(&cdc, &table.prepare_descriptor(&schema, WriteMode::Cdc)));
        assert_eq!(cdc.field_descriptors.len(), 4);
        assert!(matches!(cdc.field_descriptors[0].mode, ColumnMode::Nullable));
        assert_eq!(cdc.field_descriptors[3].name, "_CHANGE_SEQUENCE_NUMBER");

        let projected = ReplicatedTableSchema::from_mask(
            Arc::new(schema.inner().clone()),
            ReplicationMask::from_bytes(vec![1, 0]),
        );
        let projected_descriptor = table.prepare_descriptor(&projected, WriteMode::Cdc);
        assert!(!Arc::ptr_eq(&cdc, &projected_descriptor));
        assert_eq!(projected_descriptor.field_descriptors.len(), 3);
        assert_eq!(projected_descriptor.field_descriptors[1].name, "_CHANGE_TYPE");

        let mut next_schema = schema.inner().clone();
        next_schema.snapshot_id = SnapshotId::new(PgLsn::from(1), PgLsn::from(1));
        next_schema.column_schemas[0].name = "renamed_id".to_owned();
        let next_schema = ReplicatedTableSchema::from_mask(
            Arc::new(next_schema),
            projected.replication_mask().clone(),
        );
        let next = table.prepare_descriptor(&next_schema, WriteMode::Cdc);
        assert!(!Arc::ptr_eq(&projected_descriptor, &next));
        assert_eq!(next.field_descriptors[0].name, "renamed_id");
        assert!(Arc::ptr_eq(&next, &table.prepare_descriptor(&next_schema, WriteMode::Cdc)));
    }

    /// Applied metadata loads without remote DDL; warm batches trust the
    /// cached endpoint until a lifecycle transition invalidates it.
    #[tokio::test]
    async fn preparation_uses_applied_cache_and_loads_metadata_only_on_a_miss() {
        let destination = unavailable_destination(false).await;
        let schema = replicated_schema(IdentityType::PrimaryKey);
        let target = table_target(&schema);
        let metadata = DestinationTableMetadata::new_applied(
            target.table_id.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), metadata.clone())
            .await
            .unwrap();

        // A cold Applied load succeeds even though every remote request fails.
        let (resolved, descriptor) =
            destination.prepare_table_for_writes(&schema, WriteMode::Copy).await.unwrap();
        assert_eq!(resolved.table_id, target.table_id);
        let pending = DestinationTableMetadata::new_creating(
            "other_table_1".to_owned(),
            metadata.snapshot_id(),
            metadata.replication_mask().clone(),
        );
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), pending.clone())
            .await
            .unwrap();
        let (reused_target, reused_descriptor) =
            destination.prepare_table_for_writes(&schema, WriteMode::Copy).await.unwrap();
        assert!(Arc::ptr_eq(&resolved, &reused_target));
        assert!(Arc::ptr_eq(&descriptor, &reused_descriptor));
        destination.handle_relation_event(&schema).await.unwrap();

        // Cache authority never permits encoding a different source endpoint.
        let mut newer = schema.inner().clone();
        newer.snapshot_id = SnapshotId::new(PgLsn::from(1), PgLsn::from(1));
        let projected = ReplicatedTableSchema::from_mask(
            Arc::new(schema.inner().clone()),
            ReplicationMask::from_bytes(vec![1, 0]),
        );
        for received in [ReplicatedTableSchema::all(Arc::new(newer)), projected] {
            assert_eq!(
                destination
                    .prepare_table_for_writes(&received, WriteMode::Copy)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::DestinationSchemaRewind
            );
        }
        let mut renamed = schema.inner().clone();
        renamed.name.name = "other".to_owned();
        assert_eq!(
            destination
                .prepare_table_for_writes(
                    &ReplicatedTableSchema::all(Arc::new(renamed)),
                    WriteMode::Copy
                )
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidState
        );

        destination.lock_table(schema.id()).await.invalidate();
        let applying = metadata
            .clone()
            .with_schema_change(
                SnapshotId::new(PgLsn::from(1), PgLsn::from(1)),
                schema.replication_mask().clone(),
            )
            .unwrap();
        for incomplete in [pending, applying] {
            destination
                .state_store
                .store_destination_table_metadata(schema.id(), incomplete.clone())
                .await
                .unwrap();
            assert_eq!(
                destination
                    .prepare_table_for_writes(&schema, WriteMode::Copy)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidState
            );
            assert_eq!(
                destination.state_store.get_destination_table_metadata(schema.id()).await.unwrap(),
                Some(incomplete)
            );
            assert!(destination.lock_table(schema.id()).await.applied.is_none());
        }
        for (snapshot, mask) in [
            (SnapshotId::new(PgLsn::from(1), PgLsn::from(1)), schema.replication_mask().clone()),
            (schema.inner().snapshot_id, ReplicationMask::from_bytes(vec![1, 0])),
        ] {
            let mismatched =
                DestinationTableMetadata::new_applied(target.table_id.clone(), snapshot, mask);
            destination
                .state_store
                .store_destination_table_metadata(schema.id(), mismatched)
                .await
                .unwrap();
            assert_eq!(
                destination
                    .prepare_table_for_writes(&schema, WriteMode::Copy)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::DestinationSchemaRewind
            );
            assert!(destination.lock_table(schema.id()).await.applied.is_none());
        }

        let replacement = table_target(&schema);
        let metadata = DestinationTableMetadata::new_applied(
            replacement.table_id.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), metadata)
            .await
            .unwrap();
        let (reloaded, _) =
            destination.prepare_table_for_writes(&schema, WriteMode::Copy).await.unwrap();
        assert_eq!(reloaded.table_id, replacement.table_id);
        assert!(!Arc::ptr_eq(&resolved, &reloaded));
    }

    /// Failed or cancelled schema DDL clears the old cache and leaves durable
    /// Applying metadata, so neither endpoint can bypass the transition.
    #[tokio::test]
    async fn interrupted_schema_change_keeps_pending_metadata_and_invalidates_cache() {
        for cancel in [false, true] {
            let destination = unavailable_destination(cancel).await;
            let before = replicated_schema(IdentityType::PrimaryKey);
            let mut after = before.inner().clone();
            after.snapshot_id = SnapshotId::new(PgLsn::from(1), PgLsn::from(1));
            after.column_schemas[1].name = "payload".to_owned();
            destination.state_store.store_table_schema(before.inner().clone()).await.unwrap();
            destination.state_store.store_table_schema(after.clone()).await.unwrap();
            let after = ReplicatedTableSchema::all(Arc::new(after));
            seed_table(&destination, &before).await;
            destination.prepare_table_for_writes(&before, WriteMode::Copy).await.unwrap();

            if cancel {
                let change = destination.handle_relation_event(&after);
                tokio::pin!(change);
                assert!(futures::poll!(change).is_pending());
            } else {
                assert_eq!(
                    destination.handle_relation_event(&after).await.unwrap_err().kind(),
                    ErrorKind::DestinationAuthenticationError
                );
            }

            let pending = destination
                .state_store
                .get_destination_table_metadata(before.id())
                .await
                .unwrap()
                .unwrap();
            assert!(pending.is_applying());
            assert_eq!(pending.snapshot_id(), after.inner().snapshot_id);
            {
                let table = destination.lock_table(before.id()).await;
                assert!(table.applied.is_none());
                assert!(table.prepared_schema.is_none());
            }
            for schema in [before, after] {
                assert_eq!(
                    destination
                        .prepare_table_for_writes(&schema, WriteMode::Copy)
                        .await
                        .unwrap_err()
                        .kind(),
                    ErrorKind::InvalidState
                );
                assert_eq!(
                    destination
                        .state_store
                        .get_destination_table_metadata(schema.id())
                        .await
                        .unwrap(),
                    Some(pending.clone())
                );
            }
        }
    }

    /// Failed or cancelled reset clears cached preparation while preserving
    /// the ownership metadata needed by the table-sync worker's next attempt.
    #[tokio::test]
    async fn interrupted_reset_invalidates_cache_and_preserves_metadata() {
        for cancel in [false, true] {
            let destination = unavailable_destination(cancel).await;
            let schema = replicated_schema(IdentityType::PrimaryKey);
            seed_table(&destination, &schema).await;
            destination.lock_table(schema.id()).await.prepare_descriptor(&schema, WriteMode::Copy);
            let before = destination
                .state_store
                .get_destination_table_metadata(schema.id())
                .await
                .unwrap()
                .unwrap();

            if cancel {
                let reset = destination.drop_table_for_copy_inner(&schema);
                tokio::pin!(reset);
                assert!(futures::poll!(reset).is_pending());
            } else {
                assert_eq!(
                    destination.drop_table_for_copy_inner(&schema).await.unwrap_err().kind(),
                    ErrorKind::DestinationAuthenticationError
                );
            }

            assert_eq!(
                destination.state_store.get_destination_table_metadata(schema.id()).await.unwrap(),
                Some(before)
            );
            let table = destination.lock_table(schema.id()).await;
            assert!(table.applied.is_none());
            assert!(table.prepared_schema.is_none());
        }
    }

    /// A failed truncate retains Creating and cannot reuse cached Applied
    /// state or implicitly restart creation on the next write.
    #[tokio::test]
    async fn failed_truncate_preserves_pending_metadata_and_invalidates_cache() {
        let destination = unavailable_destination(false).await;
        let schema = replicated_schema(IdentityType::PrimaryKey);
        seed_table(&destination, &schema).await;
        let before = destination
            .state_store
            .get_destination_table_metadata(schema.id())
            .await
            .unwrap()
            .unwrap();

        let error = destination.process_truncate_for_schemas([schema.clone()]).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DestinationAuthenticationError);
        let pending = destination
            .state_store
            .get_destination_table_metadata(schema.id())
            .await
            .unwrap()
            .unwrap();
        assert!(pending.is_creating());
        assert_ne!(pending.table_id(), before.table_id());
        assert_eq!(pending.snapshot_id(), before.snapshot_id());
        assert_eq!(pending.replication_mask(), before.replication_mask());
        {
            let table = destination.lock_table(schema.id()).await;
            assert!(table.applied.is_none());
            assert!(table.prepared_schema.is_none());
        }
        assert_eq!(
            destination
                .prepare_table_for_writes(&schema, WriteMode::Copy)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidState
        );
        assert_eq!(
            destination.state_store.get_destination_table_metadata(schema.id()).await.unwrap(),
            Some(pending)
        );
    }

    #[test]
    fn table_name_to_bigquery_view_id_uses_escaped_components() {
        for (schema, table, expected) in [
            ("schema", "table", "schema_table"),
            ("a_b", "c_d", "a__b_c__d"),
            ("a__b", "c__d", "a____b_c____d"),
        ] {
            let table_name = TableName::new(schema.to_owned(), table.to_owned());
            assert_eq!(table_name_to_bigquery_view_id(&table_name).unwrap(), expected);
        }
    }

    #[test]
    fn table_name_to_bigquery_view_id_collision_prevention() {
        // These two cases previously collided to "a_b_c"
        let table_name1 = TableName::new("a_b".to_owned(), "c".to_owned());
        let table_name2 = TableName::new("a".to_owned(), "b_c".to_owned());

        let first_view_id = table_name_to_bigquery_view_id(&table_name1).unwrap();
        let second_view_id = table_name_to_bigquery_view_id(&table_name2).unwrap();

        assert_eq!(first_view_id, "a__b_c");
        assert_eq!(second_view_id, "a_b__c");
        assert_ne!(first_view_id, second_view_id, "View IDs should not collide");
    }

    #[test]
    fn physical_bigquery_table_id_parses_and_roundtrips_valid_ids() {
        for (input, name_prefix, suffix) in [
            ("users_table_123", "users_table", 123),
            ("simple_table_0", "simple_table", 0),
            ("test_table_18446744073709551615", "test_table", u64::MAX),
            ("a__b_c__d_42", "a__b_c__d", 42),
            ("_0102_a__b_0", "_0102_a__b", 0),
            ("_0102_a__b_18446744073709551615", "_0102_a__b", u64::MAX),
            ("_0201_a__b_0", "_0201_a__b", 0),
            ("_0101_____42", "_0101____", 42),
            ("_0104_a__b_0_7", "_0104_a__b_0", 7),
            ("_0102_a_b__7", "_0102_a_b_", 7),
            ("_0607_Public__Orders_42", "_0607_Public__Orders", 42),
        ] {
            let parsed = input.parse::<PhysicalBigQueryTableId>().unwrap();
            assert_eq!(parsed.0, name_prefix);
            assert_eq!(parsed.1, TableIdSuffix::Legacy(suffix));
            assert_eq!(parsed.to_string(), input);
        }
    }

    /// Covers every UUID bit and the fixed-width distinction from legacy IDs.
    #[test]
    fn table_id_suffix_encoding_is_compact_and_lossless() {
        for value in [0, 1, u128::from(u64::MAX), u128::MAX] {
            let table_id = PhysicalBigQueryTableId(
                "users".to_owned(),
                TableIdSuffix::Random(Uuid::from_u128(value)),
            );
            let encoded = table_id.to_string();
            assert_eq!(encoded.len(), "users_".len() + 25);
            assert!(
                encoded
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            );
            assert_eq!(encoded.parse::<PhysicalBigQueryTableId>().unwrap(), table_id);
        }
    }

    #[test]
    fn metadata_table_id_uses_metadata_target() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let metadata = DestinationTableMetadata::new_applied(
            "users_table_3".to_owned(),
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );

        let table_id = parse_table_id_from_metadata(&metadata).unwrap();

        assert_eq!(
            table_id,
            PhysicalBigQueryTableId("users_table".to_owned(), TableIdSuffix::Legacy(3))
        );
    }

    #[test]
    fn bigquery_streaming_requires_applied_metadata() {
        let schema = replicated_schema(IdentityType::PrimaryKey);
        let creating_metadata = DestinationTableMetadata::new_creating(
            "users_table_0".to_owned(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );

        let error = ensure_bigquery_metadata_applied(&creating_metadata).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidState);

        let applied_metadata = creating_metadata.to_applied();
        ensure_bigquery_metadata_applied(&applied_metadata).unwrap();

        let applying_metadata = applied_metadata
            .with_schema_change(
                SnapshotId::new(PgLsn::from(1), PgLsn::from(1)),
                schema.replication_mask().clone(),
            )
            .unwrap();
        let error = ensure_bigquery_metadata_applied(&applying_metadata).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidState);
    }

    #[test]
    fn metadata_table_id_rejects_malformed_target() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let metadata = DestinationTableMetadata::new_applied(
            "invalid-target".to_owned(),
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );

        let err = parse_table_id_from_metadata(&metadata).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DestinationTableNameInvalid);
    }

    #[test]
    fn validate_bigquery_table_shape_accepts_alternative_identity() {
        let replicated_table_schema = replicated_schema(IdentityType::AlternativeKey);

        validate_bigquery_table_shape(&replicated_table_schema).unwrap();
    }

    #[test]
    fn validate_bigquery_table_shape_rejects_case_colliding_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
                ColumnSchema::new("Name".to_owned(), Type::TEXT, -1, 3, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::all(table_schema);

        let error = validate_bigquery_table_shape(&replicated_table_schema).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
        assert_eq!(error.description(), Some("Source column names collide in the destination"));
        assert!(error.detail().is_some_and(|detail| detail.contains("'name' and 'Name'")));
    }

    #[test]
    fn validate_bigquery_table_shape_rejects_reserved_column_prefixes() {
        for column_name in [
            "_TABLE_source",
            "_file_source",
            "_PARTITION_source",
            "_row_timestamp_source",
            "__ROOT__source",
            "_colidentifier_source",
            "_CHANGE_SEQUENCE_NUMBER_source",
            "_change_type_source",
            "_CHANGE_TIMESTAMP_source",
        ] {
            let replicated_table_schema = replicated_schema_with_column_name(column_name);

            let error = validate_bigquery_table_shape(&replicated_table_schema).unwrap_err();

            assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
            assert_eq!(
                error.description(),
                Some("BigQuery source column name uses a reserved prefix")
            );
        }
    }

    #[test]
    fn validate_bigquery_table_shape_accepts_flexible_column_names() {
        for column_name in ["2fa", "user-name", "naïve"] {
            let replicated_table_schema = replicated_schema_with_column_name(column_name);

            validate_bigquery_table_shape(&replicated_table_schema).unwrap();
        }
    }

    #[test]
    fn bigquery_update_new_row_rejects_partial_rows() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let partial_row =
            etl::data::PartialTableRow::new(2, TableRow::new(vec![Cell::I32(1)]), vec![1]);

        let error = bigquery_update_new_row(
            &replicated_table_schema,
            UpdatedTableRow::Partial(partial_row),
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert!(error.to_string().contains("emitted a partial update row"));
    }

    #[test]
    fn bigquery_delete_old_row_rejects_missing_old_rows() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);

        let error = bigquery_delete_old_row(&replicated_table_schema, None).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn ensure_bigquery_key_image_matches_primary_key_rejects_alternative_key() {
        let replicated_table_schema = replicated_schema(IdentityType::AlternativeKey);

        let error =
            ensure_bigquery_key_image_matches_primary_key(&replicated_table_schema).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn ensure_bigquery_update_without_old_row_rejects_alternative_key() {
        let replicated_table_schema = replicated_schema(IdentityType::AlternativeKey);

        let error =
            ensure_bigquery_update_without_old_row_can_skip_delete(&replicated_table_schema)
                .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn validate_bigquery_table_shape_rejects_partial_primary_key() {
        let replicated_table_schema = replicated_schema_with_partial_primary_key();

        let error = validate_bigquery_table_shape(&replicated_table_schema).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
        assert!(error.to_string().contains("tenant_id"));
    }

    #[test]
    fn validate_bigquery_table_shape_accepts_nullable_arrays() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("tags".to_owned(), Type::TEXT_ARRAY, -1, 2, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::all(table_schema);

        validate_bigquery_table_shape(&replicated_table_schema).unwrap();
    }

    #[test]
    fn validate_bigquery_schema_plan_rejects_publication_added_arrays() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("tags".to_owned(), Type::TEXT_ARRAY, -1, 2, false),
            ],
        ));
        let current_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            etl::schema::ReplicationMask::from_bytes(vec![1, 0]),
            IdentityMask::from_bytes(vec![1, 0]),
        );
        let new_schema = ReplicatedTableSchema::from_masks(
            table_schema,
            etl::schema::ReplicationMask::from_bytes(vec![1, 1]),
            IdentityMask::from_bytes(vec![1, 0]),
        );
        let plan =
            current_schema.plan_schema_change(&new_schema, BIGQUERY_COLUMN_NAME_MAPPING).unwrap();

        let error = validate_bigquery_schema_plan(&new_schema, &plan).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
        assert_eq!(
            error.description(),
            Some("BigQuery cannot add a source array through publication filtering")
        );
        assert!(error.to_string().contains("unknown historical values"));
    }

    #[test]
    fn validate_bigquery_schema_plan_accepts_publication_added_scalars() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, false),
            ],
        ));
        let current_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            etl::schema::ReplicationMask::from_bytes(vec![1, 0]),
            IdentityMask::from_bytes(vec![1, 0]),
        );
        let new_schema = ReplicatedTableSchema::from_masks(
            table_schema,
            etl::schema::ReplicationMask::from_bytes(vec![1, 1]),
            IdentityMask::from_bytes(vec![1, 0]),
        );
        let plan =
            current_schema.plan_schema_change(&new_schema, BIGQUERY_COLUMN_NAME_MAPPING).unwrap();

        validate_bigquery_schema_plan(&new_schema, &plan).unwrap();
    }

    #[test]
    fn ensure_bigquery_primary_key_unchanged_accepts_non_key_rename() {
        let current_schema = replicated_schema(IdentityType::PrimaryKey);
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("display_name".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let new_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            etl::schema::ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 0]),
        );

        ensure_bigquery_primary_key_unchanged(&current_schema, &new_schema).unwrap();
    }

    #[test]
    fn ensure_bigquery_primary_key_unchanged_accepts_equivalent_key_rename() {
        let current_schema = replicated_schema(IdentityType::PrimaryKey);
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("ID".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let new_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            etl::schema::ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 0]),
        );

        ensure_bigquery_primary_key_unchanged(&current_schema, &new_schema).unwrap();
    }

    #[test]
    fn ensure_bigquery_primary_key_unchanged_rejects_key_rename() {
        let current_schema = replicated_schema(IdentityType::PrimaryKey);
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("user_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let new_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            etl::schema::ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 0]),
        );

        let error =
            ensure_bigquery_primary_key_unchanged(&current_schema, &new_schema).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
        assert!(
            error.to_string().contains("primary-key columns, destination identifiers, or order")
        );
    }

    #[test]
    fn ensure_bigquery_primary_key_unchanged_rejects_membership_and_order_changes() {
        for (current_schema, target_schema) in [
            (
                replicated_schema_with_primary_key_positions(Some(1), None),
                replicated_schema_with_primary_key_positions(None, None),
            ),
            (
                replicated_schema_with_primary_key_positions(Some(1), None),
                replicated_schema_with_primary_key_positions(Some(1), Some(2)),
            ),
            (
                replicated_schema_with_primary_key_positions(Some(1), Some(2)),
                replicated_schema_with_primary_key_positions(Some(2), Some(1)),
            ),
        ] {
            let error =
                ensure_bigquery_primary_key_unchanged(&current_schema, &target_schema).unwrap_err();

            assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
        }
    }

    /// Malformed suffixes fail without slicing Unicode or accepting overflow.
    #[test]
    fn physical_bigquery_table_id_rejects_invalid_ids() {
        for input in [
            "",
            "tablewithoutversion",
            "users_table_00",
            "users_table_01",
            "users_table_not_a_number",
            "table_word",
            "users_table_-123",
            "users_table_18446744073709551616",
            "users_table_",
            "_123",
            "users_é",
            "users_１２３",
            "users_+1",
            "users_ 1",
            "users_1\n",
            "users_000000000000000000000",
            "users_000000000000000000000000",
            "users_000000000000000000000000 ",
            "_0000000000000000000000000",
            "users_zzzzzzzzzzzzzzzzzzzzzzzzz",
            "users_000000000000000000000000Z",
            "users_00000000000000000000000é",
            "users_00000000000000000000000000",
        ] {
            let error = input.parse::<PhysicalBigQueryTableId>().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DestinationTableNameInvalid);
        }
    }

    #[test]
    fn split_table_rows_distributes_rows_across_target_batches() {
        for (row_count, target_batches, expected_batch_sizes) in [
            (0, 4, vec![]),
            (1, 0, vec![1]),
            (2, 1, vec![2]),
            (2, 5, vec![2]),
            (4, 2, vec![2, 2]),
            (5, 3, vec![2, 2, 1]),
            (10, 4, vec![3, 3, 2, 2]),
        ] {
            let rows = (0..row_count)
                .map(|i| BigQueryTableRow::try_from(TableRow::new(vec![Cell::I32(i)])).unwrap())
                .collect::<Vec<_>>();
            let encoded_rows: Vec<_> = rows.iter().map(Message::encode_to_vec).collect();
            let batches = split_table_rows(rows, target_batches);
            assert_eq!(
                batches.iter().flatten().map(Message::encode_to_vec).collect::<Vec<_>>(),
                encoded_rows
            );
            let batch_sizes: Vec<_> = batches.iter().map(Vec::len).collect();

            assert_eq!(batch_sizes, expected_batch_sizes);
        }
    }

    #[test]
    fn calculate_target_batches_handles_empty_small_and_oversized_rows() {
        assert_eq!(calculate_target_batches_for_table_copy(&[]).unwrap(), 0);

        let small_row =
            BigQueryTableRow::try_from(TableRow::new(vec![Cell::String("test".to_owned())]))
                .unwrap();
        assert_eq!(calculate_target_batches_for_table_copy(&[small_row]).unwrap(), 1);

        let oversized_row = BigQueryTableRow::try_from(TableRow::new(vec![Cell::String(
            "x".repeat(MAX_BATCH_SIZE_BYTES + 1),
        )]))
        .unwrap();
        assert_eq!(calculate_target_batches_for_table_copy(&[oversized_row]).unwrap(), 1);
    }

    #[test]
    fn calculate_target_batches_estimates_multiple_batches_from_the_first_row() {
        let estimated_large_row = BigQueryTableRow::try_from(TableRow::new(vec![Cell::String(
            "x".repeat(MAX_BATCH_SIZE_BYTES / 2 + 1),
        )]))
        .unwrap();
        let rows = [
            estimated_large_row,
            BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap(),
            BigQueryTableRow::try_from(TableRow::new(vec![])).unwrap(),
        ];

        assert_eq!(calculate_target_batches_for_table_copy(&rows).unwrap(), 3);
    }

    #[test]
    fn bigquery_delete_key_row_encodes_identity_and_cdc_tags_against_descriptor_numbers() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            table_schema,
            etl::schema::ReplicationMask::from_bytes(vec![1, 1, 1]),
            IdentityMask::from_bytes(vec![1, 0, 0]),
        );

        let row = bigquery_delete_row(
            &replicated_table_schema,
            OldTableRow::Key(TableRow::new(vec![Cell::I32(42)])),
            "lsn:1".to_owned(),
        )
        .unwrap();

        let encoded = row.encode_to_vec();

        // Field tags must line up with the descriptor numbering: 1 => id, 4 =>
        // _CHANGE_TYPE, 5 => _CHANGE_SEQUENCE_NUMBER.
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
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            table_schema,
            etl::schema::ReplicationMask::from_bytes(vec![1, 1, 1]),
            IdentityMask::from_bytes(vec![1, 1, 1]),
        );

        let row = bigquery_delete_row(
            &replicated_table_schema,
            OldTableRow::Full(TableRow::new(vec![
                Cell::I32(42),
                Cell::String("alice".to_owned()),
                Cell::I32(7),
            ])),
            "lsn:1".to_owned(),
        )
        .unwrap();

        assert_eq!(
            row.encode_to_vec(),
            expected_row_bytes(vec![
                (1, Cell::I32(42)),
                (4, Cell::String("DELETE".to_owned())),
                (5, Cell::String("lsn:1".to_owned())),
            ])
        );
    }

    #[test]
    fn bigquery_sequence_number_uses_three_64_bit_hex_sections() {
        let event_position = EventPosition::new(PgLsn::from(u64::MAX), u64::MAX);

        assert_eq!(
            bigquery_sequence_number(event_position, u64::MAX),
            "ffffffffffffffff/ffffffffffffffff/ffffffffffffffff"
        );
    }

    #[test]
    fn bigquery_update_rows_emits_delete_before_upsert_when_primary_key_changes() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let event_position = EventPosition::new(PgLsn::from(1), 0);

        let rows = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_owned())]),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
            event_position,
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].encode_to_vec(),
            expected_row_bytes(vec![
                (1, Cell::I32(1)),
                (3, Cell::String("DELETE".to_owned())),
                (4, Cell::String("0000000000000001/0000000000000000/0000000000000000".to_owned(),),),
            ])
        );
        assert_eq!(
            rows[1].encode_to_vec(),
            expected_row_bytes(vec![
                (1, Cell::I32(2)),
                (2, Cell::String("updated".to_owned())),
                (3, Cell::String("UPSERT".to_owned())),
                (4, Cell::String("0000000000000001/0000000000000000/0000000000000001".to_owned(),),),
            ])
        );
    }

    #[test]
    fn bigquery_update_rows_skips_delete_when_primary_key_is_unchanged() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let event_position = EventPosition::new(PgLsn::from(1), 0);

        let rows = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(1), Cell::String("updated".to_owned())]),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
            event_position,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].encode_to_vec(),
            expected_row_bytes(vec![
                (1, Cell::I32(1)),
                (2, Cell::String("updated".to_owned())),
                (3, Cell::String("UPSERT".to_owned())),
                (4, Cell::String("0000000000000001/0000000000000000/0000000000000000".to_owned(),),),
            ])
        );
    }

    #[test]
    fn bigquery_update_rows_emits_delete_before_upsert_for_full_identity_primary_key_change() {
        let replicated_table_schema = replicated_schema(IdentityType::Full);
        let event_position = EventPosition::new(PgLsn::from(1), 0);

        let rows = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_owned())]),
            Some(OldTableRow::Full(TableRow::new(vec![
                Cell::I32(1),
                Cell::String("before".to_owned()),
            ]))),
            event_position,
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].encode_to_vec(),
            expected_row_bytes(vec![
                (1, Cell::I32(1)),
                (3, Cell::String("DELETE".to_owned())),
                (4, Cell::String("0000000000000001/0000000000000000/0000000000000000".to_owned(),),),
            ])
        );
        assert_eq!(
            rows[1].encode_to_vec(),
            expected_row_bytes(vec![
                (1, Cell::I32(2)),
                (2, Cell::String("updated".to_owned())),
                (3, Cell::String("UPSERT".to_owned())),
                (4, Cell::String("0000000000000001/0000000000000000/0000000000000001".to_owned(),),),
            ])
        );
    }

    #[test]
    fn bigquery_update_rows_accepts_alternative_identity_when_full_old_row_is_available() {
        let replicated_table_schema = replicated_schema(IdentityType::AlternativeKey);
        let event_position = EventPosition::new(PgLsn::from(1), 0);

        let rows = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_owned())]),
            Some(OldTableRow::Full(TableRow::new(vec![
                Cell::I32(1),
                Cell::String("before".to_owned()),
            ]))),
            event_position,
        )
        .unwrap();

        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn bigquery_update_rows_rejects_alternative_identity_without_old_row() {
        let replicated_table_schema = replicated_schema(IdentityType::AlternativeKey);
        let event_position = EventPosition::new(PgLsn::from(1), 0);

        let error = bigquery_update_rows(
            &replicated_table_schema,
            TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_owned())]),
            None,
            event_position,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }
}
