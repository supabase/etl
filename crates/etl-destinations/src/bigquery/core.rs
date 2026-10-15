use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::Arc,
};

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
        ColumnAlterationKind, ColumnPresenceChangeReason, IdentityType, ReplicatedTableSchema,
        ReplicationMask, SchemaOperation, SchemaPlan, SnapshotId, TableId, TableName,
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
        schema::{column_default_sql, column_schemas_to_table_descriptor},
    },
    recovery::{
        ensure_destination_schema_matches_metadata, ensure_relation_schema_transition,
        warn_unsupported_column_type_change,
    },
    table_name::try_stringify_table_name,
};

/// Width of a full 128-bit UUID encoded in base 36, distinct from legacy u64
/// IDs.
const TABLE_VERSION_ID_LENGTH: usize = 25;

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

/// Returns the [`BigQueryTableId`] for a supplied [`TableName`].
///
/// Uses the shared destination naming logic, preserving existing identifiers.
pub fn table_name_to_bigquery_table_id(table_name: &TableName) -> EtlResult<BigQueryTableId> {
    try_stringify_table_name(table_name)
}

/// A physical table version, including identifiers from older releases.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum TableVersionId {
    /// A legacy numeric suffix, retained only for reading existing metadata.
    Legacy(u64),
    /// A random identity independent of clocks, resets, and metadata history.
    Random(Uuid),
}

impl Display for TableVersionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(version) => write!(f, "{version}"),
            Self::Random(uuid) => {
                // Base 36 retains every UUID bit in 25 lowercase characters.
                // Fixed width distinguishes it from every legacy u64 suffix;
                // lowercase also works in case-insensitive BigQuery datasets.
                let mut value = uuid.as_u128();
                let mut digits = ['0'; TABLE_VERSION_ID_LENGTH];
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
/// Each creation attempt receives a fresh UUID. A deleted physical name must
/// never be deliberately reused: Storage Write API appends can otherwise be
/// acknowledged without reaching the replacement table.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct VersionedBigQueryTableId(BigQueryTableId, TableVersionId);

impl VersionedBigQueryTableId {
    /// Allocates a fresh physical identity without consulting wall-clock time.
    fn new(table_id: BigQueryTableId) -> Self {
        Self(table_id, TableVersionId::Random(Uuid::new_v4()))
    }

    /// Borrows the logical BigQuery table ID without the version suffix.
    fn base_table_id(&self) -> &BigQueryTableId {
        &self.0
    }

    /// Returns whether this physical table belongs to `base_table_id`.
    fn belongs_to_base(&self, base_table_id: &BigQueryTableId) -> bool {
        &self.0 == base_table_id
    }

    /// Parses a physical table ID only when it belongs to `base_table_id`.
    fn parse_for_base(table_id: &str, base_table_id: &BigQueryTableId) -> Option<Self> {
        table_id.parse::<Self>().ok().filter(|table_id| table_id.belongs_to_base(base_table_id))
    }
}

impl FromStr for VersionedBigQueryTableId {
    type Err = EtlError;

    /// Parses legacy numeric suffixes and fixed-width lowercase base-36 UUIDs.
    fn from_str(table_id: &str) -> Result<Self, Self::Err> {
        let parsed = table_id.rsplit_once('_').and_then(|(base, suffix)| {
            if base.is_empty() || suffix.is_empty() {
                return None;
            }

            let version = if suffix.len() == TABLE_VERSION_ID_LENGTH {
                if !suffix.bytes().all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase()) {
                    return None;
                }
                TableVersionId::Random(Uuid::from_u128(u128::from_str_radix(suffix, 36).ok()?))
            } else {
                if suffix.len() > 20
                    || (suffix.len() > 1 && suffix.starts_with('0'))
                    || !suffix.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return None;
                }
                TableVersionId::Legacy(suffix.parse().ok()?)
            };

            Some(Self(base.to_owned(), version))
        });

        parsed.ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid versioned BigQuery table ID",
                format!(
                    "Table ID '{table_id}' must end in a u64 version or a 25-character lowercase \
                     base-36 version"
                )
            )
        })
    }
}

impl Display for VersionedBigQueryTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.0, self.1)
    }
}

/// Returns the versioned table id stored in destination metadata.
fn metadata_versioned_table_id_for_base(
    metadata: &DestinationTableMetadata,
    base_bigquery_table_id: &BigQueryTableId,
) -> EtlResult<VersionedBigQueryTableId> {
    let versioned_table_id = metadata.table_id().parse::<VersionedBigQueryTableId>()?;

    if !versioned_table_id.belongs_to_base(base_bigquery_table_id) {
        bail!(
            ErrorKind::InvalidState,
            "Destination table metadata points to a different BigQuery table",
            format!(
                "Destination table metadata points to '{}', but BigQuery table resolution \
                 expected a versioned table for '{}'",
                metadata.table_id(),
                base_bigquery_table_id
            )
        );
    }

    Ok(versioned_table_id)
}

/// Attempts physical-table deletion without making replication depend on
/// cleanup.
///
/// Call only for obsolete versions after switching the view and persisting
/// metadata, or after dropping the logical view during an explicit reset.
/// Failed deletions leave orphans for a later cleanup attempt or the operator.
async fn delete_table_versions(
    client: &BigQueryClient,
    dataset_id: &BigQueryDatasetId,
    table_ids: HashSet<VersionedBigQueryTableId>,
) {
    for table_id in table_ids {
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
}

/// Ensures BigQuery metadata is ready for streaming operations.
fn ensure_bigquery_metadata_applied(metadata: &DestinationTableMetadata) -> EtlResult<()> {
    if metadata.is_applied() {
        return Ok(());
    }

    Err(etl_error!(
        ErrorKind::InvalidState,
        "BigQuery destination table schema is not applied",
        format!(
            "Table '{}' has schema state '{:?}'; resynchronize the table to recover",
            metadata.table_id(),
            metadata.table_schema()
        )
    ))
}

/// An immutable physical target with its rendered Storage Write table ID.
///
/// Retaining the parsed and rendered forms avoids reparsing and formatting
/// the UUID suffix on every batch. Metadata still validates each use.
#[derive(Debug, PartialEq, Eq)]
struct TableTarget {
    /// Parsed identity used to validate the logical table name.
    versioned_id: VersionedBigQueryTableId,
    /// Rendered identity used by BigQuery requests and destination metadata.
    physical_id: BigQueryTableId,
    /// Source name whose encoding was validated against the physical identity.
    source_name: TableName,
}

impl TableTarget {
    /// Renders a physical identity once for all batches targeting it.
    fn new(versioned_id: VersionedBigQueryTableId, source_name: TableName) -> Self {
        Self { physical_id: versioned_id.to_string(), versioned_id, source_name }
    }
}

impl Display for TableTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.physical_id)
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
    /// CDC delete rows may omit non-key scalar columns.
    allow_missing_columns: bool,
    /// Shared descriptor, cloned into SDK requests only after unlocking.
    descriptor: Arc<TableDescriptor>,
}

impl PreparedTableSchema {
    /// Checks the snapshot, replication mask, and copy/CDC mode before reuse.
    fn matches(&self, schema: &ReplicatedTableSchema, allow_missing_columns: bool) -> bool {
        self.snapshot_id == schema.inner().snapshot_id
            && self.replication_mask == *schema.replication_mask()
            && self.allow_missing_columns == allow_missing_columns
    }
}

/// Derived view state protected by one table's preparation and lifecycle lock.
///
/// Durable destination metadata remains authoritative and is checked on every
/// write, even when the view is cached. ETL exclusively owns these destination
/// objects; external table or view changes have undefined behavior.
#[derive(Debug, Default)]
struct TableState {
    /// Confirmed view target; absent while view DDL may be incomplete.
    view_target: Option<Arc<TableTarget>>,
    /// Last compiled row schema; cleared before view DDL, including resets.
    prepared_schema: Option<PreparedTableSchema>,
}

impl TableState {
    /// Clears the cached view target and descriptor before view DDL.
    fn invalidate(&mut self) {
        *self = Self::default();
    }

    /// Compiles once per endpoint and mode; warm batches only clone an Arc.
    fn prepare_descriptor(
        &mut self,
        schema: &ReplicatedTableSchema,
        allow_missing_columns: bool,
    ) -> Arc<TableDescriptor> {
        if let Some(prepared) = &self.prepared_schema
            && prepared.matches(schema, allow_missing_columns)
        {
            return Arc::clone(&prepared.descriptor);
        }

        let descriptor =
            Arc::new(column_schemas_to_table_descriptor(schema, allow_missing_columns));
        self.prepared_schema = Some(PreparedTableSchema {
            snapshot_id: schema.inner().snapshot_id,
            replication_mask: schema.replication_mask().clone(),
            allow_missing_columns,
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
    async fn lock_table(&self, table_id: TableId) -> OwnedMutexGuard<TableState> {
        let table = {
            let mut tables = self.tables.lock().await;
            Arc::clone(tables.entry(table_id).or_default())
        };

        table.lock_owned().await
    }

    /// Discovers cleanup candidates without making listing a replication
    /// prerequisite.
    ///
    /// The caller adds the durable metadata target even when listing fails or
    /// lags. Collect candidates under serialization before creating a
    /// replacement.
    async fn list_table_versions_for_cleanup(
        &self,
        base_table_id: &BigQueryTableId,
    ) -> HashSet<VersionedBigQueryTableId> {
        match self.client.list_physical_table_ids(&self.dataset_id, base_table_id).await {
            Ok(physical_table_ids) => physical_table_ids
                .into_iter()
                .filter_map(|physical_table_id| {
                    VersionedBigQueryTableId::parse_for_base(&physical_table_id, base_table_id)
                })
                .collect(),
            Err(error) => {
                warn!(
                    dataset_id = %self.dataset_id,
                    %base_table_id,
                    error = %error,
                    "failed to list bigquery table versions; unlisted tables may require manual cleanup"
                );
                HashSet::new()
            }
        }
    }

    /// Schedules optimistic deletion only after the replacement is
    /// authoritative.
    ///
    /// Failures are logged and leave unused tables behind. A later truncate or
    /// explicit reset can retry cleanup; there is no persistent cleanup queue.
    async fn schedule_table_cleanup(&self, obsolete_table_ids: HashSet<VersionedBigQueryTableId>) {
        let client = self.client.clone();
        let dataset_id = self.dataset_id.clone();
        self.tasks
            .spawn_with(move || async move {
                delete_table_versions(&client, &dataset_id, obsolete_table_ids).await;
            })
            .await;
    }

    /// Resolves or creates the versioned BigQuery table for a schema endpoint.
    ///
    /// Applied metadata is authoritative: it selects the current physical
    /// version and proves that ETL completed table preparation for the
    /// recorded snapshot and replication mask. The physical table is not probed
    /// or recreated in that state because replacing a missing applied table
    /// with an empty table would silently discard replicated data.
    ///
    /// Initial creation is bracketed by `Creating` and `Applied`. Interrupted
    /// creation abandons its old identity and creates a fresh version;
    /// `Applying` remains blocked because BigQuery schema DDL can be partially
    /// applied.
    async fn resolve_or_create_table_for_schema(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        cached_target: Option<&Arc<TableTarget>>,
    ) -> EtlResult<Arc<TableTarget>> {
        let table_id = replicated_table_schema.id();
        let existing_metadata = self.state_store.get_destination_table_metadata(table_id).await?;

        // Applied metadata supplies the physical identity without probing
        // BigQuery.
        if let Some(metadata) = existing_metadata {
            let table_target = match cached_target {
                Some(target)
                    if target.physical_id == metadata.table_id()
                        && &target.source_name == replicated_table_schema.name() =>
                {
                    Arc::clone(target)
                }
                _ => Arc::new(TableTarget::new(
                    metadata_versioned_table_id_for_base(
                        &metadata,
                        &table_name_to_bigquery_table_id(replicated_table_schema.name())?,
                    )?,
                    replicated_table_schema.name().clone(),
                )),
            };

            if metadata.is_applying() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "BigQuery table schema is still being applied",
                    format!(
                        "Table {table_id} has an interrupted schema change and cannot accept \
                         writes until it is recovered or resynchronized."
                    )
                ));
            }

            ensure_destination_schema_matches_metadata(
                "BigQuery",
                table_id,
                &metadata,
                replicated_table_schema,
            )?;

            if metadata.is_applied() {
                return Ok(table_target);
            }
        }

        // We validate that the table is compatible with BigQuery.
        validate_bigquery_table_shape(replicated_table_schema)?;

        // A Creating target is abandoned without deletion here. Truncate or
        // explicit reset can collect it after redirecting or removing the view.
        let bigquery_table_id = table_name_to_bigquery_table_id(replicated_table_schema.name())?;
        let table_target = Arc::new(TableTarget::new(
            VersionedBigQueryTableId::new(bigquery_table_id),
            replicated_table_schema.name().clone(),
        ));
        let metadata = DestinationTableMetadata::new_creating(
            table_target.physical_id.clone(),
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );

        self.state_store.store_destination_table_metadata(table_id, metadata.clone()).await?;

        self.client
            .create_table(
                &self.dataset_id,
                &table_target.physical_id,
                replicated_table_schema,
                self.max_staleness_mins,
                self.table_options.get(&table_id),
            )
            .await?;

        self.state_store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;

        debug!(%table_target, "created physical bigquery table");

        Ok(table_target)
    }

    /// Prepares a table for BigQuery writes with schema-aware table creation.
    async fn prepare_table_for_writes(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        allow_missing_columns: bool,
    ) -> EtlResult<(Arc<TableTarget>, Arc<TableDescriptor>)> {
        // Copy partitions share preparation for this table, while other tables
        // can prepare and write independently of its DDL.
        let mut table = self.lock_table(replicated_table_schema.id()).await;
        let table_target = self
            .resolve_or_create_table_for_schema(replicated_table_schema, table.view_target.as_ref())
            .await?;
        self.ensure_view_points_to_table(&mut table, &table_target).await?;

        let table_descriptor =
            table.prepare_descriptor(replicated_table_schema, allow_missing_columns);

        Ok((table_target, table_descriptor))
    }

    /// Loads destination metadata and requires an applied schema state.
    async fn get_applied_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let metadata = self.state_store.get_destination_table_metadata(table_id).await?;
        if let Some(metadata) = &metadata {
            ensure_bigquery_metadata_applied(metadata)?;
        }

        Ok(metadata)
    }

    /// Installs the target view unless this table's cache already certifies it.
    async fn ensure_view_points_to_table(
        &self,
        table: &mut TableState,
        table_target: &Arc<TableTarget>,
    ) -> EtlResult<()> {
        if table.view_target.as_ref() == Some(table_target) {
            return Ok(());
        }

        self.recreate_view_for_table(table, table_target).await
    }

    /// Recreates a view even when its target is unchanged, refreshing its
    /// schema.
    async fn recreate_view_for_table(
        &self,
        table: &mut TableState,
        table_target: &Arc<TableTarget>,
    ) -> EtlResult<()> {
        // Invalidate before DDL so failed or cancelled requests cannot leave
        // the previous view target certified in the cache.
        table.invalidate();

        let bigquery_table_id = table_target.versioned_id.base_table_id();
        self.client
            .create_or_replace_view(&self.dataset_id, bigquery_table_id, &table_target.physical_id)
            .await?;

        table.view_target = Some(Arc::clone(table_target));

        debug!(
            %bigquery_table_id,
            %table_target,
            "view recreated for target table"
        );

        Ok(())
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
            self.prepare_table_for_writes(replicated_table_schema, false).await?;

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
        let physical_table_id = &table_target.physical_id;

        // Create append requests from the split rows.
        let mut append_requests = Vec::with_capacity(table_rows_batches.len());
        for (request_index, table_rows) in table_rows_batches.into_iter().enumerate() {
            if !table_rows.is_empty() {
                histogram!(ETL_BQ_APPEND_BATCHES_BATCH_SIZE).record(table_rows.len() as f64);
                let append_request = self.client.create_append_request(
                    self.pipeline_id,
                    request_index,
                    &self.dataset_id,
                    physical_table_id,
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
    async fn handle_relation_event(
        &self,
        new_replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        validate_bigquery_table_capabilities(new_replicated_table_schema)?;

        let table_id = new_replicated_table_schema.id();
        let mut table = self.lock_table(table_id).await;
        let new_snapshot_id = new_replicated_table_schema.inner().snapshot_id;

        let Some(metadata) = self.get_applied_destination_table_metadata(table_id).await? else {
            // No metadata exists, this is a broken invariant since the metadata
            // should have been recorded during write_table_rows before any
            // Relation event.
            bail!(
                ErrorKind::CorruptedTableSchema,
                "Destination metadata missing for BigQuery schema change",
                format!(
                    "Table {} received schema snapshot {}, but destination metadata from initial \
                     synchronization was not found.",
                    table_id, new_snapshot_id
                )
            );
        };
        let current_snapshot_id = metadata.snapshot_id();
        let current_replication_mask = metadata.replication_mask().clone();
        let new_replication_mask = new_replicated_table_schema.replication_mask().clone();

        // BigQuery CDC sequence numbers order row mutations, but they do not
        // make destructive or ambiguously ordered DDL safe. A relation has no
        // DML sequence number of its own, so reject older snapshots and
        // equal-snapshot mask conflicts before computing a diff.
        ensure_relation_schema_transition(
            "BigQuery",
            table_id,
            current_snapshot_id,
            &current_replication_mask,
            new_snapshot_id,
            &new_replication_mask,
        )?;

        // The guard above proves that equal snapshots have equal masks.
        if current_snapshot_id == new_snapshot_id {
            // Schema hasn't changed, nothing to do.
            debug!(
                table_id = %table_id,
                snapshot_id = %new_snapshot_id,
                replication_mask = %new_replication_mask,
                "bigquery table schema unchanged"
            );

            return Ok(());
        }

        info!(
            table_id = %table_id,
            current_snapshot_id = %current_snapshot_id,
            new_snapshot_id = %new_snapshot_id,
            current_replication_mask = %current_replication_mask,
            new_replication_mask = %new_replication_mask,
            "bigquery table schema change detected"
        );

        // Get the current schema from the schema store to compute the diff.
        let current_table_schema =
            self.state_store.get_table_schema(&table_id, current_snapshot_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "Stored schema snapshot missing for BigQuery schema change",
                        format!(
                            "Table {} needs stored schema snapshot {} to compare with incoming \
                             snapshot {}, but it was not found.",
                            table_id, current_snapshot_id, new_snapshot_id
                        )
                    )
                },
            )?;

        // Build a ReplicatedTableSchema using the stored replication mask.
        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );
        ensure_bigquery_primary_key_unchanged(&current_schema, new_replicated_table_schema)?;

        let plan = current_schema
            .plan_schema_change(new_replicated_table_schema, BIGQUERY_COLUMN_NAME_MAPPING)?;
        validate_bigquery_schema_plan(new_replicated_table_schema, &plan)?;
        // The view cache certifies the source name as well as the physical
        // target. Validate that association before changing durable state.
        let bigquery_table_id =
            table_name_to_bigquery_table_id(new_replicated_table_schema.name())?;
        let versioned_table_id =
            metadata_versioned_table_id_for_base(&metadata, &bigquery_table_id)?;

        // Mark as applying before making changes (with the NEW snapshot_id and
        // mask).
        //
        // NOTE: BigQuery does not support transactional DDL, so if the system
        // crashes while in 'Applying' state, the destination table may be in an
        // inconsistent state and manual intervention may be required. The
        // previous endpoint is stored for debugging purposes but automatic
        // recovery is not possible.
        let updated_metadata = DestinationTableMetadata::new_applied(
            metadata.table_id().to_owned(),
            current_snapshot_id,
            current_replication_mask.clone(),
        )
        .with_schema_change(new_snapshot_id, new_replication_mask.clone())?;
        self.state_store
            .store_destination_table_metadata(table_id, updated_metadata.clone())
            .await?;

        if let Err(err) = self.apply_schema_plan(&table_id, &versioned_table_id, &plan).await {
            warn!(
                table_id = %table_id,
                error = %err,
                "bigquery table schema change failed; manual intervention may be required"
            );
            return Err(err);
        }

        let table_target = Arc::new(TableTarget::new(
            versioned_table_id,
            new_replicated_table_schema.name().clone(),
        ));
        self.recreate_view_for_table(&mut table, &table_target).await?;

        // Mark as applied after successful changes.
        self.state_store
            .store_destination_table_metadata(table_id, updated_metadata.to_applied())
            .await?;

        // We must invalidate all connections here to make sure that caches on
        // the connection side on BigQuery do not interfere with the schema
        // changes. Each worker processes invalidations in channel order and
        // this call waits for all workers to ack them. Parallel invalidations
        // may interleave, but once this returns, later appends will use fresh
        // connections.
        self.client.invalidate_all_connections().await;

        info!(
            table_id = %table_id,
            snapshot_id = %new_snapshot_id,
            "bigquery table schema change completed"
        );

        Ok(())
    }

    /// Translates a validated schema plan into BigQuery DDL.
    ///
    /// Operations execute in the supplied order. Shared planning already owns
    /// name-equivalence validation; this layer only applies BigQuery behavior
    /// for structural, nullability, and default operations and warns when
    /// skipping an unsupported type alteration.
    async fn apply_schema_plan(
        &self,
        table_id: &TableId,
        versioned_table_id: &VersionedBigQueryTableId,
        plan: &SchemaPlan,
    ) -> EtlResult<()> {
        if plan.is_empty() {
            debug!(%table_id, "no schema changes to apply for table");
            return Ok(());
        }

        debug!(
            source_table_id = %table_id,
            destination_table_id = %versioned_table_id,
            added_column_count = plan.diff().added_columns.len(),
            removed_column_count = plan.diff().dropped_columns.len(),
            altered_column_count = plan.diff().altered_columns.len(),
            "applying bigquery table schema plan"
        );

        let physical_table_id = versioned_table_id.to_string();

        // Apply the shared plan without regrouping operations.
        for operation in plan.ordered_operations() {
            match operation {
                SchemaOperation::DropColumn { before_column_schema, reason: _ } => {
                    self.client
                        .drop_column(
                            &self.dataset_id,
                            &physical_table_id,
                            &before_column_schema.name,
                        )
                        .await?;
                }
                SchemaOperation::AddColumn { after_column_schema, reason: _ } => {
                    self.client
                        .add_column(&self.dataset_id, &physical_table_id, after_column_schema)
                        .await?;

                    // BigQuery adds the column as nullable, leaving existing
                    // rows NULL. SET DEFAULT affects only future inserts, so
                    // supported defaults are safe for both physical table
                    // additions and publication-mask additions.
                    if let Some(default_expression) = column_default_sql(after_column_schema) {
                        self.client
                            .set_column_default(
                                &self.dataset_id,
                                &physical_table_id,
                                &after_column_schema.name,
                                &default_expression,
                            )
                            .await?;
                    } else if after_column_schema.default_expression.is_some() {
                        warn!(
                            table_id = %table_id,
                            column_name = %after_column_schema.name,
                            "skipping unsupported source column default for bigquery"
                        );
                    }
                }
                SchemaOperation::AlterColumn { alteration } => {
                    let before = alteration.before_column_schema();
                    let after = alteration.after_column_schema();
                    match alteration.kind() {
                        ColumnAlterationKind::Rename => {
                            self.client
                                .rename_column(
                                    &self.dataset_id,
                                    &physical_table_id,
                                    &before.name,
                                    &after.name,
                                )
                                .await?;
                        }
                        ColumnAlterationKind::Type => {
                            warn_unsupported_column_type_change(
                                "bigquery",
                                versioned_table_id,
                                alteration,
                            );
                        }
                        ColumnAlterationKind::Nullability => {
                            // Unsupported metadata changes can leave BigQuery
                            // intentionally divergent, so later operations must
                            // tolerate that state.
                            if !before.nullable && after.nullable {
                                self.client
                                    .drop_column_not_null(
                                        &self.dataset_id,
                                        &physical_table_id,
                                        &before.name,
                                    )
                                    .await?;
                            } else {
                                warn!(
                                    table_id = %table_id,
                                    column_name = %before.name,
                                    "bigquery does not support setting not null on an existing \
                                     column; keeping the destination column nullable"
                                );
                            }
                        }
                        ColumnAlterationKind::Default => {
                            if before.default_expression.is_some() {
                                self.client
                                    .clear_column_default(
                                        &self.dataset_id,
                                        &physical_table_id,
                                        &before.name,
                                    )
                                    .await?;
                            }

                            if after.default_expression.is_some() {
                                if let Some(default_expression) = column_default_sql(after) {
                                    self.client
                                        .set_column_default(
                                            &self.dataset_id,
                                            &physical_table_id,
                                            &before.name,
                                            &default_expression,
                                        )
                                        .await?;
                                } else {
                                    warn!(
                                        table_id = %table_id,
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
            destination_table_id = %versioned_table_id,
            "bigquery table schema plan applied"
        );

        Ok(())
    }

    /// Processes CDC events in batches with proper ordering and truncate
    /// handling.
    ///
    /// Groups streaming operations (insert/update/delete) by table and
    /// processes them together, then handles truncate events separately by
    /// creating new versioned tables. Uses the schema from the first event of
    /// each table for table creation and descriptor building.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut events_iter = events.into_iter().peekable();

        while events_iter.peek().is_some() {
            // Maps table ID to (schema, rows). We are assuming that the table
            // schema is the same for all events within two Relation event
            // boundaries.
            let mut table_id_to_data: HashMap<
                TableId,
                (ReplicatedTableSchema, Vec<BigQueryTableRow>),
            > = HashMap::new();

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

                        let table_id = insert.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
                            (insert.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.1.push(BigQueryTableRow::try_from(insert.table_row)?);
                    }
                    Event::Update(update) => {
                        let event_position = update.event_position();
                        let table_id = update.replicated_table_schema.id();
                        let table_row = bigquery_update_new_row(
                            &update.replicated_table_schema,
                            update.updated_table_row,
                        )?;

                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
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

                        let table_id = delete.replicated_table_schema.id();
                        let entry = table_id_to_data.entry(table_id).or_insert_with(|| {
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
            if !table_id_to_data.is_empty() {
                let mut append_requests = Vec::with_capacity(table_id_to_data.len());

                for (request_index, (_, (replicated_table_schema, table_rows))) in
                    table_id_to_data.into_iter().enumerate()
                {
                    let (table_target, table_descriptor) =
                        self.prepare_table_for_writes(&replicated_table_schema, true).await?;
                    let physical_table_id = &table_target.physical_id;
                    histogram!(ETL_BQ_APPEND_BATCHES_BATCH_SIZE).record(table_rows.len() as f64);

                    let append_request = self.client.create_append_request(
                        self.pipeline_id,
                        request_index,
                        &self.dataset_id,
                        physical_table_id,
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

    /// Handles table truncation by creating new versioned tables and updating
    /// views.
    ///
    /// Creates fresh empty tables with independently allocated version IDs,
    /// updates views to point to new tables, and schedules cleanup of old
    /// table versions. Uses the provided schemas directly instead of
    /// looking them up from a store.
    async fn process_truncate_for_schemas(
        &self,
        replicated_table_schemas: impl IntoIterator<Item = ReplicatedTableSchema>,
    ) -> EtlResult<()> {
        for replicated_table_schema in replicated_table_schemas {
            let table_id = replicated_table_schema.id();
            let mut table = self.lock_table(table_id).await;
            let bigquery_table_id =
                table_name_to_bigquery_table_id(replicated_table_schema.name())?;

            // We need to determine the current versioned table ID for this
            // table.
            //
            // If no destination table metadata exists, it means the table was
            // never created in BigQuery (e.g., due to validation errors during
            // copy). In this case, we skip the truncate since there's nothing
            // to truncate.
            let Some(metadata) = self.get_applied_destination_table_metadata(table_id).await?
            else {
                warn!(
                    %table_id,
                    "destination metadata not found while processing bigquery truncate event"
                );
                continue;
            };
            ensure_destination_schema_matches_metadata(
                "BigQuery",
                table_id,
                &metadata,
                &replicated_table_schema,
            )?;
            let versioned_table_id =
                metadata_versioned_table_id_for_base(&metadata, &bigquery_table_id)?;

            // Capture candidates before creation while holding serialization.
            // Metadata supplies the previous target even if listing fails or
            // lags. Cleanup must never discover a newer table after the switch.
            let mut obsolete_table_ids =
                self.list_table_versions_for_cleanup(&bigquery_table_id).await;
            obsolete_table_ids.insert(versioned_table_id);
            let next_table_target = Arc::new(TableTarget::new(
                VersionedBigQueryTableId::new(bigquery_table_id.clone()),
                replicated_table_schema.name().clone(),
            ));

            // Create-only DDL fails on a collision instead of replacing data.
            self.client
                .create_table(
                    &self.dataset_id,
                    &next_table_target.physical_id,
                    &replicated_table_schema,
                    self.max_staleness_mins,
                    self.table_options.get(&table_id),
                )
                .await?;

            // Update the view to point to the new table.
            self.ensure_view_points_to_table(&mut table, &next_table_target).await?;

            // Update the metadata to point to the new table.
            let metadata = DestinationTableMetadata::new_applied(
                next_table_target.physical_id.clone(),
                replicated_table_schema.inner().snapshot_id,
                replicated_table_schema.replication_mask().clone(),
            );
            self.state_store.store_destination_table_metadata(table_id, metadata).await?;

            // These operations are not transactional. If metadata persistence
            // fails, replay still starts from the old authoritative target.
            // The view can temporarily expose the empty new table, but replay
            // creates another fresh version and switches it again. If
            // metadata commits but acknowledgement or checkpointing fails,
            // replay also allocates a fresh version for the truncate.
            // Cleanup only targets obsolete identities after persistence;
            // delayed deletion cannot affect a future incarnation of a name.

            info!(
                table_id = table_id.0,
                %next_table_target,
                "successfully processed truncate, view updated"
            );

            self.schedule_table_cleanup(obsolete_table_ids).await;
        }

        Ok(())
    }

    /// Drops the logical view and optimistically deletes physical versions.
    ///
    /// View deletion must succeed before reset can proceed. Physical deletion
    /// and discovery failures are logged and may leave tables for manual
    /// cleanup.
    async fn drop_table_for_copy_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let mut table = self.lock_table(replicated_table_schema.id()).await;
        let base_bigquery_table_id =
            table_name_to_bigquery_table_id(replicated_table_schema.name())?;
        let table_id = replicated_table_schema.id();
        let mut table_ids = HashSet::new();

        // Always include the durable metadata target. BigQuery
        // INFORMATION_SCHEMA can lag behind recently-created tables, but
        // destination metadata is the source of truth for the table this
        // pipeline last wrote to.
        if let Some(metadata) = self.state_store.get_destination_table_metadata(table_id).await? {
            table_ids
                .insert(metadata_versioned_table_id_for_base(&metadata, &base_bigquery_table_id)?);
        }

        table_ids.extend(self.list_table_versions_for_cleanup(&base_bigquery_table_id).await);

        // Invalidate before DDL: cancellation or an ambiguous error can leave
        // the view removed even when this call does not report success.
        table.invalidate();

        // Removing the logical view is required. Once it is gone, failures to
        // delete its old physical targets cannot expose stale data after
        // recopy.
        self.client.drop_view_if_exists(&self.dataset_id, &base_bigquery_table_id).await?;

        delete_table_versions(&self.client, &self.dataset_id, table_ids).await;

        // Release Storage Write API connections that may still refer to the
        // deleted physical tables. Subsequent copies allocate fresh names.
        self.client.invalidate_all_connections().await;

        info!(table_id = table_id.0, "removed bigquery view before copy");

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
    for operation in plan.ordered_operations() {
        let SchemaOperation::AddColumn { after_column_schema, reason } = operation else {
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
        store::{MemoryStore, StateStore},
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
            VersionedBigQueryTableId::new(table_name_to_bigquery_table_id(schema.name()).unwrap()),
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

    /// Seeds metadata and the view cache so requests reach append without DDL.
    async fn seed_table(
        destination: &BigQueryDestination<MemoryStore>,
        schema: &ReplicatedTableSchema,
    ) {
        let target = table_target(schema);
        let metadata = DestinationTableMetadata::new_applied(
            target.physical_id.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), metadata)
            .await
            .unwrap();
        destination.lock_table(schema.id()).await.view_target = Some(target);
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
            destination.prepare_table_for_writes(&warm_schema, false).await.unwrap();

        let pending = destination.prepare_table_for_writes(&cold_schema, false);
        tokio::pin!(pending);
        assert!(pending.as_mut().now_or_never().is_none());

        let (reused_target, reused_descriptor) = destination
            .prepare_table_for_writes(&warm_schema, false)
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
        let copy = table.prepare_descriptor(&schema, false);
        assert!(Arc::ptr_eq(&copy, &table.prepare_descriptor(&schema, false)));
        assert_eq!(copy.field_descriptors.len(), 4);
        assert!(matches!(copy.field_descriptors[0].mode, ColumnMode::Required));

        let cdc = table.prepare_descriptor(&schema, true);
        assert!(!Arc::ptr_eq(&copy, &cdc));
        assert_eq!(cdc.field_descriptors.len(), 4);
        assert!(matches!(cdc.field_descriptors[0].mode, ColumnMode::Nullable));
        assert_eq!(cdc.field_descriptors[3].name, "_CHANGE_SEQUENCE_NUMBER");

        let projected = ReplicatedTableSchema::from_mask(
            Arc::new(schema.inner().clone()),
            ReplicationMask::from_bytes(vec![1, 0]),
        );
        let projected_descriptor = table.prepare_descriptor(&projected, true);
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
        let next = table.prepare_descriptor(&next_schema, true);
        assert!(!Arc::ptr_eq(&projected_descriptor, &next));
        assert_eq!(next.field_descriptors[0].name, "renamed_id");
        assert!(Arc::ptr_eq(&next, &table.prepare_descriptor(&next_schema, true)));
    }

    /// A warm view cache cannot bypass authoritative schema state or identity.
    #[tokio::test]
    async fn warm_cache_revalidates_destination_metadata() {
        let destination = unavailable_destination(false).await;
        let schema = replicated_schema(IdentityType::PrimaryKey);
        let target = table_target(&schema);
        let metadata = DestinationTableMetadata::new_applied(
            target.physical_id.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), metadata.clone())
            .await
            .unwrap();
        destination.lock_table(schema.id()).await.view_target = Some(Arc::clone(&target));

        let (resolved, descriptor) =
            destination.prepare_table_for_writes(&schema, false).await.unwrap();
        assert!(Arc::ptr_eq(&target, &resolved));
        let (_, reused) = destination.prepare_table_for_writes(&schema, false).await.unwrap();
        assert!(Arc::ptr_eq(&descriptor, &reused));

        // The cached target and descriptor do not authorize a different
        // endpoint, even when its physical table is unchanged.
        for (snapshot_id, mask) in [
            (SnapshotId::new(PgLsn::from(1), PgLsn::from(1)), schema.replication_mask().clone()),
            (schema.inner().snapshot_id, ReplicationMask::from_bytes(vec![1, 0])),
        ] {
            destination
                .state_store
                .store_destination_table_metadata(
                    schema.id(),
                    DestinationTableMetadata::new_applied(
                        target.physical_id.clone(),
                        snapshot_id,
                        mask,
                    ),
                )
                .await
                .unwrap();
            assert_eq!(
                destination.prepare_table_for_writes(&schema, false).await.unwrap_err().kind(),
                ErrorKind::DestinationSchemaRewind
            );
        }

        let applying = metadata
            .with_schema_change(
                SnapshotId::new(PgLsn::from(1), PgLsn::from(1)),
                schema.replication_mask().clone(),
            )
            .unwrap();
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), applying)
            .await
            .unwrap();
        assert_eq!(
            destination.prepare_table_for_writes(&schema, false).await.unwrap_err().kind(),
            ErrorKind::InvalidState
        );

        let replacement = table_target(&schema);
        let metadata = DestinationTableMetadata::new_applied(
            replacement.physical_id.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );
        destination
            .state_store
            .store_destination_table_metadata(schema.id(), metadata)
            .await
            .unwrap();
        // A different durable target requires view DDL, which this fixture
        // rejects.
        assert_eq!(
            destination.prepare_table_for_writes(&schema, false).await.unwrap_err().kind(),
            ErrorKind::DestinationAuthenticationError
        );
        let table = destination.lock_table(schema.id()).await;
        assert!(table.view_target.is_none());
        assert!(table.prepared_schema.is_none());
    }

    /// Even a same-target schema refresh must discard cached state on failure.
    #[tokio::test]
    async fn failed_view_refresh_invalidates_cache() {
        let destination = unavailable_destination(false).await;
        let schema = replicated_schema(IdentityType::PrimaryKey);
        let target = table_target(&schema);
        let mut table = destination.lock_table(schema.id()).await;
        table.view_target = Some(Arc::clone(&target));
        table.prepare_descriptor(&schema, false);

        destination.ensure_view_points_to_table(&mut table, &target).await.unwrap();
        assert_eq!(
            destination.recreate_view_for_table(&mut table, &target).await.unwrap_err().kind(),
            ErrorKind::DestinationAuthenticationError
        );
        assert!(table.view_target.is_none());
        assert!(table.prepared_schema.is_none());
    }

    /// Cancelling in-flight view DDL must not leave the old target certified.
    #[tokio::test]
    async fn cancelled_view_refresh_invalidates_cache() {
        let destination = unavailable_destination(true).await;
        let schema = replicated_schema(IdentityType::PrimaryKey);
        let target = table_target(&schema);
        let mut table = destination.lock_table(schema.id()).await;
        table.view_target = Some(Arc::clone(&target));
        table.prepare_descriptor(&schema, false);

        {
            let refresh = destination.recreate_view_for_table(&mut table, &target);
            tokio::pin!(refresh);
            assert!(futures::poll!(refresh).is_pending());
        }

        assert!(table.view_target.is_none());
        assert!(table.prepared_schema.is_none());
    }

    /// Reset invalidates before attempting the drop, including failed drops.
    #[tokio::test]
    async fn failed_view_drop_invalidates_cache() {
        let destination = unavailable_destination(false).await;
        let schema = replicated_schema(IdentityType::PrimaryKey);
        {
            let mut table = destination.lock_table(schema.id()).await;
            table.view_target = Some(table_target(&schema));
            table.prepare_descriptor(&schema, false);
        }

        assert_eq!(
            destination.drop_table_for_copy_inner(&schema).await.unwrap_err().kind(),
            ErrorKind::DestinationAuthenticationError
        );
        let table = destination.lock_table(schema.id()).await;
        assert!(table.view_target.is_none());
        assert!(table.prepared_schema.is_none());
    }

    #[test]
    fn table_name_to_bigquery_table_id_uses_escaped_components() {
        for (schema, table, expected) in [
            ("schema", "table", "schema_table"),
            ("a_b", "c_d", "a__b_c__d"),
            ("a__b", "c__d", "a____b_c____d"),
        ] {
            let table_name = TableName::new(schema.to_owned(), table.to_owned());
            assert_eq!(table_name_to_bigquery_table_id(&table_name).unwrap(), expected);
        }
    }

    #[test]
    fn table_name_to_bigquery_table_id_collision_prevention() {
        // These two cases previously collided to "a_b_c"
        let table_name1 = TableName::new("a_b".to_owned(), "c".to_owned());
        let table_name2 = TableName::new("a".to_owned(), "b_c".to_owned());

        let id1 = table_name_to_bigquery_table_id(&table_name1).unwrap();
        let id2 = table_name_to_bigquery_table_id(&table_name2).unwrap();

        assert_eq!(id1, "a__b_c");
        assert_eq!(id2, "a_b__c");
        assert_ne!(id1, id2, "Table IDs should not collide");
    }

    #[test]
    fn versioned_bigquery_table_id_parses_and_roundtrips_valid_ids() {
        for (input, base, version) in [
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
            let parsed = input.parse::<VersionedBigQueryTableId>().unwrap();
            assert_eq!(parsed.base_table_id(), base);
            assert_eq!(parsed.1, TableVersionId::Legacy(version));
            assert_eq!(parsed.to_string(), input);
        }
    }

    /// Covers every UUID bit and the fixed-width distinction from legacy IDs.
    #[test]
    fn version_encoding_is_compact_and_lossless() {
        for value in [0, 1, u128::from(u64::MAX), u128::MAX] {
            let id = VersionedBigQueryTableId(
                "users".to_owned(),
                TableVersionId::Random(Uuid::from_u128(value)),
            );
            let encoded = id.to_string();
            assert_eq!(encoded.len(), "users_".len() + 25);
            assert!(
                encoded
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            );
            assert_eq!(encoded.parse::<VersionedBigQueryTableId>().unwrap(), id);
        }
    }

    #[test]
    fn versioned_bigquery_table_id_belongs_to_base() {
        let base_table_id = "users_table".to_owned();
        let users_table_id =
            VersionedBigQueryTableId(base_table_id.clone(), TableVersionId::Legacy(2));
        let orders_table_id =
            VersionedBigQueryTableId("orders_table".to_owned(), TableVersionId::Legacy(2));

        assert!(users_table_id.belongs_to_base(&base_table_id));
        assert!(!orders_table_id.belongs_to_base(&base_table_id));
    }

    #[test]
    fn versioned_bigquery_table_id_parse_for_base_ignores_unrelated_tables() {
        let base_table_id = "users_table".to_owned();

        assert_eq!(
            VersionedBigQueryTableId::parse_for_base("users_table_2", &base_table_id),
            Some(VersionedBigQueryTableId("users_table".to_owned(), TableVersionId::Legacy(2)))
        );
        assert_eq!(
            VersionedBigQueryTableId::parse_for_base("users_table_backup", &base_table_id),
            None
        );
        assert_eq!(
            VersionedBigQueryTableId::parse_for_base("orders_table_2", &base_table_id),
            None
        );
        assert_eq!(VersionedBigQueryTableId::parse_for_base("users_table", &base_table_id), None);

        let current = VersionedBigQueryTableId::new(base_table_id.clone());
        let neighboring = VersionedBigQueryTableId::new(format!("{base_table_id}_archive"));
        assert_eq!(
            VersionedBigQueryTableId::parse_for_base(&current.to_string(), &base_table_id),
            Some(current)
        );
        assert_eq!(
            VersionedBigQueryTableId::parse_for_base(&neighboring.to_string(), &base_table_id),
            None
        );
    }

    #[test]
    fn metadata_versioned_table_id_for_base_uses_metadata_target() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let metadata = DestinationTableMetadata::new_applied(
            "users_table_3".to_owned(),
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );

        let versioned_table_id =
            metadata_versioned_table_id_for_base(&metadata, &"users_table".to_owned()).unwrap();

        assert_eq!(
            versioned_table_id,
            VersionedBigQueryTableId("users_table".to_owned(), TableVersionId::Legacy(3))
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
    fn metadata_versioned_table_id_for_base_rejects_unrelated_metadata_target() {
        let replicated_table_schema = replicated_schema(IdentityType::PrimaryKey);
        let metadata = DestinationTableMetadata::new_applied(
            "orders_table_0".to_owned(),
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );

        let err =
            metadata_versioned_table_id_for_base(&metadata, &"users_table".to_owned()).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidState);
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
    fn versioned_bigquery_table_id_rejects_invalid_ids() {
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
            let error = input.parse::<VersionedBigQueryTableId>().unwrap_err();
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
