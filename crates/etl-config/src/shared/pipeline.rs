use std::collections::HashSet;

use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::shared::{
    PgConnectionConfig, PgConnectionConfigWithoutSecrets, Validate, ValidationError,
};

/// Batch processing configuration for pipelines.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct BatchConfig {
    /// Maximum time, in milliseconds, to wait before flushing a partially
    /// filled batch.
    ///
    /// This is the latency bound for stream batching: once the first item
    /// enters a batch, the batch is flushed when this timer elapses, even
    /// if byte/row targets were not met.
    ///
    /// In practice, flush happens on the first trigger between this timeout and
    /// the memory-based byte target driven by
    /// [`Self::memory_budget_ratio`].
    #[serde(default = "default_batch_max_fill_ms")]
    #[cfg_attr(feature = "utoipa", schema(example = 0))]
    pub max_fill_ms: u64,
    /// Maximum ratio of memory capacity targeted for decoded source batches.
    ///
    /// This value is expressed as a ratio in the `(0.0, 1.0]` interval.
    /// The resulting advisory global target includes batches being accumulated
    /// and batches retained by asynchronous destination writes. When memory
    /// backpressure is configured, current system or cgroup usage can reduce
    /// this target further as usage approaches the midpoint between the
    /// resume and activation thresholds. The effective target is divided
    /// across potential concurrently retained batches.
    ///
    /// Together with [`Self::max_fill_ms`], this controls stream flushes:
    /// batches flush either when their accumulated size estimate reaches
    /// the advisory per-batch byte target or when the fill timeout elapses,
    /// whichever happens first.
    ///
    /// This is a batching heuristic, not a memory allocator or a hard bound.
    /// ETL compares decoded size estimates with the target after each item, so
    /// actual allocations can differ and one indivisible item can overshoot it.
    /// System memory and tracked batch bytes are sampled separately, so the
    /// derived target may also contain temporal sampling skew. ETL freezes that
    /// approximate global target for each memory-snapshot revision; changes in
    /// active batch slots only repartition it until the next revision.
    /// The goal is to keep batches approximately below the target as available
    /// memory changes while preserving headroom for allocations beyond incoming
    /// rows, such as destination batch building and serialization buffers.
    ///
    /// ETL accounts a decoded batch until the corresponding destination async
    /// result completes. Completion is an ownership boundary, not a guarantee
    /// that the allocator has returned physical memory. If a destination keeps
    /// the input or derived buffers after completion, the next system or cgroup
    /// sample still observes that memory and classifies it as non-batch usage,
    /// which can make later batches smaller.
    #[serde(default = "default_memory_budget_ratio")]
    #[cfg_attr(feature = "utoipa", schema(example = 0.2))]
    pub memory_budget_ratio: f32,
    /// Maximum preferred byte size for one source batch.
    ///
    /// This bounds multi-row accumulation within one memory sampling interval,
    /// even when a large pod has enough headroom to derive a much larger
    /// dynamic target. It also limits destination batch latency and
    /// serialization work. The runtime chooses the smaller value between this
    /// ceiling and the target computed from [`Self::memory_budget_ratio`]. A
    /// single source row may exceed it because rows cannot be split safely
    /// after decoding.
    #[serde(default = "default_batch_max_bytes")]
    #[cfg_attr(feature = "utoipa", schema(example = 33554432))]
    pub max_bytes: usize,
}

impl BatchConfig {
    /// Default maximum fill time in milliseconds.
    pub const DEFAULT_MAX_FILL_MS: u64 = 10000;

    /// Default fraction used for batch byte budgeting.
    ///
    /// The governor targets at most 20% of the current system capacity or
    /// cgroup memory limit for decoded batches, and may target less when other
    /// allocations consume the normal operating headroom. An indivisible row
    /// can exceed the target after it has already been decoded.
    pub const DEFAULT_MEMORY_BUDGET_RATIO: f32 = 0.2;

    /// Default maximum preferred source batch size in bytes.
    ///
    /// The 32 MiB cap supplies substantial destination work while bounding
    /// multi-row accumulation before reactive memory backpressure can observe
    /// new allocations. An indivisible source row can exceed the cap.
    pub const DEFAULT_MAX_BYTES: usize = 32 * 1024 * 1024;
}

impl Validate for BatchConfig {
    /// Validates batch configuration settings.
    fn validate(&self) -> Result<(), ValidationError> {
        if !(0.0..=1.0).contains(&self.memory_budget_ratio) || self.memory_budget_ratio == 0.0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "batch.memory_budget_ratio".to_owned(),
                constraint: "must be in the (0.0, 1.0] interval".to_owned(),
            });
        }

        if self.max_bytes == 0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "batch.max_bytes".to_owned(),
                constraint: "must be greater than 0".to_owned(),
            });
        }

        Ok(())
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_fill_ms: default_batch_max_fill_ms(),
            memory_budget_ratio: default_memory_budget_ratio(),
            max_bytes: default_batch_max_bytes(),
        }
    }
}

const fn default_batch_max_fill_ms() -> u64 {
    BatchConfig::DEFAULT_MAX_FILL_MS
}

const fn default_memory_budget_ratio() -> f32 {
    BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO
}

const fn default_batch_max_bytes() -> usize {
    BatchConfig::DEFAULT_MAX_BYTES
}

/// Behavior when the main replication slot is found to be invalidated.
///
/// A replication slot can become invalidated when it falls too far behind the
/// current WAL position (e.g., when `max_slot_wal_keep_size` is exceeded) or
/// when PostgreSQL explicitly invalidates it. This enum controls how the
/// pipeline responds to such situations.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, Default)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum InvalidatedSlotBehavior {
    /// Prevents pipeline startup when the slot is invalidated.
    ///
    /// The pipeline will fail with an error indicating that the slot needs to
    /// be manually addressed before replication can continue. This is the
    /// safest option as it requires explicit operator intervention.
    #[default]
    Error,
    /// Automatically recreates the slot and restarts replication from scratch.
    ///
    /// When an invalidated slot is detected, the pipeline will:
    /// 1. Reset all table states to `Init`
    /// 2. Delete all existing replication slots for the pipeline
    /// 3. Create a new replication slot
    /// 4. Run table sync for all tables, respecting [`TableSyncCopyConfig`]
    ///    rules
    ///
    /// This option allows the pipeline to restart replication and automatically
    /// recover.
    Recreate,
}

/// Controls which tables are eligible for initial table copy and streaming.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
#[derive(Default)]
pub enum TableSyncCopyConfig {
    /// Performs the initial copy for all tables.
    #[default]
    IncludeAllTables,
    /// Skips the initial copy for all tables.
    SkipAllTables,
    /// Performs the initial copy for the specified table ids.
    IncludeTables {
        /// Table ids of the table for which copy should be performed.
        table_ids: Vec<u32>,
    },
    /// Skips the initial copy for the specified table ids.
    SkipTables {
        /// Table ids of the table for which copy should be skipped.
        table_ids: Vec<u32>,
    },
}

impl TableSyncCopyConfig {
    /// Returns `true` if the table should be copied during initial sync,
    /// `false` otherwise.
    pub fn should_copy_table(&self, table_id: u32) -> bool {
        match self {
            TableSyncCopyConfig::IncludeAllTables => true,
            TableSyncCopyConfig::SkipAllTables => false,
            TableSyncCopyConfig::IncludeTables { table_ids } => table_ids.contains(&table_id),
            TableSyncCopyConfig::SkipTables { table_ids } => !table_ids.contains(&table_id),
        }
    }
}

impl Validate for TableSyncCopyConfig {
    fn validate(&self) -> Result<(), ValidationError> {
        let table_ids = match self {
            TableSyncCopyConfig::IncludeAllTables | TableSyncCopyConfig::SkipAllTables => {
                return Ok(());
            }
            TableSyncCopyConfig::IncludeTables { table_ids }
            | TableSyncCopyConfig::SkipTables { table_ids } => table_ids,
        };

        let mut seen_table_ids = HashSet::with_capacity(table_ids.len());
        for table_id in table_ids {
            if !seen_table_ids.insert(table_id) {
                return Err(ValidationError::InvalidFieldValue {
                    field: "table_sync_copy.table_ids".to_owned(),
                    constraint: format!(
                        "must be unique; table id {table_id} is configured more than once"
                    ),
                });
            }
        }

        Ok(())
    }
}

/// Emergency memory backpressure configuration.
///
/// Dynamic batch sizing normally reduces ETL-owned decoded memory before this
/// signal activates. The signal remains a whole-workload safety boundary for
/// memory outside that accounting, an oversized source row, or a cgroup limit
/// reduction. Its resume threshold provides recovery headroom before source
/// polling restarts.
///
/// Activating backpressure stops source polling but does not cancel destination
/// work or prevent already buffered ETL batches from being flushed. This lets
/// governed memory drain without introducing a lock cycle. If measured usage
/// remains at or above the resume threshold after every ETL-owned batch has
/// drained, polling intentionally remains paused: allocator-retained pages,
/// destination work, database-driver queues, page cache, or another part of the
/// measured memory domain is still consuming the recovery headroom. Resuming
/// speculatively in that state would weaken the OOM safety boundary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct MemoryBackpressureConfig {
    /// Memory usage ratio at or above which source polling is paused.
    ///
    /// Valid range is `(0.0, 1.0]`.
    pub activate_threshold: f32,
    /// Memory usage ratio below which source polling resumes.
    ///
    /// Valid range is `[0.0, 1.0)`, and this value must be lower than
    /// [`Self::activate_threshold`].
    pub resume_threshold: f32,
}

impl MemoryBackpressureConfig {
    /// Default memory usage ratio to activate backpressure.
    ///
    /// The normal batch-sizing target is the midpoint between the default
    /// thresholds (80%). Activating at 85% leaves a 5% margin for allocations
    /// that have not yet appeared in a sample or decoded-batch accounting.
    pub const DEFAULT_ACTIVATE_THRESHOLD: f32 = 0.85;
    /// Default memory usage ratio to release backpressure.
    ///
    /// Resuming below 75% restores a 5% margin below the normal batch-sizing
    /// target. The resulting 10% hysteresis absorbs allocator lag and unbounded
    /// allocations that are not represented by decoded-batch accounting.
    pub const DEFAULT_RESUME_THRESHOLD: f32 = 0.75;
}

impl Validate for MemoryBackpressureConfig {
    /// Validates memory backpressure thresholds.
    fn validate(&self) -> Result<(), ValidationError> {
        if !(0.0..=1.0).contains(&self.activate_threshold) || self.activate_threshold == 0.0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "memory_backpressure.activate_threshold".to_owned(),
                constraint: "must be in the (0.0, 1.0] interval".to_owned(),
            });
        }

        if !(0.0..=1.0).contains(&self.resume_threshold) || self.resume_threshold == 1.0 {
            return Err(ValidationError::InvalidFieldValue {
                field: "memory_backpressure.resume_threshold".to_owned(),
                constraint: "must be in the [0.0, 1.0) interval".to_owned(),
            });
        }

        if self.resume_threshold >= self.activate_threshold {
            return Err(ValidationError::InvalidFieldValue {
                field: "memory_backpressure.resume_threshold".to_owned(),
                constraint: "must be lower than memory_backpressure.activate_threshold".to_owned(),
            });
        }

        Ok(())
    }
}

impl Default for MemoryBackpressureConfig {
    fn default() -> Self {
        Self {
            activate_threshold: Self::DEFAULT_ACTIVATE_THRESHOLD,
            resume_threshold: Self::DEFAULT_RESUME_THRESHOLD,
        }
    }
}

/// Configuration for a pipeline's PostgreSQL logical replication slots.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct ReplicationSlotConfig {
    /// Enables PostgreSQL failover support for logical replication slots.
    ///
    /// On PostgreSQL 17 or newer, ETL creates new slots with the `FAILOVER`
    /// option and upgrades an existing apply-worker slot that is not already
    /// failover-enabled. Standbys must still be configured to synchronize
    /// failover slots before those slots can be used after promotion.
    #[serde(default)]
    #[cfg_attr(feature = "utoipa", schema(example = false))]
    pub failover: bool,
}

/// Configuration for an ETL pipeline.
///
/// Contains all settings required to run a replication pipeline including
/// source database connection, optional store database connection, batching
/// parameters, and worker limits.
///
/// This intentionally does not implement [`Serialize`] to avoid accidentally
/// leaking secrets in the config into serialized forms.
#[derive(Clone, Debug, Deserialize)]
pub struct PipelineConfig {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of
    /// replication slots and state store.
    pub id: u64,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
    /// The connection configuration for the Postgres instance to which the
    /// pipeline connects for replication.
    pub pg_connection: PgConnectionConfig,
    /// Optional Postgres connection configuration for pipeline state storage.
    ///
    /// When `None`, the pipeline state store should use
    /// [`Self::pg_connection`]. This allows logical replication and table
    /// copy to read from a standby while keeping the Postgres-backed state
    /// store on a writable endpoint.
    #[serde(default)]
    pub store_pg_connection: Option<PgConnectionConfig>,
    /// Configuration for the pipeline's logical replication slots.
    #[serde(default)]
    pub replication_slot: ReplicationSlotConfig,
    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another for timed worker
    /// retries.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_delay_ms")]
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic timed retry attempts before failing the
    /// worker.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_max_attempts")]
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    #[serde(default = "default_max_table_sync_workers")]
    pub max_table_sync_workers: u16,
    /// Maximum worker connections per table during initial copy.
    ///
    /// Initial copy always uses ctid range work items, including when this is
    /// set to 1. ETL may plan more ctid ranges than worker connections so
    /// workers can pull new ranges as they finish.
    #[serde(default = "default_max_copy_connections_per_table")]
    pub max_copy_connections_per_table: u16,
    /// Number of milliseconds between coherent memory snapshots.
    ///
    /// One shared sampler drives dynamic batch targets and emergency
    /// backpressure. Batch hot paths reuse each snapshot instead of reading
    /// operating-system or cgroup files per row.
    #[serde(default = "default_memory_refresh_interval_ms")]
    pub memory_refresh_interval_ms: u64,
    /// Optional memory-based backpressure configuration.
    ///
    /// `None` disables memory backpressure. When omitted, this defaults to
    /// `Some(MemoryBackpressureConfig::default())`.
    #[serde(default = "default_memory_backpressure")]
    pub memory_backpressure: Option<MemoryBackpressureConfig>,
    /// Selection rules for tables participating in replication.
    #[serde(default)]
    pub table_sync_copy: TableSyncCopyConfig,
    /// Number of milliseconds between periodic table sync monitor checks,
    /// such as reporting replication lag and checking replication slot
    /// validity during table copy. Also used by the apply worker's periodic
    /// replication lag sampling.
    #[serde(default = "default_table_sync_monitor_refresh_interval_ms")]
    pub table_sync_monitor_refresh_interval_ms: u64,
    /// Behavior when the main replication slot is found to be invalidated.
    #[serde(default)]
    pub invalidated_slot_behavior: InvalidatedSlotBehavior,
    /// Whether [`Pipeline::start`] should run the source migrations that
    /// install the schema helper functions and the `ddl_command_end` event
    /// trigger.
    ///
    /// Defaults to `true`, preserving the existing behavior. Set to `false`
    /// when the replication role is intentionally de-elevated and lacks the
    /// superuser privilege required to `CREATE EVENT TRIGGER`; in that case
    /// the source objects must be installed out-of-band by an admin (see
    /// the source migrations under `crates/etl/migrations/source`).
    /// Pipelines that do not rely on DDL-change propagation can safely run
    /// with this disabled.
    #[serde(default = "default_run_source_migrations")]
    pub run_source_migrations: bool,
}

impl PipelineConfig {
    /// Default retry delay in milliseconds between table error retries.
    pub const DEFAULT_TABLE_ERROR_RETRY_DELAY_MS: u64 = 10000;

    /// Default maximum number of retry attempts for table errors.
    pub const DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS: u32 = 5;

    /// Default maximum number of concurrent table sync workers.
    pub const DEFAULT_MAX_TABLE_SYNC_WORKERS: u16 = 4;

    /// Default maximum worker connections per table during initial copy.
    pub const DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE: u16 = 4;

    /// Default interval in milliseconds between coherent memory snapshots.
    ///
    /// A 100 ms interval reacts quickly to allocation bursts and cgroup limit
    /// changes without moving operating-system reads into per-row hot paths.
    pub const DEFAULT_MEMORY_REFRESH_INTERVAL_MS: u64 = 100;

    /// Default interval in milliseconds between periodic replication monitor
    /// checks.
    pub const DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS: u64 = 10_000;

    /// Returns the Postgres connection configuration for state storage.
    pub fn store_pg_connection(&self) -> &PgConnectionConfig {
        self.store_pg_connection.as_ref().unwrap_or(&self.pg_connection)
    }
}

impl Validate for PipelineConfig {
    /// Validates pipeline configuration settings.
    fn validate(&self) -> Result<(), ValidationError> {
        validate_pipeline_settings(
            &self.batch,
            self.max_table_sync_workers,
            self.table_error_retry_max_attempts,
            self.max_copy_connections_per_table,
            self.memory_backpressure.as_ref(),
            self.memory_refresh_interval_ms,
            self.table_sync_monitor_refresh_interval_ms,
        )?;
        self.table_sync_copy.validate()
    }
}

/// Validates pipeline settings shared by secret and without-secret configs.
fn validate_pipeline_settings(
    batch: &BatchConfig,
    max_table_sync_workers: u16,
    table_error_retry_max_attempts: u32,
    max_copy_connections_per_table: u16,
    memory_backpressure: Option<&MemoryBackpressureConfig>,
    memory_refresh_interval_ms: u64,
    table_sync_monitor_refresh_interval_ms: u64,
) -> Result<(), ValidationError> {
    batch.validate()?;

    if max_table_sync_workers == 0 {
        return Err(ValidationError::InvalidFieldValue {
            field: "max_table_sync_workers".to_owned(),
            constraint: "must be greater than 0".to_owned(),
        });
    }

    if table_error_retry_max_attempts == 0 {
        return Err(ValidationError::InvalidFieldValue {
            field: "table_error_retry_max_attempts".to_owned(),
            constraint: "must be greater than 0".to_owned(),
        });
    }

    if max_copy_connections_per_table == 0 {
        return Err(ValidationError::InvalidFieldValue {
            field: "max_copy_connections_per_table".to_owned(),
            constraint: "must be greater than 0".to_owned(),
        });
    }

    if let Some(memory_backpressure) = memory_backpressure {
        memory_backpressure.validate()?;
    }

    if memory_refresh_interval_ms == 0 {
        return Err(ValidationError::InvalidFieldValue {
            field: "memory_refresh_interval_ms".to_owned(),
            constraint: "must be greater than 0".to_owned(),
        });
    }

    if table_sync_monitor_refresh_interval_ms == 0 {
        return Err(ValidationError::InvalidFieldValue {
            field: "table_sync_monitor_refresh_interval_ms".to_owned(),
            constraint: "must be greater than 0".to_owned(),
        });
    }

    Ok(())
}

const fn default_table_error_retry_delay_ms() -> u64 {
    PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS
}

const fn default_table_error_retry_max_attempts() -> u32 {
    PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS
}

const fn default_max_table_sync_workers() -> u16 {
    PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS
}

const fn default_max_copy_connections_per_table() -> u16 {
    PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE
}

const fn default_memory_refresh_interval_ms() -> u64 {
    PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS
}

const fn default_table_sync_monitor_refresh_interval_ms() -> u64 {
    PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS
}

fn default_memory_backpressure() -> Option<MemoryBackpressureConfig> {
    Some(MemoryBackpressureConfig::default())
}

const fn default_run_source_migrations() -> bool {
    true
}

/// Same as [`PipelineConfig`] but without secrets. This type
/// implements [`Serialize`] because it does not contains secrets
/// so is safe to serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineConfigWithoutSecrets {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of
    /// replication slots and state store.
    pub id: u64,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
    /// The connection configuration for the Postgres instance to which the
    /// pipeline connects for replication.
    pub pg_connection: PgConnectionConfigWithoutSecrets,
    /// Optional Postgres connection configuration for pipeline state storage.
    ///
    /// When `None`, state storage uses [`Self::pg_connection`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_pg_connection: Option<PgConnectionConfigWithoutSecrets>,
    /// Configuration for the pipeline's logical replication slots.
    #[serde(default)]
    pub replication_slot: ReplicationSlotConfig,
    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,
    /// Number of milliseconds between one retry and another for timed worker
    /// retries.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_delay_ms")]
    pub table_error_retry_delay_ms: u64,
    /// Maximum number of automatic timed retry attempts before failing the
    /// worker.
    ///
    /// This setting is shared by table sync and apply workers.
    #[serde(default = "default_table_error_retry_max_attempts")]
    pub table_error_retry_max_attempts: u32,
    /// Maximum number of table sync workers that can run at a time
    #[serde(default = "default_max_table_sync_workers")]
    pub max_table_sync_workers: u16,
    /// Maximum worker connections per table during initial copy.
    ///
    /// Initial copy always uses ctid range work items, including when this is
    /// set to 1. ETL may plan more ctid ranges than worker connections so
    /// workers can pull new ranges as they finish.
    #[serde(default = "default_max_copy_connections_per_table")]
    pub max_copy_connections_per_table: u16,
    /// Number of milliseconds between one memory usage refresh and another.
    #[serde(default = "default_memory_refresh_interval_ms")]
    pub memory_refresh_interval_ms: u64,
    /// Optional memory-based backpressure configuration.
    ///
    /// `None` disables memory backpressure. When omitted, this defaults to
    /// `Some(MemoryBackpressureConfig::default())`.
    #[serde(default = "default_memory_backpressure")]
    pub memory_backpressure: Option<MemoryBackpressureConfig>,
    /// Selection rules for tables participating in replication.
    #[serde(default)]
    pub table_sync_copy: TableSyncCopyConfig,
    /// Number of milliseconds between periodic table sync monitor checks. See
    /// the field of the same name on [`PipelineConfig`].
    #[serde(default = "default_table_sync_monitor_refresh_interval_ms")]
    pub table_sync_monitor_refresh_interval_ms: u64,
    /// Behavior when the main replication slot is found to be invalidated.
    #[serde(default)]
    pub invalidated_slot_behavior: InvalidatedSlotBehavior,
    /// Whether [`Pipeline::start`] should run the source migrations. See the
    /// field of the same name on [`PipelineConfig`].
    #[serde(default = "default_run_source_migrations")]
    pub run_source_migrations: bool,
}

impl Validate for PipelineConfigWithoutSecrets {
    /// Validates pipeline configuration settings.
    fn validate(&self) -> Result<(), ValidationError> {
        validate_pipeline_settings(
            &self.batch,
            self.max_table_sync_workers,
            self.table_error_retry_max_attempts,
            self.max_copy_connections_per_table,
            self.memory_backpressure.as_ref(),
            self.memory_refresh_interval_ms,
            self.table_sync_monitor_refresh_interval_ms,
        )?;
        self.table_sync_copy.validate()
    }
}

impl From<PipelineConfig> for PipelineConfigWithoutSecrets {
    fn from(value: PipelineConfig) -> Self {
        PipelineConfigWithoutSecrets {
            id: value.id,
            publication_name: value.publication_name,
            pg_connection: value.pg_connection.into(),
            store_pg_connection: value.store_pg_connection.map(Into::into),
            replication_slot: value.replication_slot,
            batch: value.batch,
            table_error_retry_delay_ms: value.table_error_retry_delay_ms,
            table_error_retry_max_attempts: value.table_error_retry_max_attempts,
            max_table_sync_workers: value.max_table_sync_workers,
            max_copy_connections_per_table: value.max_copy_connections_per_table,
            memory_refresh_interval_ms: value.memory_refresh_interval_ms,
            memory_backpressure: value.memory_backpressure,
            table_sync_copy: value.table_sync_copy,
            table_sync_monitor_refresh_interval_ms: value.table_sync_monitor_refresh_interval_ms,
            invalidated_slot_behavior: value.invalidated_slot_behavior,
            run_source_migrations: value.run_source_migrations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::{TcpKeepaliveConfig, TlsConfig};

    fn pg_connection(host: &str, port: u16) -> PgConnectionConfig {
        PgConnectionConfig {
            host: host.to_owned(),
            hostaddr: None,
            port,
            name: "postgres".to_owned(),
            username: "postgres".to_owned(),
            password: None,
            tls: TlsConfig::disabled(),
            keepalive: TcpKeepaliveConfig::default(),
        }
    }

    #[test]
    fn batch_config_deserializes_defaults_overrides_and_unknown_fields() {
        for (json, expected_max_bytes) in [
            (r#"{"max_fill_ms":5000,"memory_budget_ratio":0.2}"#, BatchConfig::DEFAULT_MAX_BYTES),
            (
                r#"{"max_fill_ms":5000,"memory_budget_ratio":0.2,"max_bytes":4194304}"#,
                4 * 1024 * 1024,
            ),
            (
                r#"{"max_fill_ms":5000,"memory_budget_ratio":0.2,"max_bytes":4194304,"future_field":true}"#,
                4 * 1024 * 1024,
            ),
        ] {
            let config: BatchConfig = serde_json::from_str(json).unwrap();

            assert_eq!(config.max_fill_ms, 5000);
            assert_eq!(config.memory_budget_ratio, 0.2);
            assert_eq!(config.max_bytes, expected_max_bytes);
            config.validate().unwrap();
        }
    }

    #[test]
    fn pipeline_config_deserializes_missing_run_source_migrations_as_true() {
        let json = r#"{
            "id": 1,
            "publication_name": "publication",
            "pg_connection": {
                "host": "localhost",
                "hostaddr": null,
                "port": 5432,
                "name": "postgres",
                "username": "postgres",
                "password": null,
                "tls": {
                    "trusted_root_certs": "",
                    "enabled": false
                }
            }
        }"#;

        let config: PipelineConfig = serde_json::from_str(json).unwrap();

        assert!(config.run_source_migrations);
        assert_eq!(config.replication_slot, ReplicationSlotConfig::default());
    }

    #[test]
    fn pipeline_config_deserializes_run_source_migrations_false() {
        let json = r#"{
            "id": 1,
            "publication_name": "publication",
            "pg_connection": {
                "host": "localhost",
                "hostaddr": null,
                "port": 5432,
                "name": "postgres",
                "username": "postgres",
                "password": null,
                "tls": {
                    "trusted_root_certs": "",
                    "enabled": false
                }
            },
            "replication_slot": {
                "failover": true
            },
            "run_source_migrations": false
        }"#;

        let config: PipelineConfig = serde_json::from_str(json).unwrap();

        assert!(!config.run_source_migrations);
        assert!(config.replication_slot.failover);

        let without_secrets = PipelineConfigWithoutSecrets::from(config);
        assert!(!without_secrets.run_source_migrations);
        assert!(without_secrets.replication_slot.failover);
    }

    #[test]
    fn table_sync_copy_serialization_roundtrips_variants() {
        for selection in [
            TableSyncCopyConfig::SkipAllTables,
            TableSyncCopyConfig::IncludeTables { table_ids: vec![1, 2, 3] },
            TableSyncCopyConfig::SkipTables { table_ids: vec![4, 5] },
        ] {
            let json = serde_json::to_string(&selection).unwrap();
            let decoded: TableSyncCopyConfig = serde_json::from_str(&json).unwrap();

            assert_eq!(selection, decoded);
        }
    }

    #[test]
    fn table_sync_copy_validate_accepts_unique_table_ids() {
        TableSyncCopyConfig::IncludeAllTables.validate().unwrap();
        TableSyncCopyConfig::SkipAllTables.validate().unwrap();
        TableSyncCopyConfig::IncludeTables { table_ids: vec![1, 2, 3] }.validate().unwrap();
        TableSyncCopyConfig::SkipTables { table_ids: vec![4, 5] }.validate().unwrap();
    }

    #[test]
    fn table_sync_copy_validate_rejects_duplicate_table_ids() {
        TableSyncCopyConfig::IncludeTables { table_ids: vec![1, 2, 1] }.validate().unwrap_err();
        TableSyncCopyConfig::SkipTables { table_ids: vec![4, 4] }.validate().unwrap_err();
    }

    #[test]
    fn pipeline_config_store_pg_connection_defaults_to_pg_connection() {
        let pg_connection = pg_connection("replica.local", 5432);
        let config = PipelineConfig {
            id: 1,
            publication_name: "publication".to_owned(),
            pg_connection,
            store_pg_connection: None,
            replication_slot: ReplicationSlotConfig::default(),
            batch: BatchConfig::default(),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::default(),
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            run_source_migrations: true,
        };

        assert_eq!(config.store_pg_connection().host, "replica.local");
        assert_eq!(config.store_pg_connection().port, 5432);
    }

    #[test]
    fn pipeline_config_store_pg_connection_uses_override() {
        let config = PipelineConfig {
            id: 1,
            publication_name: "publication".to_owned(),
            pg_connection: pg_connection("replica.local", 5432),
            store_pg_connection: Some(pg_connection("primary.local", 6432)),
            replication_slot: ReplicationSlotConfig::default(),
            batch: BatchConfig::default(),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::default(),
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            run_source_migrations: true,
        };

        assert_eq!(config.store_pg_connection().host, "primary.local");
        assert_eq!(config.store_pg_connection().port, 6432);
    }
}
