# Extension Points

**Traits you implement to customize ETL behavior**

ETL provides four traits for customization. Implement these to control where data goes and how state is stored.

## Destination

Receives replicated data. This is the primary extension point for sending data to custom systems.

```rust
pub trait Destination {
    fn name() -> &'static str;
    fn shutdown(&self) -> impl Future<Output = EtlResult<()>> + Send { async { Ok(()) } }
    fn drop_table_for_copy(&self, replicated_table_schema: &ReplicatedTableSchema, async_result: DropTableForCopyResult<()>) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_table_rows(&self, replicated_table_schema: &ReplicatedTableSchema, table_rows: Vec<TableRow>, async_result: WriteTableRowsResult<()>) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_events(&self, events: Vec<Event>, async_result: WriteEventsResult<()>) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `name()` | Returns identifier for logging and diagnostics |
| `shutdown()` | Called when the pipeline shuts down. Default is a no-op. Override for cleanup or bookkeeping |
| `drop_table_for_copy()` | Drops the existing destination object and destination-private replay state before a fresh initial copy or copy retry. Receives the current replicated schema for the table |
| `write_table_rows()` | Writes rows during initial table copy. Receives the current replicated schema and may get an empty vector for tables with no data |
| `write_events()` | Processes streaming replication events (inserts, updates, deletes). Batches may span multiple tables |

### Implementation Notes

- `drop_table_for_copy()` should be idempotent. ETL calls it before clearing copy-scoped store state, so implementations can still use the supplied schema and existing destination metadata to locate the old object.
- Other operations should be idempotent when possible (ETL may retry on failure)
- Handle concurrent calls safely (parallel table sync workers)
- Process events in order to maintain data consistency
- All three write-like methods use async results, but ETL waits differently. `drop_table_for_copy()` waits immediately before copy-scoped store cleanup. `write_table_rows()` also waits immediately, requesting the next batch only after the current one finishes for that copy partition. `write_events()` is the method where ETL can keep processing while the destination finishes the current batch.

See [Event Types](events.md) for details on the events received by `write_events()`.

## SchemaStore

Stores versioned table schema information (column names, types, primary keys,
and snapshot IDs).

```rust
pub trait SchemaStore {
    fn get_table_schema(&self, table_id: &TableId, snapshot_id: SnapshotId) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;
    fn load_table_schemas(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn store_table_schema(&self, table_schema: TableSchema) -> impl Future<Output = EtlResult<Arc<TableSchema>>> + Send;
    fn prune_table_schemas(&self, table_schema_retentions: HashMap<TableId, TableSchemaRetention>) -> impl Future<Output = EtlResult<u64>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `get_table_schema()` | Returns the schema version with the largest `snapshot_id <= requested_snapshot_id` |
| `get_table_schemas()` | Returns all cached schemas |
| `load_table_schemas()` | Loads schemas from persistent storage into cache. Call once at startup. Returns the number of schemas loaded |
| `store_table_schema()` | Saves a schema version to both cache and persistent storage and returns the cached `Arc` |
| `prune_table_schemas()` | For the supplied per-table retention boundaries, preserves the newest schema version at or before each retention LSN and removes older versions. Implementations with both cache and persistent storage must prune both |

## StateStore

Tracks replication progress and destination table metadata.

```rust
pub trait StateStore {
    // Replication state
    fn get_table_replication_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<TableReplicationPhase>>> + Send;
    fn get_table_replication_states(&self) -> impl Future<Output = EtlResult<TableReplicationStates>> + Send;
    fn load_table_replication_states(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn update_table_replication_states(&self, updates: Vec<(TableId, TableReplicationPhase)>) -> impl Future<Output = EtlResult<()>> + Send;
    fn update_table_replication_state(&self, table_id: TableId, state: TableReplicationPhase) -> impl Future<Output = EtlResult<()>> + Send;
    fn rollback_table_replication_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<TableReplicationPhase>> + Send;

    // Durable replication progress
    fn get_replication_progress(&self, worker_type: WorkerType) -> impl Future<Output = EtlResult<Option<PgLsn>>> + Send;
    fn upsert_replication_progress(&self, worker_type: WorkerType, flush_lsn: PgLsn) -> impl Future<Output = EtlResult<PgLsn>> + Send;
    fn delete_replication_progress(&self, worker_type: WorkerType) -> impl Future<Output = EtlResult<()>> + Send;

    // Destination table metadata
    fn get_destination_table_metadata(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<DestinationTableMetadata>>> + Send;
    fn get_applied_destination_table_metadata(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<AppliedDestinationTableMetadata>>> + Send;
    fn load_destination_tables_metadata(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn store_destination_table_metadata(&self, table_id: TableId, metadata: DestinationTableMetadata) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Replication State Methods

| Method | Purpose |
|--------|---------|
| `get_table_replication_state()` | Returns current phase for a table from cache |
| `get_table_replication_states()` | Returns phases for all tables from cache as [`TableReplicationStates`] |
| `load_table_replication_states()` | Loads phases from persistent storage into cache. Call once at startup. Returns the number of states loaded |
| `update_table_replication_states()` | Atomically updates multiple table phases in both cache and persistent storage |
| `update_table_replication_state()` | Updates phase in both cache and persistent storage |
| `rollback_table_replication_state()` | Reverts table to previous phase. Returns the phase after rollback |

### Durable Progress Methods

Durable replication progress records the latest flushed LSN for the apply worker
and table sync workers. It lets ETL resume from a safe boundary even when a slot
or worker restarts.

| Method | Purpose |
|--------|---------|
| `get_replication_progress()` | Returns stored flush progress for a worker, if present |
| `upsert_replication_progress()` | Monotonically stores flush progress and returns the stored LSN. Implementations must not move progress backward |
| `delete_replication_progress()` | Deletes progress when a worker slot lineage is intentionally reset |

### Destination Metadata Methods

Destination table metadata connects source table IDs to destination state, including the destination table identifier, the currently applied schema snapshot, and the replication mask.

| Method | Purpose |
|--------|---------|
| `get_destination_table_metadata()` | Returns destination table metadata for a source table from cache |
| `get_applied_destination_table_metadata()` | Returns destination table metadata only when the destination schema is fully applied |
| `load_destination_tables_metadata()` | Loads destination table metadata from persistent storage into cache |
| `store_destination_table_metadata()` | Saves destination table metadata to both cache and persistent storage |

### Table Replication Phases

Tables progress through these phases:

| Phase | Persisted | Description |
|-------|-----------|-------------|
| `Init` | Yes | Table discovered, ready to start |
| `DataSync` | Yes | Initial data being copied |
| `FinishedCopy` | Yes | Copy complete, waiting for coordination |
| `SyncWait` | No | Table sync worker signaling apply worker to pause |
| `Catchup { lsn }` | No | Apply worker paused, table sync worker catching up to LSN |
| `SyncDone { lsn }` | Yes | Caught up to LSN, ready for handoff |
| `Ready` | Yes | Streaming changes via apply worker |
| `Errored { reason, solution, retry_policy }` | Yes | Error occurred, excluded until rollback |

## CleanupStore

Removes ETL metadata for table-copy restarts and publication removals.

```rust
pub trait CleanupStore {
    fn clear_table_copy_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
    fn delete_table_pipeline_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
}
```

| Method | Purpose |
|--------|---------|
| `clear_table_copy_state()` | Clears destination metadata, schema versions, and durable table-sync progress while preserving the table replication phase. This is called only after the destination object was dropped for a fresh copy |
| `delete_table_pipeline_state()` | Deletes all stored state for a table removed from the publication. Does not modify destination tables |

## Combining Traits

A single type typically implements all store traits:

```rust
pub struct MyStore { /* ... */ }

impl SchemaStore for MyStore { /* ... */ }
impl StateStore for MyStore { /* ... */ }
impl CleanupStore for MyStore { /* ... */ }
```

ETL provides two built-in implementations:

- `MemoryStore`: In-memory storage, not persistent across restarts
- `PostgresStore`: Persistent storage backed by PostgreSQL

`PostgresStore::new()` runs only the Postgres-backed state-store migrations.
`Pipeline::start()` runs the source migrations required by ETL itself, including
the schema helper functions and DDL event trigger, regardless of which store
implementation you use.

## Thread Safety

All trait implementations must be thread-safe. ETL calls these methods concurrently from:

- Multiple table sync workers (parallel initial copy)
- Apply worker (streaming changes)
- Pipeline coordination

Use `Arc<Mutex<_>>`, `RwLock`, or similar synchronization primitives for shared state.

## Next Steps

- [Custom Stores and Destinations](../guides/custom-implementations.md): Implement these traits
- [Event Types](events.md): Events received by `write_events()`
- [Architecture](architecture.md): How these components fit together
