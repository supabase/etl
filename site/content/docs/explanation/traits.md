---
title: Extension Points
description: Traits you implement to customize Supabase ETL behavior.
icon: Puzzle
---

Implement these traits to control where replicated data goes and how ETL state
is stored.

## Destination

Receives replicated data. This is the **primary extension point** for sending data to custom systems. ETL is **at least once**, so destinations must tolerate duplicate writes and concurrent calls.

```rust
pub trait Destination {
    fn name() -> &'static str;
    fn shutdown(&self) -> impl Future<Output = EtlResult<()>> + Send { async { Ok(()) } }
    fn startup(&self) -> impl Future<Output = EtlResult<()>> + Send { async { Ok(()) } }
    fn drop_table_for_copy(&self, replicated_table_schema: &ReplicatedTableSchema, async_result: DropTableForCopyResult<()>) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_table_rows(&self, replicated_table_schema: &ReplicatedTableSchema, batch_id: Option<TableCopyBatchId>, table_rows: Vec<TableRow>, async_result: WriteTableRowsResult) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_events(&self, events: Vec<Event>, durability: WriteEventsDurability, async_result: WriteEventsResult) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `name()` | Returns identifier for logging and diagnostics |
| `shutdown()` | Called when the pipeline shuts down. Default is a no-op. Override for cleanup or bookkeeping |
| `startup()` | Called after store caches are loaded, removed-publication tables are purged, and before workers start. Default is a no-op. Override to recover pending operations or rebuild process-local state |
| `drop_table_for_copy()` | Drops the existing destination object and destination-private replay state before restarting a table copy. Receives the previously stored replicated schema for locating the old object |
| `write_table_rows()` | Writes rows during initial table copy. Receives the current replicated schema, an optional batch ID, and the rows |
| `write_events()` | Processes ongoing replication events (inserts, updates, deletes, truncates, relations, and transaction markers). Batches may span multiple tables, or be empty for a required durability barrier |

### Implementation Notes

- `drop_table_for_copy()` should be **idempotent**. ETL calls it before clearing copy-scoped store state, so implementations can still use the supplied schema and existing destination metadata to locate the old object. Before returning success, it must also drain or invalidate writes accepted by an earlier copy attempt so stale work cannot mutate the recreated table.
- Nonempty `write_table_rows()` calls carry `Some(batch_id)`. ETL also calls the method with `None` and an empty row vector for empty source tables and as the terminal durability barrier when required.
- An immediate `write_table_rows()` implementation returns `DestinationWriteStatus::Durable` after the batch is durable. A deferred implementation may return `Accepted` after taking ownership of a batch. It must bound its accepted-but-not-durable backlog and delay `Accepted` when no capacity is available. If any batch returns `Accepted`, ETL sends the empty terminal call after all copy workers finish; the destination must return `Durable` from that call only after all rows accepted during the current copy attempt are durable.
- One table-copy attempt is one execution against one source snapshot. ETL abandons an interrupted attempt instead of resuming its batches; a restart creates a new snapshot and attempt ID.
- A `TableCopyBatchId` combines the fresh attempt ID with an attempt-local `u64` sequence allocated across all parallel copy workers. A redelivered batch retains its ID and distinct batches retain distinct IDs, including across attempts and when their rows are identical. The sequence does not define source order, and the complete ID is an idempotency key rather than a source-resume cursor.
- `WriteEventsDurability::MayDefer` permits `write_events()` to return `Accepted` or `Durable`. ETL may issue `write_events(Vec::new(), WriteEventsDurability::RequireDurable, ...)` as a durability-only barrier, but never an empty `MayDefer` write. The empty vector carries no new replication events, but the call may flush or wait for earlier accepted work and must return `Durable` only after all writes covered by the destination's ordering state are durable. A destination may use a stronger barrier scope than the originating apply-loop stream.
- `write_table_rows()` and `write_events()` must tolerate **duplicate delivery** because ETL may retry or replay after failure.
- Handle **concurrent calls** safely, especially from parallel table sync workers.
- Preserve **per-table event order**. During initial sync and catch-up, transaction markers are not a reliable all-tables transaction boundary.
- Treat `Event::Relation` as an ordered schema transition, not a `write_events()` batch boundary. ETL batches ongoing replication events by size and time, so one call can contain multiple schema changes, including multiple relation events for the same table.
- Always complete the supplied async result handle. Dropping it reports a destination error to ETL.
- `startup()` runs after ETL has loaded destination metadata and table schemas from the store and purged tables removed from the publication. It may recover `Creating` or destination-recoverable `Applying` operations and rebuild process-local state. It should trust `Applied` metadata rather than recreate or structurally repair the data-bearing destination table; read-only destination metadata calls are appropriate only when indispensable to rebuild local write state or fail fast.
- All three write-like methods use async results, but ETL waits differently. `drop_table_for_copy()` waits immediately before copy-scoped store cleanup. `write_table_rows()` also waits immediately, requesting the next batch only after the current one reports `Accepted` or `Durable` for that copy partition. `write_events()` is the method where ETL can keep processing other work while the destination finishes the current batch; ETL still waits for that result before handing over the next ongoing-replication batch.

See [Events](/explanation/events/) for details on the events received by `write_events()`.

`PipelineDestination` is a blanket-implemented facade for destinations that
also satisfy the pipeline runtime clone and thread-safety bounds. Pipeline
runtime code uses this facade when it needs to move destinations across worker
tasks, but custom destinations only implement `Destination` directly.

## SchemaStore

Stores **versioned table schema information** (column names, types, primary keys,
and snapshot IDs). A `SnapshotId` compares its commit LSN first and its message
LSN second; store implementations should compare the type directly rather than
its variable-width decimal display string.

```rust
pub trait SchemaStore {
    fn get_table_schema(&self, table_id: &TableId, snapshot_id: SnapshotId) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;
    fn load_table_schemas(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn store_table_schema(&self, table_schema: TableSchema) -> impl Future<Output = EtlResult<Arc<TableSchema>>> + Send;
    fn prune_table_schemas(&self, retention_snapshot_ids: BTreeMap<TableId, SnapshotId>) -> impl Future<Output = EtlResult<u64>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `get_table_schema()` | Returns the schema version with the largest `snapshot_id <= requested_snapshot_id`. If it misses cache, it may load from persistent storage |
| `get_table_schemas()` | Returns all cached schemas without reading persistent storage |
| `load_table_schemas()` | Loads schemas from persistent storage into cache. Call once at startup. Returns the number of schemas loaded |
| `store_table_schema()` | Saves a schema version to both cache and persistent storage and returns the cached `Arc` |
| `prune_table_schemas()` | For the supplied per-table snapshot boundaries, preserves the newest schema version at or before each boundary, preserves newer versions, and removes older versions. The `BTreeMap` provides deterministic table-ID iteration. Implementations with both cache and persistent storage must prune both |

## StateStore

Tracks **table states**, **persisted replication checkpoints**, and
**destination table metadata**.

```rust
pub trait StateStore {
    // Table state
    fn get_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<TableState>>> + Send;
    fn get_table_states(&self) -> impl Future<Output = EtlResult<TableStates>> + Send;
    fn load_table_states(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> impl Future<Output = EtlResult<()>> + Send;
    fn update_table_state(&self, table_id: TableId, state: TableState) -> impl Future<Output = EtlResult<()>> + Send;
    fn rollback_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<TableState>> + Send;

    // Persisted replication checkpoints
    fn get_replication_checkpoint(&self, worker_type: WorkerType) -> impl Future<Output = EtlResult<Option<PgLsn>>> + Send;
    fn upsert_replication_checkpoint(&self, worker_type: WorkerType, checkpoint_lsn: PgLsn) -> impl Future<Output = EtlResult<PgLsn>> + Send;
    fn delete_replication_checkpoint(&self, worker_type: WorkerType) -> impl Future<Output = EtlResult<()>> + Send;

    // Destination table metadata
    fn get_destination_table_metadata(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<DestinationTableMetadata>>> + Send;
    fn load_destination_tables_metadata(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn store_destination_table_metadata(&self, table_id: TableId, metadata: DestinationTableMetadata) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Table State Methods

| Method | Purpose |
|--------|---------|
| `get_table_state()` | Returns current state for a table from cache |
| `get_table_states()` | Returns states for all tables from cache as [`TableStates`] |
| `load_table_states()` | Loads states from persistent storage into cache. Call once at startup. Returns the number of states loaded |
| `update_table_states()` | Atomically updates multiple table states in both cache and persistent storage |
| `update_table_state()` | Updates state in both cache and persistent storage |
| `rollback_table_state()` | Reverts table to previous state. Returns the state after rollback |

### Replication Checkpoint Methods

A persisted replication checkpoint records a safe replay frontier for the apply
worker or a table-sync worker. ETL can select a durably flushed commit boundary
or, when the apply loop is fully idle, its last received LSN. The checkpoint
lets the worker resume safely after a restart.

| Method | Purpose |
|--------|---------|
| `get_replication_checkpoint()` | Returns the persisted checkpoint for a worker, if present |
| `upsert_replication_checkpoint()` | Monotonically stores a checkpoint and returns the stored LSN. Implementations must not move it backward |
| `delete_replication_checkpoint()` | Deletes the checkpoint when a worker slot lineage is intentionally reset |

### Destination Metadata Methods

Destination table metadata connects source table IDs to destination state. Its schema is explicitly `Creating`, `Applying`, or `Applied`; each variant contains the snapshots and replication masks required for that state. Destinations match the schema variant and decide whether to recover or reject an incomplete operation. `Applied` is authoritative: an empty process cache may be repopulated from it, but must not cause the data-bearing table to be created, inspected for repair, or structurally reconciled. External changes to ETL-owned tables are unsupported and should fail through ordinary destination operations.

| Method | Purpose |
|--------|---------|
| `get_destination_table_metadata()` | Returns destination table metadata for a source table from cache |
| `load_destination_tables_metadata()` | Loads destination table metadata from persistent storage into cache. Call once during startup |
| `store_destination_table_metadata()` | Saves destination table metadata to both cache and persistent storage |

### Table States

Tables progress through these states:

| State | Persisted | Description |
|-------|-----------|-------------|
| `Init` | Yes | Table discovered, ready to start |
| `DataSync` | Yes | Initial data being copied |
| `FinishedCopy` | Yes | Copy complete, waiting for coordination |
| `SyncWait` | No | Table sync worker signaling apply worker to pause |
| `Catchup { lsn }` | No | Apply worker paused, table sync worker catching up to LSN |
| `SyncDone { lsn }` | Yes | Caught up to LSN; durable decoder retained until Apply materializes local state |
| `Ready` | Yes | Changes via apply worker |
| `Errored { reason, solution, retry_policy }` | Yes | Error occurred, excluded until rollback |

## TableStateLifecycleStore

Coordinates ETL table-state lifecycle operations across state, schema,
destination metadata, persisted checkpoints, and any store caches.

```rust
pub trait TableStateLifecycleStore {
    fn apply_table_state_operation(
        &self,
        operation: TableStateOperation,
    ) -> impl Future<Output = EtlResult<usize>> + Send;

    fn prepare_table_state_for_copy(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    fn reset_table_states_for_resync(
        &self,
    ) -> impl Future<Output = EtlResult<usize>> + Send;

    fn delete_table_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
```

| Method | Purpose |
|--------|---------|
| `apply_table_state_operation()` | Single implementation point for [`TableStateOperation`]. Custom stores implement the prepare, reset, and delete semantics here |
| `prepare_table_state_for_copy()` | Deletes destination metadata, schema versions, and the table-sync checkpoint while preserving the table state. This is called only after the destination object was dropped for a fresh copy |
| `reset_table_states_for_resync()` | Resets all current table states to `Init` and deletes the apply-worker checkpoint while preserving destination metadata, schema versions, and table-sync checkpoints |
| `delete_table_state()` | Deletes all stored ETL-owned state for a table removed from the publication. Does not modify destination tables |

## Combining Traits

A single type typically implements **all store traits**:

```rust
pub struct MyStore { /* ... */ }

impl SchemaStore for MyStore { /* ... */ }
impl StateStore for MyStore { /* ... */ }
impl TableStateLifecycleStore for MyStore { /* ... */ }
```

`PipelineStore` is a blanket-implemented facade for stores that satisfy the
full pipeline runtime store bounds. Pipeline runtime code uses this facade,
while code that only needs one capability should depend on the narrower trait
directly.

`DestinationStore` is a blanket-implemented facade for stores that satisfy the
destination runtime store bounds. Destination implementations use this when
they need schema and state metadata but do not need lifecycle reset/removal
operations. `SharedStateStore` covers state-only users with the corresponding
worker-safe bounds.

ETL provides two built-in implementations:

- `MemoryStore`: In-memory storage, not persistent across restarts
- `PostgresStore`: Persistent storage backed by PostgreSQL

`PostgresStore::new()` runs only the Postgres-backed state-store migrations.
`Pipeline::start()` runs the source migrations required by ETL itself, including
the schema helper functions and DDL event trigger, regardless of which store
implementation you use.

## Thread Safety

All trait implementations must be **thread-safe**. ETL calls these methods concurrently from:

- Multiple table sync workers (parallel initial sync)
- Apply worker (ongoing replication)
- Pipeline coordination

Use `Arc<Mutex<_>>`, `RwLock`, or similar synchronization primitives for shared state.
