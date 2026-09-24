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
| `shutdown()` | Called during controlled pipeline teardown after workers complete successfully. Default is a no-op. Override to finish owned background work and release resources |
| `startup()` | Called after store caches are loaded, removed-publication tables are purged, and before workers start. Default is a no-op. Override to recover pending operations or rebuild process-local state |
| `drop_table_for_copy()` | Drops the existing destination object and destination-private replay state before restarting a table copy. Receives the previously stored replicated schema for locating the old object |
| `write_table_rows()` | Writes rows during initial table copy. Receives the current replicated schema, an optional batch ID, and the rows |
| `write_events()` | Processes catch-up and ongoing replication events (inserts, updates, deletes, truncates, relations, and transaction markers). Batches may span multiple tables, or be empty for a required durability barrier |

### Implementation Notes

- `drop_table_for_copy()` should be **idempotent**. ETL calls it before clearing copy-scoped store state, so implementations can still use the supplied schema and existing destination metadata to locate the old object. Before returning success, it must also drain or invalidate writes accepted by an earlier copy attempt so stale work cannot mutate the recreated table.
- Nonempty `write_table_rows()` calls carry `Some(batch_id)`. ETL also calls the method with `None` and an empty row vector for empty source tables and as the terminal durability barrier when required.
- An immediate `write_table_rows()` implementation returns `DestinationWriteStatus::Durable` after the batch is durable. A deferred implementation may return `Accepted` after taking ownership of a batch. It must bound its accepted-but-not-durable backlog and delay `Accepted` when no capacity is available. If any batch returns `Accepted`, ETL sends the empty terminal call after all copy workers finish; the destination must return `Durable` from that call only after all rows accepted during the current copy attempt are durable.
- One table-copy attempt is one execution against one source snapshot. ETL can cancel the `write_table_rows()` method or its result wait, including the final empty call. It abandons the interrupted attempt instead of resuming its batches; a restart creates a new snapshot and attempt ID. Cancellation does not undo remote writes or stop offloaded work, so the destination still owns that work and must fence it before completing `drop_table_for_copy()`.
- A `TableCopyBatchId` combines the fresh attempt ID with an attempt-local `u64` sequence allocated across all parallel copy workers. A redelivered batch retains its ID and distinct batches retain distinct IDs, including across attempts and when their rows are identical. The sequence does not define source order, and the complete ID is an idempotency key rather than a source-resume cursor.
- `WriteEventsDurability::MayDefer` permits `write_events()` to return `Accepted` or `Durable`. ETL may issue `write_events(Vec::new(), WriteEventsDurability::RequireDurable, ...)` as a durability-only barrier, but never an empty `MayDefer` write. The empty vector carries no new replication events, but the call may flush or wait for earlier accepted work and must return `Durable` only after all writes covered by the destination's ordering state are durable. A destination may use a stronger barrier scope than the originating apply-loop stream.
- `write_table_rows()` and `write_events()` must tolerate **duplicate delivery** because ETL may retry or replay after failure.
- Handle **concurrent calls** safely, especially from parallel table sync workers.
- Preserve **per-table event order**. During initial sync and catch-up, transaction markers are not a reliable all-tables transaction boundary.
- Treat `Event::Relation` as an ordered schema transition, not a `write_events()` batch boundary. ETL batches ongoing replication events by size and time, so one call can contain multiple schema changes, including multiple relation events for the same table.
- Always complete the supplied async result handle. Dropping it reports a destination error to ETL.
- `startup()` runs after ETL has loaded destination metadata and table schemas from the store and purged tables removed from the publication. It may recover `Creating` or destination-recoverable `Applying` operations and rebuild process-local state. It should trust `Applied` metadata rather than recreate or structurally repair the data-bearing destination table; read-only destination metadata calls are appropriate only when indispensable to rebuild local write state or fail fast.
- All three write-like methods use async results, but ETL awaits each method call before it can observe that result. `write_events()` should dispatch long-running work to owned tasks or queues and return promptly: an inline write stalls the apply loop, including its shutdown handling. After dispatch, the loop can process WAL while the result is pending, but it waits for that result before dispatching another batch. Shutdown drains apply writes rather than cancelling them.
- `write_table_rows()` may perform I/O inline because copy partitions already run in separate worker tasks. Each partition waits for both the method and its result before requesting another batch; parallelism comes from other copy workers. `Accepted` permits the next batch without proving durability. `drop_table_for_copy()` waits immediately before copy-scoped store cleanup.
- Stop producers before draining destination tasks in `shutdown()`. `etl::task::TaskRegistry` supports registration, reaping, and exclusive draining. Avoid tasks capturing their registry: ownership cycles require explicit cleanup, including when worker failures skip the destination hook. `Pipeline::shutdown_and_wait()` still calls the hook after failed or cancelled startup.
- Controlled teardown joins tasks and silently accepts requested cancellations. Panics, task errors, and unexpected cancellations return immediately and drop remaining owned handles without awaiting them. This requests abort; it does not roll back remote writes or stop native work already running.

See [Events](/explanation/events/) for details on the events received by `write_events()`.

`PipelineDestination` is a blanket-implemented facade for destinations that
also satisfy the pipeline runtime clone and thread-safety bounds. Pipeline
runtime code uses this facade when it needs to move destinations across worker
tasks, but custom destinations only implement `Destination` directly.

## CachedStore

Prepares every cache owned by a store through one method. `StateStore`,
`SchemaStore`, and `TableStateLifecycleStore` require this capability as a
supertrait. Code using those traits inherits the cache contract without
repeating `+ CachedStore` on individual functions.

```rust
pub trait CachedStore {
    fn load_cache(&self) -> impl Future<Output = EtlResult<()>> + Send;
}
```

Pipeline startup calls `load_cache()` before initialization reads, destination
startup, or workers. Repeated calls refresh all domains together. Implementations
must serialize loading with mutations and publish only a complete, consistent
result. Failed or cancelled loads must not expose partial state; subsequent
access must recover or return an error. In-memory stores can return success
immediately. Wrappers forward the method to the underlying store.

`PostgresStore` loads table states, destination metadata, all retained schema
versions, and checkpoints in one transaction. It also performs this full load
when a getter or mutation encounters an uninitialized or unusable cache.
Healthy getters, including missing-entry lookups, remain memory-only.

### PostgreSQL ownership and concurrency

Use **one active `PostgresStore` and its clones per pipeline**. Clones share all
caches and one async mutex. Independent cached owners and external metadata
writes are unsupported. A replacement store can recover after the old owner
stops issuing operations, even if its last database transaction is still finishing.

The mutex covers cache reads, database mutations, and cache publication.
Lifecycle operations span several cache domains, so one mutex prevents partial
updates without additional lock ordering. Returned values remain snapshots;
separate getter calls do not form one transaction.

### Cancellation and recovery

PostgreSQL is the system of record. Each mutation:

1. Acquires the mutex and recovers an unusable cache if needed.
2. Marks the cache unusable before database awaits, leaving its contents unchanged.
3. Begins a transaction, acquires the pipeline's transaction advisory lock, and
   performs the database changes.
4. Awaits `COMMIT`, then updates the cache and marks it usable without another await.

Dropping a future releases the mutex but does not synchronously stop PostgreSQL.
A commit can succeed even if cancellation or connection loss hides its result.
The usability flag is cleared before database work because cancellation skips
normal error handling; updating the cache only on acknowledged success would
otherwise leave stale data accessible after an uncertain commit.

The next accessor reloads every domain, first acquiring the same transaction
advisory lock as mutations. This waits for the previous database outcome even
on a different connection. PostgreSQL releases the lock at transaction end.
The lock coordinates database transactions, not independently cached owners.

Reloads build a private replacement and publish it only after all queries,
conversions, and commit succeed. They also clear remembered pruning boundaries,
which an interrupted schema change may have invalidated. Failed or cancelled
reloads remain unusable. Errors, including lock timeouts, reach the caller;
later access retries the full load instead of returning stale or partial data.

### Isolation level

The store explicitly selects `READ COMMITTED`, including when the database has
a stricter default. Lock acquisition and data reads are separate statements, so
the reads see commits completed during the lock wait. `REPEATABLE READ` could
retain the snapshot established before that wait.

The advisory lock remains held across all reload queries and excludes every
participating mutation, keeping the cache domains consistent across their
statement-level snapshots. Ordinary reads do not wait on row-update locks, so
those locks alone would not protect the reload.

## SchemaStore

Stores **versioned table schema information** (column names, types, primary keys,
and snapshot IDs). A `SnapshotId` compares its commit LSN first and its message
LSN second; store implementations should compare the type directly rather than
its variable-width decimal display string.

```rust
pub trait SchemaStore: CachedStore {
    fn get_table_schema(&self, table_id: &TableId, snapshot_id: SnapshotId) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;
    fn store_table_schema(&self, table_schema: TableSchema) -> impl Future<Output = EtlResult<Arc<TableSchema>>> + Send;
    fn prune_table_schemas(&self, retention_snapshot_ids: BTreeMap<TableId, SnapshotId>) -> impl Future<Output = EtlResult<u64>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `get_table_schema()` | Returns the newest cached schema at or before the requested snapshot, or `None` |
| `get_table_schemas()` | Returns all cached schemas, recovering an unusable cache when needed |
| `store_table_schema()` | Saves a schema version to both cache and persistent storage and returns the cached `Arc` |
| `prune_table_schemas()` | Keeps the newest schema at or before each table's boundary and all newer versions; removes older versions from storage and cache |

Stores can skip completed cleanup boundaries until schema writes or lifecycle
changes require another pass.

## StateStore

Tracks **table states**, **persisted replication checkpoints**, and
**destination table metadata**.

```rust
pub trait StateStore: CachedStore {
    // Table state
    fn get_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<TableState>>> + Send;
    fn get_table_states(&self) -> impl Future<Output = EtlResult<TableStates>> + Send;
    fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> impl Future<Output = EtlResult<()>> + Send;
    fn update_table_state(&self, table_id: TableId, state: TableState) -> impl Future<Output = EtlResult<()>> + Send;
    fn rollback_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<TableState>> + Send;

    // Persisted replication checkpoints
    fn get_replication_checkpoint(&self, worker_type: WorkerType) -> impl Future<Output = EtlResult<Option<PgLsn>>> + Send;
    fn upsert_replication_checkpoint(&self, worker_type: WorkerType, checkpoint_lsn: PgLsn) -> impl Future<Output = EtlResult<PgLsn>> + Send;
    fn delete_replication_checkpoint(&self, worker_type: WorkerType) -> impl Future<Output = EtlResult<()>> + Send;

    // Destination table metadata
    fn get_destination_table_metadata(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<DestinationTableMetadata>>> + Send;
    fn store_destination_table_metadata(&self, table_id: TableId, metadata: DestinationTableMetadata) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Table State Methods

| Method | Purpose |
|--------|---------|
| `get_table_state()` | Returns current state for a table from cache |
| `get_table_states()` | Returns states for all tables from cache as [`TableStates`] |
| `update_table_states()` | Persists table-state updates atomically, then updates the cache |
| `update_table_state()` | Updates state in both cache and persistent storage |
| `rollback_table_state()` | Reverts table to previous state. Returns the state after rollback |

### Replication Checkpoint Methods

A persisted replication checkpoint records a safe replay frontier for the apply
worker or a table-sync worker. ETL saves progress at commit boundaries after
the corresponding destination work is durable. PostgreSQL slot feedback
can also advance when the loop is fully idle, but that feedback is distinct
from the checkpoint saved in the store. The persisted checkpoint participates
in selecting a safe restart position.

| Method | Purpose |
|--------|---------|
| `get_replication_checkpoint()` | Returns the cached checkpoint for a worker, or `None`, recovering an unusable cache when needed |
| `upsert_replication_checkpoint()` | Monotonically persists a checkpoint, then caches and returns the actual stored LSN. It can be higher than the requested LSN; failed writes must not advance the cache |
| `delete_replication_checkpoint()` | Deletes the persisted and cached checkpoint when a worker slot lineage is intentionally reset |

Resets remove cached checkpoints only after persistence succeeds. Interrupted
mutations must leave the cache unavailable until its durable state is recovered.

### Destination Metadata Methods

Destination table metadata connects source table IDs to destination state. Its schema is explicitly `Creating`, `Applying`, or `Applied`; each variant contains the snapshots and replication masks required for that state. Destinations match the schema variant and decide whether to recover or reject an incomplete operation. `Applied` is authoritative: an empty process cache may be repopulated from it, but must not cause the data-bearing table to be created, inspected for repair, or structurally reconciled. External changes to ETL-owned tables are unsupported.

| Method | Purpose |
|--------|---------|
| `get_destination_table_metadata()` | Returns destination table metadata for a source table from cache |
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
| `SyncDone { lsn }` | Yes | Caught up to LSN; awaiting a persisted apply checkpoint and local decoder before handover |
| `Ready` | Yes | Changes via apply worker |
| `Errored { reason, solution, retry_policy }` | Yes | Error occurred, excluded until rollback |

## TableStateLifecycleStore

Coordinates ETL table-state lifecycle operations across state, schema,
destination metadata, persisted checkpoints, and any store caches.

```rust
pub trait TableStateLifecycleStore: CachedStore {
    fn apply_table_state_operation(
        &self,
        operation: TableStateOperation,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    fn prepare_table_state_for_copy(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    fn reset_table_states_for_resync(
        &self,
    ) -> impl Future<Output = EtlResult<()>> + Send;

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

impl CachedStore for MyStore { /* ... */ }
impl SchemaStore for MyStore { /* ... */ }
impl StateStore for MyStore { /* ... */ }
impl TableStateLifecycleStore for MyStore { /* ... */ }
```

The runtime traits compose these capabilities and have blanket implementations:

| Trait | Adds |
| --- | --- |
| `SharedStateStore` | `StateStore` plus `Clone + Send + Sync + 'static` for worker tasks |
| `DestinationStore` | `SchemaStore` on top of `SharedStateStore` |
| `PipelineStore` | `TableStateLifecycleStore` on top of `DestinationStore` |

Use the narrowest trait that describes the caller's role. A state-only helper
can require `StateStore` without requiring schemas, cloning, or ownership by
spawned tasks. It still inherits the cache contract. Only code specifically
preparing caches needs a standalone `CachedStore` bound.

ETL provides two built-in implementations:

- `MemoryStore`: In-memory storage, not persistent across restarts
- `PostgresStore`: Persistent storage backed by PostgreSQL

`PostgresStore::new()` runs only the Postgres-backed state-store migrations.
By default, `Pipeline::start()` runs the source migrations required by ETL
itself, including the schema helper functions and DDL event trigger, regardless
of the store implementation. With `run_source_migrations: false` or a read-only
source replica, apply those migrations on the primary before starting ETL.

## Thread Safety

All trait implementations must be **thread-safe**. ETL calls these methods concurrently from:

- Multiple table sync workers (parallel initial sync)
- Apply worker (ongoing replication)
- Pipeline coordination

Use `Arc<Mutex<_>>`, `RwLock`, or similar synchronization primitives for shared state.
