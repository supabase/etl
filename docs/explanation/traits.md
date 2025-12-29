# Extension Points

**Traits you implement to customize ETL behavior**

ETL provides four traits for customization. Implement these to control where data goes and how state is stored.

## Destination

Receives replicated data. This is the primary extension point for sending data to custom systems.

```rust
pub trait Destination {
    fn name() -> &'static str;
    fn truncate_table(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_table_rows(&self, table_id: TableId, table_rows: Vec<TableRow>) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `name()` | Returns identifier for logging and diagnostics |
| `truncate_table()` | Clears table data before initial sync. Called unconditionally, even if the table does not exist |
| `write_table_rows()` | Writes rows during initial table copy. May receive an empty vector for tables with no data |
| `write_events()` | Processes streaming replication events (inserts, updates, deletes). Batches may span multiple tables |

### Implementation Notes

- Operations should be idempotent when possible (ETL may retry on failure)
- Handle concurrent calls safely (parallel table sync workers)
- Process events in order to maintain data consistency

See [Event Types](events.md) for details on the events received by `write_events()`.

## SchemaStore

Stores table schema information (column names, types, primary keys).

```rust
pub trait SchemaStore {
    fn get_table_schema(&self, table_id: &TableId) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;
    fn load_table_schemas(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn store_table_schema(&self, table_schema: TableSchema) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `get_table_schema()` | Returns schema for a table from cache. Does not load from persistent storage |
| `get_table_schemas()` | Returns all cached schemas |
| `load_table_schemas()` | Loads schemas from persistent storage into cache. Call once at startup. Returns the number of schemas loaded |
| `store_table_schema()` | Saves schema to both cache and persistent storage |

## StateStore

Tracks replication progress and table mappings.

```rust
pub trait StateStore {
    // Replication state
    fn get_table_replication_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<Option<TableReplicationPhase>>> + Send;
    fn get_table_replication_states(&self) -> impl Future<Output = EtlResult<HashMap<TableId, TableReplicationPhase>>> + Send;
    fn load_table_replication_states(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn update_table_replication_state(&self, table_id: TableId, state: TableReplicationPhase) -> impl Future<Output = EtlResult<()>> + Send;
    fn rollback_table_replication_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<TableReplicationPhase>> + Send;

    // Table mappings
    fn get_table_mapping(&self, source_table_id: &TableId) -> impl Future<Output = EtlResult<Option<String>>> + Send;
    fn get_table_mappings(&self) -> impl Future<Output = EtlResult<HashMap<TableId, String>>> + Send;
    fn load_table_mappings(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn store_table_mapping(&self, source_table_id: TableId, destination_table_id: String) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Replication State Methods

| Method | Purpose |
|--------|---------|
| `get_table_replication_state()` | Returns current phase for a table from cache |
| `get_table_replication_states()` | Returns phases for all tables from cache |
| `load_table_replication_states()` | Loads phases from persistent storage into cache. Call once at startup. Returns the number of states loaded |
| `update_table_replication_state()` | Updates phase in both cache and persistent storage |
| `rollback_table_replication_state()` | Reverts table to previous phase. Returns the phase after rollback |

### Table Mapping Methods

Table mappings connect source table IDs to destination table names.

| Method | Purpose |
|--------|---------|
| `get_table_mapping()` | Returns destination table name for a source table from cache |
| `get_table_mappings()` | Returns all mappings from cache |
| `load_table_mappings()` | Loads mappings from persistent storage into cache. Can be called lazily when needed |
| `store_table_mapping()` | Saves mapping to both cache and persistent storage |

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

Removes ETL metadata when tables are removed from the publication.

```rust
pub trait CleanupStore {
    fn cleanup_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
}
```

| Method | Purpose |
|--------|---------|
| `cleanup_table_state()` | Deletes all stored state for a table: replication state, schema, and mappings. Does not modify destination tables |

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
