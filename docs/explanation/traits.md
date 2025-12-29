# Extension Points

**Traits you implement to customize ETL behavior**

ETL provides four traits for customization. Implement these to control where data goes and how state is stored.

## Destination

Receives replicated data. This is the primary extension point for sending data to custom systems.

```rust
pub trait Destination {
    fn name() -> &'static str;
    fn truncate_table(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> impl Future<Output = EtlResult<()>> + Send;
    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Methods

| Method | When Called | Purpose |
|--------|-------------|---------|
| `name()` | Startup | Return destination identifier for logging |
| `truncate_table()` | Before initial copy | Clear table before bulk loading |
| `write_table_rows()` | During initial copy | Receive rows from source table |
| `write_events()` | After initial copy | Receive streaming changes |

### Implementation Notes

- `truncate_table()` is called unconditionally before copying, even if the table doesn't exist yet
- `write_table_rows()` may be called with an empty vector to signal table creation
- `write_events()` receives batches that may span multiple tables
- Operations should be idempotent when possible (ETL may retry on failure)
- Handle concurrent calls safely (parallel table sync workers)

See [Event Types](events.md) for details on the events received by `write_events()`.

## SchemaStore

Stores table schema information (column names, types, etc.).

```rust
pub trait SchemaStore {
    fn get_table_schema(&self, table_id: &TableId) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;
    fn load_table_schemas(&self) -> impl Future<Output = EtlResult<usize>> + Send;
    fn store_table_schema(&self, schema: TableSchema) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### Methods

| Method | Purpose |
|--------|---------|
| `get_table_schema()` | Get schema for one table from cache |
| `get_table_schemas()` | Get all cached schemas |
| `load_table_schemas()` | Load schemas from persistent storage into cache (called at startup) |
| `store_table_schema()` | Save schema to both cache and persistent storage |

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
| `get_table_replication_state()` | Get current phase for one table |
| `get_table_replication_states()` | Get phases for all tables |
| `load_table_replication_states()` | Load from persistent storage (startup) |
| `update_table_replication_state()` | Update phase (cache + persistent) |
| `rollback_table_replication_state()` | Revert to previous phase |

### Table Mapping Methods

Table mappings connect source table IDs to destination table names.

| Method | Purpose |
|--------|---------|
| `get_table_mapping()` | Get destination name for source table |
| `get_table_mappings()` | Get all mappings |
| `load_table_mappings()` | Load from persistent storage |
| `store_table_mapping()` | Save mapping (cache + persistent) |

### Table Replication Phases

Tables progress through these phases:

| Phase | Description |
|-------|-------------|
| `Init` | Table discovered, ready to start |
| `DataSync` | Initial data being copied |
| `FinishedCopy` | Copy done, waiting for coordination |
| `SyncWait` | Signaling apply worker to pause |
| `Catchup` | Catching up to apply worker position |
| `SyncDone` | Caught up, ready for handoff |
| `Ready` | Streaming normally |
| `Errored` | Error occurred, excluded until rollback |

## CleanupStore

Removes ETL metadata when tables leave the publication.

```rust
pub trait CleanupStore {
    fn cleanup_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
}
```

| Method | Purpose |
|--------|---------|
| `cleanup_table_state()` | Remove all ETL metadata for a table (state, schema, mappings) |

## Combining Traits

A single type typically implements all store traits:

```rust
pub struct MyStore { /* ... */ }

impl SchemaStore for MyStore { /* ... */ }
impl StateStore for MyStore { /* ... */ }
impl CleanupStore for MyStore { /* ... */ }
```

ETL provides `MemoryStore` (in-memory, non-persistent) and `PostgresStore` (persistent to Postgres) as built-in implementations.

## Thread Safety

All trait implementations must be thread-safe. ETL calls these methods from:

- Multiple table sync workers (parallel initial copy)
- Apply worker (streaming changes)
- Pipeline coordination

Use `Arc<Mutex<_>>` or similar patterns for shared state.

## Next Steps

- [Custom Stores and Destinations](../guides/custom-implementations.md): Implement these traits
- [Event Types](events.md): Events received by `write_events()`
- [Architecture](architecture.md): How these components fit together
