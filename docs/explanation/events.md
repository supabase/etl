# Event Types

**Understanding the events ETL delivers to your destination**

ETL streams events from Postgres logical replication to your destination via `write_events()`. This page documents all event types and how to handle them.

## Event Overview

| Event | Description | Has Table ID |
|-------|-------------|--------------|
| `Begin` | Transaction start | No |
| `Commit` | Transaction end | No |
| `Insert` | New row added | Yes |
| `Update` | Row modified | Yes |
| `Delete` | Row removed | Yes |
| `Relation` | Table schema | Yes |
| `Truncate` | Table cleared | Yes |
| `Unsupported` | Unknown event | No |

## Data Modification Events

These events carry row data and are associated with specific tables.

### Insert

A new row was added to a table.

```rust
pub struct InsertEvent {
    pub start_lsn: PgLsn,      // LSN where event started
    pub commit_lsn: PgLsn,     // LSN where transaction commits
    pub table_id: TableId,     // Which table
    pub table_row: TableRow,   // The new row data
}
```

### Update

An existing row was modified.

```rust
pub struct UpdateEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub table_id: TableId,
    pub table_row: TableRow,                    // New row data
    pub old_table_row: Option<(bool, TableRow)>, // Previous row data
}
```

The `old_table_row` field depends on Postgres `REPLICA IDENTITY` setting:

| REPLICA IDENTITY | `old_table_row` contains |
|------------------|--------------------------|
| `DEFAULT` | Primary key columns only (`true`, row) |
| `FULL` | All columns (`false`, row) |
| `NOTHING` | `None` |

### Delete

A row was removed from a table.

```rust
pub struct DeleteEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub table_id: TableId,
    pub old_table_row: Option<(bool, TableRow)>, // Deleted row data
}
```

Same `REPLICA IDENTITY` rules apply as Update.

### Truncate

One or more tables were truncated (all rows deleted).

```rust
pub struct TruncateEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub options: i8,       // Postgres truncate options
    pub rel_ids: Vec<u32>, // List of truncated table IDs
}
```

Note: A single Truncate event can affect multiple tables when using `TRUNCATE ... CASCADE`.

## Transaction Events

These events mark transaction boundaries.

### Begin

Marks the start of a transaction.

```rust
pub struct BeginEvent {
    pub start_lsn: PgLsn,   // Where transaction started
    pub commit_lsn: PgLsn,  // Where it will commit
    pub timestamp: i64,     // Transaction start time
    pub xid: u32,           // Transaction ID
}
```

### Commit

Marks successful transaction completion.

```rust
pub struct CommitEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub flags: i8,        // Postgres commit flags
    pub end_lsn: u64,     // Final LSN after commit
    pub timestamp: i64,   // Commit time
}
```

## Schema Events

### Relation

Provides table schema information. Sent before data events for a table.

```rust
pub struct RelationEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub table_schema: TableSchema, // Column definitions, types, etc.
}
```

## Important: Begin/Commit Behavior

During the initial copy phase, `Begin` and `Commit` events may be delivered **multiple times**. This happens because:

1. ETL uses parallel Table Sync Workers to copy tables concurrently
2. Each worker creates its own replication slot
3. Transaction markers from the main replication stream are delivered alongside table copy operations

**This does not cause data duplication.** Only transaction markers repeat - the actual row data (Insert, Update, Delete) is delivered exactly once.

Your destination should handle this by:

- **Option 1:** Track LSNs to detect duplicate Begin/Commit events
- **Option 2:** Ignore Begin/Commit entirely if your destination doesn't support transactions (e.g., BigQuery, most data warehouses)

Example handling:

```rust
async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
    for event in events {
        match event {
            Event::Insert(e) => self.handle_insert(e).await?,
            Event::Update(e) => self.handle_update(e).await?,
            Event::Delete(e) => self.handle_delete(e).await?,
            Event::Truncate(e) => self.handle_truncate(e).await?,
            Event::Relation(e) => self.handle_schema(e).await?,
            // Transaction markers - safe to ignore for most destinations
            Event::Begin(_) | Event::Commit(_) => {}
            Event::Unsupported => {}
        }
    }
    Ok(())
}
```

## Understanding LSN Fields

Every event includes two LSN (Log Sequence Number) fields that are critical for understanding event ordering and deduplication.

### What is an LSN?

An LSN is a pointer to a position in Postgres's Write-Ahead Log (WAL). It's a monotonically increasing 64-bit integer that uniquely identifies a location in the transaction log. Format: `0/16B3748` (segment/offset).

### start_lsn vs commit_lsn

| Field | Meaning | Use Case |
|-------|---------|----------|
| `start_lsn` | Where this specific event was recorded in the WAL | Deduplication, ordering within transaction |
| `commit_lsn` | Where the containing transaction commits | Transaction grouping, recovery checkpoints |

**Key insight:** Multiple events can share the same `commit_lsn` (they're in the same transaction) but each has a unique `start_lsn`.

### Example

Consider a transaction that inserts two rows:

```
BEGIN;                    -- Transaction starts
INSERT INTO users ...;    -- start_lsn: 0/16B3700, commit_lsn: 0/16B3800
INSERT INTO users ...;    -- start_lsn: 0/16B3750, commit_lsn: 0/16B3800
COMMIT;                   -- Transaction commits at 0/16B3800
```

Both inserts have the same `commit_lsn` (they commit together) but different `start_lsn` values (they're distinct events).

### Using LSNs

**For ordering:** Events are delivered in `start_lsn` order within a transaction, and transactions are ordered by `commit_lsn`.

**For deduplication:** If you see the same `start_lsn` twice, it's a duplicate event (can happen with Begin/Commit during initial copy).

**For checkpointing:** Store the highest `commit_lsn` you've processed. On restart, you can resume from that point.

## Event Batching

ETL batches events before calling `write_events()`. A single batch may contain:

- Events from multiple tables
- Events from multiple transactions
- A mix of event types

Events within the same table maintain their original order. Events for different tables can be processed concurrently.

## Next Steps

- [Custom Destinations](../guides/custom-implementations.md): Implement your own event handling
- [Architecture](architecture.md): How events flow through ETL
