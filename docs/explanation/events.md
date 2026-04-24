# Event Types

**Understanding the events ETL emits from Postgres logical replication**

ETL streams events from Postgres logical replication via `write_events()`.
This page documents the event types and the PostgreSQL semantics they preserve.

## Event Overview

| Event | Description | Has Table ID |
|-------|-------------|--------------|
| `Begin` | Transaction start | No |
| `Commit` | Transaction end | No |
| `Insert` | New row added | Yes |
| `Update` | Row modified | Yes |
| `Delete` | Row removed | Yes |
| `Truncate` | Table cleared | Yes |
| `Relation` | Table schema | Yes |
| `Unsupported` | Unknown event | No |

## Data Modification Events

These events carry row data and are associated with specific tables.

### Row Images

Data modification events use row-image helper types:

```rust
pub enum UpdatedTableRow {
    Full(TableRow),
    Partial(PartialTableRow),
}

pub enum OldTableRow {
    Full(TableRow),
    Key(TableRow),
}
```

`TableRow` is a complete dense row. Its values are ordered to match the
replicated table-column order.

`PartialTableRow` is used when an update row is not complete. It exposes:

- `total_columns()`: the number of replicated columns in the table schema;
- `values()`: present values in replicated table-column order, excluding missing columns;
- `missing_column_indexes()`: zero-based replicated-column indexes for values ETL could not reconstruct.

`OldTableRow::Full(row)` contains a complete old row. `OldTableRow::Key(row)`
contains only replica-identity columns, densely packed in replicated
table-column order.

### Insert

A new row was added to a table.

```rust
pub struct InsertEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub tx_ordinal: u64,
    pub replicated_table_schema: ReplicatedTableSchema,
    pub table_row: TableRow,
}
```

### Update

An existing row was modified.

```rust
pub struct UpdateEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub tx_ordinal: u64,
    pub replicated_table_schema: ReplicatedTableSchema,
    pub updated_table_row: UpdatedTableRow,
    pub old_table_row: Option<OldTableRow>,
}
```

`updated_table_row` is the authoritative post-update payload:

- `UpdatedTableRow::Full` when ETL knows every replicated column value after decoding the update.
- `UpdatedTableRow::Partial` when PostgreSQL emitted `UnchangedToast` fields that ETL could not reconstruct safely.

The `old_table_row` field is auxiliary old-side context that may be needed for
row matching, key changes, or reconstruction logic.

#### Unchanged Toast

PostgreSQL may encode an unchanged toasted value as `UnchangedToast` in the
update's new tuple instead of resending the full column value. ETL can turn the
update into `UpdatedTableRow::Full` only when that value can be recovered from
the old-side row image:

- a `FULL` old row can recover unchanged toasted values for any replicated column;
- a key-only old row can recover unchanged toasted values only for
  replica-identity columns included in that key image;
- no old row means unchanged toasted values cannot be recovered from the event.

When any `UnchangedToast` field cannot be recovered, ETL emits
`UpdatedTableRow::Partial` with known values and missing column indexes instead
of pretending the unknown value is `NULL` or a replacement value.

Destinations that need complete replacement rows, full before/after comparison,
or complete audit records should require `REPLICA IDENTITY FULL` for tables with
toasted columns, or keep enough prior state to fill missing values themselves.

#### Old Row Mapping

ETL maps PostgreSQL pgoutput update tuple markers directly:

| pgoutput marker | ETL field |
|-----------------|-----------|
| `O` old tuple | `Some(OldTableRow::Full(row))` |
| `K` old key | `Some(OldTableRow::Key(row))` |
| no old tuple/key marker | `None` |
| `N` new tuple | `updated_table_row` |

PostgreSQL chooses which old-side marker to emit from the table's replica
identity:

| REPLICA IDENTITY | `old_table_row` contains for published updates |
|------------------|----------------------------------------------|
| `FULL` | `Some(OldTableRow::Full(row))` |
| `DEFAULT` with a primary key | `Some(OldTableRow::Key(row))` when PostgreSQL determines the old key must be logged, otherwise `None` |
| `DEFAULT` without a primary key | Source `UPDATE` is rejected when the table publishes updates |
| `USING INDEX` | `Some(OldTableRow::Key(row))` when PostgreSQL determines the old key must be logged, otherwise `None` |
| `NOTHING` | Source `UPDATE` is rejected when the table publishes updates |

`OldTableRow::Key(row)` stores only the replica-identity columns, normalized into replicated table-column order.

For `UPDATE`, PostgreSQL only sends an old key image under `DEFAULT` or
`USING INDEX` when it determines the old key must be logged. In practice, that
happens when:

- any replica-identity column changed, or
- any replica-identity column contains external data, such as a toasted value
  that must be available from the old tuple.

That means non-identity updates under `DEFAULT` or `USING INDEX` often arrive with `old_table_row = None`.
Under `FULL`, PostgreSQL sends a full old row for every published update.
A `FULL` update with `old_table_row = None` is not a shape emitted by
PostgreSQL pgoutput.

Key points for update handling:

- `old_table_row = None` on an update is a valid `pgoutput` shape for `DEFAULT` or `USING INDEX`. It does not mean the table has no replica identity.
- `OldTableRow::Key(row)` contains only replica-identity columns, in replicated table-column order. It is not necessarily the source primary key.
- Consumers that match source rows by replica identity should use identity columns from `old_table_row` when present.
- Consumers that match source rows by another key, such as the source primary key, can project the old or new row down to that key.
- Consumers that need before-and-after values for arbitrary columns need `REPLICA IDENTITY FULL`; key-based identity only gives old identity columns, and only when PostgreSQL logs them.

For destination implementations, a practical update flow is:

1. Treat `updated_table_row` as the post-update payload.
2. Handle `UpdatedTableRow::Partial` before constructing a complete replacement row.
3. Use `OldTableRow::Key` only as old replica-identity values.
4. Use `OldTableRow::Full` when full before-image values are required.
5. If `old_table_row` is `None`, match or upsert from the new row according to the destination's own keying model.

### Delete

A row was removed from a table.

```rust
pub struct DeleteEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub tx_ordinal: u64,
    pub replicated_table_schema: ReplicatedTableSchema,
    pub old_table_row: Option<OldTableRow>,
}
```

For `DELETE`, valid PostgreSQL publications send an old-side image:

| REPLICA IDENTITY | `old_table_row` contains for published deletes |
|------------------|---------------------------------------------|
| `FULL` | `Some(OldTableRow::Full(row))` |
| `DEFAULT` with a primary key | `Some(OldTableRow::Key(row))` |
| `DEFAULT` without a primary key | Source `DELETE` is rejected when the table publishes deletes |
| `USING INDEX` | `Some(OldTableRow::Key(row))` |
| `NOTHING` | Source `DELETE` is rejected when the table publishes deletes |

Important implications:

- Deletes do not carry a new row image. `old_table_row` is the delete payload.
- `OldTableRow::Key(row)` again means replica-identity columns only, not necessarily the table's primary key.
- Consumers that require full old rows for deletes need `REPLICA IDENTITY FULL`.
- The Rust field is optional at the event API boundary, but PostgreSQL pgoutput
  populates it for every published delete. Tables without usable replica
  identity fail at the source when they publish deletes.
- Destination implementations should decide up front whether key-only deletes
  are enough. If they are not, require `REPLICA IDENTITY FULL` for those tables.

### Truncate

One or more tables were truncated (all rows deleted).

```rust
pub struct TruncateEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub tx_ordinal: u64,
    pub options: i8,
    pub truncated_tables: Vec<ReplicatedTableSchema>,
}
```

Note: A single Truncate event can affect multiple tables when using `TRUNCATE ... CASCADE`.

## Transaction Events

These events mark transaction boundaries.

### Begin

Marks the start of a transaction.

```rust
pub struct BeginEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub tx_ordinal: u64,
    pub timestamp: i64,
    pub xid: u32,
}
```

### Commit

Marks successful transaction completion.

```rust
pub struct CommitEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub tx_ordinal: u64,
    pub flags: i8,
    pub end_lsn: PgLsn,
    pub timestamp: i64,
}
```

## Schema Events

### Relation

Provides table schema information. Sent before data events for a table.

```rust
pub struct RelationEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub tx_ordinal: u64,
    pub replicated_table_schema: ReplicatedTableSchema,
}
```

PostgreSQL pgoutput builds relation messages by walking the table descriptor in
`pg_attribute.attnum` order and skipping columns that are not published. It
sends tuple data in the same order as the relation message.

ETL builds replication and identity masks from relation-message column names, so
the order is not needed to decide which columns are included. That name-based
matching is sound because PostgreSQL live column names are unique within a table
schema version. The order matters only after the masks are applied: stored table
schemas are also ordered by `attnum`, so
`ReplicatedTableSchema::column_schemas()` becomes a positional view that matches
the tuple payloads exactly, even when a publication filters columns.

## Begin/Commit Behavior

During initial copy, `Begin` and `Commit` events may be delivered **multiple times** due to parallel Table Sync Workers creating separate replication slots. Row data (Insert, Update, Delete) is delivered exactly once.

Handle this by either:
- Tracking LSNs to detect duplicate Begin/Commit events
- Ignoring Begin/Commit if transaction markers are not required

```rust
async fn write_events(&self, events: Vec<Event>, async_result: WriteEventsResult<()>) -> EtlResult<()> {
    let result = async {
        for event in events {
            match event {
                Event::Insert(e) => self.handle_insert(e).await?,
                Event::Update(e) => self.handle_update(e).await?,
                Event::Delete(e) => self.handle_delete(e).await?,
                Event::Truncate(e) => self.handle_truncate(e).await?,
                Event::Relation(e) => self.handle_schema(e).await?,
                // Transaction markers - safe to ignore for most consumers.
                Event::Begin(_) | Event::Commit(_) => {}
                Event::Unsupported => {}
            }
        }
        Ok(())
    }
    .await;

    async_result.send(result);
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
| `start_lsn` | Position where this event was recorded in the WAL | Deduplication, ordering within transaction |
| `commit_lsn` | Position where the transaction will commit | Transaction grouping, recovery checkpoints |

**Key insight:** Multiple events share the same `commit_lsn` (same transaction) but each has a unique `start_lsn`.

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

ETL batches events before calling `write_events()`. A batch may contain events from multiple tables, multiple transactions, and mixed event types.

**Ordering requirement:** Events affecting the same row (by primary key) must be processed in order. Events for different rows can be processed concurrently.

## Next Steps

- [Custom Destinations](../guides/custom-implementations.md): Implement your own event handling
- [Architecture](architecture.md): How events flow through ETL
