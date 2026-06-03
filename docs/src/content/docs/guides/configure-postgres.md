---
title: Configure Postgres for Replication
description: Set up Postgres with the correct permissions and settings for ETL logical replication.
---

**Set up Postgres with the correct permissions and settings for ETL logical replication**

This guide covers the essential **Postgres settings, slots, publications, and version-specific features** needed for logical replication with ETL.

*Using a Supabase-hosted database?* Also see the Supabase product guide to [database replication](https://supabase.com/docs/guides/database/replication). It covers the Supabase-facing replication workflow and is the best companion reference when you are using ETL against a Supabase project.

## Prerequisites

- **PostgreSQL 14, 15, 16, 17, or 18** (officially supported and tested versions)
  - PostgreSQL 15+ recommended for advanced publication filtering (column-level, row-level, `FOR ALL TABLES IN SCHEMA`)
  - PostgreSQL 16+ required when logical replication is read from a physical read replica
  - PostgreSQL 14 supported with table-level filtering only
- Superuser access to the Postgres server
- Ability to restart Postgres (required for `wal_level` changes)

## Enable Logical WAL

Set `wal_level = logical` to enable Postgres to record **logical change data** in the WAL, which external tools can then decode and stream.

```ini
# postgresql.conf
wal_level = logical
```

**Restart Postgres** after changing this setting.

## Replication Slots

Replication slots ensure Postgres retains **WAL data for replication consumers**, even if they disconnect temporarily. They are:

- **Persistent markers** that track replication progress
- **WAL retention mechanisms** that prevent cleanup until consumers catch up
- **Consistency guarantees** across disconnections

### Creating Replication Slots

```sql
-- Create a logical replication slot
SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
```

### Viewing Replication Slots

```sql
-- See all replication slots
SELECT slot_name, slot_type, active, restart_lsn
FROM pg_replication_slots;
```

### Deleting Replication Slots

```sql
-- Drop a replication slot when no longer needed
SELECT pg_drop_replication_slot('my_slot');
```

**Warning:** Only delete slots when you are sure they are not in use. Deleting an active slot will break replication.

## Max Replication Slots

Controls how many **replication slots** Postgres can maintain simultaneously.

```ini
# postgresql.conf (default is 10)
max_replication_slots = 20
```

ETL uses a **single replication slot** for its main apply worker. Additional slots are created for parallel table copies during initial sync or when new tables are added to the publication. The `max_table_sync_workers` pipeline parameter controls parallel copies, so total slots used by ETL never exceed `max_table_sync_workers + 1`.

**When to increase:**

- Running multiple ETL pipelines against the same database
- Development/testing environments with frequent slot creation

## Max WAL Senders

Controls the maximum number of concurrent connections for streaming replication. **Each replication slot uses one WAL sender connection.**

```ini
# postgresql.conf (default is 10)
max_wal_senders = 20
```

Set this to at least `max_replication_slots` to ensure all slots can connect.

## WAL Keep Size

Determines how much **WAL data** to retain on disk, providing a safety buffer for replication consumers.

```ini
# postgresql.conf
wal_keep_size = 1GB
```

This setting:

- Prevents WAL deletion when replication consumers fall behind
- Provides recovery time if ETL pipelines temporarily disconnect
- Balances disk usage with replication reliability

## WAL Buildup and Disk Usage

Replication slots prevent Postgres from deleting WAL files until all consumers have processed them. This can cause **significant disk usage** if the pipeline falls behind or encounters errors.

### Common Causes of WAL Buildup

**1. Tables in Errored State**

When a table enters an errored state, ETL keeps its replication slot active to maintain data consistency. This prevents WAL cleanup for that slot, causing Postgres to accumulate WAL files. If you have tables stuck in an errored state:

- Investigate and resolve the error cause
- Remove the table from the publication if no longer needed
- Increase available disk space as a temporary measure

**2. Slow Pipeline Performance**

If your destination cannot keep up with the rate of changes in Postgres, WAL will accumulate. Common scenarios:

- High destination latency (network or processing)
- Large transactions generating many changes at once
- Destination temporarily unavailable

**3. Long-Running Initial Table Copies**

During initial sync, ETL creates a replication slot for each table being copied. Large tables with millions of rows can take significant time to copy, during which Postgres continues accumulating WAL.

**Warning:** If WAL grows beyond the configured limit, Postgres will terminate the replication slot. Control this with `max_slot_wal_keep_size`:

```ini
# postgresql.conf
# -1 = unlimited (dangerous for disk space)
max_slot_wal_keep_size = 10GB
```

If a slot is terminated due to exceeding this limit, ETL will restart the table sync from scratch.

### Monitoring WAL Usage

```sql
-- Check replication slot lag (how far behind each slot is)
SELECT slot_name,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag_pretty,
       active
FROM pg_replication_slots;

-- Check total WAL directory size
SELECT pg_size_pretty(sum(size)) AS wal_size
FROM pg_ls_waldir();
```

### Recommendations

- Set `max_slot_wal_keep_size` to a reasonable limit based on available disk space
- Monitor replication slot lag and alert when it exceeds acceptable thresholds
- Address errored tables promptly to prevent indefinite WAL accumulation
- Size initial sync workers appropriately (`max_table_sync_workers`) to balance parallelism with resource usage

## Read Replicas

ETL can read logical replication from a physical read replica when the replica runs **PostgreSQL 16 or newer**. PostgreSQL 14 and 15 can still be used with ETL, but logical decoding must run on the primary. See the PostgreSQL documentation on [logical slots on hot standby](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS).

When using a read replica:

- Configure `pg_connection` to point at the replica. ETL uses this connection for logical replication, table copy, schema reads, publications, slots, keepalives, and status updates.
- Configure `store_pg_connection` when using `PostgresStore` and `pg_connection` points at a read-only replica. The store connection must be writable because it runs store migrations and persists pipeline state.
- Apply ETL source migrations on the primary before starting the pipeline. Standby connections are read-only, so ETL skips source migration execution when the configured source is in recovery.
- Let ETL create its logical replication slots on the read replica. Do not pre-create ETL logical slots on the primary for this mode.

Publication, table, and ETL source-migration changes are ordinary WAL records. When you create them on the primary, the read replica can only see them after replay reaches that WAL position. Do not wait a fixed number of seconds; wait for a concrete replay LSN:

```sql
-- Run on the primary after creating tables, source migrations, and publications.
SELECT pg_current_wal_flush_lsn();
```

Then wait on the read replica until replay reaches that LSN. If you just created a new database on the primary, connect to an existing maintenance database such as `postgres` for this check; the new database might not exist on the replica until replay catches up.

```sql
-- Run on the read replica before starting ETL.
SELECT pg_last_wal_replay_lsn() >= '0/16B6C50'::pg_lsn AS ready;
```

For extra confidence, also check that the expected publication is visible on the replica:

```sql
SELECT 1 FROM pg_publication WHERE pubname = 'my_publication';
```

If ETL only receives the replica `pg_connection`, it cannot derive the primary's setup LSN itself. The orchestrator that creates or updates the source-side schema and publication should perform this LSN barrier before starting the pipeline, or should retry pipeline startup until the replica catches up. Once the logical slot exists on the replica, ongoing primary writes can lag normally; ETL will decode them as the replica replays WAL and the slot keeps the replica-side restart position.

The primary must generate logical WAL, and each server needs enough sender and slot capacity for the role it plays. On the primary, count the physical slots used by read replicas. On the read replica, count the logical slots ETL creates for its apply worker and table sync workers:

```ini
wal_level = logical
max_replication_slots = 20
max_wal_senders = 20
```

For the physical replication link between the primary and the read replica, use a [physical replication slot](https://www.postgresql.org/docs/16/warm-standby.html#STREAMING-REPLICATION-SLOTS) and enable standby feedback:

```ini
# on the standby
primary_conninfo = 'host=primary.example.com port=5432 dbname=postgres user=replicator password=...'
primary_slot_name = 'etl_read_replica'
hot_standby = on
hot_standby_feedback = on
wal_receiver_status_interval = '1s'
```

`hot_standby_feedback` helps prevent required catalog rows from being vacuumed away on the primary while standby logical slots need them. A physical slot between the primary and the standby keeps that protection across standby reconnects and restarts.

If initial copies can run for longer than your standby conflict delay, tune `max_standby_streaming_delay` for that replica. A larger value reduces copy cancellations at the cost of allowing more replay lag while conflicting standby queries finish.

Logical slot creation on a standby needs information about transactions running on the primary. If the primary is idle, creating a logical slot on the standby can wait until the primary emits that snapshot information. To speed this up during setup or tests, run this on the primary:

```sql
SELECT pg_log_standby_snapshot();
```

This is only a setup-time nudge for slot creation. ETL uses the regular PostgreSQL logical replication protocol for keepalives and status updates after streaming starts.

PostgreSQL 17+ also has logical failover slot synchronization, where failover-enabled logical slots on the primary are synchronized to standbys. That is a separate high-availability feature for resuming logical replication after promoting a standby. It is not required for ETL to read from a current read replica, and synchronized standby slots cannot be consumed on the standby while they are marked as synced.

## Publications

Publications define **which tables and operations** to replicate.

### Creating Publications

```sql
-- Create publication for specific tables
CREATE PUBLICATION my_publication FOR TABLE users, orders;

-- Create publication for all tables (use with caution)
CREATE PUBLICATION all_tables FOR ALL TABLES;

-- Include only specific operations
CREATE PUBLICATION inserts_only FOR TABLE users WITH (publish = 'insert');
```

#### Partitioned Tables

ETL supports PostgreSQL partition publications with either
`publish_via_partition_root = true` or `publish_via_partition_root = false`.
ETL reads the effective table list from `pg_publication_tables`, so it tracks
the same relation identities and schemas PostgreSQL uses for logical
replication messages.

For the easiest destination shape, prefer `publish_via_partition_root = true`.
This tells PostgreSQL to publish changes using the schema and identity of the
[topmost partitioned table included in the publication](https://www.postgresql.org/docs/current/sql-createpublication.html#SQL-CREATEPUBLICATION-PARAMS-WITH-PUBLISH-VIA-PARTITION-ROOT).
If you publish a subtree such as `orders_2026`, changes under that subtree are
published as `orders_2026`, not as the absolute root table:

```sql
-- Publish the selected partitioned tables as logical root tables
CREATE PUBLICATION my_publication FOR TABLE users, orders WITH (publish_via_partition_root = true);

-- For all tables including partitioned tables
CREATE PUBLICATION all_tables FOR ALL TABLES WITH (publish_via_partition_root = true);
```

When publications are created through the ETL API, ETL uses
`publish_via_partition_root = true` by default. Manually-created publications
can use either setting.

| Publication shape | `publish_via_partition_root` | PostgreSQL publishes as | ETL tracks |
| --- | --- | --- | --- |
| `FOR TABLE orders` where `orders` is the top partitioned table | `true` | `orders` | `orders` |
| `FOR TABLE orders` where `orders` is the top partitioned table | `false` | Leaf partitions under `orders` | The leaf partitions |
| `FOR TABLE orders_2026` where `orders_2026` is a partitioned subtree | `true` | `orders_2026` | `orders_2026` |
| `FOR TABLE orders_2026` where `orders_2026` is a partitioned subtree | `false` | Leaf partitions under `orders_2026` | The leaf partitions under `orders_2026` |
| `FOR TABLE orders_2026_01` where `orders_2026_01` is a leaf partition | Either | `orders_2026_01` | `orders_2026_01` |
| `FOR ALL TABLES` or `FOR TABLES IN SCHEMA ...` | `true` | Partition roots plus regular tables | Partition roots plus regular tables |
| `FOR ALL TABLES` or `FOR TABLES IN SCHEMA ...` | `false` | Leaf partitions plus regular tables | Leaf partitions plus regular tables |

For example, with this hierarchy:

```text
orders
  ├── orders_2025
  │   ├── orders_2025_01
  │   └── orders_2025_02
  └── orders_2026
      ├── orders_2026_01
      └── orders_2026_02
```

`FOR TABLE orders_2026 WITH (publish_via_partition_root = true)` replicates the
2026 subtree as `orders_2026`. `FOR TABLE orders_2026 WITH
(publish_via_partition_root = false)` replicates `orders_2026_01` and
`orders_2026_02` as separate leaf tables.

**Limitation:** With `publish_via_partition_root = true`, `TRUNCATE` operations on individual partitions are not replicated. Execute truncates on the published partition root table instead:

```sql
-- This will NOT be replicated
TRUNCATE TABLE orders_2024_q1;

-- This WILL be replicated
TRUNCATE TABLE orders;
```

### Managing Publications

```sql
-- View existing publications
SELECT * FROM pg_publication;

-- See which tables are in a publication
SELECT * FROM pg_publication_tables WHERE pubname = 'my_publication';

-- Add tables to existing publication
ALTER PUBLICATION my_publication ADD TABLE products;

-- Remove tables from publication
ALTER PUBLICATION my_publication DROP TABLE products;

-- Drop publication
DROP PUBLICATION my_publication;
```

## Version-Specific Features

ETL supports **PostgreSQL 14 through 18**, with enhanced publication features available in newer versions:

### PostgreSQL 16+ Features

**Logical decoding on read replicas:**

PostgreSQL 16 introduced logical replication slots on hot standby servers. Use this when ETL should read WAL from a physical read replica instead of the primary.

### PostgreSQL 15+ Features

**Column-Level Filtering:**

```sql
-- Replicate only specific columns from a table
CREATE PUBLICATION user_basics FOR TABLE users (id, email, created_at);
```

**Row-Level Filtering:**

```sql
-- Replicate only rows that match a condition
CREATE PUBLICATION active_users FOR TABLE users WHERE (status = 'active');
```

**Schema-Level Publications:**

```sql
-- Replicate all tables in a schema
CREATE PUBLICATION schema_pub FOR ALL TABLES IN SCHEMA public;
```

### PostgreSQL 14 Limitations

PostgreSQL 14 supports table-level publication filtering only. Column-level and row-level filters are not available. Filter data at the application level if selective replication is required.

### Feature Compatibility Matrix

| Feature | PostgreSQL 14 | PostgreSQL 15 | PostgreSQL 16+ |
|---------|---------------|---------------|----------------|
| Table-level publication | Yes | Yes | Yes |
| Column-level filtering | No | Yes | Yes |
| Row-level filtering | No | Yes | Yes |
| `FOR ALL TABLES IN SCHEMA` | No | Yes | Yes |
| Partitioned table support | Yes | Yes | Yes |
| Logical decoding on physical read replicas | No | No | Yes |

## Complete Configuration Example

Minimal `postgresql.conf` setup:

```ini
# Enable logical replication
wal_level = logical

# Replication capacity
max_replication_slots = 20
max_wal_senders = 20

# WAL retention
wal_keep_size = 1GB

# Limit WAL retention per slot (optional but recommended)
max_slot_wal_keep_size = 10GB
```

After editing the configuration:

1. Restart Postgres
2. Create your publication:
   ```sql
   CREATE PUBLICATION etl_publication FOR TABLE your_table;
   ```
3. Verify the setup:
   ```sql
   SHOW wal_level;
   SHOW max_replication_slots;
   SELECT * FROM pg_publication WHERE pubname = 'etl_publication';
   ```

## Next Steps

- [Your First Pipeline](/etl/guides/first-pipeline/): Hands-on tutorial using these settings
- [Custom Stores and Destinations](/etl/guides/custom-implementations/): Build your own components
- [ETL Architecture](/etl/explanation/architecture/): How ETL uses these settings
