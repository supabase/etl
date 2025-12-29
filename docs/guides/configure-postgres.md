# Configure Postgres for Replication

**Set up Postgres with the correct permissions and settings for ETL logical replication**

This guide covers the essential Postgres concepts and configuration needed for logical replication with ETL.

## Prerequisites

- **PostgreSQL 14, 15, 16, or 17** (officially supported and tested versions)
  - PostgreSQL 15+ is recommended for advanced publication filtering features (column-level and row-level filters, `FOR ALL TABLES IN SCHEMA` syntax)
  - PostgreSQL 14 is supported but has limited publication filtering capabilities
- Superuser access to the Postgres server
- Ability to restart Postgres server (for configuration changes)

## Understanding WAL Logical

Postgres's Write-Ahead Log (WAL) is the foundation of logical replication. When `wal_level = logical`, Postgres:

- Records detailed information about data changes (not just physical changes)
- Includes enough metadata to reconstruct logical changes
- Allows external tools to decode and stream these changes

```ini
# Enable logical replication in postgresql.conf
wal_level = logical
```

Restart Postgres after changing this setting.

## Replication Slots

Replication slots ensure that Postgres retains WAL data for logical replication consumers, even if they disconnect temporarily.

### What are Replication Slots?

- **Persistent markers** in Postgres that track replication progress
- **Prevent WAL cleanup** until the consumer catches up
- **Guarantee data consistency** across disconnections

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

**Warning:** Only delete slots when you're sure they're not in use. Deleting an active slot will fail replication.

## Max Replication Slots

Controls how many replication slots Postgres can maintain simultaneously.

```ini
# Increase max replication slots (default is 10)
max_replication_slots = 20
```

ETL uses a **single replication slot** for its main apply worker. However, additional slots may be created for parallel table
copies when the pipeline is initialized or when a new table is added to the publication. The `max_table_sync_workers` parameter
controls the number of these parallel copies, ensuring that the total replication slots used by ETL never exceed `max_table_sync_workers + 1`.

**When to increase:**

- Running multiple ETL pipelines
- Development/testing with frequent slot creation

## WAL Keep Size

Determines how much WAL data to retain on disk, providing a safety buffer for replication consumers.

```ini
# Keep 1GB of WAL data (Postgres 13+)
wal_keep_size = 1GB

# For Postgres 12 and earlier, use:
# wal_keep_segments = 256  # Each segment is typically 16MB
```

**Purpose:**

- Prevents WAL deletion when replication consumers fall behind
- Provides recovery time if ETL pipelines temporarily disconnect
- Balances disk usage with replication reliability

## WAL Buildup and Disk Usage

Replication slots prevent Postgres from deleting WAL files until all consumers have processed them. This can cause significant disk usage if the pipeline falls behind or encounters errors.

### Common Causes of WAL Buildup

**1. Tables in Errored State**

When a table enters an errored state, ETL keeps its replication slot active to maintain data consistency. This prevents WAL cleanup for that slot, causing Postgres to accumulate WAL files. If you have tables stuck in an errored state, monitor your disk usage and consider:

- Investigating and resolving the error cause
- Manually removing the table from the publication if it's no longer needed
- Increasing available disk space as a temporary measure

**2. Slow Pipeline Performance**

During normal operation, if your destination can't keep up with the rate of changes in Postgres, WAL will accumulate. This is especially common when:

- The destination has high latency (network or processing)
- Large transactions generate many changes at once
- The destination is temporarily unavailable

**3. Long-Running Initial Table Copies**

During the initial sync phase, ETL creates a replication slot for each table being copied. If a table has millions of rows, the copy operation can take a long time. Meanwhile, Postgres continues accumulating WAL because the slot hasn't started streaming yet.

**Warning:** If the copy takes too long and WAL grows beyond Postgres's configured limits, Postgres may terminate the replication slot. This is controlled by `max_slot_wal_keep_size`:

```ini
# Maximum WAL size to retain for replication slots (Postgres 13+)
# -1 means unlimited (dangerous for disk space)
# Set this based on your available disk space
max_slot_wal_keep_size = 10GB
```

If a slot is terminated due to exceeding this limit, ETL will need to restart the table sync from scratch.

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

- **Set `max_slot_wal_keep_size`** to a reasonable limit based on your available disk space
- **Monitor replication slot lag** and alert when it exceeds acceptable thresholds
- **Address errored tables promptly** to prevent indefinite WAL accumulation
- **Size initial sync workers appropriately** (`max_table_sync_workers`) to balance parallelism with resource usage

## Publications

Publications define which tables and operations to replicate.

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

If you want to replicate partitioned tables, you must use `publish_via_partition_root = true` when creating your publication. This option tells Postgres to treat the [partitioned table as a single table](https://www.postgresql.org/docs/current/sql-createpublication.html#SQL-CREATEPUBLICATION-PARAMS-WITH-PUBLISH-VIA-PARTITION-ROOT) from the replication perspective, rather than replicating each partition individually. All changes to any partition will be published as changes to the parent table:

```sql
-- Create publication with partitioned table support
CREATE PUBLICATION my_publication FOR TABLE users, orders WITH (publish_via_partition_root = true);

-- For all tables including partitioned tables
CREATE PUBLICATION all_tables FOR ALL TABLES WITH (publish_via_partition_root = true);
```

**Limitation:** If this option is enabled, `TRUNCATE` operations performed directly on individual partitions are not replicated. To replicate a truncate operation, you must execute it on the parent table instead:

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

ETL supports PostgreSQL versions 14 through 17, with enhanced features available in newer versions:

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

PostgreSQL 14 supports table-level publication filtering only. Column-level and row-level filters are not available. When using PostgreSQL 14, you'll need to filter data at the application level if selective replication is required.

### Feature Compatibility Matrix

| Feature | PostgreSQL 14 | PostgreSQL 15+ |
|---------|--------------|----------------|
| Table-level publication | Yes | Yes |
| Column-level filtering | No | Yes |
| Row-level filtering | No | Yes |
| `FOR ALL TABLES IN SCHEMA` | No | Yes |
| Partitioned table support | Yes | Yes |

## Complete Configuration Example

Here's a minimal `postgresql.conf` setup:

```ini
# Enable logical replication
wal_level = logical

# Increase replication capacity
max_replication_slots = 20
max_wal_senders = 20

# Keep WAL data for safety
wal_keep_size = 1GB  # Postgres 13+
# wal_keep_segments = 64  # Postgres 12 and earlier
```

After editing the configuration:

1. **Restart Postgres**
2. **Create your publication**:
   ```sql
   CREATE PUBLICATION etl_publication FOR TABLE your_table;
   ```
3. **Verify the setup**:
   ```sql
   SHOW wal_level;
   SHOW max_replication_slots;
   SELECT * FROM pg_publication WHERE pubname = 'etl_publication';
   ```

## Next Steps

- [Your First Pipeline](first-pipeline.md): Hands-on tutorial using these settings
- [Custom Stores and Destinations](custom-implementations.md): Build your own components
- [ETL Architecture](../explanation/architecture.md): How ETL uses these settings
