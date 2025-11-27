# Configure Postgres for Replication

**Set up Postgres with the correct permissions and settings for ETL logical replication**

This guide covers the essential Postgres concepts and configuration needed for logical replication with ETL.

## Prerequisites

- **PostgreSQL 14, 15, 16, 17, or 18** (officially supported and tested versions)
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
# Enable logical replication in Postgres.conf
wal_level = logical
```

**Restart Postgres** after changing this setting:

```bash
sudo systemctl restart Postgres
```

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

**Warning:** Only delete slots when you're sure they're not in use. Deleting an active slot can cause data loss.

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

ETL supports PostgreSQL versions 14 through 18, with enhanced features available in newer versions:

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
| Table-level publication | ✅ | ✅ |
| Column-level filtering | ❌ | ✅ |
| Row-level filtering | ❌ | ✅ |
| `FOR ALL TABLES IN SCHEMA` | ❌ | ✅ |
| Partitioned table support | ✅ | ✅ |

## Complete Configuration Example

Here's a minimal `Postgres.conf` setup:

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

- **Build your first pipeline** → [Build Your First ETL Pipeline](../tutorials/first-pipeline.md)
- **Build custom implementations** → [Custom Stores and Destinations](../tutorials/custom-implementations.md)

## See Also

- [Build Your First ETL Pipeline](../tutorials/first-pipeline.md) - Hands-on tutorial using these settings
- [ETL Architecture](../explanation/architecture.md) - Understanding how ETL uses these settings
- [API Reference](../reference/index.md) - All available connection options
