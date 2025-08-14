---
type: explanation
title: Why PostgreSQL Logical Replication?
last_reviewed: 2025-01-14
---

# Why PostgreSQL Logical Replication?

**Understanding the foundation technology that powers ETL and its advantages over alternatives**

PostgreSQL logical replication is the core technology that ETL builds upon. This document explains how it works, why it's well-suited for ETL use cases, and how it compares to other change data capture approaches.

## What is Logical Replication?

Logical replication streams changes from PostgreSQL databases at the **logical level** (rows and operations) rather than the **physical level** (disk blocks and binary changes). This means ETL receives structured, interpretable data changes that can be easily transformed and routed to different destinations.

### Key Characteristics

- **Row-based:** Changes are captured as individual row operations (INSERT, UPDATE, DELETE)
- **Selective:** Choose which tables to replicate via publications  
- **Real-time:** Changes stream immediately as they're committed
- **Durable:** Uses PostgreSQL's Write-Ahead Log (WAL) for reliability
- **Ordered:** Changes arrive in commit order within each table

## How Logical Replication Works

### The WAL-Based Foundation

PostgreSQL's logical replication is built on its Write-Ahead Log (WAL):

1. **Transaction commits** are written to WAL before being applied to data files
2. **Logical decoding** translates WAL entries into structured change events
3. **Replication slots** track which changes have been consumed
4. **Publications** define which tables and operations to replicate

```
Application     PostgreSQL              ETL Pipeline
     │               │                       │
     │──── INSERT ────│                       │
     │               │──── WAL entry ────────│
     │               │                       │──── Structured change
     │               │                       │     (table, operation, data)
     │◄─── SUCCESS ───│                       │
```

### Publications and Subscriptions

**Publications** define what to replicate:

```sql
-- Replicate specific tables
CREATE PUBLICATION app_data FOR TABLE users, orders, products;

-- Replicate all tables (use with caution)
CREATE PUBLICATION all_data FOR ALL TABLES;

-- Replicate only specific operations
CREATE PUBLICATION inserts_only FOR TABLE users WITH (publish = 'insert');
```

**Replication slots** track consumption:

```sql
-- ETL creates and manages these automatically
SELECT pg_create_logical_replication_slot('etl_slot', 'pgoutput');
```

### Data Consistency Guarantees

Logical replication provides strong consistency:

- **Transactional consistency:** All changes from a transaction arrive together
- **Ordering guarantees:** Changes within a table maintain commit order
- **Durability:** WAL ensures no committed changes are lost
- **At-least-once delivery:** Changes may be delivered multiple times but never lost

## Why ETL Uses Logical Replication

### Real-Time Performance

Unlike polling-based approaches, logical replication provides **immediate change notification**:

- **Low latency:** Changes stream as they happen (milliseconds to seconds)
- **No database overhead:** No impact on application queries
- **Efficient bandwidth:** Only actual changes are transmitted

### Operational Simplicity

Logical replication is **built into PostgreSQL**:

- **No triggers to maintain:** Changes are captured automatically
- **No application changes:** Existing applications work unchanged  
- **Reliable recovery:** Built-in WAL retention and replay
- **Minimal configuration:** Just enable logical replication and create publications

### Complete Change Capture

Captures **all types of changes**:

- **DML operations:** INSERT, UPDATE, DELETE operations
- **Bulk operations:** COPY, bulk updates, and imports
- **Transaction boundaries:** Commit and rollback information
- **Schema information:** Column types and table structure

## Comparing Replication Approaches

### Logical Replication vs. Physical Replication

| Aspect | Logical Replication | Physical Replication |
|--------|-------------------|-------------------|
| **Granularity** | Table/row level | Entire database cluster |
| **Selectivity** | Choose specific tables | All or nothing |
| **Version compatibility** | Cross-version support | Same major version only |
| **Overhead** | Moderate (logical decoding) | Low (binary copy) |
| **Use case** | ETL, selective sync | Backup, disaster recovery |

### Logical Replication vs. Trigger-Based CDC

| Aspect | Logical Replication | Trigger-Based CDC |
|--------|-------------------|-----------------|
| **Performance impact** | Minimal on source | High (trigger execution) |
| **Change coverage** | All operations including bulk | Only row-by-row operations |
| **Maintenance** | Built-in PostgreSQL feature | Custom triggers to maintain |
| **Reliability** | WAL-based durability | Depends on trigger implementation |
| **Schema changes** | Handles automatically | Triggers need updates |

### Logical Replication vs. Query-Based Polling

| Aspect | Logical Replication | Query-Based Polling |
|--------|-------------------|-------------------|
| **Latency** | Real-time (seconds) | Polling interval (minutes) |
| **Source load** | Minimal | Repeated full table scans |
| **Delete detection** | Automatic | Requires soft deletes |
| **Infrastructure** | Simple (ETL + PostgreSQL) | Complex (schedulers, state tracking) |
| **Change ordering** | Guaranteed | Can miss intermediate states |

## Limitations and Considerations

### What Logical Replication Doesn't Capture

- **DDL operations:** Schema changes (CREATE, ALTER, DROP) are not replicated
- **TRUNCATE operations:** Not captured by default (can be enabled in PostgreSQL 11+)  
- **Sequence changes:** nextval() calls on sequences
- **Large object changes:** BLOB/CLOB modifications
- **Temporary table operations:** Temp tables are not replicated

### Performance Considerations

**WAL generation overhead:**
- Logical replication increases WAL volume by ~10-30%
- More detailed logging required for logical decoding
- May require WAL retention tuning for catch-up scenarios

**Replication slot management:**
- Unused slots prevent WAL cleanup (disk space growth)
- Slow consumers can cause WAL buildup
- Need monitoring and automatic cleanup

**Network bandwidth:**
- All change data flows over network
- Large transactions can cause bandwidth spikes
- Consider batching and compression for high-volume scenarios

## ETL's Enhancements to Logical Replication

ETL builds on PostgreSQL's logical replication with additional features:

### Intelligent Batching

- **Configurable batch sizes:** Balance latency vs. throughput
- **Time-based batching:** Ensure maximum latency bounds
- **Backpressure handling:** Slow down if destinations can't keep up

### Error Handling and Recovery

- **Retry logic:** Handle transient destination failures
- **Circuit breakers:** Prevent cascade failures
- **State persistence:** Resume from exact WAL positions after restarts

### Multi-Destination Routing

- **Fan-out replication:** Send same data to multiple destinations
- **Selective routing:** Different tables to different destinations
- **Transformation pipelines:** Modify data en route to destinations

### Operational Features

- **Metrics and monitoring:** Track replication lag, throughput, errors
- **Schema change detection:** Automatic handling of table structure changes
- **Resource management:** Memory and connection pooling

## Use Cases and Patterns

### Real-Time Analytics

Stream transactional data to analytical systems:

```
PostgreSQL (OLTP)  ──ETL──▷  BigQuery (OLAP)
     │                            │
     ├── Users insert orders      ├── Real-time dashboards
     ├── Inventory updates        ├── Business intelligence
     └── Payment processing       └── Data science workflows
```

### Event-Driven Architecture

Use database changes as event sources:

```
PostgreSQL  ──ETL──▷  Event Bus  ──▷  Microservices
     │                    │             │
     ├── Order created    ├── Events    ├── Email service
     ├── User updated     ├── Topics    ├── Notification service  
     └── Inventory low    └── Streams   └── Recommendation engine
```

### Data Lake Ingestion

Continuously populate data lakes:

```
PostgreSQL  ──ETL──▷  Data Lake  ──▷  ML/Analytics
     │                    │             │
     ├── App database     ├── Parquet   ├── Feature stores
     ├── User behavior    ├── Delta     ├── Model training
     └── Business data    └── Iceberg   └── Batch processing
```

## Choosing Logical Replication

**Logical replication is ideal when you need:**

- Real-time or near real-time change capture
- Selective table replication  
- Cross-version or cross-platform data movement
- Minimal impact on source database performance
- Built-in reliability and durability guarantees

**Consider alternatives when you need:**

- **Immediate consistency:** Use synchronous replication or 2PC
- **Schema change replication:** Consider schema migration tools
- **Cross-database replication:** Look at database-specific solutions
- **Complex transformations:** ETL tools might be simpler

## Future of Logical Replication

PostgreSQL continues to enhance logical replication:

- **Row-level security:** Filter replicated data by user permissions
- **Binary protocol improvements:** Faster, more efficient encoding
- **Cross-version compatibility:** Better support for version differences
- **Performance optimizations:** Reduced overhead and increased throughput

ETL evolves alongside these improvements, providing a stable interface while leveraging new capabilities as they become available.

## Next Steps

Now that you understand the foundation:

- **See it in practice** → [ETL Architecture](architecture/)
- **Compare alternatives** → [ETL vs. Other Tools](comparisons/)
- **Build your first pipeline** → [First Pipeline Tutorial](../tutorials/first-pipeline/)
- **Configure PostgreSQL** → [PostgreSQL Setup](../how-to/configure-postgres/)

## See Also

- [PostgreSQL Logical Replication Docs](https://www.postgresql.org/docs/current/logical-replication.html) - Official documentation
- [Design decisions](design/) - Why ETL is built the way it is
- [Performance characteristics](performance/) - Understanding ETL's behavior under load