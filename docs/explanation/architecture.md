---
type: explanation
title: ETL Architecture Overview
last_reviewed: 2025-01-14
---

# ETL Architecture Overview

**Understanding how ETL components work together to replicate data from PostgreSQL**

ETL's architecture is built around a few key abstractions that work together to provide reliable, high-performance data replication. This document explains how these components interact and why they're designed the way they are.

## The Big Picture

At its core, ETL connects PostgreSQL's logical replication stream to configurable destination systems:

```
PostgreSQL        ETL Pipeline           Destination
┌─────────────┐   ┌──────────────┐      ┌─────────────┐
│ WAL Stream  │──▷│ Data Processing │────▷│ BigQuery    │
│ Publications│   │ Batching        │     │ Custom API  │
│ Repl. Slots │   │ Error Handling  │     │ Memory      │
└─────────────┘   └──────────────────┘     └─────────────┘
                          │
                    ┌──────▼──────┐
                    │ State Store │
                    │ Schema Info │
                    └─────────────┘
```

The architecture separates concerns to make the system extensible, testable, and maintainable.

## Core Components

### Pipeline: The Orchestrator

The [`Pipeline`](../reference/pipeline/) is ETL's central component that coordinates all other parts:

**Responsibilities:**
- Establishes connection to PostgreSQL replication stream
- Manages initial table synchronization ("backfill")  
- Processes ongoing change events from WAL
- Coordinates batching and delivery to destinations
- Handles errors and retries

**Why this design?** By centralizing orchestration in one component, we can ensure consistent behavior across all operations while keeping the interface simple for users.

### Destinations: Where Data Goes

The [`Destination`](../reference/destination-trait/) trait defines how data leaves ETL:

```rust
trait Destination {
    async fn write_batch(&mut self, batch: BatchedData) -> Result<(), DestinationError>;
    async fn flush(&mut self) -> Result<(), DestinationError>;
}
```

**Built-in implementations:**
- [`MemoryDestination`](../reference/memory-destination/) - For testing and development
- [`BigQueryDestination`](../reference/bigquery-destination/) - Google BigQuery integration

**Why this abstraction?** The trait allows ETL to support any output system while providing consistent batching, error handling, and retry behavior. New destinations get all the pipeline reliability features automatically.

### Stores: Managing State and Schemas  

ETL uses two types of storage via the [`Store`](../reference/store-trait/) trait:

**State storage** tracks replication progress:
- WAL positions for recovery
- Table synchronization status  
- Retry counters and backoff timers

**Schema storage** manages table structures:
- Column names and types
- Primary key information
- Schema evolution tracking

**Implementation options:**
- [`MemoryStore`](../reference/memory-store/) - Fast, but loses state on restart
- [`PostgresStore`](../reference/postgres-store/) - Persistent, production-ready

**Why separate storage?** This allows ETL to work in different deployment scenarios: development (memory), cloud-native (external databases), or embedded (SQLite, eventually).

## Data Flow Architecture

### Initial Synchronization

When a pipeline starts, ETL performs a full synchronization of existing data:

1. **Discovery:** Query PostgreSQL catalogs to find tables in the publication
2. **Schema capture:** Extract column information and primary keys
3. **Snapshot:** Copy existing rows in batches to the destination
4. **State tracking:** Record progress to support resumption

This ensures the destination has complete data before processing real-time changes.

### Ongoing Replication  

After initial sync, ETL processes the PostgreSQL WAL stream:

1. **Stream connection:** Attach to the replication slot
2. **Event parsing:** Decode WAL records into structured changes  
3. **Batching:** Group changes for efficient destination writes
4. **Delivery:** Send batches to destinations with retry logic
5. **Acknowledgment:** Confirm WAL position to PostgreSQL

### Error Handling Strategy

ETL's error handling follows a layered approach:

**Transient errors** (network issues, destination overload):
- Exponential backoff retry
- Circuit breaker to prevent cascading failures
- Eventual resumption from last known good state

**Permanent errors** (schema mismatches, authentication failures):
- Immediate pipeline halt
- Clear error reporting to operators
- Manual intervention required

**Partial failures** (some tables succeed, others fail):
- Per-table error tracking
- Independent retry schedules
- Healthy tables continue processing

## Scalability Patterns

### Vertical Scaling

ETL supports scaling up through configuration:

- **Batch sizes:** Larger batches for higher throughput
- **Worker threads:** Parallel table synchronization
- **Buffer sizes:** More memory for better batching

### Horizontal Scaling  

For massive databases, ETL supports:

- **Multiple pipelines:** Split tables across different pipeline instances
- **Destination sharding:** Route different tables to different destinations
- **Read replicas:** Reduce load on primary database

### Resource Management

ETL is designed to be resource-predictable:

- **Memory bounds:** Configurable limits on batch sizes and buffers
- **Connection pooling:** Reuse PostgreSQL connections efficiently  
- **Backpressure:** Slow down if destinations can't keep up

## Extension Points

### Custom Destinations

The [`Destination`](../reference/destination-trait/) trait makes it straightforward to add support for new output systems:

- **REST APIs:** HTTP-based services
- **Message queues:** Kafka, RabbitMQ, etc.
- **Databases:** Any database with bulk insert capabilities
- **File systems:** Parquet, JSON, CSV outputs

### Custom Stores

The [`Store`](../reference/store-trait/) trait allows different persistence strategies:

- **Cloud databases:** RDS, CloudSQL, etc.
- **Key-value stores:** Redis, DynamoDB
- **Local storage:** SQLite, embedded databases

### Plugin Architecture

ETL's trait-based design enables:

- **Runtime plugin loading:** Dynamic destination discovery
- **Configuration-driven setup:** Choose implementations via config
- **Testing isolation:** Mock implementations for unit tests

## Design Philosophy

### Correctness First

ETL prioritizes data consistency over raw speed:
- **At-least-once delivery:** Better to duplicate than lose data
- **State durability:** Persist progress before acknowledging
- **Schema safety:** Validate destination compatibility

### Operational Simplicity  

ETL aims to be easy to operate:
- **Clear error messages:** Actionable information for operators
- **Predictable behavior:** Minimal configuration surprises
- **Observable:** Built-in metrics and logging

### Performance Where It Matters

ETL optimizes the bottlenecks:
- **Batching:** Amortize per-operation overhead
- **Async I/O:** Maximize network utilization
- **Zero-copy:** Minimize data copying where possible

## Next Steps

Now that you understand ETL's architecture:

- **See it in action** → [Build your first pipeline](../tutorials/first-pipeline/)
- **Learn about performance** → [Performance characteristics](performance/)
- **Understand the foundation** → [PostgreSQL logical replication](replication/)
- **Compare with alternatives** → [ETL vs. other tools](comparisons/)

## See Also

- [Design decisions](design/) - Why ETL is built the way it is
- [Crate structure](crate-structure/) - How code is organized  
- [State management](state-management/) - Deep dive on state handling