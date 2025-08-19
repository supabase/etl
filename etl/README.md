# `etl` - Core

This is the main crate of the ETL system, providing the core functionality for Postgres logical replication. It abstracts the complexities of Postgres's logical streaming replication protocol and provides a unified interface for data replication and transformation.

## Features

| Feature                  | Description                                                   |
| ------------------------ | ------------------------------------------------------------- |
| `unknown-types-to-bytes` | Converts unknown Postgres types to bytes (enabled by default) |
| `test-utils`             | Enables testing utilities and helpers                         |
| `failpoints`             | Enables failure injection for testing                         |

## Architecture

The ETL core implements a pipeline architecture that replicates data from Postgres to various destinations.

### Key Components

- **Pipeline**: Main orchestrator that manages the replication process
- **Replication Client**: Connects to Postgres's logical replication protocol
- **Apply Worker**: Main worker that handles the creation of table sync workers and processes CDC events
- **Table Sync Worker**: Handles initial copying of existing table data and processes CDC events until it has caught up
  to the apply worker
- **State Store**: Stores the state of the pipeline
- **Schema Store**: Stores the table schemas of the tables involved in the replication

### Information Flow

```mermaid
graph TB
    subgraph "ETL Pipeline"
        Pipeline["🎭 Pipeline"]

        ApplyWorker["⚙️ Apply Worker"]

        subgraph "Worker Pool"
            TSWorker1["🔄 Table Sync Worker 1"]
            TSWorkerN["🔄 Table Sync Worker N"]
        end

        subgraph "Store"
            StateStore["💾 State Store"]
            SchemaStore["📋 Schema Store"]
        end
    end

    PG[("🐘 Postgres<br/>Source Database")]

    Destination[("🎯 Destination<br/>BigQuery, etc.")]

    Pipeline --> ApplyWorker

    ApplyWorker --> TSWorker1
    ApplyWorker --> TSWorkerN

    ApplyWorker --> Destination
    TSWorker1 --> Destination
    TSWorkerN --> Destination

    ApplyWorker <--> PG
    TSWorker1 <--> PG

    ApplyWorker <--> StateStore
    ApplyWorker <--> SchemaStore

    TSWorker1 <--> StateStore
    TSWorker1 <--> SchemaStore

    TSWorker2 <--> StateStore
    TSWorker2 <--> SchemaStore
```
