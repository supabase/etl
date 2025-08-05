# `etl` - Core

This is the main crate of the ETL system, providing the core functionality for PostgreSQL logical replication. It abstracts the complexities of PostgreSQL's logical streaming replication protocol and provides a unified interface for data replication and transformation.

## Key Components

### Replication Engine
- **Logical Replication Client**: Connects to PostgreSQL's logical replication stream
- **Table Synchronization**: Initial table copying and schema synchronization  
- **Change Data Capture**: Real-time processing of INSERT, UPDATE, DELETE operations
- **Replication Slot Management**: Automatic creation and management of replication slots

### Pipeline Architecture
- **Worker Pool**: Concurrent processing of replication events
- **State Management**: Persistent tracking of replication progress and table states
- **Schema Caching**: Efficient handling of PostgreSQL schema information
- **Error Handling**: Robust error recovery and retry mechanisms

### Data Processing
- **Type Conversions**: Automatic conversion between PostgreSQL and destination types
- **Event Processing**: Transformation of raw replication events into structured data
- **Batching**: Efficient grouping of changes for destination systems

## Features

| Feature                  | Description                                |
| ------------------------ | ------------------------------------------ |
| `unknown-types-to-bytes` | Converts unknown PostgreSQL types to bytes (enabled by default) |
| `test-utils`             | Enables testing utilities and helpers      |
| `failpoints`             | Enables failure injection for testing      |

## Architecture

The ETL core is built around a pipeline architecture that processes PostgreSQL logical replication events:

1. **Connection**: Establishes connection to PostgreSQL with replication permissions
2. **Slot Creation**: Creates or connects to a logical replication slot
3. **Table Sync**: Performs initial synchronization of existing table data
4. **Stream Processing**: Continuously processes change events from the replication stream
5. **Destination Writing**: Applies changes to configured destination systems

## Usage

The core crate is typically used in conjunction with:
- `etl-destinations`: Destination implementations (BigQuery, etc.)
- `config`: Configuration management
- `postgres`: PostgreSQL utilities and connection handling

See the `etl-examples` crate for practical usage examples.