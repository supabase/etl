# Databend Destination Implementation

This document provides an overview of the Databend destination implementation for the ETL system.

## Implementation Summary

The Databend destination has been fully implemented following the same patterns as BigQuery and PostgreSQL destinations. This implementation enables real-time data replication from PostgreSQL to Databend with full CDC (Change Data Capture) support.

## Files Created

### Core Implementation (`src/databend/`)

1. **`mod.rs`** - Module exports and public API
   - Exports main types: `DatabendClient`, `DatabendDestination`, `DatabendDsn`
   - Exports utility function: `table_name_to_databend_table_id`

2. **`client.rs`** - Databend client wrapper 
   - `DatabendClient` struct with DSN-based connection
   - Table management: create, truncate, drop, check existence
   - Batch insert operations
   - Type mapping from PostgreSQL to Databend
   - Comprehensive unit tests

3. **`core.rs`** - Core destination implementation 
   - `DatabendDestination` implementing the `Destination` trait
   - Table versioning for TRUNCATE operations
   - View management for seamless table switching
   - CDC event handling (INSERT, UPDATE, DELETE)
   - Concurrent operation support
   - Comprehensive unit tests

4. **`encoding.rs`** - Data encoding logic 
   - Row-to-SQL value encoding
   - Type-specific encoding (primitives, strings, dates, JSON, arrays)
   - Special value handling (NaN, Infinity for floats)
   - Binary data encoding (hex format)
   - Comprehensive unit tests

5. **`validation.rs`** - Input validation
   - Column schema validation
   - Table name validation
   - Length and format checks
   - Comprehensive unit tests

### Tests (`tests/`)

1. **`databend_pipeline.rs`** - Integration tests 
   - `table_copy_and_streaming_with_restart` - Full pipeline test with restart
   - `table_insert_update_delete` - CDC operations test
   - `table_truncate` - TRUNCATE operation test
   - `concurrent_table_operations` - Concurrency test

2. **`support/databend.rs`** - Test utilities
   - `DatabendDatabase` - Test database wrapper
   - `DatabendRow` - Row result wrapper
   - Helper structs: `DatabendUser`, `DatabendOrder`
   - Setup and teardown utilities

### Documentation

1. **`docs/how-to/configure-databend.md`** - Complete configuration guide
   - Connection setup
   - Type mapping reference
   - CDC operations explanation
   - Performance tuning tips
   - Troubleshooting guide

## Features Implemented

### ✅ Core Functionality

- [x] DSN-based connection to Databend
- [x] Automatic table creation with schema mapping
- [x] Bulk data loading for initial sync
- [x] Real-time CDC event streaming
- [x] INSERT, UPDATE, DELETE operation handling
- [x] TRUNCATE operation with table versioning
- [x] View management for seamless table switching


### ✅ Testing

- [x] Unit tests for all modules
- [x] Integration tests with real Databend
- [x] Pipeline restart tests
- [x] CDC operation tests
- [x] Concurrent operation tests
- [x] Test utilities and helpers

### ✅ Documentation

- [x] Configuration guide
- [x] Type mapping reference
- [x] Usage examples
- [x] Troubleshooting guide
- [x] Best practices

## Design Patterns

The implementation follows the same architectural patterns as BigQuery and PostgreSQL destinations:

1. **Separation of Concerns**
   - `client.rs` - Low-level database operations
   - `core.rs` - High-level destination logic
   - `encoding.rs` - Data type conversions
   - `validation.rs` - Input validation

2. **Table Versioning**
   - Uses sequence numbers for table versions (e.g., `table_0`, `table_1`)
   - Views provide stable access points
   - Automatic cleanup of old versions

3. **Caching Strategy**
   - Created tables cache to avoid redundant existence checks
   - View mapping cache for efficient view updates
   - Schema caching through the store interface

4. **Error Handling**
   - Comprehensive error types with context
   - Graceful degradation for non-critical errors
   - Detailed error messages for debugging

5. **Concurrency**
   - Lock-free reads where possible
   - Minimal lock contention with Arc<Mutex<Inner>>
   - Async operations throughout

## Type Mapping Details

| PostgreSQL | Databend | Notes |
|------------|----------|-------|
| BOOLEAN | BOOLEAN | Direct mapping |
| SMALLINT | SMALLINT | 16-bit integer |
| INTEGER | INT | 32-bit integer |
| BIGINT | BIGINT | 64-bit integer |
| REAL | FLOAT | 32-bit float |
| DOUBLE PRECISION | DOUBLE | 64-bit float |
| NUMERIC | DECIMAL(38, 18) | High precision |
| TEXT/VARCHAR/CHAR | STRING | Variable length text |
| BYTEA | BINARY | Binary data as hex |
| DATE | DATE | Date only |
| TIME | STRING | No native TIME type |
| TIMESTAMP | TIMESTAMP | Without timezone |
| TIMESTAMPTZ | TIMESTAMP | With timezone info |
| UUID | STRING | String representation |
| JSON/JSONB | VARIANT | Databend's JSON type |
| Arrays | ARRAY(T) | Nested type support |

## CDC Operations

The implementation adds two special columns for CDC:

- `_etl_operation`: Operation type (INSERT, UPDATE, DELETE)
- `_etl_sequence`: Sequence number for ordering

These columns enable:
- Tracking of all changes
- Ordering of operations
- Deduplication in downstream systems
- Time-travel queries

## Performance Characteristics

1. **Initial Sync**
   - Batch inserts for high throughput
   - Parallel table synchronization
   - Efficient schema caching

2. **CDC Streaming**
   - Low-latency event processing
   - Batched inserts for efficiency
   - Minimal lock contention

3. **TRUNCATE Operations**
   - Zero-downtime table replacement
   - Automatic old table cleanup
   - View-based access maintains consistency

## Dependencies Added

```toml
databend-driver = "0.23"  # Official Databend Rust driver
hex = "0.4"               # For binary data encoding
```

## Testing Requirements

To run tests, set these environment variables:

```bash
export TESTS_DATABEND_DSN="databend://root:password@localhost:8000"
export TESTS_DATABEND_DATABASE="test_db"
```

Then run:

```bash
cargo test --features databend
```

## Comparison with Other Destinations

| Feature | BigQuery | Databend | PostgreSQL |
|---------|----------|----------|------------|
| Connection | Service Account Key | DSN | Connection String |
| Table Versioning | ✅ | ✅ | N/A |
| View Management | ✅ | ✅ | N/A |
| CDC Columns | ✅ | ✅ | ✅ |
| Array Support | ✅ | ✅ | ✅ |
| JSON Support | ✅ | ✅ (VARIANT) | ✅ |
| Concurrent Ops | ✅ | ✅ | ✅ |


## Usage Example

```rust
use etl_destinations::databend::DatabendDestination;
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let dsn = "databend://root:password@localhost:8000/mydb".to_string();
    let database = "mydb".to_string();
    let store = MemoryStore::new();

    // Create destination
    let destination = DatabendDestination::new(dsn, database, store.clone()).await?;

    // Use in pipeline
    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;

    Ok(())
}
```

## Support

For issues or questions:
- File an issue on GitHub
- Refer to the configuration guide in `docs/how-to/configure-databend.md`
- Check Databend documentation at https://docs.databend.com/
