# ETL Destination Implementation Structure Document

## Overview
This document defines the structure and components required to implement a new data sync destination in the ETL framework. It's based on the BigQuery implementation and serves as a template for creating production-ready destinations.

## Core Components Required

### 1. Destination Trait Implementation
Every destination must implement the `Destination` trait from `etl/src/destination/base.rs`:

```rust
pub trait Destination {
    // Truncate a table (may involve versioning strategy)
    fn truncate_table(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
    
    // Write bulk rows during initial table sync
    fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> impl Future<Output = EtlResult<()>> + Send;
    
    // Write CDC events during streaming replication
    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = EtlResult<()>> + Send;
}
```

### 2. Module Structure
```
destination_name/
├── mod.rs          # Public API and exports
├── core.rs         # Main Destination trait implementation
├── client.rs       # Target system client and operations
├── encoding.rs     # Data format conversion and serialization
├── config.rs       # Configuration structures
└── error.rs        # Error mapping and handling
```

### 3. Core Destination Structure
```rust
pub struct MyDestination<S> {
    inner: Arc<Mutex<Inner<S>>>,
}

struct Inner<S> {
    client: MyClient,                    // Target system client
    config: MyConfig,                     // Configuration
    store: S,                            // StateStore + SchemaStore
    cache: MyCache,                      // Performance optimizations
}
```

## Required Functionality

### Schema Management
- **Schema Translation**: PostgreSQL types → Target system types
- **Table Creation**: DDL generation from source schema
- **Schema Evolution**: Handle ALTER TABLE operations
- **Column Mapping**: Name and type transformations

### Data Operations
- **Bulk Loading**: Efficient initial data sync
- **CDC Streaming**: Real-time change data capture
- **Truncate Handling**: Table versioning or clearing strategy
- **Transaction Semantics**: Consistency guarantees

### CDC Metadata
Every destination should add CDC metadata columns:
- `_CHANGE_TYPE`: Operation type (INSERT/UPDATE/DELETE/UPSERT)
- `_LSN`: Log sequence number for ordering
- `_TIMESTAMP`: Event timestamp

## Production Requirements

### Error Handling
```rust
// Comprehensive error mapping
impl From<TargetError> for EtlError {
    fn from(err: TargetError) -> Self {
        match err {
            TargetError::NotFound => EtlError::TableNotFound,
            TargetError::PermissionDenied => EtlError::PermissionDenied,
            TargetError::ConnectionFailed => EtlError::ConnectionError,
            // ... comprehensive mapping
        }
    }
}
```

### Retry Logic
- **Transient Errors**: Automatic retry with exponential backoff
- **Fatal Errors**: Clean failure and error reporting
- **Resource Creation**: Retry table creation on permission errors
- **Connection Management**: Connection pooling and recovery

### Performance Optimizations
- **Batching**: Efficient grouping of operations
- **Caching**: Table existence and metadata caching
- **Parallelism**: Concurrent operations where safe
- **Resource Pooling**: Connection and thread management

### Monitoring & Observability
```rust
// Structured logging
tracing::info!(
    table = %table_id,
    rows = row_count,
    duration = ?elapsed,
    "Completed bulk write"
);

// Metrics collection
metrics.record_rows_written(table_id, row_count);
metrics.record_operation_duration("write_rows", elapsed);

// Error context
.with_context(|| format!("Failed to write {} rows to {}", count, table))?
```

### Configuration
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyDestinationConfig {
    // Connection parameters
    endpoint: String,
    
    // Authentication
    credentials: SerializableSecretString,
    
    // Performance tuning
    batch_size: usize,
    parallelism: usize,
    
    // Target-specific options
    custom_options: MyOptions,
}
```

## Integration Points

### State Management
The destination receives a store implementing:
- **StateStore**: Track table mappings and sync progress
- **SchemaStore**: Access table schemas and metadata

### Configuration Integration
```rust
// Add to DestinationConfig enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DestinationConfig {
    BigQuery { /* ... */ },
    MyDestination {
        config: MyDestinationConfig,
    },
}

// Implement conversion
impl DestinationConfig {
    pub async fn to_destination(self, store: S) -> EtlResult<impl Destination> {
        match self {
            Self::MyDestination { config } => {
                MyDestination::new(config, store).await
            }
            // ...
        }
    }
}
```

### Feature Flag
```toml
# Cargo.toml
[features]
my-destination = ["dep:my-client-crate"]
```

## Testing Requirements

### Unit Tests
- Schema translation correctness
- Error mapping validation
- Configuration parsing
- Data encoding/decoding

### Integration Tests
```rust
#[tokio::test]
async fn test_destination_operations() {
    // Setup
    let destination = create_test_destination().await;
    let test_data = generate_test_data();
    
    // Test table creation
    destination.create_table(schema).await.unwrap();
    
    // Test bulk write
    destination.write_table_rows(table_id, rows).await.unwrap();
    
    // Test CDC events
    destination.write_events(events).await.unwrap();
    
    // Verify
    assert_destination_state(&destination, expected).await;
}
```

### Test Utilities
- Mock client for unit testing
- Test data generators
- Validation helpers
- Environment setup/teardown

## CLI Integration

### Example Binary
Create `etl-examples/examples/my_destination.rs`:
```rust
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize
    let config = load_config()?;
    let destination = create_destination(config).await?;
    
    // Run pipeline
    let pipeline = EtlPipeline::new(source, destination);
    pipeline.run().await?;
}
```

### Documentation
- Configuration examples
- Setup instructions
- Performance tuning guide
- Troubleshooting section

## Checklist for New Destination

### Core Implementation
- [ ] Implement Destination trait
- [ ] Create client for target system
- [ ] Implement schema translation
- [ ] Add data encoding/serialization
- [ ] Handle CDC metadata

### Production Features
- [ ] Comprehensive error mapping
- [ ] Retry logic with backoff
- [ ] Connection pooling
- [ ] Performance caching
- [ ] Batch optimization
- [ ] Resource cleanup

### Observability
- [ ] Structured logging
- [ ] Metrics collection
- [ ] Error context
- [ ] Debug information
- [ ] Performance tracking

### Testing
- [ ] Unit test coverage
- [ ] Integration tests
- [ ] Performance benchmarks
- [ ] Error scenario tests
- [ ] Example implementation

### Documentation
- [ ] Configuration guide
- [ ] Setup instructions
- [ ] API documentation
- [ ] Performance guide
- [ ] Troubleshooting

### Integration
- [ ] Feature flag
- [ ] Config enum variant
- [ ] CLI example
- [ ] CI/CD integration
- [ ] Release notes