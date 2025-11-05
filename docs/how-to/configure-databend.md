# Configure Databend Destination

This guide explains how to configure the ETL system to replicate data to a Databend database.

## Overview

The Databend destination enables real-time data synchronization from PostgreSQL to Databend using Change Data Capture (CDC). It supports:

- Initial table synchronization with bulk data loading
- Real-time streaming of INSERT, UPDATE, and DELETE operations
- Automatic table schema creation and management
- TRUNCATE operation handling with table versioning
- Concurrent data processing for high throughput

## Prerequisites

- A running Databend instance (self-hosted or Databend Cloud)
- PostgreSQL source database with logical replication enabled
- Network connectivity between ETL system and Databend

## Connection Configuration

### DSN Format

The Databend destination uses a DSN (Data Source Name) connection string:

```
databend://<user>:<password>@<host>:<port>/<database>?<params>
```

### Example Configurations

**Self-hosted Databend:**
```rust
let dsn = "databend://root:password@localhost:8000?sslmode=disable/my_database";
```

**Databend Cloud:**
```rust
let dsn = "databend://user:password@tenant.databend.cloud:443/database?warehouse=my_warehouse";
```

## Basic Usage

### Creating a Databend Destination

```rust
use etl_destinations::databend::DatabendDestination;
use etl::store::both::memory::MemoryStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = "databend://root:password@localhost:8000/my_db".to_string();
    let database = "my_db".to_string();

    let store = MemoryStore::new();
    let destination = DatabendDestination::new(dsn, database, store).await?;

    // Use destination in your ETL pipeline
    Ok(())
}
```

### Using with Pipeline

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};
use etl_destinations::databend::DatabendDestination;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure PostgreSQL source
    let pg_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "source_db".to_string(),
        username: "postgres".to_string(),
        password: Some("password".to_string().into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
    };

    // Configure Databend destination
    let dsn = "databend://root:password@localhost:8000/target_db".to_string();
    let database = "target_db".to_string();
    let store = MemoryStore::new();
    let destination = DatabendDestination::new(dsn, database, store.clone()).await?;

    // Configure pipeline
    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: pg_config,
        batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };

    // Create and start pipeline
    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;

    Ok(())
}
```

## Type Mapping

The Databend destination automatically maps PostgreSQL types to Databend types:

| PostgreSQL Type | Databend Type | Notes |
|----------------|---------------|-------|
| BOOLEAN | BOOLEAN | |
| SMALLINT | SMALLINT | |
| INTEGER | INT | |
| BIGINT | BIGINT | |
| REAL | FLOAT | |
| DOUBLE PRECISION | DOUBLE | |
| NUMERIC/DECIMAL | DECIMAL(38, 18) | |
| TEXT, VARCHAR, CHAR | STRING | |
| BYTEA | BINARY | |
| DATE | DATE | |
| TIME | STRING | Databend doesn't have native TIME type |
| TIMESTAMP | TIMESTAMP | |
| TIMESTAMPTZ | TIMESTAMP | |
| UUID | STRING | Stored as string representation |
| JSON, JSONB | VARIANT | Databend's JSON type |
| Arrays | ARRAY(element_type) | Recursive type mapping |

## CDC Operations

### Change Tracking

The destination tracks CDC operations using special columns:

- `_etl_operation`: Operation type (INSERT, UPDATE, DELETE)
- `_etl_sequence`: Sequence number for ordering

### Truncate Handling

When a TRUNCATE operation occurs:

1. A new versioned table is created (e.g., `table_name_1`, `table_name_2`)
2. A view is updated to point to the new table
3. The old table is asynchronously dropped

This ensures data integrity and allows for safe truncate operations.

## Testing

### Environment Variables

Set these environment variables for testing:

```bash
export TESTS_DATABEND_DSN="databend://root:password@localhost:8000"
export TESTS_DATABEND_DATABASE="test_db"
```

### Running Tests

```bash
# Run Databend-specific tests
cargo test --features databend

# Run all destination tests
cargo test --features databend,bigquery,iceberg
```

## Performance Tuning

### Connection Pooling

The Databend driver handles connection pooling internally. No additional configuration is needed.

### Batch Size

Adjust the batch configuration for optimal performance:

```rust
let config = PipelineConfig {
    // ... other config
    batch: BatchConfig {
        max_size: 5000,      // Larger batches for better throughput
        max_fill_ms: 10000,  // Wait longer to fill batches
    },
    max_table_sync_workers: 8,  // More workers for parallel sync
};
```

## Troubleshooting

### Connection Issues

**Problem:** "Failed to connect to Databend"

**Solutions:**
- Verify the DSN format is correct
- Check network connectivity to Databend
- Ensure Databend is running and accessible
- Verify credentials are correct

### Type Conversion Errors

**Problem:** "Failed to encode cell"

**Solutions:**
- Check for unsupported PostgreSQL types
- Verify data values are within Databend's limits
- Review the type mapping table above

### Performance Issues

**Problem:** Slow data replication

**Solutions:**
- Increase batch size in configuration
- Use more sync workers for parallel processing
- Optimize Databend table indexes
- Consider data compression in Databend

## Best Practices

1. **Schema Design**
   - Use appropriate column types for your data
   - Add indexes on frequently queried columns
   - Consider partitioning for large tables

2. **Monitoring**
   - Monitor replication lag using metrics
   - Track error rates and retry counts
   - Set up alerts for prolonged failures

3. **Resource Management**
   - Adjust worker counts based on available resources
   - Monitor memory usage during large table syncs
   - Use appropriate batch sizes for your workload

4. **Data Consistency**
   - Verify data after initial sync
   - Monitor CDC event processing
   - Test truncate operations in non-production first

## Security

### Connection Security

For production deployments, use secure connections:

```rust
let dsn = "databend+https://user:password@host:443/database?ssl=true";
```

### Credential Management

Store credentials securely:
- Use environment variables
- Use secret management systems (HashiCorp Vault, AWS Secrets Manager)
- Never commit credentials to version control

## Additional Resources

- [Databend Documentation](https://docs.databend.com/)
- [ETL Architecture Guide](../explanation/architecture.md)
- [Destination Trait Documentation](https://docs.rs/etl)

## Support

For issues or questions:
- [GitHub Issues](https://github.com/supabase/etl/issues)
- [Databend Community](https://databend.com/community)
