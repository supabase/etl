# Iceberg Destination Development Plan for Production Readiness

## Executive Summary
This comprehensive plan outlines the development of a production-ready Apache Iceberg destination for the Supabase ETL framework. The implementation will provide feature parity with the existing BigQuery destination while leveraging Iceberg's unique capabilities for data lakehouse architectures.

## Phase 1: Foundation and Setup (Week 1-2)

### 1.1 Project Structure Setup
```
etl-destinations/src/iceberg/
├── mod.rs              # Public API and feature flags
├── core.rs             # Main Destination trait implementation
├── client.rs           # Iceberg catalog and table operations
├── encoding.rs         # Arrow/Parquet data conversion
├── writer.rs           # Iceberg writer management
├── config.rs           # Configuration structures
├── error.rs            # Error mapping and handling
├── schema.rs           # PostgreSQL to Iceberg schema mapping
├── partition.rs        # Partition strategy implementation
└── metrics.rs          # Telemetry and monitoring
```

### 1.2 Dependencies
```toml
[dependencies]
# Core Iceberg functionality
iceberg = { version = "0.3", features = ["arrow", "async"] }
iceberg-catalog-rest = "0.3"
iceberg-catalog-sql = "0.3"

# Data formats
arrow = { version = "53.0", features = ["prettyprint"] }
parquet = { version = "53.0", features = ["async", "arrow"] }

# Storage backends
object_store = { version = "0.11", features = ["aws", "gcp", "azure"] }
opendal = { version = "0.50", optional = true }

# Utilities
uuid = { version = "1.0", features = ["v4"] }
chrono = "0.4"
url = "2.5"
```

### 1.3 Configuration Structure
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    // Catalog configuration
    pub catalog: CatalogConfig,
    
    // Table location
    pub namespace: String,
    pub table_prefix: Option<String>,
    
    // Storage configuration
    pub storage: StorageConfig,
    
    // Performance tuning
    pub writer_config: WriterConfig,
    
    // CDC configuration
    pub cdc_config: CdcConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogConfig {
    Rest {
        uri: String,
        warehouse: String,
        credentials: Option<Credentials>,
    },
    Sql {
        uri: String,
        warehouse: String,
    },
    Glue {
        region: String,
        warehouse: String,
        credentials: AwsCredentials,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfig {
    pub target_file_size_bytes: usize,  // Default: 512MB
    pub max_open_files: usize,          // Default: 10
    pub compression: CompressionType,    // Default: Snappy
    pub batch_size: usize,              // Default: 1000
    pub commit_interval_ms: u64,        // Default: 60000
}
```

## Phase 2: Core Implementation (Week 3-5)

### 2.1 Destination Trait Implementation
```rust
pub struct IcebergDestination<S> {
    inner: Arc<Mutex<Inner<S>>>,
}

struct Inner<S> {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    storage: Arc<dyn ObjectStore>,
    writer_pool: WriterPool,
    store: S,
    table_cache: HashMap<TableId, Table>,
    metrics: IcebergMetrics,
}

impl<S> Destination for IcebergDestination<S>
where
    S: StateStore + SchemaStore,
{
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        // Use Iceberg's snapshot expiration for efficient truncation
        let table = self.get_or_create_table(table_id).await?;
        table.expire_snapshots()
            .older_than(SystemTime::now())
            .commit()
            .await?;
        Ok(())
    }
    
    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // Convert to Arrow RecordBatch and write via Iceberg writer
        let batch = self.rows_to_arrow(table_id, table_rows).await?;
        let writer = self.get_writer(table_id).await?;
        writer.write(batch).await?;
        Ok(())
    }
    
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        // Group events by table and operation type
        let grouped = self.group_events(events);
        
        for (table_id, ops) in grouped {
            match ops {
                Operations::Inserts(rows) => {
                    self.append_rows(table_id, rows).await?;
                }
                Operations::Updates(rows) => {
                    self.upsert_rows(table_id, rows).await?;
                }
                Operations::Deletes(keys) => {
                    self.delete_rows(table_id, keys).await?;
                }
            }
        }
        
        // Commit transaction
        self.commit_pending_writes().await?;
        Ok(())
    }
}
```

### 2.2 Schema Management
```rust
impl SchemaMapper {
    pub fn postgres_to_iceberg(pg_schema: &PgSchema) -> IcebergSchema {
        let fields = pg_schema.columns.iter().map(|col| {
            Field::new(
                col.name.clone(),
                map_postgres_type(&col.data_type),
                col.is_nullable,
            )
        }).collect();
        
        Schema::new(fields)
            .with_identifier_field_ids(pg_schema.primary_keys.clone())
    }
    
    fn map_postgres_type(pg_type: &str) -> IcebergType {
        match pg_type {
            "bigint" | "int8" => Type::Primitive(PrimitiveType::Long),
            "integer" | "int4" => Type::Primitive(PrimitiveType::Int),
            "smallint" | "int2" => Type::Primitive(PrimitiveType::Int),
            "text" | "varchar" => Type::Primitive(PrimitiveType::String),
            "timestamp" => Type::Primitive(PrimitiveType::TimestampTz),
            "date" => Type::Primitive(PrimitiveType::Date),
            "boolean" => Type::Primitive(PrimitiveType::Boolean),
            "float8" | "double precision" => Type::Primitive(PrimitiveType::Double),
            "float4" | "real" => Type::Primitive(PrimitiveType::Float),
            "uuid" => Type::Primitive(PrimitiveType::Uuid),
            "json" | "jsonb" => Type::Primitive(PrimitiveType::String),
            "bytea" => Type::Primitive(PrimitiveType::Binary),
            _ if pg_type.starts_with("array<") => {
                // Handle array types
                Type::List(Box::new(map_element_type(pg_type)))
            }
            _ => Type::Primitive(PrimitiveType::String), // Fallback
        }
    }
}
```

### 2.3 Writer Management
```rust
pub struct WriterPool {
    writers: Arc<Mutex<HashMap<TableId, TableWriter>>>,
    config: WriterConfig,
}

struct TableWriter {
    table: Table,
    parquet_writer: ArrowWriter<Vec<u8>>,
    current_batch: Vec<RecordBatch>,
    current_size: usize,
    last_commit: Instant,
}

impl WriterPool {
    async fn get_or_create_writer(&self, table_id: TableId) -> Arc<TableWriter> {
        // Implement writer pooling with automatic file rotation
    }
    
    async fn write_batch(&self, table_id: TableId, batch: RecordBatch) -> EtlResult<()> {
        let writer = self.get_or_create_writer(table_id).await;
        
        // Add CDC metadata columns
        let batch_with_cdc = add_cdc_columns(batch, operation_type, lsn)?;
        
        writer.write(batch_with_cdc).await?;
        
        // Check if we should rotate file
        if writer.should_rotate() {
            self.rotate_file(table_id).await?;
        }
        
        Ok(())
    }
    
    async fn commit_all(&self) -> EtlResult<()> {
        // Commit all pending writes across all tables
        for (table_id, writer) in self.writers.lock().await.iter_mut() {
            writer.commit().await?;
        }
        Ok(())
    }
}
```

## Phase 3: Advanced Features (Week 6-8)

### 3.1 Partition Strategy
```rust
pub struct PartitionStrategy {
    strategy: PartitionType,
}

enum PartitionType {
    Daily(String),           // Partition by day on column
    Monthly(String),         // Partition by month on column
    Hourly(String),         // Partition by hour on column
    Hash(String, u32),      // Hash partition on column with N buckets
    Custom(Box<dyn Fn(&Row) -> String>),
}

impl PartitionStrategy {
    pub fn apply(&self, batch: &RecordBatch) -> HashMap<String, RecordBatch> {
        // Split batch by partition
        match &self.strategy {
            PartitionType::Daily(column) => {
                // Group rows by day extracted from timestamp column
                self.partition_by_time(batch, column, ChronoUnit::Day)
            }
            PartitionType::Hash(column, buckets) => {
                // Hash partition implementation
                self.partition_by_hash(batch, column, *buckets)
            }
            // ... other strategies
        }
    }
}
```

### 3.2 Delete Operations (CDC)
```rust
impl IcebergDestination {
    async fn delete_rows(&self, table_id: TableId, keys: Vec<PrimaryKey>) -> EtlResult<()> {
        let table = self.get_table(table_id).await?;
        
        // Create equality delete file
        let delete_writer = table.new_equality_delete_writer()
            .with_equality_ids(table.schema().identifier_field_ids())
            .build()?;
        
        for key in keys {
            delete_writer.write(key.to_row()).await?;
        }
        
        let delete_file = delete_writer.close().await?;
        
        // Add to pending transaction
        self.add_delete_file(table_id, delete_file).await?;
        
        Ok(())
    }
}
```

### 3.3 Upsert Implementation
```rust
impl IcebergDestination {
    async fn upsert_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        // Strategy: Write as new data files + equality deletes for existing keys
        
        // Extract primary keys
        let keys: Vec<_> = rows.iter()
            .map(|row| extract_primary_key(row))
            .collect();
        
        // Write deletes for existing keys
        self.delete_rows(table_id, keys).await?;
        
        // Write new data
        self.append_rows(table_id, rows).await?;
        
        Ok(())
    }
}
```

## Phase 4: Production Features (Week 9-11)

### 4.1 Error Handling
```rust
#[derive(Debug, thiserror::Error)]
pub enum IcebergError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iceberg::Error),
    
    #[error("Storage error: {0}")]
    Storage(#[from] object_store::Error),
    
    #[error("Schema mismatch: expected {expected}, got {actual}")]
    SchemaMismatch { expected: String, actual: String },
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Write failed after {retries} retries: {error}")]
    WriteFailed { retries: u32, error: String },
}

impl From<IcebergError> for EtlError {
    fn from(err: IcebergError) -> Self {
        match err {
            IcebergError::TableNotFound(_) => EtlError::TableNotFound,
            IcebergError::Storage(_) => EtlError::StorageError,
            IcebergError::SchemaMismatch { .. } => EtlError::SchemaError,
            _ => EtlError::DestinationError(err.to_string()),
        }
    }
}

// Retry logic
pub struct RetryPolicy {
    max_retries: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
}

impl RetryPolicy {
    pub async fn execute<F, T>(&self, mut f: F) -> Result<T, IcebergError>
    where
        F: FnMut() -> Future<Output = Result<T, IcebergError>>,
    {
        let mut retries = 0;
        loop {
            match f().await {
                Ok(result) => return Ok(result),
                Err(e) if self.is_retryable(&e) && retries < self.max_retries => {
                    let delay = self.calculate_delay(retries);
                    tokio::time::sleep(delay).await;
                    retries += 1;
                }
                Err(e) => return Err(IcebergError::WriteFailed {
                    retries,
                    error: e.to_string(),
                }),
            }
        }
    }
}
```

### 4.2 Monitoring and Metrics
```rust
pub struct IcebergMetrics {
    // Counters
    rows_written: Counter,
    files_created: Counter,
    commits_successful: Counter,
    commits_failed: Counter,
    
    // Histograms
    write_duration: Histogram,
    commit_duration: Histogram,
    file_size_bytes: Histogram,
    batch_size: Histogram,
    
    // Gauges
    open_files: Gauge,
    pending_commits: Gauge,
    table_count: Gauge,
}

impl IcebergMetrics {
    pub fn record_write(&self, table: &str, rows: usize, duration: Duration) {
        self.rows_written.increment(rows as u64);
        self.write_duration.record(duration.as_millis() as f64);
        self.batch_size.record(rows as f64);
        
        tracing::info!(
            table = %table,
            rows = rows,
            duration_ms = duration.as_millis(),
            "Completed batch write to Iceberg"
        );
    }
    
    pub fn record_commit(&self, table: &str, success: bool, duration: Duration) {
        if success {
            self.commits_successful.increment(1);
        } else {
            self.commits_failed.increment(1);
        }
        self.commit_duration.record(duration.as_millis() as f64);
        
        tracing::info!(
            table = %table,
            success = success,
            duration_ms = duration.as_millis(),
            "Iceberg commit completed"
        );
    }
}
```

### 4.3 Logging Strategy
```rust
// Structured logging throughout
tracing::info!(
    table = %table_id,
    snapshot_id = %snapshot.snapshot_id(),
    manifest_count = manifest_list.len(),
    "Created new Iceberg snapshot"
);

tracing::debug!(
    table = %table_id,
    batch_size = batch.num_rows(),
    "Writing batch to Iceberg"
);

tracing::warn!(
    table = %table_id,
    error = %e,
    retry_attempt = attempt,
    "Retrying Iceberg write after error"
);

tracing::error!(
    table = %table_id,
    error = %e,
    "Failed to write to Iceberg table"
);
```

## Phase 5: Performance Optimization (Week 12-13)

### 5.1 Caching Strategy
```rust
pub struct TableCache {
    tables: Arc<RwLock<HashMap<TableId, CachedTable>>>,
    ttl: Duration,
}

struct CachedTable {
    table: Table,
    schema: Schema,
    last_refresh: Instant,
    writer: Option<Arc<TableWriter>>,
}

impl TableCache {
    async fn get_table(&self, table_id: &TableId) -> Option<Table> {
        let tables = self.tables.read().await;
        if let Some(cached) = tables.get(table_id) {
            if cached.last_refresh.elapsed() < self.ttl {
                return Some(cached.table.clone());
            }
        }
        None
    }
}
```

### 5.2 Batch Optimization
```rust
pub struct BatchOptimizer {
    target_batch_size: usize,
    max_batch_memory: usize,
}

impl BatchOptimizer {
    pub fn optimize_batches(&self, rows: Vec<TableRow>) -> Vec<RecordBatch> {
        // Sort by size and pack efficiently
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_size = 0;
        
        for row in rows {
            let row_size = self.estimate_row_size(&row);
            
            if current_size + row_size > self.max_batch_memory 
                || current_batch.len() >= self.target_batch_size {
                // Flush current batch
                batches.push(self.create_record_batch(current_batch));
                current_batch = Vec::new();
                current_size = 0;
            }
            
            current_size += row_size;
            current_batch.push(row);
        }
        
        if !current_batch.is_empty() {
            batches.push(self.create_record_batch(current_batch));
        }
        
        batches
    }
}
```

### 5.3 Compaction Strategy
```rust
pub struct CompactionManager {
    strategy: CompactionStrategy,
    executor: Arc<TaskExecutor>,
}

impl CompactionManager {
    pub async fn compact_table(&self, table: &Table) -> EtlResult<()> {
        // Run compaction in background
        let table_clone = table.clone();
        self.executor.spawn(async move {
            // Rewrite small files into larger ones
            let plan = table_clone.plan_compaction()
                .with_strategy(self.strategy.clone())
                .build()?;
            
            plan.execute().await?;
            
            tracing::info!(
                table = %table.name(),
                files_before = plan.files_before(),
                files_after = plan.files_after(),
                "Completed table compaction"
            );
            
            Ok(())
        });
        
        Ok(())
    }
}
```

## Phase 6: Testing and Integration (Week 14-15)

### 6.1 Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_schema_mapping() {
        let pg_schema = create_test_pg_schema();
        let iceberg_schema = SchemaMapper::postgres_to_iceberg(&pg_schema);
        
        assert_eq!(iceberg_schema.fields().len(), pg_schema.columns.len());
        // Verify type mappings
    }
    
    #[test]
    fn test_partition_strategy() {
        let strategy = PartitionStrategy::daily("created_at");
        let batch = create_test_batch();
        let partitions = strategy.apply(&batch);
        
        assert!(partitions.len() > 0);
        // Verify partitioning logic
    }
    
    #[tokio::test]
    async fn test_error_handling() {
        let destination = create_test_destination();
        let invalid_table = TableId::new("nonexistent");
        
        let result = destination.write_table_rows(invalid_table, vec![]).await;
        assert!(matches!(result, Err(EtlError::TableNotFound)));
    }
}
```

### 6.2 Integration Tests
```rust
#[tokio::test]
async fn test_end_to_end_replication() {
    // Setup
    let catalog = create_test_catalog().await;
    let destination = IcebergDestination::new(test_config(), catalog).await.unwrap();
    
    // Create table
    let schema = create_test_schema();
    destination.create_table("test_table", schema).await.unwrap();
    
    // Write initial data
    let rows = generate_test_rows(1000);
    destination.write_table_rows("test_table", rows).await.unwrap();
    
    // Write CDC events
    let events = generate_cdc_events();
    destination.write_events(events).await.unwrap();
    
    // Verify
    let table = catalog.load_table(&table_ident).await.unwrap();
    let scan = table.scan().build().unwrap();
    let batches = scan.to_arrow().await.unwrap();
    
    assert_eq!(count_rows(&batches), expected_count);
}
```

### 6.3 Performance Tests
```rust
#[tokio::test]
async fn benchmark_write_throughput() {
    let destination = create_test_destination().await;
    let rows = generate_large_dataset(1_000_000);
    
    let start = Instant::now();
    destination.write_table_rows("bench_table", rows).await.unwrap();
    let duration = start.elapsed();
    
    let throughput = 1_000_000.0 / duration.as_secs_f64();
    println!("Write throughput: {:.2} rows/sec", throughput);
    
    assert!(throughput > 10_000.0, "Throughput below threshold");
}
```

## Phase 7: CLI and Documentation (Week 16)

### 7.1 CLI Example
```rust
// etl-examples/examples/postgres_to_iceberg.rs
use etl::{Pipeline, Source, Destination};
use etl_destinations::iceberg::{IcebergDestination, IcebergConfig};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    // Parse arguments
    let args = Args::parse();
    
    // Configure source
    let source = PostgresSource::new(args.source_config)?;
    
    // Configure Iceberg destination
    let iceberg_config = IcebergConfig {
        catalog: CatalogConfig::Rest {
            uri: args.catalog_uri,
            warehouse: args.warehouse,
            credentials: load_credentials()?,
        },
        namespace: args.namespace,
        storage: StorageConfig::S3 {
            bucket: args.bucket,
            prefix: args.prefix,
            region: args.region,
        },
        writer_config: WriterConfig::default(),
        cdc_config: CdcConfig {
            enable_deletes: true,
            track_changes: true,
        },
    };
    
    let destination = IcebergDestination::new(iceberg_config).await?;
    
    // Run pipeline
    let pipeline = Pipeline::builder()
        .source(source)
        .destination(destination)
        .with_metrics()
        .build()?;
    
    pipeline.run().await?;
    
    Ok(())
}
```

### 7.2 Documentation
```markdown
# Iceberg Destination

## Overview
The Iceberg destination enables real-time replication from PostgreSQL to Apache Iceberg tables.

## Features
- Full CDC support (INSERT, UPDATE, DELETE)
- Automatic schema evolution
- Partition support
- Multiple catalog backends (REST, SQL, Glue)
- Multiple storage backends (S3, GCS, Azure)
- Compaction and optimization
- Comprehensive metrics and monitoring

## Configuration
[Detailed configuration guide]

## Performance Tuning
[Optimization strategies]

## Troubleshooting
[Common issues and solutions]
```

## Phase 8: Deployment and Operations (Week 17-18)

### 8.1 Docker Support
```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release --features iceberg

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/etl-replicator /usr/local/bin/
ENV RUST_LOG=info
CMD ["etl-replicator"]
```

### 8.2 Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: etl-iceberg-replicator
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: replicator
        image: etl-replicator:latest
        env:
        - name: ICEBERG_CATALOG_URI
          value: "http://iceberg-rest:8181"
        - name: S3_BUCKET
          value: "my-iceberg-bucket"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
```

### 8.3 Monitoring Setup
```yaml
# Prometheus scrape config
scrape_configs:
  - job_name: 'etl-iceberg'
    static_configs:
      - targets: ['etl-replicator:9090']
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: 'iceberg_.*'
        action: keep
```

## Timeline Summary

| Phase | Duration | Deliverables |
|-------|----------|-------------|
| Phase 1: Foundation | 2 weeks | Project structure, dependencies, configuration |
| Phase 2: Core Implementation | 3 weeks | Destination trait, schema mapping, writer management |
| Phase 3: Advanced Features | 3 weeks | Partitions, deletes, upserts |
| Phase 4: Production Features | 3 weeks | Error handling, monitoring, logging |
| Phase 5: Performance | 2 weeks | Caching, batching, compaction |
| Phase 6: Testing | 2 weeks | Unit, integration, performance tests |
| Phase 7: CLI & Docs | 1 week | Examples, documentation |
| Phase 8: Deployment | 2 weeks | Docker, Kubernetes, monitoring |

**Total: 18 weeks for production-ready implementation**

## Risk Mitigation

### Technical Risks
1. **Iceberg-rust maturity**: Use stable features only, contribute fixes upstream
2. **Performance bottlenecks**: Implement comprehensive benchmarking early
3. **Schema evolution complexity**: Extensive testing of edge cases

### Operational Risks
1. **Data consistency**: Implement checksums and validation
2. **Storage costs**: Implement aggressive compaction strategies
3. **Monitoring gaps**: Comprehensive metrics from day one

## Success Criteria

1. **Functional Requirements**
   - ✅ Full CDC support (INSERT, UPDATE, DELETE, TRUNCATE)
   - ✅ Automatic schema evolution
   - ✅ Partition support
   - ✅ Multiple catalog backends

2. **Performance Requirements**
   - ✅ >10,000 rows/second throughput
   - ✅ <1 minute end-to-end latency
   - ✅ <5% storage overhead vs raw Parquet

3. **Operational Requirements**
   - ✅ 99.9% uptime SLA
   - ✅ Comprehensive monitoring
   - ✅ Automated error recovery
   - ✅ Production documentation

## Conclusion

This plan provides a comprehensive roadmap for implementing a production-ready Iceberg destination that matches or exceeds the capabilities of the existing BigQuery destination. The phased approach ensures incremental delivery while maintaining high quality and production readiness standards.