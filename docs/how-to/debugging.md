---
type: how-to
audience: developers, operators
prerequisites:
  - Basic understanding of ETL pipelines
  - Access to PostgreSQL and ETL logs
  - Familiarity with ETL configuration
version_last_tested: 0.1.0
last_reviewed: 2025-01-14
risk_level: low
---

# Debug Pipeline Issues

**Diagnose and resolve common ETL pipeline problems quickly and systematically**

This guide helps you identify, diagnose, and fix issues with ETL pipelines using a structured troubleshooting approach.

## Goal

Learn to systematically debug ETL issues:

- Identify the source of pipeline problems
- Use logging and monitoring to diagnose issues
- Apply appropriate fixes for common failure patterns
- Prevent similar issues in the future

## Prerequisites

- Running ETL pipeline (even if failing)
- Access to PostgreSQL server and logs
- ETL application logs and configuration
- Basic SQL knowledge for diagnostic queries

## Decision Points

**Choose your debugging approach based on symptoms:**

| Symptom | Most Likely Cause | Start Here |
|---------|-------------------|------------|
| Pipeline won't start | Configuration/connection issues | [Connection Problems](#connection-problems) |
| Pipeline starts but no data | Publication/replication setup | [Replication Issues](#replication-issues) |
| Pipeline stops unexpectedly | Resource/permission problems | [Runtime Failures](#runtime-failures) |
| Data missing or incorrect | Schema/destination issues | [Data Quality Problems](#data-quality-problems) |
| Slow performance | Batching/network issues | [Performance Issues](#performance-issues) |

## Systematic Debugging Process

### Step 1: Gather Information

Before diving into fixes, collect diagnostic information:

**Check ETL logs:**
```bash
# If using structured logging
grep -E "(ERROR|FATAL|PANIC)" etl.log | tail -20

# Look for specific patterns
grep "connection" etl.log
grep "replication slot" etl.log  
grep "publication" etl.log
```

**Check PostgreSQL logs:**
```sql
-- Recent PostgreSQL errors
SELECT pg_current_logfile();
-- Then check that file for errors around your pipeline start time
```

**Collect system information:**
```sql
-- Check replication slots
SELECT slot_name, slot_type, active, confirmed_flush_lsn 
FROM pg_replication_slots;

-- Check publications
SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete
FROM pg_publication;

-- Check database connections
SELECT pid, usename, application_name, state, query_start
FROM pg_stat_activity 
WHERE application_name LIKE '%etl%';
```

### Step 2: Identify the Problem Category

Use this decision tree to narrow down the issue:

```
Pipeline fails to start?
├─ YES → Connection Problems
└─ NO → Pipeline starts but...
    ├─ No data flowing → Replication Issues  
    ├─ Pipeline crashes → Runtime Failures
    ├─ Wrong/missing data → Data Quality Problems
    └─ Slow performance → Performance Issues
```

## Common Problem Categories

### Connection Problems

**Symptoms:**
- "Connection refused" errors
- "Authentication failed" errors  
- "Database does not exist" errors
- Pipeline exits immediately on startup

**Diagnosis:**

```bash
# Test basic connection
psql -h your-host -p 5432 -U etl_user -d your_db -c "SELECT 1;"

# Test from ETL server specifically
# (run this from where ETL runs)
telnet your-host 5432
```

**Common causes and fixes:**

| Error Message | Cause | Fix |
|--------------|-------|-----|
| "Connection refused" | PostgreSQL not running or firewall | Check `systemctl status postgresql` and firewall rules |
| "Authentication failed" | Wrong password/user | Verify credentials and `pg_hba.conf` |  
| "Database does not exist" | Wrong database name | Check database name in connection string |
| "SSL required" | TLS configuration mismatch | Update `TlsConfig` to match server requirements |

### Replication Issues

**Symptoms:**
- Pipeline starts successfully but no data flows
- "Publication not found" errors
- "Replication slot already exists" errors
- Initial sync never completes

**Diagnosis:**

```sql
-- Check if publication exists and has tables
SELECT schemaname, tablename 
FROM pg_publication_tables 
WHERE pubname = 'your_publication_name';

-- Check if replication slot is active
SELECT slot_name, active, confirmed_flush_lsn
FROM pg_replication_slots 
WHERE slot_name = 'your_slot_name';

-- Check table permissions
SELECT grantee, table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'etl_user' AND table_name = 'your_table';
```

**Common fixes:**

**Publication doesn't exist:**
```sql
CREATE PUBLICATION your_publication FOR TABLE table1, table2;
```

**No tables in publication:**
```sql
-- Add tables to existing publication
ALTER PUBLICATION your_publication ADD TABLE missing_table;
```

**Permission denied on tables:**
```sql
GRANT SELECT ON TABLE your_table TO etl_user;
```

**Stale replication slot:**
```sql
-- Drop and recreate (will lose position)
SELECT pg_drop_replication_slot('stale_slot_name');
```

### Runtime Failures

**Symptoms:**
- Pipeline runs for a while then crashes
- "Out of memory" errors
- "Too many open files" errors
- Destination write failures

**Diagnosis:**

```bash
# Check system resources
htop  # or top
df -h  # disk space
ulimit -n  # file descriptor limit

# Check ETL memory usage
ps aux | grep etl
```

**Common fixes:**

**Memory issues:**
```rust
// Reduce batch sizes in configuration
BatchConfig {
    max_size: 500,  // Reduce from 1000+
    max_fill_ms: 2000,
}
```

**File descriptor limits:**
```bash
# Temporary fix
ulimit -n 10000

# Permanent fix (add to /etc/security/limits.conf)
etl_user soft nofile 65536
etl_user hard nofile 65536
```

**Destination timeouts:**
```rust
// Add retry configuration or connection pooling
// Check destination system health and capacity
```

### Data Quality Problems

**Symptoms:**
- Some rows missing in destination
- Data appears corrupted or truncated
- Schema mismatch errors
- Timestamp/timezone issues

**Diagnosis:**

```sql
-- Compare row counts between source and destination
SELECT COUNT(*) FROM source_table;
-- vs destination count

-- Check for recent schema changes
SELECT schemaname, tablename, attname, atttypid 
FROM pg_attribute 
JOIN pg_class ON attrelid = oid 
JOIN pg_namespace ON relnamespace = pg_namespace.oid
WHERE schemaname = 'public' AND tablename = 'your_table';

-- Check for problematic data types
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'your_table'
  AND data_type IN ('json', 'jsonb', 'text', 'bytea');
```

**Common fixes:**

**Schema evolution:**
```sql
-- Restart pipeline after schema changes
-- ETL will detect and adapt to new schema
```

**Data type issues:**
```rust
// Enable feature flag for unknown types
etl = { git = "https://github.com/supabase/etl", features = ["unknown-types-to-bytes"] }
```

**Character encoding problems:**
```sql
-- Check database encoding
SHOW server_encoding;
SHOW client_encoding;
```

### Performance Issues

**Symptoms:**
- Very slow initial sync
- High replication lag
- High CPU/memory usage
- Destination write bottlenecks

**Diagnosis:**

```sql
-- Monitor replication lag
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
FROM pg_replication_slots;

-- Check WAL generation rate  
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal;

-- Monitor long-running queries
SELECT pid, now() - pg_stat_activity.query_start AS duration, query
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';
```

**Performance tuning:**

```rust
// Optimize batch configuration
PipelineConfig {
    batch: BatchConfig {
        max_size: 2000,  // Increase batch size
        max_fill_ms: 10000,  // Allow longer batching
    },
    max_table_sync_workers: 8,  // Increase parallelism
    // ... other config
}
```

```sql
-- PostgreSQL tuning
-- In postgresql.conf:
-- shared_buffers = 1GB
-- effective_cache_size = 4GB
-- wal_buffers = 16MB
-- checkpoint_completion_target = 0.9
```

## Advanced Debugging Techniques

### Enable Debug Logging

**For ETL:**
```bash
# Set environment variable
export ETL_LOG_LEVEL=debug

# Or in configuration
RUST_LOG=etl=debug cargo run
```

**For PostgreSQL:**
```sql
-- Temporarily enable detailed logging
SET log_statement = 'all';
SET log_min_duration_statement = 0;
```

### Monitor Replication in Real-Time

```sql
-- Create a monitoring query
WITH replication_status AS (
    SELECT 
        slot_name,
        active,
        pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag_size,
        extract(EPOCH FROM (now() - pg_stat_replication.reply_time))::int as lag_seconds
    FROM pg_replication_slots 
    LEFT JOIN pg_stat_replication ON slot_name = application_name
    WHERE slot_name LIKE '%etl%'
)
SELECT * FROM replication_status;
```

### Test Individual Components

**Test publication setup:**
```sql
-- Simulate ETL's publication query
SELECT schemaname, tablename 
FROM pg_publication_tables 
WHERE pubname = 'your_publication';
```

**Test replication slot consumption:**
```sql
-- Create a test logical replication session
SELECT * FROM pg_logical_slot_get_changes('your_slot', NULL, NULL, 'pretty-print', '1');
```

### Memory and Resource Analysis

```bash
# Monitor ETL resource usage over time
while true; do
    echo "$(date): $(ps -o pid,vsz,rss,pcpu -p $(pgrep etl))"
    sleep 30
done >> etl_resources.log

# Analyze memory patterns
cat etl_resources.log | grep -E "RSS|VSZ" | tail -20
```

## Prevention Best Practices

### Configuration Validation

```rust
// Always validate configuration before starting
impl PipelineConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.batch.max_size > 10000 {
            return Err(ConfigError::BatchSizeTooLarge);
        }
        // ... other validations
    }
}
```

### Health Checks

```rust
// Implement health check endpoints
async fn health_check() -> Result<HealthStatus, Error> {
    // Check PostgreSQL connection
    // Check replication slot status
    // Check destination connectivity
    // Return overall status
}
```

### Monitoring and Alerting

```sql
-- Set up monitoring queries to run periodically
-- Alert on:
-- - Replication lag > 1GB or 5 minutes
-- - Inactive replication slots
-- - Failed pipeline restarts
-- - Unusual error rates
```

## Recovery Procedures

### Recovering from WAL Position Loss

```sql
-- If replication slot is lost, you may need to recreate
-- WARNING: This will cause a full resync
SELECT pg_create_logical_replication_slot('new_slot_name', 'pgoutput');
```

### Handling Destination Failures

```rust
// ETL typically handles this automatically with retries
// For manual intervention:
// 1. Fix destination issues
// 2. ETL will resume from last known WAL position
// 3. May see duplicate data (destinations should handle this)
```

### Schema Change Recovery

```sql
-- After schema changes, ETL usually adapts automatically
-- If not, restart the pipeline to force schema refresh
```

## Getting Help

When you need additional support:

1. **Search existing issues:** Check [GitHub issues](https://github.com/supabase/etl/issues)
2. **Collect diagnostic information:** Use queries and commands from this guide
3. **Prepare a minimal reproduction:** Isolate the problem to its essential parts
4. **Open an issue:** Include PostgreSQL version, ETL version, configuration, and logs

### Information to Include in Bug Reports

- ETL version and build information
- PostgreSQL version and configuration relevant settings
- Complete error messages and stack traces
- Configuration files (with sensitive information redacted)
- Steps to reproduce the issue
- Expected vs. actual behavior

## Next Steps

After resolving your immediate issue:

- **Optimize performance** → [Performance Tuning](performance/)
- **Implement monitoring** → [Monitoring best practices](../explanation/monitoring/)
- **Plan for schema changes** → [Schema Change Handling](schema-changes/)
- **Understand the architecture** → [ETL Architecture](../explanation/architecture/)

## See Also

- [PostgreSQL setup guide](configure-postgres/) - Prevent configuration issues
- [Performance optimization](performance/) - Tune for better throughput
- [ETL architecture](../explanation/architecture/) - Understand system behavior