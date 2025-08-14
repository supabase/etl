---
type: how-to
audience: developers, database administrators
prerequisites:
  - PostgreSQL server access with superuser privileges
  - Understanding of PostgreSQL configuration
  - Knowledge of PostgreSQL user management
version_last_tested: 0.1.0
last_reviewed: 2025-01-14
risk_level: medium
---

# Configure PostgreSQL for Replication

**Set up PostgreSQL with the correct permissions and settings for ETL logical replication**

This guide walks you through configuring PostgreSQL to support logical replication for ETL, including WAL settings, user permissions, and publication setup.

## Goal

Configure PostgreSQL to:

- Enable logical replication at the server level
- Create appropriate user accounts with minimal required permissions
- Set up publications for the tables you want to replicate
- Configure replication slots for reliable WAL consumption

## Prerequisites

- PostgreSQL 12 or later
- Superuser access to the PostgreSQL server
- Ability to restart PostgreSQL server (for configuration changes)
- Network connectivity from ETL to PostgreSQL

## Decision Points

**Choose your approach based on your environment:**

| Environment | Security Level | Recommended Setup |
|-------------|----------------|-------------------|
| **Development** | Low | Single superuser account |
| **Staging** | Medium | Dedicated replication user with specific permissions |
| **Production** | High | Least-privilege user with row-level security |

## Configuration Steps

### Step 1: Enable Logical Replication

Edit your PostgreSQL configuration file (usually `postgresql.conf`):

```ini
# Enable logical replication
wal_level = logical

# Increase max replication slots (default is 10)
max_replication_slots = 20

# Increase max WAL senders (default is 10)  
max_wal_senders = 20

# Optional: Increase checkpoint segments for better performance
checkpoint_segments = 32
checkpoint_completion_target = 0.9
```

**If using PostgreSQL 13+**, also consider:

```ini
# Enable publication of truncate operations (optional)
wal_sender_timeout = 60s

# Improve WAL retention for catching up
wal_keep_size = 1GB
```

**Restart PostgreSQL** to apply these settings:

```bash
# On systemd systems
sudo systemctl restart postgresql

# On other systems
sudo pg_ctl restart -D /path/to/data/directory
```

### Step 2: Create a Replication User

Create a dedicated user with appropriate permissions:

```sql
-- Create replication user
CREATE USER etl_replicator WITH PASSWORD 'secure_password_here';

-- Grant replication privileges
ALTER USER etl_replicator REPLICATION;

-- Grant connection privileges
GRANT CONNECT ON DATABASE your_database TO etl_replicator;

-- Grant schema usage (adjust schema names as needed)
GRANT USAGE ON SCHEMA public TO etl_replicator;

-- Grant select on specific tables (more secure than all tables)
GRANT SELECT ON TABLE users, orders, products TO etl_replicator;

-- Alternative: Grant select on all tables in schema (less secure but easier)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl_replicator;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO etl_replicator;
```

### Step 3: Configure Connection Security  

**For development (less secure):**

Edit `pg_hba.conf` to allow connections:

```
# Allow local connections with password
host    your_database    etl_replicator    localhost               md5

# Allow connections from specific IP range
host    your_database    etl_replicator    10.0.0.0/8             md5
```

**For production (more secure):**

Use SSL/TLS connections:

```
# Require SSL connections
hostssl your_database    etl_replicator    10.0.0.0/8             md5
```

Reload PostgreSQL configuration:

```sql
SELECT pg_reload_conf();
```

### Step 4: Create Publications

Connect as a superuser or table owner and create publications:

```sql
-- Create publication for specific tables
CREATE PUBLICATION etl_publication FOR TABLE users, orders, products;

-- Alternative: Create publication for all tables (use with caution)
-- CREATE PUBLICATION etl_publication FOR ALL TABLES;

-- View existing publications
SELECT * FROM pg_publication;

-- View tables in a publication
SELECT * FROM pg_publication_tables WHERE pubname = 'etl_publication';
```

### Step 5: Test the Configuration

Verify your setup works:

```sql
-- Test replication slot creation (as etl_replicator user)
SELECT pg_create_logical_replication_slot('test_slot', 'pgoutput');

-- Verify the slot was created
SELECT * FROM pg_replication_slots WHERE slot_name = 'test_slot';

-- Clean up test slot
SELECT pg_drop_replication_slot('test_slot');
```

### Step 6: Configure ETL Connection

Update your ETL configuration to use the new setup:

```rust
use etl::config::{PgConnectionConfig, TlsConfig};

let pg_config = PgConnectionConfig {
    host: "your-postgres-server.com".to_string(),
    port: 5432,
    name: "your_database".to_string(),
    username: "etl_replicator".to_string(),
    password: Some("secure_password_here".into()),
    tls: TlsConfig {
        enabled: true,  // Enable for production
        trusted_root_certs: "/path/to/ca-certificates.crt".to_string(),
    },
};
```

## Validation

Verify your configuration:

### Test 1: Connection Test

```bash
# Test connection from ETL server
psql -h your-postgres-server.com -p 5432 -U etl_replicator -d your_database -c "SELECT 1;"
```

### Test 2: Replication Permissions

```sql
-- As etl_replicator user, verify you can:
-- 1. Create replication slots
SELECT pg_create_logical_replication_slot('validation_slot', 'pgoutput');

-- 2. Read from tables in the publication
SELECT COUNT(*) FROM users;

-- 3. Access publication information
SELECT * FROM pg_publication_tables WHERE pubname = 'etl_publication';

-- Clean up
SELECT pg_drop_replication_slot('validation_slot');
```

### Test 3: ETL Pipeline Test

Run a simple ETL pipeline to verify end-to-end functionality:

```rust
// Use your configuration to create a test pipeline
// This should complete initial sync successfully
```

## Troubleshooting

### "ERROR: logical decoding requires wal_level >= logical"

**Solution:** Update `postgresql.conf` with `wal_level = logical` and restart PostgreSQL.

### "ERROR: permission denied to create replication slot"

**Solutions:**
- Ensure user has `REPLICATION` privilege: `ALTER USER etl_replicator REPLICATION;`
- Check if you're connecting to the right database
- Verify `pg_hba.conf` allows the connection

### "ERROR: publication does not exist"

**Solutions:**
- Verify publication name matches exactly: `SELECT * FROM pg_publication;`
- Ensure you're connected to the correct database
- Check if publication was created by another user

### "Connection refused" or timeout issues

**Solutions:**
- Check `postgresql.conf` has `listen_addresses = '*'` (or specific IPs)
- Verify `pg_hba.conf` allows your connection
- Check firewall settings on PostgreSQL server
- Confirm PostgreSQL is running: `sudo systemctl status postgresql`

### "ERROR: too many replication slots"

**Solutions:**
- Increase `max_replication_slots` in `postgresql.conf`
- Clean up unused replication slots: `SELECT pg_drop_replication_slot('unused_slot');`
- Monitor slot usage: `SELECT * FROM pg_replication_slots;`

## Security Best Practices

### Principle of Least Privilege

- **Don't use superuser accounts** for ETL in production
- **Grant SELECT only on tables** that need replication
- **Use specific database names** instead of template1 or postgres
- **Limit connection sources** with specific IP ranges in pg_hba.conf

### Network Security

- **Always use SSL/TLS** in production: `hostssl` in pg_hba.conf
- **Use certificate authentication** for highest security
- **Restrict network access** with firewalls and VPCs
- **Monitor connections** with log_connections = on

### Operational Security

- **Rotate passwords regularly** for replication users
- **Monitor replication slots** for unused or stalled slots
- **Set up alerting** for replication lag and failures
- **Audit publication changes** in your change management process

## Performance Considerations

### WAL Configuration

```ini
# For high-throughput systems
wal_buffers = 16MB
checkpoint_completion_target = 0.9
wal_writer_delay = 200ms
commit_delay = 1000
```

### Monitoring Queries

Track replication performance:

```sql
-- Monitor replication lag
SELECT 
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
FROM pg_replication_slots;

-- Monitor WAL generation rate
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal;
```

## Next Steps

- **Build your first pipeline** → [First ETL Pipeline](../tutorials/first-pipeline/)
- **Handle schema changes** → [Schema Change Management](schema-changes/)
- **Optimize performance** → [Performance Tuning](performance/)
- **Set up monitoring** → [Debugging Guide](debugging/)

## See Also

- [PostgreSQL Logical Replication Documentation](https://www.postgresql.org/docs/current/logical-replication.html)
- [ETL Architecture](../explanation/architecture/) - Understanding how ETL uses these settings
- [Connection Configuration Reference](../reference/pg-connection-config/) - All available connection options