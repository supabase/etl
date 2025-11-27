# Plan: Add Postgres TCP Keepalives

## Summary

Add TCP-level keepalive configuration to `PgReplicationClient` to improve connection reliability, especially for connections through NAT gateways or load balancers that may drop idle connections.

## Background

The etl codebase currently uses application-level Standby Status Update messages (~10 seconds) for connection health, but lacks TCP-level keepalives. The `pg_replicate` fork added these settings:
```rust
.keepalives(true)
.keepalives_idle(Duration::from_secs(30))
.keepalives_interval(Duration::from_secs(30))
.keepalives_retries(3)
```

TCP keepalives provide network-level dead connection detection that complements the existing application-level heartbeats.

## Files to Modify

1. `etl-config/src/shared/connection.rs` - Add keepalive configuration struct
2. `etl/src/replication/client.rs` - Apply keepalive settings during connection

## Implementation Steps

### Step 1: Add Keepalive Configuration

Create a new `TcpKeepaliveConfig` struct in `etl-config/src/shared/connection.rs`:

```rust
/// TCP keepalive configuration for Postgres connections.
///
/// TCP keepalives provide network-level connection health checking which can
/// detect dead connections faster than application-level heartbeats alone.
/// This is especially useful for connections through NAT gateways or load
/// balancers that may drop idle connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpKeepaliveConfig {
    /// Time in seconds a connection must be idle before sending keepalive probes.
    pub idle_secs: u64,
    /// Time in seconds between individual keepalive probes.
    pub interval_secs: u64,
    /// Number of keepalive probes to send before considering the connection dead.
    pub retries: u32,
}

impl Default for TcpKeepaliveConfig {
    fn default() -> Self {
        Self {
            idle_secs: 30,
            interval_secs: 30,
            retries: 3,
        }
    }
}
```

### Step 2: Add to PgConnectionConfig

Add the keepalive field to `PgConnectionConfig` as an `Option`:

```rust
pub struct PgConnectionConfig {
    // ... existing fields ...
    /// TCP keepalive configuration for connection health monitoring.
    /// When `None`, TCP keepalives are disabled (default).
    pub keepalive: Option<TcpKeepaliveConfig>,
}
```

Also add to `PgConnectionConfigWithoutSecrets` for consistency.

### Step 3: Update IntoConnectOptions Implementation

Update the `IntoConnectOptions<TokioPgConnectOptions>` implementation to apply keepalive settings when present:

```rust
impl IntoConnectOptions<TokioPgConnectOptions> for PgConnectionConfig {
    fn without_db(&self) -> TokioPgConnectOptions {
        // ... existing setup ...

        if let Some(keepalive) = &self.keepalive {
            config
                .keepalives(true)
                .keepalives_idle(Duration::from_secs(keepalive.idle_secs))
                .keepalives_interval(Duration::from_secs(keepalive.interval_secs))
                .keepalives_retries(keepalive.retries);
        }

        // ... rest of method ...
    }
}
```

### Step 4: Verify Connection Methods

The `PgReplicationClient::connect_no_tls()` and `connect_tls()` methods in `etl/src/replication/client.rs` use `pg_connection_config.clone().with_db()`, so they will automatically pick up the keepalive settings from the updated `IntoConnectOptions` implementation.

No changes needed in `client.rs` itself since keepalives are applied in the config conversion.

## Testing

1. Unit test: Verify `TcpKeepaliveConfig::default()` values match expected (30s idle, 30s interval, 3 retries)
2. Integration test: Verify connections work with and without keepalives configured
3. Manual test: Confirm keepalive settings are applied via `pg_stat_activity` or network inspection

## Rollout Considerations

- Keepalives are disabled by default (`None`) for backwards compatibility
- To enable, add keepalive config to `PgConnectionConfig`:
  ```toml
  [source.keepalive]
  idle_secs = 30
  interval_secs = 30
  retries = 3
  ```
- Can use `TcpKeepaliveConfig::default()` for standard settings

## References

- `../pg_replicate/FORK_CHANGES.diff` - Original implementation in pg_replicate
- `TODO.md` - Task description
- `AGENTS.md` - Coding guidelines
