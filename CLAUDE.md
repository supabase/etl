# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETL is a Rust framework for building production-grade, fault-tolerant PostgreSQL replication applications. It provides a high-level API on top of PostgreSQL's logical streaming replication protocol for building ETL pipelines, CDC systems, and data synchronization solutions.

## Key Commands

### Development
```bash
# Build all crates
cargo build

# Run tests (requires PostgreSQL with logical replication)
cargo test

# Run specific test
cargo test --package etl --test integration_tests test_name

# Run with debug logging
RUST_LOG=debug cargo run --bin etl-replicator

# Format code
cargo fmt

# Lint code
cargo clippy --all-targets --all-features -- -D warnings

# Check types
cargo check --all-targets --all-features
```

### Database Setup
```bash
# Run API migrations (from etl-api directory)
sqlx migrate run

# Create test database with logical replication
psql -c "ALTER SYSTEM SET wal_level = logical;"
psql -c "SELECT pg_reload_conf();"
```

### Docker
```bash
# Build images
docker build -f docker/Dockerfile.api -t etl-api .
docker build -f docker/Dockerfile.replicator -t etl-replicator .

# Run with docker-compose
docker-compose up
```

## Architecture

### Crate Structure
The workspace consists of specialized crates that work together:

- **etl**: Core framework with pipeline orchestration, workers, and state management
- **etl-api**: Multi-tenant REST API service for managing pipelines  
- **etl-replicator**: Standalone binary for running single pipelines
- **etl-postgres**: PostgreSQL utilities and replication protocol implementation
- **etl-destinations**: Destination implementations (currently BigQuery)
- **etl-config**: Shared configuration types across crates
- **etl-telemetry**: Observability with tracing and metrics

### Pipeline Architecture

The pipeline consists of coordinated workers:

1. **Apply Worker** (`etl/src/apply_worker.rs`): Main CDC event processor that:
   - Consumes replication stream
   - Dispatches table sync operations
   - Manages transaction boundaries
   - Handles schema changes

2. **Table Sync Workers** (`etl/src/table_sync_worker.rs`): Parallel workers for initial data copy:
   - Each worker handles one table
   - Paginated copying with resumable state
   - Coordinates with apply worker for consistency

3. **State Management** (`etl/src/state_store/`): Persistent state for resumability:
   - Replication positions (LSN tracking)
   - Table sync progress
   - Schema mappings
   - Transaction boundaries

4. **Destination Interface** (`etl/src/destination.rs`): Plugin architecture for targets:
   - Begin/commit transaction semantics
   - Schema evolution support
   - Batch operations for efficiency

### Key Design Patterns

- **Worker Pool Pattern**: Fixed-size pools for table sync operations with backpressure
- **State Store Pattern**: All progress is persistently tracked for crash recovery
- **Pipeline Lifecycle**: Clear initialization → running → shutdown phases
- **Error Classification**: Retryable vs fatal errors with exponential backoff
- **Schema Evolution**: Automatic detection and propagation of DDL changes

### Testing Strategy

- Unit tests for individual components
- Integration tests requiring real PostgreSQL (`tests/integration_tests.rs`)
- Failpoint tests for error injection (`#[cfg(feature = "failpoints")]`)
- Example pipelines in `etl-examples/` for reference implementations

## Important Considerations

- All workers coordinate through channels (`tokio::sync::mpsc`)
- LSN (Log Sequence Number) tracking is critical for resumability
- Schema changes are detected via `pg_catalog` queries
- Destinations must implement idempotent operations
- The API service uses SQLx for async PostgreSQL operations
- Metrics are exposed via Prometheus format on `/metrics` endpoint