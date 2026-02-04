# ETL-Merger Demo

This demo shows the `etl-merger` in action, merging CDC (Change Data Capture) events from a changelog table into a mirror table using Apache Iceberg.

## Infrastructure Requirements

The demo requires:

- PostgreSQL (source database) - port 5430
- Lakekeeper (Iceberg catalog) - port 8182
- MinIO (object storage) - port 9010

These should already be running via docker-compose.

## Demo Flow

### 1. Setup: Create Changelog with Initial Data

```bash
cargo run --release --features iceberg,test-utils -p etl-merger --bin demo_setup
```

This creates:

- A new Lakekeeper warehouse
- Changelog table (`changelog.demo_users_changelog`)
- 3 INSERT events simulating Postgres replication:
  - id=1, Alice, value=100
  - id=2, Bob, value=200
  - id=3, Carol, value=300

**Save the warehouse name** from the output:

```bash
export DEMO_WAREHOUSE="<warehouse-id-from-output>"
```

### 2. First Merge: Process Initial Inserts

```bash
cargo run -p etl-merger --bin etl-merger --release --features iceberg -- \
  --catalog-url http://localhost:8182/catalog \
  --warehouse "$DEMO_WAREHOUSE" \
  --changelog-namespace changelog \
  --mirror-namespace mirror \
  --changelog-table demo_users_changelog \
  --mirror-table demo_users \
  --s3-endpoint http://localhost:9010 \
  --s3-access-key-id minio-admin \
  --s3-secret-access-key minio-admin-password
```

The merger will:

- Create the mirror table (`mirror.demo_users`)
- Build an in-memory index (initially empty)
- Process 3 INSERT events
- Insert 3 rows into the mirror table

### 3. Query the Results

```bash
DEMO_WAREHOUSE="$DEMO_WAREHOUSE" cargo run --release --features iceberg,test-utils -p etl-merger --bin demo_query
```

Shows metadata for both changelog and mirror tables.

### 4. Add More CDC Events (UPDATE, DELETE, INSERT)

```bash
DEMO_WAREHOUSE="$DEMO_WAREHOUSE" cargo run --release --features iceberg,test-utils -p etl-merger --bin demo_add_events
```

Adds:

- UPDATE: Alice's value 100 → 150
- DELETE: Bob removed
- INSERT: Dave added with value=400

### 5. Second Merge: Process Incremental Changes

Run the merger again (same command as step 2):

```bash
cargo run -p etl-merger --bin etl-merger --release --features iceberg -- \
  --catalog-url http://localhost:8182/catalog \
  --warehouse "$DEMO_WAREHOUSE" \
  --changelog-namespace changelog \
  --mirror-namespace mirror \
  --changelog-table demo_users_changelog \
  --mirror-table demo_users \
  --s3-endpoint http://localhost:9010 \
  --s3-access-key-id minio-admin \
  --s3-secret-access-key minio-admin-password
```

The merger will:

- Build index from existing mirror table (3 rows)
- Process only NEW events (checkpoint-based)
- Handle UPDATE using deletion vectors + insert
- Handle DELETE using deletion vectors
- Handle INSERT normally

### 6. Query Final State

```bash
DEMO_WAREHOUSE="$DEMO_WAREHOUSE" cargo run --release --features iceberg,test-utils -p etl-merger --bin demo_query
```

Expected logical state:

- Alice: id=1, value=150 (updated)
- Carol: id=3, value=300 (unchanged)
- Dave: id=4, value=400 (new)
- Bob: deleted (no longer visible)

## Key Features Demonstrated

1. **Changelog to Mirror**: CDC events → queryable mirror table
2. **Incremental Merging**: Checkpoint-based resumption
3. **Index Building**: Reconstructs in-memory index from existing data
4. **UPDATE Operations**: Deletion vectors + new row insertion
5. **DELETE Operations**: Deletion vectors (copy-on-write avoidance)
6. **Iceberg Integration**: Apache Iceberg format for analytics

## Resetting the Demo

To run the demo again from scratch:

```bash
# Just run demo_setup again - it creates a new warehouse each time
cargo run --release --features iceberg,test-utils -p etl-merger --bin demo_setup
```

Each run creates an isolated warehouse, so no cleanup is needed between runs.
