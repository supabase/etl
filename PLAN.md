Plan: Iceberg Changelog Merger with Puffin-based Secondary Index
A background merge runner that reads raw CDC events from Iceberg changelog tables, uses a Puffin-file-backed secondary index (adapted from moonlink's design) to deduplicate and merge events, and produces clean mirror tables. Implemented as a standalone binary (etl-iceberg-merger) that runs periodically in Kubernetes to process batches of changelog records.

Steps
Create new binary crate etl-iceberg-merger with structure matching etl-replicator: main.rs with jemalloc/tracing/sentry setup, config.rs for merger configuration (Iceberg connection, table list, batch size, interval), and core merge orchestration module.

Implement Puffin-based index module by adapting moonlink's GlobalIndex structure: replace memory-mapped local files with Iceberg FileIO, serialize index blocks to Parquet format within Puffin files, store index metadata (hash_bits, num_rows) in Puffin file properties, implement IndexBuilder::build_from_changelog() to scan Parquet files and create PK hash → (file_path, row_offset) mappings.

Implement merge/compaction logic that loads Puffin index from changelog table metadata, scans changelog Parquet files using index for deduplication (keeping latest by cdc_sequence_number), builds compacted mirror Parquet files (INSERT/UPDATE → row upsert, DELETE → row omission), writes new Puffin index for mirror table, and commits Iceberg transaction to swap manifest.

Build scheduler loop in main.rs that runs on fixed time interval (configurable), iterates through configured tables sequentially, auto-creates mirror tables with schema derived from changelog (minus CDC columns) on first run, executes merge for each table, handles errors with retry logic and metrics emission.

Add Dockerfile and K8s manifests following Dockerfile pattern, create CronJob manifest for periodic execution, add configuration via ConfigMap/Secret for Iceberg credentials and table list, include resource limits and monitoring labels.

Implement observability using existing etl-telemetry patterns: emit metrics for rows processed, merge duration, index size, changelog files processed; add structured logging with table_id and batch_id context; integrate Sentry for error reporting.

Further Considerations
Index compaction strategy? As mirror tables grow, Puffin index files will accumulate. Should the merger periodically rebuild/merge index files (similar to moonlink's build_from_merge), or rely on Iceberg's snapshot expiration to clean old indices?

Partial failure handling? If merge succeeds but Puffin index write fails, should the merger retry the entire batch, or track progress checkpoints (e.g., last processed cdc_sequence_number) to resume?

Schema evolution support? When source table schema changes (columns added/removed), should the merger detect schema drift and automatically propagate changes to mirror tables, or require manual intervention?