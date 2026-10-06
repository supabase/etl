# `etl`

Core library for [Supabase ETL](https://supabase.github.io/etl/). It exposes the
public pipeline, configuration, destination, store, schema, event, and row-data
APIs used to embed Postgres logical replication in a Rust application.

Start with the [First Pipeline](https://supabase.github.io/etl/guides/first-pipeline/)
tutorial, then see [Extension Points](https://supabase.github.io/etl/explanation/traits/)
when you implement a store or destination.

## Features

| Feature      | Description                           |
| ------------ | ------------------------------------- |
| `test-utils` | Enables testing utilities and helpers |
| `failpoints` | Enables failure injection for testing |
| `egress`     | Enables structured billing usage logs |

## Architecture

The crate runs one pipeline per publication in two phases:

1. **Initial sync:** Copy the existing rows selected by the publication, then
   catch up changes that occurred while the copy was running.
2. **Ongoing replication:** Capture subsequent inserts, updates, deletes, and
   truncates, then deliver those changes as ordered events.

Copy and change data capture (CDC) are replication paths, not customer-visible
phases. See the [architecture overview](https://supabase.github.io/etl/explanation/architecture/)
for the worker model.

### Initial-copy locks and beta schema-change handling

The parent transaction locks each copy unit and its descendants in
`ACCESS SHARE` mode **before** the slot snapshot and retains those locks until
all source copy workers finish. Workers import the snapshot and acquire their
own locks with `NOWAIT`, retaining them across chunks. A conflict starts a fresh
copy under the configured retry policy.

For partitioned tables, the unit is the published root/subtree when
`publish_via_partition_root = true`, otherwise an individual leaf. Ordinary
tables are separate units. Locks never climb to ancestors.

On a primary, normal DML remains compatible, while `ACCESS EXCLUSIVE` DDL can
wait and delay later queries. Ordinary vacuum can run, but the snapshot may
prevent cleanup. On a standby, locks affect local WAL replay and can lead to
recovery-conflict cancellation; they do not lock tables on the primary.

The 30-second copy lock timeout bounds individual lock acquisitions, not lock
lifetime or how long migration sessions wait. ETL does not automatically stop a
copy to release queued application traffic. Long copies and slow destinations
extend snapshot and WAL retention; resuming an interrupted copy requires a fresh
snapshot. `max_slot_wal_keep_size` bounds slot retention at checkpoints, not
snapshot-related bloat or total disk usage. Losing a table-sync slot during
copy requires a table reset and fresh initial sync. Retained slots continue
holding WAL while the pipeline is stopped. See [Planning long initial copies](../../site/content/docs/guides/configure-postgres.mdx#planning-long-initial-copies)
for migration scheduling, storage, and monitoring guidance.

Schema-change handling is in **public beta**. The trigger captures an
unprojected schema; each pipeline combines it with its replication mask.
This also turns publication column-list changes into logical schema changes.
Copy locks do not fix catalog-helper visibility, stale concurrent-DDL snapshots,
or relation-mask ambiguity. See [Schema Changes](../../site/content/docs/explanation/schema-changes.mdx)
for guarantees, retries, and operating cautions, and the [source migration notes](migrations/README.md#schema-capture-contract-and-limitations)
for the trigger boundary.

### Key Components

- **Pipeline**: Main orchestrator that manages the replication process
- **Postgres Client**: Connects to Postgres's logical replication protocol
- **Apply Worker**: Main runtime worker that starts table sync workers and processes CDC events
- **Table Sync Worker**: Copies existing table data, then processes CDC events until it has caught up to the apply worker
- **State Store**: Stores table state, persisted replication checkpoints, and destination metadata
- **Schema Store**: Stores versioned table schemas and prunes obsolete schema versions after acknowledged progress
- **TableStateLifecycleStore**: Prepares fresh copies, resets resync state, and deletes ETL-owned state for tables removed from a publication

### Information Flow

```mermaid
graph TB
    subgraph "ETL Pipeline"
        Pipeline["Pipeline"]

        ApplyWorker["Apply Worker"]

        subgraph "Worker Pool"
            TSWorker1["Table Sync Worker 1"]
            TSWorkerN["Table Sync Worker N"]
        end

        subgraph "Store"
            StateStore["State Store"]
            SchemaStore["Schema Store"]
            LifecycleStore["Table State Lifecycle Store"]
        end
    end

    PG[("Postgres<br/>Source Database")]

    Destination[("Destination<br/>ClickHouse, etc.")]

    Pipeline --> ApplyWorker

    ApplyWorker --> TSWorker1
    ApplyWorker --> TSWorkerN

    ApplyWorker --> Destination
    TSWorker1 --> Destination
    TSWorkerN --> Destination

    ApplyWorker <--> PG
    TSWorker1 <--> PG

    ApplyWorker <--> StateStore
    ApplyWorker <--> SchemaStore
    ApplyWorker <--> LifecycleStore

    TSWorker1 <--> StateStore
    TSWorker1 <--> SchemaStore
    TSWorker1 <--> LifecycleStore

    TSWorkerN <--> StateStore
    TSWorkerN <--> SchemaStore
    TSWorkerN <--> LifecycleStore
```
