# Explanations

**Understand how ETL works and why**

## Where to Start

**New to Postgres logical replication?** Start with [Postgres Replication Concepts](concepts.md). It explains WAL, publications, replication slots, and why ETL works the way it does.

**Already familiar with replication?** Jump to [Architecture](architecture.md) to understand ETL's two-phase approach and delivery guarantees.

## All Topics

1. **[Postgres Replication Concepts](concepts.md)**: The fundamentals - WAL, publications, slots, pgoutput, and why two phases.

2. **[Architecture](architecture.md)**: How ETL works - initial copy, streaming, workers, and delivery guarantees.

3. **[Event Types](events.md)**: All events your destination receives - Insert, Update, Delete, Begin, Commit, and their fields.

4. **[Extension Points](traits.md)**: The traits you implement - Destination, SchemaStore, StateStore, CleanupStore.

## Next Steps

When you're ready to build, head to the [Guides](../guides/index.md).
