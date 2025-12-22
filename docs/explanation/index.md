# Explanations

**Deep dives into ETL concepts and design**

## Topics

### [Architecture](architecture.md)

How ETL replicates data from Postgres to destinations. Covers the two-phase approach (initial copy + streaming), workers, and component interactions.

### [Event Types](events.md)

All events delivered to your destination: Insert, Update, Delete, Begin, Commit, Relation, Truncate. Includes LSN field explanations and Begin/Commit behavior during initial copy.

### [Extension Points](traits.md)

The four traits you implement to customize ETL: Destination, SchemaStore, StateStore, CleanupStore. Full method documentation and implementation patterns.

## Reading Guide

| Goal | Start Here |
|------|------------|
| Understand how ETL works | [Architecture](architecture.md) |
| Build a custom destination | [Extension Points](traits.md), [Event Types](events.md) |
| Build a custom store | [Extension Points](traits.md) |

## Next Steps

- [Tutorials](../tutorials/index.md) - Build something
- [How-To Guides](../how-to/index.md) - Solve specific problems
