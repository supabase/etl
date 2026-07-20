---
title: BigQuery Destination
description: BigQuery-specific replication behavior and limitations.
---

**BigQuery-specific replication behavior and limitations**

BigQuery is ETL's most mature built-in destination. It uses BigQuery change data
capture (CDC) rows to apply inserts, updates, and deletes.

## Primary-Key Changes

Changing a primary-key value to one that is not currently used by another row
is supported. ETL represents the change as a BigQuery CDC delete for the old
key followed by an upsert for the new key.

Directly swapping or cyclically reusing primary-key values between rows is not
supported. For example, changing one row from `1` to `2` while changing another
row from `2` to `1` produces independent BigQuery mutation histories for keys
`1` and `2`. BigQuery does not apply those mutations as one atomic cross-key
operation, so the resulting table can be inconsistent.

PostgreSQL normally rejects a direct swap for a non-deferrable primary key. A
swap can still occur when uniqueness checking is deferred and the table uses
`REPLICA IDENTITY FULL` for logical replication.

Use immutable primary keys, move rows through a temporary unused key, or
resynchronize the table after performing such a rewrite.

## Related Documentation

- [Event Types](/etl/explanation/events/): The source event contract consumed by destinations.
- [Schema Changes](/etl/explanation/schema-changes/): BigQuery schema-change behavior and limitations.
