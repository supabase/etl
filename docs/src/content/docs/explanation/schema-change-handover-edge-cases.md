---
title: Schema-Change Handover Limitation
description: The unsupported DDL and relation ordering during table-sync catch-up.
---

ETL transfers ownership of a table from its table-sync worker to the main apply
worker through the durable `SyncDone` state. A complete handover contains:

- the table schema snapshot ID;
- the publication mask, which identifies the columns pgoutput sends; and
- the replica-identity mask, which identifies the columns used to decode old
  row keys.

Together, these values are enough to reconstruct the
`ReplicatedTableSchema` used to decode the first row owned by the apply worker.
The implementation currently persists only this complete form. An incomplete
handover is intentionally rejected.

## DDL Without a Following Relation During Catch-Up

A transactional DDL message is self-describing for the physical table schema,
but it does not contain the exact pgoutput publication and replica-identity
masks. PostgreSQL supplies those masks in a later `RELATION` message. pgoutput
normally emits that relation immediately before the first row change that needs
it, not merely because the DDL message was decoded.

The problematic ordering is:

```text
table-sync worker copies the initial table
table-sync worker streams WAL during catch-up
DDL or publication-column change is decoded during catch-up
no row for that table is decoded
the table-sync worker reaches its SyncDone boundary
```

At the boundary, ETL knows the new schema snapshot ID but has not received a
relation for that snapshot. Reusing the previous masks is unsafe:

- a publication change may have added or removed replicated columns;
- a physical column add, drop, or rename may have changed mask positions; and
- a replica-identity change may have changed the old-row key.

Reading the current PostgreSQL catalog is also not equivalent to decoding the
historical relation. The catalog can already contain changes that occurred
after the handover boundary, so it does not necessarily describe the WAL
position being handed over.

For now, ETL fails the table sync before writing `SyncDone` when catch-up
reaches its handover boundary while the local decoding state is still waiting
for a relation. It does not persist a partial handover and does not guess the
masks. Operators should avoid running `ALTER TABLE` or supported `ALTER
PUBLICATION` operations that affect a table while that table is in initial
sync. If this state is reached, the table must be manually retried or
resynchronized after the schema activity has settled.

A future solution would need one of the following:

1. include the exact publication and identity masks in the transactional schema
   message;
2. durably preserve enough historical relation state to materialize the masks;
   or
3. keep the table-sync worker alive until PostgreSQL emits a matching relation,
   with a defined timeout and operator recovery path.

## Supported Handover Paths

This limitation does not affect the ordinary paths:

- the initial copy seeds snapshot `0/0` together with its publication and
  identity masks;
- a DDL followed by the relation required for later row decoding produces a
  complete handover using the DDL message LSN as its snapshot ID; and
- once the table is `Ready`, ordinary DDL, relation, and row ordering is
  handled by the main apply worker without a table-sync ownership transfer.

`SyncDone` keeps the complete handover until the apply worker has both
materialized a connection-local decoder and persisted an apply checkpoint at or
beyond the handover LSN. An eventless handover therefore remains in `SyncDone`;
the first apply-owned relation materializes its schema directly, while the first
relation-less DML restores the stored decoder on demand. Only then can `Ready`
discard the stored decoder. This keeps the supported paths auditable and makes
the catch-up ordering above fail closed instead of constructing schema state
that may not match the WAL being decoded.
