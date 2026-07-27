---
title: Schema-Change Handover Edge Cases
description: Rejected and currently unsupported cases at the table-sync and apply-worker handover.
---

ETL transfers ownership of a table from its table-sync worker to the main apply
worker through the durable `SyncDone` state. A complete handover contains:

- the table schema snapshot id;
- the publication mask, which identifies the columns pgoutput sends; and
- the replica-identity mask, which identifies the columns used to decode old
  row keys.

Together, these values are enough to reconstruct the
`ReplicatedTableSchema` used to decode the first row owned by the apply worker.
The implementation currently persists only this complete form. An incomplete
handover is intentionally rejected.

## DDL Without a Following Relation

A transactional DDL message is self-describing for the physical table schema,
but it does not contain the exact pgoutput publication and replica-identity
masks. PostgreSQL supplies those masks in a later `RELATION` message. pgoutput
normally emits that relation immediately before the first row change that needs
it, not merely because the DDL message was decoded.

The problematic ordering is:

```text
table-sync worker copies the initial table
DDL or publication-column change is decoded
no row for that table is decoded
the table-sync worker reaches its SyncDone boundary
```

At the boundary, ETL knows the new schema snapshot id but has not received a
relation for that snapshot. Reusing the previous masks is unsafe:

- a publication change may have added or removed replicated columns;
- a physical column add, drop, or rename may have changed mask positions; and
- a replica-identity change may have changed the old-row key.

Reading the current PostgreSQL catalog is also not equivalent to decoding the
historical relation. The catalog can already contain changes that occurred
after the handover boundary, so it does not necessarily describe the WAL
position being handed over.

For now, ETL fails the table sync before writing `SyncDone` when its local
decoding state is still waiting for a relation. It does not persist a partial
handover and does not guess the masks. Operators should avoid running `ALTER
TABLE` or `ALTER PUBLICATION` operations that affect a table while that table
is in initial sync. If this state is reached, the table must be manually retried
or resynchronized after the schema activity has settled.

A future solution would need one of the following:

1. include the exact publication and identity masks in the transactional schema
   message;
2. durably preserve enough historical relation state to materialize the masks;
   or
3. keep the table-sync worker alive until PostgreSQL emits a matching relation,
   with a defined timeout and operator recovery path.

## Restart Before the Ready Progress Boundary

`SyncDone` records the complete decoder state at the ownership boundary. The
apply worker later installs that state in its local decoding map and replaces
`SyncDone` with `Ready`. The `Ready` state does not retain the handover payload.

After the destination acknowledges a batch as durable, ETL replaces `SyncDone`
with `Ready` before it stores the resulting apply-worker progress. A crash can
therefore leave this state:

```text
SyncDone stores schema state X at handover LSN H
the destination durably applies through restart position P
ETL stores Ready
the process crashes before storing durable apply progress P
```

Normally, replay before `H` only redelivers already durable rows, which is
allowed by ETL's at-least-once contract. Schema selection has one additional
case. The copy-time schema is stored with bootstrap snapshot id `0/0`, even
when the PostgreSQL snapshot was taken after a DDL at WAL position `X`. If
restart resumes before `X`, an older relation can be paired with that newer
bootstrap schema before replay reaches the DDL.

This is not currently supported and is not guaranteed to fail closed. If the
older relation's columns are a compatible subset of the newer `0/0` schema,
ETL can build a different replication mask for the same snapshot id. A
destination may interpret that as a forward schema change and remove columns
that were present in the copy. Replaying the later DDL can recreate the columns
but cannot restore copied values that the reverse change removed. Same-named
type changes can also decode a row using the wrong physical type. Other changes
fail when the relation contains a column that does not exist in the selected
schema.

The affected table must be reset and resynchronized even if replay later
appears to converge, because ETL cannot prove that no values were lost in the
intermediate schema application.

Persisting `Ready` before apply progress preserves the core durability
invariant: ETL never advances its restart position before the destination has
made the corresponding data durable. Closing this narrower schema window
requires retaining the handover boundary and schema state after `Ready`, or
atomically storing the `Ready` transition with apply progress. That recovery
work is intentionally outside the current handover implementation.

## Supported Handover Paths

These edge cases do not affect the ordinary paths:

- the initial copy seeds snapshot `0/0` together with its publication and
  identity masks;
- a DDL followed by the relation required for later row decoding produces a
  complete handover using the DDL message LSN as its snapshot id; and
- once the table is `Ready`, ordinary DDL, relation, and row ordering is
  handled by the main apply worker without a table-sync ownership transfer.

Keeping the durable handover complete and single-shaped makes these supported
paths auditable. The exceptional cases remain explicit instead of silently
constructing a schema state that may not match the WAL being decoded.
