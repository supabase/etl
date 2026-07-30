---
title: Schema Changes During Initial-Sync Handover
description: Public-beta behavior and a known schema-change edge case during table-sync catch-up.
---

Schema-change replication is in public beta. We are expanding support
incrementally while prioritizing decoding correctness. One known edge case
occurs when schema metadata changes while a table-sync worker is handing
ownership to the main apply worker. ETL stops the affected table sync rather
than persist schema metadata that may not match the WAL being decoded.

ETL transfers ownership of a table from its table-sync worker to the main apply
worker through the durable `SyncDone` state. A complete handover contains:

- the table schema snapshot ID;
- the publication mask, which identifies the columns pgoutput sends; and
- the replica-identity mask, which identifies the columns used to decode old
  row keys.

Together, these values are enough to reconstruct the
`ReplicatedTableSchema` used to decode the first row owned by the apply worker.
ETL persists only this complete form.

## Known Edge Case: DDL Without a Following Relation During Catch-Up

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

If catch-up reaches the handover boundary while still waiting for that relation,
ETL fails the table sync before writing `SyncDone`. It does not persist a
partial handover or guess the masks.

Avoid running `ALTER TABLE` or supported `ALTER PUBLICATION` operations that
affect a table while that table is in initial sync. If this edge case occurs,
retry or resynchronize the table after the schema activity has settled.

## Supported Handover Paths

This limitation does not affect the ordinary paths:

- the initial copy seeds snapshot `0/0` together with its publication and
  identity masks;
- a DDL followed by the relation required for later row decoding produces a
  complete handover using the DDL message LSN as its snapshot ID; and
- once the table is `Ready`, supported schema changes, relation messages, and
  row ordering are handled by the main apply worker without a table-sync
  ownership transfer.

`SyncDone` retains the complete decoder until the apply worker has materialized
it for the current connection and persisted a checkpoint at or beyond the
handover LSN. Only then can `Ready` discard the durable decoder.

Remaining in `SyncDone` indefinitely is intentional when no apply-owned
relation or row arrives. Initial copy and catch-up are complete, and the retained
decoder allows the first future row to be processed safely even if PostgreSQL
does not repeat the relation first.
