---
title: Schema Changes During Initial-Sync Handover
description: Public-beta behavior and a known schema-change edge case during table-sync catch-up.
---

Schema-change replication is in public beta. We are expanding support
incrementally and prioritizing decoding correctness while we evaluate the
cleanest long-term designs. Common schema-change paths are supported, but a few
known edge cases remain around ownership handover during initial sync.

This page documents one such edge case so that users can plan around the current
behavior. ETL stops the affected table sync rather than guessing schema metadata
that may not match the WAL being decoded. We are working to find a clean
solution, and suggestions or sanitized minimal reproductions are welcome in
GitHub issues.

ETL transfers ownership of a table from its table-sync worker to the main apply
worker through the durable `SyncDone` state. A complete handover contains:

- the table schema snapshot ID;
- the publication mask, which identifies the columns pgoutput sends; and
- the replica-identity mask, which identifies the columns used to decode old
  row keys.

Together, these values are enough to reconstruct the
`ReplicatedTableSchema` used to decode the first row owned by the apply worker.
The implementation persists only this complete form. An incomplete handover is
intentionally rejected to protect decoding correctness.

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

ETL currently fails the table sync before writing `SyncDone` when catch-up
reaches its handover boundary while the local decoding state is still waiting
for a relation. It does not persist a partial handover and does not guess the
masks.

Avoid running `ALTER TABLE` or supported `ALTER PUBLICATION` operations that
affect a table while that table is in initial sync. If this edge case occurs,
retry or resynchronize the table after the schema activity has settled.

## Why This Remains in Beta

Recovering the exact historical schema while ownership moves between
replication workers is a difficult coordination problem. Possible solutions
introduce different protocol, durable-state, and lifecycle tradeoffs. Rather
than add a fragile special case, we are evaluating the cleanest approach while
rolling out schema-change support step by step.

Options under evaluation include:

1. including the exact publication and identity masks in the transactional
   schema message;
2. durably preserving enough historical relation state to materialize the
   masks; or
3. keeping the table-sync worker alive until PostgreSQL emits a matching
   relation, with a defined timeout and recovery path.

This list is not exhaustive. If you encounter this behavior or have suggestions
for handling it, please open a GitHub issue with a sanitized description and
minimal reproduction. Schema-change support is actively improving, and feedback
from beta usage helps us prioritize the remaining edge cases.

## Supported Handover Paths

This limitation does not affect the ordinary paths:

- the initial copy seeds snapshot `0/0` together with its publication and
  identity masks;
- a DDL followed by the relation required for later row decoding produces a
  complete handover using the DDL message LSN as its snapshot ID; and
- once the table is `Ready`, supported schema changes, relation messages, and
  row ordering are handled by the main apply worker without a table-sync
  ownership transfer.

`SyncDone` keeps the complete handover until the apply worker has both
materialized the current connection's decoding state as `WithSchema` and
persisted an apply checkpoint at or beyond the handover LSN. Reaching
`WithSchema` means that this connection either received a relation or restored
the complete `SyncDone` decoder while handling relation-less DML. Only then can
`Ready` discard the durable `SyncDone` decoder. This keeps the supported paths
auditable and makes the catch-up ordering above fail closed instead of
constructing schema state that may not match the WAL being decoded.

Remaining in `SyncDone` indefinitely is intentional when no apply-owned
relation or row arrives. Initial copy and catch-up are already durably complete
in this state, and the first future row can still be streamed; retaining it only
preserves the decoder needed if that row arrives without a preceding relation.
ETL does not add a separate retained-decoder lifecycle solely to make an idle
table display `Ready`, because doing so would add state without improving
decoding correctness.
