# Scope-consistent replication mode

`pipeline::ScopePipeline` is an opt-in mode for destinations that need a complete
consistent source cut across a selected table scope. Ordinary per-table bootstrap
and catch-up callbacks cannot establish that guarantee for the destination.

The engine exports one snapshot, imports it into every reader, freezes a disjoint
partition plan, and seals the snapshot only after every partition (including empty
tables) completes at the common cut. Streaming then emits complete source
transactions and explicitly qualified quiet intervals. Physical WAL heads and
server keepalives do not establish complete input.

Readers scan their frozen partitions concurrently with one bounded page and one
lookahead row per reader. Destination callbacks remain serialized. Source FETCH
counts bound wire batches; encoded byte counts bound retained pages across
multiple batches. Partition completion remains exact at the common cut. A reader
or destination failure cancels the cohort before sealing.

The embedding host supplies a `SourceFactory` for secured transports and a
`CaptureDestination` for provisional snapshot pages and complete capture units.
Only `seal` transfers durability responsibility. The returned position must name
that exact complete unit and mean its data, schema and ordering can be recovered.
Feedback cannot exceed that destination-confirmed durable position.

Execution authority and feedback authority are checked separately, including after
sealing. An embedding host can complete its capture lease in the same commit as
the receipt, retain input independently of its query visibility, and stop reading
when its own custody budget fills. A lost acknowledgement resumes from the same
durable frontier and owned slot. A missing slot, changed source identity/timeline,
unavailable WAL or acknowledgement beyond retained input refuses continuity.

This is an additive mode. It shares the existing connection driver, replication
command path, guarded pgoutput parser, feedback payload encoding and schema-message
interpretation with ordinary synchronization. Common-cut bootstrap and transaction
assembly are mode-specific. Address policy, credentials, retention, business
admission and query visibility belong to the embedding destination.

The `scope` integration test target uses in-memory PostgreSQL protocol peers. It
covers allocation guards, complete transaction boundaries, publication scoping,
common-snapshot partition completeness, large byte-packed baselines, concurrent
readers, empty partitions, oversized rows, connection lifetime, exact quiet markers,
feedback ceilings and preservation of typed schema refusals. It requires no
PostgreSQL server or external destination.

This initial interface is deliberately a draft for upstream discussion. In
particular, the configuration and error surface should be reviewed alongside the
existing pipeline builders and destination/store contracts. These tests do not
qualify live PostgreSQL restart, failover or durable destination storage.
